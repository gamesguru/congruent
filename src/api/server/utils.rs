use conduwuit::{Err, Result, err, implement, info, warn};
use conduwuit_service::Services;
use ruma::{EventId, RoomId, ServerName};

pub(super) struct AccessCheck<'a> {
	pub(super) services: &'a Services,
	pub(super) origin: &'a ServerName,
	pub(super) room_id: &'a RoomId,
	pub(super) event_id: Option<&'a EventId>,
}

#[implement(AccessCheck, params = "<'_>")]
pub(super) async fn check(&self) -> Result {
	let local_server = self.services.globals.server_name();
	let local_is_participant = self
		.services
		.rooms
		.state_cache
		.server_is_participant(local_server, self.room_id)
		.await;

	if !local_is_participant {
		conduwuit::info!(
			origin = self.origin.as_str(),
			room_id = %self.room_id,
			"Refusing to serve state for room we aren't participating in"
		);
		return Err!(Request(NotFound("This server is not participating in that room.")));
	}

	self.services
		.rooms
		.event_handler
		.acl_check(self.origin, self.room_id)
		.await?;

	let world_readable = self
		.services
		.rooms
		.state_accessor
		.is_world_readable(self.room_id)
		.await;

	if world_readable {
		return Ok(());
	}

	let server_is_participant = self
		.services
		.rooms
		.state_cache
		.server_is_participant(self.origin, self.room_id)
		.await;

	if !server_is_participant {
		return Err!(Request(Forbidden(warn!(
			%self.origin,
			%self.room_id,
			"Server is not participating in room and room is not world-readable."
		))));
	}

	if let Some(event_id) = self.event_id {
		let can_see = self
			.services
			.rooms
			.state_accessor
			.server_can_see_event(
				self.origin.to_owned(),
				self.room_id.to_owned(),
				event_id.to_owned(),
			)
			.await;

		if !can_see {
			return Err!(Request(Forbidden(warn!(
				%self.origin,
				%self.room_id,
				?self.event_id,
				"Server is not allowed to see event."
			))));
		}
	}

	Ok(())
}

pub(super) async fn verify_make_membership(
	services: &Services,
	origin: &ServerName,
	room_id: &RoomId,
	user_id: &ruma::UserId,
) -> Result<()> {
	if !services.rooms.metadata.exists(room_id).await {
		return Err!(Request(NotFound("Room is unknown to this server.")));
	}

	let local_server = services.globals.server_name();
	if !services
		.rooms
		.state_cache
		.server_is_participant(local_server, room_id)
		.await
	{
		conduwuit::info!(
			%origin,
			%room_id,
			"Refusing to serve make_* for room we aren't participating in"
		);
		return Err!(Request(NotFound("This server is not participating in that room.")));
	}

	if user_id.server_name() != origin {
		return Err!(Request(Forbidden("Not allowed to act on behalf of another server/user.")));
	}

	services
		.rooms
		.event_handler
		.acl_check(origin, room_id)
		.await?;

	if services.moderation.is_remote_server_forbidden(origin) {
		conduwuit::warn!(
			%origin,
			%user_id,
			%room_id,
			"Server tried joining/knocking/leaving but is globally forbidden. Rejecting.",
		);
		return Err!(Request(Forbidden("Server is banned on this homeserver.")));
	}

	if let Some(server) = room_id.server_name() {
		if services.moderation.is_remote_server_forbidden(server) {
			return Err!(Request(Forbidden("Server is banned on this homeserver.")));
		}
	}

	Ok(())
}

pub(super) async fn verify_send_membership(
	services: &Services,
	origin: &ServerName,
	room_id: &RoomId,
	pdu: &serde_json::value::RawValue,
	expected_membership: ruma::events::room::member::MembershipState,
) -> Result<(
	ruma::OwnedEventId,
	ruma::CanonicalJsonObject,
	ruma::events::room::member::RoomMemberEventContent,
	ruma::RoomVersionId,
	ruma::OwnedUserId,
	ruma::OwnedUserId,
)> {
	if services.moderation.is_remote_server_forbidden(origin) {
		warn!(
			%origin,
			%room_id,
			"Server tried sending membership event but is globally forbidden. Rejecting.",
		);
		return Err!(Request(Forbidden("Server is banned on this homeserver.")));
	}

	if let Some(server) = room_id.server_name() {
		if services.moderation.is_remote_server_forbidden(server) {
			warn!(
				%origin,
				%room_id,
				"Server tried sending membership event to a banned room server. Rejecting.",
			);
			return Err!(Request(Forbidden("Server is banned on this homeserver.")));
		}
	}

	if !services.rooms.metadata.exists(room_id).await {
		return Err!(Request(NotFound("Room is unknown to this server.")));
	}

	let local_server = services.globals.server_name();
	if !services
		.rooms
		.state_cache
		.server_is_participant(local_server, room_id)
		.await
	{
		info!(
			%origin,
			%room_id,
			"Refusing to serve send_* for room we aren't participating in"
		);
		return Err!(Request(NotFound("This server is not participating in that room.")));
	}

	// ACL check origin server
	services
		.rooms
		.event_handler
		.acl_check(origin, room_id)
		.await?;

	let room_version_id = services.rooms.state.get_room_version(room_id).await?;

	let Ok((event_id, value)) =
		conduwuit::matrix::event::gen_event_id_canonical_json(pdu, &room_version_id)
	else {
		return Err!(Request(BadJson("Could not convert event to canonical json.")));
	};

	let event_room_id: ruma::OwnedRoomId = if let Some(room_id_val) = value.get("room_id") {
		serde_json::from_value(room_id_val.clone().into()).map_err(|e| {
			err!(Request(BadJson(warn!("room_id field is not a valid room ID: {e}"))))
		})?
	} else if conduwuit::matrix::state_res::RoomVersion::new(&room_version_id)
		.is_ok_and(|v| v.room_ids_as_hashes)
	{
		room_id.to_owned()
	} else {
		return Err!(Request(BadJson("Event missing room_id property.")));
	};

	if event_room_id != room_id {
		return Err!(Request(BadJson("Event room_id does not match request path room ID.")));
	}

	let event_type: ruma::events::StateEventType = serde_json::from_value(
		value
			.get("type")
			.ok_or_else(|| err!(Request(BadJson("Event missing type property."))))?
			.clone()
			.into(),
	)
	.map_err(|e| err!(Request(BadJson(warn!("Event has invalid state event type: {e}")))))?;

	if event_type != ruma::events::StateEventType::RoomMember {
		return Err!(Request(BadJson(
			"Not allowed to send non-membership state event to membership endpoint."
		)));
	}

	let content: ruma::events::room::member::RoomMemberEventContent = serde_json::from_value(
		value
			.get("content")
			.ok_or_else(|| err!(Request(BadJson("Event missing content property"))))?
			.clone()
			.into(),
	)
	.map_err(|e| err!(Request(BadJson(warn!("Event content is empty or invalid: {e}")))))?;

	if content.membership != expected_membership {
		return Err!(Request(BadJson(
			"Not allowed to send an unexpected membership event to this endpoint."
		)));
	}

	let sender: ruma::OwnedUserId = serde_json::from_value(
		value
			.get("sender")
			.ok_or_else(|| err!(Request(BadJson("Event missing sender property."))))?
			.clone()
			.into(),
	)
	.map_err(|e| err!(Request(BadJson(warn!("sender property is not a valid user ID: {e}")))))?;

	if sender.server_name() != origin {
		return Err!(Request(Forbidden(
			"Not allowed to send membership event on behalf of another server."
		)));
	}

	services
		.rooms
		.event_handler
		.acl_check(sender.server_name(), room_id)
		.await?;

	let state_key: ruma::OwnedUserId = serde_json::from_value(
		value
			.get("state_key")
			.ok_or_else(|| err!(Request(BadJson("Event missing state_key property."))))?
			.clone()
			.into(),
	)
	.map_err(|e| err!(Request(BadJson(warn!("State key is not a valid user ID: {e}")))))?;

	if state_key != sender {
		return Err!(Request(BadJson("State key does not match sender user.")));
	}

	Ok((event_id, value, content, room_version_id, sender, state_key))
}

pub(super) async fn build_membership_template_pdu(
	services: &Services,
	room_id: &RoomId,
	user_id: &ruma::UserId,
	content: ruma::events::room::member::RoomMemberEventContent,
) -> Result<Box<serde_json::value::RawValue>> {
	let state_lock = services.rooms.state.mutex.lock(room_id).await;

	let (_, mut pdu_json) = services
		.rooms
		.timeline
		.create_event(
			conduwuit::matrix::pdu::PduBuilder::state(user_id.to_string(), &content),
			user_id,
			Some(room_id),
			&state_lock,
		)
		.await?;

	drop(state_lock);
	pdu_json.remove("event_id");
	pdu_json.remove("hashes");
	pdu_json.remove("signatures");

	Ok(serde_json::value::to_raw_value(&pdu_json)
		.expect("CanonicalJson can be serialized to JSON"))
}

pub(super) async fn handle_and_send_incoming_pdu(
	services: &Services,
	origin: &ServerName,
	room_id: &RoomId,
	event_id: &EventId,
	value: ruma::CanonicalJsonObject,
	room_version_id: Option<&ruma::RoomVersionId>,
) -> Result<conduwuit_core::pdu::RawPduId> {
	use futures::FutureExt;

	let mutex_lock = services
		.rooms
		.event_handler
		.mutex_federation
		.lock(room_id)
		.await;

	let pdu_id = services
		.rooms
		.event_handler
		.handle_incoming_pdu(origin, room_id, event_id, value, true, room_version_id)
		.boxed()
		.await?
		.ok_or_else(|| err!(Request(InvalidParam("Could not accept as timeline event."))))?;

	drop(mutex_lock);

	services.sending.send_pdu_room(room_id, &pdu_id).await?;

	Ok(pdu_id)
}

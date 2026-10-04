use std::{collections::HashSet, iter::once};

use conduwuit::{debug, info, trace};
use conduwuit_core::{
	Err, Result, err, implement,
	matrix::{event::Event, pdu::PduBuilder},
	utils::{IterStream, ReadyExt},
};
use futures::{FutureExt, StreamExt};
use slipstream::{
	OwnedEventId, OwnedServerName, RoomId, UserId,
	events::{
		TimelineEventType,
		room::member::{MembershipState, RoomMemberEventContent},
	},
};

use super::{ExtractBody, RoomMutexGuard};

/// Creates a new persisted data unit and adds it to a room. This function
/// takes a roomid_mutex_state, meaning that only this function is able to
/// mutate the room state.
#[implement(super::Service)]
#[tracing::instrument(skip(self, state_lock, pdu_builder), level = "trace")]
pub async fn build_and_append_pdu(
	&self,
	pdu_builder: PduBuilder,
	sender: &UserId,
	room_id: Option<&RoomId>,
	state_lock: &RoomMutexGuard,
) -> Result<OwnedEventId> {
	let (pdu, pdu_json) = self
		.create_hash_and_sign_event(pdu_builder, sender, room_id, state_lock)
		.await?;

	let room_id = room_id
		.map(ToOwned::to_owned)
		.or_else(|| pdu.room_id_or_hash())
		.ok_or_else(|| err!(Request(Forbidden("Event has no room_id"))))?;
	if self.services.admin.is_admin_room(&room_id).await {
		self.check_pdu_for_admin_room(&pdu, sender).boxed().await?;
	}

	// If redaction event is not authorized, do not append it to the timeline
	if *pdu.kind() == TimelineEventType::RoomRedaction {
		trace!("Running redaction checks for room {room_id}");
		let room_version_id = self.services.state.get_room_version(&room_id).await?;
		if let Some(redact_id) = pdu.redacts_id(&room_version_id) {
			if !self
				.services
				.state_accessor
				.user_can_redact(&redact_id, pdu.sender(), &room_id, false)
				.await?
			{
				return Err!(Request(Forbidden("User cannot redact this event.")));
			}
		}
	}

	if *pdu.kind() == TimelineEventType::RoomMember {
		trace!("Running room member checks for room {room_id}");
		let content: RoomMemberEventContent = pdu.get_content()?;

		if content.join_authorized_via_users_server.is_some()
			&& content.membership != MembershipState::Join
		{
			return Err!(Request(BadJson(
				"join_authorised_via_users_server is only for member joins"
			)));
		}

		if content
			.join_authorized_via_users_server
			.as_ref()
			.is_some_and(|authorising_user| {
				!self.services.globals.user_is_local(authorising_user)
			}) {
			return Err!(Request(InvalidParam(
				"Authorising user does not belong to this homeserver"
			)));
		}
	}
	if *pdu.kind() == TimelineEventType::RoomCreate {
		trace!("Creating shortroomid for {room_id}");
		self.services
			.short
			.get_or_create_shortroomid(&room_id)
			.await;
	}

	let previous_root_handle = self.services.state.get_room_state_hamt(&room_id).await.ok();

	// We append to state before appending the pdu, so we don't have a moment in
	// time with the pdu without it's state. This is okay because append_pdu can't
	// fail. Only state events mutate the room state; a non-state event must not be
	// routed through append_to_state, which rejects non-state PDUs.
	let (state_root_handle, state_node) = if pdu.state_key().is_some() {
		trace!("Appending {} state for room {room_id}", pdu.event_id());
		self.services
			.state
			.append_to_state(&pdu, &room_id, state_lock, None)
			.await
			.map(|(handle, node)| (handle, Some(node)))?
	} else {
		// A non-state event does not change the room state, so reuse the current
		// room root and do not persist any new HAMT node.
		let root = previous_root_handle
			.clone()
			.ok_or_else(|| err!(Request(NotFound("Room has no state to append"))))?;
		(root, None)
	};

	if let Some(node) = &state_node {
		self.services
			.state_hamt
			.store
			.persist_node_recursive(node.clone());
	}

	trace!("Generating raw ID for PDU {}", pdu.event_id());
	let pdu_id = self
		.append_pdu(
			&pdu,
			pdu_json,
			// Since this PDU references all pdu_leaves we can update the leaves
			// of the room
			once(pdu.event_id()),
			*pdu.kind() != TimelineEventType::RoomMember,
			crate::rooms::timeline::AppendPduContext {
				state_lock,
				room_id: &room_id,
				state_root_handle: Some(state_root_handle.clone()),
				prev_state_root_handle: previous_root_handle,
				advance_current_state: false,
				was_joined_before_state_install: None,
			},
		)
		.boxed()
		.await?;

	// Process admin commands for locally sent events
	if *pdu.kind() == TimelineEventType::RoomMessage {
		let content: ExtractBody = pdu.get_content()?;
		if let Some(body) = content.body {
			if let Some(source) = self
				.services
				.admin
				.is_admin_command(&pdu, &body, true)
				.await
			{
				self.services.admin.command_with_sender(
					body,
					Some(pdu.event_id().into()),
					source,
					pdu.sender.clone().into(),
				)?;
			}
		}
	}

	// The room pointer is advanced inside `append_pdu`'s atomic batch for state
	// events, together with the timeline write, so a flush can never expose the
	// PDU without its state root. Only a non-state event still needs it set here.
	if !crate::rooms::state::is_state_event(&pdu) {
		self.services.globals.with_cork_and_flush(|| {
			self.services
				.state
				.set_room_state_hamt(&room_id, &state_root_handle, state_lock);
		});
	}

	let mut servers: HashSet<OwnedServerName> = self
		.services
		.state_cache
		.room_servers(&room_id)
		.map(ToOwned::to_owned)
		.collect()
		.await;

	// In case we are kicking or banning a user, we need to inform their server of
	// the change
	//
	// This block's tracing is `debug!`, not `info!`: it runs unconditionally on
	// every locally-created PDU (not just RoomMember ones), and `?servers`
	// formats the whole destination set. At `info!` it would run at full cost
	// on every send in the default log config (`info,memory_serve=warn` in
	// release builds, `debug` in dev builds) -- `debug!` keeps it opt-in via
	// the `membership_destination_debug` target (e.g.
	// `RUST_LOG=membership_destination_debug=debug`) without paying that cost
	// by default.
	debug!(
		target: "membership_destination_debug",
		event_id = %pdu.event_id(), kind = ?pdu.kind(), state_key = ?pdu.state_key,
		room_servers_count = servers.len(),
		"build_and_append_pdu: pre-special-case servers"
	);
	if *pdu.kind() == TimelineEventType::RoomMember {
		if let Some(state_key_uid) = &pdu
			.state_key
			.as_ref()
			.and_then(|state_key| UserId::parse(state_key.as_str()).ok())
		{
			debug!(
				target: "membership_destination_debug",
				event_id = %pdu.event_id(), %state_key_uid,
				"build_and_append_pdu: inserting affected user's server as destination"
			);
			servers.insert(state_key_uid.server_name().to_owned());
		} else {
			debug!(
				target: "membership_destination_debug",
				event_id = %pdu.event_id(), state_key = ?pdu.state_key,
				"build_and_append_pdu: RoomMember event but state_key didn't parse as a UserId"
			);
		}
	}

	// Remove our server from the server list since it will be added to it by
	// room_servers() and/or the if statement above
	servers.remove(self.services.globals.server_name());

	debug!(
		target: "membership_destination_debug",
		event_id = %pdu.event_id(), final_servers = ?servers,
		"build_and_append_pdu: final destination set"
	);
	trace!("Sending PDU {} to {} servers", pdu.event_id(), servers.len());
	let num_sent = self
		.services
		.sending
		.send_pdu_servers(servers.iter().map(AsRef::as_ref).stream(), &pdu_id)
		.await?;

	if num_sent > 0 {
		let _span = tracing::info_span!(
			"broadcast",
			event_id = %pdu.event_id(),
			%room_id,
			servers = num_sent,
		)
		.entered();
		info!("Sending to federation");
	}

	trace!("Event {} in room {:?} has been appended", pdu.event_id(), room_id);
	Ok(pdu.event_id().to_owned())
}

/// Assert invariants about the admin room, to prevent (for example) all admins
/// from leaving or being banned from the room
#[implement(super::Service)]
#[tracing::instrument(skip_all, level = "debug")]
async fn check_pdu_for_admin_room<Pdu>(&self, pdu: &Pdu, sender: &UserId) -> Result
where
	Pdu: Event + Send + Sync,
{
	match pdu.kind() {
		| TimelineEventType::RoomEncryption => {
			return Err!(Request(Forbidden(error!("Encryption not supported in admins room."))));
		},
		| TimelineEventType::RoomMember => {
			let target = pdu
				.state_key()
				.filter(|v| v.starts_with('@'))
				.unwrap_or(sender.as_str());

			let server_user = &self.services.globals.server_user.to_string();

			let content: RoomMemberEventContent = pdu.get_content()?;
			let room_id = pdu
				.room_id_or_hash()
				.ok_or_else(|| err!(Request(Forbidden("Event has no room_id"))))?;

			match content.membership {
				| MembershipState::Leave => {
					if target == server_user {
						return Err!(Request(Forbidden(error!(
							"Server user cannot leave the admins room."
						))));
					}

					let count = self
						.services
						.state_cache
						.room_members(&room_id) // Avoid redundant re-evaluation
						.ready_filter(|user| self.services.globals.user_is_local(user))
						.ready_filter(|user| *user != target)
						.boxed()
						.count()
						.await;

					if count < 2 {
						return Err!(Request(Forbidden(error!(
							"Last admin cannot leave the admins room."
						))));
					}
				},

				| MembershipState::Ban if pdu.state_key().is_some() => {
					if target == server_user {
						return Err!(Request(Forbidden(error!(
							"Server cannot be banned from admins room."
						))));
					}

					let count = self
						.services
						.state_cache
						.room_members(&room_id) // Avoid redundant re-evaluation
						.ready_filter(|user| self.services.globals.user_is_local(user))
						.ready_filter(|user| *user != target)
						.boxed()
						.count()
						.await;

					if count < 2 {
						return Err!(Request(Forbidden(error!(
							"Last admin cannot be banned from admins room."
						))));
					}
				},
				| _ => {},
			}
		},
		| _ => {},
	}

	Ok(())
}

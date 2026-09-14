use conduwuit::{Err, Result, RoomVersion, debug_info, implement, matrix::Event};
use ruma::{
	EventId, RoomId, UserId,
	events::{
		StateEventType, TimelineEventType,
		room::{
			create::RoomCreateEventContent,
			history_visibility::{HistoryVisibility, RoomHistoryVisibilityEventContent},
			power_levels::{RoomPowerLevels, RoomPowerLevelsEventContent},
		},
	},
};

/// Checks if a given user can redact a given event
///
/// If federation is true, it allows redaction events from any user of the
/// same server as the original event sender
#[implement(super::Service)]
pub async fn user_can_redact(
	&self,
	redacts: &EventId,
	sender: &UserId,
	room_id: &RoomId,
	federation: bool,
) -> Result<bool> {
	let redacting_event = self.services.timeline.get_pdu(redacts).await;

	if redacting_event
		.as_ref()
		.is_ok_and(|pdu| *pdu.kind() == TimelineEventType::RoomCreate)
	{
		return Err!(Request(Forbidden("Redacting m.room.create is not safe, forbidding.")));
	}

	if redacting_event
		.as_ref()
		.is_ok_and(|pdu| *pdu.kind() == TimelineEventType::RoomServerAcl)
	{
		return Err!(Request(Forbidden(
			"Redacting m.room.server_acl will result in the room being inaccessible for \
			 everyone (empty allow key), forbidding."
		)));
	}

	let room_create = self
		.room_state_get(room_id, &StateEventType::RoomCreate, "")
		.await?;
	let create_content: RoomCreateEventContent =
		serde_json::from_str(room_create.content().get())?;
	let room_features = RoomVersion::new(&create_content.room_version)?;
	if room_features.explicitly_privilege_room_creators {
		let sender_owned = sender.to_owned();
		if sender == room_create.sender()
			|| create_content
				.additional_creators
				.is_some_and(|cs| cs.contains(&sender_owned))
		{
			return Ok(true);
		}
	}

	match self
		.room_state_get_content::<RoomPowerLevelsEventContent>(
			room_id,
			&StateEventType::RoomPowerLevels,
			"",
		)
		.await
	{
		| Ok(pl_event_content) => {
			let pl_event: RoomPowerLevels = pl_event_content.into();
			Ok(pl_event.user_can_redact_event_of_other(sender)
				|| pl_event.user_can_redact_own_event(sender)
					&& match redacting_event {
						| Ok(redacting_event) =>
							if federation {
								redacting_event.sender().server_name() == sender.server_name()
							} else {
								redacting_event.sender() == sender
							},
						| _ => false,
					})
		},
		| _ => {
			// Falling back on m.room.create to judge power level
			Ok(room_create.sender() == sender
				|| redacting_event
					.as_ref()
					.is_ok_and(|redacting_event| redacting_event.sender() == sender))
		},
	}
}

/// Whether a user is allowed to see an event, based on
/// the room's history_visibility at that event's state.
#[implement(super::Service)]
#[tracing::instrument(skip_all, level = "trace")]
pub async fn user_can_see_event(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	event_id: &EventId,
) -> bool {
	if let Ok(pdu) = self.services.timeline.get_pdu(event_id).await {
		if pdu.sender == user_id {
			debug_info!("visibility {event_id}: sender match -> true");
			return true;
		}
	} else {
		debug_info!("visibility {event_id}: get_pdu failed");
	}

	let currently_member = self.services.state_cache.is_joined(user_id, room_id).await;
	let was_member = currently_member
		|| self
			.services
			.state_cache
			.once_joined(user_id, room_id)
			.await;

	if currently_member
		&& self
			.services
			.globals
			.allow_local_users_to_bypass_history_visibility()
		&& self.services.globals.server_name() == user_id.server_name()
	{
		return true;
	}

	let Ok(shortstatehash) = self.pdu_shortstatehash(event_id).await else {
		// No historical state snapshot for this event. Use the current room state's
		// history_visibility as a best-effort fallback. For shared/world_readable
		// policies, allow if currently a member. For joined/invited, deny since we
		// cannot verify historical membership without the shortstatehash.
		debug_info!(
			"visibility {event_id}: no shortstatehash, is_joined={currently_member}, \
			 was_member={was_member}"
		);
		let history_visibility = self
			.room_state_get_content(room_id, &StateEventType::RoomHistoryVisibility, "")
			.await
			.map_or(HistoryVisibility::Shared, |c: RoomHistoryVisibilityEventContent| {
				c.history_visibility
			});

		return match history_visibility {
			| HistoryVisibility::WorldReadable => true,
			| HistoryVisibility::Shared => was_member,
			| _ => false,
		};
	};

	let history_visibility = self
		.state_get_content(shortstatehash, &StateEventType::RoomHistoryVisibility, "")
		.await
		.map_or(HistoryVisibility::Shared, |c: RoomHistoryVisibilityEventContent| {
			c.history_visibility
		});

	debug_info!(
		"visibility {event_id}: ssh={shortstatehash} hv={history_visibility:?} \
		 member={currently_member}"
	);
	match history_visibility {
		| HistoryVisibility::Invited => {
			// Allow if any member on requesting server was AT LEAST invited, else deny
			self.user_was_invited(shortstatehash, user_id).await
		},
		| HistoryVisibility::Joined => {
			// Allow if any member on requested server was joined, else deny
			self.user_was_joined(shortstatehash, user_id).await
		},
		| HistoryVisibility::WorldReadable => true,
		| HistoryVisibility::Shared | _ => {
			// Shared: visible if user was ever a member of the room.
			// Per spec §11.5 rule 3: "If the user's membership was join at
			// any point after the event, allow."
			was_member
		},
	}
}

/// Whether a user is allowed to see room state events.
///
/// Per Matrix spec §11.5, users who have ever been joined to a room can
/// see state when history_visibility is `shared` (the default) or
/// `invited`. Currently-joined users always have access.
#[implement(super::Service)]
#[tracing::instrument(skip_all, level = "trace")]
pub async fn user_can_see_state_events(&self, user_id: &UserId, room_id: &RoomId) -> bool {
	if self.services.state_cache.is_joined(user_id, room_id).await {
		return true;
	}

	if self
		.services
		.globals
		.allow_local_users_to_bypass_history_visibility()
		&& self.services.globals.server_name() == user_id.server_name()
	{
		return true;
	}

	let was_member = self
		.services
		.state_cache
		.once_joined(user_id, room_id)
		.await;

	let history_visibility = self
		.room_state_get_content(room_id, &StateEventType::RoomHistoryVisibility, "")
		.await
		.map_or(HistoryVisibility::Shared, |c: RoomHistoryVisibilityEventContent| {
			c.history_visibility
		});

	match history_visibility {
		| HistoryVisibility::WorldReadable => true,
		| HistoryVisibility::Shared => was_member,
		| HistoryVisibility::Invited =>
			self.services.state_cache.is_invited(user_id, room_id).await || was_member,
		| _ => false,
	}
}

#[implement(super::Service)]
pub async fn user_can_invite(&self, room_id: &RoomId, sender: &UserId) -> bool {
	let Ok(state) = crate::rooms::auth_adapter::RoomStateProvider::new(room_id, self).await
	else {
		return false;
	};
	rezzy::auth::user::user_can_invite(sender.as_str(), &state.provider, state.version)
}

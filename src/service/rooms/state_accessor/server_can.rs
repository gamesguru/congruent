use conduwuit::{Event, implement};
use futures::StreamExt;
use slipstream::{
	OwnedEventId, OwnedRoomId, OwnedServerName, UserId,
	events::{
		StateEventType, TimelineEventType,
		room::history_visibility::{HistoryVisibility, RoomHistoryVisibilityEventContent},
	},
};

/// Whether a server is allowed to see an event through federation, based on
/// the room's history_visibility at that event's state.
#[implement(super::Service)]
pub async fn server_can_see_event(
	&self,
	origin: OwnedServerName,
	room_id: OwnedRoomId,
	event_id: OwnedEventId,
) -> bool {
	if event_id.server_name().as_ref() == Some(&origin) {
		return true;
	}

	if let Ok(pdu) = self.services.timeline.get_pdu(&event_id).await {
		if pdu.sender.server_name() == origin
			|| pdu.origin.as_deref() == Some(&origin)
			|| pdu.kind == TimelineEventType::RoomCreate
			|| (pdu.kind == TimelineEventType::RoomMember
				&& pdu
					.state_key()
					.and_then(|k| UserId::parse(k).ok())
					.is_some_and(|u| u.server_name() == origin))
		{
			return true;
		}
	}

	// Fast path: check current room visibility
	if self.is_world_readable(&room_id).await {
		return true;
	}

	let server_in_room = self
		.services
		.state_cache
		.server_in_room(&origin, &room_id)
		.await;

	// Fast path: if the server has joined users and visibility is Shared,
	// all history is visible. Invited/knocked servers don't qualify.
	if server_in_room {
		if let Ok(room_root) = self.services.state.get_room_state_hamt(&room_id).await {
			let history_visibility = self
				.state_get_content_hamt(
					&room_id,
					&room_root,
					&StateEventType::RoomHistoryVisibility,
					"",
				)
				.await
				.map_or(HistoryVisibility::Shared, |c: RoomHistoryVisibilityEventContent| {
					c.history_visibility
				});

			if history_visibility == HistoryVisibility::Shared {
				return true;
			}
		}
	}

	// Fallback when the event's state root is missing (outliers, force-set
	// imports, DB corruption). Check current room visibility instead of blindly
	// granting.
	let Ok(root_handle) = self.pdu_roothandle_before_event(&event_id).await else {
		if let Ok(room_root) = self.services.state.get_room_state_hamt(&room_id).await {
			let hv = self
				.state_get_content_hamt(
					&room_id,
					&room_root,
					&StateEventType::RoomHistoryVisibility,
					"",
				)
				.await
				.map_or(HistoryVisibility::Shared, |c: RoomHistoryVisibilityEventContent| {
					c.history_visibility
				});

			return match hv {
				| HistoryVisibility::WorldReadable => true,
				| HistoryVisibility::Shared => server_in_room,
				| _ => false,
			};
		}

		return false;
	};

	let history_visibility = self
		.state_get_content_hamt(
			&room_id,
			&root_handle,
			&StateEventType::RoomHistoryVisibility,
			"",
		)
		.await
		.map_or(HistoryVisibility::Shared, |c: RoomHistoryVisibilityEventContent| {
			c.history_visibility
		});

	match history_visibility {
		| HistoryVisibility::WorldReadable => true,
		| HistoryVisibility::Shared | HistoryVisibility::Custom(_) => {
			// Spec: servers with joined users can see all history.
			// Invited/knocked servers do NOT qualify for shared visibility.
			server_in_room
		},
		| HistoryVisibility::Invited => {
			// Allow if any member on requesting server was AT LEAST invited at that state
			let mut members = self
				.services
				.state_cache
				.room_useroncejoined(&room_id)
				.chain(self.services.state_cache.room_members_invited(&room_id));

			while let Some(member) = members.next().await {
				if member.server_name() == origin
					&& self
						.user_was_invited_hamt(&room_id, &root_handle, &member)
						.await
				{
					return true;
				}
			}

			false
		},
		| HistoryVisibility::Joined => {
			// Allow if any member on requesting server was joined at that state
			let mut members = self.services.state_cache.room_useroncejoined(&room_id);

			while let Some(member) = members.next().await {
				if member.server_name() == origin
					&& self
						.user_was_joined_hamt(&room_id, &root_handle, &member)
						.await
				{
					return true;
				}
			}

			false
		},
	}
}

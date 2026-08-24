use conduwuit::{implement, utils::stream::ReadyExt, warn};
use futures::StreamExt;
use ruma::{
	EventId, RoomId, ServerName,
	events::{
		StateEventType,
		room::history_visibility::{HistoryVisibility, RoomHistoryVisibilityEventContent},
	},
};

/// Whether a server is allowed to see an event through federation, based on
/// the room's history_visibility at that event's state.
#[implement(super::Service)]
#[tracing::instrument(skip_all, level = "trace")]
pub async fn server_can_see_event(
	&self,
	origin: &ServerName,
	room_id: &RoomId,
	event_id: &EventId,
) -> bool {
	let Ok(shortstatehash) = self.pdu_shortstatehash(event_id).await else {
		warn!(
			"Unable to visibility check event {} in room {} for server {}: shortstatehash not \
			 found; falling back to current membership",
			event_id, room_id, origin
		);
		// We can't evaluate history_visibility/membership at the event's state, so
		// only allow servers that are currently participating in the room rather
		// than failing open to any requester.
		return self
			.services
			.state_cache
			.server_in_room(origin, room_id)
			.await;
	};

	let history_visibility = self
		.state_get_content(shortstatehash, &StateEventType::RoomHistoryVisibility, "")
		.await
		.map_or(HistoryVisibility::Shared, |c: RoomHistoryVisibilityEventContent| {
			c.history_visibility
		});

	let current_server_members = self
		.services
		.state_cache
		.room_members(room_id)
		.ready_filter(|member| member.server_name() == origin);

	match history_visibility {
		| HistoryVisibility::Invited => {
			// Allow if any member on requesting server was AT LEAST invited, else deny
			current_server_members
				.any(|member| self.user_was_invited(shortstatehash, member))
				.await
		},
		| HistoryVisibility::Joined => {
			// Allow if any member on requested server was joined, else deny
			current_server_members
				.any(|member| self.user_was_joined(shortstatehash, member))
				.await
		},
		| HistoryVisibility::WorldReadable | HistoryVisibility::Shared | _ => true,
	}
}

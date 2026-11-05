use conduwuit::{
	Event, PduCount, PduEvent, Result, at, debug_warn, info,
	matrix::event::Matches,
	pdu::EventHash,
	trace,
	utils::{
		self, IterStream,
		future::ReadyEqExt,
		stream::{ReadyExt, WidebandExt as _},
	},
	warn,
};
use futures::{StreamExt, future::join};
use service::{Services, rooms::lazy_loading::MemberSet};
use slipstream::{
	EventId, OwnedRoomId, RoomId, UserId,
	api::client::sync::sync_events::v3::{LeftRoom, RoomAccountData, State, Timeline},
	events::{
		AnySyncStateEvent, StateEventType, TimelineEventType,
		room::member::{MembershipState, RoomMemberEventContent},
	},
	sswire::Raw,
	uint,
};

use crate::client::{
	TimelinePdus, ignored_filter,
	sync::{
		load_timeline,
		v3::{
			DEFAULT_TIMELINE_LIMIT, SyncContext, prepare_lazily_loaded_members,
			state::{build_state_incremental, build_state_initial},
		},
	},
};

/// Whether the leave membership event is a ban or a kick by another user.
///
/// A ban or a kick is always reported on an initial sync, even when the filter
/// does not set `include_leave`; only a self-leave is suppressed. This mirrors
/// Synapse and is required for a client to observe a remote ban on its first
/// sync.
fn ban_or_kick(membership: &MembershipState, sender: &UserId, syncing_user: &UserId) -> bool {
	match membership {
		| MembershipState::Ban => true,
		| MembershipState::Leave => sender != syncing_user,
		| _ => false,
	}
}

fn leave_is_ban_or_kick(
	leave_membership_event: Option<&PduEvent>,
	syncing_user: &UserId,
) -> bool {
	leave_membership_event.is_some_and(|pdu| {
		pdu.get_content::<RoomMemberEventContent>()
			.is_ok_and(|content| ban_or_kick(&content.membership, pdu.sender(), syncing_user))
	})
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn load_left_room(
	services: &Services,
	sync_context: SyncContext<'_>,
	ref room_id: OwnedRoomId,
	leave_membership_event: Option<PduEvent>,
) -> Result<Option<(LeftRoom, Vec<Raw<AnySyncStateEvent>>)>> {
	let SyncContext {
		syncing_user,
		last_sync_end_count,
		current_count,
		filter,
		..
	} = sync_context;

	// the global count as of the moment the user left the room
	let Some(left_count) = services
		.rooms
		.state_cache
		.get_left_count(room_id, syncing_user)
		.await
		.ok()
	else {
		// if we get here, the membership cache is incorrect, likely due to a state
		// reset
		debug_warn!("attempting to sync left room but no left count exists");
		return Ok(None);
	};

	// return early if we haven't gotten to this leave yet.
	// this can happen if the user leaves while a sync response is being generated
	if current_count < left_count {
		return Ok(None);
	}

	let is_forgotten = services
		.rooms
		.state_cache
		.is_forgotten(syncing_user, room_id)
		.await;

	// Bans and kicks are reported even without `include_leave`. On an initial
	// sync that is the only chance the client gets: the next sync starts from a
	// token past `left_count` and would treat the leave as already delivered.
	// Incremental syncs keep the normal `left_count` gate so an already-delivered
	// leave is not repeated on every sync.
	let is_ban_or_kick = leave_is_ban_or_kick(leave_membership_event.as_ref(), syncing_user);

	match last_sync_end_count {
		| None =>
			if is_forgotten || (!filter.room.include_leave && !is_ban_or_kick) {
				return Ok(None);
			},
		| Some(last_sync_end_count) => {
			if last_sync_end_count >= left_count && !filter.room.include_leave {
				return Ok(None);
			}
		},
	}

	if let Some(ref leave_membership_event) = leave_membership_event {
		debug_assert_eq!(
			leave_membership_event.kind,
			TimelineEventType::RoomMember,
			"leave PDU should be m.room.member"
		);
	}

	let last_sync_end_root_handle = match last_sync_end_count {
		| Some(last_sync_end_count) => services
			.rooms
			.timeline
			.prev_root_handle(room_id, PduCount::Normal(last_sync_end_count.saturating_add(1)))
			.await
			.ok(),
		| None => None,
	};

	let does_not_exist = services.rooms.metadata.exists(room_id).eq(&false).await;

	let (timeline, state_events, leave_root_handle) = match leave_membership_event {
		| Some(ref leave_membership_event) if does_not_exist => {
			/*
			we have none PDUs with left beef for this room, likely because it was a rejected invite to a room
			which nobody on this homeserver is in. `leave_pdu` is the remote-assisted outlier leave event for the room,
			which is all we can send to the client.

			if this is an initial sync, don't include this room at all to keep the client from asking for
			state that we don't have. bans and kicks are the exception: they must reach the client on its
			first sync, and the membership event alone is enough.
			*/

			if last_sync_end_count.is_none() && !is_ban_or_kick {
				return Ok(None);
			}

			trace!("syncing remote-assisted leave PDU");
			(TimelinePdus::default(), vec![leave_membership_event.clone()], None)
		},
		| Some(ref leave_membership_event) => {
			// we have this room in our DB, and can fetch the state and timeline from when
			// the user left.

			let loaded = async {
				let leave_state_key = syncing_user;
				debug_assert_eq!(
					Some(leave_state_key.as_str()),
					leave_membership_event.state_key(),
					"leave PDU should be for the user requesting the sync"
				);

				// `pdu_roothandle_after_event` resolves the state *after* a PDU, so the
				// leave event's own root includes `leave_membership_event`. For the
				// membership immediately before the user left we walk back one
				// event to the pre-leave root.
				let leave_root_handle = services
					.rooms
					.state_accessor
					.pdu_roothandle_after_event(&leave_membership_event.event_id)
					.await?;

				let before_leave_root_handle = services
					.rooms
					.state_accessor
					.pdu_roothandle_before_event(&leave_membership_event.event_id)
					.await
					.unwrap_or_else(|_| leave_root_handle.clone());

				let prev_membership_event = services
					.rooms
					.state_accessor
					.state_get_in_room_hamt(
						room_id,
						&before_leave_root_handle,
						&StateEventType::RoomMember,
						leave_state_key.as_str(),
					)
					.await?;

				let last_sync_end_root_handle = if last_sync_end_root_handle.is_none()
					&& last_sync_end_count.is_some_and(|c| c >= left_count)
				{
					Some(leave_root_handle.clone())
				} else {
					last_sync_end_root_handle
				};

				let (timeline, state_events) = build_left_state_and_timeline(
					services,
					sync_context,
					room_id,
					leave_membership_event,
					&leave_root_handle,
					&prev_membership_event,
					last_sync_end_root_handle,
				)
				.await?;

				Ok::<_, conduwuit::Error>((timeline, state_events, Some(leave_root_handle)))
			}
			.await;

			match loaded {
				| Ok(loaded) => loaded,
				| Err(error)
					if error.is_not_found()
						&& (last_sync_end_count.is_some() || is_ban_or_kick) =>
				{
					// The membership cache is updated before the leave event's timeline
					// row and state root are visible (see the append path). A sync that
					// wakes in that window must not drop the leave: the next_batch
					// advances past left_count, so the client would never see the ban.
					// Send the membership event we already have and let a later sync
					// fill in the timeline/state.
					warn!(
						%room_id,
						?error,
						"leave event is not fully loadable yet; sending only the membership event"
					);
					(TimelinePdus::default(), vec![leave_membership_event.clone()], None)
				},
				| Err(error) => return Err(error),
			}
		},

		| None => {
			/*
			no leave event was actually sent in this room, but we still need to pretend
			like the user left it. this is usually because the room was banned by a server admin.

			if this is an incremental sync, generate a fake leave event to make the room vanish from clients.
			otherwise we don't tell the client about this room at all.
			*/
			if last_sync_end_count.is_none() {
				return Ok(None);
			}

			trace!("syncing dummy leave event");
			(
				TimelinePdus::default(),
				vec![create_dummy_leave_event(services, sync_context, room_id)],
				None,
			)
		},
	};

	let state_after = if services.config.experimental_features.msc4222_enabled
		&& sync_context.use_state_after
	{
		if let Some(root_handle) = leave_root_handle {
			let lazily_loaded_members: Option<MemberSet> = prepare_lazily_loaded_members(
				services,
				sync_context,
				room_id,
				timeline.members(),
			)
			.await;

			build_state_initial(
				services,
				room_id,
				syncing_user,
				&root_handle,
				lazily_loaded_members.as_ref(),
				None,
			)
			.await?
			.into_iter()
			.map(Event::into_format)
			.collect()
		} else {
			Vec::new()
		}
	} else {
		Vec::new()
	};

	let TimelinePdus { pdus, limited, prev_batch } = timeline;

	// filter out ignored events from the timeline
	let raw_timeline_pdus: Vec<PduEvent> = pdus
		.into_iter()
		.stream()
		.wide_filter_map(|item| ignored_filter(services, item, syncing_user))
		.ready_filter(|(_, pdu): &(PduCount, PduEvent)| {
			let filter = &filter.room.timeline;
			filter.matches(pdu)
		})
		.map(at!(1))
		.collect::<Vec<_>>()
		.await;

	let mut state_events: Vec<PduEvent> = state_events;

	// `state` should only ever include one membership event for the syncing user
	let membership_event_index = state_events.iter().position(|pdu: &PduEvent| {
		*pdu.event_type() == TimelineEventType::RoomMember
			&& pdu.state_key() == Some(syncing_user.as_str())
	});

	let in_timeline = raw_timeline_pdus.iter().any(|pdu: &PduEvent| {
		*pdu.event_type() == TimelineEventType::RoomMember
			&& pdu.state_key() == Some(syncing_user.as_str())
	});

	if let Some(index) = membership_event_index {
		if in_timeline {
			// remove the syncing user's membership event from `state` if the timeline
			// already contains a membership event for that user (for example, a leave)
			state_events.swap_remove(index);
		} else if last_sync_end_count.is_none_or(|c| c < left_count) {
			// otherwise, ensure the membership in state is the actual leave event
			// so the client knows they have left, even if the timeline is empty/limited.
			// we only do this if we haven't synced the leave yet.
			if let Some(ref leave_membership_event) = leave_membership_event {
				state_events[index] = leave_membership_event.clone();
			}
		}
	} else if !in_timeline && last_sync_end_count.is_none_or(|c| c < left_count) {
		// if the user's membership is missing from both state and timeline,
		// we must add it to state so the client knows they have left.
		// we only do this if we haven't synced the leave yet.
		if let Some(leave_membership_event) = leave_membership_event {
			state_events.push(leave_membership_event);
		}
	}

	let timeline_ids: std::collections::HashSet<&str> =
		raw_timeline_pdus.iter().map(|pdu| &*pdu.event_id).collect();

	let raw_state_events: Vec<Raw<AnySyncStateEvent>> = state_events
		.into_iter()
		.filter(|pdu: &PduEvent| !timeline_ids.contains(&*pdu.event_id))
		.filter(|pdu: &PduEvent| {
			let filter = &filter.room.state;
			filter.matches(pdu)
		})
		.map(Event::into_format)
		.collect();

	if last_sync_end_count.is_some()
		&& raw_timeline_pdus.is_empty()
		&& raw_state_events.is_empty()
	{
		return Ok(None);
	}

	Ok(Some((
		LeftRoom {
			account_data: RoomAccountData { events: Vec::new() },
			timeline: Timeline {
				limited,
				prev_batch: prev_batch.map(|c| c.to_string()),
				events: raw_timeline_pdus
					.into_iter()
					.map(Event::into_format)
					.collect(),
			},
			state: State { events: raw_state_events },
		},
		state_after,
	)))
}

#[allow(clippy::too_many_arguments)]
async fn build_left_state_and_timeline(
	services: &Services,
	sync_context: SyncContext<'_>,
	room_id: &RoomId,
	leave_membership_event: &PduEvent,
	leave_root_handle: &rezzy::hamt::RootHandle,
	prev_membership_event: &PduEvent,
	last_sync_end_root_handle: Option<rezzy::hamt::RootHandle>,
) -> Result<(TimelinePdus, Vec<PduEvent>)> {
	let SyncContext {
		syncing_user,
		last_sync_end_count,
		filter,
		..
	} = sync_context;

	let join_count = services
		.rooms
		.timeline
		.get_pdu_count(&prev_membership_event.event_id)
		.await?;

	// if this is an incremental sync, we only need to return events since the last
	// sync.
	let timeline_start_count = last_sync_end_count
		.map(PduCount::Normal)
		.filter(|&last_sync_end_count| last_sync_end_count > join_count)
		.unwrap_or(join_count);

	// end the timeline at the user's leave event
	let timeline_end_count = services
		.rooms
		.timeline
		.get_pdu_count(leave_membership_event.event_id())
		.await?;

	// limit the timeline using the same logic as for joined rooms
	let timeline_limit = filter
		.room
		.timeline
		.limit
		.and_then(|limit| limit.try_into().ok())
		.unwrap_or(DEFAULT_TIMELINE_LIMIT);

	// A zero timeline limit explicitly requests no timeline events. Do not pass
	// it through to `load_timeline`: that helper fetches one extra event to
	// determine whether the result is limited, so a zero there would still
	// return one event. The leave-state snapshot is built below independently.
	let raw_timeline = if timeline_limit == 0 {
		TimelinePdus::default()
	} else {
		load_timeline(
			services,
			syncing_user,
			room_id,
			Some(timeline_start_count),
			Some(timeline_end_count),
			timeline_limit,
			false,
		)
		.await?
	};

	let mut stream = raw_timeline
		.pdus
		.into_iter()
		.stream()
		// filter out ignored events from the timeline
		.wide_filter_map(|item| ignored_filter(services, item, syncing_user))
		.ready_filter(|(_, pdu): &(PduCount, PduEvent)| {
			use conduwuit::matrix::event::Matches;
			(&sync_context.filter.room.timeline).matches(pdu)
		});

	let mut filtered_pdus = std::collections::VecDeque::new();
	while let Some(item) = stream.next().await {
		filtered_pdus.push_back(item);
	}

	let timeline = TimelinePdus {
		pdus: filtered_pdus,
		prev_batch: raw_timeline.prev_batch,
		limited: raw_timeline.limited,
	};

	let timeline_start_root_handle = async {
		if let Some((_, pdu)) = timeline.pdus.front() {
			if let Ok(root_handle) = services
				.rooms
				.state_accessor
				.pdu_roothandle_before_event(&pdu.event_id)
				.await
			{
				return root_handle;
			}
		}

		// the timeline generally should not be empty, but in case it is we use
		// `leave_root_handle` as the state to send
		leave_root_handle.clone()
	};

	let lazily_loaded_members =
		prepare_lazily_loaded_members(services, sync_context, room_id, timeline.members());

	let (timeline_start_root_handle, lazily_loaded_members) =
		join(timeline_start_root_handle, lazily_loaded_members).await;

	// compute the state delta between the previous sync and this sync.
	let state = match (last_sync_end_count, last_sync_end_root_handle) {
		| (Some(last_sync_end_count), Some(last_sync_end_root_handle)) => {
			let timeline_end_root_handle = services
				.rooms
				.state_accessor
				.pdu_roothandle_after_event(leave_membership_event.event_id())
				.await?;

			build_state_incremental(
				services,
				syncing_user,
				room_id,
				PduCount::Normal(last_sync_end_count),
				&last_sync_end_root_handle,
				&timeline_start_root_handle,
				&timeline_end_root_handle,
				&timeline,
				lazily_loaded_members.as_ref(),
			)
			.await?
		},
		| _ =>
			build_state_initial(
				services,
				room_id,
				syncing_user,
				&timeline_start_root_handle,
				lazily_loaded_members.as_ref(),
				None,
			)
			.await?,
	};

	info!(
		target: "conduwuit::api::client::sync",
		%timeline_start_count,
		%timeline_end_count,
		"syncing {} timeline events (limited = {}) and {} state events",
		timeline.pdus.len(),
		timeline.limited,
		state.len()
	);

	Ok((timeline, state))
}

fn create_dummy_leave_event(
	services: &Services,
	SyncContext { syncing_user, .. }: SyncContext<'_>,
	room_id: &RoomId,
) -> PduEvent {
	// Keep each synthetic event ID unique so clients do not merge dummy events
	// from different rooms or sync responses.
	PduEvent {
		event_id: EventId::parse(format!(
			"${}:{}",
			utils::random_string(18),
			services.globals.server_name()
		))
		.expect("synthetic leave event ID must be valid"),
		sender: syncing_user.to_owned(),
		origin: None,
		origin_server_ts: utils::millis_since_unix_epoch(),
		kind: TimelineEventType::RoomMember,
		content: Raw::from_json_text(r#"{"membership": "leave"}"#)
			.expect("static synthetic leave content is valid JSON"),
		state_key: Some(syncing_user.as_str().into()),
		unsigned: None,
		// The following keys are dropped on conversion
		room_id: Some(room_id.to_owned()),
		prev_events: vec![],
		depth: uint!(1),
		auth_events: vec![],
		redacts: None,
		hashes: EventHash { sha256: String::new() },
		signatures: None,
		rejected: false,
	}
}

#[cfg(test)]
mod tests {
	use slipstream::{events::room::member::MembershipState, user_id};

	use super::ban_or_kick;

	#[test]
	fn ban_and_kick_are_reported_without_include_leave() {
		let alice = user_id!("@alice:example.org");
		let bob = user_id!("@bob:example.org");

		// A ban is a leave caused by someone else; it must reach an initial sync.
		assert!(ban_or_kick(&MembershipState::Ban, &bob, &alice));

		// A kick is a plain leave caused by someone else.
		assert!(ban_or_kick(&MembershipState::Leave, &bob, &alice));

		// A self-leave is only reported with `include_leave`.
		assert!(!ban_or_kick(&MembershipState::Leave, &alice, &alice));

		// Other membership states are not "left" rooms.
		assert!(!ban_or_kick(&MembershipState::Join, &alice, &alice));
		assert!(!ban_or_kick(&MembershipState::Invite, &bob, &alice));
		assert!(!ban_or_kick(&MembershipState::Knock, &alice, &alice));
	}
}

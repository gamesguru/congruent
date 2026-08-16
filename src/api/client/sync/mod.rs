mod v3;
mod v5;

use std::collections::VecDeque;

use conduwuit::{
	Event, PduCount, Result, debug_warn, err, info,
	matrix::pdu::PduEvent,
	result::LogErr,
	utils::stream::{BroadbandExt, ReadyExt, TryIgnore, WidebandExt},
	warn,
};
use conduwuit_service::Services;
use futures::{StreamExt, TryStreamExt};
use ruma::{
	OwnedUserId, RoomId, UserId,
	events::TimelineEventType::{
		self, Beacon, CallInvite, PollStart, RoomEncrypted, RoomMessage, Sticker,
	},
};

pub(crate) use self::{
	v3::sync_events_route,
	v5::{sync_events_unstable_msc3575_route, sync_events_v5_route},
};

pub(crate) const DEFAULT_BUMP_TYPES: &[TimelineEventType; 6] =
	&[CallInvite, PollStart, Beacon, RoomEncrypted, RoomMessage, Sticker];

#[derive(Default)]
pub(crate) struct TimelinePdus {
	pub pdus: VecDeque<(PduCount, PduEvent)>,
	pub prev_batch: Option<PduCount>,
	pub limited: bool,
}

impl TimelinePdus {
	pub(crate) fn members(&self) -> impl Iterator<Item = OwnedUserId> + '_ {
		self.pdus.iter().flat_map(|(_, pdu)| {
			let mut users = vec![pdu.sender.clone()];
			if pdu.event_type().to_string() == "m.room.member" {
				if let Some(state_key) = &pdu.state_key {
					if let Ok(user_id) = UserId::parse(state_key.as_str()) {
						users.push(user_id.to_owned());
					}
				}
			}
			users
		})
	}
}

/// Load up to `limit` PDUs in the range (starting_count, ending_count].
async fn load_timeline(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	starting_count: Option<PduCount>,
	ending_count: Option<PduCount>,
	limit: usize,
	is_expanded_timeline: bool,
) -> Result<TimelinePdus> {
	info!(
		target: "timeline_debug",
		"load_timeline entry: room={} sender={} starting={:?} ending={:?} limit={}",
		room_id, sender_user, starting_count, ending_count, limit
	);

	// `fetch_limit` items are drained via `.take(fetch_limit)` below, then one
	// more is peeked via `.next()` to determine `limited` -- so the stream only
	// ever needs to yield `fetch_limit + 1` items. Bounding it to `stream_limit`
	// here, before `wide_then`, keeps its concurrent per-item DB work
	// proportional to that instead of `wide_then`'s full concurrency width.
	let fetch_limit = limit.saturating_add(1);
	let stream_limit = fetch_limit.saturating_add(1);

	let mut pdu_stream = match starting_count {
		| Some(starting_count) => {
			let last_timeline_count = services
				.rooms
				.timeline
				.last_timeline_count(room_id)
				.await
				.map_err(|err| {
					err!(Database(warn!("Failed to fetch end of room timeline: {}", err)))
				})?;

			if !is_expanded_timeline && last_timeline_count <= starting_count {
				// no messages have been sent in this room since `starting_count`
				info!(
					target: "timeline_debug",
					"load_timeline early return for {}: last_timeline_count={:?} <= \
					 starting_count={:?} sender={}",
					room_id, last_timeline_count, starting_count, sender_user
				);
				return Ok(TimelinePdus::default());
			}

			// for incremental sync, stream from the DB all PDUs which were sent after
			// `starting_count` but before `ending_count`, including `ending_count` but
			// not `starting_count`. this code is pretty similar to the initial sync
			// branch, they're separate to allow for future optimization
			services
				.rooms
				.timeline
				.pdus_rev(
					room_id,
					ending_count.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Included),
				)
				.inspect_err(|e| warn!("sync timeline pdus_rev error for {room_id}: {e}"))
				.ignore_err()
				.inspect(move |(pducount, _)| {
					info!(
						target: "timeline_debug",
						"sync filter check for {}: pducount={:?}, starting_count={:?}, \
						 passes={:?}",
						room_id,
						pducount,
						starting_count,
						is_expanded_timeline || *pducount > starting_count
					);
				})
				.ready_take_while(move |&(pducount, _)| {
					is_expanded_timeline || pducount > starting_count
				})
				// Bound *before* wide_then: wide_then's concurrent `.buffered(width)`
				// (width defaults to 32, see `automatic_width`) eagerly pulls and starts
				// up to `width` upstream items to fill its concurrency window before
				// yielding anything, regardless of how many the caller actually wants.
				// Left unbounded here, every sync poll for every room ran up to 32
				// concurrent add_membership_to_unsigned + add_bundled_aggregations_to_pdu
				// DB lookups even for a `limit=3` request, discarding all but a handful
				// of the results.
				.take(stream_limit)
				.map(move |mut pdu| {
					pdu.1.set_unsigned(Some(sender_user));
					pdu
				})
				.wide_then(move |mut pdu| async move {
					add_membership_to_unsigned(services, sender_user, &mut pdu.1).await;
					if let Err(e) = services
						.rooms
						.pdu_metadata
						.add_bundled_aggregations_to_pdu(sender_user, &mut pdu.1)
						.await
					{
						debug_warn!("Failed to add bundled aggregations: {e}");
					}
					pdu
				})
				.boxed()
		},
		| None => {
			// For initial sync, stream from the DB all PDUs before and including
			// `ending_count` in reverse order
			services
				.rooms
				.timeline
				.pdus_rev(
					room_id,
					ending_count.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Included),
				)
				.inspect_err(|e| warn!("sync initial timeline pdus_rev error for {room_id}: {e}"))
				.ignore_err()
				// See the comment on the incremental-sync branch above -- same reasoning.
				.take(stream_limit)
				.map(move |mut pdu| {
					pdu.1.set_unsigned(Some(sender_user));
					pdu
				})
				.wide_then(move |mut pdu| async move {
					add_membership_to_unsigned(services, sender_user, &mut pdu.1).await;
					if let Err(e) = services
						.rooms
						.pdu_metadata
						.add_bundled_aggregations_to_pdu(sender_user, &mut pdu.1)
						.await
					{
						debug_warn!("Failed to add bundled aggregations: {e}");
					}
					pdu
				})
				.boxed()
		},
	};

	// 1. `fetch_limit` (defined above) fetches one extra PDU to evaluate layout
	//    limits without shifting the window.
	// 2. Stream layout into a temporary sequence container
	let mut pdus = pdu_stream
		.by_ref()
		.take(fetch_limit)
		.ready_fold(VecDeque::with_capacity(fetch_limit), |mut pdus, item| {
			pdus.push_front(item);
			pdus
		})
		.await;

	// 3. Establish initial constraint boundaries using lookahead markers
	let mut limited = pdus.len() > limit || pdu_stream.next().await.is_some();

	// If we didn't hit the limit, check if there is a topological gap.
	// A topological gap exists if the oldest returned event references a
	// prev_event that is not stored in the local timeline.
	if !limited && starting_count.is_some() {
		if let Some((_, oldest_pdu)) = pdus.front() {
			for prev_id in oldest_pdu.prev_events() {
				if services
					.rooms
					.timeline
					.get_pdu_count(prev_id)
					.await
					.is_err()
				{
					limited = true;
					break;
				}
			}
		}
	}

	// 4. Capture chronological batch boundaries BEFORE topo sort shuffles order
	//
	// prev_batch only needs to sit strictly BEFORE the oldest event when the
	// timeline was actually truncated (there's a real gap before the batch we
	// returned). If everything fit (not `limited`), there is no gap -- the
	// spec defines `state` as "the state between the previous sync and the
	// start of the timeline", and with no gap that boundary is just the
	// current sync position, same as Synapse: `_load_filtered_recents` seeds
	// `room_key` from `upto_token` and only overwrites it with
	// `oldest.stream_ordering - 1` inside the `len(filtered_recents) >
	// timeline_limit` truncation branch. Using the oldest event's position
	// unconditionally (as we did before) made e.g. `/members?at=prev_batch`
	// resolve to state before the room's first event for any room small
	// enough to fit in one sync, instead of the state right after the
	// client's last known position.
	let mut prev_batch = if limited {
		if pdus.len() > limit {
			pdus.get(pdus.len().saturating_sub(limit))
				.map(|(count, _)| count.saturating_inc(ruma::api::Direction::Backward))
		} else {
			pdus.front()
				.map(|(count, _)| count.saturating_inc(ruma::api::Direction::Backward))
		}
	} else {
		ending_count
	};

	// 5. Trim off the lookahead element from the primary evaluation window
	if pdus.len() > limit {
		let drop_count = pdus.len().saturating_sub(limit);
		pdus.drain(0..drop_count);
	}

	// 6. Execute hotfix branch's Topo Sort for correct DAG traversal ordering
	if !pdus.is_empty() {
		let mut event_to_count = std::collections::HashMap::new();
		let events: Vec<_> = pdus
			.into_iter()
			.map(|(count, pdu)| {
				event_to_count.insert(pdu.event_id.clone(), count);
				pdu
			})
			.collect();

		let sorted_events = conduwuit::matrix::dag::sort_topologically(events);

		pdus = sorted_events
			.into_iter()
			.map(|pdu| {
				let count = event_to_count
					.remove(&pdu.event_id)
					.expect("event count exists");
				(count, pdu)
			})
			.collect();
	}

	// 7. Execute HEAD branch's Backward Topological Gap Truncation logic
	if starting_count.is_some() {
		let mut gap_idx = None;

		// Traverse newest to oldest to pinpoint structural graph breaks
		for (i, (_, pdu)) in pdus.iter().enumerate().rev() {
			let mut gap_found = false;
			for prev_id in pdu.prev_events() {
				if services
					.rooms
					.timeline
					.get_pdu_count(prev_id)
					.await
					.is_err()
				{
					gap_found = true;
					break;
				}
			}

			if gap_found {
				gap_idx = Some(i);
				info!(
					"Topological gap in timeline for {} before PDU {}. Truncating.",
					room_id,
					pdu.event_id()
				);
				break;
			}
		}

		// If a break is found, drop broken history and rewrite the pagination tokens
		if let Some(i) = gap_idx {
			pdus.drain(0..i);
			limited = true;

			// The chronological edge has shifted; point prev_batch to the new front
			prev_batch = pdus.iter().map(|(count, _)| *count).min();
		}
	}

	// 8. Unified Telemetry Logging
	if pdus.is_empty() && starting_count.is_some() {
		info!(
			target: "timeline_debug",
			"sync: 0 timeline pdus for {} from {:?} to {:?} (limited = {:?}) sender={}",
			room_id, starting_count, ending_count, limited, sender_user,
		);
	} else {
		info!(
			target: "timeline_debug",
			"sync: {:?} timeline pdus for {} from {:?} to {:?} (limited = {:?})",
			pdus.len(),
			room_id,
			starting_count,
			ending_count,
			limited,
		);
	}

	Ok(TimelinePdus { pdus, prev_batch, limited })
}

async fn share_encrypted_room(
	services: &Services,
	sender_user: &UserId,
	user_id: &UserId,
	ignore_room: Option<&RoomId>,
) -> bool {
	services
		.rooms
		.state_cache
		.get_shared_rooms(sender_user, user_id)
		.ready_filter(|&room_id| Some(room_id) != ignore_room)
		.map(ToOwned::to_owned)
		.broad_any(|other_room_id| async move {
			services
				.rooms
				.state_accessor
				.is_encrypted_room(&other_room_id)
				.await
		})
		.await
}

async fn shares_a_room(
	services: &Services,
	sender_user: &UserId,
	user_id: &UserId,
	ignore_room: Option<&RoomId>,
) -> bool {
	use conduwuit::utils::stream::ReadyExt;
	services
		.rooms
		.state_cache
		.get_shared_rooms(sender_user, user_id)
		.ready_any(|room_id| Some(room_id) != ignore_room)
		.await
}

/// Look up the requesting user's membership at the event's state snapshot
/// and set `unsigned.membership` accordingly. Mirrors the pattern used by
/// `repair_unsigned` (delegates to `user_membership_at_event` on the
/// state_accessor service).
pub(crate) async fn add_membership_to_unsigned(
	services: &Services,
	user_id: &UserId,
	pdu: &mut PduEvent,
) {
	let Some(room_id) = pdu.room_id_or_hash() else {
		return;
	};

	// Is this a membership event for the syncing user?
	let is_own_membership = pdu.kind == TimelineEventType::RoomMember
		&& pdu.state_key.as_deref() == Some(user_id.as_str());

	let membership = if is_own_membership {
		// MSC4115: "Consider the room state just *after* event E landed. Any changes
		// caused by the event itself... are included."
		// For a user's own membership event, the state after the event is just the
		// event itself.
		serde_json::from_str::<ruma::events::room::member::RoomMemberEventContent>(
			pdu.content.get(),
		)
		.map_or(ruma::events::room::member::MembershipState::Leave, |c| c.membership)
	} else if pdu.kind == TimelineEventType::RoomCreate {
		ruma::events::room::member::MembershipState::Leave
	} else {
		services
			.rooms
			.state_accessor
			.user_membership_at_event(pdu.event_id(), &room_id, user_id)
			.await
	};

	pdu.set_membership(membership.as_str()).log_err().ok();
}

mod v3;
mod v5;

use std::collections::VecDeque;

use conduwuit::{
	Event, PduCount, Result, debug_warn, err,
	matrix::pdu::PduEvent,
	trace,
	utils::stream::{BroadbandExt, ReadyExt, TryIgnore},
};
use conduwuit_service::Services;
use futures::StreamExt;
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
				return Ok(TimelinePdus::default());
			}

			// for incremental sync, stream from the DB all PDUs which were sent after
			// `starting_count` but before `ending_count`, including `ending_count` but
			// not `starting_count`. this code is pretty similar to the initial sync
			// branch, they're separate to allow for future optimization
			services
				.rooms
				.timeline
				.pdus_rev(room_id, ending_count.map(|count| count.saturating_add(1)))
				.ignore_err()
				.ready_take_while(move |&(pducount, _)| {
					is_expanded_timeline || pducount > starting_count
				})
				.map(move |mut pdu| {
					pdu.1.set_unsigned(Some(sender_user));
					pdu
				})
				.then(async move |mut pdu| {
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
				.pdus_rev(room_id, ending_count.map(|count| count.saturating_add(1)))
				.ignore_err()
				.map(move |mut pdu| {
					pdu.1.set_unsigned(Some(sender_user));
					pdu
				})
				.then(async move |mut pdu| {
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

	// Fetch one extra PDU to determine whether the timeline is limited without
	// changing the returned chronological window.
	let fetch_limit = limit.saturating_add(1);

	// Return at most `fetch_limit` PDUs from the stream
	let mut pdus = pdu_stream
		.by_ref()
		.take(fetch_limit)
		.ready_fold(VecDeque::with_capacity(fetch_limit), |mut pdus, item| {
			pdus.push_front(item);
			pdus
		})
		.await;

	// The timeline is limited if there are still more PDUs in the stream or if we
	// fetched more than `limit`
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

	// capture the count of the absolute earliest PDU we will return as the
	// prev_batch token. This must be determined before topological sort changes
	// the order of the PDUs.
	let prev_batch = if pdus.len() > limit {
		pdus.get(pdus.len().saturating_sub(limit))
			.map(|(count, _)| *count)
	} else {
		pdus.front().map(|(count, _)| *count)
	};

	if pdus.len() > limit {
		let drop_count = pdus.len().saturating_sub(limit);
		pdus.drain(0..drop_count);
	}

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

	trace!(
		"syncing {:?} timeline pdus from {:?} to {:?} (limited = {:?})",
		pdus.len(),
		starting_count,
		ending_count,
		limited,
	);

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

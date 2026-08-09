use std::collections::{HashSet, VecDeque};

use axum::extract::State;
use conduwuit::{Err, Event, Result, debug, info, trace, utils::to_canonical_object, warn};
use ruma::{OwnedEventId, api::federation::event::get_missing_events};
use serde_json::{json, value::RawValue};

use super::AccessCheck;
use crate::Ruma;

/// arbitrary number but synapse's is 20 and we can handle lots of these anyways
const LIMIT_MAX: usize = 50;
/// spec says default is 10
const LIMIT_DEFAULT: usize = 10;

/// # `POST /_matrix/federation/v1/get_missing_events/{roomId}`
///
/// Retrieves events that the sender is missing.
pub(crate) async fn get_missing_events_route(
	State(services): State<crate::State>,
	body: Ruma<get_missing_events::v1::Request>,
) -> Result<get_missing_events::v1::Response> {
	AccessCheck {
		services: &services,
		origin: body.origin(),
		room_id: &body.room_id,
		event_id: None,
	}
	.check()
	.await?;

	let limit = body
		.limit
		.try_into()
		.unwrap_or(LIMIT_DEFAULT)
		.min(LIMIT_MAX);

	info!(
		origin = body.origin().as_str(),
		room_id = %body.room_id,
		limit,
		latest = body.latest_events.len(),
		earliest = body.earliest_events.len(),
		"Serving get_missing_events request"
	);

	let room_version = services.rooms.state.get_room_version(&body.room_id).await?;

	let mut queue: VecDeque<OwnedEventId> = VecDeque::from(body.latest_events.clone());
	let mut results: Vec<(OwnedEventId, Vec<OwnedEventId>, ruma::UInt, Box<RawValue>)> =
		Vec::with_capacity(limit);
	let mut seen: HashSet<OwnedEventId> = HashSet::from_iter(body.earliest_events.clone());

	while let Some(next_event_id) = queue.pop_front() {
		if seen.contains(&next_event_id) {
			trace!(%next_event_id, "already seen event, skipping");
			continue;
		}

		if results.len() >= limit {
			debug!(%next_event_id, "reached limit of events to return, breaking");
			break;
		}

		let mut pdu = match services.rooms.timeline.get_pdu(&next_event_id).await {
			| Ok(pdu) => pdu,
			| Err(e) => {
				warn!("could not find event {next_event_id} while walking missing events: {e}");
				continue;
			},
		};
		if pdu.room_id_or_hash().as_deref() != Some(body.room_id.as_ref()) {
			return Err!(Request(Unknown(
				"Event {next_event_id} is not in room {}",
				body.room_id
			)));
		}

		if !services
			.rooms
			.state_accessor
			.server_can_see_event(
				body.origin().to_owned(),
				body.room_id.clone(),
				pdu.event_id().to_owned(),
			)
			.await
		{
			debug!(%next_event_id, origin = %body.origin(), "redacting event origin cannot see");
			pdu.redact(&room_version, json!({}))?;
		}

		trace!(
			%next_event_id,
			prev_events = ?pdu.prev_events().collect::<Vec<_>>(),
			"adding event to results and queueing prev events"
		);
		queue.extend(pdu.prev_events.clone());
		seen.insert(next_event_id.clone());
		if body.latest_events.contains(&next_event_id) {
			continue; // Don't include latest_events in results,
			// but do include their prev_events in the queue
		}
		results.push((
			next_event_id.clone(),
			pdu.prev_events.clone(),
			pdu.depth,
			services
				.sending
				.convert_to_outgoing_federation_event(to_canonical_object(pdu)?)
				.await,
		));
		trace!(
			%next_event_id,
			queue_len = queue.len(),
			seen_len = seen.len(),
			results_len = results.len(),
			"event added to results"
		);
	}

	if !queue.is_empty() {
		debug!("limit reached before queue was empty");
	}

	let sorted_ids = topo_sort_events(
		results
			.iter()
			.map(|(id, prevs, depth, _)| (id.clone(), prevs.clone(), *depth)),
	);

	let mut event_map: std::collections::BTreeMap<OwnedEventId, Box<RawValue>> = results
		.into_iter()
		.map(|(id, _, _, raw)| (id, raw))
		.collect();

	let events = sorted_ids
		.into_iter()
		.filter_map(|id| event_map.remove(&id))
		.collect();

	Ok(get_missing_events::v1::Response { events })
}

/// Topologically sort events using Kahn's algorithm.
///
/// Returns event IDs ordered such that an event always appears after its
/// prev_events (i.e. oldest first). Events at the same depth are
/// tie-broken by event ID (lexicographic ascending).
///
/// Only events present in the input set participate in the graph — external
/// prev_events (e.g. `earliest_events`) are treated as implicit roots.
pub(crate) fn topo_sort_events(
	events: impl IntoIterator<Item = (OwnedEventId, Vec<OwnedEventId>, ruma::UInt)>,
) -> Vec<OwnedEventId> {
	conduwuit::utils::kahns_sort::kahn_sort(events.into_iter().map(|(id, prevs, depth)| {
		// get_missing_events tiebreaks by oldest first (smallest depth), then event_id.
		// The TieBreaker previously used `db.cmp(&da)` (descending sort), so
		// popping the end of the vector yielded the smallest depth.
		// Our kahn_sort pops the smallest key first, so we just use the depth directly.
		let key = (depth, id.clone());
		(id, prevs, key)
	}))
}

#[cfg(test)]
mod tests {
	use ruma::OwnedEventId;

	use super::topo_sort_events;

	fn eid(s: &str) -> OwnedEventId { format!("${s}:example.com").try_into().unwrap() }

	fn depth(n: u64) -> ruma::UInt { ruma::UInt::new(n).unwrap() }

	/// Linear chain: A ← B ← C
	/// Expected output: [A, B, C] (oldest first)
	#[test]
	fn linear_chain() {
		let a = eid("a");
		let b = eid("b");
		let c = eid("c");

		let events = vec![
			(c.clone(), vec![b.clone()], depth(3)),
			(b.clone(), vec![a.clone()], depth(2)),
			(a.clone(), vec![eid("root")], depth(1)),
		];

		let sorted = topo_sort_events(events);
		assert_eq!(sorted, vec![a, b, c]);
	}

	/// Fork and merge:
	///   A ← B
	///   A ← C
	///   B,C ← D
	/// Expected: A first, D last, B and C in between (ordered by depth/id)
	#[test]
	fn fork_and_merge() {
		let a = eid("a");
		let b = eid("b");
		let c = eid("c");
		let d = eid("d");

		let events = vec![
			(a.clone(), vec![eid("root")], depth(1)),
			(b.clone(), vec![a.clone()], depth(2)),
			(c.clone(), vec![a.clone()], depth(2)),
			(d.clone(), vec![b.clone(), c.clone()], depth(3)),
		];

		let sorted = topo_sort_events(events);
		assert_eq!(sorted[0], a, "A must be first");
		assert_eq!(sorted[3], d, "D must be last");
		// B and C are both at depth 2, order by event ID
		assert!(sorted[1..3].contains(&b));
		assert!(sorted[1..3].contains(&c));
	}

	/// Single event with no in-set prev_events
	#[test]
	fn single_event() {
		let a = eid("a");
		let events = vec![(a.clone(), vec![eid("external")], depth(5))];
		let sorted = topo_sort_events(events);
		assert_eq!(sorted, vec![a]);
	}

	/// Empty input returns empty output
	#[test]
	fn empty() {
		let sorted = topo_sort_events(std::iter::empty());
		assert!(sorted.is_empty());
	}

	/// Events at the same depth should be sorted by event ID (deterministic)
	#[test]
	fn same_depth_tiebreak() {
		let a = eid("aaa");
		let b = eid("bbb");
		let c = eid("ccc");

		let events = vec![
			(c.clone(), vec![eid("root")], depth(1)),
			(a.clone(), vec![eid("root")], depth(1)),
			(b.clone(), vec![eid("root")], depth(1)),
		];

		let sorted = topo_sort_events(events);
		// All independent, same depth → sorted by event ID lexicographically
		assert_eq!(sorted, vec![a, b, c]);
	}

	/// Depth ordering takes precedence over insertion order
	#[test]
	fn depth_ordering() {
		let shallow = eid("shallow");
		let deep = eid("deep");

		let events = vec![
			(deep.clone(), vec![eid("root")], depth(10)),
			(shallow.clone(), vec![eid("root")], depth(1)),
		];

		let sorted = topo_sort_events(events);
		assert_eq!(sorted, vec![shallow, deep]);
	}
}

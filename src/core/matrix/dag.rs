use std::collections::{BTreeMap, BTreeSet};

use ruma::OwnedEventId;

use super::Event;

/// Topologically sorts a collection of events based on their `prev_events`.
///
/// Events will be yielded in causal order (oldest first).
/// When events are concurrent (no direct causal relationship), they are sorted
/// by `origin_server_ts` and then by `event_id` to ensure a deterministic
/// order.
pub fn sort_topologically<T, I>(events: I) -> Vec<T>
where
	T: Event,
	I: IntoIterator<Item = T>,
{
	let mut events_by_id = BTreeMap::new();
	let mut in_degree: BTreeMap<OwnedEventId, usize> = BTreeMap::new();
	let mut adjacency_list: BTreeMap<OwnedEventId, Vec<OwnedEventId>> = BTreeMap::new();

	// Collect all events and initialize structures
	for event in events {
		let event_id = event.event_id().to_owned();
		in_degree.insert(event_id.clone(), 0);
		events_by_id.insert(event_id, event);
	}

	// Build graph edges. Only consider edges between events within the batch.
	for (event_id, event) in &events_by_id {
		for prev_event_id in event.prev_events() {
			let prev_event_id_owned = prev_event_id.to_owned();
			if events_by_id.contains_key(&prev_event_id_owned) {
				// There is an edge from prev_event to current event
				adjacency_list
					.entry(prev_event_id_owned)
					.or_default()
					.push(event_id.clone());

				let degree = in_degree.entry(event_id.clone()).or_insert(0);
				*degree = degree.saturating_add(1);
			}
		}
	}

	// Find initial nodes with no incoming edges (in-degree == 0)
	// To ensure deterministic tie-breaking, we use a BTreeSet.
	// The tuple is (origin_server_ts, event_id)
	let mut zero_in_degree = BTreeSet::new();
	for (event_id, degree) in &in_degree {
		if *degree == 0 {
			if let Some(event) = events_by_id.get(event_id) {
				zero_in_degree.insert((event.origin_server_ts(), event_id.clone()));
			}
		}
	}

	let mut sorted_events = Vec::with_capacity(events_by_id.len());

	// Kahn's Algorithm
	while let Some(first) = zero_in_degree.pop_first() {
		let event_id = first.1;

		if let Some(event) = events_by_id.remove(&event_id) {
			sorted_events.push(event);
		}

		if let Some(neighbors) = adjacency_list.get(&event_id) {
			for neighbor_id in neighbors {
				if let Some(degree) = in_degree.get_mut(neighbor_id) {
					*degree = degree.saturating_sub(1);
					if *degree == 0 {
						if let Some(neighbor_event) = events_by_id.get(neighbor_id) {
							zero_in_degree
								.insert((neighbor_event.origin_server_ts(), neighbor_id.clone()));
						}
					}
				}
			}
		}
	}

	// Handle potential cycles (Matrix DAGs shouldn't have them, but for safety)
	// Any remaining events in `events_by_id` are part of a cycle.
	// We can just append them sorted by ts, id.
	if !events_by_id.is_empty() {
		let mut remaining: Vec<_> = events_by_id.into_values().collect();
		remaining.sort_by_key(|e| (e.origin_server_ts(), e.event_id().to_owned()));
		sorted_events.extend(remaining);
	}

	sorted_events
}

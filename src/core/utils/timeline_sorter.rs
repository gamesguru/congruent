use std::collections::{HashMap, HashSet};

use ruma::OwnedEventId;

use crate::PduCount;

/// Topological sort of a DAG using Kahn's algorithm.
///
/// Expects `graph[event_id]` = set of **parent** event IDs (i.e., the
/// `prev_events` or `auth_events` of `event_id`). This is the natural
/// representation in the Matrix event model.
///
/// Returns events in parent-before-child order. When multiple events have
/// in-degree 0 simultaneously, tiebreaks on `origin_server_ts` first
/// (chronological ordering within the same DAG level), then falls back to
/// `event_id` (content hash) for determinism when timestamps collide.
/// Events involved in cycles are appended at the end in the same order.
#[must_use]
pub fn sort_timeline_events<S: std::hash::BuildHasher>(
	entries: &HashMap<OwnedEventId, (PduCount, u64, u64), S>,
	graph: &HashMap<OwnedEventId, HashSet<OwnedEventId, S>, S>,
) -> Vec<OwnedEventId> {
	super::kahns_sort::kahn_sort(entries.keys().map(|event_id| {
		let parents = graph
			.get(event_id)
			.map(|parents| parents.iter().cloned())
			.into_iter()
			.flatten();
		let (depth, ts) = entries.get(event_id).map_or((0, 0), |(_, d, t)| (*d, *t));
		// Min-heap tiebreaker:
		// 1. smallest ts (chronological sorting of concurrent forks)
		// 2. smallest depth (fallback for identically-timestamped events)
		// 3. smallest event_id (deterministic cryptographic fallback)
		let key = (ts, depth, event_id.clone());
		(event_id.clone(), parents, key)
	}))
}

#[cfg(test)]
mod tests {
	use ruma::event_id;

	use super::*;

	#[test]
	fn test_topological_sort_clean() {
		let mut entries = HashMap::new();
		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();

		// Linear chain: A -> B -> C (B's parent is A, C's parent is B)
		let a = event_id!("$A").to_owned();
		let b = event_id!("$B").to_owned();
		let c = event_id!("$C").to_owned();

		entries.insert(a.clone(), (0_u64.into(), 1, 1));
		entries.insert(b.clone(), (0_u64.into(), 2, 2));
		entries.insert(c.clone(), (0_u64.into(), 3, 3));

		// graph[child] = {parents}
		graph.insert(b.clone(), vec![a.clone()].into_iter().collect());
		graph.insert(c.clone(), vec![b.clone()].into_iter().collect());

		let sorted = sort_timeline_events(&entries, &graph);
		// Parent-before-child: A, B, C
		assert_eq!(sorted, vec![a, b, c]);
	}

	#[test]
	fn test_topological_sort_with_cycle() {
		let mut entries = HashMap::new();
		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();

		// A -> B -> C -> A (cycle) with D disconnected (no parents)
		let a = event_id!("$A").to_owned();
		let b = event_id!("$B").to_owned();
		let c = event_id!("$C").to_owned();
		let d = event_id!("$D").to_owned();

		entries.insert(a.clone(), (0_u64.into(), 1, 10));
		entries.insert(b.clone(), (0_u64.into(), 2, 20));
		entries.insert(c.clone(), (0_u64.into(), 3, 30));
		entries.insert(d.clone(), (0_u64.into(), 0, 5));

		// graph[child] = {parents}
		graph.insert(b.clone(), vec![a.clone()].into_iter().collect());
		graph.insert(c.clone(), vec![b.clone()].into_iter().collect());
		graph.insert(a.clone(), vec![c.clone()].into_iter().collect());

		let sorted = sort_timeline_events(&entries, &graph);

		// D has 0 in-degree (no parents in the set), so it goes first.
		// A, B, C form a cycle, so they fall back to timestamp sorting.
		assert_eq!(sorted, vec![d, a, b, c]);
	}

	#[test]
	fn test_topological_sort_fork() {
		let mut entries = HashMap::new();
		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();

		// Root A, then B and C both have A as parent (fork)
		let a = event_id!("$A").to_owned();
		let b = event_id!("$B").to_owned();
		let c = event_id!("$C").to_owned();

		entries.insert(a.clone(), (0_u64.into(), 1, 1));
		entries.insert(b.clone(), (0_u64.into(), 2, 2));
		entries.insert(c.clone(), (0_u64.into(), 2, 3));

		graph.insert(b.clone(), vec![a.clone()].into_iter().collect());
		graph.insert(c.clone(), vec![a.clone()].into_iter().collect());

		let sorted = sort_timeline_events(&entries, &graph);
		// A first (root), then B and C ordered by timestamp
		assert_eq!(sorted[0], a);
		assert!(sorted.contains(&b));
		assert!(sorted.contains(&c));
	}

	#[test]
	fn test_topological_sort_diamond() {
		let mut entries = HashMap::new();
		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();

		// A -> B -> D, A -> C -> D (diamond)
		let a = event_id!("$A").to_owned();
		let b = event_id!("$B").to_owned();
		let c = event_id!("$C").to_owned();
		let d = event_id!("$D").to_owned();

		entries.insert(a.clone(), (0_u64.into(), 1, 1));
		entries.insert(b.clone(), (0_u64.into(), 2, 2));
		entries.insert(c.clone(), (0_u64.into(), 2, 3));
		entries.insert(d.clone(), (0_u64.into(), 3, 4));

		graph.insert(b.clone(), vec![a.clone()].into_iter().collect());
		graph.insert(c.clone(), vec![a.clone()].into_iter().collect());
		graph.insert(d.clone(), vec![b, c].into_iter().collect());

		let sorted = sort_timeline_events(&entries, &graph);
		// A must be first, D must be last
		assert_eq!(sorted[0], a);
		assert_eq!(sorted[3], d);
	}

	#[test]
	fn test_topological_sort_external_parents_ignored() {
		let mut entries = HashMap::new();
		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();

		// B's parent is Z, which is NOT in entries (external/already-known)
		let b = event_id!("$B").to_owned();
		let z = event_id!("$Z").to_owned();

		entries.insert(b.clone(), (0_u64.into(), 1, 1));

		graph.insert(b.clone(), vec![z].into_iter().collect());

		let sorted = sort_timeline_events(&entries, &graph);
		// B should still appear (Z is ignored because it's not in entries)
		assert_eq!(sorted, vec![b]);
	}

	#[test]
	fn test_tiebreak_by_ts() {
		let mut entries = HashMap::new();
		let graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();

		let a = event_id!("$A").to_owned();
		let b = event_id!("$B").to_owned();
		let c = event_id!("$C").to_owned();

		// All events have 0 in-degree (no parents).
		// B has the lowest depth, A has the lowest timestamp.
		entries.insert(a.clone(), (0_u64.into(), 3, 10));
		entries.insert(b.clone(), (0_u64.into(), 1, 30));
		entries.insert(c.clone(), (0_u64.into(), 2, 20));

		let sorted = sort_timeline_events(&entries, &graph);

		// The tie-breaker should sort by ts first: A (10), C (20), B (30)
		assert_eq!(sorted, vec![a, c, b]);
	}
}

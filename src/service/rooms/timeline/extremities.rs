use std::collections::{HashMap, HashSet};

use conduwuit_core::{Result, info, warn};
use futures::StreamExt;
use roaring::RoaringBitmap;
use ruma::{EventId, OwnedEventId};

use super::Service;
use crate::rooms::short::ShortEventId;

/// Detect stored extremities that are provably broken: they appear in the
/// window graph AND have children there (meaning they shouldn't be tips).
///
/// Returns the set of phantom tip event IDs. An empty return means the stored
/// extremities are consistent with the local DAG window.
///
/// This avoids false positives from:
/// - Stored extremities outside the tail window (unverifiable)
/// - MAX_FORWARD_EXTREMITIES capping (stored set is a subset of true tips)
pub fn detect_phantom_extremities<S1, S2, S3>(
	graph: &HashMap<OwnedEventId, HashSet<OwnedEventId, S2>, S1>,
	stored_extremities: &HashSet<OwnedEventId, S3>,
) -> Vec<OwnedEventId>
where
	S1: std::hash::BuildHasher,
	S2: std::hash::BuildHasher,
	S3: std::hash::BuildHasher,
{
	let has_children: HashSet<&OwnedEventId> =
		graph.values().flat_map(|parents| parents.iter()).collect();

	stored_extremities
		.iter()
		.filter(|eid| has_children.contains(eid))
		.cloned()
		.collect()
}

/// Calculate the true forward extremities of a DAG: events that have no
/// children (i.e., are not referenced as a parent by any other event).
///
/// Falls back to the last element in `sorted` if the entire graph is cyclic.
pub fn calculate_true_extremities<'a, S1, S2>(
	graph: &HashMap<OwnedEventId, HashSet<OwnedEventId, S2>, S1>,
	sorted: &'a [OwnedEventId],
) -> Vec<&'a EventId>
where
	S1: std::hash::BuildHasher,
	S2: std::hash::BuildHasher,
{
	let mut has_children: HashSet<OwnedEventId> = HashSet::new();
	for parents in graph.values() {
		for parent in parents {
			has_children.insert(parent.clone());
		}
	}

	let mut true_extremities: Vec<&EventId> = sorted
		.iter()
		.filter(|eid| !has_children.contains(*eid))
		.map(AsRef::as_ref)
		.collect();

	if true_extremities.is_empty() {
		if let Some(last_event_id) = sorted.last() {
			true_extremities.push(last_event_id.as_ref());
		}
	}

	true_extremities
}

/// Merge newly calculated true extremities with the current stored set,
/// removing phantom tips that have been proven stale.
pub fn merge_true_extremities<S: ::std::hash::BuildHasher>(
	true_extremities: Vec<&EventId>,
	current_set: &HashSet<OwnedEventId, S>,
	phantom_tips: &[OwnedEventId],
) -> HashSet<OwnedEventId> {
	let mut true_extremities_set: HashSet<OwnedEventId> = true_extremities
		.into_iter()
		.map(ToOwned::to_owned)
		.collect();

	for eid in current_set {
		if !phantom_tips.contains(eid) {
			true_extremities_set.insert(eid.clone());
		}
	}

	true_extremities_set
}

// --- Roaring bitmap variants for high-performance extremity calculations ---

#[must_use]
pub fn detect_phantom_extremities_roaring(
	graph: &[RoaringBitmap],
	stored_extremities: &RoaringBitmap,
) -> RoaringBitmap {
	let mut has_children = RoaringBitmap::new();
	for parents in graph {
		has_children |= parents;
	}

	stored_extremities & has_children
}

#[must_use]
pub fn calculate_true_extremities_roaring(
	graph: &[RoaringBitmap],
	sorted: &[u32],
) -> RoaringBitmap {
	let mut has_children = RoaringBitmap::new();
	for parents in graph {
		has_children |= parents;
	}

	let mut true_extremities = RoaringBitmap::new();
	for &id in sorted {
		if !has_children.contains(id) {
			true_extremities.insert(id);
		}
	}

	if true_extremities.is_empty() {
		if let Some(&last_id) = sorted.last() {
			true_extremities.insert(last_id);
		}
	}

	true_extremities
}

#[must_use]
pub fn merge_true_extremities_roaring(
	true_extremities: &RoaringBitmap,
	current_set: &RoaringBitmap,
	phantom_tips: &RoaringBitmap,
) -> RoaringBitmap {
	let mut true_extremities_set = true_extremities.clone();
	let valid_current = <&RoaringBitmap as std::ops::Sub>::sub(current_set, phantom_tips);
	true_extremities_set |= valid_current;
	true_extremities_set
}

fn merge_recalculated_extremities<I>(
	mut true_extremities: HashSet<OwnedEventId>,
	graph_event_ids: &HashSet<ShortEventId>,
	current_extremities: I,
) -> HashSet<OwnedEventId>
where
	I: IntoIterator<Item = (OwnedEventId, Option<ShortEventId>)>,
{
	for (event_id, shorteventid) in current_extremities {
		if shorteventid.is_none_or(|short| !graph_event_ids.contains(&short)) {
			true_extremities.insert(event_id);
		}
	}

	true_extremities
}

fn accepted_extremity_graph(
	raw_edges: &HashMap<ShortEventId, Vec<ShortEventId>>,
	accepted_events: &HashSet<ShortEventId>,
	bridge_events: &HashSet<ShortEventId>,
) -> Vec<(ShortEventId, Vec<ShortEventId>)> {
	let mut graph_edges = Vec::with_capacity(accepted_events.len());

	for &short_id in accepted_events {
		let mut parents = Vec::new();
		let mut stack = raw_edges.get(&short_id).cloned().unwrap_or_default();
		let mut seen = HashSet::new();

		while let Some(prev) = stack.pop() {
			if !seen.insert(prev) {
				continue;
			}

			if bridge_events.contains(&prev) {
				if let Some(prevs) = raw_edges.get(&prev) {
					stack.extend(prevs.iter().copied());
				}
			} else {
				parents.push(prev);
			}
		}

		graph_edges.push((short_id, parents));
	}

	graph_edges
}

impl Service {
	/// Prune fork storms down to operationally relevant tips using tail-based
	/// recalculation. This is a convenience wrapper around
	/// `recalculate_extremities` with standardized logging.
	pub async fn prune_extremities(&self, room_id: &ruma::RoomId, tail: usize) {
		match self.recalculate_extremities(room_id, true).await {
			| Ok((true, tips)) => info!(
				%room_id, tail, tips,
				"pruned extremities via tail-based recalculation"
			),
			| Ok((false, tips)) => info!(
				%room_id, tail, tips,
				"extremities already consistent after recalculation"
			),
			| Err(e) => warn!(
				%room_id, tail,
				"failed to prune extremities: {e}"
			),
		}
	}

	/// Automatically recalculates the true topological DAG forward extremities
	/// by querying the last `tail` events from the room's timeline and
	/// analyzing their `prev_events` graph to find all nodes with out-degree
	/// 0. Optionally overwrites the stored forward extremities if `update_db`
	///    is true.
	/// Returns true if the extremities were changed (or would be changed).
	#[tracing::instrument(skip(self), level = "info")]
	pub async fn recalculate_extremities(
		&self,
		room_id: &ruma::RoomId,
		update_db: bool,
	) -> Result<(bool, usize)> {
		let state_lock = self.services.state.mutex.lock(room_id).await;

		let mut stream =
			std::pin::pin!(self.db.room_shorteventids_rev(room_id, None).chunks(1024));
		let mut raw_edges = HashMap::new();
		let mut accepted_event_ids = HashSet::new();
		let mut bridge_event_ids = HashSet::new();
		let mut outlier_event_ids = HashSet::new();
		let mut missing_shortprevevents = 0_usize;
		let mut missing_shortprevevents_samples = Vec::new();
		let mut skipped_unresolved_shortids = 0_usize;
		let mut missing_metadata = 0_usize;

		while let Some(chunk) = stream.next().await {
			let short_ids: Vec<ShortEventId> = chunk.into_iter().filter_map(Result::ok).collect();

			// Fast path: bulk resolve ShortEventId -> shortprevevents
			let prevs_stream = self
				.db
				.multi_get_shortprevevents(futures::stream::iter(short_ids.clone()));
			let all_prevs: Vec<Result<Vec<ShortEventId>>> = prevs_stream.collect().await;

			for (short_id, prevs_res) in short_ids.into_iter().zip(all_prevs.into_iter()) {
				let prevs = match prevs_res {
					| Ok(prevs) => prevs,
					| Err(_) => {
						missing_shortprevevents = missing_shortprevevents.saturating_add(1);
						if missing_shortprevevents_samples.len() < 8 {
							missing_shortprevevents_samples.push(short_id);
						}
						Vec::new()
					},
				};
				raw_edges.insert(short_id, prevs);

				let Ok(event_id) = self
					.services
					.short
					.get_eventid_from_short::<OwnedEventId>(short_id)
					.await
				else {
					skipped_unresolved_shortids = skipped_unresolved_shortids.saturating_add(1);
					continue;
				};

				match self.db.get_event_metadata(&event_id).await {
					| Ok(meta) if meta.is_outlier => {
						outlier_event_ids.insert(short_id);
					},
					| Ok(meta) if meta.rejected || meta.soft_failed => {
						bridge_event_ids.insert(short_id);
					},
					| Ok(_) => {
						accepted_event_ids.insert(short_id);
					},
					| Err(_) => {
						missing_metadata = missing_metadata.saturating_add(1);
						accepted_event_ids.insert(short_id);
					},
				}
			}
		}

		if missing_shortprevevents > 0 {
			warn!(
				%room_id,
				scanned_events = raw_edges.len(),
				missing_shortprevevents,
				?missing_shortprevevents_samples,
				"shortprevevents index holes while recalculating extremities; affected events \
				 were treated as DAG roots"
			);
		}
		if skipped_unresolved_shortids > 0 || missing_metadata > 0 {
			warn!(
				%room_id,
				scanned_events = raw_edges.len(),
				skipped_unresolved_shortids,
				missing_metadata,
				"event metadata gaps while recalculating extremities"
			);
		}

		let graph_edges =
			accepted_extremity_graph(&raw_edges, &accepted_event_ids, &bridge_event_ids);

		// Lightning fast true forward extremity computation (entire room history) via
		// rezzy
		let true_tips_short = rezzy::state::at::find_forward_extremities_roaring(graph_edges);
		let calculated_tips = true_tips_short.len();

		let current_extremities = self.services.state.get_forward_extremities(room_id);
		let current_set: HashSet<_> = current_extremities.collect().await;

		let mut true_extremities_set: HashSet<OwnedEventId> =
			HashSet::with_capacity(true_tips_short.len());
		for short in true_tips_short {
			if let Ok(eid) = self
				.services
				.short
				.get_eventid_from_short::<OwnedEventId>(short)
				.await
			{
				true_extremities_set.insert(eid.clone());
			}
		}

		let mut current_extremities = Vec::with_capacity(current_set.len());
		for eid in &current_set {
			let shorteventid = self.services.short.get_shorteventid(eid).await.ok();
			current_extremities.push((eid.clone(), shorteventid));
		}

		let retained_current_outside_graph = current_extremities
			.iter()
			.filter(|(_, short)| short.is_none_or(|short| !accepted_event_ids.contains(&short)))
			.count();
		let dropped_stale_current = current_extremities
			.iter()
			.filter(|(event_id, short)| {
				short.is_some_and(|short| accepted_event_ids.contains(&short))
					&& !true_extremities_set.contains(event_id)
			})
			.count();

		let true_extremities_set = merge_recalculated_extremities(
			true_extremities_set,
			&accepted_event_ids,
			current_extremities,
		);

		let mut final_extremities: Vec<OwnedEventId> = true_extremities_set.into_iter().collect();

		let mut final_ts_map = HashMap::with_capacity(final_extremities.len());
		for eid in &final_extremities {
			let ts = self
				.db
				.get_origin_server_ts(eid)
				.await
				.unwrap_or_else(|_| ruma::MilliSecondsSinceUnixEpoch(0_u32.into()));
			final_ts_map.insert(eid.clone(), ts);
		}

		final_extremities.sort_by_key(|eid| {
			final_ts_map
				.get(eid)
				.copied()
				.unwrap_or_else(|| ruma::MilliSecondsSinceUnixEpoch(0_u32.into()))
		});

		let num_true_extremities = final_extremities.len();

		// If the finalized extremities perfectly match the current DB, we skip
		let final_set: HashSet<_> = final_extremities.iter().cloned().collect();
		let changed = final_set != current_set;
		if missing_shortprevevents > 0 || dropped_stale_current > 0 {
			warn!(
				%room_id,
				update_db,
				changed,
				scanned_events = raw_edges.len(),
				accepted_events = accepted_event_ids.len(),
				bridge_events = bridge_event_ids.len(),
				outlier_events = outlier_event_ids.len(),
				current_tips = current_set.len(),
				calculated_tips,
				final_tips = num_true_extremities,
				retained_current_outside_graph,
				dropped_stale_current,
				missing_shortprevevents,
				skipped_unresolved_shortids,
				missing_metadata,
				"extremity recalculation found DAG/index anomalies"
			);
		} else if changed {
			info!(
				%room_id,
				update_db,
				scanned_events = raw_edges.len(),
				accepted_events = accepted_event_ids.len(),
				bridge_events = bridge_event_ids.len(),
				outlier_events = outlier_event_ids.len(),
				current_tips = current_set.len(),
				calculated_tips,
				final_tips = num_true_extremities,
				retained_current_outside_graph,
				"extremity recalculation changed room tips"
			);
		}

		if final_set == current_set {
			return Ok((false, num_true_extremities));
		}

		if update_db {
			self.services
				.state
				.set_forward_extremities(
					room_id,
					final_extremities.into_iter(),
					None,
					&state_lock,
				)
				.await;
		}

		Ok((true, num_true_extremities))
	}
}

#[cfg(test)]
mod tests {
	use HashMap;
	use ruma::{OwnedEventId, event_id};

	use super::*;

	#[test]
	fn test_calculate_true_extremities_00_single_tip() {
		let a = event_id!("$a").to_owned();
		let b = event_id!("$b").to_owned();
		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();
		graph.insert(b.clone(), vec![a.clone()].into_iter().collect());

		let sorted = vec![a, b.clone()];
		let tips = calculate_true_extremities(&graph, &sorted);
		let expected: Vec<&EventId> = vec![b.as_ref()];
		assert_eq!(tips, expected);
	}

	#[test]
	fn test_calculate_true_extremities_01_fork() {
		let a = event_id!("$a").to_owned();
		let b = event_id!("$b").to_owned();
		let c = event_id!("$c").to_owned();

		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();
		graph.insert(b.clone(), vec![a.clone()].into_iter().collect());
		graph.insert(c.clone(), vec![a.clone()].into_iter().collect());

		let sorted = vec![a, b.clone(), c.clone()];
		let tips = calculate_true_extremities(&graph, &sorted);
		let expected: Vec<&EventId> = vec![b.as_ref(), c.as_ref()];
		assert_eq!(tips, expected);
	}

	#[test]
	fn test_calculate_true_extremities_02_diamond() {
		let a = event_id!("$a").to_owned();
		let b = event_id!("$b").to_owned();
		let c = event_id!("$c").to_owned();
		let d = event_id!("$d").to_owned();
		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();

		graph.insert(b.clone(), vec![a.clone()].into_iter().collect());
		graph.insert(c.clone(), vec![a.clone()].into_iter().collect());
		graph.insert(d.clone(), vec![b.clone(), c.clone()].into_iter().collect());

		let sorted = vec![a, b, c, d.clone()];
		let tips = calculate_true_extremities(&graph, &sorted);
		let expected: Vec<&EventId> = vec![&*d];
		assert_eq!(tips, expected);
	}

	#[test]
	fn test_calculate_true_extremities_03_islands() {
		let a = event_id!("$a").to_owned();
		let b = event_id!("$b").to_owned();
		let x = event_id!("$x").to_owned();
		let y = event_id!("$y").to_owned();
		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();

		graph.insert(b.clone(), vec![a.clone()].into_iter().collect());
		graph.insert(y.clone(), vec![x.clone()].into_iter().collect());

		let sorted = vec![a, b.clone(), x, y.clone()];
		let mut tips = calculate_true_extremities(&graph, &sorted);
		tips.sort();

		let mut expected: Vec<&EventId> = vec![&*b, &*y];
		expected.sort();

		assert_eq!(tips, expected);
	}

	#[test]
	fn test_calculate_true_extremities_04_missing_parents() {
		let a = event_id!("$a").to_owned();
		let z = event_id!("$z").to_owned(); // not in sorted, but referenced
		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();

		graph.insert(a.clone(), vec![z].into_iter().collect());

		let sorted = vec![a.clone()];
		let tips = calculate_true_extremities(&graph, &sorted);
		let expected: Vec<&EventId> = vec![&*a];
		assert_eq!(tips, expected);
	}

	#[test]
	fn test_calculate_true_extremities_05_missing_from_graph() {
		let a = event_id!("$a").to_owned();
		let b = event_id!("$b").to_owned();

		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();
		// Graph only knows about A's parents (none). B is omitted from the map
		// entirely.
		graph.insert(a.clone(), HashSet::new());

		let sorted = vec![a.clone(), b.clone()];
		let mut tips = calculate_true_extremities(&graph, &sorted);

		// Because B is in `sorted` and nothing in `graph` lists B as a parent, B must
		// be a tip. A is also a tip because nothing lists it as a parent.
		tips.sort();
		let mut expected: Vec<&EventId> = vec![&*a, &*b];
		expected.sort();

		assert_eq!(tips, expected);
	}

	#[test]
	fn test_calculate_true_extremities_06_cycle_fallback() {
		let a = event_id!("$a").to_owned();
		let b = event_id!("$b").to_owned();

		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();
		graph.insert(b.clone(), vec![a.clone()].into_iter().collect());
		graph.insert(a.clone(), vec![b.clone()].into_iter().collect());

		let sorted = vec![a, b.clone()];
		let tips = calculate_true_extremities(&graph, &sorted);

		// Fallback returns the last element in `sorted`
		let expected: Vec<&EventId> = vec![&*b];
		assert_eq!(tips, expected);
	}

	#[test]
	fn test_calculate_true_extremities_07_no_cap() {
		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();
		let mut sorted = Vec::new();
		let root = event_id!("$root").to_owned();
		sorted.push(root.clone());

		for i in 0..25 {
			let id: OwnedEventId = format!("$tip{i}").try_into().unwrap();
			graph.insert(id.clone(), vec![root.clone()].into_iter().collect());
			sorted.push(id);
		}

		let tips = calculate_true_extremities(&graph, &sorted);
		// No cap here — capping is done at the DB writer level
		// (set_forward_extremities) with MAX_FORWARD_EXTREMITIES = 10.
		assert_eq!(tips.len(), 25);
		assert_eq!(tips[0].as_str(), "$tip0");
		assert_eq!(tips[24].as_str(), "$tip24");
	}

	#[test]
	fn test_calculate_true_extremities_08_empty_input() {
		let graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();
		let sorted = vec![];
		let tips = calculate_true_extremities(&graph, &sorted);
		assert!(tips.is_empty(), "Empty graph should return empty extremities");
	}

	#[test]
	fn test_calculate_true_extremities_09_extraneous_graph_data() {
		let a = event_id!("$a").to_owned();
		let b = event_id!("$b").to_owned();
		let old = event_id!("$old").to_owned();
		let older = event_id!("$older").to_owned();

		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();

		graph.insert(b.clone(), vec![a.clone()].into_iter().collect());
		// Extraneous data outside the 'sorted' window
		graph.insert(old, vec![older].into_iter().collect());

		let sorted = vec![a, b.clone()];
		let tips = calculate_true_extremities(&graph, &sorted);

		let expected: Vec<&EventId> = vec![&*b];
		assert_eq!(tips, expected);
	}

	#[test]
	fn test_calculate_true_extremities_10_out_of_order() {
		let a = event_id!("$a").to_owned();
		let b = event_id!("$b").to_owned();
		let c = event_id!("$c").to_owned();

		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();

		// Linear chain: A -> B -> C
		graph.insert(b.clone(), vec![a.clone()].into_iter().collect());
		graph.insert(c.clone(), vec![b.clone()].into_iter().collect());

		// Array is passed in completely scrambled chronological order
		let sorted = vec![c.clone(), a, b];
		let tips = calculate_true_extremities(&graph, &sorted);

		// Even though C was first in the array, A and B are in has_children.
		// The algorithm correctly identifies C as the sole extremity.
		let expected: Vec<&EventId> = vec![&*c];
		assert_eq!(tips, expected);
	}

	// --- detect_phantom_extremities tests ---

	#[test]
	fn test_phantom_11_no_drift_linear() {
		// Linear chain A -> B -> C, stored extremity is C (correct)
		let a = event_id!("$a").to_owned();
		let b = event_id!("$b").to_owned();
		let c = event_id!("$c").to_owned();

		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();
		graph.insert(b.clone(), vec![a].into_iter().collect());
		graph.insert(c.clone(), vec![b].into_iter().collect());

		let stored: HashSet<OwnedEventId> = vec![c].into_iter().collect();
		let phantoms = detect_phantom_extremities(&graph, &stored);
		assert!(phantoms.is_empty(), "correct tip should not be phantom");
	}

	#[test]
	fn test_phantom_12_real_drift() {
		// Linear chain A -> B -> C, but stored extremity is A (has children)
		let a = event_id!("$a").to_owned();
		let b = event_id!("$b").to_owned();
		let c = event_id!("$c").to_owned();

		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();
		graph.insert(b.clone(), vec![a.clone()].into_iter().collect());
		graph.insert(c, vec![b].into_iter().collect());

		let stored: HashSet<OwnedEventId> = vec![a.clone()].into_iter().collect();
		let phantoms = detect_phantom_extremities(&graph, &stored);
		assert_eq!(phantoms, vec![a], "A has children and is phantom");
	}

	#[test]
	fn test_phantom_13_out_of_window_tolerated() {
		// Window only has B -> C, but stored extremity includes $old (outside window)
		let b = event_id!("$b").to_owned();
		let c = event_id!("$c").to_owned();
		let old = event_id!("$old").to_owned();

		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();
		graph.insert(c.clone(), vec![b].into_iter().collect());

		// $old is stored but not in the window graph at all
		let stored: HashSet<OwnedEventId> = vec![c, old].into_iter().collect();
		let phantoms = detect_phantom_extremities(&graph, &stored);
		assert!(phantoms.is_empty(), "out-of-window extremity should not be flagged");
	}

	#[test]
	fn test_phantom_14_capped_subset_ok() {
		// 25 fork tips from a root, but stored set is capped to 10
		let root = event_id!("$root").to_owned();
		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();

		let mut all_tips = Vec::new();
		for i in 0..25 {
			let id: OwnedEventId = format!("$tip{i}").try_into().unwrap();
			graph.insert(id.clone(), vec![root.clone()].into_iter().collect());
			all_tips.push(id);
		}

		// Stored set is a capped subset (first 10 tips)
		let stored: HashSet<OwnedEventId> = all_tips[..10].iter().cloned().collect();
		let phantoms = detect_phantom_extremities(&graph, &stored);
		assert!(phantoms.is_empty(), "capped tips are still valid tips");
	}

	#[test]
	fn test_phantom_15_mixed_valid_and_phantom() {
		// A -> B -> C, stored = {A, C}. A is phantom (has child B), C is valid.
		let a = event_id!("$a").to_owned();
		let b = event_id!("$b").to_owned();
		let c = event_id!("$c").to_owned();

		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();
		graph.insert(b.clone(), vec![a.clone()].into_iter().collect());
		graph.insert(c.clone(), vec![b].into_iter().collect());

		let stored: HashSet<OwnedEventId> = vec![a.clone(), c].into_iter().collect();
		let phantoms = detect_phantom_extremities(&graph, &stored);
		assert_eq!(phantoms, vec![a], "only A is phantom, C is valid");
	}

	// --- merge_true_extremities tests ---

	#[test]
	fn test_merge_true_extremities() {
		let e1 = event_id!("$1").to_owned();
		let e2 = event_id!("$2").to_owned();
		let e3 = event_id!("$3").to_owned();
		let e4 = event_id!("$4").to_owned();

		// newly discovered true extremity
		let true_exts = vec![&*e1];

		// current tips in DB
		let current_set: HashSet<OwnedEventId> = vec![e2.clone(), e3.clone(), e4.clone()]
			.into_iter()
			.collect();

		// phantom tips: e2 and e3 are phantoms
		let phantoms = vec![e2.clone(), e3.clone()];

		let result = merge_true_extremities(true_exts, &current_set, &phantoms);

		// Result should be e1 (true) and e4 (preserved from current_set because it's
		// not a phantom)
		assert_eq!(result.len(), 2);
		assert!(result.contains(&e1));
		assert!(result.contains(&e4));
		assert!(!result.contains(&e2));
		assert!(!result.contains(&e3));
	}

	#[test]
	fn test_merge_recalculated_extremities_drops_accepted_graph_stale_leaf() {
		let stale_leaf = event_id!("$stale").to_owned();
		let child_tip = event_id!("$child").to_owned();
		let out_of_graph = event_id!("$outlier").to_owned();

		let true_extremities: HashSet<OwnedEventId> =
			vec![child_tip.clone()].into_iter().collect();
		let graph_event_ids: HashSet<ShortEventId> = vec![1, 2].into_iter().collect();
		let current_extremities = vec![
			(stale_leaf.clone(), Some(1)),
			(child_tip.clone(), Some(2)),
			(out_of_graph.clone(), None),
		];

		let result = merge_recalculated_extremities(
			true_extremities,
			&graph_event_ids,
			current_extremities,
		);

		assert!(result.contains(&child_tip));
		assert!(result.contains(&out_of_graph));
		assert!(
			!result.contains(&stale_leaf),
			"stored extremity with an accepted graph child must not survive recalculation"
		);
	}

	#[test]
	fn test_accepted_extremity_graph_ignores_bad_children_but_bridges_through_them() {
		let root = 1;
		let rejected = 2;
		let accepted_tip = 3;
		let accepted_sibling = 4;
		let outlier = 5;
		let accepted_after_outlier = 6;

		let raw_edges: HashMap<ShortEventId, Vec<ShortEventId>> = HashMap::from([
			(root, Vec::new()),
			(rejected, vec![root]),
			(accepted_tip, vec![rejected]),
			(accepted_sibling, vec![root]),
			(outlier, vec![root]),
			(accepted_after_outlier, vec![outlier]),
		]);
		let accepted_events: HashSet<ShortEventId> =
			HashSet::from([root, accepted_tip, accepted_sibling, accepted_after_outlier]);
		let bridge_events: HashSet<ShortEventId> = HashSet::from([rejected]);

		let graph = accepted_extremity_graph(&raw_edges, &accepted_events, &bridge_events);
		let parents_by_event: HashMap<ShortEventId, HashSet<ShortEventId>> = graph
			.into_iter()
			.map(|(event, parents)| (event, parents.into_iter().collect()))
			.collect();

		assert_eq!(parents_by_event.get(&root).unwrap().len(), 0);
		assert!(
			parents_by_event.get(&accepted_tip).unwrap().contains(&root),
			"accepted child beyond rejected/soft-failed events should retire the older root"
		);
		assert!(
			parents_by_event
				.get(&accepted_sibling)
				.unwrap()
				.contains(&root)
		);
		assert!(
			!parents_by_event.contains_key(&rejected),
			"rejected/soft-failed events must not become recalculated tips"
		);
		assert!(
			!parents_by_event
				.get(&accepted_after_outlier)
				.unwrap()
				.contains(&root),
			"outliers must not bridge an accepted event back to an older root"
		);
	}

	#[test]
	fn test_accepted_extremity_graph_stress_mixed_real_world_edges() {
		let root = 1;
		let fork_a = 2;
		let fork_b = 3;
		let rejected_a = 4;
		let soft_failed_a = 5;
		let accepted_after_bad_chain = 6;
		let outlier_a = 7;
		let accepted_after_outlier = 8;
		let merge_tip = 9;
		let independent_tip = 10;
		let rejected_only_child = 11;
		let outlier_only_child = 12;
		let accepted_after_rejected_only_child = 13;

		let raw_edges: HashMap<ShortEventId, Vec<ShortEventId>> = HashMap::from([
			(root, Vec::new()),
			(fork_a, vec![root]),
			(fork_b, vec![root]),
			(rejected_a, vec![fork_a]),
			(soft_failed_a, vec![rejected_a]),
			(accepted_after_bad_chain, vec![soft_failed_a]),
			(outlier_a, vec![fork_b]),
			(accepted_after_outlier, vec![outlier_a]),
			(merge_tip, vec![accepted_after_bad_chain, fork_b]),
			(independent_tip, vec![root]),
			(rejected_only_child, vec![independent_tip]),
			(outlier_only_child, vec![accepted_after_outlier]),
			(accepted_after_rejected_only_child, vec![rejected_only_child]),
		]);
		let accepted_events: HashSet<ShortEventId> = HashSet::from([
			root,
			fork_a,
			fork_b,
			accepted_after_bad_chain,
			accepted_after_outlier,
			merge_tip,
			independent_tip,
			accepted_after_rejected_only_child,
		]);
		let bridge_events: HashSet<ShortEventId> =
			HashSet::from([rejected_a, soft_failed_a, rejected_only_child]);

		let graph = accepted_extremity_graph(&raw_edges, &accepted_events, &bridge_events);
		let tips = rezzy::state::at::find_forward_extremities_roaring(graph.clone());
		let tips: HashSet<ShortEventId> = tips.into_iter().collect();

		assert!(tips.contains(&merge_tip));
		assert!(tips.contains(&accepted_after_outlier));
		assert!(tips.contains(&accepted_after_rejected_only_child));
		assert_eq!(
			tips.len(),
			3,
			"only accepted timeline leaves should remain as recalculated tips"
		);
		assert!(
			!tips.contains(&independent_tip),
			"accepted child beyond rejected event should retire the older tip"
		);
		assert!(
			!tips.contains(&fork_a),
			"accepted child beyond rejected/soft-failed chain should retire fork_a"
		);
		assert!(
			!tips.contains(&fork_b),
			"direct accepted merge should retire fork_b (even if an outlier also references it)"
		);

		let current_event_ids = HashSet::from([
			event_id!("$fork_a").to_owned(),
			event_id!("$fork_b").to_owned(),
			event_id!("$merge_tip").to_owned(),
			event_id!("$accepted_after_outlier").to_owned(),
			event_id!("$external_tip").to_owned(),
		]);
		let true_extremities = HashSet::from([
			event_id!("$merge_tip").to_owned(),
			event_id!("$accepted_after_outlier").to_owned(),
			event_id!("$accepted_after_rejected_only_child").to_owned(),
		]);
		let current_extremities = vec![
			(event_id!("$fork_a").to_owned(), Some(fork_a)),
			(event_id!("$fork_b").to_owned(), Some(fork_b)),
			(event_id!("$merge_tip").to_owned(), Some(merge_tip)),
			(event_id!("$accepted_after_outlier").to_owned(), Some(accepted_after_outlier)),
			(event_id!("$external_tip").to_owned(), None),
		];

		let merged = merge_recalculated_extremities(
			true_extremities,
			&accepted_events,
			current_extremities,
		);

		assert!(!merged.contains(&event_id!("$fork_a").to_owned()));
		assert!(!merged.contains(&event_id!("$fork_b").to_owned()));
		assert!(merged.contains(&event_id!("$merge_tip").to_owned()));
		assert!(merged.contains(&event_id!("$accepted_after_outlier").to_owned()));
		assert!(merged.contains(&event_id!("$accepted_after_rejected_only_child").to_owned()));
		assert!(
			merged.contains(&event_id!("$external_tip").to_owned()),
			"current tips outside the accepted graph remain unverifiable and should be preserved"
		);
		assert_ne!(merged, current_event_ids);
	}

	// --- Roaring bitmap variant tests ---

	#[test]
	fn test_calculate_true_extremities_roaring_fork() {
		use roaring::RoaringBitmap;

		let mut graph = vec![RoaringBitmap::new(); 3];
		graph[1].insert(0); // 1 depends on 0
		graph[2].insert(0); // 2 depends on 0

		let sorted = vec![0, 1, 2];
		let tips = calculate_true_extremities_roaring(&graph, &sorted);

		let mut expected = RoaringBitmap::new();
		expected.insert(1);
		expected.insert(2);

		assert_eq!(tips, expected);
	}

	#[test]
	fn test_detect_phantom_extremities_roaring() {
		use roaring::RoaringBitmap;

		let mut graph = vec![RoaringBitmap::new(); 3];
		graph[1].insert(0);
		graph[2].insert(1);

		let mut stored_extremities = RoaringBitmap::new();
		stored_extremities.insert(0); // 0 is a phantom tip because it's a parent
		stored_extremities.insert(2); // 2 is a true tip

		let phantoms = detect_phantom_extremities_roaring(&graph, &stored_extremities);

		let mut expected = RoaringBitmap::new();
		expected.insert(0); // 0 should be detected as phantom

		assert_eq!(phantoms, expected);
	}

	#[test]
	fn test_merge_true_extremities_roaring() {
		use roaring::RoaringBitmap;

		let mut true_exts = RoaringBitmap::new();
		true_exts.insert(1);

		let mut current_set = RoaringBitmap::new();
		current_set.insert(2);
		current_set.insert(3);
		current_set.insert(4);

		let mut phantoms = RoaringBitmap::new();
		phantoms.insert(2);
		phantoms.insert(3);

		let result = merge_true_extremities_roaring(&true_exts, &current_set, &phantoms);

		let mut expected = RoaringBitmap::new();
		expected.insert(1); // from true_exts
		expected.insert(4); // from current_set (not a phantom)

		assert_eq!(result, expected);
	}
}

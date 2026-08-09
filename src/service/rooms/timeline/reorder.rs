use std::collections::HashMap;

use conduwuit_core::{
	Err, Result, debug, info,
	matrix::pdu::{PduCount, PduId, RawPduId},
	warn,
};
use ruma::{OwnedEventId, RoomId};

use super::Service;

impl Service {
	/// Rebuild the topological index for a room using proper DAG
	/// topological sort.
	///
	/// Reads all PDUs, builds the DAG from `prev_events`, performs a
	/// topological sort (parents before children, Kahn's algorithm with
	/// chronological tiebreaking), then rebuilds the
	/// `roomid_topologicalorder_pducount` index with correct
	/// `deprecated_local_topo_depth` values computed as
	/// `max(parent_depths) + 1`. Stream order
	/// (`room_pducount_eventid`) is NEVER modified — it is immutable
	/// arrival-time ordering.
	///
	/// Optionally recomputes state snapshots incrementally and repairs
	/// `unsigned.prev_content` on state events.
	pub async fn reorder_timeline(
		&self,
		room_id: &RoomId,
		no_compute_state: bool,
		force_reindex: bool,
	) -> Result<usize> {
		let shortroomid = self.services.short.get_or_create_shortroomid(room_id).await;
		let _insert_lock = if force_reindex {
			Some(self.mutex_insert.lock(room_id).await)
		} else {
			None
		};

		// Lightweight collection: reads only metadata + shortprevevents,
		// avoids full PDU JSON deserialization.
		debug!("reorder_timeline: collecting timeline entries (lightweight)...");
		let collect_start = std::time::Instant::now();
		let (entries, mut graph, mut metadata_cache) =
			self.db.collect_reorder_entries(room_id).await?;
		debug!(
			"reorder_timeline: collected {} PDUs in {:?} (lightweight)",
			entries.len(),
			collect_start.elapsed()
		);

		if entries.is_empty() {
			return Ok(0);
		}

		// Retain only edges within our event set for both topo sort and extremities.
		for parents in graph.values_mut() {
			parents.retain(|prev_id| entries.contains_key(prev_id));
		}

		// Topological sort: parents before children (Kahn's algorithm).
		// Tiebreak on origin_server_ts then event_id for determinism.
		let start = std::time::Instant::now();
		debug!("reorder_timeline: topologically sorting {} events...", entries.len());
		let sorted = conduwuit::utils::timeline_sorter::sort_timeline_events(&entries, &graph);
		debug!(
			"reorder_timeline: topo sort took {:?} ({} events)",
			start.elapsed(),
			sorted.len()
		);

		if sorted.len() != entries.len() {
			return Err!(Database(
				"reorder_timeline: topo sort dropped {} events (cycles or disconnected)",
				entries.len().saturating_sub(sorted.len())
			));
		}

		// Rebuild topological index
		let count = sorted.len();
		let reindex_start = std::time::Instant::now();
		debug!("reorder_timeline: rebuilding topological index for {count} events...");
		let mut available_counts: Vec<PduCount> = Vec::new();
		if force_reindex {
			available_counts = entries
				.values()
				.map(|(c, ..)| match c {
					| PduCount::Normal(n) => PduCount::Normal(*n),
					| PduCount::Backfilled(n) => PduCount::Normal(n.unsigned_abs()),
				})
				.collect();
			available_counts.sort();
			available_counts.dedup();
			// If dedup removed duplicates (abs collision), fill gaps with fresh
			// counts to maintain 1:1 mapping
			while available_counts.len() < entries.len() {
				let max = available_counts.last().map_or(1, |c| match c {
					| PduCount::Normal(n) => n.saturating_add(1),
					| PduCount::Backfilled(_) => 1,
				});
				available_counts.push(PduCount::Normal(max));
			}
		}

		// Fast mode: rebuild topo index only, no state computation.
		// Uses cached metadata to avoid all blocking DB reads.
		// Position in Kahn's sort IS the correct topological depth.
		// This ensures disconnected segments are interleaved chronologically
		// (Kahn's sort uses origin_server_ts as the tiebreaker for roots).
		let mut depths: HashMap<OwnedEventId, u64> = HashMap::with_capacity(sorted.len());
		for (topo_position, event_id) in sorted.iter().enumerate() {
			depths.insert(
				event_id.clone(),
				u64::try_from(topo_position)
					.expect("topo position fits u64")
					.saturating_add(1),
			);
		}

		// Every stream/eventid_pduid/metadata/topo write for the whole
		// rebuild goes into one `database::Batch`, applied atomically at the
		// end. The previous version wrote each of those four maps with its
		// own independent `.insert()`/`.remove()` call under a `cork()` --
		// `cork()` only buffers writes for I/O throughput, it gives no
		// atomicity guarantee, so a crash (or, since these aren't behind the
		// per-key locks the normal insert paths use, a concurrent reader)
		// mid-loop could observe `eventid_pduid`/`eventid_metadata` updated
		// for an event while `roomid_topologicalorder_pducount` still had
		// its old entry, or no entry at all. That split is exactly the
		// "phantom mapping" failure mode `get_pdu_id`'s callers can't
		// distinguish from a real timeline position -- see
		// docs/development-gg/backfill-v12-phantom-timeline-membership.md.
		let mut batch = database::Batch::new();
		let cleared_topo = self
			.db
			.clear_room_topo_index_into_batch(&mut batch, room_id)
			.await?;
		debug!("reorder_timeline: queued deletion of {cleared_topo} existing topo index rows");
		if force_reindex {
			for (event_id, &(old_count, ..)) in &entries {
				let old_pdu_id: RawPduId = PduId { shortroomid, shorteventid: old_count }.into();
				// Use cached depth to avoid blocking metadata read
				if let Some(meta) = metadata_cache.get(event_id) {
					self.db.remove_stream_and_topo_pducount_from_batch(
						&mut batch,
						&old_pdu_id,
						event_id.as_bytes(),
						Some(meta.deprecated_local_topo_depth),
					);
				} else {
					self.db.remove_stream_and_topo_pducount_into_batch(
						&mut batch,
						&old_pdu_id,
						event_id.as_bytes(),
					);
				}
			}
		}
		for (i, event_id) in sorted.iter().enumerate() {
			let &(existing_count, ..) = entries.get(event_id).expect("in sorted list");
			let new_count = if force_reindex {
				available_counts[i]
			} else {
				existing_count
			};
			let pdu_id: RawPduId = PduId { shortroomid, shorteventid: new_count }.into();
			let local_topo_depth = depths.get(event_id).copied().unwrap_or(0);

			if force_reindex {
				// Use cached metadata to avoid blocking DB reads
				if let Some(meta) = metadata_cache.get_mut(event_id) {
					self.db.replace_stream_topo_with_cached_metadata_batch(
						&mut batch,
						&pdu_id,
						event_id,
						local_topo_depth,
						new_count,
						meta,
					);
				} else {
					self.db.replace_stream_and_topo_pducount_batch(
						&mut batch,
						&pdu_id,
						event_id,
						local_topo_depth,
						new_count,
					);
				}
			} else {
				// Use cached metadata to avoid blocking DB reads
				if let Some(meta) = metadata_cache.get_mut(event_id) {
					self.db.reindex_topo_with_cached_metadata_batch(
						&mut batch,
						&pdu_id,
						event_id,
						local_topo_depth,
						meta,
					);
				} else {
					self.db
						.reindex_topo_batch(&mut batch, &pdu_id, event_id, local_topo_depth);
				}
			}
		}
		self.db.db_apply_batch(batch);
		debug!("reorder_timeline: topo rebuild took {:?}", reindex_start.elapsed());

		// Final batch: cork_and_sync ensures WAL is durable when dropped
		let final_sync = self.db.db.cork_and_sync();
		drop(final_sync);
		debug!("reorder_timeline: topo rebuild complete, calculating forward extremities...");

		let (_, true_extremities_count) = self.recalculate_extremities(room_id, true).await?;

		info!(
			"reorder_timeline: set forward extremities to {} true DAG tips",
			true_extremities_count
		);
		if true_extremities_count > 0 {
			if !no_compute_state {
				info!("reorder_timeline: bulk rebuilding state using rezzy...");
				if let Err(e) = self.rebuild_state(room_id).await {
					warn!("reorder_timeline: rebuild_state failed: {e:?}");
				}
			}
		}

		debug!("reorder_timeline: skipped repair unsigned per metadata design");

		// Rebuild membership cache from the authoritative state snapshot.
		// This fixes stale/missing entries left by previous DAG fractures.
		// Bootstrap room state if missing (e.g. first reorder after import).
		if self
			.services
			.state
			.get_room_shortstatehash(room_id)
			.await
			.is_err()
		{
			if let Some(latest_eid) = sorted.last() {
				if let Ok(ssh) = self
					.services
					.state_accessor
					.pdu_shortstatehash(latest_eid)
					.await
				{
					let state_lock = self.services.state.mutex.lock(room_id).await;
					self.services
						.state
						.set_room_state(room_id, ssh, &state_lock);
					info!(
						"reorder_timeline: bootstrapped room state to shortstatehash {ssh} from \
						 latest event {latest_eid}"
					);
				}
			}
		}

		self.services
			.state_cache
			.reconcile_membership(room_id)
			.await;

		debug!("reorder_timeline: complete, {count} events reordered (topo index/state)");

		Ok(count)
	}
}
#[cfg(test)]
mod tests {
	use std::collections::{HashMap, HashSet};

	use conduwuit::utils::timeline_sorter::sort_timeline_events;
	use conduwuit_core::PduCount;
	use ruma::{OwnedEventId, event_id};

	/// Compute position-based depths from a Kahn's sort result.
	/// This mirrors the logic in reorder_timeline /
	/// rebuild_topo_index_with_state.
	fn position_depths(sorted: &[OwnedEventId]) -> HashMap<OwnedEventId, u64> {
		sorted
			.iter()
			.enumerate()
			.map(|(pos, eid)| (eid.clone(), u64::try_from(pos).expect("fits").saturating_add(1)))
			.collect()
	}

	#[test]
	fn test_position_depths_linear_chain() {
		let a = event_id!("$A").to_owned();
		let b = event_id!("$B").to_owned();
		let c = event_id!("$C").to_owned();

		let mut entries = HashMap::new();
		entries.insert(a.clone(), (PduCount::from(0_u64), 1, 10));
		entries.insert(b.clone(), (PduCount::from(0_u64), 2, 20));
		entries.insert(c.clone(), (PduCount::from(0_u64), 3, 30));

		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();
		graph.insert(b.clone(), [a.clone()].into());
		graph.insert(c.clone(), [b.clone()].into());

		let sorted = sort_timeline_events(&entries, &graph);
		let depths = position_depths(&sorted);

		// Linear: A=1, B=2, C=3 — strictly increasing
		assert_eq!(depths[&a], 1);
		assert_eq!(depths[&b], 2);
		assert_eq!(depths[&c], 3);
	}

	#[test]
	fn test_position_depths_disconnected_segments() {
		// Two disconnected chains: A→B (ts 10,20) and X→Y (ts 15,25).
		// Position-based depths should interleave them chronologically.
		let a = event_id!("$A").to_owned();
		let b = event_id!("$B").to_owned();
		let x = event_id!("$X").to_owned();
		let y = event_id!("$Y").to_owned();

		let mut entries = HashMap::new();
		entries.insert(a.clone(), (PduCount::from(0_u64), 1, 10));
		entries.insert(b.clone(), (PduCount::from(0_u64), 2, 20));
		entries.insert(x.clone(), (PduCount::from(0_u64), 1, 15));
		entries.insert(y.clone(), (PduCount::from(0_u64), 2, 25));

		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();
		graph.insert(b.clone(), [a.clone()].into());
		graph.insert(y.clone(), [x.clone()].into());

		let sorted = sort_timeline_events(&entries, &graph);
		let depths = position_depths(&sorted);

		// All 4 events get unique, monotonically increasing depths 1..4
		let mut all_depths: Vec<u64> = depths.values().copied().collect();
		all_depths.sort();
		assert_eq!(all_depths, vec![1, 2, 3, 4]);

		// Parent always has lower depth than child
		assert!(depths[&a] < depths[&b]);
		assert!(depths[&x] < depths[&y]);
	}

	#[test]
	fn test_position_depths_diamond() {
		// A → B, A → C, B+C → D
		let a = event_id!("$A").to_owned();
		let b = event_id!("$B").to_owned();
		let c = event_id!("$C").to_owned();
		let d = event_id!("$D").to_owned();

		let mut entries = HashMap::new();
		entries.insert(a.clone(), (PduCount::from(0_u64), 1, 10));
		entries.insert(b.clone(), (PduCount::from(0_u64), 2, 20));
		entries.insert(c.clone(), (PduCount::from(0_u64), 2, 25));
		entries.insert(d.clone(), (PduCount::from(0_u64), 3, 30));

		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();
		graph.insert(b.clone(), [a.clone()].into());
		graph.insert(c.clone(), [a.clone()].into());
		graph.insert(d.clone(), [b.clone(), c.clone()].into());

		let sorted = sort_timeline_events(&entries, &graph);
		let depths = position_depths(&sorted);

		// A must have the lowest depth, D must have the highest
		assert_eq!(depths[&a], 1);
		assert_eq!(depths[&d], 4);
		// B and C are between A and D
		assert!(depths[&b] > depths[&a] && depths[&b] < depths[&d]);
		assert!(depths[&c] > depths[&a] && depths[&c] < depths[&d]);
	}

	#[test]
	fn test_position_depths_single_event() {
		let a = event_id!("$A").to_owned();
		let entries: HashMap<OwnedEventId, (PduCount, u64, u64)> =
			[(a.clone(), (PduCount::from(0_u64), 1, 10))].into();
		let graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();

		let sorted = sort_timeline_events(&entries, &graph);
		let depths = position_depths(&sorted);

		assert_eq!(depths[&a], 1);
	}

	#[test]
	fn test_position_depths_all_roots_sorted_by_ts() {
		// 5 unconnected events — should be sorted by timestamp
		let a = event_id!("$A").to_owned();
		let b = event_id!("$B").to_owned();
		let c = event_id!("$C").to_owned();

		let mut entries = HashMap::new();
		entries.insert(a.clone(), (PduCount::from(0_u64), 0, 30));
		entries.insert(b.clone(), (PduCount::from(0_u64), 0, 10));
		entries.insert(c.clone(), (PduCount::from(0_u64), 0, 20));

		let graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();

		let sorted = sort_timeline_events(&entries, &graph);
		let depths = position_depths(&sorted);

		// Sorted by ts: B(10)=1, C(20)=2, A(30)=3
		assert_eq!(depths[&b], 1);
		assert_eq!(depths[&c], 2);
		assert_eq!(depths[&a], 3);
	}

	#[test]
	fn test_position_depths_complex_fork_and_join() {
		// A -> B -> C -> F
		//   -> D -> E -/
		// (A forks to B and D. B->C, D->E. C and E join at F)
		let a = event_id!("$A").to_owned();
		let b = event_id!("$B").to_owned();
		let c = event_id!("$C").to_owned();
		let d = event_id!("$D").to_owned();
		let e = event_id!("$E").to_owned();
		let f = event_id!("$F").to_owned();

		let mut entries = HashMap::new();
		entries.insert(a.clone(), (PduCount::from(0_u64), 1, 10));
		entries.insert(b.clone(), (PduCount::from(0_u64), 2, 20));
		entries.insert(c.clone(), (PduCount::from(0_u64), 3, 30));
		entries.insert(d.clone(), (PduCount::from(0_u64), 2, 25));
		entries.insert(e.clone(), (PduCount::from(0_u64), 3, 35));
		entries.insert(f.clone(), (PduCount::from(0_u64), 4, 40));

		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();
		graph.insert(b.clone(), [a.clone()].into());
		graph.insert(c.clone(), [b.clone()].into());
		graph.insert(d.clone(), [a.clone()].into());
		graph.insert(e.clone(), [d.clone()].into());
		graph.insert(f.clone(), [c.clone(), e.clone()].into());

		let sorted = sort_timeline_events(&entries, &graph);
		let depths = position_depths(&sorted);

		// A is the root, F is the tip
		assert_eq!(depths[&a], 1);
		assert_eq!(depths[&f], 6);

		// Parents must strictly be lower depth than children
		assert!(depths[&a] < depths[&b]);
		assert!(depths[&a] < depths[&d]);
		assert!(depths[&b] < depths[&c]);
		assert!(depths[&d] < depths[&e]);
		assert!(depths[&c] < depths[&f]);
		assert!(depths[&e] < depths[&f]);
	}
}

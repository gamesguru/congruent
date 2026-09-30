use std::collections::HashMap;

use conduwuit_core::{
	Result, info,
	matrix::{
		event::Event,
		pdu::{PduCount, PduEvent, PduId, RawPduId},
	},
	warn,
};
use futures::StreamExt;
use roaring::RoaringBitmap;
use ruma::{CanonicalJsonObject, OwnedEventId, RoomId};

use super::update_unsigned_prev_content;

/// Options controlling what `heal_room` computes and writes.
pub struct HealOptions {
	/// Clear soft-fail/rejected markers on healed events.
	pub clear_markers: bool,

	/// Compute state snapshots incrementally for each event.
	/// When false, events are inserted without state computation (faster).
	pub compute_state: bool,

	/// Rebuild the membership cache from the final state snapshot.
	pub rebuild_membership: bool,

	/// Whether this heal invocation is being called from the reorder path.
	/// When true, events are expected to already be in the timeline.
	pub is_reorder: bool,
}

/// Result of a `heal_room` operation.
pub struct HealResult {
	/// Number of events successfully inserted into the timeline.
	pub inserted: usize,

	/// Number of events skipped (already in timeline, no JSON, etc.)
	pub skipped: usize,

	/// Number of events that failed to process.
	pub failed: usize,

	/// The forward extremities computed for this room.
	pub extremities: Vec<OwnedEventId>,
}

/// Safe u32 index from usize, saturating at u32::MAX.
#[inline]
fn idx32(v: usize) -> u32 { u32::try_from(v).unwrap_or(u32::MAX) }

/// Safe usize from u32.
#[inline]
fn idx(v: u32) -> usize { usize::try_from(v).unwrap_or(usize::MAX) }

/// Compact u32-indexed DAG for efficient topological sort and extremity
/// calculation. Avoids cloning `OwnedEventId` in the hot path by mapping
/// all event IDs to dense u32 indices.
struct CompactDag {
	/// Bidirectional mapping: event_id ↔ u32 index.
	id_to_idx: HashMap<OwnedEventId, u32>,
	idx_to_id: Vec<OwnedEventId>,

	/// origin_server_ts per index for sort tiebreaking.
	timestamps: Vec<u64>,

	/// Reverse adjacency: which indices have at least one child within our set.
	/// Used for extremity calculation (events NOT in this set are DAG tips).
	has_children: RoaringBitmap,
}

impl CompactDag {
	/// Build from a map of events, retaining only edges within the set.
	fn build(events: &HashMap<OwnedEventId, PduEvent>) -> Self {
		let n = events.len();
		let mut id_to_idx = HashMap::with_capacity(n);
		let mut idx_to_id = Vec::with_capacity(n);
		let mut timestamps = Vec::with_capacity(n);

		for (event_id, pdu) in events {
			let idx = u32::try_from(idx_to_id.len()).expect("event count fits u32");
			id_to_idx.insert(event_id.clone(), idx);
			idx_to_id.push(event_id.clone());
			timestamps.push(u64::from(pdu.origin_server_ts));
		}

		let mut has_children = RoaringBitmap::new();

		for event_id in &idx_to_id {
			let pdu = &events[event_id];
			for prev_id in pdu.prev_events() {
				if let Some(&pidx) = id_to_idx.get(prev_id) {
					has_children.insert(pidx);
				}
			}
		}

		Self {
			id_to_idx,
			idx_to_id,
			timestamps,
			has_children,
		}
	}

	/// Chronological sort by origin_server_ts ascending, then event_id.
	/// Returns sorted indices (oldest first). DAG edges are only used for
	/// extremity calculation, not ordering — topological sort produces bad
	/// client UX by interleaving old state events with recent messages.
	fn chrono_sort(&self) -> Vec<u32> {
		let n = self.idx_to_id.len();
		let mut indices: Vec<u32> = (0..idx32(n)).collect();
		indices.sort_by(|&a, &b| {
			self.timestamps[idx(a)]
				.cmp(&self.timestamps[idx(b)])
				.then_with(|| self.idx_to_id[idx(a)].cmp(&self.idx_to_id[idx(b)]))
		});
		indices
	}

	/// Compute forward extremities: events in sorted that have no children.
	fn extremities(&self, sorted: &[u32]) -> Vec<OwnedEventId> {
		sorted
			.iter()
			.filter(|&&i| !self.has_children.contains(i))
			.map(|&i| self.idx_to_id[idx(i)].clone())
			.collect()
	}

	/// Map a u32 index back to its event ID.
	#[inline]
	fn id(&self, i: u32) -> &OwnedEventId { &self.idx_to_id[idx(i)] }
}

#[conduwuit_core::implement(super::Service)]
pub async fn heal_room(
	&self,
	room_id: &RoomId,
	events: HashMap<OwnedEventId, PduEvent>,
	old_counts: Option<&HashMap<OwnedEventId, PduCount>>,
	options: &HealOptions,
) -> Result<HealResult> {
	if events.is_empty() {
		return Ok(HealResult {
			inserted: 0,
			skipped: 0,
			failed: 0,
			extremities: Vec::new(),
		});
	}

	let shortroomid = self.services.short.get_or_create_shortroomid(room_id).await;
	let state_lock = self.services.state.mutex.lock(room_id).await;

	info!("heal_room: processing {} events for {}", events.len(), room_id);

	// Phase 1: Build compact DAG with u32 indices + roaring bitmap
	let dag = CompactDag::build(&events);

	// Phase 2: Chronological sort by origin_server_ts
	info!("heal_room: sorting {} events by origin_server_ts...", events.len());
	let sorted = dag.chrono_sort();
	info!("heal_room: sorted {} events", sorted.len());

	if sorted.len() != events.len() {
		warn!("heal_room: sort dropped {} events", events.len().saturating_sub(sorted.len()));
	}

	// Phase 3: Stream order is immutable — we do NOT remove or backup
	// existing timeline entries. Events that already have a stream order
	// keep it. Only outlier promotions (events with no existing stream
	// order) will get a new count in Phase 4.

	// Phase 4: Insert events into the timeline. For events that already
	// have a stream order (existing timeline entries), preserve it. For
	// outlier promotions, assign a new stream count.
	let count = sorted.len();

	info!("heal_room: processing {count} events for timeline insertion");

	let mut current_shortstatehash = if options.compute_state {
		// Try to seed from the oldest event's prev_events state
		let mut ssh = 0_u64;
		if let Some(&oldest_idx) = sorted.first() {
			if let Some(oldest_pdu) = events.get(dag.id(oldest_idx)) {
				if let Some(prev) = oldest_pdu.prev_events.first() {
					if let Ok(prev_ssh) =
						self.services.state_accessor.pdu_shortstatehash(prev).await
					{
						ssh = prev_ssh;
					}
				}
			}
		}
		Some(ssh)
	} else {
		None
	};

	let mut inserted = 0_usize;
	let mut skipped = 0_usize;
	let mut failed = 0_usize;
	let mut cork = Some(self.db.db.cork());

	for &idx in &sorted {
		let event_id = dag.id(idx);
		let Some(pdu) = events.get(event_id) else {
			skipped = skipped.saturating_add(1);
			continue;
		};

		// Get the canonical JSON for this event
		let (pdu_from_db, mut json) = match self.db.get_from_eventid_pdu(event_id).await {
			| Ok(res) => res,
			| Err(e) => {
				warn!(%event_id, "heal_room: PDU JSON missing (skipping): {e}");
				failed = failed.saturating_add(1);
				continue;
			},
		};

		// Clear markers if requested
		if options.clear_markers {
			self.services.pdu_metadata.clear_pdu_markers(event_id);
		}

		// Use existing stream order if the event is already in the timeline.
		// Only assign a new count for outlier promotions (no existing count).
		let (pdu_count, pdu_id, is_new_insertion) =
			if let Some(&existing_count) = old_counts.and_then(|m| m.get(event_id)) {
				// Event already has a stream order — preserve it
				let pdu_id: RawPduId = PduId {
					shortroomid,
					shorteventid: existing_count,
				}
				.into();
				(existing_count, pdu_id, false)
			} else if let Ok(existing_pdu_id) = self.db.get_pdu_id(event_id).await {
				// Event is in timeline via eventid_pduid — preserve its count
				let existing_count = existing_pdu_id.pdu_count();
				(existing_count, existing_pdu_id, false)
			} else {
				// Outlier promotion — assign a new stream count
				let new_count = self.services.globals.next_count()?;
				let pdu_count = PduCount::Normal(new_count);
				let pdu_id: RawPduId = PduId { shortroomid, shorteventid: pdu_count }.into();
				(pdu_count, pdu_id, true)
			};

		// Rebuild topo depth. Since `sorted` is ordered, use max(parents) + 1.
		let mut max_depth = 0_u64;
		for prev_id in pdu.prev_events() {
			if let Some(meta) = self.db.get_event_metadata_blocking(prev_id) {
				max_depth = max_depth.max(meta.deprecated_local_topo_depth);
			}
		}
		let new_topo_depth = max_depth.saturating_add(1);

		// Compute state incrementally if enabled
		if let Some(mut ssh) = current_shortstatehash {
			Box::pin(self.compute_state_for_event(pdu, event_id, &mut json, &mut ssh, &pdu_id))
				.await;
			current_shortstatehash = Some(ssh);
		}

		// Write to timeline
		if is_new_insertion {
			// Pre-emptively update existing_metadata (if any) with the new depth
			// so append_pdu preserves the CORRECT depth instead of the old one.
			self.db.set_event_metadata_depth(event_id, new_topo_depth);
			self.db
				.append_pdu(&pdu_id, &pdu_from_db, &json, pdu_count)
				.await;
		} else {
			// Event already in timeline -- only update JSON if state repair modified it
			self.db.update_pdu_json(event_id, &json);
			// Explicitly rewrite the topological index entry and update its metadata
			self.db.reindex_topo(&pdu_id, event_id, new_topo_depth);
		}

		// Index searchable content
		self.index_pdu_search(shortroomid, &pdu_id, pdu);

		// Clean up the outlier table entry if it exists.
		self.services.outlier.remove_outlier(event_id).await;

		inserted = inserted.saturating_add(1);

		if inserted.is_multiple_of(2000) {
			info!("heal_room: inserted {inserted}/{count} events...");
		}
		if inserted.is_multiple_of(10_000) {
			drop(cork.take());
			tokio::task::yield_now().await;
			cork = Some(self.db.db.cork());
		}
	}

	drop(cork.take());

	// Update the room's authoritative shortstatehash to match the
	// recomputed state at the tip.
	if let Some(ssh) = current_shortstatehash {
		if ssh != 0 {
			self.services
				.state
				.set_room_state(room_id, ssh, &state_lock);
			info!("heal_room: updated room shortstatehash to {ssh}");
		}
	}

	// Phase 5: Calculate forward extremities using roaring bitmap
	let mut true_extremities = dag.extremities(&sorted);

	// Preserve outlier extremities not in our event set
	let current_exts: Vec<OwnedEventId> = self
		.services
		.state
		.get_forward_extremities(room_id)
		.collect()
		.await;
	for ext in current_exts {
		if !dag.id_to_idx.contains_key(&ext) {
			true_extremities.push(ext);
		}
	}

	if !true_extremities.is_empty() {
		self.services
			.state
			.set_forward_extremities(room_id, true_extremities.clone().into_iter(), &state_lock)
			.await;

		info!("heal_room: set forward extremities to {} true DAG tips", true_extremities.len());
	}

	// Phase 6: Repair unsigned payload values
	if let Err(e) = self.repair_room_unsigned(room_id).await {
		warn!("heal_room: failed to repair unsigned payload values: {e}");
	}

	// Phase 7: Rebuild membership cache if requested
	if options.rebuild_membership {
		self.services
			.state_cache
			.reconcile_membership(room_id)
			.await;
	}

	// Ensure WAL is durable
	let final_sync = self.db.db.cork_and_sync();
	drop(final_sync);

	drop(state_lock);

	info!("heal_room: complete — {inserted} inserted, {skipped} skipped, {failed} failed");

	Ok(HealResult {
		inserted,
		skipped,
		failed,
		extremities: true_extremities,
	})
}

/// Incrementally compute and store the state snapshot for a single event.
/// Extracted to keep the main loop readable.
#[conduwuit_core::implement(super::Service)]
pub(super) async fn compute_state_for_event(
	&self,
	pdu: &PduEvent,
	event_id: &OwnedEventId,
	json: &mut CanonicalJsonObject,
	ssh: &mut u64,
	_pdu_id: &RawPduId,
) {
	let shorteventid = self
		.services
		.short
		.get_or_create_shorteventid(&pdu.event_id)
		.await;
	self.services
		.state
		.set_pdu_shortstatehash(shorteventid, *ssh);

	let Some(state_key) = &pdu.state_key else {
		return;
	};

	// Repair unsigned.prev_content
	if *ssh != 0 {
		if let Ok(prev_state) = self
			.services
			.state_accessor
			.state_get(*ssh, &pdu.kind.to_string().into(), state_key)
			.await
		{
			if let Err(e) = update_unsigned_prev_content(json, &prev_state) {
				warn!(
					%event_id,
					"heal_room: failed to repair unsigned.prev_content: {e}"
				);
			}
		}
	}

	let shortstatekey = self
		.services
		.short
		.get_or_create_shortstatekey(&pdu.kind.to_string().into(), state_key)
		.await;

	let new_ssh = Box::pin(self.services.state_compressor.append_state_pdu(
		*ssh,
		shortstatekey,
		&pdu.event_id,
		|| self.services.globals.next_count(),
	))
	.await;

	if let Ok(Some(new_ssh)) = new_ssh {
		*ssh = new_ssh;
	}
}

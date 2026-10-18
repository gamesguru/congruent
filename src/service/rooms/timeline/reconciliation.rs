//! MSC4521 algebraic set-reconciliation snapshot of a room's timeline.
//!
//! The snapshot is rebuilt by scanning the room's timeline, then cached for a
//! short time. It is deliberately not maintained incrementally: the digest is
//! an XOR accumulator, so a duplicate insert silently removes an event, and the
//! sorted `h64` index would need re-sorting on every append. A TTL cache keeps
//! each served response self-consistent (digest, strata, frame and index all
//! describe the same point in time) at the cost of lagging the newest events.

use std::{
	collections::{BTreeMap, HashSet},
	sync::Arc,
};

use conduwuit_core::{
	Result, debug_warn, err,
	matrix::pdu::{PduCount, PduEvent},
};
use futures::StreamExt;
use rezzy_recon::{
	AlgebraicError, ElementHash, EventIdFormat, ResidentKernel, RoomEventIdKind, SortedPopulation,
};
use slipstream::{OwnedEventId, RoomId, RoomVersionId};

use super::Service;

/// Point-in-time reconciliation view of one room.
#[derive(Debug)]
pub struct RoomReconciliationState {
	/// Level-0 accumulator and strata over the room's event set.
	pub resident: ResidentKernel,
	/// Events sorted by `(h64, h128)`; serves bucket sketches.
	pub population: SortedPopulation,
	/// Every event behind a given `h64`. More than one entry means a short
	/// identifier collision.
	pub h64_to_events: BTreeMap<u64, Vec<(OwnedEventId, ElementHash)>>,
	/// Forward extremities when the scan started, sorted and deduplicated.
	pub frame_event_ids: Vec<OwnedEventId>,
	pub depth_range: [u64; 2],
	pub origin_server_ts_range: [u64; 2],
	/// The room's last timeline position when the scan started. A scan
	/// that saw the position move is repeated before it is cached.
	pub timeline_count: Option<PduCount>,
}

impl RoomReconciliationState {
	#[must_use]
	pub fn known_event_count(&self) -> u64 { self.resident.accumulator().known_event_count() }
}

/// Maps a room version onto what MSC4521 needs to know about its event IDs.
#[must_use]
pub(super) fn event_id_kind(room_version: &RoomVersionId) -> RoomEventIdKind {
	match room_version {
		| RoomVersionId::V1 | RoomVersionId::V2 => RoomEventIdKind::SenderChosen,
		| _ => RoomEventIdKind::ReferenceHash,
	}
}

/// Event-ID decoding format for a reference-hash room version.
#[must_use]
pub(super) fn event_id_format(room_version: &RoomVersionId) -> EventIdFormat {
	match room_version {
		| RoomVersionId::V3 => EventIdFormat::V3,
		| _ => EventIdFormat::V4Plus,
	}
}

/// Derives the MSC4521 element hash for an event ID.
pub(super) fn element_hash(
	event_id: &str,
	room_version: &RoomVersionId,
) -> Result<ElementHash, AlgebraicError> {
	ElementHash::from_matrix_event_id(event_id, event_id_format(room_version))
}

#[derive(Default)]
struct Builder {
	resident: ResidentKernel,
	elements: Vec<ElementHash>,
	h64_to_events: BTreeMap<u64, Vec<(OwnedEventId, ElementHash)>>,
	seen: HashSet<OwnedEventId>,
	depth_range: Option<[u64; 2]>,
	ts_range: Option<[u64; 2]>,
}

impl Builder {
	fn push(&mut self, pdu: &PduEvent, room_version: &RoomVersionId) {
		// The accumulator is XOR-based: a repeated event would cancel itself.
		if !self.seen.insert(pdu.event_id.clone()) {
			return;
		}

		let hash = match element_hash(pdu.event_id.as_str(), room_version) {
			| Ok(hash) => hash,
			| Err(e) => {
				debug_warn!(event_id = %pdu.event_id, "skipping event in reconciliation digest: {e:?}");
				return;
			},
		};
		if let Err(e) = self.resident.insert(hash) {
			debug_warn!(event_id = %pdu.event_id, "skipping event in reconciliation digest: {e:?}");
			return;
		}

		self.elements.push(hash);
		self.h64_to_events
			.entry(hash.h64)
			.or_default()
			.push((pdu.event_id.clone(), hash));

		let depth = pdu.depth;
		let ts = pdu.origin_server_ts;
		let [d_min, d_max] = self.depth_range.get_or_insert([depth, depth]);
		*d_min = (*d_min).min(depth);
		*d_max = (*d_max).max(depth);
		let [t_min, t_max] = self.ts_range.get_or_insert([ts, ts]);
		*t_min = (*t_min).min(ts);
		*t_max = (*t_max).max(ts);
	}

	fn finish(
		self,
		frame_event_ids: Vec<OwnedEventId>,
		timeline_count: Option<PduCount>,
	) -> RoomReconciliationState {
		RoomReconciliationState {
			resident: self.resident,
			population: SortedPopulation::new(self.elements),
			h64_to_events: self.h64_to_events,
			frame_event_ids,
			depth_range: self.depth_range.unwrap_or([0, 0]),
			origin_server_ts_range: self.ts_range.unwrap_or([0, 0]),
			timeline_count,
		}
	}
}

/// Rescans allowed when the room moves while a snapshot is being built.
const MAX_SCAN_ATTEMPTS: u8 = 3;

impl Service {
	/// Returns the room's reconciliation snapshot, rebuilding it when the
	/// cached one has expired. Backfills and live appends invalidate the cache
	/// after insertion, including when an append races snapshot publication.
	///
	/// # Errors
	/// Fails with `M_UNSUPPORTED_ROOM_VERSION` for room versions 1 and 2,
	/// whose event IDs are sender-chosen and cannot be hashed.
	pub async fn reconciliation_state(
		&self,
		room_id: &RoomId,
	) -> Result<Arc<RoomReconciliationState>> {
		if let Some(state) = self.reconciliation_cache.get(room_id) {
			return Ok(state);
		}

		// Singleflight the (expensive) scan per room.
		let _guard = self.mutex_reconciliation.lock(room_id).await;
		if let Some(state) = self.reconciliation_cache.get(room_id) {
			return Ok(state);
		}

		let room_version = self.services.state.get_room_version(room_id).await?;
		event_id_kind(&room_version)
			.require_event_ids_frame()
			.map_err(|_| {
				err!(Request(UnsupportedRoomVersion("Room version has sender-chosen event IDs.")))
			})?;

		// The scan is not atomic with the extremities: if they moved while it
		// ran, the digest and the frame describe different points, so scan again.
		let mut attempts = 0_u8;
		let state = loop {
			let timeline_count = self.last_timeline_count(room_id).await.ok();
			let frame_event_ids = self.forward_extremities_sorted(room_id).await;

			let mut builder = Builder::default();
			{
				let pdus = self.all_pdus(room_id);
				futures::pin_mut!(pdus);
				while let Some((_, pdu)) = pdus.next().await {
					builder.push(&pdu, &room_version);
				}
			}

			attempts = attempts.saturating_add(1);
			let after_frame = self.forward_extremities_sorted(room_id).await;
			let after_count = self.last_timeline_count(room_id).await.ok();
			let settled = after_frame == frame_event_ids && after_count == timeline_count;
			if settled || attempts >= MAX_SCAN_ATTEMPTS {
				// A room that keeps changing gets a best-effort snapshot: the
				// position read after the final scan, which is at least as new
				// as anything the scan could have seen. The short cache TTL
				// bounds how long it is served.
				break Arc::new(builder.finish(after_frame, after_count));
			}
		};
		self.reconciliation_cache
			.insert(room_id.to_owned(), Arc::clone(&state));
		// An append that finished after the scan's last check but before this
		// insert invalidated an empty slot. Re-check now that the snapshot is
		// published, so it is not served stale for the whole TTL.
		if self.last_timeline_count(room_id).await.ok() != state.timeline_count {
			self.reconciliation_cache.invalidate(room_id);
		}
		Ok(state)
	}

	async fn forward_extremities_sorted(&self, room_id: &RoomId) -> Vec<OwnedEventId> {
		let mut ids: Vec<OwnedEventId> = self
			.services
			.state
			.get_forward_extremities(room_id)
			.collect()
			.await;
		ids.sort();
		ids.dedup();
		ids
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// 43 characters of unpadded base64 decode to 32 bytes, and the last one must
	// carry zero trailing bits for the decoder to accept it, so it ends in `A`.
	const ID_A: &str = "$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
	const ID_B: &str = "$BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBA";

	#[test]
	fn event_id_kind_tracks_room_version() {
		assert_eq!(event_id_kind(&RoomVersionId::V1), RoomEventIdKind::SenderChosen);
		assert_eq!(event_id_kind(&RoomVersionId::V2), RoomEventIdKind::SenderChosen);
		assert_eq!(event_id_kind(&RoomVersionId::V3), RoomEventIdKind::ReferenceHash);
		assert_eq!(event_id_kind(&RoomVersionId::V11), RoomEventIdKind::ReferenceHash);
	}

	#[test]
	fn event_id_format_tracks_room_version() {
		assert_eq!(event_id_format(&RoomVersionId::V3), EventIdFormat::V3);
		assert_eq!(event_id_format(&RoomVersionId::V4), EventIdFormat::V4Plus);
		assert_eq!(event_id_format(&RoomVersionId::V11), EventIdFormat::V4Plus);
	}

	#[test]
	fn element_hash_is_deterministic_and_distinct() {
		let a = element_hash(ID_A, &RoomVersionId::V11).expect("hash a");
		let a2 = element_hash(ID_A, &RoomVersionId::V11).expect("hash a again");
		let b = element_hash(ID_B, &RoomVersionId::V11).expect("hash b");
		assert_eq!(a, a2);
		assert_ne!(a, b);
		assert_ne!(a.h64, 0);
	}

	#[test]
	fn element_hash_rejects_non_reference_hash_ids() {
		element_hash("$not-a-reference-hash:example.com", &RoomVersionId::V11).unwrap_err();
	}
}

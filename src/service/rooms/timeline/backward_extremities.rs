//! Tier 3 of the backfill scan perf work (see
//! `docs/development-gg/backfill-extremities-write-time-design.md`): persist
//! backward extremities at write time instead of rediscovering them by
//! scanning the timeline on every backward `/messages` call.
//!
//! This module holds the pure, DB-free pieces -- key packing/parsing and the
//! "which prev_events are missing" decision -- so they're directly unit
//! testable, the same way `data.rs`'s `boundary_tests` module tests the
//! `pdus`/`pdus_rev` boundary arithmetic in isolation from RocksDB. The
//! stateful half (reading/writing the two column families) lives in
//! `data.rs` next to `append_pdu_batch`/`prepend_backfill_pdu_batch`, the two
//! functions every insert path in the codebase ultimately calls.
//!
//! ## Schema
//!
//! Two column families, mirroring the existing `room_pducount_eventid` /
//! `roomid_topologicalorder_pducount` dual-index pattern (same facts, two
//! sort orders, because RocksDB has no joins to compute one from the other
//! on demand):
//!
//! - `roomid_depth_missingeventid`: `[shortroomid: 8B][depth: 8B][event_id]` ->
//!   `()`. Sorted by depth for the read-path range scan (not wired up yet --
//!   `backfill_if_required` still uses the old scan; see the design doc for why
//!   the migration has to land first).
//! - `roomid_missingeventid_depth`: `[shortroomid: 8B][event_id]` -> `depth:
//!   8B`. Keyed by event_id for an O(1) lookup when a previously-missing event
//!   finally arrives, so we know which `roomid_depth_missingeventid` entry to
//!   delete without a scan.

use ruma::{EventId, OwnedEventId};

/// Marker key in the `global` CF set once `populate_backward_extremities`
/// (`src/service/migrations.rs`) has finished backfilling this index for
/// pre-existing rooms. Shared between the migration (which sets it) and the
/// read path (which checks it) so there's one definition of "is the index
/// ready to trust yet" instead of two copies that could drift.
pub(crate) const MIGRATION_MARKER: &[u8] = b"populate_backward_extremities";

/// Packs the read-path key: `[shortroomid][depth][event_id]`.
pub(super) fn pack_depth_key(shortroomid: [u8; 8], depth: u64, event_id: &EventId) -> Vec<u8> {
	let event_id_bytes = event_id.as_bytes();
	let mut key = Vec::with_capacity(16_usize.saturating_add(event_id_bytes.len()));
	key.extend_from_slice(&shortroomid);
	key.extend_from_slice(&depth.to_be_bytes());
	key.extend_from_slice(event_id_bytes);
	key
}

/// Packs the delete-path key: `[shortroomid][event_id]`.
pub(super) fn pack_event_key(shortroomid: [u8; 8], event_id: &EventId) -> Vec<u8> {
	let event_id_bytes = event_id.as_bytes();
	let mut key = Vec::with_capacity(8_usize.saturating_add(event_id_bytes.len()));
	key.extend_from_slice(&shortroomid);
	key.extend_from_slice(event_id_bytes);
	key
}

/// Decodes a `roomid_missingeventid_depth` value back into a depth.
///
/// Returns `None` on a malformed value (wrong length) rather than panicking
/// -- this reads data another process wrote, and a length mismatch should
/// be treated as "we don't know", not crash the insert path.
pub(super) fn unpack_depth_value(value: &[u8]) -> Option<u64> {
	<[u8; 8]>::try_from(value).ok().map(u64::from_be_bytes)
}

/// Filters `prev_events` down to the ones not known locally, per
/// `known_locally`. Pure and DB-free so the "is this event a new backward
/// extremity" decision is testable without RocksDB -- this is the exact
/// shape of logic (a boundary/inclusion decision made once, at exactly one
/// call site, then trusted everywhere) that regressed twice in
/// `pdus`/`pdus_rev` before those got the same treatment.
pub(super) fn missing_prev_events(
	prev_events: &[OwnedEventId],
	mut known_locally: impl FnMut(&EventId) -> bool,
) -> Vec<&EventId> {
	prev_events
		.iter()
		.map(AsRef::as_ref)
		.filter(|id| !known_locally(id))
		.collect()
}

#[cfg(test)]
mod tests {
	use ruma::owned_event_id;

	use super::*;

	/// `OwnedEventId` has several ambiguous `AsRef` impls (`[u8]`, `str`,
	/// `EventId`); pinning the return type here resolves the coercion once
	/// instead of needing turbofish at every call site below.
	fn eid(id: &OwnedEventId) -> &EventId { id }

	#[test]
	fn depth_key_sorts_by_depth_within_a_room() {
		let room = [0_u8; 8];
		let id = owned_event_id!("$a:example.org");
		let a = pack_depth_key(room, 5, eid(&id));
		let b = pack_depth_key(room, 10, eid(&id));
		assert!(a < b, "lower depth must sort first for the range-scan read path");
	}

	#[test]
	fn depth_key_partitions_by_room_before_depth() {
		// A key from a numerically "later" room must never sort before an
		// earlier room's key regardless of depth, or a shortroomid-prefixed
		// range scan would leak across rooms.
		let room_a = 1_u64.to_be_bytes();
		let room_b = 2_u64.to_be_bytes();
		let id = owned_event_id!("$a:example.org");
		let high_depth_room_a = pack_depth_key(room_a, u64::MAX, eid(&id));
		let low_depth_room_b = pack_depth_key(room_b, 0, eid(&id));
		assert!(high_depth_room_a < low_depth_room_b);
	}

	#[test]
	fn event_key_round_trips_depth_through_the_value() {
		let depth = 12345_u64;
		assert_eq!(unpack_depth_value(&depth.to_be_bytes()), Some(depth));
	}

	#[test]
	fn unpack_depth_value_rejects_malformed_lengths() {
		assert_eq!(unpack_depth_value(&[1, 2, 3]), None);
		assert_eq!(unpack_depth_value(&[]), None);
		assert_eq!(unpack_depth_value(&[0; 9]), None);
	}

	#[test]
	fn missing_prev_events_filters_known_locally() {
		let known = owned_event_id!("$known:example.org");
		let missing = owned_event_id!("$missing:example.org");
		let prev_events = vec![known.clone(), missing.clone()];

		let result = missing_prev_events(&prev_events, |id| id == eid(&known));

		assert_eq!(result, vec![eid(&missing)]);
	}

	#[test]
	fn missing_prev_events_empty_when_all_known() {
		let a = owned_event_id!("$a:example.org");
		let b = owned_event_id!("$b:example.org");
		let prev_events = vec![a, b];

		let result = missing_prev_events(&prev_events, |_| true);

		assert!(result.is_empty());
	}

	#[test]
	fn missing_prev_events_all_missing_when_none_known() {
		let a = owned_event_id!("$a:example.org");
		let b = owned_event_id!("$b:example.org");
		let prev_events = vec![a.clone(), b.clone()];

		let result = missing_prev_events(&prev_events, |_| false);

		assert_eq!(result, vec![eid(&a), eid(&b)]);
	}
}

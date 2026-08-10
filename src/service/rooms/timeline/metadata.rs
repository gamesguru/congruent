use conduwuit::matrix::pdu::PduCount;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct EventMetadata {
	pub short_room_id: u64,
	pub is_outlier: bool,
	pub origin_server_ts: ruma::UInt,
	pub depth: ruma::UInt,
	pub soft_failed: bool,
	pub rejected: bool,
	pub redacted_by: Option<ruma::OwnedEventId>,
	pub short_state_hash: Option<u64>,
	#[serde(default)]
	pub deprecated_local_topo_depth: u64,
	/// Timeline position counter for `Normal` (live) events. `None` means
	/// either a legacy record (not yet migrated) or a `Backfilled` event --
	/// the exact negative counter is never stored here (see
	/// `matches_timeline_position`). `Some(0)` = outlier / not in timeline.
	/// Normal events start at 1.
	///
	/// Some pre-existing databases may carry a stale `Some(n)` for a
	/// `Backfilled` event, written by the `populate_pdu_count_in_metadata`
	/// migration before it was taught to skip backfilled counts: it took
	/// `unsigned_abs()` of the signed count, which collides `Backfilled(-n)`
	/// with `Normal(n)`. `matches_timeline_position` tolerates that legacy
	/// encoding.
	#[serde(default)]
	pub pdu_count: Option<u64>,
	/// Reason why this event was soft-failed (empty = no reason stored).
	#[serde(default)]
	pub soft_fail_reason: String,
	/// Reason why this event was rejected (empty = no reason stored).
	#[serde(default)]
	pub rejection_reason: String,
}

/// Pre-v19 schema: only 8 fields. Used as a fallback when bincode
/// deserialization of the current struct fails on old DB entries.
#[derive(Deserialize)]
struct EventMetadataV1 {
	short_room_id: u64,
	is_outlier: bool,
	origin_server_ts: ruma::UInt,
	depth: ruma::UInt,
	soft_failed: bool,
	rejected: bool,
	redacted_by: Option<ruma::OwnedEventId>,
	short_state_hash: Option<u64>,
}

impl EventMetadata {
	#[inline]
	#[must_use]
	pub fn matches_timeline_position(&self, depth: u64, pdu_count: PduCount) -> bool {
		!self.is_outlier
			&& self.deprecated_local_topo_depth == depth
			&& match pdu_count {
				| PduCount::Normal(_) => self.pdu_count == Some(pdu_count.into_unsigned()),
				// Every current write site (insert_pdu, replace_pdu, reindex.rs,
				// reorder.rs) stores `None` for a Backfilled event, so `None` is
				// the expected value here. But some pre-existing rows were
				// written by the older `populate_pdu_count_in_metadata`
				// migration, which took `unsigned_abs()` of the signed count
				// before it was taught to skip Backfilled events -- that
				// collides `Backfilled(-n)` with `Normal(n)` and left
				// `Some(n.unsigned_abs())` on disk. Accept that specific legacy
				// encoding too, so those rows aren't dropped from pagination
				// until they're naturally rewritten. This is an exact per-key
				// comparison (unlike the bare `is_none()` case), so it does not
				// re-introduce the coarse "any backfilled record matches"
				// acceptance for rows using this encoding.
				| PduCount::Backfilled(n) => match self.pdu_count {
					| None => true,
					| Some(legacy) => legacy == n.unsigned_abs(),
				},
			}
	}

	/// Deserialize from bincode bytes, falling back to the old 8-field
	/// schema if the current 12-field layout fails (e.g. pre-migration
	/// entries written before `local_topological_depth`, `pdu_count`,
	/// `soft_fail_reason`, and `rejection_reason` were added).
	pub fn from_bincode(bytes: &[u8]) -> Result<Self, bincode::Error> {
		bincode::deserialize::<Self>(bytes).or_else(|_| {
			let old = bincode::deserialize::<EventMetadataV1>(bytes)?;
			Ok(Self {
				short_room_id: old.short_room_id,
				is_outlier: old.is_outlier,
				origin_server_ts: old.origin_server_ts,
				depth: old.depth,
				soft_failed: old.soft_failed,
				rejected: old.rejected,
				redacted_by: old.redacted_by,
				short_state_hash: old.short_state_hash,
				..Default::default()
			})
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn base() -> EventMetadata {
		EventMetadata {
			deprecated_local_topo_depth: 5,
			is_outlier: false,
			..Default::default()
		}
	}

	#[test]
	fn normal_matches_exact_count_only() {
		let meta = EventMetadata { pdu_count: Some(3), ..base() };
		assert!(meta.matches_timeline_position(5, PduCount::Normal(3)));
		assert!(!meta.matches_timeline_position(5, PduCount::Normal(4)));
		assert!(!meta.matches_timeline_position(6, PduCount::Normal(3)));
	}

	#[test]
	fn backfilled_matches_none_pdu_count() {
		// Current write sites always store None for Backfilled events.
		let meta = EventMetadata { pdu_count: None, ..base() };
		assert!(meta.matches_timeline_position(5, PduCount::Backfilled(-3)));
		assert!(meta.matches_timeline_position(5, PduCount::Backfilled(-999)));
		assert!(!meta.matches_timeline_position(6, PduCount::Backfilled(-3)));
	}

	#[test]
	fn backfilled_matches_legacy_unsigned_abs_encoding() {
		// The pre-fix `populate_pdu_count_in_metadata` migration wrote
		// `Some(count.unsigned_abs())` for Backfilled rows. Those rows must
		// still be recognized until they are naturally rewritten.
		let meta = EventMetadata { pdu_count: Some(7), ..base() };
		assert!(meta.matches_timeline_position(5, PduCount::Backfilled(-7)));
	}

	#[test]
	fn backfilled_rejects_mismatched_legacy_count() {
		// A legacy Some(n) must not match a Backfilled key whose exact count
		// doesn't correspond -- otherwise this degenerates back into "any
		// backfilled record matches any backfilled key at this depth".
		let meta = EventMetadata { pdu_count: Some(7), ..base() };
		assert!(!meta.matches_timeline_position(5, PduCount::Backfilled(-8)));
	}

	#[test]
	fn outlier_never_matches() {
		let meta = EventMetadata {
			pdu_count: None,
			is_outlier: true,
			..base()
		};
		assert!(!meta.matches_timeline_position(5, PduCount::Backfilled(-3)));
	}
}

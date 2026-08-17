use conduwuit::matrix::pdu::PduCount;
use serde::{Deserialize, Serialize};

use crate::rooms::pdu_metadata::{EventStatus, RejectionCode, SoftFailCode};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct EventMetadata {
	pub short_room_id: u64,
	pub is_outlier: bool,
	pub origin_server_ts: ruma::UInt,
	pub depth: ruma::UInt,
	/// The single validation verdict for this event. Replaces the previous
	/// independent `soft_failed`/`rejected` booleans + reason strings.
	#[serde(default)]
	pub status: EventStatus,
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
}

/// Pre-`EventStatus` schema with the 12 fields (including the
/// `soft_failed`/`rejected` booleans and `soft_fail_reason`/`rejection_reason`
/// strings) that were in place just before the `EventStatus` enum landed. Used
/// as a bincode fallback so existing rows migrate transparently.
#[derive(Deserialize)]
struct EventMetadataV2 {
	short_room_id: u64,
	is_outlier: bool,
	origin_server_ts: ruma::UInt,
	depth: ruma::UInt,
	soft_failed: bool,
	rejected: bool,
	redacted_by: Option<ruma::OwnedEventId>,
	short_state_hash: Option<u64>,
	#[serde(default)]
	deprecated_local_topo_depth: u64,
	#[serde(default)]
	pdu_count: Option<u64>,
	#[serde(default)]
	soft_fail_reason: String,
	#[serde(default)]
	rejection_reason: String,
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
					// `None` can't distinguish *which* Backfilled counter this
					// metadata belongs to -- it matches every Backfilled key at
					// this depth, not just the one the topo entry actually
					// points at. That's intentional: this function has no DB
					// access to disambiguate further. Callers that resolve a
					// topo key to metadata for a Backfilled count (currently
					// only `parse_topo_stream`) MUST additionally confirm the
					// topo key's `PduId` matches the event's canonical position
					// (e.g. via `eventid_pduid`) before accepting a `None`
					// match, or a stale/orphaned topo entry left behind by a
					// reindex/reorder can be returned as if it were current.
					| None => true,
					| Some(legacy) => legacy == n.unsigned_abs(),
				},
			}
	}

	/// Deserialize from bincode bytes, falling back through the two previous
	/// on-disk layouts (the 12-field pre-`EventStatus` schema, then the old
	/// 8-field schema) when the current 9-field layout fails.
	pub fn from_bincode(bytes: &[u8]) -> Result<Self, bincode::Error> {
		if let Ok(meta) = bincode::deserialize::<Self>(bytes) {
			return Ok(meta);
		}
		if let Ok(v2) = bincode::deserialize::<EventMetadataV2>(bytes) {
			return Ok(Self::from_v2(v2));
		}
		let old = bincode::deserialize::<EventMetadataV1>(bytes)?;
		Ok(Self {
			short_room_id: old.short_room_id,
			is_outlier: old.is_outlier,
			origin_server_ts: old.origin_server_ts,
			depth: old.depth,
			status: status_from_bools(old.is_outlier, old.soft_failed, old.rejected, None, None),
			redacted_by: old.redacted_by,
			short_state_hash: old.short_state_hash,
			..Default::default()
		})
	}

	fn from_v2(v2: EventMetadataV2) -> Self {
		Self {
			short_room_id: v2.short_room_id,
			is_outlier: v2.is_outlier,
			origin_server_ts: v2.origin_server_ts,
			depth: v2.depth,
			status: status_from_bools(
				v2.is_outlier,
				v2.soft_failed,
				v2.rejected,
				(!v2.soft_fail_reason.is_empty()).then_some(v2.soft_fail_reason.as_str()),
				(!v2.rejection_reason.is_empty()).then_some(v2.rejection_reason.as_str()),
			),
			redacted_by: v2.redacted_by,
			short_state_hash: v2.short_state_hash,
			deprecated_local_topo_depth: v2.deprecated_local_topo_depth,
			pdu_count: v2.pdu_count,
		}
	}
}

/// Map the pre-`EventStatus` booleans + optional reason strings to a single
/// `EventStatus`. `rejected` takes precedence over `soft_failed`; a bare
/// outlier with no markers maps to `Pending` (its verdict is simply unknown
/// until re-validated), and a timeline event with no markers maps to
/// `Accepted`.
#[must_use]
pub fn status_from_bools(
	is_outlier: bool,
	soft_failed: bool,
	rejected: bool,
	soft_fail_reason: Option<&str>,
	rejection_reason: Option<&str>,
) -> EventStatus {
	if rejected {
		let code = rejection_reason
			.and_then(RejectionCode::parse)
			.unwrap_or(RejectionCode::Unknown);
		EventStatus::Rejected(code)
	} else if soft_failed {
		let code = soft_fail_reason
			.and_then(SoftFailCode::parse)
			.unwrap_or(SoftFailCode::Unknown);
		EventStatus::SoftFailed(code)
	} else if is_outlier {
		EventStatus::Pending
	} else {
		EventStatus::Accepted
	}
}

/// Build `status` for a freshly-written metadata row: preserve a prior
/// soft-fail verdict and honour the PDU's own `rejected` flag. Reasons are
/// deliberately not carried (the legacy write sites always reset them empty).
#[must_use]
pub fn status_from_prior(
	prior: Option<&EventMetadata>,
	is_outlier: bool,
	pdu_rejected: bool,
) -> EventStatus {
	if pdu_rejected {
		EventStatus::Rejected(RejectionCode::Unknown)
	} else if let Some(code) = prior.and_then(|m| match m.status {
		| EventStatus::SoftFailed(code) => Some(code),
		| _ => None,
	}) {
		EventStatus::SoftFailed(code)
	} else if is_outlier {
		EventStatus::Pending
	} else {
		EventStatus::Accepted
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

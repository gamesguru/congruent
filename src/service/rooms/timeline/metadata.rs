use conduwuit::matrix::pdu::PduCount;
use serde::{Deserialize, Serialize};

use crate::rooms::pdu_metadata::EventStatus;

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

impl EventMetadata {
	#[inline]
	#[must_use]
	pub fn matches_timeline_position(&self, depth: u64, pdu_count: PduCount) -> bool {
		!self.is_outlier
			&& self.deprecated_local_topo_depth == depth
			&& match pdu_count {
				| PduCount::Normal(_) => self.pdu_count == Some(pdu_count.into_unsigned()),
				| PduCount::Backfilled(n) => match self.pdu_count {
					| None => true,
					| Some(legacy) => legacy == n.unsigned_abs(),
				},
			}
	}

	pub fn from_bincode(bytes: &[u8]) -> Result<Self, bincode::Error> {
		bincode::deserialize(bytes)
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

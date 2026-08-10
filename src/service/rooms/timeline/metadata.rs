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
	/// Timeline position counter. `None` = legacy record (not yet migrated).
	/// `Some(0)` = outlier / not in timeline. Normal events start at 1.
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
				| PduCount::Backfilled(_) => self.pdu_count.is_none(),
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

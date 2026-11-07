//! Checkpoint-chain encoding for content-addressed state lattices.
//!
//! A state lattice is small enough to copy, but writing all 2 KiB on every
//! state event is wasted work: most roots differ from their parent by one
//! `(event_type, state_key, event_id)` entry. This module stores every
//! [`CHECKPOINT_INTERVAL`]-th root as a full lattice and the roots between
//! checkpoints as a linked list of one-transition deltas.
//!
//! A record is either a tagged full lattice or a tagged delta chain. Legacy
//! records are the raw 2 KiB lattice written before tagging; decoding accepts
//! both so an upgrade does not need a migration.
//!
//! The codec lives at the integration boundary; the interval itself comes from
//! mtxdb's `STATE_LATTICE_KEYFRAME_HOPS` so the live server, the importer, and
//! the measurement harness all agree on the same checkpoint cadence.

use std::sync::Arc;

use rezzy::incremental::LtHash;

/// Size of an encoded [`LtHash`] lattice (1024 little-endian `u16` lanes).
pub const LATTICE_BYTES: usize = 2048;

/// Number of chain hops between full-lattice checkpoints.
///
/// A checkpoint is written when a chain would otherwise reach this many hops,
/// so a reader replays at most `CHECKPOINT_INTERVAL - 1` deltas. mtxdb owns the
/// shared value so the storage layer and the server cannot drift apart.
pub const CHECKPOINT_INTERVAL: u32 = mtxdb::STATE_LATTICE_KEYFRAME_HOPS;

/// Tag byte for a full lattice record.
pub const RECORD_CHECKPOINT: u8 = 0x01;
/// Tag byte for a delta-chain record.
pub const RECORD_CHAIN: u8 = 0x02;

/// One add or subtract operation needed to move a lattice from its checkpoint
/// to the root it describes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LatticeDelta {
	/// Add one `(event_type, state_key, event_id)` entry.
	Insert {
		event_type: String,
		state_key: String,
		event_id: String,
	},
	/// Remove one `(event_type, state_key, event_id)` entry.
	Remove {
		event_type: String,
		state_key: String,
		event_id: String,
	},
}

impl LatticeDelta {
	/// Apply this operation to a lattice.
	pub fn apply(&self, lattice: &mut LtHash) {
		match self {
			| Self::Insert { event_type, state_key, event_id } =>
				lattice.insert(event_type, state_key, event_id),
			| Self::Remove { event_type, state_key, event_id } =>
				lattice.remove(event_type, state_key, event_id),
		}
	}

	fn encode_into(&self, out: &mut Vec<u8>) -> Option<()> {
		let (operation, event_type, state_key, event_id) = match self {
			| Self::Insert { event_type, state_key, event_id } =>
				(0_u8, event_type, state_key, event_id),
			| Self::Remove { event_type, state_key, event_id } =>
				(1_u8, event_type, state_key, event_id),
		};
		out.push(operation);
		for field in [event_type, state_key, event_id] {
			let length = u16::try_from(field.len()).ok()?;
			out.extend_from_slice(&length.to_le_bytes());
			out.extend_from_slice(field.as_bytes());
		}
		Some(())
	}

	fn decode_from(bytes: &[u8], at: &mut usize) -> Option<Self> {
		let operation = *bytes.get(*at)?;
		*at = at.checked_add(1)?;
		let mut fields = Vec::with_capacity(3);
		for _ in 0..3 {
			let length = usize::from(u16::from_le_bytes([
				*bytes.get(*at)?,
				*bytes.get(at.checked_add(1)?)?,
			]));
			*at = at.checked_add(2)?;
			let end = at.checked_add(length)?;
			let field = std::str::from_utf8(bytes.get(*at..end)?).ok()?.to_owned();
			*at = end;
			fields.push(field);
		}
		let event_type = fields.remove(0);
		let state_key = fields.remove(0);
		let event_id = fields.remove(0);
		match operation {
			| 0 => Some(Self::Insert { event_type, state_key, event_id }),
			| 1 => Some(Self::Remove { event_type, state_key, event_id }),
			| _ => None,
		}
	}
}

/// One link in a delta chain: the parent root it applies to and the operations
/// for that single transition.
///
/// Links are deliberately not cumulative. Writing the whole chain-to-checkpoint
/// delta list into every root would make total storage quadratic in the
/// interval. A link stores one transition and points at its parent, so a cold
/// read walks at most [`CHECKPOINT_INTERVAL`] small records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LatticeChain {
	/// Structural hash of the checkpoint the chain ultimately replays from.
	pub base: [u8; 32],
	/// Structural hash of the previous root this link applies to.
	pub parent: [u8; 32],
	/// Number of state transitions from the checkpoint, including this root.
	pub hops: u32,
	/// Operations for this transition only.
	pub deltas: Vec<LatticeDelta>,
}

impl LatticeChain {
	/// Encode the chain payload (without the record tag).
	///
	/// Returns `None` if a field is longer than `u16::MAX`, which cannot occur
	/// for valid Matrix state but keeps the encoder total.
	#[must_use]
	pub fn encode(&self) -> Option<Vec<u8>> {
		let mut out = Vec::new();
		out.extend_from_slice(&self.hops.to_le_bytes());
		out.extend_from_slice(&self.base);
		out.extend_from_slice(&self.parent);
		out.extend_from_slice(&u32::try_from(self.deltas.len()).ok()?.to_le_bytes());
		for delta in &self.deltas {
			delta.encode_into(&mut out)?;
		}
		Some(out)
	}

	/// Decode a chain payload produced by [`Self::encode`].
	#[must_use]
	pub fn decode(bytes: &[u8]) -> Option<Self> {
		let hops = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?);
		let base: [u8; 32] = bytes.get(4..36)?.try_into().ok()?;
		let parent: [u8; 32] = bytes.get(36..68)?.try_into().ok()?;
		let count =
			usize::try_from(u32::from_le_bytes(bytes.get(68..72)?.try_into().ok()?)).ok()?;
		// Each delta has at least a 1-byte operation and 6 length bytes, so a
		// count larger than the remaining bytes is malformed.
		if count > bytes.len().saturating_sub(72) {
			return None;
		}
		let mut at = 72_usize;
		let mut deltas = Vec::with_capacity(count);
		for _ in 0..count {
			deltas.push(LatticeDelta::decode_from(bytes, &mut at)?);
		}
		if at != bytes.len() {
			return None;
		}
		Some(Self { base, parent, hops, deltas })
	}

	/// Apply this link's operations to its parent lattice.
	#[must_use]
	pub fn apply(&self, parent: &LtHash) -> LtHash {
		let mut lattice = *parent;
		for delta in &self.deltas {
			delta.apply(&mut lattice);
		}
		lattice
	}
}

/// A decoded lattice record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LatticeRecord {
	/// A full 2 KiB lattice.
	Checkpoint(Vec<u8>),
	/// A delta chain against another root's checkpoint.
	Chain(LatticeChain),
}

/// Extend a chain with one transition, or return `None` when the caller should
/// compact to a full checkpoint.
///
/// `base` is the record of the root being extended and `base_hash` is that
/// root's structural hash. A checkpoint starts a new chain; a chain keeps its
/// original checkpoint as the base.
#[must_use]
pub fn extend_chain(
	base: Option<&LatticeRecord>,
	base_hash: [u8; 32],
	deltas: &[LatticeDelta],
) -> Option<LatticeChain> {
	if deltas.is_empty() {
		return None;
	}
	let (checkpoint, hops) = match base? {
		| LatticeRecord::Checkpoint(_) => (base_hash, 0_u32),
		| LatticeRecord::Chain(chain) => (chain.base, chain.hops),
	};
	let hops = hops.checked_add(1)?;
	if hops >= CHECKPOINT_INTERVAL {
		return None;
	}
	Some(LatticeChain {
		base: checkpoint,
		parent: base_hash,
		hops,
		deltas: deltas.to_vec(),
	})
}

/// Tag a full lattice for storage.
#[must_use]
pub fn encode_checkpoint(lattice: &[u8]) -> Option<Vec<u8>> {
	if lattice.len() != LATTICE_BYTES {
		return None;
	}
	let mut out = Vec::with_capacity(LATTICE_BYTES.saturating_add(1));
	out.push(RECORD_CHECKPOINT);
	out.extend_from_slice(lattice);
	Some(out)
}

/// Tag and encode a chain for storage.
#[must_use]
pub fn encode_chain(chain: &LatticeChain) -> Option<Vec<u8>> {
	let payload = chain.encode()?;
	let mut out = Vec::with_capacity(payload.len().saturating_add(1));
	out.push(RECORD_CHAIN);
	out.extend_from_slice(&payload);
	Some(out)
}

/// Decode a stored record, accepting both tagged records and the legacy raw
/// 2 KiB lattice.
#[must_use]
pub fn decode_record(bytes: &[u8]) -> Option<LatticeRecord> {
	match bytes.split_first() {
		| Some((&RECORD_CHECKPOINT, payload)) if payload.len() == LATTICE_BYTES =>
			Some(LatticeRecord::Checkpoint(payload.to_vec())),
		| Some((&RECORD_CHAIN, payload)) =>
			Some(LatticeRecord::Chain(LatticeChain::decode(payload)?)),
		| _ if bytes.len() == LATTICE_BYTES => Some(LatticeRecord::Checkpoint(bytes.to_vec())),
		| _ => None,
	}
}

/// A lattice and the exact bytes that persist it.
///
/// A checkpoint stores the lattice itself; a chain stores only the delta list.
/// Keeping both lets callers answer reads from memory while staging the small
/// record for the next publication transaction.
#[derive(Clone)]
pub struct CachedLattice {
	/// Full 2 KiB lattice.
	pub lattice: Arc<[u8]>,
	/// Tagged checkpoint or chain bytes to write to the lattice collection.
	pub record: Arc<[u8]>,
}

impl CachedLattice {
	/// Build a cache entry from a full lattice and its record.
	#[must_use]
	pub fn new(lattice: Arc<[u8]>, record: Arc<[u8]>) -> Self { Self { lattice, record } }

	/// Whether the record is a delta chain rather than a checkpoint.
	#[must_use]
	pub fn is_chain(&self) -> bool { self.record.first() == Some(&RECORD_CHAIN) }
}

#[cfg(test)]
mod tests {
	use super::*;

	fn sample_delta(event: &str) -> LatticeDelta {
		LatticeDelta::Insert {
			event_type: "m.room.member".to_owned(),
			state_key: "@alice:example.org".to_owned(),
			event_id: event.to_owned(),
		}
	}

	#[test]
	fn chain_round_trips() {
		let chain = LatticeChain {
			base: [7; 32],
			parent: [8; 32],
			hops: 42,
			deltas: vec![sample_delta("$one"), LatticeDelta::Remove {
				event_type: "m.room.member".to_owned(),
				state_key: "@bob:example.org".to_owned(),
				event_id: "$two".to_owned(),
			}],
		};
		let encoded = chain.encode().unwrap();
		assert_eq!(LatticeChain::decode(&encoded).unwrap(), chain);
	}

	#[test]
	fn record_accepts_legacy_and_tagged_checkpoints() {
		let lattice = vec![9_u8; LATTICE_BYTES];
		assert_eq!(decode_record(&lattice), Some(LatticeRecord::Checkpoint(lattice.clone())));
		let tagged = encode_checkpoint(&lattice).unwrap();
		assert_eq!(decode_record(&tagged), Some(LatticeRecord::Checkpoint(lattice)));
	}

	#[test]
	fn chain_replay_is_the_same_as_direct_accumulation() {
		let mut checkpoint = LtHash::default();
		checkpoint.insert("m.room.create", "", "$create");
		let mut expected = checkpoint;
		expected.insert("m.room.member", "@a:x", "$a");
		expected.replace("m.room.member", "@a:x", "$a", "$a2");

		let chain = LatticeChain {
			base: [0; 32],
			parent: [0; 32],
			hops: 1,
			deltas: vec![
				LatticeDelta::Insert {
					event_type: "m.room.member".to_owned(),
					state_key: "@a:x".to_owned(),
					event_id: "$a".to_owned(),
				},
				LatticeDelta::Remove {
					event_type: "m.room.member".to_owned(),
					state_key: "@a:x".to_owned(),
					event_id: "$a".to_owned(),
				},
				LatticeDelta::Insert {
					event_type: "m.room.member".to_owned(),
					state_key: "@a:x".to_owned(),
					event_id: "$a2".to_owned(),
				},
			],
		};
		assert_eq!(chain.apply(&checkpoint), expected);
	}

	#[test]
	fn chain_compacts_at_the_checkpoint_interval() {
		let checkpoint = LatticeRecord::Checkpoint(vec![0; LATTICE_BYTES]);
		let delta = sample_delta("$one");
		let mut chain = extend_chain(Some(&checkpoint), [3; 32], std::slice::from_ref(&delta))
			.expect("first hop starts a chain");
		assert_eq!(chain.hops, 1);
		assert_eq!(chain.base, [3; 32]);
		assert_eq!(chain.parent, [3; 32]);

		// Advance the chain to one hop before the interval.
		chain.hops = CHECKPOINT_INTERVAL - 1;
		assert!(extend_chain(Some(&LatticeRecord::Chain(chain)), [9; 32], &[delta]).is_none());
	}

	#[test]
	fn empty_delta_list_requests_a_checkpoint() {
		let checkpoint = LatticeRecord::Checkpoint(vec![0; LATTICE_BYTES]);
		assert!(extend_chain(Some(&checkpoint), [0; 32], &[]).is_none());
	}

	#[test]
	fn missing_base_requests_a_checkpoint() {
		assert!(extend_chain(None, [0; 32], &[sample_delta("$one")]).is_none());
	}

	#[test]
	fn truncated_chain_is_rejected() {
		let chain = LatticeChain {
			base: [1; 32],
			parent: [2; 32],
			hops: 1,
			deltas: vec![sample_delta("$one")],
		};
		let encoded = chain.encode().unwrap();
		assert!(LatticeChain::decode(&encoded[..encoded.len() - 1]).is_none());
	}
}

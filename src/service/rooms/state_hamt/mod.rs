use std::sync::Arc;

use conduwuit::{Result, err, matrix::StateKey};
use slipstream::{OwnedEventId, RoomId, RoomVersionId, events::StateEventType};

pub mod orphan_scan;
pub mod store;

pub struct Service {
	pub store: store::Store,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			store: store::Store::new(args.mtxdb.clone()),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// A state-HAMT leaf key: the `(event_type, state_key)` strings.
///
/// Leaves hold canonical bytes rather than this server's short IDs, so the
/// encoded nodes, and therefore their structural hashes, are identical on
/// every server holding the same state of the same room.
pub type StateHamtKey = (String, String);

/// A state-HAMT leaf value: the event ID string.
///
/// Storing the ID itself keeps leaves self-describing and works for every room
/// version, including v1/v2 opaque IDs. The dense codec length-prefixes it.
pub type StateHamtValue = String;

/// A node of a room's state HAMT.
pub type StateHamtNode = rezzy::hamt::HamtNode<StateHamtKey, StateHamtValue>;

/// The leaf key for `(event_type, state_key)`.
#[must_use]
pub fn hamt_key(event_type: &StateEventType, state_key: &str) -> StateHamtKey {
	(event_type.to_string(), state_key.to_owned())
}

#[allow(
	clippy::unused_self,
	reason = "the leaf is the event ID itself; the service holds no leaf state"
)]
impl Service {
	/// The leaf value for `event_id`.
	///
	/// `room_id` is accepted for call-site symmetry with the other
	/// room-scoped accessors and is otherwise unused.
	pub fn leaf_value(&self, _room_id: &RoomId, event_id: &str) -> Result<StateHamtValue> {
		Ok(event_id.to_owned())
	}

	/// The leaf value for `event_id` in a room of `room_version`.
	///
	/// The version is no longer needed now that the leaf stores the ID itself.
	pub fn leaf_value_in(
		&self,
		_room_version: &RoomVersionId,
		event_id: &str,
	) -> Result<StateHamtValue> {
		Ok(event_id.to_owned())
	}

	/// The event ID a leaf value stands for.
	pub fn leaf_event_id(
		&self,
		_room_id: &RoomId,
		value: &StateHamtValue,
	) -> Result<OwnedEventId> {
		OwnedEventId::try_from(value.as_str())
			.map_err(|e| err!(Database(error!("Invalid event ID in state HAMT leaf: {e}"))))
	}

	/// The event ID a leaf value stands for.
	pub fn leaf_event_id_in(
		&self,
		_room_version: &RoomVersionId,
		value: &StateHamtValue,
	) -> Result<OwnedEventId> {
		OwnedEventId::try_from(value.as_str())
			.map_err(|e| err!(Database(error!("Invalid event ID in state HAMT leaf: {e}"))))
	}

	/// Parses a leaf back into its typed `(event_type, state_key)` and event
	/// ID.
	pub fn parse_entry_in(
		&self,
		room_version: &RoomVersionId,
		key: &StateHamtKey,
		value: &StateHamtValue,
	) -> Result<((StateEventType, StateKey), OwnedEventId)> {
		let event_id = self.leaf_event_id_in(room_version, value)?;
		Ok(((key.0.as_str().into(), key.1.as_str().into()), event_id))
	}
}

/// The routing hash of a leaf key.
///
/// It hashes the key's persisted encoding, not its in-memory `Hash` stream,
/// which Rust does not promise to keep stable, so every server routes the
/// same key the same way. Every build, mutation and lookup goes through it.
#[must_use]
pub fn route(structural_key: &[u8], key: &StateHamtKey) -> rezzy::hamt::StructuralHash {
	rezzy::hamt::canonical_key_path_hash(structural_key, key)
}

/// Builds a room's state tree from `entries`, with its root handle.
pub fn build_root<I>(
	structural_key: &[u8],
	lattice: &rezzy::incremental::LtHash,
	entries: I,
) -> std::result::Result<(rezzy::hamt::RootHandle, Arc<StateHamtNode>), rezzy::hamt::HamtBuildError>
where
	I: IntoIterator<Item = (StateHamtKey, StateHamtValue)>,
{
	let root = rezzy::hamt::build_hamt_with_key_hash(structural_key, entries, |key| {
		route(structural_key, key)
	})?;
	Ok((rezzy::hamt::RootHandle::from_lthash(root.structural_hash, lattice), root))
}

/// Inserts, replaces or (with `None`) removes one leaf.
pub fn persist_mutation<F, E>(
	prev_root: &Arc<StateHamtNode>,
	structural_key: &[u8],
	key: StateHamtKey,
	value: Option<StateHamtValue>,
	resolver: &mut F,
) -> rezzy::hamt::PersistMutationResult<StateHamtKey, StateHamtValue, E>
where
	F: rezzy::hamt::NodeResolver<StateHamtKey, StateHamtValue, E>,
{
	rezzy::hamt::persist_mutation_with_key_hash(
		prev_root,
		structural_key,
		key,
		value,
		|key| route(structural_key, key),
		resolver,
	)
}

/// Applies a batch of leaf mutations, publishing one final root.
pub fn persist_mutations<I, F, E>(
	prev_root: &Arc<StateHamtNode>,
	structural_key: &[u8],
	mutations: I,
	resolver: &mut F,
) -> rezzy::hamt::PersistMutationsResult<StateHamtKey, StateHamtValue, E>
where
	I: IntoIterator<Item = (StateHamtKey, Option<StateHamtValue>)>,
	F: rezzy::hamt::NodeResolver<StateHamtKey, StateHamtValue, E>,
{
	rezzy::hamt::HamtMutator::new(|key: &StateHamtKey| route(structural_key, key), resolver)
		.persist_mutations(prev_root, structural_key, mutations)
}

/// Looks up the leaf value for `key`.
pub fn search<F, E>(
	root: &StateHamtNode,
	structural_key: &[u8],
	key: &StateHamtKey,
	resolver: &mut F,
) -> std::result::Result<Option<StateHamtValue>, E>
where
	F: rezzy::hamt::NodeResolver<StateHamtKey, StateHamtValue, E>,
{
	root.search_with_key_hash(key, |key| route(structural_key, key), resolver)
}

/// Computes the structural key for a room's state HAMT.
///
/// It depends only on the room ID, so every server derives the same key and
/// builds identical nodes for identical state. The key is public: it separates
/// rooms' hash domains but does not stop a sender from choosing state keys
/// whose routing hashes share a prefix. That costs about 2^(5d) hashes per key
/// to share `d` levels, and only deepens the paths it targets; a full routing
/// collision (256 bits) is infeasible.
#[must_use]
pub fn room_structural_key(room_id: &RoomId) -> [u8; 32] {
	use sha2::{Digest, Sha256};

	let mut hasher = Sha256::new();
	hasher.update(b"congruent state hamt structural key\0");
	hasher.update(room_id.as_bytes());
	hasher.finalize().into()
}

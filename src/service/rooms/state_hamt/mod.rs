use std::sync::Arc;

use conduwuit::{Result, err, matrix::StateKey};
use conduwuit_database::Map;
use slipstream::{OwnedEventId, RoomId, RoomVersionId, events::StateEventType};

use crate::{Dep, rooms};

pub mod orphan_scan;
pub mod store;

pub struct Service {
	pub store: store::Store,
	services: Services,
	/// Leaf value to event ID, for room versions 1 and 2. Their leaf values
	/// hash the opaque event ID and cannot be decoded back into it.
	eventhash_eventid: Arc<Map>,
}

struct Services {
	short: Dep<rooms::short::Service>,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			store: store::Store::new(args.mtxdb.clone()),
			services: Services {
				short: args.depend::<rooms::short::Service>("rooms::short"),
			},
			eventhash_eventid: args.db["eventhash_eventid"].clone(),
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

/// A state-HAMT leaf value: the 32-byte hash that identifies the event.
///
/// See [`rezzy::hamt::state_hamt_event_id`]: the reference hash a room-v3+
/// event ID encodes, or a hash of the opaque ID in room versions 1 and 2.
pub type StateHamtValue = rezzy::hamt::StateHamtEventId;

/// A node of a room's state HAMT.
pub type StateHamtNode = rezzy::hamt::HamtNode<StateHamtKey, StateHamtValue>;

/// The leaf key for `(event_type, state_key)`.
#[must_use]
pub fn hamt_key(event_type: &StateEventType, state_key: &str) -> StateHamtKey {
	(event_type.to_string(), state_key.to_owned())
}

impl Service {
	/// The leaf value for `event_id` in `room_id`.
	pub fn leaf_value(&self, room_id: &RoomId, event_id: &str) -> Result<StateHamtValue> {
		let room_version = self.services.short.get_room_version_blocking(room_id)?;
		self.leaf_value_in(&room_version, event_id)
	}

	/// The leaf value for `event_id` in a room of `room_version`.
	///
	/// This is the only way leaf values are made, so every one of them can be
	/// read back: for room versions 1 and 2 it records the hash's event ID.
	pub fn leaf_value_in(
		&self,
		room_version: &RoomVersionId,
		event_id: &str,
	) -> Result<StateHamtValue> {
		let value =
			rezzy::hamt::state_hamt_event_id(event_id, room_version.as_str()).map_err(|e| {
				err!(Request(InvalidParam("{event_id} in a {room_version} room: {e}")))
			})?;
		if rezzy::has_opaque_event_ids(room_version.as_str()) {
			self.eventhash_eventid.insert(&value, event_id);
		}
		Ok(value)
	}

	/// The event ID a leaf value in `room_id` stands for.
	pub fn leaf_event_id(
		&self,
		room_id: &RoomId,
		value: &StateHamtValue,
	) -> Result<OwnedEventId> {
		let room_version = self.services.short.get_room_version_blocking(room_id)?;
		self.leaf_event_id_in(&room_version, value)
	}

	/// The event ID a leaf value in a room of `room_version` stands for.
	pub fn leaf_event_id_in(
		&self,
		room_version: &RoomVersionId,
		value: &StateHamtValue,
	) -> Result<OwnedEventId> {
		let event_id = match rezzy::hamt::state_hamt_event_id_string(value, room_version.as_str())
		{
			| Some(event_id) => event_id,
			| None => {
				let handle = self.eventhash_eventid.get_blocking(value).map_err(|_| {
					err!(Database(error!("No event ID recorded for a state HAMT leaf value")))
				})?;
				String::from_utf8(handle.to_vec()).map_err(|e| {
					err!(Database(error!("Invalid event ID recorded for a leaf value: {e}")))
				})?
			},
		};
		OwnedEventId::try_from(event_id)
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

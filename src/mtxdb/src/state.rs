//! Domain adapter for Matrix room-state roots and encoded HAMT nodes.

use std::sync::Arc;

use mtxdb::ShardType;

use crate::{
	StorageError,
	keys::{
		event_root_node_id, hamt_node_id, room_root_node_id, room_roots_collection_id,
		room_state_collection_id,
	},
	store::{CollectionId, MtxdbStore, MtxdbTransaction, NodeId},
};

/// Physical state adapter. HAMT encoding and traversal remain above this
/// boundary; this type stores opaque root and node bytes.
pub struct StateAdapter {
	store: Arc<MtxdbStore>,
	shard: ShardType,
}

impl StateAdapter {
	#[must_use]
	pub fn new(store: Arc<MtxdbStore>) -> Self { Self { store, shard: ShardType::State } }

	fn stat_collection(&self, room_id: &[u8]) -> CollectionId {
		room_state_collection_id(room_id)
	}

	fn roots_collection(&self, room_id: &[u8]) -> CollectionId {
		room_roots_collection_id(room_id)
	}

	/// Read a room's current state root.
	pub fn root_for_room(&self, room_id: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
		self.store
			.get(self.shard, &self.roots_collection(room_id), room_root_node_id())
	}

	/// Read an event's associated state root from its room's collection.
	pub fn root_for_event(
		&self,
		room_id: &[u8],
		event_id: &[u8],
	) -> Result<Option<Vec<u8>>, StorageError> {
		self.store
			.get(self.shard, &self.roots_collection(room_id), event_root_node_id(event_id))
	}

	/// Read HAMT nodes by their full 32-byte structural hashes.
	///
	/// Internally truncates to 16-byte node IDs. Callers should verify the
	/// returned bytes against the expected full hash using
	/// `decode_v1_verified` with the room's structural key.
	pub fn get_nodes(
		&self,
		room_id: &[u8],
		full_hashes: &[[u8; 32]],
	) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
		let node_ids: Vec<NodeId> = full_hashes.iter().map(|h| hamt_node_id(h)).collect();
		self.store
			.get_many(self.shard, &self.stat_collection(room_id), &node_ids)
	}

	/// Shadow-write a room's current root pointer in its roots collection.
	pub fn put_room_root(&self, room_id: &[u8], root_bytes: &[u8]) -> Result<(), StorageError> {
		let tx = self.store.begin_transaction();
		tx.put(self.shard, self.roots_collection(room_id), room_root_node_id(), root_bytes)?;
		tx.commit()
	}

	/// Shadow-write an event's associated state root in its room's roots collection.
	pub fn put_event_root(
		&self,
		room_id: &[u8],
		event_id: &[u8],
		root_bytes: &[u8],
	) -> Result<(), StorageError> {
		let tx = self.store.begin_transaction();
		tx.put(
			self.shard,
			self.roots_collection(room_id),
			event_root_node_id(event_id),
			root_bytes,
		)?;
		tx.commit()
	}

	/// Shadow-write one content-addressed HAMT node in its room's STAT collection.
	pub fn put_node(
		&self,
		room_id: &[u8],
		full_hash: &[u8; 32],
		bytes: &[u8],
	) -> Result<(), StorageError> {
		let tx = self.store.begin_transaction();
		tx.put(self.shard, self.stat_collection(room_id), hamt_node_id(full_hash), bytes)?;
		tx.commit()
	}

	/// Atomically publish new HAMT nodes and their roots in a single transaction.
	///
	/// Writes new nodes to STAT, and both the event-root and room-root pointers
	/// to the roots collection, ensuring readers never see a root before its nodes.
	pub fn publish_state_update(
		&self,
		tx: &MtxdbTransaction<'_>,
		room_id: &[u8],
		event_id: &[u8],
		new_nodes: &[([u8; 32], &[u8])],
		new_root_bytes: &[u8],
		is_state_event: bool,
	) -> Result<(), StorageError> {
		let stat_coll = self.stat_collection(room_id);
		let roots_coll = self.roots_collection(room_id);
		for (full_hash, data) in new_nodes {
			tx.put(self.shard, stat_coll, hamt_node_id(full_hash), data)?;
		}
		tx.put(self.shard, roots_coll, event_root_node_id(event_id), new_root_bytes)?;
		if is_state_event {
			tx.put(self.shard, roots_coll, room_root_node_id(), new_root_bytes)?;
		}
		Ok(())
	}
}

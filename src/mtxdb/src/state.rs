//! Domain adapter for Matrix room-state roots and encoded HAMT nodes.

use std::sync::Arc;

use mtxdb::ShardType;

use crate::{
	StorageError,
	keys::derive_id,
	store::{CollectionId, MtxdbStore, MtxdbTransaction, NodeId},
};

/// Physical state adapter. HAMT encoding and traversal remain above this
/// boundary; this type stores opaque root and node bytes.
pub struct StateAdapter {
	store: Arc<MtxdbStore>,
	shard: ShardType,
	nodes_collection: CollectionId,
	room_root_collection: CollectionId,
	event_root_collection: CollectionId,
}

impl StateAdapter {
	#[must_use]
	pub fn new(store: Arc<MtxdbStore>) -> Self {
		Self {
			store,
			shard: ShardType::State,
			nodes_collection: derive_id(b"sys", b"hamt-nodes"),
			room_root_collection: derive_id(b"sys", b"room-current-root"),
			event_root_collection: derive_id(b"sys", b"event-to-root"),
		}
	}

	pub fn root_for_room(&self, room_id: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
		self.store
			.get(self.shard, &self.room_root_collection, derive_id(b"room", room_id))
	}

	pub fn root_for_event(&self, event_id: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
		self.store
			.get(self.shard, &self.event_root_collection, derive_id(b"event", event_id))
	}

	pub fn get_nodes(&self, node_ids: &[NodeId]) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
		self.store
			.get_many(self.shard, &self.nodes_collection, node_ids)
	}

	pub fn publish_state_update(
		&self,
		tx: &MtxdbTransaction<'_>,
		room_id: &[u8],
		event_id: &[u8],
		new_nodes: &[(NodeId, &[u8])],
		new_root_bytes: &[u8],
	) -> Result<(), StorageError> {
		for &(node_id, data) in new_nodes {
			tx.put(self.shard, self.nodes_collection, node_id, data)?;
		}
		tx.put(
			self.shard,
			self.event_root_collection,
			derive_id(b"event", event_id),
			new_root_bytes,
		)?;
		tx.put(
			self.shard,
			self.room_root_collection,
			derive_id(b"room", room_id),
			new_root_bytes,
		)
	}
}

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

	fn stat_collection(room_id: &[u8]) -> CollectionId { room_state_collection_id(room_id) }

	fn roots_collection(room_id: &[u8]) -> CollectionId { room_roots_collection_id(room_id) }

	/// Read a room's current state root.
	pub fn root_for_room(&self, room_id: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
		self.store
			.get(self.shard, &Self::roots_collection(room_id), room_root_node_id())
	}

	/// Read an event's associated state root from its room's collection.
	pub fn root_for_event(
		&self,
		room_id: &[u8],
		event_id: &[u8],
	) -> Result<Option<Vec<u8>>, StorageError> {
		self.store
			.get(self.shard, &Self::roots_collection(room_id), event_root_node_id(event_id))
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
		let node_ids: Vec<NodeId> = full_hashes.iter().map(hamt_node_id).collect();
		self.store
			.get_many(self.shard, &Self::stat_collection(room_id), &node_ids)
	}

	/// Shadow-write a room's current root pointer in its roots collection.
	pub fn put_room_root(&self, room_id: &[u8], root_bytes: &[u8]) -> Result<(), StorageError> {
		let tx = self.store.begin_transaction();
		tx.put(self.shard, Self::roots_collection(room_id), room_root_node_id(), root_bytes)?;
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
			Self::roots_collection(room_id),
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
		tx.put(self.shard, Self::stat_collection(room_id), hamt_node_id(full_hash), bytes)?;
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
		let stat_coll = Self::stat_collection(room_id);
		let roots_coll = Self::roots_collection(room_id);
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

#[cfg(test)]
mod tests {
	use std::{fs, path::PathBuf};

	use super::*;

	fn test_path(name: &str) -> PathBuf {
		std::env::temp_dir().join(format!("conduwuit-mtxdb-state-{name}-{}", std::process::id()))
	}

	fn hash(seed: u8) -> [u8; 32] { [seed; 32] }

	#[test]
	fn publish_is_atomic_and_advances_both_roots() {
		let path = test_path("publish");
		let _ = fs::remove_dir_all(&path);
		let room = b"!room:example.com";
		let event = b"$event:example.com";

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = StateAdapter::new(store.clone());
		let nodes = [(hash(1), &b"node-one"[..]), (hash(2), &b"node-two"[..])];

		let tx = store.begin_transaction();
		adapter
			.publish_state_update(&tx, room, event, &nodes, b"root-bytes", true)
			.unwrap();

		// Nothing is visible before commit: no root without its nodes.
		assert!(adapter.root_for_room(room).unwrap().is_none());
		assert!(adapter.root_for_event(room, event).unwrap().is_none());
		assert!(
			adapter
				.get_nodes(room, &[hash(1), hash(2)])
				.unwrap()
				.iter()
				.all(Option::is_none)
		);

		tx.commit().unwrap();

		assert_eq!(adapter.root_for_room(room).unwrap().as_deref(), Some(&b"root-bytes"[..]));
		assert_eq!(
			adapter.root_for_event(room, event).unwrap().as_deref(),
			Some(&b"root-bytes"[..])
		);
		let got = adapter
			.get_nodes(room, &[hash(1), hash(3), hash(2)])
			.unwrap();
		assert_eq!(got[0].as_deref(), Some(&b"node-one"[..]));
		assert_eq!(got[1], None);
		assert_eq!(got[2].as_deref(), Some(&b"node-two"[..]));

		drop(adapter);
		drop(store);
		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn non_state_event_does_not_advance_room_root() {
		let path = test_path("non-state");
		let _ = fs::remove_dir_all(&path);
		let room = b"!room:example.com";

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = StateAdapter::new(store.clone());

		let tx = store.begin_transaction();
		adapter
			.publish_state_update(&tx, room, b"$state:example.com", &[], b"current", true)
			.unwrap();
		tx.commit().unwrap();

		let tx = store.begin_transaction();
		adapter
			.publish_state_update(&tx, room, b"$message:example.com", &[], b"historical", false)
			.unwrap();
		tx.commit().unwrap();

		assert_eq!(adapter.root_for_room(room).unwrap().as_deref(), Some(&b"current"[..]));
		assert_eq!(
			adapter
				.root_for_event(room, b"$message:example.com")
				.unwrap()
				.as_deref(),
			Some(&b"historical"[..])
		);

		drop(adapter);
		drop(store);
		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn rooms_are_isolated() {
		let path = test_path("isolation");
		let _ = fs::remove_dir_all(&path);
		let room_a = b"!a:example.com";
		let room_b = b"!b:example.com";

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = StateAdapter::new(store.clone());

		adapter.put_node(room_a, &hash(7), b"a-node").unwrap();
		adapter.put_room_root(room_a, b"a-root").unwrap();
		adapter
			.put_event_root(room_a, b"$e:example.com", b"a-event-root")
			.unwrap();

		assert!(adapter.get_nodes(room_b, &[hash(7)]).unwrap()[0].is_none());
		assert!(adapter.root_for_room(room_b).unwrap().is_none());
		assert!(
			adapter
				.root_for_event(room_b, b"$e:example.com")
				.unwrap()
				.is_none()
		);

		assert_eq!(
			adapter.get_nodes(room_a, &[hash(7)]).unwrap()[0].as_deref(),
			Some(&b"a-node"[..])
		);

		drop(adapter);
		drop(store);
		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn state_survives_restart() {
		let path = test_path("restart");
		let _ = fs::remove_dir_all(&path);
		let room = b"!room:example.com";
		let event = b"$event:example.com";

		let () = {
			let store = Arc::new(MtxdbStore::open(&path).unwrap());
			let adapter = StateAdapter::new(store.clone());
			let tx = store.begin_transaction();
			adapter
				.publish_state_update(&tx, room, event, &[(hash(9), &b"node"[..])], b"root", true)
				.unwrap();
			tx.commit().unwrap();
			store.sync(ShardType::State).unwrap();
		};

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = StateAdapter::new(store);
		assert_eq!(adapter.root_for_room(room).unwrap().as_deref(), Some(&b"root"[..]));
		assert_eq!(adapter.root_for_event(room, event).unwrap().as_deref(), Some(&b"root"[..]));
		assert_eq!(
			adapter.get_nodes(room, &[hash(9)]).unwrap()[0].as_deref(),
			Some(&b"node"[..])
		);

		drop(adapter);
		let _ = fs::remove_dir_all(path);
	}
}

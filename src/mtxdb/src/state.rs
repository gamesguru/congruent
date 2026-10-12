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

/// A room's roots and the roots collection's version at the time they were read.
pub struct RoomRoots {
	pub version: u64,
	pub roots: Vec<Vec<u8>>,
}

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

	/// A room's complete root set: its current room root and every event root,
	/// which all live in the room's one roots collection.
	///
	/// The collection version is read *before* the scan and returned with it.
	/// Hand it to [`Self::expect_roots_unchanged`] on the sweep's transaction: a
	/// root published after this read advances the version and makes the sweep
	/// fail instead of removing a node the new root may reference. A publisher
	/// that skips writing a node it believes already exists cannot defeat it,
	/// because the check is on the roots, not on the nodes.
	pub fn room_roots(&self, room_id: &[u8]) -> Result<RoomRoots, StorageError> {
		let collection = Self::roots_collection(room_id);
		let version = self.store.collection_version(self.shard, &collection)?;
		let roots = self
			.store
			.scan(self.shard, &collection)?
			.into_iter()
			.map(|(_, bytes)| bytes)
			.collect();
		Ok(RoomRoots { version, roots })
	}

	/// The truncated ids of every HAMT node stored for a room.
	pub fn room_node_ids(&self, room_id: &[u8]) -> Result<Vec<NodeId>, StorageError> {
		Ok(self
			.store
			.scan(self.shard, &Self::stat_collection(room_id))?
			.into_iter()
			.map(|(node, _)| node)
			.collect())
	}

	/// Stage removal of HAMT nodes from a room's collection in `tx`.
	///
	/// Mark reachability by the truncated id ([`hamt_node_id`]): a prefix
	/// collision can then only cause a node to be kept, never removed.
	pub fn remove_nodes(
		&self,
		tx: &MtxdbTransaction<'_>,
		room_id: &[u8],
		node_ids: &[NodeId],
	) -> Result<(), StorageError> {
		tx.remove_many(self.shard, Self::stat_collection(room_id), node_ids)
	}

	/// Make `tx` fail if the room's roots changed since `version` was read by
	/// [`Self::room_roots`].
	pub fn expect_roots_unchanged(
		&self,
		tx: &MtxdbTransaction<'_>,
		room_id: &[u8],
		version: u64,
	) -> Result<(), StorageError> {
		tx.expect_collection_version(self.shard, Self::roots_collection(room_id), version)
	}

	/// Publish nodes and roots in one committed transaction.
	///
	/// Convenience over [`Self::publish_state_update`] for callers that own no
	/// transaction of their own.
	pub fn publish(
		&self,
		room_id: &[u8],
		event_id: &[u8],
		new_nodes: &[([u8; 32], Vec<u8>)],
		new_root_bytes: &[u8],
		is_state_event: bool,
	) -> Result<(), StorageError> {
		let refs: Vec<([u8; 32], &[u8])> = new_nodes
			.iter()
			.map(|(hash, bytes)| (*hash, bytes.as_slice()))
			.collect();
		let tx = self.store.begin_transaction();
		self.publish_state_update(&tx, room_id, event_id, &refs, new_root_bytes, is_state_event)?;
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
	#[test]
	fn publish_convenience_commits_nodes_and_roots_together() {
		let path = test_path("publish-convenience");
		let _ = fs::remove_dir_all(&path);
		let room = b"!room:example.com";
		let event = b"$event:example.com";

		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = StateAdapter::new(store);
		let nodes = vec![(hash(1), b"node-one".to_vec()), (hash(2), b"node-two".to_vec())];
		adapter.publish(room, event, &nodes, b"root", true).unwrap();

		assert_eq!(adapter.root_for_room(room).unwrap().as_deref(), Some(&b"root"[..]));
		assert_eq!(adapter.root_for_event(room, event).unwrap().as_deref(), Some(&b"root"[..]));
		let got = adapter.get_nodes(room, &[hash(1), hash(2)]).unwrap();
		assert_eq!(got[0].as_deref(), Some(&b"node-one"[..]));
		assert_eq!(got[1].as_deref(), Some(&b"node-two"[..]));

		drop(adapter);
		let _ = fs::remove_dir_all(path);
	}
	/// A root is the concatenation of the 32-byte hashes of the nodes it
	/// references; enough structure to run a reachability walk in a test.
	fn root_of(hashes: &[[u8; 32]]) -> Vec<u8> { hashes.concat() }

	fn reachable(roots: &RoomRoots) -> std::collections::HashSet<NodeId> {
		roots
			.roots
			.iter()
			.flat_map(|root| root.chunks(32))
			.map(|chunk| hamt_node_id(&<[u8; 32]>::try_from(chunk).unwrap()))
			.collect()
	}

	#[test]
	fn room_roots_returns_the_room_root_and_every_event_root() {
		let path = test_path("room-roots");
		let _ = fs::remove_dir_all(&path);
		let room = b"!room:example.com";
		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = StateAdapter::new(store.clone());

		adapter
			.publish(room, b"$state:example.com", &[(hash(1), b"n1".to_vec())], b"r1", true)
			.unwrap();
		adapter
			.publish(room, b"$msg:example.com", &[], b"r2", false)
			.unwrap();

		let mut roots = adapter.room_roots(room).unwrap().roots;
		roots.sort();
		// The state event writes its event root and the room root; the message
		// writes only its event root.
		assert_eq!(roots, vec![b"r1".to_vec(), b"r1".to_vec(), b"r2".to_vec()]);
		assert!(adapter.room_roots(b"!empty:example.com").unwrap().roots.is_empty());

		drop(adapter);
		drop(store);
		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn a_sweep_removes_only_the_unreachable_nodes_of_its_room() {
		let path = test_path("sweep");
		let _ = fs::remove_dir_all(&path);
		let room = b"!room:example.com";
		let other = b"!other:example.com";
		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = StateAdapter::new(store.clone());

		let nodes = [(hash(1), b"n1".to_vec()), (hash(2), b"n2".to_vec())];
		adapter
			.publish(room, b"$a:example.com", &nodes, &root_of(&[hash(1), hash(2)]), true)
			.unwrap();
		// hash(9) is stored but no root references it, in this room and another.
		adapter.put_node(room, &hash(9), b"orphan").unwrap();
		adapter.put_node(other, &hash(9), b"other-room-copy").unwrap();

		let roots = adapter.room_roots(room).unwrap();
		let live = reachable(&roots);
		let orphans: Vec<NodeId> = adapter
			.room_node_ids(room)
			.unwrap()
			.into_iter()
			.filter(|node| !live.contains(node))
			.collect();
		assert_eq!(orphans, vec![hamt_node_id(&hash(9))]);

		let tx = store.begin_transaction();
		adapter.expect_roots_unchanged(&tx, room, roots.version).unwrap();
		adapter.remove_nodes(&tx, room, &orphans).unwrap();
		tx.commit().unwrap();

		let got = adapter
			.get_nodes(room, &[hash(1), hash(2), hash(9)])
			.unwrap();
		assert!(got[0].is_some() && got[1].is_some(), "reachable nodes stay");
		assert!(got[2].is_none(), "the orphan is gone");
		assert!(
			adapter.get_nodes(other, &[hash(9)]).unwrap()[0].is_some(),
			"another room's copy of the same hash is untouched"
		);

		drop(adapter);
		drop(store);
		let _ = fs::remove_dir_all(path);
	}

	#[test]
	fn a_root_published_after_the_scan_aborts_the_sweep() {
		let path = test_path("sweep-race");
		let _ = fs::remove_dir_all(&path);
		let room = b"!room:example.com";
		let store = Arc::new(MtxdbStore::open(&path).unwrap());
		let adapter = StateAdapter::new(store.clone());

		adapter
			.publish(room, b"$a:example.com", &[(hash(1), b"n1".to_vec())], &root_of(&[hash(1)]), true)
			.unwrap();
		adapter.put_node(room, &hash(9), b"soon-referenced").unwrap();

		let roots = adapter.room_roots(room).unwrap();
		let live = reachable(&roots);
		let orphans: Vec<NodeId> = adapter
			.room_node_ids(room)
			.unwrap()
			.into_iter()
			.filter(|node| !live.contains(node))
			.collect();
		assert_eq!(orphans, vec![hamt_node_id(&hash(9))]);

		let sweep = store.begin_transaction();
		adapter.expect_roots_unchanged(&sweep, room, roots.version).unwrap();
		adapter.remove_nodes(&sweep, room, &orphans).unwrap();

		// A publisher now references the node without writing it again, because
		// it already exists. Only the roots collection changes.
		adapter
			.publish(room, b"$b:example.com", &[], &root_of(&[hash(1), hash(9)]), true)
			.unwrap();

		assert!(
			matches!(sweep.commit(), Err(StorageError::StaleRead { .. })),
			"the sweep must lose to a root published after its scan"
		);
		assert!(
			adapter.get_nodes(room, &[hash(9)]).unwrap()[0].is_some(),
			"the node the new root references must survive"
		);

		drop(adapter);
		drop(store);
		let _ = fs::remove_dir_all(path);
	}
}

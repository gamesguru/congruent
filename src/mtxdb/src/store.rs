//! Generic, Matrix-independent access to the mtxdb shard pools.

use std::sync::Arc;

use mtxdb::{Database, DatabaseTransaction, ShardType};

use crate::StorageError;

pub type CollectionId = [u8; 16];
pub type NodeId = [u8; 16];

/// Physical mtxdb boundary shared by event, state, edge, and metadata
/// adapters. Matrix codecs and key derivation do not belong here.
pub struct MtxdbStore {
	db: Arc<Database>,
}

impl MtxdbStore {
	#[must_use]
	pub fn new(db: Arc<Database>) -> Self { Self { db } }

	pub fn open<P: Into<std::path::PathBuf>>(path: P) -> Result<Self, StorageError> {
		Ok(Self::new(Arc::new(Database::open(path.into())?)))
	}

	#[must_use]
	pub fn database(&self) -> &Arc<Database> { &self.db }

	pub fn get(
		&self,
		shard: ShardType,
		collection: &CollectionId,
		node: NodeId,
	) -> Result<Option<Vec<u8>>, StorageError> {
		self.get_many(shard, collection, std::slice::from_ref(&node))
			.map(|mut values| values.pop().flatten())
	}

	pub fn get_many(
		&self,
		shard: ShardType,
		collection: &CollectionId,
		nodes: &[NodeId],
	) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
		self.db
			.pool(shard)
			.get_read_committed(collection, nodes)
			.map(|values| {
				values
					.into_iter()
					.map(|value| value.map(|node| node.bytes.to_vec()))
					.collect()
			})
	}

	#[must_use]
	pub fn begin_transaction(&self) -> MtxdbTransaction<'_> {
		MtxdbTransaction { tx: self.db.begin_transaction() }
	}

	pub fn sync(&self, shard: ShardType) -> Result<(), StorageError> {
		self.db.pool(shard).sync_all()
	}
}

pub struct MtxdbTransaction<'a> {
	tx: DatabaseTransaction<'a>,
}

#[cfg(test)]
mod tests {
	use std::{fs, path::PathBuf};

	use super::*;
	use crate::keys::derive_id;

	fn test_path(name: &str) -> PathBuf {
		std::env::temp_dir().join(format!("conduwuit-mtxdb-{name}-{}", std::process::id()))
	}

	#[test]
	fn cross_shard_atomicity_and_sync() {
		let directory = test_path("cross-shard");
		let _ = fs::remove_dir_all(&directory);
		let collection = derive_id(b"sys", b"test-cross-shard");
		let event_node = derive_id(b"event", b"$event:example.com");
		let state_node = derive_id(b"state", b"root");

		{
			let store = MtxdbStore::open(&directory).expect("open mtxdb");
			let tx = store.begin_transaction();
			tx.put(ShardType::EventDag, collection, event_node, b"event")
				.expect("stage event");
			tx.put(ShardType::State, collection, state_node, b"state")
				.expect("stage state");

			assert!(
				store
					.get(ShardType::EventDag, &collection, event_node)
					.expect("read event")
					.is_none()
			);
			assert!(
				store
					.get(ShardType::State, &collection, state_node)
					.expect("read state")
					.is_none()
			);

			tx.commit().expect("commit transaction");
			assert_eq!(
				store
					.get(ShardType::EventDag, &collection, event_node)
					.unwrap()
					.as_deref(),
				Some(&b"event"[..])
			);
			assert_eq!(
				store
					.get(ShardType::State, &collection, state_node)
					.unwrap()
					.as_deref(),
				Some(&b"state"[..])
			);

			store.sync(ShardType::EventDag).expect("sync event pool");
			store.sync(ShardType::State).expect("sync state pool");
		}

		let reopened = MtxdbStore::open(&directory).expect("reopen mtxdb");
		assert_eq!(
			reopened
				.get(ShardType::EventDag, &collection, event_node)
				.unwrap()
				.as_deref(),
			Some(&b"event"[..])
		);
		assert_eq!(
			reopened
				.get(ShardType::State, &collection, state_node)
				.unwrap()
				.as_deref(),
			Some(&b"state"[..])
		);
		let _ = fs::remove_dir_all(directory);
	}
}

impl MtxdbTransaction<'_> {
	pub fn put(
		&self,
		shard: ShardType,
		collection: CollectionId,
		node: NodeId,
		data: &[u8],
	) -> Result<(), StorageError> {
		self.tx
			.put(shard, collection, node, &mtxdb::storage::NodeData::from_slice(data))?;
		Ok(())
	}

	pub fn commit(self) -> Result<(), StorageError> { self.tx.commit() }
}

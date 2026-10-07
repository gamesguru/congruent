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
	pub fn new(db: Arc<Database>) -> Self {
		Self { db }
	}

	pub fn open(path: impl Into<std::path::PathBuf>) -> Result<Self, StorageError> {
		Ok(Self::new(Arc::new(Database::open(path.into())?)))
	}

	#[must_use]
	pub fn database(&self) -> &Arc<Database> {
		&self.db
	}

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

	pub fn commit(self) -> Result<(), StorageError> {
		self.tx.commit()
	}
}

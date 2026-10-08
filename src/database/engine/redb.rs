//! Redb physical backend for the metadata compatibility API.
//!
//! Maps are represented as prefixes in one ordered table. This avoids leaking
//! redb's `TableDefinition` lifetime requirement into the logical `Map` API,
//! while retaining ordered scans and atomic multi-map commits.

use std::{path::Path, sync::Arc};

use conduwuit::{Result, err};
use redb::{Database, ReadableTable, TableDefinition};

use crate::map::batch::DbOp;

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("conduwuit_metadata");

pub(crate) struct RedbEngine {
	db: Arc<Database>,
}

impl RedbEngine {
	pub(crate) fn open(path: &Path) -> Result<Self> {
		let db = Database::create(path)
			.map_err(|error| err!(Database("failed to open redb metadata store: {error}")))?;
		Ok(Self { db: Arc::new(db) })
	}

	pub(crate) fn commit(&self, operations: Vec<DbOp>) -> Result<()> {
		let transaction = self
			.db
			.begin_write()
			.map_err(|error| err!(Database("failed to begin redb write: {error}")))?;
		{
			let mut table = transaction
				.open_table(TABLE)
				.map_err(|error| err!(Database("failed to open redb metadata table: {error}")))?;
			for operation in operations {
				match operation {
					| DbOp::Insert { map_name, key, value } => {
						let composite = composite_key(map_name, &key);
						table
							.insert(composite.as_slice(), value.as_slice())
							.map_err(|error| {
								err!(Database("failed to insert redb metadata: {error}"))
							})?;
					},
					| DbOp::Remove { map_name, key } => {
						let composite = composite_key(map_name, &key);
						table.remove(composite.as_slice()).map_err(|error| {
							err!(Database("failed to remove redb metadata: {error}"))
						})?;
					},
				}
			}
		}
		transaction
			.commit()
			.map_err(|error| err!(Database("failed to commit redb metadata: {error}")))
	}

	pub(crate) fn get(&self, map_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
		let transaction = self
			.db
			.begin_read()
			.map_err(|error| err!(Database("failed to begin redb read: {error}")))?;
		let table = transaction
			.open_table(TABLE)
			.map_err(|error| err!(Database("failed to open redb metadata table: {error}")))?;
		let composite = composite_key(map_name, key);
		table
			.get(composite.as_slice())
			.map_err(|error| err!(Database("failed to read redb metadata: {error}")))
			.map(|value| value.map(|value| value.value().to_vec()))
	}

	pub(crate) fn contains(&self, map_name: &str, key: &[u8]) -> Result<bool> {
		Ok(self.get(map_name, key)?.is_some())
	}
}

fn composite_key(map_name: &str, key: &[u8]) -> Vec<u8> {
	let map = map_name.as_bytes();
	let mut composite = Vec::with_capacity(4 + map.len() + key.len());
	composite.extend_from_slice(&(map.len() as u32).to_be_bytes());
	composite.extend_from_slice(map);
	composite.extend_from_slice(key);
	composite
}

//! Redb physical backend for the metadata compatibility API.
//!
//! Maps are represented as prefixes in one ordered table. This avoids leaking
//! redb's `TableDefinition` lifetime requirement into the logical `Map` API,
//! while retaining ordered scans and atomic multi-map commits.

use std::{
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicU32, Ordering},
	},
};

use conduwuit::{Result, err};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::{
	map::batch::DbOp,
	util::{Direction, IteratorMode},
};

const TABLE: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new("conduwuit_metadata");

pub(crate) struct RedbEngine {
	db: Arc<Database>,
	corks: AtomicU32,
	lifts: AtomicU32,
}

impl RedbEngine {
	pub(crate) fn open(path: &Path) -> Result<Self> {
		let path = metadata_path(path);
		if let Some(parent) = path.parent() {
			std::fs::create_dir_all(parent).map_err(|error| {
				err!(Database("failed to create redb metadata directory: {error}"))
			})?;
		}
		let db = Database::create(path)
			.map_err(|error| err!(Database("failed to open redb metadata store: {error}")))?;
		Ok(Self {
			db: Arc::new(db),
			corks: AtomicU32::new(0),
			lifts: AtomicU32::new(0),
		})
	}

	pub(crate) fn commit_batch(&self, operations: Vec<DbOp>) -> Result<()> {
		self.commit(operations)
	}

	pub(crate) fn flush(&self) -> Result<()> { Ok(()) }

	pub(crate) fn sync(&self) -> Result<()> { Ok(()) }

	pub(crate) fn sort(&self) -> Result<()> { Ok(()) }

	pub(crate) fn update(&self) -> Result<()> { Ok(()) }

	pub(crate) fn wait_compactions_blocking(&self) -> Result<()> { Ok(()) }

	pub(crate) fn cork(&self) { self.corks.fetch_add(1, Ordering::Relaxed); }

	pub(crate) fn uncork(&self) { self.corks.fetch_sub(1, Ordering::Relaxed); }

	pub(crate) fn lift(&self) { self.lifts.fetch_add(1, Ordering::Relaxed); }

	pub(crate) fn unlift(&self) { self.lifts.fetch_sub(1, Ordering::Relaxed); }

	pub(crate) fn has_corks(&self) -> bool { self.corks.load(Ordering::Relaxed) > 0 }

	pub(crate) fn corked(&self) -> bool {
		self.corks.load(Ordering::Relaxed) > self.lifts.load(Ordering::Relaxed)
	}

	pub(crate) fn cf_exists(&self, _name: &str) -> bool { true }

	pub(crate) fn drop_cf(&self, _name: &str) -> Result<()> { Ok(()) }

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

	pub(crate) fn entries(&self, map_name: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		let transaction = self
			.db
			.begin_read()
			.map_err(|error| err!(Database("failed to begin redb read: {error}")))?;
		let table = transaction
			.open_table(TABLE)
			.map_err(|error| err!(Database("failed to open redb metadata table: {error}")))?;
		let prefix = composite_key(map_name, &[]);
		let mut entries = Vec::new();
		for item in table
			.iter()
			.map_err(|error| err!(Database("failed to iterate redb metadata: {error}")))?
		{
			let (key, value) =
				item.map_err(|error| err!(Database("failed to read redb metadata: {error}")))?;
			let key = key.value();
			if !key.starts_with(&prefix) {
				continue;
			}
			entries.push((key[prefix.len()..].to_vec(), value.value().to_vec()));
		}
		Ok(entries)
	}

	pub(crate) fn scan(
		&self,
		map_name: &str,
		mode: IteratorMode<'_>,
	) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		let mut entries = self.entries(map_name)?;
		match mode {
			| IteratorMode::Start => {},
			| IteratorMode::End => entries.reverse(),
			| IteratorMode::From(key, direction) => {
				entries.retain(|(candidate, _)| match direction {
					| Direction::Forward => candidate.as_slice() >= key,
					| Direction::Reverse => candidate.as_slice() <= key,
				});
				if matches!(direction, Direction::Reverse) {
					entries.reverse();
				}
			},
		}
		Ok(entries)
	}
}

fn metadata_path(path: &Path) -> PathBuf {
	if path.extension().is_some() {
		path.to_owned()
	} else {
		path.join("metadata.redb")
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

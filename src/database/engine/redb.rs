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

pub struct RedbEngine {
	db: Arc<Database>,
	corks: AtomicU32,
	lifts: AtomicU32,
}

#[derive(Clone, Debug)]
pub struct FileInfo {
	pub name: String,
	pub level: i32,
	pub num_entries: u64,
	pub num_deletions: u64,
	pub size: u64,
	pub column_family_name: String,
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

	pub(crate) fn flush(&self) -> Result<()> {
		let _ = Arc::strong_count(&self.db);
		Ok(())
	}

	pub(crate) fn sync(&self) -> Result<()> {
		let _ = Arc::strong_count(&self.db);
		Ok(())
	}

	pub fn sort(&self) -> Result<()> {
		let _ = Arc::strong_count(&self.db);
		Ok(())
	}

	pub(crate) fn cork(&self) { self.corks.fetch_add(1, Ordering::Relaxed); }

	pub(crate) fn uncork(&self) { self.corks.fetch_sub(1, Ordering::Relaxed); }

	pub(crate) fn lift(&self) { self.lifts.fetch_add(1, Ordering::Relaxed); }

	pub(crate) fn unlift(&self) { self.lifts.fetch_sub(1, Ordering::Relaxed); }

	pub(crate) fn has_corks(&self) -> bool { self.corks.load(Ordering::Relaxed) > 0 }

	pub fn cf_exists(&self, _name: &str) -> bool {
		let _ = Arc::strong_count(&self.db);
		true
	}

	pub fn drop_cf(&self, _name: &str) -> Result<()> {
		let _ = Arc::strong_count(&self.db);
		Ok(())
	}

	pub fn file_list(&self) -> impl Iterator<Item = Result<FileInfo>> {
		let _ = Arc::strong_count(&self.db);
		Vec::<Result<FileInfo>>::new().into_iter()
	}

	pub fn memory_usage(&self) -> Result<String> {
		let _ = Arc::strong_count(&self.db);
		Ok(String::from("redb metadata backend; allocator statistics unavailable"))
	}

	pub fn backup_list(&self) -> Result<std::vec::IntoIter<String>> {
		let _ = Arc::strong_count(&self.db);
		Ok(Vec::new().into_iter())
	}

	pub fn backup(&self) -> Result<()> {
		let _ = Arc::strong_count(&self.db);
		Err(err!(Database("online backups are not supported by the redb backend")))
	}

	pub fn backup_count(&self) -> Result<usize> {
		let _ = Arc::strong_count(&self.db);
		Ok(0)
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
	let map_len = u32::try_from(map.len()).expect("metadata map name exceeds u32 length");
	let capacity = map
		.len()
		.checked_add(key.len())
		.and_then(|length| length.checked_add(4))
		.expect("metadata composite key length overflow");
	let mut composite = Vec::with_capacity(capacity);
	composite.extend_from_slice(&map_len.to_be_bytes());
	composite.extend_from_slice(map);
	composite.extend_from_slice(key);
	composite
}

//! Redb physical backend for the metadata compatibility API.
//!
//! Maps are represented as prefixes in one ordered table. This avoids leaking
//! redb's `TableDefinition` lifetime requirement into the logical `Map` API,
//! while retaining ordered scans and atomic multi-map commits.

use std::{
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicU32, AtomicU64, Ordering},
	},
	time::Instant,
};

use conduwuit::{Result, err};
use redb::{Database, ReadableDatabase, TableDefinition};

use crate::{
	map::batch::DbOp,
	util::{Direction, IteratorMode},
};

/// Log cumulative write stats every this many commits.
const STATS_INTERVAL: u64 = 250;

const TABLE: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new("conduwuit_metadata");

pub struct RedbEngine {
	db: Arc<Database>,
	corks: AtomicU32,
	lifts: AtomicU32,
	stats: CommitStats,
}

/// Write-path counters, logged when the engine is dropped. Totals over the
/// engine's lifetime; commit time is summed across threads, not wall-clock.
#[derive(Default)]
struct CommitStats {
	commits: AtomicU64,
	operations: AtomicU64,
	corked_commits: AtomicU64,
	commit_nanos: AtomicU64,
	max_commit_nanos: AtomicU64,
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
		let transaction = db
			.begin_write()
			.map_err(|error| err!(Database("failed to initialize redb metadata: {error}")))?;
		transaction
			.open_table(TABLE)
			.map_err(|error| err!(Database("failed to create redb metadata table: {error}")))?;
		transaction.commit().map_err(|error| {
			err!(Database("failed to commit redb metadata initialization: {error}"))
		})?;
		Ok(Self {
			db: Arc::new(db),
			corks: AtomicU32::new(0),
			lifts: AtomicU32::new(0),
			stats: CommitStats::default(),
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
		let started = Instant::now();
		let count = u64::try_from(operations.len()).unwrap_or(u64::MAX);
		let corked = self.has_corks();
		let result = self.commit_inner(operations);
		let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
		let stats = &self.stats;
		let commits = stats
			.commits
			.fetch_add(1, Ordering::Relaxed)
			.saturating_add(1);
		stats.operations.fetch_add(count, Ordering::Relaxed);
		stats.commit_nanos.fetch_add(nanos, Ordering::Relaxed);
		stats.max_commit_nanos.fetch_max(nanos, Ordering::Relaxed);
		if corked {
			stats.corked_commits.fetch_add(1, Ordering::Relaxed);
		}

		// Periodic report so short-lived processes (killed mid-shutdown) still
		// leave measurements behind.
		if commits.is_multiple_of(STATS_INTERVAL) {
			self.log_stats();
		}

		result
	}

	fn commit_inner(&self, operations: Vec<DbOp>) -> Result<()> {
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

	/// Collects `[lower, upper)` (unbounded above when `upper` is `None`) in key
	/// order, stripping `strip` leading bytes (the map prefix) from each key.
	/// Seeks by range instead of walking the shared table.
	fn range(
		&self,
		lower: &[u8],
		upper: Option<&[u8]>,
		strip: usize,
	) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		let transaction = self
			.db
			.begin_read()
			.map_err(|error| err!(Database("failed to begin redb read: {error}")))?;
		let table = transaction
			.open_table(TABLE)
			.map_err(|error| err!(Database("failed to open redb metadata table: {error}")))?;
		let iter = match upper {
			| Some(upper) => table.range::<&[u8]>(lower..upper),
			| None => table.range::<&[u8]>(lower..),
		}
		.map_err(|error| err!(Database("failed to iterate redb metadata: {error}")))?;

		let mut entries = Vec::new();
		for item in iter {
			let (key, value) =
				item.map_err(|error| err!(Database("failed to read redb metadata: {error}")))?;
			entries.push((key.value()[strip..].to_vec(), value.value().to_vec()));
		}
		Ok(entries)
	}

	pub(crate) fn scan(
		&self,
		map_name: &str,
		mode: IteratorMode<'_>,
	) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		let prefix = composite_key(map_name, &[]);
		let end = prefix_end(&prefix);
		match mode {
			| IteratorMode::Start => self.range(&prefix, end.as_deref(), prefix.len()),
			| IteratorMode::End => {
				let mut entries = self.range(&prefix, end.as_deref(), prefix.len())?;
				entries.reverse();
				Ok(entries)
			},
			| IteratorMode::From(key, Direction::Forward) => {
				let lower = composite_key(map_name, key);
				self.range(&lower, end.as_deref(), prefix.len())
			},
			| IteratorMode::From(key, Direction::Reverse) => {
				// Keys <= `key`: the smallest key above it is `key` + 0x00.
				let mut upper = composite_key(map_name, key);
				upper.push(0);
				let mut entries = self.range(&prefix, Some(&upper), prefix.len())?;
				entries.reverse();
				Ok(entries)
			},
		}
	}
}

/// Smallest byte string greater than every string starting with `prefix`.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
	let mut end = prefix.to_vec();
	while let Some(last) = end.pop() {
		if let Some(next) = last.checked_add(1) {
			end.push(next);
			return Some(end);
		}
	}

	None
}

impl RedbEngine {
	/// Logs the write-path totals accumulated since the engine opened. Safe to
	/// call repeatedly; counters are cumulative and never reset.
	pub(crate) fn log_stats(&self) {
		let stats = &self.stats;
		let commits = stats.commits.load(Ordering::Relaxed);
		let operations = stats.operations.load(Ordering::Relaxed);
		let corked = stats.corked_commits.load(Ordering::Relaxed);
		let total_ms = stats.commit_nanos.load(Ordering::Relaxed) / 1_000_000;
		let max_ms = stats.max_commit_nanos.load(Ordering::Relaxed) / 1_000_000;
		conduwuit::info!(
			"redb write stats (lifetime totals): commits={commits} operations={operations} \
			 corked_commits={corked} commit_ms_total={total_ms} commit_ms_max={max_ms}"
		);
	}
}

impl Drop for RedbEngine {
	fn drop(&mut self) { self.log_stats(); }
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn prefix_end_increments_with_carry() {
		assert_eq!(prefix_end(&[1, 2, 3]), Some(vec![1, 2, 4]));
		assert_eq!(prefix_end(&[1, 255]), Some(vec![2]));
		assert_eq!(prefix_end(&[255, 255]), None);
	}

	#[test]
	fn scan_is_scoped_to_one_map_and_seeks() {
		let dir = std::env::temp_dir().join(format!("redb-scan-{}", std::process::id()));
		let engine = RedbEngine::open(&dir).expect("open");
		let put = |map: &'static str, key: &[u8]| DbOp::Insert {
			map_name: map,
			key: key.to_vec(),
			value: key.to_vec(),
		};
		engine
			.commit_batch(vec![put("a", b"1"), put("a", b"2"), put("a", b"3"), put("b", b"2")])
			.expect("commit");

		let keys = |entries: Vec<(Vec<u8>, Vec<u8>)>| {
			entries
				.into_iter()
				.map(|(k, _)| String::from_utf8(k).expect("utf8"))
				.collect::<Vec<_>>()
		};
		assert_eq!(keys(engine.scan("a", IteratorMode::Start).unwrap()), ["1", "2", "3"]);
		assert_eq!(keys(engine.scan("a", IteratorMode::End).unwrap()), ["3", "2", "1"]);
		assert_eq!(
			keys(
				engine
					.scan("a", IteratorMode::From(b"2", Direction::Forward))
					.unwrap()
			),
			["2", "3"]
		);
		assert_eq!(
			keys(
				engine
					.scan("a", IteratorMode::From(b"2", Direction::Reverse))
					.unwrap()
			),
			["2", "1"]
		);
		let _ = std::fs::remove_dir_all(dir);
	}
}

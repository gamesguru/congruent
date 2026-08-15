use std::sync::Arc;

use conduwuit::{Result, SyncRwLock, err, utils};
use database::{Database, Deserialized, Map};

pub struct Data {
	global: Arc<Map>,
	counter: SyncRwLock<u64>,
	pub(super) db: Arc<Database>,
}

const COUNTER: &[u8] = b"c";

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let db = &args.db;
		Self {
			global: db["global"].clone(),
			counter: SyncRwLock::new(Self::stored_count(&db["global"]).unwrap_or_default()),
			db: args.db.clone(),
		}
	}

	// TODO: migrate SyncRwLock<u64> to AtomicU64 with Block Reservation Strategy
	// (reserve N IDs in RocksDB, issue from RAM via atomic fetch-add, gaps on
	// crash are acceptable since Matrix only requires monotonic stream IDs)
	pub fn next_count(&self) -> Result<u64> {
		self.next_count_batch(1)
			.map(|start| start.saturating_add(1))
	}

	pub fn next_count_batch(&self, diff: u64) -> Result<u64> {
		let _cork = self.db.cork();
		let mut lock = self.counter.write();
		let counter: &mut u64 = &mut lock;

		#[cfg(debug_assertions)]
		debug_assert!(
			*counter == Self::stored_count(&self.global).unwrap_or_default(),
			"counter mismatch"
		);

		let start = *counter;
		*counter = counter
			.checked_add(diff)
			.ok_or_else(|| err!(Arithmetic("Counter overflow")))?;

		self.global.insert(COUNTER, counter.to_be_bytes());

		Ok(start)
	}

	#[inline]
	pub fn current_count(&self) -> u64 {
		let lock = self.counter.read();
		let counter: &u64 = &lock;
		debug_assert!(
			*counter == Self::stored_count(&self.global).unwrap_or_default(),
			"counter mismatch"
		);

		*counter
	}

	fn stored_count(global: &Arc<Map>) -> Result<u64> {
		global
			.get_blocking(COUNTER)
			.as_deref()
			.map_or(Ok(0_u64), utils::u64_from_bytes)
	}

	pub async fn database_version(&self) -> u64 {
		self.global
			.get(b"version")
			.await
			.deserialized()
			.unwrap_or(0)
	}

	#[inline]
	pub fn bump_database_version(&self, new_version: u64) {
		self.global.raw_put(b"version", new_version);
	}

	pub async fn schema_fingerprint(&self) -> Option<[u8; 32]> {
		self.global
			.get(b"schema_fingerprint")
			.await
			.ok()
			.and_then(|bytes| bytes.as_ref().try_into().ok())
	}

	pub fn set_schema_fingerprint(&self, hash: &[u8; 32]) {
		self.global.insert(b"schema_fingerprint", hash);
	}
}

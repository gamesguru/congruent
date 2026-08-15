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
			.ok_or_else(|| err!(Database("global counter overflow")))?;

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
}

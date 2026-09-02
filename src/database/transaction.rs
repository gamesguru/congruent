use std::sync::Arc;

use conduwuit::SyncMutex;
use rocksdb::WriteBatchWithTransaction;

pub struct TransactionContext {
	pub batch: WriteBatchWithTransaction<false>,
	pub wake_closures: Vec<Box<dyn FnOnce() + Send>>,
	pub globals_count: Option<u64>,
}

impl Default for TransactionContext {
	#[inline]
	fn default() -> Self { Self::new() }
}

impl TransactionContext {
	#[inline]
	#[must_use]
	pub fn new() -> Self {
		Self {
			batch: WriteBatchWithTransaction::<false>::default(),
			wake_closures: Vec::new(),
			globals_count: None,
		}
	}
}

tokio::task_local! {
	pub static TRANSACTION_BATCH: Arc<SyncMutex<TransactionContext>>;
}

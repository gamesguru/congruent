use std::sync::Arc;

use conduwuit::SyncMutex;
use rocksdb::WriteBatchWithTransaction;

pub type TransactionContext = (WriteBatchWithTransaction<false>, Vec<Box<dyn FnOnce() + Send>>);

tokio::task_local! {
	pub static TRANSACTION_BATCH: Arc<SyncMutex<TransactionContext>>;
}

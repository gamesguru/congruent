use std::{collections::BTreeMap, sync::Arc};

use conduwuit::{Result, Server, SyncMutex, debug, utils::math::usize_from_f64};
use rocksdb::{Cache, LruCacheOptions};

use super::env::Env as SharedEnv;
use crate::pool::Pool;

/// Some components are constructed prior to opening the database and must
/// outlive the database. These can also be shared between database instances
/// though at the time of this comment we only open one database per process.
/// These assets are housed in the shared Context.
pub(crate) struct Context {
	pub(crate) pool: Arc<Pool>,
	pub(crate) col_cache: SyncMutex<BTreeMap<String, Cache>>,
	pub(crate) row_cache: SyncMutex<Cache>,
	pub(crate) env: Arc<SharedEnv>,
	pub(crate) server: Arc<Server>,
}

impl Context {
	pub(crate) fn new(server: &Arc<Server>) -> Result<Arc<Self>> {
		let config = &server.config;
		let cache_capacity_bytes = config.db_cache_capacity_mb * 1024.0 * 1024.0;

		let col_shard_bits = 7;
		let col_cache_capacity_bytes = usize_from_f64(cache_capacity_bytes * 0.50)?;

		let row_shard_bits = 7;
		let row_cache_capacity_bytes = usize_from_f64(cache_capacity_bytes * 0.50)?;

		let mut row_cache_opts = LruCacheOptions::default();
		row_cache_opts.set_num_shard_bits(row_shard_bits);
		row_cache_opts.set_capacity(row_cache_capacity_bytes);
		let row_cache = Cache::new_lru_cache_opts(&row_cache_opts);

		let mut col_cache_opts = LruCacheOptions::default();
		col_cache_opts.set_num_shard_bits(col_shard_bits);
		col_cache_opts.set_capacity(col_cache_capacity_bytes);
		let col_cache = Cache::new_lru_cache_opts(&col_cache_opts);
		let col_cache: BTreeMap<_, _> = [("Shared".to_owned(), col_cache)].into();

		let env = SharedEnv::acquire(server)?;

		Ok(Arc::new(Self {
			pool: Pool::new(server)?,
			col_cache: col_cache.into(),
			row_cache: row_cache.into(),
			env,
			server: server.clone(),
		}))
	}
}

impl Drop for Context {
	#[cold]
	fn drop(&mut self) {
		// Background-thread shutdown for the shared rocksdb environment is
		// deferred to `env::Env`'s drop, which runs only when the last context
		// holding a reference goes away. Joining here would block on thread
		// pools still in use by other live databases in this process.
		debug!("Closing frontend pool");
		self.pool.close();
	}
}

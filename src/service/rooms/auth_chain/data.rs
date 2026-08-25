use std::sync::Arc;

use conduwuit::{Err, Result, SyncMutex, err, utils::math::usize_from_f64};
use database::Map;
use lru_cache::LruCache;
use roaring::RoaringTreemap;

pub(super) struct Data {
	shorteventid_authchain: Arc<Map>,
	pub(super) auth_chain_cache: SyncMutex<LruCache<Vec<u64>, Arc<RoaringTreemap>>>,
}

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let db = &args.db;
		let config = &args.server.config;
		let cache_size = f64::from(config.auth_chain_cache_capacity);
		let cache_size = usize_from_f64(cache_size * config.cache_capacity_modifier)
			.expect("valid cache size");
		Self {
			shorteventid_authchain: db["shorteventid_authchain"].clone(),
			auth_chain_cache: SyncMutex::new(LruCache::new(cache_size)),
		}
	}

	pub(super) async fn get_cached_eventid_authchain(
		&self,
		key: &[u64],
	) -> Result<Arc<RoaringTreemap>> {
		debug_assert!(!key.is_empty(), "auth_chain key must not be empty");

		// Check RAM cache
		if let Some(result) = self.auth_chain_cache.lock().get_mut(key) {
			return Ok(Arc::clone(result));
		}

		// We only save auth chains for single events in the db
		if key.len() != 1 {
			return Err!(Request(NotFound("auth_chain not cached")));
		}

		// Check database (stored as serialized `RoaringTreemap`)
		let raw = self
			.shorteventid_authchain
			.qry(&key[0])
			.await
			.map_err(|_| err!(Request(NotFound("auth_chain not found"))))?;

		let chain =
			Arc::new(RoaringTreemap::deserialize_from(raw.as_ref()).unwrap_or_else(|_| {
				// Legacy format: packed u64 big-endian
				let mut bm = RoaringTreemap::new();
				for chunk in raw.as_chunks::<{ size_of::<u64>() }>().0 {
					let id = u64::from_be_bytes(*chunk);
					bm.insert(id);
				}
				bm
			}));

		// Cache in RAM
		self.auth_chain_cache
			.lock()
			.insert(vec![key[0]], Arc::clone(&chain));

		Ok(chain)
	}

	pub(super) fn cache_auth_chain(&self, key: Vec<u64>, auth_chain: Arc<RoaringTreemap>) {
		debug_assert!(!key.is_empty(), "auth_chain key must not be empty");

		// Only persist single events in db
		if key.len() == 1 {
			let key_bytes = key[0].to_be_bytes();
			let mut val = Vec::new();
			auth_chain
				.serialize_into(&mut val)
				.expect("RoaringTreemap serialization cannot fail into Vec");

			self.shorteventid_authchain.insert(&key_bytes, &val);
		}

		// Cache in RAM
		self.auth_chain_cache.lock().insert(key, auth_chain);
	}

	pub(super) async fn clear_db_cache(&self) {
		self.auth_chain_cache.lock().clear();
		self.shorteventid_authchain.clear().await;
	}
}

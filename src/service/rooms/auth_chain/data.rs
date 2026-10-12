use std::sync::Arc;

use conduwuit::{Result, SyncMutex, err, utils::math::usize_from_f64};
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
		shortroomid: u64,
		shorteventid: u64,
	) -> Result<Arc<RoaringTreemap>> {
		let key = [shortroomid, shorteventid];

		// Check RAM cache
		if let Some(result) = self.auth_chain_cache.lock().get_mut(key.as_slice()) {
			return Ok(Arc::clone(result));
		}

		// Key by room first so a room's closures occupy a contiguous key range.
		// Stored as a serialized `RoaringTreemap`.
		let key_bytes = pack_key(shortroomid, shorteventid);
		let raw = self
			.shorteventid_authchain
			.qry(&key_bytes)
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
			.insert(key.to_vec(), Arc::clone(&chain));

		Ok(chain)
	}

	pub(super) fn cache_auth_chain(
		&self,
		shortroomid: u64,
		shorteventid: u64,
		auth_chain: Arc<RoaringTreemap>,
	) {
		let key = [shortroomid, shorteventid];
		let key_bytes = pack_key(shortroomid, shorteventid);
		let mut val = Vec::new();
		auth_chain
			.serialize_into(&mut val)
			.expect("RoaringTreemap serialization cannot fail into Vec");

		self.shorteventid_authchain.insert(&key_bytes, &val);

		// Cache in RAM
		self.auth_chain_cache
			.lock()
			.insert(key.to_vec(), auth_chain);
	}

	pub(super) async fn clear_db_cache(&self) {
		self.auth_chain_cache.lock().clear();
		self.shorteventid_authchain.clear().await;
	}
}

/// Packs the storage key as `[shortroomid: 8B][shorteventid: 8B]`.
fn pack_key(shortroomid: u64, shorteventid: u64) -> [u8; 16] {
	let mut key = [0_u8; 16];
	key[..8].copy_from_slice(&shortroomid.to_be_bytes());
	key[8..].copy_from_slice(&shorteventid.to_be_bytes());
	key
}

#[cfg(test)]
mod tests {
	use super::pack_key;

	#[test]
	fn auth_chain_keys_are_room_prefixed_big_endian() {
		assert_eq!(pack_key(0x0102_0304_0506_0708, 0x1112_1314_1516_1718), [
			0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
			0x17, 0x18,
		]);
	}
}

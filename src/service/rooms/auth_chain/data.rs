use std::{future::Future, sync::Arc};

use conduwuit::{Result, SyncMutex, err, utils::math::usize_from_f64};
use conduwuit_mtxdb::{events::EventAdapter, shortids::ShortIdAdapter};
use lru_cache::LruCache;
use roaring::RoaringTreemap;

pub(super) struct Data {
	events: EventAdapter,
	shortids: ShortIdAdapter,
	pub(super) auth_chain_cache: SyncMutex<LruCache<Vec<u64>, Arc<RoaringTreemap>>>,
}

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let config = &args.server.config;
		let cache_size = f64::from(config.auth_chain_cache_capacity);
		let cache_size = usize_from_f64(cache_size * config.cache_capacity_modifier)
			.expect("valid cache size");
		Self {
			events: EventAdapter::new(args.mtxdb.clone()),
			shortids: ShortIdAdapter::new(args.mtxdb.clone())
				.expect("mtxdb short IDs must open before auth-chain service"),
			auth_chain_cache: SyncMutex::new(LruCache::new(cache_size)),
		}
	}

	pub(super) fn get_cached_eventid_authchain(
		&self,
		shortroomid: u64,
		shorteventid: u64,
	) -> impl Future<Output = Result<Arc<RoaringTreemap>>> {
		std::future::ready(self.get_cached_eventid_authchain_blocking(shortroomid, shorteventid))
	}

	fn get_cached_eventid_authchain_blocking(
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
		let room_id = self
			.shortids
			.get_room_id(shortroomid)
			.map_err(|e| err!(Database("mtxdb room short-id lookup failed: {e}")))?
			.ok_or_else(|| err!(Request(NotFound("room for short room id not found"))))?;
		let event_id = self
			.shortids
			.get_event_id(shorteventid)
			.map_err(|e| err!(Database("mtxdb event short-id lookup failed: {e}")))?
			.ok_or_else(|| err!(Request(NotFound("event for short event id not found"))))?;
		let ancestors = self
			.events
			.get_auth_chain(&room_id, &event_id)
			.map_err(|e| err!(Database("mtxdb auth-chain lookup failed: {e}")))?;
		let mut bitmap = RoaringTreemap::new();
		for ancestor in ancestors {
			let short = self
				.shortids
				.get_event(&ancestor)
				.map_err(|e| err!(Database("mtxdb auth short-id lookup failed: {e}")))?
				.ok_or_else(|| err!(Request(NotFound("short ID for auth ancestor not found"))))?;
			bitmap.insert(short);
		}
		let chain = Arc::new(bitmap);

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
		// Cache in RAM
		self.auth_chain_cache
			.lock()
			.insert(key.to_vec(), auth_chain);
	}

	pub(super) async fn clear_db_cache(&self) { self.auth_chain_cache.lock().clear(); }
}

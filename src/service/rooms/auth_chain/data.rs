use std::sync::Arc;

use conduwuit::{Result, RuntimeHandle, SyncMutex, err, utils::math::usize_from_f64};
use conduwuit_mtxdb::{events::EventAdapter, shortids::ShortIdAdapter};
use lru_cache::LruCache;
use rezzy::bitmap::Bitmap64;

pub(super) struct Data {
	events: EventAdapter,
	shortids: ShortIdAdapter,
	runtime: RuntimeHandle,
	pub(super) auth_chain_cache: SyncMutex<LruCache<Vec<u64>, Arc<Bitmap64>>>,
}

impl Data {
	/// Create the RAM cache and adapters for persisted auth-chain lookups.
	///
	/// # Panics
	///
	/// Panics if the configured cache size is invalid or short IDs cannot be opened.
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let config = &args.server.config;
		let cache_size = f64::from(config.auth_chain_cache_capacity);
		let cache_size = usize_from_f64(cache_size * config.cache_capacity_modifier)
			.expect("valid cache size");
		Self {
			events: EventAdapter::new(args.mtxdb.clone()),
			shortids: ShortIdAdapter::new(args.mtxdb.clone())
				.expect("mtxdb short IDs must open before auth-chain service"),
			runtime: args.server.runtime().clone(),
			auth_chain_cache: SyncMutex::new(LruCache::new(cache_size)),
		}
	}

	/// Return the RAM-cached chain or load a persisted closure and cache its
	/// ancestor short IDs. This does not walk auth edges when a closure is missing.
	///
	/// Missing room, event, ancestor mappings, or closures return `NotFound`.
	/// Storage and blocking-task failures return database errors.
	pub(super) async fn get_cached_eventid_authchain(
		&self,
		shortroomid: u64,
		shorteventid: u64,
	) -> Result<Arc<Bitmap64>> {
		let key = [shortroomid, shorteventid];

		// Check RAM cache
		if let Some(result) = self.auth_chain_cache.lock().get_mut(key.as_slice()) {
			return Ok(Arc::clone(result));
		}

		// Keep synchronous mtxdb reads off executor workers. Only persisted
		// closures are cacheable; a missing closure uses the server's checked PDU
		// walk instead.
		let events = self.events.clone();
		let shortids = self.shortids.clone();
		let chain = self
			.runtime
			.spawn_blocking(move || {
				let room_id = shortids
					.get_room_id(shortroomid)
					.map_err(|e| err!(Database("mtxdb room short-id lookup failed: {e}")))?
					.ok_or_else(|| err!(Request(NotFound("room for short room id not found"))))?;
				let event_id = shortids
					.get_event_id(shorteventid)
					.map_err(|e| err!(Database("mtxdb event short-id lookup failed: {e}")))?
					.ok_or_else(|| {
						err!(Request(NotFound("event for short event id not found")))
					})?;
				let Some(ancestors) = events
					.persisted_auth_chain(&room_id, &event_id)
					.map_err(|e| err!(Database("mtxdb auth-chain lookup failed: {e}")))?
				else {
					return Err(err!(Request(NotFound("no persisted auth chain for event"))));
				};
				let mut bitmap = Bitmap64::new();
				for ancestor in ancestors {
					let short = shortids
						.get_event(&ancestor)
						.map_err(|e| err!(Database("mtxdb auth short-id lookup failed: {e}")))?
						.ok_or_else(|| {
							err!(Request(NotFound("short ID for auth ancestor not found")))
						})?;
					bitmap.insert(short);
				}
				Ok::<_, conduwuit::Error>(Arc::new(bitmap))
			})
			.await
			.map_err(|e| err!(Database("mtxdb blocking auth-chain lookup failed: {e}")))??;

		// Cache in RAM
		self.auth_chain_cache
			.lock()
			.insert(key.to_vec(), Arc::clone(&chain));

		Ok(chain)
	}

	/// Load several complete chains as one room-local union, translating labels once.
	pub(super) async fn get_cached_authchain_union(
		&self,
		shortroomid: u64,
		starting: &[(u64, &slipstream::EventId)],
	) -> Result<Arc<Bitmap64>> {
		let key = union_cache_key(shortroomid, starting.iter().map(|(id, _)| *id));
		if let Some(cached) = self.auth_chain_cache.lock().get_mut(key.as_slice()) {
			return Ok(Arc::clone(cached));
		}
		let starting: Vec<Vec<u8>> = starting
			.iter()
			.map(|(_, id)| id.as_bytes().to_vec())
			.collect();
		let events = self.events.clone();
		let shortids = self.shortids.clone();
		let chain = self
			.runtime
			.spawn_blocking(move || {
				let room = shortids
					.get_room_id(shortroomid)
					.map_err(|error| err!(Database("room short-id lookup failed: {error}")))?
					.ok_or_else(|| err!(Request(NotFound("room short ID not found"))))?;
				let starting: Vec<&[u8]> = starting.iter().map(Vec::as_slice).collect();
				let union = events
					.complete_auth_chain_union(&room, &starting)
					.map_err(|error| err!(Database("complete auth union failed: {error}")))?;
				let mut bitmap = Bitmap64::new();
				for event in union {
					let id = shortids
						.get_event(&event)
						.map_err(|error| err!(Database("auth short-id lookup failed: {error}")))?
						.ok_or_else(|| err!(Request(NotFound("auth short ID not found"))))?;
					bitmap.insert(id);
				}
				Ok::<_, conduwuit::Error>(Arc::new(bitmap))
			})
			.await
			.map_err(|error| err!(Database("blocking auth union failed: {error}")))??;
		self.auth_chain_cache.lock().insert(key, Arc::clone(&chain));
		Ok(chain)
	}

	/// Cache the supplied ancestor short IDs in RAM for this room and event.
	/// Does not persist the chain or verify its completeness.
	pub(super) fn cache_auth_chain(
		&self,
		shortroomid: u64,
		shorteventid: u64,
		auth_chain: Arc<Bitmap64>,
	) {
		let key = [shortroomid, shorteventid];
		// Cache in RAM
		self.auth_chain_cache
			.lock()
			.insert(key.to_vec(), auth_chain);
	}

	/// Drop any RAM entry and load the closure mtxdb actually persisted.
	/// `NotFound` means no complete closure exists (an auth ancestor lacks one),
	/// so callers must not cache a locally assembled chain for the event.
	pub(super) async fn load_persisted_authchain(
		&self,
		shortroomid: u64,
		shorteventid: u64,
	) -> Result<Arc<Bitmap64>> {
		let key = [shortroomid, shorteventid];
		self.auth_chain_cache.lock().remove(key.as_slice());
		self.get_cached_eventid_authchain(shortroomid, shorteventid)
			.await
	}

	/// Clear the RAM auth-chain cache, leaving persisted mtxdb closures intact.
	pub(super) async fn clear_db_cache(&self) { self.auth_chain_cache.lock().clear(); }
}

/// Cache key for a room-local auth-chain union: the room, the canonical
/// (sorted, deduplicated) starting short IDs, and their count.
///
/// The count keeps a one-element union distinct from the single-event key
/// `[shortroomid, shorteventid]`, so the two entry kinds can never collide.
fn union_cache_key(shortroomid: u64, ids: impl IntoIterator<Item = u64>) -> Vec<u64> {
	let mut ids: Vec<u64> = ids.into_iter().collect();
	ids.sort_unstable();
	ids.dedup();
	let mut key = Vec::with_capacity(ids.len().saturating_add(2));
	key.push(shortroomid);
	key.push(u64::try_from(ids.len()).expect("starting-event count fits u64"));
	key.extend(ids);
	key
}

#[cfg(test)]
mod tests {
	use super::union_cache_key;

	#[test]
	fn union_cache_key_is_order_and_duplicate_insensitive() {
		let ordered = union_cache_key(7, [1, 2, 3]);
		assert_eq!(ordered, vec![7, 3, 1, 2, 3]);
		assert_eq!(ordered, union_cache_key(7, [3, 1, 2, 2, 3]));
		assert_ne!(ordered, union_cache_key(8, [1, 2, 3]));
		assert_ne!(ordered, union_cache_key(7, [1, 2]));
	}

	#[test]
	fn union_cache_key_never_collides_with_an_event_key() {
		// Single-event entries are `[shortroomid, shorteventid]`; a union of one
		// id must not share that shape.
		assert_eq!(union_cache_key(9, [42]), vec![9, 1, 42]);
		assert_ne!(union_cache_key(9, [42]), vec![9, 42]);
		assert_ne!(union_cache_key(9, [42]), union_cache_key(9, [43]));
	}

	#[test]
	fn empty_union_key_is_just_room_and_count() {
		assert_eq!(union_cache_key(5, []), vec![5, 0]);
	}
}

use std::{
	borrow::Borrow,
	mem::{size_of, size_of_val},
	sync::Arc,
};

pub use conduwuit::matrix::pdu::{ShortEventId, ShortId, ShortRoomId, ShortStateKey};
use conduwuit::{
	Result, SyncMutex, err, implement,
	matrix::StateKey,
	pair_of,
	utils::{self, IterStream, ReadyExt},
};
use database::{Deserialized, Get, Map, Qry};
use futures::{
	Stream, StreamExt,
	stream::{self},
};
use ruma::{EventId, OwnedEventId, RoomId, RoomVersionId, events::StateEventType};
use serde::Deserialize;

use crate::{Dep, globals};

pub struct Service {
	db: Data,
	services: Services,
	pub eventid_shorteventid_cache: moka::sync::Cache<OwnedEventId, ShortEventId>,
	pub shorteventid_eventid_cache: moka::sync::Cache<ShortEventId, OwnedEventId>,
	pub statekey_shortstatekey_cache:
		moka::sync::Cache<(StateEventType, StateKey), ShortStateKey>,
	pub shortstatekey_statekey_cache:
		moka::sync::Cache<ShortStateKey, (StateEventType, StateKey)>,
	shorteventid_create_mutex: SyncMutex<()>,
}

struct Data {
	eventid_shorteventid: Arc<Map>,
	shorteventid_eventid: Arc<Map>,
	statekey_shortstatekey: Arc<Map>,
	shortstatekey_statekey: Arc<Map>,
	roomid_shortroomid: Arc<Map>,
	roomid_roomversion: Arc<Map>,
	statehash_shortstatehash: Arc<Map>,
}

struct Services {
	globals: Dep<globals::Service>,
}

pub type ShortStateHash = ShortId;

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let eventidshort_cap = utils::math::usize_from_f64(
			f64::from(args.server.config.eventidshort_cache_capacity)
				* args.server.config.cache_capacity_modifier,
		)
		.expect("valid cache size")
		.try_into()
		.unwrap_or(args.server.config.eventidshort_cache_capacity);

		let shorteventid_cap = utils::math::usize_from_f64(
			f64::from(args.server.config.shorteventid_cache_capacity)
				* args.server.config.cache_capacity_modifier,
		)
		.expect("valid cache size")
		.try_into()
		.unwrap_or(args.server.config.shorteventid_cache_capacity);

		let statekeyshort_cap = utils::math::usize_from_f64(
			f64::from(args.server.config.statekeyshort_cache_capacity)
				* args.server.config.cache_capacity_modifier,
		)
		.expect("valid cache size")
		.try_into()
		.unwrap_or(args.server.config.statekeyshort_cache_capacity);

		let shortstatekey_cap = utils::math::usize_from_f64(
			f64::from(args.server.config.shortstatekey_cache_capacity)
				* args.server.config.cache_capacity_modifier,
		)
		.expect("valid cache size")
		.try_into()
		.unwrap_or(args.server.config.shortstatekey_cache_capacity);

		Ok(Arc::new(Self {
			db: Data {
				eventid_shorteventid: args.db["eventid_shorteventid"].clone(),
				shorteventid_eventid: args.db["shorteventid_eventid"].clone(),
				statekey_shortstatekey: args.db["statekey_shortstatekey"].clone(),
				shortstatekey_statekey: args.db["shortstatekey_statekey"].clone(),
				roomid_shortroomid: args.db["roomid_shortroomid"].clone(),
				roomid_roomversion: args.db["roomid_roomversion"].clone(),
				statehash_shortstatehash: args.db["statehash_shortstatehash"].clone(),
			},
			services: Services {
				globals: args.depend::<globals::Service>("globals"),
			},
			eventid_shorteventid_cache: moka::sync::Cache::builder()
				.max_capacity(eventidshort_cap.into())
				.build(),
			shorteventid_eventid_cache: moka::sync::Cache::builder()
				.max_capacity(shorteventid_cap.into())
				.build(),
			statekey_shortstatekey_cache: moka::sync::Cache::builder()
				.max_capacity(statekeyshort_cap.into())
				.build(),
			shortstatekey_statekey_cache: moka::sync::Cache::builder()
				.max_capacity(shortstatekey_cap.into())
				.build(),
			shorteventid_create_mutex: SyncMutex::new(()),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

#[implement(Service)]
pub async fn get_or_create_shorteventid(&self, event_id: &EventId) -> ShortEventId {
	if let Ok(shorteventid) = self.get_shorteventid(event_id).await {
		return shorteventid;
	}

	self.create_shorteventid(event_id)
}

#[implement(Service)]
pub fn get_or_create_shorteventid_blocking(&self, event_id: &EventId) -> ShortEventId {
	if let Some(short) = self.eventid_shorteventid_cache.get(&event_id.to_owned()) {
		return short;
	}

	if let Ok(handle) = self
		.db
		.eventid_shorteventid
		.get_blocking(event_id.as_bytes())
	{
		let short = utils::u64_from_u8(&handle);
		self.eventid_shorteventid_cache
			.insert(event_id.to_owned(), short);
		self.shorteventid_eventid_cache
			.insert(short, event_id.to_owned());
		return short;
	}

	self.create_shorteventid(event_id)
}

#[implement(Service)]
pub fn get_shorteventid_blocking(&self, event_id: &EventId) -> Result<ShortEventId> {
	if let Some(short) = self.eventid_shorteventid_cache.get(&event_id.to_owned()) {
		return Ok(short);
	}

	let handle = self
		.db
		.eventid_shorteventid
		.get_blocking(event_id.as_bytes())?;
	let short = utils::u64_from_u8(&handle);
	self.eventid_shorteventid_cache
		.insert(event_id.to_owned(), short);
	self.shorteventid_eventid_cache
		.insert(short, event_id.to_owned());
	Ok(short)
}

#[implement(Service)]
pub fn multi_get_or_create_shorteventid<'a, I>(
	&'a self,
	event_ids: I,
) -> impl Stream<Item = ShortEventId> + Send + 'a
where
	I: Iterator<Item = &'a EventId> + Clone + Send + 'a,
{
	event_ids
		.stream()
		.ready_chunks(256)
		.then(move |chunk| async move {
			let mut results = Vec::with_capacity(chunk.len());
			let mut misses = Vec::new();
			let mut miss_indices = Vec::new();

			for (i, &event_id) in chunk.iter().enumerate() {
				if let Some(short) = self.eventid_shorteventid_cache.get(&event_id.to_owned()) {
					results.push(Some(short));
				} else {
					results.push(None);
					misses.push(event_id);
					miss_indices.push(i);
				}
			}

			if !misses.is_empty() {
				const BUFSIZE: usize = size_of::<ShortEventId>();
				let db_results: Vec<Result<database::Handle<'_>>> = stream::iter(misses.clone())
					.get(&self.db.eventid_shorteventid)
					.collect()
					.await;

				let mut unresolved = Vec::new();

				for (idx, (result, event_id)) in miss_indices
					.into_iter()
					.zip(db_results.into_iter().zip(misses))
				{
					let short = match result {
						| Ok(ref handle) => {
							let short = utils::u64_from_u8(handle);
							self.eventid_shorteventid_cache
								.insert(event_id.to_owned(), short);
							self.shorteventid_eventid_cache
								.insert(short, event_id.to_owned());
							short
						},
						| Err(_) => {
							unresolved.push((idx, event_id));
							continue;
						},
					};
					results[idx] = Some(short);
				}

				if !unresolved.is_empty() {
					let _guard = self.shorteventid_create_mutex.lock();
					let mut new_event_ids = Vec::new();
					let mut new_allocations = std::collections::HashMap::new();

					for &(_, event_id) in &unresolved {
						if new_allocations.contains_key(event_id)
							|| new_event_ids.contains(&event_id)
						{
							continue;
						}

						if let Ok(handle) = self
							.db
							.eventid_shorteventid
							.get_blocking(event_id.as_bytes())
						{
							let short = utils::u64_from_u8(&handle);
							new_allocations.insert(event_id, short);
							self.eventid_shorteventid_cache
								.insert(event_id.to_owned(), short);
							self.shorteventid_eventid_cache
								.insert(short, event_id.to_owned());
						} else {
							new_event_ids.push(event_id);
						}
					}

					if !new_event_ids.is_empty() {
						let mut next_id = self
							.services
							.globals
							.next_count_batch(u64::try_from(new_event_ids.len()).unwrap())
							.unwrap();

						for event_id in new_event_ids {
							let short = next_id.saturating_add(1);
							next_id = short;

							self.db
								.eventid_shorteventid
								.raw_aput::<BUFSIZE, _, _>(event_id, short);
							self.db
								.shorteventid_eventid
								.aput_raw::<BUFSIZE, _, _>(short, event_id);

							new_allocations.insert(event_id, short);
							self.eventid_shorteventid_cache
								.insert(event_id.to_owned(), short);
							self.shorteventid_eventid_cache
								.insert(short, event_id.to_owned());
						}
					}

					for (idx, event_id) in unresolved {
						results[idx] = Some(new_allocations[event_id]);
					}
				}
			}

			stream::iter(results.into_iter().map(Option::unwrap))
		})
		.flatten()
}

#[implement(Service)]
fn create_shorteventid(&self, event_id: &EventId) -> ShortEventId {
	const BUFSIZE: usize = size_of::<ShortEventId>();

	let _guard = self.shorteventid_create_mutex.lock();

	if let Ok(handle) = self
		.db
		.eventid_shorteventid
		.get_blocking(event_id.as_bytes())
	{
		let short = utils::u64_from_u8(&handle);
		self.eventid_shorteventid_cache
			.insert(event_id.to_owned(), short);
		self.shorteventid_eventid_cache
			.insert(short, event_id.to_owned());
		return short;
	}

	let short = self.services.globals.next_count().unwrap();
	debug_assert!(size_of_val(&short) == BUFSIZE, "buffer requirement changed");

	self.db
		.eventid_shorteventid
		.raw_aput::<BUFSIZE, _, _>(event_id, short);

	self.db
		.shorteventid_eventid
		.aput_raw::<BUFSIZE, _, _>(short, event_id);

	self.eventid_shorteventid_cache
		.insert(event_id.to_owned(), short);
	self.shorteventid_eventid_cache
		.insert(short, event_id.to_owned());

	short
}

#[implement(Service)]
pub async fn get_shorteventid(&self, event_id: &EventId) -> Result<ShortEventId> {
	if let Some(short) = self.eventid_shorteventid_cache.get(&event_id.to_owned()) {
		return Ok(short);
	}

	let short = self
		.db
		.eventid_shorteventid
		.get(event_id)
		.await
		.deserialized()?;

	self.eventid_shorteventid_cache
		.insert(event_id.to_owned(), short);
	self.shorteventid_eventid_cache
		.insert(short, event_id.to_owned());
	Ok(short)
}

#[implement(Service)]
pub async fn get_or_create_shortstatekey(
	&self,
	event_type: &StateEventType,
	state_key: &str,
) -> ShortStateKey {
	const BUFSIZE: usize = size_of::<ShortStateKey>();

	if let Ok(shortstatekey) = self.get_shortstatekey(event_type, state_key).await {
		return shortstatekey;
	}

	let key = (event_type, state_key);
	let shortstatekey = self.services.globals.next_count().unwrap();
	debug_assert!(size_of_val(&shortstatekey) == BUFSIZE, "buffer requirement changed");

	self.db
		.statekey_shortstatekey
		.put_aput::<BUFSIZE, _, _>(key, shortstatekey);

	self.db
		.shortstatekey_statekey
		.aput_put::<BUFSIZE, _, _>(shortstatekey, key);

	let cached_key = (event_type.clone(), StateKey::from(state_key));
	self.statekey_shortstatekey_cache
		.insert(cached_key.clone(), shortstatekey);
	self.shortstatekey_statekey_cache
		.insert(shortstatekey, cached_key);

	shortstatekey
}

#[implement(Service)]
pub async fn get_shortstatekey(
	&self,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<ShortStateKey> {
	let cached_key = (event_type.clone(), StateKey::from(state_key));
	if let Some(short) = self.statekey_shortstatekey_cache.get(&cached_key) {
		return Ok(short);
	}

	let key = (event_type, state_key);
	let short: ShortStateKey = self
		.db
		.statekey_shortstatekey
		.qry(&key)
		.await
		.deserialized()?;

	self.statekey_shortstatekey_cache
		.insert(cached_key.clone(), short);
	self.shortstatekey_statekey_cache.insert(short, cached_key);
	Ok(short)
}

#[implement(Service)]
pub async fn get_eventid_from_short<Id>(&self, shorteventid: ShortEventId) -> Result<Id>
where
	Id: for<'de> Deserialize<'de> + Sized + ToOwned,
	<Id as ToOwned>::Owned: Borrow<EventId>,
{
	const BUFSIZE: usize = size_of::<ShortEventId>();

	if let Some(cached) = self.shorteventid_eventid_cache.get(&shorteventid) {
		let s = serde_json::to_vec(&cached)
			.map_err(|e| err!(Database("Failed to serialize cached EventId: {e:?}")))?;
		return serde_json::from_slice::<Id>(&s)
			.map_err(|e| err!(Database("Failed to deserialize EventId from cache: {e:?}")));
	}

	let res: Id = self
		.db
		.shorteventid_eventid
		.aqry::<BUFSIZE, _>(&shorteventid)
		.await
		.deserialized()
		.map_err(|e| {
			err!(Database("Failed to find EventId from short {shorteventid:?}: {e:?}"))
		})?;

	let owned = res.to_owned();
	let event_id: &EventId = owned.borrow();
	self.shorteventid_eventid_cache
		.insert(shorteventid, event_id.to_owned());
	self.eventid_shorteventid_cache
		.insert(event_id.to_owned(), shorteventid);

	Ok(res)
}

#[implement(Service)]
pub fn multi_get_eventid_from_short<'a, Id, S>(
	&'a self,
	shorteventid: S,
) -> impl Stream<Item = Result<Id>> + Send + 'a
where
	S: Stream<Item = ShortEventId> + Send + 'a,
	Id: for<'de> Deserialize<'de> + Sized + ToOwned + Send + 'a,
	<Id as ToOwned>::Owned: Borrow<EventId>,
{
	shorteventid
		.ready_chunks(256)
		.then(move |chunk| async move {
			let mut results = Vec::with_capacity(chunk.len());
			let mut misses = Vec::new();
			let mut miss_indices = Vec::new();

			for (i, key) in chunk.iter().copied().enumerate() {
				if let Some(cached) = self.shorteventid_eventid_cache.get(&key) {
					let res = serde_json::to_vec(&cached)
						.map_err(|e| err!(Database("Failed to serialize cached EventId: {e:?}")))
						.and_then(|s| {
							serde_json::from_slice::<Id>(&s).map_err(|e| {
								err!(Database("Failed to deserialize EventId from cache: {e:?}"))
							})
						});
					results.push(Some(res));
				} else {
					results.push(None);
					misses.push(key);
					miss_indices.push(i);
				}
			}

			if !misses.is_empty() {
				let db_results: Vec<Result<database::Handle<'_>>> = stream::iter(misses.clone())
					.qry(&self.db.shorteventid_eventid)
					.collect()
					.await;

				for ((&miss_key, res), idx) in misses.iter().zip(db_results).zip(miss_indices) {
					let val: Result<Id> = res.deserialized();

					if let Ok(ref val) = val {
						let owned = val.to_owned();
						let event_id: &EventId = owned.borrow();
						self.shorteventid_eventid_cache
							.insert(miss_key, event_id.to_owned());
						self.eventid_shorteventid_cache
							.insert(event_id.to_owned(), miss_key);
					}

					results[idx] = Some(val);
				}
			}

			stream::iter(results.into_iter().map(Option::unwrap))
		})
		.flatten()
}

#[implement(Service)]
pub async fn get_statekey_from_short(
	&self,
	shortstatekey: ShortStateKey,
) -> Result<(StateEventType, StateKey)> {
	const BUFSIZE: usize = size_of::<ShortStateKey>();

	if let Some(cached) = self.shortstatekey_statekey_cache.get(&shortstatekey) {
		return Ok(cached);
	}

	let res: (StateEventType, StateKey) = self
		.db
		.shortstatekey_statekey
		.aqry::<BUFSIZE, _>(&shortstatekey)
		.await
		.deserialized()
		.map_err(|e| {
			err!(Database(
				"Failed to find (StateEventType, state_key) from short {shortstatekey:?}: {e:?}"
			))
		})?;

	self.shortstatekey_statekey_cache
		.insert(shortstatekey, res.clone());
	self.statekey_shortstatekey_cache
		.insert(res.clone(), shortstatekey);
	Ok(res)
}

#[implement(Service)]
pub fn multi_get_statekey_from_short<'a, S>(
	&'a self,
	shortstatekey: S,
) -> impl Stream<Item = Result<(StateEventType, StateKey)>> + Send + 'a
where
	S: Stream<Item = ShortStateKey> + Send + 'a,
{
	shortstatekey
		.ready_chunks(256)
		.then(move |chunk| async move {
			let mut results = Vec::with_capacity(chunk.len());
			let mut misses = Vec::new();
			let mut miss_indices = Vec::new();

			for (i, key) in chunk.iter().copied().enumerate() {
				if let Some(cached) = self.shortstatekey_statekey_cache.get(&key) {
					results.push(Some(Ok(cached.clone())));
				} else {
					results.push(None);
					misses.push(key);
					miss_indices.push(i);
				}
			}

			if !misses.is_empty() {
				let db_results: Vec<Result<(StateEventType, StateKey)>> =
					stream::iter(misses.clone())
						.qry(&self.db.shortstatekey_statekey)
						.map(Deserialized::deserialized)
						.collect()
						.await;

				for ((&miss_key, res), idx) in misses.iter().zip(db_results).zip(miss_indices) {
					if let Ok(ref val) = res {
						self.shortstatekey_statekey_cache
							.insert(miss_key, val.clone());
						self.statekey_shortstatekey_cache
							.insert(val.clone(), miss_key);
					}
					results[idx] = Some(res);
				}
			}

			stream::iter(results.into_iter().map(Option::unwrap))
		})
		.flatten()
}

/// Returns (shortstatehash, already_existed)
#[implement(Service)]
pub async fn get_or_create_shortstatehash(&self, state_hash: &[u8]) -> (ShortStateHash, bool) {
	const BUFSIZE: usize = size_of::<ShortStateHash>();

	if let Ok(shortstatehash) = self
		.db
		.statehash_shortstatehash
		.get(state_hash)
		.await
		.deserialized()
	{
		return (shortstatehash, true);
	}

	let shortstatehash = self.services.globals.next_count().unwrap();
	debug_assert!(size_of_val(&shortstatehash) == BUFSIZE, "buffer requirement changed");

	self.db
		.statehash_shortstatehash
		.raw_aput::<BUFSIZE, _, _>(state_hash, shortstatehash);

	(shortstatehash, false)
}

#[implement(Service)]
pub async fn get_shortroomid(&self, room_id: &RoomId) -> Result<ShortRoomId> {
	self.db.roomid_shortroomid.get(room_id).await.deserialized()
}

#[implement(Service)]
pub async fn get_or_create_shortroomid(&self, room_id: &RoomId) -> ShortRoomId {
	self.db
		.roomid_shortroomid
		.get(room_id)
		.await
		.deserialized()
		.unwrap_or_else(|_| {
			const BUFSIZE: usize = size_of::<ShortRoomId>();

			let short = self.services.globals.next_count().unwrap();
			debug_assert!(size_of_val(&short) == BUFSIZE, "buffer requirement changed");

			self.db
				.roomid_shortroomid
				.raw_aput::<BUFSIZE, _, _>(room_id, short);

			short
		})
}

#[implement(Service)]
pub async fn multi_get_state_from_short<'a, S>(
	&'a self,
	short_state: S,
) -> impl Stream<Item = Result<((StateEventType, StateKey), OwnedEventId)>> + Send + 'a
where
	S: Stream<Item = (ShortStateKey, ShortEventId)> + Send + 'a,
{
	let (short_state_keys, short_event_ids): pair_of!(Vec<_>) = short_state.unzip().await;

	StreamExt::zip(
		self.multi_get_statekey_from_short(stream::iter(short_state_keys)),
		self.multi_get_eventid_from_short(stream::iter(short_event_ids)),
	)
	.ready_filter_map(|state_event| match state_event {
		| (Ok(state_key), Ok(event_id)) => Some(Ok((state_key, event_id))),
		| (Err(e), _) | (_, Err(e)) => Some(Err(e)),
	})
}

#[implement(Service)]
pub async fn get_room_version(&self, room_id: &RoomId) -> Result<RoomVersionId> {
	self.db.roomid_roomversion.get(room_id).await.deserialized()
}

#[implement(Service)]
pub fn set_room_version(&self, room_id: &RoomId, version: &RoomVersionId) {
	self.db.roomid_roomversion.insert(room_id, version);
}

#[cfg(test)]
mod tests {
	use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

	// RocksDB Env::new() returns the global default env. Context::Drop calls
	// env.join_all_threads() which kills background threads shared by ALL
	// databases in the process. Tests must run serially to prevent the first
	// test's teardown from deadlocking the others.
	static DB_TEST_MUTEX: std::sync::LazyLock<tokio::sync::Mutex<()>> =
		std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

	use conduwuit::{
		Server,
		config::Config,
		log::{Log, LogLevelReloadHandles, capture},
		matrix::StateKey,
	};
	use database::Database;
	use figment::providers::Format;
	use futures::stream::{self, StreamExt};
	use ruma::{OwnedEventId, event_id, events::StateEventType};

	use super::*;
	use crate::Service as _;

	struct TempDbGuard {
		path: PathBuf,
	}

	impl Drop for TempDbGuard {
		fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.path); }
	}

	async fn setup_test_service()
	-> (TempDbGuard, Arc<globals::Service>, Arc<Service>, Arc<crate::service::Map>) {
		static TEST_DB_COUNTER: std::sync::atomic::AtomicU64 =
			std::sync::atomic::AtomicU64::new(0);
		let count = TEST_DB_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
		let db_path = std::env::temp_dir().join(format!("conduwuit_test_db_{count}"));
		let _ = std::fs::remove_dir_all(&db_path); // ensure it's clean

		let guard = TempDbGuard { path: db_path.clone() };

		let figment = figment::Figment::new().merge(figment::providers::Toml::string(&format!(
			r#"
				server_name = "test.conduwuit.local"
				database_path = "{}"
				"#,
			db_path.to_string_lossy().replace('\\', "/")
		)));

		let config = Config::new(&figment).expect("failed to parse config");
		let runtime_handle = tokio::runtime::Handle::current();
		let server = Arc::new(Server::new(config, Some(&runtime_handle), Log {
			reload: LogLevelReloadHandles::default(),
			capture: Arc::new(capture::State::default()),
		}));

		let db = Database::open(&server)
			.await
			.expect("failed to open database");
		let service_map = Arc::new(conduwuit::SyncRwLock::new(BTreeMap::new()));

		let globals_service = globals::Service::build(crate::Args {
			db: &db,
			server: &server,
			service: &service_map,
		})
		.expect("failed to build globals service");

		let globals_service_dyn: Arc<dyn crate::Service> = globals_service.clone();
		let globals_any_dyn: Arc<dyn std::any::Any + Send + Sync> = globals_service.clone();
		service_map.write().insert(
			"globals".to_owned(),
			(Arc::downgrade(&globals_service_dyn), Arc::downgrade(&globals_any_dyn)),
		);

		let short_service = Service::build(crate::Args {
			db: &db,
			server: &server,
			service: &service_map,
		})
		.expect("failed to build short service");

		(guard, globals_service, short_service, service_map)
	}

	#[tokio::test]
	async fn test_shorteventid_caching() {
		let _serial = DB_TEST_MUTEX.lock().await;
		let (_guard, _globals, service, _map) = setup_test_service().await;
		let event_id = event_id!("$abc:test.conduwuit.local");

		// Initial lookup should result in cache miss and query DB (or allocate new
		// since not in DB) Since it doesn't exist in DB, get_shorteventid returns
		// Err, but get_or_create resolves/creates it
		let short_id1 = service.get_or_create_shorteventid(event_id).await;

		// Cache should now contain the mappings
		assert_eq!(service.eventid_shorteventid_cache.get(&event_id.to_owned()), Some(short_id1));
		assert_eq!(service.shorteventid_eventid_cache.get(&short_id1), Some(event_id.to_owned()));

		// Clear cache and retrieve via get_shorteventid to verify DB storage
		service.eventid_shorteventid_cache.invalidate_all();
		service.shorteventid_eventid_cache.invalidate_all();
		service.eventid_shorteventid_cache.run_pending_tasks();
		service.shorteventid_eventid_cache.run_pending_tasks();
		assert_eq!(service.eventid_shorteventid_cache.get(&event_id.to_owned()), None);

		let short_id2 = service.get_shorteventid(event_id).await.unwrap();
		assert_eq!(short_id1, short_id2);

		// Cache should be repopulated after DB hit
		assert_eq!(service.eventid_shorteventid_cache.get(&event_id.to_owned()), Some(short_id1));

		// Test retrieve event_id from short_id
		let retrieved: OwnedEventId = service.get_eventid_from_short(short_id1).await.unwrap();
		assert_eq!(retrieved, event_id.to_owned());
	}

	#[tokio::test]
	async fn test_shortstatekey_caching() {
		let _serial = DB_TEST_MUTEX.lock().await;
		let (_guard, _globals, service, _map) = setup_test_service().await;
		let event_type = StateEventType::RoomName;
		let state_key = "";

		// Initial get should fail
		service
			.get_shortstatekey(&event_type, state_key)
			.await
			.unwrap_err();

		// Create/get state key
		let short_key1 = service
			.get_or_create_shortstatekey(&event_type, state_key)
			.await;

		// Cache should contain mappings
		let cache_key = (event_type.clone(), StateKey::from(state_key));
		assert_eq!(service.statekey_shortstatekey_cache.get(&cache_key), Some(short_key1));
		assert_eq!(
			service.shortstatekey_statekey_cache.get(&short_key1),
			Some(cache_key.clone())
		);

		// Invalidate and check DB persistence
		service.statekey_shortstatekey_cache.invalidate_all();
		service.shortstatekey_statekey_cache.invalidate_all();
		service.statekey_shortstatekey_cache.run_pending_tasks();
		service.shortstatekey_statekey_cache.run_pending_tasks();

		let short_key2 = service
			.get_shortstatekey(&event_type, state_key)
			.await
			.unwrap();
		assert_eq!(short_key1, short_key2);

		// Cache should be repopulated
		assert_eq!(service.statekey_shortstatekey_cache.get(&cache_key), Some(short_key1));

		// Retrieve original key from short state key
		let (ret_type, ret_key) = service.get_statekey_from_short(short_key1).await.unwrap();
		assert_eq!(ret_type, event_type);
		assert_eq!(ret_key.as_str(), state_key);
	}

	#[tokio::test]
	async fn test_multi_lookups() {
		let _serial = DB_TEST_MUTEX.lock().await;
		let (_guard, _globals, service, _map) = setup_test_service().await;

		let event1 = event_id!("$event1:test.conduwuit.local");
		let event2 = event_id!("$event2:test.conduwuit.local");

		// Multi create/get
		let stream = service.multi_get_or_create_shorteventid(vec![event1, event2].into_iter());
		let mut stream = std::pin::pin!(stream);
		let short1 = stream.next().await.unwrap();
		let short2 = stream.next().await.unwrap();
		assert!(stream.next().await.is_none());

		// Check caches
		assert_eq!(service.eventid_shorteventid_cache.get(&event1.to_owned()), Some(short1));
		assert_eq!(service.eventid_shorteventid_cache.get(&event2.to_owned()), Some(short2));

		// Retrieve batch from short ids
		let short_stream = stream::iter(vec![short1, short2]);
		let event_stream = service.multi_get_eventid_from_short::<OwnedEventId, _>(short_stream);
		let mut event_stream = std::pin::pin!(event_stream);
		assert_eq!(event_stream.next().await.unwrap().unwrap(), event1.to_owned());
		assert_eq!(event_stream.next().await.unwrap().unwrap(), event2.to_owned());
		assert!(event_stream.next().await.is_none());

		// Test state keys batch lookup
		let type1 = StateEventType::RoomTopic;
		let type2 = StateEventType::RoomAvatar;
		let sk1 = service.get_or_create_shortstatekey(&type1, "key1").await;
		let sk2 = service.get_or_create_shortstatekey(&type2, "key2").await;

		let statekey_stream = stream::iter(vec![sk1, sk2]);
		let state_result_stream = service.multi_get_statekey_from_short(statekey_stream);
		let mut state_result_stream = std::pin::pin!(state_result_stream);
		let res1 = state_result_stream.next().await.unwrap().unwrap();
		let res2 = state_result_stream.next().await.unwrap().unwrap();

		assert_eq!(res1.0, type1);
		assert_eq!(res1.1.as_str(), "key1");
		assert_eq!(res2.0, type2);
		assert_eq!(res2.1.as_str(), "key2");
		assert!(state_result_stream.next().await.is_none());
	}
}

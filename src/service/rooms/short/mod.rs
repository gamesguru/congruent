use std::{
	borrow::Borrow,
	mem::{size_of, size_of_val},
	sync::Arc,
};

pub use conduwuit::matrix::pdu::{ShortEventId, ShortId, ShortRoomId, ShortStateKey};
use conduwuit::{
	Result, err, implement,
	matrix::StateKey,
	pair_of,
	utils::{self, IterStream, ReadyExt},
};
use conduwuit_mtxdb::shortids::ShortIdAdapter;
use database::{Deserialized, Map};
use futures::{
	Stream, StreamExt,
	stream::{self},
};
use slipstream::{
	EventId, OwnedEventId, RoomId, RoomVersionId, codec::Deserialize, events::StateEventType,
};

use crate::{Dep, globals};

/// Short ids for events and state keys. They are allocated and stored by
/// mtxdb, which is also where the HAMT leaves that carry them live; the
/// caches only sit in front of that.
pub struct Service {
	db: Data,
	shortids: ShortIdAdapter,
	services: Services,
	pub eventid_shorteventid_cache: moka::sync::Cache<OwnedEventId, ShortEventId>,
	pub shorteventid_eventid_cache: moka::sync::Cache<ShortEventId, OwnedEventId>,
	pub statekey_shortstatekey_cache:
		moka::sync::Cache<(StateEventType, StateKey), ShortStateKey>,
	pub shortstatekey_statekey_cache:
		moka::sync::Cache<ShortStateKey, (StateEventType, StateKey)>,
	pub leanevent_cache: moka::sync::Cache<OwnedEventId, Arc<rezzy::LeanEvent<String>>>,
}

struct Data {
	roomid_shortroomid: Arc<Map>,
	roomid_roomversion: Arc<Map>,
}

struct Services {
	globals: Dep<globals::Service>,
}

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
				roomid_shortroomid: args.db["roomid_shortroomid"].clone(),
				roomid_roomversion: args.db["roomid_roomversion"].clone(),
			},
			shortids: ShortIdAdapter::new(args.mtxdb.clone())
				.map_err(|error| err!(Database("Failed to open mtxdb short ids: {error}")))?,
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
			leanevent_cache: moka::sync::Cache::builder()
				.max_capacity(args.server.config.leanevent_cache_capacity.into())
				.build(),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(module_path!()) }
}
fn short_error(what: &str, error: impl std::fmt::Display) -> conduwuit::Error {
	err!(Database("mtxdb short id {what} failed: {error}"))
}

fn not_found(what: &str) -> conduwuit::Error { err!(Request(NotFound("{what} not found"))) }

impl Service {
	fn cache_event(&self, event_id: &EventId, short: ShortEventId) {
		self.eventid_shorteventid_cache
			.insert(event_id.to_owned(), short);
		self.shorteventid_eventid_cache
			.insert(short, event_id.to_owned());
	}

	fn cache_state_key(&self, key: (StateEventType, StateKey), short: ShortStateKey) {
		self.statekey_shortstatekey_cache.insert(key.clone(), short);
		self.shortstatekey_statekey_cache.insert(short, key);
	}

	/// Resolve a stored event id string into the caller's id type.
	fn parse_event_id<Id>(short: ShortEventId, bytes: &[u8]) -> Result<Id>
	where
		Id: Deserialize + Sized + ToOwned,
		<Id as ToOwned>::Owned: Borrow<EventId>,
	{
		let text = utils::string::str_from_bytes(bytes)?;
		Id::from_json(&slipstream::json::Value::String(text.to_owned()))
			.map_err(|e| err!(Database("Failed to parse EventId from short {short:?}: {e:?}")))
	}
}

#[implement(Service)]
pub async fn get_or_create_shorteventid(&self, event_id: &EventId) -> ShortEventId {
	self.get_or_create_shorteventid_blocking(event_id)
}

#[implement(Service)]
pub fn get_or_create_shorteventid_blocking(&self, event_id: &EventId) -> ShortEventId {
	if let Some(short) = self.eventid_shorteventid_cache.get(&event_id.to_owned()) {
		return short;
	}

	let short = self
		.shortids
		.allocate_events(&[event_id.as_bytes()])
		.expect("mtxdb short event id allocation")[0];
	self.cache_event(event_id, short);
	short
}

#[implement(Service)]
pub fn get_shorteventid_blocking(&self, event_id: &EventId) -> Result<ShortEventId> {
	if let Some(short) = self.eventid_shorteventid_cache.get(&event_id.to_owned()) {
		return Ok(short);
	}

	let short = self
		.shortids
		.get_event(event_id.as_bytes())
		.map_err(|e| short_error("event lookup", e))?
		.ok_or_else(|| not_found("short event id"))?;
	self.cache_event(event_id, short);
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
		.map(move |chunk| {
			let mut results: Vec<Option<ShortEventId>> = chunk
				.iter()
				.map(|&event_id| self.eventid_shorteventid_cache.get(&event_id.to_owned()))
				.collect();

			let misses: Vec<&EventId> = chunk
				.iter()
				.zip(&results)
				.filter(|(_, hit)| hit.is_none())
				.map(|(&event_id, _)| event_id)
				.collect();

			if !misses.is_empty() {
				let miss_bytes: Vec<&[u8]> = misses.iter().map(|id| id.as_bytes()).collect();
				let allocated = self
					.shortids
					.allocate_events(&miss_bytes)
					.expect("mtxdb short event id allocation");

				let mut allocated = allocated.into_iter();
				for (event_id, slot) in chunk
					.iter()
					.zip(results.iter_mut())
					.filter(|(_, slot)| slot.is_none())
				{
					let short = allocated.next().expect("one short id per miss");
					self.cache_event(event_id, short);
					*slot = Some(short);
				}
			}

			stream::iter(results.into_iter().map(Option::unwrap))
		})
		.flatten()
}

#[implement(Service)]
pub async fn get_shorteventid(&self, event_id: &EventId) -> Result<ShortEventId> {
	self.get_shorteventid_blocking(event_id)
}

#[implement(Service)]
pub async fn get_or_create_shortstatekey(
	&self,
	event_type: &StateEventType,
	state_key: &str,
) -> ShortStateKey {
	let cached_key = (event_type.clone(), StateKey::from(state_key));
	if let Some(short) = self.statekey_shortstatekey_cache.get(&cached_key) {
		return short;
	}

	// The adapter serializes allocation, so two concurrent callers that both
	// miss resolve to the same short key.
	let short = self
		.shortids
		.allocate_state_key(event_type.to_string().as_str(), state_key)
		.expect("mtxdb short state key allocation");
	self.cache_state_key(cached_key, short);
	short
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

	let short = self
		.shortids
		.get_state_key(event_type.to_string().as_str(), state_key)
		.map_err(|e| short_error("state key lookup", e))?
		.ok_or_else(|| not_found("short state key"))?;
	self.cache_state_key(cached_key, short);
	Ok(short)
}

#[implement(Service)]
pub async fn get_eventid_from_short<Id>(&self, shorteventid: ShortEventId) -> Result<Id>
where
	Id: Deserialize + Sized + ToOwned,
	<Id as ToOwned>::Owned: Borrow<EventId>,
{
	if let Some(cached) = self.shorteventid_eventid_cache.get(&shorteventid) {
		let s = slipstream::codec::to_string(&cached);
		return slipstream::codec::from_str::<Id>(&s)
			.map_err(|e| err!(Database("Failed to deserialize EventId from cache: {e:?}")));
	}

	let bytes = self
		.shortids
		.get_event_id(shorteventid)
		.map_err(|e| short_error("event id lookup", e))?
		.ok_or_else(|| not_found("event id for short"))?;
	let res = Self::parse_event_id::<Id>(shorteventid, &bytes)?;

	let owned = res.to_owned();
	let event_id: &EventId = owned.borrow();
	self.cache_event(event_id, shorteventid);
	Ok(res)
}

#[implement(Service)]
pub fn multi_get_eventid_from_short<'a, Id, S>(
	&'a self,
	shorteventid: S,
) -> impl Stream<Item = Result<Id>> + Send + 'a
where
	S: Stream<Item = ShortEventId> + Send + 'a,
	Id: Deserialize + Sized + ToOwned + Send + 'a,
	<Id as ToOwned>::Owned: Borrow<EventId>,
{
	shorteventid
		.ready_chunks(256)
		.map(move |chunk| {
			let results: Vec<Result<Id>> = chunk
				.into_iter()
				.map(|short| {
					if let Some(cached) = self.shorteventid_eventid_cache.get(&short) {
						return slipstream::codec::from_str::<Id>(&slipstream::codec::to_string(
							&cached,
						))
						.map_err(|e| {
							err!(Database("Failed to deserialize EventId from cache: {e:?}"))
						});
					}

					let bytes = self
						.shortids
						.get_event_id(short)
						.map_err(|e| short_error("event id lookup", e))?
						.ok_or_else(|| not_found("event id for short"))?;
					let val = Self::parse_event_id::<Id>(short, &bytes)?;
					let owned = val.to_owned();
					let event_id: &EventId = owned.borrow();
					self.cache_event(event_id, short);
					Ok(val)
				})
				.collect();

			stream::iter(results)
		})
		.flatten()
}

#[implement(Service)]
pub async fn get_statekey_from_short(
	&self,
	shortstatekey: ShortStateKey,
) -> Result<(StateEventType, StateKey)> {
	if let Some(cached) = self.shortstatekey_statekey_cache.get(&shortstatekey) {
		return Ok(cached);
	}

	let (event_type, state_key) = self
		.shortids
		.get_state_key_from_short(shortstatekey)
		.map_err(|e| short_error("state key lookup", e))?
		.ok_or_else(|| {
			err!(Database(
				"Failed to find (StateEventType, state_key) from short {shortstatekey:?}"
			))
		})?;
	let res = (StateEventType::from(event_type.as_str()), StateKey::from(state_key));
	self.cache_state_key(res.clone(), shortstatekey);
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
		.map(move |chunk| {
			let results: Vec<Result<(StateEventType, StateKey)>> = chunk
				.into_iter()
				.map(|short| {
					if let Some(cached) = self.shortstatekey_statekey_cache.get(&short) {
						return Ok(cached);
					}

					let (event_type, state_key) = self
						.shortids
						.get_state_key_from_short(short)
						.map_err(|e| short_error("state key lookup", e))?
						.ok_or_else(|| {
							err!(Database(
								"Failed to find (StateEventType, state_key) from short {short:?}"
							))
						})?;
					let res =
						(StateEventType::from(event_type.as_str()), StateKey::from(state_key));
					self.cache_state_key(res.clone(), short);
					Ok(res)
				})
				.collect();

			stream::iter(results)
		})
		.flatten()
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
pub fn get_or_create_shortroomid_blocking(&self, room_id: &RoomId) -> ShortRoomId {
	if let Ok(handle) = self.db.roomid_shortroomid.get_blocking(room_id.as_bytes()) {
		utils::u64_from_u8(&handle)
	} else {
		const BUFSIZE: usize = size_of::<ShortRoomId>();

		let short = self.services.globals.next_count().unwrap();
		debug_assert!(size_of_val(&short) == BUFSIZE, "buffer requirement changed");

		self.db
			.roomid_shortroomid
			.raw_aput::<BUFSIZE, _, _>(room_id, short);

		short
	}
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
	use futures::stream::{self, StreamExt};
	use slipstream::{OwnedEventId, event_id, events::StateEventType};

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

		let config_raw = conduwuit::config::RawConfig::from_toml(&format!(
			r#"
				server_name = "test.conduwuit.local"
				database_path = "{}"
				"#,
			db_path.to_string_lossy().replace('\\', "/")
		))
		.expect("failed to parse test config");

		let config = Config::new(&config_raw).expect("failed to parse config");
		let runtime_handle = ();
		let server = Arc::new(Server::new(config, Some(&runtime_handle), Log {
			reload: LogLevelReloadHandles,
			capture: Arc::new(capture::State),
		}));

		let db = Database::open(&server).expect("failed to open database");
		let mtxdb_path = server.config.database_path.join("mtxdb-test");
		let mtxdb = Arc::new(
			conduwuit_mtxdb::store::MtxdbStore::open(mtxdb_path).expect("failed to open mtxdb"),
		);
		let service_map = Arc::new(conduwuit::SyncRwLock::new(BTreeMap::new()));

		let globals_service = globals::Service::build(crate::Args {
			db: &db,
			mtxdb: &mtxdb,
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
			mtxdb: &mtxdb,
			server: &server,
			service: &service_map,
		})
		.expect("failed to build short service");

		(guard, globals_service, short_service, service_map)
	}

	#[conduwuit_macros::async_test]
	async fn test_shorteventid_caching() {
		let _serial = DB_TEST_MUTEX.lock().await;
		let (_guard, _globals, service, _map) = setup_test_service().await;
		let event_id = event_id!("$abc:test.conduwuit.local");

		// Initial lookup should result in cache miss and query DB (or allocate new
		// since not in DB) Since it doesn't exist in DB, get_shorteventid returns
		// Err, but get_or_create resolves/creates it
		let short_id1 = service.get_or_create_shorteventid(&event_id).await;

		// Cache should now contain the mappings
		assert_eq!(service.eventid_shorteventid_cache.get(&event_id), Some(short_id1));
		assert_eq!(service.shorteventid_eventid_cache.get(&short_id1), Some(event_id.clone()));

		// Clear cache and retrieve via get_shorteventid to verify DB storage
		service.eventid_shorteventid_cache.invalidate_all();
		service.shorteventid_eventid_cache.invalidate_all();
		service.eventid_shorteventid_cache.run_pending_tasks();
		service.shorteventid_eventid_cache.run_pending_tasks();
		assert_eq!(service.eventid_shorteventid_cache.get(&event_id), None);

		let short_id2 = service.get_shorteventid(&event_id).await.unwrap();
		assert_eq!(short_id1, short_id2);

		// Cache should be repopulated after DB hit
		assert_eq!(service.eventid_shorteventid_cache.get(&event_id), Some(short_id1));

		// Test retrieve event_id from short_id
		let retrieved: OwnedEventId = service.get_eventid_from_short(short_id1).await.unwrap();
		assert_eq!(retrieved, event_id);
	}

	#[conduwuit_macros::async_test]
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

	#[conduwuit_macros::async_test]
	async fn test_multi_lookups() {
		let _serial = DB_TEST_MUTEX.lock().await;
		let (_guard, _globals, service, _map) = setup_test_service().await;

		let event1 = event_id!("$event1:test.conduwuit.local");
		let event2 = event_id!("$event2:test.conduwuit.local");

		// Multi create/get
		let stream = service.multi_get_or_create_shorteventid(vec![&event1, &event2].into_iter());
		let mut stream = std::pin::pin!(stream);
		let short1 = stream.next().await.unwrap();
		let short2 = stream.next().await.unwrap();
		assert!(stream.next().await.is_none());

		// Check caches
		assert_eq!(service.eventid_shorteventid_cache.get(&event1.clone()), Some(short1));
		assert_eq!(service.eventid_shorteventid_cache.get(&event2.clone()), Some(short2));

		// Retrieve batch from short ids
		let short_stream = stream::iter(vec![short1, short2]);
		let event_stream = service.multi_get_eventid_from_short::<OwnedEventId, _>(short_stream);
		let mut event_stream = std::pin::pin!(event_stream);
		assert_eq!(event_stream.next().await.unwrap().unwrap(), event1.clone());
		assert_eq!(event_stream.next().await.unwrap().unwrap(), event2.clone());
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

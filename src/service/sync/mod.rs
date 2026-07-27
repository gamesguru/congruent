mod watch;

use std::{
	collections::{BTreeMap, BTreeSet},
	mem::take,
	sync::Arc,
	time::Duration,
};

use conduwuit::{Result, Server, SyncMutex};
use database::Map;
use moka::sync::Cache;
use ruma::{
	OwnedDeviceId, OwnedRoomId, OwnedUserId,
	api::client::sync::sync_events::{
		self,
		v4::{ExtensionsConfig, SyncRequestList},
		v5::{self, request as v5_request},
	},
	directory::RoomTypeFilter,
	events::StateEventType,
	uint,
};
use serde::{Deserialize, Serialize};

use crate::{Dep, rooms};

/// Sticky (compat-only, non-MSC3575) list filters for MSC4186's simplified
/// sliding sync. Kept here so they can be cached alongside the rest of a
/// snake-cased connection's sticky parameters and survive follow-up requests
/// that omit `lists` entirely.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CompatListFilters {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub is_dm: Option<bool>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub is_encrypted: Option<bool>,

	#[serde(skip_serializing_if = "Option::is_none")]
	#[serde(alias = "is_invited")]
	pub is_invite: Option<bool>,

	#[serde(default, skip_serializing_if = "<[_]>::is_empty")]
	pub room_types: Vec<RoomTypeFilter>,

	#[serde(default, skip_serializing_if = "<[_]>::is_empty")]
	pub not_room_types: Vec<RoomTypeFilter>,

	#[serde(default, skip_serializing_if = "<[_]>::is_empty")]
	pub tags: Vec<String>,

	#[serde(default, skip_serializing_if = "<[_]>::is_empty")]
	pub not_tags: Vec<String>,

	#[serde(default, skip_serializing_if = "<[_]>::is_empty")]
	pub spaces: Vec<OwnedRoomId>,
}

impl From<&v5_request::ListFilters> for CompatListFilters {
	fn from(value: &v5_request::ListFilters) -> Self {
		Self {
			is_dm: None,
			is_encrypted: None,
			is_invite: value.is_invite,
			room_types: Vec::new(),
			not_room_types: value.not_room_types.clone(),
			tags: Vec::new(),
			not_tags: Vec::new(),
			spaces: Vec::new(),
		}
	}
}

/// Sticky (compat-only) `required_state.exclude` entries, keyed the same way
/// as [`CompatListFilters`].
#[derive(Clone, Debug, Default)]
pub struct CompatRequiredStateExcludes {
	pub lists: BTreeMap<String, Vec<(StateEventType, String)>>,
	pub room_subscriptions: BTreeMap<OwnedRoomId, Vec<(StateEventType, String)>>,
}

pub struct Service {
	db: Data,
	services: Services,
	connections: DbConnections<DbConnectionsKey, DbConnectionsVal>,
	snake_connections: DbConnections<SnakeConnectionsKey, SnakeConnectionsVal>,
}

pub struct Data {
	todeviceid_events: Arc<Map>,
	userroomid_joined: Arc<Map>,
	userroomid_invitestate: Arc<Map>,
	userroomid_leftstate: Arc<Map>,
	userroomid_notificationcount: Arc<Map>,
	userroomid_highlightcount: Arc<Map>,
	pduid_pdu: Arc<Map>,
	keychangeid_userid: Arc<Map>,
	roomusertype_roomuserdataid: Arc<Map>,
	readreceiptid_readreceipt: Arc<Map>,
	userid_lastonetimekeyupdate: Arc<Map>,
}

struct Services {
	server: Arc<Server>,
	short: Dep<rooms::short::Service>,
	state_cache: Dep<rooms::state_cache::Service>,
	typing: Dep<rooms::typing::Service>,
}

struct SlidingSyncCache {
	lists: BTreeMap<String, SyncRequestList>,
	subscriptions: BTreeMap<OwnedRoomId, sync_events::v4::RoomSubscription>,
	// For every room, the roomsince number
	known_rooms: BTreeMap<String, BTreeMap<OwnedRoomId, u64>>,
	extensions: ExtensionsConfig,
}

#[derive(Default)]
struct SnakeSyncCache {
	lists: BTreeMap<String, v5_request::List>,
	subscriptions: BTreeMap<OwnedRoomId, v5_request::RoomSubscription>,
	extensions: v5_request::Extensions,
	known_rooms: BTreeMap<String, BTreeMap<OwnedRoomId, u64>>,
	timeline_limits: BTreeMap<OwnedRoomId, usize>,
	last_pos: Option<u64>,
	compat_list_filters: BTreeMap<String, CompatListFilters>,
	compat_required_state_excludes: CompatRequiredStateExcludes,
}

type DbConnections<K, V> = Cache<K, V>;
type DbConnectionsKey = (OwnedUserId, OwnedDeviceId, String);
type DbConnectionsVal = Arc<SyncMutex<SlidingSyncCache>>;
type SnakeConnectionsKey = (OwnedUserId, OwnedDeviceId, Option<String>);
type SnakeConnectionsVal = Arc<SyncMutex<SnakeSyncCache>>;

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				todeviceid_events: args.db["todeviceid_events"].clone(),
				userroomid_joined: args.db["userroomid_joined"].clone(),
				userroomid_invitestate: args.db["userroomid_invitestate"].clone(),
				userroomid_leftstate: args.db["userroomid_leftstate"].clone(),
				userroomid_notificationcount: args.db["userroomid_notificationcount"].clone(),
				userroomid_highlightcount: args.db["userroomid_highlightcount"].clone(),
				pduid_pdu: args.db["pduid_pdu"].clone(),
				keychangeid_userid: args.db["keychangeid_userid"].clone(),
				roomusertype_roomuserdataid: args.db["roomusertype_roomuserdataid"].clone(),
				readreceiptid_readreceipt: args.db["readreceiptid_readreceipt"].clone(),
				userid_lastonetimekeyupdate: args.db["userid_lastonetimekeyupdate"].clone(),
			},
			services: Services {
				server: args.server.clone(),
				short: args.depend::<rooms::short::Service>("rooms::short"),
				state_cache: args.depend::<rooms::state_cache::Service>("rooms::state_cache"),
				typing: args.depend::<rooms::typing::Service>("rooms::typing"),
			},
			connections: Cache::builder()
				.time_to_idle(Duration::from_secs(12 * 60 * 60))
				.build(),
			snake_connections: Cache::builder()
				.time_to_idle(Duration::from_secs(12 * 60 * 60))
				.build(),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	pub fn snake_connection_cached(&self, key: &SnakeConnectionsKey) -> bool {
		self.snake_connections.contains_key(key)
	}

	pub fn snake_connection_token_valid(&self, key: &SnakeConnectionsKey, pos: u64) -> bool {
		self.snake_connections
			.get(key)
			.is_some_and(|cached| cached.lock().last_pos == Some(pos))
	}

	pub fn forget_snake_sync_connection(&self, key: &SnakeConnectionsKey) {
		self.snake_connections.invalidate(key);
	}

	pub fn remembered(&self, key: &DbConnectionsKey) -> bool {
		self.connections.contains_key(key)
	}

	pub fn forget_sync_request_connection(&self, key: &DbConnectionsKey) {
		self.connections.invalidate(key);
	}

	pub fn update_snake_sync_request_with_cache(
		&self,
		snake_key: &SnakeConnectionsKey,
		request: &mut v5::Request,
	) -> (BTreeMap<String, BTreeMap<OwnedRoomId, u64>>, BTreeMap<OwnedRoomId, usize>) {
		let cached_arc = self
			.snake_connections
			.get_with(snake_key.clone(), || Arc::new(SyncMutex::new(SnakeSyncCache::default())));
		let mut cached = cached_arc.lock();

		for (list_id, list) in &mut request.lists {
			if let Some(cached_list) = cached.lists.get(list_id) {
				list_or_sticky(&mut list.ranges, &cached_list.ranges);
				list_or_sticky(
					&mut list.room_details.required_state,
					&cached_list.room_details.required_state,
				);
				if list.room_details.timeline_limit == uint!(0) {
					list.room_details.timeline_limit = cached_list.room_details.timeline_limit;
				}
				match (&mut list.filters, cached_list.filters.clone()) {
					| (Some(filter), Some(cached_filter)) => {
						some_or_sticky(&mut filter.is_invite, cached_filter.is_invite);
						list_or_sticky(&mut filter.not_room_types, &cached_filter.not_room_types);
					},
					| (_, Some(cached_filters)) => list.filters = Some(cached_filters),
					| (Some(list_filters), _) => list.filters = Some(list_filters.clone()),
					| (..) => {},
				}
			}
			cached.lists.insert(list_id.clone(), list.clone());
		}
		request.lists.extend(cached.lists.clone());

		cached
			.subscriptions
			.extend(take(&mut request.room_subscriptions));
		request
			.room_subscriptions
			.extend(cached.subscriptions.clone());

		sticky_v5_extensions(&mut request.extensions, &cached.extensions);
		cached.extensions = request.extensions.clone();

		(cached.known_rooms.clone(), cached.timeline_limits.clone())
	}

	/// Sticky merge for the compat-only (non-MSC3575) list filters and
	/// required_state excludes: a list/subscription whose entry is absent
	/// from this request (whether because the list itself was omitted, or
	/// just its `filters`/exclude entry was) keeps whatever was cached from
	/// a previous request. An entry present in this request, even an empty
	/// one, always overrides the cache.
	pub fn update_snake_compat_sticky(
		&self,
		snake_key: &SnakeConnectionsKey,
		list_filters: &mut BTreeMap<String, CompatListFilters>,
		required_state_excludes: &mut CompatRequiredStateExcludes,
	) {
		let cached_arc = self
			.snake_connections
			.get_with(snake_key.clone(), || Arc::new(SyncMutex::new(SnakeSyncCache::default())));
		let mut cached = cached_arc.lock();

		for (list_id, cached_filters) in &cached.compat_list_filters {
			list_filters
				.entry(list_id.clone())
				.or_insert_with(|| cached_filters.clone());
		}
		cached.compat_list_filters = list_filters.clone();

		for (list_id, cached_excludes) in &cached.compat_required_state_excludes.lists {
			required_state_excludes
				.lists
				.entry(list_id.clone())
				.or_insert_with(|| cached_excludes.clone());
		}
		for (room_id, cached_excludes) in
			&cached.compat_required_state_excludes.room_subscriptions
		{
			required_state_excludes
				.room_subscriptions
				.entry(room_id.clone())
				.or_insert_with(|| cached_excludes.clone());
		}
		cached.compat_required_state_excludes = required_state_excludes.clone();
	}

	pub fn update_sync_request_with_cache(
		&self,
		key: &SnakeConnectionsKey,
		request: &mut sync_events::v4::Request,
	) -> BTreeMap<String, BTreeMap<OwnedRoomId, u64>> {
		let Some(conn_id) = request.conn_id.clone() else {
			return BTreeMap::new();
		};

		let key = into_db_key(key.0.clone(), key.1.clone(), conn_id);
		let cached_arc = self.connections.get_with(key, || {
			Arc::new(SyncMutex::new(SlidingSyncCache {
				lists: BTreeMap::new(),
				subscriptions: BTreeMap::new(),
				known_rooms: BTreeMap::new(),
				extensions: ExtensionsConfig::default(),
			}))
		});
		let mut cached = cached_arc.lock();

		for (list_id, list) in &mut request.lists {
			if let Some(cached_list) = cached.lists.get(list_id) {
				list_or_sticky(&mut list.sort, &cached_list.sort);
				list_or_sticky(
					&mut list.room_details.required_state,
					&cached_list.room_details.required_state,
				);
				some_or_sticky(
					&mut list.room_details.timeline_limit,
					cached_list.room_details.timeline_limit,
				);
				some_or_sticky(
					&mut list.include_old_rooms,
					cached_list.include_old_rooms.clone(),
				);
				match (&mut list.filters, cached_list.filters.clone()) {
					| (Some(filter), Some(cached_filter)) => {
						some_or_sticky(&mut filter.is_dm, cached_filter.is_dm);
						list_or_sticky(&mut filter.spaces, &cached_filter.spaces);
						some_or_sticky(&mut filter.is_encrypted, cached_filter.is_encrypted);
						some_or_sticky(&mut filter.is_invite, cached_filter.is_invite);
						list_or_sticky(&mut filter.room_types, &cached_filter.room_types);
						// Should be made possible to change
						list_or_sticky(&mut filter.not_room_types, &cached_filter.not_room_types);
						some_or_sticky(&mut filter.room_name_like, cached_filter.room_name_like);
						list_or_sticky(&mut filter.tags, &cached_filter.tags);
						list_or_sticky(&mut filter.not_tags, &cached_filter.not_tags);
					},
					| (_, Some(cached_filters)) => list.filters = Some(cached_filters),
					| (Some(list_filters), _) => list.filters = Some(list_filters.clone()),
					| (..) => {},
				}
				list_or_sticky(&mut list.bump_event_types, &cached_list.bump_event_types);
			}
			cached.lists.insert(list_id.clone(), list.clone());
		}

		cached
			.subscriptions
			.extend(request.room_subscriptions.clone());
		request
			.room_subscriptions
			.extend(cached.subscriptions.clone());

		request.extensions.e2ee.enabled = request
			.extensions
			.e2ee
			.enabled
			.or(cached.extensions.e2ee.enabled);

		request.extensions.to_device.enabled = request
			.extensions
			.to_device
			.enabled
			.or(cached.extensions.to_device.enabled);

		request.extensions.account_data.enabled = request
			.extensions
			.account_data
			.enabled
			.or(cached.extensions.account_data.enabled);
		request.extensions.account_data.lists = request
			.extensions
			.account_data
			.lists
			.clone()
			.or_else(|| cached.extensions.account_data.lists.clone());
		request.extensions.account_data.rooms = request
			.extensions
			.account_data
			.rooms
			.clone()
			.or_else(|| cached.extensions.account_data.rooms.clone());

		cached.extensions = request.extensions.clone();

		cached.known_rooms.clone()
	}

	pub fn update_sync_subscriptions(
		&self,
		key: &DbConnectionsKey,
		subscriptions: BTreeMap<OwnedRoomId, sync_events::v4::RoomSubscription>,
	) {
		let cached_arc = self.connections.get_with(key.clone(), || {
			Arc::new(SyncMutex::new(SlidingSyncCache {
				lists: BTreeMap::new(),
				subscriptions: BTreeMap::new(),
				known_rooms: BTreeMap::new(),
				extensions: ExtensionsConfig::default(),
			}))
		});
		let mut cached = cached_arc.lock();

		cached.subscriptions = subscriptions;
	}

	pub fn update_sync_known_rooms(
		&self,
		key: &DbConnectionsKey,
		list_id: String,
		new_cached_rooms: BTreeSet<OwnedRoomId>,
		globalsince: u64,
	) {
		let cached_arc = self.connections.get_with(key.clone(), || {
			Arc::new(SyncMutex::new(SlidingSyncCache {
				lists: BTreeMap::new(),
				subscriptions: BTreeMap::new(),
				known_rooms: BTreeMap::new(),
				extensions: ExtensionsConfig::default(),
			}))
		});
		let mut cached = cached_arc.lock();

		for (room_id, lastsince) in cached
			.known_rooms
			.entry(list_id.clone())
			.or_default()
			.iter_mut()
		{
			if !new_cached_rooms.contains(room_id) {
				*lastsince = 0;
			}
		}
		let list = cached.known_rooms.entry(list_id).or_default();
		for room_id in new_cached_rooms {
			list.insert(room_id, globalsince);
		}
	}

	pub fn update_snake_sync_known_rooms(
		&self,
		key: &SnakeConnectionsKey,
		list_id: String,
		new_cached_rooms: BTreeSet<OwnedRoomId>,
		globalsince: u64,
	) {
		let cached_arc = self
			.snake_connections
			.get_with(key.clone(), || Arc::new(SyncMutex::new(SnakeSyncCache::default())));
		let mut cached = cached_arc.lock();

		for (room_id, lastsince) in cached
			.known_rooms
			.entry(list_id.clone())
			.or_default()
			.iter_mut()
		{
			if !new_cached_rooms.contains(room_id) {
				*lastsince = 0;
			}
		}
		let list = cached.known_rooms.entry(list_id).or_default();
		for room_id in new_cached_rooms {
			list.insert(room_id, globalsince);
		}
	}

	pub fn update_snake_sync_pos(&self, key: &SnakeConnectionsKey, pos: u64) {
		let cached_arc = self
			.snake_connections
			.get_with(key.clone(), || Arc::new(SyncMutex::new(SnakeSyncCache::default())));
		cached_arc.lock().last_pos = Some(pos);
	}

	pub fn update_snake_sync_timeline_limits(
		&self,
		key: &SnakeConnectionsKey,
		limits: BTreeMap<OwnedRoomId, usize>,
	) {
		let cached_arc = self
			.snake_connections
			.get_with(key.clone(), || Arc::new(SyncMutex::new(SnakeSyncCache::default())));
		let mut cached = cached_arc.lock();
		for (room_id, limit) in limits {
			cached.timeline_limits.insert(room_id, limit);
		}
	}
}

#[inline]
pub fn into_snake_key<U, D, C>(user_id: U, device_id: D, conn_id: C) -> SnakeConnectionsKey
where
	U: Into<OwnedUserId>,
	D: Into<OwnedDeviceId>,
	C: Into<Option<String>>,
{
	(user_id.into(), device_id.into(), conn_id.into())
}

#[inline]
pub fn into_db_key<U, D, C>(user_id: U, device_id: D, conn_id: C) -> DbConnectionsKey
where
	U: Into<OwnedUserId>,
	D: Into<OwnedDeviceId>,
	C: Into<String>,
{
	(user_id.into(), device_id.into(), conn_id.into())
}

/// load params from cache if body doesn't contain it, as long as it's allowed
/// in some cases we may need to allow an empty list as an actual value
fn list_or_sticky<T: Clone>(target: &mut Vec<T>, cached: &Vec<T>) {
	if target.is_empty() {
		target.clone_from(cached);
	}
}

fn some_or_sticky<T>(target: &mut Option<T>, cached: Option<T>) {
	if target.is_none() {
		*target = cached;
	}
}

fn sticky_v5_extensions(target: &mut v5_request::Extensions, cache: &v5_request::Extensions) {
	target.e2ee.enabled = target.e2ee.enabled.or(cache.e2ee.enabled);

	target.to_device.enabled = target.to_device.enabled.or(cache.to_device.enabled);
	target.to_device.limit = target.to_device.limit.or(cache.to_device.limit);
	target.to_device.since = target
		.to_device
		.since
		.clone()
		.or_else(|| cache.to_device.since.clone());

	sticky_v5_room_extension(
		&mut target.account_data.enabled,
		&mut target.account_data.lists,
		&mut target.account_data.rooms,
		cache.account_data.enabled,
		cache.account_data.lists.as_ref(),
		cache.account_data.rooms.as_ref(),
	);
	sticky_v5_room_extension(
		&mut target.receipts.enabled,
		&mut target.receipts.lists,
		&mut target.receipts.rooms,
		cache.receipts.enabled,
		cache.receipts.lists.as_ref(),
		cache.receipts.rooms.as_ref(),
	);
	sticky_v5_room_extension(
		&mut target.typing.enabled,
		&mut target.typing.lists,
		&mut target.typing.rooms,
		cache.typing.enabled,
		cache.typing.lists.as_ref(),
		cache.typing.rooms.as_ref(),
	);
}

fn sticky_v5_room_extension<T: Clone>(
	target_enabled: &mut Option<bool>,
	target_lists: &mut Option<Vec<String>>,
	target_rooms: &mut Option<Vec<T>>,
	cache_enabled: Option<bool>,
	cache_lists: Option<&Vec<String>>,
	cache_rooms: Option<&Vec<T>>,
) {
	*target_enabled = target_enabled.or(cache_enabled);
	*target_lists = target_lists.clone().or_else(|| cache_lists.cloned());
	*target_rooms = target_rooms.clone().or_else(|| cache_rooms.cloned());
}

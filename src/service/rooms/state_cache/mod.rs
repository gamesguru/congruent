mod update;
mod via;

use std::{collections::HashMap, sync::Arc};

use conduwuit::{
	Pdu, Result, SyncRwLock, implement,
	result::LogErr,
	utils::{ReadyExt, stream::TryIgnore},
	warn,
};
use database::{Deserialized, Ignore, Interfix, Map};
use futures::{Stream, StreamExt, future::join5, pin_mut};
use moka::sync::Cache;
use ruma::{
	OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, ServerName, UserId,
	events::{AnyStrippedStateEvent, room::member::MembershipState},
	serde::Raw,
};

use crate::{Dep, account_data, appservice::RegistrationInfo, config, globals, rooms, users};

pub struct Service {
	appservice_in_room_cache: AppServiceInRoomCache,
	server_visibility_cache: Cache<(OwnedServerName, OwnedUserId), bool>,
	user_visibility_cache: Cache<(OwnedUserId, OwnedUserId), bool>,
	services: Services,
	db: Data,
	pub rooms_joining: SyncRwLock<std::collections::HashSet<OwnedRoomId>>,
}

struct Services {
	account_data: Dep<account_data::Service>,
	config: Dep<config::Service>,
	globals: Dep<globals::Service>,
	metadata: Dep<rooms::metadata::Service>,
	state: Dep<rooms::state::Service>,
	state_accessor: Dep<rooms::state_accessor::Service>,
	users: Dep<users::Service>,
}

struct Data {
	roomid_invitedcount: Arc<Map>,
	roomid_inviteviaservers: Arc<Map>,
	roomid_joinedcount: Arc<Map>,
	roomserverids: Arc<Map>,
	roomuserid_invitecount: Arc<Map>,
	roomuserid_joined: Arc<Map>,
	roomuserid_leftcount: Arc<Map>,
	roomuserid_forgotten: Arc<Map>,
	roomuserid_knockedcount: Arc<Map>,
	roomuseroncejoinedids: Arc<Map>,
	serverroomids: Arc<Map>,
	userroomid_invitestate: Arc<Map>,
	userroomid_joined: Arc<Map>,
	userroomid_leftstate: Arc<Map>,
	userroomid_knockedstate: Arc<Map>,
	userroomid_invitesender: Arc<Map>,
}

type AppServiceInRoomCache = SyncRwLock<HashMap<OwnedRoomId, HashMap<String, bool>>>;
type StrippedStateEventItem = (OwnedRoomId, Vec<Raw<AnyStrippedStateEvent>>);

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			appservice_in_room_cache: SyncRwLock::new(HashMap::new()),
			rooms_joining: SyncRwLock::new(std::collections::HashSet::new()),
			// Ugly way to build the cache with a dynamic capacity
			server_visibility_cache: Cache::builder()
				.max_capacity(
					conduwuit::utils::math::usize_from_f64(
						25_000.0
							* args
								.depend::<config::Service>("config")
								.cache_capacity_modifier,
					)
					.unwrap_or(25_000)
					.try_into()
					.unwrap_or(25_000),
				)
				.time_to_live(std::time::Duration::from_mins(5))
				.build(),
			user_visibility_cache: Cache::builder()
				.max_capacity(
					conduwuit::utils::math::usize_from_f64(
						25_000.0
							* args
								.depend::<config::Service>("config")
								.cache_capacity_modifier,
					)
					.unwrap_or(25_000)
					.try_into()
					.unwrap_or(25_000),
				)
				.time_to_live(std::time::Duration::from_mins(5))
				.build(),
			services: Services {
				account_data: args.depend::<account_data::Service>("account_data"),
				config: args.depend::<config::Service>("config"),
				globals: args.depend::<globals::Service>("globals"),
				metadata: args.depend::<rooms::metadata::Service>("rooms::metadata"),
				state: args.depend::<rooms::state::Service>("rooms::state"),
				state_accessor: args
					.depend::<rooms::state_accessor::Service>("rooms::state_accessor"),
				users: args.depend::<users::Service>("users"),
			},
			db: Data {
				roomid_invitedcount: args.db["roomid_invitedcount"].clone(),
				roomid_inviteviaservers: args.db["roomid_inviteviaservers"].clone(),
				roomid_joinedcount: args.db["roomid_joinedcount"].clone(),
				roomserverids: args.db["roomserverids"].clone(),
				roomuserid_invitecount: args.db["roomuserid_invitecount"].clone(),
				roomuserid_joined: args.db["roomuserid_joined"].clone(),
				roomuserid_leftcount: args.db["roomuserid_leftcount"].clone(),
				roomuserid_forgotten: args.db["roomuserid_forgotten"].clone(),
				roomuserid_knockedcount: args.db["roomuserid_knockedcount"].clone(),
				roomuseroncejoinedids: args.db["roomuseroncejoinedids"].clone(),
				serverroomids: args.db["serverroomids"].clone(),
				userroomid_invitestate: args.db["userroomid_invitestate"].clone(),
				userroomid_joined: args.db["userroomid_joined"].clone(),
				userroomid_leftstate: args.db["userroomid_leftstate"].clone(),
				userroomid_knockedstate: args.db["userroomid_knockedstate"].clone(),
				userroomid_invitesender: args.db["userroomid_invitesender"].clone(),
			},
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

#[implement(Service)]
#[tracing::instrument(level = "trace", skip_all)]
pub async fn appservice_in_room(&self, room_id: &RoomId, appservice: &RegistrationInfo) -> bool {
	if let Some(cached) = self
		.appservice_in_room_cache
		.read()
		.get(room_id)
		.and_then(|map| map.get(&appservice.registration.id))
		.copied()
	{
		return cached;
	}

	let bridge_user_id = UserId::parse_with_server_name(
		appservice.registration.sender_localpart.as_str(),
		self.services.globals.server_name(),
	);

	let Ok(bridge_user_id) = bridge_user_id.log_err() else {
		return false;
	};

	let in_room = self.is_joined(&bridge_user_id, room_id).await
		|| self
			.room_members(room_id)
			.ready_any(|user_id| appservice.users.is_match(user_id.as_str()))
			.await;

	self.appservice_in_room_cache
		.write()
		.entry(room_id.into())
		.or_default()
		.insert(appservice.registration.id.clone(), in_room);

	in_room
}

#[implement(Service)]
pub fn get_appservice_in_room_cache_usage(&self) -> (usize, usize) {
	let cache = self.appservice_in_room_cache.read();

	(cache.len(), cache.capacity())
}

#[implement(Service)]
#[tracing::instrument(level = "debug", skip_all)]
pub fn clear_appservice_in_room_cache(&self) { self.appservice_in_room_cache.write().clear(); }

/// Returns an iterator of all servers participating in this room.
#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn room_servers<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = &'a ServerName> + Send + 'a {
	let prefix = (room_id, Interfix);
	self.db
		.roomserverids
		.keys_prefix(&prefix)
		.ignore_err()
		.map(|(_, server): (Ignore, &ServerName)| server)
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn server_in_room<'a>(&'a self, server: &'a ServerName, room_id: &'a RoomId) -> bool {
	let key = (server, room_id);
	self.db.serverroomids.qry(&key).await.is_ok()
}

#[implement(Service)]
pub fn is_joining(&self, room_id: &RoomId) -> bool { self.rooms_joining.read().contains(room_id) }

/// Returns true if the server is participating in the room (joined, invited, or
/// knocked).
#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn server_is_participant<'a>(
	&'a self,
	server: &'a ServerName,
	room_id: &'a RoomId,
) -> bool {
	if self.server_in_room(server, room_id).await {
		return true;
	}

	self.room_members_invited(room_id)
		.chain(self.room_members_knocked(room_id))
		.ready_any(|user| user.server_name() == server)
		.await
}

/// Returns an iterator of all rooms a server participates in (as far as we
/// know).
#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn server_rooms<'a>(
	&'a self,
	server: &'a ServerName,
) -> impl Stream<Item = &'a RoomId> + Send + 'a {
	let prefix = (server, Interfix);
	self.db
		.serverroomids
		.keys_prefix(&prefix)
		.ignore_err()
		.map(|(_, room_id): (Ignore, &RoomId)| room_id)
		.ready_filter(|room_id| <&RoomId>::try_from(room_id.as_str()).is_ok())
}

/// Expose raw keys for the clean_corrupt_rooms command
#[implement(Service)]
pub fn server_rooms_raw_keys_prefix<'a>(
	&'a self,
	prefix: &'a (&'a ServerName, Interfix),
) -> impl Stream<Item = Result<database::keyval::Key<'a>>> + Send + 'a {
	self.db.serverroomids.keys_prefix_raw(prefix)
}

/// Remove a malformed room from the server_rooms index using raw bytes.
#[implement(Service)]
pub fn server_rooms_remove_raw(&self, key: &[u8]) -> Result<()> {
	self.db.serverroomids.remove_raw(key);
	Ok(())
}

/// Returns true if a server can see a user by having any active membership
/// (joined, invited, or knocked) in at least one shared room.
#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn server_sees_user(&self, server: &ServerName, user_id: &UserId) -> bool {
	let key = (server.to_owned(), user_id.to_owned());
	if let Some(sees) = self.server_visibility_cache.get(&key) {
		return sees;
	}

	let sees = self
		.server_rooms(server)
		.any(|room_id| async move {
			self.is_invited_or_joined(user_id, room_id).await
				|| self.is_knocked(user_id, room_id).await
		})
		.await;

	self.server_visibility_cache.insert(key, sees);
	sees
}

/// Returns true if user_a and user_b share at least one room.
#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn user_sees_user(&self, user_a: &UserId, user_b: &UserId) -> bool {
	if user_a == user_b {
		return true;
	}

	let key = if user_a < user_b {
		(user_a.to_owned(), user_b.to_owned())
	} else {
		(user_b.to_owned(), user_a.to_owned())
	};

	if let Some(sees) = self.user_visibility_cache.get(&key) {
		return sees;
	}

	let get_shared_rooms = self.get_shared_rooms(user_a, user_b);

	pin_mut!(get_shared_rooms);
	let sees = get_shared_rooms.next().await.is_some();

	self.user_visibility_cache.insert(key, sees);
	sees
}

/// List the rooms common between two users
#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn get_shared_rooms<'a>(
	&'a self,
	user_a: &'a UserId,
	user_b: &'a UserId,
) -> impl Stream<Item = &'a RoomId> + Send + 'a {
	use conduwuit::utils::set;

	let a = self.rooms_joined(user_a);
	let b = self.rooms_joined(user_b);
	set::intersection_sorted_stream2(a, b)
}

/// Returns an iterator of all joined members of a room.
#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn room_members<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = &'a UserId> + Send + 'a {
	let prefix = (room_id, Interfix);
	self.db
		.roomuserid_joined
		.keys_prefix(&prefix)
		.ignore_err()
		.map(|(_, user_id): (Ignore, &UserId)| user_id)
}

/// Invalidate user visibility cache for all users in the room.
#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn invalidate_user_visibility(&self, user_id: &UserId, room_id: &RoomId) {
	self.room_members(room_id)
		.ready_for_each(|other_user| {
			let key = if user_id < other_user {
				(user_id.to_owned(), other_user.to_owned())
			} else {
				(other_user.to_owned(), user_id.to_owned())
			};
			self.user_visibility_cache.invalidate(&key);
		})
		.await;
}

/// Invalidate server visibility cache for the user and all servers in the room.
#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn invalidate_server_visibility(&self, user_id: &UserId, room_id: &RoomId) {
	self.room_servers(room_id)
		.ready_for_each(|server| {
			self.server_visibility_cache
				.invalidate(&(server.to_owned(), user_id.to_owned()));
		})
		.await;
}

/// Returns the number of users which are currently in a room
#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn room_joined_count(&self, room_id: &RoomId) -> Result<u64> {
	self.db.roomid_joinedcount.get(room_id).await.deserialized()
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
/// Returns an iterator of all our local users in the room, even if they're
/// deactivated/guests
pub fn local_users_in_room<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = &'a UserId> + Send + 'a {
	self.room_members(room_id)
		.ready_filter(|user| self.services.globals.user_is_local(user))
}

/// Returns an iterator of all our local joined users in a room who are
/// active (not deactivated, not guest)
#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub fn active_local_users_in_room<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = &'a UserId> + Send + 'a {
	self.local_users_in_room(room_id)
		.filter(|user| self.services.users.is_active(user))
}

/// Returns the number of users which are currently invited to a room
#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn room_invited_count(&self, room_id: &RoomId) -> Result<u64> {
	self.db
		.roomid_invitedcount
		.get(room_id)
		.await
		.deserialized()
}

/// Returns an iterator over all User IDs who ever joined a room.
#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn room_useroncejoined<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = &'a UserId> + Send + 'a {
	let prefix = (room_id, Interfix);
	self.db
		.roomuseroncejoinedids
		.keys_prefix(&prefix)
		.ignore_err()
		.map(|(_, user_id): (Ignore, &UserId)| user_id)
}

/// Returns an iterator over all invited members of a room.
#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn room_members_invited<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = &'a UserId> + Send + 'a {
	let prefix = (room_id, Interfix);
	self.db
		.roomuserid_invitecount
		.keys_prefix(&prefix)
		.ignore_err()
		.map(|(_, user_id): (Ignore, &UserId)| user_id)
}

/// Returns an iterator over all knocked members of a room.
#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn room_members_knocked<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = &'a UserId> + Send + 'a {
	let prefix = (room_id, Interfix);
	self.db
		.roomuserid_knockedcount
		.keys_prefix(&prefix)
		.ignore_err()
		.map(|(_, user_id): (Ignore, &UserId)| user_id)
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn get_invite_count(&self, room_id: &RoomId, user_id: &UserId) -> Result<u64> {
	let key = (room_id, user_id);
	self.db
		.roomuserid_invitecount
		.qry(&key)
		.await
		.deserialized()
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn get_knock_count(&self, room_id: &RoomId, user_id: &UserId) -> Result<u64> {
	let key = (room_id, user_id);
	self.db
		.roomuserid_knockedcount
		.qry(&key)
		.await
		.deserialized()
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn get_left_count(&self, room_id: &RoomId, user_id: &UserId) -> Result<u64> {
	let key = (room_id, user_id);
	self.db.roomuserid_leftcount.qry(&key).await.deserialized()
}

/// Returns an iterator over all rooms this user joined.
#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn rooms_joined<'a>(
	&'a self,
	user_id: &'a UserId,
) -> impl Stream<Item = &'a RoomId> + Send + 'a {
	let prefix = (user_id, Interfix);
	self.db
		.userroomid_joined
		.keys_prefix(&prefix)
		.ignore_err()
		.map(|(_, room_id): (Ignore, &RoomId)| room_id)
}

/// Returns an iterator over all rooms a user was invited to.
#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn rooms_invited<'a>(
	&'a self,
	user_id: &'a UserId,
) -> impl Stream<Item = StrippedStateEventItem> + Send + 'a {
	type KeyVal<'a> = (Key<'a>, &'a [u8]);
	type Key<'a> = (&'a UserId, &'a RoomId);

	let prefix = (user_id, Interfix);
	self.db
		.userroomid_invitestate
		.stream_prefix(&prefix)
		.ignore_err()
		.map(|((_, room_id), state): KeyVal<'_>| (room_id.to_owned(), state))
		.map(|(room_id, state)| Ok((room_id, serde_json::from_slice(state)?)))
		.ignore_err()
}

/// Returns an iterator over all rooms a user is currently knocking.
#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn rooms_knocked<'a>(
	&'a self,
	user_id: &'a UserId,
) -> impl Stream<Item = StrippedStateEventItem> + Send + 'a {
	// TODO: other places we depend on shorteventid but might not update it?
	type KeyVal<'a> = (Key<'a>, &'a [u8]);
	type Key<'a> = (&'a UserId, &'a RoomId);

	let prefix = (user_id, Interfix);
	self.db
		.userroomid_knockedstate
		.stream_prefix(&prefix)
		.ignore_err()
		.map(|((_, room_id), state): KeyVal<'_>| (room_id.to_owned(), state))
		.map(|(room_id, state)| Ok((room_id, serde_json::from_slice(state)?)))
		.inspect(|res| {
			if let Err(e) = res {
				conduwuit::warn!("rooms_knocked deserialize error: {e}");
			}
		})
		.ignore_err()
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn invite_state(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
) -> Result<Vec<Raw<AnyStrippedStateEvent>>> {
	let key = (user_id, room_id);
	self.db
		.userroomid_invitestate
		.qry(&key)
		.await
		.and_then(|handle| serde_json::from_slice(&handle).map_err(Into::into))
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn knock_state(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
) -> Result<Vec<Raw<AnyStrippedStateEvent>>> {
	let key = (user_id, room_id);
	self.db
		.userroomid_knockedstate
		.qry(&key)
		.await
		.and_then(|handle| serde_json::from_slice(&handle).map_err(Into::into))
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn left_state(&self, user_id: &UserId, room_id: &RoomId) -> Result<Option<Pdu>> {
	let key = (user_id, room_id);
	self.db
		.userroomid_leftstate
		.qry(&key)
		.await
		.and_then(|handle| serde_json::from_slice(&handle).map_err(Into::into))
}

/// Returns an iterator over all rooms a user left.
#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn rooms_left<'a>(
	&'a self,
	user_id: &'a UserId,
) -> impl Stream<Item = (OwnedRoomId, Option<Pdu>)> + Send + 'a {
	type KeyVal<'a> = (Key<'a>, Raw<Option<Pdu>>);
	type Key<'a> = (&'a UserId, &'a RoomId);

	let prefix = (user_id, Interfix);
	self.db
		.userroomid_leftstate
		.stream_prefix(&prefix)
		.ignore_err()
		.map(|((_, room_id), state): KeyVal<'_>| (room_id.to_owned(), state))
		.map(|(room_id, state)| Ok((room_id, state.deserialize()?)))
		.ignore_err()
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn user_membership(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
) -> Option<MembershipState> {
	let states = join5(
		self.is_joined(user_id, room_id),
		self.is_left(user_id, room_id),
		self.is_knocked(user_id, room_id),
		self.is_invited(user_id, room_id),
		self.once_joined(user_id, room_id),
	)
	.await;

	match states {
		| (true, ..) => Some(MembershipState::Join),
		| (_, true, ..) => {
			if let Ok(Some(pdu)) = self.left_state(user_id, room_id).await {
				if let Ok(content) = serde_json::from_str::<serde_json::Value>(pdu.content.get())
				{
					if content.get("membership").and_then(|m| m.as_str()) == Some("ban") {
						return Some(MembershipState::Ban);
					}
				}
			}
			Some(MembershipState::Leave)
		},
		| (_, _, true, ..) => Some(MembershipState::Knock),
		| (_, _, _, true, ..) => Some(MembershipState::Invite),
		| (false, false, false, false, true) => Some(MembershipState::Ban),
		| _ => None,
	}
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn once_joined(&self, user_id: &UserId, room_id: &RoomId) -> bool {
	let key = (user_id, room_id);
	self.db.roomuseroncejoinedids.qry(&key).await.is_ok()
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn is_joined<'a>(&'a self, user_id: &'a UserId, room_id: &'a RoomId) -> bool {
	let key = (user_id, room_id);
	self.db.userroomid_joined.qry(&key).await.is_ok()
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn is_knocked<'a>(&'a self, user_id: &'a UserId, room_id: &'a RoomId) -> bool {
	let key = (user_id, room_id);
	self.db.userroomid_knockedstate.qry(&key).await.is_ok()
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn is_invited(&self, user_id: &UserId, room_id: &RoomId) -> bool {
	let key = (user_id, room_id);
	self.db.userroomid_invitestate.qry(&key).await.is_ok()
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn is_invited_or_joined(&self, user_id: &UserId, room_id: &RoomId) -> bool {
	self.is_joined(user_id, room_id).await || self.is_invited(user_id, room_id).await
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn is_knocked_or_joined(&self, user_id: &UserId, room_id: &RoomId) -> bool {
	self.is_joined(user_id, room_id).await || self.is_knocked(user_id, room_id).await
}
#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn is_left(&self, user_id: &UserId, room_id: &RoomId) -> bool {
	let key = (user_id, room_id);
	self.db.userroomid_leftstate.qry(&key).await.is_ok()
}

/// Whether `user_id` has called `/forget` on `room_id`. Independent of
/// `is_left`/`get_left_count`: forgetting no longer deletes those records
/// (see `forget()`), since /sync needs the real leave position to still
/// surface the leave to other devices via `include_leave`, even after one
/// device forgets the room.
#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn is_forgotten(&self, user_id: &UserId, room_id: &RoomId) -> bool {
	let key = (room_id, user_id);
	self.db.roomuserid_forgotten.qry(&key).await.is_ok()
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn can_access_history(&self, user_id: &UserId, room_id: &RoomId) -> bool {
	if self.is_joined(user_id, room_id).await {
		return true;
	}

	self.is_left(user_id, room_id).await && !self.is_forgotten(user_id, room_id).await
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "trace")]
pub async fn invite_sender(&self, user_id: &UserId, room_id: &RoomId) -> Result<OwnedUserId> {
	let key = (user_id, room_id);
	self.db
		.userroomid_invitesender
		.qry(&key)
		.await
		.deserialized()
}
#[cfg(test)]
mod serde_test3 {
	use ruma::events::room::member::RoomMemberEventContent;
	#[test]
	fn test_serde() {
		let s = r#"{"displayname":"user-2 🏳️‍⚧️","membership":"join"}"#;
		match serde_json::from_str::<RoomMemberEventContent>(s) {
			| Ok(c) => println!("Success: {:?}", c.membership),
			| Err(e) => panic!("Error: {}", e),
		}
	}
}

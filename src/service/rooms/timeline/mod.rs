mod append;
mod backfill;
mod build;
mod create;
mod data;
mod helpers;
mod redact;

use std::{fmt::Write, mem::size_of, sync::Arc};

use async_trait::async_trait;
pub use conduwuit_core::matrix::pdu::{PduId, RawPduId};
use conduwuit_core::{
	Result, Server, SyncMutex, at, err,
	matrix::{
		event::Event,
		pdu::{PduCount, PduEvent},
	},
	utils::{
		MutexMap, MutexMapGuard, bytes, future::TryExtExt, math::usize_from_f64,
		stream::TryIgnore,
	},
	warn,
};
use futures::{Future, Stream, TryStreamExt, pin_mut};
use lru_cache::LruCache;
use ruma::{
	CanonicalJsonObject, EventId, OwnedEventId, OwnedRoomId, RoomId, api::Direction,
	events::room::encrypted::Relation,
};
use serde::Deserialize;

use self::data::Data;
pub use self::{create::pdu_fits, data::PdusIterItem};
use crate::{
	Dep, account_data, admin, appservice, config, globals, pusher, rooms,
	rooms::short::{ShortEventId, ShortRoomId, ShortStateHash},
	sending, server_keys, users,
};

// Update Relationships
#[derive(Deserialize)]
struct ExtractRelatesTo {
	#[serde(rename = "m.relates_to")]
	relates_to: Relation,
}

#[derive(Clone, Debug, Deserialize)]
struct ExtractEventId {
	event_id: OwnedEventId,
}
#[derive(Clone, Debug, Deserialize)]
struct ExtractRelatesToEventId {
	#[serde(rename = "m.relates_to")]
	relates_to: ExtractEventId,
}

#[derive(Deserialize)]
struct ExtractBody {
	body: Option<String>,
}

pub struct Service {
	services: Services,
	db: Data,
	pub mutex_insert: RoomMutexMap,
	pub next_shortstatehash_cache: SyncMutex<LruCache<(ShortRoomId, PduCount), ShortStateHash>>,
	pub prev_shortstatehash_cache: SyncMutex<LruCache<(ShortRoomId, PduCount), ShortStateHash>>,
}

struct Services {
	server: Arc<Server>,
	account_data: Dep<account_data::Service>,
	appservice: Dep<appservice::Service>,
	admin: Dep<admin::Service>,
	alias: Dep<rooms::alias::Service>,
	config: Dep<config::Service>,
	directory: Dep<rooms::directory::Service>,
	globals: Dep<globals::Service>,
	short: Dep<rooms::short::Service>,
	state: Dep<rooms::state::Service>,
	state_cache: Dep<rooms::state_cache::Service>,
	state_accessor: Dep<rooms::state_accessor::Service>,
	pdu_metadata: Dep<rooms::pdu_metadata::Service>,
	read_receipt: Dep<rooms::read_receipt::Service>,
	sending: Dep<sending::Service>,
	server_keys: Dep<server_keys::Service>,
	user: Dep<rooms::user::Service>,
	users: Dep<users::Service>,
	pusher: Dep<pusher::Service>,
	threads: Dep<rooms::threads::Service>,
	search: Dep<rooms::search::Service>,
	spaces: Dep<rooms::spaces::Service>,
	event_handler: Dep<rooms::event_handler::Service>,
}

type RoomMutexMap = MutexMap<OwnedRoomId, ()>;
pub type RoomMutexGuard = MutexMapGuard<OwnedRoomId, ()>;

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let config = &args.server.config;
		let cache_capacity =
			f64::from(config.shortstatehash_cache_capacity) * config.cache_capacity_modifier;
		let cache_capacity = usize_from_f64(cache_capacity)?;
		let per_cache = usize::max(1, cache_capacity / 2);

		Ok(Arc::new(Self {
			next_shortstatehash_cache: SyncMutex::new(LruCache::new(per_cache)),
			prev_shortstatehash_cache: SyncMutex::new(LruCache::new(per_cache)),
			services: Services {
				server: args.server.clone(),
				account_data: args.depend::<account_data::Service>("account_data"),
				appservice: args.depend::<appservice::Service>("appservice"),
				admin: args.depend::<admin::Service>("admin"),
				alias: args.depend::<rooms::alias::Service>("rooms::alias"),
				config: args.depend::<config::Service>("config"),
				directory: args.depend::<rooms::directory::Service>("rooms::directory"),
				globals: args.depend::<globals::Service>("globals"),
				short: args.depend::<rooms::short::Service>("rooms::short"),
				state: args.depend::<rooms::state::Service>("rooms::state"),
				state_cache: args.depend::<rooms::state_cache::Service>("rooms::state_cache"),
				state_accessor: args
					.depend::<rooms::state_accessor::Service>("rooms::state_accessor"),
				pdu_metadata: args.depend::<rooms::pdu_metadata::Service>("rooms::pdu_metadata"),
				read_receipt: args.depend::<rooms::read_receipt::Service>("rooms::read_receipt"),
				sending: args.depend::<sending::Service>("sending"),
				server_keys: args.depend::<server_keys::Service>("server_keys"),
				user: args.depend::<rooms::user::Service>("rooms::user"),
				users: args.depend::<users::Service>("users"),
				pusher: args.depend::<pusher::Service>("pusher"),
				threads: args.depend::<rooms::threads::Service>("rooms::threads"),
				search: args.depend::<rooms::search::Service>("rooms::search"),
				spaces: args.depend::<rooms::spaces::Service>("rooms::spaces"),
				event_handler: args
					.depend::<rooms::event_handler::Service>("rooms::event_handler"),
			},
			db: Data::new(&args),
			mutex_insert: RoomMutexMap::new(),
		}))
	}

	async fn memory_usage(&self, out: &mut (dyn Write + Send)) -> Result {
		let next_cache_len = self.next_shortstatehash_cache.lock().len();
		let next_cache_bytes = next_cache_len.saturating_mul(
			size_of::<(ShortRoomId, PduCount)>().saturating_add(size_of::<ShortStateHash>()),
		);
		let next_bytes = bytes::pretty(next_cache_bytes);
		writeln!(out, "next_shortstatehash_cache: {next_cache_len} ({next_bytes})")?;

		let prev_cache_len = self.prev_shortstatehash_cache.lock().len();
		let prev_cache_bytes = prev_cache_len.saturating_mul(
			size_of::<(ShortRoomId, PduCount)>().saturating_add(size_of::<ShortStateHash>()),
		);
		let prev_bytes = bytes::pretty(prev_cache_bytes);
		writeln!(out, "prev_shortstatehash_cache: {prev_cache_len} ({prev_bytes})")?;

		let mutex_insert = self.mutex_insert.len();
		writeln!(out, "insert_mutex: {mutex_insert}")?;

		Ok(())
	}

	async fn clear_cache(&self) {
		self.next_shortstatehash_cache.lock().clear();
		self.prev_shortstatehash_cache.lock().clear();
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn first_pdu_in_room(&self, room_id: &RoomId) -> Result<impl Event> {
		self.first_item_in_room(room_id).await.map(at!(1))
	}

	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn first_item_in_room(&self, room_id: &RoomId) -> Result<(PduCount, impl Event)> {
		let pdus = self.pdus(room_id, None);

		pin_mut!(pdus);
		pdus.try_next()
			.await?
			.ok_or_else(|| err!(Request(NotFound("No PDU found in room"))))
	}

	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn latest_pdu_in_room(&self, room_id: &RoomId) -> Result<impl Event> {
		self.db.latest_pdu_in_room(room_id).await
	}

	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn last_timeline_count(&self, room_id: &RoomId) -> Result<PduCount> {
		self.db.last_timeline_count(room_id).await
	}

	/// Returns the `count` of this pdu's id.
	pub async fn get_pdu_count(&self, event_id: &EventId) -> Result<PduCount> {
		self.db.get_pdu_count(event_id).await
	}

	/// Returns the json of a pdu.
	pub async fn get_pdu_json(&self, event_id: &EventId) -> Result<CanonicalJsonObject> {
		self.db.get_pdu_json(event_id).await
	}

	/// Returns the json of a pdu.
	#[inline]
	pub async fn get_non_outlier_pdu_json(
		&self,
		event_id: &EventId,
	) -> Result<CanonicalJsonObject> {
		self.db.get_non_outlier_pdu_json(event_id).await
	}

	/// Returns the pdu's id.
	#[inline]
	pub async fn get_pdu_id(&self, event_id: &EventId) -> Result<RawPduId> {
		self.db.get_pdu_id(event_id).await
	}

	/// Returns the pdu.
	#[inline]
	pub async fn get_non_outlier_pdu(&self, event_id: &EventId) -> Result<PduEvent> {
		self.db.get_non_outlier_pdu_in_room(None, event_id).await
	}

	/// Returns the pdu, populating room_id.
	#[inline]
	pub async fn get_non_outlier_pdu_in_room(
		&self,
		room_id: Option<&RoomId>,
		event_id: &EventId,
	) -> Result<PduEvent> {
		self.db.get_non_outlier_pdu_in_room(room_id, event_id).await
	}

	/// Returns the pdu.
	///
	/// Checks the `eventid_outlierpdu` Tree if not found in the timeline.
	#[inline]
	pub async fn get_pdu(&self, event_id: &EventId) -> Result<PduEvent> {
		self.db.get_pdu_in_room(None, event_id).await
	}

	/// Returns the pdu, populating room_id.
	///
	/// Checks the `eventid_outlierpdu` Tree if not found in the timeline.
	#[inline]
	pub async fn get_pdu_in_room(
		&self,
		room_id: Option<&RoomId>,
		event_id: &EventId,
	) -> Result<PduEvent> {
		self.db.get_pdu_in_room(room_id, event_id).await
	}

	/// Returns the pdu.
	///
	/// This does __NOT__ check the outliers `Tree`.
	#[inline]
	pub async fn get_pdu_from_id(&self, pdu_id: &RawPduId) -> Result<PduEvent> {
		self.db.get_pdu_from_id_in_room(None, pdu_id).await
	}

	/// Returns the pdu, populating room_id.
	///
	/// This does __NOT__ check the outliers `Tree`.
	#[inline]
	pub async fn get_pdu_from_id_in_room(
		&self,
		room_id: Option<&RoomId>,
		pdu_id: &RawPduId,
	) -> Result<PduEvent> {
		self.db.get_pdu_from_id_in_room(room_id, pdu_id).await
	}

	/// Returns the pdu as a `BTreeMap<String, CanonicalJsonValue>`.
	#[inline]
	pub async fn get_pdu_json_from_id(&self, pdu_id: &RawPduId) -> Result<CanonicalJsonObject> {
		self.db.get_pdu_json_from_id(pdu_id).await
	}

	/// Returns the shortstatehash of the room at the event directly preceding
	/// the exclusive `before` param. `before` does not have to be a valid
	/// count or in the room.
	///
	/// Note: Caching is restricted to `PduCount::Normal` to prevent stale cache
	/// entries from being served if historical PDUs with backfilled counts are
	/// subsequently inserted.
	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn prev_shortstatehash(
		&self,
		room_id: &RoomId,
		before: PduCount,
	) -> Result<ShortStateHash> {
		let shortroomid: ShortRoomId = self
			.services
			.short
			.get_shortroomid(room_id)
			.await
			.map_err(|e| err!(Request(NotFound("Room {room_id:?} not found: {e:?}"))))?;

		if let Some(hash) = self
			.prev_shortstatehash_cache
			.lock()
			.get_mut(&(shortroomid, before))
		{
			return Ok(*hash);
		}

		let before_pdu = PduId { shortroomid, shorteventid: before };

		let prev_count = self.db.prev_timeline_count(&before_pdu).await?;
		let prev_pdu = PduId { shortroomid, shorteventid: prev_count };

		let shorteventid = self.get_shorteventid_from_pdu_id(&prev_pdu).await?;

		let result = self.services.state.get_shortstatehash(shorteventid).await;

		if let Ok(hash) = result {
			if matches!(before, PduCount::Normal(_))
				&& self.db.pduid_exists(&before_pdu.into()).await
			{
				self.prev_shortstatehash_cache
					.lock()
					.insert((shortroomid, before), hash);
			}
		}

		result
	}

	/// Returns the shortstatehash of the room at the event directly following
	/// the exclusive `after` param. `after` does not have to be a valid count
	/// or in the room.
	///
	/// Note: Caching is restricted to `PduCount::Normal` to prevent stale cache
	/// entries from being served if historical PDUs with backfilled counts are
	/// subsequently inserted.
	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn next_shortstatehash(
		&self,
		room_id: &RoomId,
		after: PduCount,
	) -> Result<ShortStateHash> {
		let shortroomid: ShortRoomId = self
			.services
			.short
			.get_shortroomid(room_id)
			.await
			.map_err(|e| err!(Request(NotFound("Room {room_id:?} not found: {e:?}"))))?;

		if let Some(hash) = self
			.next_shortstatehash_cache
			.lock()
			.get_mut(&(shortroomid, after))
		{
			return Ok(*hash);
		}

		let after_pdu = PduId { shortroomid, shorteventid: after };

		let next_count = self.db.next_timeline_count(&after_pdu).await?;
		let next_pdu = PduId { shortroomid, shorteventid: next_count };

		let shorteventid = self.get_shorteventid_from_pdu_id(&next_pdu).await?;

		let result = self.services.state.get_shortstatehash(shorteventid).await;

		if let Ok(hash) = result {
			if matches!(after, PduCount::Normal(_))
				&& self.db.pduid_exists(&after_pdu.into()).await
			{
				self.next_shortstatehash_cache
					.lock()
					.insert((shortroomid, after), hash);
			}
		}

		result
	}

	/// Returns the shortstatehash of the room at the event
	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn get_shortstatehash(
		&self,
		room_id: &RoomId,
		count: PduCount,
	) -> Result<ShortStateHash> {
		let shortroomid: ShortRoomId = self
			.services
			.short
			.get_shortroomid(room_id)
			.await
			.map_err(|e| err!(Request(NotFound("Room {room_id:?} not found: {e:?}"))))?;

		let pdu_id = PduId { shortroomid, shorteventid: count };

		let shorteventid = self.get_shorteventid_from_pdu_id(&pdu_id).await?;

		self.services.state.get_shortstatehash(shorteventid).await
	}

	/// Returns the `shorteventid` from the `pdu_id`
	pub async fn get_shorteventid_from_pdu_id(&self, pdu_id: &PduId) -> Result<ShortEventId> {
		let event_id = self.get_event_id_from_pdu_id(pdu_id).await?;

		self.services.short.get_shorteventid(&event_id).await
	}

	/// Returns the `event_id` from the `pdu_id`
	pub async fn get_event_id_from_pdu_id(&self, pdu_id: &PduId) -> Result<OwnedEventId> {
		let pdu_id: RawPduId = (*pdu_id).into();

		self.db.get_event_id_from_pdu_id(&pdu_id).await
	}

	/// Checks if pdu exists
	///
	/// Checks the `eventid_outlierpdu` Tree if not found in the timeline.
	#[inline]
	pub fn pdu_exists<'a>(
		&'a self,
		event_id: &'a EventId,
	) -> impl Future<Output = bool> + Send + 'a {
		self.db.pdu_exists(event_id).is_ok()
	}

	/// Removes a pdu and creates a new one with the same id.
	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn replace_pdu(&self, pdu_id: &RawPduId, pdu_json: &CanonicalJsonObject) -> Result {
		self.db.replace_pdu(pdu_id, pdu_json).await
	}

	/// Returns an iterator over all PDUs in a room. Unknown rooms produce no
	/// items.
	#[inline]
	pub fn all_pdus<'a>(
		&'a self,
		room_id: &'a RoomId,
	) -> impl Stream<Item = PdusIterItem> + Send + 'a {
		self.pdus(room_id, None).ignore_err()
	}

	/// Reverse iteration starting after `until`.
	#[tracing::instrument(skip(self), level = "debug")]
	pub fn pdus_rev<'a>(
		&'a self,
		room_id: &'a RoomId,
		until: Option<PduCount>,
	) -> impl Stream<Item = Result<PdusIterItem>> + Send + 'a {
		self.db
			.pdus_rev(room_id, until.unwrap_or_else(PduCount::max))
	}

	/// Forward iteration starting after `from`.
	#[tracing::instrument(skip(self), level = "debug")]
	pub fn pdus<'a>(
		&'a self,
		room_id: &'a RoomId,
		from: Option<PduCount>,
	) -> impl Stream<Item = Result<PdusIterItem>> + Send + 'a {
		self.db.pdus(room_id, from.unwrap_or_else(PduCount::min))
	}

	/// Returns a stream of PDUs starting from the nearest event to the given
	/// timestamp, walking in the given direction.
	#[tracing::instrument(skip(self), level = "debug")]
	pub fn pdus_by_timestamp<'a>(
		&'a self,
		room_id: &'a RoomId,
		timestamp: u64,
		dir: Direction,
	) -> impl Stream<Item = Result<PduEvent>> + Send + 'a {
		self.db.pdus_by_timestamp(room_id, timestamp, dir)
	}

	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn backfill_timestamp_index(&self, room_id: &RoomId) -> Result {
		self.db.backfill_timestamp_index(room_id).await
	}
}

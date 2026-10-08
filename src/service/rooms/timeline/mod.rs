mod append;
mod backfill;
pub use backfill::PromoteOutlierOutcome;
mod backward_extremities;
mod build;
mod create;
mod dag_audit;
mod data;
pub mod extremities;
mod helpers;
mod metadata;
mod notifications;
mod promotion_claims;
mod reachability;
mod rebuild_state;
mod redact;
pub mod reindex;
mod reorder;
mod repair_unsigned;

use std::{fmt::Write, ops::Bound, sync::Arc};

pub use append::AppendPduContext;
use async_trait::async_trait;
pub use conduwuit_core::matrix::pdu::{PduId, RawPduId, ShortRoomId, TopoToken};
/// Proof that the caller already holds `Service::mutex_insert` for a room.
/// Threaded through the state-transition path so the outlier demotion step
/// can skip re-acquiring the same non-reentrant per-room lock when called
/// from inside `append_pdu` (which holds it for the whole insert), while
/// still self-locking when called from anywhere else.
pub type InsertMutexGuard = MutexMapGuard<OwnedRoomId, ()>;
use conduwuit_core::{
	Result, Server, at, err, info,
	matrix::{
		event::Event,
		pdu::{PduCount, PduEvent},
	},
	utils::{MutexMap, MutexMapGuard, future::TryExtExt, stream::TryIgnore},
};
use futures::{Future, Stream, StreamExt, TryStreamExt, pin_mut};
use slipstream::{
	CanonicalJsonObject, EventId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId,
	codec::{DeError, Deserialize as CodecDeserialize, Serialize as CodecSerialize},
	endpoint::body_field,
	events::{GlobalAccountDataEventType, push_rules::PushRulesEvent, room::encrypted::Relation},
	json::Value,
};

// Update Relationships
struct ExtractRelatesTo {
	// JSON key: `m.relates_to`
	relates_to: Relation,
}

#[derive(Clone, Debug)]
struct ExtractEventId {
	event_id: OwnedEventId,
}
#[derive(Clone, Debug)]
struct ExtractRelatesToEventId {
	// JSON key: `m.relates_to`
	relates_to: ExtractEventId,
}

struct ExtractBody {
	body: Option<String>,
}

/// MSC2836 threading: `content.m.relationship = { rel_type, event_id }`
/// pointing at this event's parent. Distinct from `m.relates_to` above.
#[derive(Debug)]
pub(crate) struct Msc2836Relationship {
	pub(crate) rel_type: String,
	pub(crate) event_id: OwnedEventId,
}

#[derive(Debug)]
pub(crate) struct ExtractMsc2836Relationship {
	// JSON key: `m.relationship`
	relationship: Option<Msc2836Relationship>,
}

impl CodecDeserialize for ExtractRelatesTo {
	fn from_json(value: &Value) -> Result<Self, DeError> {
		Ok(Self {
			relates_to: body_field(Some(value), "m.relates_to")?,
		})
	}
}

impl CodecDeserialize for ExtractEventId {
	fn from_json(value: &Value) -> Result<Self, DeError> {
		Ok(Self {
			event_id: body_field(Some(value), "event_id")?,
		})
	}
}

impl CodecDeserialize for ExtractRelatesToEventId {
	fn from_json(value: &Value) -> Result<Self, DeError> {
		Ok(Self {
			relates_to: body_field(Some(value), "m.relates_to")?,
		})
	}
}

impl CodecDeserialize for ExtractBody {
	fn from_json(value: &Value) -> Result<Self, DeError> {
		Ok(Self { body: body_field(Some(value), "body")? })
	}
}

impl CodecDeserialize for Msc2836Relationship {
	fn from_json(value: &Value) -> Result<Self, DeError> {
		Ok(Self {
			rel_type: body_field(Some(value), "rel_type")?,
			event_id: body_field(Some(value), "event_id")?,
		})
	}
}

impl CodecDeserialize for ExtractMsc2836Relationship {
	fn from_json(value: &Value) -> Result<Self, DeError> {
		Ok(Self {
			relationship: body_field(Some(value), "m.relationship")?,
		})
	}
}

use self::data::Data;
pub use self::{
	create::pdu_fits,
	data::{PdusIterItem, TopoIterItem},
	metadata::EventMetadata,
	promotion_claims::{PromotionClaims, PromotionDisposition},
	reachability::LiveReachability,
	repair_unsigned::update_unsigned_prev_content,
};
use crate::{
	Dep, account_data, admin, appservice, globals, pusher, rooms, rooms::short::ShortEventId,
	sending, server_keys, users,
};

pub struct Service {
	services: Services,
	db: Data,
	pub mutex_insert: RoomMutexMap,
	pub mutex_fetch: MutexMap<OwnedEventId, ()>,
	/// Singleflights `backfill_if_required`'s gap scan per room. Without
	/// this, N concurrent backward `/messages` calls on the same room each
	/// run their own full scan-and-decide pass; the eventual inserts are
	/// already safe (see `backfill_pdu`'s `mutex_federation` lock + TOCTOU
	/// recheck), but the redundant scans and duplicate `/backfill` requests
	/// are pure waste. See
	/// `docs/development-gg/fable/boundary-flake-advisory.md` §3.
	pub mutex_backfill: RoomMutexMap,
	/// Tier 2 of the backfill scan perf work (see
	/// `docs/development-gg/room-issues.csv`): remembers the exact
	/// `(state_hash, from, limit)` of the most recent gap-free
	/// `backfill_if_required` scan per room. A repeat request for that
	/// *exact* `from` with a `limit` no larger than what was already
	/// verified skips the scan entirely, but only if the room's current
	/// short-state-hash still matches the one observed during the scan.
	///
	/// This is intentionally narrower than a "gap-free below count X"
	/// boundary cache: `backfill_if_required` verifies gap-freedom only
	/// within the fixed-size window it scans (anchored at `from`), not
	/// "everything below `from`", so a boundary-style cache would need to
	/// track ranges, not a single count, to stay correct. The exact-tuple
	/// form sidesteps that while still allowing the cache to survive
	/// unrelated writes: the room state hash is the invalidation token.
	pub backfill_gap_free_cache: moka::sync::Cache<OwnedRoomId, (u64, TopoToken, usize)>,
	/// Short-lived suppression for repeated unresolved backfill windows.
	/// If the same room/window/gap signature comes back unchanged after a
	/// failed federation attempt, re-scanning and re-requesting it again is
	/// pure CPU/network waste. This is intentionally short TTL so a transient
	/// remote failure can still be retried shortly after.
	pub backfill_gap_repeat_cache: moka::sync::Cache<(OwnedRoomId, u64), ()>,
	pub last_timeline_count_cache: moka::sync::Cache<OwnedRoomId, PduCount>,
	/// Arbiter between outlier promotions (see
	/// [`Self::promote_outlier_batch`]) and event rejections racing for the
	/// same event ID -- see [`PromotionClaims`]'s doc comment (and its
	/// module's unit tests) for the concurrency invariant this enforces and
	/// why it has to be a single shared claim map rather than two separate
	/// stores each checked-then-acted-on independently.
	pub pending_promotions: PromotionClaims,
}

struct Services {
	server: Arc<Server>,
	account_data: Dep<account_data::Service>,
	appservice: Dep<appservice::Service>,
	admin: Dep<admin::Service>,
	alias: Dep<rooms::alias::Service>,
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
	state_hamt: Dep<rooms::state_hamt::Service>,
	event_handler: Dep<rooms::event_handler::Service>,
	outlier: Dep<rooms::outlier::Service>,
	auth_chain: Dep<rooms::auth_chain::Service>,
}

type RoomMutexMap = MutexMap<OwnedRoomId, ()>;
pub type RoomMutexGuard = MutexMapGuard<OwnedRoomId, ()>;

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			last_timeline_count_cache: moka::sync::Cache::builder()
				.max_capacity(100_000)
				.time_to_idle(std::time::Duration::from_mins(10))
				.build(),
			backfill_gap_free_cache: moka::sync::Cache::builder()
				.max_capacity(100_000)
				.time_to_idle(std::time::Duration::from_mins(10))
				.build(),
			backfill_gap_repeat_cache: moka::sync::Cache::builder()
				.max_capacity(100_000)
				.time_to_live(std::time::Duration::from_secs(15))
				.build(),
			services: Services {
				server: args.server.clone(),
				account_data: args.depend::<account_data::Service>("account_data"),
				appservice: args.depend::<appservice::Service>("appservice"),
				admin: args.depend::<admin::Service>("admin"),
				alias: args.depend::<rooms::alias::Service>("rooms::alias"),
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
				outlier: args.depend::<rooms::outlier::Service>("rooms::outlier"),
				state_hamt: args.depend::<rooms::state_hamt::Service>("rooms::state_hamt"),
				event_handler: args
					.depend::<rooms::event_handler::Service>("rooms::event_handler"),
				auth_chain: args.depend::<rooms::auth_chain::Service>("rooms::auth_chain"),
			},
			db: Data::new(&args),
			mutex_insert: RoomMutexMap::new(),
			mutex_fetch: MutexMap::new(),
			mutex_backfill: RoomMutexMap::new(),
			pending_promotions: PromotionClaims::new(),
		}))
	}

	async fn memory_usage(&self, out: &mut (dyn Write + Send)) -> Result {
		let mutex_insert = self.mutex_insert.len();
		writeln!(out, "insert_mutex: {mutex_insert}")?;
		let mutex_fetch = self.mutex_fetch.len();
		writeln!(out, "fetch_mutex: {mutex_fetch}")?;

		Ok(())
	}

	async fn clear_cache(&self) {}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	#[inline]
	fn backfill_gap_free_cache_hit(
		cached: Option<(u64, TopoToken, usize)>,
		current_statehash: u64,
		from: TopoToken,
		scan_limit: usize,
	) -> bool {
		current_statehash != 0
			&& cached.is_some_and(|(verified_statehash, verified_from, verified_limit)| {
				verified_statehash == current_statehash
					&& verified_from == from
					&& verified_limit >= scan_limit
			})
	}

	/// Index a PDU's body for full-text search if it's a RoomMessage.
	/// Encapsulates the pattern duplicated across append, backfill, and heal.
	pub(super) fn index_pdu_search(
		&self,
		shortroomid: ShortRoomId,
		pdu_id: &RawPduId,
		pdu: &PduEvent,
	) {
		use slipstream::events::TimelineEventType;
		if pdu.kind == TimelineEventType::RoomMessage {
			if let Ok(content) = pdu.get_content::<ExtractBody>() {
				if let Some(body) = &content.body {
					self.services.search.index_pdu(shortroomid, pdu_id, body);
				}
			}
		}
	}

	pub fn db_batch(&self) -> database::Batch<'_> { self.db.db_batch() }

	pub fn db_apply_batch(&self, batch: database::Batch<'_>) { self.db.db_apply_batch(batch); }

	pub async fn first_pdu_in_room(&self, room_id: &RoomId) -> Result<impl Event> {
		self.first_item_in_room(room_id).await.map(at!(1))
	}

	pub async fn first_item_in_room(&self, room_id: &RoomId) -> Result<(TopoToken, impl Event)> {
		let pdus = self.topo_pdus(room_id, None);

		pin_mut!(pdus);
		pdus.try_next()
			.await?
			.ok_or_else(|| err!(Request(NotFound("No PDU found in room"))))
	}

	pub async fn latest_pdu_in_room(&self, room_id: &RoomId) -> Result<impl Event> {
		self.db.latest_pdu_in_room(room_id).await
	}

	pub async fn last_timeline_count(&self, room_id: &RoomId) -> Result<PduCount> {
		if let Some(count) = self.last_timeline_count_cache.get(&room_id.to_owned()) {
			info!(
				target: "watermark_debug",
				%room_id, ?count,
				"last_timeline_count: cache hit"
			);
			return Ok(count);
		}
		let count = self.db.last_timeline_count(room_id).await?;
		info!(
			target: "watermark_debug",
			%room_id, ?count,
			"last_timeline_count: cache miss, read from DB"
		);
		self.last_timeline_count_cache
			.insert(room_id.to_owned(), count);
		Ok(count)
	}

	/// Returns the `shorteventid` from the `pdu_id`
	pub async fn get_shorteventid_from_pdu_id(&self, pdu_id: &PduId) -> Result<ShortEventId> {
		let event_id = self.get_event_id_from_pdu_id(pdu_id).await?;

		self.services.short.get_shorteventid(&event_id).await
	}

	/// Returns the `event_id` from the `pdu_id`
	pub async fn get_event_id_from_pdu_id(&self, pdu_id: &PduId) -> Result<OwnedEventId> {
		let pdu_id: RawPduId = (*pdu_id).into();

		self.get_pdu_from_id(&pdu_id).await.map(|pdu| pdu.event_id)
	}

	/// Returns the `count` of this pdu's id.
	pub async fn get_pdu_count(&self, event_id: &EventId) -> Result<PduCount> {
		self.db.get_pdu_count(event_id).await
	}

	pub async fn outlier_pdu_exists(&self, event_id: &EventId) -> Result<()> {
		self.db.outlier_pdu_exists(event_id).await
	}

	/// Returns the EventMetadata for a PDU.
	pub async fn get_event_metadata(&self, event_id: &EventId) -> Result<EventMetadata> {
		self.db.get_event_metadata(event_id).await
	}

	/// Returns the json of a pdu.
	pub async fn get_pdu_json(&self, event_id: &EventId) -> Result<CanonicalJsonObject> {
		self.db.get_pdu_json(event_id).await
	}

	#[inline]
	pub async fn get_outlier_pdu_json(&self, event_id: &EventId) -> Result<CanonicalJsonObject> {
		self.db.get_outlier_pdu_json(event_id).await
	}

	#[inline]
	pub async fn remove_from_timeline(&self, event_id: &EventId) {
		self.db.remove_from_timeline(event_id).await;
	}

	/// Strips only the event's timeline pointers plus its now-stale
	/// `is_outlier: false` metadata, leaving the PDU JSON itself intact.
	/// Callers demoting a timeline event to an outlier must hold
	/// `mutex_insert` for the room across this call and the subsequent
	/// `add_pdu_outlier_locked` call, so no concurrent writer can observe
	/// the event with neither timeline pointers nor outlier metadata.
	#[inline]
	pub async fn remove_timeline_pointers(&self, event_id: &EventId) {
		self.db.remove_timeline_pointers(event_id).await;
	}

	#[inline]
	pub async fn remove_timeline_pointers_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		event_id: &EventId,
	) {
		self.db
			.remove_timeline_pointers_batch(batch, event_id)
			.await;
	}

	#[inline]
	pub fn apply_batch(&self, batch: database::Batch<'_>) { self.db.apply_batch(batch); }

	#[inline]
	pub async fn drop_duplicate_pdu(&self, pdu_id: &RawPduId) {
		self.db.drop_duplicate_pdu(pdu_id);
	}

	#[inline]
	pub async fn reindex_timeline(&self, room_id: &RoomId) -> Result<usize> {
		self.db.reindex_timeline(room_id).await
	}
}

/// Copy room push rules from an upgraded room to its replacement.
///
/// This is used both for local room upgrades and for tombstones received
/// over federation so the replacement room inherits the same per-room
/// notification rules on every homeserver that knows about the room.
pub async fn copy_room_push_rules_for_upgrade(
	service: &Service,
	room_id: &RoomId,
	replacement_room: &RoomId,
) -> Result {
	let local_users = service
		.services
		.users
		.list_local_users()
		.map(|user_id: OwnedUserId| user_id)
		.collect::<Vec<_>>()
		.await;

	for user_id in local_users {
		let _push_rules_lock = service
			.services
			.account_data
			.push_rules_lock(&user_id)
			.await;
		let Ok(mut push_rules): Result<PushRulesEvent> = service
			.services
			.account_data
			.get_global(&user_id, GlobalAccountDataEventType::PushRules)
			.await
		else {
			continue;
		};

		let Some(mut rule) = push_rules
			.content
			.global
			.room
			.iter()
			.find(|rule| rule.rule_id == room_id)
			.cloned()
		else {
			continue;
		};

		rule.rule_id = replacement_room.to_owned();
		push_rules.content.global.room.insert(rule);

		service
			.services
			.account_data
			.update(
				None,
				&user_id,
				GlobalAccountDataEventType::PushRules.to_string().into(),
				&push_rules.to_json(),
			)
			.await?;
	}

	Ok(())
}

impl Service {
	#[inline]
	pub fn pdus_by_timestamp<'a>(
		&'a self,
		room_id: &'a RoomId,
		timestamp: u64,
		dir: slipstream::api::Direction,
	) -> impl Stream<Item = Result<PduEvent>> + Send + 'a {
		self.db.pdus_by_timestamp(room_id, timestamp, dir)
	}

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

	#[inline]
	pub async fn get_pdu_outlier(&self, event_id: &EventId) -> Result<PduEvent> {
		self.services.outlier.get_pdu_outlier(event_id).await
	}

	#[inline]
	pub fn clear_outlier_flag(&self, event_id: &EventId) {
		self.services.outlier.clear_outlier_flag(event_id);
	}

	#[inline]
	pub async fn add_pdu_outlier(
		&self,
		event_id: &EventId,
		pdu: &CanonicalJsonObject,
		room_id: Option<&RoomId>,
	) {
		self.services
			.outlier
			.add_pdu_outlier(event_id, pdu, room_id)
			.await;
	}

	#[inline]
	pub fn add_pdu_outlier_locked(
		&self,
		event_id: &EventId,
		pdu: &CanonicalJsonObject,
		room_id: Option<&RoomId>,
		insert_lock: &InsertMutexGuard,
	) {
		self.services
			.outlier
			.add_pdu_outlier_locked(event_id, pdu, room_id, insert_lock);
	}

	#[inline]
	pub async fn remove_outlier(&self, event_id: &EventId) {
		self.services.outlier.remove_outlier(event_id).await;
	}

	#[inline]
	pub fn room_outlier_stream<'a>(
		&'a self,
		room_id: &'a RoomId,
	) -> impl Stream<Item = (OwnedEventId, PduEvent)> + Send + 'a {
		self.services.outlier.room_stream(room_id)
	}

	/// Checks if pdu exists directly in the timeline (non-outlier).
	#[inline]
	pub async fn non_outlier_pdu_exists(&self, event_id: &EventId) -> bool {
		self.db.non_outlier_pdu_exists(event_id).await.is_ok()
	}

	/// Fetch multiple PDUs in parallel from the database.
	pub fn multi_get_pdus<'a, S>(
		&'a self,
		room_id: Option<&'a RoomId>,
		event_ids: S,
	) -> impl Stream<Item = Result<PduEvent>> + Send + 'a
	where
		S: Stream<Item = OwnedEventId> + Send + 'a,
	{
		self.db.multi_get_pdus(room_id, event_ids)
	}

	/// Checks if all PDUs exist directly in the timeline (non-outlier).
	#[inline]
	pub async fn non_outlier_pdus_exist<'a, I>(&self, event_ids: I) -> bool
	where
		I: Iterator<Item = &'a EventId> + Send,
	{
		for event_id in event_ids {
			if !self.non_outlier_pdu_exists(event_id).await {
				return false;
			}
		}
		true
	}

	/// Returns the pdu.
	///
	/// Checks the `eventid_outlierpdu` Tree if not found in the timeline.
	#[inline]
	pub fn get_pdu(
		&self,
		event_id: &EventId,
	) -> impl Future<Output = Result<PduEvent>> + Send + 'static {
		let db = self.db.clone();
		let event_id = event_id.to_owned();
		async move { db.get_pdu_in_room(None, &event_id).await }
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

	/// Returns the HAMT root handle of the room at the event directly following
	/// the exclusive `after` param. `after` does not have to be a valid count
	/// or in the room.
	///
	/// Note: deliberately uncached for now.
	pub async fn next_root_handle(
		&self,
		room_id: &RoomId,
		after: PduCount,
	) -> Result<rezzy::hamt::RootHandle> {
		let shortroomid: ShortRoomId = self
			.services
			.short
			.get_shortroomid(room_id)
			.await
			.map_err(|e| err!(Request(NotFound("Room {room_id:?} not found: {e:?}"))))?;

		let after_pdu = PduId { shortroomid, shorteventid: after };

		let next_count = self.db.next_timeline_count(&after_pdu).await?;
		let next_pdu = PduId { shortroomid, shorteventid: next_count };

		let shorteventid = self.get_shorteventid_from_pdu_id(&next_pdu).await?;

		self.services.state.get_roothandle(shorteventid).await
	}

	/// Returns the HAMT root handle of the room at the event directly preceding
	/// the exclusive `before` param. `before` does not have to be a valid count
	/// or in the room.
	///
	/// Note: deliberately uncached for now.
	pub async fn prev_root_handle(
		&self,
		room_id: &RoomId,
		before: PduCount,
	) -> Result<rezzy::hamt::RootHandle> {
		let shortroomid: ShortRoomId = self
			.services
			.short
			.get_shortroomid(room_id)
			.await
			.map_err(|e| err!(Request(NotFound("Room {room_id:?} not found: {e:?}"))))?;

		let before_pdu = PduId { shortroomid, shorteventid: before };

		let prev_count = self.db.prev_timeline_count(&before_pdu).await?;
		let prev_pdu = PduId { shortroomid, shorteventid: prev_count };

		let shorteventid = self.get_shorteventid_from_pdu_id(&prev_pdu).await?;

		self.services.state.get_roothandle(shorteventid).await
	}

	/// Returns the HAMT root handle of the room at the given event count.
	pub async fn get_root_handle(
		&self,
		room_id: &RoomId,
		count: PduCount,
	) -> Result<rezzy::hamt::RootHandle> {
		let shortroomid: ShortRoomId = self
			.services
			.short
			.get_shortroomid(room_id)
			.await
			.map_err(|e| err!(Request(NotFound("Room {room_id:?} not found: {e:?}"))))?;

		let pdu_id = PduId { shortroomid, shorteventid: count };

		let shorteventid = self.get_shorteventid_from_pdu_id(&pdu_id).await?;

		self.services.state.get_roothandle(shorteventid).await
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
	pub async fn replace_pdu(
		&self,
		pdu_id: &RawPduId,
		pdu_json: &CanonicalJsonObject,
		event_id: &EventId,
	) -> Result {
		self.db.replace_pdu(pdu_id, pdu_json, event_id).await
	}

	/// Returns an iterator over all PDUs in a room. Unknown rooms produce no
	/// items.
	#[inline]
	pub fn all_pdus<'a>(
		&'a self,
		room_id: &'a RoomId,
	) -> impl Stream<Item = PdusIterItem> + Send + 'a {
		self.pdus(room_id, Bound::Unbounded).ignore_err()
	}

	/// Reverse iteration over PDUs bounded by `until`.
	///
	/// `until` states its own inclusivity — `Bound::Excluded(count)` to stop
	/// before `count`, `Bound::Included(count)` to yield `count` first, or
	/// `Bound::Unbounded` to start from the newest event in the room. There
	/// is deliberately no `Option<PduCount>` overload: that shape is what let
	/// two different call sites independently forget which way to adjust the
	/// boundary (see `docs/development-gg/fable/boundary-flake-advisory.md`).
	pub fn pdus_rev<'a>(
		&'a self,
		room_id: &'a RoomId,
		until: Bound<PduCount>,
	) -> impl Stream<Item = Result<PdusIterItem>> + Send + 'a {
		self.db.pdus_rev(room_id, until)
	}

	pub fn topo_pdus_rev<'a>(
		&'a self,
		room_id: &'a RoomId,
		until: Option<TopoToken>,
	) -> impl Stream<Item = Result<TopoIterItem>> + Send + 'a {
		self.db
			.topo_pdus_rev(room_id, until.unwrap_or_else(TopoToken::max))
	}

	pub async fn fix_pdu_event_ids(&self) -> Result<usize> { self.db.fix_pdu_event_ids().await }

	/// Forward iteration over PDUs bounded by `from`.
	///
	/// `from` states its own inclusivity — see `pdus_rev`'s doc comment.
	/// Note the adjustment `pdus` applies internally for `Bound::Excluded` is
	/// the opposite sign from `pdus_rev`'s; that asymmetry is exactly why
	/// callers should never hand-roll it (see the doc comment on
	/// `Data::pdus` in `data.rs`).
	pub fn pdus<'a>(
		&'a self,
		room_id: &'a RoomId,
		from: Bound<PduCount>,
	) -> impl Stream<Item = Result<PdusIterItem>> + Send + 'a {
		self.db.pdus(room_id, from)
	}

	/// Forward iteration using topological ordering, starting after `from`.
	pub fn topo_pdus<'a>(
		&'a self,
		room_id: &'a RoomId,
		from: Option<TopoToken>,
	) -> impl Stream<Item = Result<TopoIterItem>> + Send + 'a {
		self.db
			.topo_pdus(room_id, from.unwrap_or_else(TopoToken::min))
	}

	/// Coalesce a group of timeline writes into one flush boundary.
	///
	/// Used by federation intake so a room transaction's prev-event repairs
	/// and the incoming event itself become visible together to `/sync`.
	pub async fn with_cork_and_flush<R, F, Fut>(&self, f: F) -> R
	where
		F: FnOnce() -> Fut,
		Fut: Future<Output = R>,
	{
		let _cork = self.db.db.cork_and_flush();
		f().await
	}

	/// Coalesce a group of timeline writes without forcing a flush when `f`
	/// completes. Unlike `with_cork_and_flush`, callers are expected to
	/// either be nested inside an outer flush boundary or not need one
	/// (e.g. batching outlier persistence ahead of a later
	/// `with_cork_and_flush`) -- use this when per-write flushing, not
	/// durability, is the problem being solved.
	pub async fn with_cork<R, F, Fut>(&self, f: F) -> R
	where
		F: FnOnce() -> Fut,
		Fut: Future<Output = R>,
	{
		let _cork = self.db.db.cork();
		f().await
	}

	/// Briefly lift an enclosing `with_cork_and_flush` boundary around
	/// `f`, so remote I/O run in the middle of a corked write phase (e.g.
	/// federation fetches performed while resolving a prev-event's missing
	/// state/auth events) doesn't suppress unrelated WAL flushes across the
	/// whole server for the duration. The outer cork is restored once `f`
	/// completes. Harmless to call when no cork is currently held.
	pub async fn without_cork<R, F, Fut>(&self, f: F) -> R
	where
		F: FnOnce() -> Fut,
		Fut: Future<Output = R>,
	{
		let _uncork = self.db.db.uncork_briefly();
		f().await
	}
}

impl Service {
	pub fn multi_get_shortprevevents<'a, I>(
		&'a self,
		shorteventids: I,
	) -> impl Stream<Item = Result<Vec<ShortEventId>>> + Send + 'a
	where
		I: Stream<Item = ShortEventId> + Send + 'a,
	{
		self.db.multi_get_shortprevevents(shorteventids)
	}

	pub fn multi_get_shortauthevents<'a, I>(
		&'a self,
		shorteventids: I,
	) -> impl Stream<Item = Result<Vec<ShortEventId>>> + Send + 'a
	where
		I: Stream<Item = ShortEventId> + Send + 'a,
	{
		self.db.multi_get_shortauthevents(shorteventids)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn backfill_gap_cache_requires_matching_state_hash() {
		let from = TopoToken {
			depth: 42,
			pdu_count: PduCount::Normal(42),
		};
		let cached = Some((7, from, 500));

		assert!(Service::backfill_gap_free_cache_hit(cached, 7, from, 100));
		assert!(!Service::backfill_gap_free_cache_hit(cached, 8, from, 100));
	}

	#[test]
	fn backfill_gap_cache_rejects_zero_state_hash() {
		let from = TopoToken {
			depth: 42,
			pdu_count: PduCount::Normal(42),
		};
		let cached = Some((7, from, 500));

		assert!(!Service::backfill_gap_free_cache_hit(cached, 0, from, 100));
	}

	#[test]
	fn backfill_gap_cache_requires_sufficient_limit() {
		let from = TopoToken {
			depth: 42,
			pdu_count: PduCount::Normal(42),
		};
		let cached = Some((7, from, 100));

		assert!(Service::backfill_gap_free_cache_hit(cached, 7, from, 100));
		assert!(!Service::backfill_gap_free_cache_hit(cached, 7, from, 101));
	}
}

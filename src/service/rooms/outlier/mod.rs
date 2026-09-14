use std::sync::Arc;

use conduwuit::{
	Result, implement, info,
	matrix::{Event, PduEvent},
	utils::{
		ReadyExt,
		stream::{BroadbandExt, TryIgnore},
	},
};
use database::{Deserialized, Json, Map};
use futures::{FutureExt, Stream, StreamExt};
use ruma::{CanonicalJsonObject, CanonicalJsonValue, EventId, OwnedEventId, OwnedRoomId, RoomId};

use crate::{Dep, rooms, rooms::short::ShortRoomId};

pub struct Service {
	db: Data,
	services: Services,
}

struct Data {
	eventid_pdu: Arc<Map>,
	eventid_metadata: Arc<Map>,
	shorteventid_shortprevevents: Arc<Map>,
	shorteventid_shortauthevents: Arc<Map>,
}

struct Services {
	short: Dep<rooms::short::Service>,
	timeline: Dep<rooms::timeline::Service>,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				eventid_pdu: args.db["eventid_pdu"].clone(),
				eventid_metadata: args.db["eventid_metadata"].clone(),
				shorteventid_shortprevevents: args.db["shorteventid_shortprevevents"].clone(),
				shorteventid_shortauthevents: args.db["shorteventid_shortauthevents"].clone(),
			},
			services: Services {
				short: args.depend::<rooms::short::Service>("rooms::short"),
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
			},
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Returns the pdu from the outlier tree.
#[implement(Service)]
pub async fn get_outlier_pdu_json(&self, event_id: &EventId) -> Result<CanonicalJsonObject> {
	self.db
		.eventid_pdu
		.get(event_id.as_bytes())
		.await
		.deserialized()
}

/// Returns the pdu from the outlier tree.
#[implement(Service)]
pub async fn get_pdu_outlier(&self, event_id: &EventId) -> Result<PduEvent> {
	self.db
		.eventid_pdu
		.get_nocache(event_id.as_bytes())
		.await
		.deserialized()
}

#[implement(Service)]
pub fn stream_keys(&self) -> impl Stream<Item = OwnedEventId> + Send + '_ {
	self.db
		.eventid_metadata
		.raw_stream()
		.ignore_err()
		.ready_filter_map(|(key, val)| {
			let eid = OwnedEventId::try_from(std::str::from_utf8(key).ok()?).ok()?;
			let meta = rooms::timeline::EventMetadata::from_bincode(val).ok()?;
			meta.is_outlier.then_some(eid)
		})
}

#[implement(Service)]
pub fn stream(&self) -> impl Stream<Item = (OwnedEventId, PduEvent)> + Send + '_ {
	self.stream_keys().broad_filter_map(move |eid| async move {
		let pdu = self.get_pdu_outlier(&eid).await.ok()?;
		Some((eid, pdu))
	})
}

#[implement(Service)]
pub fn room_stream<'a>(
	&'a self,
	room_id: &'a RoomId,
) -> impl Stream<Item = (OwnedEventId, PduEvent)> + Send + 'a {
	let short_room_id = self
		.services
		.short
		.get_shortroomid(room_id)
		.map(std::result::Result::ok);

	let room_id_clone = room_id.to_owned();

	futures::stream::once(short_room_id)
		.filter_map(|opt| async move { opt })
		.flat_map(move |target_short: ShortRoomId| {
			self.db
				.eventid_metadata
				.raw_stream()
				.ignore_err()
				.ready_filter_map(move |(key, val)| {
					let eid = OwnedEventId::try_from(std::str::from_utf8(key).ok()?).ok()?;
					let meta: rooms::timeline::EventMetadata = bincode::deserialize(val).ok()?;
					if !meta.is_outlier {
						return None;
					}
					// If short_room_id is 0, we must defer the check to when we load the PDU JSON
					if meta.short_room_id != 0 && meta.short_room_id != target_short {
						return None;
					}
					Some((eid, meta.short_room_id))
				})
		})
		.broad_filter_map(move |(eid, meta_short_room_id)| {
			let room_id = room_id_clone.clone();
			async move {
				let pdu = self.get_pdu_outlier(&eid).await.ok()?;
				// If metadata had a 0 short_room_id, we must check the actual PDU room_id
				if meta_short_room_id == 0 && pdu.room_id() != Some(&*room_id) {
					return None;
				}
				Some((eid, pdu))
			}
		})
}

/// Append the PDU as an outlier.
///
/// Holds the same per-room `mutex_insert` lock that
/// `append_pdu`/`backfill_pdu`/`promote_outlier`/`force_insert_pdu` use for
/// their own check-then-insert of `eventid_metadata`. Without this, the
/// guard in `add_pdu_outlier_batch` (skip if the event is already in the
/// timeline) is a plain read-then-write with nothing stopping a concurrent
/// timeline insert from landing between the read and the write, letting
/// this call clobber a just-appended timeline event back into an outlier.
#[implement(Service)]
#[tracing::instrument(skip(self, pdu), level = "debug")]
pub async fn add_pdu_outlier(
	&self,
	event_id: &EventId,
	pdu: &CanonicalJsonObject,
	room_id: Option<&RoomId>,
) {
	let room_id_for_lock = derive_room_id(pdu, room_id, event_id);
	let _guard = match room_id_for_lock.as_deref() {
		| Some(room_id) => Some(self.services.timeline.mutex_insert.lock(room_id).await),
		// No determinable room_id (should be rare/impossible outside of malformed
		// input); nothing else can be racing writes for a room we can't identify.
		| None => None,
	};

	self.add_pdu_outlier_inner(event_id, pdu, room_id);
}

/// Same as `add_pdu_outlier`, but for callers that already hold
/// `rooms::timeline::Service::mutex_insert` for this room (e.g.
/// `force_state` invoked from inside `append_pdu`). `mutex_insert` is not
/// reentrant, so re-locking here from the same call stack would deadlock;
/// the `&InsertMutexGuard` parameter is unused beyond proving at the call
/// site that the lock is genuinely already held.
#[implement(Service)]
#[tracing::instrument(skip(self, pdu, _insert_lock), level = "debug")]
pub fn add_pdu_outlier_locked(
	&self,
	event_id: &EventId,
	pdu: &CanonicalJsonObject,
	room_id: Option<&RoomId>,
	_insert_lock: &rooms::timeline::InsertMutexGuard,
) {
	self.add_pdu_outlier_inner(event_id, pdu, room_id);
}

#[implement(Service)]
fn add_pdu_outlier_inner(
	&self,
	event_id: &EventId,
	pdu: &CanonicalJsonObject,
	room_id: Option<&RoomId>,
) {
	let mut batch = database::Batch::new();
	self.add_pdu_outlier_batch(&mut batch, event_id, pdu, room_id);
	self.db.eventid_pdu.apply_batch(batch);
}

/// Determine the room a PDU belongs to, mirroring the priority order used in
/// `add_pdu_outlier_batch`'s `room_id_from_pdu`: the PDU's own `room_id`
/// field first (authoritative once present), then the caller-supplied hint,
/// then (for `m.room.create` only) the room id derivable from the event id
/// itself.
fn derive_room_id(
	pdu: &CanonicalJsonObject,
	room_id: Option<&RoomId>,
	event_id: &EventId,
) -> Option<OwnedRoomId> {
	pdu.get("room_id")
		.and_then(CanonicalJsonValue::as_str)
		.and_then(|r| <&RoomId>::try_from(r).ok())
		.map(ToOwned::to_owned)
		.or_else(|| room_id.map(ToOwned::to_owned))
		.or_else(|| {
			let is_create =
				pdu.get("type").and_then(CanonicalJsonValue::as_str) == Some("m.room.create");
			is_create
				.then(|| event_id.as_str().replace('$', "!"))
				.and_then(|r| OwnedRoomId::parse(r).ok())
		})
}

/// Append the PDU as an outlier using a Batch.
#[implement(Service)]
#[tracing::instrument(skip(self, batch, pdu), level = "debug")]
pub fn add_pdu_outlier_batch<'a>(
	&'a self,
	batch: &mut database::Batch<'a>,
	event_id: &EventId,
	pdu: &CanonicalJsonObject,
	room_id: Option<&RoomId>,
) {
	self.add_pdu_outlier_batch_impl(batch, event_id, pdu, room_id, false);
}

/// Append the PDU as an outlier during demotion of a timeline event.
///
/// Bypasses the `!meta.is_outlier` guard because the event's timeline pointers
/// are being removed in the same `Batch`.
#[implement(Service)]
#[tracing::instrument(skip(self, batch, pdu), level = "debug")]
pub fn add_pdu_outlier_batch_demote<'a>(
	&'a self,
	batch: &mut database::Batch<'a>,
	event_id: &EventId,
	pdu: &CanonicalJsonObject,
	room_id: Option<&RoomId>,
) {
	self.add_pdu_outlier_batch_impl(batch, event_id, pdu, room_id, true);
}

#[implement(Service)]
fn add_pdu_outlier_batch_impl<'a>(
	&'a self,
	batch: &mut database::Batch<'a>,
	event_id: &EventId,
	pdu: &CanonicalJsonObject,
	room_id: Option<&RoomId>,
	demote: bool,
) {
	// Guard: never overwrite an event that already has a timeline entry.
	// The eventid_pdu and eventid_metadata tables are shared between timeline
	// and outlier paths. Re-adding a timeline event as an outlier would set
	// is_outlier=true and zero out deprecated_local_topo_depth, making the event
	// invisible to /sync's timeline iterator (the "stuck state" bug).
	if let Ok(existing_meta) = self.db.eventid_metadata.get_blocking(event_id.as_bytes()) {
		if let Ok(meta) = rooms::timeline::EventMetadata::from_bincode(&existing_meta) {
			if !demote && !meta.is_outlier {
				info!(
					%event_id,
					"add_pdu_outlier: skipping, event already in timeline"
				);
				return;
			}
		}
	}

	let mut pdu = pdu.clone();
	pdu.insert("event_id".to_owned(), CanonicalJsonValue::String(event_id.as_str().to_owned()));

	let room_id_from_pdu = derive_room_id(&pdu, room_id, event_id);

	// --- Phase 1: Write ---
	self.db
		.eventid_pdu
		.batch_raw_put(batch, event_id.as_bytes(), Json(&pdu));

	if let Ok(parsed_pdu) =
		serde_json::from_value::<PduEvent>(serde_json::to_value(&pdu).unwrap())
	{
		let short_room_id = room_id_from_pdu
			.as_deref()
			.map_or(0, |rid| self.services.short.get_or_create_shortroomid_blocking(rid));

		// `PduEvent::rejected` is `#[serde(skip)]` (bookkeeping only, never on
		// the wire), so a freshly-parsed `parsed_pdu` always reports
		// `rejected() == false` here. Rejection/soft-fail verdicts live in
		// the independent `eventid_rejections` / `eventid_softfailed`
		// stores, which this function never touches, so a rejection recorded
		// by `mark_event_rejected()` is preserved across the outlier write.
		let metadata = rooms::timeline::EventMetadata {
			short_room_id,
			is_outlier: true,
			origin_server_ts: parsed_pdu.origin_server_ts().0,
			depth: parsed_pdu.depth(),
			redacted_by: parsed_pdu.redacts().map(ToOwned::to_owned),
			short_state_hash: None,
			deprecated_local_topo_depth: 0,
			pdu_count: None,
		};
		if let Ok(metadata_bytes) = bincode::serialize(&metadata) {
			self.db
				.eventid_metadata
				.batch_put(batch, event_id.as_bytes(), &metadata_bytes);
		}

		let short_event_id = self
			.services
			.short
			.get_or_create_shorteventid_blocking(event_id);
		let prev_shorts: Vec<rooms::short::ShortEventId> = parsed_pdu
			.prev_events()
			.map(|prev| {
				self.services
					.short
					.get_or_create_shorteventid_blocking(prev)
			})
			.collect();

		let key_bytes = short_event_id.to_be_bytes();
		let val_bytes = prev_shorts
			.iter()
			.flat_map(|s| s.to_be_bytes())
			.collect::<Vec<u8>>();

		self.db
			.shorteventid_shortprevevents
			.batch_put(batch, &key_bytes, &val_bytes);

		let auth_shorts: Vec<rooms::short::ShortEventId> = parsed_pdu
			.auth_events()
			.map(|auth| {
				self.services
					.short
					.get_or_create_shorteventid_blocking(auth)
			})
			.collect();

		let auth_val_bytes = auth_shorts
			.iter()
			.flat_map(|s| s.to_be_bytes())
			.collect::<Vec<u8>>();

		self.db
			.shorteventid_shortauthevents
			.batch_put(batch, &key_bytes, &auth_val_bytes);
	}
}

/// Apply a batch of outlier insertions
#[implement(Service)]
pub fn apply_outlier_batch(&self, batch: database::Batch<'_>) {
	self.db.eventid_pdu.apply_batch(batch);
}

/// Remove the PDU from the outlier tree.
#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn remove_outlier(&self, event_id: &EventId) {
	if !self
		.services
		.timeline
		.non_outlier_pdu_exists(event_id)
		.await
	{
		self.db.eventid_pdu.remove(event_id.as_bytes());
		self.db.eventid_metadata.remove(event_id.as_bytes());
	} else {
		self.clear_outlier_flag(event_id);
	}
}

/// Clears the outlier flag from an event's metadata.
/// This is used when an event is promoted to or already exists in the timeline.
#[implement(Service)]
pub fn clear_outlier_flag(&self, event_id: &EventId) {
	if let Ok(metadata_bytes) = self.db.eventid_metadata.get_blocking(event_id.as_bytes()) {
		if let Ok(mut meta) = rooms::timeline::EventMetadata::from_bincode(&metadata_bytes) {
			if meta.is_outlier {
				meta.is_outlier = false;
				if let Ok(new_bytes) = bincode::serialize(&meta) {
					self.db
						.eventid_metadata
						.insert(event_id.as_bytes(), &new_bytes);
				}
			}
		}
	}
}

#[implement(Service)]
pub fn fix_pdu_event_ids(&self) -> Result<usize> { Ok(0) }

#[implement(Service)]
#[tracing::instrument(skip(self), level = "info")]
pub async fn startup_janitor(&self) {
	info!("Outlier janitor is disabled.");
}

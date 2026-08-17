use std::{
	collections::{HashMap, HashSet},
	ops::Bound,
	sync::Arc,
};

use conduwuit::{
	Err, Event, PduCount, PduEvent, Result, at, err,
	matrix::pdu::{TimelineKey, TopoToken},
	result::NotFound,
	utils::{
		self,
		stream::{ReadyExt, TryReadyExt, WidebandExt},
	},
};
use database::{Database, Deserialized, Json, KeyVal, Map, serialize_key};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt, pin_mut};
use ruma::{
	CanonicalJsonObject, EventId, OwnedEventId, OwnedUserId, RoomId, UserId, api::Direction,
};

use super::{PduId, RawPduId, backward_extremities};
use crate::{Dep, rooms, rooms::short::ShortRoomId};

pub(super) struct Data {
	eventid_pduid: Arc<Map>,
	userroomid_highlightcount: Arc<Map>,
	userroomid_notificationcount: Arc<Map>,
	eventid_pdu: Arc<Map>,
	eventid_metadata: Arc<Map>,
	room_pducount_eventid: Arc<Map>,
	roomid_topologicalorder_pducount: Arc<Map>,
	shorteventid_shortauthevents: Arc<Map>,
	shorteventid_shortprevevents: Arc<Map>,
	pub(super) db: Arc<Database>,
	services: Services,
}

struct Services {
	short: Dep<rooms::short::Service>,
}

pub type PdusIterItem = (PduCount, PduEvent);
pub type TopoIterItem = (TopoToken, PduEvent);

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let db = &args.db;
		Self {
			eventid_pduid: db["eventid_pduid"].clone(),
			userroomid_highlightcount: db["userroomid_highlightcount"].clone(),
			userroomid_notificationcount: db["userroomid_notificationcount"].clone(),
			eventid_pdu: db["eventid_pdu"].clone(),
			eventid_metadata: db["eventid_metadata"].clone(),
			room_pducount_eventid: db["room_pducount_eventid"].clone(),
			roomid_topologicalorder_pducount: db["roomid_topologicalorder_pducount"].clone(),
			shorteventid_shortauthevents: db["shorteventid_shortauthevents"].clone(),
			shorteventid_shortprevevents: db["shorteventid_shortprevevents"].clone(),
			db: args.db.clone(),
			services: Services {
				short: args.depend::<rooms::short::Service>("rooms::short"),
			},
		}
	}

	#[inline]
	pub(super) async fn last_timeline_count(&self, room_id: &RoomId) -> Result<PduCount> {
		let current = self
			.count_to_id(room_id, PduCount::max(), Direction::Backward)
			.await?;

		let prefix = current.shortroomid();
		let last_count = self
			.room_pducount_eventid
			.rev_raw_stream_from(&current)
			.ready_try_take_while(move |(key, _)| Ok(key.starts_with(&prefix)))
			.map_ok(|(key, _)| RawPduId::from(key).pdu_count())
			.try_next()
			.await?
			.unwrap_or(PduCount::min());

		conduwuit::debug!(
			target: "timeline_debug",
			"last_timeline_count for {}: {:?} (seek from {:?})",
			room_id,
			last_count,
			PduCount::max()
		);

		Ok(last_count)
	}

	#[inline]
	pub(super) async fn latest_pdu_in_room(&self, room_id: &RoomId) -> Result<PduEvent> {
		let pdus_rev = self.pdus_rev(room_id, Bound::Unbounded);

		pin_mut!(pdus_rev);
		pdus_rev
			.try_next()
			.await?
			.map(at!(1))
			.ok_or_else(|| err!(Request(NotFound("no PDUs found in room"))))
	}

	/// Returns the `count` of this pdu's id.
	pub(super) async fn get_pdu_count(&self, event_id: &EventId) -> Result<PduCount> {
		self.get_pdu_id(event_id)
			.await
			.map(|pdu_id| pdu_id.pdu_count())
	}

	/// Returns the EventMetadata for a PDU.
	pub(super) async fn get_event_metadata(
		&self,
		event_id: &EventId,
	) -> Result<rooms::timeline::EventMetadata> {
		let bytes = self.eventid_metadata.get(event_id.as_bytes()).await?;
		rooms::timeline::EventMetadata::from_bincode(&bytes)
			.map_err(|e| err!(Database("Failed to deserialize EventMetadata: {e}")))
	}

	/// Batch-fetch `EventMetadata` for a list of event ids.
	pub(super) async fn get_event_metadata_batch(
		&self,
		event_ids: &[OwnedEventId],
	) -> Vec<Result<rooms::timeline::EventMetadata>> {
		self.eventid_metadata
			.get_batch(futures::stream::iter(event_ids.iter().map(|id| id.as_bytes())))
			.map(|res| {
				res.and_then(|handle| {
					rooms::timeline::EventMetadata::from_bincode(&handle)
						.map_err(|e| err!(Database("Failed to deserialize EventMetadata: {e}")))
				})
			})
			.collect()
			.await
	}

	pub(super) fn store_eventid_metadata(&self, event_id_bytes: &[u8], metadata_bytes: Vec<u8>) {
		self.eventid_metadata.insert(event_id_bytes, metadata_bytes);
	}

	/// Returns the json of a pdu.
	pub(super) async fn get_pdu_json(&self, event_id: &EventId) -> Result<CanonicalJsonObject> {
		self.eventid_pdu
			.get(event_id.as_bytes())
			.await?
			.deserialized()
	}

	pub(super) async fn get_outlier_pdu_json(
		&self,
		event_id: &EventId,
	) -> Result<CanonicalJsonObject> {
		self.eventid_pdu
			.get_nocache(event_id.as_bytes())
			.await?
			.deserialized()
	}

	/// Returns the json of a pdu.
	pub(super) async fn get_non_outlier_pdu_json(
		&self,
		event_id: &EventId,
	) -> Result<CanonicalJsonObject> {
		let _pduid = self.get_pdu_id(event_id).await?;

		self.eventid_pdu
			.get(event_id.as_bytes())
			.await
			.deserialized()
	}

	/// Directly gets the PDU and JSON from the double-write `eventid_pdu` tree.
	/// Used for timeline re-insertion when other indices are cleared.
	pub(super) async fn get_from_eventid_pdu(
		&self,
		event_id: &EventId,
	) -> Result<(PduEvent, CanonicalJsonObject)> {
		let handle = self.eventid_pdu.get(event_id.as_bytes()).await?;
		let pdu: PduEvent = handle.deserialized()?;
		let json: CanonicalJsonObject = handle.deserialized()?;
		Ok((pdu, json))
	}

	/// Directly get raw PDU bytes from double-write `eventid_pdu` tree.
	pub(super) async fn get_pdu_and_raw_bytes(
		&self,
		event_id: &EventId,
	) -> Result<(PduEvent, Vec<u8>)> {
		let handle = self.eventid_pdu.get(event_id.as_bytes()).await?;
		let pdu: PduEvent = handle.deserialized()?;
		let raw_bytes = handle.as_ref().to_vec();
		Ok((pdu, raw_bytes))
	}

	pub(super) async fn reindex_timeline(&self, room_id: &RoomId) -> Result<usize> {
		let mut count = 0_usize;
		let pdus = self.pdus(room_id, Bound::Unbounded);
		pin_mut!(pdus);

		while let Some((_, pdu)) = pdus.try_next().await? {
			if let Ok(json) = self.get_non_outlier_pdu_json(&pdu.event_id).await {
				// `raw_put` already wakes watchers for this key (see
				// `Map::insert`); an extra explicit wake here would be redundant.
				self.eventid_pdu
					.raw_put(pdu.event_id.as_bytes(), Json(&json));
				count = count.saturating_add(1);
			}
		}
		Ok(count)
	}

	pub(super) fn apply_batch(&self, batch: database::Batch<'_>) {
		self.eventid_pdu.apply_batch(batch);
	}

	pub(super) async fn fallback_prev_events(&self, event_id: &EventId) -> HashSet<OwnedEventId> {
		let mut prevs = HashSet::new();
		if let Ok((pdu, _)) = self.get_from_eventid_pdu(event_id).await {
			for prev_id in pdu.prev_events() {
				prevs.insert(prev_id.to_owned());
			}
		}
		prevs
	}

	/// Reads prev_events from PDU JSON and lazily populates the
	/// `shortprevevents` cache so future lookups avoid full JSON
	/// deserialization.
	pub(super) async fn fallback_and_cache_prev_events(
		&self,
		event_id: &EventId,
	) -> HashSet<OwnedEventId> {
		let prevs = self.fallback_prev_events(event_id).await;

		if !prevs.is_empty() {
			let short_eid = self
				.services
				.short
				.get_or_create_shorteventid(event_id)
				.await;
			let mut prev_shorts = Vec::with_capacity(prevs.len());
			for prev_id in &prevs {
				prev_shorts.push(
					self.services
						.short
						.get_or_create_shorteventid(prev_id)
						.await,
				);
			}
			self.store_shortprevevents(short_eid, &prev_shorts);
		}

		prevs
	}

	/// Lightweight collection of all timeline entries for a room, suitable
	/// for reorder-timeline. Returns:
	///  - `entries`: event_id → (PduCount, origin_server_ts)
	///  - `graph`: event_id → set of prev_event_ids
	///  - `metadata_cache`: event_id → EventMetadata (for reuse in Phase 2)
	///
	/// This avoids deserializing full PDU JSON by reading only the
	/// small bincode `EventMetadata` and packed `shorteventid_shortprevevents`
	/// tables — orders of magnitude cheaper for large rooms.
	pub(super) async fn collect_reorder_entries(
		&self,
		room_id: &RoomId,
	) -> Result<(
		HashMap<OwnedEventId, (PduCount, u64, u64)>,
		HashMap<OwnedEventId, HashSet<OwnedEventId>>,
		HashMap<OwnedEventId, rooms::timeline::EventMetadata>,
	)> {
		let shortroomid = self.services.short.get_or_create_shortroomid(room_id).await;
		let seek_backfill =
			Self::pdu_count_to_id(shortroomid, PduCount::min(), Direction::Forward);
		let seek_normal =
			Self::pdu_count_to_id(shortroomid, PduCount::Normal(0), Direction::Forward);
		let prefix = seek_backfill.shortroomid();

		let mut entries: HashMap<OwnedEventId, (PduCount, u64, u64)> = HashMap::new();
		let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::new();
		let mut metadata_cache: HashMap<OwnedEventId, rooms::timeline::EventMetadata> =
			HashMap::new();

		// Phase 1a: Iterate the stream index to get (pdu_id, event_id_bytes) pairs.
		// No JSON deserialization — just raw key/value from room_pducount_eventid.
		let mut all_event_ids: Vec<(PduCount, OwnedEventId)> = Vec::new();

		// Iterate backfill range
		let backfill_stream = self.room_pducount_eventid.raw_stream_from(&seek_backfill);
		pin_mut!(backfill_stream);
		while let Some(Ok((key, val))) = backfill_stream.next().await {
			if !key.starts_with(&prefix) {
				break;
			}
			let pdu_id = RawPduId::from(key);
			let count = pdu_id.pdu_count();
			if matches!(count, PduCount::Normal(_)) {
				break; // crossed into normal range
			}
			if let Ok(s) = std::str::from_utf8(val) {
				if let Ok(event_id) = OwnedEventId::try_from(s) {
					all_event_ids.push((count, event_id));
				}
			}
		}

		// Iterate normal range
		let normal_stream = self.room_pducount_eventid.raw_stream_from(&seek_normal);
		pin_mut!(normal_stream);
		while let Some(Ok((key, val))) = normal_stream.next().await {
			if !key.starts_with(&prefix) {
				break;
			}
			let pdu_id = RawPduId::from(key);
			let count = pdu_id.pdu_count();
			if let Ok(s) = std::str::from_utf8(val) {
				if let Ok(event_id) = OwnedEventId::try_from(s) {
					all_event_ids.push((count, event_id));
				}
			}
			if all_event_ids.len().is_multiple_of(10000) {
				tokio::task::yield_now().await;
			}
		}

		// Phase 1b: For each event, read metadata (origin_server_ts) and
		// resolve prev_events from the shortprevevents table.
		for (count, event_id) in &all_event_ids {
			// Read metadata
			let meta_opt = if let Ok(bytes) = self.eventid_metadata.get(event_id.as_bytes()).await
			{
				rooms::timeline::EventMetadata::from_bincode(&bytes).ok()
			} else {
				None
			};

			let ts = meta_opt.as_ref().map_or(0, |m| m.origin_server_ts.into());
			let depth = meta_opt.as_ref().map_or(0, |m| m.depth.into());

			entries.insert(event_id.clone(), (*count, depth, ts));

			if let Some(meta) = meta_opt {
				metadata_cache.insert(event_id.clone(), meta);
			}

			// Resolve prev_events via shorteventid → shortprevevents → eventid
			let prev_events: HashSet<OwnedEventId> =
				if let Ok(short_eid) = self.services.short.get_shorteventid(event_id).await {
					if let Ok(short_prevs) = self.get_shortprevevents(short_eid).await {
						if !short_prevs.is_empty() {
							let mut prevs = HashSet::with_capacity(short_prevs.len());
							for short_prev in short_prevs {
								if let Ok(prev_id) = self
									.services
									.short
									.get_eventid_from_short::<OwnedEventId>(short_prev)
									.await
								{
									prevs.insert(prev_id);
								}
							}
							prevs
						} else {
							self.fallback_and_cache_prev_events(event_id).await
						}
					} else {
						self.fallback_and_cache_prev_events(event_id).await
					}
				} else {
					self.fallback_and_cache_prev_events(event_id).await
				};

			graph.insert(event_id.clone(), prev_events);

			if entries.len().is_multiple_of(10000) {
				conduwuit::debug!(
					"collect_reorder_entries: processed {} events so far...",
					entries.len()
				);
				tokio::task::yield_now().await;
			}
		}

		Ok((entries, graph, metadata_cache))
	}

	pub(super) async fn fix_pdu_event_ids(&self) -> Result<usize> {
		use futures::TryStreamExt;
		let mut fixed: usize = 0;
		// Use raw_stream to iterate eventid_pduid mapping
		let iter = self.eventid_pduid.raw_stream();
		pin_mut!(iter);

		while let Some((event_id_bytes, pdu_id_bytes)) = iter.try_next().await? {
			if let Ok(event_id_str) = std::str::from_utf8(event_id_bytes) {
				if let Ok(event_id) = OwnedEventId::try_from(event_id_str) {
					let _pdu_id: RawPduId = pdu_id_bytes.into();
					if let Ok(mut json) = self
						.eventid_pdu
						.get(&event_id_bytes)
						.await
						.deserialized::<CanonicalJsonObject>()
					{
						if !json.contains_key("event_id") {
							json.insert(
								"event_id".into(),
								ruma::CanonicalJsonValue::String(event_id.as_str().to_owned()),
							);
							self.eventid_pdu.raw_put(event_id_bytes, Json(&json));
							fixed = fixed.saturating_add(1);
						}
					}
				}
			}
		}
		Ok(fixed)
	}

	pub(crate) fn topo_pducount_key(pdu_id: &RawPduId, depth: u64) -> Vec<u8> {
		let mut shorteventid = [0_u8; 8];
		shorteventid.copy_from_slice(&pdu_id.shorteventid());
		let stream_ordering = i64::from_be_bytes(PduCount::offset_binary_encoding(shorteventid));
		let timeline_key = TimelineKey::new(depth, stream_ordering);

		let mut topo_key = Vec::with_capacity(24);
		topo_key.extend_from_slice(&pdu_id.shortroomid());
		topo_key.extend_from_slice(&timeline_key.to_be_bytes());
		topo_key
	}

	pub(super) async fn clear_room_topo_index_into_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		room_id: &RoomId,
	) -> Result<usize> {
		let shortroomid = self.services.short.get_shortroomid(room_id).await?;
		let prefix = shortroomid.to_be_bytes();
		let keys = self
			.roomid_topologicalorder_pducount
			.raw_stream_prefix(&prefix)
			.map_ok(|(key, _)| key.to_vec())
			.try_collect::<Vec<_>>()
			.await?;

		if keys.is_empty() {
			return Ok(0);
		}

		for key in &keys {
			self.roomid_topologicalorder_pducount
				.batch_delete(batch, key);
		}

		Ok(keys.len())
	}

	pub(super) async fn clear_room_topo_index(&self, room_id: &RoomId) -> Result<usize> {
		let mut batch = database::Batch::new();
		let cleared = self
			.clear_room_topo_index_into_batch(&mut batch, room_id)
			.await?;
		self.roomid_topologicalorder_pducount.apply_batch(batch);
		Ok(cleared)
	}

	pub(super) fn insert_topo_pducount_into_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		pdu_id: &RawPduId,
		event_id: &EventId,
		depth: u64,
	) {
		let topo_key = Self::topo_pducount_key(pdu_id, depth);
		self.roomid_topologicalorder_pducount
			.batch_put(batch, &topo_key, event_id.as_bytes());
	}

	pub(super) fn topo_key_to_pdu_id(topo_key: &[u8]) -> RawPduId {
		let mut pdu_id_bytes = [0_u8; 16];
		pdu_id_bytes[0..8].copy_from_slice(&topo_key[0..8]);

		let mut timeline_bytes = [0_u8; 16];
		timeline_bytes.copy_from_slice(&topo_key[8..24]);
		let timeline_key = TimelineKey::from_bytes(&timeline_bytes);

		let count_bytes =
			PduCount::offset_binary_encoding(timeline_key.stream_ordering.to_be_bytes());
		pdu_id_bytes[8..16].copy_from_slice(&count_bytes);

		pdu_id_bytes.as_slice().into()
	}

	pub(super) async fn pdu_id_to_depth(&self, pdu_id: &RawPduId) -> Result<u64> {
		let event_id_bytes = self.room_pducount_eventid.get(pdu_id).await?;
		let metadata_bytes = self.eventid_metadata.get(&event_id_bytes).await?;
		let meta = rooms::timeline::EventMetadata::from_bincode(&metadata_bytes)
			.map_err(|e| err!(Database("Failed to deserialize EventMetadata: {e}")))?;
		Ok(meta.depth.into())
	}

	pub(super) fn remove_topo_pducount(&self, pdu_id: &RawPduId, event_id_bytes: &[u8]) {
		if let Ok(bytes) = self.eventid_metadata.get_blocking(event_id_bytes) {
			if let Ok(meta) = rooms::timeline::EventMetadata::from_bincode(&bytes) {
				self.roomid_topologicalorder_pducount
					.remove(&Self::topo_pducount_key(pdu_id, meta.depth.into()));
			}
		}
	}

	/// Remove topo entry using a **known** depth, avoiding the `get_blocking`
	/// call that `remove_topo_pducount` does.
	pub(super) fn remove_stream_and_topo_pducount_from_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		pdu_id: &RawPduId,
		event_id_bytes: &[u8],
		depth: Option<u64>,
	) {
		self.room_pducount_eventid
			.batch_delete(batch, pdu_id.as_bytes());
		self.eventid_pduid.batch_delete(batch, event_id_bytes);

		if let Some(depth) = depth {
			self.roomid_topologicalorder_pducount
				.batch_delete(batch, &Self::topo_pducount_key(pdu_id, depth));
		}
	}

	/// Batched equivalent of `remove_stream_and_topo_pducount`: resolves the
	/// depth via the same blocking metadata read (`meta.depth`, matching
	/// `remove_topo_pducount`'s field exactly -- not
	/// `deprecated_local_topo_depth`, which is a different field with its own
	/// separate callers), then routes the three deletes into `batch` instead
	/// of writing them individually.
	pub(super) fn remove_stream_and_topo_pducount_into_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		pdu_id: &RawPduId,
		event_id_bytes: &[u8],
	) {
		let depth = self
			.eventid_metadata
			.get_blocking(event_id_bytes)
			.ok()
			.and_then(|bytes| rooms::timeline::EventMetadata::from_bincode(&bytes).ok())
			.map(|meta| meta.depth.into());
		self.remove_stream_and_topo_pducount_from_batch(batch, pdu_id, event_id_bytes, depth);
	}

	/// Batched equivalent of `replace_stream_and_topo_pducount`. All four
	/// writes (stream, `eventid_pduid`, metadata, topo) land in the same
	/// `database::Batch` and therefore the same atomic RocksDB write --
	/// unlike the individually-`.insert()`ed version above, a crash (or a
	/// concurrent reader) can never observe `eventid_pduid`/`eventid_metadata`
	/// updated while the topo index still points at the old position, or
	/// vice versa.
	pub(super) fn replace_stream_and_topo_pducount_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		pdu_id: &RawPduId,
		event_id: &EventId,
		local_topo_depth: u64,
		pdu_count: PduCount,
	) {
		self.room_pducount_eventid
			.batch_put(batch, pdu_id, event_id.as_bytes());
		self.eventid_pduid
			.batch_put(batch, event_id.as_bytes(), pdu_id);
		self.set_event_metadata_depth_and_count_into_batch(
			batch,
			event_id,
			local_topo_depth,
			pdu_count,
		);
		let topo_key = Self::topo_pducount_key(pdu_id, local_topo_depth);
		self.roomid_topologicalorder_pducount
			.batch_put(batch, &topo_key, event_id.as_bytes());
	}

	/// Batched equivalent of `replace_stream_topo_with_cached_metadata` --
	/// same reasoning as `replace_stream_and_topo_pducount_batch`.
	pub(super) fn replace_stream_topo_with_cached_metadata_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		pdu_id: &RawPduId,
		event_id: &EventId,
		local_topo_depth: u64,
		pdu_count: PduCount,
		meta: &mut rooms::timeline::EventMetadata,
	) {
		self.room_pducount_eventid
			.batch_put(batch, pdu_id, event_id.as_bytes());
		self.eventid_pduid
			.batch_put(batch, event_id.as_bytes(), pdu_id);

		meta.deprecated_local_topo_depth = local_topo_depth;
		meta.pdu_count = match pdu_count {
			| PduCount::Normal(x) => Some(x),
			| PduCount::Backfilled(_) => None, /* Force fallback to eventid_pduid for proper
			                                    * decoding */
		};
		match bincode::serialize(meta) {
			| Ok(metadata_bytes) => {
				self.eventid_metadata
					.batch_put(batch, event_id.as_bytes(), metadata_bytes);
			},
			| Err(e) => {
				// The stream/topo writes for this event are still going into
				// `batch` below and will land -- only the metadata write
				// (and its `eventid_metadata` fast-path lookup for
				// get_pdu_id) is skipped, leaving that one event to fall
				// back to the `eventid_pduid` legacy path. Not silent: this
				// is the same shape of write inconsistency
				// docs/development-gg/backfill-v12-phantom-timeline-membership.md
				// is about, just smaller, so it's worth knowing about even
				// though EventMetadata's all-primitive fields make it very
				// unlikely to actually fire.
				conduwuit::warn!(%event_id, "Failed to serialize EventMetadata for batch write: {e}");
			},
		}

		let topo_key = Self::topo_pducount_key(pdu_id, local_topo_depth);
		self.roomid_topologicalorder_pducount
			.batch_put(batch, &topo_key, event_id.as_bytes());
	}

	/// Batched equivalent of `reindex_topo_with_cached_metadata`. The old
	/// topo entry's removal and the new one's insertion land in the same
	/// batch as the metadata update, so a reader can never observe the old
	/// and new topo keys simultaneously absent (or the metadata pointing at
	/// a depth neither key uses).
	pub(super) fn reindex_topo_with_cached_metadata_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		pdu_id: &RawPduId,
		event_id: &EventId,
		new_topo_depth: u64,
		meta: &mut rooms::timeline::EventMetadata,
	) {
		let old_topo_key = Self::topo_pducount_key(pdu_id, meta.deprecated_local_topo_depth);
		self.roomid_topologicalorder_pducount
			.batch_delete(batch, &old_topo_key);

		let topo_key = Self::topo_pducount_key(pdu_id, new_topo_depth);
		self.roomid_topologicalorder_pducount
			.batch_put(batch, &topo_key, event_id.as_bytes());

		meta.deprecated_local_topo_depth = new_topo_depth;
		match bincode::serialize(meta) {
			| Ok(metadata_bytes) => {
				self.eventid_metadata
					.batch_put(batch, event_id.as_bytes(), metadata_bytes);
			},
			| Err(e) => conduwuit::warn!(
				%event_id,
				"Failed to serialize EventMetadata for batch write: {e}"
			),
		}
	}

	pub(super) async fn remove_from_timeline(&self, event_id: &EventId) {
		if let Ok(pduid) = self.get_pdu_id(event_id).await {
			self.eventid_pduid.remove(event_id);
			self.room_pducount_eventid.remove(&pduid);
			self.remove_topo_pducount(&pduid, event_id.as_bytes());

			if self.outlier_pdu_exists(event_id).await.is_err() {
				self.eventid_pdu.remove(event_id.as_bytes());
				self.eventid_metadata.remove(event_id.as_bytes());
			}
		}
	}

	/// Strips only the timeline-membership of `event_id` (the
	/// `eventid_pduid`/`room_pducount_eventid`/topo pointers, plus the stale
	/// `eventid_metadata` entry those pointers were keyed against), leaving
	/// `eventid_pdu` untouched.
	///
	/// This exists for callers that intend to immediately re-persist the
	/// event as an outlier (e.g. `add_pdu_outlier`/`add_pdu_outlier_locked`)
	/// under the same `mutex_insert` guard: `add_pdu_outlier_batch`'s "never
	/// overwrite a timeline event" guard keys off the *existing*
	/// `eventid_metadata` entry, so as long as that entry still says
	/// `is_outlier: false` (which it does for any event currently in the
	/// timeline) the outlier write is silently skipped. Clearing the
	/// metadata here first lets the subsequent outlier write land.
	///
	/// Mirrors `remove_from_timeline`'s statement order --
	/// `remove_topo_pducount` still needs to read the old `eventid_metadata`
	/// for the event's depth, so that removal must happen before this
	/// function's own metadata deletion, not after.
	pub(super) async fn remove_timeline_pointers(&self, event_id: &EventId) {
		if let Ok(pduid) = self.get_pdu_id(event_id).await {
			self.eventid_pduid.remove(event_id);
			self.room_pducount_eventid.remove(&pduid);
			self.remove_topo_pducount(&pduid, event_id.as_bytes());
		}

		self.eventid_metadata.remove(event_id.as_bytes());
	}

	/// Batched equivalent of `remove_timeline_pointers`. This is for demotion
	/// paths that will atomically rewrite the same event as an outlier in the
	/// same RocksDB commit, so a crash cannot leave the JSON reachable through
	/// neither the timeline indices nor outlier metadata.
	pub(super) async fn remove_timeline_pointers_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		event_id: &EventId,
	) {
		if let Ok(pduid) = self.get_pdu_id(event_id).await {
			let depth = self
				.eventid_metadata
				.get_blocking(event_id.as_bytes())
				.ok()
				.and_then(|bytes| rooms::timeline::EventMetadata::from_bincode(&bytes).ok())
				.map(|meta| meta.depth.into());
			self.remove_stream_and_topo_pducount_from_batch(
				batch,
				&pduid,
				event_id.as_bytes(),
				depth,
			);
		}

		self.eventid_metadata
			.batch_delete(batch, event_id.as_bytes());
	}

	/// Rebuild the topological index entry for a single event without
	/// touching stream order: removes the old topo key, computes a new
	/// `deprecated_local_topo_depth`, writes the new topo key, and updates
	/// metadata. Both blocking reads (old depth via the same lookup
	/// `remove_topo_pducount` does, then metadata for the update) still
	/// happen outside the batch -- RocksDB batches are write-only -- but
	/// every write lands in `batch` together.
	pub(super) fn reindex_topo_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		pdu_id: &RawPduId,
		event_id: &EventId,
		new_topo_depth: u64,
	) {
		let event_id_bytes = event_id.as_bytes();

		if let Ok(bytes) = self.eventid_metadata.get_blocking(event_id_bytes) {
			if let Ok(meta) = rooms::timeline::EventMetadata::from_bincode(&bytes) {
				let old_topo_key =
					Self::topo_pducount_key(pdu_id, meta.deprecated_local_topo_depth);
				self.roomid_topologicalorder_pducount
					.batch_delete(batch, &old_topo_key);
			}
		}

		let topo_key = Self::topo_pducount_key(pdu_id, new_topo_depth);
		self.roomid_topologicalorder_pducount
			.batch_put(batch, &topo_key, event_id_bytes);

		if let Ok(bytes) = self.eventid_metadata.get_blocking(event_id_bytes) {
			if let Ok(mut meta) = rooms::timeline::EventMetadata::from_bincode(&bytes) {
				meta.deprecated_local_topo_depth = new_topo_depth;
				match bincode::serialize(&meta) {
					| Ok(metadata_bytes) => {
						self.eventid_metadata
							.batch_put(batch, event_id_bytes, metadata_bytes);
					},
					| Err(e) => conduwuit::warn!(
						%event_id,
						"Failed to serialize EventMetadata for batch write: {e}"
					),
				}
			}
		}
	}

	/// Batched equivalent of `set_event_metadata_depth_and_count`. The
	/// read-modify-write still does its read outside the batch (RocksDB
	/// batches are write-only), but the write half lands in `batch` instead
	/// of as its own independent `.insert()`.
	pub(super) fn set_event_metadata_depth_and_count_into_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		event_id: &EventId,
		depth: u64,
		pdu_count: PduCount,
	) {
		if let Ok(bytes) = self.eventid_metadata.get_blocking(event_id.as_bytes()) {
			if let Ok(mut meta) = rooms::timeline::EventMetadata::from_bincode(&bytes) {
				meta.deprecated_local_topo_depth = depth;
				meta.pdu_count = match pdu_count {
					| PduCount::Normal(x) => Some(x),
					| PduCount::Backfilled(_) => None, // Force fallback to eventid_pduid
				};
				match bincode::serialize(&meta) {
					| Ok(metadata_bytes) => {
						self.eventid_metadata.batch_put(
							batch,
							event_id.as_bytes(),
							metadata_bytes,
						);
					},
					| Err(e) => conduwuit::warn!(
						%event_id,
						"Failed to serialize EventMetadata for batch write: {e}"
					),
				}
			}
		}
	}

	/// Drop a duplicate PDU by ID without removing the event mapping
	pub(super) fn drop_duplicate_pdu(&self, pdu_id: &RawPduId) {
		if let Ok(event_id_bytes) = self.room_pducount_eventid.get_blocking(pdu_id) {
			self.remove_topo_pducount(pdu_id, &event_id_bytes);
		}
		self.room_pducount_eventid.remove(pdu_id);
	}

	/// Returns the pdu's id. Tries metadata `pdu_count` first (fast path),
	/// then falls back to the legacy `eventid_pduid` table.
	pub(super) async fn get_pdu_id(&self, event_id: &EventId) -> Result<RawPduId> {
		// Fast path: metadata has pdu_count
		let meta_result = self.eventid_metadata.get(event_id.as_bytes()).await;
		if let Ok(bytes) = &meta_result {
			if let Ok(meta) = rooms::timeline::EventMetadata::from_bincode(bytes) {
				if let Some(count) = meta.pdu_count {
					let pdu_count = PduCount::from_unsigned(count);
					return Ok(PduId {
						shortroomid: meta.short_room_id,
						shorteventid: pdu_count,
					}
					.into());
				}
			}
		}

		// Legacy fallback
		self.eventid_pduid
			.get(event_id)
			.await
			.map(|handle| RawPduId::from(&*handle))
	}

	/// Returns the pdu directly from `eventid_pduid` only.
	/// If `room_id` is provided, validates the PDU belongs to that room.
	pub(super) async fn get_non_outlier_pdu_in_room(
		&self,
		room_id: Option<&RoomId>,
		event_id: &EventId,
	) -> Result<PduEvent> {
		let pduid = self.get_pdu_id(event_id).await?;
		let pdu: PduEvent = self
			.eventid_pdu
			.get(event_id.as_bytes())
			.await
			.deserialized()?;

		// Enforce cross-room boundary: verify the PDU belongs to the expected room
		if let Some(expected_room) = room_id {
			let actual_room = pdu.room_id_or_hash();
			if let Some(actual_room) = actual_room {
				if actual_room != expected_room {
					return Err!(Database(
						"PDU {event_id} does belong to room {actual_room} (expected \
						 {expected_room})"
					));
				}
			} else {
				// v12 create events do not contain room_id in the JSON.
				// Verify room association by comparing ShortRoomId from pdu_id.
				let expected_shortroomid =
					self.services.short.get_shortroomid(expected_room).await?;
				if pduid.shortroomid() != expected_shortroomid.to_be_bytes() {
					return Err!(Database(
						"PDU {event_id} does not belong to room {expected_room}"
					));
				}
			}
		}

		Ok(pdu)
	}

	pub(super) async fn prev_timeline_count(&self, before: &PduId) -> Result<PduCount> {
		let before_pdu =
			Self::pdu_count_to_id(before.shortroomid, before.shorteventid, Direction::Backward);

		let prefix = before_pdu.shortroomid();
		let pdu_ids = self
			.room_pducount_eventid
			.rev_keys_raw_from(&before_pdu)
			.ready_try_take_while(move |pdu_bytes: &&[u8]| Ok(pdu_bytes.starts_with(&prefix)))
			.ready_and_then(|pdu_bytes: &[u8]| {
				let pdu_id = RawPduId::from(pdu_bytes);
				Ok(pdu_id.pdu_count())
			});

		pin_mut!(pdu_ids);
		pdu_ids
			.try_next()
			.await?
			.ok_or_else(|| err!(Request(NotFound("No earlier PDUs found in room"))))
	}

	pub(super) async fn next_timeline_count(&self, after: &PduId) -> Result<PduCount> {
		let after_pdu =
			Self::pdu_count_to_id(after.shortroomid, after.shorteventid, Direction::Forward);

		let prefix = after_pdu.shortroomid();
		let pdu_ids = self
			.room_pducount_eventid
			.keys_raw_from(&after_pdu)
			.ready_try_take_while(move |pdu_bytes: &&[u8]| Ok(pdu_bytes.starts_with(&prefix)))
			.ready_and_then(|pdu_bytes: &[u8]| {
				let pdu_id = RawPduId::from(pdu_bytes);
				Ok(pdu_id.pdu_count())
			});

		pin_mut!(pdu_ids);
		pdu_ids
			.try_next()
			.await?
			.ok_or_else(|| err!(Request(NotFound("No more PDUs found in room"))))
	}

	fn pdu_count_to_id(
		shortroomid: ShortRoomId,
		shorteventid: PduCount,
		dir: Direction,
	) -> RawPduId {
		// +1 so we don't send the base event
		let pdu_id = PduId {
			shortroomid,
			shorteventid: shorteventid.saturating_inc(dir),
		};

		pdu_id.into()
	}

	/// Like get_non_outlier_pdu(), but without the expense of fetching and
	/// parsing the PduEvent
	pub(super) async fn non_outlier_pdu_exists(&self, event_id: &EventId) -> Result {
		let pduid = self.get_pdu_id(event_id).await?;

		self.room_pducount_eventid.exists(&pduid).await
	}

	/// Returns the pdu.
	///
	/// Checks the `eventid_pdu` Tree if not found in the timeline.
	/// If `room_id` is provided, validates the PDU belongs to that room.
	pub(super) async fn get_pdu_in_room(
		&self,
		room_id: Option<&RoomId>,
		event_id: &EventId,
	) -> Result<PduEvent> {
		let pdu: PduEvent = self
			.eventid_pdu
			.get(event_id.as_bytes())
			.await?
			.deserialized()?;

		if let Some(expected_room) = room_id {
			let actual_room = pdu.room_id_or_hash();
			if let Some(actual_room) = actual_room {
				if actual_room != expected_room {
					return Err!(Database(
						"PDU {event_id} does belong to room {actual_room} (expected \
						 {expected_room})"
					));
				}
			} else {
				// v12 create events do not contain room_id in the JSON.
				// Verify room association.
				if let Ok(expected_short) =
					self.services.short.get_shortroomid(expected_room).await
				{
					if let Ok(pduid) = self.get_pdu_id(event_id).await {
						if pduid.shortroomid() != expected_short.to_be_bytes() {
							return Err!(Database(
								"PDU {event_id} is not associated with room {expected_room}"
							));
						}
					} else if let Ok(meta_bytes) =
						self.eventid_metadata.get(event_id.as_bytes()).await
					{
						if let Ok(meta) =
							rooms::timeline::EventMetadata::from_bincode(&meta_bytes)
						{
							if meta.short_room_id != expected_short {
								return Err!(Database(
									"PDU {event_id} is not associated with room {expected_room}"
								));
							}
						} else {
							return Err!(Database("corrupt metadata"));
						}
					} else {
						return Err!(Database("PDU has no room association metadata"));
					}
				}
			}
		}

		Ok(pdu)
	}

	pub(super) async fn get_pdus_in_room_batch(
		&self,
		room_id: Option<&RoomId>,
		event_ids: &[OwnedEventId],
	) -> Vec<Result<PduEvent>> {
		use futures::StreamExt;
		let mut results = Vec::with_capacity(event_ids.len());

		let mut expected_shortroomid: Option<ShortRoomId> = None;
		if let Some(expected_room) = room_id {
			if let Ok(id) = self.services.short.get_shortroomid(expected_room).await {
				expected_shortroomid = Some(id);
			}
		}

		// Batch fetch from eventid_pduid
		let pdu_ids: Vec<Result<database::Handle<'_>>> = self
			.eventid_pduid
			.get_batch(futures::stream::iter(event_ids.iter().map(|id| id.as_bytes())))
			.collect()
			.await;

		// Separate into hits and misses
		let mut valid_pdu_ids = Vec::with_capacity(event_ids.len());
		let mut missing_event_ids = Vec::with_capacity(event_ids.len());

		for (i, pdu_id_res) in pdu_ids.iter().enumerate() {
			match pdu_id_res {
				| Ok(handle) => valid_pdu_ids.push(RawPduId::from(&**handle)),
				| Err(_) => missing_event_ids.push(event_ids[i].as_bytes()),
			}
		}

		// Two-hop resolve: room_pducount_eventid → eventid_pdu
		let pdu_events = self.resolve_pdu_batch(&valid_pdu_ids).await;

		// Batch fetch outliers directly from eventid_pdu
		let outlier_events = if !missing_event_ids.is_empty() {
			self.eventid_pdu
				.get_batch(futures::stream::iter(missing_event_ids))
				.map(|res: Result<database::Handle<'_>>| {
					res.and_then(|handle| handle.deserialized::<PduEvent>())
				})
				.collect()
				.await
		} else {
			Vec::new()
		};

		// Re-assemble results in original order
		let mut pdu_iter = pdu_events.into_iter();
		let mut outlier_iter = outlier_events.into_iter();

		for pdu_id_res in &pdu_ids {
			if let Ok(pdu_id_handle) = pdu_id_res {
				// Result comes from timeline
				let pdu_res: Result<PduEvent> = pdu_iter
					.next()
					.expect("length matches timeline fetch count");
				match pdu_res {
					| Ok(pdu) => {
						let short = expected_shortroomid.map(|s| {
							RawPduId::from(&**pdu_id_handle).shortroomid() == s.to_be_bytes()
						});
						results.push(Self::check_room_boundary(pdu, room_id, short));
					},
					| Err(e) => results.push(Err(e)),
				}
			} else {
				// Result comes from outlier
				let outlier_res: Result<PduEvent> = outlier_iter
					.next()
					.expect("length matches outlier fetch count");
				match outlier_res {
					| Ok(pdu) => {
						results.push(Self::check_room_boundary(pdu, room_id, None));
					},
					| Err(_) => {
						results.push(Err!(Request(NotFound(
							"PDU not found in timeline or outliers"
						))));
					},
				}
			}
		}

		results
	}

	pub(super) fn multi_get_pdus<'a, S>(
		&'a self,
		room_id: Option<&'a RoomId>,
		event_ids: S,
	) -> impl Stream<Item = Result<PduEvent>> + Send + 'a
	where
		S: Stream<Item = OwnedEventId> + Send + 'a,
	{
		use conduwuit::utils::stream::{automatic_amplification, automatic_width};
		use futures::StreamExt;

		event_ids
			.boxed()
			.ready_chunks(automatic_amplification())
			.widen_then(automatic_width(), move |chunk| async move {
				self.get_pdus_in_room_batch(room_id, &chunk).await
			})
			.map(futures::stream::iter)
			.flatten()
	}

	/// Like get_non_outlier_pdu(), but without the expense of fetching and
	/// parsing the PduEvent
	#[inline]
	pub(super) async fn outlier_pdu_exists(&self, event_id: &EventId) -> Result<()> {
		let bytes = self.eventid_metadata.get(event_id.as_bytes()).await?;
		let meta: rooms::timeline::EventMetadata =
			rooms::timeline::EventMetadata::from_bincode(&bytes)
				.map_err(|e| err!(Database("corrupt metadata: {e}")))?;
		if meta.is_outlier {
			Ok(())
		} else {
			Err(err!(Request(NotFound("Not an outlier"))))
		}
	}

	/// Like get_pdu(), but without the expense of fetching and parsing the data
	pub(super) async fn pdu_exists(&self, event_id: &EventId) -> Result {
		self.eventid_pdu.exists(event_id.as_bytes()).await
	}

	/// Returns the pdu.
	///
	/// This does __NOT__ check the outliers `Tree`.
	/// If `room_id` is provided, validates the PDU belongs to that room.
	pub(super) async fn get_pdu_from_id_in_room(
		&self,
		room_id: Option<&RoomId>,
		pdu_id: &RawPduId,
	) -> Result<PduEvent> {
		let event_id_bytes = self.room_pducount_eventid.get(pdu_id).await?;
		let pdu: PduEvent = self.eventid_pdu.get(&event_id_bytes).await.deserialized()?;

		if let Some(expected_room) = room_id {
			let actual_room = pdu.room_id_or_hash();
			if let Some(actual_room) = actual_room {
				if actual_room != expected_room {
					return Err!(Database(
						"PDU does belong to room {actual_room} (expected {expected_room})"
					));
				}
			} else {
				// v12 hashed-room PDUs may not contain room_id in the JSON.
				// Verify room association by comparing ShortRoomId from pdu_id.
				let expected_shortroomid =
					self.services.short.get_shortroomid(expected_room).await?;
				if pdu_id.shortroomid() != expected_shortroomid.to_be_bytes() {
					return Err!(Database("PDU does not belong to room {expected_room}"));
				}
			}
		}

		Ok(pdu)
	}

	/// Returns the pdu as a `BTreeMap<String, CanonicalJsonValue>`.
	pub(super) async fn get_pdu_json_from_id(
		&self,
		pdu_id: &RawPduId,
	) -> Result<CanonicalJsonObject> {
		let event_id_bytes = self.room_pducount_eventid.get(pdu_id).await?;
		self.eventid_pdu.get(&event_id_bytes).await.deserialized()
	}

	#[allow(clippy::unused_self)]
	pub(super) fn db_batch(&self) -> database::Batch<'_> { database::Batch::new() }

	pub(super) fn db_apply_batch(&self, batch: database::Batch<'_>) {
		self.eventid_pdu.apply_batch(batch);
	}

	pub(super) async fn append_pdu(
		&self,
		pdu_id: &RawPduId,
		pdu: &PduEvent,
		json: &CanonicalJsonObject,
		count: PduCount,
	) {
		let mut batch = database::Batch::new();
		self.append_pdu_batch(&mut batch, pdu_id, pdu, json, count)
			.await;
		self.eventid_pdu.apply_batch(batch);
	}

	pub(super) async fn append_pdu_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		pdu_id: &RawPduId,
		pdu: &PduEvent,
		json: &CanonicalJsonObject,
		count: PduCount,
	) {
		debug_assert!(matches!(count, PduCount::Normal(_)), "PduCount not Normal");

		let event_id_bytes = pdu.event_id.as_bytes();
		let existing_metadata = if let Ok(bytes) = self.eventid_metadata.get(event_id_bytes).await
		{
			rooms::timeline::EventMetadata::from_bincode(&bytes).ok()
		} else {
			None
		};

		if let Ok(existing_pdu_id) = self
			.eventid_pduid
			.get(event_id_bytes)
			.await
			.map(|handle| RawPduId::from(&*handle))
		{
			if existing_pdu_id != *pdu_id {
				self.remove_stream_and_topo_pducount_from_batch(
					batch,
					&existing_pdu_id,
					event_id_bytes,
					existing_metadata
						.as_ref()
						.map(|meta| meta.deprecated_local_topo_depth),
				);
			}
		}

		// Map event_id -> pdu_id
		self.eventid_pduid.batch_put(batch, &event_id_bytes, pdu_id);

		self.eventid_pdu
			.batch_raw_put(batch, event_id_bytes, Json(json));

		self.room_pducount_eventid
			.batch_put(batch, pdu_id, event_id_bytes);

		let topo_key = Self::topo_pducount_key(pdu_id, pdu.depth().into());
		self.roomid_topologicalorder_pducount
			.batch_put(batch, &topo_key, event_id_bytes);

		// Integrate hotfix timestamp index into WriteBatch
		if let Some(ruma::CanonicalJsonValue::Integer(ts)) = json.get("origin_server_ts") {
			if let Ok(ts) = ruma::UInt::try_from(i64::from(*ts)) {
				let ts_key =
					pack_timestamp_key(pdu_id.shortroomid(), u64::from(ts), pdu_id.pdu_count());
				self.db["roomid_timestamp_pducount"].batch_put(batch, &ts_key, []);
			}
		}

		let metadata = rooms::timeline::EventMetadata {
			short_room_id: u64::from_be_bytes(pdu_id.shortroomid()),
			is_outlier: false,
			origin_server_ts: pdu.origin_server_ts().0,
			depth: pdu.depth(),
			status: rooms::timeline::status_from_prior(
				existing_metadata.as_ref(),
				false,
				pdu.rejected(),
			),
			redacted_by: pdu.redacts().map(ToOwned::to_owned),
			short_state_hash: existing_metadata.and_then(|m| m.short_state_hash),
			deprecated_local_topo_depth: pdu.depth().into(),
			pdu_count: Some(count.into_unsigned()),
		};
		if let Ok(metadata_bytes) = bincode::serialize(&metadata) {
			self.eventid_metadata
				.batch_put(batch, event_id_bytes, metadata_bytes);
		}

		let short_event_id = self
			.services
			.short
			.get_or_create_shorteventid(&pdu.event_id)
			.await;
		let prev_shorts: Vec<_> = self
			.services
			.short
			.multi_get_or_create_shorteventid(pdu.prev_events())
			.collect()
			.await;
		self.store_shortprevevents_into_batch(batch, short_event_id, &prev_shorts);

		let auth_shorts: Vec<_> = self
			.services
			.short
			.multi_get_or_create_shorteventid(pdu.auth_events())
			.collect()
			.await;
		self.store_shortauthevents_into_batch(batch, short_event_id, &auth_shorts);

		self.record_backward_extremities_into_batch(batch, pdu_id, pdu)
			.await;
	}

	/// Tier 3 write-time bookkeeping (see `backward_extremities.rs` and
	/// `docs/development-gg/backfill-extremities-write-time-design.md`).
	/// Called from both `append_pdu_batch` and `prepend_backfill_pdu_batch`
	/// -- every insert path in the codebase funnels through one of those
	/// two, so this is the single place this bookkeeping needs to happen.
	///
	/// Not yet read by anything (`backfill_if_required` still uses the old
	/// scan) -- this only writes the index so it's ready once the read path
	/// and the existing-room migration land. Landing the write path first
	/// and separately is intentional: it's independently testable and
	/// reviewable, and a bug here before anything reads the index is inert,
	/// where a bug in the read-path swap would not be.
	async fn record_backward_extremities_into_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		pdu_id: &RawPduId,
		pdu: &PduEvent,
	) {
		let shortroomid = pdu_id.shortroomid();

		// This event may itself have been a recorded extremity (something
		// else's missing parent). Resolve it now that it's arriving.
		let event_key = backward_extremities::pack_event_key(shortroomid, &pdu.event_id);
		if let Ok(depth_bytes) = self.db["roomid_missingeventid_depth"].get(&event_key).await {
			if let Some(depth) = backward_extremities::unpack_depth_value(&depth_bytes) {
				let depth_key =
					backward_extremities::pack_depth_key(shortroomid, depth, &pdu.event_id);
				self.db["roomid_depth_missingeventid"].batch_delete(batch, &depth_key);
			}
			self.db["roomid_missingeventid_depth"].batch_delete(batch, &event_key);
		}

		// `get_pdu_id` is async, but `missing_prev_events` takes a sync
		// predicate on purpose (see its doc comment -- that's what keeps it
		// unit-testable without a DB). Resolve existence for every
		// prev_event first, then hand the pure function a synchronous
		// lookup over that already-resolved set.
		let mut known_locally: HashSet<OwnedEventId> = HashSet::new();
		for prev_id in &pdu.prev_events {
			if self.get_pdu_id(prev_id).await.is_ok() {
				known_locally.insert(prev_id.clone());
			}
		}

		let depth = u64::from(pdu.depth());
		for prev_id in backward_extremities::missing_prev_events(&pdu.prev_events, |id| {
			known_locally.contains(id)
		}) {
			let depth_key = backward_extremities::pack_depth_key(shortroomid, depth, prev_id);
			let event_key = backward_extremities::pack_event_key(shortroomid, prev_id);
			self.db["roomid_depth_missingeventid"].batch_put(batch, &depth_key, []);
			self.db["roomid_missingeventid_depth"].batch_put(
				batch,
				&event_key,
				depth.to_be_bytes(),
			);
		}
	}

	pub(super) async fn prepend_backfill_pdu(
		&self,
		pdu_id: &RawPduId,
		event_id: &EventId,
		json: &CanonicalJsonObject,
		pdu: &PduEvent,
	) {
		let mut batch = database::Batch::new();
		self.prepend_backfill_pdu_batch(&mut batch, pdu_id, event_id, json, pdu)
			.await;
		self.eventid_pdu.apply_batch(batch);
	}

	pub(super) async fn prepend_backfill_pdu_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		pdu_id: &RawPduId,
		event_id: &EventId,
		json: &CanonicalJsonObject,
		pdu: &PduEvent,
	) {
		let event_id_bytes = event_id.as_bytes();
		let existing_metadata = if let Ok(bytes) = self.eventid_metadata.get(event_id_bytes).await
		{
			rooms::timeline::EventMetadata::from_bincode(&bytes).ok()
		} else {
			None
		};

		if let Ok(existing_pdu_id) = self
			.eventid_pduid
			.get(event_id_bytes)
			.await
			.map(|handle| RawPduId::from(&*handle))
		{
			if existing_pdu_id != *pdu_id {
				self.remove_stream_and_topo_pducount_from_batch(
					batch,
					&existing_pdu_id,
					event_id_bytes,
					existing_metadata
						.as_ref()
						.map(|meta| meta.deprecated_local_topo_depth),
				);
			}
		}

		self.eventid_pduid.batch_put(batch, &event_id_bytes, pdu_id);

		self.eventid_pdu
			.batch_raw_put(batch, event_id_bytes, Json(json));
		self.room_pducount_eventid
			.batch_put(batch, pdu_id, event_id_bytes);

		let topo_key = Self::topo_pducount_key(pdu_id, pdu.depth().into());
		self.roomid_topologicalorder_pducount
			.batch_put(batch, &topo_key, event_id_bytes);

		// Integrate hotfix timestamp index into WriteBatch
		if let Some(ruma::CanonicalJsonValue::Integer(ts)) = json.get("origin_server_ts") {
			if let Ok(ts) = ruma::UInt::try_from(i64::from(*ts)) {
				let ts_key =
					pack_timestamp_key(pdu_id.shortroomid(), u64::from(ts), pdu_id.pdu_count());
				self.db["roomid_timestamp_pducount"].batch_put(batch, &ts_key, []);
			}
		}

		let metadata = rooms::timeline::EventMetadata {
			short_room_id: u64::from_be_bytes(pdu_id.shortroomid()),
			is_outlier: false,
			origin_server_ts: pdu.origin_server_ts().0,
			depth: pdu.depth(),
			status: rooms::timeline::status_from_prior(
				existing_metadata.as_ref(),
				false,
				pdu.rejected(),
			),
			redacted_by: pdu.redacts().map(ToOwned::to_owned),
			short_state_hash: existing_metadata.and_then(|m| m.short_state_hash),
			deprecated_local_topo_depth: pdu.depth().into(),
			pdu_count: match pdu_id.pdu_count() {
				| PduCount::Normal(x) => Some(x),
				| PduCount::Backfilled(_) => None,
			},
		};
		if let Ok(metadata_bytes) = bincode::serialize(&metadata) {
			self.eventid_metadata
				.batch_put(batch, event_id_bytes, metadata_bytes);
		}

		let short_event_id = self
			.services
			.short
			.get_or_create_shorteventid(event_id)
			.await;

		let prev_shorts: Vec<_> = self
			.services
			.short
			.multi_get_or_create_shorteventid(pdu.prev_events())
			.collect()
			.await;
		self.store_shortprevevents_into_batch(batch, short_event_id, &prev_shorts);

		let auth_shorts: Vec<_> = self
			.services
			.short
			.multi_get_or_create_shorteventid(pdu.auth_events())
			.collect()
			.await;
		self.store_shortauthevents_into_batch(batch, short_event_id, &auth_shorts);

		self.record_backward_extremities_into_batch(batch, pdu_id, pdu)
			.await;
	}

	/// Removes a pdu and creates a new one with the same id.
	pub(super) async fn replace_pdu(
		&self,
		pdu_id: &RawPduId,
		pdu_json: &CanonicalJsonObject,
		event_id: &EventId,
	) -> Result {
		if self.room_pducount_eventid.get(pdu_id).await.is_not_found() {
			return Err!(Request(NotFound("PDU does not exist.")));
		}

		let mut batch = database::Batch::new();

		let event_id_bytes = event_id.as_bytes();

		// --- Phase 1: Double-Write ---
		self.eventid_pdu
			.batch_raw_put(&mut batch, event_id_bytes, Json(pdu_json));

		if let Ok(pdu) =
			serde_json::from_value::<PduEvent>(serde_json::to_value(pdu_json).unwrap())
		{
			let existing_metadata =
				if let Ok(bytes) = self.eventid_metadata.get(event_id_bytes).await {
					rooms::timeline::EventMetadata::from_bincode(&bytes).ok()
				} else {
					None
				};

			let topo_key = Self::topo_pducount_key(pdu_id, pdu.depth().into());
			self.roomid_topologicalorder_pducount.batch_put(
				&mut batch,
				&topo_key,
				event_id_bytes,
			);

			let metadata = rooms::timeline::EventMetadata {
				short_room_id: u64::from_be_bytes(pdu_id.shortroomid()),
				is_outlier: false,
				origin_server_ts: pdu.origin_server_ts().0,
				depth: pdu.depth(),
				status: rooms::timeline::status_from_prior(
					existing_metadata.as_ref(),
					false,
					pdu.rejected(),
				),
				redacted_by: pdu.redacts().map(ToOwned::to_owned),
				short_state_hash: existing_metadata.and_then(|m| m.short_state_hash),
				deprecated_local_topo_depth: pdu.depth().into(),
				pdu_count: match pdu_id.pdu_count() {
					| PduCount::Normal(x) => Some(x),
					| PduCount::Backfilled(_) => None,
				},
			};
			if let Ok(metadata_bytes) = bincode::serialize(&metadata) {
				self.eventid_metadata
					.batch_put(&mut batch, event_id_bytes, metadata_bytes);
			}

			let short_event_id = self
				.services
				.short
				.get_or_create_shorteventid(event_id)
				.await;
			let prev_shorts: Vec<_> = self
				.services
				.short
				.multi_get_or_create_shorteventid(pdu.prev_events())
				.collect()
				.await;
			self.store_shortprevevents_into_batch(&mut batch, short_event_id, &prev_shorts);

			let auth_shorts: Vec<_> = self
				.services
				.short
				.multi_get_or_create_shorteventid(pdu.auth_events())
				.collect()
				.await;
			self.store_shortauthevents_into_batch(&mut batch, short_event_id, &auth_shorts);
		}

		self.eventid_pdu.apply_batch(batch);
		Ok(())
	}

	/// Returns an iterator over all events and their tokens in a room that
	/// happened before (and optionally including) `until`, in
	/// reverse-chronological order.
	///
	/// `until` states its own inclusivity, so there is no separate "which way
	/// do I bump this" step for callers to get backwards:
	/// - `Bound::Excluded(count)`: the event at `count` is never yielded.
	/// - `Bound::Included(count)`: the event at `count` is yielded first.
	/// - `Bound::Unbounded`: start from the newest event in the room.
	pub(super) fn pdus_rev<'a>(
		&'a self,
		room_id: &'a RoomId,
		until: Bound<PduCount>,
	) -> impl Stream<Item = Result<PdusIterItem>> + Send + 'a {
		let seek_count = pdus_rev_exclusive_until(until).saturating_inc(Direction::Backward);
		self.count_to_id(room_id, seek_count, Direction::Backward)
			.map_ok(move |current| {
				let prefix = current.shortroomid();
				self.room_pducount_eventid
					.rev_raw_stream_from(&current)
					.ready_try_take_while(move |(key, _)| Ok(key.starts_with(&prefix)))
					// Clone raw bytes to owned before async resolve to avoid
					// RocksDB cursor invalidation through try_buffered
					.map_ok(|(key, val)| (key.to_vec(), val.to_vec()))
					.and_then(move |(key, val)| async move {
						self.resolve_pdu((&key, &val)).await
					})
			})
			.inspect_err(|e| conduwuit::warn!("pdus_rev count_to_id failed: {e}"))
			.try_flatten_stream()
	}

	/// Returns an iterator over all events and their tokens in a room that
	/// happened after (and optionally including) `from`, in chronological
	/// order.
	///
	/// `from` states its own inclusivity — see `pdus_rev`'s doc comment.
	/// Forward and reverse iteration need *opposite-signed* adjustments to
	/// achieve the same inclusivity (this one seeks `+1` for `Excluded`
	/// where `pdus_rev` seeks `-1`), which is exactly the trap that made
	/// this boundary handling worth centralizing here instead of leaving it
	/// to call sites: see the `Bound` match below and its mirror in
	/// `pdus_rev` above.
	pub(super) fn pdus<'a>(
		&'a self,
		room_id: &'a RoomId,
		from: Bound<PduCount>,
	) -> impl Stream<Item = Result<PdusIterItem>> + Send + 'a {
		let from = pdus_exclusive_from(from);
		self.count_to_id(room_id, from.saturating_inc(Direction::Forward), Direction::Forward)
			.map_ok(move |current| {
				let prefix = current.shortroomid();
				self.room_pducount_eventid
					.raw_stream_from(&current)
					.ready_try_take_while(move |(key, _)| Ok(key.starts_with(&prefix)))
					// Clone raw bytes to owned before async resolve to avoid
					// RocksDB cursor invalidation through try_buffered
					.map_ok(|(key, val)| (key.to_vec(), val.to_vec()))
					.and_then(move |(key, val)| async move {
						self.resolve_pdu((&key, &val)).await
					})
			})
			.try_flatten_stream()
	}
}

/// Resolves `pdus_rev`'s `until` bound to the exclusive count its seek needs.
///
/// Pulled out of `pdus_rev` as a free, DB-free function specifically so the
/// boundary arithmetic can be unit tested without a database — this exact
/// arithmetic has regressed twice (`cf208c1a5`, `f1415e22a`), both times only
/// caught by slow integration tests whose failures looked environmental. See
/// `docs/development-gg/fable/boundary-flake-advisory.md`.
fn pdus_rev_exclusive_until(until: Bound<PduCount>) -> PduCount {
	match until {
		| Bound::Excluded(count) => count,
		| Bound::Included(count) => count.saturating_inc(Direction::Forward),
		| Bound::Unbounded => PduCount::max(),
	}
}

/// Resolves `pdus`'s `from` bound to the exclusive count its seek needs.
///
/// Mirrors `pdus_rev_exclusive_until` but with the *opposite-signed*
/// adjustment for `Bound::Included` — forward iteration needs to step
/// backward to include its boundary where reverse iteration steps forward.
/// This asymmetry is the actual trap in this API (see the advisory doc); the
/// paired test `pdus_rev_and_pdus_bound_adjustments_are_mirror_opposite`
/// pins it.
fn pdus_exclusive_from(from: Bound<PduCount>) -> PduCount {
	match from {
		| Bound::Excluded(count) => count,
		| Bound::Included(count) => count.saturating_inc(Direction::Backward),
		| Bound::Unbounded => PduCount::min(),
	}
}

#[cfg(test)]
mod boundary_tests {
	use std::ops::Bound;

	use conduwuit::PduCount;

	use super::{pdus_exclusive_from, pdus_rev_exclusive_until};

	#[test]
	fn pdus_rev_excluded_is_passthrough() {
		let mid = PduCount::Normal(42);
		assert_eq!(pdus_rev_exclusive_until(Bound::Excluded(mid)), mid);
	}

	#[test]
	fn pdus_rev_included_steps_forward_past_the_boundary() {
		// pdus_rev's underlying seek is exclusive, so to make `mid` the first
		// (most recent) yielded event, the resolved count must be one past
		// `mid` in the direction pdus_rev walks away from (i.e. +1).
		let mid = PduCount::Normal(42);
		assert_eq!(pdus_rev_exclusive_until(Bound::Included(mid)), PduCount::Normal(43));
	}

	#[test]
	fn pdus_rev_unbounded_is_max() {
		assert_eq!(pdus_rev_exclusive_until(Bound::Unbounded), PduCount::max());
	}

	#[test]
	fn pdus_excluded_is_passthrough() {
		let mid = PduCount::Normal(42);
		assert_eq!(pdus_exclusive_from(Bound::Excluded(mid)), mid);
	}

	#[test]
	fn pdus_included_steps_backward_past_the_boundary() {
		// Opposite of pdus_rev: pdus walks forward away from `from`, so
		// including `from` itself means resolving to one *before* it (-1).
		let mid = PduCount::Normal(42);
		assert_eq!(pdus_exclusive_from(Bound::Included(mid)), PduCount::Normal(41));
	}

	#[test]
	fn pdus_unbounded_is_min() {
		assert_eq!(pdus_exclusive_from(Bound::Unbounded), PduCount::min());
	}

	/// This is the test that would have caught both `cf208c1a5` (flipped
	/// `pdus`/`pdus_rev` to inclusive-by-default, breaking every caller that
	/// already compensated manually) and `f1415e22a` (dropped the
	/// compensation entirely, breaking backfill's gap scan) in milliseconds,
	/// instead of via three flaky-looking complement test families.
	///
	/// `pdus_rev`'s `Bound::Included` adjustment and `pdus`'s `Bound::Included`
	/// adjustment must move in *opposite* directions for the same boundary
	/// count, because the two functions walk away from that boundary in
	/// opposite directions. A "fix" that makes both add (or both subtract) is
	/// wrong for one of the two, silently, at whichever call site adopts it
	/// next.
	#[test]
	fn pdus_rev_and_pdus_bound_adjustments_are_mirror_opposite() {
		let boundary = PduCount::Normal(100);

		let rev_resolved = pdus_rev_exclusive_until(Bound::Included(boundary));
		let fwd_resolved = pdus_exclusive_from(Bound::Included(boundary));

		assert_eq!(rev_resolved, PduCount::Normal(101), "pdus_rev must step +1 to include");
		assert_eq!(fwd_resolved, PduCount::Normal(99), "pdus must step -1 to include");
		assert_ne!(
			rev_resolved, fwd_resolved,
			"pdus_rev and pdus resolve an Included(boundary) to different counts by design -- a \
			 shared helper that returns one value for both directions is the exact bug this \
			 test guards against"
		);
	}

	/// Sparse, non-adjacent counts (simulating the global event counter
	/// interleaving multiple rooms' events, as it does in production and in
	/// the `TestJumpToDateEndpoint` parallel subtests) — the resolved seek
	/// count doesn't need to correspond to a real event for the arithmetic
	/// to be correct; `count_to_id` handles rounding to the nearest actual
	/// event. This just pins that the arithmetic itself doesn't assume
	/// adjacency.
	#[test]
	fn bound_resolution_does_not_assume_adjacent_counts() {
		let sparse = PduCount::Normal(17);
		assert_eq!(pdus_rev_exclusive_until(Bound::Included(sparse)), PduCount::Normal(18));
		assert_eq!(pdus_exclusive_from(Bound::Included(sparse)), PduCount::Normal(16));
	}

	/// Class-boundary case: including the newest backfilled event should
	/// stay in the `Backfilled` variant (see `Count::saturating_add`), not
	/// jump to `Normal`. Backward pagination tokens routinely sit in the
	/// backfilled range (this repo's regression tokens included `t7_-114`),
	/// so this is not a hypothetical edge.
	#[test]
	fn pdus_rev_included_backfilled_boundary_stays_backfilled() {
		let boundary = PduCount::Backfilled(-1);
		assert_eq!(pdus_rev_exclusive_until(Bound::Included(boundary)), PduCount::Backfilled(0));
	}
}

impl Data {
	/// Resolve a (pdu_id, event_id_bytes) pair from `room_pducount_eventid`
	/// into a full `PdusIterItem` by looking up the PDU JSON in
	/// `eventid_pdu`.
	async fn resolve_pdu(&self, (pdu_id, event_id_bytes): KeyVal<'_>) -> Result<PdusIterItem> {
		let json_bytes = match self.eventid_pdu.get(&event_id_bytes).await {
			| Ok(h) => h,
			| Err(e) => {
				return Err(e);
			},
		};
		Self::parse_json_slice(None, (pdu_id, json_bytes.as_ref()))
	}

	/// Resolve a batch of `pdu_id`s via the two-hop path:
	/// `room_pducount_eventid` → event_id_bytes → `eventid_pdu` → PduEvent.
	async fn resolve_pdu_batch(&self, pdu_ids: &[RawPduId]) -> Vec<Result<PduEvent>> {
		use futures::StreamExt;

		if pdu_ids.is_empty() {
			return Vec::new();
		}

		let event_id_batch: Vec<Result<database::Handle<'_>>> = self
			.room_pducount_eventid
			.get_batch(futures::stream::iter(pdu_ids.iter().map(AsRef::as_ref)))
			.collect()
			.await;

		let mut results = Vec::with_capacity(event_id_batch.len());
		for res in event_id_batch {
			match res {
				| Ok(event_id_handle) => {
					results.push(
						self.eventid_pdu
							.get(&*event_id_handle)
							.await
							.and_then(|h| h.deserialized::<PduEvent>()),
					);
				},
				| Err(e) => results.push(Err(e)),
			}
		}
		results
	}

	/// Validate that a PDU belongs to the expected room.
	/// `shortroomid_match` is a pre-computed fallback check for v12 PDUs
	/// without room_id in the JSON. Pass `None` to skip the shortid check.
	fn check_room_boundary(
		pdu: PduEvent,
		expected_room: Option<&RoomId>,
		shortroomid_match: Option<bool>,
	) -> Result<PduEvent> {
		let Some(expected_room) = expected_room else {
			return Ok(pdu);
		};

		if let Some(actual_room) = pdu.room_id_or_hash() {
			if actual_room != expected_room {
				return Err!(Database(
					"PDU {} belongs to room {actual_room} (expected {expected_room})",
					pdu.event_id()
				));
			}
		} else if let Some(matches) = shortroomid_match {
			if !matches {
				return Err!(Database(
					"PDU {} does not belong to room {expected_room}",
					pdu.event_id()
				));
			}
		}

		Ok(pdu)
	}

	pub(super) fn topo_pdus_rev<'a>(
		&'a self,
		room_id: &'a RoomId,
		until: TopoToken,
	) -> impl Stream<Item = Result<TopoIterItem>> + Send + 'a {
		let stream = async move {
			let prefix = self
				.services
				.short
				.get_shortroomid(room_id)
				.await?
				.to_be_bytes()
				.to_vec();

			let current = self
				.count_to_id(room_id, until.pdu_count, Direction::Backward)
				.await?;

			let token_topo_key = if until.is_legacy() {
				None
			} else {
				let token_pdu_id = self
					.count_to_id(room_id, until.pdu_count, Direction::Backward)
					.await?;
				Some(Self::topo_pducount_key(&token_pdu_id, until.depth))
			};

			let topo_key = if until.is_legacy() {
				// Legacy tokens don't have depth, fallback to the old buggy behavior just for
				// them
				self.legacy_seek_topo_key(room_id, until.pdu_count, &current, Direction::Backward)
					.await?
			} else {
				// Resume concrete topo tokens from the top of the room's topo index, then
				// trim by the exact token boundary below. Seeking directly from
				// `(until.depth, until.count)` misses older events which are inserted later
				// (e.g. backfill gap-fillers) but sort *before* the stale token.
				Self::topo_pducount_key(&current, u64::MAX)
			};

			conduwuit::debug!(
				target: "pagination_debug",
				%room_id, until_depth = until.depth, until_pdu_count = ?until.pdu_count,
				is_legacy = until.is_legacy(), seek_key = ?topo_key,
				"topo_pdus_rev: seeking"
			);

			// Legacy tokens are stream positions, not concrete topo cursors. When
			// seeking them from u64::MAX depth, exclude events which arrived after the
			// sync position by count. Concrete t<depth>_<count> tokens instead use the
			// inclusive topo boundary filter below, which still admits older events
			// inserted later with higher stream positions.
			let count_ceiling = until.is_legacy().then_some(until.pdu_count);

			let raw_stream = self
				.roomid_topologicalorder_pducount
				.rev_raw_stream_from(&topo_key)
				.ready_try_filter_map(move |(key, val)| match &token_topo_key {
					| Some(token_topo_key) if key > token_topo_key.as_slice() => Ok(None),
					| _ => Ok(Some((key, val))),
				});
			Ok(self
				.parse_topo_stream(raw_stream, prefix)
				.ready_try_filter_map(move |item| match count_ceiling {
					| Some(ceiling) if item.0.pdu_count >= ceiling => Ok(None),
					| _ => Ok(Some(item)),
				}))
		};
		stream.try_flatten_stream()
	}

	pub(super) fn topo_pdus<'a>(
		&'a self,
		room_id: &'a RoomId,
		from: TopoToken,
	) -> impl Stream<Item = Result<TopoIterItem>> + Send + 'a {
		let stream = async move {
			let prefix = self
				.services
				.short
				.get_shortroomid(room_id)
				.await?
				.to_be_bytes()
				.to_vec();

			let topo_key = if from.is_legacy() {
				// Legacy tokens don't have depth, fallback to the old buggy behavior just for
				// them
				self.count_to_id(
					room_id,
					from.pdu_count.saturating_inc(Direction::Forward),
					Direction::Forward,
				)
				.and_then(move |current| async move {
					self.legacy_seek_topo_key(
						room_id,
						from.pdu_count,
						&current,
						Direction::Forward,
					)
					.await
				})
				.await?
			} else {
				let current = self
					.count_to_id(
						room_id,
						from.pdu_count.saturating_inc(Direction::Forward),
						Direction::Forward,
					)
					.await?;
				Self::topo_pducount_key(&current, from.depth)
			};

			conduwuit::debug!(
				target: "pagination_debug",
				%room_id, from_depth = from.depth, from_pdu_count = ?from.pdu_count,
				is_legacy = from.is_legacy(), seek_key = ?topo_key,
				"topo_pdus: seeking"
			);

			let count_floor = from.is_legacy().then_some(from.pdu_count);

			let raw_stream = self
				.roomid_topologicalorder_pducount
				.raw_stream_from(&topo_key);
			Ok(self
				.parse_topo_stream(raw_stream, prefix)
				.ready_try_filter_map(move |item| match count_floor {
					| Some(floor) if item.0.pdu_count <= floor => Ok(None),
					| _ => Ok(Some(item)),
				}))
		};
		stream.try_flatten_stream()
	}

	fn parse_json_slice(
		room_id: Option<&RoomId>,
		(pdu_id, pdu): KeyVal<'_>,
	) -> Result<PdusIterItem> {
		let pdu_id: RawPduId = pdu_id.into();
		let pdu = match serde_json::from_slice::<PduEvent>(pdu) {
			| Ok(p) => p,
			| Err(e) => {
				conduwuit::warn!(
					"parse_json_slice failed: {e}. JSON: {}",
					String::from_utf8_lossy(pdu)
				);
				return Err(e.into());
			},
		};

		// Check for room ID
		if let Some(expected_room) = room_id {
			if pdu
				.room_id_or_hash()
				.is_some_and(|actual| actual != expected_room)
			{
				return Err(conduwuit::err!(Database(
					"PDU belongs to room {} (expected {expected_room})",
					pdu.room_id_or_hash().expect("just checked")
				)));
			}
		}

		Ok((pdu_id.pdu_count(), pdu))
	}

	pub(super) fn increment_notification_counts(
		&self,
		room_id: &RoomId,
		notifies: Vec<OwnedUserId>,
		highlights: Vec<OwnedUserId>,
		thread_root: Option<&EventId>,
	) {
		let _cork = self.db.cork();

		for user in notifies {
			match thread_root {
				| Some(thread_root) => {
					Self::increment_thread(
						&self.userroomid_notificationcount,
						(user.as_ref(), room_id, thread_root),
					);
				},
				| None => {
					let mut userroom_id = user.as_bytes().to_vec();
					userroom_id.push(0xFF);
					userroom_id.extend_from_slice(room_id.as_bytes());
					increment(&self.userroomid_notificationcount, &userroom_id);
				},
			}
		}

		for user in highlights {
			match thread_root {
				| Some(thread_root) => {
					Self::increment_thread(
						&self.userroomid_highlightcount,
						(user.as_ref(), room_id, thread_root),
					);
				},
				| None => {
					let mut userroom_id = user.as_bytes().to_vec();
					userroom_id.push(0xFF);
					userroom_id.extend_from_slice(room_id.as_bytes());
					increment(&self.userroomid_highlightcount, &userroom_id);
				},
			}
		}
	}

	fn increment_thread(db: &Arc<Map>, key: (&UserId, &RoomId, &EventId)) {
		let key = serialize_key(key).expect("failed to serialize thread notification key");
		increment(db, &key);
	}

	async fn count_to_id(
		&self,
		room_id: &RoomId,
		shorteventid: PduCount,
		_dir: Direction,
	) -> Result<RawPduId> {
		let shortroomid: ShortRoomId = self
			.services
			.short
			.get_shortroomid(room_id)
			.await
			.map_err(|e| err!(Request(NotFound("Room {room_id:?} not found: {e:?}"))))?;

		let pdu_id = PduId { shortroomid, shorteventid };

		Ok(pdu_id.into())
	}

	async fn legacy_seek_topo_key(
		&self,
		room_id: &RoomId,
		token: PduCount,
		current: &RawPduId, // This is token +/- 1
		dir: Direction,
	) -> Result<Vec<u8>> {
		use futures::StreamExt;

		if token == PduCount::max() {
			Ok(Self::topo_pducount_key(current, u64::MAX))
		} else if token == PduCount::min() {
			Ok(Self::topo_pducount_key(current, 0))
		} else {
			let token_pdu_id = self.count_to_id(room_id, token, dir).await?;

			let token_depth = match self.pdu_id_to_depth(&token_pdu_id).await {
				| Ok(depth) => depth,
				| Err(_) => {
					// Fallback: find the nearest existing event in the requested direction
					let prefix = current.shortroomid();

					let nearest_pdu_id = if dir == Direction::Forward {
						let mut stream = Box::pin(
							self.room_pducount_eventid
								.raw_stream_from(&token_pdu_id)
								.ready_try_take_while(|(k, _)| Ok(k.starts_with(&prefix))),
						);
						stream
							.next()
							.await
							.map(|res| res.map(|(k, _)| RawPduId::from(k)))
					} else {
						let mut stream = Box::pin(
							self.room_pducount_eventid
								.rev_raw_stream_from(&token_pdu_id)
								.ready_try_take_while(|(k, _)| Ok(k.starts_with(&prefix))),
						);
						stream
							.next()
							.await
							.map(|res| res.map(|(k, _)| RawPduId::from(k)))
					};

					if let Some(Ok(nearest_pdu_id)) = nearest_pdu_id {
						if let Ok(depth) = self.pdu_id_to_depth(&nearest_pdu_id).await {
							// Return EXACT depth and EXACT nearest_pdu_id to prevent skipping OR
							// time-traveling!
							return Ok(Self::topo_pducount_key(&nearest_pdu_id, depth));
						}
					}

					// If no nearest event found in DAG, fallback without guessing depths
					if dir == Direction::Forward { u64::MAX } else { 0 }
				},
			};

			// For backward pagination, start from the TOP of the topo index
			// (u64::MAX depth) so we capture events at ANY depth — including
			// high-depth remote branch events. The stream count filter in
			// topo_pdus_rev ensures we don't return events that arrived
			// after the sync position.
			//
			// For forward pagination, use exact depth to avoid re-scanning.
			let seek_depth = match dir {
				| Direction::Backward => u64::MAX,
				| Direction::Forward => token_depth,
			};

			Ok(Self::topo_pducount_key(current, seek_depth))
		}
	}

	fn parse_topo_stream<'a>(
		&'a self,
		stream: impl Stream<Item = Result<KeyVal<'a>>> + Send + 'a,
		prefix: Vec<u8>,
	) -> impl Stream<Item = Result<TopoIterItem>> + Send + 'a {
		stream
			.ready_try_take_while(move |(key, _)| Ok(key.starts_with(&prefix)))
			// Clone raw bytes to owned before async resolve to avoid
			// RocksDB cursor invalidation through try_buffered
			.map_ok(|(key, val)| (key.to_vec(), val.to_vec()))
			.try_filter_map(move |(topo_key, event_id_bytes)| async move {
				let mut timeline_bytes = [0_u8; 16];
				timeline_bytes.copy_from_slice(&topo_key[8..24]);
				let timeline_key = TimelineKey::from_bytes(&timeline_bytes);
				let depth = timeline_key.depth;

				let pdu_id = Self::topo_key_to_pdu_id(&topo_key);
				let json_bytes = self.eventid_pdu.get(&event_id_bytes).await?;
				let (pdu_count, pdu) = Self::parse_json_slice(None, (pdu_id.as_ref(), json_bytes.as_ref()))?;
				let metadata_bytes = self.eventid_metadata.get(&event_id_bytes).await?;
				let Ok(metadata) = rooms::timeline::EventMetadata::from_bincode(&metadata_bytes) else {
					conduwuit::debug!(
						target: "pagination_debug",
						event_id = %String::from_utf8_lossy(&event_id_bytes),
						?depth, ?pdu_count,
						"parse_topo_stream: DROPPED (metadata deserialize failed)"
					);
					return Ok(None);
				};
				if !metadata.matches_timeline_position(depth, pdu_count) {
					conduwuit::debug!(
						target: "pagination_debug",
						event_id = %String::from_utf8_lossy(&event_id_bytes),
						key_depth = depth,
						key_pdu_count = ?pdu_count,
						meta_depth = metadata.deprecated_local_topo_depth,
						meta_pdu_count = ?metadata.pdu_count,
						meta_is_outlier = metadata.is_outlier,
						"parse_topo_stream: DROPPED (matches_timeline_position == false)"
					);
					return Ok(None);
				}

				// `EventMetadata::pdu_count` never records the exact negative counter
				// for a Backfilled event (see its doc comment), so
				// `matches_timeline_position` treats `None` as matching *any*
				// Backfilled key at the right depth -- it can't by itself tell a
				// stale/orphaned topo entry (left behind by a reindex/reorder that
				// moved this event_id to a different depth or counter) from the
				// entry that is actually this event's current position. Cross-check
				// against `eventid_pduid`, which every write site (append_pdu_batch,
				// prepend_backfill_pdu_batch, reindex.rs, reorder.rs) keeps pointed
				// at the event's live position, and drop the key if it disagrees.
				if matches!(pdu_count, PduCount::Backfilled(_)) {
					let Ok(canonical_id) = self.eventid_pduid.get(&event_id_bytes).await else {
						conduwuit::debug!(
							target: "pagination_debug",
							event_id = %String::from_utf8_lossy(&event_id_bytes),
							?depth, ?pdu_count,
							"parse_topo_stream: DROPPED (no eventid_pduid entry for backfilled event)"
						);
						return Ok(None);
					};
					if RawPduId::from(&*canonical_id) != pdu_id {
						conduwuit::debug!(
							target: "pagination_debug",
							event_id = %String::from_utf8_lossy(&event_id_bytes),
							?depth, ?pdu_count,
							canonical_id = ?RawPduId::from(&*canonical_id),
							"parse_topo_stream: DROPPED (stale backfilled topo entry, event has moved)"
						);
						return Ok(None);
					}
				}

				conduwuit::debug!(
					target: "pagination_debug",
					event_id = %String::from_utf8_lossy(&event_id_bytes),
					?depth, ?pdu_count,
					"parse_topo_stream: yielding"
				);

				Ok(Some((TopoToken { depth, pdu_count }, pdu)))
			})
	}

	pub(super) fn room_event_ids_rev<'a>(
		&'a self,
		room_id: &'a RoomId,
		until: Option<PduCount>,
	) -> impl Stream<Item = Result<OwnedEventId>> + Send + 'a {
		let seek_count = until
			.unwrap_or_else(PduCount::max)
			.saturating_inc(Direction::Backward);
		self.count_to_id(room_id, seek_count, Direction::Backward)
			.map_ok(move |current| {
				let prefix = current.shortroomid();
				self.room_pducount_eventid
					.rev_raw_stream_from(&current)
					.ready_try_take_while(move |(key, _)| Ok(key.starts_with(&prefix)))
					.map_ok(|(_key, val)| val.to_vec())
					.and_then(move |val| async move {
						let s = std::str::from_utf8(&val)
							.map_err(|e| err!(Database("Invalid UTF-8 in event ID: {e:?}")))?;
						OwnedEventId::parse(s)
							.map_err(|e| err!(Database("Invalid EventId: {e:?}")))
					})
			})
			.try_flatten_stream()
	}

	pub(super) fn room_shorteventids_rev<'a>(
		&'a self,
		room_id: &'a RoomId,
		until: Option<PduCount>,
	) -> impl Stream<Item = Result<rooms::short::ShortEventId>> + Send + 'a {
		let seek_count = until
			.unwrap_or_else(PduCount::max)
			.saturating_inc(Direction::Backward);
		self.count_to_id(room_id, seek_count, Direction::Backward)
			.map_ok(move |current| {
				let prefix = current.shortroomid();
				self.room_pducount_eventid
					.rev_raw_stream_from(&current)
					.ready_try_take_while(move |(key, _)| Ok(key.starts_with(&prefix)))
					.and_then(move |(_key, val)| async move {
						let s = std::str::from_utf8(val)
							.map_err(|e| err!(Database("Invalid event id utf8: {e:?}")))?;
						let event_id = <&EventId>::try_from(s)
							.map_err(|e| err!(Database("Invalid event id bytes: {e:?}")))?;
						self.services
							.short
							.get_shorteventid(event_id)
							.await
							.map_err(|e| err!(Database("Missing short event id: {e}")))
					})
			})
			.try_flatten_stream()
	}

	#[allow(dead_code)]
	pub(super) fn store_shortprevevents(
		&self,
		shorteventid: rooms::short::ShortEventId,
		shortprevevents: &[rooms::short::ShortEventId],
	) {
		let key = shorteventid.to_be_bytes();
		let val = shortprevevents
			.iter()
			.flat_map(|s| s.to_be_bytes())
			.collect::<Vec<u8>>();
		self.shorteventid_shortprevevents.insert(&key, &val);
	}

	pub(super) fn store_shortprevevents_into_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		shorteventid: rooms::short::ShortEventId,
		shortprevevents: &[rooms::short::ShortEventId],
	) {
		let key = shorteventid.to_be_bytes();
		let val = shortprevevents
			.iter()
			.flat_map(|s| s.to_be_bytes())
			.collect::<Vec<u8>>();
		self.shorteventid_shortprevevents
			.batch_put(batch, &key, &val);
	}

	pub(super) async fn get_shortprevevents(
		&self,
		shorteventid: rooms::short::ShortEventId,
	) -> Result<Vec<rooms::short::ShortEventId>> {
		let key = shorteventid.to_be_bytes();
		let val = self.shorteventid_shortprevevents.get(&key).await?;
		let prev_shorts = val
			.as_chunks::<{ size_of::<u64>() }>()
			.0
			.iter()
			.map(|c| u64::from_be_bytes(*c))
			.collect();
		Ok(prev_shorts)
	}

	pub(super) fn store_shortauthevents(
		&self,
		shorteventid: rooms::short::ShortEventId,
		shortauthevents: &[rooms::short::ShortEventId],
	) {
		let key = shorteventid.to_be_bytes();
		let val = shortauthevents
			.iter()
			.flat_map(|s| s.to_be_bytes())
			.collect::<Vec<u8>>();
		self.shorteventid_shortauthevents.insert(&key, &val);
	}

	pub(super) async fn get_shortauthevents(
		&self,
		shorteventid: rooms::short::ShortEventId,
	) -> Result<Vec<rooms::short::ShortEventId>> {
		let key = shorteventid.to_be_bytes();
		let val = self.shorteventid_shortauthevents.get(&key).await?;
		let auth_shorts = val
			.as_chunks::<{ size_of::<u64>() }>()
			.0
			.iter()
			.map(|c| u64::from_be_bytes(*c))
			.collect();
		Ok(auth_shorts)
	}

	pub(super) fn store_shortauthevents_into_batch<'a>(
		&'a self,
		batch: &mut database::Batch<'a>,
		shorteventid: rooms::short::ShortEventId,
		shortauthevents: &[rooms::short::ShortEventId],
	) {
		let key = shorteventid.to_be_bytes();
		let val = shortauthevents
			.iter()
			.flat_map(|s| s.to_be_bytes())
			.collect::<Vec<u8>>();
		self.shorteventid_shortauthevents
			.batch_put(batch, &key, &val);
	}

	pub(super) fn multi_get_shortprevevents<'a, I>(
		&'a self,
		shorteventids: I,
	) -> impl Stream<Item = Result<Vec<rooms::short::ShortEventId>>> + Send + 'a
	where
		I: Stream<Item = rooms::short::ShortEventId> + Send + 'a,
	{
		use futures::StreamExt;
		self.shorteventid_shortprevevents
			.get_batch(shorteventids.map(u64::to_be_bytes))
			.map(|res| {
				let val = res?;
				let prev_shorts = val
					.as_chunks::<{ size_of::<u64>() }>()
					.0
					.iter()
					.map(|c| u64::from_be_bytes(*c))
					.collect();
				Ok(prev_shorts)
			})
	}

	pub(super) fn multi_get_shortauthevents<'a, I>(
		&'a self,
		shorteventids: I,
	) -> impl Stream<Item = Result<Vec<rooms::short::ShortEventId>>> + Send + 'a
	where
		I: Stream<Item = rooms::short::ShortEventId> + Send + 'a,
	{
		use futures::StreamExt;
		self.shorteventid_shortauthevents
			.get_batch(shorteventids.map(u64::to_be_bytes))
			.map(|res| {
				let val = res?;
				let auth_shorts = val
					.as_chunks::<{ size_of::<u64>() }>()
					.0
					.iter()
					.map(|c| u64::from_be_bytes(*c))
					.collect();
				Ok(auth_shorts)
			})
	}

	pub(super) async fn get_origin_server_ts(
		&self,
		event_id: &EventId,
	) -> Result<ruma::MilliSecondsSinceUnixEpoch> {
		let bytes = self.eventid_metadata.get(event_id.as_bytes()).await?;
		let meta = rooms::timeline::EventMetadata::from_bincode(&bytes)
			.map_err(|e| err!(Database("Failed to deserialize EventMetadata: {e:?}")))?;
		Ok(ruma::MilliSecondsSinceUnixEpoch(meta.origin_server_ts))
	}

	pub(super) fn pdus_by_timestamp<'a>(
		&'a self,
		room_id: &'a RoomId,
		timestamp: u64,
		dir: Direction,
	) -> impl Stream<Item = Result<PduEvent>> + Send + 'a {
		// Define rules of the stream
		let setup = async move {
			let short: u64 = self
				.services
				.short
				.get_shortroomid(room_id)
				.await
				.map_err(|e| err!(Request(NotFound("Room {room_id:?} not found: {e:?}"))))?;

			let (seek_ts, count) = match dir {
				| Direction::Forward => (timestamp, PduCount::min()),
				// Must be inclusive (at or before) according to Matrix MSC3030.
				// Do NOT subtract 1 from timestamp (which breaks tie-breaking/pagination).
				| Direction::Backward => (timestamp, PduCount::max()),
			};

			let key = pack_timestamp_key(short.to_be_bytes(), seek_ts, count);
			Ok::<_, conduwuit::Error>((short, key.to_vec()))
		};

		// Main stream
		setup
			.map_ok(move |(short, key): (u64, Vec<u8>)| {
				if key.is_empty() {
					return futures::stream::empty().boxed();
				}

				let prefix = short.to_be_bytes();
				let map = &self.db["roomid_timestamp_pducount"];

				// Get stream w/ matching DB keys, in requested direction
				let stream = match dir {
					| Direction::Forward => map.raw_stream_from(&key).boxed(),
					| Direction::Backward => map.rev_raw_stream_from(&key).boxed(),
				};

				stream
					.ready_try_take_while(move |&(k, _)| Ok(k.starts_with(&prefix)))
					// Extract PDU count via key lookup (shortroomid, timestamp, count)
					.ready_filter_map(|res| {
						let (k, _) = match res {
							Ok(kv) => kv,
							Err(e) => return Some(Err(e)),
						};

						if k.len() != 25 {
							tracing::warn!("Invalid timestamp index key length: {}", k.len());
							return None;
						}

						let variant = k[16];
						if variant != 0 && variant != 1 {
							tracing::warn!("Invalid timestamp index variant byte: {}", variant);
							return None;
						}

						let is_normal = variant == 1;
						let c_bytes: [u8; 8] = k[17..25].try_into().expect("valid slice");
						let count = if is_normal {
							PduCount::Normal(u64::from_be_bytes(c_bytes))
						} else {
							let sortable_c = u64::from_be_bytes(c_bytes);
							PduCount::Backfilled((sortable_c ^ (1 << 63)).cast_signed())
						};

						Some(Ok(count))
					})
					// Using PDU count, fetch full PDU event object
					.filter_map(move |count| async move {
						let count = match count {
							Ok(c) => c,
							Err(e) => return Some(Err(e)),
						};
						let pdu_id = PduId { shortroomid: short, shorteventid: count };
						match self.get_pdu_from_id_in_room(None, &pdu_id.into()).await {
							Ok(pdu) => Some(Ok(pdu)),
							Err(e) if e.is_not_found() => Some(Err(err!(
								Database(
									"Timestamp index points to missing PDU {pdu_id:?}: {e}"
								)
							))),
							Err(e) => Some(Err(e)),
						}
					})
					.boxed()
			})
			.try_flatten_stream()
	}
}

fn pack_timestamp_key(shortroomid: [u8; 8], ts: u64, count: PduCount) -> [u8; 25] {
	let mut key = [0_u8; 25];
	key[0..8].copy_from_slice(&shortroomid);
	key[8..16].copy_from_slice(&ts.to_be_bytes());
	match count {
		| PduCount::Backfilled(c) => {
			key[16] = 0;
			// Map negative i64 to correctly ordered u64 for RocksDB sorting
			let sortable_c = c.cast_unsigned() ^ (1 << 63);
			key[17..25].copy_from_slice(&sortable_c.to_be_bytes());
		},
		| PduCount::Normal(c) => {
			key[16] = 1;
			key[17..25].copy_from_slice(&c.to_be_bytes());
		},
	}
	key
}

const INCREMENT_LOCK_SHARDS: usize = 256;

static INCREMENT_LOCKS: std::sync::LazyLock<[conduwuit::SyncMutex<()>; INCREMENT_LOCK_SHARDS]> =
	std::sync::LazyLock::new(|| std::array::from_fn(|_| conduwuit::SyncMutex::new(())));

fn increment(db: &Arc<Map>, key: &[u8]) {
	use std::hash::{DefaultHasher, Hash, Hasher};
	let mut hasher = DefaultHasher::new();
	key.hash(&mut hasher);
	let shard_count = u64::try_from(INCREMENT_LOCK_SHARDS).expect("lock shard count fits in u64");
	let lock_index = usize::try_from(
		hasher
			.finish()
			.checked_rem(shard_count)
			.expect("lock shard count is non-zero"),
	)
	.expect("hash remainder fits in usize");
	let _lock = INCREMENT_LOCKS[lock_index].lock();
	let old = db.get_blocking(key);
	let new = utils::increment(old.ok().as_deref());
	db.insert(key, new);
}

#[cfg(test)]
mod tests {
	use conduwuit::Result;
	use conduwuit_core::matrix::pdu::{Count as PduCount, Id as PduId, RawId as RawPduId};
	use rezzy::{HashMap, LeanEvent, verify_pagination};
	use ruma::api::Direction;

	use super::Data;

	/// Helper: build a RawPduId from (room, count).
	fn make_pdu_id(room: u64, count: i64) -> RawPduId {
		let shorteventid = if count >= 0 {
			PduCount::Normal(count as u64)
		} else {
			PduCount::Backfilled(count)
		};
		PduId { shortroomid: room, shorteventid }.into()
	}

	/// Build a forked DAG for pagination testing:
	///
	/// ```text
	///         A (depth=1)
	///        / \
	///       B   C  (B at depth 2, C at depth 5 — federation fork)
	///       |
	///       D      (depth 3)
	///       |
	///       E      (depth 4, the tip we paginate from)
	/// ```
	///
	/// The fork at C (depth=5) is the scenario that triggers max() inflation:
	/// when paginating backward from E and hitting C's depth, the old code
	/// would inflate the seek position.
	fn build_forked_dag() -> (HashMap<String, LeanEvent>, Vec<(String, u64, i64)>) {
		let events: Vec<LeanEvent<String>> = vec![
			LeanEvent {
				event_id: "A".into(),
				depth: 1,
				prev_events: vec![],
				event_type: "m.room.create".into(),
				state_key: Some(String::new()),
				sender: "@x:x".into(),
				content: serde_json::json!({"room_version": "10", "creator": "@x:x"}),
				..Default::default()
			},
			LeanEvent {
				event_id: "B".into(),
				depth: 2,
				prev_events: vec!["A".into()],
				event_type: "m.room.message".into(),
				sender: "@x:x".into(),
				..Default::default()
			},
			LeanEvent {
				event_id: "C".into(),
				depth: 5,
				prev_events: vec!["A".into()],
				event_type: "m.room.message".into(),
				sender: "@x:x".into(),
				..Default::default()
			},
			LeanEvent {
				event_id: "D".into(),
				depth: 3,
				prev_events: vec!["B".into()],
				event_type: "m.room.message".into(),
				sender: "@x:x".into(),
				..Default::default()
			},
			LeanEvent {
				event_id: "E".into(),
				depth: 4,
				prev_events: vec!["D".into()],
				event_type: "m.room.message".into(),
				sender: "@x:x".into(),
				..Default::default()
			},
		];

		let mut events_map = HashMap::new();
		for ev in &events {
			events_map.insert(ev.event_id.clone(), ev.clone());
		}

		// Topo index entries: (event_id, depth, pdu_count)
		// pdu_count simulates insertion order. C (the fork at depth 5)
		// arrived via federation at count=3, making it adjacent to E at count=4.
		// This triggers max(token_depth=4, adjacent_depth=5) = 5 in the old code.
		let topo_entries = vec![
			("A".into(), 1_u64, 1_i64),
			("B".into(), 2, 2),
			("C".into(), 5, 3), // federation fork: high depth, mid-stream count
			("E".into(), 4, 4),
			("D".into(), 3, 5),
		];

		(events_map, topo_entries)
	}

	/// Extract the ordering that c10y's topo keys would produce for
	/// the given `(event_id, federation_depth, pdu_count)` entries.
	/// This is the order a RocksDB iterator would yield.
	fn c10y_topo_order(room: u64, topo_entries: &[(String, u64, i64)]) -> Vec<String> {
		let mut keyed: Vec<(Vec<u8>, String)> = topo_entries
			.iter()
			.map(|(id, depth, count)| {
				(Data::topo_pducount_key(&make_pdu_id(room, *count), *depth), id.clone())
			})
			.collect();
		keyed.sort_by(|a, b| a.0.cmp(&b.0));
		keyed.into_iter().map(|(_, id)| id).collect()
	}

	/// When federation depth is honest, c10y's topo key ordering matches
	/// rezzy's DAG-derived ordering (parents before children).
	#[test]
	fn honest_depth_matches_rezzy_ordering() {
		let (events_map, _) = build_forked_dag();

		// Honest depths: use rezzy's compute_depths (derived from prev_events)
		let depths = rezzy::compute_depths(&events_map);
		let honest_entries: Vec<(String, u64, i64)> = vec![
			("A".into(), depths["A"], 1),
			("B".into(), depths["B"], 2),
			("C".into(), depths["C"], 3),
			("D".into(), depths["D"], 4),
			("E".into(), depths["E"], 5),
		];

		let c10y_order = c10y_topo_order(1, &honest_entries);
		let rezzy_order =
			rezzy::compute_topo_positions(&events_map, |a: &String, b: &String| a.cmp(b));

		assert_eq!(
			c10y_order, rezzy_order,
			"with honest depths, c10y key ordering must match rezzy's topo ordering"
		);
	}

	/// When federation depth is inflated (C claims depth=5 instead of 2),
	/// c10y's topo key ordering diverges from rezzy's DAG-derived ordering.
	/// This is the P0.1 bug: the RocksDB index sorts C after D and E,
	/// but rezzy knows C is at the same level as B (both are children of A).
	///
	/// Regression test for 7ffebce75.
	#[test]
	fn inflated_depth_diverges_from_rezzy_ordering() {
		let (events_map, topo_entries) = build_forked_dag();

		// c10y uses federation-supplied depth (C has depth=5, INFLATED)
		let c10y_order = c10y_topo_order(1, &topo_entries);

		// rezzy derives depth from prev_events (C has depth=2, CORRECT)
		let rezzy_order =
			rezzy::compute_topo_positions(&events_map, |a: &String, b: &String| a.cmp(b));

		assert_ne!(
			c10y_order, rezzy_order,
			"inflated federation depth MUST produce a different ordering than rezzy's \
			 DAG-derived order — that's the bug. c10y={c10y_order:?}, rezzy={rezzy_order:?}"
		);

		// Specifically: rezzy puts C at position 2 (sibling of B), but c10y's
		// key ordering puts C after D/E due to inflated depth=5.
		let c10y_pos = |id: &str| c10y_order.iter().position(|x| x == id).unwrap();
		let rezzy_pos = |id: &str| rezzy_order.iter().position(|x| x == id).unwrap();

		assert!(
			rezzy_pos("C") < c10y_pos("C"),
			"rezzy places C earlier (depth=2) than c10y (depth=5): rezzy_pos={}, c10y_pos={}",
			rezzy_pos("C"),
			c10y_pos("C")
		);
	}

	/// Verify the `topo_pducount_key` -> `topo_key_to_pdu_id` round-trip is
	/// exact for `Backfilled` (negative) counts specifically — every other
	/// test in this module only exercises small positive (`Normal`) counts.
	/// This is the untested case relevant to the
	/// `TestMessagesOverFederation` backfill investigation (see
	/// docs/development-gg/backfill-append-toctou-race.md): if this
	/// round-trip is lossy for negative counts, a backfilled event's topo
	/// index entry could silently point at the wrong (or a colliding)
	/// pdu_id.
	#[test]
	fn topo_key_roundtrips_backfilled_counts() {
		let room = 1_u64;
		for count in [-1_i64, -2, -3, -100, -9999, i64::MIN + 1] {
			let pdu_id = make_pdu_id(room, count);
			let depth = 7_u64;
			let key = Data::topo_pducount_key(&pdu_id, depth);
			let recovered = Data::topo_key_to_pdu_id(&key);
			assert_eq!(
				recovered.as_ref(),
				pdu_id.as_ref(),
				"round-trip must be exact for Backfilled count {count}"
			);
		}
	}

	/// Verify that a `Backfilled` (negative) count at a lower depth sorts
	/// *before* (i.e. older than) `Normal` (positive) counts at higher
	/// depths, and that among same-depth entries mixing polarities, the
	/// depth-then-count ordering invariant still holds. This is the exact
	/// shape of the `TestMessagesOverFederation` scenario: a
	/// backfill-discovered gap-filler event (negative count) sitting
	/// between live (positive count) events at adjacent depths.
	#[test]
	fn topo_keys_order_backfilled_and_normal_consistently() {
		let room = 1_u64;

		let key_backfilled_low_depth = Data::topo_pducount_key(&make_pdu_id(room, -50), 3);
		let key_backfilled_high_depth = Data::topo_pducount_key(&make_pdu_id(room, -1), 4);
		let key_normal_higher_depth = Data::topo_pducount_key(&make_pdu_id(room, 10), 5);

		assert!(
			key_backfilled_low_depth < key_backfilled_high_depth,
			"lower depth (3) must sort before higher depth (4) regardless of Backfilled count \
			 magnitude"
		);
		assert!(
			key_backfilled_high_depth < key_normal_higher_depth,
			"Backfilled entry at depth 4 must sort before Normal entry at depth 5"
		);
	}

	/// Verify that topo keys sort by depth first, then count — the
	/// structural invariant that makes Synapse-style pagination correct.
	#[test]
	fn topo_keys_sort_by_depth_then_count() {
		let room = 1_u64;

		let key_d5_c10 = Data::topo_pducount_key(&make_pdu_id(room, 10), 5);
		let key_d5_c11 = Data::topo_pducount_key(&make_pdu_id(room, 11), 5);
		let key_d8_c3 = Data::topo_pducount_key(&make_pdu_id(room, 3), 8);
		let key_d10_c1 = Data::topo_pducount_key(&make_pdu_id(room, 1), 10);

		assert!(key_d5_c10 < key_d5_c11, "same depth: lower count sorts first");
		assert!(key_d5_c11 < key_d8_c3, "lower depth sorts before higher depth");
		assert!(key_d8_c3 < key_d10_c1, "depth 8 before depth 10");
	}

	/// Simulate backward pagination through the topo index.
	///
	/// Returns `None` if the loop guard fires (more than `max_pages` pages),
	/// indicating the seek logic diverges (infinite loop).
	///
	/// When `inflate_depth` is true, uses the old buggy `max(token_depth,
	/// adjacent_depth)` seek logic. When false, uses the fixed exact-depth
	/// seek.
	fn simulate_backward_pagination(
		room: u64,
		topo_entries: &[(String, u64, i64)],
		limit: usize,
		inflate_depth: bool,
		start_from: Option<(u64, i64)>,
	) -> Option<Vec<Vec<String>>> {
		const MAX_PAGES: usize = 20;

		// Build sorted key index (descending — backward pagination reads high→low)
		let mut keyed: Vec<(Vec<u8>, String, u64, i64)> = topo_entries
			.iter()
			.map(|(id, depth, count)| {
				let key = Data::topo_pducount_key(&make_pdu_id(room, *count), *depth);
				(key, id.clone(), *depth, *count)
			})
			.collect();
		keyed.sort_by(|a, b| b.0.cmp(&a.0)); // descending

		// Depth lookup by count (simulates pdu_id_to_depth)
		let depth_by_count: HashMap<i64, u64> =
			topo_entries.iter().map(|(_, d, c)| (*c, *d)).collect();

		let mut pages: Vec<Vec<String>> = Vec::new();
		let mut seek_from: Option<(u64, i64)> = start_from;

		loop {
			if pages.len() >= MAX_PAGES {
				return None; // loop guard fired — divergent
			}

			let seek_key = seek_from.map(|(token_depth, token_count)| {
				let adjacent_depth = depth_by_count
					.get(&(token_count - 1))
					.copied()
					.unwrap_or(token_depth);

				let effective_depth = if inflate_depth {
					// OLD BUG: max(token_depth, adjacent_depth)
					token_depth.max(adjacent_depth)
				} else if pages.is_empty() && start_from.is_some() {
					// First page from mid-stream position — seek from
					// top of topo index to capture events at any depth.
					// Stream count filter (below) excludes post-sync events.
					u64::MAX
				} else {
					// Subsequent pages or first page from MAX: exact depth
					token_depth
				};

				Data::topo_pducount_key(&make_pdu_id(room, token_count), effective_depth)
			});

			// Stream count ceiling: when starting from a mid-stream position,
			// events with count > start_count arrived AFTER the sync token and
			// must be excluded from backward pagination. This models Synapse's
			// SQL: WHERE (topo, stream) <= (from_topo, from_stream)
			let count_ceiling: Option<i64> = start_from.map(|(_, c)| c);

			let page: Vec<_> = keyed
				.iter()
				.filter(|(key, _, _, count)| {
					// Stream count filter: skip events after the sync position
					if let Some(ceil) = count_ceiling {
						if *count >= ceil {
							return false;
						}
					}
					// Topo key filter: skip events at or above seek position
					if let Some(ref sk) = seek_key {
						*key < *sk
					} else {
						true // First page: start from MAX
					}
				})
				.take(limit)
				.map(|(_, id, depth, count)| (id.clone(), *depth, *count))
				.collect();

			if page.is_empty() {
				break;
			}

			let last = page.last().unwrap();
			seek_from = Some((last.1, last.2));
			pages.push(page.iter().map(|(id, ..)| id.clone()).collect());
		}

		Some(pages)
	}

	/// The OLD buggy max() seek logic causes an infinite loop (the loop guard
	/// fires before all events are yielded).
	///
	/// Regression test for commit 250e12817 — proves the bug diverges.
	#[test]
	fn inflated_seek_causes_infinite_loop() {
		let (_, topo_entries) = build_forked_dag();
		let result = simulate_backward_pagination(1, &topo_entries, 2, true, None);

		assert!(
			result.is_none(),
			"buggy max() seek logic must hit the loop guard (infinite loop), but it terminated \
			 — the simulation does not reproduce the bug"
		);
	}

	/// The FIXED exact-depth seek logic terminates correctly and produces
	/// no pagination violations (no duplicates, correct ordering).
	///
	/// Regression test for commit 250e12817 — proves the fix works.
	#[test]
	fn fixed_seek_terminates_with_no_violations() {
		let (events_map, topo_entries) = build_forked_dag();
		let pages = simulate_backward_pagination(1, &topo_entries, 2, false, None);

		let pages =
			pages.expect("fixed exact-depth seek logic must terminate, but hit loop guard");

		// All 5 events must be yielded
		let total: usize = pages.iter().map(Vec::len).sum();
		assert_eq!(total, 5, "all 5 events must be yielded across pages");

		// No duplicates or ordering violations
		let violations = verify_pagination(&events_map, &pages);
		assert!(
			violations.is_empty(),
			"fixed seek logic must produce no pagination violations, got: {violations:?}"
		);
	}

	/// Build a network partition DAG:
	///
	/// ```text
	///         A (depth=1, create)
	///         |
	///         B (depth=2, join)
	///        / \
	///       C   E  (C local depth=3, E remote depth=3)
	///       |   |
	///       D   F  (D local depth=4, F remote depth=4)
	///        \ /
	///         G (depth=5, merge after partition heals)
	/// ```
	///
	/// Events arrive in timeline order:
	///   A(1), B(2), C(3), D(4) — during partition (local branch)
	///   E(5), F(6) — received from remote after partition heals
	///   G(7) — merge event
	///
	/// The remote events E,F have lower depth (3,4) but higher count (5,6).
	/// In the topo index, they sort BEFORE the merge event G but AFTER local
	/// events at the same depth. When sync delivers G and the client paginates
	/// backward from G's topo token, exact-depth seek works fine here because
	/// all prior events have depth <= 5.
	///
	/// The REAL problem: sync delivers G at topo token (depth=5, count=7).
	/// The client's first backward page returns events at depth=5 and below.
	/// No events are missed because G has the highest depth.
	///
	/// But what if the remote branch has HIGHER depth than local?
	fn build_partition_dag() -> (HashMap<String, LeanEvent>, Vec<(String, u64, i64)>) {
		let events: Vec<LeanEvent<String>> = vec![
			LeanEvent {
				event_id: "A".into(),
				depth: 1,
				prev_events: vec![],
				event_type: "m.room.create".into(),
				state_key: Some(String::new()),
				sender: "@x:x".into(),
				content: serde_json::json!({"room_version": "10", "creator": "@x:x"}),
				..Default::default()
			},
			LeanEvent {
				event_id: "B".into(),
				depth: 2,
				prev_events: vec!["A".into()],
				event_type: "m.room.message".into(),
				sender: "@x:x".into(),
				..Default::default()
			},
			// Local branch (low depth)
			LeanEvent {
				event_id: "C".into(),
				depth: 3,
				prev_events: vec!["B".into()],
				event_type: "m.room.message".into(),
				sender: "@local:x".into(),
				..Default::default()
			},
			LeanEvent {
				event_id: "D".into(),
				depth: 4,
				prev_events: vec!["C".into()],
				event_type: "m.room.message".into(),
				sender: "@local:x".into(),
				..Default::default()
			},
			// Remote branch (HIGH depth — remote server had more activity)
			LeanEvent {
				event_id: "E".into(),
				depth: 6,
				prev_events: vec!["B".into()],
				event_type: "m.room.message".into(),
				sender: "@remote:y".into(),
				..Default::default()
			},
			LeanEvent {
				event_id: "F".into(),
				depth: 7,
				prev_events: vec!["E".into()],
				event_type: "m.room.message".into(),
				sender: "@remote:y".into(),
				..Default::default()
			},
			// Merge event
			LeanEvent {
				event_id: "G".into(),
				depth: 8,
				prev_events: vec!["D".into(), "F".into()],
				event_type: "m.room.message".into(),
				sender: "@local:x".into(),
				..Default::default()
			},
		];

		let mut events_map = HashMap::new();
		for ev in &events {
			events_map.insert(ev.event_id.clone(), ev.clone());
		}

		// Timeline insertion order:
		// A, B, C, D arrived during partition (counts 1-4)
		// E, F arrived from remote after partition heals (counts 5-6)
		// G is the merge (count 7)
		let topo_entries = vec![
			("A".into(), 1_u64, 1_i64),
			("B".into(), 2, 2),
			("C".into(), 3, 3),
			("D".into(), 4, 4),
			("E".into(), 6, 5), // remote: high depth, arrived late
			("F".into(), 7, 6), // remote: high depth, arrived late
			("G".into(), 8, 7), // merge
		];

		(events_map, topo_entries)
	}

	/// Simulate backward pagination starting from a specific topo token
	/// (not MAX). This models what happens when sync delivers recent events
	/// and the client paginates backward from a mid-stream position.
	///
	/// `start_from`: (depth, count) of the topo token to start from.
	/// If None, starts from MAX (same as original
	/// simulate_backward_pagination).
	fn simulate_backward_pagination_from(
		room: u64,
		topo_entries: &[(String, u64, i64)],
		limit: usize,
		inflate_depth: bool,
		start_from: (u64, i64),
	) -> Option<Vec<Vec<String>>> {
		simulate_backward_pagination(room, topo_entries, limit, inflate_depth, Some(start_from))
	}

	/// Simulate backward pagination from a concrete topo token while filtering
	/// by the token's exact topo boundary rather than by stream count alone.
	fn simulate_backward_pagination_from_concrete_token(
		room: u64,
		topo_entries: &[(String, u64, i64)],
		limit: usize,
		start_from: (u64, i64),
	) -> Vec<Vec<String>> {
		let mut keyed: Vec<(Vec<u8>, String)> = topo_entries
			.iter()
			.map(|(id, depth, count)| {
				let key = Data::topo_pducount_key(&make_pdu_id(room, *count), *depth);
				(key, id.clone())
			})
			.collect();
		keyed.sort_by(|a, b| b.0.cmp(&a.0));

		let token_key = Data::topo_pducount_key(&make_pdu_id(room, start_from.1), start_from.0);
		let mut pages = Vec::new();
		let mut remaining: Vec<String> = keyed
			.into_iter()
			.filter(|(key, _)| *key < token_key)
			.map(|(_, id)| id)
			.collect();

		while !remaining.is_empty() {
			let split_at = remaining.len().min(limit);
			pages.push(remaining.drain(..split_at).collect());
		}

		pages
	}

	/// Regression test for TestNetworkPartitionOrdering.
	///
	/// Models the real complement scenario:
	///   1. Sync delivers events A,B,C,D (counts 1-4) during partition
	///   2. Partition heals, remote events E,F arrive (counts 5-6)
	///   3. Merge G arrives (count 7)
	///   4. Client paginates backward from sync position (count=4)
	///
	/// Backward pagination from count=4 must return ONLY events at or
	/// before the sync position (D,C,B,A), NOT events that arrived later
	/// (E,F,G). Events E,F,G will be delivered via the next /sync.
	#[test]
	fn partition_backward_pagination_excludes_future_events() {
		let (events_map, topo_entries) = build_partition_dag();

		// Sync token is ONE PAST the last delivered event (D at count=4).
		// So from=5 means "give me everything before count 5."
		// Events E(count=5), F(count=6), G(count=7) arrived AFTER this position.
		let pages = simulate_backward_pagination_from(1, &topo_entries, 3, false, (4, 5));

		let pages = pages.expect("must terminate");
		let all_events: Vec<String> = pages.iter().flatten().cloned().collect();

		// Only events at count <= 4 should be returned: D, C, B, A
		assert_eq!(
			all_events.len(),
			4,
			"backward pagination from count=4 must return only 4 events (D,C,B,A), got \
			 {all_events:?}"
		);

		// Must not contain any post-sync events
		assert!(
			!all_events.contains(&"E".to_string())
				&& !all_events.contains(&"F".to_string())
				&& !all_events.contains(&"G".to_string()),
			"backward pagination must NOT return events after sync position (got {all_events:?})"
		);

		let violations = verify_pagination(&events_map, &pages);
		assert!(violations.is_empty(), "pagination must have no violations, got: {violations:?}");
	}

	/// A stale concrete topo token must still discover older events which
	/// arrive later via backfill. Count-only filtering would wrongly drop
	/// `MISSING` here because it arrived after the token, even though it sorts
	/// before the token in topo order.
	#[test]
	fn concrete_backward_token_includes_late_inserted_older_event() {
		let topo_entries = vec![
			("CREATE".into(), 1_u64, -3_i64),
			("JOIN".into(), 2, -2),
			("POWER".into(), 3, -1),
			("M0".into(), 4, 10),
			("M1".into(), 5, 11),
			("MISSING".into(), 6, 99),
			("M2".into(), 7, 12),
			("M3".into(), 8, 13),
		];

		let pages =
			simulate_backward_pagination_from_concrete_token(1, &topo_entries, 10, (7, 12));
		let all_events: Vec<String> = pages.into_iter().flatten().collect();

		assert!(
			all_events.contains(&"MISSING".to_owned()),
			"late-inserted older event must still be reachable from a stale concrete token, got \
			 {all_events:?}"
		);
		assert!(
			!all_events.contains(&"M2".to_owned()) && !all_events.contains(&"M3".to_owned()),
			"events at or after the concrete token must remain excluded, got {all_events:?}"
		);
	}

	/// max() seek recovers remote branch events in the partition scenario,
	/// but only when the adjacent event has the right depth.
	#[test]
	fn partition_inflated_seek_recovers_some_events() {
		let (_, topo_entries) = build_partition_dag();

		// Start from G (the merge at depth=8, count=7) — this is the normal
		// case where sync delivers the merge event. Backward pagination from
		// MAX should capture everything regardless of seek strategy.
		let pages_exact = simulate_backward_pagination(1, &topo_entries, 3, false, None);
		let pages_inflate = simulate_backward_pagination(1, &topo_entries, 3, true, None);

		let exact = pages_exact.expect("must terminate");
		let inflate = pages_inflate.expect("must terminate with partition DAG");

		let exact_total: usize = exact.iter().map(Vec::len).sum();
		let inflate_total: usize = inflate.iter().map(Vec::len).sum();

		assert_eq!(exact_total, 7, "exact seek from MAX must return all 7 events");
		assert_eq!(inflate_total, 7, "inflated seek from MAX must also return all 7 events");
	}

	// Tests for edge cases and out-of-order events.

	// Helper to make a BTreeSet act like our database queries
	fn simulate_pdus_by_timestamp(
		index: &std::collections::BTreeSet<(u64, u64)>,
		search_ts: u64,
		dir: Direction,
	) -> Vec<(u64, u64)> {
		// Keys are (timestamp, count)
		let start_count = match dir {
			| Direction::Forward => u64::MIN,
			| Direction::Backward => u64::MAX,
		};
		let start_key = (search_ts, start_count);

		match dir {
			| Direction::Forward => index.range(start_key..).copied().collect(),
			| Direction::Backward => index.range(..=start_key).rev().copied().collect(),
		}
	}

	#[tokio::test]
	async fn test_pdus_by_timestamp_complex_walk() -> Result<()> {
		// Test a messy timeline where timestamps don't always go up in order.
		//
		// Example timeline:
		// E1: 1000ms, Count 1
		// E2: 2000ms, Count 2
		// E3: 2000ms, Count 3 (Duplicate TS, arrived after E2)
		// E4: 1500ms, Count 4 (Clock Skew - arrived later but has earlier TS)
		// E5: 3000ms, Count 5
		//
		// How it looks in the database (sorted by time, then count):
		// 1. (1000ms, Count 1)
		// 2. (1500ms, Count 4)
		// 3. (2000ms, Count 2)
		// 4. (2000ms, Count 3)
		// 5. (3000ms, Count 5)

		let mut index = std::collections::BTreeSet::new();
		index.insert((1000, 1));
		index.insert((2000, 2));
		index.insert((2000, 3));
		index.insert((1500, 4)); // Non-monotonic TS relative to count
		index.insert((3000, 5));

		// Searching forward from 1700ms finds the 2000ms and 3000ms events
		let fwd = simulate_pdus_by_timestamp(&index, 1700, Direction::Forward);
		assert_eq!(fwd, vec![(2000, 2), (2000, 3), (3000, 5)]);

		// Searching backward from 1700ms finds the 1500ms and 1000ms events
		let bwd = simulate_pdus_by_timestamp(&index, 1700, Direction::Backward);
		assert_eq!(bwd, vec![(1500, 4), (1000, 1)]);

		Ok(())
	}

	#[tokio::test]
	async fn test_pdus_by_timestamp_large_sparse_gaps() -> Result<()> {
		// Check we jump straight to the next event, not scan huge empty gaps.

		let mut index = std::collections::BTreeSet::new();

		// 1st group of events: 100,000 to 101,000
		for i in 100_000..=101_000 {
			index.insert((i, i));
		}

		// 2nd group of events: 964,000 to 965,000
		for i in 964_000..=965_000 {
			index.insert((i, i));
		}

		// Searching forward from the middle should find next group.
		let fwd = simulate_pdus_by_timestamp(&index, 500_000, Direction::Forward);
		assert_eq!(fwd.first(), Some(&(964_000, 964_000)));

		// Searching backward should find the first group.
		let bwd = simulate_pdus_by_timestamp(&index, 500_000, Direction::Backward);
		assert_eq!(bwd.first(), Some(&(101_000, 101_000)));

		Ok(())
	}

	#[tokio::test]
	async fn test_pdus_by_timestamp_wild_jitter_staircase() -> Result<()> {
		// Create 1000 events where the time generally goes up but sometimes jumps back
		let timeline = (0..1000_u64).map(|i| {
			let i_signed = i as i64;
			let ts = i_signed * 10 + (i_signed % 11) * 5 - (i_signed % 13) * 7;
			(ts.max(0) as u64, i)
		});

		// Set sorts like RocksDB, luckily
		let mut index = std::collections::BTreeSet::new();
		for (ts, count) in timeline {
			index.insert((ts, count));
		}

		// Check we find correct starting point even if the timestamps jump around
		let search_ts = 5000_u64;
		let results = simulate_pdus_by_timestamp(&index, search_ts, Direction::Forward);

		// Check first event we find is at (or after) our search time
		if let Some(&(ts, _count)) = results.first() {
			assert!(ts >= search_ts);
		} else {
			panic!("Search yielded no results");
		}

		Ok(())
	}
}

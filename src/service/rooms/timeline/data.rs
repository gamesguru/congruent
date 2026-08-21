use std::{
	hash::{DefaultHasher, Hash, Hasher},
	sync::{Arc, LazyLock},
};

use conduwuit::{
	Err, Event, PduCount, PduEvent, Result, at, err,
	result::NotFound,
	utils::{
		self,
		stream::{ReadyExt, TryReadyExt},
	},
};
use database::{Database, Deserialized, Json, KeyVal, Map};
use futures::{
	FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt, future::select_ok, pin_mut,
};
use ruma::{
	CanonicalJsonObject, EventId, MilliSecondsSinceUnixEpoch, OwnedUserId, RoomId, UInt,
	api::Direction,
};

use super::{PduId, RawPduId};
use crate::{Dep, rooms, rooms::short::ShortRoomId};

pub(super) struct Data {
	eventid_outlierpdu: Arc<Map>,
	eventid_pduid: Arc<Map>,
	pduid_pdu: Arc<Map>,
	userroomid_highlightcount: Arc<Map>,
	userroomid_notificationcount: Arc<Map>,
	pub(super) db: Arc<Database>,
	services: Services,
}

struct Services {
	short: Dep<rooms::short::Service>,
}

pub type PdusIterItem = (PduCount, PduEvent);

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let db = &args.db;
		Self {
			eventid_outlierpdu: db["eventid_outlierpdu"].clone(),
			eventid_pduid: db["eventid_pduid"].clone(),
			pduid_pdu: db["pduid_pdu"].clone(),
			userroomid_highlightcount: db["userroomid_highlightcount"].clone(),
			userroomid_notificationcount: db["userroomid_notificationcount"].clone(),
			db: args.db.clone(),
			services: Services {
				short: args.depend::<rooms::short::Service>("rooms::short"),
			},
		}
	}

	#[inline]
	pub(super) async fn last_timeline_count(&self, room_id: &RoomId) -> Result<PduCount> {
		let pdus_rev = self.pdus_rev(room_id, PduCount::max());

		pin_mut!(pdus_rev);
		let last_count = pdus_rev
			.try_next()
			.await?
			.map(at!(0))
			.filter(|&count| matches!(count, PduCount::Normal(_)))
			.unwrap_or_else(PduCount::max);

		Ok(last_count)
	}

	#[inline]
	pub(super) async fn latest_pdu_in_room(&self, room_id: &RoomId) -> Result<PduEvent> {
		let pdus_rev = self.pdus_rev(room_id, PduCount::max());

		pin_mut!(pdus_rev);
		pdus_rev
			.try_next()
			.await?
			.map(at!(1))
			.ok_or_else(|| err!(Request(NotFound("no PDU's found in room"))))
	}

	/// Returns the `count` of this pdu's id.
	pub(super) async fn get_pdu_count(&self, event_id: &EventId) -> Result<PduCount> {
		self.get_pdu_id(event_id)
			.await
			.map(|pdu_id| pdu_id.pdu_count())
	}

	/// Returns the json of a pdu.
	pub(super) async fn get_pdu_json(&self, event_id: &EventId) -> Result<CanonicalJsonObject> {
		let accepted = self.get_non_outlier_pdu_json(event_id).boxed();
		let outlier = self
			.eventid_outlierpdu
			.get(event_id)
			.map(Deserialized::deserialized)
			.boxed();

		select_ok([accepted, outlier]).await.map(at!(0))
	}

	/// Returns the json of a pdu.
	pub(super) async fn get_non_outlier_pdu_json(
		&self,
		event_id: &EventId,
	) -> Result<CanonicalJsonObject> {
		let pduid = self.get_pdu_id(event_id).await?;

		self.pduid_pdu.get(&pduid).await.deserialized()
	}

	/// Returns the pdu's id.
	#[inline]
	pub(super) async fn get_pdu_id(&self, event_id: &EventId) -> Result<RawPduId> {
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
		let pdu: PduEvent = self.pduid_pdu.get(&pduid).await.deserialized()?;

		// Enforce cross-room boundary: verify the PDU belongs to the expected room
		if let Some(expected_room) = room_id {
			if pdu.room_id_or_hash().as_deref() != Some(expected_room) {
				return Err!(Database("PDU {event_id} does not belong to room {expected_room}"));
			}
		}

		Ok(pdu)
	}

	/// Like get_non_outlier_pdu(), but without the expense of fetching and
	/// parsing the PduEvent
	pub(super) async fn non_outlier_pdu_exists(&self, event_id: &EventId) -> Result {
		let pduid = self.get_pdu_id(event_id).await?;

		self.pduid_pdu.exists(&pduid).await
	}

	/// Returns the pdu.
	///
	/// Checks the `eventid_outlierpdu` Tree if not found in the timeline.
	/// If `room_id` is provided, validates the PDU belongs to that room.
	pub(super) async fn get_pdu_in_room(
		&self,
		room_id: Option<&RoomId>,
		event_id: &EventId,
	) -> Result<PduEvent> {
		let accepted = self.get_non_outlier_pdu_in_room(room_id, event_id).boxed();
		let outlier = self
			.eventid_outlierpdu
			.get(event_id)
			.map(move |handle| {
				let pdu: PduEvent = handle.deserialized()?;

				// Enforce cross-room boundary
				if let Some(expected_room) = room_id {
					if pdu.room_id_or_hash().as_deref() != Some(expected_room) {
						return Err(conduwuit::err!(Database(
							"Outlier PDU {event_id} does not belong to room {expected_room}"
						)));
					}
				}

				Ok(pdu)
			})
			.boxed();

		select_ok([accepted, outlier]).await.map(at!(0))
	}

	/// Like get_non_outlier_pdu(), but without the expense of fetching and
	/// parsing the PduEvent
	#[inline]
	pub(super) async fn outlier_pdu_exists(&self, event_id: &EventId) -> Result {
		self.eventid_outlierpdu.exists(event_id).await
	}

	/// Like get_pdu(), but without the expense of fetching and parsing the data
	pub(super) async fn pdu_exists(&self, event_id: &EventId) -> Result {
		let non_outlier = self.non_outlier_pdu_exists(event_id).boxed();
		let outlier = self.outlier_pdu_exists(event_id).boxed();

		select_ok([non_outlier, outlier]).await.map(at!(0))
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
		let pdu: PduEvent = self.pduid_pdu.get(pdu_id).await.deserialized()?;

		if let Some(expected_room) = room_id {
			if pdu.room_id_or_hash().as_deref() != Some(expected_room) {
				return Err!(Database("PDU does not belong to room {expected_room}"));
			}
		}

		Ok(pdu)
	}

	/// Returns the event_id from the pdu directly.
	pub(super) async fn get_event_id_from_pdu_id(
		&self,
		pdu_id: &RawPduId,
	) -> Result<ruma::OwnedEventId> {
		#[derive(serde::Deserialize)]
		struct EventIdExtract {
			event_id: ruma::OwnedEventId,
		}

		let extract: EventIdExtract = self.pduid_pdu.get(pdu_id).await.deserialized()?;
		Ok(extract.event_id)
	}

	pub(super) async fn pduid_exists(&self, pdu_id: &RawPduId) -> bool {
		self.pduid_pdu.exists(pdu_id).await.is_ok()
	}

	/// Returns the pdu as a `BTreeMap<String, CanonicalJsonValue>`.
	pub(super) async fn get_pdu_json_from_id(
		&self,
		pdu_id: &RawPduId,
	) -> Result<CanonicalJsonObject> {
		self.pduid_pdu.get(pdu_id).await.deserialized()
	}

	pub(super) async fn append_pdu(
		&self,
		pdu_id: &RawPduId,
		pdu: &PduEvent,
		json: &CanonicalJsonObject,
		count: PduCount,
	) {
		debug_assert!(matches!(count, PduCount::Normal(_)), "PduCount not Normal");

		self.pduid_pdu.raw_put(pdu_id, Json(json));
		self.eventid_pduid.insert(pdu.event_id.as_bytes(), pdu_id);
		self.eventid_outlierpdu.remove(pdu.event_id.as_bytes());
		self.append_timestamp_index(pdu_id, MilliSecondsSinceUnixEpoch(pdu.origin_server_ts));
	}

	pub(super) fn prepend_backfill_pdu(
		&self,
		pdu_id: &RawPduId,
		event_id: &EventId,
		json: &CanonicalJsonObject,
	) {
		self.pduid_pdu.raw_put(pdu_id, Json(json));
		self.eventid_pduid.insert(event_id, pdu_id);
		self.eventid_outlierpdu.remove(event_id);
		if let Some(ruma::CanonicalJsonValue::Integer(ts)) = json.get("origin_server_ts") {
			if let Ok(ts) = UInt::try_from(i64::from(*ts)) {
				self.append_timestamp_index(pdu_id, MilliSecondsSinceUnixEpoch(ts));
			}
		}
	}

	fn append_timestamp_index(&self, pdu_id: &RawPduId, ts: MilliSecondsSinceUnixEpoch) {
		let key = pack_timestamp_key(pdu_id.shortroomid(), u64::from(ts.0), pdu_id.pdu_count());

		self.db["roomid_timestamp_pducount"].insert(&key, []);
	}

	/// Removes a pdu and creates a new one with the same id.
	pub(super) async fn replace_pdu(
		&self,
		pdu_id: &RawPduId,
		pdu_json: &CanonicalJsonObject,
	) -> Result {
		if self.pduid_pdu.get(pdu_id).await.is_not_found() {
			return Err!(Request(NotFound("PDU does not exist.")));
		}

		self.pduid_pdu.raw_put(pdu_id, Json(pdu_json));

		Ok(())
	}

	/// Returns an iterator over all events and their tokens in a room that
	/// happened before the event with id `until` in reverse-chronological
	/// order.
	pub(super) fn pdus_rev<'a>(
		&'a self,
		room_id: &'a RoomId,
		until: PduCount,
	) -> impl Stream<Item = Result<PdusIterItem>> + Send + 'a {
		self.count_to_id(room_id, until, Direction::Backward)
			.map_ok(move |current| {
				let prefix = current.shortroomid();
				self.pduid_pdu
					.rev_raw_stream_from(&current)
					.ready_try_take_while(move |(key, _)| Ok(key.starts_with(&prefix)))
					.ready_and_then(move |kv| Self::parse_json_slice(Some(room_id), kv))
			})
			.try_flatten_stream()
	}

	pub(super) fn pdus<'a>(
		&'a self,
		room_id: &'a RoomId,
		from: PduCount,
	) -> impl Stream<Item = Result<PdusIterItem>> + Send + 'a {
		self.count_to_id(room_id, from, Direction::Forward)
			.map_ok(move |current| {
				let prefix = current.shortroomid();
				self.pduid_pdu
					.raw_stream_from(&current)
					.ready_try_take_while(move |(key, _)| Ok(key.starts_with(&prefix)))
					.ready_and_then(move |kv| Self::parse_json_slice(Some(room_id), kv))
			})
			.try_flatten_stream()
	}

	/// Returns a stream of PDUs starting at `timestamp` in `dir`.
	pub(super) fn pdus_by_timestamp<'a>(
		&'a self,
		room_id: &'a RoomId,
		timestamp: u64,
		dir: Direction,
	) -> impl Stream<Item = Result<PduEvent>> + Send + 'a {
		// Define rules of the stream
		let setup = async move {
			let short = self
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
			.map_ok(move |(short, key)| {
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
					.ready_try_take_while(move |(k, _)| Ok(k.starts_with(&prefix)))
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

	pub(super) async fn backfill_timestamp_index(&self, room_id: &RoomId) -> Result {
		let shortroomid: ShortRoomId = self.services.short.get_shortroomid(room_id).await?;
		let pdus = self.pdus(room_id, PduCount::min());
		pin_mut!(pdus);

		let mut yield_count = 0_usize;
		let mut cork = self.db.cork(); // Hold writes in memory

		while let Some(res) = pdus.next().await {
			let (count, pdu) = match res {
				| Ok(p) => p,
				| Err(e) if e.is_not_found() => {
					tracing::info!("Skipping unresolvable event, ts-backfill: {e}");
					continue;
				},
				| Err(e) => return Err(e),
			};

			let pdu_id = PduId { shortroomid, shorteventid: count };
			self.append_timestamp_index(
				&pdu_id.into(),
				MilliSecondsSinceUnixEpoch(pdu.origin_server_ts),
			);

			// Write to DB in batched queries of 1000; avoid nightmare 1 write per iteration
			yield_count = yield_count.saturating_add(1);
			if yield_count.is_multiple_of(1000) {
				drop(cork); // flush to disk
				tokio::task::yield_now().await;
				cork = self.db.cork(); // re-acquire hold
			}
		}

		Ok(())
	}

	fn parse_json_slice(
		room_id: Option<&RoomId>,
		(pdu_id, pdu): KeyVal<'_>,
	) -> Result<PdusIterItem> {
		let pdu_id: RawPduId = pdu_id.into();
		let pdu = serde_json::from_slice::<PduEvent>(pdu)?;

		if let Some(expected_room) = room_id {
			if pdu.room_id_or_hash().as_deref() != Some(expected_room) {
				return Err(conduwuit::err!(Database(
					"PDU does not belong to expected room {expected_room}"
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
	) {
		let _cork = self.db.cork();

		for user in notifies {
			let mut userroom_id = user.as_bytes().to_vec();
			userroom_id.push(0xFF);
			userroom_id.extend_from_slice(room_id.as_bytes());
			increment(&self.userroomid_notificationcount, &userroom_id);
		}

		for user in highlights {
			let mut userroom_id = user.as_bytes().to_vec();
			userroom_id.push(0xFF);
			userroom_id.extend_from_slice(room_id.as_bytes());
			increment(&self.userroomid_highlightcount, &userroom_id);
		}
	}

	pub(super) async fn prev_timeline_count(&self, before: &PduId) -> Result<PduCount> {
		let before_pdu =
			Self::pdu_count_to_id(before.shortroomid, before.shorteventid, Direction::Backward);

		let prefix = before_pdu.shortroomid();
		let pdu_ids = self
			.pduid_pdu
			.rev_keys_raw_from(&before_pdu)
			.ready_try_take_while(move |pdu_bytes: &&[u8]| Ok(pdu_bytes.starts_with(&prefix)))
			.ready_and_then(|pdu_bytes: &[u8]| {
				let pdu_id = RawPduId::from(pdu_bytes);
				Ok(pdu_id.pdu_count())
			})
			.ready_try_filter_map(|count| {
				Ok(matches!(count, PduCount::Normal(_)).then_some(count))
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
			.pduid_pdu
			.keys_raw_from(&after_pdu)
			.ready_try_take_while(move |pdu_bytes: &&[u8]| Ok(pdu_bytes.starts_with(&prefix)))
			.ready_and_then(|pdu_bytes: &[u8]| {
				let pdu_id = RawPduId::from(pdu_bytes);
				Ok(pdu_id.pdu_count())
			})
			.ready_try_filter_map(|count| {
				Ok(matches!(count, PduCount::Normal(_)).then_some(count))
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

	async fn count_to_id(
		&self,
		room_id: &RoomId,
		shorteventid: PduCount,
		dir: Direction,
	) -> Result<RawPduId> {
		let shortroomid: ShortRoomId = self
			.services
			.short
			.get_shortroomid(room_id)
			.await
			.map_err(|e| err!(Request(NotFound("Room {room_id:?} not found: {e:?}"))))?;

		Ok(Self::pdu_count_to_id(shortroomid, shorteventid, dir))
	}
}

/// Build a fixed-width 25-byte binary key for the `roomid_timestamp_pducount`
/// index without going through conduwuit's serde serializer. The serde tuple
/// format uses `0xFF` as a field delimiter, so raw big-endian integers that
/// happen to contain a `0xFF` byte would inject false delimiters and corrupt
/// the key. Manual packing avoids this entirely.
///
/// Layout: `[shortroomid:8][timestamp:8][variant:1][count:8]`
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

static INCREMENT_LOCKS: LazyLock<[conduwuit::SyncMutex<()>; INCREMENT_LOCK_SHARDS]> =
	LazyLock::new(|| std::array::from_fn(|_| conduwuit::SyncMutex::new(())));

fn increment(db: &Arc<Map>, key: &[u8]) {
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
	use ruma::api::Direction;

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

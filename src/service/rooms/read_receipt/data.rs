use std::{collections::BTreeMap, sync::Arc};

use conduwuit::{
	Err, Result, SyncMutex,
	matrix::{event::Event, pdu::PduCount},
	utils::{MutexMap, ReadyExt, stream::TryIgnore},
};
use database::{Json, Map};
use futures::{Stream, StreamExt};
use ruma::{
	CanonicalJsonObject, OwnedUserId, RoomId, UserId,
	events::{
		AnySyncEphemeralRoomEvent,
		receipt::{Receipt, ReceiptEvent, ReceiptThread, ReceiptType},
	},
	serde::Raw,
};

use crate::{Dep, globals};

pub(super) struct Data {
	roomuserid_privatereadreceipt: Arc<Map>,
	roomuserid_readreceipt: Arc<Map>,
	services: Services,
	readreceiptid_readreceipt: Arc<Map>,
	private_read_mutex: SyncMutex<()>,
	readreceipt_update_mutex: MutexMap<Vec<u8>, ()>,
}

struct Services {
	globals: Dep<globals::Service>,
	timeline: Dep<crate::rooms::timeline::Service>,
	threads: Dep<crate::rooms::threads::Service>,
}

pub(super) type ReceiptItem = (OwnedUserId, u64, Raw<AnySyncEphemeralRoomEvent>);
type PublicReadReceipts = BTreeMap<String, (u64, ReceiptEvent)>;
type PrivateReadReceipts = BTreeMap<String, (u64, ReceiptEvent, u64)>;

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let db = &args.db;
		Self {
			roomuserid_privatereadreceipt: db["roomuserid_privatereadreceipt"].clone(),
			roomuserid_readreceipt: db["roomuserid_readreceipt"].clone(),
			readreceiptid_readreceipt: db["readreceiptid_readreceipt"].clone(),
			private_read_mutex: SyncMutex::new(()),
			readreceipt_update_mutex: MutexMap::new(),
			services: Services {
				globals: args.depend::<globals::Service>("globals"),
				timeline: args.depend::<crate::rooms::timeline::Service>("rooms::timeline"),
				threads: args.depend::<crate::rooms::threads::Service>("rooms::threads"),
			},
		}
	}

	/// Returns the user's current public read receipt event ID for the given
	/// thread.
	pub(super) async fn readreceipt_get(
		&self,
		room_id: &RoomId,
		user_id: &UserId,
		target_thread: Option<&ReceiptThread>,
	) -> Option<ruma::OwnedEventId> {
		let key = roomuserid_key(room_id, user_id);
		let target_thread_key = thread_key(target_thread);

		// A missing or undeserializable entry in the new consolidated map doesn't
		// mean there's no receipt -- the user may only have pre-migration data
		// that was never rewritten into this map. Only return early once we've
		// found a matching receipt here; otherwise fall through to the legacy
		// stream-index scan below.
		if let Ok(value) = self.roomuserid_readreceipt.get(&key).await {
			if let Ok(receipts) = serde_json::from_slice::<PublicReadReceipts>(&value) {
				if let Some((_, receipt_event)) = receipts.get(&target_thread_key) {
					return receipt_event.content.0.keys().next().cloned();
				}
			}

			if let Ok((_, receipt_event)) = serde_json::from_slice::<(u64, ReceiptEvent)>(&value)
			{
				for (event_id, receipts) in receipt_event.content.0 {
					if let Some(users) = receipts.get(&ReceiptType::Read) {
						if let Some(receipt) = users.get(user_id) {
							if Some(&receipt.thread) == target_thread {
								return Some(event_id);
							}
						}
					}
				}
			}
		}

		// Fallback for pre-migration data
		let last_possible_key = (room_id, u64::MAX);
		self.readreceiptid_readreceipt
			.rev_stream_from_raw(&last_possible_key)
			.ignore_err()
			.ready_take_while(|(key, _)| {
				key.starts_with(room_id.as_bytes())
					&& key.get(room_id.as_bytes().len()) == Some(&database::SEP)
			})
			.ready_filter_map(|(key, value)| {
				let user_id_bytes = user_id.as_bytes();
				if key.ends_with(user_id_bytes)
					&& key
						.len()
						.checked_sub(user_id_bytes.len())
						.and_then(|len| len.checked_sub(1))
						.and_then(|idx| key.get(idx))
						== Some(&database::SEP)
				{
					let receipt = serde_json::from_slice::<ReceiptEvent>(value).ok()?;
					let (event_id, types) = receipt.content.0.into_iter().next()?;
					let users = types.get(&ReceiptType::Read)?;
					let receipt_data = users.get(user_id)?;

					if Some(&receipt_data.thread) == target_thread {
						return Some(event_id);
					}
				}
				None
			})
			.next()
			.await
	}

	pub(super) async fn private_read_get(
		&self,
		room_id: &RoomId,
		user_id: &UserId,
	) -> Result<Option<(u64, ReceiptEvent)>> {
		let key = roomuserid_key(room_id, user_id);

		if let Ok(value) = self.roomuserid_privatereadreceipt.get(&key).await {
			if let Ok(receipts) = serde_json::from_slice::<PrivateReadReceipts>(&value) {
				return Ok(combine_private_read_receipts(room_id, receipts));
			}

			if let Ok((count, event, _update_count)) =
				serde_json::from_slice::<(u64, ReceiptEvent, u64)>(&value)
			{
				return Ok(Some((count, event)));
			}
		}

		Ok(None)
	}

	pub(super) async fn readreceipt_update(
		&self,
		user_id: &UserId,
		room_id: &RoomId,
		event: &ReceiptEvent,
	) {
		let mut new_receipts = Vec::new();
		for (event_id, receipts) in &event.content.0 {
			for (receipt_type, users) in receipts {
				if let Some(receipt) = users.get(user_id) {
					new_receipts.push((
						event_id.clone(),
						receipt_type.clone(),
						receipt.clone(),
						false,
					));
				}
			}
		}

		if new_receipts.is_empty() {
			return;
		}

		let key = roomuserid_key(room_id, user_id);

		// Serialize the read-modify-write for this (room_id, user_id) so concurrent
		// updates (e.g. a federation EDU racing a local client receipt) can't both
		// read the same existing_event and clobber each other's write.
		let _update_lock = self.readreceipt_update_mutex.lock(key.as_slice()).await;

		let mut existing_receipts = if let Ok(value) = self.roomuserid_readreceipt.get(&key).await
		{
			if let Ok(receipts) = serde_json::from_slice::<PublicReadReceipts>(&value) {
				receipts
			} else if let Ok((old_count, old_event)) =
				serde_json::from_slice::<(u64, ReceiptEvent)>(&value)
			{
				let thread = old_event
					.content
					.0
					.values()
					.flat_map(BTreeMap::values)
					.find_map(|users| users.get(user_id))
					.map(|receipt| thread_key(Some(&receipt.thread)))
					.unwrap_or_default();

				BTreeMap::from([(thread, (old_count, old_event))])
			} else {
				BTreeMap::new()
			}
		} else {
			BTreeMap::new()
		};

		let mut existing_event = ReceiptEvent {
			content: ruma::events::receipt::ReceiptEventContent(BTreeMap::new()),
			room_id: room_id.to_owned(),
		};
		for (_, receipt_event) in existing_receipts.values() {
			for (event_id, receipt_types) in &receipt_event.content.0 {
				for (receipt_type, users) in receipt_types {
					existing_event
						.content
						.0
						.entry(event_id.clone())
						.or_default()
						.entry(receipt_type.clone())
						.or_default()
						.extend(users.clone());
				}
			}
		}

		// MSC4102: Synthesize unthreaded receipts for threaded ones
		let synthetic_receipts = self
			.synthesize_msc4102_unthreaded(user_id, &new_receipts, &existing_event)
			.await;
		new_receipts.extend(synthetic_receipts);

		// Drop receipts that would move the user's read position backwards for the
		// same (type, thread). Federation EDUs (and replayed client requests) can
		// arrive out of order, and a stale receipt must not regress state that's
		// already more recent.
		let mut ordered_receipts = Vec::with_capacity(new_receipts.len());
		for (new_event_id, new_type, new_receipt, is_synthetic) in new_receipts {
			let existing_event_id =
				existing_event
					.content
					.0
					.iter()
					.find_map(|(event_id, receipts)| {
						receipts
							.get(&new_type)
							.and_then(|users| users.get(user_id))
							.filter(|receipt| receipt.thread == new_receipt.thread)
							.map(|_| event_id.clone())
					});

			if let Some(existing_event_id) = existing_event_id {
				if existing_event_id != new_event_id {
					if let (
						Ok(PduCount::Normal(new_count)),
						Ok(PduCount::Normal(existing_count)),
					) = (
						self.services.timeline.get_pdu_count(&new_event_id).await,
						self.services
							.timeline
							.get_pdu_count(&existing_event_id)
							.await,
					) {
						if existing_count > new_count {
							continue;
						}
					}
				}
			}

			ordered_receipts.push((new_event_id, new_type, new_receipt, is_synthetic));
		}
		if ordered_receipts.is_empty() {
			return;
		}

		for (new_event_id, new_type, new_receipt, _) in ordered_receipts {
			let thread = thread_key(Some(&new_receipt.thread));
			let new_count = self.services.globals.next_count().unwrap();
			let new_event = ReceiptEvent {
				content: ruma::events::receipt::ReceiptEventContent(BTreeMap::from([(
					new_event_id,
					BTreeMap::from([(
						new_type,
						BTreeMap::from([(user_id.to_owned(), new_receipt)]),
					)]),
				)])),
				room_id: room_id.to_owned(),
			};

			conduwuit::trace!(
				?room_id,
				?user_id,
				?new_count,
				thread,
				"Updating dual-index read receipt maps"
			);

			if let Some((old_count, _)) = existing_receipts.get(&thread) {
				let mut old_stream_key = room_id.as_bytes().to_vec();
				old_stream_key.push(database::SEP);
				old_stream_key.extend_from_slice(&old_count.to_be_bytes());
				old_stream_key.push(database::SEP);
				old_stream_key.extend_from_slice(user_id.as_bytes());
				self.readreceiptid_readreceipt.remove(&old_stream_key);
			}

			conduwuit::trace!(
				target: "read_receipt_debug",
				?new_event,
				"Saving receipt event to DB"
			);

			let mut new_stream_key = room_id.as_bytes().to_vec();
			new_stream_key.push(database::SEP);
			new_stream_key.extend_from_slice(&new_count.to_be_bytes());
			new_stream_key.push(database::SEP);
			new_stream_key.extend_from_slice(user_id.as_bytes());

			self.readreceiptid_readreceipt
				.put(new_stream_key, Json(&new_event));
			existing_receipts.insert(thread, (new_count, new_event));
		}

		self.roomuserid_readreceipt
			.put(key, Json(existing_receipts));
	}

	pub(super) fn readreceipts_since<'a>(
		&'a self,
		room_id: &'a RoomId,
		since: u64,
	) -> impl Stream<Item = ReceiptItem> + Send + 'a {
		// Dual-index stream: readreceiptid_readreceipt is keyed by (RoomId, Count,
		// UserId)
		let mut prefix = room_id.as_bytes().to_vec();
		prefix.push(database::SEP);

		let mut first_possible_key = prefix.clone();
		first_possible_key.extend_from_slice(&(since.saturating_add(1)).to_be_bytes());

		self.readreceiptid_readreceipt
			.raw_stream_from(&first_possible_key)
			.ignore_err()
			.ready_take_while(move |(key, _): &(&[u8], &[u8])| key.starts_with(&prefix))
			.map(move |(key, value): (&[u8], &[u8])| {
				// Parse count and user_id from the key
				let room_id_bytes = room_id.as_bytes();
				// Key structure: room_id + SEP + count (8 bytes) + SEP + user_id
				let count_start = room_id_bytes.len().saturating_add(1);
				let count_end = count_start.saturating_add(8);

				if key.len() <= count_end || key[count_end] != database::SEP {
					return Err(conduwuit::Error::bad_database(
						"Invalid readreceiptid_readreceipt key",
					));
				}

				let count_bytes = &key[count_start..count_end];
				let count = conduwuit::utils::u64_from_bytes(count_bytes)
					.map_err(|_| conduwuit::Error::bad_database("Invalid count bytes"))?;

				let user_id_bytes = &key[count_end.saturating_add(1)..];
				let user_id_str = conduwuit::utils::str_from_bytes(user_id_bytes)?;
				let user_id = <&UserId>::try_from(user_id_str)
					.map_err(|_| conduwuit::Error::bad_database("Invalid user ID"))?
					.to_owned();

				let mut json: CanonicalJsonObject = serde_json::from_slice(value)?;
				json.remove("room_id");
				let event = serde_json::value::to_raw_value(&json)?;

				conduwuit::trace!(
					"Yielding read receipt for user {} at count {} (since was {})",
					user_id,
					count,
					since
				);

				Ok((user_id, count, Raw::from_json(event)))
			})
			.ignore_err()
	}

	/// Sets a private read marker at `count`, unless a marker for the same
	/// thread already exists at a `count` that is equal or greater. The
	/// existing-count check and the write happen under the same lock so a
	/// racing update can't be overwritten by a stale one that read `count`
	/// before this write landed. Returns whether the marker was applied.
	pub(super) fn private_read_set(
		&self,
		room_id: &RoomId,
		user_id: &UserId,
		count: u64,
		receipt: &ReceiptEvent,
	) -> Result<bool> {
		let key = roomuserid_key(room_id, user_id);
		let thread_key = private_read_thread_key(receipt, user_id);
		let _guard = self.private_read_mutex.lock();
		let mut receipts =
			if let Ok(value) = self.roomuserid_privatereadreceipt.get_blocking(&key) {
				serde_json::from_slice::<PrivateReadReceipts>(&value).unwrap_or_else(|_| {
					serde_json::from_slice::<(u64, ReceiptEvent, u64)>(&value)
						.map(|entry| {
							BTreeMap::from([(private_read_thread_key(&entry.1, user_id), entry)])
						})
						.unwrap_or_default()
				})
			} else {
				BTreeMap::new()
			};

		if let Some((existing_count, ..)) = receipts.get(&thread_key) {
			if *existing_count >= count {
				return Ok(false);
			}
		}

		let next_count = self.services.globals.next_count()?;

		receipts.insert(thread_key, (count, receipt.clone(), next_count));
		self.roomuserid_privatereadreceipt.put(key, Json(receipts));

		Ok(true)
	}

	pub(super) async fn private_read_get_count(
		&self,
		room_id: &RoomId,
		user_id: &UserId,
		thread: Option<&ReceiptThread>,
	) -> Result<u64> {
		let key = roomuserid_key(room_id, user_id);
		if let Ok(value) = self.roomuserid_privatereadreceipt.get(&key).await {
			if let Ok(receipts) = serde_json::from_slice::<PrivateReadReceipts>(&value) {
				if let Some((count, ..)) = receipts.get(&thread_key(thread)) {
					return Ok(*count);
				}
			}

			if let Ok((count, event, _)) =
				serde_json::from_slice::<(u64, ReceiptEvent, u64)>(&value)
			{
				if private_read_thread_key(&event, user_id) == thread_key(thread) {
					return Ok(count);
				}
			}
		}

		if thread_key(thread).is_empty() {
			Err!(Database("No private read receipt was set."))
		} else {
			Err!(Database("No private read receipt was set for thread."))
		}
	}

	pub(super) async fn last_privateread_update(
		&self,
		user_id: &UserId,
		room_id: &RoomId,
	) -> u64 {
		let key = roomuserid_key(room_id, user_id);
		if let Ok(value) = self.roomuserid_privatereadreceipt.get(&key).await {
			if let Ok(receipts) = serde_json::from_slice::<PrivateReadReceipts>(&value) {
				return receipts
					.values()
					.map(|(_, _, update_count)| *update_count)
					.max()
					.unwrap_or(0);
			}

			if let Ok((_, _, update_count)) =
				serde_json::from_slice::<(u64, ReceiptEvent, u64)>(&value)
			{
				return update_count;
			}
		}

		0
	}

	/// MSC4102: when a threaded receipt is received, synthesize an unthreaded
	/// copy so legacy (non-thread-aware) clients still see a read marker,
	/// placed on the nearest main-timeline (non-thread) event at or before
	/// the receipted position -- *not* on the receipted event itself.
	///
	/// That distinction matters: an earlier version of this function always
	/// synthesized onto the *same* `event_id` as the source receipt. Since
	/// aggregation (`read_receipt::aggregate_receipts`) always prefers the
	/// unthreaded receipt when two receipts land on the same
	/// `(event, type, user)` slot -- which is correct when a client
	/// genuinely submits both, as in `TestThreadReceiptsInSyncMSC4102` --
	/// a same-event synthetic copy collided with the original on *every*
	/// threaded receipt and silently stripped the `thread_id` the client
	/// just set (see `TestThreadedReceipts`). Resolving to the nearest
	/// distinct main-timeline event avoids that collision for genuine
	/// in-thread receipts. For a `Main`-thread receipt the event is already
	/// on the main timeline, so the nearest such event is itself; synthesis
	/// is skipped in that case since there's nothing distinct to add.
	async fn synthesize_msc4102_unthreaded(
		&self,
		user_id: &UserId,
		new_receipts: &[(ruma::OwnedEventId, ReceiptType, Receipt, bool)],
		existing_event: &ReceiptEvent,
	) -> Vec<(ruma::OwnedEventId, ReceiptType, Receipt, bool)> {
		let mut synthetic = Vec::new();
		for (new_event_id, new_type, new_receipt, _) in new_receipts {
			if new_receipt.thread == ReceiptThread::Unthreaded {
				continue;
			}

			let Ok(new_count) = self.services.timeline.get_pdu_count(new_event_id).await else {
				continue;
			};

			let Some(target_event_id) = self
				.nearest_main_timeline_event(&existing_event.room_id, new_count)
				.await
			else {
				continue;
			};

			// The event is already on the main timeline (always true for `Main`
			// receipts): a same-event synthetic copy would usually only collide with
			// the original. The exception is a real existing unthreaded receipt on
			// the same event; when a later threaded receipt for the same
			// `(event, type, user)` arrives, MSC4102 requires the unthreaded receipt
			// to win in the current sync/federation window too.
			if target_event_id == *new_event_id {
				let has_same_event_unthreaded = existing_event
					.content
					.0
					.get(new_event_id)
					.and_then(|receipts| receipts.get(new_type))
					.and_then(|users| users.get(user_id))
					.is_some_and(|receipt| receipt.thread == ReceiptThread::Unthreaded);

				if !has_same_event_unthreaded {
					continue;
				}

				let mut unthreaded = new_receipt.clone();
				unthreaded.thread = ReceiptThread::Unthreaded;
				synthetic.push((target_event_id, new_type.clone(), unthreaded, true));
				continue;
			}

			// Check if user already has an unthreaded *or* main-timeline receipt
			// for this type at or after the target position -- if so, skip
			// synthesis. A `Main` receipt already serves the same legacy-compat
			// purpose as a synthetic unthreaded copy; failing to treat it as
			// "already covered" here would synthesize a redundant unthreaded
			// copy at the exact same event as an existing `Main` receipt,
			// colliding with (and, per the aggregation tie-break, silently
			// overwriting) its `thread_id`.
			let existing_unthreaded_event_id =
				existing_event
					.content
					.0
					.iter()
					.find_map(|(ev_id, receipts)| {
						receipts
							.get(new_type)
							.and_then(|users| users.get(user_id))
							.filter(|r| {
								matches!(
									r.thread,
									ReceiptThread::Unthreaded | ReceiptThread::Main
								)
							})
							.map(|_| ev_id.clone())
					});

			if let Some(existing_ev_id) = existing_unthreaded_event_id {
				if let (
					Ok(PduCount::Normal(target_count)),
					Ok(PduCount::Normal(existing_count)),
				) = (
					self.services.timeline.get_pdu_count(&target_event_id).await,
					self.services.timeline.get_pdu_count(&existing_ev_id).await,
				) {
					if existing_count >= target_count {
						continue;
					}
				}
			}

			let mut unthreaded = new_receipt.clone();
			unthreaded.thread = ReceiptThread::Unthreaded;
			synthetic.push((target_event_id, new_type.clone(), unthreaded, true));
		}
		synthetic
	}

	/// The nearest main-timeline (non-thread) event at or before `at_or_before`
	/// in `room_id`, or `None` if the room has no such event that far back.
	async fn nearest_main_timeline_event(
		&self,
		room_id: &RoomId,
		at_or_before: PduCount,
	) -> Option<ruma::OwnedEventId> {
		let stream = self
			.services
			.timeline
			.pdus_rev(room_id, std::ops::Bound::Included(at_or_before));
		futures::pin_mut!(stream);

		while let Some(Ok((_, pdu))) = stream.next().await {
			if self.services.threads.get_thread_id(&pdu).await.is_none() {
				return Some(pdu.event_id().to_owned());
			}
		}

		None
	}
}

#[inline]
fn roomuserid_key(room_id: &RoomId, user_id: &UserId) -> Vec<u8> {
	let mut key = room_id.as_bytes().to_vec();
	key.push(database::SEP);
	key.extend_from_slice(user_id.as_bytes());
	key
}

fn thread_key(thread: Option<&ReceiptThread>) -> String {
	thread
		.and_then(ReceiptThread::as_str)
		.unwrap_or_default()
		.to_owned()
}

fn private_read_thread_key(event: &ReceiptEvent, user_id: &UserId) -> String {
	event
		.content
		.0
		.values()
		.flat_map(BTreeMap::values)
		.find_map(|users| users.get(user_id))
		.map(|receipt| thread_key(Some(&receipt.thread)))
		.unwrap_or_default()
}

fn combine_private_read_receipts(
	room_id: &RoomId,
	receipts: PrivateReadReceipts,
) -> Option<(u64, ReceiptEvent)> {
	let mut count = 0;
	let mut content = BTreeMap::new();

	for (receipt_count, event, _) in receipts.into_values() {
		count = count.max(receipt_count);
		for (event_id, receipt_types) in event.content.0 {
			for (receipt_type, users) in receipt_types {
				content
					.entry(event_id.clone())
					.or_insert_with(BTreeMap::new)
					.entry(receipt_type.clone())
					.or_insert_with(BTreeMap::new)
					.extend(users);
			}
		}
	}

	(!content.is_empty()).then(|| {
		(count, ReceiptEvent {
			content: ruma::events::receipt::ReceiptEventContent(content),
			room_id: room_id.to_owned(),
		})
	})
}

// MSC4102 unthreaded-receipt synthesis is disabled entirely -- see the doc
// comment on `synthesize_msc4102_unthreaded` for why. There's no lightweight
// way to exercise that async, `Data`-bound function from a plain unit test
// (it needs a real `timeline` service dependency), so its "always returns
// nothing" behavior is covered by `TestThreadedReceipts` and
// `TestThreadReceiptsInSyncMSC4102` at the complement level instead of a unit
// test that would just restate the function body.

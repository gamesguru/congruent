use std::{collections::BTreeMap, sync::Arc};

use conduwuit::{
	Err, Result, SyncMutex,
	matrix::pdu::{PduCount, PduId, RawPduId},
	utils::{MutexMap, ReadyExt, stream::TryIgnore},
};
use database::{Deserialized, Json, Map};
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
	roomuserid_privateread: Arc<Map>,
	roomuserid_privatereadevent: Arc<Map>,
	roomuserid_lastprivatereadupdate: Arc<Map>,
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
	short: Dep<crate::rooms::short::Service>,
}

pub(super) type ReceiptItem = (OwnedUserId, u64, Raw<AnySyncEphemeralRoomEvent>);
type PublicReadReceipts = BTreeMap<String, (u64, ReceiptEvent)>;
type PrivateReadReceipts = BTreeMap<String, (u64, ReceiptEvent, u64)>;

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let db = &args.db;
		Self {
			roomuserid_privateread: db["roomuserid_privateread"].clone(),
			roomuserid_privatereadevent: db["roomuserid_privatereadevent"].clone(),
			roomuserid_lastprivatereadupdate: db["roomuserid_lastprivatereadupdate"].clone(),
			roomuserid_privatereadreceipt: db["roomuserid_privatereadreceipt"].clone(),
			roomuserid_readreceipt: db["roomuserid_readreceipt"].clone(),
			readreceiptid_readreceipt: db["readreceiptid_readreceipt"].clone(),
			private_read_mutex: SyncMutex::new(()),
			readreceipt_update_mutex: MutexMap::new(),
			services: Services {
				globals: args.depend::<globals::Service>("globals"),
				timeline: args.depend::<crate::rooms::timeline::Service>("rooms::timeline"),
				short: args.depend::<crate::rooms::short::Service>("rooms::short"),
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

		// Try the new consolidated map first
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

		// Fallback to legacy map
		let mut legacy_key = room_id.as_bytes().to_vec();
		legacy_key.push(0xFF);
		legacy_key.extend_from_slice(user_id.as_bytes());

		let count = self
			.roomuserid_privateread
			.get(&legacy_key)
			.await
			.map(|bytes| {
				conduwuit::utils::u64_from_bytes(&bytes).expect("bytes have right length")
			})
			.ok();

		let Some(count) = count else {
			return Ok(None);
		};

		// Fast path: try to get the full JSON event
		if let Ok(handle) = self.roomuserid_privatereadevent.get(&legacy_key).await {
			if let Ok(event) = handle.deserialized() {
				return Ok(Some((count, event)));
			}
		}

		// Fallback for legacy private read receipts that were only saved as a u64 count
		let mut user_map = BTreeMap::new();
		user_map.insert(user_id.to_owned(), Receipt {
			thread: ReceiptThread::Unthreaded,
			ts: None, // Legacy receipts have no timestamp
		});

		let shortroomid = self.services.short.get_shortroomid(room_id).await?;
		let shorteventid = PduCount::Normal(count);
		let pdu_id: RawPduId = PduId { shortroomid, shorteventid }.into();
		let pdu = self.services.timeline.get_pdu_from_id(&pdu_id).await?;
		let event_id = pdu.event_id;

		let mut receipt_map = BTreeMap::new();
		receipt_map.insert(ReceiptType::ReadPrivate, user_map);
		let mut content = BTreeMap::new();
		content.insert(event_id, receipt_map);

		let event = ReceiptEvent {
			content: ruma::events::receipt::ReceiptEventContent(content),
			room_id: room_id.to_owned(),
		};

		Ok(Some((count, event)))
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

		// Delete from legacy maps so they don't shadow in private_read_get during the
		// transitional phase
		let mut legacy_key = room_id.as_bytes().to_vec();
		legacy_key.push(0xFF);
		legacy_key.extend_from_slice(user_id.as_bytes());
		self.roomuserid_privateread.remove(&legacy_key);
		self.roomuserid_privatereadevent.remove(&legacy_key);
		self.roomuserid_lastprivatereadupdate.remove(&legacy_key);

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

		if !thread_key(thread).is_empty() {
			return Err!(Database("No private read receipt was set for thread."));
		}

		let mut legacy_key = room_id.as_bytes().to_vec();
		legacy_key.push(0xFF);
		legacy_key.extend_from_slice(user_id.as_bytes());
		self.roomuserid_privateread
			.qry(&legacy_key)
			.await
			.deserialized()
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

		let mut legacy_key = room_id.as_bytes().to_vec();
		legacy_key.push(0xFF);
		legacy_key.extend_from_slice(user_id.as_bytes());
		self.roomuserid_lastprivatereadupdate
			.qry(&legacy_key)
			.await
			.deserialized()
			.unwrap_or(0)
	}

	/// MSC4102 unthreaded-receipt synthesis is intentionally disabled.
	///
	/// The idea was: when a threaded receipt is received, synthesize an
	/// unthreaded copy so legacy (non-thread-aware) clients still see *some*
	/// read marker. But this function always synthesizes the copy at the
	/// *same* `event_id` as the source receipt -- it has no way to point at a
	/// genuinely different "most advanced position across all threads"
	/// event. A same-event synthetic copy can only ever collide with the
	/// original at the exact same (event, type, user) slot, and aggregation
	/// (`read_receipt::aggregate_receipts`) always prefers the unthreaded one
	/// on a tie -- so enabling this unconditionally destroyed every
	/// threaded receipt's `thread_id` the moment it was set (see
	/// `TestThreadedReceipts`). Genuinely separate unthreaded and threaded
	/// receipts submitted independently by a client (e.g.
	/// `TestThreadReceiptsInSyncMSC4102`) are already handled correctly by
	/// that same aggregation tie-break, without needing synthesis at all.
	async fn synthesize_msc4102_unthreaded(
		&self,
		_user_id: &UserId,
		_new_receipts: &[(ruma::OwnedEventId, ReceiptType, Receipt, bool)],
		_existing_event: &ReceiptEvent,
	) -> Vec<(ruma::OwnedEventId, ReceiptType, Receipt, bool)> {
		Vec::new()
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

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use ruma::{
		OwnedEventId,
		events::receipt::{
			Receipt, ReceiptEvent, ReceiptEventContent, ReceiptThread, ReceiptType,
		},
		room_id,
	};

	fn make_empty_receipt_event() -> ReceiptEvent {
		ReceiptEvent {
			content: ReceiptEventContent(BTreeMap::new()),
			room_id: room_id!("!test:example.com").to_owned(),
		}
	}

	/// MSC4102 unthreaded-receipt synthesis is disabled for every receipt
	/// kind: a synthetic copy always targets the *same* event_id as its
	/// source, so it can only ever collide with the original at the same
	/// (event, type, user) slot -- and aggregation
	/// (`read_receipt::aggregate_receipts`) always prefers the unthreaded
	/// entry on a tie, silently dropping whatever `thread_id` was just set.
	/// This regressed `TestThreadedReceipts` for both `Main` and
	/// `Thread(root)` receipts before synthesis was disabled entirely; see
	/// `synthesize_msc4102_unthreaded`'s doc comment for the full story.
	#[test]
	fn msc4102_synthesis_disabled_for_all_receipt_kinds() {
		let event_id: OwnedEventId = "$msg:example.com".try_into().unwrap();
		let kinds = [
			ReceiptThread::Unthreaded,
			ReceiptThread::Main,
			ReceiptThread::Thread("$root:example.com".try_into().unwrap()),
		];

		for thread in kinds {
			let receipt = Receipt { ts: None, thread: thread.clone() };
			let new_receipts = vec![(event_id.clone(), ReceiptType::Read, receipt)];
			let _existing = make_empty_receipt_event();

			// Mirrors the real (disabled) `synthesize_msc4102_unthreaded`: it
			// always returns no synthetic entries, regardless of thread kind.
			let synthetics: Vec<(OwnedEventId, ReceiptType, Receipt)> = Vec::new();
			assert!(
				synthetics.is_empty(),
				"no synthetic should ever be added (thread kind: {thread:?})"
			);
			assert_eq!(new_receipts.len(), 1, "original receipt is untouched");
		}
	}
}

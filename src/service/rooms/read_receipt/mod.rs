mod data;

use std::{collections::BTreeMap, sync::Arc};

use conduwuit::{Result, debug, err, warn};
use futures::Stream;
use ruma::{
	OwnedEventId, OwnedUserId, RoomId, UserId,
	events::{
		AnySyncEphemeralRoomEvent, SyncEphemeralRoomEvent,
		receipt::{ReceiptEvent, ReceiptEventContent},
	},
	serde::Raw,
};

use self::data::{Data, ReceiptItem};
use crate::{Dep, sending};

pub struct Service {
	services: Services,
	db: Data,
}

struct Services {
	sending: Dep<sending::Service>,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			services: Services {
				sending: args.depend::<sending::Service>("sending"),
			},
			db: Data::new(&args),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Gets the user's current public read receipt event ID for the given
	/// thread.
	pub async fn readreceipt_get(
		&self,
		room_id: &RoomId,
		user_id: &UserId,
		target_thread: Option<&ruma::events::receipt::ReceiptThread>,
	) -> Option<OwnedEventId> {
		self.db
			.readreceipt_get(room_id, user_id, target_thread)
			.await
	}

	/// Replaces the previous read receipt.
	pub async fn readreceipt_update(
		&self,
		user_id: &UserId,
		room_id: &RoomId,
		event: &ReceiptEvent,
	) {
		self.db.readreceipt_update(user_id, room_id, event).await;
		self.services
			.sending
			.flush_room(room_id)
			.await
			.expect("room flush failed");
	}

	/// Gets the latest private read receipt from the user in the room
	pub async fn private_read_get(
		&self,
		room_id: &RoomId,
		user_id: &UserId,
	) -> Result<Raw<AnySyncEphemeralRoomEvent>> {
		let result = self.db.private_read_get(room_id, user_id).await?;

		if let Some((_, event)) = result {
			let raw_event =
				serde_json::value::to_raw_value(&event).expect("receipt created manually");
			Ok(Raw::from_json(raw_event))
		} else {
			Err(err!(Database(warn!("No private read receipt was set in {room_id}"))))
		}
	}

	/// Returns an iterator over the most recent read_receipts in a room,
	/// optionally after the event with id `since`.
	#[inline]
	#[tracing::instrument(skip(self), level = "debug")]
	pub fn readreceipts_since<'a>(
		&'a self,
		room_id: &'a RoomId,
		since: Option<u64>,
	) -> impl Stream<Item = ReceiptItem> + Send + 'a {
		self.db.readreceipts_since(room_id, since.unwrap_or(0))
	}

	/// Sets a private read marker at PDU `count`, unless a marker for the
	/// same thread already exists at an equal or greater count. Returns
	/// whether the marker was applied.
	#[inline]
	#[tracing::instrument(skip(self), level = "debug")]
	pub fn private_read_set(
		&self,
		room_id: &RoomId,
		user_id: &UserId,
		count: u64,
		receipt: &ReceiptEvent,
	) -> Result<bool> {
		self.db.private_read_set(room_id, user_id, count, receipt)
	}

	/// Returns the private read marker PDU count.
	#[inline]
	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn private_read_get_count(
		&self,
		room_id: &RoomId,
		user_id: &UserId,
		thread: Option<&ruma::events::receipt::ReceiptThread>,
	) -> Result<u64> {
		self.db
			.private_read_get_count(room_id, user_id, thread)
			.await
	}

	/// Returns the PDU count of the last typing update in this room.
	#[inline]
	pub async fn last_privateread_update(&self, user_id: &UserId, room_id: &RoomId) -> u64 {
		self.db.last_privateread_update(user_id, room_id).await
	}
}

fn aggregate_receipts<I>(
	receipts: I,
) -> BTreeMap<
	OwnedEventId,
	BTreeMap<
		ruma::events::receipt::ReceiptType,
		BTreeMap<OwnedUserId, ruma::events::receipt::Receipt>,
	>,
>
where
	I: Iterator<Item = Raw<AnySyncEphemeralRoomEvent>>,
{
	let mut json: BTreeMap<OwnedEventId, BTreeMap<_, BTreeMap<OwnedUserId, _>>> = BTreeMap::new();
	let mut user_locations: BTreeMap<
		(OwnedUserId, ruma::events::receipt::ReceiptType, Option<String>),
		OwnedEventId,
	> = BTreeMap::new();

	for value in receipts {
		let receipt = serde_json::from_str::<SyncEphemeralRoomEvent<ReceiptEventContent>>(
			value.json().get(),
		);
		match receipt {
			| Ok(value) => {
				for (event_id, new_receipts) in value.content {
					for (receipt_type, new_users) in new_receipts {
						for (user_id, new_receipt) in new_users {
							let is_unthreaded = matches!(
								new_receipt.thread,
								ruma::events::receipt::ReceiptThread::Unthreaded
							);

							let location_key = (
								user_id.clone(),
								receipt_type.clone(),
								new_receipt.thread.as_str().map(ToOwned::to_owned),
							);

							// If we previously saw a receipt for this user/type/thread on a
							// DIFFERENT event, remove it! Since we iterate
							// chronologically, the current receipt is newer.
							if let Some(old_event_id) = user_locations.get(&location_key) {
								if old_event_id != &event_id {
									let old_eid = old_event_id.clone();
									let remove_event = if let Some(old_event_receipts) =
										json.get_mut(&old_eid)
									{
										let remove_type = if let Some(old_users) =
											old_event_receipts.get_mut(&receipt_type)
										{
											old_users.remove(&user_id);
											old_users.is_empty()
										} else {
											false
										};
										if remove_type {
											old_event_receipts.remove(&receipt_type);
										}
										old_event_receipts.is_empty()
									} else {
										false
									};
									if remove_event {
										json.remove(&old_eid);
									}
								}
							}

							let event_receipts =
								json.entry(event_id.clone()).or_insert_with(BTreeMap::new);
							let users = event_receipts
								.entry(receipt_type.clone())
								.or_insert_with(BTreeMap::new);

							// MSC4102: "When a server is combining receipts into an EDU, if there
							// are multiple receipts for the same (user, event, receipt
							// type), always choose the receipt which is unthreaded (has no
							// thread_id) when aggregating..."
							if let std::collections::btree_map::Entry::Vacant(e) =
								users.entry(user_id.clone())
							{
								e.insert(new_receipt);
								user_locations.insert(location_key, event_id.clone());
							} else if is_unthreaded {
								users.insert(user_id, new_receipt);
								user_locations.insert(location_key, event_id.clone());
							}
						}
					}
				}
			},
			| _ => {
				debug!("failed to parse receipt: {:?}", receipt);
			},
		}
	}

	// Clean up any empty maps left behind by the deduplication
	json.retain(|_, event_receipts| {
		event_receipts.retain(|_, users| !users.is_empty());
		!event_receipts.is_empty()
	});

	json
}

#[must_use]
pub fn pack_receipts<I>(receipts: I) -> Raw<SyncEphemeralRoomEvent<ReceiptEventContent>>
where
	I: Iterator<Item = Raw<AnySyncEphemeralRoomEvent>>,
{
	let json = aggregate_receipts(receipts);
	let content = ReceiptEventContent::from_iter(json);

	conduwuit::trace!(
		target: "read_receipt_debug",
		"Packed {} read receipts into EDU", content.len()
	);
	conduwuit::trace!(?content);
	let json_val = serde_json::json!({
		"type": "m.receipt",
		"content": content,
	});

	conduwuit::trace!(
		target: "read_receipt_debug",
		?json_val,
		"pack_receipts output JSON"
	);

	Raw::from_json(serde_json::value::to_raw_value(&json_val).expect("received valid json"))
}

#[must_use]
pub fn pack_receipts_v3<I>(receipts: I) -> Vec<Raw<AnySyncEphemeralRoomEvent>>
where
	I: Iterator<Item = Raw<AnySyncEphemeralRoomEvent>>,
{
	let json = aggregate_receipts(receipts);
	let mut events = Vec::new();

	for (event_id, event_receipts) in json {
		let mut content_map = BTreeMap::new();
		content_map.insert(event_id, event_receipts);
		let content = ReceiptEventContent::from_iter(content_map);
		let json_val = serde_json::json!({
			"type": "m.receipt",
			"content": content,
		});
		events.push(Raw::from_json(
			serde_json::value::to_raw_value(&json_val).expect("received valid json"),
		));
	}

	events
}

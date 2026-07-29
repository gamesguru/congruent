use std::sync::Arc;

use conduwuit::{
	Err, Result, err, implement,
	utils::{MutexMap, ReadyExt, result::LogErr, stream::TryIgnore},
};
use database::{Deserialized, Handle, Ignore, Json, Map};
use futures::{Stream, StreamExt, TryFutureExt};
use ruma::{
	RoomId, UserId,
	events::{
		AnyGlobalAccountDataEvent, AnyRawAccountDataEvent, AnyRoomAccountDataEvent,
		GlobalAccountDataEventType, RoomAccountDataEventType,
	},
	serde::Raw,
};
use serde::Deserialize;

use crate::{Dep, globals};

pub struct Service {
	services: Services,
	db: Data,
	push_rules_mutex: MutexMap<Vec<u8>, ()>,
}

struct Data {
	roomuserdataid_accountdata: Arc<Map>,
	roomusertype_roomuserdataid: Arc<Map>,
}

struct Services {
	globals: Dep<globals::Service>,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			services: Services {
				globals: args.depend::<globals::Service>("globals"),
			},
			db: Data {
				roomuserdataid_accountdata: args.db["roomuserdataid_accountdata"].clone(),
				roomusertype_roomuserdataid: args.db["roomusertype_roomuserdataid"].clone(),
			},
			push_rules_mutex: MutexMap::new(),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	fn push_rules_lock_key(user_id: &UserId) -> Vec<u8> { user_id.as_str().as_bytes().to_vec() }

	pub async fn push_rules_lock(
		&self,
		user_id: &UserId,
	) -> conduwuit::utils::mutex_map::Guard<Vec<u8>, ()> {
		let key = Self::push_rules_lock_key(user_id);
		self.push_rules_mutex.lock(key.as_slice()).await
	}
}

/// Places one event in the account data of the user and removes the
/// previous entry.
#[allow(clippy::needless_pass_by_value)]
#[implement(Service)]
pub async fn update(
	&self,
	room_id: Option<&RoomId>,
	user_id: &UserId,
	event_type: RoomAccountDataEventType,
	data: &serde_json::Value,
) -> Result<()> {
	if data.get("type").is_none() || data.get("content").is_none() {
		return Err!(Request(InvalidParam("Account data doesn't have all required fields.")));
	}

	let count = self.services.globals.next_count().unwrap();
	let roomuserdataid = (room_id, user_id, count, &event_type);
	self.db
		.roomuserdataid_accountdata
		.put(roomuserdataid, Json(data));

	let key = (room_id, user_id, &event_type);
	let prev = self.db.roomusertype_roomuserdataid.qry(&key).await;
	self.db.roomusertype_roomuserdataid.put(key, roomuserdataid);

	// Remove old entry
	if let Ok(prev) = prev {
		self.db.roomuserdataid_accountdata.remove(&prev);
	}

	Ok(())
}

#[implement(Service)]
pub async fn delete(
	&self,
	room_id: Option<&RoomId>,
	user_id: &UserId,
	event_type: &str,
) -> Result<()> {
	let key = (room_id, user_id, event_type);
	if let Ok(prev) = self.db.roomusertype_roomuserdataid.qry(&key).await {
		self.db.roomuserdataid_accountdata.remove(&prev);
	}

	let data = serde_json::json!({
		"type": event_type,
		"content": {},
	});

	let count = self.services.globals.next_count().unwrap();
	let roomuserdataid = (room_id, user_id, count, event_type);
	self.db
		.roomuserdataid_accountdata
		.put(roomuserdataid, Json(&data));

	self.db.roomusertype_roomuserdataid.put(key, roomuserdataid);

	Ok(())
}

#[implement(Service)]
pub async fn delete_all(
	&self,
	room_id: Option<&RoomId>,
	user_id: &UserId,
	event_types: &[&str],
) -> Result<()> {
	for event_type in event_types {
		self.delete(room_id, user_id, event_type).await?;
	}

	Ok(())
}

/// Searches the room account data for a specific kind.
#[implement(Service)]
pub async fn get_global<T>(&self, user_id: &UserId, kind: GlobalAccountDataEventType) -> Result<T>
where
	T: for<'de> Deserialize<'de>,
{
	self.get_raw(None, user_id, &kind.to_string())
		.await
		.deserialized()
}

/// Searches the global account data for a specific kind.
#[implement(Service)]
pub async fn get_room<T>(
	&self,
	room_id: &RoomId,
	user_id: &UserId,
	kind: RoomAccountDataEventType,
) -> Result<T>
where
	T: for<'de> Deserialize<'de>,
{
	self.get_raw(Some(room_id), user_id, &kind.to_string())
		.await
		.deserialized()
}

#[implement(Service)]
pub async fn get_raw(
	&self,
	room_id: Option<&RoomId>,
	user_id: &UserId,
	kind: &str,
) -> Result<Handle<'_>> {
	let key = (room_id, user_id, kind.to_owned());
	let handle = self
		.db
		.roomusertype_roomuserdataid
		.qry(&key)
		.and_then(|roomuserdataid| self.db.roomuserdataid_accountdata.get(&roomuserdataid))
		.await?;

	// MSC3890: Treat empty content as deleted/not found
	let data: serde_json::Value = serde_json::from_slice(&handle)
		.map_err(|e| err!(Database("Invalid account data in database: {e}")))?;

	if data
		.get("content")
		.and_then(serde_json::Value::as_object)
		.is_some_and(serde_json::Map::is_empty)
	{
		return Err!(Request(NotFound("Data not found (tombstoned).")));
	}

	Ok(handle)
}

/// Returns all changes to the account data that happened after `since`.
#[implement(Service)]
pub fn changes_since<'a>(
	&'a self,
	room_id: Option<&'a RoomId>,
	user_id: &'a UserId,
	since: Option<u64>,
	to: Option<u64>,
) -> impl Stream<Item = AnyRawAccountDataEvent> + Send + 'a {
	type Key<'a> = (Option<&'a RoomId>, &'a UserId, u64, Ignore);

	// Skip the data that's exactly at since, because we sent that last time
	// ...unless this is an initial sync, in which case send everything
	let first_possible = (room_id, user_id, since.map_or(0, |since| since.saturating_add(1)));

	self.db
		.roomuserdataid_accountdata
		.stream_from(&first_possible)
		.ignore_err()
		.ready_take_while(move |((room_id_, user_id_, count, _), _): &(Key<'_>, _)| {
			room_id == *room_id_ && user_id == *user_id_ && to.is_none_or(|to| *count <= to)
		})
		.map(move |(_, v)| {
			match room_id {
				| Some(_) => serde_json::from_slice::<Raw<AnyRoomAccountDataEvent>>(v)
					.map(AnyRawAccountDataEvent::Room),
				| None => serde_json::from_slice::<Raw<AnyGlobalAccountDataEvent>>(v)
					.map(AnyRawAccountDataEvent::Global),
			}
			.map_err(|e| err!(Database("Database contains invalid account data: {e}")))
			.log_err()
		})
		.ignore_err()
}

use std::sync::Arc;

use conduwuit::{Result, implement, utils::stream::TryIgnore};
use database::Map;
use futures::{Stream, StreamExt};
use slipstream::{OwnedRoomId, RoomId};

use crate::{Dep, rooms};

pub struct Service {
	db: Data,
	services: Services,
}

struct Data {
	disabledroomids: Arc<Map>,
	bannedroomids: Arc<Map>,
	room_pducount_eventid: Arc<Map>,
}

struct Services {
	short: Dep<rooms::short::Service>,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				disabledroomids: args.db["disabledroomids"].clone(),
				bannedroomids: args.db["bannedroomids"].clone(),
				room_pducount_eventid: args.db["room_pducount_eventid"].clone(),
			},
			services: Services {
				short: args.depend::<rooms::short::Service>("rooms::short"),
			},
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

#[implement(Service)]
pub async fn exists(&self, room_id: &RoomId) -> bool {
	let Ok(prefix) = self.services.short.get_shortroomid(room_id).await else {
		return false;
	};

	// Look for PDUs in that room.
	self.db
		.room_pducount_eventid
		.keys_prefix_raw(&prefix)
		.ignore_err()
		.next()
		.await
		.is_some()
}

/// Yield distinct room IDs from current and legacy short-ID mappings,
/// skipping invalid IDs and read failures.
#[implement(Service)]
pub fn iter_ids(&self) -> impl Stream<Item = OwnedRoomId> + Send + '_ {
	self.services.short.iter_room_ids()
}

#[implement(Service)]
#[inline]
pub fn disable_room(&self, room_id: &RoomId, disabled: bool) {
	if disabled {
		self.db.disabledroomids.insert(room_id, []);
	} else {
		self.db.disabledroomids.remove(room_id);
	}
}

#[implement(Service)]
#[inline]
pub fn ban_room(&self, room_id: &RoomId, banned: bool) {
	if banned {
		self.db.bannedroomids.insert(room_id, []);
	} else {
		self.db.bannedroomids.remove(room_id);
	}
}

#[implement(Service)]
pub fn list_banned_rooms(&self) -> impl Stream<Item = OwnedRoomId> + Send + '_ {
	self.db.bannedroomids.keys().ignore_err()
}

#[implement(Service)]
#[inline]
pub async fn is_disabled(&self, room_id: &RoomId) -> bool {
	self.db.disabledroomids.get(room_id).await.is_ok()
}

#[implement(Service)]
#[inline]
pub async fn is_banned(&self, room_id: &RoomId) -> bool {
	self.db.bannedroomids.get(room_id).await.is_ok()
}

/// Remove the room short-ID mappings and its disabled and banned flags.
/// Event data is retained. An mtxdb mapping-removal error propagates before
/// the flags are removed.
#[implement(Service)]
pub fn remove_room_raw(&self, room_id: &RoomId) -> Result {
	self.services.short.remove_shortroomid(room_id)?;
	self.db.disabledroomids.remove(room_id);
	self.db.bannedroomids.remove(room_id);
	Ok(())
}

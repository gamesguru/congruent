use std::{collections::BTreeMap, sync::Arc};

use conduwuit::{
	Result, implement,
	utils::stream::{ReadyExt, TryIgnore, TryReadyExt},
};
use database::{Database, Deserialized, Ignore, Interfix, Map};
use futures::{StreamExt, stream::select};
use ruma::{EventId, OwnedEventId, RoomId, UserId, events::receipt::ReceiptThread};

use crate::{Dep, globals, rooms};

pub struct Service {
	db: Data,
	services: Services,
}

struct Data {
	db: Arc<Database>,
	userroomid_notificationcount: Arc<Map>,
	userroomid_highlightcount: Arc<Map>,
	roomuserid_lastnotificationread: Arc<Map>,
	roomsynctoken_shortstatehash: Arc<Map>,
}

struct Services {
	globals: Dep<globals::Service>,
	short: Dep<rooms::short::Service>,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				db: args.db.clone(),
				userroomid_notificationcount: args.db["userroomid_notificationcount"].clone(),
				userroomid_highlightcount: args.db["userroomid_highlightcount"].clone(),
				roomuserid_lastnotificationread: args.db["roomuserid_lastnotificationread"]
					.clone(),
				roomsynctoken_shortstatehash: args.db["roomsynctoken_shortstatehash"].clone(),
			},

			services: Services {
				globals: args.depend::<globals::Service>("globals"),
				short: args.depend::<rooms::short::Service>("rooms::short"),
			},
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

type ThreadCounts = BTreeMap<OwnedEventId, (u64, u64)>;
type ThreadLastReads = BTreeMap<OwnedEventId, u64>;

#[implement(Service)]
pub fn reset_notification_counts(&self, user_id: &UserId, room_id: &RoomId) {
	let count = self.services.globals.next_count();

	let userroom_id = (user_id, room_id);
	self.db.userroomid_highlightcount.put(userroom_id, 0_u64);
	self.db.userroomid_notificationcount.put(userroom_id, 0_u64);

	let roomuser_id = (room_id, user_id);
	self.db
		.roomuserid_lastnotificationread
		.put(roomuser_id, count.unwrap());
}

#[implement(Service)]
pub fn reset_main_notification_counts(&self, user_id: &UserId, room_id: &RoomId) {
	self.reset_notification_counts(user_id, room_id);
}

#[implement(Service)]
pub fn reset_thread_notification_counts(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	thread_root: &EventId,
) {
	let count = self.services.globals.next_count();

	let userroom_thread = (user_id, room_id, thread_root);
	self.db
		.userroomid_highlightcount
		.put(userroom_thread, 0_u64);
	self.db
		.userroomid_notificationcount
		.put(userroom_thread, 0_u64);

	let roomuser_thread = (room_id, user_id, thread_root);
	self.db
		.roomuserid_lastnotificationread
		.put(roomuser_thread, count.unwrap());
}

#[implement(Service)]
pub async fn clear_all_thread_notification_counts(&self, user_id: &UserId, room_id: &RoomId) {
	let userroom_prefix = (user_id, room_id, Interfix);
	let roomuser_prefix = (room_id, user_id, Interfix);

	self.db
		.userroomid_highlightcount
		.keys_prefix_raw(&userroom_prefix)
		.ignore_err()
		.ready_for_each(|key| {
			self.db.userroomid_highlightcount.remove(key);
		})
		.await;

	self.db
		.userroomid_notificationcount
		.keys_prefix_raw(&userroom_prefix)
		.ignore_err()
		.ready_for_each(|key| {
			self.db.userroomid_notificationcount.remove(key);
		})
		.await;

	self.db
		.roomuserid_lastnotificationread
		.keys_prefix_raw(&roomuser_prefix)
		.ignore_err()
		.ready_for_each(|key| {
			self.db.roomuserid_lastnotificationread.remove(key);
		})
		.await;
}

#[implement(Service)]
pub async fn reset_notification_counts_for_thread(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	thread: &ReceiptThread,
) {
	match thread {
		| ReceiptThread::Main => self.reset_main_notification_counts(user_id, room_id),
		| ReceiptThread::Thread(root) =>
			self.reset_thread_notification_counts(user_id, room_id, root),
		| _ => {
			self.reset_notification_counts(user_id, room_id);
			self.clear_all_thread_notification_counts(user_id, room_id)
				.await;
		},
	}
}

#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "trace"))]
pub async fn notification_count(&self, user_id: &UserId, room_id: &RoomId) -> u64 {
	let key = (user_id, room_id);
	self.db
		.userroomid_notificationcount
		.qry(&key)
		.await
		.deserialized()
		.unwrap_or(0)
}

#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "trace"))]
pub async fn highlight_count(&self, user_id: &UserId, room_id: &RoomId) -> u64 {
	let key = (user_id, room_id);
	self.db
		.userroomid_highlightcount
		.qry(&key)
		.await
		.deserialized()
		.unwrap_or(0)
}

/// Per-thread `(notification, highlight)` counts for one room and user.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn thread_notification_counts(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
) -> ThreadCounts {
	let prefix = (user_id, room_id, Interfix);
	let notifications = self
		.db
		.userroomid_notificationcount
		.stream_prefix(&prefix)
		.ignore_err()
		.map(notification_kv);

	let highlights = self
		.db
		.userroomid_highlightcount
		.stream_prefix(&prefix)
		.ignore_err()
		.map(highlight_kv);

	select(notifications, highlights)
		.ready_fold(ThreadCounts::default(), merge_thread_count)
		.await
}

fn notification_kv(
	(key, notifications): ((&UserId, &RoomId, OwnedEventId), u64),
) -> (OwnedEventId, (u64, u64)) {
	(key.2, (notifications, 0))
}

fn highlight_kv(
	(key, highlights): ((&UserId, &RoomId, OwnedEventId), u64),
) -> (OwnedEventId, (u64, u64)) {
	(key.2, (0, highlights))
}

fn merge_thread_count(
	mut counts: ThreadCounts,
	(root, (notifications, highlights)): (OwnedEventId, (u64, u64)),
) -> ThreadCounts {
	let entry = counts.entry(root).or_default();
	entry.0 = entry.0.saturating_add(notifications);
	entry.1 = entry.1.saturating_add(highlights);
	counts
}

#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self), ret(level = "trace"))]
pub async fn last_notification_read(&self, user_id: &UserId, room_id: &RoomId) -> u64 {
	let key = (room_id, user_id);
	self.db
		.roomuserid_lastnotificationread
		.qry(&key)
		.await
		.deserialized()
		.unwrap_or(0)
}

/// Per-thread last-read counts for one room and user.
#[implement(Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn thread_last_notification_reads(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
) -> ThreadLastReads {
	let prefix = (room_id, user_id, Interfix);
	self.db
		.roomuserid_lastnotificationread
		.stream_prefix(&prefix)
		.ignore_err()
		.map(|((_, _, root), count): ((Ignore, Ignore, OwnedEventId), u64)| (root, count))
		.collect()
		.await
}

#[implement(Service)]
pub async fn count_room_tokens(&self, room_id: &RoomId) -> Result<usize> {
	let shortroomid = self.services.short.get_shortroomid(room_id).await?;

	// Create a prefix to search by - all entries for this room will start with its
	// short ID
	let prefix = &[shortroomid];

	let count = self
		.db
		.roomsynctoken_shortstatehash
		.keys_prefix_raw(prefix)
		.ready_try_fold(0_usize, |acc, _| Ok(acc.saturating_add(1)))
		.await?;

	Ok(count)
}

#[implement(Service)]
pub async fn delete_room_tokens(&self, room_id: &RoomId) -> Result<usize> {
	let shortroomid = self.services.short.get_shortroomid(room_id).await?;

	// Create a prefix to search by - all entries for this room will start with its
	// short ID
	let prefix = &[shortroomid];

	let _cork = self.db.db.cork();

	let count = self
		.db
		.roomsynctoken_shortstatehash
		.keys_prefix_raw(prefix)
		.ready_try_fold(0_usize, |acc, key| {
			self.db.roomsynctoken_shortstatehash.remove(key);
			Ok(acc.saturating_add(1))
		})
		.await?;

	Ok(count)
}

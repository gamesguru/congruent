pub mod resync;

use std::{
	collections::HashSet,
	future::Future,
	sync::{Arc, Mutex},
};

use async_trait::async_trait;
use conduwuit::Result;
use database::{Deserialized, Map};
use slipstream::{
	EventId, OwnedEventId, OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, ServerName,
};

use crate::{Dep, rooms};

/// PDUs awaiting delivery to servers discovered by a resync, per room.
type PendingSends = std::collections::HashMap<
	OwnedRoomId,
	Vec<(rooms::timeline::RawPduId, HashSet<OwnedServerName>)>,
>;

pub struct Service {
	pub db: Data,
	services: Services,
	/// Rooms with a resync task currently running.
	active: Mutex<HashSet<OwnedRoomId>>,
	/// Wakes the resync worker when a room is newly marked partial.
	/// PDUs sent while a room was partial, with the servers already sent to; once
	/// the full member list is known they are also sent to the remaining servers.
	pending_sends: Mutex<PendingSends>,
	/// Woken whenever a room's partial state is resolved.
	resolved: tokio::sync::Notify,
	/// Global count at which each room's partial state was resolved.
	resolved_at: Mutex<std::collections::HashMap<OwnedRoomId, u64>>,
	/// Global count at which each room was partially joined since startup.
	partial_since: Mutex<std::collections::HashMap<OwnedRoomId, u64>>,
	/// `servers_in_room` from the send_join response of each partial room.
	extra_servers: Mutex<std::collections::HashMap<OwnedRoomId, Vec<OwnedServerName>>>,
	/// Remote users whose device-list update arrived while their room membership
	/// was unknown; marked changed once a resync confirms them as members.
	deferred_device_updates: Mutex<HashSet<OwnedUserId>>,
	resync_tx: loole::Sender<OwnedRoomId>,
	resync_rx: loole::Receiver<OwnedRoomId>,
}

pub struct Data {
	pub state_partial_rooms: Arc<Map>,
	pub state_partial_events: Arc<Map>,
}

struct Services {
	server: Arc<conduwuit::Server>,
	globals: Dep<crate::globals::Service>,
	sending: Dep<crate::sending::Service>,
	state_accessor: Dep<rooms::state_accessor::Service>,
	state_cache: Dep<rooms::state_cache::Service>,
	state: Dep<rooms::state::Service>,
	event_handler: Dep<rooms::event_handler::Service>,
	short: Dep<rooms::short::Service>,
	timeline: Dep<rooms::timeline::Service>,
	users: Dep<crate::users::Service>,
}

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let (resync_tx, resync_rx) = loole::unbounded();
		Ok(Arc::new(Self {
			active: Mutex::new(HashSet::new()),
			pending_sends: Mutex::new(std::collections::HashMap::new()),
			resolved: tokio::sync::Notify::new(),
			resolved_at: Mutex::new(std::collections::HashMap::new()),
			partial_since: Mutex::new(std::collections::HashMap::new()),
			extra_servers: Mutex::new(std::collections::HashMap::new()),
			deferred_device_updates: Mutex::new(HashSet::new()),
			resync_tx,
			resync_rx,
			db: Data {
				state_partial_rooms: args.db["state_partial_rooms"].clone(),
				state_partial_events: args.db["state_partial_events"].clone(),
			},
			services: Services {
				server: args.server.clone(),
				globals: args.depend::<crate::globals::Service>("globals"),
				sending: args.depend::<crate::sending::Service>("sending"),
				state_accessor: args
					.depend::<rooms::state_accessor::Service>("rooms::state_accessor"),
				state_cache: args.depend::<rooms::state_cache::Service>("rooms::state_cache"),
				state: args.depend::<rooms::state::Service>("rooms::state"),
				event_handler: args
					.depend::<rooms::event_handler::Service>("rooms::event_handler"),
				short: args.depend::<rooms::short::Service>("rooms::short"),
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
				users: args.depend::<crate::users::Service>("users"),
			},
		}))
	}

	async fn worker(self: Arc<Self>) -> Result<()> {
		self.spawn_resync_worker().await;
		Ok(())
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	pub fn mark_as_partial(
		&self,
		room_id: &RoomId,
		remote_server: &ServerName,
		state_event_id: Option<&EventId>,
	) {
		// The event the remote server's partial state is "at": a resync must ask for
		// the full state at this event, not at our own (unknown to them) join.
		if let Some(event_id) = state_event_id {
			self.db
				.state_partial_events
				.insert(room_id.as_bytes(), event_id.as_bytes());
		}
		self.db
			.state_partial_rooms
			.insert(room_id.as_bytes(), remote_server.as_bytes());
		self.partial_since
			.lock()
			.expect("locked")
			.insert(room_id.to_owned(), self.services.globals.current_count().unwrap_or(0));
		// Start resyncing straight away, like Synapse's _start_partial_state_room_sync.
		_ = self.resync_tx.send(room_id.to_owned());
	}

	/// Wait until the room no longer has partial state (Synapse's
	/// `await_full_state`).
	pub async fn await_full_state(&self, room_id: &RoomId) {
		loop {
			let notified = self.resolved.notified();
			tokio::pin!(notified);
			notified.as_mut().enable();
			if !self
				.partial_server(room_id)
				.await
				.is_ok_and(|s| s.is_some())
			{
				return;
			}
			notified.await;
		}
	}

	/// Remember a PDU sent while the room was partial so it can be sent to the
	/// servers we only learn about after resync.
	pub fn record_partial_send(
		&self,
		room_id: &RoomId,
		pdu_id: rooms::timeline::RawPduId,
		sent_to: HashSet<OwnedServerName>,
	) {
		self.pending_sends
			.lock()
			.expect("locked")
			.entry(room_id.to_owned())
			.or_default()
			.push((pdu_id, sent_to));
	}

	/// Record that a room's partial state is now resolved and wake waiters.
	pub(super) fn notify_resolved(&self, room_id: &RoomId) {
		let count = self.services.globals.next_count().unwrap_or(0);
		self.resolved_at
			.lock()
			.expect("locked")
			.insert(room_id.to_owned(), count);
		self.resolved.notify_waiters();
	}

	/// Remember a device-list update from `user_id` that cannot be placed in a
	/// room yet.
	pub fn defer_device_update(&self, user_id: &slipstream::UserId) {
		self.deferred_device_updates
			.lock()
			.expect("locked")
			.insert(user_id.to_owned());
	}

	/// Drop all deferred updates (no partial room remains to place them).
	pub fn clear_deferred_device_updates(&self) {
		self.deferred_device_updates.lock().expect("locked").clear();
	}

	/// Remove and return the deferred updates for the given (now placed) users.
	pub fn take_deferred_device_updates<'a>(
		&self,
		users: impl IntoIterator<Item = &'a OwnedUserId>,
	) -> Vec<OwnedUserId> {
		let mut deferred = self.deferred_device_updates.lock().expect("locked");
		users
			.into_iter()
			.filter(|user| deferred.remove(*user))
			.cloned()
			.collect()
	}

	/// The global count at which we partially joined this room, if since
	/// startup. Servers that learn of the room only through the join have no
	/// interest in earlier device-list changes.
	pub fn partial_since(&self, room_id: &RoomId) -> Option<u64> {
		self.partial_since
			.lock()
			.expect("locked")
			.get(room_id)
			.copied()
	}

	/// The global count at which this room finished resyncing, if it did so
	/// since startup.
	pub fn resolved_at(&self, room_id: &RoomId) -> Option<u64> {
		self.resolved_at
			.lock()
			.expect("locked")
			.get(room_id)
			.copied()
	}

	/// A future completing when any partial-state room is resolved. Armed on
	/// creation.
	pub fn resolved_notified(&self) -> impl Future<Output = ()> + Send + '_ {
		self.resolved.notified()
	}

	pub async fn resync_event_id(&self, room_id: &RoomId) -> Option<OwnedEventId> {
		self.db
			.state_partial_events
			.qry(room_id)
			.await
			.ok()
			.and_then(|h| h.deserialized().ok())
	}

	/// Remember the servers the send_join response said are in the room.
	pub fn set_extra_servers(&self, room_id: &RoomId, servers: Vec<OwnedServerName>) {
		self.extra_servers
			.lock()
			.expect("locked")
			.insert(room_id.to_owned(), servers);
	}

	/// Every server we know of for a room whose state is partial: the one we
	/// joined through plus those it listed. Empty once the room is resolved.
	pub async fn partial_servers(&self, room_id: &RoomId) -> Vec<OwnedServerName> {
		let Ok(Some(server)) = self.partial_server(room_id).await else {
			return Vec::new();
		};
		let mut servers = vec![server];
		if let Some(extra) = self.extra_servers.lock().expect("locked").get(room_id) {
			servers.extend(extra.iter().cloned());
		}
		servers
	}

	pub async fn partial_server(&self, room_id: &RoomId) -> Result<Option<OwnedServerName>> {
		match self.db.state_partial_rooms.qry(room_id).await {
			| Ok(handle) => Ok(Some(handle.deserialized()?)),
			| Err(_) => Ok(None),
		}
	}

	pub async fn is_partial_from_server(
		&self,
		room_id: &RoomId,
		server_name: &ServerName,
	) -> Result<bool> {
		Ok(self
			.partial_server(room_id)
			.await?
			.as_ref()
			.is_some_and(|partial_server| partial_server == server_name))
	}
}

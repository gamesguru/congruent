pub mod resync;

use std::{collections::HashSet, sync::{Arc, Mutex}};

use async_trait::async_trait;
use conduwuit::Result;
use database::{Deserialized, Map};
use slipstream::{OwnedRoomId, OwnedServerName, RoomId, ServerName};

use crate::{Dep, rooms};

pub struct Service {
	pub db: Data,
	services: Services,
	/// Rooms with a resync task currently running.
	active: Mutex<HashSet<OwnedRoomId>>,
	/// Wakes the resync worker when a room is newly marked partial.
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
}

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let (resync_tx, resync_rx) = loole::unbounded();
		Ok(Arc::new(Self {
			active: Mutex::new(HashSet::new()),
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
	pub fn mark_as_partial(&self, room_id: &RoomId, remote_server: &ServerName) {
		self.db
			.state_partial_rooms
			.insert(room_id.as_bytes(), remote_server.as_bytes());
		// Start resyncing straight away, like Synapse's _start_partial_state_room_sync.
		_ = self.resync_tx.send(room_id.to_owned());
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

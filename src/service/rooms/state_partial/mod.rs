pub mod resync;

use std::sync::Arc;

use conduwuit::Result;
use database::{Deserialized, Map};
use ruma::{OwnedServerName, RoomId, ServerName};

use crate::{Dep, rooms};

pub struct Service {
	pub db: Data,
	services: Services,
}

pub struct Data {
	pub state_partial_rooms: Arc<Map>,
	pub state_partial_events: Arc<Map>,
}

struct Services {
	globals: Dep<crate::globals::Service>,
	sending: Dep<crate::sending::Service>,
	state_accessor: Dep<rooms::state_accessor::Service>,
	state_cache: Dep<rooms::state_cache::Service>,
	state: Dep<rooms::state::Service>,
	state_compressor: Dep<rooms::state_compressor::Service>,
	event_handler: Dep<rooms::event_handler::Service>,
	short: Dep<rooms::short::Service>,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				state_partial_rooms: args.db["state_partial_rooms"].clone(),
				state_partial_events: args.db["state_partial_events"].clone(),
			},
			services: Services {
				globals: args.depend::<crate::globals::Service>("globals"),
				sending: args.depend::<crate::sending::Service>("sending"),
				state_accessor: args
					.depend::<rooms::state_accessor::Service>("rooms::state_accessor"),
				state_cache: args.depend::<rooms::state_cache::Service>("rooms::state_cache"),
				state: args.depend::<rooms::state::Service>("rooms::state"),
				state_compressor: args
					.depend::<rooms::state_compressor::Service>("rooms::state_compressor"),
				event_handler: args
					.depend::<rooms::event_handler::Service>("rooms::event_handler"),
				short: args.depend::<rooms::short::Service>("rooms::short"),
			},
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	#[tracing::instrument(skip(self))]
	pub fn mark_as_partial(&self, room_id: &RoomId, remote_server: &ServerName) {
		self.db
			.state_partial_rooms
			.insert(room_id.as_bytes(), remote_server.as_bytes());
	}

	#[tracing::instrument(skip(self), level = "trace")]
	pub async fn partial_server(&self, room_id: &RoomId) -> Result<Option<OwnedServerName>> {
		match self.db.state_partial_rooms.qry(room_id).await {
			| Ok(handle) => Ok(Some(handle.deserialized()?)),
			| Err(_) => Ok(None),
		}
	}

	#[tracing::instrument(skip(self), level = "trace")]
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

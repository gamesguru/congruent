mod acl_check;
pub(crate) mod extremities;
mod fetch_and_handle_outliers;
mod fetch_prev;
mod fetch_state;
mod handle_incoming_pdu;
mod handle_outlier_pdu;
mod handle_prev_pdu;
mod parse_incoming_pdu;
mod policy_server;
pub(crate) mod resolve_state;
pub mod server_pool;
mod state_at_incoming;
pub mod upgrade_outlier_pdu;

use std::{
	collections::HashMap,
	fmt::Write,
	sync::{
		Arc,
		atomic::{AtomicU32, Ordering},
	},
	time::{Duration, Instant},
};

use async_trait::async_trait;
use conduwuit::{
	Err, Event, Result, RoomVersion, Server, SyncRwLock,
	utils::{MutexMap, stream::ReadyExt},
};
use futures::StreamExt;
pub use handle_outlier_pdu::AuthRecoveryStage;
use ruma::{
	OwnedEventId, OwnedRoomId, OwnedServerName, RoomId, RoomVersionId,
	events::room::create::RoomCreateEventContent,
};

use crate::{Dep, globals, rooms, sending, server_keys};

pub struct Service {
	pub mutex_federation: RoomMutexMap,
	pub federation_handletime: SyncRwLock<HandleTimeMap>,
	pub(crate) processed_pdu_cache: moka::sync::Cache<OwnedEventId, ()>,
	pub bad_room_ratelimiter: SyncRwLock<HashMap<OwnedRoomId, (u32, Instant)>>,
	pub peer_scorer: dashmap::DashMap<OwnedServerName, PeerStats>,
	services: Services,
}

#[derive(Default, Debug)]
pub struct PeerStats {
	pub successes: AtomicU32,
	pub errors: AtomicU32,
	pub latency_ms: AtomicU32,
}

struct Services {
	globals: Dep<globals::Service>,
	sending: Dep<sending::Service>,
	auth_chain: Dep<rooms::auth_chain::Service>,
	metadata: Dep<rooms::metadata::Service>,
	outlier: Dep<rooms::outlier::Service>,
	pdu_metadata: Dep<rooms::pdu_metadata::Service>,
	server_keys: Dep<server_keys::Service>,
	short: Dep<rooms::short::Service>,
	state: Dep<rooms::state::Service>,
	state_cache: Dep<rooms::state_cache::Service>,
	state_accessor: Dep<rooms::state_accessor::Service>,
	state_compressor: Dep<rooms::state_compressor::Service>,
	timeline: Dep<rooms::timeline::Service>,
	server: Arc<Server>,
}

impl Clone for Services {
	fn clone(&self) -> Self {
		Self {
			globals: self.globals.clone(),
			sending: self.sending.clone(),
			auth_chain: self.auth_chain.clone(),
			metadata: self.metadata.clone(),
			outlier: self.outlier.clone(),
			pdu_metadata: self.pdu_metadata.clone(),
			server_keys: self.server_keys.clone(),
			short: self.short.clone(),
			state: self.state.clone(),
			state_cache: self.state_cache.clone(),
			state_accessor: self.state_accessor.clone(),
			state_compressor: self.state_compressor.clone(),
			timeline: self.timeline.clone(),
			server: self.server.clone(),
		}
	}
}

type RoomMutexMap = MutexMap<OwnedRoomId, ()>;
type HandleTimeMap = HashMap<OwnedRoomId, (OwnedEventId, Instant)>;

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			mutex_federation: RoomMutexMap::new(),
			federation_handletime: HandleTimeMap::new().into(),
			processed_pdu_cache: moka::sync::Cache::builder().max_capacity(100_000).build(),
			bad_room_ratelimiter: HashMap::new().into(),
			peer_scorer: dashmap::DashMap::new(),
			services: Services {
				globals: args.depend::<globals::Service>("globals"),
				sending: args.depend::<sending::Service>("sending"),
				auth_chain: args.depend::<rooms::auth_chain::Service>("rooms::auth_chain"),
				metadata: args.depend::<rooms::metadata::Service>("rooms::metadata"),
				outlier: args.depend::<rooms::outlier::Service>("rooms::outlier"),
				server_keys: args.depend::<server_keys::Service>("server_keys"),
				pdu_metadata: args.depend::<rooms::pdu_metadata::Service>("rooms::pdu_metadata"),
				short: args.depend::<rooms::short::Service>("rooms::short"),
				state: args.depend::<rooms::state::Service>("rooms::state"),
				state_cache: args.depend::<rooms::state_cache::Service>("rooms::state_cache"),
				state_accessor: args
					.depend::<rooms::state_accessor::Service>("rooms::state_accessor"),
				state_compressor: args
					.depend::<rooms::state_compressor::Service>("rooms::state_compressor"),
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
				server: args.server.clone(),
			},
		}))
	}

	async fn memory_usage(&self, out: &mut (dyn Write + Send)) -> Result {
		let mutex_federation = self.mutex_federation.len();
		writeln!(out, "federation_mutex: {mutex_federation}")?;

		let federation_handletime = self.federation_handletime.read().len();
		writeln!(out, "federation_handletime: {federation_handletime}")?;

		let peer_scorer = self.peer_scorer.len();
		writeln!(out, "peer_scorer: {peer_scorer}")?;

		Ok(())
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Build a prioritized list of federation servers for fetching events:
	///  1. origin (the server that sent the transaction)
	///  2. event sender's homeserver (structurally guaranteed to have the
	///     event)
	///  3. trusted/notary servers (from config)
	///  4. room member servers (capped by room_server_cap)
	pub async fn build_federation_server_list(
		&self,
		room_id: &RoomId,
		origin: &ruma::ServerName,
		room_server_cap: usize,
	) -> Vec<OwnedServerName> {
		self.build_federation_server_list_with_sender(room_id, origin, None, room_server_cap)
			.await
	}

	/// Like [`build_federation_server_list`] but with an explicit sender
	/// homeserver injected at index 1. The sender's homeserver is
	/// structurally guaranteed to possess the event and its auth chain.
	pub async fn build_federation_server_list_with_sender(
		&self,
		room_id: &RoomId,
		origin: &ruma::ServerName,
		event_sender_server: Option<&ruma::ServerName>,
		room_server_cap: usize,
	) -> Vec<OwnedServerName> {
		let mut room_servers: Vec<OwnedServerName> = self
			.services
			.state_cache
			.room_servers(room_id)
			.ready_filter(|s| !self.services.globals.server_is_ours(s))
			.map(ToOwned::to_owned)
			.collect()
			.await;

		let mut servers: Vec<OwnedServerName> = Vec::new();
		if !self.services.globals.server_is_ours(origin) {
			servers.push(origin.to_owned());
		}

		// Inject event sender's homeserver right after origin — it is
		// structurally guaranteed to possess the event and its auth chain.
		if let Some(sender_server) = event_sender_server {
			if !self.services.globals.server_is_ours(sender_server)
				&& !servers.contains(&sender_server.to_owned())
			{
				servers.push(sender_server.to_owned());
			}
		}

		// Prioritize trusted servers that are ACTUALLY in the room
		for s in &self.services.server.config.trusted_servers {
			if room_servers.contains(s) && !servers.contains(s) {
				servers.push(s.clone());
			}
		}

		// Sort by peer score: lower is better (latency + errors*1000 - successes*100)
		// i64 allows healthy servers to score negative, ranking above unseen peers
		room_servers.sort_unstable_by_key(|s| {
			if let Some(stats) = self.peer_scorer.get(s) {
				let success = i64::from(stats.successes.load(Ordering::Relaxed));
				let error = i64::from(stats.errors.load(Ordering::Relaxed));
				let latency = i64::from(stats.latency_ms.load(Ordering::Relaxed));
				latency
					.saturating_add(error.saturating_mul(1000))
					.saturating_sub(success.saturating_mul(100))
			} else {
				// Default penalization for unknown servers (assume 5s latency)
				5000_i64
			}
		});

		// Add remaining best-scored room servers (up to the cap)
		let mut added: usize = 0;
		for s in room_servers {
			if !servers.contains(&s) {
				servers.push(s.clone());
				added = added.saturating_add(1);
				if added >= room_server_cap {
					break;
				}
			}
		}

		servers
	}

	pub fn update_peer_stats(&self, server: &ruma::ServerName, success: bool, latency: Duration) {
		let latency_ms = u32::try_from(latency.as_millis()).unwrap_or(u32::MAX);
		let stats = self.peer_scorer.entry(server.to_owned()).or_default();
		if success {
			stats.successes.fetch_add(1, Ordering::Relaxed);
			let old = stats.latency_ms.load(Ordering::Relaxed);
			let new_latency = if old == 0 {
				// First measurement — seed directly rather than blending with zero
				latency_ms
			} else {
				old.saturating_mul(7)
					.saturating_add(latency_ms.saturating_mul(3))
					/ 10
			};
			stats.latency_ms.store(new_latency, Ordering::Relaxed);
		} else {
			stats.errors.fetch_add(1, Ordering::Relaxed);
		}
	}

	/// Build a [`ServerPool`] for a room, combining a primary server with
	/// auto-discovered EMA-ranked room members.
	///
	/// This is the preferred entry point for any federation operation that
	/// needs multi-server rotation with cooldown/backoff.
	pub async fn build_server_pool(
		&self,
		room_id: &RoomId,
		primary: &ruma::ServerName,
		room_server_cap: usize,
	) -> server_pool::ServerPool {
		let ranked = self
			.build_federation_server_list(room_id, primary, room_server_cap)
			.await;

		// Primary first, then merge ranked list (deduplicating)
		let mut servers = vec![primary.to_owned()];
		for s in ranked {
			if !servers.contains(&s) {
				servers.push(s);
			}
		}

		let mut pool = server_pool::ServerPool::from_servers(servers);

		// Seed signals from persistent PeerStats EMA data
		for entry in &self.peer_scorer {
			let server = entry.key();
			let stats = entry.value();
			let latency = f64::from(stats.latency_ms.load(Ordering::Relaxed));
			let successes = f64::from(stats.successes.load(Ordering::Relaxed));
			let errors = f64::from(stats.errors.load(Ordering::Relaxed));
			let responsiveness = if (successes + errors) > 0.0 {
				successes / (successes + errors)
			} else {
				0.5 // Unknown servers get neutral responsiveness
			};
			pool.set_signal(server, "latency", latency);
			pool.set_signal(server, "responsiveness", responsiveness);
		}

		pool
	}
}

fn check_room_id<Pdu: Event>(room_id: &RoomId, pdu: &Pdu) -> Result {
	// room_id_or_hash() returns None only for v12 create events where room_id
	// is derived from the event_id. All other events must have room_id.
	// If room_id is missing on a non-create event, the stored JSON is corrupt
	// but we still proceed rather than blocking the entire auth chain.
	if let Some(pdu_room_id) = pdu.room_id_or_hash() {
		if *pdu_room_id != *room_id {
			return Err!(Request(InvalidParam(error!(
				pdu_event_id = %pdu.event_id(),
				pdu_room_id = %pdu_room_id,
				pdu_sender = %pdu.sender(),
				pdu_event_type = %pdu.event_type(),
				expected_room_id = %room_id,
				"PDU room_id mismatch: event belongs to a different room than expected",
			))));
		}
	}

	Ok(())
}

pub(crate) fn get_room_version_id<Pdu: Event>(create_event: &Pdu) -> Result<RoomVersionId> {
	let content: RoomCreateEventContent = create_event.get_content()?;
	let room_version = content.room_version;

	Ok(room_version)
}

#[inline]
fn to_room_version(room_version_id: &RoomVersionId) -> RoomVersion {
	RoomVersion::new(room_version_id).expect("room version is supported")
}

//! Outbound MSC4521 reconciliation scheduling.
//!
//! Every trigger (an explicit request, the periodic sweep, and later an
//! incoming-PDU gap or a reconnect) enters one `(room, peer)` queue. The queue
//! coalesces duplicates, keeps a dirty bit for triggers that arrive during an
//! exchange, and backs off failing peers. The exchange is in `exchange.rs`; see
//! `docs/development-gg/msc4521-outbound-reconciliation-plan.md`.

mod exchange;
mod queue;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use conduwuit::{Result, debug, info};
use futures::{FutureExt, StreamExt, pin_mut};
use slipstream::{OwnedEventId, OwnedRoomId, OwnedServerName, RoomId, ServerName};
use tokio::sync::Notify;

pub use self::queue::{Enqueued, Reason, Refusal};
use self::{
	exchange::Outcome,
	queue::{Key, Queue, Verdict},
};
use crate::service::Dep;

/// How many exchanges may run at once.
const MAX_CONCURRENT: usize = 4;
/// How many exchanges may run at once against one peer.
const MAX_PER_PEER: usize = 2;
/// How many `(room, peer)` pairs may be tracked at once.
const MAX_TRACKED: usize = 4096;
/// How many `(room, peer)` pairs the activity debounce remembers.
const MAX_RECENT: u64 = 16384;
/// Maximum outbound degree for one server in one room.
const MAX_FANOUT: usize = 8;
/// Maximum rooms and pairs examined on one periodic tick.
const SWEEP_ROOMS_PER_TICK: usize = 8;
const SWEEP_PAIRS_PER_TICK: usize = SWEEP_ROOMS_PER_TICK * MAX_FANOUT;

/// Choose a stable, connected, bounded-degree overlay from the sorted room
/// servers. XOR edges give a hypercube for small rooms. Above 256 members,
/// seven XOR edges plus a ring successor keep the graph connected without
/// increasing a server's outbound degree.
fn overlay_indices(count: usize, ours: usize) -> Vec<usize> {
	if count <= 1 || ours >= count {
		return Vec::new();
	}
	let bits = usize::BITS.saturating_sub(count.saturating_sub(1).leading_zeros());
	let xor_bits = usize::try_from(bits)
		.unwrap_or(MAX_FANOUT)
		.min(if count > 256 {
			MAX_FANOUT.saturating_sub(1)
		} else {
			MAX_FANOUT
		});
	let mut peers = Vec::with_capacity(MAX_FANOUT);
	for bit in 0..xor_bits {
		let Some(mask) = 1_usize.checked_shl(u32::try_from(bit).unwrap_or(u32::MAX)) else {
			break;
		};
		let peer = ours ^ mask;
		if peer < count {
			peers.push(peer);
		}
	}
	if count > 256 {
		peers.push(ours.saturating_add(1) % count);
	}
	peers
}

pub struct Service {
	services: Services,
	queue: parking_lot::Mutex<Queue>,
	wake: Notify,
	/// Events a peer offered that we fetched and could not use, so they are not
	/// fetched again every round.
	remembered: parking_lot::Mutex<std::collections::HashMap<OwnedEventId, std::time::Instant>>,
	/// Pairs an activity trigger queued within the debounce window.
	recent: moka::sync::Cache<Key, ()>,
	/// Avoid rescanning a large room's member index for every PDU in a burst.
	recent_rooms: moka::sync::Cache<OwnedRoomId, ()>,
}

struct Services {
	server: Arc<conduwuit::Server>,
	globals: Dep<crate::globals::Service>,
	state_cache: Dep<crate::rooms::state_cache::Service>,
	event_handler: Dep<crate::rooms::event_handler::Service>,
	sending: Dep<crate::sending::Service>,
	timeline: Dep<crate::rooms::timeline::Service>,
	state: Dep<crate::rooms::state::Service>,
}

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			services: Services {
				server: args.server.clone(),
				globals: args.depend::<crate::globals::Service>("globals"),
				state_cache: args
					.depend::<crate::rooms::state_cache::Service>("rooms::state_cache"),
				event_handler: args
					.depend::<crate::rooms::event_handler::Service>("rooms::event_handler"),
				sending: args.depend::<crate::sending::Service>("sending"),
				timeline: args.depend::<crate::rooms::timeline::Service>("rooms::timeline"),
				state: args.depend::<crate::rooms::state::Service>("rooms::state"),
			},
			queue: parking_lot::Mutex::new(Queue::new(MAX_TRACKED)),
			wake: Notify::new(),
			remembered: parking_lot::Mutex::default(),
			recent: moka::sync::Cache::builder()
				.max_capacity(MAX_RECENT)
				.time_to_live(Duration::from_secs(
					args.server
						.config
						.experimental_features
						.msc4521_reconcile_activity_interval_secs,
				))
				.build(),
			recent_rooms: moka::sync::Cache::builder()
				.max_capacity(MAX_RECENT)
				.time_to_live(Duration::from_secs(
					args.server
						.config
						.experimental_features
						.msc4521_reconcile_activity_interval_secs,
				))
				.build(),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result<()> { self.run().await }

	fn interrupt(&self) { self.wake.notify_waiters(); }

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	fn enabled(&self) -> bool {
		self.services
			.server
			.config
			.experimental_features
			.msc4521_enabled
	}

	/// Asks for a reconciliation of `room_id` with `peer`.
	///
	/// Admission is checked here, before any network I/O: the feature is on,
	/// the peer is another server, both servers are in the room and the peer
	/// is not ACL-denied.
	pub async fn enqueue(&self, room_id: &RoomId, peer: &ServerName, reason: Reason) -> Enqueued {
		if !self.enabled() {
			return Self::record(room_id, peer, reason, Enqueued::Refused(Refusal::Disabled));
		}
		let key = Key::new(room_id, peer);
		if reason == Reason::Activity {
			let window = self
				.services
				.server
				.config
				.experimental_features
				.msc4521_reconcile_activity_interval_secs;
			if window == 0 {
				return Self::record(room_id, peer, reason, Enqueued::Refused(Refusal::Disabled));
			}
			if self.recent.contains_key(&key) {
				return Self::record(room_id, peer, reason, Enqueued::Refused(Refusal::Recent));
			}
		}
		let ours = self.services.globals.server_name();
		if peer == ours {
			return Self::record(room_id, peer, reason, Enqueued::Refused(Refusal::OurServer));
		}
		if !self
			.services
			.state_cache
			.server_in_room(ours, room_id)
			.await
		{
			return Self::record(room_id, peer, reason, Enqueued::Refused(Refusal::NotInRoom));
		}
		if !self
			.services
			.state_cache
			.server_in_room(peer, room_id)
			.await
		{
			return Self::record(
				room_id,
				peer,
				reason,
				Enqueued::Refused(Refusal::PeerNotInRoom),
			);
		}
		if self
			.services
			.event_handler
			.acl_check(peer, room_id)
			.await
			.is_err()
		{
			return Self::record(room_id, peer, reason, Enqueued::Refused(Refusal::Denied));
		}

		let outcome = self
			.queue
			.lock()
			.enqueue(key.clone(), reason, std::time::Instant::now());
		if outcome == Enqueued::Queued {
			if reason == Reason::Activity {
				self.recent.insert(key, ());
			}
			self.wake.notify_one();
		}
		Self::record(room_id, peer, reason, outcome)
	}

	fn record(
		room_id: &RoomId,
		peer: &ServerName,
		reason: Reason,
		outcome: Enqueued,
	) -> Enqueued {
		debug!(target: "reconcile", %room_id, %peer, ?reason, ?outcome, "reconciliation trigger");
		outcome
	}

	async fn run(self: Arc<Self>) -> Result<()> {
		let interval = self
			.services
			.server
			.config
			.experimental_features
			.msc4521_reconcile_interval_secs;
		let mut shutdown = self.services.server.signal.subscribe();
		let mut activity = self.services.timeline.activity.subscribe();
		let ours = self.services.globals.server_name();
		let mut rooms = Box::pin(self.services.state_cache.server_rooms(ours));
		let mut sweep =
			(interval != 0).then(|| async_io::Timer::interval(Duration::from_secs(interval)));

		loop {
			self.start_ready();

			let woken = self.wake.notified();
			let signal = shutdown.recv();
			let local = activity.recv();
			let tick = async {
				match sweep.as_mut() {
					| Some(timer) => {
						timer.next().await;
					},
					| None => std::future::pending::<()>().await,
				}
			};
			// Backed-off entries become ready by the clock alone.
			let retry = smol::Timer::after(Duration::from_secs(1));
			pin_mut!(woken, signal, local, tick, retry);
			futures::select_biased! {
				_ = signal.fuse() => {},
				room = local.fuse() => {
					if let Ok(room_id) = room {
						self.activity(&room_id).await;
					}
				},
				() = tick.fuse() => {
					if self.sweep(&mut rooms).await {
						rooms = Box::pin(self.services.state_cache.server_rooms(ours));
					}
				},
				() = woken.fuse() => {},
				_ = FutureExt::fuse(retry) => {},
			}

			if !self.services.server.running() {
				return Ok(());
			}
		}
	}

	/// Stable sparse overlay peers for this room's current server membership.
	async fn pick_peers(&self, room_id: &RoomId) -> Vec<OwnedServerName> {
		let ours = self.services.globals.server_name();
		let mut peers: Vec<OwnedServerName> = self
			.services
			.state_cache
			.room_servers(room_id)
			.collect()
			.await;
		peers.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));
		peers.dedup();
		let Some(index) = peers.iter().position(|peer| peer.as_str() == ours.as_str()) else {
			return Vec::new();
		};
		overlay_indices(peers.len(), index)
			.into_iter()
			.map(|index| peers[index].clone())
			.collect()
	}

	/// A local write or inbound transaction touched `room_id`. Use the same
	/// bounded overlay as the periodic sweep, regardless of who sent the event.
	pub async fn activity(&self, room_id: &RoomId) {
		if !self.enabled()
			|| !self.services.server.config.allow_federation
			|| self
				.services
				.server
				.config
				.experimental_features
				.msc4521_reconcile_activity_interval_secs
				== 0
		{
			return;
		}
		let room_key = room_id.to_owned();
		if self.recent_rooms.contains_key(&room_key) {
			return;
		}
		self.recent_rooms.insert(room_key, ());
		for peer in self.pick_peers(room_id).await {
			self.enqueue(room_id, &peer, Reason::Activity).await;
		}
	}

	/// Walks the room index in bounded slices. One tick never scans every room
	/// or floods the queue, regardless of the size of the deployment.
	async fn sweep(
		&self,
		rooms: &mut (impl futures::Stream<Item = OwnedRoomId> + Unpin),
	) -> bool {
		if !self.enabled() || !self.services.server.config.allow_federation {
			return false;
		}
		let mut pairs = 0_usize;
		for _ in 0..SWEEP_ROOMS_PER_TICK {
			let Some(room_id) = rooms.next().await else {
				return true;
			};
			for peer in self.pick_peers(&room_id).await {
				if pairs >= SWEEP_PAIRS_PER_TICK {
					return false;
				}
				let outcome = self.enqueue(&room_id, &peer, Reason::Periodic).await;
				if outcome == Enqueued::Refused(Refusal::QueueFull) {
					return false;
				}
				pairs = pairs.saturating_add(1);
			}
		}
		false
	}

	/// Starts as many queued exchanges as the concurrency limits allow.
	fn start_ready(self: &Arc<Self>) {
		loop {
			let Some(key) = self.queue.lock().next_ready(
				std::time::Instant::now(),
				MAX_CONCURRENT,
				MAX_PER_PEER,
			) else {
				return;
			};

			let this = Arc::clone(self);
			let runtime = self.services.server.runtime().clone();
			drop(runtime.spawn(async move {
				let started = std::time::Instant::now();
				let outcome = this.exchange(&key).await;
				let elapsed_ms = started.elapsed().as_millis();
				let outcome_text = outcome.describe();
				// Only exchanges that moved events or gave up are worth an INFO line;
				// the sweep revisits every pair constantly.
				if matches!(outcome, Outcome::Reconciled { .. } | Outcome::Abandoned(_)) {
					info!(
						target: "reconcile",
						room_id = %key.room_id(), peer = %key.peer(),
						elapsed_ms, outcome = %outcome_text,
						"reconciliation exchange finished"
					);
				} else {
					debug!(
						target: "reconcile",
						room_id = %key.room_id(), peer = %key.peer(),
						elapsed_ms, outcome = %outcome_text,
						"reconciliation exchange finished"
					);
				}
				let verdict = match outcome {
					| Outcome::Failed(_) | Outcome::Abandoned(_) => Verdict::Failed,
					| Outcome::Unsupported => Verdict::Unsupported,
					| Outcome::Synchronized
					| Outcome::Differs(_)
					| Outcome::Reconciled { .. } => Verdict::Done,
				};
				let requeue = this
					.queue
					.lock()
					.finish(&key, verdict, std::time::Instant::now());
				if requeue {
					this.wake.notify_one();
				}
			}));
		}
	}
}

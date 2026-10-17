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
use rand::seq::SliceRandom;
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
/// The most peers of a room a local write or a sweep reconciles with.
const MAX_FANOUT: usize = 8;

/// How many of a room's `n` remote servers to reconcile with: about log2(n),
/// at least one. Asking every member would be all-to-all; a few random peers
/// per trigger, re-drawn each time, spread a difference through the room in a
/// logarithmic number of rounds and route around an unavailable link.
fn fanout(n: usize) -> usize {
	let bits = usize::try_from(usize::BITS.saturating_sub(n.leading_zeros())).unwrap_or(1);
	bits.clamp(1, MAX_FANOUT)
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
		let mut sweep =
			(interval != 0).then(|| async_io::Timer::interval(Duration::from_secs(interval)));

		loop {
			self.start_ready();

			let woken = self.wake.notified();
			let signal = shutdown.recv();
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
			pin_mut!(woken, signal, tick, retry);
			futures::select_biased! {
				_ = signal.fuse() => {},
				() = tick.fuse() => self.sweep().await,
				() = woken.fuse() => {},
				_ = FutureExt::fuse(retry) => {},
			}

			if !self.services.server.running() {
				return Ok(());
			}
		}
	}

	/// About log2(n) random remote servers of the room.
	async fn pick_peers(&self, room_id: &RoomId) -> Vec<OwnedServerName> {
		let ours = self.services.globals.server_name();
		let mut peers: Vec<OwnedServerName> = self
			.services
			.state_cache
			.room_servers(room_id)
			.filter(|peer| std::future::ready(peer.as_str() != ours.as_str()))
			.collect()
			.await;
		peers.shuffle(&mut rand::rng());
		peers.truncate(fanout(peers.len()));
		peers
	}

	/// A local write landed in `room_id`: reconcile with a few of its peers.
	async fn local_activity(&self, room_id: &RoomId) {
		for peer in self.pick_peers(room_id).await {
			self.enqueue(room_id, &peer, Reason::Activity).await;
		}
	}

	/// Enqueues a few random remote servers of every room we are in. Stops
	/// early once the queue is full: the next pass continues.
	async fn sweep(&self) {
		if !self.enabled() || !self.services.server.config.allow_federation {
			return;
		}
		let ours = self.services.globals.server_name();
		let rooms: Vec<OwnedRoomId> =
			self.services.state_cache.server_rooms(ours).collect().await;
		for room_id in rooms {
			for peer in self.pick_peers(&room_id).await {
				let outcome = self.enqueue(&room_id, &peer, Reason::Periodic).await;
				if outcome == Enqueued::Refused(Refusal::QueueFull) {
					return;
				}
			}
		}
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

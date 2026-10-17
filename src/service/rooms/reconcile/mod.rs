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

pub struct Service {
	services: Services,
	queue: parking_lot::Mutex<Queue>,
	wake: Notify,
	/// Events a peer offered that we fetched and could not use, so they are not
	/// fetched again every round.
	remembered: parking_lot::Mutex<std::collections::HashMap<OwnedEventId, std::time::Instant>>,
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

		let outcome =
			self.queue
				.lock()
				.enqueue(Key::new(room_id, peer), reason, std::time::Instant::now());
		if outcome == Enqueued::Queued {
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

	/// Enqueues every remote server of every room we are in.
	async fn sweep(&self) {
		if !self.enabled() || !self.services.server.config.allow_federation {
			return;
		}
		let ours = self.services.globals.server_name();
		let rooms: Vec<OwnedRoomId> =
			self.services.state_cache.server_rooms(ours).collect().await;
		for room_id in rooms {
			let peers: Vec<OwnedServerName> = self
				.services
				.state_cache
				.room_servers(&room_id)
				.collect()
				.await;
			for peer in peers {
				self.enqueue(&room_id, &peer, Reason::Periodic).await;
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
				info!(
					target: "reconcile",
					room_id = %key.room_id(), peer = %key.peer(),
					elapsed_ms = started.elapsed().as_millis(), outcome = %outcome.describe(),
					"reconciliation exchange finished"
				);
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

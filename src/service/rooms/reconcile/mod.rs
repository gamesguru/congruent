//! Outbound MSC4521 reconciliation scheduling.
//!
//! Every trigger (an explicit request, the periodic sweep, and later an
//! incoming-PDU gap or a reconnect) enters one `(room, peer)` queue. The queue
//! coalesces duplicates, keeps a dirty bit for triggers that arrive during an
//! exchange, and backs off failing peers. The exchange is in `exchange.rs`; see
//! `docs/development-gg/msc4521-outbound-reconciliation-plan.md`.

mod exchange;
mod queue;

use std::{
	collections::{HashSet, hash_map::DefaultHasher},
	hash::{Hash, Hasher},
	sync::{Arc, atomic::Ordering},
	time::{Duration, Instant},
};

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
/// How many peers' advertisements are cached at once.
const MAX_ADVERTISED: u64 = 4096;
const MAX_PENDING_ACTIVITY: usize = 16384;
/// Stable peers provide continuity; rotating peers explore alternatives.
const STABLE_PEERS: usize = 2;
const ROTATING_PEERS: usize = 2;
/// How long a peer's advertisement of the digest profile is trusted before its
/// `/version` is read again.
const ADVERTISED_FOR: Duration = Duration::from_hours(1);
/// Maximum rooms examined on one periodic tick.
const SWEEP_ROOMS_PER_TICK: usize = 8;
const MAX_EXCHANGES_PER_SECOND: u32 = 4;

struct SweepCursor {
	rooms: Vec<OwnedRoomId>,
	next: usize,
	catch_up: bool,
}

struct RateBudget {
	window_start: Instant,
	started: u32,
}

impl RateBudget {
	fn available(&mut self, now: Instant) -> bool {
		if now.duration_since(self.window_start) >= Duration::from_secs(1) {
			self.window_start = now;
			self.started = 0;
		}
		self.started < MAX_EXCHANGES_PER_SECOND
	}

	fn wait(&self, now: Instant) -> Duration {
		if self.started < MAX_EXCHANGES_PER_SECOND {
			Duration::from_secs(1)
		} else {
			Duration::from_secs(1)
				.saturating_sub(now.saturating_duration_since(self.window_start))
		}
	}
}

/// Keep configured debounce TTLs below Moka's 1000-year maximum.
fn activity_window(secs: u64) -> Duration {
	const MAX_SECS: u64 = 999 * 365 * 24 * 60 * 60;
	Duration::from_secs(secs.min(MAX_SECS))
}

/// The feature a server lists in its federation `/version` when it serves the
/// digest profile the exchange speaks; advertised and checked by the same key.
pub const DIGEST_FEATURE: &str = "tk.nutra.msc4521.digest.algebraic_v1";

pub struct Service {
	services: Services,
	queue: parking_lot::Mutex<Queue>,
	rate: parking_lot::Mutex<RateBudget>,
	wake: Notify,
	/// Events a peer offered that we fetched and could not use, so they are not
	/// fetched again every round.
	remembered: parking_lot::Mutex<std::collections::HashMap<OwnedEventId, Instant>>,
	/// Peers whose `/version` advertised the digest profile. A peer that does
	/// not is not cached here: the queue backs it off as unsupported.
	advertised: moka::sync::Cache<OwnedServerName, ()>,
	/// Pairs an activity trigger queued within the debounce window.
	recent: moka::sync::Cache<Key, ()>,
	/// Avoid rescanning a large room's member index for every PDU in a burst.
	recent_rooms: moka::sync::Cache<OwnedRoomId, ()>,
	/// Trailing-edge room activity suppressed by the debounce window.
	pending_activity: parking_lot::Mutex<HashSet<OwnedRoomId>>,
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
			queue: parking_lot::Mutex::new(
				Queue::new(MAX_TRACKED).with_failure_backoff(
					Duration::from_secs(
						args.server
							.config
							.experimental_features
							.msc4521_reconcile_failure_backoff_base_secs
							.max(1),
					),
					Duration::from_secs(
						args.server
							.config
							.experimental_features
							.msc4521_reconcile_failure_backoff_max_secs,
					),
				),
			),
			rate: parking_lot::Mutex::new(RateBudget {
				window_start: Instant::now(),
				started: 0,
			}),
			wake: Notify::new(),
			remembered: parking_lot::Mutex::default(),
			advertised: moka::sync::Cache::builder()
				.max_capacity(MAX_ADVERTISED)
				.time_to_live(ADVERTISED_FOR)
				.build(),
			recent: moka::sync::Cache::builder()
				.max_capacity(MAX_RECENT)
				.time_to_live(activity_window(
					args.server
						.config
						.experimental_features
						.msc4521_reconcile_activity_interval_secs,
				))
				.build(),
			recent_rooms: moka::sync::Cache::builder()
				.max_capacity(MAX_RECENT)
				.time_to_live(activity_window(
					args.server
						.config
						.experimental_features
						.msc4521_reconcile_activity_interval_secs,
				))
				.build(),
			pending_activity: parking_lot::Mutex::default(),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result<()> {
		self.run().await
	}

	fn interrupt(&self) {
		self.wake.notify_waiters();
	}

	fn name(&self) -> &str {
		crate::service::make_name(std::module_path!())
	}
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
			.enqueue(key.clone(), reason, Instant::now());
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

	/// When the debounced activity is next due, `now` plus the rate window.
	fn next_flush(&self, now: Instant) -> Instant {
		let wait = self.rate.lock().wait(now);
		now.checked_add(wait).unwrap_or(now)
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
		let mut cursor = SweepCursor {
			rooms: Vec::new(),
			next: 0,
			catch_up: false,
		};
		let mut sweep =
			(interval != 0).then(|| async_io::Timer::interval(Duration::from_secs(interval)));

		// Persists across wake-ups so repeated interruptions cannot postpone
		// the flush of debounced activity.
		let mut flush_at = self.next_flush(Instant::now());

		loop {
			// Check the persistent deadline before polling other ready sources.
			// The biased select below prioritizes activity and wakeups, so an
			// expired timer alone could otherwise starve under a steady stream.
			if Instant::now() >= flush_at {
				self.flush_pending_activity().await;
				let now = Instant::now();
				flush_at = self.next_flush(now);
			}
			if cursor.catch_up {
				self.sweep(&mut cursor).await;
			}
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
			let retry = smol::Timer::at(flush_at);
			pin_mut!(woken, signal, local, tick, retry);
			futures::select_biased! {
				_ = signal.fuse() => {},
				room = local.fuse() => {
					match room {
						| Ok(room_id) => self.activity(&room_id).await,
						// Notifications were dropped in a write burst; the
						// rooms they named are unknown, so sweep instead.
						| Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
							cursor.rooms = self
								.services
								.state_cache
								.server_rooms(self.services.globals.server_name())
								.collect()
								.await;
							cursor.next = 0;
							cursor.catch_up = true;
							self.sweep(&mut cursor).await;
						},
						| Err(_) => {},
					}
				},
				() = tick.fuse() => self.sweep(&mut cursor).await,
				() = woken.fuse() => {},
				_ = FutureExt::fuse(retry) => {
					self.flush_pending_activity().await;
					let now = Instant::now();
					flush_at = self.next_flush(now);
				},
			}

			if !self.services.server.running() {
				return Ok(());
			}
		}
	}

	/// Two stable scored peers and two rotating peers, excluding pairs in
	/// cooldown. The stable hash uses our name so different servers spread
	/// their choices across the room.
	async fn pick_peers(&self, room_id: &RoomId) -> Vec<OwnedServerName> {
		let ours = self.services.globals.server_name();
		let mut peers: Vec<OwnedServerName> = self
			.services
			.state_cache
			.room_servers(room_id)
			.collect()
			.await;
		let now = Instant::now();
		let queue = self.queue.lock();
		let mut seen = HashSet::new();
		peers.retain(|peer| {
			peer.as_str() != ours.as_str()
				&& seen.insert(peer.clone())
				&& !queue.cooling_down(&Key::new(room_id, peer), now)
		});
		drop(queue);
		let rendezvous = |peer: &OwnedServerName| {
			let mut hash = DefaultHasher::new();
			room_id.hash(&mut hash);
			ours.hash(&mut hash);
			peer.hash(&mut hash);
			hash.finish()
		};
		// Health only reorders a small, server-specific rendezvous shortlist;
		// a globally popular server cannot become every room's stable peer.
		let shortlist_len = peers.len().min(8);
		if shortlist_len < peers.len() {
			peers.select_nth_unstable_by_key(shortlist_len, &rendezvous);
		} else {
			peers.sort_unstable_by_key(&rendezvous);
		}
		let mut shortlist: Vec<_> = peers.drain(..shortlist_len).collect();
		shortlist.sort_unstable_by_key(|peer| {
			self.services
				.event_handler
				.peer_scorer
				.get(peer)
				.map_or(0_i64, |stats| {
					let successes = i64::from(stats.successes.load(Ordering::Relaxed));
					let errors = i64::from(stats.errors.load(Ordering::Relaxed));
					errors
						.saturating_mul(1000)
						.saturating_sub(successes.saturating_mul(100))
				})
		});
		let stable_count = STABLE_PEERS.min(shortlist.len());
		let mut rotating = shortlist.split_off(stable_count);
		rotating.extend(peers);
		rotating.shuffle(&mut rand::rng());
		rotating.truncate(ROTATING_PEERS);
		shortlist.extend(rotating);
		shortlist
	}

	/// A peer made a reconciliation request to us. That proves the link works,
	/// so failure backoff toward it is stale; the next trigger or sweep may
	/// reach it without waiting out the timer.
	pub fn peer_reachable(&self, room_id: &RoomId, peer: &ServerName) {
		self.queue.lock().peer_reachable(&Key::new(room_id, peer));
	}

	/// A local write or inbound transaction touched `room_id`. Use the same
	/// bounded peer selection as the periodic sweep.
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
			let mut pending = self.pending_activity.lock();
			if pending.len() < MAX_PENDING_ACTIVITY {
				pending.insert(room_key);
			}
			return;
		}
		self.pending_activity.lock().remove(&room_key);
		self.queue.lock().reset_synchronized_for_room(room_id);
		self.recent_rooms.insert(room_key.clone(), ());
		let peers = self.pick_peers(room_id).await;
		let mut queued = false;
		for peer in &peers {
			let outcome = self.enqueue(room_id, peer, Reason::Activity).await;
			queued |= matches!(outcome, Enqueued::Queued | Enqueued::Coalesced);
		}
		if !queued {
			self.recent_rooms.invalidate(&room_key);
			debug!(
				target: "reconcile",
				%room_id,
				selected_peers = peers.len(),
				"room activity did not queue a reconciliation"
			);
		}
	}

	/// Replays a bounded batch of activity that arrived during the room debounce.
	async fn flush_pending_activity(&self) {
		const PENDING_BATCH: usize = 64;
		let rooms: Vec<OwnedRoomId> = {
			let pending = self.pending_activity.lock();
			pending.iter().take(PENDING_BATCH).cloned().collect()
		};
		for room_id in rooms {
			self.pending_activity.lock().remove(&room_id);
			if self.recent_rooms.contains_key(&room_id) {
				let mut pending = self.pending_activity.lock();
				if pending.len() < MAX_PENDING_ACTIVITY {
					pending.insert(room_id);
				}
				continue;
			}
			self.activity(&room_id).await;
		}
	}

	/// The sender of an inbound transaction gets a direct chance first. This
	/// matters when its previous events were missed while the link was down.
	pub async fn activity_from(&self, room_id: &RoomId, peer: &ServerName) {
		self.enqueue(room_id, peer, Reason::Activity).await;
		self.activity(room_id).await;
	}

	/// Walks the room index in bounded slices. One tick never scans every room
	/// or floods the queue, regardless of the size of the deployment.
	async fn sweep(&self, cursor: &mut SweepCursor) {
		if !self.enabled() || !self.services.server.config.allow_federation {
			return;
		}
		if cursor.next >= cursor.rooms.len() {
			if cursor.catch_up {
				cursor.catch_up = false;
				return;
			}
			cursor.rooms = self
				.services
				.state_cache
				.server_rooms(self.services.globals.server_name())
				.collect()
				.await;
			cursor.next = 0;
		}
		for _ in 0..SWEEP_ROOMS_PER_TICK {
			let Some(room_id) = cursor.rooms.get(cursor.next) else {
				return;
			};
			for peer in self.pick_peers(room_id).await {
				let outcome = self.enqueue(room_id, &peer, Reason::Periodic).await;
				if outcome == Enqueued::Refused(Refusal::QueueFull) {
					return;
				}
			}
			cursor.next = cursor.next.saturating_add(1);
		}
		if cursor.catch_up && cursor.next >= cursor.rooms.len() {
			cursor.catch_up = false;
		}
	}

	/// Starts as many queued exchanges as the concurrency limits allow.
	fn start_ready(self: &Arc<Self>) {
		loop {
			let now = Instant::now();
			let mut rate = self.rate.lock();
			let periodic_allowed = rate.available(now);
			let Some((key, reason)) =
				self.queue
					.lock()
					.next_ready(now, MAX_CONCURRENT, MAX_PER_PEER, periodic_allowed)
			else {
				return;
			};
			if reason == Reason::Periodic {
				rate.started = rate.started.saturating_add(1);
			}
			drop(rate);

			let this = Arc::clone(self);
			let runtime = self.services.server.runtime().clone();
			drop(runtime.spawn(async move {
				let started = Instant::now();
				// A panic must still release the pair's in-flight slot.
				let outcome = match std::panic::AssertUnwindSafe(this.exchange(&key))
					.catch_unwind()
					.await
				{
					| Ok(outcome) => outcome,
					| Err(_) => Outcome::Failed("exchange panicked".to_owned()),
				};
				let elapsed = started.elapsed();
				let elapsed_ms = elapsed.as_millis();
				match &outcome {
					| Outcome::Synchronized | Outcome::Reconciled { .. } => this
						.services
						.event_handler
						.update_peer_stats(key.peer(), true, elapsed),
					| Outcome::Unsupported | Outcome::Failed(_) => this
						.services
						.event_handler
						.update_peer_stats(key.peer(), false, elapsed),
					| Outcome::Differs(_) | Outcome::Abandoned(_) => {},
				}
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
					| Outcome::Synchronized => Verdict::Synchronized,
					| Outcome::Differs(_) | Outcome::Reconciled { .. } => Verdict::Done,
				};
				let requeue = this.queue.lock().finish(&key, verdict, Instant::now());
				if requeue {
					this.wake.notify_one();
				}
			}));
		}
	}
}

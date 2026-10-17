//! The coalescing `(room, peer)` queue behind the reconciliation worker. Pure
//! bookkeeping: no I/O and no clock reads, so it can be unit tested.

use std::{
	collections::{HashMap, VecDeque},
	time::{Duration, Instant},
};

use slipstream::{OwnedRoomId, OwnedServerName, RoomId, ServerName};

/// What caused a reconciliation to be requested.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Reason {
	/// An administrator or a test asked for it.
	Explicit,
	/// The periodic sweep.
	#[default]
	Periodic,
	/// A transaction from the peer carried a PDU or an EDU for the room.
	Activity,
	/// An incoming PDU referred to predecessors we lacked.
	IncomingGap,
	/// Federation with the peer resumed after an outage.
	Reconnect,
}

/// Why a trigger was not queued.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
	Disabled,
	OurServer,
	NotInRoom,
	PeerNotInRoom,
	Denied,
	QueueFull,
	BackingOff,
	/// An activity trigger within the debounce window of the last one.
	Recent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Enqueued {
	/// Newly queued.
	Queued,
	/// Already queued or running; a running exchange is marked dirty so it is
	/// followed by one more.
	Coalesced,
	Refused(Refusal),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct Key {
	room_id: OwnedRoomId,
	peer: OwnedServerName,
}

impl Key {
	pub(super) fn new(room_id: &RoomId, peer: &ServerName) -> Self {
		Self {
			room_id: room_id.to_owned(),
			peer: peer.to_owned(),
		}
	}

	pub(super) fn room_id(&self) -> &RoomId { &self.room_id }

	pub(super) fn peer(&self) -> &ServerName { &self.peer }
}

#[derive(Default)]
struct Entry {
	queued: bool,
	in_flight: bool,
	reason: Reason,
	/// A trigger arrived during the running exchange.
	dirty: bool,
	failures: u32,
	not_before: Option<Instant>,
	identical_digests: u32,
	synchronized_until: Option<Instant>,
}

pub(super) struct Queue {
	entries: HashMap<Key, Entry>,
	ready: VecDeque<Key>,
	max_tracked: usize,
	failure_backoff: Duration,
	failure_backoff_max: Duration,
}

const BACKOFF_BASE: Duration = Duration::from_secs(5);
const BACKOFF_MAX: Duration = Duration::from_mins(15);
/// A peer that does not implement the endpoints is not asked again for this
/// long, much longer than a transient failure.
const UNSUPPORTED_BACKOFF: Duration = Duration::from_hours(6);
const IDENTICAL_DIGESTS_BEFORE_BACKOFF: u32 = 3;
const IDENTICAL_DIGEST_BACKOFF_BASE: Duration = Duration::from_mins(1);
const IDENTICAL_DIGEST_BACKOFF_MAX: Duration = Duration::from_hours(24);

/// How an exchange ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Verdict {
	Done,
	Synchronized,
	Failed,
	/// The peer does not serve the reconciliation endpoints.
	Unsupported,
}

/// Exponential backoff after `failures` consecutive failures.
fn backoff(base: Duration, max: Duration, failures: u32) -> Duration {
	base.saturating_mul(
		1_u32
			.checked_shl(failures.saturating_sub(1))
			.unwrap_or(u32::MAX),
	)
	.min(max.max(base))
}

impl Queue {
	/// Whether an automatic trigger should avoid this pair for now. The
	/// existing queue retains the cooldown for unsupported and failed peers.
	pub(super) fn cooling_down(&self, key: &Key, now: Instant) -> bool {
		self.entries.get(key).is_some_and(|entry| {
			entry.not_before.is_some_and(|until| until > now)
				|| entry.synchronized_until.is_some_and(|until| until > now)
		})
	}

	/// New room activity invalidates synchronized backoff for every peer in the
	/// room. Transport and unsupported-peer backoff remain intact.
	pub(super) fn reset_synchronized_for_room(&mut self, room_id: &RoomId) {
		for (key, entry) in &mut self.entries {
			if key.room_id() == room_id {
				entry.identical_digests = 0;
				entry.synchronized_until = None;
			}
		}
	}

	/// The peer just called us, so it is reachable and implements the
	/// endpoints: drop any failure or unsupported backoff toward it.
	pub(super) fn peer_reachable(&mut self, key: &Key) {
		if let Some(entry) = self.entries.get_mut(key) {
			entry.failures = 0;
			entry.not_before = None;
		}
	}

	pub(super) fn new(max_tracked: usize) -> Self {
		Self {
			entries: HashMap::new(),
			ready: VecDeque::new(),
			max_tracked,
			failure_backoff: BACKOFF_BASE,
			failure_backoff_max: BACKOFF_MAX,
		}
	}

	pub(super) fn with_failure_backoff(mut self, base: Duration, max: Duration) -> Self {
		self.failure_backoff = base;
		self.failure_backoff_max = max;
		self
	}

	pub(super) fn enqueue(&mut self, key: Key, reason: Reason, now: Instant) -> Enqueued {
		if let Some(entry) = self.entries.get_mut(&key) {
			// New activity invalidates equal-digest observations, but does not
			// erase transport or unsupported-peer backoff.
			if reason != Reason::Periodic {
				entry.identical_digests = 0;
				entry.synchronized_until = None;
			}
			if entry.in_flight {
				entry.dirty = true;
				if reason != Reason::Periodic {
					entry.reason = reason;
				}
				return Enqueued::Coalesced;
			}
			if entry.queued {
				if reason != Reason::Periodic {
					entry.reason = reason;
				}
				return Enqueued::Coalesced;
			}
			// An explicit request overrides backoff; nothing else does.
			if reason == Reason::Explicit {
				entry.not_before = None;
			} else if entry.not_before.is_some_and(|t| t > now)
				|| entry.synchronized_until.is_some_and(|t| t > now)
			{
				return Enqueued::Refused(Refusal::BackingOff);
			}
			entry.queued = true;
			entry.reason = reason;
			self.ready.push_back(key);
			return Enqueued::Queued;
		}

		if self.entries.len() >= self.max_tracked && !self.evict_idle(now) {
			return Enqueued::Refused(Refusal::QueueFull);
		}
		self.entries
			.insert(key.clone(), Entry { queued: true, reason, ..Entry::default() });
		self.ready.push_back(key);
		Enqueued::Queued
	}

	/// Drops an idle failing pair whose backoff has already been served, to
	/// make room for a new one. Returns whether anything was dropped. Pairs
	/// that are queued, running or still backing off are never dropped, so
	/// eviction cannot cut a dead peer's backoff short.
	fn evict_idle(&mut self, now: Instant) -> bool {
		let victim = self
			.entries
			.iter()
			.filter(|(_, e)| {
				!e.queued
					&& !e.in_flight && e.not_before.is_none_or(|t| t <= now)
					&& e.synchronized_until.is_none_or(|t| t <= now)
			})
			.min_by_key(|(_, e)| e.not_before)
			.map(|(k, _)| k.clone());
		victim.is_some_and(|key| self.entries.remove(&key).is_some())
	}

	/// Takes the next queued pair that fits the concurrency limits and marks
	/// it in flight.
	pub(super) fn next_ready(
		&mut self,
		now: Instant,
		max_concurrent: usize,
		max_per_peer: usize,
		periodic_allowed: bool,
	) -> Option<(Key, Reason)> {
		let running = self.entries.values().filter(|e| e.in_flight).count();
		if running >= max_concurrent {
			return None;
		}

		for priority in [true, false] {
			for index in 0..self.ready.len() {
				let key = &self.ready[index];
				if self
					.entries
					.get(key)
					.is_none_or(|e| (e.reason != Reason::Periodic) != priority)
					|| (!priority && !periodic_allowed)
				{
					continue;
				}
				let peer_running = self
					.entries
					.iter()
					.filter(|(k, e)| e.in_flight && k.peer == key.peer)
					.count();
				let waiting = self.entries.get(key).is_some_and(|e| {
					e.not_before.is_some_and(|t| t > now)
						|| e.synchronized_until.is_some_and(|t| t > now)
				});
				if peer_running >= max_per_peer || waiting {
					continue;
				}

				let key = self.ready.remove(index)?;
				let entry = self.entries.get_mut(&key)?;
				let reason = entry.reason;
				entry.queued = false;
				entry.in_flight = true;
				entry.dirty = false;
				entry.reason = Reason::Periodic;
				return Some((key, reason));
			}
		}
		None
	}

	/// Records the end of an exchange. Returns whether the pair was queued
	/// again because a trigger arrived while it ran.
	pub(super) fn finish(&mut self, key: &Key, verdict: Verdict, now: Instant) -> bool {
		let Some(entry) = self.entries.get_mut(key) else {
			return false;
		};
		entry.in_flight = false;
		let dirty = entry.dirty;
		match verdict {
			| Verdict::Done => {
				entry.failures = 0;
				entry.not_before = None;
				entry.identical_digests = 0;
				entry.synchronized_until = None;
			},
			| Verdict::Synchronized => {
				entry.failures = 0;
				entry.not_before = None;
				if dirty {
					entry.identical_digests = 0;
					entry.synchronized_until = None;
				} else {
					entry.identical_digests = entry.identical_digests.saturating_add(1);
					if entry.identical_digests >= IDENTICAL_DIGESTS_BEFORE_BACKOFF {
						let shift = entry
							.identical_digests
							.saturating_sub(IDENTICAL_DIGESTS_BEFORE_BACKOFF);
						let delay = IDENTICAL_DIGEST_BACKOFF_BASE
							.saturating_mul(1_u32.checked_shl(shift).unwrap_or(u32::MAX))
							.min(IDENTICAL_DIGEST_BACKOFF_MAX);
						entry.synchronized_until = now.checked_add(delay);
					}
				}
			},
			| Verdict::Failed => {
				entry.failures = entry.failures.saturating_add(1);
				entry.not_before = now.checked_add(backoff(
					self.failure_backoff,
					self.failure_backoff_max,
					entry.failures,
				));
				entry.identical_digests = 0;
				entry.synchronized_until = None;
			},
			| Verdict::Unsupported => {
				entry.failures = entry.failures.saturating_add(1);
				entry.not_before = now.checked_add(UNSUPPORTED_BACKOFF);
				entry.identical_digests = 0;
				entry.synchronized_until = None;
			},
		}

		if entry.dirty {
			entry.dirty = false;
			entry.queued = true;
			self.ready.push_back(key.clone());
			return true;
		}
		// Idle and healthy: stop tracking so a dead peer cannot accumulate
		// entries. Failing pairs stay until their backoff has been served.
		if entry.failures == 0 && entry.identical_digests == 0 {
			self.entries.remove(key);
		}
		false
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn key(room: &str, peer: &str) -> Key {
		Key::new(&RoomId::parse(room).unwrap(), &ServerName::parse(peer).unwrap())
	}

	#[test]
	fn duplicate_triggers_coalesce() {
		let now = Instant::now();
		let mut queue = Queue::new(8);
		let k = key("!a:example.org", "peer.example");
		assert_eq!(queue.enqueue(k.clone(), Reason::Periodic, now), Enqueued::Queued);
		assert_eq!(queue.enqueue(k.clone(), Reason::Periodic, now), Enqueued::Coalesced);
		assert_eq!(queue.next_ready(now, 4, 2, true), Some((k, Reason::Periodic)));
		assert_eq!(queue.next_ready(now, 4, 2, true), None);
	}

	#[test]
	fn a_trigger_during_an_exchange_schedules_one_more() {
		let now = Instant::now();
		let mut queue = Queue::new(8);
		let k = key("!a:example.org", "peer.example");
		queue.enqueue(k.clone(), Reason::Periodic, now);
		assert_eq!(queue.next_ready(now, 4, 2, true), Some((k.clone(), Reason::Periodic)));
		assert_eq!(queue.enqueue(k.clone(), Reason::IncomingGap, now), Enqueued::Coalesced);
		assert_eq!(queue.enqueue(k.clone(), Reason::IncomingGap, now), Enqueued::Coalesced);
		assert!(queue.finish(&k, Verdict::Done, now));
		assert_eq!(queue.next_ready(now, 4, 2, true), Some((k.clone(), Reason::IncomingGap)));
		assert!(!queue.finish(&k, Verdict::Done, now));
		assert_eq!(queue.next_ready(now, 4, 2, true), None);
	}

	#[test]
	fn failure_backs_off_and_explicit_overrides_it() {
		let now = Instant::now();
		let mut queue = Queue::new(8);
		let k = key("!a:example.org", "peer.example");
		queue.enqueue(k.clone(), Reason::Periodic, now);
		queue.next_ready(now, 4, 2, true);
		queue.finish(&k, Verdict::Failed, now);

		assert_eq!(
			queue.enqueue(k.clone(), Reason::Periodic, now),
			Enqueued::Refused(Refusal::BackingOff)
		);
		assert_eq!(queue.enqueue(k.clone(), Reason::Explicit, now), Enqueued::Queued);
		assert_eq!(queue.next_ready(now, 4, 2, true), Some((k, Reason::Explicit)));
	}

	#[test]
	fn an_unsupported_peer_is_not_asked_again_soon() {
		let now = Instant::now();
		let mut queue = Queue::new(8);
		let k = key("!a:example.org", "peer.example");
		queue.enqueue(k.clone(), Reason::Periodic, now);
		queue.next_ready(now, 4, 2, true);
		queue.finish(&k, Verdict::Unsupported, now);
		let later = now + BACKOFF_MAX + Duration::from_secs(1);
		assert_eq!(
			queue.enqueue(k, Reason::Periodic, later),
			Enqueued::Refused(Refusal::BackingOff)
		);
	}

	#[test]
	fn inbound_request_clears_failure_backoff() {
		let now = Instant::now();
		let mut queue = Queue::new(8);
		let k = key("!a:example.org", "peer.example");
		queue.enqueue(k.clone(), Reason::Periodic, now);
		queue.next_ready(now, 4, 2, true);
		queue.finish(&k, Verdict::Failed, now);
		assert_eq!(
			queue.enqueue(k.clone(), Reason::Periodic, now),
			Enqueued::Refused(Refusal::BackingOff)
		);
		queue.peer_reachable(&k);
		assert_eq!(queue.enqueue(k, Reason::Periodic, now), Enqueued::Queued);
	}

	#[test]
	fn backoff_grows_and_is_capped() {
		assert_eq!(backoff(BACKOFF_BASE, BACKOFF_MAX, 1), BACKOFF_BASE);
		assert_eq!(backoff(BACKOFF_BASE, BACKOFF_MAX, 2), BACKOFF_BASE * 2);
		assert_eq!(backoff(BACKOFF_BASE, BACKOFF_MAX, 40), BACKOFF_MAX);
		let one = Duration::from_secs(1);
		assert_eq!(backoff(one, BACKOFF_MAX, 3), Duration::from_secs(4));
		// The cap is configurable and never below the base.
		assert_eq!(backoff(one, Duration::from_secs(3), 5), Duration::from_secs(3));
		assert_eq!(backoff(BACKOFF_BASE, one, 5), BACKOFF_BASE);
	}

	#[test]
	fn concurrency_limits_hold() {
		let now = Instant::now();
		let mut queue = Queue::new(8);
		for room in ["!a:example.org", "!b:example.org", "!c:example.org"] {
			queue.enqueue(key(room, "peer.example"), Reason::Periodic, now);
		}
		assert!(queue.next_ready(now, 4, 2, true).is_some());
		assert!(queue.next_ready(now, 4, 2, true).is_some());
		// Two are running against the same peer.
		assert_eq!(queue.next_ready(now, 4, 2, true), None);
	}

	#[test]
	fn exhausted_periodic_budget_still_allows_activity() {
		let now = Instant::now();
		let mut queue = Queue::new(8);
		let periodic = key("!periodic:example.org", "peer.example");
		let activity = key("!activity:example.org", "peer.example");
		queue.enqueue(periodic, Reason::Periodic, now);
		queue.enqueue(activity.clone(), Reason::Activity, now);

		assert_eq!(queue.next_ready(now, 4, 2, false), Some((activity, Reason::Activity)));
		assert_eq!(queue.next_ready(now, 4, 2, false), None);
	}

	#[test]
	fn tracking_is_bounded() {
		let now = Instant::now();
		let mut queue = Queue::new(1);
		assert_eq!(
			queue.enqueue(key("!a:example.org", "p.example"), Reason::Periodic, now),
			Enqueued::Queued
		);
		assert_eq!(
			queue.enqueue(key("!b:example.org", "p.example"), Reason::Periodic, now),
			Enqueued::Refused(Refusal::QueueFull)
		);
	}

	#[test]
	fn a_full_queue_evicts_only_a_pair_that_served_its_backoff() {
		let now = Instant::now();
		let mut q = Queue::new(2);
		let a = key("!a:example.org", "peer.example");
		let b = key("!b:example.org", "peer.example");
		let c = key("!c:example.org", "peer.example");

		assert_eq!(q.enqueue(a.clone(), Reason::Periodic, now), Enqueued::Queued);
		assert_eq!(q.next_ready(now, 4, 4, true), Some((a.clone(), Reason::Periodic)));
		q.finish(&a, Verdict::Failed, now);
		assert_eq!(q.enqueue(b, Reason::Periodic, now), Enqueued::Queued);

		// `a` is still backing off, `b` is queued: nothing may go.
		assert_eq!(
			q.enqueue(c.clone(), Reason::Periodic, now),
			Enqueued::Refused(Refusal::QueueFull)
		);
		assert!(q.entries.contains_key(&a));

		// Once `a` has served its backoff it may be evicted.
		let later = now + BACKOFF_MAX;
		assert_eq!(q.enqueue(c, Reason::Periodic, later), Enqueued::Queued);
		assert!(!q.entries.contains_key(&a));
	}

	#[test]
	fn a_full_queue_of_busy_pairs_still_refuses() {
		let now = Instant::now();
		let mut q = Queue::new(1);
		let a = key("!a:example.org", "peer.example");
		let b = key("!b:example.org", "peer.example");
		assert_eq!(q.enqueue(a, Reason::Periodic, now), Enqueued::Queued);
		assert_eq!(q.enqueue(b, Reason::Periodic, now), Enqueued::Refused(Refusal::QueueFull));
	}
}

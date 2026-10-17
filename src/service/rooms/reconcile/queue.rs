//! The coalescing `(room, peer)` queue behind the reconciliation worker. Pure
//! bookkeeping: no I/O and no clock reads, so it can be unit tested.

use std::{
	collections::{HashMap, VecDeque},
	time::{Duration, Instant},
};

use slipstream::{OwnedRoomId, OwnedServerName, RoomId, ServerName};

/// What caused a reconciliation to be requested.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
	/// An administrator or a test asked for it.
	Explicit,
	/// The periodic sweep.
	Periodic,
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
	/// A trigger arrived during the running exchange.
	dirty: bool,
	failures: u32,
	not_before: Option<Instant>,
}

pub(super) struct Queue {
	entries: HashMap<Key, Entry>,
	ready: VecDeque<Key>,
	max_tracked: usize,
}

const BACKOFF_BASE: Duration = Duration::from_secs(5);
const BACKOFF_MAX: Duration = Duration::from_mins(15);
/// A peer that does not implement the endpoints is not asked again for this
/// long, much longer than a transient failure.
const UNSUPPORTED_BACKOFF: Duration = Duration::from_hours(6);

/// How an exchange ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Verdict {
	Done,
	Failed,
	/// The peer does not serve the reconciliation endpoints.
	Unsupported,
}

/// Exponential backoff after `failures` consecutive failures.
fn backoff(failures: u32) -> Duration {
	BACKOFF_BASE
		.saturating_mul(
			1_u32
				.checked_shl(failures.saturating_sub(1))
				.unwrap_or(u32::MAX),
		)
		.min(BACKOFF_MAX)
}

impl Queue {
	pub(super) fn new(max_tracked: usize) -> Self {
		Self {
			entries: HashMap::new(),
			ready: VecDeque::new(),
			max_tracked,
		}
	}

	pub(super) fn enqueue(&mut self, key: Key, reason: Reason, now: Instant) -> Enqueued {
		if let Some(entry) = self.entries.get_mut(&key) {
			if entry.in_flight {
				entry.dirty = true;
				return Enqueued::Coalesced;
			}
			if entry.queued {
				return Enqueued::Coalesced;
			}
			// An explicit request overrides backoff; nothing else does.
			if reason == Reason::Explicit {
				entry.not_before = None;
			} else if entry.not_before.is_some_and(|t| t > now) {
				return Enqueued::Refused(Refusal::BackingOff);
			}
			entry.queued = true;
			self.ready.push_back(key);
			return Enqueued::Queued;
		}

		if self.entries.len() >= self.max_tracked && !self.evict_idle() {
			return Enqueued::Refused(Refusal::QueueFull);
		}
		self.entries
			.insert(key.clone(), Entry { queued: true, ..Entry::default() });
		self.ready.push_back(key);
		Enqueued::Queued
	}

	/// Drops the idle failing pair whose backoff ends soonest, to make room for
	/// a new one. Returns whether anything was dropped. Pairs that are queued
	/// or running are never dropped.
	fn evict_idle(&mut self) -> bool {
		let victim = self
			.entries
			.iter()
			.filter(|(_, e)| !e.queued && !e.in_flight)
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
	) -> Option<Key> {
		let running = self.entries.values().filter(|e| e.in_flight).count();
		if running >= max_concurrent {
			return None;
		}

		for index in 0..self.ready.len() {
			let key = &self.ready[index];
			let peer_running = self
				.entries
				.iter()
				.filter(|(k, e)| e.in_flight && k.peer == key.peer)
				.count();
			let waiting = self
				.entries
				.get(key)
				.is_some_and(|e| e.not_before.is_some_and(|t| t > now));
			if peer_running >= max_per_peer || waiting {
				continue;
			}

			let key = self.ready.remove(index)?;
			let entry = self.entries.get_mut(&key)?;
			entry.queued = false;
			entry.in_flight = true;
			entry.dirty = false;
			return Some(key);
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
		match verdict {
			| Verdict::Done => {
				entry.failures = 0;
				entry.not_before = None;
			},
			| Verdict::Failed => {
				entry.failures = entry.failures.saturating_add(1);
				entry.not_before = now.checked_add(backoff(entry.failures));
			},
			| Verdict::Unsupported => {
				entry.failures = entry.failures.saturating_add(1);
				entry.not_before = now.checked_add(UNSUPPORTED_BACKOFF);
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
		if entry.failures == 0 {
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
		assert_eq!(queue.next_ready(now, 4, 2), Some(k));
		assert_eq!(queue.next_ready(now, 4, 2), None);
	}

	#[test]
	fn a_trigger_during_an_exchange_schedules_one_more() {
		let now = Instant::now();
		let mut queue = Queue::new(8);
		let k = key("!a:example.org", "peer.example");
		queue.enqueue(k.clone(), Reason::Periodic, now);
		assert_eq!(queue.next_ready(now, 4, 2), Some(k.clone()));
		assert_eq!(queue.enqueue(k.clone(), Reason::IncomingGap, now), Enqueued::Coalesced);
		assert_eq!(queue.enqueue(k.clone(), Reason::IncomingGap, now), Enqueued::Coalesced);
		assert!(queue.finish(&k, Verdict::Done, now));
		assert_eq!(queue.next_ready(now, 4, 2), Some(k.clone()));
		assert!(!queue.finish(&k, Verdict::Done, now));
		assert_eq!(queue.next_ready(now, 4, 2), None);
	}

	#[test]
	fn failure_backs_off_and_explicit_overrides_it() {
		let now = Instant::now();
		let mut queue = Queue::new(8);
		let k = key("!a:example.org", "peer.example");
		queue.enqueue(k.clone(), Reason::Periodic, now);
		queue.next_ready(now, 4, 2);
		queue.finish(&k, Verdict::Failed, now);

		assert_eq!(
			queue.enqueue(k.clone(), Reason::Periodic, now),
			Enqueued::Refused(Refusal::BackingOff)
		);
		assert_eq!(queue.enqueue(k.clone(), Reason::Explicit, now), Enqueued::Queued);
		assert_eq!(queue.next_ready(now, 4, 2), Some(k));
	}

	#[test]
	fn an_unsupported_peer_is_not_asked_again_soon() {
		let now = Instant::now();
		let mut queue = Queue::new(8);
		let k = key("!a:example.org", "peer.example");
		queue.enqueue(k.clone(), Reason::Periodic, now);
		queue.next_ready(now, 4, 2);
		queue.finish(&k, Verdict::Unsupported, now);
		let later = now + BACKOFF_MAX + Duration::from_secs(1);
		assert_eq!(
			queue.enqueue(k, Reason::Periodic, later),
			Enqueued::Refused(Refusal::BackingOff)
		);
	}

	#[test]
	fn backoff_grows_and_is_capped() {
		assert_eq!(backoff(1), BACKOFF_BASE);
		assert_eq!(backoff(2), BACKOFF_BASE * 2);
		assert_eq!(backoff(40), BACKOFF_MAX);
	}

	#[test]
	fn concurrency_limits_hold() {
		let now = Instant::now();
		let mut queue = Queue::new(8);
		for room in ["!a:example.org", "!b:example.org", "!c:example.org"] {
			queue.enqueue(key(room, "peer.example"), Reason::Periodic, now);
		}
		assert!(queue.next_ready(now, 4, 2).is_some());
		assert!(queue.next_ready(now, 4, 2).is_some());
		// Two are running against the same peer.
		assert_eq!(queue.next_ready(now, 4, 2), None);
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
	fn a_full_queue_evicts_an_idle_failing_pair() {
		let now = Instant::now();
		let mut q = Queue::new(2);
		let a = key("!a:example.org", "peer.example");
		let b = key("!b:example.org", "peer.example");
		let c = key("!c:example.org", "peer.example");

		assert_eq!(q.enqueue(a.clone(), Reason::Periodic, now), Enqueued::Queued);
		assert_eq!(q.next_ready(now, 4, 4), Some(a.clone()));
		q.finish(&a, Verdict::Failed, now);
		assert_eq!(q.enqueue(b, Reason::Periodic, now), Enqueued::Queued);

		// `a` is idle and failing, `b` is queued: only `a` may go.
		assert_eq!(q.enqueue(c, Reason::Periodic, now), Enqueued::Queued);
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

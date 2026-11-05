//! Outbound federation send measurements.
//!
//! Plain atomics, no allocation on the hot path. Everything here is
//! cumulative since process start, except `time_max_us`, which is a lifetime
//! maximum.
//!
//! `time_us` measures one federation transaction from the start of
//! `send_events_dest_federation` to the response being collected. That
//! includes the sender-path DB reads, request framing, the wait for a
//! sending-semaphore permit, and the remote round trip. It is *not* the cost
//! of writing to the socket.

use std::{
	sync::{
		Mutex,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration,
};

/// Number of latency buckets. Bucket `i` (for `i < LATENCY_BUCKETS - 1`) holds
/// sends that took at most `1 ms << i`; the last bucket is the overflow
/// (> 16 s).
pub const LATENCY_BUCKETS: usize = 16;

/// Where in the sender's destination-selection path a DB call was made.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SelectionSource {
	/// Pending-transaction and queue maps (`active_requests_for`,
	/// `mark_as_active`), plus the EDU watermark read.
	Queue,
	/// Device-list change scan: room membership, per-room key changes and
	/// per-user device lookups.
	DeviceLists,
	/// Read-receipt scan: room membership, per-room receipt streams and
	/// per-receipt PDU count lookups.
	Receipts,
	/// Presence: per-user presence reads and shared-room visibility checks.
	Presence,
}

/// Number of [`SelectionSource`] variants.
pub const SELECTION_SOURCES: usize = 4;

impl SelectionSource {
	pub const ALL: [Self; SELECTION_SOURCES] =
		[Self::Queue, Self::DeviceLists, Self::Receipts, Self::Presence];

	/// Index into per-source arrays.
	#[must_use]
	pub const fn index(self) -> usize {
		match self {
			| Self::Queue => 0,
			| Self::DeviceLists => 1,
			| Self::Receipts => 2,
			| Self::Presence => 3,
		}
	}

	#[must_use]
	pub const fn label(self) -> &'static str {
		match self {
			| Self::Queue => "queue",
			| Self::DeviceLists => "device lists",
			| Self::Receipts => "receipts",
			| Self::Presence => "presence",
		}
	}
}

/// How an outbound federation transaction ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SendOutcome {
	/// The remote answered and the transaction was accepted.
	Ok,
	/// The request or the federation round trip timed out.
	Timeout,
	/// Connecting (DNS, TCP, TLS) to the remote failed.
	Connect,
	/// The remote answered with an error response.
	Remote,
	/// Anything else (serialization, local errors, ...).
	Other,
	/// The send was abandoned before a request was issued because its
	/// destination was aborted or its epoch went stale.
	Aborted,
}

/// Baseline and result of the once-a-minute roll-up.
#[derive(Default)]
struct MinuteRoll {
	baseline: OutboundSnapshot,
	last: OutboundSnapshot,
}

#[derive(Default)]
pub struct OutboundMetrics {
	pub attempts: AtomicU64,
	pub ok: AtomicU64,
	pub timeout: AtomicU64,
	pub connect: AtomicU64,
	pub remote: AtomicU64,
	pub other: AtomicU64,
	pub aborted: AtomicU64,

	/// Sum of per-transaction durations, microseconds.
	pub time_us: AtomicU64,
	/// Longest single transaction since start, microseconds.
	pub time_max_us: AtomicU64,
	pub latency: [AtomicU64; LATENCY_BUCKETS],

	/// Accessor calls made explicitly on the sender path (PDU fetches in
	/// `send_events_dest_federation`). This is a call-site count, not a count
	/// of redb or mtxdb operations: nested lookups inside those accessors and
	/// the destination-selection path are not included.
	pub sender_path_db_calls: AtomicU64,

	/// Destination selections performed (`select_events` entries).
	pub selections: AtomicU64,
	/// Accessor calls made while selecting what to send, by source. Counted
	/// at the sender's call sites, one per accessor invocation (a per-room or
	/// per-user lookup in a loop counts once per iteration). Not a count of
	/// redb or mtxdb operations.
	pub selection_calls: [AtomicU64; SELECTION_SOURCES],

	minute: Mutex<MinuteRoll>,
}

/// Plain-value copy of [`OutboundMetrics`] for reporting.
#[derive(Clone, Copy, Debug, Default)]
pub struct OutboundSnapshot {
	pub attempts: u64,
	pub ok: u64,
	pub timeout: u64,
	pub connect: u64,
	pub remote: u64,
	pub other: u64,
	pub aborted: u64,
	pub time_us: u64,
	pub time_max_us: u64,
	pub latency: [u64; LATENCY_BUCKETS],
	pub sender_path_db_calls: u64,
	pub selections: u64,
	pub selection_calls: [u64; SELECTION_SOURCES],
}

impl OutboundSnapshot {
	/// Counter growth since `prev`. `time_max_us` is a lifetime maximum and is
	/// carried over unchanged.
	#[must_use]
	pub fn delta_since(&self, prev: &Self) -> Self {
		Self {
			attempts: self.attempts.saturating_sub(prev.attempts),
			ok: self.ok.saturating_sub(prev.ok),
			timeout: self.timeout.saturating_sub(prev.timeout),
			connect: self.connect.saturating_sub(prev.connect),
			remote: self.remote.saturating_sub(prev.remote),
			other: self.other.saturating_sub(prev.other),
			aborted: self.aborted.saturating_sub(prev.aborted),
			time_us: self.time_us.saturating_sub(prev.time_us),
			time_max_us: self.time_max_us,
			latency: std::array::from_fn(|i| self.latency[i].saturating_sub(prev.latency[i])),
			sender_path_db_calls: self
				.sender_path_db_calls
				.saturating_sub(prev.sender_path_db_calls),
			selections: self.selections.saturating_sub(prev.selections),
			selection_calls: std::array::from_fn(|i| {
				self.selection_calls[i].saturating_sub(prev.selection_calls[i])
			}),
		}
	}

	/// Number of sends that have a recorded duration.
	#[must_use]
	pub fn timed(&self) -> u64 {
		self.ok
			.saturating_add(self.timeout)
			.saturating_add(self.connect)
			.saturating_add(self.remote)
			.saturating_add(self.other)
	}
}

impl OutboundMetrics {
	/// Take the growth since the previous call, remember it for
	/// [`Self::last_minute`], and return it. Called once a minute by the
	/// metrics aggregator.
	pub fn roll_minute(&self) -> OutboundSnapshot {
		let now = self.snapshot();
		let mut roll = self
			.minute
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner);
		let delta = now.delta_since(&roll.baseline);
		roll.baseline = now;
		roll.last = delta;
		delta
	}

	/// The most recent one-minute growth recorded by [`Self::roll_minute`].
	#[must_use]
	pub fn last_minute(&self) -> OutboundSnapshot {
		self.minute
			.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
			.last
	}

	/// Count a transaction about to be sent.
	#[inline]
	pub fn record_attempt(&self) { self.attempts.fetch_add(1, Ordering::Relaxed); }

	/// Count explicit sender-path accessor calls.
	#[inline]
	pub fn record_db_calls(&self, n: u64) {
		self.sender_path_db_calls.fetch_add(n, Ordering::Relaxed);
	}

	/// Count one destination selection.
	#[inline]
	pub fn record_selection(&self) { self.selections.fetch_add(1, Ordering::Relaxed); }

	/// Count `n` accessor calls made by the selection path for `source`.
	#[inline]
	pub fn record_selection_calls(&self, source: SelectionSource, n: u64) {
		self.selection_calls[source.index()].fetch_add(n, Ordering::Relaxed);
	}

	/// Record how a transaction ended and, unless it was aborted before a
	/// request was issued, how long it took.
	pub fn record_outcome(&self, outcome: SendOutcome, elapsed: Duration) {
		let counter = match outcome {
			| SendOutcome::Ok => &self.ok,
			| SendOutcome::Timeout => &self.timeout,
			| SendOutcome::Connect => &self.connect,
			| SendOutcome::Remote => &self.remote,
			| SendOutcome::Other => &self.other,
			| SendOutcome::Aborted => {
				self.aborted.fetch_add(1, Ordering::Relaxed);
				return;
			},
		};
		counter.fetch_add(1, Ordering::Relaxed);

		let us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
		self.time_us.fetch_add(us, Ordering::Relaxed);
		self.time_max_us.fetch_max(us, Ordering::Relaxed);
		self.latency[bucket_for(elapsed)].fetch_add(1, Ordering::Relaxed);
	}

	#[must_use]
	pub fn snapshot(&self) -> OutboundSnapshot {
		let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
		OutboundSnapshot {
			attempts: load(&self.attempts),
			ok: load(&self.ok),
			timeout: load(&self.timeout),
			connect: load(&self.connect),
			remote: load(&self.remote),
			other: load(&self.other),
			aborted: load(&self.aborted),
			time_us: load(&self.time_us),
			time_max_us: load(&self.time_max_us),
			latency: std::array::from_fn(|i| load(&self.latency[i])),
			sender_path_db_calls: load(&self.sender_path_db_calls),
			selections: load(&self.selections),
			selection_calls: std::array::from_fn(|i| load(&self.selection_calls[i])),
		}
	}
}

/// Bucket index for a duration: the smallest `i` with `elapsed <= 1 ms << i`,
/// capped at the overflow bucket.
#[must_use]
pub fn bucket_for(elapsed: Duration) -> usize {
	let ms = elapsed.as_millis();
	let mut bound = 1_u128;
	for i in 0..LATENCY_BUCKETS - 1 {
		if ms <= bound {
			return i;
		}
		bound <<= 1;
	}
	LATENCY_BUCKETS - 1
}

/// Upper bound of bucket `i` in milliseconds, or `None` for the overflow bucket.
#[must_use]
pub fn bucket_upper_ms(i: usize) -> Option<u64> { (i < LATENCY_BUCKETS - 1).then(|| 1_u64 << i) }

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn buckets_are_log_spaced_and_capped() {
		assert_eq!(bucket_for(Duration::from_micros(10)), 0);
		assert_eq!(bucket_for(Duration::from_millis(1)), 0);
		assert_eq!(bucket_for(Duration::from_millis(2)), 1);
		assert_eq!(bucket_for(Duration::from_millis(3)), 2);
		assert_eq!(bucket_for(Duration::from_millis(16_384)), LATENCY_BUCKETS - 2);
		assert_eq!(bucket_for(Duration::from_secs(60)), LATENCY_BUCKETS - 1);
		assert_eq!(bucket_upper_ms(0), Some(1));
		assert_eq!(bucket_upper_ms(LATENCY_BUCKETS - 1), None);
	}

	#[test]
	fn roll_minute_reports_growth_only() {
		let m = OutboundMetrics::default();
		m.record_attempt();
		m.record_selection();
		m.record_selection_calls(SelectionSource::Presence, 7);
		m.record_outcome(SendOutcome::Ok, Duration::from_millis(5));
		let first = m.roll_minute();
		assert_eq!((first.attempts, first.selections, first.selection_calls[3]), (1, 1, 7));

		m.record_attempt();
		m.record_outcome(SendOutcome::Remote, Duration::from_millis(5));
		let second = m.roll_minute();
		assert_eq!((second.attempts, second.selections, second.selection_calls[3]), (1, 0, 0));
		assert_eq!((second.ok, second.remote), (0, 1));
		assert_eq!(m.last_minute().remote, 1);
	}

	#[test]
	fn aborted_sends_are_not_timed() {
		let m = OutboundMetrics::default();
		m.record_attempt();
		m.record_outcome(SendOutcome::Aborted, Duration::from_secs(1));
		let s = m.snapshot();
		assert_eq!((s.attempts, s.aborted, s.time_us), (1, 1, 0));
		assert_eq!(s.latency.iter().sum::<u64>(), 0);
	}

	#[test]
	fn outcomes_accumulate_time_and_buckets() {
		let m = OutboundMetrics::default();
		m.record_outcome(SendOutcome::Ok, Duration::from_millis(5));
		m.record_outcome(SendOutcome::Timeout, Duration::from_secs(30));
		let s = m.snapshot();
		assert_eq!((s.ok, s.timeout), (1, 1));
		assert_eq!(s.time_us, 30_005_000);
		assert_eq!(s.time_max_us, 30_000_000);
		assert_eq!(s.latency[3], 1);
		assert_eq!(s.latency[LATENCY_BUCKETS - 1], 1);
	}
}

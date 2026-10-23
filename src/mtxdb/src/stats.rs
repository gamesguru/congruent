//! Process-wide mtxdb operation counters, cheap enough to leave always on.
//!
//! Totals only: callers that want a rate or per-phase cost take a
//! [`Snapshot`] before and after and subtract.

use std::{
	fmt,
	sync::atomic::{AtomicU64, Ordering::Relaxed},
	time::Duration,
};

static READS: AtomicU64 = AtomicU64::new(0);
static READ_NODES: AtomicU64 = AtomicU64::new(0);
static READ_NANOS: AtomicU64 = AtomicU64::new(0);
static READ_ERRORS: AtomicU64 = AtomicU64::new(0);
static PUTS: AtomicU64 = AtomicU64::new(0);
static PUT_BYTES: AtomicU64 = AtomicU64::new(0);
static REMOVES: AtomicU64 = AtomicU64::new(0);
static COMMITS: AtomicU64 = AtomicU64::new(0);
static COMMIT_NANOS: AtomicU64 = AtomicU64::new(0);
static COMMIT_ERRORS: AtomicU64 = AtomicU64::new(0);

fn nanos(elapsed: Duration) -> u64 { u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX) }

pub(crate) fn record_read(nodes: usize, elapsed: Duration, ok: bool) {
	READS.fetch_add(1, Relaxed);
	READ_NODES.fetch_add(nodes as u64, Relaxed);
	READ_NANOS.fetch_add(nanos(elapsed), Relaxed);
	if !ok {
		READ_ERRORS.fetch_add(1, Relaxed);
	}
}

pub(crate) fn record_put(bytes: usize) {
	PUTS.fetch_add(1, Relaxed);
	PUT_BYTES.fetch_add(bytes as u64, Relaxed);
}

pub(crate) fn record_removes(count: usize) { REMOVES.fetch_add(count as u64, Relaxed); }

pub(crate) fn record_commit(elapsed: Duration, ok: bool) {
	COMMITS.fetch_add(1, Relaxed);
	COMMIT_NANOS.fetch_add(nanos(elapsed), Relaxed);
	if !ok {
		COMMIT_ERRORS.fetch_add(1, Relaxed);
	}
}

/// A point-in-time copy of the counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
	pub reads: u64,
	pub read_nodes: u64,
	pub read_micros: u64,
	pub read_errors: u64,
	pub puts: u64,
	pub put_bytes: u64,
	pub removes: u64,
	pub commits: u64,
	pub commit_micros: u64,
	pub commit_errors: u64,
}

#[must_use]
pub fn snapshot() -> Snapshot {
	Snapshot {
		reads: READS.load(Relaxed),
		read_nodes: READ_NODES.load(Relaxed),
		read_micros: READ_NANOS.load(Relaxed) / 1_000,
		read_errors: READ_ERRORS.load(Relaxed),
		puts: PUTS.load(Relaxed),
		put_bytes: PUT_BYTES.load(Relaxed),
		removes: REMOVES.load(Relaxed),
		commits: COMMITS.load(Relaxed),
		commit_micros: COMMIT_NANOS.load(Relaxed) / 1_000,
		commit_errors: COMMIT_ERRORS.load(Relaxed),
	}
}

impl Snapshot {
	/// Counters accumulated since `earlier`.
	#[must_use]
	pub fn since(self, earlier: Self) -> Self {
		Self {
			reads: self.reads.saturating_sub(earlier.reads),
			read_nodes: self.read_nodes.saturating_sub(earlier.read_nodes),
			read_micros: self.read_micros.saturating_sub(earlier.read_micros),
			read_errors: self.read_errors.saturating_sub(earlier.read_errors),
			puts: self.puts.saturating_sub(earlier.puts),
			put_bytes: self.put_bytes.saturating_sub(earlier.put_bytes),
			removes: self.removes.saturating_sub(earlier.removes),
			commits: self.commits.saturating_sub(earlier.commits),
			commit_micros: self.commit_micros.saturating_sub(earlier.commit_micros),
			commit_errors: self.commit_errors.saturating_sub(earlier.commit_errors),
		}
	}
}

impl fmt::Display for Snapshot {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"reads={} read_nodes={} read_us={} read_errors={} puts={} put_bytes={} removes={} \
			 commits={} commit_us={} commit_errors={}",
			self.reads,
			self.read_nodes,
			self.read_micros,
			self.read_errors,
			self.puts,
			self.put_bytes,
			self.removes,
			self.commits,
			self.commit_micros,
			self.commit_errors,
		)
	}
}

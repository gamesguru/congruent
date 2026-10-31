//! Temporary `StaleRead` retry counters, to measure real same-room write
//! contention. Read with [`snapshot`].

use std::sync::atomic::{AtomicU64, Ordering};

static EVENT_RETRIES: AtomicU64 = AtomicU64::new(0);
static EVENT_EXHAUSTED: AtomicU64 = AtomicU64::new(0);
static TIMELINE_RETRIES: AtomicU64 = AtomicU64::new(0);
static TIMELINE_EXHAUSTED: AtomicU64 = AtomicU64::new(0);

/// Which write path hit the conflict.
#[derive(Clone, Copy)]
pub enum Site {
	Event,
	Timeline,
}

#[allow(
	clippy::disallowed_macros,
	reason = "conduwuit_core::debug expands through the log crate"
)]
pub(crate) fn retried(site: Site) {
	let n = match site {
		| Site::Event => EVENT_RETRIES.fetch_add(1, Ordering::Relaxed),
		| Site::Timeline => TIMELINE_RETRIES.fetch_add(1, Ordering::Relaxed),
	};
	conduwuit_core::debug!("mtxdb StaleRead retry (total before: {n})");
}

pub(crate) fn exhausted(site: Site) {
	match site {
		| Site::Event => EVENT_EXHAUSTED.fetch_add(1, Ordering::Relaxed),
		| Site::Timeline => TIMELINE_EXHAUSTED.fetch_add(1, Ordering::Relaxed),
	};
}

/// `(event_retries, event_exhausted, timeline_retries, timeline_exhausted)`.
#[must_use]
pub fn snapshot() -> (u64, u64, u64, u64) {
	(
		EVENT_RETRIES.load(Ordering::Relaxed),
		EVENT_EXHAUSTED.load(Ordering::Relaxed),
		TIMELINE_RETRIES.load(Ordering::Relaxed),
		TIMELINE_EXHAUSTED.load(Ordering::Relaxed),
	)
}

/// Held by tests that read [`snapshot`] deltas, so they don't interleave.
#[cfg(test)]
pub(crate) static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

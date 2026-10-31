//! Explicit retry policy for optimistic `StaleRead` conflicts.
//!
//! mtxdb surfaces a conflict instead of absorbing it (the Postgres `40001` /
//! Scylla CAS convention). Retrying is then an application decision, because
//! only the caller knows whether the operation is idempotent and how much
//! contention it can absorb.
//!
//! This module is the single place that decision is made, so the bound and
//! backoff are named and consistent instead of a magic number per call site.
//! Exhaustion returns the `StaleRead` unchanged; callers must handle it (for
//! example by surfacing it to the request) rather than log and carry on.

use std::{
	sync::atomic::{AtomicU64, Ordering},
	time::Duration,
};

use conduwuit_mtxdb::StorageError;

/// Attempts for one logical write, including the first.
pub(super) const MAX_ATTEMPTS: u32 = 8;

/// First backoff. Conflicts are rare behind the adapters' write locks, so this stays
/// small; it exists to break up retry storms, not to wait out a long writer.
const BASE_BACKOFF: Duration = Duration::from_micros(50);

/// Upper bound on one backoff step.
const MAX_BACKOFF: Duration = Duration::from_millis(2);

/// How many conflicts were absorbed by this policy. One counter for the whole
/// backstop, so operators can tell whether it ever fires; exhausted conflicts
/// are not counted here because they are returned to the caller.
static RETRIES: AtomicU64 = AtomicU64::new(0);

/// Number of conflicts retried so far.
#[must_use]
pub(super) fn retry_count() -> u64 { RETRIES.load(Ordering::Relaxed) }

/// Run `op`, retrying only [`StorageError::StaleRead`] with bounded exponential
/// backoff and jitter. Every other error, and an exhausted conflict, is
/// returned unchanged.
pub(super) async fn retry_stale<T>(
	mut op: impl FnMut() -> Result<T, StorageError>,
) -> Result<T, StorageError> {
	let mut attempt = 0_u32;
	loop {
		match op() {
			| Err(error) if error.is_stale_read() && attempt < MAX_ATTEMPTS.saturating_sub(1) => {
				attempt = attempt.saturating_add(1);
				RETRIES.fetch_add(1, Ordering::Relaxed);
				// Tests run on smol with no tokio reactor; smol's timer works
				// under either runtime.
				smol::Timer::after(backoff(attempt)).await;
			},
			| result => {
				if let Err(error) = &result
					&& error.is_stale_read()
				{
					conduwuit::warn!(
						retries = retry_count(),
						"mtxdb write exhausted its StaleRead retry budget: {error}"
					);
				}
				return result;
			},
		}
	}
}

/// Exponential backoff with +/-25% jitter, so two writers that collided do not
/// wake together and collide again.
fn backoff(attempt: u32) -> Duration {
	let shift = attempt.saturating_sub(1).min(6);
	let base = BASE_BACKOFF.saturating_mul(1_u32 << shift).min(MAX_BACKOFF);
	let span = base / 2;
	base.saturating_sub(span / 2).saturating_add(jitter(span))
}

fn jitter(span: Duration) -> Duration {
	static STATE: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
	if span.is_zero() {
		return Duration::ZERO;
	}
	let mut x = STATE.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
	x ^= x >> 12;
	x ^= x << 25;
	x ^= x >> 27;
	let span_nanos = u64::try_from(span.as_nanos()).unwrap_or(u64::MAX);
	Duration::from_nanos(
		x.wrapping_mul(0x2545_F491_4F6C_DD1D)
			.rem_euclid(span_nanos.max(1)),
	)
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicU32, Ordering};

	use conduwuit_mtxdb::ShardType;

	use super::*;

	fn stale() -> StorageError {
		StorageError::StaleRead {
			pool: ShardType::EventDag,
			collection_id: [0; 16],
			expected: 1,
			actual: 2,
		}
	}

	// Exercises the backoff timer under the executor the server actually uses.
	#[test]
	fn retries_a_stale_read_then_succeeds() {
		smol::block_on(async {
			let attempts = AtomicU32::new(0);
			let before = retry_count();
			let result = retry_stale(|| {
				if attempts.fetch_add(1, Ordering::Relaxed) < 2 {
					Err(stale())
				} else {
					Ok(7_u32)
				}
			})
			.await;
			assert_eq!(result.unwrap(), 7);
			assert_eq!(attempts.load(Ordering::Relaxed), 3);
			// The counter is process-wide; other tests may add to it concurrently.
			assert!(retry_count() >= before + 2);
		});
	}

	#[test]
	fn exhaustion_returns_the_conflict() {
		smol::block_on(async {
			let attempts = AtomicU32::new(0);
			let result = retry_stale(|| {
				attempts.fetch_add(1, Ordering::Relaxed);
				Err::<(), _>(stale())
			})
			.await;
			assert!(result.is_err_and(|error| error.is_stale_read()));
			assert_eq!(attempts.load(Ordering::Relaxed), MAX_ATTEMPTS);
		});
	}

	#[test]
	fn non_conflict_errors_are_not_retried() {
		smol::block_on(async {
			let attempts = AtomicU32::new(0);
			let result = retry_stale(|| {
				attempts.fetch_add(1, Ordering::Relaxed);
				Err::<(), _>(StorageError::Internal("boom".to_owned()))
			})
			.await;
			assert!(result.is_err());
			assert_eq!(attempts.load(Ordering::Relaxed), 1);
		});
	}
}

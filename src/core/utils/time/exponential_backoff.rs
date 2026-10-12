use std::{cmp, time::Duration};

/// Returns false if the exponential backoff has expired based on the inputs
#[inline]
#[must_use]
pub fn continue_exponential_backoff_secs(
	min: u64,
	max: u64,
	elapsed: Duration,
	tries: u32,
) -> bool {
	let min = Duration::from_secs(min);
	let max = Duration::from_secs(max);
	continue_exponential_backoff(min, max, elapsed, tries)
}

/// Returns false if the exponential backoff has expired based on the inputs
#[inline]
#[must_use]
pub fn continue_exponential_backoff(
	min: Duration,
	max: Duration,
	elapsed: Duration,
	tries: u32,
) -> bool {
	let min = min.saturating_mul(1_u32.checked_shl(tries).unwrap_or(u32::MAX));
	let min = cmp::min(min, max);
	elapsed < min
}

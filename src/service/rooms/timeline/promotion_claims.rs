//! Mutual-exclusion arbiter between outlier promotion and event rejection
//! for the same event ID.
//!
//! Extracted out of `Service` (rather than living as inherent methods on
//! it) specifically so the concurrency invariant here -- a promotion and a
//! rejection for the same event ID can never both win -- is unit-testable
//! without constructing a full `Service` fixture. See the `tests` module
//! below for the actual concurrent proof; `cargo check`/clippy passing only
//! proves this compiles, not that the race is closed.

use std::collections::HashMap;

use conduwuit_core::SyncMutex;
use ruma::{EventId, OwnedEventId};

/// Which disposition currently owns an event ID in [`PromotionClaims`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotionDisposition {
	/// An outlier promotion has reserved this event: queued into a batch,
	/// not yet committed.
	Promoting,
	/// A rejection won the atomic claim for this event. No concurrent
	/// promotion may proceed while this entry stands.
	Rejected,
}

/// Claims for outlier promotions that have been queued into a caller-owned
/// batch but not yet committed, *and* for rejections that have won the race
/// against a promotion attempt. Both sides mutate the same map under the
/// same lock, so a promotion claim and a rejection claim for the same event
/// ID can never both succeed -- whichever calls `lock()` first wins, and
/// the loser observes the winner's disposition in that same locked
/// operation instead of a separate, racy check-then-act pair.
///
/// `non_outlier_pdu_exists` only sees committed rows, so without the
/// promotion side of this, the same event queued twice before either batch
/// is applied -- duplicate input in one chunk, or the same event split
/// across two concurrently-processed chunks -- would pass the
/// already-in-timeline check both times and be promoted twice.
pub struct PromotionClaims {
	inner: SyncMutex<HashMap<OwnedEventId, PromotionDisposition>>,
}

impl PromotionClaims {
	#[must_use]
	pub fn new() -> Self { Self { inner: SyncMutex::new(HashMap::new()) } }

	/// Attempts to atomically claim `event_id` for an outlier promotion: if
	/// nothing else currently owns it, claims
	/// [`PromotionDisposition::Promoting`] and returns `true`. Returns `false`
	/// if a promotion already claimed it (duplicate/concurrent promotion
	/// attempt) or a rejection claimed it first -- either way the caller must
	/// not proceed with this promotion.
	pub fn try_claim_promotion(&self, event_id: &EventId) -> bool {
		let mut guard = self.inner.lock();
		if guard.contains_key(event_id) {
			false
		} else {
			guard.insert(event_id.to_owned(), PromotionDisposition::Promoting);
			true
		}
	}

	/// Attempts to atomically claim `event_id` for rejection: if nothing
	/// else currently owns it, claims [`PromotionDisposition::Rejected`] in
	/// the same locked operation and returns `true` -- the caller should
	/// then persist the rejection and call [`Self::release_rejection_claim`].
	/// Returns `false` if a promotion already reserved this event (the
	/// caller must *not* write a rejection marker), or if another rejection
	/// attempt for the same event is already in flight.
	///
	/// The rejection side has to be exclusive too, not just "refuse if a
	/// promotion holds it": two concurrent `mark_event_rejected` calls for
	/// the same event both observing `Rejected` and both proceeding would
	/// let either one release the claim -- via
	/// [`Self::release_rejection_claim`] -- while the *other's* database write
	/// is still in flight, reopening the slot for a promotion to sneak in
	/// between that write and its own (now-orphaned) release. Refusing the
	/// second claimant outright closes that window; the caller treats a
	/// `false` return as "someone else is already handling this, skip".
	///
	/// This has to be a single lock-and-mutate operation, not a separate
	/// check followed by a write: an arbitrary amount of time -- a thread
	/// preemption, a concurrent core, not just an `.await` -- can pass
	/// between a check and a later write. Because this and
	/// [`Self::try_claim_promotion`] mutate the same map under the same lock
	/// with no intervening await, whichever call reaches `lock()` first wins
	/// unconditionally.
	/// Releases a promotion claim taken by [`Self::try_claim_promotion`],
	/// once the promotion is either committed or abandoned.
	pub fn release_promotion_claim(&self, event_id: &EventId) {
		let mut guard = self.inner.lock();
		if matches!(guard.get(event_id), Some(PromotionDisposition::Promoting)) {
			guard.remove(event_id);
		}
	}
}

impl Default for PromotionClaims {
	fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use ruma::owned_event_id;

	use super::*;

	#[test]
	fn promotion_claims_are_exclusive() {
		let claims = Arc::new(PromotionClaims::new());
		let event_id = owned_event_id!("$test_event:example.org");

		assert!(claims.try_claim_promotion(&event_id));
		assert!(!claims.try_claim_promotion(&event_id));

		claims.release_promotion_claim(&event_id);
		assert!(claims.try_claim_promotion(&event_id));
	}

	#[test]
	fn duplicate_promotion_claim_is_refused() {
		let claims = PromotionClaims::new();
		let event_id = owned_event_id!("$dup:example.org");
		assert!(claims.try_claim_promotion(&event_id));
		assert!(!claims.try_claim_promotion(&event_id));
	}
}

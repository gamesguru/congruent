//! Access-token lifetime rules for MSC2918 refresh tokens.
//!
//! Tokens are stored with an optional absolute expiry (milliseconds since the
//! Unix epoch). A token without an entry belongs to a client which did not
//! opt into refresh tokens, and never expires.

/// Absolute expiry of a token issued at `now` with a lifetime of `ttl` ms.
pub(super) fn expires_at(now: u64, ttl: u64) -> u64 { now.saturating_add(ttl) }

/// Whether a token with this recorded expiry has lapsed at `now`.
pub(super) fn has_expired(expires_at: Option<u64>, now: u64) -> bool {
	expires_at.is_some_and(|expires_at| expires_at <= now)
}

/// Whether a refresh replaces this token. Only expiring tokens are superseded;
/// non-expiring ones were issued to clients without refresh support.
pub(super) fn superseded_on_refresh(expires_at: Option<u64>) -> bool { expires_at.is_some() }

#[cfg(test)]
mod tests {
	use super::{expires_at, has_expired, superseded_on_refresh};

	#[test]
	fn expiry_is_issue_time_plus_lifetime() {
		assert_eq!(expires_at(1_000, 300_000), 301_000);
	}

	#[test]
	fn expiry_saturates_instead_of_overflowing() {
		assert_eq!(expires_at(u64::MAX - 1, 10), u64::MAX);
	}

	#[test]
	fn a_token_without_an_expiry_never_expires() {
		assert!(!has_expired(None, 0));
		assert!(!has_expired(None, u64::MAX));
	}

	#[test]
	fn a_token_expires_at_its_deadline_and_not_before() {
		assert!(!has_expired(Some(1_000), 999));
		assert!(has_expired(Some(1_000), 1_000));
		assert!(has_expired(Some(1_000), 1_001));
	}

	#[test]
	fn refresh_supersedes_only_expiring_tokens() {
		assert!(superseded_on_refresh(Some(1_000)));
		assert!(!superseded_on_refresh(None));
	}
}

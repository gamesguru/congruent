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

/// The tokens of a device (paired with their recorded expiry) which a refresh
/// replaces, in their original order. The most recent expiring token is kept:
/// a client which lost the previous refresh response may still hold it.
pub(super) fn superseded_tokens(tokens: Vec<(String, Option<u64>)>) -> Vec<String> {
	let mut expiring: Vec<(String, u64)> = tokens
		.into_iter()
		.filter(|(_, expires_at)| superseded_on_refresh(*expires_at))
		.filter_map(|(token, expires_at)| expires_at.map(|at| (token, at)))
		.collect();
	if let Some(newest) = expiring
		.iter()
		.enumerate()
		.max_by_key(|(_, (_, at))| *at)
		.map(|(i, _)| i)
	{
		expiring.remove(newest);
	}
	expiring.into_iter().map(|(token, _)| token).collect()
}

/// A device's token list with one token revoked.
pub(super) fn without_token(mut tokens: Vec<String>, token: &str) -> Vec<String> {
	tokens.retain(|t| t != token);
	tokens
}

#[cfg(test)]
mod tests {
	use super::{
		expires_at, has_expired, superseded_on_refresh, superseded_tokens, without_token,
	};

	/// Verifies that expiry is the issue time plus the requested lifetime.
	#[test]
	fn expiry_is_issue_time_plus_lifetime() {
		assert_eq!(expires_at(1_000, 300_000), 301_000);
	}

	/// Verifies that expiry saturates at the maximum timestamp on overflow.
	#[test]
	fn expiry_saturates_instead_of_overflowing() {
		assert_eq!(expires_at(u64::MAX - 1, 10), u64::MAX);
	}

	/// Verifies that a token without an expiry remains valid at any timestamp.
	#[test]
	fn a_token_without_an_expiry_never_expires() {
		assert!(!has_expired(None, 0));
		assert!(!has_expired(None, u64::MAX));
	}

	/// Verifies that expiry begins exactly at the recorded deadline.
	#[test]
	fn a_token_expires_at_its_deadline_and_not_before() {
		assert!(!has_expired(Some(1_000), 999));
		assert!(has_expired(Some(1_000), 1_000));
		assert!(has_expired(Some(1_000), 1_001));
	}

	/// Verifies that refresh supersedes only tokens with a recorded expiry.
	#[test]
	fn refresh_supersedes_only_expiring_tokens() {
		assert!(superseded_on_refresh(Some(1_000)));
		assert!(!superseded_on_refresh(None));
	}

	/// Builds an owned token and optional expiry pair for refresh tests.
	fn owned(token: &str, expires_at: Option<u64>) -> (String, Option<u64>) {
		(token.to_owned(), expires_at)
	}

	/// Verifies that refresh selects expiring tokens in their original order.
	#[test]
	fn a_refresh_replaces_older_expiring_tokens_and_keeps_the_rest() {
		let tokens = vec![owned("old", Some(1)), owned("legacy", None), owned("older", Some(2))];
		assert_eq!(superseded_tokens(tokens), ["old"]);
	}

	/// Verifies that refresh preserves legacy tokens and accepts an empty token list.
	#[test]
	fn a_refresh_with_no_expiring_tokens_replaces_nothing() {
		assert!(superseded_tokens(vec![owned("legacy", None)]).is_empty());
		assert!(superseded_tokens(Vec::new()).is_empty());
	}

	/// Verifies that refresh keeps the most recent expiring token for lost-response recovery.
	#[test]
	fn the_most_recent_expiring_token_survives_a_refresh() {
		// A client which lost the last response still holds it, and needs it to
		// be expired rather than unknown so that it can refresh again.
		assert!(superseded_tokens(vec![owned("lapsed", Some(0))]).is_empty());
		let tokens = vec![owned("a", Some(1)), owned("b", Some(2)), owned("c", Some(3))];
		assert_eq!(superseded_tokens(tokens), ["a", "b"]);
	}

	/// Verifies that revocation preserves other device tokens in their original order.
	#[test]
	fn revoking_a_token_leaves_the_devices_others() {
		let tokens = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
		assert_eq!(without_token(tokens, "b"), ["a", "c"]);
	}

	/// Verifies that revoking an absent token leaves the token list unchanged.
	#[test]
	fn revoking_an_unknown_token_changes_nothing() {
		let tokens = vec!["a".to_owned()];
		assert_eq!(without_token(tokens, "z"), ["a"]);
	}
}

//! Tombstone detection for stored account data. The decode/re-encode golden
//! fixtures for the typed events live in slipstream
//! (`tests/account_data_golden.rs`), next to the codec they pin.

use super::is_account_data_tombstone;

#[test]
fn tombstones_are_empty_content_only() {
	assert!(is_account_data_tombstone(br#"{"content":{},"type":"m.direct"}"#));
	assert!(!is_account_data_tombstone(br#"{"content":{"a":1},"type":"m.direct"}"#));
	// Not an object / unparsable data is never treated as a tombstone.
	assert!(!is_account_data_tombstone(br#"{"content":[],"type":"m.direct"}"#));
	assert!(!is_account_data_tombstone(b"not json"));
}

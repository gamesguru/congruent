use ruma::CanonicalJsonObject;

/// Strips internal database keys and injected fields from a PDU's JSON
/// representation so it can be hashed or verified according to Matrix canonical
/// JSON rules.
#[inline]
pub fn pdu_json_canonical_strip(event: &mut CanonicalJsonObject) {
	event.remove("__shortstatehash");
	event.remove("__outlier");
	event.remove("__soft_failed");
	event.remove("__rejected");
	event.remove("event_id");
	event.remove("prev_state_events");
	event.remove("state_jump_pointers");
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use ruma::CanonicalJsonValue;

	use super::*;

	#[test]
	fn test_pdu_json_canonical_strip() {
		let mut event = BTreeMap::new();
		event.insert("__shortstatehash".to_owned(), CanonicalJsonValue::Integer(12345.into()));
		event.insert("event_id".to_owned(), CanonicalJsonValue::String("$abc123".to_owned()));
		event.insert("prev_state_events".to_owned(), CanonicalJsonValue::Array(vec![]));
		event.insert("state_jump_pointers".to_owned(), CanonicalJsonValue::Array(vec![]));
		event.insert("type".to_owned(), CanonicalJsonValue::String("m.room.message".to_owned()));

		pdu_json_canonical_strip(&mut event);

		assert!(!event.contains_key("__shortstatehash"));
		assert!(!event.contains_key("event_id"));
		assert!(!event.contains_key("prev_state_events"));
		assert!(!event.contains_key("state_jump_pointers"));
		assert!(event.contains_key("type"));
	}
}

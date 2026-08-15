use ruma::{CanonicalJsonObject, CanonicalJsonValue, OwnedEventId, RoomId, RoomVersionId};

use crate::{PduEvent, Result};

/// Parses a raw JSON string into a CanonicalJsonObject, strips diagnostic
/// fields, handles room_id stripping based on room version, extracts the
/// event_id, and returns the fully parsed PduEvent and its cleaned
/// CanonicalJsonObject.
pub fn parse_and_clean_pdu(
	mut value: CanonicalJsonObject,
	room_id: &RoomId,
	room_version: &RoomVersionId,
) -> Result<(OwnedEventId, CanonicalJsonObject, PduEvent)> {
	let event_id = match value
		.get("event_id")
		.and_then(CanonicalJsonValue::as_str)
		.and_then(|id| OwnedEventId::parse(id).ok())
	{
		| Some(id) => id,
		| None => crate::matrix::event::gen_event_id(&value, room_version)?,
	};

	// Strip diagnostic/internal fields that were injected during export or
	// debugging
	crate::utils::pdu_json_canonical_strip(&mut value);

	let room_features = crate::RoomVersion::new(room_version).unwrap_or(crate::RoomVersion::V1);

	let is_create =
		value.get("type").and_then(CanonicalJsonValue::as_str) == Some("m.room.create");

	if room_features.strips_room_id(is_create) {
		value.remove("room_id");
	}

	let pdu = PduEvent::from_id_val(&event_id, value.clone(), Some(room_id))?;

	Ok((event_id, value, pdu))
}

#[cfg(test)]
mod tests {
	use ruma::{room_id, room_version_id};
	use serde_json::json;

	use super::*;

	#[test]
	fn test_parse_and_clean_pdu() {
		let room_id = room_id!("!test:example.com");
		let version = room_version_id!("10"); // V3+ strips room_id

		let raw_json = json!({
			"event_id": "$test_event",
			"type": "m.room.message",
			"room_id": "!test:example.com",
			"sender": "@user:example.com",
			"origin_server_ts": 12345,
			"content": {"body": "hello"},
			"auth_events": [],
			"prev_events": [],
			"depth": 1,
			"hashes": {
				"sha256": "fakehash"
			},
			"signatures": {
				"example.com": {
					"ed25519:1": "fakesig"
				}
			},
			"__shortstatehash": 42, // Should be stripped
			"prev_state_events": [] // Should be stripped
		})
		.to_string();

		let value: CanonicalJsonObject = serde_json::from_str(&raw_json).unwrap();
		let (eid, clean_val, pdu) = parse_and_clean_pdu(value, room_id, &version).unwrap();

		assert_eq!(eid.as_str(), "$test_event");
		assert!(!clean_val.contains_key("__shortstatehash"));
		assert!(!clean_val.contains_key("prev_state_events"));
		assert!(clean_val.contains_key("room_id")); // Only stripped for m.room.create in v11+
		assert_eq!(pdu.sender.as_str(), "@user:example.com");
	}
}

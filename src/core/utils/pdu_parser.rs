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
	// Preserve an explicit event_id (e.g. from an imported/exported PDU) before
	// it gets stripped below, so it isn't needlessly regenerated.
	let explicit_event_id = value
		.get("event_id")
		.and_then(CanonicalJsonValue::as_str)
		.and_then(|id| OwnedEventId::parse(id).ok());

	// Strip diagnostic/internal fields that were injected during export or
	// debugging
	crate::utils::pdu_json_canonical_strip(&mut value);

	let room_features = crate::RoomVersion::new(room_version).unwrap_or(crate::RoomVersion::V1);

	let is_create =
		value.get("type").and_then(CanonicalJsonValue::as_str) == Some("m.room.create");

	if room_features.strips_room_id(is_create) {
		value.remove("room_id");
	}

	let event_id = match explicit_event_id {
		| Some(id) => id,
		| None => crate::matrix::event::gen_event_id(&value, room_version)?,
	};

	let pdu = PduEvent::from_id_val(&event_id, value.clone(), Some(room_id))?;

	Ok((event_id, value, pdu))
}

#[cfg(test)]
mod tests {
	use ruma::{events::TimelineEventType, room_id, room_version_id};
	use serde_json::json;

	use super::*;
	use crate::matrix::event::gen_event_id;

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

	#[test]
	fn v12_create_event_hashes_after_room_id_stripping() {
		let version = room_version_id!("12");

		let make_value = || {
			serde_json::from_value::<CanonicalJsonObject>(json!({
				"type": "m.room.create",
				"sender": "@alice:example.org",
				"origin_server_ts": 12345,
				"content": { "creator": "@alice:example.org", "room_version": "12" },
				"auth_events": [],
				"prev_events": [],
				"depth": 1,
				"hashes": { "sha256": "fakehash" },
				"signatures": {
					"example.org": { "ed25519:1": "fakesig" }
				}
			}))
			.unwrap()
		};

		// V12 room_ids are derived from the create event's own reference hash
		// ($ -> !), so the expected room_id can't be chosen up front: compute
		// it the same way the real PDU-import path does, from the hash of the
		// room_id-less event.
		let expected_event_id = gen_event_id(&make_value(), &version).unwrap();
		let room_id_str = expected_event_id.as_str().replacen('$', "!", 1);
		let room_id = RoomId::parse(&room_id_str).unwrap();

		let mut value = make_value();
		value.insert(
			"room_id".to_owned(),
			CanonicalJsonValue::String(room_id.as_str().to_owned()),
		);

		let (event_id, clean_val, pdu) = parse_and_clean_pdu(value, &room_id, &version).unwrap();

		assert!(
			!clean_val.contains_key("room_id"),
			"V12 create events must drop room_id before hashing"
		);
		assert_eq!(
			event_id,
			gen_event_id(&clean_val, &version).unwrap(),
			"derived event_id must match cleaned canonical JSON"
		);
		assert_eq!(pdu.kind, TimelineEventType::RoomCreate);
	}

	#[test]
	fn v12_non_create_event_keeps_room_id_before_hashing() {
		let room_id = room_id!("!test:example.com");
		let version = room_version_id!("12");

		let value: CanonicalJsonObject = serde_json::from_value(json!({
			"type": "m.room.member",
			"room_id": room_id.as_str(),
			"sender": "@alice:example.org",
			"state_key": "@alice:example.org",
			"origin_server_ts": 12345,
			"content": { "membership": "join" },
			"auth_events": [],
			"prev_events": [],
			"depth": 1,
			"hashes": { "sha256": "fakehash" },
			"signatures": {
				"example.org": { "ed25519:1": "fakesig" }
			}
		}))
		.unwrap();

		let (event_id, clean_val, pdu) = parse_and_clean_pdu(value, room_id, &version).unwrap();

		assert!(clean_val.contains_key("room_id"), "non-create events must retain room_id");
		assert_eq!(
			event_id,
			gen_event_id(&clean_val, &version).unwrap(),
			"derived event_id must still match the cleaned canonical JSON"
		);
		assert_eq!(pdu.kind, TimelineEventType::RoomMember);
	}
}

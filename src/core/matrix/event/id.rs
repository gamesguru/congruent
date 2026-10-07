use slipstream::{CanonicalJsonObject, OwnedEventId, RoomVersionId};

use super::super::pdu::RawJson;
use crate::{Result, err, utils::pdu_json_canonical_strip};

/// Generates a correct eventId for the incoming pdu.
///
/// Returns a tuple of the new `EventId` and the PDU as a `BTreeMap<String,
/// CanonicalJsonValue>`.
pub fn gen_event_id_canonical_json(
	pdu: &RawJson,
	room_version_id: &RoomVersionId,
) -> Result<(OwnedEventId, CanonicalJsonObject)> {
	let value = slipstream::canonical_json::from_json_str(pdu.get())
		.map_err(|e| err!(BadServerResponse(warn!("Error parsing incoming event: {e:?}"))))?;
	let value = slipstream::canonical_json::into_object(value)
		.ok_or_else(|| err!(BadServerResponse(warn!("incoming event is not an object"))))?;

	let event_id = gen_event_id(&value, room_version_id)?;

	Ok((event_id, value))
}

/// Whether event IDs in this room version are server-assigned strings carried
/// in the event's own `event_id` field, rather than derived from a reference
/// hash (room versions 1 and 2).
#[must_use]
pub fn has_opaque_event_ids(room_version_id: &RoomVersionId) -> bool {
	matches!(room_version_id, RoomVersionId::V1 | RoomVersionId::V2)
}

/// Generates a correct eventId for the incoming pdu.
pub fn gen_event_id(
	value: &CanonicalJsonObject,
	room_version_id: &RoomVersionId,
) -> Result<OwnedEventId> {
	if has_opaque_event_ids(room_version_id) {
		// Room versions 1 and 2: the ID is a distinct field of the event.
		let event_id = value
			.get("event_id")
			.and_then(slipstream::json::Value::as_str)
			.ok_or_else(|| {
				err!(Request(BadJson("Event has no event_id, which room versions 1 and 2 need")))
			})?;
		return Ok(OwnedEventId::parse(event_id)?);
	}

	let reference_hash = slipstream::signatures::reference_hash(value, room_version_id)?;
	let event_id = OwnedEventId::parse(format!("${reference_hash}"))?;

	Ok(event_id)
}

/// Generates a correct eventId from raw stored bytes, avoiding serde
/// round-trip issues that would produce false hash mismatches.
pub fn gen_event_id_from_bytes(
	raw_bytes: &[u8],
	room_version_id: &RoomVersionId,
) -> Result<OwnedEventId> {
	let raw_str = std::str::from_utf8(raw_bytes)
		.map_err(|e| err!(Database("stored PDU is not valid UTF-8: {e}")))?;

	let value = slipstream::canonical_json::from_json_str(raw_str)
		.map_err(|e| err!(Database("stored PDU is not valid JSON: {e}")))?;
	let mut value = slipstream::canonical_json::into_object(value)
		.ok_or_else(|| err!(Database("stored PDU is not an object")))?;
	// Room versions 1 and 2 keep their own `event_id`, which the strip removes.
	let own_event_id = value.get("event_id").cloned();
	pdu_json_canonical_strip(&mut value);
	if has_opaque_event_ids(room_version_id) {
		if let Some(event_id) = own_event_id {
			value.insert("event_id".to_owned(), event_id);
		}
	}

	gen_event_id(&value, room_version_id)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn room_versions_one_and_two_use_the_events_own_event_id() {
		let mut event = CanonicalJsonObject::new();
		assert!(gen_event_id(&event, &RoomVersionId::V1).is_err(), "no event_id field");

		event.insert(
			"event_id".to_owned(),
			slipstream::CanonicalJsonValue::String("$abc:example.org".to_owned()),
		);
		for version in [RoomVersionId::V1, RoomVersionId::V2] {
			assert_eq!(gen_event_id(&event, &version).unwrap().as_str(), "$abc:example.org");
		}
		assert!(has_opaque_event_ids(&RoomVersionId::V2));
		assert!(!has_opaque_event_ids(&RoomVersionId::V3));
	}

	#[test]
	fn gen_event_id_from_bytes_ignores_internal_fields() {
		let room_version = RoomVersionId::V12;
		let canonical = r#"{
			"auth_events": [],
			"content": {},
			"depth": 2,
			"origin_server_ts": 1,
			"prev_events": [],
			"room_id": "!test:example.org",
			"sender": "@alice:example.org",
			"type": "m.room.message"
		}"#;
		let with_internal_fields = r#"{
			"__rejected": true,
			"event_id": "$ignored:example.org",
			"auth_events": [],
			"content": {},
			"depth": 2,
			"origin_server_ts": 1,
			"prev_events": [],
			"room_id": "!test:example.org",
			"sender": "@alice:example.org",
			"type": "m.room.message"
		}"#;

		let expected = gen_event_id(
			&slipstream::canonical_json::into_object(
				slipstream::canonical_json::from_json_str(canonical)
					.expect("valid canonical JSON"),
			)
			.expect("canonical JSON object"),
			&room_version,
		)
		.expect("event id");
		let actual = gen_event_id_from_bytes(with_internal_fields.as_bytes(), &room_version)
			.expect("event id");

		assert_eq!(actual, expected);
	}
}

use ruma::{CanonicalJsonObject, OwnedEventId, RoomVersionId};
use serde_json::value::RawValue as RawJsonValue;

use crate::{Result, err, utils::pdu_json_canonical_strip};

/// Generates a correct eventId for the incoming pdu.
///
/// Returns a tuple of the new `EventId` and the PDU as a `BTreeMap<String,
/// CanonicalJsonValue>`.
pub fn gen_event_id_canonical_json(
	pdu: &RawJsonValue,
	room_version_id: &RoomVersionId,
) -> Result<(OwnedEventId, CanonicalJsonObject)> {
	let value: CanonicalJsonObject = serde_json::from_str(pdu.get())
		.map_err(|e| err!(BadServerResponse(warn!("Error parsing incoming event: {e:?}"))))?;

	let event_id = gen_event_id(&value, room_version_id)?;

	Ok((event_id, value))
}

/// Generates a correct eventId for the incoming pdu.
pub fn gen_event_id(
	value: &CanonicalJsonObject,
	room_version_id: &RoomVersionId,
) -> Result<OwnedEventId> {
	let reference_hash = ruma::signatures::reference_hash(value, room_version_id)?;
	let event_id: OwnedEventId = format!("${reference_hash}").try_into()?;

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

	let mut value: CanonicalJsonObject = serde_json::from_str(raw_str)
		.map_err(|e| err!(Database("stored PDU is not valid JSON: {e}")))?;
	pdu_json_canonical_strip(&mut value);

	gen_event_id(&value, room_version_id)
}

#[cfg(test)]
mod tests {
	use super::*;

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
			&serde_json::from_str(canonical).expect("valid canonical JSON"),
			&room_version,
		)
		.expect("event id");
		let actual = gen_event_id_from_bytes(with_internal_fields.as_bytes(), &room_version)
			.expect("event id");

		assert_eq!(actual, expected);
	}
}

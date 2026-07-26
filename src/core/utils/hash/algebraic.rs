use rezzy::reconcile::{ElementHash, EventIdFormat, RoomAccumulator};
use ruma::RoomVersionId;

use crate::{Pdu, Result, err};

/// Puts a Matrix room version in MSC0500 event-ID decoding format
#[must_use]
pub fn event_id_format(room_version: &RoomVersionId) -> EventIdFormat {
	match room_version {
		| RoomVersionId::V1 | RoomVersionId::V2 => EventIdFormat::Legacy,
		| RoomVersionId::V3 => EventIdFormat::V3,
		| _ => EventIdFormat::V4Plus,
	}
}

/// Derives the MSC0500 element hash for a stored PDU.
pub fn element_hash_for_pdu(pdu: &Pdu, room_version: &RoomVersionId) -> Result<ElementHash> {
	let format = event_id_format(room_version);
	ElementHash::from_matrix_event_id(pdu.event_id.as_str(), format)
		.map_err(|e| err!("failed to derive algebraic hash for {}: {e:?}", pdu.event_id))
}

/// Builds the MSC0500 level-0 accumulator over a sequence of PDUs.
pub fn accumulator_for_pdus<'a, I>(
	pdus: I,
	room_version: &RoomVersionId,
) -> Result<RoomAccumulator>
where
	I: IntoIterator<Item = &'a Pdu>,
{
	let mut accumulator = RoomAccumulator::new();
	for pdu in pdus {
		let hash = element_hash_for_pdu(pdu, room_version)?;
		accumulator
			.insert(hash)
			.map_err(|e| err!("failed to update algebraic accumulator: {e:?}"))?;
	}
	Ok(accumulator)
}

#[cfg(test)]
mod tests {
	use ruma::{RoomVersionId, UInt};
	use serde_json::{json, value::to_raw_value};

	use super::*;
	use crate::utils::to_canonical_object;

	fn test_pdu(event_id: &str, room_id: &str) -> Pdu {
		let event_id = event_id.try_into().expect("valid event id");
		let room_id = room_id.try_into().expect("valid room id");
		let json = to_canonical_object(json!({
			"room_id": room_id.as_str(),
			"sender": "@alice:example.com",
			"origin_server_ts": 1_234_567_890_u64,
			"type": "m.room.message",
			"content": {"msgtype": "m.text", "body": "hello"},
			"prev_events": [],
			"depth": 1_u64,
			"auth_events": [],
			"redacts": null,
			"hashes": {"sha256": "0123456789abcdef"},
		}))
		.expect("canonical json");

		Pdu::from_id_val(&event_id, json, Some(room_id.as_ref())).expect("valid pdu")
	}

	#[test]
	fn event_id_format_tracks_room_version() {
		assert_eq!(event_id_format(&RoomVersionId::V1), EventIdFormat::Legacy);
		assert_eq!(event_id_format(&RoomVersionId::V2), EventIdFormat::Legacy);
		assert_eq!(event_id_format(&RoomVersionId::V3), EventIdFormat::V3);
		assert_eq!(event_id_format(&RoomVersionId::V11), EventIdFormat::V4Plus);
	}

	#[test]
	fn accumulator_matches_manual_insertion() {
		let room_version = RoomVersionId::V11;
		let first = test_pdu("$a:example.com", "!room:example.com");
		let second = test_pdu("$b:example.com", "!room:example.com");

		let manual = {
			let mut accumulator = RoomAccumulator::new();
			accumulator
				.insert(element_hash_for_pdu(&first, &room_version).expect("hash first"))
				.expect("insert first");
			accumulator
				.insert(element_hash_for_pdu(&second, &room_version).expect("hash second"))
				.expect("insert second");
			accumulator
		};

		let derived =
			accumulator_for_pdus([&first, &second], &room_version).expect("accumulator");

		assert_eq!(derived.digest(), manual.digest());
		assert_eq!(derived.known_event_count(), manual.known_event_count());
	}

	#[test]
	fn element_hash_respects_room_version_format() {
		let pdu = test_pdu("$a:example.com", "!room:example.com");
		let legacy = element_hash_for_pdu(&pdu, &RoomVersionId::V1).expect("legacy hash");
		let modern = element_hash_for_pdu(&pdu, &RoomVersionId::V11).expect("modern hash");

		assert_ne!(legacy, modern);
	}
}

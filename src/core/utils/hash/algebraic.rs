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

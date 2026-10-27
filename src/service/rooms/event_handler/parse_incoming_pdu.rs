use std::str::FromStr;

use conduwuit::{
	Err, Event, Result, err, implement,
	matrix::{event::gen_event_id_canonical_json, pdu::RawJson as RawJsonValue},
};
use itertools::Itertools;
use slipstream::{
	CanonicalJsonObject, CanonicalJsonValue, OwnedEventId, OwnedRoomId, RoomVersionId,
};

type Parsed = (OwnedRoomId, OwnedEventId, CanonicalJsonObject);

const MAX_AUTH_EVENTS_ROOM_ID_FALLBACK: usize = 10;

/// Parses every entry in an array as an event ID, returning an error if any
/// step fails.
fn expect_event_id_array(value: &CanonicalJsonObject, field: &str) -> Result<Vec<OwnedEventId>> {
	value
		.get(field)
		.ok_or_else(|| err!(Request(BadJson("missing field `{field}` on PDU"))))?
		.as_array()
		.ok_or_else(|| err!(Request(BadJson("expected an array PDU field `{field}`"))))?
		.iter()
		.map(|v| {
			v.as_str()
				.ok_or_else(|| {
					err!(Request(BadJson("expected an array of event IDs for `{field}`")))
				})
				.and_then(|s| {
					OwnedEventId::parse(s)
						.map_err(|e| err!(Request(BadJson("invalid event ID in `{field}`: {e}"))))
				})
		})
		.try_collect()
}

/// Performs some basic validation on the PDU to make sure it's not obviously
/// malformed. This is not a full validation, but guards against extreme errors.
///
/// Currently, this just validates that prev/auth events are within acceptable
/// ranges. Other servers do some additional things like checking depth range,
/// but serde will do that later when converting the object to a PduEvent.
#[implement(super::Service)]
pub fn validate_pdu(&self, pdu: &CanonicalJsonObject) -> Result {
	// Since v3:
	// `event_id` should not be present on the PDU.
	// NOTE: The above is ignored since technically it's still allowed to be
	// included, but should be ignored instead.
	// `auth_events` and `prev_events` must be an array of event IDs
	let auth_events = expect_event_id_array(pdu, "auth_events")?;
	if auth_events.len() > 10 {
		return Err!(Request(BadJson("PDU has too many auth events")));
	}
	let prev_events = expect_event_id_array(pdu, "prev_events")?;
	if prev_events.len() > 20 {
		return Err!(Request(BadJson("PDU has too many prev events")));
	}
	Ok(())
}

/// Best-effort room version for a room this server has no state for, for
/// example one where it only holds an invite (the PDU may be a rescind).
///
/// The invite's stripped state includes the create event, which names the
/// version. Failing that, only room versions 1 and 2 carry their own
/// `event_id` on the wire, so its presence says which family the event is in.
#[implement(super::Service)]
async fn room_version_without_room_state(
	&self,
	room_id: &slipstream::RoomId,
	pdu: &CanonicalJsonObject,
) -> RoomVersionId {
	let invitee = pdu
		.get("state_key")
		.and_then(CanonicalJsonValue::as_str)
		.and_then(|state_key| slipstream::OwnedUserId::parse(state_key).ok());

	if let Some(invitee) = invitee {
		if let Ok(invite_state) = self
			.services
			.state_cache
			.invite_state(&invitee, room_id)
			.await
		{
			for event in invite_state {
				let is_create = event
					.get_field::<String>("type")
					.ok()
					.flatten()
					.is_some_and(|kind| kind == "m.room.create");
				if !is_create {
					continue;
				}
				let version = event
					.get_field::<slipstream::json::Value>("content")
					.ok()
					.flatten()
					.and_then(|content| {
						content
							.get("room_version")
							.and_then(slipstream::json::Value::as_str)
							.map(str::to_owned)
					})
					// An omitted `room_version` means version 1.
					.map_or(Some(RoomVersionId::V1), |version| {
						RoomVersionId::from_str(&version).ok()
					});
				if let Some(version) = version {
					return version;
				}
			}
		}
	}

	if pdu.contains_key("event_id") {
		RoomVersionId::V1
	} else {
		// Hash-derived event IDs; the newest widely used format.
		RoomVersionId::V11
	}
}

/// Return the room ID, derived event ID, and canonical PDU after basic
/// prev/auth-event validation. Create events select their own room version,
/// defaulting to version 1 when `room_version` is absent or not a string. Other
/// events use the stored version or a best-effort fallback if lookup fails.
///
/// If room-ID extraction fails, try at most ten auth events for a known room.
/// Returns an error for invalid canonical JSON, unresolved room IDs, invalid
/// create content or versions, and failed event-ID generation or basic validation.
#[implement(super::Service)]
pub async fn parse_incoming_pdu(&self, pdu: &RawJsonValue) -> Result<Parsed> {
	let value = slipstream::canonical_json::into_object(
		slipstream::canonical_json::from_json_str(pdu.get()).map_err(|_| {
			err!(BadServerResponse(debug_warn!("Incoming event is not valid canonical JSON")))
		})?,
	)
	.ok_or_else(|| err!(Request(BadJson("Incoming event must be a JSON object"))))?;
	let event_type = value
		.get("type")
		.and_then(CanonicalJsonValue::as_str)
		.ok_or_else(|| err!(Request(InvalidParam("Missing or invalid type in pdu"))))?;

	let room_id = match conduwuit::matrix::event::room_id_from_pdu(&value, None) {
		| Ok(room_id) => room_id,
		| Err(_) => {
			// V12 non-create event without room_id
			// Try to find it from auth_events, but do not allow untrusted input to
			// trigger an unbounded number of sequential DB lookups.
			let auth_events = value
				.get("auth_events")
				.and_then(|v| v.as_array())
				.ok_or_else(|| err!(Request(InvalidParam("Missing room_id in PDU"))))?;

			let mut found_room_id = None;
			for auth_event_id in auth_events.iter().take(MAX_AUTH_EVENTS_ROOM_ID_FALLBACK) {
				if let Some(auth_event_id) = auth_event_id.as_str() {
					if let Ok(auth_event_id) = OwnedEventId::parse(auth_event_id) {
						if let Ok(pdu) = self.services.timeline.get_pdu(&auth_event_id) {
							if let Some(room_id) = pdu.room_id_or_hash() {
								found_room_id = Some(room_id);
								break;
							}
						}
					}
				}
			}

			found_room_id.ok_or_else(|| err!(Request(InvalidParam("Missing room_id in PDU"))))?
		},
	};

	let room_version_id = if event_type == "m.room.create" {
		let content = value
			.get("content")
			.and_then(CanonicalJsonValue::as_object)
			.ok_or_else(|| err!(Request(BadJson("Missing or invalid create-event content"))))?;
		RoomVersionId::from_str(
			content
				.get("room_version")
				.and_then(CanonicalJsonValue::as_str)
				.unwrap_or("1"),
		)
		.map_err(|error| {
			err!(Request(BadJson("Invalid room_version in create event: {error}")))
		})?
	} else {
		match self.services.state.get_room_version(&room_id).await {
			| Ok(room_version_id) => room_version_id,
			| Err(_) => self.room_version_without_room_state(&room_id, &value).await,
		}
	};
	// The underlying error can quote peer-supplied content, so it is not kept.
	let (event_id, value) = gen_event_id_canonical_json(pdu, &room_version_id).map_err(|_| {
		err!(Request(InvalidParam(warn!("Could not convert event to canonical json"))))
	})?;
	self.validate_pdu(&value)?;
	Ok((room_id, event_id, value))
}

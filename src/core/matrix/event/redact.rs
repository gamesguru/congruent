use serde::Deserialize;
use serde_json::value::RawValue as RawJsonValue;
use slipstream::{OwnedEventId, RoomVersionId, events::TimelineEventType};

use super::Event;

/// Copies the `redacts` property of the event to the `content` dict and
/// vice-versa.
///
/// This follows the specification's
/// [recommendation](https://spec.matrix.org/v1.10/rooms/v11/#moving-the-redacts-property-of-mroomredaction-events-to-a-content-property):
///
/// > For backwards-compatibility with older clients, servers should add a
/// > redacts property to the top level of m.room.redaction events in when
/// > serving such events over the Client-Server API.
///
/// > For improved compatibility with newer clients, servers should add a
/// > redacts property to the content of m.room.redaction events in older
/// > room versions when serving such events over the Client-Server API.
#[must_use]
pub(super) fn copy<E: Event>(event: &E) -> (Option<OwnedEventId>, Box<RawJsonValue>) {
	if *event.event_type() != TimelineEventType::RoomRedaction {
		return (event.redacts().map(ToOwned::to_owned), event.content().to_owned());
	}

	let Ok(value) = slipstream::canonical_json::from_json_str(event.content().get()) else {
		return (event.redacts().map(ToOwned::to_owned), event.content().to_owned());
	};
	let Some(mut content) = slipstream::canonical_json::into_object(value) else {
		return (event.redacts().map(ToOwned::to_owned), event.content().to_owned());
	};

	if let Some(redacts) = content.get("redacts").and_then(|value| value.as_str()) {
		return (Some(OwnedEventId::from(redacts)), event.content().to_owned());
	}

	if let Some(redacts) = event.redacts().map(ToOwned::to_owned) {
		content.insert(
			"redacts".to_owned(),
			slipstream::canonical_json::Value::String(redacts.to_string()),
		);
		return (
			event.redacts().map(ToOwned::to_owned),
			RawJsonValue::from_string(slipstream::codec::to_string(
				&slipstream::canonical_json::Value::Object(content),
			))
			.expect("Must be valid, we only added redacts field"),
		);
	}

	(event.redacts().map(ToOwned::to_owned), event.content().to_owned())
}

#[must_use]
pub(super) fn is_redacted<E: Event>(event: &E) -> bool {
	let Some(unsigned) = event.unsigned() else {
		return false;
	};

	let Ok(unsigned) = ExtractRedactedBecause::deserialize(unsigned) else {
		return false;
	};

	unsigned.redacted_because.is_some()
}

#[must_use]
pub(super) fn redacts_id<E: Event>(
	event: &E,
	room_version: &RoomVersionId,
) -> Option<OwnedEventId> {
	use RoomVersionId::*;

	if *event.kind() != TimelineEventType::RoomRedaction {
		return None;
	}

	match *room_version {
		| V1 | V2 | V3 | V4 | V5 | V6 | V7 | V8 | V9 | V10 =>
			event.redacts().map(ToOwned::to_owned),
		| _ if crate::info::room_version::is_msc3389(room_version) =>
			event.redacts().map(ToOwned::to_owned),
		| _ => event.redacts().map(ToOwned::to_owned),
	}
}

#[derive(Deserialize)]
struct ExtractRedactedBecause {
	redacted_because: Option<serde::de::IgnoredAny>,
}

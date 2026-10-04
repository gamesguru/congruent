use serde_json::{Value as JsonValue, json, value::to_raw_value};
use slipstream::{RoomVersionId, canonical_json::redact_content_in_place};

use crate::{Error, Result, err, implement};

#[implement(super::Pdu)]
pub fn redact(&mut self, room_version_id: &RoomVersionId, reason: JsonValue) -> Result {
	self.unsigned = None;

	let content = slipstream::canonical_json::from_json_str(self.content.get())
		.map_err(|e| err!(Request(BadJson("Failed to deserialize content into type: {e}"))))?;
	let mut content = slipstream::canonical_json::into_object(content)
		.ok_or_else(|| err!(Request(BadJson("event content must be an object"))))?;

	redact_content_in_place(&mut content, room_version_id, self.kind.to_string())
		.map_err(|e| Error::Redaction(self.sender.server_name().to_owned(), e))?;

	let reason = slipstream::canonical_json::from_json_str(&reason.to_string())
		.expect("Failed to preserialize reason");

	let mut redacted_because = slipstream::canonical_json::Object::new();
	redacted_because.insert("redacted_because".to_owned(), reason);
	self.unsigned = serde_json::value::RawValue::from_string(slipstream::codec::to_string(
		&slipstream::canonical_json::Value::Object(redacted_because),
	))
	.expect("Failed to serialize unsigned")
	.into();

	self.content = serde_json::value::RawValue::from_string(slipstream::codec::to_string(
		&slipstream::canonical_json::Value::Object(content),
	))
	.expect("Failed to serialize content");

	Ok(())
}

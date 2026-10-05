use slipstream::{RoomVersionId, canonical_json::redact_content_in_place};

use crate::{Error, Result, err, implement};

#[implement(super::Pdu)]
pub fn redact(&mut self, room_version_id: &RoomVersionId, reason: &slipstream::json::Value) -> Result {
	self.unsigned = None;

	let content =
		slipstream::codec::from_str::<slipstream::canonical_json::Value>(self.content.get())
			.map_err(|e| {
				err!(Request(BadJson("Failed to deserialize content into type: {e}")))
			})?;
	let mut content = slipstream::canonical_json::into_object(content)
		.ok_or_else(|| err!(Request(BadJson("event content must be an object"))))?;

	redact_content_in_place(&mut content, room_version_id, &self.kind)
		.map_err(|e| Error::Redaction(self.sender.server_name(), e))?;

	let reason =
		slipstream::codec::from_str::<slipstream::canonical_json::Value>(&slipstream::codec::to_string(reason))
			.expect("Failed to preserialize reason");

	let mut redacted_because = slipstream::canonical_json::Object::new();
	redacted_because.insert("redacted_because".to_owned(), reason);
	self.unsigned = Some(super::RawJson::from_value(&slipstream::canonical_json::Value::Object(
		redacted_because,
	)));

	self.content =
		super::RawJson::from_value(&slipstream::canonical_json::Value::Object(content));

	Ok(())
}

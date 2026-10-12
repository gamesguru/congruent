use slipstream::json::Value as JsonValue;

use super::Event;

#[must_use]
pub(super) fn get_unsigned_as_value<E>(event: &E) -> JsonValue
where
	E: Event,
{
	event
		.unsigned()
		.as_ref()
		.and_then(|raw| slipstream::codec::from_str(raw.get()).ok())
		.unwrap_or_default()
}

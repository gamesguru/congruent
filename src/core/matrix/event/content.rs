use slipstream::json::Value as JsonValue;

use super::Event;
use crate::{Result, err};

#[inline]
#[must_use]
pub(super) fn as_value<E: Event>(event: &E) -> JsonValue {
	slipstream::codec::from_str(event.content().get())
		.expect("Failed to represent Event content as JsonValue")
}

#[inline]
pub(super) fn get_codec<T, E>(event: &E) -> Result<T>
where
	T: slipstream::codec::Deserialize,
	E: Event,
{
	slipstream::serde::Raw::<()>::from_json_text(event.content().get())
		.and_then(|raw| raw.deserialize_as::<T>())
		.map_err(|e| err!(Request(BadJson("Failed to deserialize content into type: {e}"))))
}

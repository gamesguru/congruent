use bytes::Bytes;
use http_body_util::Full;
use slipstream::codec::Serialize;

use crate::router::response::Response;

pub(crate) fn single_field<T: Serialize + ?Sized>(
	key: &str,
	value: &T,
) -> slipstream::json::Value {
	let mut object = slipstream::ObjectBuilder::new();
	object.field(key, value);
	object.finish()
}

/// Event content must be a JSON object. A request body that is any other JSON
/// value (a string, number, array, ...) is rejected with `M_BAD_JSON`.
pub(crate) fn require_object_content<T>(
	content: &slipstream::sswire::Raw<T>,
) -> conduwuit::Result<()> {
	// Raw event endpoints otherwise bypass the canonical-JSON parser used by
	// normal event decoding. Validate here so NaN/Infinity, fractional numbers,
	// and integers outside Matrix's exactly-representable range are rejected
	// consistently with federation and state-event handling.
	let value = slipstream::canonical_json::from_json_str(content.get())
		.map_err(|e| conduwuit::err!(Request(BadJson("Invalid event content: {e}"))))?;
	if value.as_object().is_none() {
		return conduwuit::Err!(Request(BadJson("Event content must be a JSON object")));
	}
	Ok(())
}

pub(crate) fn empty_events() -> slipstream::json::Value {
	single_field("events", &Vec::<slipstream::json::Value>::new())
}

pub(crate) fn json_response(value: slipstream::json::Value) -> Response {
	let body = slipstream::codec::to_string(&value);
	// Release the JSON tree before constructing the response body to reduce peak memory use.
	drop(value);
	http::Response::builder()
		.header(http::header::CONTENT_TYPE, "application/json")
		.body(Full::new(Bytes::from(body)))
		.expect("static JSON response builder is valid")
}

#[cfg(test)]
mod tests {
	#[test]
	fn single_field_preserves_null() {
		let value = super::single_field("profile_updates", &slipstream::json::Value::Null);
		assert_eq!(slipstream::codec::to_string(&value), r#"{"profile_updates":null}"#);
	}
}

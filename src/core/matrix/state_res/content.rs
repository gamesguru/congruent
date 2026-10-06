//! Helpers for reading the handful of fields state resolution needs from event
//! content, without a serde dependency.
//!
//! These preserve the behaviour of the serde structs they replace: content
//! must be a JSON object, optional fields treat `null` like absence, and a
//! present field of the wrong type is an error.

use slipstream::{
	codec::{self, DeError},
	json::Value,
};

/// Parses `content` and requires it to be a JSON object.
pub(super) fn object(content: &str) -> Result<Value, DeError> {
	let value = Value::parse(content).map_err(|e| DeError(e.to_string()))?;
	if value.as_object().is_some() {
		Ok(value)
	} else {
		Err(DeError::expected("object"))
	}
}

/// A field that may be absent or `null`.
pub(super) fn optional<'a>(object: &'a Value, key: &str) -> Option<&'a Value> {
	object.get(key).filter(|value| !value.is_null())
}

/// A field that must be present.
pub(super) fn required<'a>(object: &'a Value, key: &str) -> Result<&'a Value, DeError> {
	object
		.get(key)
		.ok_or_else(|| DeError(format!("missing field `{key}`")))
}

pub(super) fn decode<T: codec::Deserialize>(value: &Value) -> Result<T, DeError> {
	codec::from_value(value)
}

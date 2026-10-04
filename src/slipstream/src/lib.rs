//! The local Matrix compatibility surface.

pub use mtx_slipstream::*;

/// Slipstream's serde-free value codec.
pub mod codec {
	pub use mtx_slipstream::codec::*;
}

/// Canonical JSON entry points used at the remaining serde boundary.
pub mod canonical_json {
	pub use mtx_slipstream::canonical_json::*;

	pub fn from_json_str(input: &str) -> Result<Value, mtx_slipstream::codec::DeError> {
		Value::parse(input).map_err(|error| mtx_slipstream::codec::DeError(error.to_string()))
	}

	pub fn into_object(value: Value) -> Option<Object> {
		match value {
			| Value::Object(object) => Some(object),
			| _ => None,
		}
	}
}

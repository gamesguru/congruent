//! The local Matrix compatibility surface.

pub use mtx_slipstream::*;

/// Slipstream's serde-free value codec.
pub mod codec {
	pub use mtx_slipstream::codec::*;
}

/// Small, macro-free builder for JSON objects backed by Slipstream's codec.
pub struct ObjectBuilder {
	object: json::Object,
}

impl ObjectBuilder {
	#[must_use]
	pub fn new() -> Self { Self { object: json::Object::new() } }

	pub fn field<T: codec::Serialize + ?Sized>(&mut self, key: &str, value: &T) {
		self.object.insert(key.to_owned(), codec::to_value(value));
	}

	pub fn field_opt<T: codec::Serialize + ?Sized>(&mut self, key: &str, value: Option<&T>) {
		if let Some(value) = value {
			self.field(key, value);
		}
	}

	#[must_use]
	pub fn finish(self) -> json::Value { json::Value::Object(self.object) }
}

impl Default for ObjectBuilder {
	fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
	use super::ObjectBuilder;

	#[test]
	fn object_builder_omits_none_and_preserves_codec_values() {
		let mut object = ObjectBuilder::new();
		object.field("z", &3_u64);
		object.field_opt("missing", None::<&u64>);
		object.field("a", &"value");

		assert_eq!(crate::codec::to_string(&object.finish()), r#"{"a":"value","z":3}"#);
	}
}

/// Canonical JSON entry points used at the remaining serde boundary.
pub mod canonical_json {
	pub use mtx_slipstream::canonical_json::*;

	pub fn from_json_str(input: &str) -> Result<Value, mtx_slipstream::codec::DeError> {
		Value::parse(input).map_err(|error| mtx_slipstream::codec::DeError(error.to_string()))
	}

	#[must_use]
	pub fn into_object(value: Value) -> Option<Object> {
		match value {
			| Value::Object(object) => Some(object),
			| _ => None,
		}
	}
}

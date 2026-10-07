//! The local Matrix compatibility surface.

pub use mtx_slipstream::*;

/// Builds a Slipstream JSON value from JSON-like literal syntax.
#[macro_export]
macro_rules! json {
	({ "users": { ($key:expr): $value:expr $(,)? } }) => {{
		let mut users = rezzy::json::Object::new();
		users.insert(($key).to_string(), rezzy::json!($value));
		let mut object = rezzy::json::Object::new();
		object.insert("users".to_owned(), rezzy::json::Value::Object(users));
		rezzy::json::Value::Object(object)
	}};
	({ "users": { ($key1:expr): $value1:expr, ($key2:expr): $value2:expr $(,)? } }) => {{
		let mut users = rezzy::json::Object::new();
		users.insert(($key1).to_string(), rezzy::json!($value1));
		users.insert(($key2).to_string(), rezzy::json!($value2));
		let mut object = rezzy::json::Object::new();
		object.insert("users".to_owned(), rezzy::json::Value::Object(users));
		rezzy::json::Value::Object(object)
	}};
	({ "users": { ($key1:expr): $value1:expr, ($key2:expr): $value2:expr, ($key3:expr): $value3:expr $(,)? } }) => {{
		let mut users = rezzy::json::Object::new();
		users.insert(($key1).to_string(), rezzy::json!($value1));
		users.insert(($key2).to_string(), rezzy::json!($value2));
		users.insert(($key3).to_string(), rezzy::json!($value3));
		let mut object = rezzy::json::Object::new();
		object.insert("users".to_owned(), rezzy::json::Value::Object(users));
		rezzy::json::Value::Object(object)
	}};
	($($tokens:tt)*) => { rezzy::json!($($tokens)*) };
}

/// Slipstream's serde-free value codec.
pub mod codec {
	pub use mtx_slipstream::codec::*;
}

/// Slipstream's wire-format types and serde bridge.
pub mod sswire {
	pub use mtx_slipstream::sswire::*;
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

	/// The largest integer Matrix canonical JSON allows: 2^53 - 1.
	const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

	/// Parses JSON, then enforces the Matrix canonical JSON restrictions that a
	/// plain JSON parser does not: no floats, and integers only within
	/// +/-(2^53 - 1). Non-finite literals (`NaN`, `Infinity`) are not integers
	/// either, so they are rejected here too.
	///
	/// # Errors
	///
	/// Returns an error if the input is not valid canonical JSON.
	pub fn from_json_str(input: &str) -> Result<Value, mtx_slipstream::codec::DeError> {
		let value = Value::parse(input)
			.map_err(|error| mtx_slipstream::codec::DeError(error.to_string()))?;
		validate_canonical(&value)?;
		Ok(value)
	}

	fn validate_canonical(value: &Value) -> Result<(), mtx_slipstream::codec::DeError> {
		match value {
			| Value::Number(number) => match number.as_i64() {
				| Some(integer) if (-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&integer) =>
					Ok(()),
				| _ => Err(mtx_slipstream::codec::DeError(
					"number is not a canonical JSON integer within +/-(2^53 - 1)".to_owned(),
				)),
			},
			| Value::Array(items) => items.iter().try_for_each(validate_canonical),
			| Value::Object(object) => object.values().try_for_each(validate_canonical),
			| _ => Ok(()),
		}
	}

	#[must_use]
	pub fn into_object(value: Value) -> Option<Object> {
		match value {
			| Value::Object(object) => Some(object),
			| _ => None,
		}
	}
}

#[cfg(test)]
mod canonical_json_tests {
	use super::canonical_json::from_json_str;

	#[test]
	fn rejects_what_matrix_canonical_json_forbids() {
		for bad in [
			r#"{"body": 9007199254740992}"#,
			r#"{"body": -9007199254740992}"#,
			r#"{"body": 1.1}"#,
			r#"{"body": 1e3}"#,
			r#"{"nested": [{"deep": 18446744073709551615}]}"#,
			r#"{"body": Infinity}"#,
			r#"{"body": NaN}"#,
		] {
			assert!(from_json_str(bad).is_err(), "{bad} must be rejected");
		}
	}

	#[test]
	fn accepts_integers_up_to_two_to_the_53_minus_one() {
		for good in [
			r#"{"a": 9007199254740991, "b": -9007199254740991, "c": 0, "d": [1, 2]}"#,
			r#"{"s": "1.1", "t": true, "n": null}"#,
		] {
			assert!(from_json_str(good).is_ok(), "{good} must parse");
		}
	}
}

use std::{fmt, marker::PhantomData, str::FromStr};

use slipstream::{CanonicalJsonError, CanonicalJsonObject};

use crate::Result;

pub trait OwnedEventType: Sized {
	#[must_use]
	fn owned_event_type(&self) -> Self;
}

impl OwnedEventType for slipstream::events::StateEventType {
	fn owned_event_type(&self) -> Self { Self::from(self.as_str()) }
}

impl OwnedEventType for slipstream::events::TimelineEventType {
	fn owned_event_type(&self) -> Self { Self::from(self.as_str()) }
}

/// Clone a Slipstream raw JSON value without requiring `Raw<T>: Clone`.
#[must_use]
pub fn clone_raw<T>(raw: &slipstream::sswire::Raw<T>) -> slipstream::sswire::Raw<T> {
	slipstream::sswire::Raw(raw.0.clone(), PhantomData)
}

/// Fallible conversion from any value that implements Slipstream's `Serialize` to a
/// `CanonicalJsonObject`.
///
/// `value` must serialize to a JSON object.
pub fn to_canonical_object<T: slipstream::codec::Serialize>(
	value: T,
) -> Result<CanonicalJsonObject, CanonicalJsonError> {
	use CanonicalJsonError::SerDe;

	let encoded = slipstream::codec::to_string(&value);
	let value =
		slipstream::canonical_json::from_json_str(&encoded).map_err(|e| SerDe(e.to_string()))?;
	slipstream::canonical_json::into_object(value)
		.ok_or_else(|| SerDe("serialized value was not an object".to_owned()))
}

pub fn deserialize_from_str<'de, D, T, E>(deserializer: D) -> Result<T, D::Error>
where
	D: serde::de::Deserializer<'de>,
	T: FromStr<Err = E>,
	E: fmt::Display,
{
	struct Visitor<T: FromStr<Err = E>, E>(PhantomData<T>);

	impl<T, Err> serde::de::Visitor<'_> for Visitor<T, Err>
	where
		T: FromStr<Err = Err>,
		Err: fmt::Display,
	{
		type Value = T;

		fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
			write!(formatter, "a parsable string")
		}

		fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
		where
			E: serde::de::Error,
		{
			v.parse().map_err(serde::de::Error::custom)
		}
	}

	deserializer.deserialize_str(Visitor(PhantomData))
}

/// A Slipstream JSON value read through a serde `Deserializer`.
///
/// For the few boundaries (configuration, YAML) where a serde deserializer is
/// mandated by the format crate, this carries the data into Slipstream's value
/// model so it can be decoded with the codec.
pub struct SerdeValue(pub slipstream::json::Value);

impl<'de> serde::Deserialize<'de> for SerdeValue {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		deserializer.deserialize_any(ValueVisitor).map(Self)
	}
}

struct ValueVisitor;

impl<'de> serde::de::Visitor<'de> for ValueVisitor {
	type Value = slipstream::json::Value;

	fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str("any JSON-compatible value")
	}

	fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> { Ok(v.into()) }

	fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> { Ok(v.into()) }

	fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> { Ok(v.into()) }

	fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Self::Value, E> {
		slipstream::json::Number::from_f64(v)
			.map(slipstream::json::Value::Number)
			.ok_or_else(|| E::custom("non-finite number"))
	}

	fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> { Ok(v.into()) }

	fn visit_string<E>(self, v: String) -> Result<Self::Value, E> { Ok(v.into()) }

	fn visit_none<E>(self) -> Result<Self::Value, E> { Ok(slipstream::json::Value::Null) }

	fn visit_unit<E>(self) -> Result<Self::Value, E> { Ok(slipstream::json::Value::Null) }

	fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		serde::Deserialize::deserialize(deserializer).map(|SerdeValue(value)| value)
	}

	fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
	where
		A: serde::de::SeqAccess<'de>,
	{
		let mut values = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(4096));
		while let Some(SerdeValue(value)) = seq.next_element()? {
			values.push(value);
		}

		Ok(slipstream::json::Value::Array(values))
	}

	fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
	where
		A: serde::de::MapAccess<'de>,
	{
		let mut object = slipstream::json::Object::new();
		while let Some((key, SerdeValue(value))) = map.next_entry::<String, SerdeValue>()? {
			object.insert(key, value);
		}

		Ok(slipstream::json::Value::Object(object))
	}
}

/// Writes a Slipstream JSON value through a serde `Serializer`.
pub struct SerdeValueRef<'a>(pub &'a slipstream::json::Value);

impl serde::Serialize for SerdeValueRef<'_> {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		use serde::ser::{SerializeMap, SerializeSeq};
		use slipstream::json::Value;

		match self.0 {
			| Value::Null => serializer.serialize_unit(),
			| Value::Bool(v) => serializer.serialize_bool(*v),
			| Value::Number(_) =>
				if let Some(v) = self.0.as_i64() {
					serializer.serialize_i64(v)
				} else if let Some(v) = self.0.as_u64() {
					serializer.serialize_u64(v)
				} else if let Some(v) = self.0.as_f64() {
					serializer.serialize_f64(v)
				} else {
					Err(serde::ser::Error::custom("unrepresentable number"))
				},
			| Value::String(v) => serializer.serialize_str(v),
			| Value::Array(values) => {
				let mut seq = serializer.serialize_seq(Some(values.len()))?;
				for value in values {
					seq.serialize_element(&SerdeValueRef(value))?;
				}
				seq.end()
			},
			| Value::Object(object) => {
				let mut map = serializer.serialize_map(Some(object.len()))?;
				for (key, value) in object {
					map.serialize_entry(key, &SerdeValueRef(value))?;
				}
				map.end()
			},
		}
	}
}

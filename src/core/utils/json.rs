use std::{fmt, marker::PhantomData, str::FromStr};

use slipstream::{CanonicalJsonError, CanonicalJsonObject};

use crate::Result;

pub trait OwnedEventType: Sized {
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
pub fn clone_raw<T>(raw: &slipstream::serde::Raw<T>) -> slipstream::serde::Raw<T> {
	slipstream::serde::Raw(raw.0.clone(), PhantomData)
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

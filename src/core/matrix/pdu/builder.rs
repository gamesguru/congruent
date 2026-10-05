use std::collections::BTreeMap;

use serde::Deserialize;
use slipstream::{
	MilliSecondsSinceUnixEpoch, OwnedEventId,
	events::{EventContent, MessageLikeEventType, StateEventType, TimelineEventType},
};

use super::{RawJson, StateKey};

fn deserialize_codec<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
	D: serde::Deserializer<'de>,
	T: slipstream::codec::Deserialize,
{
	let value = serde_json::Value::deserialize(deserializer)?;
	slipstream::codec::from_str(&value.to_string()).map_err(serde::de::Error::custom)
}

fn deserialize_codec_opt<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
	D: serde::Deserializer<'de>,
	T: slipstream::codec::Deserialize,
{
	let value = Option::<serde_json::Value>::deserialize(deserializer)?;
	value
		.map(|value| {
			slipstream::codec::from_str(&value.to_string()).map_err(serde::de::Error::custom)
		})
		.transpose()
}

fn deserialize_raw<'de, D>(deserializer: D) -> Result<RawJson, D::Error>
where
	D: serde::Deserializer<'de>,
{
	let value = serde_json::Value::deserialize(deserializer)?;
	RawJson::from_json_string(value.to_string()).map_err(serde::de::Error::custom)
}

/// Build the start of a PDU in order to add it to the Database.
#[derive(Debug, Deserialize)]
pub struct Builder {
	#[serde(rename = "type")]
	#[serde(deserialize_with = "deserialize_codec")]
	pub event_type: TimelineEventType,

	#[serde(deserialize_with = "deserialize_raw")]
	pub content: RawJson,

	#[serde(deserialize_with = "deserialize_unsigned")]
	pub unsigned: Option<Unsigned>,

	pub state_key: Option<StateKey>,

	#[serde(deserialize_with = "deserialize_codec_opt")]
	pub redacts: Option<OwnedEventId>,

	/// For timestamped messaging, should only be used for appservices.
	/// Will be set to current time if None
	#[serde(deserialize_with = "deserialize_codec_opt")]
	pub timestamp: Option<MilliSecondsSinceUnixEpoch>,
}

type Unsigned = BTreeMap<String, slipstream::json::Value>;

fn deserialize_unsigned<'de, D>(deserializer: D) -> Result<Option<Unsigned>, D::Error>
where
	D: serde::Deserializer<'de>,
{
	let value = Option::<serde_json::Value>::deserialize(deserializer)?;
	value
		.map(|value| {
			slipstream::codec::from_str(&value.to_string()).map_err(serde::de::Error::custom)
		})
		.transpose()
}

impl Builder {
	pub fn state<S, T>(state_key: S, content: &T) -> Self
	where
		T: EventContent<EventType = StateEventType> + slipstream::codec::Serialize,
		S: Into<StateKey>,
	{
		Self {
			event_type: content.event_type().into(),
			content: RawJson::from_value(content),
			state_key: Some(state_key.into()),
			..Self::default()
		}
	}

	pub fn timeline<T>(content: &T) -> Self
	where
		T: EventContent<EventType = MessageLikeEventType> + slipstream::codec::Serialize,
	{
		Self {
			event_type: content.event_type().into(),
			content: RawJson::from_value(content),
			..Self::default()
		}
	}
}

impl Default for Builder {
	fn default() -> Self {
		Self {
			event_type: "m.room.message".into(),
			content: RawJson::empty_object(),
			unsigned: None,
			state_key: None,
			redacts: None,
			timestamp: None,
		}
	}
}

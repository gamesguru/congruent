use std::collections::BTreeMap;

use slipstream::{
	MilliSecondsSinceUnixEpoch, OwnedEventId,
	events::{EventContent, MessageLikeEventType, StateEventType, TimelineEventType},
};

use super::{RawJson, StateKey};

/// Build the start of a PDU in order to add it to the Database.
#[derive(Debug)]
pub struct Builder {
	pub event_type: TimelineEventType,

	pub content: RawJson,

	pub unsigned: Option<Unsigned>,

	pub state_key: Option<StateKey>,

	pub redacts: Option<OwnedEventId>,

	/// For timestamped messaging, should only be used for appservices.
	/// Will be set to current time if None
	pub timestamp: Option<MilliSecondsSinceUnixEpoch>,
}

type Unsigned = BTreeMap<String, slipstream::json::Value>;

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

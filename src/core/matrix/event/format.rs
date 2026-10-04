use serde_json::json;
use slipstream::{
	OwnedRoomId,
	events::{
		AnyMessageLikeEvent, AnyStateEvent, AnyStrippedStateEvent, AnySyncStateEvent,
		AnySyncTimelineEvent, AnyTimelineEvent, StateEvent, room::member::RoomMemberEventContent,
		space::child::HierarchySpaceChildEvent,
	},
	serde::Raw,
};

use super::{Event, redact};

pub struct Owned<E: Event>(pub(super) E);

pub struct Ref<'a, E: Event>(pub(super) &'a E);

impl<E: Event> From<Owned<E>> for Raw<AnySyncTimelineEvent> {
	fn from(event: Owned<E>) -> Self { Ref(&event.0).into() }
}

impl<'a, E: Event> From<Ref<'a, E>> for Raw<AnySyncTimelineEvent> {
	fn from(event: Ref<'a, E>) -> Self {
		let event = event.0;
		let (redacts, content) = redact::copy(event);
		let mut json = json!({
			"content": content,
			"event_id": event.event_id().as_str(),
			"origin_server_ts": event.origin_server_ts().0,
			"sender": event.sender().as_str(),
			"type": event.event_type().to_string(),
		});

		if let Some(redacts) = redacts {
			json["redacts"] = json!(redacts.as_str());
		}
		if let Some(state_key) = event.state_key() {
			json["state_key"] = json!(state_key);
		}

		if let Some(unsigned) = event.unsigned() {
			json["unsigned"] = json!(unsigned);
		}

		Self::from_json_text(&json.to_string()).expect("Failed to serialize Event value")
	}
}

impl<E: Event> From<Owned<E>> for Raw<AnyTimelineEvent> {
	fn from(event: Owned<E>) -> Self { Ref(&event.0).into() }
}

impl<'a, E: Event> From<Ref<'a, E>> for Raw<AnyTimelineEvent> {
	fn from(event: Ref<'a, E>) -> Self {
		let event = event.0;
		let (redacts, content) = redact::copy(event);
		let mut json = json!({
			"content": content,
			"event_id": event.event_id().as_str(),
			"origin_server_ts": event.origin_server_ts().0,
			"room_id": event.room_id_or_hash().as_ref().map(OwnedRoomId::as_str),
			"sender": event.sender().as_str(),
			"type": event.kind().to_string(),
		});

		if let Some(redacts) = redacts {
			json["redacts"] = json!(redacts.as_str());
		}
		if let Some(state_key) = event.state_key() {
			json["state_key"] = json!(state_key);
		}
		if let Some(unsigned) = event.unsigned() {
			json["unsigned"] = json!(unsigned);
		}

		Self::from_json_text(&json.to_string()).expect("Failed to serialize Event value")
	}
}

impl<E: Event> From<Owned<E>> for Raw<AnyMessageLikeEvent> {
	fn from(event: Owned<E>) -> Self { Ref(&event.0).into() }
}

impl<'a, E: Event> From<Ref<'a, E>> for Raw<AnyMessageLikeEvent> {
	fn from(event: Ref<'a, E>) -> Self {
		let event = event.0;
		let (redacts, content) = redact::copy(event);
		let mut json = json!({
			"content": content,
			"event_id": event.event_id().as_str(),
			"origin_server_ts": event.origin_server_ts().0,
			"room_id": event.room_id().map(slipstream::OwnedRoomId::as_str),
			"sender": event.sender().as_str(),
			"type": event.kind().to_string(),
		});

		if let Some(redacts) = &redacts {
			json["redacts"] = json!(redacts.as_str());
		}
		if let Some(state_key) = event.state_key() {
			json["state_key"] = json!(state_key);
		}
		if let Some(unsigned) = event.unsigned() {
			json["unsigned"] = json!(unsigned);
		}

		Self::from_json_text(&json.to_string()).expect("Failed to serialize Event value")
	}
}

impl<E: Event> From<Owned<E>> for Raw<AnyStateEvent> {
	fn from(event: Owned<E>) -> Self { Ref(&event.0).into() }
}

impl<'a, E: Event> From<Ref<'a, E>> for Raw<AnyStateEvent> {
	fn from(event: Ref<'a, E>) -> Self {
		let event = event.0;
		let mut json = json!({
			"content": event.content(),
			"event_id": event.event_id().as_str(),
			"origin_server_ts": event.origin_server_ts().0,
			"room_id": event.room_id_or_hash().as_ref().map(OwnedRoomId::as_str),
			"sender": event.sender().as_str(),
			"state_key": event.state_key(),
			"type": event.kind().to_string(),
		});

		if let Some(unsigned) = event.unsigned() {
			json["unsigned"] = json!(unsigned);
		}

		Self::from_json_text(&json.to_string()).expect("Failed to serialize Event value")
	}
}

impl<E: Event> From<Owned<E>> for Raw<AnySyncStateEvent> {
	fn from(event: Owned<E>) -> Self { Ref(&event.0).into() }
}

impl<'a, E: Event> From<Ref<'a, E>> for Raw<AnySyncStateEvent> {
	fn from(event: Ref<'a, E>) -> Self {
		let event = event.0;
		let mut json = json!({
			"content": event.content(),
			"event_id": event.event_id().as_str(),
			"origin_server_ts": event.origin_server_ts().0,
			"sender": event.sender().as_str(),
			"state_key": event.state_key(),
			"type": event.kind().to_string(),
		});

		if let Some(unsigned) = event.unsigned() {
			json["unsigned"] = json!(unsigned);
		}

		Self::from_json_text(&json.to_string()).expect("Failed to serialize Event value")
	}
}

impl<E: Event> From<Owned<E>> for Raw<AnyStrippedStateEvent> {
	fn from(event: Owned<E>) -> Self { Ref(&event.0).into() }
}

impl<'a, E: Event> From<Ref<'a, E>> for Raw<AnyStrippedStateEvent> {
	fn from(event: Ref<'a, E>) -> Self {
		let event = event.0;
		let json = json!({
			"content": event.content(),
			"origin_server_ts": event.origin_server_ts().0,
			"room_id": event.room_id_or_hash().as_ref().map(OwnedRoomId::as_str),
			"sender": event.sender().as_str(),
			"state_key": event.state_key(),
			"type": event.kind().to_string(),
		});

		Self::from_json_text(&json.to_string()).expect("Failed to serialize Event value")
	}
}

impl<E: Event> From<Owned<E>> for Raw<HierarchySpaceChildEvent> {
	fn from(event: Owned<E>) -> Self { Ref(&event.0).into() }
}

impl<'a, E: Event> From<Ref<'a, E>> for Raw<HierarchySpaceChildEvent> {
	fn from(event: Ref<'a, E>) -> Self {
		let event = event.0;
		let json = json!({
			"content": event.content(),
			"origin_server_ts": event.origin_server_ts().0,
			"sender": event.sender().as_str(),
			"state_key": event.state_key(),
			"type": event.kind().to_string(),
		});

		Self::from_json_text(&json.to_string()).expect("Failed to serialize Event value")
	}
}

impl<E: Event> From<Owned<E>> for Raw<StateEvent<RoomMemberEventContent>> {
	fn from(event: Owned<E>) -> Self { Ref(&event.0).into() }
}

impl<'a, E: Event> From<Ref<'a, E>> for Raw<StateEvent<RoomMemberEventContent>> {
	fn from(event: Ref<'a, E>) -> Self {
		let event = event.0;
		let mut json = json!({
			"content": event.content(),
			"event_id": event.event_id().as_str(),
			"origin_server_ts": event.origin_server_ts().0,
			"redacts": event.redacts().map(slipstream::OwnedEventId::as_str),
			"room_id": event.room_id().map(slipstream::OwnedRoomId::as_str),
			"sender": event.sender().as_str(),
			"state_key": event.state_key(),
			"type": event.kind().to_string(),
		});

		if let Some(unsigned) = event.unsigned() {
			json["unsigned"] = json!(unsigned);
		}

		Self::from_json_text(&json.to_string()).expect("Failed to serialize Event value")
	}
}

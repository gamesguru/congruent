use ruma::api::client::filter::{RoomEventFilter, UrlFilter};
use serde::Deserialize;
use serde_json::Value;

use super::Event;
use crate::is_equal_to;

pub trait Matches<E: Event> {
	fn matches(&self, event: &E) -> bool;
}

impl<E: Event> Matches<E> for &RoomEventFilter {
	#[inline]
	fn matches(&self, event: &E) -> bool {
		if !matches_sender(event, self) {
			return false;
		}

		if !matches_room(event, self) {
			return false;
		}

		if !matches_type(event, self) {
			return false;
		}

		if !matches_url(event, self) {
			return false;
		}

		if !matches_rel_type(event, self) {
			return false;
		}

		true
	}
}

fn matches_room<E: Event>(event: &E, filter: &RoomEventFilter) -> bool {
	let room_id = event.room_id_or_hash();

	if !filter.not_rooms.is_empty() {
		if let Some(ref rid) = room_id {
			if filter.not_rooms.iter().any(is_equal_to!(&**rid)) {
				return false;
			}
		}
	}

	if let Some(rooms) = filter.rooms.as_ref() {
		if let Some(ref rid) = room_id {
			if !rooms.iter().any(is_equal_to!(&**rid)) {
				return false;
			}
		} else if !rooms.is_empty() {
			// If we have a filter but the event (e.g. v12 create) has no room_id
			return false;
		}
	}

	true
}

fn matches_sender<E: Event>(event: &E, filter: &RoomEventFilter) -> bool {
	if filter.not_senders.iter().any(is_equal_to!(event.sender())) {
		return false;
	}

	if let Some(senders) = filter.senders.as_ref() {
		if !senders.iter().any(is_equal_to!(event.sender())) {
			return false;
		}
	}

	true
}

fn matches_type<E: Event>(event: &E, filter: &RoomEventFilter) -> bool {
	let kind = event.kind().to_cow_str();

	if filter.not_types.iter().any(is_equal_to!(&kind)) {
		return false;
	}

	if let Some(types) = filter.types.as_ref() {
		if !types.iter().any(is_equal_to!(&kind)) {
			return false;
		}
	}

	true
}

#[derive(Deserialize)]
struct ExtractRelType {
	rel_type: String,
}

#[derive(Deserialize)]
struct ExtractRelatesTo {
	#[serde(rename = "m.relates_to")]
	relates_to: Option<ExtractRelType>,
}

/// Per [MSC3874](https://github.com/matrix-org/matrix-spec-proposals/pull/3874).
fn matches_rel_type<E: Event>(event: &E, filter: &RoomEventFilter) -> bool {
	if filter.rel_types.is_none() && filter.not_rel_types.is_empty() {
		return true;
	}

	let rel_type = event
		.get_content::<ExtractRelatesTo>()
		.ok()
		.and_then(|c| c.relates_to)
		.map(|r| r.rel_type);

	if let Some(rel_types) = filter.rel_types.as_ref() {
		match &rel_type {
			| Some(rt) if rel_types.iter().any(is_equal_to!(rt)) => {},
			| _ => return false,
		}
	}

	if let Some(rt) = &rel_type {
		if filter.not_rel_types.iter().any(is_equal_to!(rt)) {
			return false;
		}
	}

	true
}

fn matches_url<E: Event>(event: &E, filter: &RoomEventFilter) -> bool {
	let Some(url_filter) = filter.url_filter.as_ref() else {
		return true;
	};

	//TODO: might be better to use Ruma's Raw rather than serde here
	let url = event
		.get_content_as_value()
		.get("url")
		.is_some_and(Value::is_string);

	match url_filter {
		| UrlFilter::EventsWithUrl => url,
		| UrlFilter::EventsWithoutUrl => !url,
	}
}

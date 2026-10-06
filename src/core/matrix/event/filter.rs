use slipstream::api::client::filter::{RoomEventFilter, UrlFilter};

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

		true
	}
}

fn matches_room<E: Event>(event: &E, filter: &RoomEventFilter) -> bool {
	let Some(room_id) = event.room_id_or_hash() else {
		return false;
	};

	if !filter.not_rooms.is_empty() {
		if filter.not_rooms.iter().any(is_equal_to!(room_id)) {
			return false;
		}
	}

	if let Some(rooms) = filter.rooms.as_ref() {
		if !rooms.iter().any(is_equal_to!(room_id)) {
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

	if filter.not_types.iter().any(is_equal_to!(kind, cow)) {
		return false;
	}

	if let Some(types) = filter.types.as_ref() {
		if !types.iter().any(is_equal_to!(kind, cow)) {
			return false;
		}
	}

	true
}

// Relation filters are intentionally handled by the core event matcher. The
// upstream API filter no longer owns these fields; keeping this logic here
// avoids pulling the full API model into slipstream.

fn matches_url<E: Event>(event: &E, filter: &RoomEventFilter) -> bool {
	let Some(url_filter) = filter.url_filter.as_ref() else {
		return true;
	};

	let url = event
		.get_content_as_value()
		.get("url")
		.and_then(|url| url.as_str())
		.is_some();

	match url_filter {
		| UrlFilter::EventsWithUrl => url,
		| UrlFilter::EventsWithoutUrl => !url,
	}
}

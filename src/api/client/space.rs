use std::{collections::BTreeSet, str::FromStr};

use axum::extract::State;
use conduwuit::{Err, Result};
use conduwuit_service::{
	Services,
	rooms::spaces::{
		PaginationToken, SummaryAccessibility, get_parent_children_via, summary_to_chunk,
	},
};
use futures::future::OptionFuture;
use ruma::{
	OwnedRoomId, OwnedServerName, RoomId, UInt, UserId, api::client::space::get_hierarchy,
	events::space::child::HierarchySpaceChildEvent, room::RoomType,
};

use crate::Ruma;

/// # `GET /_matrix/client/v1/rooms/{room_id}/hierarchy`
///
/// Paginates over the space tree in a depth-first manner to locate child rooms
/// of a given space.
pub(crate) async fn get_hierarchy_route(
	State(services): State<crate::State>,
	body: Ruma<get_hierarchy::v1::Request>,
) -> Result<get_hierarchy::v1::Response> {
	let limit = body
		.limit
		.unwrap_or_else(|| UInt::from(10_u32))
		.min(UInt::from(100_u32));

	let max_depth = body
		.max_depth
		.unwrap_or_else(|| UInt::from(3_u32))
		.min(UInt::from(10_u32));

	let key = body
		.from
		.as_ref()
		.and_then(|s| PaginationToken::from_str(s).ok());

	// Should prevent unexpected behaviour in (bad) clients
	if let Some(ref token) = key {
		if token.suggested_only != body.suggested_only || token.max_depth != max_depth {
			return Err!(Request(InvalidParam(
				"suggested_only and max_depth cannot change on paginated requests"
			)));
		}
	}

	get_client_hierarchy(
		&services,
		body.sender_user(),
		&body.room_id,
		limit.try_into().unwrap_or(10),
		max_depth.try_into().unwrap_or(usize::MAX),
		body.suggested_only,
		key.as_ref().map_or(0, |t| t.offset),
	)
	.await
}

async fn get_client_hierarchy(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	limit: usize,
	max_depth: usize,
	suggested_only: bool,
	offset: u64,
) -> Result<get_hierarchy::v1::Response> {
	type Via = Vec<OwnedServerName>;
	type Entry = (OwnedRoomId, Via, usize);

	// Depth-first pre-order traversal. The stack holds the rooms still to be
	// visited, each tagged with its depth in the tree; children are pushed in
	// reverse so that the first child is processed first.
	let mut stack: Vec<Entry> = vec![(
		room_id.to_owned(),
		room_id
			.server_name()
			.map(ToOwned::to_owned)
			.into_iter()
			.collect(),
		0,
	)];

	let mut rooms = Vec::with_capacity(limit);
	let mut visited = BTreeSet::new();
	let mut to_skip = offset;

	while let Some((current_room, via, depth)) = stack.pop() {
		let summary = services
			.rooms
			.spaces
			.get_summary_and_children_client(&current_room, suggested_only, sender_user, &via)
			.await?;

		match (summary, current_room == room_id) {
			| (None | Some(SummaryAccessibility::Inaccessible), false) => {
				// Just ignore other unavailable rooms
			},
			| (None, true) => {
				return Err!(Request(Forbidden("The requested room was not found")));
			},
			| (Some(SummaryAccessibility::Inaccessible), true) => {
				return Err!(Request(Forbidden("The requested room is inaccessible")));
			},
			| (Some(SummaryAccessibility::Accessible(mut summary)), _) => {
				if !visited.insert(current_room.clone()) {
					// Skip already-visited rooms (cycle safety).
					continue;
				}

				if to_skip > 0 {
					to_skip = to_skip.saturating_sub(1);
				} else {
					if suggested_only {
						// In a `suggested_only` walk the returned children_state
						// must only carry the suggested links.
						summary.children_state.retain(|raw| {
							raw.deserialize_as::<HierarchySpaceChildEvent>()
								.map(|ce| ce.content.suggested)
								.unwrap_or(false)
						});
					}
					rooms.push(summary_to_chunk(summary.clone()));
				}

				if rooms.len() >= limit {
					break;
				}

				// Only rooms of type `m.space` are expanded; a non-space room's
				// children links are ignored. Rooms deeper than max_depth are
				// returned but not expanded.
				let is_space = summary.room_type == Some(RoomType::Space);
				if !is_space || depth >= max_depth {
					continue;
				}

				let mut children: Vec<Entry> = get_parent_children_via(&summary, suggested_only)
					.filter(|(room, _)| !visited.contains(room))
					.map(|(room, via)| (room, via.collect(), depth.saturating_add(1)))
					.collect();

				// Push reversed so the first child is processed first.
				children.reverse();
				stack.extend(children);
			},
		}
	}

	let next_offset = offset.saturating_add(u64::try_from(rooms.len()).unwrap_or(u64::MAX));
	let next_batch: OptionFuture<_> = (!stack.is_empty())
		.then_some(async move {
			PaginationToken {
				offset: next_offset,
				limit: limit.try_into().ok().unwrap_or_default(),
				max_depth: max_depth.try_into().ok().unwrap_or_default(),
				suggested_only,
			}
			.to_string()
		})
		.into();

	Ok(get_hierarchy::v1::Response { next_batch: next_batch.await, rooms })
}

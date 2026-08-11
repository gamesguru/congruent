use std::{
	collections::{BTreeSet, VecDeque},
	str::FromStr,
};

use axum::extract::State as AxumState;
use conduwuit::{Err, Result, utils::stream::IterStream};
use conduwuit_service::rooms::spaces::{
	PaginationToken, SummaryAccessibility, get_parent_children_via, summary_to_chunk,
};
use futures::{StreamExt, future::OptionFuture};
use ruma::{
	OwnedRoomId, OwnedServerName, RoomId, UInt, UserId, api::client::space::get_hierarchy,
};

use crate::{Ruma, router::State};

/// # `GET /_matrix/client/v1/rooms/{room_id}/hierarchy`
///
/// Paginates over the space tree in a depth-first manner to locate child rooms
/// of a given space.
pub(crate) async fn get_hierarchy_route(
	AxumState(services): AxumState<State>,
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
		services,
		body.sender_user(),
		&body.room_id,
		limit.try_into().unwrap_or(10),
		max_depth.try_into().unwrap_or(usize::MAX),
		body.suggested_only,
		key.as_ref()
			.into_iter()
			.flat_map(|t| t.short_room_ids.iter()),
	)
	.await
}

async fn get_client_hierarchy<'a, ShortRoomIds>(
	services: State,
	sender_user: &UserId,
	room_id: &RoomId,
	limit: usize,
	max_depth: usize,
	suggested_only: bool,
	short_room_ids: ShortRoomIds,
) -> Result<get_hierarchy::v1::Response>
where
	ShortRoomIds: Iterator<Item = &'a u64> + Clone + Send + Sync + 'a,
{
	type Via = Vec<OwnedServerName>;
	type Entry = (OwnedRoomId, Via, usize, bool);
	type Rooms = VecDeque<Entry>;

	let mut queue: Rooms = [(
		room_id.to_owned(),
		room_id
			.server_name()
			.map(ToOwned::to_owned)
			.into_iter()
			.collect(),
		0,
		true,
	)]
	.into();

	let mut rooms = Vec::with_capacity(limit);

	let mut path = Vec::new();
	let mut visited = BTreeSet::new();

	while let Some((current_room, via, depth, on_token_path)) = queue.pop_back() {
		if !visited.insert(current_room.clone()) {
			continue;
		}

		if rooms.len() >= limit {
			queue.push_back((current_room, via, depth, on_token_path));
			break;
		}

		let summary_res = services
			.rooms
			.spaces
			.get_summary_and_children_client(&current_room, suggested_only, sender_user, &via)
			.await;

		let summary = match summary_res {
			| Ok(s) => s,
			| Err(e) => return Err(e),
		};

		match (summary, current_room == *room_id) {
			| (None | Some(SummaryAccessibility::Inaccessible), false) => {
				conduwuit::info!(
					room_id = %current_room,
					"spaces: ignoring unavailable room in hierarchy (inaccessible or missing summary)"
				);
			},
			| (None, true) => {
				conduwuit::info!(room_id = %current_room, "spaces: requested root room was not found");
				return Err!(Request(Forbidden("The requested room was not found")));
			},
			| (Some(SummaryAccessibility::Inaccessible), true) => {
				conduwuit::info!(room_id = %current_room, "spaces: requested root room is inaccessible");
				return Err!(Request(Forbidden("The requested room is inaccessible")));
			},
			| (Some(SummaryAccessibility::Accessible(summary)), _) => {
				path.truncate(depth);
				path.push(current_room.clone());

				let populate = !on_token_path || path.len() > short_room_ids.clone().count();

				let mut children: Vec<Entry> = get_parent_children_via(&summary, suggested_only)
					.into_iter()
					.rev()
					.map(|(key, val)| (key, val, depth.saturating_add(1), false))
					.collect();

				if populate {
					rooms.push(summary_to_chunk(summary.clone(), suggested_only));
				} else {
					let mut s_ids = short_room_ids.clone();
					let target = s_ids.nth(depth);
					children = {
						let mut valid = vec![];
						let mut reached_target = false;
						for (room, via, child_depth, _) in children.iter().rev() {
							if !reached_target {
								if let Ok(short) =
									services.rooms.short.get_shortroomid(room).await
								{
									if Some(&short) == target {
										reached_target = true;
										valid.push((
											room.clone(),
											via.clone(),
											*child_depth,
											true,
										));
									}
								}
							} else {
								valid.push((room.clone(), via.clone(), *child_depth, false));
							}
						}
						valid.reverse();
						valid
					};
				}

				if !populate && queue.is_empty() && children.is_empty() {
					break;
				}

				if path.len() > max_depth {
					continue;
				}

				queue.extend(children);
			},
		}
	}

	let next_batch: OptionFuture<_> = queue
		.pop_back()
		.map(|(room, _, depth, _)| {
			let mut path = path.clone();
			async move {
				path.truncate(depth);
				path.push(room);

				let next_short_room_ids: Vec<u64> = path
					.iter()
					.stream()
					.filter_map(|room_id| async move {
						services.rooms.short.get_shortroomid(room_id).await.ok()
					})
					.collect()
					.await;

				// Exclude root from token
				let next_short_room_ids: Vec<_> =
					next_short_room_ids.into_iter().skip(1).collect();

				(!next_short_room_ids.is_empty())
					.then_some(PaginationToken {
						short_room_ids: next_short_room_ids,
						limit: limit.try_into().ok()?,
						max_depth: max_depth.try_into().ok()?,
						suggested_only,
					})
					.as_ref()
					.map(|t| format!("{t}"))
			}
		})
		.into();

	Ok(get_hierarchy::v1::Response {
		next_batch: next_batch.await.flatten(),
		rooms,
	})
}

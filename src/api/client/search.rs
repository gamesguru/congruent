use std::collections::BTreeMap;

use axum::extract::State;
use conduwuit::{
	Err, Result, at, debug_warn, is_true,
	matrix::Event,
	result::FlatOk,
	utils::{IterStream, stream::ReadyExt},
};
use conduwuit_service::{Services, rooms::search::RoomQuery};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt, future::OptionFuture};
use ruma::{
	OwnedRoomId, RoomId, UInt, UserId,
	api::client::search::search_events::{
		self,
		v3::{Criteria, EventContextResult, ResultCategories, ResultRoomEvents, SearchResult},
	},
	events::AnyStateEvent,
	serde::Raw,
};
use search_events::v3::{Request, Response};

use super::message::visibility_filter;
use crate::Ruma;

type RoomStates = BTreeMap<OwnedRoomId, RoomState>;
type RoomState = Vec<Raw<AnyStateEvent>>;

const LIMIT_DEFAULT: usize = 10;
const LIMIT_MAX: usize = 100;
const BATCH_MAX: usize = 20;

/// # `POST /_matrix/client/r0/search`
///
/// Searches rooms for messages.
///
/// - Only works if the user is currently joined to the room (TODO: Respect
///   history visibility)
pub(crate) async fn search_events_route(
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	let sender_user = body.sender_user();
	let next_batch = body.next_batch.as_deref();
	let room_events_result: OptionFuture<_> = body
		.search_categories
		.room_events
		.as_ref()
		.map(|criteria| category_room_events(&services, sender_user, next_batch, criteria))
		.into();

	Ok(Response {
		search_categories: ResultCategories {
			room_events: Box::pin(room_events_result)
				.await
				.unwrap_or_else(|| Ok(ResultRoomEvents::default()))?,
		},
	})
}

async fn category_room_events(
	services: &Services,
	sender_user: &UserId,
	next_batch: Option<&str>,
	criteria: &Criteria,
) -> Result<ResultRoomEvents> {
	let filter = &criteria.filter;

	let limit: usize = filter
		.limit
		.map(TryInto::try_into)
		.flat_ok()
		.unwrap_or(LIMIT_DEFAULT)
		.min(LIMIT_MAX);

	let next_batch: usize = next_batch
		.map(str::parse)
		.transpose()?
		.unwrap_or(0)
		.min(limit.saturating_mul(BATCH_MAX));

	let rooms = filter
		.rooms
		.clone()
		.map(IntoIterator::into_iter)
		.map(IterStream::stream)
		.map_or_else(
			|| {
				services
					.rooms
					.state_cache
					.rooms_joined(sender_user)
					.map(ToOwned::to_owned)
					.boxed()
			},
			StreamExt::boxed,
		);

	let results: Vec<_> = rooms
		.filter_map(|room_id| {
			async move {
				check_room_visible(services, sender_user, &room_id, criteria)
					.await
					.is_ok()
					.then_some(room_id)
			}
			.boxed()
		})
		.filter_map(|room_id| {
			async move {
				let query = RoomQuery {
					room_id: &room_id,
					user_id: Some(sender_user),
					criteria,
					skip: next_batch,
					limit,
				};

				let (count, results) = services
					.rooms
					.search
					.search_pdus(&query, sender_user)
					.await
					.ok()?;

				results
					.collect::<Vec<_>>()
					.map(|results| (room_id.clone(), count, results))
					.map(Some)
					.await
			}
			.boxed()
		})
		.collect()
		.await;

	let total: UInt = results
		.iter()
		.fold(0, |a: usize, (_, count, _)| a.saturating_add(*count))
		.try_into()?;

	let state: RoomStates = results
		.iter()
		.stream()
		.ready_filter(|_| criteria.include_state.is_some_and(is_true!()))
		.filter_map(|(room_id, ..)| async move {
			procure_room_state(services, room_id)
				.map_ok(|state| (room_id.clone(), state))
				.await
				.ok()
		})
		.collect()
		.await;

	let mut search_results = Vec::new();
	for mut pdu in results.into_iter().map(at!(2)).flatten() {
		if let Err(e) = services
			.rooms
			.pdu_metadata
			.add_bundled_aggregations_to_pdu(sender_user, &mut pdu)
			.await
		{
			debug_warn!("Failed to add bundled aggregations to search result: {e}");
		}

		search_results.push(build_search_result(services, sender_user, criteria, pdu).await);
	}
	let results: Vec<SearchResult> = search_results;

	let highlights = criteria
		.search_term
		.split_terminator(|c: char| !c.is_alphanumeric())
		.map(str::to_lowercase)
		.collect();

	let next_batch = (results.len() >= limit)
		.then_some(next_batch.saturating_add(results.len()))
		.as_ref()
		.map(|n| format!("{n}"));

	Ok(ResultRoomEvents {
		count: Some(total),
		next_batch,
		results,
		state,
		highlights,
		groups: BTreeMap::new(), // TODO
	})
}

async fn build_search_result(
	services: &Services,
	sender_user: &UserId,
	criteria: &Criteria,
	pdu: conduwuit::matrix::pdu::PduEvent,
) -> SearchResult {
	let before_limit = usize::try_from(criteria.event_context.before_limit).unwrap_or(5);
	let after_limit = usize::try_from(criteria.event_context.after_limit).unwrap_or(5);
	let context =
		load_event_context(services, sender_user, &pdu, before_limit, after_limit).await;

	SearchResult {
		rank: None,
		result: Some(pdu.into_format()),
		context,
	}
}

async fn load_event_context(
	services: &Services,
	sender_user: &UserId,
	pdu: &conduwuit::matrix::pdu::PduEvent,
	before_limit: usize,
	after_limit: usize,
) -> EventContextResult {
	let mut events_before = Vec::new();
	let mut events_after = Vec::new();

	if before_limit == 0 && after_limit == 0 {
		return EventContextResult {
			profile_info: BTreeMap::new(), //TODO
			events_after,
			events_before,
			start: None, //TODO
			end: None,   //TODO
		};
	}

	let Some(room_id) = pdu.room_id_or_hash() else {
		return EventContextResult {
			profile_info: BTreeMap::new(), //TODO
			events_after,
			events_before,
			start: None, //TODO
			end: None,   //TODO
		};
	};

	let Ok(count) = services.rooms.timeline.get_pdu_count(pdu.event_id()).await else {
		return EventContextResult {
			profile_info: BTreeMap::new(), //TODO
			events_after,
			events_before,
			start: None, //TODO
			end: None,   //TODO
		};
	};

	if before_limit > 0 {
		use futures::{StreamExt, pin_mut};
		let stream = services
			.rooms
			.timeline
			.pdus_rev(&room_id, std::ops::Bound::Excluded(count))
			.map_ok(|item| ((), item.1));
		pin_mut!(stream);
		while let Some(Ok(item)) = stream.next().await {
			let Some(((), prev_pdu)) = visibility_filter(services, item, sender_user).await
			else {
				continue;
			};

			events_before.push(prev_pdu.into_format());
			if events_before.len() >= before_limit {
				break;
			}
		}
	}

	if after_limit > 0 {
		use futures::{StreamExt, pin_mut};
		let stream = services
			.rooms
			.timeline
			.pdus(&room_id, std::ops::Bound::Excluded(count))
			.map_ok(|item| ((), item.1));
		pin_mut!(stream);
		while let Some(Ok(item)) = stream.next().await {
			let Some(((), next_pdu)) = visibility_filter(services, item, sender_user).await
			else {
				continue;
			};

			events_after.push(next_pdu.into_format());
			if events_after.len() >= after_limit {
				break;
			}
		}
	}

	EventContextResult {
		profile_info: BTreeMap::new(), //TODO
		events_after,
		events_before,
		start: None, //TODO
		end: None,   //TODO
	}
}

async fn procure_room_state(services: &Services, room_id: &RoomId) -> Result<RoomState> {
	let state = services
		.rooms
		.state_accessor
		.room_state_full_pdus(room_id)
		.map_ok(Event::into_format)
		.try_collect()
		.await?;

	Ok(state)
}

async fn check_room_visible(
	services: &Services,
	user_id: &UserId,
	room_id: &RoomId,
	search: &Criteria,
) -> Result {
	let check_visible = search.filter.rooms.is_some();
	let check_state = check_visible && search.include_state.is_some_and(is_true!());

	let is_joined =
		!check_visible || services.rooms.state_cache.is_joined(user_id, room_id).await;

	let state_visible = !check_state
		|| services
			.rooms
			.state_accessor
			.user_can_see_state_events(user_id, room_id)
			.await;

	if !is_joined || !state_visible {
		return Err!(Request(Forbidden("You don't have permission to view {room_id:?}")));
	}

	Ok(())
}

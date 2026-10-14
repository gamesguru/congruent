use std::{collections::BTreeSet, ops::ControlFlow};

use conduwuit::{
	Result, at, is_equal_to,
	matrix::{
		Event,
		pdu::{PduCount, PduEvent},
	},
	trace,
	utils::{
		BoolExt, IterStream, ReadyExt,
		stream::{BroadbandExt, TryIgnore},
	},
};
use conduwuit_service::{Services, rooms::lazy_loading::MemberSet};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use service::rooms::short::ShortEventId;
use slipstream::{OwnedEventId, RoomId, UserId, events::StateEventType};

use crate::client::TimelinePdus;

/// Look up a lazily-loaded member's membership event.
///
/// In a partial-state room (MSC3902) the room state omits remote members, so
/// the lookup falls back to the membership event cited in the `auth_events` of
/// an event that user sent in the timeline.
async fn lazy_member_event(
	services: &Services,
	room_id: &RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	user_id: &UserId,
	timeline: Option<&TimelinePdus>,
) -> Option<PduEvent> {
	if let Ok(pdu) = services
		.rooms
		.state_accessor
		.state_get_in_room_hamt(
			room_id,
			root_handle,
			&StateEventType::RoomMember,
			user_id.as_str(),
		)
		.await
	{
		return Some(pdu);
	}

	let timeline = timeline?;
	if !services
		.rooms
		.state_partial
		.partial_server(room_id)
		.await
		.is_ok_and(|s| s.is_some())
	{
		return None;
	}

	for (_, pdu) in timeline
		.pdus
		.iter()
		.filter(|(_, pdu)| pdu.sender() == user_id)
	{
		for auth_event_id in pdu.auth_events() {
			if let Ok(auth_pdu) = services.rooms.timeline.get_pdu(auth_event_id).await {
				if *auth_pdu.kind() == slipstream::events::TimelineEventType::RoomMember
					&& auth_pdu.state_key() == Some(user_id.as_str())
				{
					return Some(auth_pdu);
				}
			}
		}
	}

	None
}

/// Calculate the state events to include in an initial sync response.
///
/// If lazy-loading is enabled (`lazily_loaded_members` is Some), the returned
/// Vec will include the membership events of exclusively the members in
/// `lazily_loaded_members`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn build_state_initial(
	services: &Services,
	room_id: &RoomId,
	sender_user: &UserId,
	timeline_start_root_handle: &rezzy::hamt::RootHandle,
	lazily_loaded_members: Option<&MemberSet>,
	timeline: Option<&TimelinePdus>,
) -> Result<Vec<PduEvent>> {
	// load the keys and event IDs of the state events at the start of the timeline
	let (shortstatekeys, event_ids): (Vec<_>, Vec<_>) = services
		.rooms
		.state_accessor
		.state_full_ids_hamt(room_id, timeline_start_root_handle)
		.try_collect::<Vec<_>>()
		.await?
		.into_iter()
		.unzip();

	trace!("performing initial sync of {} state events", event_ids.len());

	let pdus = services
		.rooms
		.short
		// look up the full state keys
		.multi_get_statekey_from_short(shortstatekeys.into_iter().stream())
		.zip(event_ids.into_iter().stream())
		.ready_filter_map(|item| Some((item.0.ok()?, item.1)))
		.ready_filter_map(|((event_type, state_key), event_id)| {
			if let Some(lazily_loaded_members) = lazily_loaded_members {
				/*
				if lazy loading is enabled, filter out membership events which aren't for a user
				included in `lazily_loaded_members` or for the user requesting the sync.
				*/
				let event_is_redundant = event_type == StateEventType::RoomMember
					&& UserId::parse(state_key.as_str()).is_ok_and(|user_id| {
						*sender_user != user_id && !lazily_loaded_members.contains(&user_id)
					});

				event_is_redundant.or_some(event_id)
			} else {
				Some(event_id)
			}
		})
		.broad_filter_map(|event_id: OwnedEventId| async move {
			services.rooms.timeline.get_pdu(&event_id).await.ok()
		})
		.collect::<Vec<PduEvent>>()
		.await;

	let mut pdus = pdus;
	// lazily-loaded members that weren't in the (possibly partial) state
	if let Some(lazily_loaded_members) = lazily_loaded_members {
		let present: std::collections::HashSet<String> = pdus
			.iter()
			.filter(|pdu| *pdu.kind() == slipstream::events::TimelineEventType::RoomMember)
			.filter_map(|pdu| pdu.state_key().map(ToOwned::to_owned))
			.collect();

		for user_id in lazily_loaded_members {
			if user_id == sender_user || present.contains(user_id.as_str()) {
				continue;
			}
			if let Some(pdu) = lazy_member_event(
				services,
				room_id,
				timeline_start_root_handle,
				user_id,
				timeline,
			)
			.await
			{
				pdus.push(pdu);
			}
		}
	}

	Ok(pdus)
}

/// Calculate the state events to include in an incremental sync response.
///
/// If lazy-loading is enabled (`lazily_loaded_members` is Some), the returned
/// Vec will include the membership events of all the members in
/// `lazily_loaded_members`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn build_state_incremental<'a>(
	services: &Services,
	sender_user: &'a UserId,
	room_id: &RoomId,
	last_sync_end_count: PduCount,
	last_sync_end_root_handle: &rezzy::hamt::RootHandle,
	timeline_start_root_handle: &rezzy::hamt::RootHandle,
	timeline_end_root_handle: &rezzy::hamt::RootHandle,
	timeline: &TimelinePdus,
	lazily_loaded_members: Option<&'a MemberSet>,
) -> Result<Vec<PduEvent>> {
	/*
	NB: a limited sync is one where `timeline.limited == true`. Synapse calls this a "gappy" sync internally.

	The algorithm implemented in this function is, currently, quite different from the algorithm vaguely described
	by the Matrix specification. This is because the specification's description of the `state` property does not accurately
	reflect how Synapse behaves, and therefore how client SDKs behave. Notable differences include:
	1. We do not compute the delta using the naive approach of "every state event from the end of the last sync
	   up to the start of this sync's timeline". see below for details.
	2. If lazy-loading is enabled, we include lazily-loaded membership events. The specific users to include are determined
	   elsewhere and supplied to this function in the `lazily_loaded_members` parameter.
	*/

	/*
	the `state` property of an incremental sync which isn't limited are _usually_ empty.
	(note: the specification says that the `state` property is _always_ empty for limited syncs, which is incorrect.)
	however, if an event in the timeline (`timeline.pdus`) merges a split in the room's DAG (i.e. has multiple `prev_events`),
	the state at the _end_ of the timeline may include state events which were merged in and don't exist in the state
	at the _start_ of the timeline. because this is uncommon, we check here to see if any events in the timeline
	merged a split in the DAG.

	see: https://github.com/element-hq/synapse/issues/16941
	*/

	let timeline_is_linear = timeline.pdus.is_empty() || {
		let last_pdu_of_last_sync = services
			.rooms
			.timeline
			.pdus_rev(room_id, std::ops::Bound::Included(last_sync_end_count))
			.boxed()
			.next()
			.await
			.transpose()
			.expect("last sync should have had some PDUs")
			.map(at!(1));

		// make sure the prev_events of each pdu in the timeline refer only to the
		// previous pdu
		timeline
			.pdus
			.iter()
			.try_fold(last_pdu_of_last_sync.map(|pdu| pdu.event_id), |prev_event_id, (_, pdu)| {
				if let Ok(pdu_prev_event_id) = pdu.prev_events.iter().exactly_one() {
					if prev_event_id
						.as_ref()
						.is_none_or(is_equal_to!(pdu_prev_event_id))
					{
						return ControlFlow::Continue(Some(pdu_prev_event_id.to_owned()));
					}
				}

				trace!(
					"pdu {:?} has split prev_events (expected {:?}): {:?}",
					pdu.event_id, prev_event_id, pdu.prev_events
				);
				ControlFlow::Break(())
			})
			.is_continue()
	};

	if timeline_is_linear && !timeline.limited {
		// if there are no splits in the DAG and the timeline isn't limited, then
		// `state` will always be empty unless lazy loading is enabled.

		if let Some(lazily_loaded_members) = lazily_loaded_members {
			if !timeline.pdus.is_empty() {
				// lazy loading is enabled, so we return the membership events which were
				// requested by the caller.
				let lazy_membership_events: Vec<_> = lazily_loaded_members
					.iter()
					.stream()
					.broad_filter_map(|user_id| async move {
						if user_id == sender_user {
							return None;
						}

						lazy_member_event(
							services,
							room_id,
							timeline_start_root_handle,
							user_id,
							Some(timeline),
						)
						.await
					})
					.collect()
					.await;

				if !lazy_membership_events.is_empty() {
					trace!(
						"syncing lazy membership events for members: {:?}",
						lazy_membership_events
							.iter()
							.map(|pdu| pdu.state_key().unwrap())
							.collect::<Vec<_>>()
					);
				}
				return Ok(lazy_membership_events);
			}
		}

		// lazy loading is disabled, `state` is empty.
		return Ok(vec![]);
	}

	/*
	at this point, either the timeline is `limited` or the DAG has a split in it. this necessitates
	computing the incremental state (which may be empty).

	NOTE: this code path does not use the `lazy_membership_events` parameter. any changes to membership will be included
	in the incremental state. therefore, the incremental state may include "redundant" membership events,
	which we do not filter out because A. the spec forbids lazy-load filtering if the timeline is `limited`,
	and B. DAG splits which require sending extra membership state events are (probably) uncommon enough that
	the performance penalty is acceptable.
	*/

	trace!(%timeline_is_linear, %timeline.limited, "computing state for incremental sync");

	// fetch the shorteventids of state events in the timeline
	let state_events_in_timeline: BTreeSet<ShortEventId> = services
		.rooms
		.short
		.multi_get_or_create_shorteventid(timeline.pdus.iter().filter_map(|(_, pdu)| {
			if pdu.state_key().is_some() {
				Some(pdu.event_id.as_ref())
			} else {
				None
			}
		}))
		.collect()
		.await;

	trace!("{} state events in timeline", state_events_in_timeline.len());

	/*
	fetch the state events which were added since the last sync.

	specifically we fetch the difference between the state at the last sync and the state at the _end_
	of the timeline, and then we filter out state events in the timeline itself using the shorteventids we fetched.
	this is necessary to account for splits in the DAG, as explained above.
	*/
	let state_diff = services
		.rooms
		.short
		.multi_get_eventid_from_short::<'_, OwnedEventId, _>(
			services
				.rooms
				.state_accessor
				.state_added_hamt(room_id, (last_sync_end_root_handle, timeline_end_root_handle))?
				.stream()
				.ready_filter_map(|(_, shorteventid)| {
					if state_events_in_timeline.contains(&shorteventid) {
						None
					} else {
						Some(shorteventid)
					}
				}),
		)
		.ignore_err();

	// finally, fetch the PDU contents and collect them into a vec
	let mut state_diff_pdus = state_diff
		.broad_filter_map(|event_id| async move {
			services
				.rooms
				.timeline
				.get_non_outlier_pdu(&event_id)
				.await
				.ok()
		})
		.collect::<Vec<_>>()
		.await;

	// With lazy-loading the client never received most memberships, so the diff
	// against the previous sync's state cannot be trusted to contain the
	// memberships of the timeline's senders. `lazily_loaded_members` has already
	// been narrowed to the ones this device hasn't been sent yet (and is now
	// marked as sent), so they must be included here too.
	if let Some(lazily_loaded_members) = lazily_loaded_members {
		let already_included: std::collections::HashSet<OwnedEventId> = state_diff_pdus
			.iter()
			.map(|pdu| pdu.event_id.clone())
			.collect();

		let lazy_membership_events: Vec<PduEvent> = lazily_loaded_members
			.iter()
			.stream()
			.broad_filter_map(|user_id| async move {
				if user_id == sender_user {
					return None;
				}

				lazy_member_event(
					services,
					room_id,
					timeline_start_root_handle,
					user_id,
					Some(timeline),
				)
				.await
			})
			.collect()
			.await;

		state_diff_pdus.extend(
			lazy_membership_events
				.into_iter()
				.filter(|pdu| !already_included.contains(&pdu.event_id)),
		);
	}

	trace!(
		target: "conduwuit::api::client::sync",
		?state_diff_pdus,
		"collected state PDUs for incremental sync"
	);
	Ok(state_diff_pdus)
}

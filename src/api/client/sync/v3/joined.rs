use std::collections::{BTreeMap, HashSet};

use conduwuit::{
	Error, Result, at, debug, debug_warn, extract_variant,
	matrix::{
		Event,
		event::Matches,
		pdu::{PduCount, PduEvent},
	},
	trace,
	utils::{
		BoolExt, IterStream, ReadyExt, TryFutureExtExt,
		math::ruma_from_u64,
		stream::{TryIgnore, WidebandExt},
	},
	warn,
};
use conduwuit_service::Services;
use futures::{
	FutureExt, StreamExt,
	future::{join, join3, join4, try_join, try_join3},
};
use ruma::{
	OwnedRoomId, OwnedUserId, RoomId, UserId,
	api::client::sync::sync_events::{
		UnreadNotificationsCount,
		v3::{Ephemeral, JoinedRoom, RoomAccountData, RoomSummary, State as RoomState, Timeline},
	},
	events::{
		AnyRawAccountDataEvent, AnySyncStateEvent, StateEventType,
		TimelineEventType::*,
		room::member::{MembershipState, RoomMemberEventContent},
	},
	serde::Raw,
	uint,
};
use service::rooms::short::ShortStateHash;

use super::load_timeline;
use crate::client::{
	TimelinePdus, ignored_filter,
	sync::{
		shares_a_room,
		v3::{
			DEFAULT_TIMELINE_LIMIT, DeviceListUpdates, SyncContext,
			prepare_lazily_loaded_members,
			state::{build_state_incremental, build_state_initial},
		},
	},
};

/// Generate the sync response for a room the user is joined to.
#[tracing::instrument(
	name = "joined",
	level = "debug",
	skip_all,
	fields(
		room_id = %room_id,
		syncing_user = %sync_context.syncing_user,
	),
)]
pub(super) async fn load_joined_room(
	services: &Services,
	sync_context: SyncContext<'_>,
	ref room_id: OwnedRoomId,
) -> Result<(JoinedRoom, Vec<Raw<AnySyncStateEvent>>, DeviceListUpdates)> {
	/*
	Building a sync response involves many steps which all depend on each other.
	To parallelize the process as much as possible, each step is divided into its own function,
	and `join*` functions are used to perform steps in parallel which do not depend on each other.
	*/

	let (
		account_data,
		ephemeral,
		StateAndTimeline {
			state_events,
			state_after,
			timeline,
			summary,
			notification_counts,
			device_list_updates,
		},
	) = try_join3(
		build_account_data(services, sync_context, room_id),
		build_ephemeral(services, sync_context, room_id),
		build_state_and_timeline(services, sync_context, room_id),
	)
	.boxed()
	.await?;

	if !timeline.is_empty() || !state_events.is_empty() {
		trace!(
			"syncing {} timeline events (limited = {}) and {} state events",
			timeline.events.len(),
			timeline.limited,
			state_events.len()
		);
	}

	let joined_room = JoinedRoom {
		account_data,
		summary: summary.unwrap_or_default(),
		unread_notifications: notification_counts.unwrap_or_default(),
		timeline,
		state: RoomState {
			events: state_events.into_iter().map(Event::into_format).collect(),
		},
		ephemeral,
		unread_thread_notifications: BTreeMap::new(),
	};

	let state_after = state_after
		.into_iter()
		.map(Event::into_format)
		.collect::<Vec<_>>();

	Ok((joined_room, state_after, device_list_updates))
}

/// Collect changes to the syncing user's account data events.
#[tracing::instrument(level = "debug", skip_all)]
async fn build_account_data(
	services: &Services,
	SyncContext {
		syncing_user,
		last_sync_end_count,
		current_count,
		..
	}: SyncContext<'_>,
	room_id: &RoomId,
) -> Result<RoomAccountData> {
	let account_data_changes = services
		.account_data
		.changes_since(Some(room_id), syncing_user, last_sync_end_count, Some(current_count))
		.ready_filter_map(|e| extract_variant!(e, AnyRawAccountDataEvent::Room))
		.collect()
		.await;

	Ok(RoomAccountData { events: account_data_changes })
}

/// Collect new ephemeral events.
#[tracing::instrument(level = "debug", skip_all)]
async fn build_ephemeral(
	services: &Services,
	SyncContext { syncing_user, last_sync_end_count, .. }: SyncContext<'_>,
	room_id: &RoomId,
) -> Result<Ephemeral> {
	// note: some of the futures below are boxed. this is because, without the box,
	// rustc produces over thirty inscrutable errors in `mod.rs` at the call-site
	// of `load_joined_room`. I don't know why boxing them fixes this -- it seems
	// to be related to the async closures and borrowing from the sync context.

	// collect updates to read receipts
	let receipt_events = services
		.rooms
		.read_receipt
		.readreceipts_since(room_id, last_sync_end_count)
		.filter_map(async |(read_user, _, edu)| {
			let is_ignored = services
				.users
				.user_is_ignored(&read_user, syncing_user)
				.await;

			// filter out read receipts for ignored users
			is_ignored.or_some(edu)
		})
		.collect::<Vec<_>>()
		.boxed();

	// collect the updated list of typing users, if it's changed
	let typing_event = async {
		let should_send_typing_event = match last_sync_end_count {
			| Some(last_sync_end_count) => {
				match services.rooms.typing.last_typing_update(room_id).await {
					| Ok(last_typing_update) => {
						// update the typing list if the users typing have changed since the last
						// sync
						last_typing_update > last_sync_end_count
					},
					| Err(err) => {
						warn!("Error checking last typing update: {}", err);
						return None;
					},
				}
			},
			// always update the typing list on an initial sync
			| None => true,
		};

		if should_send_typing_event {
			let event = services
				.rooms
				.typing
				.typings_event_for_user(room_id, syncing_user)
				.await;

			if let Ok(event) = event {
				// only suppress an empty typing list on a fresh (initial) sync; if the
				// list changed since the last sync, an empty list means typing was
				// explicitly stopped and clients need to observe that transition
				if event.content.user_ids.is_empty() && last_sync_end_count.is_none() {
					return None;
				}

				return Some(
					Raw::new(&event)
						.expect("typing event should be valid")
						.cast(),
				);
			}
		}

		None
	};

	// collect the syncing user's private-read marker, if it's changed
	let private_read_event = async {
		let should_send_private_read = match last_sync_end_count {
			| Some(last_sync_end_count) => {
				let last_privateread_update = services
					.rooms
					.read_receipt
					.last_privateread_update(syncing_user, room_id)
					.await;

				// update the marker if it's changed since the last sync
				last_privateread_update > last_sync_end_count
			},
			// always update the marker on an initial sync
			| None => true,
		};

		if should_send_private_read {
			services
				.rooms
				.read_receipt
				.private_read_get(room_id, syncing_user)
				.await
				.ok()
		} else {
			None
		}
	};

	let (receipt_events, typing_event, private_read_event) =
		join3(receipt_events, typing_event, private_read_event).await;

	let mut edus =
		conduwuit_service::rooms::read_receipt::pack_receipts_v3(receipt_events.into_iter());
	edus.extend(typing_event);
	edus.extend(private_read_event);

	Ok(Ephemeral { events: edus })
}

/// A struct to hold the state events, timeline, and other data which is
/// computed from them.
struct StateAndTimeline {
	state_events: Vec<PduEvent>,
	state_after: Vec<PduEvent>,
	timeline: Timeline,
	summary: Option<RoomSummary>,
	notification_counts: Option<UnreadNotificationsCount>,
	device_list_updates: DeviceListUpdates,
}

/// Compute changes to the room's state and timeline.
#[tracing::instrument(level = "debug", skip_all)]
async fn build_state_and_timeline(
	services: &Services,
	sync_context: SyncContext<'_>,
	room_id: &RoomId,
) -> Result<StateAndTimeline> {
	let shortstatehashes = fetch_shortstatehashes(services, sync_context, room_id).await?;

	let joined_since_last_sync =
		check_joined_since_last_sync(services, room_id, shortstatehashes, sync_context).await?;

	// If the syncing user joined since the last sync but their join event landed
	// after `current_count` was captured (a race between federation join and
	// sync), the timeline may come back empty here. Don't widen the fetch beyond
	// `current_count` to compensate: that would pull in events the sync token
	// doesn't yet cover, which could duplicate or skew ordering on the next
	// incremental sync. The room's state (e.g. the join itself) still syncs via
	// `build_state_events` below; the timeline catches up on the next poll once
	// `current_count` has advanced past the join event.
	let timeline =
		build_timeline(services, sync_context, room_id, joined_since_last_sync).await?;

	let (state_events, state_after, notification_counts) = try_join3(
		build_state_events(
			services,
			sync_context,
			room_id,
			shortstatehashes,
			&timeline,
			joined_since_last_sync,
		),
		build_state_after(services, sync_context, room_id, shortstatehashes, &timeline),
		build_notification_counts(services, sync_context, room_id, &timeline),
	)
	.await?;

	debug!(
		%room_id,
		joined_since_last_sync,
		timeline_len = timeline.pdus.len(),
		state_len = state_events.len(),
		"build_state_and_timeline: results"
	);

	let (summary, device_list_updates) = try_join(
		build_room_summary(
			services,
			sync_context,
			room_id,
			shortstatehashes,
			&timeline,
			&state_events,
			joined_since_last_sync,
		),
		build_device_list_updates(
			services,
			sync_context,
			room_id,
			shortstatehashes,
			&timeline,
			&state_events,
			joined_since_last_sync,
		),
	)
	.await?;

	let TimelinePdus {
		pdus,
		limited: timeline_limited,
		prev_batch,
	} = timeline;

	let user_has_join_event_in_sync = pdus
		.iter()
		.map(|(_, pdu)| pdu)
		.chain(state_events.iter())
		.filter(|event| {
			event.kind == RoomMember
				&& event.state_key.as_deref() == Some(sync_context.syncing_user.as_str())
		})
		.any(|event| {
			event
				.get_content()
				.is_ok_and(|c: RoomMemberEventContent| c.membership == MembershipState::Join)
		});

	// note: we usually indicate a limited timeline if the syncing user just joined
	// the room to trigger backfill (Synapse behavior). However, if the room
	// actually has ZERO timeline events (e.g., a space), forcing `limited: true`
	// causes clients to fail repeatedly to backfill.
	let is_space = services
		.rooms
		.state_accessor
		.get_room_type(room_id)
		.await
		.is_ok_and(|room_type| room_type == ruma::room::RoomType::Space);

	let limited = if pdus.is_empty() {
		timeline_limited || ((joined_since_last_sync || user_has_join_event_in_sync) && !is_space)
	} else {
		timeline_limited || joined_since_last_sync || user_has_join_event_in_sync
	};

	// the token which may be passed to the messages endpoint to backfill room
	// history. If the timeline is empty, fallback to the start of this sync window
	// to ensure clients always have a valid topological pagination token.
	let prev_batch = prev_batch.map(|c| c.to_string()).or_else(|| {
		limited
			.then_some(())
			.and(sync_context.last_sync_end_count)
			.map(|c| PduCount::Normal(c).to_string())
	});

	// filter out ignored events from the timeline and convert the PDUs into Ruma's
	// AnySyncTimelineEvent type
	let filtered_timeline_pdus: Vec<PduEvent> = pdus
		.into_iter()
		.stream()
		.wide_filter_map(|item| ignored_filter(services, item, sync_context.syncing_user))
		.ready_filter(|(_, pdu)| {
			let filter = &sync_context.filter.room.timeline;
			filter.matches(pdu)
		})
		.map(at!(1))
		.collect::<Vec<_>>()
		.await;

	let timeline_ids: HashSet<&ruma::EventId> = filtered_timeline_pdus
		.iter()
		.map(|pdu| &*pdu.event_id)
		.collect();

	let state_events: Vec<_> = state_events
		.into_iter()
		.filter(|pdu| !timeline_ids.contains(&*pdu.event_id))
		.collect();

	Ok(StateAndTimeline {
		state_events,
		state_after,
		timeline: Timeline {
			limited,
			prev_batch,
			events: filtered_timeline_pdus
				.into_iter()
				.map(Event::into_format)
				.collect(),
		},
		summary,
		notification_counts,
		device_list_updates,
	})
}

/// Shortstatehashes necessary to compute what state events to sync.
#[derive(Clone, Copy)]
struct ShortStateHashes {
	/// The current state of the syncing room.
	current_shortstatehash: ShortStateHash,
	/// The state of the syncing room at the end of the last sync.
	last_sync_end_shortstatehash: Option<ShortStateHash>,
}

/// Fetch the current_shortstatehash and last_sync_end_shortstatehash.
#[tracing::instrument(level = "debug", skip_all)]
async fn fetch_shortstatehashes(
	services: &Services,
	SyncContext { last_sync_end_count, current_count, .. }: SyncContext<'_>,
	room_id: &RoomId,
) -> Result<ShortStateHashes> {
	// the room state at the end of this sync range.
	// next_shortstatehash(N) finds the first event after N and returns its
	// pre-state = the correct post-state at count N.
	// For idle rooms (no events after current_count), it fails and we fall back to
	// the global current state since the state hasn't changed.
	let current_hash = async {
		match services
			.rooms
			.timeline
			.next_shortstatehash(room_id, PduCount::Normal(current_count))
			.await
		{
			| Ok(h) => Ok(h),
			| Err(_) => services.rooms.state.get_room_shortstatehash(room_id).await,
		}
	};

	// the room state as of the end of the last sync.
	let next_hash = async {
		let hash = match last_sync_end_count {
			| Some(last_sync_end_count) => services
				.rooms
				.timeline
				.prev_shortstatehash(
					room_id,
					PduCount::Normal(last_sync_end_count.saturating_add(1)),
				)
				.await
				.ok(),
			| None => None,
		};
		Ok::<_, Error>(hash)
	};

	let (current_shortstatehash, next_hash) = try_join(current_hash, next_hash).await?;

	// the room state as of the end of the last sync. if next_shortstatehash
	// returned None (no events after last_sync_end_count), we fall back to
	// current_shortstatehash IF the room actually existed at that count.
	// if the room is brand new to this sync stream, we keep it as None so
	// that we correctly trigger an initial state sync.
	let last_sync_end_shortstatehash = next_hash;

	trace!(
		"fetch_shortstatehashes: room={room_id} last_count={last_sync_end_count:?} \
		 current={current_shortstatehash} last_end={last_sync_end_shortstatehash:?}",
	);

	Ok(ShortStateHashes {
		current_shortstatehash,
		last_sync_end_shortstatehash,
	})
}

/// Fetch recent timeline events.
#[tracing::instrument(level = "debug", skip_all)]
async fn build_timeline(
	services: &Services,
	sync_context: SyncContext<'_>,
	room_id: &RoomId,
	joined_since_last_sync: bool,
) -> Result<TimelinePdus> {
	let SyncContext {
		syncing_user,
		last_sync_end_count,
		current_count,
		filter,
		..
	} = sync_context;

	/*
	determine the maximum number of events to return in this sync.
	if the sync filter specifies a limit, that will be used, otherwise
	`DEFAULT_TIMELINE_LIMIT` will be used. `DEFAULT_TIMELINE_LIMIT` will also be
	used if the limit is somehow greater than usize::MAX.
	*/
	let timeline_limit = filter
		.room
		.timeline
		.limit
		.and_then(|limit| limit.try_into().ok())
		.unwrap_or(DEFAULT_TIMELINE_LIMIT);

	let starting_count = if joined_since_last_sync {
		None
	} else {
		last_sync_end_count.map(PduCount::Normal)
	};

	load_timeline(
		services,
		syncing_user,
		room_id,
		starting_count,
		Some(PduCount::Normal(current_count)),
		timeline_limit,
		false,
	)
	.await
}

/// Calculate the state events to sync.
async fn build_state_events(
	services: &Services,
	sync_context: SyncContext<'_>,
	room_id: &RoomId,
	shortstatehashes: ShortStateHashes,
	timeline: &TimelinePdus,
	joined_since_last_sync: bool,
) -> Result<Vec<PduEvent>> {
	let SyncContext {
		syncing_user,
		last_sync_end_count,
		full_state,
		..
	} = sync_context;

	let ShortStateHashes {
		current_shortstatehash,
		last_sync_end_shortstatehash,
	} = shortstatehashes;

	// the spec states that the `state` property only includes state events up to
	// the beginning of the timeline, so we determine the state of the syncing room
	// as of the first timeline event. NOTE: this explanation is not entirely
	// accurate; see the implementation of `build_state_incremental`.
	let timeline_start_shortstatehash = async {
		if let Some((_, pdu)) = timeline.pdus.front() {
			if let Ok(shortstatehash) = services
				.rooms
				.state_accessor
				.pdu_shortstatehash(&pdu.event_id)
				.await
			{
				// If the timeline starts exactly at the beginning of the room (like in spaces)
				// the state before that first event will be completely empty. In this case,
				// or if we fail to resolve, we use current_shortstatehash to ensure clients
				// get the m.room.create event in their initial sync state.
				if !services
					.rooms
					.state_accessor
					.state_is_empty(shortstatehash)
					.await
				{
					return shortstatehash;
				}
			}
		}

		current_shortstatehash
	};

	// the user IDs of members whose membership needs to be sent to the client, if
	// lazy-loading is enabled.
	let lazily_loaded_members =
		prepare_lazily_loaded_members(services, sync_context, room_id, timeline.members());

	let (timeline_start_shortstatehash, lazily_loaded_members) =
		join(timeline_start_shortstatehash, lazily_loaded_members).await;

	// compute the state delta between the previous sync and this sync.
	match (last_sync_end_count, last_sync_end_shortstatehash) {
		/*
		if `last_sync_end_count` is Some (meaning this is an incremental sync), and `last_sync_end_shortstatehash`
		is Some (meaning the syncing user didn't just join this room for the first time ever), and `full_state` is false,
		AND the user didn't join since the last sync (which would require a full state block),
		then use `build_state_incremental`.
		*/
		| (Some(last_sync_end_count), Some(last_sync_end_shortstatehash))
			if !full_state && !joined_since_last_sync =>
			build_state_incremental(
				services,
				syncing_user,
				room_id,
				PduCount::Normal(last_sync_end_count),
				last_sync_end_shortstatehash,
				timeline_start_shortstatehash,
				current_shortstatehash,
				timeline,
				lazily_loaded_members.as_ref(),
			)
			.boxed()
			.await,
		/*
		otherwise use `build_state_initial`. note that this branch will be taken if the user joined this room since the last sync
		for the first time ever, because in that case we have no `last_sync_end_shortstatehash` and can't correctly calculate
		the state using the incremental sync algorithm.
		*/
		| _ =>
			build_state_initial(
				services,
				syncing_user,
				timeline_start_shortstatehash,
				lazily_loaded_members.as_ref(),
			)
			.boxed()
			.await,
	}
}

/// Calculate the state events after the timeline for MSC4222.
async fn build_state_after(
	services: &Services,
	sync_context: SyncContext<'_>,
	room_id: &RoomId,
	shortstatehashes: ShortStateHashes,
	timeline: &TimelinePdus,
) -> Result<Vec<PduEvent>> {
	if !services.config.experimental_features.msc4222_enabled || !sync_context.use_state_after {
		return Ok(Vec::new());
	}

	let SyncContext { syncing_user, .. } = sync_context;
	let ShortStateHashes { current_shortstatehash, .. } = shortstatehashes;

	// the user IDs of members whose membership needs to be sent to the client, if
	// lazy-loading is enabled.
	let lazily_loaded_members =
		prepare_lazily_loaded_members(services, sync_context, room_id, timeline.members()).await;

	build_state_initial(
		services,
		syncing_user,
		current_shortstatehash,
		lazily_loaded_members.as_ref(),
	)
	.boxed()
	.await
}

/// Compute the number of unread notifications in this room.
#[tracing::instrument(level = "debug", skip_all)]
async fn build_notification_counts(
	services: &Services,
	SyncContext { syncing_user, last_sync_end_count, .. }: SyncContext<'_>,
	room_id: &RoomId,
	timeline: &TimelinePdus,
) -> Result<Option<UnreadNotificationsCount>> {
	// determine whether to actually update the notification counts
	let should_send_notification_counts = async {
		// if we're going to sync some timeline events, the notification count has
		// definitely changed to include them
		if !timeline.pdus.is_empty() {
			return true;
		}

		// if this is an initial sync, we need to send notification counts because the
		// client doesn't know what they are yet
		let Some(last_sync_end_count) = last_sync_end_count else {
			return true;
		};

		let last_notification_read = services
			.rooms
			.user
			.last_notification_read(syncing_user, room_id)
			.await;

		// if the syncing user has read the events we sent during the last sync, we need
		// to send a new notification count on this sync.
		if last_notification_read > last_sync_end_count {
			return true;
		}

		// otherwise, nothing's changed.
		false
	};

	if should_send_notification_counts.await {
		let (notification_count, highlight_count) = join(
			services
				.rooms
				.user
				.notification_count(syncing_user, room_id)
				.map(TryInto::try_into)
				.unwrap_or(uint!(0)),
			services
				.rooms
				.user
				.highlight_count(syncing_user, room_id)
				.map(TryInto::try_into)
				.unwrap_or(uint!(0)),
		)
		.await;

		trace!(%notification_count, %highlight_count, "syncing new notification counts");

		Ok(Some(UnreadNotificationsCount {
			notification_count: Some(notification_count),
			highlight_count: Some(highlight_count),
		}))
	} else {
		Ok(None)
	}
}

/// Check if the syncing user joined the room since their last incremental sync.
#[tracing::instrument(level = "debug", skip_all)]
async fn check_joined_since_last_sync(
	services: &Services,
	room_id: &RoomId,
	ShortStateHashes {
		last_sync_end_shortstatehash,
		current_shortstatehash,
	}: ShortStateHashes,
	SyncContext {
		syncing_user,
		last_sync_end_count,
		current_count,
		..
	}: SyncContext<'_>,
) -> Result<bool> {
	// TODO: If the requesting user got state-reset out of the room, this
	// will be `true` when it shouldn't be. this function should never be called
	// in that situation, but it may be if the membership cache didn't get updated.
	// the root cause of this needs to be addressed
	let joined_since_last_sync = if last_sync_end_count.is_some() {
		// Incremental sync
		// fetch the syncing user's membership event during the last sync.
		// this will be None if `previous_sync_end_shortstatehash` is None.
		let membership_during_previous_sync: Option<RoomMemberEventContent> =
			match last_sync_end_shortstatehash {
				| Some(last_sync_end_shortstatehash) => services
					.rooms
					.state_accessor
					.state_get_content(
						last_sync_end_shortstatehash,
						&StateEventType::RoomMember,
						syncing_user.as_str(),
					)
					.await
					.inspect_err(|_| debug_warn!("User has no previous membership"))
					.ok(),
				| None => None,
			};

		let membership_during_current_sync: Option<RoomMemberEventContent> = services
			.rooms
			.state_accessor
			.state_get_content(
				current_shortstatehash,
				&StateEventType::RoomMember,
				syncing_user.as_str(),
			)
			.await
			.ok();

		let prev_membership = membership_during_previous_sync
			.as_ref()
			.map(|c| c.membership.clone());
		let curr_membership = membership_during_current_sync
			.as_ref()
			.map(|c| c.membership.clone());

		let prelim_joined_since_last_sync = prev_membership
			.as_ref()
			.is_none_or(|m| m != &MembershipState::Join)
			&& curr_membership
				.as_ref()
				.is_some_and(|m| m == &MembershipState::Join);

		let mut join_count = None;
		if prelim_joined_since_last_sync && last_sync_end_count.is_some() {
			if let Ok(event_id) = services
				.rooms
				.state_accessor
				.state_get_id::<ruma::OwnedEventId>(
					current_shortstatehash,
					&StateEventType::RoomMember,
					syncing_user.as_str(),
				)
				.await
			{
				if let Ok(PduCount::Normal(jc)) =
					services.rooms.timeline.get_pdu_count(&event_id).await
				{
					join_count = Some(jc);
				}
			}
		}

		let joined_since_last_sync = calculate_joined_since_last_sync(
			prev_membership.as_ref(),
			curr_membership.as_ref(),
			last_sync_end_count,
			join_count,
		);

		if joined_since_last_sync && membership_during_previous_sync.is_some() {
			warn!(
				%room_id,
				user_joined_since_last_sync = syncing_user.as_str(),
				?last_sync_end_shortstatehash,
				last_sync_end_count,
				current_count,
				membership = ?membership_during_previous_sync,
			);
		}

		joined_since_last_sync
	} else {
		// Initial sync
		false
	};

	Ok(joined_since_last_sync)
}

/// Build the `summary` field of the room object, which includes
/// the number of joined and invited users and the room's heroes.
#[tracing::instrument(level = "debug", skip_all)]
async fn build_room_summary(
	services: &Services,
	SyncContext { syncing_user, .. }: SyncContext<'_>,
	room_id: &RoomId,
	ShortStateHashes { current_shortstatehash, .. }: ShortStateHashes,
	timeline: &TimelinePdus,
	state_events: &[PduEvent],
	joined_since_last_sync: bool,
) -> Result<Option<RoomSummary>> {
	// determine whether any events in the state or timeline are membership events.
	let are_syncing_membership_events = timeline
		.pdus
		.iter()
		.map(|(_, pdu)| pdu)
		.chain(state_events.iter())
		.any(|event| event.kind == RoomMember);

	/*
	we only need to send an updated room summary if:
	1. there are membership events in the state or timeline, because they might have changed the
	   membership counts or heroes, or
	2. the syncing user just joined this room, which usually implies #1 because their join event should be in the timeline.
	*/
	if !(are_syncing_membership_events || joined_since_last_sync) {
		return Ok(None);
	}

	let joined_member_count = services
		.rooms
		.state_cache
		.room_joined_count(room_id)
		.unwrap_or(0);

	let invited_member_count = services
		.rooms
		.state_cache
		.room_invited_count(room_id)
		.unwrap_or(0);

	let has_name = services
		.rooms
		.state_accessor
		.state_contains_type(current_shortstatehash, &StateEventType::RoomName);

	let has_canonical_alias = services
		.rooms
		.state_accessor
		.state_contains_type(current_shortstatehash, &StateEventType::RoomCanonicalAlias);

	let (joined_member_count, invited_member_count, has_name, has_canonical_alias) =
		join4(joined_member_count, invited_member_count, has_name, has_canonical_alias).await;

	// only send heroes if the room has neither a name nor a canonical alias
	let heroes = if !(has_name || has_canonical_alias) {
		Some(build_heroes(services, room_id, syncing_user, current_shortstatehash).await)
	} else {
		None
	};

	trace!(
		%joined_member_count,
		%invited_member_count,
		heroes_length = heroes.as_ref().map(HashSet::len),
		"syncing updated summary"
	);

	Ok(Some(RoomSummary {
		heroes: heroes
			.map(|heroes| heroes.into_iter().collect())
			.unwrap_or_default(),
		joined_member_count: Some(ruma_from_u64(joined_member_count)),
		invited_member_count: Some(ruma_from_u64(invited_member_count)),
	}))
}

/// Fetch the user IDs to include in the `m.heroes` property of the room
/// summary.
async fn build_heroes(
	services: &Services,
	room_id: &RoomId,
	syncing_user: &UserId,
	current_shortstatehash: ShortStateHash,
) -> HashSet<OwnedUserId> {
	const MAX_HERO_COUNT: usize = 5;

	// fetch joined members from the state cache first
	let joined_members_stream = services
		.rooms
		.state_cache
		.room_members(room_id)
		.map(ToOwned::to_owned);

	// then fetch invited members
	let invited_members_stream = services
		.rooms
		.state_cache
		.room_members_invited(room_id)
		.map(ToOwned::to_owned);

	// then as a last resort fetch every membership event
	let all_members_stream = services
		.rooms
		.short
		.multi_get_statekey_from_short(
			services
				.rooms
				.state_accessor
				.state_full_shortids(current_shortstatehash)
				.ignore_err()
				.ready_filter_map(|(key, _)| Some(key)),
		)
		.ignore_err()
		.ready_filter_map(|(event_type, state_key)| {
			if event_type == StateEventType::RoomMember {
				state_key.to_string().try_into().ok()
			} else {
				None
			}
		});

	joined_members_stream
		.chain(invited_members_stream)
		.chain(all_members_stream)
		// the hero list should never include the syncing user
		.ready_filter(|user_id| user_id != syncing_user)
		.take(MAX_HERO_COUNT)
		.collect()
		.await
}

/// Collect updates to users' device lists for E2EE.
#[tracing::instrument(level = "debug", skip_all)]
async fn build_device_list_updates(
	services: &Services,
	SyncContext {
		syncing_user,
		last_sync_end_count,
		current_count,
		..
	}: SyncContext<'_>,
	room_id: &RoomId,
	ShortStateHashes { .. }: ShortStateHashes,
	timeline: &TimelinePdus,
	state_events: &[PduEvent],
	_joined_since_last_sync: bool,
) -> Result<DeviceListUpdates> {
	// initial syncs don't include device updates, so return early
	if last_sync_end_count.is_none() {
		return Ok(DeviceListUpdates::new());
	}

	let mut device_list_updates = DeviceListUpdates::new();

	// add users with changed keys to the `changed` list
	services
		.users
		.room_keys_changed(room_id, last_sync_end_count, Some(current_count))
		.map(at!(0))
		.map(ToOwned::to_owned)
		.ready_for_each(|user_id| {
			device_list_updates.changed.insert(user_id);
		})
		.await;

	// add users who now share encrypted rooms to `changed` and
	// users who no longer share encrypted rooms to `left`
	let mem_events = timeline
		.pdus
		.iter()
		.map(|(_, pdu)| pdu)
		.chain(state_events.iter())
		.filter(|event| event.kind == RoomMember);

	for state_event in mem_events {
		let Some(content): Option<RoomMemberEventContent> = state_event.get_content().ok() else {
			continue;
		};

		let Some(user_id): Option<OwnedUserId> = state_event
			.state_key
			.as_ref()
			.and_then(|key| key.parse().ok())
		else {
			continue;
		};

		{
			use MembershipState::*;

			if matches!(content.membership, Leave | Join) {
				let shares_room =
					shares_a_room(services, syncing_user, &user_id, Some(room_id)).await;
				match content.membership {
					| Leave if !shares_room => {
						device_list_updates.left.insert(user_id);
					},
					| Join => {
						device_list_updates.changed.insert(user_id);
					},
					| _ => (),
				}
			}
		}
	}

	if !device_list_updates.is_empty() {
		trace!(
			changed = device_list_updates.changed.len(),
			left = device_list_updates.left.len(),
			"syncing device list updates"
		);
	}

	Ok(device_list_updates)
}

#[inline]
pub(super) fn calculate_joined_since_last_sync(
	membership_during_previous_sync: Option<&MembershipState>,
	membership_during_current_sync: Option<&MembershipState>,
	last_sync_end_count: Option<u64>,
	join_count: Option<u64>,
) -> bool {
	let mut joined_since_last_sync = membership_during_previous_sync
		.is_none_or(|m| m != &MembershipState::Join)
		&& membership_during_current_sync.is_some_and(|m| m == &MembershipState::Join);

	if joined_since_last_sync {
		if let Some(last_sync_end_count) = last_sync_end_count {
			if let Some(join_count) = join_count {
				if join_count <= last_sync_end_count {
					joined_since_last_sync = false;
				}
			}
		}
	}

	joined_since_last_sync
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_joined_since_last_sync_idle_room_joined_before() {
		// If last_sync_end_shortstatehash is None (idle room),
		// membership_during_previous_sync is None. If the user's current membership
		// is Join, and they joined at count 50, but last sync ended at 100.
		// Since 50 <= 100, they did NOT join since the last sync.
		assert!(!calculate_joined_since_last_sync(
			None,
			Some(&MembershipState::Join),
			Some(100),
			Some(50),
		));
	}

	#[test]
	fn test_joined_since_last_sync_idle_room_joined_after() {
		// If last_sync_end_shortstatehash is None, and the user joined at count 150,
		// and last sync ended at 100. Since 150 > 100, they DID join since the last
		// sync.
		assert!(calculate_joined_since_last_sync(
			None,
			Some(&MembershipState::Join),
			Some(100),
			Some(150),
		));
	}

	#[test]
	fn test_joined_since_last_sync_normal_incremental_join() {
		// User was previously Left (not Join), and is now Join. They joined since the
		// last sync.
		assert!(calculate_joined_since_last_sync(
			Some(&MembershipState::Leave),
			Some(&MembershipState::Join),
			Some(100),
			Some(150),
		));
	}

	#[test]
	fn test_joined_since_last_sync_normal_incremental_already_joined() {
		// User was already Join, and is still Join. They did NOT join since the last
		// sync.
		assert!(!calculate_joined_since_last_sync(
			Some(&MembershipState::Join),
			Some(&MembershipState::Join),
			Some(100),
			Some(150),
		));
	}

	#[test]
	fn test_joined_since_last_sync_initial_sync() {
		// Initial sync (last_sync_end_count is None).
		// Note that check_joined_since_last_sync resolves to false on initial sync
		// anyway, but the helper function should handle the None last_sync_end_count
		// correctly.
		assert!(
			calculate_joined_since_last_sync(None, Some(&MembershipState::Join), None, None,)
		);
	}
}

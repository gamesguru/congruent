mod joined;
mod left;
mod state;

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	time::Duration,
};

use axum::{extract::State, response::IntoResponse};
use axum_client_ip::ClientIp;
use conduwuit::{
	Result, at, extract_variant,
	matrix::pdu::PduCount,
	utils::{
		ReadyExt, TryFutureExtExt,
		stream::{Tools, WidebandExt},
	},
	warn,
};
use conduwuit_service::Services;
use futures::{
	FutureExt, StreamExt, TryFutureExt,
	future::{OptionFuture, join3, join4},
};
use ruma::{
	DeviceId, OwnedRoomId, OwnedUserId, RoomId, UserId,
	api::{
		OutgoingResponse,
		client::{
			filter::FilterDefinition,
			sync::sync_events::{
				self, DeviceLists,
				v3::{
					Filter, GlobalAccountData, InviteState, InvitedRoom, KnockState, KnockedRoom,
					Presence, Rooms, ToDevice,
				},
			},
			uiaa::UiaaResponse,
		},
	},
	events::{
		AnyGlobalAccountDataEvent, AnyRawAccountDataEvent,
		presence::{PresenceEvent, PresenceEventContent},
	},
	serde::Raw,
};
use service::rooms::lazy_loading::{self, MemberSet, Options as _};

use super::load_timeline;
use crate::{
	Ruma, RumaResponse,
	client::{
		is_ignored_invite,
		sync::v3::{joined::load_joined_room, left::load_left_room},
	},
};

/// The default maximum number of events to return in the `timeline` key of
/// joined and left rooms. If the number of events sent since the last sync
/// exceeds this number, the `timeline` will be `limited`.
const DEFAULT_TIMELINE_LIMIT: usize = 10;

/// A collection of updates to users' device lists, used for E2EE.
#[derive(Clone)]
struct DeviceListUpdates {
	changed: HashSet<OwnedUserId>,
	left: HashSet<OwnedUserId>,
}

impl DeviceListUpdates {
	fn new() -> Self {
		Self {
			changed: HashSet::new(),
			left: HashSet::new(),
		}
	}

	fn merge(&mut self, other: Self) {
		self.changed.extend(other.changed);
		self.left.extend(other.left);
	}

	fn is_empty(&self) -> bool { self.changed.is_empty() && self.left.is_empty() }
}

impl From<DeviceListUpdates> for DeviceLists {
	fn from(val: DeviceListUpdates) -> Self {
		Self {
			changed: val.changed.into_iter().collect(),
			left: val.left.into_iter().collect(),
		}
	}
}

/// References to common data needed to calculate the sync response.
#[derive(Clone, Copy)]
struct SyncContext<'a> {
	/// The ID of the user requesting this sync.
	syncing_user: &'a UserId,
	/// The ID of the device requesting this sync, which will belong to
	/// `syncing_user`.
	syncing_device: &'a DeviceId,
	/// The global count at the end of the previous sync response.
	/// The previous sync's `current_count` will become the next sync's
	/// `last_sync_end_count`. This will be None if no `since` query parameter
	/// was specified, indicating an initial sync.
	last_sync_end_count: Option<u64>,
	/// The global count as of when we started building the sync response.
	/// This is used as an upper bound when querying the database to ensure the
	/// response represents a snapshot in time and doesn't include data which
	/// appeared while the response was being built.
	current_count: u64,
	/// The `full_state` query parameter, used when syncing state for joined and
	/// left rooms.
	full_state: bool,
	/// The sync filter, which the client uses to specify what data should be
	/// included in the sync response.
	filter: &'a FilterDefinition,
	/// Whether MSC4222 state_after was requested by the client.
	use_state_after: bool,
}

impl<'a> SyncContext<'a> {
	fn lazy_loading_context(&self, room_id: &'a RoomId) -> lazy_loading::Context<'a> {
		lazy_loading::Context {
			user_id: self.syncing_user,
			device_id: Some(self.syncing_device),
			room_id,
			token: self.last_sync_end_count,
			options: Some(&self.filter.room.state.lazy_load_options),
		}
	}

	#[inline]
	fn lazy_loading_enabled(&self) -> bool {
		(self.filter.room.state.lazy_load_options.is_enabled()
			|| self.filter.room.timeline.lazy_load_options.is_enabled())
			&& !self.full_state
	}
}

type PresenceUpdates = HashMap<OwnedUserId, PresenceEventContent>;

/// # `GET /_matrix/client/r0/sync`
///
/// Synchronize the client's state with the latest state on the server.
///
/// - This endpoint takes a `since` parameter which should be the `next_batch`
///   value from a previous request for incremental syncs.
///
/// Calling this endpoint without a `since` parameter returns:
/// - Some of the most recent events of each timeline
/// - Notification counts for each room
/// - Joined and invited member counts, heroes
/// - All state events
///
/// Calling this endpoint with a `since` parameter from a previous `next_batch`
/// returns: For joined rooms:
/// - Some of the most recent events of each timeline that happened after since
/// - If user joined the room after since: All state events (unless lazy loading
///   is activated) and all device list updates in that room
/// - If the user was already in the room: A list of all events that are in the
///   state now, but were not in the state at `since`
/// - If the state we send contains a member event: Joined and invited member
///   counts, heroes
/// - Device list updates that happened after `since`
/// - If there are events in the timeline we send or the user send updated his
///   read mark: Notification counts
/// - EDUs that are active now (read receipts, typing updates, presence)
/// - TODO: Allow multiple sync streams to support Pantalaimon
///
/// For invited rooms:
/// - If the user was invited after `since`: A subset of the state of the room
///   at the point of the invite
///
/// For left rooms:
/// - If the user left after `since`: `prev_batch` token, empty state (TODO:
///   subset of the state at the point of the leave)
#[tracing::instrument(
	name = "sync",
	level = "debug",
	skip_all,
	fields(
		since = %body.body.since.as_deref().unwrap_or_default(),
    )
)]
pub(crate) async fn sync_events_route(
	State(services): State<crate::State>,
	ClientIp(client_ip): ClientIp,
	axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
	body: Ruma<sync_events::v3::Request>,
) -> Result<axum::response::Response, RumaResponse<UiaaResponse>> {
	let timer = std::time::Instant::now();
	let (sender_user, sender_device) = body.sender();

	// Presence update
	if services.config.allow_local_presence
		&& body.body.set_presence != ruma::presence::PresenceState::Offline
	{
		services
			.presence
			.ping_presence(sender_user, &body.body.set_presence)
			.await?;
	}

	// Increment the "device last active" metadata
	services
		.users
		.update_device_last_seen(sender_user, Some(sender_device), client_ip)
		.await;

	// Setup watchers, so if there's no response, we can wait for them
	let watcher = services.sync.setup_watch(sender_user, sender_device).await;

	let mut use_state_after = false;
	if let Some(q) = raw_query.as_deref() {
		for (key, value) in url::form_urlencoded::parse(q.as_bytes()) {
			if key == "use_state_after" && value == "true" {
				use_state_after = true;
				break;
			}
		}
	}

	let log_time = |response: &serde_json::Value| {
		if !is_sync_response_empty(response) && timer.elapsed().as_millis() > 1000 {
			// log syncs if they took > 1s
			conduwuit::info!(
				"Large sync for {} completed in {:.2} s",
				sender_user,
				timer.elapsed().as_secs_f64()
			);
		}
	};

	let response = build_sync_events(&services, &body, use_state_after).await?;
	if body.body.since.is_none() || body.body.full_state || !is_sync_response_empty(&response) {
		log_time(&response);
		return Ok(axum::Json(response).into_response());
	}

	// Hang until new info arrives, or the client's timeout expires. A single
	// wake can be spurious -- a write to a watched prefix that produces no
	// visible sync delta (count bumps, an invite-sender write, expired
	// typing) -- so loop rather than treating one wake as authoritative and
	// returning an empty 200. Each iteration re-arms the watcher *before*
	// rebuilding, matching the arm-before-read ordering above; re-arming
	// after the build would reopen the TOCTOU fixed in c8f9083c9.
	if let Some(timeout) = body.body.timeout {
		if timeout > Duration::from_secs(0) {
			let Some(deadline) = timer.checked_add(timeout) else {
				log_time(&response);
				return Ok(axum::Json(response).into_response());
			};
			let mut watcher = watcher;
			while let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now())
			{
				if tokio::time::timeout(remaining, watcher).await.is_err() {
					break;
				}

				watcher = services.sync.setup_watch(sender_user, sender_device).await;
				let response = build_sync_events(&services, &body, use_state_after).await?;
				if !is_sync_response_empty(&response) {
					log_time(&response);
					return Ok(axum::Json(response).into_response());
				}
			}

			// Deadline hit without ever producing a non-empty response.
			let response = build_sync_events(&services, &body, use_state_after).await?;
			log_time(&response);
			return Ok(axum::Json(response).into_response());
		}
	}

	log_time(&response);
	Ok(axum::Json(response).into_response())
}

fn is_sync_response_empty(val: &serde_json::Value) -> bool {
	let Some(obj) = val.as_object() else {
		return true;
	};

	if obj.contains_key("presence")
		|| obj.contains_key("account_data")
		|| obj.contains_key("to_device")
	{
		return false;
	}

	match obj.get("device_lists") {
		| Some(serde_json::Value::Object(d)) => {
			let changed_empty = d
				.get("changed")
				.is_none_or(|c| c.as_array().is_none_or(Vec::is_empty));

			let left_empty = d
				.get("left")
				.is_none_or(|l| l.as_array().is_none_or(Vec::is_empty));

			if !changed_empty || !left_empty {
				return false;
			}
		},
		// device_lists present but unexpectedly shaped: play it safe and treat
		// the response as non-empty rather than risking an endless long-poll.
		| Some(_) => return false,
		| None => {},
	}

	// A room is only a real sync delta if it carries timeline/state/account-data/
	// ephemeral content. A joined room serialized *solely* for a stale
	// unread-notification count (empty timeline, no state/account-data/ephemeral,
	// no summary, unchanged next_batch) is NOT a real delta: returning it early
	// forces the client into a second round-trip to fetch the actual event,
	// compounding latency and dropping the event out of the live-timeline window
	// (this is the server-side half of the TestSpoofedEventSenderHandling race).
	// Treat such rooms as empty and keep the long-poll waiting for the real event.
	if let Some(rooms) = obj.get("rooms").and_then(|r| r.as_object()) {
		let joined_has_delta = rooms
			.get("join")
			.and_then(|j| j.as_object())
			.is_some_and(|joins| joins.values().any(joined_room_has_delta));

		let invited_has_delta = rooms
			.get("invite")
			.is_some_and(|i| !i.as_object().is_none_or(serde_json::Map::is_empty));

		let knocked_has_delta = rooms
			.get("knock")
			.is_some_and(|k| !k.as_object().is_none_or(serde_json::Map::is_empty));

		let left_has_delta = rooms
			.get("leave")
			.and_then(|l| l.as_object())
			.is_some_and(|leaves| leaves.values().any(left_room_has_delta));

		if joined_has_delta || invited_has_delta || knocked_has_delta || left_has_delta {
			return false;
		}
	}

	true
}

/// A joined room counts as a real sync delta if it carries any content beyond a
/// bare unread-notification count (and the always-serialized empty timeline).
fn joined_room_has_delta(room: &serde_json::Value) -> bool {
	let Some(room) = room.as_object() else {
		return false;
	};

	// Timeline events.
	if let Some(events) = room
		.get("timeline")
		.and_then(|t| t.get("events"))
		.and_then(|e| e.as_array())
	{
		if !events.is_empty() {
			return true;
		}
	} else if room.get("timeline").is_some() {
		// timeline present without events -> unexpected, treat as delta.
		return true;
	}

	// Gappy sync (`limited: true`).
	if room
		.get("timeline")
		.and_then(|t| t.get("limited"))
		.and_then(serde_json::Value::as_bool)
		.is_some_and(|limited| limited)
	{
		return true;
	}

	// State / account-data / ephemeral events.
	for key in ["state", "account_data", "ephemeral"] {
		if let Some(events) = room
			.get(key)
			.and_then(|o| o.get("events"))
			.and_then(|e| e.as_array())
		{
			if !events.is_empty() {
				return true;
			}
		} else if room.get(key).is_some() {
			return true;
		}
	}

	// Summary (membership counts etc.).
	room
		.get("summary")
		.and_then(|s| s.as_object())
		.is_some_and(|s| !s.is_empty())
}

/// A left room counts as a real sync delta if it carries timeline/state/
/// account-data content (minus the always-serialized empty timeline).
fn left_room_has_delta(room: &serde_json::Value) -> bool {
	let Some(room) = room.as_object() else {
		return false;
	};

	for key in ["timeline", "state", "account_data"] {
		if let Some(events) = room
			.get(key)
			.and_then(|o| o.get("events"))
			.and_then(|e| e.as_array())
		{
			if !events.is_empty() {
				return true;
			}
		} else if room.get(key).is_some() {
			return true;
		}
	}

	false
}

pub(crate) async fn build_sync_events(
	services: &Services,
	body: &Ruma<sync_events::v3::Request>,
	use_state_after: bool,
) -> Result<serde_json::Value, RumaResponse<UiaaResponse>> {
	let (syncing_user, syncing_device) = body.sender();

	let current_count = services.globals.current_count()?;

	// the `since` token is the last sync end count stringified
	let last_sync_end_count = body
		.body
		.since
		.as_ref()
		.and_then(|string| string.parse().ok());

	let full_state = body.body.full_state;

	// FilterDefinition is very large (0x1000 bytes), let's put it on the heap
	let filter = Box::new(match body.body.filter.as_ref() {
		// use the default filter if none was specified
		| None => FilterDefinition::default(),
		// use inline filters directly
		| Some(Filter::FilterDefinition(filter)) => filter.clone(),
		// look up filter IDs from the database
		| Some(Filter::FilterId(filter_id)) =>
			if filter_id.starts_with('{') {
				serde_json::from_str(filter_id).unwrap_or_else(|e| {
					conduwuit::warn!("Failed to parse inline filter JSON: {}", e);
					FilterDefinition::default()
				})
			} else {
				services
					.users
					.get_filter(syncing_user, filter_id)
					.await
					.unwrap_or_default()
			},
	});

	let context = SyncContext {
		syncing_user,
		syncing_device,
		last_sync_end_count,
		current_count,
		full_state,
		filter: &filter,
		use_state_after,
	};

	let is_initial_sync = last_sync_end_count.is_none();
	let joined_rooms = services
		.rooms
		.state_cache
		.rooms_joined(syncing_user)
		.map(ToOwned::to_owned)
		.map(|room_id| async move {
			let joined_room = load_joined_room(services, context, room_id.clone()).await;

			match joined_room {
				| Ok((room, state_after, updates)) => Some((room_id, room, state_after, updates)),
				| Err(err) => {
					warn!(?err, %room_id, "error loading joined room");
					None
				},
			}
		})
		.buffer_unordered(10)
		.filter_map(std::future::ready)
		.ready_fold(
			(BTreeMap::new(), BTreeMap::new(), DeviceListUpdates::new()),
			|(mut joined_rooms, mut joined_state_after, mut all_updates),
			 (room_id, joined_room, state_after, updates)| {
				all_updates.merge(updates);

				// During initial sync, include every joined room so the client
				// is aware of all rooms. During incremental sync, skip rooms
				// with no updates to reduce response size.
				if is_initial_sync || !joined_room.is_empty() {
					joined_rooms.insert(room_id.clone(), joined_room);
					if !state_after.is_empty() {
						joined_state_after.insert(room_id, state_after);
					}
				} else {
					conduwuit::info!(
						target: "timeline_debug",
						"incremental sync skipping empty room {} for {}",
						room_id, context.syncing_user
					);
				}

				(joined_rooms, joined_state_after, all_updates)
			},
		);

	let left_rooms = services
		.rooms
		.state_cache
		.rooms_left(syncing_user)
		.map(|(room_id, leave_pdu)| async move {
			let left_room =
				Box::pin(load_left_room(services, context, room_id.clone(), leave_pdu)).await;

			match left_room {
				| Ok(Some((room, state_after))) => Some((room_id, room, state_after)),
				| Ok(None) => None,
				| Err(err) => {
					warn!(?err, %room_id, "error loading joined room");
					None
				},
			}
		})
		.buffer_unordered(10)
		.filter_map(std::future::ready)
		.fold(
			(BTreeMap::new(), BTreeMap::new()),
			|(mut left_rooms, mut left_state_after), (room_id, left_room, state_after)| async move {
				left_rooms.insert(room_id.clone(), left_room);
				if !state_after.is_empty() {
					left_state_after.insert(room_id, state_after);
				}
				(left_rooms, left_state_after)
			},
		);

	let invited_rooms = services
		.rooms
		.state_cache
		.rooms_invited(syncing_user)
		.wide_filter_map(|(room_id, invite_state)| async move {
			if is_ignored_invite(services, syncing_user, &room_id).await {
				None
			} else {
				Some((room_id, invite_state))
			}
		})
		.fold_default(|mut invited_rooms: BTreeMap<_, _>, (room_id, invite_state)| async move {
			let invite_count = services
				.rooms
				.state_cache
				.get_invite_count(&room_id, syncing_user)
				.await;
			let invite_count = match invite_count {
				| Ok(invite_count) => Some(invite_count),
				| Err(err) => {
					warn!(
						target: "sync_invite_debug",
						%room_id,
						%syncing_user,
						?err,
						"invite state present with no invite count"
					);
					None
				},
			};

			// only sync this invite if it was sent after the last /sync call
			let include_invite = match (last_sync_end_count, invite_count) {
				| (None, _) | (_, None) => true,
				| (Some(last_sync_end_count), Some(invite_count)) =>
					last_sync_end_count < invite_count,
			};

			if include_invite {
				conduwuit::info!(
					target: "sync_invite_debug",
					%room_id,
					%syncing_user,
					?invite_count,
					?last_sync_end_count,
					current_count,
					"including room in invite section"
				);
				let invited_room = InvitedRoom {
					invite_state: InviteState { events: invite_state },
				};

				invited_rooms.insert(room_id, invited_room);
			} else {
				conduwuit::info!(
					target: "sync_invite_debug",
					%room_id,
					%syncing_user,
					?invite_count,
					?last_sync_end_count,
					current_count,
					"skipping room from invite section (invite already seen in an earlier sync)"
				);
			}
			invited_rooms
		});

	let knocked_rooms = services
		.rooms
		.state_cache
		.rooms_knocked(syncing_user)
		.fold_default(|mut knocked_rooms: BTreeMap<_, _>, (room_id, knock_state)| async move {
			let knock_count = services
				.rooms
				.state_cache
				.get_knock_count(&room_id, syncing_user)
				.await;
			let knock_count = match knock_count {
				| Ok(knock_count) => Some(knock_count),
				| Err(err) => {
					warn!(
						target: "knock_debug",
						%room_id,
						%syncing_user,
						?err,
						"knock state present with no knock count"
					);
					None
				},
			};

			tracing::info!(
				target: "knock_debug",
				"get_knock_count for room_id={} user_id={} returned {:?} last_sync_end_count={:?}",
				room_id, syncing_user, knock_count, last_sync_end_count
			);

			// only sync this knock if it was sent after the last /sync call
			let include_knock = match (last_sync_end_count, knock_count) {
				| (None, _) | (_, None) => true,
				| (Some(last_sync_end_count), Some(knock_count)) =>
					last_sync_end_count < knock_count,
			};

			if include_knock {
				let knocked_room = KnockedRoom {
					knock_state: KnockState { events: knock_state },
				};

				knocked_rooms.insert(room_id, knocked_room);
			}
			knocked_rooms
		});

	let (joined_rooms, left_rooms, invited_rooms, knocked_rooms) =
		join4(joined_rooms, left_rooms, invited_rooms, knocked_rooms).await;

	let (joined_rooms, joined_state_after, mut device_list_updates) = joined_rooms;
	let (left_rooms, left_state_after) = left_rooms;

	for room_id in joined_rooms.keys() {
		if invited_rooms.contains_key(room_id) {
			conduwuit::warn!(
				target: "sync_invite_debug",
				%room_id,
				%syncing_user,
				?last_sync_end_count,
				current_count,
				"room appears in both joined and invite sync sections"
			);
		}
	}

	for room_id in left_rooms.keys() {
		if invited_rooms.contains_key(room_id) {
			conduwuit::warn!(
				target: "sync_invite_debug",
				%room_id,
				%syncing_user,
				?last_sync_end_count,
				current_count,
				"room appears in both left and invite sync sections"
			);
		}
	}

	let presence_updates: OptionFuture<_> = services
		.config
		.allow_local_presence
		.then(|| process_presence_updates(services, last_sync_end_count, syncing_user))
		.into();

	let account_data: Vec<Raw<AnyGlobalAccountDataEvent>> = services
		.account_data
		.changes_since(None, syncing_user, last_sync_end_count, Some(current_count))
		.ready_filter_map(|e| extract_variant!(e, AnyRawAccountDataEvent::Global))
		.collect()
		.await;

	// Look for device list updates of this account
	let keys_changed = services
		.users
		.keys_changed(syncing_user, last_sync_end_count, None)
		.map(ToOwned::to_owned)
		.collect::<HashSet<_>>();

	let to_device_events = services
		.users
		.get_to_device_events(
			syncing_user,
			syncing_device,
			last_sync_end_count,
			Some(current_count),
		)
		.map(at!(1))
		.collect::<Vec<_>>();

	let device_one_time_keys_count = services
		.users
		.count_one_time_keys(syncing_user, syncing_device);
	let unused_fallback_key_types = services
		.users
		.list_unused_fallback_key_types(syncing_user, syncing_device);

	// Remove all to-device events the device received *last time*
	let remove_to_device_events =
		services
			.users
			.remove_to_device_events(syncing_user, syncing_device, last_sync_end_count);

	let ephemeral = join3(remove_to_device_events, to_device_events, presence_updates);
	let top = join3(ephemeral, device_one_time_keys_count, keys_changed)
		.boxed()
		.await;

	let (ephemeral, device_one_time_keys_count, keys_changed) = top;
	let ((), to_device_events, presence_updates) = ephemeral;
	let unused_fallback_key_types = unused_fallback_key_types.await;
	device_list_updates.changed.extend(keys_changed);

	if last_sync_end_count.is_some() {
		device_list_updates.left.extend(
			services
				.users
				.device_list_left(syncing_user, last_sync_end_count, Some(current_count))
				.map(|(user_id, _)| user_id)
				.map(ToOwned::to_owned)
				.collect::<Vec<_>>()
				.await,
		);
	}

	// A leave and subsequent visibility-restoring update (for example, an invite
	// arriving before the next /sync response is built) can otherwise surface as
	// both `left` and `changed` for the same user in one response. Prefer the
	// current visibility-restoring state and suppress the stale `left`.
	let changed_users = device_list_updates.changed.iter().collect::<HashSet<_>>();
	device_list_updates
		.left
		.retain(|user_id| !changed_users.contains(user_id));

	let mut presence_updates = presence_updates.unwrap_or_default();
	if services.config.allow_local_presence {
		collect_member_presence(
			services,
			syncing_user,
			last_sync_end_count,
			&joined_rooms,
			&mut presence_updates,
		)
		.await;
	}

	let device_lists_json = (!device_list_updates.is_empty()).then(|| {
		serde_json::json!({
			"changed": device_list_updates.changed.iter().collect::<Vec<_>>(),
			"left": device_list_updates.left.iter().collect::<Vec<_>>(),
		})
	});

	let ruma_response = sync_events::v3::Response {
		next_batch: current_count.to_string(),
		rooms: Rooms {
			leave: left_rooms,
			join: joined_rooms,
			invite: invited_rooms,
			knock: knocked_rooms.clone(),
		},
		presence: Presence {
			events: presence_updates
				.into_iter()
				.map(|(sender, content)| PresenceEvent { content, sender })
				.map(|ref event| Raw::new(event))
				.filter_map(Result::ok)
				.collect(),
		},
		account_data: GlobalAccountData { events: account_data },
		to_device: ToDevice { events: to_device_events },
		device_lists: device_list_updates.into(),
		device_one_time_keys_count,
		device_unused_fallback_key_types: Some(unused_fallback_key_types),
	};

	let mut val: serde_json::Value = serde_json::from_slice(
		ruma_response
			.try_into_http_response::<bytes::BytesMut>()
			.expect("ruma response is valid")
			.body(),
	)
	.expect("ruma response is valid JSON");

	// Manually insert state_after data for MSC4222 and inject missing ephemeral
	// objects
	if let Some(join) = val.get_mut("rooms").and_then(|r| r.get_mut("join")) {
		// inject state_after
		for (room_id, state_after) in joined_state_after {
			if let Some(room) = join.get_mut(room_id.as_str()) {
				let state_after_obj = serde_json::json!({ "events": state_after });
				room.as_object_mut()
					.unwrap()
					.insert("state_after".to_owned(), state_after_obj.clone());
				room.as_object_mut()
					.unwrap()
					.insert("org.matrix.msc4222.state_after".to_owned(), state_after_obj);
			}
		}

		// inject missing ephemeral to satisfy complement
		for (_room_id, room_val) in join.as_object_mut().unwrap() {
			let room = room_val.as_object_mut().unwrap();
			if !room.contains_key("ephemeral") {
				room.insert("ephemeral".to_owned(), serde_json::json!({ "events": [] }));
			}

			if is_initial_sync && !room.contains_key("account_data") {
				room.insert("account_data".to_owned(), serde_json::json!({ "events": [] }));
			}
		}
	}

	if let Some(leave) = val.get_mut("rooms").and_then(|r| r.get_mut("leave")) {
		for (room_id, state_after) in left_state_after {
			if let Some(room) = leave.get_mut(room_id.as_str()) {
				let state_after_obj = serde_json::json!({ "events": state_after });
				room.as_object_mut()
					.unwrap()
					.insert("state_after".to_owned(), state_after_obj.clone());
				room.as_object_mut()
					.unwrap()
					.insert("org.matrix.msc4222.state_after".to_owned(), state_after_obj);
			}
		}
	}

	// ruma's Rooms::is_empty() ignores knock, so when only knocked rooms exist the
	// entire "rooms" key is omitted from the serialized output. Manually inject it
	// so clients receive rooms.knock and the sync token advances.
	if !knocked_rooms.is_empty() && val.get("rooms").is_none_or(|r| r.get("knock").is_none()) {
		if let Ok(knock_val) = serde_json::to_value(&knocked_rooms) {
			let rooms_obj = val.as_object_mut().and_then(|o| {
				o.entry("rooms")
					.or_insert_with(|| serde_json::json!({}))
					.as_object_mut()
			});
			if let Some(rooms) = rooms_obj {
				rooms.insert("knock".to_owned(), knock_val);
			}
		}
	}

	// Ruma may omit non-empty device_lists during serialization in some edge
	// cases. Re-inject the computed payload so /sync cannot lose a one-shot
	// device-list update between internal assembly and the final JSON body.
	if let Some(device_lists_json) = device_lists_json {
		tracing::info!(
			changed = device_lists_json
				.get("changed")
				.and_then(|v| v.as_array())
				.map_or(0, Vec::len),
			left = device_lists_json
				.get("left")
				.and_then(|v| v.as_array())
				.map_or(0, Vec::len),
			"sync response device_lists"
		);

		if let Some(obj) = val.as_object_mut() {
			obj.insert("device_lists".to_owned(), device_lists_json);
		}
	}

	Ok(val)
}

/// Collect presence updates for users relevant to the current sync window.
///
/// This gathers presence for:
/// 1. Members of rooms the syncing user has newly joined since their last sync
/// 2. Users who joined any room in the current sync's timeline
///
/// Presence is only fetched for users not already in `presence_updates` and
/// excludes the syncing user themselves.
#[tracing::instrument(name = "member_presence", level = "debug", skip_all)]
async fn collect_member_presence(
	services: &Services,
	syncing_user: &UserId,
	last_sync_end_count: Option<u64>,
	joined_rooms: &BTreeMap<OwnedRoomId, sync_events::v3::JoinedRoom>,
	presence_updates: &mut PresenceUpdates,
) {
	use ruma::events::{
		StateEventType,
		room::member::{MembershipState, RoomMemberEventContent},
	};

	let mut extra_users = HashSet::new();

	// Phase 1: Collect users from rooms the syncing user newly joined
	if let Some(last_sync_end_count) = last_sync_end_count {
		for room_id in joined_rooms.keys() {
			let shortstatehash = services
				.rooms
				.timeline
				.next_shortstatehash(room_id, PduCount::Normal(last_sync_end_count))
				.await
				.ok();

			let was_joined = match shortstatehash {
				| Some(ssh) => services
					.rooms
					.state_accessor
					.state_get_content::<RoomMemberEventContent>(
						ssh,
						&StateEventType::RoomMember,
						syncing_user.as_str(),
					)
					.await
					.is_ok_and(|c| c.membership == MembershipState::Join),
				| None => false,
			};

			if !was_joined {
				services
					.rooms
					.state_cache
					.room_members(room_id)
					.map(ToOwned::to_owned)
					.ready_for_each(|uid| {
						extra_users.insert(uid);
					})
					.await;
			}
		}
	}

	// Phase 2: Collect users whose join events appear in the timeline
	for joined_room in joined_rooms.values() {
		collect_timeline_join_users(&joined_room.timeline.events, &mut extra_users);
	}

	// Phase 3: Fetch presence for collected users (skip self and already-known)
	for user_id in extra_users {
		if user_id != syncing_user {
			if let std::collections::hash_map::Entry::Vacant(e) = presence_updates.entry(user_id)
			{
				if let Ok(presence_event) = services.presence.get_presence(e.key()).await {
					e.insert(presence_event.content);
				}
			}
		}
	}
}

/// Extract user IDs from join membership events in a list of timeline events.
///
/// This is a pure function (no I/O) for testability: given raw timeline events,
/// it parses each one looking for `m.room.member` events with `membership:
/// "join"` and collects the `state_key` (the user who joined).
fn collect_timeline_join_users(
	events: &[Raw<ruma::events::AnySyncTimelineEvent>],
	users: &mut HashSet<OwnedUserId>,
) {
	#[derive(serde::Deserialize)]
	struct MemberHelper {
		#[serde(rename = "type")]
		event_type: String,
		content: Option<MemberContent>,
		state_key: Option<String>,
	}

	#[derive(serde::Deserialize)]
	struct MemberContent {
		membership: String,
	}

	for event in events {
		if let Ok(helper) = event.deserialize_as::<MemberHelper>() {
			if helper.event_type == "m.room.member" {
				if let Some(content) = helper.content {
					if content.membership == "join" {
						if let Some(ref state_key) = helper.state_key {
							if let Ok(user_id) = UserId::parse(state_key) {
								users.insert(user_id.to_owned());
							}
						}
					}
				}
			}
		}
	}
}

#[tracing::instrument(name = "presence", level = "debug", skip_all)]
async fn process_presence_updates(
	services: &Services,
	last_sync_end_count: Option<u64>,
	syncing_user: &UserId,
) -> PresenceUpdates {
	services
		.presence
		.presence_since(last_sync_end_count.unwrap_or(0)) // send all presences on initial sync
		.filter(|(user_id, ..)| {
			services
				.rooms
				.state_cache
				.user_sees_user(syncing_user, user_id)
		})
		.filter_map(|(user_id, _, presence_bytes)| {
			services
				.presence
				.from_json_bytes_to_event(presence_bytes, user_id)
				.map_ok(move |event| (user_id, event))
				.ok()
		})
		.map(|(user_id, event)| (user_id.to_owned(), event.content))
		.collect()
		.await
}

/// Using the provided sync context and an iterator of user IDs in the
/// `timeline`, return a HashSet of user IDs whose membership events should be
/// sent to the client if lazy-loading is enabled.
#[allow(clippy::let_and_return)]
async fn prepare_lazily_loaded_members(
	services: &Services,
	sync_context: SyncContext<'_>,
	room_id: &RoomId,
	timeline_members: impl Iterator<Item = OwnedUserId>,
) -> Option<MemberSet> {
	let lazy_loading_context = &sync_context.lazy_loading_context(room_id);

	// reset lazy loading state on initial sync.
	// do this even if lazy loading is disabled so future lazy loads
	// will have the correct members.
	if sync_context.last_sync_end_count.is_none() {
		services
			.rooms
			.lazy_loading
			.reset(lazy_loading_context)
			.await;
	}

	// filter the input members through `retain_lazy_members`, which
	// contains the actual lazy loading logic.
	let mut lazily_loaded_members =
		OptionFuture::from(sync_context.lazy_loading_enabled().then(|| {
			services
				.rooms
				.lazy_loading
				.retain_lazy_members(timeline_members.collect(), lazy_loading_context)
		}))
		.await;

	// Matrix spec requires that the syncing user's own membership event is always
	// included in the state, even if it otherwise would not be included due to
	// lazy-loading!
	if let Some(members) = &mut lazily_loaded_members {
		members.insert(sync_context.syncing_user.into());
	}

	lazily_loaded_members
}

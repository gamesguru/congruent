use std::{
	collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
	time::Duration,
};

use conduwuit::{
	Err, Error, Result, at, err, error, extract_variant, is_equal_to,
	matrix::{Event, TypeStateKey, pdu::PduCount},
	trace,
	utils::{
		BoolExt, FutureBoolExt, IterStream, ReadyExt, TryFutureExtExt,
		future::ReadyEqExt,
		math::{ruma_from_usize, usize_from_ruma},
		stream::WidebandExt,
	},
	warn,
};
use conduwuit_service::{
	Services,
	rooms::read_receipt::pack_receipts,
	sync::{CompatListFilters, CompatRequiredStateExcludes, into_snake_key},
};
use futures::{
	FutureExt, StreamExt, TryFutureExt,
	future::{OptionFuture, join3, try_join4},
	pin_mut,
};
use slipstream::{
	DeviceId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UInt, UserId,
	api::{
		EndpointRequest,
		client::sync::sync_events::{self, DeviceLists, UnreadNotificationsCount},
	},
	codec::{DeError, Deserialize},
	directory::RoomTypeFilter,
	endpoint::EndpointResponse,
	events::{
		AnyRawAccountDataEvent, AnySyncEphemeralRoomEvent, AnySyncStateEvent,
		GlobalAccountDataEventType, RoomAccountDataEventType, StateEventType, TimelineEventType,
		direct::DirectEvent,
		room::member::{MembershipState, RoomMemberEventContent},
		space::child::SpaceChildEventContent,
		tag::TagEvent,
		typing::TypingEventContent,
	},
	json::{Object, Value},
	presence::PresenceState,
	sswire::Raw,
	uint,
};

use super::{json_response, share_encrypted_room};
use crate::{
	Ruma,
	client::{
		DEFAULT_BUMP_TYPES, TimelinePdus, ignored_filter, is_ignored_invite, sync::load_timeline,
	},
	router::{
		ApiError,
		extract::{ClientIp, State},
	},
};

type SyncInfo<'a> = (&'a UserId, &'a DeviceId, u64, u64, &'a sync_events::v5::Request);
type TodoRooms = BTreeMap<OwnedRoomId, (RequiredStateSelection, usize, u64)>;
type KnownRooms = BTreeMap<String, BTreeMap<OwnedRoomId, u64>>;
type RoomExtras = BTreeMap<OwnedRoomId, RoomExtra>;
type CompatRanges = Vec<(UInt, UInt)>;

#[derive(Clone, Debug, Default)]
struct CompatRequiredState {
	include: Vec<(StateEventType, String)>,
	// `None` means the request didn't specify `exclude` for this selector (sticky:
	// keep whatever was cached from a previous request). `Some(vec![])` means the
	// client explicitly sent an empty exclude list, which must override -- and can
	// clear -- a previously cached sticky exclusion.
	exclude: Option<Vec<(StateEventType, String)>>,
}

/// One list's or room subscription's required_state request for a room.
/// Kept separate per selector (rather than merged into one include/exclude
/// pair) so an exclude from one list/subscription can't suppress state that
/// another list/subscription for the same room explicitly asked to include.
#[derive(Debug, Default)]
struct RequiredStateSelector {
	include: BTreeSet<TypeStateKey>,
	exclude: BTreeSet<TypeStateKey>,
}

#[derive(Debug, Default)]
struct RequiredStateSelection {
	selectors: Vec<RequiredStateSelector>,
}

impl RequiredStateSelection {
	fn is_empty(&self) -> bool { self.selectors.iter().all(|s| s.include.is_empty()) }

	fn push<I, E>(&mut self, include: I, exclude: E)
	where
		I: IntoIterator<Item = TypeStateKey>,
		E: IntoIterator<Item = TypeStateKey>,
	{
		let include: BTreeSet<_> = include.into_iter().collect();
		if include.is_empty() {
			return;
		}

		self.selectors.push(RequiredStateSelector {
			include,
			exclude: exclude.into_iter().collect(),
		});
	}
}

#[derive(Clone, Copy)]
enum SyncEndpoint {
	StableV5,
	UnstableMsc3575,
}

impl SyncEndpoint {
	fn stores_connection_without_id(self) -> bool { matches!(self, Self::StableV5) }

	fn validates_exact_pos(self) -> bool { matches!(self, Self::StableV5) }

	fn enforces_stable_limits(self) -> bool { matches!(self, Self::StableV5) }
}

#[derive(Clone, Copy)]
struct CachePolicy {
	endpoint: SyncEndpoint,
	persist: bool,
}

impl CachePolicy {
	fn should_store(self, conn_id: Option<&String>) -> bool {
		self.persist && (self.endpoint.stores_connection_without_id() || conn_id.is_some())
	}
}

struct BuildContext<'a> {
	sender_user: &'a UserId,
	sender_device: &'a DeviceId,
	globalsince: u64,
	body: &'a sync_events::v5::Request,
	list_filters: Option<&'a BTreeMap<String, CompatListFilters>>,
	required_state_excludes: Option<&'a CompatRequiredStateExcludes>,
	known_rooms: &'a KnownRooms,
	timeline_limits: &'a BTreeMap<OwnedRoomId, usize>,
	endpoint: SyncEndpoint,
	persist_cache: bool,
}

#[derive(Default)]
struct CompatRequest {
	pos: Option<String>,
	conn_id: Option<String>,
	txn_id: Option<String>,
	timeout: Option<Duration>,
	set_presence: PresenceState,
	lists: BTreeMap<String, CompatList>,
	room_subscriptions: BTreeMap<OwnedRoomId, CompatRoomSubscription>,
	extensions: sync_events::v5::request::Extensions,
}

#[derive(Clone, Debug, Default)]
struct CompatList {
	ranges: CompatRanges,
	room_details: CompatRoomDetails,
	include_heroes: Option<bool>,
	filters: Option<CompatListFilters>,
}

#[derive(Clone, Debug, Default)]
struct CompatRoomSubscription {
	required_state: CompatRequiredState,
	timeline_limit: UInt,
	include_heroes: Option<bool>,
}

#[derive(Clone, Debug, Default)]
struct CompatRoomDetails {
	required_state: CompatRequiredState,
	timeline_limit: UInt,
}

fn expect_object(value: &Value) -> Result<&Object, DeError> {
	value
		.as_object()
		.ok_or_else(|| DeError("expected a JSON object".to_owned()))
}

/// The named field, or its default when the key is absent. A key that is
/// present must parse, including `null`, as with the serde `default`
/// attribute this replaces.
fn field<T: Deserialize + Default>(object: &Object, key: &str) -> Result<T, DeError> {
	object
		.get(key)
		.map_or_else(|| Ok(T::default()), T::from_json)
}

impl Deserialize for CompatRequest {
	fn from_json(value: &Value) -> Result<Self, DeError> {
		let object = expect_object(value)?;
		Ok(Self {
			pos: field(object, "pos")?,
			conn_id: field(object, "conn_id")?,
			txn_id: field(object, "txn_id")?,
			timeout: field::<Option<UInt>>(object, "timeout")?.map(Duration::from_millis),
			set_presence: match object.get("set_presence") {
				| Some(value) if !value.is_null() => PresenceState::from_json(value)?,
				| _ => PresenceState::Online,
			},
			lists: field(object, "lists")?,
			room_subscriptions: field(object, "room_subscriptions")?,
			extensions: field(object, "extensions")?,
		})
	}
}

fn required_state_field(object: &Object) -> Result<CompatRequiredState, DeError> {
	object
		.get("required_state")
		.map_or_else(|| Ok(CompatRequiredState::default()), parse_required_state)
}

impl Deserialize for CompatList {
	fn from_json(value: &Value) -> Result<Self, DeError> {
		let object = expect_object(value)?;
		Ok(Self {
			ranges: object
				.get("ranges")
				.or_else(|| object.get("range"))
				.map_or_else(|| Ok(CompatRanges::default()), parse_ranges)?,
			room_details: CompatRoomDetails {
				required_state: required_state_field(object)?,
				timeline_limit: field(object, "timeline_limit")?,
			},
			include_heroes: field(object, "include_heroes")?,
			filters: field(object, "filters")?,
		})
	}
}

impl Deserialize for CompatRoomSubscription {
	fn from_json(value: &Value) -> Result<Self, DeError> {
		let object = expect_object(value)?;
		Ok(Self {
			required_state: required_state_field(object)?,
			timeline_limit: field(object, "timeline_limit")?,
			include_heroes: field(object, "include_heroes")?,
		})
	}
}

impl From<CompatRequest> for sync_events::v5::Request {
	fn from(value: CompatRequest) -> Self {
		Self {
			pos: value.pos,
			conn_id: value.conn_id,
			txn_id: value.txn_id,
			timeout: value.timeout,
			lists: value
				.lists
				.into_iter()
				.map(|(list_id, list)| {
					(list_id, sync_events::v5::request::List {
						ranges: list.ranges,
						room_details: sync_events::v5::request::RoomDetails {
							required_state: list.room_details.required_state.include,
							timeline_limit: list.room_details.timeline_limit,
						},
						include_heroes: list.include_heroes,
						filters: list.filters.map(|filters| {
							sync_events::v5::request::ListFilters {
								is_invite: filters.is_invite,
								not_room_types: filters.not_room_types,
							}
						}),
					})
				})
				.collect(),
			room_subscriptions: value
				.room_subscriptions
				.into_iter()
				.map(|(room_id, room)| {
					(room_id, sync_events::v5::request::RoomSubscription {
						required_state: room.required_state.include,
						timeline_limit: room.timeline_limit,
						include_heroes: room.include_heroes,
					})
				})
				.collect(),
			extensions: value.extensions,
		}
	}
}

fn parse_range_entry(value: &Value) -> Result<(UInt, UInt), DeError> {
	match value {
		| Value::Array(_) => <(UInt, UInt)>::from_json(value),
		| Value::Object(object) => {
			let start = object
				.get("start")
				.ok_or_else(|| DeError("missing field `start`".to_owned()))?;
			let end = object
				.get("end")
				.ok_or_else(|| DeError("missing field `end`".to_owned()))?;
			Ok((UInt::from_json(start)?, UInt::from_json(end)?))
		},
		| _ => Err(DeError("expected a range".to_owned())),
	}
}

/// Accepts a list of ranges, one range, or an object of ranges keyed by
/// anything, in that order of preference.
fn parse_ranges(value: &Value) -> Result<CompatRanges, DeError> {
	let invalid = || DeError("data did not match any variant of CompatRangesRepr".to_owned());
	match value {
		| Value::Array(entries) => entries
			.iter()
			.map(parse_range_entry)
			.collect::<Result<_, _>>()
			.or_else(|_| parse_range_entry(value).map(|entry| vec![entry])),
		| Value::Object(entries) => {
			parse_range_entry(value)
				.map(|entry| vec![entry])
				.or_else(|_| {
					// `BTreeMap` iteration is key-ordered, matching the collected map.
					entries
						.values()
						.map(parse_range_entry)
						.collect::<Result<_, _>>()
				})
		},
		| _ => Err(invalid()),
	}
}

fn parse_required_state_entry(value: &Value) -> Result<(StateEventType, String), DeError> {
	match value {
		| Value::Array(_) => <(StateEventType, String)>::from_json(value),
		| Value::Object(object) => {
			let event_type = object
				.get("type")
				.or_else(|| object.get("event_type"))
				.ok_or_else(|| DeError("missing field `type`".to_owned()))?;
			let state_key = object
				.get("state_key")
				.ok_or_else(|| DeError("missing field `state_key`".to_owned()))?;
			Ok((StateEventType::from_json(event_type)?, String::from_json(state_key)?))
		},
		| _ => Err(DeError("expected a required_state entry".to_owned())),
	}
}

fn parse_required_state_entries(value: &Value) -> Result<Vec<(StateEventType, String)>, DeError> {
	match value {
		| Value::Array(entries) => entries.iter().map(parse_required_state_entry).collect(),
		| _ => Err(DeError("expected a list of required_state entries".to_owned())),
	}
}

/// The stable `{include, exclude, lazy_members}` form; unknown keys are
/// rejected.
fn parse_required_state_object(object: &Object) -> Result<CompatRequiredState, DeError> {
	if let Some(key) = object
		.keys()
		.find(|key| !["include", "exclude", "lazy_members"].contains(&key.as_str()))
	{
		return Err(DeError(format!("unknown field `{key}`")));
	}

	let mut include = object
		.get("include")
		.map_or_else(|| Ok(Vec::new()), parse_required_state_entries)?;

	if field::<bool>(object, "lazy_members")? {
		include.push((StateEventType::RoomMember, "$LAZY".to_owned()));
	}

	// `None` when `exclude` is absent or null, so omission stays distinct from
	// an explicit `[]` (see `CompatRequiredState`).
	let exclude = match object.get("exclude") {
		| None | Some(Value::Null) => None,
		| Some(entries) => Some(parse_required_state_entries(entries)?),
	};

	Ok(CompatRequiredState { include, exclude })
}

/// Accepts the stable object form, an object of entries, an object keyed by
/// event type, a list of entries, or a single entry.
fn parse_required_state(value: &Value) -> Result<CompatRequiredState, DeError> {
	let include = match value {
		| Value::Object(entries) => {
			let reserved = ["include", "exclude", "lazy_members"];
			if entries.keys().any(|key| reserved.contains(&key.as_str())) {
				return parse_required_state_object(entries);
			}

			let entries_by_key = entries
				.iter()
				.map(|(key, entry)| Ok((key.clone(), parse_required_state_entry(entry)?)))
				.collect::<Result<BTreeMap<String, _>, DeError>>();

			if let Ok(entries) = entries_by_key {
				entries.into_values().collect()
			} else {
				BTreeMap::<StateEventType, Vec<String>>::from_json(value)?
					.into_iter()
					.flat_map(|(event_type, state_keys)| {
						state_keys
							.into_iter()
							.map(move |state_key| (event_type.clone(), state_key))
					})
					.collect()
			}
		},
		| value => parse_required_state_entries(value)
			.or_else(|_| parse_required_state_entry(value).map(|entry| vec![entry]))
			.map_err(|_| {
				DeError("data did not match any variant of required_state".to_owned())
			})?,
	};

	Ok(CompatRequiredState { include, exclude: None })
}

fn required_state_excludes(
	entry: &(StateEventType, String),
	excludes: &BTreeSet<TypeStateKey>,
) -> bool {
	excludes.iter().any(|(event_type, state_key)| {
		(event_type == "*" || *event_type == entry.0)
			&& (state_key.as_str() == "*" || state_key.as_str() == entry.1)
	})
}

pub(crate) struct CompatSyncRequest {
	request: sync_events::v5::Request,
	list_filters: BTreeMap<String, CompatListFilters>,
	required_state_excludes: CompatRequiredStateExcludes,
	set_presence: PresenceState,
	thread_subscriptions_enabled: bool,
}

impl EndpointRequest for CompatSyncRequest {
	type Response = <sync_events::v5::Request as EndpointRequest>::Response;

	const METADATA: slipstream::api::Metadata =
		<sync_events::v5::Request as EndpointRequest>::METADATA;

	fn path_args(&self) -> Vec<String> { self.request.path_args() }

	fn query(&self) -> Vec<(String, String)> { self.request.query() }

	fn body(&self) -> Option<Value> { self.request.body() }

	fn from_parts(
		path: &[String],
		query: &[(String, String)],
		body: Option<&Value>,
	) -> Result<Self, DeError> {
		let thread_subscriptions_enabled = body
			.and_then(|body| {
				body.get("extensions")
					.and_then(|extensions| {
						extensions.get("io.element.msc4308.thread_subscriptions")
					})
					.and_then(|extension| extension.get("enabled"))
					.and_then(Value::as_bool)
			})
			.unwrap_or(false);
		let (request, list_filters, required_state_excludes, set_presence) =
			if let Some(body) = body {
				let compat = CompatRequest::from_json(body)?;
				let set_presence = compat.set_presence;
				let list_filters = compat
					.lists
					.iter()
					.filter_map(|(list_id, list)| {
						list.filters
							.clone()
							.map(|filters| (list_id.clone(), filters))
					})
					.collect();
				// Only carry a list/subscription's exclude into the sticky-override map if
				// the request actually specified `exclude` (`Some`, even if empty) --
				// that's what lets an explicit `"exclude": []` clear a previously cached
				// sticky exclusion instead of being indistinguishable from omission.
				let required_state_excludes = CompatRequiredStateExcludes {
					lists: compat
						.lists
						.iter()
						.filter_map(|(list_id, list)| {
							list.room_details
								.required_state
								.exclude
								.clone()
								.map(|exclude| (list_id.clone(), exclude))
						})
						.collect(),
					room_subscriptions: compat
						.room_subscriptions
						.iter()
						.filter_map(|(room_id, room)| {
							room.required_state
								.exclude
								.clone()
								.map(|exclude| (room_id.clone(), exclude))
						})
						.collect(),
				};
				(
					sync_events::v5::Request::from(compat),
					list_filters,
					required_state_excludes,
					set_presence,
				)
			} else {
				(
					sync_events::v5::Request::default(),
					BTreeMap::new(),
					CompatRequiredStateExcludes::default(),
					PresenceState::Online,
				)
			};

		let mut parsed = sync_events::v5::Request::from_parts(
			path,
			query,
			Some(&Value::Object(Object::new())),
		)?;

		if request.pos.is_some() {
			parsed.pos = request.pos;
		}
		parsed.conn_id = request.conn_id;
		parsed.txn_id = request.txn_id;
		parsed.timeout = request.timeout;
		parsed.lists = request.lists;
		parsed.room_subscriptions = request.room_subscriptions;
		parsed.extensions = request.extensions;

		Ok(Self {
			request: parsed,
			list_filters,
			required_state_excludes,
			set_presence,
			thread_subscriptions_enabled,
		})
	}
}

#[derive(Default)]
struct RoomExtra {
	lists: BTreeSet<String>,
	membership: Option<MembershipState>,
	expanded_timeline: bool,
	force_update: bool,
}

/// `POST /_matrix/client/v5/sync`
/// `POST /_matrix/client/unstable/org.matrix.simplified_msc3575/sync`
/// ([MSC4186])
///
/// A simplified version of sliding sync ([MSC3575]).
///
/// Get all new events in a sliding window of rooms since the last sync or a
/// given point in time.
///
/// [MSC3575]: https://github.com/matrix-org/matrix-spec-proposals/pull/3575
/// [MSC4186]: https://github.com/matrix-org/matrix-spec-proposals/pull/4186
pub(crate) async fn sync_events_v5_route(
	State(ref services): State<crate::State>,
	ClientIp(client_ip): ClientIp,
	body: Ruma<CompatSyncRequest>,
) -> std::result::Result<crate::router::response::Response, ApiError> {
	Box::pin(sync_events_v5_route_inner(services, client_ip, body, SyncEndpoint::StableV5))
		.await
		.map_err(Into::into)
}

pub(crate) async fn sync_events_unstable_msc3575_route(
	State(ref services): State<crate::State>,
	ClientIp(client_ip): ClientIp,
	body: Ruma<CompatSyncRequest>,
) -> std::result::Result<crate::router::response::Response, ApiError> {
	Box::pin(sync_events_v5_route_inner(
		services,
		client_ip,
		body,
		SyncEndpoint::UnstableMsc3575,
	))
	.await
	.map_err(Into::into)
}

async fn sync_events_v5_route_inner(
	services: &Services,
	client_ip: std::net::IpAddr,
	body: Ruma<CompatSyncRequest>,
	endpoint: SyncEndpoint,
) -> Result<crate::router::response::Response> {
	let sender_user = body.sender_user.as_ref().expect("user is authenticated");
	let sender_device = body.sender_device.as_ref().expect("user is authenticated");

	services
		.users
		.update_device_last_seen(sender_user, Some(sender_device), client_ip)
		.await;

	let CompatSyncRequest {
		mut request,
		mut list_filters,
		mut required_state_excludes,
		set_presence,
		thread_subscriptions_enabled,
	} = body.body;

	if endpoint.enforces_stable_limits() && request.lists.len() > 100 {
		return Err!(Request(InvalidParam("More than 100 lists are not supported.")));
	}

	if endpoint.enforces_stable_limits() && request.room_subscriptions.len() > 100 {
		return Err!(Request(InvalidParam(
			"More than 100 room subscriptions are not supported."
		)));
	}

	if services.config.allow_local_presence {
		services
			.presence
			.ping_presence(sender_user, &set_presence)
			.await?;
	}

	// Setup watchers, so if there's no response, we can wait for them
	let watcher = services.sync.setup_watch(sender_user, sender_device).await;

	let conn_id = request.conn_id.clone();

	let globalsince = match request.pos.as_ref() {
		| Some(pos) => pos.parse().map_err(|_| {
			err!(Request(UnknownPos(
				"Connection data unknown to server; restarting sync stream."
			)))
		})?,
		| None => 0,
	};

	let snake_key = into_snake_key(sender_user, sender_device, conn_id);

	let known_connection = if endpoint.validates_exact_pos() {
		services
			.sync
			.snake_connection_token_valid(&snake_key, globalsince)
	} else {
		services.sync.snake_connection_cached(&snake_key)
	};

	if globalsince != 0 && !known_connection {
		return Err!(Request(UnknownPos(
			"Connection data unknown to server; restarting sync stream."
		)));
	}

	// Client / User requested an initial sync
	if globalsince == 0 {
		services.sync.forget_snake_sync_connection(&snake_key);
	}

	// Get sticky parameters from cache
	let (known_rooms, timeline_limits) = services
		.sync
		.update_snake_sync_request_with_cache(&snake_key, &mut request);
	if endpoint.stores_connection_without_id() {
		services.sync.update_snake_compat_sticky(
			&snake_key,
			&mut list_filters,
			&mut required_state_excludes,
		);
	}
	let list_filters = endpoint
		.stores_connection_without_id()
		.then_some(&list_filters);
	let required_state_excludes = endpoint
		.stores_connection_without_id()
		.then_some(&required_state_excludes);
	let waits_for_updates = request
		.timeout
		.is_some_and(|timeout| timeout > Duration::from_secs(0));
	let mut context = BuildContext {
		sender_user,
		sender_device,
		globalsince,
		body: &request,
		list_filters,
		required_state_excludes,
		known_rooms: &known_rooms,
		timeline_limits: &timeline_limits,
		endpoint,
		persist_cache: !waits_for_updates,
	};

	let (mut response, mut room_extras) = build_sync_events_v5(services, &context).await?;

	if waits_for_updates {
		if response.rooms.is_empty() && response.extensions.is_empty() {
			if let Some(timeout) = request.timeout {
				// Hang until new info arrives, or the client's timeout expires. A
				// single wake can be spurious -- a write to a watched prefix that
				// produces no visible delta here -- so loop rather than treating
				// one wake as authoritative, matching the v3 sync fix. Re-arm the
				// watcher before each rebuild, not after, to keep the
				// arm-before-read ordering that avoids the TOCTOU fixed in
				// c8f9083c9.
				if let Some(deadline) = std::time::Instant::now().checked_add(timeout) {
					let mut watcher = watcher;
					while let Some(remaining) =
						deadline.checked_duration_since(std::time::Instant::now())
					{
						if conduwuit::timeout(remaining, watcher).await.is_err() {
							break;
						}

						watcher = services.sync.setup_watch(sender_user, sender_device).await;
						let (r, re) = build_sync_events_v5(services, &context).await?;
						response = r;
						// Read after the loop (or on the next iteration's break check);
						// clippy can't see across the loop boundary that this is used.
						#[allow(unused_assignments)]
						(room_extras = re);
						// A room can be present merely because its list entry was
						// rebuilt.  That is not necessarily the update which woke us:
						// writes made while a PDU is being appended can wake the watcher
						// before the new timeline row is visible.  Keep waiting in that
						// case so long-polling does not return an empty timeline.
						let has_room_update = response.rooms.values().any(|room| {
							!room.timeline.is_empty()
								|| !room.required_state.is_empty()
								|| room.invite_state.is_some()
						});
						if has_room_update || !response.extensions.is_empty() {
							break;
						}
					}
				}
			}
		}

		// Rebuild the response after waking up to avoid returning advanced tokens
		// without their associated events. The probe above intentionally did not
		// update sticky room state because no response had been delivered yet.
		context.persist_cache = true;
		(response, room_extras) = build_sync_events_v5(services, &context).await?;
	}

	trace!(
		rooms = ?response.rooms.len(),
		account_data = ?response.extensions.account_data.rooms.len(),
		receipts = ?response.extensions.receipts.rooms.len(),
		"responding to request with"
	);
	if endpoint.validates_exact_pos() {
		services
			.sync
			.update_snake_sync_pos(&snake_key, response.pos.parse().unwrap_or(globalsince));
	}
	sync_events_v5_json_response(
		&response,
		room_extras,
		collect_thread_subscriptions_extension(
			services,
			sender_user,
			globalsince,
			thread_subscriptions_enabled,
		)
		.await?,
	)
}

async fn build_sync_events_v5(
	services: &Services,
	context: &BuildContext<'_>,
) -> Result<(sync_events::v5::Response, RoomExtras)> {
	let BuildContext {
		sender_user,
		sender_device,
		globalsince,
		body,
		list_filters,
		required_state_excludes,
		known_rooms,
		timeline_limits,
		endpoint,
		persist_cache,
	} = *context;
	// See `globals::Service::edu_barrier`.
	let next_batch = {
		let _barrier = services.globals.edu_barrier.write().await;
		services.globals.current_count()?
	};

	let all_joined_rooms = services
		.rooms
		.state_cache
		.rooms_joined(sender_user)
		.collect::<Vec<OwnedRoomId>>();

	let all_invited_rooms = services
		.rooms
		.state_cache
		.rooms_invited(sender_user)
		.wide_filter_map(async |(room_id, invite_state)| {
			if is_ignored_invite(services, sender_user, &room_id).await {
				None
			} else {
				Some((room_id, invite_state))
			}
		})
		.map(|r| r.0)
		.collect::<Vec<OwnedRoomId>>();

	let all_knocked_rooms = services
		.rooms
		.state_cache
		.rooms_knocked(sender_user)
		.map(|r| r.0)
		.collect::<Vec<OwnedRoomId>>();
	let all_left_rooms = services
		.rooms
		.state_cache
		.rooms_left(sender_user)
		.ready_filter(|(room_id, pdu)| {
			pdu.as_ref().is_some_and(|pdu| pdu.sender != sender_user)
				|| known_rooms
					.values()
					.any(|rooms| rooms.contains_key(room_id))
		})
		.map(at!(0))
		.collect::<Vec<OwnedRoomId>>();

	let ((all_joined_rooms, all_invited_rooms, all_knocked_rooms), all_left_rooms) = futures::join!(
		join3(all_joined_rooms, all_invited_rooms, all_knocked_rooms),
		all_left_rooms
	);

	let all_joined_rooms = all_joined_rooms.iter().map(AsRef::as_ref);
	let all_invited_rooms = all_invited_rooms.iter().map(AsRef::as_ref);
	let all_knocked_rooms = all_knocked_rooms.iter().map(AsRef::as_ref);
	let all_left_rooms = all_left_rooms.iter().map(AsRef::as_ref);
	let all_rooms = all_joined_rooms
		.clone()
		.chain(all_invited_rooms.clone())
		.chain(all_knocked_rooms.clone())
		.chain(all_left_rooms.clone());

	let pos = next_batch.clone().to_string();

	let mut todo_rooms: TodoRooms = BTreeMap::new();

	let sync_info: SyncInfo<'_> = (sender_user, sender_device, globalsince, next_batch, body);

	let account_data = collect_account_data(services, sync_info).map(Ok);

	let e2ee = collect_e2ee(services, sync_info, all_joined_rooms.clone());

	let to_device = collect_to_device(services, sync_info, next_batch).map(Ok);

	let receipts = collect_receipts(services).map(Ok);

	let (account_data, e2ee, to_device, receipts) =
		try_join4(account_data, e2ee, to_device, receipts).await?;

	let extensions = sync_events::v5::response::Extensions {
		account_data,
		e2ee,
		to_device,
		receipts,
		typing: sync_events::v5::response::Typing::default(),
	};

	let mut response = sync_events::v5::Response {
		txn_id: body.txn_id.clone(),
		pos,
		lists: BTreeMap::new(),
		rooms: BTreeMap::new(),
		extensions,
	};
	let mut room_extras = RoomExtras::new();

	handle_lists(
		services,
		sync_info,
		next_batch,
		all_invited_rooms.clone(),
		all_joined_rooms.clone(),
		all_rooms,
		list_filters,
		required_state_excludes,
		endpoint,
		persist_cache,
		&mut todo_rooms,
		known_rooms,
		&mut response,
		&mut room_extras,
	)
	.await;

	fetch_subscriptions(
		services,
		sync_info,
		next_batch,
		known_rooms,
		required_state_excludes,
		CachePolicy { endpoint, persist: persist_cache },
		&mut todo_rooms,
	)
	.await;

	response.rooms = process_rooms(
		services,
		sender_user,
		next_batch,
		all_invited_rooms.clone(),
		&todo_rooms,
		&mut response,
		body,
		timeline_limits,
		&mut room_extras,
	)
	.await?;

	let typing = collect_typing_events(services, sender_user, body, &todo_rooms).await?;
	response.extensions.typing = typing;

	if persist_cache && (endpoint.stores_connection_without_id() || body.conn_id.is_some()) {
		// Save the current timeline limits back into our snake connections cache.
		let snake_key = into_snake_key(sender_user, sender_device, body.conn_id.clone());
		let next_limits: BTreeMap<OwnedRoomId, usize> = todo_rooms
			.iter()
			.map(|(room_id, (_, limit, roomsince))| {
				(
					room_id.clone(),
					effective_timeline_limit(room_id, *limit, *roomsince, timeline_limits),
				)
			})
			.collect();
		services
			.sync
			.update_snake_sync_timeline_limits(&snake_key, next_limits);
	}

	Ok((response, room_extras))
}

async fn fetch_subscriptions(
	services: &Services,
	(sender_user, sender_device, _, _, body): SyncInfo<'_>,
	next_batch: u64,
	known_rooms: &KnownRooms,
	required_state_excludes: Option<&CompatRequiredStateExcludes>,
	cache_policy: CachePolicy,
	todo_rooms: &mut TodoRooms,
) {
	let mut known_subscription_rooms = BTreeSet::new();
	for (room_id, room) in &body.room_subscriptions {
		let not_exists = services.rooms.metadata.exists(room_id).eq(&false);

		let is_disabled = services.rooms.metadata.is_disabled(room_id);

		let is_banned = services.rooms.metadata.is_banned(room_id);

		pin_mut!(not_exists, is_disabled, is_banned);
		if not_exists.or(is_disabled).or(is_banned).await {
			continue;
		}

		let todo_room = todo_rooms
			.entry(room_id.to_owned())
			.or_insert_with(|| (RequiredStateSelection::default(), 0_usize, u64::MAX));

		let limit: usize = usize_from_ruma(room.timeline_limit).min(100);

		let excludes = required_state_excludes
			.and_then(|excludes| excludes.room_subscriptions.get(room_id))
			.into_iter()
			.flatten()
			.cloned()
			.map(|(ty, sk)| (ty, sk.as_str().into()));
		todo_room.0.push(
			room.required_state
				.iter()
				.cloned()
				.map(|(ty, sk)| (ty, sk.as_str().into())),
			excludes,
		);
		todo_room.1 = todo_room.1.max(limit);
		// 0 means unknown because it got out of date
		todo_room.2 = todo_room.2.min(
			known_rooms
				.get("subscriptions")
				.and_then(|k| k.get(room_id))
				.copied()
				.unwrap_or(0),
		);
		known_subscription_rooms.insert(room_id.to_owned());
	}
	// where this went (protomsc says it was removed)
	//for r in body.unsubscribe_rooms {
	//	known_subscription_rooms.remove(&r);
	//	body.room_subscriptions.remove(&r);
	//}

	if cache_policy.should_store(body.conn_id.as_ref()) {
		let snake_key = into_snake_key(sender_user, sender_device, body.conn_id.clone());
		services.sync.update_snake_sync_known_rooms(
			&snake_key,
			"subscriptions".to_owned(),
			known_subscription_rooms,
			next_batch,
		);
	}
}

#[allow(clippy::too_many_arguments)]
async fn handle_lists<'a, Rooms, AllRooms>(
	services: &Services,
	(sender_user, sender_device, _, _, body): SyncInfo<'_>,
	next_batch: u64,
	all_invited_rooms: Rooms,
	all_joined_rooms: Rooms,
	all_rooms: AllRooms,
	list_filters: Option<&BTreeMap<String, CompatListFilters>>,
	required_state_excludes: Option<&CompatRequiredStateExcludes>,
	endpoint: SyncEndpoint,
	persist_cache: bool,
	todo_rooms: &'a mut TodoRooms,
	known_rooms: &'a KnownRooms,
	response: &'_ mut sync_events::v5::Response,
	room_extras: &mut RoomExtras,
) -> KnownRooms
where
	Rooms: Iterator<Item = &'a RoomId> + Clone + Send + 'a,
	AllRooms: Iterator<Item = &'a RoomId> + Clone + Send + 'a,
{
	let direct_rooms = direct_rooms_for_user(services, sender_user).await;
	let mut bump_timestamps = HashMap::new();
	for (list_id, list) in &body.lists {
		let active_rooms: Vec<_> = match list.filters.as_ref().and_then(|f| f.is_invite) {
			| None => all_rooms.clone().collect(),
			| Some(true) => all_invited_rooms.clone().collect(),
			| Some(false) => all_joined_rooms.clone().collect(),
		};
		let merged_filters =
			merge_list_filters(list.filters.as_ref(), list_filters.and_then(|f| f.get(list_id)));

		let active_rooms = filter_active_rooms(
			services,
			sender_user,
			merged_filters.as_ref(),
			&direct_rooms,
			active_rooms,
		)
		.await;

		let missing_rooms: Vec<_> = active_rooms
			.iter()
			.filter(|room| !bump_timestamps.contains_key::<RoomId>(*room))
			.map(|room| (*room).to_owned())
			.collect::<BTreeSet<_>>()
			.into_iter()
			.collect();

		let fetched_timestamps = missing_rooms
			.into_iter()
			.stream()
			.widen_then(10, |room_id| async move {
				let ts = match services.rooms.timeline.latest_pdu_in_room(&room_id).await {
					| Ok(pdu) => pdu.origin_server_ts().get(),
					| Err(_) => 0_u64,
				};
				(room_id, ts)
			})
			.collect::<Vec<_>>()
			.await;

		bump_timestamps.extend(fetched_timestamps);

		let mut active_rooms_with_ts = Vec::with_capacity(active_rooms.len());
		for room in active_rooms {
			active_rooms_with_ts.push((room, bump_timestamps.get(room).copied().unwrap_or(0)));
		}

		// Sort descending by timestamp (most recent first), then by room ID for a
		// deterministic order when multiple rooms have the same bump timestamp.
		active_rooms_with_ts.sort_by(|(room_a, ts_a), (room_b, ts_b)| {
			ts_b.cmp(ts_a)
				.then_with(|| room_a.as_str().cmp(room_b.as_str()))
		});
		let active_rooms: Vec<&RoomId> =
			active_rooms_with_ts.into_iter().map(|(r, _)| r).collect();

		let mut new_known_rooms: BTreeSet<OwnedRoomId> = BTreeSet::new();

		let ranges = list.ranges.clone();

		for mut range in ranges {
			range.0 = range
				.0
				.min(UInt::try_from(active_rooms.len()).unwrap_or(UInt::MAX));
			range.1 = range.1.checked_add(uint!(1)).unwrap_or(range.1);
			range.1 = range
				.1
				.clamp(range.0, UInt::try_from(active_rooms.len()).unwrap_or(UInt::MAX));

			let room_ids =
				active_rooms[usize_from_ruma(range.0)..usize_from_ruma(range.1)].to_vec();

			let new_rooms: BTreeSet<OwnedRoomId> =
				room_ids.clone().into_iter().map(From::from).collect();

			new_known_rooms.extend(new_rooms);
			for room_id in room_ids {
				room_extras
					.entry(room_id.to_owned())
					.or_default()
					.lists
					.insert(list_id.clone());

				let todo_room = todo_rooms
					.entry(room_id.to_owned())
					.or_insert_with(|| (RequiredStateSelection::default(), 0_usize, u64::MAX));

				let limit: usize = usize_from_ruma(list.room_details.timeline_limit).min(100);

				let excludes = required_state_excludes
					.and_then(|excludes| excludes.lists.get(list_id))
					.into_iter()
					.flatten()
					.cloned()
					.map(|(ty, sk)| (ty, sk.as_str().into()));
				todo_room.0.push(
					list.room_details
						.required_state
						.iter()
						.cloned()
						.map(|(ty, sk)| (ty, sk.as_str().into())),
					excludes,
				);

				todo_room.1 = todo_room.1.max(limit);
				// 0 means unknown because it got out of date
				todo_room.2 = todo_room.2.min(
					known_rooms
						.get(list_id.as_str())
						.and_then(|k| k.get(room_id))
						.copied()
						.unwrap_or(0),
				);
			}
		}

		if let Some(previous_rooms) = known_rooms.get(list_id.as_str()) {
			for (room_id, roomsince) in previous_rooms {
				if *roomsince == 0 || new_known_rooms.contains(room_id) {
					continue;
				}

				room_extras.entry(room_id.clone()).or_default().force_update = true;

				let todo_room = todo_rooms
					.entry(room_id.clone())
					.or_insert_with(|| (RequiredStateSelection::default(), 0_usize, u64::MAX));
				todo_room.2 = todo_room.2.min(*roomsince);
			}
		}

		response
			.lists
			.insert(list_id.clone(), sync_events::v5::response::List {
				count: ruma_from_usize(active_rooms.len()),
			});

		if persist_cache && (endpoint.stores_connection_without_id() || body.conn_id.is_some()) {
			let snake_key = into_snake_key(sender_user, sender_device, body.conn_id.clone());
			services.sync.update_snake_sync_known_rooms(
				&snake_key,
				list_id.clone(),
				new_known_rooms,
				next_batch,
			);
		}
	}

	BTreeMap::default()
}

#[allow(clippy::too_many_arguments)]
async fn process_rooms<'a, Rooms>(
	services: &Services,
	sender_user: &UserId,
	next_batch: u64,
	all_invited_rooms: Rooms,
	todo_rooms: &TodoRooms,
	response: &mut sync_events::v5::Response,
	body: &sync_events::v5::Request,
	timeline_limits: &BTreeMap<OwnedRoomId, usize>,
	room_extras: &mut RoomExtras,
) -> Result<BTreeMap<OwnedRoomId, sync_events::v5::response::Room>>
where
	Rooms: Iterator<Item = &'a RoomId> + Clone + Send + 'a,
{
	let mut rooms = BTreeMap::new();
	for (room_id, (required_state_request, timeline_limit, roomsince)) in todo_rooms {
		let roomsincecount = PduCount::Normal(*roomsince);
		let timeline_limit =
			effective_timeline_limit(room_id, *timeline_limit, *roomsince, timeline_limits);

		let is_expanded_timeline =
			is_expanded_timeline(timeline_limits.get(room_id).copied(), timeline_limit);

		let mut timestamp: Option<_> = None;
		let mut invite_state = None;
		let (timeline_pdus, limited, prev_batch);
		let room_id_v11: &RoomId = (*room_id).as_ref();
		if all_invited_rooms.clone().any(is_equal_to!(room_id_v11)) {
			// TODO: figure out a timestamp we can use for remote invites
			invite_state = services
				.rooms
				.state_cache
				.invite_state(sender_user, room_id)
				.await
				.ok();

			(timeline_pdus, limited, prev_batch) = (VecDeque::new(), true, None);
		} else {
			TimelinePdus { pdus: timeline_pdus, limited, prev_batch } = match load_timeline(
				services,
				sender_user,
				room_id,
				Some(roomsincecount),
				Some(PduCount::from(next_batch)),
				timeline_limit,
				is_expanded_timeline,
			)
			.await
			{
				| Ok(value) => value,
				| Err(err) => {
					warn!("Encountered missing timeline in {}, error {}", room_id, err);
					continue;
				},
			};
		}

		if body.extensions.account_data.enabled == Some(true) {
			response.extensions.account_data.rooms.insert(
				room_id.to_owned(),
				services
					.account_data
					.changes_since(Some(room_id), sender_user, Some(*roomsince), Some(next_batch))
					.ready_filter_map(|e| extract_variant!(e, AnyRawAccountDataEvent::Room))
					.collect()
					.await,
			);
		}

		let last_privateread_update = services
			.rooms
			.read_receipt
			.last_privateread_update(sender_user, room_id)
			.await;

		let private_read_event: OptionFuture<_> = (last_privateread_update > *roomsince)
			.then(|| {
				services
					.rooms
					.read_receipt
					.private_read_get(room_id, sender_user)
					.ok()
			})
			.into();

		let mut receipts: Vec<Raw<AnySyncEphemeralRoomEvent>> = services
			.rooms
			.read_receipt
			.readreceipts_since(room_id, Some(*roomsince))
			.filter_map(|(read_user, _ts, v)| async move {
				services
					.users
					.user_is_ignored(&read_user, sender_user)
					.await
					.or_some(v)
			})
			.collect()
			.await;

		if let Some(private_read_event) = private_read_event.await.flatten() {
			receipts.push(private_read_event);
		}

		let receipt_size = receipts.len();

		if receipt_size > 0 {
			response
				.extensions
				.receipts
				.rooms
				.insert(room_id.clone(), pack_receipts(Box::new(receipts.into_iter())));
		}

		let last_notification_read = services
			.rooms
			.user
			.last_notification_read(sender_user, room_id)
			.await;

		if roomsince != &0
			&& timeline_pdus.is_empty()
			&& response
				.extensions
				.account_data
				.rooms
				.get(room_id)
				.is_none_or(Vec::is_empty)
			&& last_notification_read <= *roomsince
			&& required_state_request.is_empty()
			&& !room_extras
				.get(room_id)
				.is_some_and(|extra| extra.force_update)
		{
			continue;
		}

		let live_count = timeline_pdus
			.iter()
			.filter(|(count, _)| matches!(count, PduCount::Normal(count) if count > roomsince))
			.count();
		let num_live = (live_count > 0).then(|| ruma_from_usize(live_count));

		let prev_batch = prev_batch
			.map_or(Ok::<_, Error>(None), |pdu_count| {
				Ok(Some(match pdu_count {
					| PduCount::Backfilled(_) => {
						error!("timeline in backfill state?!");
						"0".to_owned()
					},
					| PduCount::Normal(c) => c.to_string(),
				}))
			})?
			.or_else(|| {
				if roomsince != &0 {
					Some(roomsince.to_string())
				} else {
					None
				}
			});

		// Fetch the current HAMT root once and thread it through
		// `collect_required_state` so its accessors share a single root lookup
		// instead of re-resolving the room root per accessor. Rooms without
		// joined state (e.g. pending invites) yield `None` and collect nothing.
		let current_root_handle = services.rooms.state.get_room_state_hamt(room_id).await.ok();

		let required_state = collect_required_state(
			services,
			sender_user,
			room_id,
			current_root_handle,
			required_state_request,
			&timeline_pdus,
		)
		.await;

		let room_name_requested = required_state_request.selectors.iter().any(|selector| {
			selector.include.iter().any(|(event_type, state_key)| {
				*event_type == StateEventType::RoomName && matches!(state_key.as_str(), "" | "*")
			})
		});
		let include_stable_room_fields = body.pos.is_none()
			|| room_name_requested
			|| timeline_pdus
				.iter()
				.any(|(_, pdu)| pdu.event_type() == "m.room.name");

		let room_events: Vec<_> = timeline_pdus
			.iter()
			.stream()
			.filter_map(|item| ignored_filter(services, item.clone(), sender_user))
			.map(at!(1))
			.map(Event::into_format)
			.collect()
			.await;

		let membership = user_membership_for_sync(services, sender_user, room_id).await;

		let extra = room_extras.entry(room_id.clone()).or_default();
		extra.membership = membership;
		extra.expanded_timeline = is_expanded_timeline && !timeline_pdus.is_empty();

		let mut fallback_timestamp = None;
		for (_, pdu) in timeline_pdus {
			let ts = pdu.origin_server_ts;
			if fallback_timestamp.is_none_or(|time| time <= ts) {
				fallback_timestamp = Some(ts);
			}
			if DEFAULT_BUMP_TYPES.contains(&pdu.kind) && timestamp.is_none_or(|time| time <= ts) {
				timestamp = Some(ts);
			}
		}
		timestamp = timestamp.or(fallback_timestamp);

		// Heroes
		let heroes: Vec<_> = services
			.rooms
			.state_cache
			.room_members(room_id)
			.ready_filter(|member| *member != sender_user)
			.filter_map(|user_id| async move {
				services
					.rooms
					.state_accessor
					.get_member(room_id, &user_id)
					.map_ok(|memberevent| sync_events::v5::response::Hero {
						user_id: user_id.clone(),
						name: memberevent.displayname,
						avatar: memberevent.avatar_url,
					})
					.ok()
					.await
			})
			.take(5)
			.collect()
			.await;

		let heroes_avatar = if heroes.len() == 1 {
			heroes[0].avatar.clone()
		} else {
			None
		};
		let thread_counts = services
			.rooms
			.user
			.thread_notification_counts(sender_user, room_id)
			.await;
		let thread_total_notifications = thread_counts
			.values()
			.map(|(notifications, _)| *notifications)
			.fold(0_u64, u64::saturating_add);
		let thread_total_highlights = thread_counts
			.values()
			.map(|(_, highlights)| *highlights)
			.fold(0_u64, u64::saturating_add);
		let notification_count = services
			.rooms
			.user
			.notification_count(sender_user, room_id)
			.await
			.saturating_add(thread_total_notifications);
		let highlight_count = services
			.rooms
			.user
			.highlight_count(sender_user, room_id)
			.await
			.saturating_add(thread_total_highlights);

		rooms.insert(room_id.clone(), sync_events::v5::response::Room {
			name: if include_stable_room_fields {
				services.rooms.state_accessor.get_name(room_id).await.ok()
			} else {
				None
			},
			avatar: match heroes_avatar {
				| Some(heroes_avatar) => slipstream::JsOption::Some(heroes_avatar),
				| _ => match services.rooms.state_accessor.get_avatar(room_id).await {
					| slipstream::JsOption::Some(avatar) =>
						slipstream::JsOption::from_option(avatar.url),
					| slipstream::JsOption::Null => slipstream::JsOption::Null,
					| slipstream::JsOption::Undefined => slipstream::JsOption::Undefined,
				},
			},
			initial: (roomsince == &0).then_some(true),
			is_dm: None,
			invite_state,
			unread_notifications: UnreadNotificationsCount {
				highlight_count: Some(highlight_count),
				notification_count: Some(notification_count),
			},
			timeline: room_events,
			required_state,
			prev_batch,
			limited,
			joined_count: Some(
				services
					.rooms
					.state_cache
					.room_joined_count(room_id)
					.await
					.unwrap_or(0),
			),
			invited_count: Some(
				services
					.rooms
					.state_cache
					.room_invited_count(room_id)
					.await
					.unwrap_or(0),
			),
			num_live,
			bump_stamp: timestamp,
			heroes: Some(heroes),
		});
	}
	Ok(rooms)
}

fn is_expanded_timeline(old_limit: Option<usize>, timeline_limit: usize) -> bool {
	old_limit.is_some_and(|old| timeline_limit > old)
}

fn effective_timeline_limit(
	room_id: &RoomId,
	timeline_limit: usize,
	roomsince: u64,
	timeline_limits: &BTreeMap<OwnedRoomId, usize>,
) -> usize {
	if roomsince == 0 || timeline_limit > 0 {
		return timeline_limit;
	}

	timeline_limits
		.get(room_id)
		.copied()
		.unwrap_or(timeline_limit)
}

fn sync_events_v5_json_response(
	response: &sync_events::v5::Response,
	room_extras: RoomExtras,
	thread_subscriptions_extension: Option<Value>,
) -> Result<crate::router::response::Response> {
	let mut value = response.to_body();
	if let Some(thread_subscriptions) = thread_subscriptions_extension {
		value
			.as_object_mut()
			.expect("sync response is a JSON object")
			.entry("extensions".to_owned())
			.or_insert_with(|| Value::Object(Object::new()))
			.as_object_mut()
			.expect("sync response extensions is a JSON object")
			.insert("io.element.msc4308.thread_subscriptions".to_owned(), thread_subscriptions);
	}
	let Some(rooms) = value.get_mut("rooms").and_then(Value::as_object_mut) else {
		return Ok(json_response(&value));
	};

	for (room_id, extra) in room_extras {
		let Some(room) = rooms
			.get_mut(room_id.as_str())
			.and_then(Value::as_object_mut)
		else {
			continue;
		};

		if let Some(membership) = extra.membership {
			room.insert(
				"membership".to_owned(),
				Value::String(membership_state_to_str(&membership).to_owned()),
			);
		}

		if let Some(invite_state) = room.get("invite_state").cloned() {
			room.insert("stripped_state".to_owned(), invite_state);
		}

		if let Some(timeline) = room.get("timeline").cloned() {
			room.insert("timeline_events".to_owned(), timeline);
		}

		room.insert(
			"lists".to_owned(),
			Value::Array(extra.lists.into_iter().map(Value::String).collect()),
		);

		if extra.expanded_timeline {
			room.insert("expanded_timeline".to_owned(), Value::Bool(true));
		}
	}

	Ok(json_response(&value))
}

async fn collect_thread_subscriptions_extension(
	services: &Services,
	sender_user: &UserId,
	globalsince: u64,
	enabled: bool,
) -> Result<Option<Value>> {
	if !enabled {
		return Ok(None);
	}

	let subscribed = services
		.rooms
		.threads
		.subscriptions_since(sender_user, globalsince)
		.await
		.into_iter()
		.map(|(room_id, subscriptions)| {
			let subscriptions = subscriptions
				.into_iter()
				.map(|(thread_id, subscription)| {
					(thread_id.to_string(), {
						let mut object = slipstream::ObjectBuilder::new();
						object.field("automatic", &subscription.automatic);
						object.field("bump_stamp", &subscription.bump_stamp);
						object.finish()
					})
				})
				.collect::<Object>();

			(room_id.to_string(), Value::Object(subscriptions))
		})
		.collect::<Object>();

	if subscribed.is_empty() {
		return Ok(Some(Value::Object(Object::new())));
	}

	let mut object = slipstream::ObjectBuilder::new();
	object.field("subscribed", &subscribed);
	Ok(Some(object.finish()))
}

fn membership_state_to_str(membership: &MembershipState) -> &str {
	match membership {
		| MembershipState::Ban => "ban",
		| MembershipState::Invite => "invite",
		| MembershipState::Join => "join",
		| MembershipState::Knock => "knock",
		| MembershipState::Leave => "leave",
	}
}

async fn user_membership_for_sync(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
) -> Option<MembershipState> {
	let membership = services
		.rooms
		.state_cache
		.user_membership(sender_user, room_id)
		.await;

	if membership != Some(MembershipState::Leave) {
		return membership;
	}

	services
		.rooms
		.state_cache
		.left_state(sender_user, room_id)
		.await
		.ok()
		.flatten()
		.and_then(|pdu| {
			(pdu.state_key.as_deref() == Some(sender_user.as_str()))
				.then(|| pdu.get_content::<RoomMemberEventContent>().ok())
				.flatten()
		})
		.map_or(membership, |content| Some(content.membership))
}

/// Collect the required state events for a room
///
/// Resolves the sentinel values from [MSC3575] / Matrix Rust SDK:
/// - `*` expands to every state key for the given event type.
/// - `$ME` is replaced by the syncing user's ID.
/// - `$LAZY` (only meaningful for `m.room.member`) expands to the member events
///   of every user that sent a timeline event plus the target of every member
///   event in the timeline.
///
/// [MSC3575]: https://github.com/matrix-org/matrix-spec-proposals/blob/main/proposals/3575-sync.md
async fn collect_required_state(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	current_root_handle: Option<rezzy::hamt::RootHandle>,
	required_state_request: &RequiredStateSelection,
	timeline_pdus: &VecDeque<(PduCount, impl Event + Sync)>,
) -> Vec<Raw<AnySyncStateEvent>> {
	// The room has no joined state (e.g. a pending invite); there is nothing to
	// collect, and the callers above only reach here for rooms in the sync set.
	let Some(current_root_handle) = current_root_handle else {
		return Vec::new();
	};

	let mut required_state = Vec::new();
	// Shared across selectors purely to avoid emitting duplicate state events;
	// never used to decide whether an entry is fetched at all, so one
	// selector's exclusions can't suppress another's includes.
	let mut fetched: HashSet<(StateEventType, String)> = HashSet::new();
	let mut lazy = false;
	let mut member_wildcarded = false;

	for selector in &required_state_request.selectors {
		let mut wildcard_types: HashSet<&StateEventType> = HashSet::new();

		lazy |= selector
			.include
			.iter()
			.any(|(ty, sk)| *ty == StateEventType::RoomMember && sk.as_str() == "$LAZY");

		for (event_type, state_key) in &selector.include {
			if wildcard_types.contains(event_type) {
				continue;
			}

			if event_type == "*" {
				let state_key_filter = state_key.as_str();
				let full_state = services
					.rooms
					.state_accessor
					.state_full_hamt(current_root_handle.clone());
				pin_mut!(full_state);
				while let Some(((state_event_type, full_state_key), event)) =
					full_state.next().await
				{
					let full_state_key = full_state_key.to_string();
					if state_key_filter != "*" && state_key_filter != full_state_key {
						continue;
					}
					if required_state_excludes(
						&(state_event_type.clone(), full_state_key.clone()),
						&selector.exclude,
					) {
						continue;
					}
					if !fetched.insert((state_event_type, full_state_key)) {
						continue;
					}
					required_state.push(Event::into_format(event));
				}
				continue;
			}

			match state_key.as_str() {
				| "*" => {
					wildcard_types.insert(event_type);
					if *event_type == StateEventType::RoomMember {
						member_wildcarded = true;
					}
					let keys: Vec<conduwuit::matrix::StateKey> = services
						.rooms
						.state_accessor
						.state_keys_with_ids_hamt::<OwnedEventId>(
							current_root_handle.clone(),
							event_type,
						)
						.map(at!(0))
						.collect()
						.await;
					for key in keys {
						if required_state_excludes(
							&(event_type.clone(), key.to_string()),
							&selector.exclude,
						) {
							continue;
						}
						if !fetched.insert((event_type.clone(), key.to_string())) {
							continue;
						}
						if let Ok(event) = services
							.rooms
							.state_accessor
							.state_get_in_room_hamt(
								room_id,
								&current_root_handle,
								event_type,
								&key,
							)
							.await
						{
							required_state.push(Event::into_format(event));
						}
					}
				},
				// Handled below via `lazy`; skip the literal "$LAZY" lookup only for member
				// state.
				| "$LAZY" if *event_type == StateEventType::RoomMember => {},
				| "$ME" => {
					let resolved_key = sender_user.as_str();
					if required_state_excludes(
						&(event_type.clone(), resolved_key.to_owned()),
						&selector.exclude,
					) {
						continue;
					}
					if !fetched.insert((event_type.clone(), resolved_key.to_owned())) {
						continue;
					}
					if let Ok(event) = services
						.rooms
						.state_accessor
						.state_get_in_room_hamt(
							room_id,
							&current_root_handle,
							event_type,
							resolved_key,
						)
						.await
					{
						required_state.push(Event::into_format(event));
					}
				},
				| _ => {
					if required_state_excludes(
						&(event_type.clone(), state_key.to_string()),
						&selector.exclude,
					) {
						continue;
					}
					if !fetched.insert((event_type.clone(), state_key.to_string())) {
						continue;
					}
					if let Ok(event) = services
						.rooms
						.state_accessor
						.state_get_in_room_hamt(
							room_id,
							&current_root_handle,
							event_type,
							state_key,
						)
						.await
					{
						required_state.push(Event::into_format(event));
					}
				},
			}
		}
	}

	if lazy && !member_wildcarded {
		let mut lazy_members: HashSet<String> = HashSet::new();
		for (_, pdu) in timeline_pdus {
			lazy_members.insert(pdu.sender().as_str().to_owned());
			if *pdu.event_type() == TimelineEventType::RoomMember {
				if let Some(target) = pdu.state_key() {
					lazy_members.insert(target.to_owned());
				}
			}
		}
		for member in lazy_members {
			// Lazy-loaded members aren't tied to any single selector's include/exclude
			// pair, so no per-selector exclude applies here.
			if !fetched.insert((StateEventType::RoomMember, member.clone())) {
				continue;
			}
			if let Ok(event) = services
				.rooms
				.state_accessor
				.state_get_in_room_hamt(
					room_id,
					&current_root_handle,
					&StateEventType::RoomMember,
					&member,
				)
				.await
			{
				required_state.push(Event::into_format(event));
			}
		}
	}

	required_state
}

async fn collect_typing_events(
	services: &Services,
	sender_user: &UserId,
	body: &sync_events::v5::Request,
	todo_rooms: &TodoRooms,
) -> Result<sync_events::v5::response::Typing> {
	if !body.extensions.typing.enabled.unwrap_or(false) {
		return Ok(sync_events::v5::response::Typing::default());
	}
	let rooms: Vec<_> = body.extensions.typing.rooms.clone().unwrap_or_else(|| {
		body.room_subscriptions
			.keys()
			.map(ToOwned::to_owned)
			.collect()
	});
	let lists: Vec<_> = body
		.extensions
		.typing
		.lists
		.clone()
		.unwrap_or_else(|| body.lists.keys().map(ToOwned::to_owned).collect::<Vec<_>>());

	if rooms.is_empty() && lists.is_empty() {
		return Ok(sync_events::v5::response::Typing::default());
	}

	let mut typing_response = sync_events::v5::response::Typing::default();
	for (room_id, (_, _, roomsince)) in todo_rooms {
		if services.rooms.typing.last_typing_update(room_id).await? <= *roomsince {
			continue;
		}

		match services
			.rooms
			.typing
			.typing_users_for_user(room_id, sender_user)
			.await
		{
			| Ok(typing_users) => {
				typing_response.rooms.insert(
					room_id.to_owned(), // Already OwnedRoomId
					Raw::new(&sync_events::v5::response::SyncTypingEvent {
						content: TypingEventContent::new(typing_users),
					})?,
				);
			},
			| Err(e) => {
				warn!(%room_id, "Failed to get typing events for room: {}", e);
			},
		}
	}

	Ok(typing_response)
}

async fn collect_account_data(
	services: &Services,
	(sender_user, _, globalsince, current_count, body): (
		&UserId,
		&DeviceId,
		u64,
		u64,
		&sync_events::v5::Request,
	),
) -> sync_events::v5::response::AccountData {
	let mut account_data = sync_events::v5::response::AccountData {
		global: Vec::new(),
		rooms: BTreeMap::new(),
	};

	if !body.extensions.account_data.enabled.unwrap_or(false) {
		return sync_events::v5::response::AccountData::default();
	}

	account_data.global = services
		.account_data
		.changes_since(None, sender_user, Some(globalsince), Some(current_count))
		.ready_filter_map(|e| extract_variant!(e, AnyRawAccountDataEvent::Global))
		.collect()
		.await;

	if let Some(rooms) = &body.extensions.account_data.rooms {
		for room in rooms {
			account_data.rooms.insert(
				room.clone(),
				services
					.account_data
					.changes_since(
						Some(room),
						sender_user,
						Some(globalsince),
						Some(current_count),
					)
					.ready_filter_map(|e| extract_variant!(e, AnyRawAccountDataEvent::Room))
					.collect()
					.await,
			);
		}
	}

	account_data
}

/// Joined members of a newly-seen encrypted room whose devices the sender must
/// be told about: every member other than the sender who shares no other
/// encrypted room with them.
async fn new_encrypted_room_members(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
) -> Vec<OwnedUserId> {
	services
		.rooms
		.state_cache
		.room_members(room_id)
		// Don't send key updates from the sender to the sender
		.ready_filter(|user_id| sender_user != user_id)
		// Only send keys if the sender doesn't share an encrypted room with the target
		// already
		.filter_map(|user_id| async move {
			(!share_encrypted_room(services, sender_user, &user_id, Some(room_id)).await)
				.then(|| user_id.clone())
		})
		.collect::<Vec<_>>()
		.await
}

async fn collect_e2ee<'a, Rooms>(
	services: &Services,
	(sender_user, sender_device, globalsince, _, body): (
		&UserId,
		&DeviceId,
		u64,
		u64,
		&sync_events::v5::Request,
	),
	all_joined_rooms: Rooms,
) -> Result<sync_events::v5::response::E2EE>
where
	Rooms: Iterator<Item = &'a RoomId> + Send + 'a,
{
	if !body.extensions.e2ee.enabled.unwrap_or(false) {
		return Ok(sync_events::v5::response::E2EE::default());
	}
	let mut left_encrypted_users = HashSet::new(); // Users that have left any encrypted rooms the sender was in
	let mut device_list_changes = HashSet::new();
	let mut device_list_left = HashSet::new();
	// Look for device list updates of this account
	device_list_changes.extend(
		services
			.users
			.keys_changed(sender_user, Some(globalsince), None)
			.collect::<Vec<_>>()
			.await,
	);

	for room_id in all_joined_rooms {
		let Ok(current_root_handle) = services.rooms.state.get_room_state_hamt(room_id).await
		else {
			error!("Room {room_id} has no state");
			continue;
		};

		let since_root_handle = match services
			.rooms
			.timeline
			.prev_root_handle(room_id, PduCount::Normal(globalsince.saturating_add(1)))
			.await
		{
			| Ok(root) => Some(root),
			| Err(error) if error.is_not_found() => None,
			| Err(error) => {
				error!(%room_id, ?error, "Failed to resolve room state at the previous sync point");
				continue;
			},
		};

		let encrypted_room = services
			.rooms
			.state_accessor
			.state_get_in_room_hamt(
				room_id,
				&current_root_handle,
				&StateEventType::RoomEncryption,
				"",
			)
			.await
			.is_ok();

		if let Some(since_root_handle) = since_root_handle {
			// Skip if there are only timeline changes
			if since_root_handle == current_root_handle {
				continue;
			}

			let since_encryption = services
				.rooms
				.state_accessor
				.state_get_in_room_hamt(
					room_id,
					&since_root_handle,
					&StateEventType::RoomEncryption,
					"",
				)
				.await;

			let since_sender_member: Option<RoomMemberEventContent> = services
				.rooms
				.state_accessor
				.state_get_content_hamt(
					room_id,
					&since_root_handle,
					&StateEventType::RoomMember,
					sender_user.as_str(),
				)
				.ok()
				.await;

			let joined_since_last_sync = since_sender_member
				.as_ref()
				.is_none_or(|member| member.membership != MembershipState::Join);

			let new_encrypted_room = encrypted_room && since_encryption.is_err();

			if encrypted_room {
				let current_state_ids: HashMap<_, OwnedEventId> = services
					.rooms
					.state_accessor
					.state_keys_with_ids_hamt(
						current_root_handle.clone(),
						&StateEventType::RoomMember,
					)
					.collect()
					.await;

				let since_state_ids: HashMap<_, _> = services
					.rooms
					.state_accessor
					.state_keys_with_ids_hamt(since_root_handle, &StateEventType::RoomMember)
					.collect()
					.await;

				for (state_key, id) in current_state_ids {
					if since_state_ids.get(&state_key) != Some(&id) {
						let Ok(user_id) = UserId::parse(&state_key) else {
							continue;
						};

						if user_id == sender_user {
							continue;
						}

						let Ok(pdu) = services.rooms.timeline.get_pdu(&id).await else {
							error!("Pdu in state not found: {id}");
							continue;
						};

						let Ok(content) = pdu.get_content::<RoomMemberEventContent>() else {
							continue;
						};

						match content.membership {
							| MembershipState::Join => {
								// A new user joined an encrypted room
								if !share_encrypted_room(
									services,
									sender_user,
									&user_id,
									Some(room_id),
								)
								.await
								{
									device_list_changes.insert(user_id.clone());
								}
							},
							| MembershipState::Leave | MembershipState::Ban => {
								// Write down users that have left encrypted rooms we
								// are in
								left_encrypted_users.insert(user_id.clone());
							},
							| _ => {},
						}
					}
				}
				if joined_since_last_sync || new_encrypted_room {
					// If the user is in a new encrypted room, give them all joined users
					device_list_changes
						.extend(new_encrypted_room_members(services, sender_user, room_id).await);
				}
			}
		} else if encrypted_room {
			// No state existed at or before `globalsince`, so the room was first
			// joined after the last sync: treat it as a new encrypted room.
			device_list_changes
				.extend(new_encrypted_room_members(services, sender_user, room_id).await);
		}
		// Look for device list updates in this room
		device_list_changes.extend(
			services
				.users
				.room_keys_changed(room_id, Some(globalsince), None)
				.map(|(user_id, _)| user_id)
				.collect::<Vec<_>>()
				.await,
		);
	}

	for user_id in left_encrypted_users {
		let dont_share_encrypted_room =
			!share_encrypted_room(services, sender_user, &user_id, None).await;

		// If the user doesn't share an encrypted room with the target anymore, we need
		// to tell them
		if dont_share_encrypted_room {
			device_list_left.insert(user_id);
		}
	}

	Ok(sync_events::v5::response::E2EE {
		device_unused_fallback_key_types: Some(
			services
				.users
				.list_unused_fallback_key_types(sender_user, sender_device)
				.await,
		),

		device_one_time_keys_count: services
			.users
			.count_one_time_keys(sender_user, sender_device)
			.await,

		device_lists: DeviceLists {
			changed: device_list_changes.into_iter().collect(),
			left: device_list_left.into_iter().collect(),
		},
	})
}

async fn collect_to_device(
	services: &Services,
	(sender_user, sender_device, _, _, body): SyncInfo<'_>,
	next_batch: u64,
) -> Option<sync_events::v5::response::ToDevice> {
	if !body.extensions.to_device.enabled.unwrap_or(false) {
		return None;
	}

	// The to-device extension has its own `since` cursor (MSC3885), independent of
	// the room-list `globalsince`. Using `globalsince` here is wrong: the room-list
	// position can advance (from unrelated room/state activity) faster than this
	// client has actually consumed a given to-device event, causing it to be
	// deleted before it's ever returned. Only prune what the client has actually
	// acknowledged via its own to-device `since`, and only once it has sent one.
	let client_to_device_since = body
		.extensions
		.to_device
		.since
		.as_deref()
		.and_then(|since| since.parse::<u64>().ok());

	if let Some(since) = client_to_device_since {
		services
			.users
			.remove_to_device_events(sender_user, sender_device, since)
			.await;
	}

	let events: Vec<_> = services
		.users
		.get_to_device_events(
			sender_user,
			sender_device,
			client_to_device_since,
			Some(next_batch),
		)
		.map(at!(1))
		.collect()
		.await;

	trace!(
		%sender_user, %sender_device, ?client_to_device_since, next_batch,
		count = events.len(),
		"collect_to_device",
	);

	Some(sync_events::v5::response::ToDevice {
		next_batch: next_batch.to_string(),
		events,
	})
}

async fn collect_receipts(_services: &Services) -> sync_events::v5::response::Receipts {
	sync_events::v5::response::Receipts { rooms: BTreeMap::new() }
	// TODO: get explicitly requested read receipts
}

async fn filter_active_rooms<'a>(
	services: &'a Services,
	sender_user: &'a UserId,
	filters: Option<&'a CompatListFilters>,
	direct_rooms: &'a HashSet<OwnedRoomId>,
	mut active_rooms: Vec<&'a RoomId>,
) -> Vec<&'a RoomId> {
	let Some(filters) = filters else {
		return active_rooms;
	};

	if let Some(is_dm) = filters.is_dm {
		active_rooms.retain(|room_id| direct_rooms.contains(*room_id) == is_dm);
	}

	if let Some(is_encrypted) = filters.is_encrypted {
		active_rooms = active_rooms
			.into_iter()
			.stream()
			.filter_map(|room_id| async move {
				(services
					.rooms
					.state_accessor
					.is_encrypted_room(room_id)
					.await == is_encrypted)
					.then_some(room_id)
			})
			.collect()
			.await;
	}

	if !filters.room_types.is_empty() {
		let mut filtered = Vec::with_capacity(active_rooms.len());
		for room_id in active_rooms {
			if room_matches_type_filter(services, room_id, &filters.room_types, false).await {
				filtered.push(room_id);
			}
		}
		active_rooms = filtered;
	}

	if !filters.not_room_types.is_empty() {
		let mut filtered = Vec::with_capacity(active_rooms.len());
		for room_id in active_rooms {
			if room_matches_type_filter(services, room_id, &filters.not_room_types, true).await {
				filtered.push(room_id);
			}
		}
		active_rooms = filtered;
	}

	if !filters.tags.is_empty() {
		let mut filtered = Vec::with_capacity(active_rooms.len());
		for room_id in active_rooms {
			if room_matches_tag_filter(services, sender_user, room_id, &filters.tags, false).await
			{
				filtered.push(room_id);
			}
		}
		active_rooms = filtered;
	}

	if !filters.not_tags.is_empty() {
		let mut filtered = Vec::with_capacity(active_rooms.len());
		for room_id in active_rooms {
			if room_matches_tag_filter(services, sender_user, room_id, &filters.not_tags, true)
				.await
			{
				filtered.push(room_id);
			}
		}
		active_rooms = filtered;
	}

	if !filters.spaces.is_empty() {
		let mut filtered = Vec::with_capacity(active_rooms.len());
		for room_id in active_rooms {
			if room_is_child_of_any_space(services, room_id, &filters.spaces).await {
				filtered.push(room_id);
			}
		}
		active_rooms = filtered;
	}

	active_rooms
}

async fn room_matches_type_filter(
	services: &Services,
	room_id: &RoomId,
	filter: &[RoomTypeFilter],
	negate: bool,
) -> bool {
	let room_type = services.rooms.state_accessor.get_room_type(room_id).await;

	if room_type.as_ref().is_err_and(|e| !e.is_not_found()) {
		return false;
	}

	let room_type_filter = RoomTypeFilter::from(room_type.ok());

	if negate {
		!filter.contains(&room_type_filter)
	} else {
		filter.is_empty() || filter.contains(&room_type_filter)
	}
}

async fn room_matches_tag_filter(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	filter: &[String],
	negate: bool,
) -> bool {
	let tags = services
		.account_data
		.get_room::<TagEvent>(room_id, sender_user, RoomAccountDataEventType::Tag)
		.await
		.map(|event| event.content.tags)
		.unwrap_or_default();

	let matches = filter
		.iter()
		.any(|tag| tags.keys().any(|room_tag| room_tag.to_string() == *tag));

	if negate { !matches } else { matches }
}

async fn room_is_child_of_any_space(
	services: &Services,
	room_id: &RoomId,
	spaces: &[OwnedRoomId],
) -> bool {
	for space_id in spaces {
		if services
			.rooms
			.state_accessor
			.room_state_get_content::<SpaceChildEventContent>(
				space_id,
				&StateEventType::SpaceChild,
				room_id.as_str(),
			)
			.await
			.is_ok_and(|content| !content.via.is_empty())
		{
			return true;
		}
	}

	false
}

fn merge_list_filters(
	base: Option<&sync_events::v5::request::ListFilters>,
	extra: Option<&CompatListFilters>,
) -> Option<CompatListFilters> {
	match (base, extra) {
		| (None, None) => None,
		| (Some(base), None) => Some(CompatListFilters::from(base)),
		| (None, Some(extra)) => Some(extra.clone()),
		| (Some(base), Some(extra)) => Some(CompatListFilters {
			is_dm: extra.is_dm,
			is_encrypted: extra.is_encrypted,
			is_invite: extra.is_invite.or(base.is_invite),
			room_types: extra.room_types.clone(),
			not_room_types: if extra.not_room_types.is_empty() {
				base.not_room_types.clone()
			} else {
				extra.not_room_types.clone()
			},
			tags: extra.tags.clone(),
			not_tags: extra.not_tags.clone(),
			spaces: extra.spaces.clone(),
		}),
	}
}

async fn direct_rooms_for_user(
	services: &Services,
	sender_user: &UserId,
) -> HashSet<OwnedRoomId> {
	services
		.account_data
		.get_global::<DirectEvent>(sender_user, GlobalAccountDataEventType::Direct)
		.await
		.map(|direct_event| {
			direct_event
				.content
				.0
				.into_values()
				.flatten()
				.collect::<HashSet<_>>()
		})
		.unwrap_or_default()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[derive(Debug)]
	struct RequiredStateFixture {
		required_state: CompatRequiredState,
	}

	impl Deserialize for RequiredStateFixture {
		fn from_json(value: &Value) -> Result<Self, DeError> {
			let required_state = expect_object(value)?
				.get("required_state")
				.ok_or_else(|| DeError("missing field `required_state`".to_owned()))?;
			Ok(Self {
				required_state: parse_required_state(required_state)?,
			})
		}
	}

	#[test]
	fn required_state_accepts_event_type_map() {
		let fixture: RequiredStateFixture = slipstream::codec::from_str(
			r#"{"required_state":{"m.room.name":[""],"m.room.member":["$LAZY"]}}"#,
		)
		.expect("event-type keyed required_state should deserialize");

		assert_eq!(fixture.required_state.include, vec![
			(StateEventType::RoomMember, "$LAZY".to_owned()),
			(StateEventType::RoomName, String::new()),
		]);
		assert_eq!(fixture.required_state.exclude, None);
	}

	#[test]
	fn required_state_accepts_stable_include_object() {
		let fixture: RequiredStateFixture = slipstream::codec::from_str(
			r#"{"required_state":{"include":[{"type":"m.room.name","state_key":""}],"lazy_members":true}}"#,
		)
		.expect("stable include/lazy_members required_state should deserialize");

		assert_eq!(fixture.required_state.include, vec![
			(StateEventType::RoomName, String::new()),
			(StateEventType::RoomMember, "$LAZY".to_owned()),
		]);
		assert_eq!(fixture.required_state.exclude, None);
	}

	#[test]
	fn required_state_accepts_stable_exclude_object() {
		let fixture: RequiredStateFixture = slipstream::codec::from_str(
			r#"{"required_state":{"include":[{"type":"*","state_key":"*"}],"exclude":[{"type":"m.room.member","state_key":"*"}]}}"#,
		)
		.expect("stable exclude required_state should deserialize");

		assert_eq!(fixture.required_state.include, vec![("*".into(), "*".to_owned())]);
		assert_eq!(
			fixture.required_state.exclude,
			Some(vec![(StateEventType::RoomMember, "*".to_owned())])
		);
	}

	#[test]
	fn required_state_distinguishes_omitted_from_explicit_empty_exclude() {
		let fixture: RequiredStateFixture = slipstream::codec::from_str(
			r#"{"required_state":{"include":[{"type":"m.room.name","state_key":""}]}}"#,
		)
		.expect("required_state without exclude should deserialize");
		assert_eq!(fixture.required_state.exclude, None, "omitted exclude must be None");

		let fixture: RequiredStateFixture = slipstream::codec::from_str(
			r#"{"required_state":{"include":[{"type":"m.room.name","state_key":""}],"exclude":[]}}"#,
		)
		.expect("required_state with explicit empty exclude should deserialize");
		assert_eq!(
			fixture.required_state.exclude,
			Some(Vec::new()),
			"explicit empty exclude must be Some(vec![]) so it can clear a sticky exclusion"
		);
	}

	#[test]
	fn required_state_accepts_exclude_only_object() {
		let fixture: RequiredStateFixture =
			slipstream::codec::from_str(r#"{"required_state":{"exclude":[]}}"#)
				.expect("exclude-only required_state object should deserialize");

		assert_eq!(fixture.required_state.include, Vec::new());
		assert_eq!(fixture.required_state.exclude, Some(Vec::new()));
	}

	#[test]
	fn required_state_rejects_unknown_object_keys() {
		slipstream::codec::from_str::<RequiredStateFixture>(
			r#"{"required_state":{"include":[],"bogus":[]}}"#,
		)
		.expect_err("unknown required_state object keys should be rejected");
	}

	#[test]
	fn stable_list_accepts_singular_range() {
		let request: CompatRequest = slipstream::codec::from_str(
			r#"{"lists":{"all":{"timeline_limit":1,"required_state":{"include":[]},"range":[0,0]}}}"#,
		)
		.expect("stable singular range should deserialize");

		assert_eq!(request.lists["all"].ranges, vec![(uint!(0), uint!(0))]);
	}

	#[test]
	fn empty_and_object_bodies_are_defaults_but_null_is_rejected() {
		let none = CompatSyncRequest::from_parts(&[], &[], None).unwrap();
		assert!(matches!(none.set_presence, PresenceState::Online));
		assert!(none.request.lists.is_empty());
		let empty = Value::Object(Object::new());
		let object = CompatSyncRequest::from_parts(&[], &[], Some(&empty)).unwrap();
		assert!(object.request.lists.is_empty() && !object.thread_subscriptions_enabled);
		// As with the serde struct this replaces, a literal `null` body is an error.
		assert!(CompatSyncRequest::from_parts(&[], &[], Some(&Value::Null)).is_err());
	}

	fn ranges_of(json: &str) -> Result<CompatRanges, DeError> {
		let request: CompatRequest = slipstream::codec::from_str(json)?;
		Ok(request.lists["l"].ranges.clone())
	}

	#[test]
	fn ranges_accept_every_shape() {
		let one = vec![(uint!(0), uint!(10))];
		for body in [
			r#"{"lists":{"l":{"ranges":[[0,10]]}}}"#,
			r#"{"lists":{"l":{"ranges":[0,10]}}}"#,
			r#"{"lists":{"l":{"range":[0,10]}}}"#,
			r#"{"lists":{"l":{"ranges":{"start":0,"end":10}}}}"#,
			r#"{"lists":{"l":{"ranges":[{"start":0,"end":10}]}}}"#,
			r#"{"lists":{"l":{"ranges":{"a":[0,10]}}}}"#,
			r#"{"lists":{"l":{"ranges":{"a":{"start":0,"end":10}}}}}"#,
		] {
			assert_eq!(ranges_of(body).unwrap(), one, "{body}");
		}
		assert_eq!(ranges_of(r#"{"lists":{"l":{"ranges":[]}}}"#).unwrap(), vec![]);
		assert_eq!(ranges_of(r#"{"lists":{"l":{}}}"#).unwrap(), vec![]);
		// Keyed ranges come back in key order.
		assert_eq!(
			ranges_of(r#"{"lists":{"l":{"ranges":{"b":[5,6],"a":[0,1]}}}}"#).unwrap(),
			vec![(uint!(0), uint!(1)), (uint!(5), uint!(6))]
		);
		// `ranges` wins over the `range` alias.
		assert_eq!(
			ranges_of(r#"{"lists":{"l":{"ranges":[[1,2]],"range":[7,8]}}}"#).unwrap(),
			vec![(uint!(1), uint!(2))]
		);
	}

	#[test]
	fn ranges_reject_malformed_shapes() {
		for body in [
			r#"{"lists":{"l":{"ranges":null}}}"#,
			r#"{"lists":{"l":{"ranges":"0-10"}}}"#,
			r#"{"lists":{"l":{"ranges":[0,1,2]}}}"#,
			r#"{"lists":{"l":{"ranges":{"start":0}}}}"#,
		] {
			assert!(ranges_of(body).is_err(), "{body}");
		}
	}

	#[test]
	fn expanded_timeline_requires_a_previous_limit() {
		assert!(!is_expanded_timeline(None, 10));
	}

	#[test]
	fn expanded_timeline_counts_zero_as_previous_limit() {
		assert!(is_expanded_timeline(Some(0), 10));
	}

	#[test]
	fn expanded_timeline_requires_a_larger_limit() {
		assert!(!is_expanded_timeline(Some(10), 10));
		assert!(!is_expanded_timeline(Some(10), 5));
		assert!(is_expanded_timeline(Some(10), 11));
	}
}

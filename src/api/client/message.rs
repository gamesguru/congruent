use axum::extract::State;
use axum_client_ip::ClientIp;
use conduwuit::{
	Err, Error, PduEvent, Result, at, debug_warn, info,
	matrix::{
		event::{Event, Matches},
		pdu::{PduCount, TopoToken},
	},
	ref_at,
	utils::{
		IterStream, ReadyExt,
		result::LogErr,
		stream::{BroadbandExt, TryIgnore},
	},
};
use conduwuit_service::{
	Services,
	rooms::{
		lazy_loading,
		lazy_loading::{MemberSet, Options},
		timeline::TopoIterItem,
	},
};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt, future::OptionFuture, pin_mut};
use ruma::{
	DeviceId, RoomId, UserId,
	api::{
		Direction,
		client::{error::ErrorKind, filter::RoomEventFilter, message::get_message_events},
	},
	events::{
		AnyStateEvent, StateEventType,
		TimelineEventType::{self, *},
		invite_permission_config::FilterLevel,
	},
	serde::Raw,
};
use tracing::warn;

use super::sync::add_membership_to_unsigned;
use crate::Ruma;

/// list of safe and common non-state events to ignore if the user is ignored
const IGNORED_MESSAGE_TYPES: &[TimelineEventType] = &[
	Audio,
	CallInvite,
	Emote,
	File,
	Image,
	KeyVerificationStart,
	Location,
	PollStart,
	UnstablePollStart,
	Beacon,
	Reaction,
	RoomEncrypted,
	RoomMessage,
	Sticker,
	Video,
	Voice,
	CallNotify,
];

const LIMIT_MAX: usize = 100;
const LIMIT_DEFAULT: usize = 10;

/// # `GET /_matrix/client/r0/rooms/{roomId}/messages`
///
/// Allows paginating through room history.
///
/// - Only works if the user is joined (TODO: always allow, but only show events
///   where the user was joined, depending on `history_visibility`)
pub(crate) async fn get_message_events_route(
	State(services): State<crate::State>,
	ClientIp(client_ip): ClientIp,
	body: Ruma<get_message_events::v3::Request>,
) -> Result<get_message_events::v3::Response> {
	debug_assert!(IGNORED_MESSAGE_TYPES.is_sorted(), "IGNORED_MESSAGE_TYPES is not sorted");
	let sender_user = body.sender_user();
	let sender_device = body.sender_device.as_deref();
	let room_id = &body.room_id;
	let filter = &body.filter;

	services
		.users
		.update_device_last_seen(sender_user, sender_device, client_ip)
		.await;

	if !services.rooms.metadata.exists(room_id).await {
		return Err!(Request(Forbidden("Room does not exist to this server")));
	}

	// Check access before parsing pagination tokens — an unauthorized user
	// should get 403 Forbidden, not a parse error from a stale token.
	// Per Matrix spec, /messages is accessible to current AND former members.
	// The per-event visibility_filter handles fine-grained history_visibility
	// checks; this gate only verifies the user has/had membership.
	//
	// A former member who has forgotten the room (POST /forget) loses access
	// even though is_left() still returns true -- forgetting no longer
	// deletes the leave record (see state_cache::forget()'s doc comment), so
	// it must be checked explicitly here rather than inferred from is_left().
	if !services
		.rooms
		.state_cache
		.can_access_history(sender_user, room_id)
		.await
	{
		return Err!(Request(Forbidden("You don't have permission to view this room.")));
	}

	let from: TopoToken = body
		.from
		.as_deref()
		.map(str::parse)
		.transpose()?
		.unwrap_or_else(|| match body.dir {
			| Direction::Forward => TopoToken { depth: 0, pdu_count: PduCount::min() },
			| Direction::Backward => TopoToken {
				depth: u64::MAX,
				pdu_count: PduCount::max(),
			},
		});
	let from = if matches!(body.dir, Direction::Backward) {
		normalize_backward_from_token(&services, room_id, from).await?
	} else {
		from
	};

	let to: Option<TopoToken> = body.to.as_deref().map(str::parse).transpose()?;

	let limit: usize = body
		.limit
		.try_into()
		.unwrap_or(LIMIT_DEFAULT)
		.min(LIMIT_MAX);

	if limit == 0 {
		return Ok(get_message_events::v3::Response {
			start: from.to_string(),
			end: None,
			chunk: Vec::new(),
			state: Vec::new(),
		});
	}

	info!(
		"/messages: room={room_id} dir={:?} from={from} to={to:?} limit={limit}",
		body.dir
	);

	if matches!(body.dir, Direction::Backward) {
		services
			.rooms
			.timeline
			.backfill_if_required(room_id, from.pdu_count, limit)
			.boxed()
			.await
			.log_err()
			.ok();
	}

	let it = match body.dir {
		| Direction::Forward => services
			.rooms
			.timeline
			.topo_pdus(room_id, Some(from))
			.ignore_err()
			.boxed(),

		| Direction::Backward => services
			.rooms
			.timeline
			.topo_pdus_rev(room_id, Some(from))
			.ignore_err()
			.boxed(),
	};

	let mut events = Vec::with_capacity(limit);
	let mut next_token = None;
	let mut exhausted = true;
	let mut consumed = 0_usize;
	let mut filtered_event = 0_usize;
	let mut filtered_ignored = 0_usize;
	let mut filtered_visibility = 0_usize;

	let mut stream = it;
	while let Some(item) = stream.next().await {
		let (token, pdu) = item;
		consumed = consumed.saturating_add(1);

		info!(
			target: "pagination_debug",
			%token, event_id = %pdu.event_id(), event_type = %pdu.kind(),
			"/messages: raw item consumed from topo stream"
		);

		if Some(token) == to {
			break;
		}

		next_token = Some(token);

		let event_id_for_trace = pdu.event_id().to_owned();
		let Some(item) = event_filter((token, pdu), filter) else {
			filtered_event = filtered_event.saturating_add(1);
			info!(
				target: "pagination_debug", %token, event_id = %event_id_for_trace,
				"/messages: DROPPED by event_filter (RoomEventFilter)"
			);
			continue;
		};

		let Some(item) = ignored_filter(&services, item, sender_user).await else {
			filtered_ignored = filtered_ignored.saturating_add(1);
			info!(
				target: "pagination_debug", %token, event_id = %event_id_for_trace,
				"/messages: DROPPED by ignored_filter"
			);
			continue;
		};

		let Some(mut item) = visibility_filter(&services, item, sender_user).await else {
			filtered_visibility = filtered_visibility.saturating_add(1);
			info!(
				target: "pagination_debug", %token, event_id = %event_id_for_trace,
				"/messages: DROPPED by visibility_filter (user_can_see_event)"
			);
			continue;
		};

		item.1.set_unsigned(Some(sender_user));
		add_membership_to_unsigned(&services, sender_user, &mut item.1).await;
		if let Err(e) = services
			.rooms
			.pdu_metadata
			.add_bundled_aggregations_to_pdu(sender_user, &mut item.1)
			.await
		{
			debug_warn!("Failed to add bundled aggregations: {e}");
		}
		events.push(item);

		if events.len() == limit {
			exhausted = false;
			break;
		}
	}

	let lazy_loading_context = lazy_loading::Context {
		user_id: sender_user,
		device_id: sender_device.or_else(|| {
			if let Some(registration) = body.appservice_info.as_ref() {
				Some(<&DeviceId>::from(registration.registration.id.as_str()))
			} else {
				warn!(
					"No device_id provided and no appservice registration found, this should be \
					 unreachable"
				);
				None
			}
		}),
		room_id,
		token: Some(from.pdu_count.into_unsigned()),
		options: Some(&filter.lazy_load_options),
	};

	let witness: OptionFuture<_> = filter
		.lazy_load_options
		.is_enabled()
		.then(|| lazy_loading_witness(&services, &lazy_loading_context, events.iter()))
		.into();

	let state = witness
		.map(Option::into_iter)
		.map(|option| option.flat_map(MemberSet::into_iter))
		.map(IterStream::stream)
		.into_stream()
		.flatten()
		.broad_filter_map(|user_id| async move {
			get_member_event(&services, room_id, &user_id).await
		})
		.collect()
		.await;

	// Return the raw cursor of the oldest event we actually consumed, not the
	// last visible event. That keeps pagination moving even when filters skip an
	// entire page. Keep the final non-empty backward token as a resumable
	// "room start" cursor for forward replay; only suppress the token once we
	// have actually exhausted the iterator *and* have no events to return.
	let next_token = if exhausted && events.is_empty() {
		None
	} else {
		next_token
	};

	let chunk = events
		.into_iter()
		.map(at!(1))
		.map(Event::into_format)
		.collect();

	let resp = get_message_events::v3::Response {
		start: from.to_string(),
		end: next_token.as_ref().map(|t| format!("{t}")),
		chunk,
		state,
	};

	info!(
		"/messages: room={room_id} returning {} events, start={}, end={:?}, consumed={}, \
		 filtered_event={}, filtered_ignored={}, filtered_visibility={}",
		resp.chunk.len(),
		resp.start,
		resp.end,
		consumed,
		filtered_event,
		filtered_ignored,
		filtered_visibility
	);

	Ok(resp)
}

async fn normalize_backward_from_token(
	services: &Services,
	room_id: &RoomId,
	from: TopoToken,
) -> Result<TopoToken> {
	let last_timeline_count = services.rooms.timeline.last_timeline_count(room_id).await?;
	if from.pdu_count <= last_timeline_count {
		return Ok(from);
	}

	let stream = services.rooms.timeline.topo_pdus_rev(room_id, None);
	pin_mut!(stream);
	let latest = stream.try_next().await?.map(at!(0)).unwrap_or(from);

	// `topo_pdus_rev` treats its `until` token as an exclusive boundary --
	// "the caller already consumed this position, don't repeat it" -- which is
	// correct for a real resume token (one returned to a client as `end`) but
	// wrong here: the caller hasn't seen anything yet, they just asked for the
	// newest messages without supplying a `from`. Returning `latest` verbatim
	// would make the boundary filter (`key >= token_topo_key`, keyed primarily
	// on depth) exclude the newest event's own key, silently dropping it from
	// every from-less backward pagination. Keep the real `pdu_count` (still
	// needed by `backfill_if_required` and lazy-loading below), but clamp
	// `depth` to u64::MAX so the boundary sorts after every real event's key
	// and excludes nothing.
	let normalized = TopoToken {
		depth: u64::MAX,
		pdu_count: latest.pdu_count,
	};

	info!(
		target: "pagination_debug",
		%room_id,
		requested_from = %from,
		normalized_from = %normalized,
		?last_timeline_count,
		"/messages: clamped backward pagination token to latest known timeline position",
	);

	Ok(normalized)
}

pub(crate) async fn lazy_loading_witness<'a, I>(
	services: &Services,
	lazy_loading_context: &lazy_loading::Context<'_>,
	events: I,
) -> MemberSet
where
	I: Iterator<Item = &'a TopoIterItem> + Clone + Send,
{
	let oldest = events
		.clone()
		.map(|(token, _)| token.pdu_count)
		.min()
		.unwrap_or_else(PduCount::max);

	let newest = events
		.clone()
		.map(|(token, _)| token.pdu_count)
		.max()
		.unwrap_or_else(PduCount::max);

	let receipts = services
		.rooms
		.read_receipt
		.readreceipts_since(lazy_loading_context.room_id, Some(oldest.into_unsigned()));

	pin_mut!(receipts);
	let witness: MemberSet = events
		.stream()
		.map(ref_at!(1))
		.map(Event::sender)
		.map(ToOwned::to_owned)
		.chain(
			receipts
				.ready_take_while(|(_, c, _)| *c <= newest.into_unsigned())
				.map(|(user_id, ..)| user_id),
		)
		.collect()
		.await;

	services
		.rooms
		.lazy_loading
		.retain_lazy_members(witness, lazy_loading_context)
		.await
}

async fn get_member_event(
	services: &Services,
	room_id: &RoomId,
	user_id: &UserId,
) -> Option<Raw<AnyStateEvent>> {
	services
		.rooms
		.state_accessor
		.room_state_get(room_id, &StateEventType::RoomMember, user_id.as_str())
		.map_ok(Event::into_format)
		.await
		.ok()
}

#[inline]
pub(crate) async fn ignored_filter<T: Send>(
	services: &Services,
	item: (T, PduEvent),
	user_id: &UserId,
) -> Option<(T, PduEvent)> {
	let (_, ref pdu) = item;

	is_ignored_pdu(services, pdu, user_id)
		.await
		.unwrap_or(true)
		.eq(&false)
		.then_some(item)
}

/// Determine whether a PDU should be ignored for a given recipient user.
/// Returns True if this PDU should be ignored, returns False otherwise.
///
/// The error SenderIgnored is returned if the sender or the sender's server is
/// ignored by the relevant user. If the error cannot be returned to the user,
/// it should equate to a true value (i.e. ignored).
#[inline]
pub(crate) async fn is_ignored_pdu<Pdu>(
	services: &Services,
	event: &Pdu,
	recipient_user: &UserId,
) -> Result<bool>
where
	Pdu: Event + Send + Sync,
{
	// exclude Synapse's dummy events from bloating up response bodies. clients
	// don't need to see this.
	if event.kind().to_cow_str() == "org.matrix.dummy_event" {
		return Ok(true);
	}

	let sender_user = event.sender();
	let type_ignored = IGNORED_MESSAGE_TYPES.binary_search(event.kind()).is_ok();
	let server_ignored = services
		.moderation
		.is_remote_server_ignored(sender_user.server_name());
	let user_ignored = services
		.users
		.user_is_ignored(sender_user, recipient_user)
		.await;

	if !type_ignored {
		// We cannot safely ignore this type
		return Ok(false);
	}

	if server_ignored {
		// the sender's server is ignored, so ignore this event
		return Err(Error::BadRequest(
			ErrorKind::SenderIgnored { sender: None },
			"The sender's server is ignored by this server.",
		));
	}

	if user_ignored && !services.config.send_messages_from_ignored_users_to_client {
		// the recipient of this PDU has the sender ignored, and we're not
		// configured to send ignored messages to clients
		return Err(Error::BadRequest(
			ErrorKind::SenderIgnored { sender: Some(event.sender().to_owned()) },
			"You have ignored this sender.",
		));
	}

	Ok(false)
}

#[inline]
pub(crate) async fn visibility_filter<T: Send>(
	services: &Services,
	item: (T, PduEvent),
	user_id: &UserId,
) -> Option<(T, PduEvent)> {
	let (_, pdu) = &item;

	let room_id = pdu.room_id_or_hash()?;

	services
		.rooms
		.state_accessor
		.user_can_see_event(user_id, &room_id, pdu.event_id())
		.await
		.then_some(item)
}

#[inline]
pub(crate) fn event_filter<T: Send>(
	item: (T, PduEvent),
	filter: &RoomEventFilter,
) -> Option<(T, PduEvent)> {
	let (_, pdu) = &item;
	filter.matches(pdu).then_some(item)
}

#[inline]
pub(crate) async fn is_ignored_invite(
	services: &Services,
	recipient_user: &UserId,
	room_id: &RoomId,
) -> bool {
	let Ok(sender_user) = services
		.rooms
		.state_cache
		.invite_sender(recipient_user, room_id)
		.await
	else {
		// the invite may have been sent before the invite_sender table existed.
		// assume it's not ignored
		return false;
	};

	services
		.users
		.invite_filter_level(&sender_user, recipient_user)
		.await == FilterLevel::Ignore
}

#![allow(deprecated)]

use std::{borrow::Borrow, time::Instant, vec};

use axum::extract::State;
use conduwuit::{
	Err, Event, Result, at, debug, err, info, trace,
	utils::stream::{BroadbandExt, IterStream, TryBroadbandExt},
	warn,
};
use conduwuit_service::Services;
use futures::{FutureExt, StreamExt, TryStreamExt};
use ruma::{
	CanonicalJsonValue, EventId, OwnedEventId, RoomId, ServerName, UserId,
	api::federation::membership::create_join_event,
	events::room::{join_rules::JoinRule, member::MembershipState},
};
use serde_json::value::{RawValue as RawJsonValue, to_raw_value};

use crate::Ruma;

/// helper method for /send_join v1 and v2
#[tracing::instrument(skip(services, pdu, omit_members), fields(room_id = room_id.as_str(), origin = origin.as_str()), level = "info")]
async fn create_join_event(
	services: &Services,
	origin: &ServerName,
	room_id: &RoomId,
	pdu: &RawJsonValue,
	omit_members: bool,
) -> Result<create_join_event::v2::RoomState> {
	let (event_id, mut value, content, room_version_id, _sender, state_key) =
		super::utils::verify_send_membership(
			services,
			origin,
			room_id,
			pdu,
			MembershipState::Join,
		)
		.await?;

	// We need to return the state prior to joining, let's keep a reference to that
	// here
	let shortstatehash = services
		.rooms
		.state
		.get_room_shortstatehash(room_id)
		.await
		.map_err(|e| err!(Request(NotFound(error!("Room has no state: {e}")))))?;

	// `servers_in_room` must reflect the servers active in the room BEFORE this
	// join, so snapshot it now — `handle_and_send_incoming_pdu` below persists
	// the join and would otherwise make the joining server show up in its own
	// "before" list.
	let servers_in_room_before_join: Vec<String> = services
		.rooms
		.state_cache
		.room_servers(room_id)
		.map(|sn| sn.as_str().to_owned())
		.collect()
		.await;

	if let Some(authorising_user) = content.join_authorized_via_users_server {
		use ruma::RoomVersionId::*;

		if matches!(room_version_id, V1 | V2 | V3 | V4 | V5 | V6 | V7) {
			return Err!(Request(InvalidParam(
				"Room version {room_version_id} does not support restricted rooms but \
				 join_authorised_via_users_server ({authorising_user}) was found in the event."
			)));
		}

		if !services.globals.user_is_local(&authorising_user) {
			return Err!(Request(InvalidParam(
				"Cannot authorise membership event through {authorising_user} as they do not \
				 belong to this homeserver"
			)));
		}

		if !services
			.rooms
			.state_cache
			.is_joined(&authorising_user, room_id)
			.await
		{
			return Err!(Request(InvalidParam(
				"Authorising user {authorising_user} is not in the room you are trying to join, \
				 they cannot authorise your join."
			)));
		}

		if super::user_can_perform_restricted_join(
			services,
			&state_key,
			room_id,
			&room_version_id,
		)
		.await?
		.is_none()
		{
			return Err!(Request(UnableToAuthorizeJoin(
				"Joining user did not pass restricted room's rules."
			)));
		}

		services
			.server_keys
			.hash_and_sign_event(&mut value, &room_version_id)
			.map_err(|e| {
				err!(Request(InvalidParam(warn!("Failed to sign send_join event: {e}"))))
			})?;
	} else {
		// Guard for restricted/knock_restricted rooms: when the join event
		// lacks join_authorized_via_users_server the user must be invited or
		// already joined.  Without this, handle_and_send_incoming_pdu would soft-fail
		// the event but send_join would still return success.
		guard_restricted_join_without_auth(services, &state_key, room_id).await?;
	}

	super::utils::handle_and_send_incoming_pdu(
		services,
		origin,
		room_id,
		&event_id,
		value.clone(),
		&room_version_id,
	)
	.await?;

	trace!("Fetching current state IDs");
	let state_ids: Vec<OwnedEventId> = services
		.rooms
		.state_accessor
		.state_full_ids(shortstatehash)
		.map(at!(1))
		.collect()
		.await;

	// Per MSC3943 (an addendum to MSC3706), a nameless room's heroes'
	// membership events must still be included in a partial-state response so
	// the joining server can compute a display name before it finishes
	// lazily-loading full state. Only applies when the room has neither
	// `m.room.name` nor `m.room.canonical_alias` — otherwise the client uses
	// those instead and doesn't need heroes at all.
	let heroes = if omit_members {
		let (has_name, has_canonical_alias) = tokio::join!(
			services
				.rooms
				.state_accessor
				.state_contains_type(shortstatehash, &ruma::events::StateEventType::RoomName),
			services.rooms.state_accessor.state_contains_type(
				shortstatehash,
				&ruma::events::StateEventType::RoomCanonicalAlias
			),
		);
		if has_name || has_canonical_alias {
			std::collections::HashSet::new()
		} else {
			build_partial_state_heroes(services, room_id).await
		}
	} else {
		std::collections::HashSet::new()
	};

	trace!(%omit_members, "Constructing current state");
	let retained_state_ids: Vec<OwnedEventId> = state_ids
		.iter()
		.try_stream::<conduwuit::Error>()
		.broad_filter_map(|event_id| {
			let heroes = &heroes;
			async move {
				if omit_members {
					if let Ok(e) = event_id.as_ref() {
						let pdu = services
							.rooms
							.timeline
							.get_pdu_in_room(Some(room_id), e)
							.await;
						if let Ok(p) = pdu {
							if p.kind().to_cow_str() == "m.room.member"
								&& !p.state_key().is_some_and(|sk| heroes.contains(sk))
							{
								trace!("omitting member event {e:?} from returned state");
								// skip members, except heroes
								return None;
							}
						}
					}
				}
				event_id.ok().cloned()
			}
		})
		.collect()
		.await;

	let state = retained_state_ids
		.iter()
		.try_stream()
		.broad_and_then(|event_id| services.rooms.timeline.get_pdu_json(event_id))
		.broad_and_then(|pdu| {
			services
				.sending
				.convert_to_outgoing_federation_event(pdu)
				.map(Ok)
		})
		.try_collect()
		.boxed()
		.await?;

	// Per MSC3706: "Any events returned within `state` can be omitted from
	// `auth_chain`." Without this, events we already kept in `state` above
	// (all state when not omitting members, or heroes' membership events
	// when we are) would be sent twice.
	let retained_state_id_set: std::collections::HashSet<&EventId> =
		retained_state_ids.iter().map(Borrow::borrow).collect();
	let starting_events = state_ids.iter().map(Borrow::borrow);
	trace!("Constructing auth chain");
	let auth_chain = services
		.rooms
		.auth_chain
		.event_ids_iter(room_id, starting_events)
		.broad_filter_map(|event_id| {
			let retained_state_id_set = &retained_state_id_set;
			async move {
				match event_id {
					| Ok(event_id) if retained_state_id_set.contains(&*event_id) => None,
					| other => Some(other),
				}
			}
		})
		.broad_and_then(|event_id| async move {
			services.rooms.timeline.get_pdu_json(&event_id).await
		})
		.broad_and_then(|pdu| {
			services
				.sending
				.convert_to_outgoing_federation_event(pdu)
				.map(Ok)
		})
		.try_collect()
		.boxed()
		.await?;
	info!(%omit_members, "Join event accepted; outbound federation queued");
	debug!("Finished sending join event");
	let servers_in_room: Option<Vec<_>> = if !omit_members {
		None
	} else {
		let servers = servers_in_room_before_join;
		// If there's no servers, just add us
		let servers = if servers.is_empty() {
			warn!("Failed to find any servers, adding our own server name as a last resort");
			vec![services.globals.server_name().to_string()]
		} else {
			trace!("Found {} servers in room", servers.len());
			servers
		};
		Some(servers)
	};
	debug!("Returning send_join data");
	Ok(create_join_event::v2::RoomState {
		auth_chain,
		state,
		event: to_raw_value(&CanonicalJsonValue::Object(value)).ok(),
		members_omitted: omit_members,
		servers_in_room,
	})
}

/// Determine the room's "heroes" (mirrors the sync `/sync` summary
/// calculation, minus the syncing-user exclusion which doesn't apply here)
/// so their membership events can be kept in a partial-state `send_join`
/// response even though other membership events are omitted.
async fn build_partial_state_heroes(
	services: &Services,
	room_id: &RoomId,
) -> std::collections::HashSet<String> {
	const MAX_HERO_COUNT: usize = 5;

	services
		.rooms
		.state_cache
		.room_members(room_id)
		.map(|user_id| user_id.as_str().to_owned())
		.chain(
			services
				.rooms
				.state_cache
				.room_members_invited(room_id)
				.map(|user_id| user_id.as_str().to_owned()),
		)
		.take(MAX_HERO_COUNT)
		.collect()
		.await
}

/// # `PUT /_matrix/federation/v1/send_join/{roomId}/{eventId}`
///
/// Submits a signed join event.
pub(crate) async fn create_join_event_v1_route(
	State(services): State<crate::State>,
	body: Ruma<create_join_event::v1::Request>,
) -> Result<create_join_event::v1::Response> {
	let now = Instant::now();
	let room_state = create_join_event(&services, body.origin(), &body.room_id, &body.pdu, false)
		.boxed()
		.await?;
	let transformed = create_join_event::v1::RoomState {
		auth_chain: room_state.auth_chain,
		state: room_state.state,
		event: room_state.event,
	};
	info!(
		"Finished sending a join for {} in {} in {:?}",
		body.origin(),
		&body.room_id,
		now.elapsed()
	);

	Ok(create_join_event::v1::Response { room_state: transformed })
}

/// # `PUT /_matrix/federation/v2/send_join/{roomId}/{eventId}`
///
/// Submits a signed join event.
pub(crate) async fn create_join_event_v2_route(
	State(services): State<crate::State>,
	body: Ruma<create_join_event::v2::Request>,
) -> Result<create_join_event::v2::Response> {
	let now = Instant::now();
	let room_state =
		create_join_event(&services, body.origin(), &body.room_id, &body.pdu, body.omit_members)
			.boxed()
			.await?;
	info!(
		"Finished sending a join for {} in {} in {:?}",
		body.origin(),
		&body.room_id,
		now.elapsed()
	);

	Ok(create_join_event::v2::Response { room_state })
}

/// Reject a join to a restricted/knock_restricted room when the event lacks
/// `join_authorized_via_users_server` and the user is neither invited nor
/// already joined.
async fn guard_restricted_join_without_auth(
	services: &Services,
	joining_user: &UserId,
	room_id: &RoomId,
) -> Result<()> {
	let join_rules = services.rooms.state_accessor.get_join_rules(room_id).await;

	if !matches!(join_rules, JoinRule::Restricted(_) | JoinRule::KnockRestricted(_)) {
		return Ok(());
	}

	let is_invited = services
		.rooms
		.state_cache
		.is_invited(joining_user, room_id)
		.await;

	let is_joined = services
		.rooms
		.state_cache
		.is_joined(joining_user, room_id)
		.await;

	if !is_invited && !is_joined {
		return Err!(Request(Forbidden(
			"Restricted room requires join_authorized_via_users_server, an invite, or existing \
			 membership."
		)));
	}

	Ok(())
}

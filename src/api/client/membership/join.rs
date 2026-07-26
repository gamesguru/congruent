use std::{borrow::Borrow, collections::HashMap, iter::once, sync::Arc, time::Duration};

use axum::extract::State;
use axum_client_ip::ClientIp;
use conduwuit::{
	Err, PduCount, Result, debug, debug_info, debug_warn, err, error, info,
	matrix::{
		event::{gen_event_id, gen_event_id_canonical_json},
		pdu::{PduBuilder, PduEvent},
	},
	trace,
	utils::{self, shuffle, stream::ReadyExt, to_canonical_object},
	warn,
};
use futures::{FutureExt, StreamExt};
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, OwnedRoomId, OwnedServerName, OwnedUserId, RoomId,
	RoomVersionId, UserId,
	api::{
		client::{
			error::ErrorKind,
			membership::{join_room_by_id, join_room_by_id_or_alias},
		},
		federation::{self, event::event_relationships as federation_event_relationships},
	},
	canonical_json::to_canonical_value,
	events::{
		StateEventType,
		room::{
			join_rules::JoinRule,
			member::{MembershipState, RoomMemberEventContent},
		},
	},
};
use service::{
	Services,
	appservice::RegistrationInfo,
	rooms::{
		state::RoomMutexGuard,
		state_compressor::{CompressedState, HashSetCompressStateEvent},
		timeline::{AppendOptions, pdu_fits},
	},
};
use tokio::join;

use super::{banned_room_check, validate_remote_member_event_stub};
use crate::{
	Ruma,
	server::{select_authorising_user, user_can_perform_restricted_join},
};

/// # `POST /_matrix/client/r0/rooms/{roomId}/join`
///
/// Tries to join the sender user into a room.
///
/// - If the server knowns about this room: creates the join event and does auth
///   rules locally
/// - If the server does not know about the room: asks other servers over
///   federation
#[tracing::instrument(skip_all, fields(%client), name = "join", level = "info")]
pub(crate) async fn join_room_by_id_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<join_room_by_id::v3::Request>,
) -> Result<join_room_by_id::v3::Response> {
	let sender_user = body.sender_user();
	if services.users.is_suspended(sender_user).await? {
		return Err!(Request(UserSuspended("You cannot perform this action while suspended.")));
	}

	banned_room_check(
		&services,
		sender_user,
		Some(&body.room_id),
		body.room_id.server_name(),
		client,
	)
	.await?;

	// There is no body.server_name for /roomId/join
	let mut servers: Vec<_> = services
		.rooms
		.state_cache
		.servers_invite_via(&body.room_id)
		.map(ToOwned::to_owned)
		.collect()
		.await;

	servers.extend(
		services
			.rooms
			.state_cache
			.invite_state(sender_user, &body.room_id)
			.await
			.unwrap_or_default()
			.iter()
			.filter_map(|event| event.get_field("sender").ok().flatten())
			.filter_map(|sender: &str| UserId::parse(sender).ok())
			.map(|user| user.server_name().to_owned()),
	);

	if let Some(server) = body.room_id.server_name() {
		servers.push(server.into());
	}

	servers.sort_unstable();
	servers.dedup();
	shuffle(&mut servers);
	let servers = deprioritize(servers, &services.config.deprioritize_joins_through_servers);

	join_room_by_id_helper(
		&services,
		sender_user,
		&body.room_id,
		body.reason.clone(),
		&servers,
		&body.appservice_info,
		body.json_body.as_ref(),
	)
	.boxed()
	.await
}

/// # `POST /_matrix/client/r0/join/{roomIdOrAlias}`
///
/// Tries to join the sender user into a room.
///
/// - If the server knowns about this room: creates the join event and does auth
///   rules locally
/// - If the server does not know about the room: use the server name query
///   param if specified. if not specified, asks other servers over federation
///   via room alias server name and room ID server name
#[tracing::instrument(skip_all, fields(%client), name = "join", level = "info")]
pub(crate) async fn join_room_by_id_or_alias_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<join_room_by_id_or_alias::v3::Request>,
) -> Result<join_room_by_id_or_alias::v3::Response> {
	let sender_user = body.sender_user();
	let appservice_info = &body.appservice_info;
	let json_body = body.json_body.clone();
	let body = &body.body;
	if services.users.is_suspended(sender_user).await? {
		return Err!(Request(UserSuspended("You cannot perform this action while suspended.")));
	}

	let (servers, room_id) = match OwnedRoomId::try_from(body.room_id_or_alias.clone()) {
		| Ok(room_id) => {
			banned_room_check(
				&services,
				sender_user,
				Some(&room_id),
				room_id.server_name(),
				client,
			)
			.boxed()
			.await?;

			let servers = super::fetch_join_knock_servers(
				&services,
				sender_user,
				&room_id,
				body.via.clone(),
				false,
			)
			.await;

			(servers, room_id)
		},
		| Err(room_alias) => {
			let (room_id, servers) = services.rooms.alias.resolve_alias(&room_alias).await?;

			banned_room_check(
				&services,
				sender_user,
				Some(&room_id),
				Some(room_alias.server_name()),
				client,
			)
			.await?;

			let servers =
				super::fetch_join_knock_servers(&services, sender_user, &room_id, servers, true)
					.await;

			(servers, room_id)
		},
	};

	let servers = deprioritize(servers, &services.config.deprioritize_joins_through_servers);
	let join_room_response = join_room_by_id_helper(
		&services,
		sender_user,
		&room_id,
		body.reason.clone(),
		&servers,
		appservice_info,
		json_body.as_ref(),
	)
	.boxed()
	.await?;

	Ok(join_room_by_id_or_alias::v3::Response { room_id: join_room_response.room_id })
}

pub async fn join_room_by_id_helper(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	reason: Option<String>,
	servers: &[OwnedServerName],
	appservice_info: &Option<RegistrationInfo>,
	json_body: Option<&CanonicalJsonValue>,
) -> Result<join_room_by_id::v3::Response> {
	let state_lock = services.rooms.state.mutex.lock(room_id).await;

	let user_is_guest = services
		.users
		.is_deactivated(sender_user)
		.await
		.unwrap_or(false)
		&& appservice_info.is_none();

	if user_is_guest && !services.rooms.state_accessor.guest_can_join(room_id).await {
		return Err!(Request(Forbidden("Guests are not allowed to join this room")));
	}

	if services
		.rooms
		.state_cache
		.is_joined(sender_user, room_id)
		.await
	{
		debug_warn!("{sender_user} is already joined in {room_id}");
		return Ok(join_room_by_id::v3::Response { room_id: room_id.into() });
	}

	if let Err(e) = services
		.antispam
		.user_may_join_room(
			sender_user.to_owned(),
			room_id.to_owned(),
			services
				.rooms
				.state_cache
				.is_invited(sender_user, room_id)
				.await,
		)
		.await
	{
		warn!("Antispam prevented user {} from joining room {}: {}", sender_user, room_id, e);
		return Err!(Request(Forbidden("You are not allowed to join this room.")));
	}

	let mut server_in_room = services
		.rooms
		.state_cache
		.server_in_room(services.globals.server_name(), room_id)
		.await;

	// If we think we're in the room but it has no state, the room is in a
	// zombie state from a previous failed join. Force the remote path so
	// state gets bootstrapped properly.
	if server_in_room
		&& services
			.rooms
			.state
			.get_room_shortstatehash(room_id)
			.await
			.is_err()
	{
		warn!("Room {room_id} has no state despite server_in_room=true, forcing remote join");
		server_in_room = false;
	}

	// Only check our known membership if we're already in the room.
	// See: https://forgejo.ellis.link/continuwuation/continuwuity/issues/855
	let membership = if server_in_room {
		services
			.rooms
			.state_accessor
			.get_member(room_id, sender_user)
			.await
	} else {
		debug!("Ignoring local state for join {room_id}, we aren't in the room yet.");
		Ok(RoomMemberEventContent::new(MembershipState::Leave))
	};
	if let Ok(m) = membership {
		if m.membership == MembershipState::Ban {
			debug_warn!("{sender_user} is banned from {room_id} but attempted to join");
			// TODO: return reason
			return Err!(Request(Forbidden("You are banned from the room.")));
		}
	}

	if !server_in_room && servers.is_empty() {
		return Err!(Request(NotFound(
			"No servers were provided to assist in joining the room remotely, and we are not \
			 already participating in the room."
		)));
	}

	if services.antispam.check_all_joins() {
		if let Err(e) = services
			.antispam
			.meowlnir_accept_make_join(room_id.to_owned(), sender_user.to_owned())
			.await
		{
			warn!("Antispam prevented user {} from joining room {}: {}", sender_user, room_id, e);
			return Err!(Request(Forbidden("Antispam rejected join request.")));
		}
	}

	if server_in_room {
		join_room_by_id_helper_local(
			services,
			sender_user,
			room_id,
			reason,
			servers,
			state_lock,
			json_body,
		)
		.boxed()
		.await?;
	} else {
		// Ask a remote server if we are not participating in this room
		join_room_by_id_helper_remote(
			services,
			sender_user,
			room_id,
			reason,
			servers,
			state_lock,
			json_body,
		)
		.boxed()
		.await?;
	}
	Ok(join_room_by_id::v3::Response::new(room_id.to_owned()))
}

#[tracing::instrument(skip_all, fields(%sender_user, %room_id), name = "join_remote", level = "info")]
async fn join_room_by_id_helper_remote(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	reason: Option<String>,
	servers: &[OwnedServerName],
	state_lock: RoomMutexGuard,
	json_body: Option<&CanonicalJsonValue>,
) -> Result {
	info!("Joining {room_id} over federation.");

	let (make_join_response, remote_server) =
		make_join_request(services, sender_user, room_id, servers)
			.boxed()
			.await?;

	info!("make_join finished");

	let room_version_id = make_join_response
		.room_version
		.clone()
		.unwrap_or(RoomVersionId::V1);

	if !services.server.supported_room_version(&room_version_id) {
		// How did we get here?
		return Err!(BadServerResponse(
			"Remote room version {room_version_id} is not supported by conduwuit"
		));
	}

	let mut join_event_stub: CanonicalJsonObject =
		serde_json::from_str(make_join_response.event.get()).map_err(|e| {
			err!(BadServerResponse(warn!(
				"Invalid make_join event json received from server: {e:?}"
			)))
		})?;

	drop(make_join_response);

	let join_authorized_via_users_server = if !matches!(
		room_version_id,
		RoomVersionId::V1
			| RoomVersionId::V2
			| RoomVersionId::V3
			| RoomVersionId::V4
			| RoomVersionId::V5
			| RoomVersionId::V6
			| RoomVersionId::V7
	) {
		join_event_stub
			.get("content")
			.and_then(|s| s.as_object())
			.and_then(|o| o.get("join_authorised_via_users_server"))
			.and_then(|v| v.as_str())
			.and_then(|s| OwnedUserId::try_from(s).ok())
	} else {
		None
	};

	join_event_stub.insert(
		"origin_server_ts".to_owned(),
		CanonicalJsonValue::Integer(
			utils::millis_since_unix_epoch()
				.try_into()
				.expect("Timestamp is valid js_int value"),
		),
	);
	let mut content = to_canonical_value(RoomMemberEventContent {
		displayname: services.users.displayname(sender_user).boxed().await.ok(),
		avatar_url: services.users.avatar_url(sender_user).boxed().await.ok(),
		blurhash: services.users.blurhash(sender_user).boxed().await.ok(),
		reason,
		join_authorized_via_users_server: join_authorized_via_users_server.clone(),
		..RoomMemberEventContent::new(MembershipState::Join)
	})
	.expect("event is valid, we just created it");

	if let Some(CanonicalJsonValue::Object(custom)) = json_body {
		if let CanonicalJsonValue::Object(ref mut map) = content {
			for (k, v) in custom {
				if !["reason", "third_party_signed", "server_name"].contains(&k.as_str()) {
					map.entry(k.clone()).or_insert_with(|| v.clone());
				}
			}
		}
	}

	join_event_stub.insert("content".to_owned(), content);

	// We keep the "event_id" in the pdu only in v1 or
	// v2 rooms
	match room_version_id {
		| RoomVersionId::V1 | RoomVersionId::V2 => {},
		| _ => {
			join_event_stub.remove("event_id");
		},
	}

	// In order to create a compatible ref hash (EventID) the `hashes` field needs
	// to be present
	services
		.server_keys
		.hash_and_sign_event(&mut join_event_stub, &room_version_id)?;

	// Generate event id
	let event_id = gen_event_id(&join_event_stub, &room_version_id)?;

	// Add event_id back
	join_event_stub
		.insert("event_id".to_owned(), CanonicalJsonValue::String(event_id.clone().into()));

	// It has enough fields to be called a proper event now
	let mut join_event = join_event_stub;

	info!("Asking {remote_server} for send_join in room {room_id}");
	let send_join_request = federation::membership::create_join_event::v2::Request {
		room_id: room_id.to_owned(),
		event_id: event_id.clone(),
		omit_members: false,
		pdu: services
			.sending
			.convert_to_outgoing_federation_event(join_event.clone())
			.boxed()
			.await,
	};

	let send_join_response = Box::new(
		match services
			.sending
			.send_synapse_request(&remote_server, send_join_request)
			.boxed()
			.await
		{
			| Ok(response) => response,
			| Err(e) => {
				error!("send_join failed: {e:?}");
				return Err(e);
			},
		},
	);

	info!("send_join finished");

	if join_authorized_via_users_server.is_some() {
		if let Some(signed_raw) = &send_join_response.room_state.event {
			debug_info!(
				"There is a signed event with join_authorized_via_users_server. This room is \
				 probably using restricted joins. Adding signature to our event"
			);

			let (signed_event_id, signed_value) =
				gen_event_id_canonical_json(signed_raw, &room_version_id).map_err(|e| {
					err!(Request(BadJson(warn!(
						"Could not convert event to canonical JSON: {e}"
					))))
				})?;

			if signed_event_id != event_id {
				return Err!(Request(BadJson(warn!(
					%signed_event_id, %event_id,
					"Server {remote_server} sent event with wrong event ID"
				))));
			}

			match signed_value["signatures"]
				.as_object()
				.ok_or_else(|| {
					err!(BadServerResponse(warn!(
						"Server {remote_server} sent invalid signatures type"
					)))
				})
				.and_then(|e| {
					e.get(remote_server.as_str()).ok_or_else(|| {
						err!(BadServerResponse(warn!(
							"Server {remote_server} did not send its signature for a restricted \
							 room"
						)))
					})
				}) {
				| Ok(signature) => {
					join_event
						.get_mut("signatures")
						.expect("we created a valid pdu")
						.as_object_mut()
						.expect("we created a valid pdu")
						.insert(remote_server.to_string(), signature.clone());
				},
				| Err(e) => {
					warn!(
						"Server {remote_server} sent invalid signature in send_join signatures \
						 for event {signed_value:?}: {e:?}",
					);
				},
			}
		}
	}

	services
		.rooms
		.short
		.get_or_create_shortroomid(room_id)
		.boxed()
		.await;

	let remote_latest_events: Vec<ruma::OwnedEventId> = join_event
		.get("prev_events")
		.and_then(|v| v.as_array())
		.map(|arr| {
			arr.iter()
				.filter_map(|v| {
					v.as_str()
						.and_then(|s| <&ruma::EventId>::try_from(s).ok().map(ToOwned::to_owned))
				})
				.collect()
		})
		.unwrap_or_default();

	Box::pin(join_room_by_id_helper_remote_process(
		services,
		sender_user,
		room_id,
		room_version_id,
		remote_server,
		join_event,
		event_id,
		state_lock,
		*send_join_response,
		remote_latest_events,
	))
	.await
}

#[tracing::instrument(skip_all, fields(%sender_user, %room_id), name = "join_remote_process", level = "info")]
#[allow(clippy::too_many_arguments)]
async fn join_room_by_id_helper_remote_process(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	room_version_id: RoomVersionId,
	remote_server: OwnedServerName,
	join_event: CanonicalJsonObject,
	event_id: ruma::OwnedEventId,
	mut state_lock: RoomMutexGuard,
	send_join_response: federation::membership::create_join_event::v2::Response,
	remote_latest_events: Vec<ruma::OwnedEventId>,
) -> Result {
	info!("Parsing join event");
	let parsed_join_pdu = Box::new(
		PduEvent::from_id_val(&event_id, join_event.clone(), Some(room_id))
			.map_err(|e| err!(BadServerResponse("Invalid join event PDU: {e:?}")))?,
	);

	info!("Acquiring server signing keys for response events");
	let resp_events = &send_join_response.room_state;
	let resp_state = &resp_events.state;
	let resp_auth = &resp_events.auth_chain;
	services
		.server_keys
		.acquire_events_pubkeys(resp_auth.iter().chain(resp_state.iter()))
		.boxed()
		.await;

	info!("Going through send_join response room_state");
	let cork = services.db.cork_and_flush();
	let mut outlier_event_ids: Vec<ruma::OwnedEventId> = Vec::new();
	let state = services
		.server_keys
		.concurrent_validate_and_add_events(send_join_response.room_state.state, &room_version_id)
		.filter_map(|res| async {
			match res {
				| Ok(val) => Some(val),
				| Err(e) => {
					info!("Could not validate send_join response room_state event: {e:?}");
					None
				},
			}
		})
		.fold(
			(HashMap::new(), Vec::new()),
			|(mut state, mut eids), (event_id, value)| async move {
				let pdu = match PduEvent::from_id_val(&event_id, value.clone(), Some(room_id)) {
					| Ok(pdu) => pdu,
					| Err(e) => {
						info!("Invalid PDU in send_join response: {e:?}: {value:#?}");
						return (state, eids);
					},
				};
				if !pdu_fits(&mut value.clone()) {
					warn!(
						"dropping incoming PDU {event_id} in room {room_id} from room join \
						 because it exceeds 65535 bytes or is otherwise too large."
					);
					return (state, eids);
				}
				services
					.rooms
					.outlier
					.add_pdu_outlier(&event_id, &value, Some(room_id));
				services.rooms.pdu_metadata.clear_pdu_markers(&event_id);
				eids.push(event_id.clone());
				if let Some(state_key) = &pdu.state_key {
					let shortstatekey = services
						.rooms
						.short
						.get_or_create_shortstatekey(&pdu.kind.to_string().into(), state_key)
						.boxed()
						.await;

					state.insert(shortstatekey, pdu.event_id.clone());
				}
				(state, eids)
			},
		)
		.boxed()
		.await;

	let (state, mut state_eids) = state;
	outlier_event_ids.append(&mut state_eids);
	// Capture this before promoting send_join state/auth outliers. Promotion makes
	// a fresh room look non-empty and otherwise misclassifies first joins as
	// rejoins.
	let had_timeline_before_join = services
		.rooms
		.timeline
		.last_timeline_count(room_id)
		.await
		.is_ok_and(|count| count != PduCount::min());
	info!(
		room_id = %room_id,
		had_timeline_before_join,
		remote_latest_count = remote_latest_events.len(),
		"join bootstrap classification before send_join outlier promotion"
	);

	drop(cork);

	info!("Going through send_join response auth_chain");
	let cork = services.db.cork_and_flush();
	let auth_eids: Vec<ruma::OwnedEventId> = services
		.server_keys
		.concurrent_validate_and_add_events(
			send_join_response.room_state.auth_chain,
			&room_version_id,
		)
		.filter_map(|res| async {
			match res {
				| Ok(val) => Some(val),
				| Err(e) => {
					info!("Could not validate send_join response auth_chain event: {e:?}");
					None
				},
			}
		})
		.fold(Vec::new(), |mut eids, (event_id, value)| async move {
			trace!(%event_id, "Adding PDU as an outlier from send_join auth_chain");
			services
				.rooms
				.outlier
				.add_pdu_outlier(&event_id, &value, Some(room_id));
			services.rooms.pdu_metadata.clear_pdu_markers(&event_id);
			eids.push(event_id);
			eids
		})
		.boxed()
		.await;

	outlier_event_ids.extend(auth_eids);

	drop(cork);

	debug!("Running send_join auth check");
	// Build auth state from the send_join response state for rezzy
	let fetch_state = &state;
	let mut auth_events: HashMap<
		(StateEventType, conduwuit::matrix::state_key::StateKey),
		PduEvent,
	> = HashMap::new();
	for (&shortstatekey, event_id) in fetch_state {
		if let Ok((ty, sk)) = services
			.rooms
			.short
			.get_statekey_from_short(shortstatekey)
			.boxed()
			.await
		{
			if let Ok(pdu) = services.rooms.timeline.get_pdu(event_id).boxed().await {
				auth_events.insert((ty, sk), pdu);
			}
		}
	}

	let state_provider =
		conduwuit_service::rooms::auth_adapter::PduStateProvider::from_ruma_map(&auth_events);
	let auth_check = conduwuit_service::rooms::auth_adapter::rezzy_auth_check(
		&parsed_join_pdu,
		&state_provider,
		conduwuit_service::rooms::auth_adapter::to_state_res_version(&room_version_id),
	);

	if !auth_check {
		return Err!(Request(Forbidden("Auth check failed")));
	}

	info!("Compressing state from send_join ({} state events)", state.len());
	let compressed: CompressedState = services
		.rooms
		.state_compressor
		.compress_state_events(state.iter().map(|(ssk, eid)| (ssk, eid.borrow())))
		.collect()
		.boxed()
		.await;

	drop(state);

	info!("Saving compressed state ({} compressed events)", compressed.len());
	let HashSetCompressStateEvent {
		shortstatehash: statehash_before_join,
		added,
		removed,
	} = services
		.rooms
		.state_compressor
		.save_state_as_root(room_id, Arc::new(compressed))
		.boxed()
		.await?;

	info!("Forcing state for new room (shortstatehash={statehash_before_join})");
	Box::pin(services.rooms.state.force_state(
		room_id,
		statehash_before_join,
		added,
		removed,
		&state_lock,
	))
	.await?;

	// Promote auth chain + state outliers to the backfilled timeline.
	// This makes the room's origin events (create, initial joins, power
	// levels) visible when users scroll up. Events are topo-sorted so
	// ancestors appear before descendants.
	if !outlier_event_ids.is_empty() {
		info!(
			"Promoting {} auth chain + state outliers to timeline in {room_id}",
			outlier_event_ids.len()
		);
		match services
			.rooms
			.timeline
			.promote_outliers_sorted(room_id, &outlier_event_ids, &room_version_id)
			.await
		{
			| Ok(count) => info!("Promoted {count} outliers to timeline in {room_id}"),
			| Err(e) => warn!("Failed to promote outliers in {room_id}: {e}"),
		}
	}

	info!("Reconciling membership cache for new room");
	services
		.rooms
		.state_cache
		.reconcile_membership(room_id)
		.boxed()
		.await;

	info!("Updating joined counts for new room");
	services
		.rooms
		.state_cache
		.update_joined_count(room_id)
		.boxed()
		.await;

	let post_force_count = services
		.rooms
		.state_cache
		.room_joined_count(room_id)
		.await
		.unwrap_or(0);
	info!(
		"join: after force_state+update_joined_count for {room_id}: \
		 joined_count={post_force_count}"
	);

	// Collect missing extremities from the remote server's DAG tips. We need to
	// handle these differently for first-join vs re-join to avoid race conditions.
	let mut missing_latest = Vec::new();
	let mut is_rejoin = false;
	if !remote_latest_events.is_empty() {
		for event_id in &remote_latest_events {
			if !services
				.rooms
				.timeline
				.non_outlier_pdu_exists(event_id)
				.await
			{
				missing_latest.push(event_id.clone());
			}
		}
		if !missing_latest.is_empty() {
			// Determine whether this is a first join or a re-join. For first
			// joins the room has no timeline events yet, so pre-join
			// extremities should be inserted as Backfilled (historical). For re-joins
			// the room already has events from prior membership, so extremities that
			// occurred while away should be Normal (live).
			is_rejoin = had_timeline_before_join;
		}
	}

	// Phase 1: For FIRST joins only, handle pre-join extremities BEFORE
	// appending the join event. This ensures extremities get lower Normal
	// PduCounts that sort before the join event, maintaining chronological
	// order in backward pagination.
	// We drop the state lock temporarily for the network requests.
	if !missing_latest.is_empty() && !is_rejoin {
		info!(
			room_id = %room_id,
			count = missing_latest.len(),
			"join bootstrap using outlier-only extremity ingestion"
		);
		drop(state_lock);

		info!(
			"Forward-filling {} missing extremities from {} before joining room {}",
			missing_latest.len(),
			remote_server,
			room_id
		);
		for event_id in &missing_latest {
			if let Err(e) =
				fetch_missing_extremity(services, &remote_server, room_id, event_id, false).await
			{
				warn!("Failed to fetch missing extremity {event_id}: {e}");
			}
		}

		// Re-acquire the state lock before appending our join event
		state_lock = services.rooms.state.mutex.lock(room_id).await;
	}

	// We append to state before appending the pdu, so we don't have a moment in
	// time with the pdu without its state. Both append_to_state and append_pdu
	// can indeed fail, in which case the local membership cache may be left in an
	// inconsistent state (where the user appears joined in the cache but the join
	// PDU is not persisted).
	// TODO: unify the state write, timeline write, and membership-cache update
	// behind a single durable operation so partial failure cannot strand cache
	// state ahead of the persisted join event.
	let statehash_after_join = services
		.rooms
		.state
		.append_to_state(&parsed_join_pdu, room_id)
		.boxed()
		.await?;

	info!("Appending new room join event");
	services
		.rooms
		.timeline
		.append_pdu(
			&parsed_join_pdu,
			join_event,
			once(parsed_join_pdu.event_id.clone()),
			AppendOptions { resolved_state: None, soft_fail: false },
			&state_lock,
			room_id,
		)
		.boxed()
		.await?;

	let post_append_count = services
		.rooms
		.state_cache
		.room_joined_count(room_id)
		.await
		.unwrap_or(0);
	info!("join: after append_pdu for {room_id}: joined_count={post_append_count}");

	info!("Setting final room state for new room");
	// We set the room state after inserting the pdu, so that we never have a moment
	// in time where events in the current room state do not exist
	services
		.rooms
		.state
		.set_room_state(room_id, statehash_after_join, &state_lock);

	// Phase 2: For RE-JOINS only, forward-fill extremities AFTER the join event
	// is committed to the timeline and room state is set. This ensures that
	// handle_incoming_pdu's server_in_room check succeeds (our join PDU proves
	// participation) and prevents the "not participating" race condition.
	if !missing_latest.is_empty() && is_rejoin {
		info!(
			room_id = %room_id,
			count = missing_latest.len(),
			"join bootstrap using timeline extremity ingestion for rejoin"
		);
		drop(state_lock);

		info!(
			"Forward-filling {} missing extremities from {} after joining room {}",
			missing_latest.len(),
			remote_server,
			room_id
		);
		for event_id in missing_latest {
			if let Err(e) =
				fetch_missing_extremity(services, &remote_server, room_id, &event_id, true).await
			{
				warn!("Failed to fetch missing extremity {event_id}: {e}");
			}
		}
	}

	Ok(())
}

#[tracing::instrument(skip_all, fields(%sender_user, %room_id), name = "join_local", level = "info")]
async fn join_room_by_id_helper_local(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	reason: Option<String>,
	servers: &[OwnedServerName],
	mut state_lock: RoomMutexGuard,
	json_body: Option<&CanonicalJsonValue>,
) -> Result {
	info!("Joining room locally");

	let (room_version, join_rules, is_invited) = join!(
		services.rooms.state.get_room_version(room_id),
		services.rooms.state_accessor.get_join_rules(room_id),
		services.rooms.state_cache.is_invited(sender_user, room_id)
	);

	let room_version = room_version?;
	let mut auth_user: Option<OwnedUserId> = None;
	let is_restricted =
		matches!(join_rules, JoinRule::Restricted(_) | JoinRule::KnockRestricted(_));

	if !is_invited && is_restricted {
		use RoomVersionId::*;
		if !matches!(room_version, V1 | V2 | V3 | V4 | V5 | V6 | V7) {
			// This is a restricted room, check if we can complete the join requirements
			// locally.
			let restricted_result =
				user_can_perform_restricted_join(services, sender_user, room_id, &room_version)
					.await;
			match &restricted_result {
				| Ok(Some(allowed_rooms)) => {
					// User qualifies via allowed room membership. Try to find a
					// local user who can issue the authorising invite.
					auth_user = select_authorising_user(services, room_id, allowed_rooms)
						.await
						.ok();

					// User qualifies but no local authorizer found.
					// A local user with invite power may exist but their power level
					// hasn't propagated over federation yet (often happens in tests).
					// Drop the state lock while waiting so inbound federation can
					// finish publishing the updated room state we are waiting on.
					if auth_user.is_none() {
						drop(state_lock);
						for _ in 0..5 {
							tokio::time::sleep(Duration::from_millis(150)).await;
							auth_user = select_authorising_user(services, room_id, allowed_rooms)
								.await
								.ok();
							if auth_user.is_some() {
								break;
							}
						}

						state_lock = services.rooms.state.mutex.lock(room_id).await;
						auth_user = select_authorising_user(services, room_id, allowed_rooms)
							.await
							.ok();
					}

					// User qualifies but no local authorizer found -- another
					// server might be able to authorize, so go remote.
					if auth_user.is_none() {
						return join_restricted_via_remote(
							services,
							sender_user,
							room_id,
							reason,
							servers,
							state_lock,
							json_body,
						)
						.await;
					}
				},
				| Err(e) if e.status_code() == http::StatusCode::FORBIDDEN => {
					// User definitively doesn't qualify (not in any allowed
					// room). Let local join attempt properly fail with 403.
					info!("User cannot perform restricted join: {e}");
				},
				| Err(_) => {
					// We couldn't verify all requirements (e.g. we're not in
					// one of the allowed rooms). Try remote and hope it works.
					return join_restricted_via_remote(
						services,
						sender_user,
						room_id,
						reason,
						servers,
						state_lock,
						json_body,
					)
					.await;
				},
				| Ok(None) => {
					// Room is not actually restricted (shouldn't reach here
					// given is_restricted check, but handle gracefully).
				},
			}
		}
	}

	let content = RoomMemberEventContent {
		displayname: services.users.displayname(sender_user).await.ok(),
		avatar_url: services.users.avatar_url(sender_user).await.ok(),
		blurhash: services.users.blurhash(sender_user).await.ok(),
		reason: reason.clone(),
		join_authorized_via_users_server: auth_user,
		..RoomMemberEventContent::new(MembershipState::Join)
	};

	let mut content = serde_json::to_value(content).expect("failed to serialize member event");
	if let Some(CanonicalJsonValue::Object(custom)) = json_body {
		if let serde_json::Value::Object(ref mut map) = content {
			for (k, v) in custom {
				if !["reason", "third_party_signed", "server_name"].contains(&k.as_str()) {
					map.entry(k.clone())
						.or_insert_with(|| serde_json::to_value(v).expect("valid json"));
				}
			}
		}
	}

	let builder = PduBuilder {
		event_type: StateEventType::RoomMember.into(),
		content: serde_json::value::to_raw_value(&content).expect("valid JSON"),
		state_key: Some(sender_user.to_string().into()),
		..Default::default()
	};

	// For non-restricted rooms, the local join is authoritative -- no fallback.
	let event_id = services
		.rooms
		.timeline
		.build_and_append_pdu(builder, sender_user, Some(room_id), &state_lock)
		.await?;

	let pdu_id = services.rooms.timeline.get_pdu_id(&event_id).await?;

	drop(state_lock);

	let remote_servers = services
		.rooms
		.state_cache
		.room_servers(room_id)
		.ready_filter(|server| !services.globals.server_is_ours(server))
		.map(ToOwned::to_owned)
		.collect::<Vec<_>>()
		.await;

	services
		.sending
		.wait_for_pdu_servers(remote_servers, &pdu_id, Duration::from_secs(15))
		.await?;

	info!("Joined room locally");
	Ok(())
}

/// For restricted/knock_restricted rooms where the local server cannot
/// authorize the join (no local user can issue the invite), attempt a
/// remote join via federation. This is the Synapse-style upfront decision
/// rather than a try-local-then-fallback approach.
async fn join_restricted_via_remote(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	reason: Option<String>,
	servers: &[OwnedServerName],
	state_lock: RoomMutexGuard,
	json_body: Option<&CanonicalJsonValue>,
) -> Result {
	let has_remote_servers = !(servers.is_empty()
		|| servers.len() == 1 && services.globals.server_is_ours(&servers[0]));

	// If the client didn't provide useful remote servers, try to discover them
	// from room state. This is essential for restricted joins where local auth
	// may fail due to stale state but a remote server can authorize via
	// make_join/send_join.
	let discovered_servers;
	let servers = if has_remote_servers {
		servers
	} else {
		discovered_servers = services
			.rooms
			.state_cache
			.room_servers(room_id)
			.ready_filter(|server| !services.globals.server_is_ours(server))
			.map(ToOwned::to_owned)
			.collect::<Vec<_>>()
			.await;

		if discovered_servers.is_empty() {
			return Err!(Request(Forbidden(
				"Cannot authorize restricted join locally and no remote servers available."
			)));
		}

		discovered_servers.as_slice()
	};

	info!(
		remote_servers = %servers.len(),
		"Cannot authorize restricted join locally, attempting remote join",
	);
	Box::pin(join_room_by_id_helper_remote(
		services,
		sender_user,
		room_id,
		reason,
		servers,
		state_lock,
		json_body,
	))
	.await
}

async fn make_join_request(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	servers: &[OwnedServerName],
) -> Result<(federation::membership::prepare_join_event::v1::Response, OwnedServerName)> {
	const MAX_SERVERS_TO_TRY: usize = 5;
	const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
	// When a server returns M_FORBIDDEN for a restricted join, the user's
	// membership in the allowed room may not have been federated yet (the
	// make_join HTTP request can outpace the queued federation transaction).
	// Retry the same server once after a short delay.
	const RESTRICTED_RETRY_DELAY: Duration = Duration::from_millis(500);

	let mut make_join_counter: usize = 0;
	let mut last_error = None;

	for remote_server in servers.iter().take(MAX_SERVERS_TO_TRY) {
		if services.globals.server_is_ours(remote_server) {
			continue;
		}
		make_join_counter = make_join_counter.saturating_add(1);
		info!(
			"Asking {remote_server} for make_join (attempt \
			 {make_join_counter}/{MAX_SERVERS_TO_TRY})",
		);

		let mut forbidden_retried = false;
		loop {
			let make_join_response = tokio::time::timeout(
				REQUEST_TIMEOUT,
				services.sending.send_federation_request(
					remote_server,
					federation::membership::prepare_join_event::v1::Request {
						room_id: room_id.to_owned(),
						user_id: sender_user.to_owned(),
						ver: services.server.supported_room_versions().collect(),
					},
				),
			)
			.await;

			trace!("make_join response: {:?}", make_join_response);

			match make_join_response {
				| Ok(Ok(response)) => {
					info!("Received make_join response from {remote_server}");
					if let Err(e) = validate_remote_member_event_stub(
						&MembershipState::Join,
						sender_user,
						room_id,
						response.room_version.as_ref().unwrap_or(&RoomVersionId::V1),
						&to_canonical_object(&response.event)?,
					) {
						warn!("make_join response from {remote_server} failed validation: {e}");
						break;
					}
					return Ok((response, remote_server.clone()));
				},
				| Ok(Err(e)) => {
					match e.kind() {
						| ErrorKind::UnableToAuthorizeJoin => {
							info!(
								"{remote_server} was unable to verify the joining user \
								 satisfied restricted join requirements: {e}. Will continue \
								 trying."
							);
						},
						| ErrorKind::UnableToGrantJoin => {
							info!(
								"{remote_server} believes the joining user satisfies restricted \
								 join rules, but is unable to authorise a join for us. Will \
								 continue trying."
							);
						},
						| ErrorKind::IncompatibleRoomVersion { room_version } => {
							warn!(
								"{remote_server} reports the room we are trying to join is \
								 v{room_version}, which we do not support."
							);
							return Err(e);
						},
						| ErrorKind::Forbidden { .. } => {
							if !forbidden_retried {
								info!(
									"{remote_server} returned M_FORBIDDEN, retrying after \
									 {RESTRICTED_RETRY_DELAY:?} to allow federation to propagate"
								);
								forbidden_retried = true;
								tokio::time::sleep(RESTRICTED_RETRY_DELAY).await;
								continue;
							}
							warn!("{remote_server} refuses to let us join: {e}.");
						},
						| ErrorKind::NotFound => {
							info!(
								"{remote_server} does not know about {room_id}: {e}. Will \
								 continue trying."
							);
						},
						| _ => {
							info!(
								"{remote_server} failed to make_join: {e}. Will continue trying."
							);
						},
					}
					last_error = Some(e);
				},
				| Err(_elapsed) => {
					info!("{remote_server} timed out for make_join. Will continue trying.");
				},
			}
			break;
		}
	}
	info!("All {make_join_counter} servers tried were unable to assist in joining {room_id} :(");
	if let Some(e) = last_error {
		return Err(e);
	}
	Err!(BadServerResponse("No server available to assist in joining."))
}

/// Moves deprioritized servers (if any) to the back of the list.
///
/// No-op if we aren't given any servers to deprioritize.
fn deprioritize(
	servers: Vec<OwnedServerName>,
	deprioritized: &[OwnedServerName],
) -> Vec<OwnedServerName> {
	if deprioritized.is_empty() {
		return servers;
	}

	let (mut depr, mut servers): (Vec<_>, Vec<_>) =
		servers.into_iter().partition(|s| deprioritized.contains(s));
	servers.append(&mut depr);
	servers
}

#[cfg(test)]
mod tests {
	use ruma::OwnedServerName;

	use super::*;

	#[test]
	fn deprioritizing_servers_works() -> Result<(), Box<dyn std::error::Error>> {
		let servers = vec![
			"example.com".try_into()?,
			"slow.invalid".try_into()?,
			"example.org".try_into()?,
		];
		let depr = vec!["slow.invalid".try_into()?];
		let expected: Vec<OwnedServerName> = vec![
			"example.com".try_into()?,
			"example.org".try_into()?,
			"slow.invalid".try_into()?,
		];

		let servers = deprioritize(servers, &depr);
		assert_eq!(servers, expected);
		Ok(())
	}

	#[test]
	fn empty_deprioritized_is_noop() -> Result<(), Box<dyn std::error::Error>> {
		let servers = vec![
			"example.com".try_into()?,
			"slow.invalid".try_into()?,
			"example.org".try_into()?,
		];

		let depr_servers = deprioritize(servers.clone(), &[]);
		assert_eq!(depr_servers, servers);
		Ok(())
	}
}

async fn fetch_missing_extremity(
	services: &Services,
	remote_server: &OwnedServerName,
	room_id: &RoomId,
	event_id: &ruma::OwnedEventId,
	is_timeline_event: bool,
) -> Result<()> {
	let request = federation_event_relationships::unstable::Request {
		event_id: event_id.clone(),
		room_id: Some(room_id.to_owned()),
		max_depth: None,
		max_breadth: None,
		limit: None,
		depth_first: None,
		recent_first: None,
		include_parent: None,
		include_children: None,
		direction: None,
		batch: None,
	};
	info!(
		%room_id,
		%event_id,
		is_timeline_event,
		"fetching join extremity with explicit ingestion mode"
	);

	if let Ok(response) = services
		.sending
		.send_federation_request(remote_server, request)
		.await
	{
		if let Some(raw_event) = response.events.into_iter().next() {
			let (parsed_room_id, parsed_event_id, value) = services
				.rooms
				.event_handler
				.parse_incoming_pdu(&raw_event)
				.await
				.map_err(|e| {
					warn!(
						"Failed to parse missing extremity {event_id} from \
						 /event_relationships: {e}"
					);
					e
				})?;

			if parsed_room_id == room_id {
				if let Err(e) = services
					.rooms
					.event_handler
					.handle_incoming_pdu(
						remote_server,
						room_id,
						&parsed_event_id,
						value,
						is_timeline_event,
						None,
					)
					.await
				{
					warn!("Failed to handle missing extremity {event_id}: {e}");
				}
				return Ok(());
			}

			warn!(
				%parsed_event_id,
				%parsed_room_id,
				%room_id,
				%remote_server,
				"Room ID mismatch in missing extremity fetch via /event_relationships"
			);
		}
	}

	let request = federation::event::get_event::v1::Request {
		event_id: event_id.clone(),
		include_unredacted_content: Some(false),
	};
	let response = match services
		.sending
		.send_federation_request(remote_server, request)
		.await
	{
		| Ok(r) => r,
		| Err(e) => {
			warn!("Failed to fetch missing extremity {event_id}: {e}");
			return Ok(());
		},
	};
	let (parsed_room_id, parsed_event_id, value) = match services
		.rooms
		.event_handler
		.parse_incoming_pdu(&response.pdu)
		.await
	{
		| Ok(v) => v,
		| Err(e) => {
			warn!("Failed to parse extremity {event_id}: {e}");
			return Ok(());
		},
	};
	if parsed_room_id != room_id {
		warn!(
			%parsed_event_id,
			%parsed_room_id,
			%room_id,
			%remote_server,
			"Room ID mismatch in send_join extremity fetch: event belongs to parsed room, expected target room"
		);
		return Ok(());
	}
	if let Err(e) = services
		.rooms
		.event_handler
		.handle_incoming_pdu(
			remote_server,
			room_id,
			&parsed_event_id,
			value,
			is_timeline_event,
			None,
		)
		.await
	{
		warn!("Failed to handle extremity {event_id}: {e}");
	}

	Ok(())
}

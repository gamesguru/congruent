use std::collections::BTreeMap;

use axum::extract::State;
use axum_client_ip::ClientIp;
use conduwuit::{Err, Result};
use futures::{FutureExt, StreamExt};
use ruma::{
	OwnedRoomId,
	api::{
		client::{
			membership::mutual_rooms,
			profile::{delete_profile_key, get_profile_key, set_profile_key},
			relations::event_relationships,
		},
		federation,
	},
	presence::PresenceState,
};

use super::{update_avatar_url, update_displayname};
use crate::{
	Ruma,
	msc2836::{self, Params, Requester},
};

/// # `GET /_matrix/client/unstable/uk.half-shot.msc2666/user/mutual_rooms`
///
/// Gets all the rooms the sender shares with the specified user.
///
/// TODO: Implement pagination, currently this just returns everything
///
/// An implementation of [MSC2666](https://github.com/matrix-org/matrix-spec-proposals/pull/2666)
#[tracing::instrument(skip_all, fields(%client), name = "mutual_rooms", level = "info")]
pub(crate) async fn get_mutual_rooms_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<mutual_rooms::unstable::Request>,
) -> Result<mutual_rooms::unstable::Response> {
	let sender_user = body.sender_user();

	if sender_user == body.user_id {
		return Err!(Request(Unknown("You cannot request rooms in common with yourself.")));
	}

	if !services.users.exists(&body.user_id).await {
		return Ok(mutual_rooms::unstable::Response { joined: vec![], next_batch_token: None });
	}

	let mutual_rooms: Vec<OwnedRoomId> = services
		.rooms
		.state_cache
		.get_shared_rooms(sender_user, &body.user_id)
		.map(ToOwned::to_owned)
		.collect()
		.await;

	Ok(mutual_rooms::unstable::Response {
		joined: mutual_rooms,
		next_batch_token: None,
	})
}

/// # `PUT /_matrix/client/unstable/uk.tcpip.msc4133/profile/{user_id}/{field}`
///
/// Updates the profile key-value field of a user, as per MSC4133.
///
/// This also handles the avatar_url and displayname being updated.
pub(crate) async fn set_profile_key_route(
	State(services): State<crate::State>,
	body: Ruma<set_profile_key::unstable::Request>,
) -> Result<set_profile_key::unstable::Response> {
	let sender_user = body.sender_user();

	if *sender_user != body.user_id && body.appservice_info.is_none() {
		return Err!(Request(Forbidden("You cannot update the profile of another user")));
	}

	if body.kv_pair.is_empty() {
		return Err!(Request(BadJson(
			"The key-value pair JSON body is empty. Use DELETE to delete a key"
		)));
	}

	if body.kv_pair.len() > 1 {
		// TODO: support PATCH or "recursively" adding keys in some sort
		return Err!(Request(BadJson(
			"This endpoint can only take one key-value pair at a time"
		)));
	}

	let Some(profile_key_value) = body.kv_pair.get(&body.key_name) else {
		return Err!(Request(BadJson(
			"The key does not match the URL field key, or JSON body is empty (use DELETE)"
		)));
	};

	if body.kv_pair.keys().any(|key| key.len() > 128) {
		return Err!(Request(BadJson("Key names cannot be longer than 128 bytes")));
	}

	if body.key_name == "displayname" {
		let Some(display_name) = profile_key_value.as_str() else {
			return Err!(Request(BadJson("displayname must be a string")));
		};
		let all_joined_rooms: Vec<OwnedRoomId> = services
			.rooms
			.state_cache
			.rooms_joined(&body.user_id)
			.map(Into::into)
			.collect()
			.await;

		update_displayname(
			&services,
			&body.user_id,
			Some(display_name.to_owned()),
			&all_joined_rooms,
		)
		.boxed()
		.await;
	} else if body.key_name == "avatar_url" {
		let Some(avatar_url) = profile_key_value.as_str() else {
			return Err!(Request(BadJson("avatar_url must be a string")));
		};
		let mxc = ruma::OwnedMxcUri::from(avatar_url);

		let all_joined_rooms: Vec<OwnedRoomId> = services
			.rooms
			.state_cache
			.rooms_joined(&body.user_id)
			.map(Into::into)
			.collect()
			.await;

		Box::pin(update_avatar_url(&services, &body.user_id, Some(mxc), None, &all_joined_rooms))
			.await;
	} else {
		services.users.set_profile_key(
			&body.user_id,
			&body.key_name,
			Some(profile_key_value.clone()),
		);
	}

	if services.config.allow_local_presence {
		// Presence update
		services
			.presence
			.ping_presence(&body.user_id, &PresenceState::Online)
			.await?;
	}

	Ok(set_profile_key::unstable::Response {})
}

/// # `DELETE /_matrix/client/unstable/uk.tcpip.msc4133/profile/{user_id}/{field}`
///
/// Deletes the profile key-value field of a user, as per MSC4133.
///
/// This also handles the avatar_url and displayname being updated.
pub(crate) async fn delete_profile_key_route(
	State(services): State<crate::State>,
	body: Ruma<delete_profile_key::unstable::Request>,
) -> Result<delete_profile_key::unstable::Response> {
	let sender_user = body.sender_user();

	if *sender_user != body.user_id && body.appservice_info.is_none() {
		return Err!(Request(Forbidden("You cannot update the profile of another user")));
	}

	if body.kv_pair.len() > 1 {
		// TODO: support PATCH or "recursively" adding keys in some sort
		return Err!(Request(BadJson(
			"This endpoint can only take one key-value pair at a time"
		)));
	}

	if body.key_name == "displayname" {
		let all_joined_rooms: Vec<OwnedRoomId> = services
			.rooms
			.state_cache
			.rooms_joined(&body.user_id)
			.map(Into::into)
			.collect()
			.await;

		update_displayname(&services, &body.user_id, None, &all_joined_rooms)
			.boxed()
			.await;
	} else if body.key_name == "avatar_url" {
		let all_joined_rooms: Vec<OwnedRoomId> = services
			.rooms
			.state_cache
			.rooms_joined(&body.user_id)
			.map(Into::into)
			.collect()
			.await;

		Box::pin(update_avatar_url(&services, &body.user_id, None, None, &all_joined_rooms))
			.await;
	} else {
		services
			.users
			.set_profile_key(&body.user_id, &body.key_name, None);
	}

	if services.config.allow_local_presence {
		// Presence update
		services
			.presence
			.ping_presence(&body.user_id, &PresenceState::Online)
			.await?;
	}

	Ok(delete_profile_key::unstable::Response {})
}

/// # `GET /_matrix/client/unstable/uk.tcpip.msc4133/profile/{userId}/{field}}`
///
/// Gets the profile key-value field of a user, as per MSC4133.
///
/// - If user is on another server and we do not have a local copy already fetch
///   the value over federation
pub(crate) async fn get_profile_key_route(
	State(services): State<crate::State>,
	body: Ruma<get_profile_key::unstable::Request>,
) -> Result<get_profile_key::unstable::Response> {
	let mut profile_key_value: BTreeMap<String, serde_json::Value> = BTreeMap::new();

	if !services.globals.user_is_local(&body.user_id) {
		// Create and update our local copy of the user
		if let Ok(response) = services
			.sending
			.send_federation_request(
				body.user_id.server_name(),
				federation::query::get_profile_information::v1::Request {
					user_id: body.user_id.clone(),
					field: None, // we want the full user's profile to update locally as well
				},
			)
			.await
		{
			if !services.users.exists(&body.user_id).await {
				services.users.create(&body.user_id, None, None).await?;
			}

			services
				.users
				.set_displayname(&body.user_id, response.displayname.clone());

			services
				.users
				.set_avatar_url(&body.user_id, response.avatar_url.clone());

			services
				.users
				.set_blurhash(&body.user_id, response.blurhash.clone());

			match response.custom_profile_fields.get(&body.key_name) {
				| Some(value) => {
					profile_key_value.insert(body.key_name.clone(), value.clone());
					services.users.set_profile_key(
						&body.user_id,
						&body.key_name,
						Some(value.clone()),
					);
				},
				| _ => {
					return Err!(Request(NotFound("The requested profile key does not exist.")));
				},
			}

			if profile_key_value.is_empty() {
				return Err!(Request(NotFound("The requested profile key does not exist.")));
			}

			return Ok(get_profile_key::unstable::Response { value: profile_key_value });
		}
	}

	if !services.users.exists(&body.user_id).await {
		// Return 404 if this user doesn't exist and we couldn't fetch it over
		// federation
		return Err!(Request(NotFound("Profile was not found.")));
	}

	match services
		.users
		.profile_key(&body.user_id, &body.key_name)
		.await
	{
		| Ok(value) => {
			profile_key_value.insert(body.key_name.clone(), value);
		},
		| _ => {
			return Err!(Request(NotFound("The requested profile key does not exist.")));
		},
	}

	if profile_key_value.is_empty() {
		return Err!(Request(NotFound("The requested profile key does not exist.")));
	}

	Ok(get_profile_key::unstable::Response { value: profile_key_value })
}

use std::{
	sync::LazyLock,
	time::{Duration, Instant},
};

use tokio::sync::RwLock;

type DagCacheMap = std::collections::HashMap<OwnedRoomId, (Instant, Vec<serde_json::Value>)>;

static DAG_CACHE: LazyLock<RwLock<DagCacheMap>> =
	LazyLock::new(|| RwLock::new(DagCacheMap::new()));

/// # `GET /_matrix/client/unstable/org.continuwuity.dag/{roomId}`
///
/// Fetches the local DAG for the given room, returning raw JSON arrays.
/// Cached for 2 seconds to support hundreds of concurrent forensic viewers.
pub(crate) async fn get_room_dag_route(
	State(services): State<crate::State>,
	axum::extract::Path(room_id_str): axum::extract::Path<String>,
	auth: Option<
		axum_extra::TypedHeader<
			axum_extra::headers::Authorization<axum_extra::headers::authorization::Bearer>,
		>,
	>,
) -> Result<impl axum::response::IntoResponse> {
	use conduwuit::{Err, err};
	use futures::StreamExt;
	use ruma::OwnedRoomId;

	let room_id = OwnedRoomId::try_from(room_id_str)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;

	let is_public = services.rooms.state_accessor.get_join_rules(&room_id).await
		== ruma::events::room::join_rules::JoinRule::Public;

	if !is_public {
		// Extract token for private rooms
		let token = match auth {
			| Some(axum_extra::TypedHeader(axum_extra::headers::Authorization(bearer))) =>
				bearer.token().to_owned(),
			| None => {
				return Err!(Request(MissingToken("Missing access token for private room.")));
			},
		};

		// Validate user
		let (user_id, _) = services.users.find_from_token(&token).await.map_err(|_| {
			conduwuit::Error::Request(
				ruma::api::client::error::ErrorKind::UnknownToken { soft_logout: false },
				"Invalid access token.".into(),
				http::StatusCode::UNAUTHORIZED,
			)
		})?;

		// Require server admin
		if !services.users.is_admin(&user_id).await {
			return Err!(Request(Forbidden(
				"You must be a server admin to view this private room's DAG."
			)));
		}
	}

	if let Some((ts, cached_events)) = DAG_CACHE.read().await.get(&room_id) {
		if ts.elapsed() < Duration::from_secs(2) {
			return Ok(axum::Json(cached_events.clone()));
		}
	}

	let mut events = Vec::new();
	// Use pdus_rev to fetch from latest to oldest, avoiding full timeline scan.
	let pdus = services
		.rooms
		.timeline
		.pdus_rev(&room_id, std::ops::Bound::Unbounded);
	futures::pin_mut!(pdus);

	// Limit to the latest 200 events for performance
	let mut count: usize = 0;
	while let Some(Ok((_, pdu))) = pdus.next().await {
		if count >= 200 {
			break;
		}

		let mut obj: serde_json::Map<String, serde_json::Value> =
			serde_json::from_value(serde_json::to_value(&pdu)?)?;

		if let Ok(ssh) = services
			.rooms
			.state_accessor
			.pdu_shortstatehash(&pdu.event_id)
			.await
		{
			obj.insert("__shortstatehash".to_owned(), serde_json::Value::from(ssh));
		}

		// Add event_id in case PduEvent serialization omits it (V3+ rooms)
		obj.insert("event_id".to_owned(), serde_json::Value::String(pdu.event_id.to_string()));

		events.push(serde_json::Value::Object(obj));
		count = count.saturating_add(1);
	}

	// Reverse so they are chronologically ordered (oldest to newest)
	events.reverse();

	// Update cache
	DAG_CACHE
		.write()
		.await
		.insert(room_id.clone(), (Instant::now(), events.clone()));

	Ok(axum::Json(events))
}

/// # `POST /_matrix/client/unstable/event_relationships`
///
/// Walks the `m.relationship` DAG from an anchor event, fetching missing
/// events over federation as needed.
///
/// An implementation of [MSC2836](https://github.com/matrix-org/matrix-spec-proposals/pull/2836)
pub(crate) async fn get_event_relationships_route(
	State(services): State<crate::State>,
	body: Ruma<event_relationships::unstable::Request>,
) -> Result<event_relationships::unstable::Response> {
	let sender_user = body.sender_user();

	let params = Params::defaulted(msc2836::DefaultedParams {
		event_id: body.event_id.clone(),
		room_id: body.room_id.clone(),
		max_depth: body.max_depth,
		max_breadth: body.max_breadth,
		limit: body.limit,
		depth_first: body.depth_first,
		recent_first: body.recent_first,
		include_parent: body.include_parent,
		include_children: body.include_children,
		direction: body.direction.clone(),
	});

	let (events, limited) =
		Box::pin(msc2836::resolve(&services, Requester::Client(sender_user), params)).await?;

	let mut raw_events = Vec::with_capacity(events.len());
	for pdu in &events {
		raw_events.push(msc2836::to_raw_json_with_children(&services, pdu).await);
	}

	Ok(event_relationships::unstable::Response {
		events: raw_events,
		next_batch: None,
		limited,
	})
}

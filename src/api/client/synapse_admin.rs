//! The few Synapse admin endpoints Complement relies on: shared-secret
//! registration and room purging.

use std::{
	collections::{BTreeMap, HashMap},
	sync::LazyLock,
	time::{Duration, Instant},
};

use conduwuit::{Err, err, pdu::PduBuilder, utils};
use futures::{FutureExt, StreamExt};
use hmac::{Hmac, KeyInit, Mac};
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use sha1::Sha1;
use slipstream::{
	OwnedDeviceId, OwnedRoomId, OwnedUserId, RoomId, RoomVersionId, UserId,
	api::{EndpointRequest, client::config::set_room_account_data},
	codec,
	events::room::{
		create::RoomCreateEventContent,
		history_visibility::{HistoryVisibility, RoomHistoryVisibilityEventContent},
		join_rules::{JoinRule, RoomJoinRulesEventContent},
		member::{MembershipState, RoomMemberEventContent},
		message::RoomMessageEventContent,
		name::RoomNameEventContent,
		power_levels::RoomPowerLevelsEventContent,
	},
	json::Value,
};

use crate::{
	client::{invite_helper, leave_room},
	json_util::json_response,
	router::{
		ApiError, authenticate_user, authenticate_user_with_body,
		extract::{Path, State},
	},
};

type HmacSha1 = Hmac<Sha1>;

const REGISTRATION_NONCE_TTL: Duration = Duration::from_mins(15);
const MAX_REGISTRATION_NONCES: usize = 10_000;

static NONCES: LazyLock<std::sync::Mutex<HashMap<String, Instant>>> =
	LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

fn issue_registration_nonce(nonce: String) {
	let now = Instant::now();
	let mut nonces = NONCES.lock().expect("locked");
	nonces.retain(|_, expires_at| *expires_at > now);

	if nonces.len() >= MAX_REGISTRATION_NONCES {
		if let Some(oldest) = nonces
			.iter()
			.min_by_key(|(_, expires_at)| **expires_at)
			.map(|(nonce, _)| nonce.clone())
		{
			nonces.remove(&oldest);
		}
	}

	nonces.insert(nonce, now.checked_add(REGISTRATION_NONCE_TTL).unwrap_or(now));
}

fn consume_registration_nonce(nonce: &str) -> bool {
	NONCES
		.lock()
		.expect("locked")
		.remove(nonce)
		.is_some_and(|expires_at| Instant::now() < expires_at)
}

/// # `GET /_synapse/admin/v1/register`
///
/// Hands out a single-use nonce for [`synapse_register`].
pub(crate) async fn synapse_register_nonce(
	State(services): State<crate::State>,
) -> Result<crate::router::response::Response, ApiError> {
	if services.server.config.registration_shared_secret.is_none() {
		return Err!(Request(Unrecognized("Shared secret registration is disabled.")))
			.map_err(Into::into);
	}

	let nonce = utils::random_string(32);
	issue_registration_nonce(nonce.clone());

	let mut object = slipstream::ObjectBuilder::new();
	object.field("nonce", &nonce);
	Ok(json_response(object.finish()))
}

/// # `POST /_synapse/admin/v1/register`
///
/// Registers a user, checking an HMAC-SHA1 over
/// `nonce \0 username \0 password \0 admin|notadmin` made with the shared
/// secret.
pub(crate) async fn synapse_register(
	State(services): State<crate::State>,
	request: hyper::Request<Incoming>,
) -> Result<crate::router::response::Response, ApiError> {
	let Some(secret) = services.server.config.registration_shared_secret.clone() else {
		return Err!(Request(Unrecognized("Shared secret registration is disabled.")))
			.map_err(Into::into);
	};

	let body = Limited::new(request.into_body(), services.server.config.max_request_size)
		.collect()
		.await
		.map_err(|e| err!(Request(TooLarge("Request body too large: {e}"))))?
		.to_bytes();
	let body = std::str::from_utf8(&body)
		.map_err(|e| err!(Request(BadJson("Body is not UTF-8: {e}"))))?;
	let json: Value =
		codec::from_str(body).map_err(|e| err!(Request(BadJson("Invalid JSON body: {e}"))))?;
	let field = |name: &str| {
		json.get(name)
			.and_then(|v| v.as_str())
			.map(ToOwned::to_owned)
			.ok_or_else(|| err!(Request(BadJson("Missing or invalid field `{name}`"))))
	};
	let nonce = field("nonce")?;
	let username = field("username")?;
	let password = field("password")?;
	let mac_hex = field("mac")?;

	if !consume_registration_nonce(&nonce) {
		return Err!(Request(Forbidden("Unrecognised nonce"))).map_err(Into::into);
	}

	let mac_bytes = (0..mac_hex.len())
		.step_by(2)
		.map(|i| {
			mac_hex
				.get(i..i.saturating_add(2))
				.and_then(|b| u8::from_str_radix(b, 16).ok())
		})
		.collect::<Option<Vec<u8>>>()
		.ok_or_else(|| err!(Request(Forbidden("Invalid HMAC"))))?;
	let verify = |admin: &str| {
		let mut mac = HmacSha1::new_from_slice(secret.as_bytes()).expect("HMAC takes any key");
		for part in [nonce.as_str(), username.as_str(), password.as_str()] {
			mac.update(part.as_bytes());
			mac.update(b"\0");
		}
		mac.update(admin.as_bytes());
		mac.verify_slice(&mac_bytes).is_ok()
	};
	let admin = if verify("admin") {
		true
	} else if verify("notadmin") {
		false
	} else {
		return Err!(Request(Forbidden("HMAC incorrect"))).map_err(Into::into);
	};

	let username = username.to_lowercase();
	if username.is_empty()
		|| !username
			.chars()
			.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._=-/+".contains(c))
	{
		return Err!(Request(InvalidUsername(
			"User ID can only contain characters a-z, 0-9, or '=_-./+'"
		)))
		.map_err(Into::into);
	}

	let user_id = OwnedUserId::parse(format!("@{username}:{}", services.globals.server_name()))
		.map_err(|_| err!(Request(InvalidUsername("Invalid username"))))?;
	if services.users.exists(&user_id).await {
		return Err!(Request(UserInUse("User ID already taken."))).map_err(Into::into);
	}

	services
		.users
		.create(&user_id, Some(password.as_str()), None)
		.await?;
	services
		.users
		.set_displayname(&user_id, Some(user_id.localpart().to_owned()))
		.await;

	let device_id = OwnedDeviceId::parse(utils::random_string(10))
		.map_err(|e| err!(Request(Unknown("Bad device id: {e}"))))?;
	let token = utils::random_string(32);
	services
		.users
		.create_device_with_ttl(&user_id, &device_id, &token, None, None, None)
		.await?;

	if admin {
		services.admin.make_user_admin(&user_id).boxed().await.ok();
	}

	let mut object = slipstream::ObjectBuilder::new();
	object.field("user_id", &user_id.to_string());
	object.field("access_token", &token);
	object.field("device_id", &device_id.to_string());
	object.field("home_server", &services.globals.server_name().to_string());
	Ok(json_response(object.finish()))
}

/// # `DELETE /_synapse/admin/v1/rooms/{room_id}`
///
/// Purges a room: every local user leaves it, it is hidden from the directory
/// and no longer served to clients.
pub(crate) async fn synapse_delete_room(
	State(services): State<crate::State>,
	Path(room_id): Path<String>,
	request: hyper::Request<Incoming>,
) -> Result<crate::router::response::Response, ApiError> {
	let room_id = OwnedRoomId::parse(room_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;
	let sender_user =
		authenticate_user(request, &services, &set_room_account_data::v3::Request::METADATA)
			.await?;
	if !services.users.is_admin(&sender_user).await {
		return Err!(Request(Forbidden("Only server administrators can use this endpoint")))
			.map_err(Into::into);
	}

	let users: Vec<OwnedUserId> = services
		.rooms
		.state_cache
		.local_users_in_room(&room_id)
		.collect()
		.await;
	let mut kicked = Vec::new();
	let mut failed = Vec::new();
	for user_id in users {
		if leave_room(&services, &user_id, &room_id, None)
			.boxed()
			.await
			.is_ok()
		{
			services.rooms.state_cache.forget(&room_id, &user_id);
			kicked.push(user_id.to_string());
		} else {
			failed.push(user_id.to_string());
		}
	}
	services.rooms.directory.set_not_public(&room_id);

	let mut object = slipstream::ObjectBuilder::new();
	object.field("kicked_users", &kicked);
	object.field("failed_to_kick_users", &failed);
	object.field("local_aliases", &Vec::<String>::new());
	object.field("new_room_id", &Value::Null);
	Ok(json_response(object.finish()))
}

/// Server notice rooms by recipient. Not persisted: a restart starts a new room.
static NOTICE_ROOMS: LazyLock<std::sync::Mutex<HashMap<OwnedUserId, OwnedRoomId>>> =
	LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

/// Responses to `PUT .../send_server_notice/{txn_id}`, by admin and txn id.
static NOTICE_TXNS: LazyLock<std::sync::Mutex<HashMap<(OwnedUserId, String), String>>> =
	LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

/// Serializes `send_server_notice`, so that retries of one transaction cannot
/// both miss `NOTICE_TXNS` and send the notice twice.
static NOTICE_SEND: LazyLock<tokio::sync::Mutex<()>> =
	LazyLock::new(|| tokio::sync::Mutex::new(()));

/// Whether `room_id` is the server notice room of `user_id`.
pub(crate) fn is_server_notice_room(user_id: &UserId, room_id: &RoomId) -> bool {
	NOTICE_ROOMS
		.lock()
		.expect("locked")
		.get(user_id)
		.is_some_and(|room| room == room_id)
}

async fn create_notice_room(
	services: &conduwuit_service::Services,
	notice_user: &UserId,
) -> conduwuit::Result<OwnedRoomId> {
	let room_id = OwnedRoomId::new_v1(services.globals.server_name());
	services
		.rooms
		.short
		.get_or_create_shortroomid(&room_id)
		.await;
	let state_lock = services.rooms.state.mutex.lock(&room_id).await;

	let power_users = BTreeMap::from_iter([(notice_user.to_owned(), 100.into())]);
	let timeline = &services.rooms.timeline;
	timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &RoomCreateEventContent {
				federate: false,
				predecessor: None,
				room_version: RoomVersionId::V11,
				..RoomCreateEventContent::new_v11()
			}),
			notice_user,
			Some(&room_id),
			&state_lock,
		)
		.boxed()
		.await?;
	timeline
		.build_and_append_pdu(
			PduBuilder::state(
				notice_user.to_string(),
				&RoomMemberEventContent::new(MembershipState::Join),
			),
			notice_user,
			Some(&room_id),
			&state_lock,
		)
		.boxed()
		.await?;
	timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &RoomPowerLevelsEventContent {
				users: power_users,
				..Default::default()
			}),
			notice_user,
			Some(&room_id),
			&state_lock,
		)
		.boxed()
		.await?;
	timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &RoomJoinRulesEventContent::new(JoinRule::Invite)),
			notice_user,
			Some(&room_id),
			&state_lock,
		)
		.boxed()
		.await?;
	timeline
		.build_and_append_pdu(
			PduBuilder::state(
				String::new(),
				&RoomHistoryVisibilityEventContent::new(HistoryVisibility::Shared),
			),
			notice_user,
			Some(&room_id),
			&state_lock,
		)
		.boxed()
		.await?;
	timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &RoomNameEventContent::new("Server Notices")),
			notice_user,
			Some(&room_id),
			&state_lock,
		)
		.boxed()
		.await?;
	drop(state_lock);

	Ok(room_id)
}

/// # `POST /_synapse/admin/v1/send_server_notice`
pub(crate) async fn synapse_send_server_notice(
	State(services): State<crate::State>,
	request: hyper::Request<Incoming>,
) -> Result<crate::router::response::Response, ApiError> {
	send_server_notice(&services, request, None).await
}

/// # `PUT /_synapse/admin/v1/send_server_notice/{txn_id}`
pub(crate) async fn synapse_send_server_notice_txn(
	State(services): State<crate::State>,
	Path(txn_id): Path<String>,
	request: hyper::Request<Incoming>,
) -> Result<crate::router::response::Response, ApiError> {
	send_server_notice(&services, request, Some(txn_id)).await
}

async fn send_server_notice(
	services: &crate::State,
	request: hyper::Request<Incoming>,
	txn_id: Option<String>,
) -> Result<crate::router::response::Response, ApiError> {
	let (admin, body) = authenticate_user_with_body(
		request,
		services,
		&set_room_account_data::v3::Request::METADATA,
	)
	.await?;
	if !services.users.is_admin(&admin).await {
		return Err!(Request(Forbidden("Only server administrators can use this endpoint")))
			.map_err(Into::into);
	}

	let _serialized = NOTICE_SEND.lock().await;

	let reply = |event_id: &str| {
		let mut object = slipstream::ObjectBuilder::new();
		object.field("event_id", event_id);
		json_response(object.finish())
	};
	if let Some(txn_id) = &txn_id {
		let key = (admin.clone(), txn_id.clone());
		if let Some(event_id) = NOTICE_TXNS.lock().expect("locked").get(&key) {
			return Ok(reply(event_id));
		}
	}

	let body = std::str::from_utf8(&body)
		.map_err(|e| err!(Request(BadJson("Body is not UTF-8: {e}"))))?;
	let json: Value =
		codec::from_str(body).map_err(|e| err!(Request(BadJson("Invalid JSON body: {e}"))))?;
	let target = json
		.get("user_id")
		.and_then(|v| v.as_str())
		.and_then(|u| OwnedUserId::parse(u).ok())
		.ok_or_else(|| err!(Request(InvalidParam("Missing or invalid `user_id`"))))?;
	let text = json
		.get("content")
		.and_then(|c| c.get("body"))
		.and_then(|b| b.as_str())
		.ok_or_else(|| err!(Request(InvalidParam("Missing `content.body`"))))?
		.to_owned();
	if !services.globals.user_is_local(&target) || !services.users.exists(&target).await {
		return Err!(Request(InvalidParam("Server notices can only be sent to local users")))
			.map_err(Into::into);
	}

	let notice_user = OwnedUserId::parse(format!("@_server:{}", services.globals.server_name()))
		.map_err(|e| err!(Request(Unknown("Bad notice user: {e}"))))?;
	if !services.users.exists(&notice_user).await {
		services.users.create(&notice_user, None, None).await?;
	}

	let existing = NOTICE_ROOMS.lock().expect("locked").get(&target).cloned();
	let room_id = match existing {
		| Some(room_id) => room_id,
		| None => {
			let room_id = create_notice_room(services, &notice_user).await?;
			NOTICE_ROOMS
				.lock()
				.expect("locked")
				.insert(target.clone(), room_id.clone());
			room_id
		},
	};
	if !services
		.rooms
		.state_cache
		.is_invited_or_joined(&target, &room_id)
		.await
	{
		invite_helper(services, &notice_user, &target, &room_id, None, false).await?;
	}

	let state_lock = services.rooms.state.mutex.lock(&room_id).await;
	let event_id = services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain(text)),
			&notice_user,
			Some(&room_id),
			&state_lock,
		)
		.boxed()
		.await?;
	drop(state_lock);

	if let Some(txn_id) = txn_id {
		NOTICE_TXNS
			.lock()
			.expect("locked")
			.insert((admin, txn_id), event_id.to_string());
	}
	Ok(reply(event_id.as_str()))
}

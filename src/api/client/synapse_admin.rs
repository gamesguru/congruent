//! The few Synapse admin endpoints Complement relies on: shared-secret
//! registration and room purging.

use std::{collections::HashSet, sync::LazyLock};

use conduwuit::{Err, err, utils};
use futures::{FutureExt, StreamExt};
use hmac::{Hmac, KeyInit, Mac};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use sha1::Sha1;
use slipstream::{
	OwnedDeviceId, OwnedRoomId, OwnedUserId,
	api::{EndpointRequest, client::config::set_room_account_data},
	codec,
	json::Value,
};

use crate::{
	client::leave_room,
	json_util::json_response,
	router::{
		ApiError, authenticate_user,
		extract::{Path, State},
	},
};

type HmacSha1 = Hmac<Sha1>;

static NONCES: LazyLock<std::sync::Mutex<HashSet<String>>> =
	LazyLock::new(|| std::sync::Mutex::new(HashSet::new()));

/// # `GET /_synapse/admin/v1/register`
///
/// Hands out a single-use nonce for [`synapse_register`].
pub(crate) async fn synapse_register_nonce(
	State(services): State<crate::State>,
) -> std::result::Result<crate::router::response::Response, ApiError> {
	if services.server.config.registration_shared_secret.is_none() {
		return Err!(Request(Unrecognized("Shared secret registration is disabled.")))
			.map_err(Into::into);
	}

	let nonce = utils::random_string(32);
	NONCES.lock().expect("locked").insert(nonce.clone());

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
) -> std::result::Result<crate::router::response::Response, ApiError> {
	let Some(secret) = services.server.config.registration_shared_secret.clone() else {
		return Err!(Request(Unrecognized("Shared secret registration is disabled.")))
			.map_err(Into::into);
	};

	let body = request
		.into_body()
		.collect()
		.await
		.map_err(|e| err!(Request(BadJson("Failed to read body: {e}"))))?
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

	if !NONCES.lock().expect("locked").remove(&nonce) {
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
		return Err!(Request(InvalidUsername("User ID can only contain characters a-z, 0-9, or '=_-./+'")))
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
) -> std::result::Result<crate::router::response::Response, ApiError> {
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
	for user_id in users {
		if leave_room(&services, &user_id, &room_id, None)
			.boxed()
			.await
			.is_ok()
		{
			services.rooms.state_cache.forget(&room_id, &user_id);
			kicked.push(user_id.to_string());
		}
	}
	services.rooms.directory.set_not_public(&room_id);

	let mut object = slipstream::ObjectBuilder::new();
	object.field("kicked_users", &kicked);
	object.field("failed_to_kick_users", &Vec::<String>::new());
	object.field("local_aliases", &Vec::<String>::new());
	object.field("new_room_id", &Value::Null);
	Ok(json_response(object.finish()))
}

#![allow(dead_code)]

use conduwuit_core::{Err, Result, err};
use service::Services;
use slipstream::{OwnedRoomId, OwnedUserId, RoomId, UserId};

pub(crate) fn escape_html(s: &str) -> String {
	s.replace('&', "&amp;")
		.replace('<', "&lt;")
		.replace('>', "&gt;")
}

pub(crate) async fn get_room_info(
	services: &Services,
	room_id: &RoomId,
) -> (OwnedRoomId, u64, String) {
	(
		room_id.into(),
		services
			.rooms
			.state_cache
			.room_joined_count(room_id)
			.await
			.unwrap_or(0),
		services
			.rooms
			.state_accessor
			.get_name(room_id)
			.await
			.unwrap_or_else(|_| room_id.to_string()),
	)
}

/// Parses user ID
pub(crate) fn parse_user_id(services: &Services, user_id: &str) -> Result<OwnedUserId> {
	UserId::parse_with_server_name(user_id.to_lowercase(), services.globals.server_name())
		.map_err(|e| err!("The supplied username is not a valid username: {e}"))
}

/// Parses user ID as our local user
pub(crate) fn parse_local_user_id(services: &Services, user_id: &str) -> Result<OwnedUserId> {
	let user_id = parse_user_id(services, user_id)?;

	if !services.globals.user_is_local(&user_id) {
		return Err!("User {user_id:?} does not belong to our server.");
	}

	Ok(user_id)
}

/// Parses user ID that is an active (not guest or deactivated) local user
pub(crate) async fn parse_active_local_user_id(
	services: &Services,
	user_id: &str,
) -> Result<OwnedUserId> {
	let user_id = parse_local_user_id(services, user_id)?;

	if !services.users.exists(&user_id).await {
		return Err!("User {user_id:?} does not exist on this server.");
	}

	if services.users.is_deactivated(&user_id).await? {
		return Err!("User {user_id:?} is deactivated.");
	}

	Ok(user_id)
}

/// Pretty-printed JSON through Slipstream's codec.
pub(crate) fn to_string_pretty<T>(value: &T) -> Result<String, slipstream::codec::DeError>
where
	T: slipstream::codec::Serialize + ?Sized,
{
	Ok(rezzy::json::write_string_pretty(&slipstream::codec::to_value(value)).unwrap_or_default())
}

/// Splits an `mxc://server/media_id` URI into owned parts.
pub(crate) fn split_mxc(uri: &str) -> Result<(slipstream::OwnedServerName, String)> {
	let parts = uri
		.strip_prefix("mxc://")
		.and_then(|rest| rest.split_once('/'))
		.filter(|(server, media_id)| !server.is_empty() && !media_id.is_empty());
	match parts {
		| Some((server, media_id)) => Ok((server.into(), media_id.to_owned())),
		| None => conduwuit::Err!("Invalid MXC URI {uri}."),
	}
}

/// Event content as a Slipstream JSON value.
pub(crate) fn content_value<E: conduwuit::matrix::Event>(event: &E) -> slipstream::json::Value {
	slipstream::codec::from_str(event.content().get()).unwrap_or(slipstream::json::Value::Null)
}

use axum::{
	body::Body,
	extract::{Path, State},
};
use conduwuit::{Err, Result, err};
use conduwuit_service::Services;
use slipstream::{
	OwnedRoomId, OwnedUserId, RoomId, UserId,
	api::{
		EndpointRequest,
		client::config::{
			get_global_account_data, get_room_account_data, set_global_account_data,
			set_room_account_data,
		},
	},
	events::RoomAccountDataEventType,
	json::Value as JsonValue,
	sswire::Raw,
};

use crate::{
	Ruma,
	router::{ApiError, authenticate_user},
};

/// # `PUT /_matrix/client/r0/user/{userId}/account_data/{type}`
///
/// Sets some account data for the sender user.
pub(crate) async fn set_global_account_data_route(
	State(services): State<crate::State>,
	body: Ruma<set_global_account_data::v3::Request>,
) -> Result<set_global_account_data::v3::Response> {
	let sender_user = body.sender_user();

	if sender_user != &*body.user_id && body.appservice_info.is_none() {
		return Err!(Request(Forbidden("You cannot set account data for other users.")));
	}

	set_account_data(&services, None, &body.user_id, body.event_type.as_ref(), &body.data)
		.await?;

	Ok(set_global_account_data::v3::Response {})
}

/// # `PUT /_matrix/client/r0/user/{userId}/rooms/{roomId}/account_data/{type}`
///
/// Sets some room account data for the sender user.
pub(crate) async fn set_room_account_data_route(
	State(services): State<crate::State>,
	body: Ruma<set_room_account_data::v3::Request>,
) -> Result<set_room_account_data::v3::Response> {
	let sender_user = body.sender_user();

	if sender_user != &*body.user_id && body.appservice_info.is_none() {
		return Err!(Request(Forbidden("You cannot set account data for other users.")));
	}

	set_account_data(
		&services,
		Some(&body.room_id),
		&body.user_id,
		body.event_type.as_ref(),
		&body.data,
	)
	.await?;

	Ok(set_room_account_data::v3::Response {})
}

/// # `GET /_matrix/client/r0/user/{userId}/account_data/{type}`
///
/// Gets some account data for the sender user.
pub(crate) async fn get_global_account_data_route(
	State(services): State<crate::State>,
	body: Ruma<get_global_account_data::v3::Request>,
) -> Result<get_global_account_data::v3::Response> {
	let sender_user = body.sender_user();

	if sender_user != &*body.user_id && body.appservice_info.is_none() {
		return Err!(Request(Forbidden("You cannot get account data of other users.")));
	}

	let account_data: JsonValue = services
		.account_data
		.get_global::<JsonValue>(&body.user_id, body.event_type.clone())
		.await
		.map_err(|_| err!(Request(NotFound("Data not found."))))?;

	// The stored value is the whole event; the endpoint returns only its content.
	let content = account_data
		.get("content")
		.ok_or_else(|| err!(Request(NotFound("Data not found."))))?;

	Ok(get_global_account_data::v3::Response { account_data: Raw::from_value(content) })
}

/// # `GET /_matrix/client/r0/user/{userId}/rooms/{roomId}/account_data/{type}`
///
/// Gets some room account data for the sender user.
pub(crate) async fn get_room_account_data_route(
	State(services): State<crate::State>,
	body: Ruma<get_room_account_data::v3::Request>,
) -> Result<get_room_account_data::v3::Response> {
	let sender_user = body.sender_user();

	if sender_user != &*body.user_id && body.appservice_info.is_none() {
		return Err!(Request(Forbidden("You cannot get account data of other users.")));
	}

	let account_data: JsonValue = services
		.account_data
		.get_room::<JsonValue>(&body.room_id, &body.user_id, body.event_type.clone())
		.await
		.map_err(|_| err!(Request(NotFound("Data not found."))))?;

	// The stored value is the whole event; the endpoint returns only its content.
	let content = account_data
		.get("content")
		.ok_or_else(|| err!(Request(NotFound("Data not found."))))?;

	Ok(get_room_account_data::v3::Response { account_data: Raw::from_value(content) })
}

/// # `DELETE /_matrix/client/unstable/org.matrix.msc3391/user/{userId}/account_data/{type}`
///
/// Removes some account data for the sender user.
pub(crate) async fn delete_global_account_data_msc3391_route(
	State(services): State<crate::State>,
	Path((user_id, event_type)): Path<(String, String)>,
	request: hyper::Request<Body>,
) -> std::result::Result<axum::response::Response, ApiError> {
	let user_id = OwnedUserId::parse(user_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid user ID."))))?;
	let sender_user =
		authenticate_user(request, &services, &set_global_account_data::v3::Request::METADATA)
			.await?;

	if sender_user != *user_id {
		return Err!(Request(Forbidden("You cannot delete account data for other users.")))
			.map_err(Into::into);
	}

	delete_account_data(&services, None, &user_id, &event_type).await?;

	Ok(crate::json_util::json_response(slipstream::json::Value::Object(
		slipstream::json::Object::new(),
	)))
}

/// # `DELETE /_matrix/client/unstable/org.matrix.msc3391/user/{userId}/rooms/{roomId}/account_data/{type}`
///
/// Removes some room account data for the sender user.
pub(crate) async fn delete_room_account_data_msc3391_route(
	State(services): State<crate::State>,
	Path((user_id, room_id, event_type)): Path<(String, String, String)>,
	request: hyper::Request<Body>,
) -> std::result::Result<axum::response::Response, ApiError> {
	let user_id = OwnedUserId::parse(user_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid user ID."))))?;
	let room_id = OwnedRoomId::parse(room_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;
	let sender_user =
		authenticate_user(request, &services, &set_room_account_data::v3::Request::METADATA)
			.await?;

	if sender_user != *user_id {
		return Err!(Request(Forbidden("You cannot delete account data for other users.")))
			.map_err(Into::into);
	}

	delete_account_data(&services, Some(&room_id), &user_id, &event_type).await?;

	Ok(crate::json_util::json_response(slipstream::json::Value::Object(
		slipstream::json::Object::new(),
	)))
}

async fn set_account_data(
	services: &Services,
	room_id: Option<&RoomId>,
	sender_user: &UserId,
	event_type_s: &str,
	data: &slipstream::json::Value,
) -> Result {
	if event_type_s == RoomAccountDataEventType::FullyRead.to_cow_str() {
		return Err!(Request(BadJson(
			"This endpoint cannot be used for marking a room as fully read (setting \
			 m.fully_read)"
		)));
	}

	let data = data.clone();

	if data
		.as_object()
		.is_some_and(slipstream::json::Object::is_empty)
	{
		return delete_account_data(services, room_id, sender_user, event_type_s).await;
	}

	let mut event = slipstream::ObjectBuilder::new();
	event.field("type", &event_type_s);
	event.field("content", &data);

	services
		.account_data
		.update(room_id, sender_user, event_type_s.into(), &event.finish())
		.await
}

async fn delete_account_data(
	services: &Services,
	room_id: Option<&RoomId>,
	sender_user: &UserId,
	event_type_s: &str,
) -> Result {
	if event_type_s == RoomAccountDataEventType::FullyRead.to_cow_str() {
		return Err!(Request(BadJson(
			"This endpoint cannot be used for marking a room as fully read (setting \
			 m.fully_read)"
		)));
	}

	services
		.account_data
		.delete(room_id, sender_user, event_type_s)
		.await
}

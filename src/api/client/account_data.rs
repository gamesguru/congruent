use axum::{
	Json,
	body::Body,
	extract::{Path, State},
};
use conduwuit::{Err, Result, err};
use conduwuit_service::Services;
use slipstream::{
	OwnedRoomId, OwnedUserId, RoomId, UserId,
	api::{
		EndpointRequest, IncomingRequest,
		client::config::{
			get_global_account_data, get_room_account_data, set_global_account_data,
			set_room_account_data,
		},
	},
	codec::{DeError, Deserialize as CodecDeserialize, from_str},
	endpoint::body_field,
	events::{
		AnyGlobalAccountDataEventContent, AnyRoomAccountDataEventContent,
		RoomAccountDataEventType,
	},
	json::Value as JsonValue,
	serde::Raw,
};

use crate::{Ruma, router::authenticate_user};

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

	set_account_data(&services, None, &body.user_id, &body.event_type.to_string(), &body.data)
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
		&body.event_type.to_string(),
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

	let account_data: Extract<AnyGlobalAccountDataEventContent> = services
		.account_data
		.get_global(&body.user_id, body.event_type.clone())
		.await
		.map_err(|_| err!(Request(NotFound("Data not found."))))?;

	Ok(get_global_account_data::v3::Response { account_data: account_data.content })
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

	let account_data: Extract<AnyRoomAccountDataEventContent> = services
		.account_data
		.get_room(&body.room_id, &body.user_id, body.event_type.clone())
		.await
		.map_err(|_| err!(Request(NotFound("Data not found."))))?;

	Ok(get_room_account_data::v3::Response { account_data: account_data.content })
}

/// # `DELETE /_matrix/client/unstable/org.matrix.msc3391/user/{userId}/account_data/{type}`
///
/// Removes some account data for the sender user.
pub(crate) async fn delete_global_account_data_msc3391_route(
	State(services): State<crate::State>,
	Path((user_id, event_type)): Path<(OwnedUserId, String)>,
	request: hyper::Request<Body>,
) -> Result<Json<slipstream::json::Value>> {
	let sender_user =
		authenticate_user(request, &services, &set_global_account_data::v3::Request::METADATA)
			.await?;

	if sender_user != &*user_id {
		return Err!(Request(Forbidden("You cannot delete account data for other users.")));
	}

	delete_account_data(&services, None, &user_id, &event_type).await?;

	Ok(Json(slipstream::json::Value::Object(slipstream::json::Object::new())))
}

/// # `DELETE /_matrix/client/unstable/org.matrix.msc3391/user/{userId}/rooms/{roomId}/account_data/{type}`
///
/// Removes some room account data for the sender user.
pub(crate) async fn delete_room_account_data_msc3391_route(
	State(services): State<crate::State>,
	Path((user_id, room_id, event_type)): Path<(OwnedUserId, OwnedRoomId, String)>,
	request: hyper::Request<Body>,
) -> Result<Json<slipstream::json::Value>> {
	let sender_user =
		authenticate_user(request, &services, &set_room_account_data::v3::Request::METADATA)
			.await?;

	if sender_user != &*user_id {
		return Err!(Request(Forbidden("You cannot delete account data for other users.")));
	}

	delete_account_data(&services, Some(&room_id), &user_id, &event_type).await?;

	Ok(Json(slipstream::json::Value::Object(slipstream::json::Object::new())))
}

async fn set_account_data(
	services: &Services,
	room_id: Option<&RoomId>,
	sender_user: &UserId,
	event_type_s: &str,
	data: &Raw<slipstream::json::Value>,
) -> Result {
	if event_type_s == RoomAccountDataEventType::FullyRead.to_cow_str() {
		return Err!(Request(BadJson(
			"This endpoint cannot be used for marking a room as fully read (setting \
			 m.fully_read)"
		)));
	}

	let data: slipstream::json::Value = from_str(data.get())
		.map_err(|e| err!(Request(BadJson(warn!("Invalid JSON provided: {e}")))))?;

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

/// Wraps stored account data so the inner content can be handed back to the
/// API response untouched.
///
/// `Raw<T>` re-serializes as the raw text it was decoded from, so `content`
/// passes through byte-for-byte rather than being rebuilt from the typed event.
/// Decoded through the codec (not serde), because
/// [`conduwuit_service::account_data`] takes `T: codec::Deserialize`.
struct Extract<T> {
	content: Raw<T>,
}

impl<T: CodecDeserialize> CodecDeserialize for Extract<T> {
	fn from_json(value: &JsonValue) -> Result<Self, DeError> {
		Ok(Self {
			content: body_field(Some(value), "content")?,
		})
	}
}

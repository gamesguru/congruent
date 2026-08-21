use axum::extract::State;
use bytes::BufMut;
use conduwuit::{Err, Result, err};
use conduwuit_service::Services;
use ruma::{
	OwnedRoomId, OwnedUserId, RoomId, UserId,
	api::{
		AuthScheme, IncomingRequest, MatrixVersion, Metadata, OutgoingResponse, VersionHistory,
		client::config::{
			get_global_account_data, get_room_account_data, set_global_account_data,
			set_room_account_data,
		},
		error::{FromHttpRequestError, IntoHttpError, MatrixError},
	},
	events::{
		AnyGlobalAccountDataEventContent, AnyRoomAccountDataEventContent,
		RoomAccountDataEventType,
	},
	serde::Raw,
};
use serde::Deserialize;
use serde_json::{json, value::RawValue as RawJsonValue};

use crate::Ruma;

mod delete_global_account_data {
	use super::{
		AuthScheme, BufMut, Deserialize, FromHttpRequestError, IncomingRequest, IntoHttpError,
		MatrixError, MatrixVersion, Metadata, OutgoingResponse, OwnedUserId, VersionHistory,
	};

	#[derive(Debug)]
	pub(crate) struct Request {
		pub user_id: OwnedUserId,
		pub event_type: String,
	}

	impl IncomingRequest for Request {
		type EndpointError = MatrixError;
		type OutgoingResponse = Response;

		const METADATA: Metadata = Metadata {
			method: http::Method::DELETE,
			rate_limited: true,
			authentication: AuthScheme::AccessToken,
			history: VersionHistory::new(
				&[],
				&[
					(
						MatrixVersion::V1_0,
						"/_matrix/client/r0/user/{user_id}/account_data/{event_type}",
					),
					(
						MatrixVersion::V1_1,
						"/_matrix/client/v3/user/{user_id}/account_data/{event_type}",
					),
				],
				None,
				None,
			),
		};

		fn try_from_http_request<B, S>(
			request: http::Request<B>,
			path_args: &[S],
		) -> Result<Self, FromHttpRequestError>
		where
			B: AsRef<[u8]>,
			S: AsRef<str>,
		{
			if request.method() != Self::METADATA.method {
				return Err(FromHttpRequestError::MethodMismatch {
					expected: Self::METADATA.method,
					received: request.method().clone(),
				});
			}

			let (user_id, event_type) =
				Deserialize::deserialize(serde::de::value::SeqDeserializer::<
					_,
					serde::de::value::Error,
				>::new(path_args.iter().map(AsRef::as_ref)))?;

			Ok(Self { user_id, event_type })
		}
	}

	#[derive(Clone, Copy, Debug)]
	pub(crate) struct Response;

	impl OutgoingResponse for Response {
		fn try_into_http_response<T: Default + BufMut>(
			self,
		) -> Result<http::Response<T>, IntoHttpError> {
			http::Response::builder()
				.header(
					http::header::CONTENT_TYPE,
					http::header::HeaderValue::from_static("application/json"),
				)
				.body(ruma::serde::slice_to_buf(b"{}"))
				.map_err(IntoHttpError::from)
		}
	}
}

mod delete_room_account_data {
	use super::{
		AuthScheme, BufMut, Deserialize, FromHttpRequestError, IncomingRequest, IntoHttpError,
		MatrixError, MatrixVersion, Metadata, OutgoingResponse, OwnedRoomId, OwnedUserId,
		VersionHistory,
	};

	#[derive(Debug)]
	pub(crate) struct Request {
		pub user_id: OwnedUserId,
		pub room_id: OwnedRoomId,
		pub event_type: String,
	}

	impl IncomingRequest for Request {
		type EndpointError = MatrixError;
		type OutgoingResponse = Response;

		const METADATA: Metadata = Metadata {
			method: http::Method::DELETE,
			rate_limited: true,
			authentication: AuthScheme::AccessToken,
			history: VersionHistory::new(
				&[],
				&[
					(
						MatrixVersion::V1_0,
						"/_matrix/client/r0/user/{user_id}/rooms/{room_id}/account_data/\
						 {event_type}",
					),
					(
						MatrixVersion::V1_1,
						"/_matrix/client/v3/user/{user_id}/rooms/{room_id}/account_data/\
						 {event_type}",
					),
				],
				None,
				None,
			),
		};

		fn try_from_http_request<B, S>(
			request: http::Request<B>,
			path_args: &[S],
		) -> Result<Self, FromHttpRequestError>
		where
			B: AsRef<[u8]>,
			S: AsRef<str>,
		{
			if request.method() != Self::METADATA.method {
				return Err(FromHttpRequestError::MethodMismatch {
					expected: Self::METADATA.method,
					received: request.method().clone(),
				});
			}

			let (user_id, room_id, event_type) =
				Deserialize::deserialize(serde::de::value::SeqDeserializer::<
					_,
					serde::de::value::Error,
				>::new(path_args.iter().map(AsRef::as_ref)))?;

			Ok(Self { user_id, room_id, event_type })
		}
	}

	#[derive(Clone, Copy, Debug)]
	pub(crate) struct Response;

	impl OutgoingResponse for Response {
		fn try_into_http_response<T: Default + BufMut>(
			self,
		) -> Result<http::Response<T>, IntoHttpError> {
			http::Response::builder()
				.header(
					http::header::CONTENT_TYPE,
					http::header::HeaderValue::from_static("application/json"),
				)
				.body(ruma::serde::slice_to_buf(b"{}"))
				.map_err(IntoHttpError::from)
		}
	}
}

/// # `PUT /_matrix/client/r0/user/{userId}/account_data/{type}`
///
/// Sets some account data for the sender user.
pub(crate) async fn set_global_account_data_route(
	State(services): State<crate::State>,
	body: Ruma<set_global_account_data::v3::Request>,
) -> Result<set_global_account_data::v3::Response> {
	let sender_user = body.sender_user();

	if sender_user != body.user_id && body.appservice_info.is_none() {
		return Err!(Request(Forbidden("You cannot set account data for other users.")));
	}

	set_account_data(
		&services,
		None,
		&body.user_id,
		&body.event_type.to_string(),
		body.data.json(),
	)
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

	if sender_user != body.user_id && body.appservice_info.is_none() {
		return Err!(Request(Forbidden("You cannot set account data for other users.")));
	}

	set_account_data(
		&services,
		Some(&body.room_id),
		&body.user_id,
		&body.event_type.to_string(),
		body.data.json(),
	)
	.await?;

	Ok(set_room_account_data::v3::Response {})
}

/// # `DELETE /_matrix/client/r0/user/{userId}/account_data/{type}`
///
/// Deletes some account data for the sender user.
pub(crate) async fn delete_global_account_data_route(
	State(services): State<crate::State>,
	body: Ruma<delete_global_account_data::Request>,
) -> Result<delete_global_account_data::Response> {
	let sender_user = body.sender_user();

	if sender_user != body.user_id && body.appservice_info.is_none() {
		return Err!(Request(Forbidden("You cannot delete account data of other users.")));
	}

	services
		.account_data
		.delete(None, &body.user_id, &body.event_type.clone())
		.await?;

	Ok(delete_global_account_data::Response {})
}

/// # `DELETE /_matrix/client/r0/user/{userId}/rooms/{roomId}/account_data/{type}`
///
/// Deletes some room account data for the sender user.
pub(crate) async fn delete_room_account_data_route(
	State(services): State<crate::State>,
	body: Ruma<delete_room_account_data::Request>,
) -> Result<delete_room_account_data::Response> {
	let sender_user = body.sender_user();

	if sender_user != body.user_id && body.appservice_info.is_none() {
		return Err!(Request(Forbidden("You cannot delete account data of other users.")));
	}

	services
		.account_data
		.delete(Some(&body.room_id), &body.user_id, &body.event_type.clone())
		.await?;

	Ok(delete_room_account_data::Response {})
}

/// # `GET /_matrix/client/r0/user/{userId}/account_data/{type}`
///
/// Gets some account data for the sender user.
pub(crate) async fn get_global_account_data_route(
	State(services): State<crate::State>,
	body: Ruma<get_global_account_data::v3::Request>,
) -> Result<get_global_account_data::v3::Response> {
	let sender_user = body.sender_user();

	if sender_user != body.user_id && body.appservice_info.is_none() {
		return Err!(Request(Forbidden("You cannot get account data of other users.")));
	}

	let account_data: ExtractGlobalEventContent = services
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

	if sender_user != body.user_id && body.appservice_info.is_none() {
		return Err!(Request(Forbidden("You cannot get account data of other users.")));
	}

	let account_data: ExtractRoomEventContent = services
		.account_data
		.get_room(&body.room_id, &body.user_id, body.event_type.clone())
		.await
		.map_err(|_| err!(Request(NotFound("Data not found."))))?;

	Ok(get_room_account_data::v3::Response { account_data: account_data.content })
}

async fn set_account_data(
	services: &Services,
	room_id: Option<&RoomId>,
	sender_user: &UserId,
	event_type_s: &str,
	data: &RawJsonValue,
) -> Result {
	if event_type_s == RoomAccountDataEventType::FullyRead.to_cow_str() {
		return Err!(Request(BadJson(
			"This endpoint cannot be used for marking a room as fully read (setting \
			 m.fully_read)"
		)));
	}

	let data: serde_json::Value = serde_json::from_str(data.get())
		.map_err(|e| err!(Request(BadJson(warn!("Invalid JSON provided: {e}")))))?;

	services
		.account_data
		.update(
			room_id,
			sender_user,
			event_type_s.into(),
			&json!({
				"type": event_type_s,
				"content": data,
			}),
		)
		.await
}

#[derive(Deserialize)]
struct ExtractRoomEventContent {
	content: Raw<AnyRoomAccountDataEventContent>,
}

#[derive(Deserialize)]
struct ExtractGlobalEventContent {
	content: Raw<AnyGlobalAccountDataEventContent>,
}

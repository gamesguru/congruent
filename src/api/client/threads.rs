use conduwuit::{
	Err, Result, at, debug_warn, err,
	matrix::{
		Event,
		pdu::{PduCount, PduEvent},
	},
};
use futures::StreamExt;
use http::StatusCode;
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use slipstream::{
	OwnedEventId, OwnedRoomId,
	api::client::threads::get_threads,
	codec::{DeError, Deserialize as CodecDeserialize},
	endpoint::body_field,
	json::Value,
	uint,
};

use crate::{
	Ruma,
	json_util::{json_response, single_field},
	router::{
		ApiError,
		extract::{Path, State},
		response::Response,
	},
};

struct ThreadSubscriptionBody {
	automatic: Option<OwnedEventId>,
}

impl CodecDeserialize for ThreadSubscriptionBody {
	fn from_json(value: &Value) -> Result<Self, DeError> {
		Ok(Self {
			automatic: body_field(Some(value), "automatic")?,
		})
	}
}

/// # `GET /_matrix/client/r0/rooms/{roomId}/threads`
pub(crate) async fn get_threads_route(
	State(services): State<crate::State>,
	ref body: Ruma<get_threads::v1::Request>,
) -> Result<get_threads::v1::Response> {
	// Use limit or else 10, with maximum 100
	let limit = body
		.limit
		.unwrap_or(uint!(10))
		.try_into()
		.unwrap_or(10)
		.min(100);

	let from: PduCount = body
		.from
		.as_deref()
		.map(str::parse)
		.transpose()?
		.unwrap_or_else(PduCount::max);

	let threads: Vec<(PduCount, PduEvent)> = services
		.rooms
		.threads
		.threads_until(body.sender_user(), &body.room_id, from, &body.include)
		.await?
		.take(limit)
		.filter_map(|(count, pdu)| async move {
			services
				.rooms
				.state_accessor
				.user_can_see_event(body.sender_user(), &body.room_id, &pdu.event_id)
				.await
				.then_some((count, pdu))
		})
		.then(|(count, mut pdu)| async move {
			if let Err(e) = services
				.rooms
				.pdu_metadata
				.add_bundled_aggregations_to_pdu(body.sender_user(), &mut pdu)
				.await
			{
				debug_warn!("Failed to add bundled aggregations to thread: {e}");
			}
			(count, pdu)
		})
		.collect()
		.await;

	Ok(get_threads::v1::Response {
		next_batch: threads
			.last()
			.filter(|_| threads.len() >= limit)
			.map(at!(0))
			.as_ref()
			.map(|c| format!("{c}")),

		chunk: threads
			.into_iter()
			.map(at!(1))
			.map(Event::into_format)
			.collect(),
	})
}

pub(crate) async fn put_thread_subscription_msc4306_route(
	State(services): State<crate::State>,
	Path((room_id, thread_id)): Path<(String, String)>,
	request: hyper::Request<Incoming>,
) -> std::result::Result<Response, ApiError> {
	let room_id = OwnedRoomId::parse(room_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;
	let thread_id = OwnedEventId::parse(thread_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid event ID."))))?;
	let (parts, body) = request.into_parts();
	let body = Limited::new(body, services.server.config.max_request_size)
		.collect()
		.await
		.map(http_body_util::Collected::to_bytes)
		.unwrap_or_default();
	let sender_user = authenticate_thread_user(&parts, &services).await?;
	let body = slipstream::codec::from_str::<ThreadSubscriptionBody>(
		std::str::from_utf8(&body).unwrap_or_default(),
	)
	.unwrap_or(ThreadSubscriptionBody { automatic: None });

	if !services
		.rooms
		.threads
		.thread_root_exists(&room_id, &thread_id)
		.await
	{
		return Err!(Request(NotFound("Thread not found."))).map_err(Into::into);
	}

	let automatic = if let Some(cause_event_id) = body.automatic.as_ref() {
		if services
			.rooms
			.threads
			.get_thread_id_for_event(cause_event_id)
			.await
			.as_deref()
			!= Some(&thread_id)
		{
			return Ok(msc4306_error(
				StatusCode::BAD_REQUEST,
				"IO.ELEMENT.MSC4306.M_NOT_IN_THREAD",
				"Automatic subscription cause event is not in the requested thread.",
			));
		}

		if let Some(previous) = services
			.rooms
			.threads
			.get_subscription(&sender_user, &room_id, &thread_id)
			.await
		{
			let cause_count = services
				.rooms
				.threads
				.thread_event_count(cause_event_id)
				.await?
				.into_unsigned();
			if !previous.subscribed && previous.last_unsubscribed >= cause_count {
				return Ok(msc4306_error(
					StatusCode::CONFLICT,
					"IO.ELEMENT.MSC4306.M_CONFLICTING_UNSUBSCRIPTION",
					"Automatic subscription conflicts with a later unsubscribe.",
				));
			}
		}

		true
	} else {
		false
	};

	services
		.rooms
		.threads
		.put_subscription(&sender_user, &room_id, &thread_id, automatic)
		.await?;

	Ok(json_response(Value::Object(slipstream::json::Object::new())))
}

pub(crate) async fn get_thread_subscription_msc4306_route(
	State(services): State<crate::State>,
	Path((room_id, thread_id)): Path<(String, String)>,
	request: hyper::Request<Incoming>,
) -> std::result::Result<Response, ApiError> {
	let room_id = OwnedRoomId::parse(room_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;
	let thread_id = OwnedEventId::parse(thread_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid event ID."))))?;
	let sender_user = authenticate_thread_user(&request.into_parts().0, &services).await?;

	if !services
		.rooms
		.threads
		.thread_root_exists(&room_id, &thread_id)
		.await
	{
		return Err!(Request(NotFound("Thread not found."))).map_err(Into::into);
	}

	let Some(subscription) = services
		.rooms
		.threads
		.get_subscription(&sender_user, &room_id, &thread_id)
		.await
		.filter(|subscription| subscription.subscribed)
	else {
		return Err!(Request(NotFound("Thread subscription not found."))).map_err(Into::into);
	};

	Ok(json_response(single_field("automatic", &subscription.automatic)))
}

pub(crate) async fn delete_thread_subscription_msc4306_route(
	State(services): State<crate::State>,
	Path((room_id, thread_id)): Path<(String, String)>,
	request: hyper::Request<Incoming>,
) -> std::result::Result<Response, ApiError> {
	let room_id = OwnedRoomId::parse(room_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;
	let thread_id = OwnedEventId::parse(thread_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid event ID."))))?;
	let sender_user = authenticate_thread_user(&request.into_parts().0, &services).await?;

	if !services
		.rooms
		.threads
		.thread_root_exists(&room_id, &thread_id)
		.await
	{
		return Err!(Request(NotFound("Thread not found."))).map_err(Into::into);
	}

	services
		.rooms
		.threads
		.delete_subscription(&sender_user, &room_id, &thread_id)?;

	Ok(json_response(Value::Object(slipstream::json::Object::new())))
}

fn msc4306_error(status: StatusCode, errcode: &str, error: &str) -> Response {
	let mut response = json_response(Value::Object(
		[
			("errcode".to_owned(), Value::String(errcode.to_owned())),
			("error".to_owned(), Value::String(error.to_owned())),
		]
		.into_iter()
		.collect(),
	));
	*response.status_mut() = status;
	response
}

async fn authenticate_thread_user(
	parts: &http::request::Parts,
	services: &crate::State,
) -> Result<slipstream::OwnedUserId, ApiError> {
	let token = parts
		.headers
		.get(http::header::AUTHORIZATION)
		.and_then(|value| value.to_str().ok())
		.and_then(|value| value.strip_prefix("Bearer "))
		.map(str::to_owned)
		.or_else(|| {
			parts.uri.query().and_then(|query| {
				serde_urlencoded::from_str::<std::collections::HashMap<String, String>>(query)
					.ok()
					.and_then(|params| params.get("access_token").cloned())
			})
		});
	let token =
		token.ok_or_else(|| conduwuit::err!(Request(MissingToken("Missing access token."))))?;
	services
		.users
		.find_from_token(&token)
		.await
		.map(|(user, _)| user)
		.map_err(Into::into)
}

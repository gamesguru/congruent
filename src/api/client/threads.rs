use axum::{
	body::{Body, to_bytes},
	extract::{Path, State},
	response::Response,
};
use conduwuit::{
	Err, Result, at, debug_warn, err,
	matrix::{
		Event,
		pdu::{PduCount, PduEvent},
	},
};
use futures::StreamExt;
use http::StatusCode;
use slipstream::{
	OwnedEventId, OwnedRoomId,
	api::{IncomingRequest, client::threads::get_threads},
	codec::{DeError, Deserialize as CodecDeserialize},
	endpoint::{EndpointRequest, body_field},
	json::Value,
	uint,
};

use crate::{
	Ruma,
	json_util::{json_response, single_field},
	router::authenticate_user,
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
	request: hyper::Request<Body>,
) -> Result<Response> {
	let room_id = OwnedRoomId::parse(room_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;
	let thread_id = OwnedEventId::parse(thread_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid event ID."))))?;
	let (parts, body) = request.into_parts();
	let body = to_bytes(body, services.server.config.max_request_size)
		.await
		.unwrap_or_default();
	let request = hyper::Request::from_parts(parts, Body::empty());
	let sender_user =
		authenticate_user(request, &services, &get_threads::v1::Request::METADATA).await?;
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
		return Err!(Request(NotFound("Thread not found.")));
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

	Ok(json_response(slipstream::json::Value::Object(slipstream::json::Object::new())))
}

pub(crate) async fn get_thread_subscription_msc4306_route(
	State(services): State<crate::State>,
	Path((room_id, thread_id)): Path<(String, String)>,
	request: hyper::Request<Body>,
) -> Result<Response> {
	let room_id = OwnedRoomId::parse(room_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;
	let thread_id = OwnedEventId::parse(thread_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid event ID."))))?;
	let sender_user =
		authenticate_user(request, &services, &get_threads::v1::Request::METADATA).await?;

	if !services
		.rooms
		.threads
		.thread_root_exists(&room_id, &thread_id)
		.await
	{
		return Err!(Request(NotFound("Thread not found.")));
	}

	let Some(subscription) = services
		.rooms
		.threads
		.get_subscription(&sender_user, &room_id, &thread_id)
		.await
		.filter(|subscription| subscription.subscribed)
	else {
		return Err!(Request(NotFound("Thread subscription not found.")));
	};

	Ok(json_response(single_field("automatic", &subscription.automatic)))
}

pub(crate) async fn delete_thread_subscription_msc4306_route(
	State(services): State<crate::State>,
	Path((room_id, thread_id)): Path<(String, String)>,
	request: hyper::Request<Body>,
) -> Result<Response> {
	let room_id = OwnedRoomId::parse(room_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid room ID."))))?;
	let thread_id = OwnedEventId::parse(thread_id)
		.map_err(|_| err!(Request(InvalidParam("Invalid event ID."))))?;
	let sender_user =
		authenticate_user(request, &services, &get_threads::v1::Request::METADATA).await?;

	if !services
		.rooms
		.threads
		.thread_root_exists(&room_id, &thread_id)
		.await
	{
		return Err!(Request(NotFound("Thread not found.")));
	}

	services
		.rooms
		.threads
		.delete_subscription(&sender_user, &room_id, &thread_id)?;

	Ok(json_response(slipstream::json::Value::Object(slipstream::json::Object::new())))
}

fn msc4306_error(status: StatusCode, errcode: &str, error: &str) -> Response {
	let mut response = json_response(slipstream::json::Value::Object(
		[
			("errcode".to_owned(), slipstream::json::Value::String(errcode.to_owned())),
			("error".to_owned(), slipstream::json::Value::String(error.to_owned())),
		]
		.into_iter()
		.collect(),
	));
	*response.status_mut() = status;
	response
}

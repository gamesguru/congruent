use conduwuit::{Err, Result};
use hyper::body::Incoming;
use slipstream::api::Metadata;

use crate::{
	json_util::single_field,
	router::{
		ApiError, authenticate_user,
		extract::{FromRequest, Path, State},
		response::Response,
	},
};

pub(crate) struct GetDelayedEventRequest;

impl GetDelayedEventRequest {
	pub(crate) const METADATA: Metadata =
		Metadata::new("GET", "/_matrix/client/unstable/org.matrix.msc4140/delayed_events/{}");
}

pub(crate) struct GetAllDelayedEventsRequest;

impl GetAllDelayedEventsRequest {
	pub(crate) const METADATA: Metadata =
		Metadata::new("GET", "/_matrix/client/unstable/org.matrix.msc4140/delayed_events");
}

struct ScheduleDelayedEventRequest;

impl ScheduleDelayedEventRequest {
	const METADATA: Metadata = Metadata::new(
		"PUT",
		"/_matrix/client/unstable/org.matrix.msc4140/rooms/{}/delayed_event/{}/{}",
	);
}

pub(crate) struct DelayedEventUser {
	pub(crate) user_id: slipstream::OwnedUserId,
}

impl FromRequest<crate::State, Incoming> for DelayedEventUser {
	type Rejection = ApiError;

	async fn from_request(
		request: hyper::Request<Incoming>,
		services: &crate::State,
	) -> Result<Self, ApiError> {
		Ok(Self {
			user_id: authenticate_user(request, services, &GetDelayedEventRequest::METADATA)
				.await
				.map_err(ApiError)?,
		})
	}
}

pub(crate) struct AllDelayedEventsUser {
	pub(crate) user_id: slipstream::OwnedUserId,
}

const DELAYED_EVENT_TXN_ID_PREFIX: &[u8] = b"\xFFdelay_id:";

fn delayed_event_txn_response(data: &[u8]) -> Result<Response, ApiError> {
	let Some(delay_id) = data.strip_prefix(DELAYED_EVENT_TXN_ID_PREFIX) else {
		return Err!(Request(InvalidParam(
			"Tried to use txn id already used for an incompatible endpoint."
		)))
		.map_err(Into::into);
	};

	let delay_id = std::str::from_utf8(delay_id).map_err(|error| {
		conduwuit::err!(Database("Invalid delayed-event txn response: {error}"))
	})?;
	Ok(crate::json_util::json_response(single_field("delay_id", &delay_id)))
}

pub(crate) async fn schedule_delayed_event_route(
	State(services): State<crate::State>,
	Path((room_id, event_type, txn_id)): Path<(
		slipstream::OwnedRoomId,
		slipstream::events::TimelineEventType,
		slipstream::OwnedTransactionId,
	)>,
	request: hyper::Request<Incoming>,
) -> Result<Response, ApiError> {
	let (sender_user, sender_device, body) =
		crate::router::authenticate_user_with_body_and_device(
			request,
			&services,
			&ScheduleDelayedEventRequest::METADATA,
		)
		.await?;
	let body = std::str::from_utf8(&body)
		.map_err(|_| conduwuit::err!(Request(NotJson("Request body is not valid UTF-8"))))?;
	let body = slipstream::canonical_json::from_json_str(body)
		.map_err(|_| conduwuit::err!(Request(BadJson("Invalid JSON body"))))?;
	let body = body
		.as_object()
		.ok_or_else(|| conduwuit::err!(Request(BadJson("Request body must be an object"))))?;
	let delay_ms = body
		.get("delay_ms")
		.and_then(slipstream::json::Value::as_i64)
		.and_then(|delay| u64::try_from(delay).ok())
		.filter(|delay| (1..=3_153_600_000_000).contains(delay))
		.ok_or_else(|| {
			conduwuit::err!(Request(InvalidParam(
				"delay_ms must be a positive integer no greater than 100 years"
			)))
		})?;
	let content = body
		.get("content")
		.filter(|content| content.as_object().is_some())
		.ok_or_else(|| conduwuit::err!(Request(BadJson("content must be a JSON object"))))?;
	let state_key = body
		.get("state_key")
		.map(|state_key| {
			state_key.as_str().map(str::to_owned).ok_or_else(|| {
				conduwuit::err!(Request(InvalidParam("state_key must be a string")))
			})
		})
		.transpose()?;
	let delay = std::time::Duration::from_millis(delay_ms);
	if std::time::SystemTime::now().checked_add(delay).is_none() {
		return Err!(Request(InvalidParam("delay_ms is too large"))).map_err(Into::into);
	}

	if let Ok(response) = services
		.transactions
		.get_room_txn(&sender_user, sender_device.as_ref(), &room_id, &txn_id)
		.await
	{
		return delayed_event_txn_response(&response);
	}

	let _txn_lock = services
		.transactions
		.lock_room_txn(&sender_user, sender_device.as_ref(), &room_id, &txn_id)
		.await;
	if let Ok(response) = services
		.transactions
		.get_room_txn(&sender_user, sender_device.as_ref(), &room_id, &txn_id)
		.await
	{
		return delayed_event_txn_response(&response);
	}

	let event = service::rooms::delayed_events::ScheduledDelayedEvent {
		event_type,
		state_key,
		content: slipstream::sswire::Raw::from_json_text(&slipstream::codec::to_string(content))?,
		user_id: sender_user.clone(),
		room_id: room_id.clone(),
		running_since: std::time::SystemTime::now(),
		delay,
	};
	let delay_id = services
		.rooms
		.delayed_events
		.queue_delayed_event(event)
		.await?;
	let mut txn_response = DELAYED_EVENT_TXN_ID_PREFIX.to_vec();
	txn_response.extend_from_slice(delay_id.as_bytes());
	services.transactions.add_room_txnid(
		&sender_user,
		sender_device.as_ref(),
		&room_id,
		&txn_id,
		&txn_response,
	);

	Ok(crate::json_util::json_response(single_field("delay_id", &delay_id)))
}

impl FromRequest<crate::State, Incoming> for AllDelayedEventsUser {
	type Rejection = ApiError;

	async fn from_request(
		request: hyper::Request<Incoming>,
		services: &crate::State,
	) -> Result<Self, ApiError> {
		Ok(Self {
			user_id: authenticate_user(request, services, &GetAllDelayedEventsRequest::METADATA)
				.await
				.map_err(ApiError)?,
		})
	}
}

// MSC4140: the delay_id itself is the bearer capability for these actions;
// per the MSC and its Complement coverage, restart/send/cancel are called
// without a user access token, so this route is intentionally unauthenticated.
pub(crate) async fn update_delayed_event_route(
	State(services): State<crate::State>,
	Path((delay_id, action)): Path<(String, String)>,
) -> Result<Response, ApiError> {
	let action = match action.as_str() {
		| "restart" => service::rooms::delayed_events::UpdateAction::Restart,
		| "send" => service::rooms::delayed_events::UpdateAction::Send,
		| "cancel" => service::rooms::delayed_events::UpdateAction::Cancel,
		| _ => return Err!(Request(NotFound("Invalid action."))).map_err(Into::into),
	};

	services
		.rooms
		.delayed_events
		.update_delayed_event(delay_id, action)
		.await?;

	Ok(crate::json_util::json_response(slipstream::json::Value::Object(
		slipstream::json::Object::new(),
	)))
}

pub(crate) async fn update_delayed_event_without_action_route(
	Path(_delay_id): Path<String>,
) -> Result<Response, ApiError> {
	Err!(Request(NotFound("Invalid action."))).map_err(Into::into)
}

pub(crate) async fn get_delayed_event_route(
	State(services): State<crate::State>,
	Path(delay_id): Path<String>,
	user: DelayedEventUser,
) -> Result<Response, ApiError> {
	let data = services
		.rooms
		.delayed_events
		.get_delayed_event(&user.user_id, delay_id)
		.await?;

	Ok(crate::json_util::json_response(single_field("delayed_event", &data)))
}

pub(crate) async fn get_all_delayed_events_route(
	State(services): State<crate::State>,
	user: AllDelayedEventsUser,
) -> Result<Response, ApiError> {
	let mut data = services
		.rooms
		.delayed_events
		.get_user_scheduled_delayed_events(&user.user_id, None)
		.await;

	data.sort_by_key(|event| {
		event
			.running_since
			.to_system_time()
			.checked_add(event.delay)
	});

	Ok(crate::json_util::json_response(single_field("delayed_events", &data)))
}

use axum::{body::Body, extract::FromRequest};
use conduwuit::{Err, Result};
use slipstream::api::Metadata;

use crate::{
	json_util::single_field,
	router::{
		ApiError, authenticate_user,
		extract::{Path, State},
	},
};

pub(crate) struct GetDelayedEventRequest;

impl GetDelayedEventRequest {
	const METADATA: Metadata =
		Metadata::new("GET", "/_matrix/client/unstable/org.matrix.msc4140/delayed_events/{}");
}

pub(crate) struct GetAllDelayedEventsRequest;

impl GetAllDelayedEventsRequest {
	const METADATA: Metadata =
		Metadata::new("GET", "/_matrix/client/unstable/org.matrix.msc4140/delayed_events");
}

pub(crate) struct DelayedEventUser {
	pub(crate) user_id: slipstream::OwnedUserId,
}

impl FromRequest<crate::State, Body> for DelayedEventUser {
	type Rejection = ApiError;

	async fn from_request(
		request: hyper::Request<Body>,
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

impl FromRequest<crate::State, Body> for AllDelayedEventsUser {
	type Rejection = ApiError;

	async fn from_request(
		request: hyper::Request<Body>,
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
) -> Result<axum::response::Response, ApiError> {
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
) -> Result<axum::response::Response, ApiError> {
	Err!(Request(NotFound("Invalid action."))).map_err(Into::into)
}

pub(crate) async fn get_delayed_event_route(
	State(services): State<crate::State>,
	Path(delay_id): Path<String>,
	user: DelayedEventUser,
) -> Result<axum::response::Response, ApiError> {
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
) -> Result<axum::response::Response, ApiError> {
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

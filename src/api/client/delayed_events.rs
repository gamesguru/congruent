use axum::{
	body::Body,
	extract::{FromRequest, State},
};
use conduwuit::{Err, Result};
use ruma::api::{AuthScheme, Metadata, VersionHistory};

use crate::router::authenticate_user;

pub(crate) struct GetDelayedEventRequest;

impl GetDelayedEventRequest {
	const METADATA: Metadata = Metadata {
		method: http::Method::GET,
		rate_limited: true,
		authentication: AuthScheme::AccessToken,
		history: VersionHistory::new(
			&["/_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delay_id}"],
			&[],
			None,
			None,
		),
	};
}

pub(crate) struct GetAllDelayedEventsRequest;

impl GetAllDelayedEventsRequest {
	const METADATA: Metadata = Metadata {
		method: http::Method::GET,
		rate_limited: true,
		authentication: AuthScheme::AccessToken,
		history: VersionHistory::new(
			&["/_matrix/client/unstable/org.matrix.msc4140/delayed_events"],
			&[],
			None,
			None,
		),
	};
}

pub(crate) struct DelayedEventUser {
	pub(crate) user_id: ruma::OwnedUserId,
}

impl FromRequest<crate::State, Body> for DelayedEventUser {
	type Rejection = conduwuit::Error;

	async fn from_request(
		request: hyper::Request<Body>,
		services: &crate::State,
	) -> Result<Self> {
		Ok(Self {
			user_id: authenticate_user(request, services, &GetDelayedEventRequest::METADATA)
				.await?,
		})
	}
}

pub(crate) struct AllDelayedEventsUser {
	pub(crate) user_id: ruma::OwnedUserId,
}

impl FromRequest<crate::State, Body> for AllDelayedEventsUser {
	type Rejection = conduwuit::Error;

	async fn from_request(
		request: hyper::Request<Body>,
		services: &crate::State,
	) -> Result<Self> {
		Ok(Self {
			user_id: authenticate_user(request, services, &GetAllDelayedEventsRequest::METADATA)
				.await?,
		})
	}
}

// MSC4140: the delay_id itself is the bearer capability for these actions;
// per the MSC and its Complement coverage, restart/send/cancel are called
// without a user access token, so this route is intentionally unauthenticated.
pub(crate) async fn update_delayed_event_route(
	State(services): State<crate::State>,
	axum::extract::Path((delay_id, action)): axum::extract::Path<(String, String)>,
) -> Result<axum::Json<serde_json::Value>> {
	let action = match action.as_str() {
		| "restart" => service::rooms::delayed_events::UpdateAction::Restart,
		| "send" => service::rooms::delayed_events::UpdateAction::Send,
		| "cancel" => service::rooms::delayed_events::UpdateAction::Cancel,
		| _ => return Err!(Request(NotFound("Invalid action."))),
	};

	services
		.rooms
		.delayed_events
		.update_delayed_event(delay_id, action)
		.await?;

	Ok(axum::Json(serde_json::json!({})))
}

pub(crate) async fn update_delayed_event_without_action_route(
	axum::extract::Path(_delay_id): axum::extract::Path<String>,
) -> Result<axum::Json<serde_json::Value>> {
	Err!(Request(NotFound("Invalid action.")))
}

pub(crate) async fn get_delayed_event_route(
	State(services): State<crate::State>,
	axum::extract::Path(delay_id): axum::extract::Path<String>,
	user: DelayedEventUser,
) -> Result<axum::Json<serde_json::Value>> {
	let data = services
		.rooms
		.delayed_events
		.get_delayed_event(&user.user_id, delay_id)
		.await?;

	Ok(axum::Json(serde_json::json!({
		"delayed_event": data,
	})))
}

pub(crate) async fn get_all_delayed_events_route(
	State(services): State<crate::State>,
	user: AllDelayedEventsUser,
) -> Result<axum::Json<serde_json::Value>> {
	let mut data = services
		.rooms
		.delayed_events
		.get_user_scheduled_delayed_events(&user.user_id, None)
		.await;

	data.sort_by_key(|event| {
		event
			.running_since
			.to_system_time()
			.and_then(|ts| ts.checked_add(event.delay))
	});

	Ok(axum::Json(serde_json::json!({
		"delayed_events": data,
	})))
}

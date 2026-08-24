#[cfg(test)]
mod tests;
use axum::{extract::State, response::IntoResponse};
use axum_client_ip::ClientIp;
use conduwuit::{Err, Result, err, matrix::Event};
use futures::{FutureExt, TryStreamExt};
use ruma::{
	RoomId,
	api::client::state::{get_state_events, get_state_events_for_key, send_state_event},
};
use serde_json::json;

use crate::{Ruma, RumaResponse};

/// # `PUT /_matrix/client/*/rooms/{roomId}/state/{eventType}/{stateKey}`
///
/// Sends a state event into the room.
pub(crate) async fn send_state_event_for_key_route(
	State(services): State<crate::State>,
	ClientIp(ip): ClientIp,
	body: Ruma<send_state_event::v3::Request>,
) -> Result<axum::response::Response> {
	let sender_user = body.sender_user();
	services
		.users
		.update_device_last_seen(sender_user, body.sender_device.as_deref(), ip)
		.await;

	if services.users.is_suspended(sender_user).await? {
		return Err!(Request(UserSuspended("You cannot perform this action while suspended.")));
	}

	if let Some(delay) = body.delay {
		if std::time::SystemTime::now().checked_add(delay).is_none() {
			return Err!(Request(InvalidParam("org.matrix.msc4140.delay is too large.")));
		}
		let event = service::rooms::delayed_events::ScheduledDelayedEvent {
			event_type: body.event_type.clone().into(),
			state_key: Some(body.state_key.clone()),
			content: body.body.body.cast_ref().clone(),
			user_id: sender_user.to_owned(),
			room_id: body.room_id.clone(),
			running_since: std::time::SystemTime::now(),
			delay,
		};
		let delay_id = services
			.rooms
			.delayed_events
			.queue_delayed_event(event)
			.await?;

		return Ok(axum::Json(serde_json::json!({
			"delay_id": delay_id,
		}))
		.into_response());
	}

	let state_lock = services
		.rooms
		.state
		.mutex
		.lock::<RoomId>(&body.room_id)
		.await;

	let event_id = services
		.rooms
		.timeline
		.send_state_event_for_key_helper(
			sender_user,
			&body.room_id,
			&state_lock,
			&body.event_type,
			&body.body.body,
			&body.state_key,
			if body.appservice_info.is_some() {
				body.timestamp
			} else {
				None
			},
			None,
		)
		.boxed()
		.await?;

	Ok(RumaResponse(send_state_event::v3::Response { event_id }).into_response())
}

/// # `PUT /_matrix/client/*/rooms/{roomId}/state/{eventType}`
///
/// Sends a state event into the room.
pub(crate) async fn send_state_event_for_empty_key_route(
	State(services): State<crate::State>,
	ClientIp(ip): ClientIp,
	body: Ruma<send_state_event::v3::Request>,
) -> Result<axum::response::Response> {
	send_state_event_for_key_route(State(services), ClientIp(ip), body)
		.boxed()
		.await
}

/// # `GET /_matrix/client/v3/rooms/{roomid}/state`
///
/// Get all state events for a room.
///
/// - If not joined: Only works if current room history visibility is world
///   readable
pub(crate) async fn get_state_events_route(
	State(services): State<crate::State>,
	body: Ruma<get_state_events::v3::Request>,
) -> Result<get_state_events::v3::Response> {
	let sender_user = body.sender_user();

	if !services
		.rooms
		.state_accessor
		.user_can_see_state_events(sender_user, &body.room_id)
		.await
	{
		return Err!(Request(Forbidden("You don't have permission to view the room state.")));
	}

	Ok(get_state_events::v3::Response {
		room_state: services
			.rooms
			.state_accessor
			.room_state_full_pdus(&body.room_id)
			.map_ok(Event::into_format)
			.try_collect()
			.await?,
	})
}

/// # `GET /_matrix/client/v3/rooms/{roomid}/state/{eventType}/{stateKey}`
///
/// Get single state event of a room with the specified state key.
/// The optional query parameter `?format=event|content` allows returning the
/// full room state event or just the state event's content (default behaviour)
///
/// - If not joined: Only works if current room history visibility is world
///   readable
pub(crate) async fn get_state_events_for_key_route(
	State(services): State<crate::State>,
	body: Ruma<get_state_events_for_key::v3::Request>,
) -> Result<get_state_events_for_key::v3::Response> {
	let sender_user = body.sender_user();

	if !services
		.rooms
		.state_accessor
		.user_can_see_state_events(sender_user, &body.room_id)
		.await
	{
		return Err!(Request(NotFound(debug_warn!(
			"You don't have permission to view the room state."
		))));
	}

	let event = services
		.rooms
		.state_accessor
		.room_state_get(&body.room_id, &body.event_type, &body.state_key)
		.await
		.map_err(|_| {
			err!(Request(NotFound(debug_warn!(
					room_id = %body.room_id,
					event_type = %body.event_type,
					"State event not found in room.",
			))))
		})?;

	let event_format = body
		.format
		.as_ref()
		.is_some_and(|f| f.to_lowercase().eq("event"));

	Ok(get_state_events_for_key::v3::Response {
		content: (!event_format).then(|| event.get_content_as_value()),
		event: event_format.then(|| {
			json!({
				"content": event.content(),
				"event_id": event.event_id(),
				"origin_server_ts": event.origin_server_ts(),
				"room_id": event.room_id_or_hash(),
				"sender": event.sender(),
				"state_key": event.state_key(),
				"type": event.kind(),
				"unsigned": event.unsigned(),
			})
		}),
	})
}

/// # `GET /_matrix/client/v3/rooms/{roomid}/state/{eventType}`
///
/// Get single state event of a room.
/// The optional query parameter `?format=event|content` allows returning the
/// full room state event or just the state event's content (default behaviour)
///
/// - If not joined: Only works if current room history visibility is world
///   readable
pub(crate) async fn get_state_events_for_empty_key_route(
	State(services): State<crate::State>,
	body: Ruma<get_state_events_for_key::v3::Request>,
) -> Result<RumaResponse<get_state_events_for_key::v3::Response>> {
	get_state_events_for_key_route(State(services), body)
		.await
		.map(RumaResponse)
}

use axum::extract::State;
use conduwuit::{Err, Event, Result, err, info};
use ruma::{MilliSecondsSinceUnixEpoch, api::federation::event::get_event};

use super::AccessCheck;
use crate::Ruma;

/// # `GET /_matrix/federation/v1/event/{eventId}`
///
/// Retrieves a single event from the server.
///
/// - Only works if a user of this server is currently invited or joined the
///   room
pub(crate) async fn get_event_route(
	State(services): State<crate::State>,
	body: Ruma<get_event::v1::Request>,
) -> Result<get_event::v1::Response> {
	let event = services
		.rooms
		.timeline
		.get_pdu(&body.event_id)
		.await
		.map_err(|_| err!(Request(NotFound("Event not found."))))?;

	let room_id = event
		.room_id_or_hash()
		.ok_or_else(|| err!(Request(NotFound("Event has no room_id."))))?;

	AccessCheck {
		services: &services,
		origin: body.origin(),
		room_id: &room_id,
		event_id: Some(&body.event_id),
	}
	.check()
	.await?;

	if !services
		.rooms
		.state_cache
		.server_in_room(services.globals.server_name(), &room_id)
		.await
	{
		info!(
			origin = body.origin().as_str(),
			"Refusing to serve state for room we aren't participating in"
		);
		return Err!(Request(NotFound("This server is not participating in that room.")));
	}

	let event_json = services
		.rooms
		.timeline
		.get_pdu_json(&body.event_id)
		.await
		.map_err(|_| err!(Request(NotFound("Event not found."))))?;

	Ok(get_event::v1::Response {
		origin: services.globals.server_name().to_owned(),
		origin_server_ts: MilliSecondsSinceUnixEpoch::now(),
		pdu: services
			.sending
			.convert_to_outgoing_federation_event(event_json)
			.await,
	})
}

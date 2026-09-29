use axum::extract::State;
use conduwuit::{Err, Result, info};
use futures::{StreamExt, pin_mut};
use ruma::{MilliSecondsSinceUnixEpoch, api::federation::event::get_event_by_timestamp};

use super::AccessCheck;
use crate::Ruma;

/// # `GET /_matrix/federation/v1/timestamp_to_event/{roomId}`
///
/// Get the ID of the event closest to the given timestamp.
///
/// Federation-side handler so other homeservers can ask us for historical
/// events via MSC3030.
pub(crate) async fn get_event_by_timestamp_route(
	State(services): State<crate::State>,
	body: Ruma<get_event_by_timestamp::v1::Request>,
) -> Result<get_event_by_timestamp::v1::Response> {
	let room_id = &body.room_id;

	AccessCheck {
		services: &services,
		origin: body.origin(),
		room_id,
		event_id: None,
	}
	.check()
	.await?;

	if !services
		.rooms
		.state_cache
		.server_in_room(services.globals.server_name(), room_id)
		.await
	{
		return Err!(Request(NotFound("This server is not participating in that room.")));
	}

	let stream = services
		.rooms
		.timeline
		.pdus_by_timestamp(room_id, body.ts.0.into(), body.dir);
	pin_mut!(stream);

	while let Some(item) = stream.next().await {
		let pdu = item?;

		if (AccessCheck {
			services: &services,
			origin: body.origin(),
			room_id,
			event_id: Some(&pdu.event_id),
		})
		.check()
		.await
		.is_ok()
		{
			return Ok(get_event_by_timestamp::v1::Response::new(
				pdu.event_id.clone(),
				MilliSecondsSinceUnixEpoch(pdu.origin_server_ts),
			));
		}
	}

	info!(
		%room_id,
		ts = ?body.ts,
		dir = ?body.dir,
		"No event found in timestamp index for federation request"
	);

	Err!(Request(NotFound("No event found near the given timestamp")))
}

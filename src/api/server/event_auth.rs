use std::{borrow::Borrow, iter::once};

use axum::extract::State;
use conduwuit::{Err, Error, Event, Result, info, utils::stream::ReadyExt};
use futures::StreamExt;
use ruma::api::{client::error::ErrorKind, federation::authorization::get_event_authorization};

use super::AccessCheck;
use crate::Ruma;

/// # `GET /_matrix/federation/v1/event_auth/{roomId}/{eventId}`
///
/// Retrieves the auth chain for a given event.
///
/// - This does not include the event itself
pub(crate) async fn get_event_authorization_route(
	State(services): State<crate::State>,
	body: Ruma<get_event_authorization::v1::Request>,
) -> Result<get_event_authorization::v1::Response> {
	AccessCheck {
		services: &services,
		origin: body.origin(),
		room_id: &body.room_id,
		event_id: None,
	}
	.check()
	.await?;

	info!(
		origin = body.origin().as_str(),
		room_id = %body.room_id,
		event_id = %body.event_id,
		"Serving event_auth request"
	);

	let event = services
		.rooms
		.timeline
		.get_pdu(&body.event_id)
		.await
		.map_err(|_| Error::BadRequest(ErrorKind::NotFound, "Event not found."))?;

	if event.room_id_or_hash().as_deref() != Some(body.room_id.as_ref()) {
		return Err!(Request(NotFound("Event does not belong to this room.")));
	}

	let auth_chain = services
		.rooms
		.auth_chain
		.event_ids_iter(&body.room_id, once(body.event_id.borrow()))
		.ready_filter_map(Result::ok)
		.ready_filter(|id| id != &body.event_id)
		.filter_map(|id| async move { services.rooms.timeline.get_pdu_json(&id).await.ok() })
		.then(|pdu| services.sending.convert_to_outgoing_federation_event(pdu))
		.collect()
		.await;

	Ok(get_event_authorization::v1::Response { auth_chain })
}

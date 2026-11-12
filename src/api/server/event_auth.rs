use std::{borrow::Borrow, iter::once};

use conduwuit::{Err, Error, Event, Result, err, info};
use futures::TryStreamExt;
use slipstream::api::{
	client::error::ErrorKind, federation::authorization::get_event_authorization,
};

use super::AccessCheck;
use crate::{Ruma, router::extract::State};

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

	let services_for_read = services.services();
	let reply_event_id = body.event_id.clone();
	let runtime = services.server.runtime().clone();
	let event = runtime
		.spawn_blocking(move || services_for_read.rooms.timeline.get_pdu(&reply_event_id))
		.await
		.map_err(|_| Error::BadRequest(ErrorKind::NotFound, "Event not found."))?
		.map_err(|_| Error::BadRequest(ErrorKind::NotFound, "Event not found."))?;

	if event.room_id_or_hash().as_deref() != Some(body.room_id.as_ref()) {
		return Err!(Request(NotFound("Event does not belong to this room.")));
	}

	let auth_chain = services
		.rooms
		.auth_chain
		.event_ids_iter(&body.room_id, once(body.event_id.borrow()))
		.try_filter(|id| futures::future::ready(id != &body.event_id))
		.and_then(|id| {
			let services = services.services();
			let runtime = services.server.runtime().clone();
			async move {
				runtime
					.spawn_blocking(move || services.rooms.timeline.get_pdu_json(&id))
					.await
					.map_err(|error| err!(Database("blocking auth PDU lookup failed: {error}")))?
			}
		})
		.and_then(|pdu| async {
			Ok(services
				.sending
				.convert_to_outgoing_federation_event(pdu)
				.await)
		})
		.try_collect()
		.await?;

	Ok(get_event_authorization::v1::Response { auth_chain })
}

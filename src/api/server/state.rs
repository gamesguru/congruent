use std::{borrow::Borrow, iter::once};

use conduwuit::{Result, info, utils::IterStream};
use futures::{FutureExt, TryStreamExt};
use slipstream::api::federation::event::get_room_state;

use super::{AccessCheck, utils::state_ids_at_event};
use crate::{Ruma, router::extract::State};

/// # `GET /_matrix/federation/v1/state/{roomId}`
///
/// Retrieves a snapshot of a room's state at a given event.
pub(crate) async fn get_room_state_route(
	State(services): State<crate::State>,
	body: Ruma<get_room_state::v1::Request>,
) -> Result<get_room_state::v1::Response> {
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
		"Serving full room state request"
	);

	let state_ids = state_ids_at_event(&services, &body.room_id, &body.event_id).await?;

	let pdus = state_ids
		.iter()
		.try_stream()
		.and_then(|id| services.rooms.timeline.get_pdu_json(id))
		.and_then(|pdu| {
			services
				.sending
				.convert_to_outgoing_federation_event(pdu)
				.map(Ok)
		})
		.try_collect()
		.await?;

	let auth_chain = services
		.rooms
		.auth_chain
		.event_ids_iter(&body.room_id, once(body.event_id.borrow()))
		.and_then(|id| async move { services.rooms.timeline.get_pdu_json(&id).await })
		.and_then(|pdu| {
			services
				.sending
				.convert_to_outgoing_federation_event(pdu)
				.map(Ok)
		})
		.try_collect()
		.await?;

	Ok(get_room_state::v1::Response { auth_chain, pdus })
}

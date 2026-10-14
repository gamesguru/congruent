use std::{borrow::Borrow, iter::once};

use conduwuit::{Result, info};
use futures::TryStreamExt;
use slipstream::api::federation::event::get_room_state_ids;

use super::{AccessCheck, utils::state_ids_at_event};
use crate::{Ruma, router::extract::State};

/// # `GET /_matrix/federation/v1/state_ids/{roomId}`
///
/// Retrieves a snapshot of a room's state at a given event, in the form of
/// event IDs.
pub(crate) async fn get_room_state_ids_route(
	State(services): State<crate::State>,
	body: Ruma<get_room_state_ids::v1::Request>,
) -> Result<get_room_state_ids::v1::Response> {
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
		"Serving state_ids request"
	);

	let pdu_ids = state_ids_at_event(&services, &body.room_id, &body.event_id).await?;

	let auth_chain_ids = services
		.rooms
		.auth_chain
		.event_ids_iter(&body.room_id, once(body.event_id.borrow()))
		.try_collect()
		.await?;

	Ok(get_room_state_ids::v1::Response { auth_chain_ids, pdu_ids })
}

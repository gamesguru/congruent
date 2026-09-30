use std::{borrow::Borrow, iter::once};

use axum::extract::State;
use conduwuit::{Result, at, err, info};
use futures::TryStreamExt;
use ruma::{OwnedEventId, api::federation::event::get_room_state_ids};

use super::AccessCheck;
use crate::Ruma;

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

	let root_handle = services
		.rooms
		.state_accessor
		.pdu_roothandle_before_event(&body.room_id, &body.event_id)
		.await
		.map_err(|_| err!(Request(NotFound("Pdu state not found."))))?;

	let pdu_ids: Vec<OwnedEventId> = services
		.rooms
		.state_accessor
		.state_full_ids_hamt(&root_handle)
		.try_collect::<Vec<_>>()
		.await?
		.into_iter()
		.map(at!(1))
		.collect();

	let auth_chain_ids = services
		.rooms
		.auth_chain
		.event_ids_iter(&body.room_id, once(body.event_id.borrow()))
		.try_collect()
		.await?;

	Ok(get_room_state_ids::v1::Response { auth_chain_ids, pdu_ids })
}

use axum::extract::State;
use conduwuit::{Err, Result, err, matrix::pdu::PduEvent};
use ruma::{
	RoomVersionId::*, api::federation::knock::send_knock, events::room::member::MembershipState,
	serde::JsonObject,
};

use crate::Ruma;

/// # `PUT /_matrix/federation/v1/send_knock/{roomId}/{eventId}`
///
/// Submits a signed knock event.
pub(crate) async fn create_knock_event_v1_route(
	State(services): State<crate::State>,
	body: Ruma<send_knock::v1::Request>,
) -> Result<send_knock::v1::Response> {
	let (event_id, value, _, room_version_id, sender, _state_key) =
		super::utils::verify_send_membership(
			&services,
			body.origin(),
			&body.room_id,
			&body.pdu,
			MembershipState::Knock,
		)
		.await?;

	if matches!(room_version_id, V1 | V2 | V3 | V4 | V5 | V6) {
		return Err!(Request(Forbidden("Room version does not support knocking.")));
	}

	let mut event: JsonObject = serde_json::from_str(body.pdu.get())
		.map_err(|e| err!(Request(InvalidParam("Invalid knock event PDU: {e}"))))?;

	event.insert("event_id".to_owned(), "$placeholder".into());

	let pdu: PduEvent = serde_json::from_value(event.into())
		.map_err(|e| err!(Request(InvalidParam("Invalid knock event PDU: {e}"))))?;

	super::utils::handle_and_send_incoming_pdu(
		&services,
		sender.server_name(),
		&body.room_id,
		&event_id,
		value,
		None,
	)
	.await?;

	let knock_room_state = services
		.rooms
		.state
		.summary_stripped(&pdu, &body.room_id)
		.await;

	Ok(send_knock::v1::Response { knock_room_state })
}

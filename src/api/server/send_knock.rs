use axum::extract::State;
use conduwuit::{Err, Result, err, matrix::pdu::PduEvent};
use slipstream::{
	RoomVersionId::*, api::federation::knock::send_knock, codec,
	events::room::member::MembershipState, serde::JsonObject,
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

	let mut event: JsonObject = codec::from_str(body.pdu.get())
		.map_err(|e| err!(Request(InvalidParam("Invalid knock event PDU: {e}"))))?;

	event.insert("event_id".to_owned(), "$placeholder".into());

	let pdu: PduEvent = codec::Deserialize::from_json(&event.into())
		.map_err(|e| err!(Request(InvalidParam("Invalid knock event PDU: {e}"))))?;

	super::utils::handle_and_send_incoming_pdu(
		&services,
		&sender.server_name(),
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

	// Ensure the remote knocker is visible to /sync before acknowledging
	// /send_knock. Invite handling performs this state-cache update explicitly;
	// relying only on append_pdu's membership side effect leaves a timing window
	// where the federation request has completed but rooms.knock is absent.
	services.rooms.state_cache.mark_as_knocked(
		&sender,
		&body.room_id,
		Some(knock_room_state.clone()),
	);

	Ok(send_knock::v1::Response { knock_room_state })
}

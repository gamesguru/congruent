use axum::extract::State;
use conduwuit::Result;
use ruma::{
	api::federation::membership::prepare_leave_event,
	events::room::member::{MembershipState, RoomMemberEventContent},
};

use crate::Ruma;

/// # `GET /_matrix/federation/v1/make_leave/{roomId}/{eventId}`
///
/// Creates a leave template.
pub(crate) async fn create_leave_event_template_route(
	State(services): State<crate::State>,
	body: Ruma<prepare_leave_event::v1::Request>,
) -> Result<prepare_leave_event::v1::Response> {
	super::utils::verify_make_membership(&services, body.origin(), &body.room_id, &body.user_id)
		.await?;

	let room_version_id = services.rooms.state.get_room_version(&body.room_id).await?;
	let event = super::utils::build_membership_template_pdu(
		&services,
		&body.room_id,
		&body.user_id,
		RoomMemberEventContent::new(MembershipState::Leave),
	)
	.await?;

	Ok(prepare_leave_event::v1::Response {
		room_version: Some(room_version_id),
		event,
	})
}

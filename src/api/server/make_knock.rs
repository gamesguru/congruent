use RoomVersionId::*;
use axum::extract::State;
use conduwuit::{Err, Error, Result, debug_warn};
use ruma::{
	RoomVersionId,
	api::{client::error::ErrorKind, federation::knock::create_knock_event_template},
	events::room::member::{MembershipState, RoomMemberEventContent},
};

use crate::Ruma;

/// # `GET /_matrix/federation/v1/make_knock/{roomId}/{userId}`
///
/// Creates a knock template.
pub(crate) async fn create_knock_event_template_route(
	State(services): State<crate::State>,
	body: Ruma<create_knock_event_template::v1::Request>,
) -> Result<create_knock_event_template::v1::Response> {
	super::utils::verify_make_membership(&services, body.origin(), &body.room_id, &body.user_id)
		.await?;

	let room_version_id = services.rooms.state.get_room_version(&body.room_id).await?;

	if matches!(room_version_id, V1 | V2 | V3 | V4 | V5 | V6) {
		return Err(Error::BadRequest(
			ErrorKind::IncompatibleRoomVersion { room_version: room_version_id },
			"Room version does not support knocking.",
		));
	}

	if !body.ver.contains(&room_version_id) {
		return Err(Error::BadRequest(
			ErrorKind::IncompatibleRoomVersion { room_version: room_version_id },
			"Your homeserver does not support the features required to knock on this room.",
		));
	}

	if let Ok(membership) = services
		.rooms
		.state_accessor
		.get_member(&body.room_id, &body.user_id)
		.await
	{
		if membership.membership == MembershipState::Ban {
			debug_warn!(
				"Remote user {} is banned from {} but attempted to knock",
				&body.user_id,
				&body.room_id
			);
			return Err!(Request(Forbidden("You cannot knock on a room you are banned from.")));
		}
	}

	let event = super::utils::build_membership_template_pdu(
		&services,
		&body.room_id,
		&body.user_id,
		RoomMemberEventContent::new(MembershipState::Knock),
	)
	.await?;

	Ok(create_knock_event_template::v1::Response { room_version: room_version_id, event })
}

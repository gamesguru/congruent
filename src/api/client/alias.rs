use axum::extract::State;
use conduwuit::{Err, Result, debug_warn, matrix::pdu::PduBuilder};
use ruma::api::client::alias::{create_alias, delete_alias, get_alias};
use ruma::events::room::canonical_alias::RoomCanonicalAliasEventContent;

use crate::Ruma;

/// # `PUT /_matrix/client/v3/directory/room/{roomAlias}`
///
/// Creates a new room alias on this server.
pub(crate) async fn create_alias_route(
	State(services): State<crate::State>,
	body: Ruma<create_alias::v3::Request>,
) -> Result<create_alias::v3::Response> {
	let sender_user = body.sender_user();
	if services.users.is_suspended(sender_user).await? {
		return Err!(Request(UserSuspended("You cannot perform this action while suspended.")));
	}

	services
		.rooms
		.alias
		.appservice_checks(&body.room_alias, &body.appservice_info)
		.await?;

	// this isn't apart of alias_checks or delete alias route because we should
	// allow removing forbidden room aliases
	if services
		.globals
		.forbidden_alias_names()
		.is_match(body.room_alias.alias())
	{
		return Err!(Request(Forbidden("Room alias is forbidden.")));
	}

	if services
		.rooms
		.alias
		.resolve_local_alias(&body.room_alias)
		.await
		.is_ok()
	{
		return Err!(Conflict("Alias already exists."));
	}

	services
		.rooms
		.alias
		.set_alias(&body.room_alias, &body.room_id, sender_user)?;

	Ok(create_alias::v3::Response::new())
}

/// # `DELETE /_matrix/client/v3/directory/room/{roomAlias}`
///
/// Deletes a room alias from this server.
pub(crate) async fn delete_alias_route(
	State(services): State<crate::State>,
	body: Ruma<delete_alias::v3::Request>,
) -> Result<delete_alias::v3::Response> {
	let sender_user = body.sender_user();
	if services.users.is_suspended(sender_user).await? {
		return Err!(Request(UserSuspended("You cannot perform this action while suspended.")));
	}

	services
		.rooms
		.alias
		.appservice_checks(&body.room_alias, &body.appservice_info)
		.await?;

	let room_id = services
		.rooms
		.alias
		.resolve_local_alias(&body.room_alias)
		.await?;
	let clears_canonical_alias = services
		.rooms
		.state_accessor
		.get_canonical_alias(&room_id)
		.await
		.is_ok_and(|alias| alias == body.room_alias);

	services
		.rooms
		.alias
		.remove_alias(&body.room_alias, sender_user)
		.await?;

	if clears_canonical_alias {
		let state_lock = services.rooms.state.mutex.lock(&room_id).await;
		if let Err(e) = services
			.rooms
			.timeline
			.build_and_append_pdu(
				PduBuilder::state(String::new(), &RoomCanonicalAliasEventContent {
					alias: None,
					alt_aliases: vec![],
				}),
				sender_user,
				Some(&room_id),
				&state_lock,
			)
			.await
		{
			debug_warn!(
				"failed to clear canonical alias after deleting {} in {}: {e}",
				body.room_alias, room_id
			);
		}
	}

	Ok(delete_alias::v3::Response::new())
}

/// # `GET /_matrix/client/v3/directory/room/{roomAlias}`
///
/// Resolve an alias locally or over federation.
pub(crate) async fn get_alias_route(
	State(services): State<crate::State>,
	body: Ruma<get_alias::v3::Request>,
) -> Result<get_alias::v3::Response> {
	let room_alias = body.body.room_alias;

	let Ok((room_id, servers)) = services.rooms.alias.resolve_alias(&room_alias).await else {
		return Err!(Request(NotFound("Room with alias not found.")));
	};

	Ok(get_alias::v3::Response::new(room_id, servers))
}

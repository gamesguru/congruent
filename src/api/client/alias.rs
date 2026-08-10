use axum::extract::State;
use conduwuit::{Err, Result, matrix::pdu::PduBuilder};
use ruma::{
	api::client::alias::{create_alias, delete_alias, get_alias},
	events::{StateEventType, room::canonical_alias::RoomCanonicalAliasEventContent},
};

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

	// Perform the permission checks up-front, before any state is mutated. This
	// duplicates the checks `remove_alias` performs internally, but ensures a
	// request that would ultimately be rejected can't first append a state event
	// or otherwise leave the room in a half-updated condition.
	services
		.rooms
		.alias
		.ensure_user_can_remove_alias(&body.room_alias, sender_user)
		.await?;

	// Hold the room's state lock across the read of the current canonical-alias
	// content and the write of its replacement, so a concurrent canonical-alias
	// update can't be clobbered by (or clobber) this deletion.
	let state_lock = services.rooms.state.mutex.lock(&room_id).await;

	let current_canonical_alias = services
		.rooms
		.state_accessor
		.room_state_get_content::<RoomCanonicalAliasEventContent>(
			&room_id,
			&StateEventType::RoomCanonicalAlias,
			"",
		)
		.await
		.ok();

	if let Some(mut content) = current_canonical_alias {
		let clears_canonical_alias = content.alias.as_ref() == Some(&body.room_alias);
		let retained_alt_aliases: Vec<_> = content
			.alt_aliases
			.iter()
			.filter(|alias| **alias != body.room_alias)
			.cloned()
			.collect();
		let removes_alt_alias = retained_alt_aliases.len() != content.alt_aliases.len();

		if clears_canonical_alias || removes_alt_alias {
			if clears_canonical_alias {
				content.alias = None;
			}
			content.alt_aliases = retained_alt_aliases;

			services
				.rooms
				.timeline
				.build_and_append_pdu(
					PduBuilder::state(String::new(), &content),
					sender_user,
					Some(&room_id),
					&state_lock,
				)
				.await?;
		}
	}

	services
		.rooms
		.alias
		.remove_alias(&body.room_alias, sender_user)
		.await?;

	drop(state_lock);

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

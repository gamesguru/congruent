use std::borrow::ToOwned;

use axum::extract::State;
use conduwuit::{Err, Error, Result, debug, debug_info, info, warn};
use conduwuit_service::Services;
use futures::StreamExt;
use ruma::{
	OwnedRoomId, OwnedUserId, RoomId, RoomVersionId, UserId,
	api::{client::error::ErrorKind, federation::membership::prepare_join_event},
	events::{
		StateEventType,
		room::{
			join_rules::{AllowRule, JoinRule, RoomJoinRulesEventContent},
			member::{MembershipState, RoomMemberEventContent},
		},
	},
};

use crate::Ruma;

/// # `GET /_matrix/federation/v1/make_join/{roomId}/{userId}`
///
/// Creates a join template.
#[tracing::instrument(skip_all, fields(room_id = %body.room_id, user_id = %body.user_id, origin = %body.origin()), level = "info")]
pub(crate) async fn create_join_event_template_route(
	State(services): State<crate::State>,
	body: Ruma<prepare_join_event::v1::Request>,
) -> Result<prepare_join_event::v1::Response> {
	super::utils::verify_make_membership(&services, body.origin(), &body.room_id, &body.user_id)
		.await?;

	let room_version_id = services.rooms.state.get_room_version(&body.room_id).await?;
	if !body.ver.contains(&room_version_id) {
		return Err(Error::BadRequest(
			ErrorKind::IncompatibleRoomVersion { room_version: room_version_id },
			"Room version not supported.",
		));
	}

	let state_lock = services.rooms.state.mutex.lock(&body.room_id).await;
	let is_invited = services
		.rooms
		.state_cache
		.is_invited(&body.user_id, &body.room_id)
		.await;
	let mut is_joined = services
		.rooms
		.state_cache
		.is_joined(&body.user_id, &body.room_id)
		.await;

	// A remote server is asking to make_join, but our cache thinks they are already
	// joined. This usually means they recently left and our federation queue
	// hasn't processed the leave event yet. Sleep briefly and re-check to let
	// federation catch up.
	if is_joined {
		for _ in 0..5 {
			tokio::time::sleep(std::time::Duration::from_millis(150)).await;
			is_joined = services
				.rooms
				.state_cache
				.is_joined(&body.user_id, &body.room_id)
				.await;
			if !is_joined {
				break;
			}
		}
	}
	let join_authorized_via_users_server: Option<OwnedUserId> = {
		use RoomVersionId::*;
		if is_joined || is_invited {
			// User is already joined or invited and consequently does not need an
			// authorising user
			None
		} else if matches!(room_version_id, V1 | V2 | V3 | V4 | V5 | V6 | V7) {
			// room version does not support restricted join rules
			None
		} else if let Some(allowed_rooms) = user_can_perform_restricted_join(
			&services,
			&body.user_id,
			&body.room_id,
			&room_version_id,
		)
		.await?
		{
			// The authorising user's power level may not have propagated yet
			// (common in test scenarios where events arrive in rapid succession).
			// Retry briefly to let federation state catch up.
			let mut auth_result =
				select_authorising_user(&services, &body.room_id, &body.user_id, &allowed_rooms)
					.await;

			if auth_result.is_err() {
				for _ in 0..5 {
					tokio::time::sleep(std::time::Duration::from_millis(150)).await;
					auth_result = select_authorising_user(
						&services,
						&body.room_id,
						&body.user_id,
						&allowed_rooms,
					)
					.await;
					if auth_result.is_ok() {
						break;
					}
				}
			}

			Some(auth_result?)
		} else {
			None
		}
	};
	if services.antispam.check_all_joins() && join_authorized_via_users_server.is_none() {
		if services
			.antispam
			.meowlnir_accept_make_join(body.room_id.clone(), body.user_id.clone())
			.await
			.is_err()
		{
			return Err!(Request(Forbidden("Antispam rejected join request.")));
		}
	}

	info!("Dropping state lock for room {}", body.room_id);
	drop(state_lock);

	let event = super::utils::build_membership_template_pdu(
		&services,
		&body.room_id,
		&body.user_id,
		RoomMemberEventContent {
			join_authorized_via_users_server,
			..RoomMemberEventContent::new(MembershipState::Join)
		},
	)
	.await?;

	Ok(prepare_join_event::v1::Response {
		room_version: Some(room_version_id),
		event,
	})
}

/// Attempts to find a user who is able to issue an invite in the target room.
/// Per spec, the authorising user must be in both the restricted room AND at
/// least one of the allowed rooms (from the join rules).
pub(crate) async fn select_authorising_user(
	services: &Services,
	room_id: &RoomId,
	target_user: &UserId,
	allowed_rooms: &[OwnedRoomId],
) -> Result<OwnedUserId> {
	if services
		.rooms
		.state_accessor
		.get_member(room_id, target_user)
		.await
		.is_ok_and(|member| member.membership == MembershipState::Ban)
	{
		return Err!(Request(UnableToGrantJoin("Joining user is banned from this room.")));
	}

	let local_members: Vec<_> = services
		.rooms
		.state_cache
		.local_users_in_room(room_id)
		.map(ToOwned::to_owned)
		.collect()
		.await;

	// Snapshot the room state once for all PL checks. State could change
	// mid-loop via federation, but re-fetching per-member is wasteful and
	// the old code had the same race.
	let state = conduwuit_service::rooms::auth_adapter::RoomStateProvider::new(
		room_id,
		&services.rooms.state_accessor,
	)
	.await?;

	for user in &local_members {
		// Must have invite power in the restricted room
		if !rezzy::auth::user::user_can_invite(user.as_str(), &state.provider, state.version) {
			continue;
		}

		// Must be in at least one of the allowed rooms
		let mut in_allowed = false;
		for allowed_room in allowed_rooms {
			if services
				.rooms
				.state_cache
				.is_joined(user, allowed_room)
				.await
			{
				in_allowed = true;
				break;
			}
		}

		if in_allowed {
			return Ok(user.clone());
		}

		warn!(
			"select_authorising_user: {user} can invite in {room_id} but is not in any allowed \
			 room — skipping"
		);
	}

	Err!(Request(UnableToGrantJoin(
		"No user on this server is able to assist in joining."
	)))
}

/// Checks whether the given user can join the given room via a restricted join.
/// Returns `Ok(Some(allowed_rooms))` if the user can perform the restricted
/// join, where `allowed_rooms` lists the rooms whose membership qualifies.
/// Returns `Ok(None)` if the room is not restricted.
pub(crate) async fn user_can_perform_restricted_join(
	services: &Services,
	user_id: &UserId,
	room_id: &RoomId,
	room_version_id: &RoomVersionId,
) -> Result<Option<Vec<OwnedRoomId>>> {
	use RoomVersionId::*;

	// restricted rooms are not supported on <=v7
	if matches!(room_version_id, V1 | V2 | V3 | V4 | V5 | V6 | V7) {
		// This should be impossible as it was checked earlier on, but retain this check
		// for safety.
		unreachable!("user_can_perform_restricted_join got incompatible room version");
	}

	let Ok(join_rules_event_content) = services
		.rooms
		.state_accessor
		.room_state_get_content::<RoomJoinRulesEventContent>(
			room_id,
			&StateEventType::RoomJoinRules,
			"",
		)
		.await
	else {
		// No join rules means there's nothing to authorise (defaults to invite)
		return Ok(None);
	};

	let (JoinRule::Restricted(r) | JoinRule::KnockRestricted(r)) =
		join_rules_event_content.join_rule
	else {
		// This is not a restricted room
		return Ok(None);
	};

	if r.allow.is_empty() {
		// This will never be authorisable, return forbidden.
		return Err!(Request(Forbidden("You are not invited to this room.")));
	}

	// Collect the full allowed room IDs for allow rules that need to return the
	// room set as-is (for example the antispam override path below).
	let allowed_rooms: Vec<OwnedRoomId> = r
		.allow
		.iter()
		.filter_map(|rule| match rule {
			| AllowRule::RoomMembership(m) => Some(m.room_id.clone()),
			| _ => None,
		})
		.collect();

	let mut valid_allowed_rooms = Vec::new();
	let mut could_satisfy = true;
	for allow_rule in &r.allow {
		match allow_rule {
			| AllowRule::RoomMembership(membership) => {
				if !services
					.rooms
					.state_cache
					.server_in_room(services.globals.server_name(), &membership.room_id)
					.await
				{
					// Since we can't check this room, mark could_satisfy as false
					// so that we can return M_UNABLE_TO_AUTHORIZE_JOIN later.
					could_satisfy = false;
					continue;
				}

				if services
					.rooms
					.state_cache
					.is_joined(user_id, &membership.room_id)
					.await
				{
					debug!(
						"User {} is allowed to join room {} via membership in room {}",
						user_id, room_id, membership.room_id
					);
					valid_allowed_rooms.push(membership.room_id.clone());
				}
			},
			| AllowRule::UnstableSpamChecker => {
				return match services
					.antispam
					.meowlnir_accept_make_join(room_id.to_owned(), user_id.to_owned())
					.await
				{
					| Ok(()) => Ok(Some(allowed_rooms.clone())),
					| Err(_) => Err!(Request(Forbidden("Antispam rejected join request."))),
				};
			},
			| _ => {
				// We don't recognise this join rule, so we cannot satisfy the request.
				could_satisfy = false;
				debug_info!(
					"Unsupported allow rule in restricted join for room {}: {:?}",
					room_id,
					allow_rule
				);
			},
		}
	}

	if !valid_allowed_rooms.is_empty() {
		return Ok(Some(valid_allowed_rooms));
	}

	if could_satisfy {
		// We were able to check all the restrictions and can be certain that the
		// prospective member is not permitted to join.
		Err!(Request(Forbidden(
			"You do not belong to any of the rooms or spaces required to join this room."
		)))
	} else {
		// We were unable to check all the restrictions. This usually means we aren't in
		// one of the rooms this one is restricted to, ergo can't check its state for
		// the user's membership, and consequently the user *might* be able to join if
		// they ask another server.
		Err!(Request(UnableToAuthorizeJoin(
			"You do not belong to any of the recognised rooms or spaces required to join this \
			 room, but this server is unable to verify every requirement. You may be able to \
			 join via another server."
		)))
	}
}

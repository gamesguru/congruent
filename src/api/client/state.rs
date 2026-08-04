#[cfg(test)]
mod tests;
use axum::{extract::State, response::IntoResponse};
use axum_client_ip::ClientIp;
use conduwuit::{
	Err, Result, RoomVersion, err, info,
	matrix::{Event, pdu::PduBuilder},
	utils::BoolExt,
};
use conduwuit_service::Services;
use futures::{FutureExt, StreamExt, TryStreamExt};
use ruma::{
	MilliSecondsSinceUnixEpoch, OwnedEventId, RoomId, UserId,
	api::client::state::{get_state_events, get_state_events_for_key, send_state_event},
	events::{
		AnyStateEventContent, StateEventType,
		room::{
			canonical_alias::RoomCanonicalAliasEventContent,
			create::RoomCreateEventContent,
			history_visibility::{HistoryVisibility, RoomHistoryVisibilityEventContent},
			join_rules::{JoinRule, RoomJoinRulesEventContent},
			member::{MembershipState, RoomMemberEventContent},
			server_acl::RoomServerAclEventContent,
		},
	},
	serde::Raw,
};
use serde_json::json;

use crate::{Ruma, RumaResponse};

/// # `PUT /_matrix/client/*/rooms/{roomId}/state/{eventType}/{stateKey}`
///
/// Sends a state event into the room.
pub(crate) async fn send_state_event_for_key_route(
	State(services): State<crate::State>,
	ClientIp(ip): ClientIp,
	body: Ruma<send_state_event::v3::Request>,
) -> Result<axum::response::Response> {
	let sender_user = body.sender_user();
	services
		.users
		.update_device_last_seen(sender_user, body.sender_device.as_deref(), ip)
		.await;

	if services.users.is_suspended(sender_user).await? {
		return Err!(Request(UserSuspended("You cannot perform this action while suspended.")));
	}

	if let Some(delay) = body.delay {
		if std::time::SystemTime::now().checked_add(delay).is_none() {
			return Err!(Request(InvalidParam("org.matrix.msc4140.delay is too large.")));
		}
		let event = conduwuit_service::rooms::delayed_events::ScheduledDelayedEvent {
			event_type: body.event_type.clone().into(),
			state_key: Some(body.state_key.clone()),
			content: body.body.body.cast_ref().clone(),
			user_id: sender_user.to_owned(),
			room_id: body.room_id.clone(),
			running_since: std::time::SystemTime::now(),
			delay,
		};
		let delay_id = services
			.rooms
			.delayed_events
			.queue_delayed_event(event)
			.await?;

		return Ok(axum::Json(serde_json::json!({
			"delay_id": delay_id,
		}))
		.into_response());
	}

	let event_id = send_state_event_for_key_helper(
		&services,
		sender_user,
		&body.room_id,
		&body.event_type,
		&body.body.body,
		&body.state_key,
		if body.appservice_info.is_some() {
			body.timestamp
		} else {
			None
		},
	)
	.boxed()
	.await?;

	Ok(RumaResponse(send_state_event::v3::Response { event_id }).into_response())
}

/// # `PUT /_matrix/client/*/rooms/{roomId}/state/{eventType}`
///
/// Sends a state event into the room.
pub(crate) async fn send_state_event_for_empty_key_route(
	State(services): State<crate::State>,
	ClientIp(ip): ClientIp,
	body: Ruma<send_state_event::v3::Request>,
) -> Result<axum::response::Response> {
	send_state_event_for_key_route(State(services), ClientIp(ip), body).await
}

/// # `GET /_matrix/client/v3/rooms/{roomid}/state`
///
/// Get all state events for a room.
///
/// - If not joined: Only works if current room history visibility is world
///   readable
pub(crate) async fn get_state_events_route(
	State(services): State<crate::State>,
	body: Ruma<get_state_events::v3::Request>,
) -> Result<get_state_events::v3::Response> {
	let sender_user = body.sender_user();
	let room_id = &body.room_id;

	let is_joined = services
		.rooms
		.state_cache
		.is_joined(sender_user, room_id)
		.await;

	if !services
		.rooms
		.state_cache
		.can_access_history(sender_user, room_id)
		.await
	{
		return Err!(Request(Forbidden("You don't have permission to view the room state.")));
	}

	// For departed users, serve state frozen at the point they left
	let shortstatehash = if !is_joined {
		let ssh = leave_shortstatehash(&services, sender_user, room_id).await;
		info!(
			target: "membership_debug",
			"/state: departed user {sender_user} in {room_id}, leave_ssh={ssh:?}"
		);
		ssh
	} else {
		None
	};

	let room_state: Vec<_> = if let Some(ssh) = shortstatehash {
		services
			.rooms
			.state_accessor
			.state_full_pdus(ssh)
			.map(Event::into_format)
			.collect()
			.await
	} else {
		services
			.rooms
			.state_accessor
			.room_state_full_pdus(room_id)
			.map_ok(Event::into_format)
			.try_collect()
			.await?
	};

	Ok(get_state_events::v3::Response { room_state })
}

/// # `GET /_matrix/client/v3/rooms/{roomid}/state/{eventType}/{stateKey}`
///
/// Get single state event of a room with the specified state key.
/// The optional query parameter `?format=event|content` allows returning the
/// full room state event or just the state event's content (default behaviour)
///
/// - If not joined: Only works if current room history visibility is world
///   readable
pub(crate) async fn get_state_events_for_key_route(
	State(services): State<crate::State>,
	body: Ruma<get_state_events_for_key::v3::Request>,
) -> Result<get_state_events_for_key::v3::Response> {
	let sender_user = body.sender_user();
	let room_id = &body.room_id;

	let is_joined = services
		.rooms
		.state_cache
		.is_joined(sender_user, room_id)
		.await;

	if !services
		.rooms
		.state_cache
		.can_access_history(sender_user, room_id)
		.await
	{
		return Err!(Request(NotFound(debug_warn!(
			"You don't have permission to view the room state."
		))));
	}

	// For departed users, look up state from the snapshot at departure
	let event = if !is_joined {
		if let Some(ssh) = leave_shortstatehash(&services, sender_user, room_id).await {
			info!(
				target: "membership_debug",
				"/state/{}: departed user {sender_user} in {room_id}, using leave_ssh={ssh}",
				body.event_type
			);
			services
				.rooms
				.state_accessor
				.state_get(ssh, &body.event_type, &body.state_key)
				.await
		} else {
			services
				.rooms
				.state_accessor
				.room_state_get(room_id, &body.event_type, &body.state_key)
				.await
		}
	} else {
		services
			.rooms
			.state_accessor
			.room_state_get(room_id, &body.event_type, &body.state_key)
			.await
	}
	.map_err(|_| {
		err!(Request(NotFound(debug_warn!(
				room_id = %body.room_id,
				event_type = %body.event_type,
				"State event not found in room.",
		))))
	})?;

	let event_format = body
		.format
		.as_ref()
		.is_some_and(|f| f.to_lowercase().eq("event"));

	Ok(get_state_events_for_key::v3::Response {
		content: event_format.or(|| event.get_content_as_value()),
		event: event_format.then(|| {
			json!({
				"content": event.content(),
				"event_id": event.event_id(),
				"origin_server_ts": event.origin_server_ts(),
				"room_id": event.room_id_or_hash(),
				"sender": event.sender(),
				"state_key": event.state_key(),
				"type": event.kind(),
				"unsigned": event.unsigned(),
			})
		}),
	})
}

/// # `GET /_matrix/client/v3/rooms/{roomid}/state/{eventType}`
///
/// Get single state event of a room.
/// The optional query parameter `?format=event|content` allows returning the
/// full room state event or just the state event's content (default behaviour)
///
/// - If not joined: Only works if current room history visibility is world
///   readable
pub(crate) async fn get_state_events_for_empty_key_route(
	State(services): State<crate::State>,
	body: Ruma<get_state_events_for_key::v3::Request>,
) -> Result<RumaResponse<get_state_events_for_key::v3::Response>> {
	get_state_events_for_key_route(State(services), body)
		.await
		.map(RumaResponse)
}

/// Get the shortstatehash for the state snapshot at the point when a user
/// departed (left/banned) from a room. Returns None if the leave event
/// can't be found or has no associated state snapshot.
async fn leave_shortstatehash(
	services: &Services,
	user_id: &UserId,
	room_id: &RoomId,
) -> Option<u64> {
	let leave_pdu = services
		.rooms
		.state_cache
		.left_state(user_id, room_id)
		.await
		.ok()
		.flatten()?;

	services
		.rooms
		.state_accessor
		.pdu_shortstatehash(leave_pdu.event_id())
		.await
		.ok()
}

async fn send_state_event_for_key_helper(
	services: &Services,
	sender: &UserId,
	room_id: &RoomId,
	event_type: &StateEventType,
	json: &Raw<AnyStateEventContent>,
	state_key: &str,
	timestamp: Option<MilliSecondsSinceUnixEpoch>,
) -> Result<OwnedEventId> {
	let json: &mut Raw<AnyStateEventContent> = &mut json.clone();
	allowed_to_send_state_event(services, room_id, event_type, state_key, json).await?;
	let state_lock = services.rooms.state.mutex.lock(room_id).await;

	if let Ok(existing_event) = services
		.rooms
		.state_accessor
		.room_state_get(room_id, event_type, state_key)
		.await
	{
		if existing_event.sender() == sender {
			if let Ok(existing_content) =
				serde_json::from_str::<serde_json::Value>(existing_event.content().get())
			{
				if let Ok(new_content) =
					serde_json::from_str::<serde_json::Value>(json.json().get())
				{
					if existing_content == new_content {
						return Ok(existing_event.event_id().into());
					}
				}
			}
		}
	}

	let event_id = services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder {
				event_type: event_type.to_string().into(),
				content: serde_json::from_str(json.json().get())?,
				state_key: Some(state_key.into()),
				timestamp,
				..Default::default()
			},
			sender,
			Some(room_id),
			&state_lock,
		)
		.await?;

	Ok(event_id)
}

async fn allowed_to_send_state_event(
	services: &Services,
	room_id: &RoomId,
	event_type: &StateEventType,
	state_key: &str,
	json: &mut Raw<AnyStateEventContent>,
) -> Result {
	match event_type {
		| StateEventType::RoomCreate => {
			return Err!(Request(BadJson(debug_warn!(
				%room_id,
				"You cannot update m.room.create after a room has been created."
			))));
		},
		| StateEventType::RoomServerAcl => {
			// prevents common ACL paw-guns as ACL management is difficult and prone to
			// irreversible mistakes
			match json.deserialize_as::<RoomServerAclEventContent>() {
				| Ok(acl_content) => {
					if acl_content.allow_is_empty() {
						return Err!(Request(BadJson(debug_warn!(
							%room_id,
							"Sending an ACL event with an empty allow key will permanently \
							 brick the room for non-conduwuit's as this equates to no servers \
							 being allowed to participate in this room."
						))));
					}

					if acl_content.deny_contains("*") && acl_content.allow_contains("*") {
						return Err!(Request(BadJson(debug_warn!(
							%room_id,
							"Sending an ACL event with a deny and allow key value of \"*\" will \
							 permanently brick the room for non-conduwuit's as this equates to \
							 no servers being allowed to participate in this room."
						))));
					}

					if acl_content.deny_contains("*")
						&& !acl_content.is_allowed(services.globals.server_name())
						&& !acl_content.allow_contains(services.globals.server_name().as_str())
					{
						return Err!(Request(BadJson(debug_warn!(
							%room_id,
							"Sending an ACL event with a deny key value of \"*\" and without \
							 your own server name in the allow key will result in you being \
							 unable to participate in this room."
						))));
					}

					if !acl_content.allow_contains("*")
						&& !acl_content.is_allowed(services.globals.server_name())
						&& !acl_content.allow_contains(services.globals.server_name().as_str())
					{
						return Err!(Request(BadJson(debug_warn!(
							%room_id,
							"Sending an ACL event for an allow key without \"*\" and without \
							 your own server name in the allow key will result in you being \
							 unable to participate in this room."
						))));
					}
				},
				| Err(e) => {
					return Err!(Request(BadJson(debug_warn!(
						"Room server ACL event is invalid: {e}"
					))));
				},
			}
		},
		| StateEventType::RoomEncryption =>
		// Forbid m.room.encryption if encryption is disabled
			if !services.config.allow_encryption {
				return Err!(Request(Forbidden("Encryption is disabled on this homeserver.")));
			},
		| StateEventType::RoomJoinRules => {
			// admin room is a sensitive room, it should not ever be made public
			if let Ok(admin_room_id) = services.admin.get_admin_room().await {
				if admin_room_id == room_id {
					match json.deserialize_as::<RoomJoinRulesEventContent>() {
						| Ok(join_rule) =>
							if join_rule.join_rule == JoinRule::Public {
								return Err!(Request(Forbidden(
									"Admin room is a sensitive room, it cannot be made public"
								)));
							},
						| Err(e) => {
							return Err!(Request(BadJson(debug_warn!(
								"Room join rules event is invalid: {e}"
							))));
						},
					}
				}
			}
		},
		| StateEventType::RoomHistoryVisibility => {
			// admin room is a sensitive room, it should not ever be made world readable
			if let Ok(admin_room_id) = services.admin.get_admin_room().await {
				match json.deserialize_as::<RoomHistoryVisibilityEventContent>() {
					| Ok(visibility_content) => {
						if admin_room_id == room_id
							&& visibility_content.history_visibility
								== HistoryVisibility::WorldReadable
						{
							return Err!(Request(Forbidden(
								"Admin room is a sensitive room, it cannot be made world \
								 readable (public room history)."
							)));
						}
					},
					| Err(e) => {
						return Err!(Request(BadJson(debug_warn!(
							"Room history visibility event is invalid: {e}"
						))));
					},
				}
			}
		},
		| StateEventType::RoomCanonicalAlias => {
			match json.deserialize_as::<RoomCanonicalAliasEventContent>() {
				| Ok(canonical_alias_content) => {
					let mut aliases = canonical_alias_content.alt_aliases.clone();

					if let Some(alias) = canonical_alias_content.alias {
						aliases.push(alias);
					}

					for alias in aliases {
						let Ok((alias_room_id, _)) =
							services.rooms.alias.resolve_alias(&alias).await
						else {
							return Err!(Request(BadAlias(debug_warn!(
								"Failed resolving alias \"{alias}\"."
							))));
						};

						if alias_room_id != room_id {
							return Err!(Request(BadAlias(debug_warn!(
								"Room alias {alias} does not belong to room {room_id}"
							))));
						}
					}
				},
				| Err(e) => {
					return Err!(Request(InvalidParam(debug_warn!(
						"Room canonical alias event is invalid: {e}"
					))));
				},
			}
		},
		| StateEventType::RoomMember => {
			// Try strict deserialization first; if it fails due to an invalid
			// join_authorised_via_users_server value (e.g. "unused"), strip
			// the field and retry — the spec says servers should ignore this
			// field on join->join transitions.
			let membership_result = json.deserialize_as::<RoomMemberEventContent>();
			let mut membership_content = match membership_result {
				| Ok(content) => content,
				| Err(e) => {
					let is_join_to_join = match UserId::parse(state_key) {
						| Ok(uid) => services.rooms.state_cache.is_joined(uid, room_id).await,
						| Err(_) => false,
					};

					if !is_join_to_join {
						return Err!(Request(BadJson(
							"Membership content must have a valid JSON body with at least a \
							 valid membership state: {e}"
						)));
					}

					// Attempt lenient parse: strip the offending field and retry
					let mut raw_value: serde_json::Value =
						serde_json::from_str(json.json().get()).map_err(|err| {
							err!(Request(BadJson(
								"Membership content must have a valid JSON body with at least a \
								 valid membership state: {err}"
							)))
						})?;

					if let Some(obj) = raw_value.as_object_mut() {
						obj.remove("join_authorised_via_users_server");
					}

					let content =
						serde_json::from_value::<RoomMemberEventContent>(raw_value.clone())
							.map_err(|err| {
								err!(Request(BadJson(
									"Membership content must have a valid JSON body with at \
									 least a valid membership state: {err}"
								)))
							})?;

					*json = Raw::<AnyStateEventContent>::from_json_string(serde_json::to_string(
						&raw_value,
					)?)
					.unwrap();
					content
				},
			};

			{
				let Ok(state_key) = UserId::parse(state_key) else {
					return Err!(Request(BadJson(
						"Membership event has invalid or non-existent state key"
					)));
				};

				if let Some(authorising_user) =
					membership_content.join_authorized_via_users_server
				{
					// join_authorized_via_users_server must be thrown away, if user is already a
					// member of the room.
					if services
						.rooms
						.state_cache
						.is_joined(state_key, room_id)
						.await
					{
						membership_content.join_authorized_via_users_server = None;
						*json = Raw::<AnyStateEventContent>::from_json_string(
							serde_json::to_string(&membership_content)?,
						)?;
						return Ok(());
					}

					if membership_content.membership != MembershipState::Join {
						return Err!(Request(BadJson(
							"join_authorised_via_users_server is only for member joins"
						)));
					}

					if !services.globals.user_is_local(&authorising_user) {
						return Err!(Request(InvalidParam(
							"Authorising user {authorising_user} does not belong to this \
							 homeserver"
						)));
					}

					if !services
						.rooms
						.state_cache
						.is_joined(&authorising_user, room_id)
						.await
					{
						return Err!(Request(InvalidParam(
							"Authorising user {authorising_user} is not in the room, they \
							 cannot authorise the join."
						)));
					}
				}
			}
		},
		| StateEventType::RoomPowerLevels => {
			// In v12 rooms, creators must not appear in the power levels users map
			let room_create = services
				.rooms
				.state_accessor
				.room_state_get(room_id, &StateEventType::RoomCreate, "")
				.await;
			if let Ok(room_create) = room_create {
				if let Ok(create_content) =
					serde_json::from_str::<RoomCreateEventContent>(room_create.content().get())
				{
					let room_features = RoomVersion::new(&create_content.room_version);
					if let Ok(room_features) = room_features {
						if room_features.explicitly_privilege_room_creators
							&& let Ok(mut pl_content) = json.deserialize_as::<serde_json::Value>()
							&& let Some(pl_obj) = pl_content.as_object_mut()
						{
							let mut creators = vec![room_create.sender().as_str().to_owned()];
							if let Some(additional) = &create_content.additional_creators {
								creators.extend(additional.iter().map(ToString::to_string));
							}

							let removed_creator = pl_obj
								.get_mut("users")
								.and_then(|u| u.as_object_mut())
								.is_some_and(|users| {
									creators
										.iter()
										.fold(false, |removed, creator| {
											users.remove(creator).is_some() || removed
										})
								});

							if removed_creator {
								let users_empty = pl_obj
									.get("users")
									.and_then(|u| u.as_object())
									.is_none_or(serde_json::Map::is_empty);
								let has_non_users_content =
									pl_obj.keys().any(|key| key != "users");

								if users_empty && !has_non_users_content {
									return Err!(Request(BadJson(
										"Room creator cannot be set in power levels users map"
									)));
								}

								*json = Raw::<AnyStateEventContent>::from_json_string(
									serde_json::to_string(&pl_content)?,
								)?;
							}
						}
					}
				}
			}
		},
		| _ => (),
	}

	Ok(())
}

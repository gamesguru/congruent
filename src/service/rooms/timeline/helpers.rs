//! Helpers for submitting events with the right checks performed

use std::collections::BTreeMap;

use conduwuit::{Err, Result, err, implement, matrix::pdu::PduBuilder};
use ruma::{
	MilliSecondsSinceUnixEpoch, OwnedEventId, RoomId, TransactionId, UserId,
	events::{
		AnyMessageLikeEventContent, AnyStateEventContent, MessageLikeEventType, StateEventType,
		room::{
			canonical_alias::RoomCanonicalAliasEventContent,
			history_visibility::{HistoryVisibility, RoomHistoryVisibilityEventContent},
			join_rules::{JoinRule, RoomJoinRulesEventContent},
			member::{MembershipState, RoomMemberEventContent},
			server_acl::RoomServerAclEventContent,
		},
	},
	serde::Raw,
};
use serde_json::Value;

use crate::rooms::state::RoomMutexGuard;

#[implement(super::Service)]
#[allow(clippy::too_many_arguments)]
pub async fn send_message_event_helper(
	&self,
	sender: &UserId,
	room_id: &RoomId,
	state_lock: &RoomMutexGuard,
	event_type: &MessageLikeEventType,
	content: &Raw<AnyMessageLikeEventContent>,
	txn_id: Option<&TransactionId>,
	timestamp: Option<MilliSecondsSinceUnixEpoch>,
	mut unsigned: Option<BTreeMap<String, Value>>,
) -> Result<OwnedEventId> {
	// let json: &mut Raw<AnyMessageLikeEventContent> = &mut json.clone();
	self.allowed_to_send_message_event(room_id, event_type)
		.await?;

	let content = serde_json::from_str(content.json().get())
		.map_err(|e| err!(Request(BadJson("Invalid JSON body: {e}"))))?;

	if let Some(txn_id) = txn_id {
		unsigned
			.get_or_insert_default()
			.insert("transaction_id".to_owned(), txn_id.to_string().into());
	}

	let event_id = self
		.build_and_append_pdu(
			PduBuilder {
				event_type: event_type.to_string().into(),
				content,
				state_key: None,
				redacts: None,
				timestamp,
				unsigned,
			},
			sender,
			Some(room_id),
			state_lock,
		)
		.await?;

	Ok(event_id)
}

#[implement(super::Service)]
async fn allowed_to_send_message_event(
	&self,
	room_id: &RoomId,
	event_type: &MessageLikeEventType,
) -> Result {
	if *event_type == MessageLikeEventType::CallInvite
		&& self.services.directory.is_public_room(room_id).await
	{
		return Err!(Request(Forbidden("Room call invites are not allowed in public rooms")));
	}

	// Forbid m.room.encrypted if encryption is disabled
	if MessageLikeEventType::RoomEncrypted == *event_type
		&& !self.services.server.config.allow_encryption
	{
		return Err!(Request(Forbidden("Encryption has been disabled")));
	}

	Ok(())
}

#[implement(super::Service)]
#[allow(clippy::too_many_arguments)]
pub async fn send_state_event_for_key_helper(
	&self,
	sender: &UserId,
	room_id: &RoomId,
	state_lock: &RoomMutexGuard,
	event_type: &StateEventType,
	content: &Raw<AnyStateEventContent>,
	state_key: &str,
	timestamp: Option<MilliSecondsSinceUnixEpoch>,
	unsigned: Option<BTreeMap<String, Value>>,
) -> Result<OwnedEventId> {
	let mut content: Raw<AnyStateEventContent> = content.clone();
	self.allowed_to_send_state_event(room_id, event_type, state_key, &mut content)
		.await?;

	let content = serde_json::from_str(content.json().get())
		.map_err(|e| err!(Request(BadJson("Invalid JSON body: {e}"))))?;

	let event_id = self
		.build_and_append_pdu(
			PduBuilder {
				event_type: event_type.to_string().into(),
				content,
				state_key: Some(state_key.into()),
				redacts: None,
				timestamp,
				unsigned,
			},
			sender,
			Some(room_id),
			state_lock,
		)
		.await?;

	Ok(event_id)
}

#[implement(super::Service)]
async fn allowed_to_send_state_event(
	&self,
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
					let allow_has_wildcard = acl_content.allow.iter().any(|entry| entry == "*");
					let deny_has_wildcard = acl_content.deny.iter().any(|entry| entry == "*");
					let allow_has_server = acl_content
						.allow
						.iter()
						.any(|entry| entry == self.services.globals.server_name().as_str());

					if acl_content.allow.is_empty() {
						return Err!(Request(BadJson(debug_warn!(
							%room_id,
							"Sending an ACL event with an empty allow key will permanently \
							 brick the room for non-conduwuit's as this equates to no servers \
							 being allowed to participate in this room."
						))));
					}

					if allow_has_wildcard && deny_has_wildcard {
						return Err!(Request(BadJson(debug_warn!(
							%room_id,
							"Sending an ACL event with a deny and allow key value of \"*\" will \
							 permanently brick the room for non-conduwuit's as this equates to \
							 no servers being allowed to participate in this room."
						))));
					}

					if deny_has_wildcard
						&& !acl_content.is_allowed(self.services.globals.server_name())
						&& !allow_has_server
					{
						return Err!(Request(BadJson(debug_warn!(
							%room_id,
							"Sending an ACL event with a deny key value of \"*\" and without \
							 your own server name in the allow key will result in you being \
							 unable to participate in this room."
						))));
					}

					if !allow_has_wildcard
						&& !acl_content.is_allowed(self.services.globals.server_name())
						&& !allow_has_server
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
			if !self.services.server.config.allow_encryption {
				return Err!(Request(Forbidden("Encryption is disabled on this homeserver.")));
			},
		| StateEventType::RoomJoinRules => {
			// admin room is a sensitive room, it should not ever be made public
			if let Ok(admin_room_id) = self.services.admin.get_admin_room().await {
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
			if let Ok(admin_room_id) = self.services.admin.get_admin_room().await {
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

					for alias in &aliases {
						let (alias_room_id, _) = self
							.services
							.alias
							.resolve_alias(alias)
							.await
							.map_err(|e| {
							err!(Request(BadAlias(
								"Failed resolving alias \"{alias}\": {e}",
								alias = alias.as_str()
							)))
						})?;

						if alias_room_id != room_id {
							return Err!(Request(BadAlias(
								"Room alias {alias} does not belong to room {room_id}",
								alias = alias.as_str()
							)));
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
		| StateEventType::RoomMember => match json.deserialize_as::<RoomMemberEventContent>() {
			| Ok(mut membership_content) => {
				let Ok(state_key) = UserId::parse(state_key) else {
					return Err!(Request(BadJson(
						"Membership event has invalid or non-existent state key"
					)));
				};

				if let Some(authorising_user) =
					membership_content.join_authorized_via_users_server
				{
					// join_authorized_via_users_server must be thrown away, if user is
					// already a member of the room.
					if self
						.services
						.state_cache
						.is_joined(state_key, room_id)
						.await && membership_content.membership == MembershipState::Join
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

					if !self.services.globals.user_is_local(&authorising_user) {
						return Err!(Request(InvalidParam(
							"Authorising user {authorising_user} does not belong to this \
							 homeserver",
							authorising_user = authorising_user.as_str()
						)));
					}

					if !self
						.services
						.state_cache
						.is_joined(&authorising_user, room_id)
						.await
					{
						return Err!(Request(InvalidParam(
							"Authorising user {authorising_user} is not in the room, they \
							 cannot authorise the join.",
							authorising_user = authorising_user.as_str()
						)));
					}
				}
			},
			| Err(e) => {
				return Err!(Request(BadJson(
					"Membership content must have a valid JSON body with at least a valid \
					 membership state: {e}"
				)));
			},
		},
		| _ => (),
	}

	Ok(())
}

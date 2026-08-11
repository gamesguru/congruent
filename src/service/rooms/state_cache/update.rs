use std::collections::HashSet;

use conduwuit::{Err, Event, Pdu, Result, implement, info, is_not_empty, utils::ReadyExt};
use database::{Json, serialize_key};
use futures::StreamExt;
use ruma::{
	OwnedServerName, OwnedUserId, RoomId, UserId,
	events::{
		AnyStrippedStateEvent, GlobalAccountDataEventType, RoomAccountDataEventType,
		StateEventType,
		direct::DirectEvent,
		invite_permission_config::FilterLevel,
		room::{
			create::RoomCreateEventContent,
			member::{MembershipState, RoomMemberEventContent},
		},
	},
	serde::Raw,
};

/// Update current membership data.
#[implement(super::Service)]
#[tracing::instrument(
		level = "debug",
		skip_all,
		fields(
			%room_id,
			%user_id,
			?pdu,
		),
	)]
#[allow(clippy::too_many_arguments)]
pub async fn update_membership(
	&self,
	room_id: &RoomId,
	user_id: &UserId,
	pdu: &Pdu,
	update_joined_count: bool,
) -> Result {
	let membership = pdu.get_content::<RoomMemberEventContent>()?;

	// Keep track what remote users exist by adding them as "deactivated" users
	//
	// TODO: use futures to update remote profiles without blocking the membership
	// update
	#[allow(clippy::collapsible_if)]
	if !self.services.globals.user_is_local(user_id) {
		if !self.services.users.exists(user_id).await {
			self.services.users.create(user_id, None, None).await?;
		}
	}

	match &membership.membership {
		| MembershipState::Join => {
			// Check if the user never joined this room
			if !self.once_joined(user_id, room_id).await {
				// Add the user ID to the join list then
				self.mark_as_once_joined(user_id, room_id);

				// Check if the room has a predecessor
				if let Ok(Some(predecessor)) = self
					.services
					.state_accessor
					.room_state_get_content(room_id, &StateEventType::RoomCreate, "")
					.await
					.map(|content: RoomCreateEventContent| content.predecessor)
				{
					// Copy old tags to new room
					if let Ok(tag_event) = self
						.services
						.account_data
						.get_room(&predecessor.room_id, user_id, RoomAccountDataEventType::Tag)
						.await
					{
						self.services
							.account_data
							.update(
								Some(room_id),
								user_id,
								RoomAccountDataEventType::Tag,
								&tag_event,
							)
							.await
							.ok();
					}

					// Copy direct chat flag
					if let Ok(mut direct_event) = self
						.services
						.account_data
						.get_global::<DirectEvent>(user_id, GlobalAccountDataEventType::Direct)
						.await
					{
						let mut room_ids_updated = false;
						for room_ids in direct_event.content.0.values_mut() {
							if room_ids.iter().any(|r| r == &predecessor.room_id) {
								room_ids.push(room_id.to_owned());
								room_ids_updated = true;
							}
						}

						if room_ids_updated {
							self.services
								.account_data
								.update(
									None,
									user_id,
									GlobalAccountDataEventType::Direct.to_string().into(),
									&serde_json::to_value(&direct_event)
										.expect("to json always works"),
								)
								.await?;
						}
					}
				}
			}

			self.mark_as_joined(user_id, room_id).await;
		},
		| MembershipState::Invite => {
			let last_state = self.services.state.summary_stripped(pdu, room_id).await;
			self.mark_as_invited(user_id, room_id, pdu.sender(), Some(last_state), None)
				.await?;
		},
		| MembershipState::Leave | MembershipState::Ban => {
			self.mark_as_left(user_id, room_id, Some(pdu.clone())).await;
		},
		| MembershipState::Knock => {
			let last_state = self.services.state.summary_stripped(pdu, room_id).await;
			self.mark_as_knocked(user_id, room_id, Some(last_state));
		},
		| _ => {},
	}

	if update_joined_count {
		self.update_joined_count(room_id).await;
	}

	Ok(())
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
pub async fn update_joined_count(&self, room_id: &RoomId) {
	let mut joinedcount = 0_u64;
	let mut invitedcount = 0_u64;
	let mut joined_servers = HashSet::new();

	self.room_members(room_id)
		.ready_for_each(|joined| {
			joined_servers.insert(joined.server_name().to_owned());
			joinedcount = joinedcount.saturating_add(1);
		})
		.await;

	invitedcount = invitedcount.saturating_add(
		self.room_members_invited(room_id)
			.count()
			.await
			.try_into()
			.unwrap_or(0),
	);

	self.db.roomid_joinedcount.raw_put(room_id, joinedcount);
	self.db.roomid_invitedcount.raw_put(room_id, invitedcount);

	info!(
		"update_joined_count: room={room_id} joined={joinedcount} invited={invitedcount} \
		 n_servers={}",
		joined_servers.len()
	);

	let mut removed_servers = Vec::new();
	self.room_servers(room_id)
		.ready_for_each(|old_joined_server| {
			if joined_servers.remove(old_joined_server) {
				return;
			}

			removed_servers.push(old_joined_server.to_owned());
			// Server not in room anymore
			let roomserver_id = (room_id, old_joined_server);
			let serverroom_id = (old_joined_server, room_id);

			self.db.roomserverids.del(roomserver_id);
			self.db.serverroomids.del(serverroom_id);
		})
		.await;

	if joinedcount > 100 {
		if !removed_servers.is_empty() || !joined_servers.is_empty() {
			self.server_visibility_cache.invalidate_all();
		}

		for server in &joined_servers {
			let roomserver_id = (room_id, server);
			let serverroom_id = (server, room_id);

			self.db.roomserverids.put_raw(roomserver_id, []);
			self.db.serverroomids.put_raw(serverroom_id, []);
		}
	} else {
		for removed_server in removed_servers {
			self.room_members(room_id)
				.ready_for_each(|user_id| {
					self.server_visibility_cache
						.invalidate(&(removed_server.clone(), user_id.to_owned()));
				})
				.await;
		}

		// Now only new servers are in joined_servers anymore
		for server in &joined_servers {
			let roomserver_id = (room_id, server);
			let serverroom_id = (server, room_id);

			self.db.roomserverids.put_raw(roomserver_id, []);
			self.db.serverroomids.put_raw(serverroom_id, []);

			self.room_members(room_id)
				.ready_for_each(|user_id| {
					self.server_visibility_cache
						.invalidate(&(server.clone(), user_id.to_owned()));
				})
				.await;
		}
	}

	self.appservice_in_room_cache.write().remove(room_id);
}

/// Which `userroomid_*`/`roomuserid_*` table currently holds a user's
/// membership in a room. Used by `set_other_membership_states` to clear
/// the tables that don't apply once one of these becomes current.
#[derive(Clone, Copy, PartialEq, Eq)]
enum MembershipKind {
	Joined,
	Left,
	Knocked,
	Invited,
}

/// Clears the membership tables other than `keep`. This is the one place
/// the set/clear matrix lives -- `mark_as_joined[_silent]`,
/// `mark_as_left[_silent]`, `mark_as_knocked`, and `mark_as_invited` all
/// delegate here instead of hand-rolling their own copy of "clear the
/// other three," which is what let them drift in the first place (see
/// `c8a7dcd5c` and the `mark_as_left_silent` asymmetry it exposed: the
/// silent variant had its own copy of this block that never got the
/// `preserve_newer_invite` guard the non-silent one gained).
///
/// `preserve_invite` lets a leave-shaped caller keep the invite state
/// instead of clearing it (see `preserve_newer_invite` in `mark_as_left`
/// and the has-pending-invite check in `mark_as_left_silent`); it has no
/// effect when `keep` is already `Invited`.
#[implement(super::Service)]
fn set_other_membership_states(
	&self,
	userroom_id: &[u8],
	roomuser_id: &[u8],
	room_id: &RoomId,
	keep: MembershipKind,
	preserve_invite: bool,
) {
	if keep != MembershipKind::Joined {
		self.db.userroomid_joined.remove(userroom_id);
		self.db.roomuserid_joined.remove(roomuser_id);
	}

	if keep != MembershipKind::Invited && !preserve_invite {
		self.db.userroomid_invitestate.remove(userroom_id);
		self.db.roomuserid_invitecount.remove(roomuser_id);
		self.db.userroomid_invitesender.remove(userroom_id);
		self.db.roomid_inviteviaservers.remove(room_id);
	}

	if keep != MembershipKind::Left {
		self.db.userroomid_leftstate.remove(userroom_id);
		self.db.roomuserid_leftcount.remove(roomuser_id);
	}

	if keep != MembershipKind::Knocked {
		self.db.userroomid_knockedstate.remove(userroom_id);
		self.db.roomuserid_knockedcount.remove(roomuser_id);
	}
}

/// Direct DB function to directly mark a user as joined. It is not
/// recommended to use this directly. You most likely should use
/// `update_membership` instead
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn mark_as_joined(&self, user_id: &UserId, room_id: &RoomId) {
	tracing::info!(
		target: "knock_debug",
		"mark_as_joined called for user_id={} room_id={}",
		user_id,
		room_id
	);
	let userroom_id = (user_id, room_id);
	let userroom_id = serialize_key(userroom_id).expect("failed to serialize userroom_id");

	let roomuser_id = (room_id, user_id);
	let roomuser_id = serialize_key(roomuser_id).expect("failed to serialize roomuser_id");

	self.db.userroomid_joined.insert(&userroom_id, []);
	self.db.roomuserid_joined.insert(&roomuser_id, []);

	self.set_other_membership_states(
		&userroom_id,
		&roomuser_id,
		room_id,
		MembershipKind::Joined,
		false,
	);
	self.unforget(room_id, user_id);

	self.invalidate_user_visibility(user_id, room_id).await;
	self.invalidate_server_visibility(user_id, room_id).await;
}

/// Silent variant of `mark_as_joined` for admin healing operations.
/// Performs the exact same DB writes but does NOT trigger
/// `update_membership`, presence updates, or device list notifications.
/// The caller MUST call `update_joined_count` after the batch completes.
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn mark_as_joined_silent(&self, user_id: &UserId, room_id: &RoomId) {
	let userroom_id = (user_id, room_id);
	let userroom_id = serialize_key(userroom_id).expect("failed to serialize userroom_id");

	let roomuser_id = (room_id, user_id);
	let roomuser_id = serialize_key(roomuser_id).expect("failed to serialize roomuser_id");

	self.db.userroomid_joined.insert(&userroom_id, []);
	self.db.roomuserid_joined.insert(&roomuser_id, []);

	self.set_other_membership_states(
		&userroom_id,
		&roomuser_id,
		room_id,
		MembershipKind::Joined,
		false,
	);
	self.unforget(room_id, user_id);

	self.invalidate_user_visibility(user_id, room_id).await;
	self.invalidate_server_visibility(user_id, room_id).await;
}

/// Silent variant of `mark_as_invited` for admin healing operations.
/// Performs the invite-state DB writes without invite filtering, device list
/// notifications, or sync-side side effects beyond the raw membership tables.
/// The caller MUST call `update_joined_count` after the batch completes.
#[implement(super::Service)]
#[tracing::instrument(skip(self, last_state, sender_user, invite_via), level = "debug")]
pub async fn mark_as_invited_silent(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	last_state: Option<Vec<Raw<AnyStrippedStateEvent>>>,
	sender_user: Option<&UserId>,
	invite_via: Option<Vec<OwnedServerName>>,
) {
	let roomuser_id = (room_id, user_id);
	let roomuser_id = serialize_key(roomuser_id).expect("failed to serialize roomuser_id");

	let userroom_id = (user_id, room_id);
	let userroom_id = serialize_key(userroom_id).expect("failed to serialize userroom_id");

	self.db
		.userroomid_invitestate
		.raw_put(&userroom_id, Json(last_state.unwrap_or_default()));
	self.db
		.roomuserid_invitecount
		.raw_aput::<8, _, _>(&roomuser_id, self.services.globals.next_count().unwrap());
	if let Some(sender_user) = sender_user {
		self.db
			.userroomid_invitesender
			.insert(&userroom_id, sender_user);
	}

	self.set_other_membership_states(
		&userroom_id,
		&roomuser_id,
		room_id,
		MembershipKind::Invited,
		false,
	);
	self.unforget(room_id, user_id);

	if let Some(servers) = invite_via.filter(is_not_empty!()) {
		self.add_servers_invite_via(room_id, servers).await;
	}

	self.invalidate_user_visibility(user_id, room_id).await;
	self.invalidate_server_visibility(user_id, room_id).await;
}

/// Silent variant of `mark_as_left` for admin healing operations.
/// Does NOT trigger `update_membership`, presence updates, or device list
/// notifications. The caller MUST call `update_joined_count` after the
/// batch completes.
///
/// Unlike `mark_as_left`, there is no `leave_pdu` here (this path runs for
/// admin/reconcile-triggered removals with no real leave event), so there's
/// no `origin_server_ts` to compare a pending invite against. Rather than
/// guess, we conservatively preserve any existing invite unconditionally:
/// clearing it here previously reproduced the `c8a7dcd5c` "stale invite
/// clear" bug through `reconcile_membership`, which calls this whenever its
/// room-state snapshot doesn't (yet) reflect a just-landed invite.
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn mark_as_left_silent(&self, user_id: &UserId, room_id: &RoomId) {
	let userroom_id = (user_id, room_id);
	let userroom_id = serialize_key(userroom_id).expect("failed to serialize userroom_id");

	let roomuser_id = (room_id, user_id);
	let roomuser_id = serialize_key(roomuser_id).expect("failed to serialize roomuser_id");

	// Write left state with no PDU (admin operation, no actual leave event)
	self.db
		.userroomid_leftstate
		.raw_put(&userroom_id, Json(Option::<Pdu>::None));
	self.db
		.roomuserid_leftcount
		.raw_aput::<8, _, _>(&roomuser_id, self.services.globals.next_count().unwrap());

	let has_pending_invite = self.invite_state(user_id, room_id).await.is_ok();
	self.set_other_membership_states(
		&userroom_id,
		&roomuser_id,
		room_id,
		MembershipKind::Left,
		has_pending_invite,
	);

	self.invalidate_user_visibility(user_id, room_id).await;
	self.invalidate_server_visibility(user_id, room_id).await;
}

/// Mark a user as having left a room.
///
/// `leave_pdu` represents the m.room.member event which the user sent to leave
/// the room. If this is None, no event was actually sent, but we must still
/// behave as if the user is no longer in the room. This may occur, for example,
/// if the room being left has been server-banned by an administrator.
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn mark_as_left(&self, user_id: &UserId, room_id: &RoomId, leave_pdu: Option<Pdu>) {
	tracing::info!(
		target: "knock_debug",
		"mark_as_left called for user_id={} room_id={}", user_id, room_id
	);
	let prior_members = self
		.room_members(room_id)
		.map(ToOwned::to_owned)
		.collect::<Vec<_>>()
		.await;
	let userroom_id = (user_id, room_id);
	let userroom_id = serialize_key(userroom_id).expect("failed to serialize userroom_id");

	let roomuser_id = (room_id, user_id);
	let roomuser_id = serialize_key(roomuser_id).expect("failed to serialize roomuser_id");
	let left_count = self.services.globals.next_count().unwrap();

	self.db
		.userroomid_leftstate
		.raw_put(&userroom_id, Json(leave_pdu));
	self.db
		.roomuserid_leftcount
		.raw_aput::<8, _, _>(&roomuser_id, left_count);

	// Compare local processing order (the monotonic counters assigned by
	// `next_count` when each event was handled) rather than `origin_server_ts`:
	// that timestamp is set by the sending server's own clock, so a leave
	// processed after an invite can still carry an earlier `origin_server_ts`
	// than the invite if the inviting server's clock is ahead of ours. The
	// counters reflect the order we actually applied the events in, which is
	// what determines which state is current.
	let preserve_newer_invite = self
		.get_invite_count(room_id, user_id)
		.await
		.is_ok_and(|invite_count| invite_count > left_count);

	self.set_other_membership_states(
		&userroom_id,
		&roomuser_id,
		room_id,
		MembershipKind::Left,
		preserve_newer_invite,
	);

	self.invalidate_user_visibility(user_id, room_id).await;
	self.invalidate_server_visibility(user_id, room_id).await;
	self.mark_device_list_lefts(user_id, &prior_members, left_count)
		.await;

	if self.services.globals.user_is_local(user_id)
		&& (self.services.config.forget_forced_upon_leave
			|| self.services.metadata.is_banned(room_id).await
			|| self.services.metadata.is_disabled(room_id).await)
	{
		self.forget(room_id, user_id);
	}
}

#[implement(super::Service)]
async fn mark_device_list_lefts(
	&self,
	user_id: &UserId,
	prior_members: &[OwnedUserId],
	count: u64,
) {
	for member in prior_members {
		if member == user_id {
			continue;
		}

		if self.services.globals.user_is_local(member)
			&& !self.user_sees_user(member, user_id).await
		{
			self.services
				.users
				.mark_device_list_left(member, user_id, count);
		}

		if self.services.globals.user_is_local(user_id)
			&& !self.user_sees_user(user_id, member).await
		{
			self.services
				.users
				.mark_device_list_left(user_id, member, count);
		}
	}
}

/// Direct DB function to directly mark a user as knocked. It is not
/// recommended to use this directly. You most likely should use
/// `update_membership` instead
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn mark_as_knocked(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	knocked_state: Option<Vec<Raw<AnyStrippedStateEvent>>>,
) {
	let userroom_id = (user_id, room_id);
	let userroom_id = serialize_key(userroom_id).expect("failed to serialize userroom_id");

	let roomuser_id = (room_id, user_id);
	let roomuser_id = serialize_key(roomuser_id).expect("failed to serialize roomuser_id");

	let new_count = self.services.globals.next_count().unwrap();
	tracing::info!(
		target: "knock_debug",
		"mark_as_knocked called for user_id={} room_id={} new_count={} knocked_state={:?}",
		user_id, room_id, new_count, knocked_state
	);

	self.db
		.userroomid_knockedstate
		.raw_put(&userroom_id, Json(knocked_state.unwrap_or_default()));
	self.db
		.roomuserid_knockedcount
		.raw_aput::<8, _, _>(&roomuser_id, new_count);

	self.set_other_membership_states(
		&userroom_id,
		&roomuser_id,
		room_id,
		MembershipKind::Knocked,
		false,
	);
	self.unforget(room_id, user_id);
}

/// Makes a user forget a room.
///
/// This must NOT delete `userroomid_leftstate`/`roomuserid_leftcount`: other
/// devices of the user have no way of knowing the room was forgotten (see
/// forget_room_route's doc comment), so /sync still needs the real leave
/// position to surface the leave via `include_leave` on a later incremental
/// sync. Forgetting is tracked as an independent flag instead; callers that
/// need to gate access on "did this user forget this room" (e.g. /messages)
/// should check `is_forgotten()`, not infer it from `is_left()` going false.
#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn forget(&self, room_id: &RoomId, user_id: &UserId) {
	let roomuser_id = (room_id, user_id);

	self.db.roomuserid_forgotten.put_raw(roomuser_id, []);
}

#[implement(super::Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub fn unforget(&self, room_id: &RoomId, user_id: &UserId) {
	let roomuser_id = (room_id, user_id);
	let roomuser_id = serialize_key(roomuser_id).expect("failed to serialize roomuser_id");

	self.db.roomuserid_forgotten.remove(&roomuser_id);
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self))]
fn mark_as_once_joined(&self, user_id: &UserId, room_id: &RoomId) {
	let key = (user_id, room_id);
	self.db.roomuseroncejoinedids.put_raw(key, []);
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip(self, last_state, invite_via))]
pub async fn mark_as_invited(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	sender_user: &UserId,
	last_state: Option<Vec<Raw<AnyStrippedStateEvent>>>,
	invite_via: Option<Vec<OwnedServerName>>,
) -> Result<()> {
	// return an error for blocked invites. ignored invites aren't handled here
	// since the recipient's membership should still be changed to `invite`.
	// they're filtered out in the individual /sync handlers
	if matches!(
		self.services
			.users
			.invite_filter_level(sender_user, user_id)
			.await,
		FilterLevel::Block
	) {
		return Err!(Request(InviteBlocked("{user_id} has blocked invites from {sender_user}.")));
	}

	let roomuser_id = (room_id, user_id);
	let roomuser_id = serialize_key(roomuser_id).expect("failed to serialize roomuser_id");

	let userroom_id = (user_id, room_id);
	let userroom_id = serialize_key(userroom_id).expect("failed to serialize userroom_id");

	self.db
		.userroomid_invitestate
		.raw_put(&userroom_id, Json(last_state.unwrap_or_default()));
	self.db
		.roomuserid_invitecount
		.raw_aput::<8, _, _>(&roomuser_id, self.services.globals.next_count().unwrap());
	self.db
		.userroomid_invitesender
		.insert(&userroom_id, sender_user);

	self.set_other_membership_states(
		&userroom_id,
		&roomuser_id,
		room_id,
		MembershipKind::Invited,
		false,
	);
	self.unforget(room_id, user_id);

	if let Some(servers) = invite_via.filter(is_not_empty!()) {
		self.add_servers_invite_via(room_id, servers).await;
	}

	Ok(())
}

/// Rebuild the membership cache from the current room state snapshot.
/// Extracted from the reorder_timeline logic for reuse.
#[implement(super::Service)]
pub async fn reconcile_membership(&self, room_id: &RoomId) {
	let mut members_synced = 0_usize;
	let mut state_joined: HashSet<OwnedUserId> = HashSet::new();
	let mut state_invited: HashSet<OwnedUserId> = HashSet::new();
	let cached_joined: HashSet<OwnedUserId> = self
		.room_members(room_id)
		.map(ToOwned::to_owned)
		.collect()
		.await;
	let cached_invited: HashSet<OwnedUserId> = self
		.room_members_invited(room_id)
		.map(ToOwned::to_owned)
		.collect()
		.await;

	let room_ssh_opt = self
		.services
		.state
		.get_room_shortstatehash(room_id)
		.await
		.ok();

	if let Some(room_ssh) = room_ssh_opt {
		let state_full = self.services.state_accessor.state_full(room_ssh);
		let mut state_full = std::pin::pin!(state_full);
		while let Some(((event_type, state_key), pdu)) = state_full.next().await {
			if event_type != StateEventType::RoomMember {
				continue;
			}
			let Ok(uid) = OwnedUserId::try_from(state_key.as_str()) else {
				continue;
			};

			let content: serde_json::Value = pdu.get_content_as_value();
			let membership = content
				.get("membership")
				.and_then(|v| v.as_str())
				.unwrap_or("leave");

			match membership {
				| "join" => {
					state_joined.insert(uid);
				},
				| "invite" => {
					state_invited.insert(uid);
				},
				| _ => {},
			}
		}
	}

	for user_id in state_joined.difference(&cached_joined) {
		self.mark_as_joined_silent(user_id, room_id).await;
		members_synced = members_synced.saturating_add(1);
	}
	for user_id in state_invited.difference(&cached_invited) {
		if let Some(room_ssh) = room_ssh_opt {
			if let Ok(pdu) = self
				.services
				.state_accessor
				.state_get(room_ssh, &StateEventType::RoomMember, user_id.as_str())
				.await
			{
				let last_state = self.services.state.summary_stripped(&pdu, room_id).await;
				self.mark_as_invited_silent(
					user_id,
					room_id,
					Some(last_state),
					Some(pdu.sender()),
					None,
				)
				.await;
			} else {
				self.mark_as_invited_silent(user_id, room_id, None, None, None)
					.await;
			}
		} else {
			self.mark_as_invited_silent(user_id, room_id, None, None, None)
				.await;
		}
		members_synced = members_synced.saturating_add(1);
	}

	let mut stale_removed = 0_usize;
	for user_id in cached_joined.difference(&state_joined) {
		if !state_invited.contains(user_id) {
			self.mark_as_left_silent(user_id, room_id).await;
			stale_removed = stale_removed.saturating_add(1);
		}
	}
	for user_id in cached_invited.difference(&state_invited) {
		if !state_joined.contains(user_id) {
			self.mark_as_left_silent(user_id, room_id).await;
			stale_removed = stale_removed.saturating_add(1);
		}
	}

	self.update_joined_count(room_id).await;
	info!(
		"heal_room: synced {members_synced} membership cache entries, removed {stale_removed} \
		 stale"
	);
}

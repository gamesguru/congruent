use conduwuit::{Err, Result};
use futures::{FutureExt, StreamExt};
use ruma::{OwnedRoomId, OwnedRoomOrAliasId};

use crate::{PAGE_SIZE, admin_command, get_room_info};

#[allow(clippy::fn_params_excessive_bools)]
#[admin_command]
pub(super) async fn list_rooms(
	&self,
	page: Option<usize>,
	exclude_disabled: bool,
	exclude_banned: bool,
	include_empty: bool,
	no_details: bool,
) -> Result {
	// TODO: i know there's a way to do this with clap, but i can't seem to find it
	let page = page.unwrap_or(1);
	let mut rooms = self
		.services
		.rooms
		.metadata
		.iter_ids()
		.filter_map(|room_id| async move {
			(!exclude_disabled || !self.services.rooms.metadata.is_disabled(room_id).await)
				.then_some(room_id)
		})
		.filter_map(|room_id| async move {
			(!exclude_banned || !self.services.rooms.metadata.is_banned(room_id).await)
				.then_some(room_id)
		})
		.then(|room_id| get_room_info(self.services, room_id))
		.then(|(room_id, total_members, name)| async move {
			let local_members: Vec<_> = self
				.services
				.rooms
				.state_cache
				.active_local_users_in_room(&room_id)
				.collect()
				.await;
			let local_members = local_members.len();
			(room_id, total_members, local_members, name)
		})
		.filter_map(|(room_id, total_members, local_members, name)| async move {
			(include_empty || local_members > 0).then_some((room_id, total_members, name))
		})
		.collect::<Vec<_>>()
		.await;

	let total_rooms = rooms.len();
	rooms.sort_by_key(|r| r.1);
	rooms.reverse();

	let rooms = rooms
		.into_iter()
		.skip(page.saturating_sub(1).saturating_mul(PAGE_SIZE))
		.take(PAGE_SIZE)
		.collect::<Vec<_>>();

	if rooms.is_empty() {
		return Err!("No more rooms.");
	}

	let body = rooms
		.iter()
		.map(|(id, members, name)| {
			if no_details {
				format!("{id}")
			} else {
				format!("{id}\tMembers: {members}\tName: {name}")
			}
		})
		.collect::<Vec<_>>()
		.join("\n");

	self.write_str(&format!("Rooms (Total: {total_rooms}, Page {page}):\n```\n{body}\n```"))
		.await
}

#[admin_command]
pub(super) async fn exists(&self, room_id: OwnedRoomId) -> Result {
	let result = self.services.rooms.metadata.exists(&room_id).await;

	self.write_str(&format!("{result}")).await
}

#[admin_command]
pub(super) async fn bump(
	&self,
	room_id: Option<OwnedRoomId>,
	all: bool,
	skip: Vec<OwnedRoomId>,
) -> Result {
	self.bail_restricted()?;

	if all {
		let skip_set: std::collections::HashSet<&ruma::RoomId> =
			skip.iter().map(AsRef::as_ref).collect();
		let ours = self.services.globals.server_name();
		let rooms = self.services.rooms.state_cache.server_rooms(ours);
		let mut room_stream = rooms.boxed();
		let mut thumper = 0_usize;
		let mut skipped = 0_usize;

		while let Some(room_id) = room_stream.next().await {
			if skip_set.contains(room_id) {
				skipped = skipped.saturating_add(1);
				continue;
			}

			if self
				.services
				.rooms
				.state_cache
				.active_local_users_in_room(room_id)
				.boxed()
				.next()
				.await
				.is_some()
			{
				self.services
					.rooms
					.monitor
					.check_room(room_id, 0)
					.boxed()
					.await?;
				thumper = thumper.saturating_add(1);
			}
		}

		return self
			.write_str(&format!(
				"Successfully triggered sync for {thumper} rooms (skipped {skipped})."
			))
			.await;
	}

	let Some(room_id) = room_id else {
		return Err!("Missing room_id or --all flag.");
	};

	if !self
		.services
		.rooms
		.state_cache
		.server_is_participant(&self.services.server.name, &room_id)
		.await
	{
		return Err!("We are not participating in the room / we don't know about the room ID.");
	}

	if self
		.services
		.rooms
		.state_cache
		.active_local_users_in_room(&room_id)
		.boxed()
		.next()
		.await
		.is_none()
	{
		return Err!("No local users in room {room_id} - cannot bump");
	}

	self.services
		.rooms
		.monitor
		.check_room(&room_id, 0)
		.boxed()
		.await?;

	self.write_str(&format!("Successfully triggered sync for room {room_id}"))
		.await
}

#[admin_command]
pub(super) async fn purge_sync_tokens(&self, room: OwnedRoomOrAliasId) -> Result {
	// Resolve the room ID from the room or alias ID
	let room_id = self.services.rooms.alias.resolve(&room).await?;

	// Delete all tokens for this room using the service method
	let deleted_count = self
		.services
		.rooms
		.user
		.delete_room_tokens(&room_id)
		.await?;

	self.write_str(&format!(
		"Successfully deleted {deleted_count} sync tokens for room {}",
		room_id.as_str()
	))
	.await
}

use std::fmt::Write;

use conduwuit::{Err, Result, utils::response::LimitReadExt};
use futures::StreamExt;
use ruma::{OwnedRoomId, OwnedServerName, OwnedUserId};

use crate::{admin_command, get_room_info};

#[admin_command]
pub(super) async fn disable_room(&self, room_id: OwnedRoomId) -> Result {
	self.bail_restricted()?;
	self.services.rooms.metadata.disable_room(&room_id, true);
	self.write_str("Room disabled.").await
}

#[admin_command]
pub(super) async fn enable_room(&self, room_id: OwnedRoomId) -> Result {
	self.bail_restricted()?;
	self.services.rooms.metadata.disable_room(&room_id, false);
	self.write_str("Room enabled.").await
}

#[admin_command]
pub(super) async fn incoming_federation(&self) -> Result {
	let msg = {
		let map = self
			.services
			.rooms
			.event_handler
			.federation_handletime
			.read();

		let active_txns = self.services.transactions.txn_active_keys();

		let mut msg = format!(
			"Handling {} incoming PDUs across {} active transactions:\n",
			map.len(),
			active_txns.len()
		);

		if !map.is_empty() {
			writeln!(msg, "PDUs being handled:")?;
			for (r, (e, i)) in map.iter() {
				let elapsed = i.elapsed();
				writeln!(
					msg,
					"  {} {}: {}m{}s",
					r,
					e,
					elapsed.as_secs() / 60,
					elapsed.as_secs() % 60
				)?;
			}
		}

		if !active_txns.is_empty() {
			writeln!(msg, "\nActive Transactions:")?;
			for (origin, txn_id) in active_txns {
				writeln!(msg, "  {origin} {txn_id}")?;
			}
		}

		msg
	};

	self.write_str(&msg).await
}

#[admin_command]
pub(super) async fn fetch_support_well_known(&self, server_name: OwnedServerName) -> Result {
	let response = self
		.services
		.client
		.default
		.get(format!("https://{server_name}/.well-known/matrix/support"))
		.send()
		.await?;

	let text = response
		.limit_read_text(
			self.services
				.config
				.max_request_size
				.try_into()
				.expect("u64 fits into usize"),
		)
		.await?;

	if text.is_empty() {
		return Err!("Response text/body is empty.");
	}

	if text.len() > 1500 {
		return Err!(
			"Response text/body is over 1500 characters, assuming no support well-known.",
		);
	}

	let json: serde_json::Value = match serde_json::from_str(&text) {
		| Ok(json) => json,
		| Err(_) => {
			return Err!("Response text/body is not valid JSON.",);
		},
	};

	let pretty_json: String = match serde_json::to_string_pretty(&json) {
		| Ok(json) => json,
		| Err(_) => {
			return Err!("Response text/body is not valid JSON.",);
		},
	};

	self.write_str(&format!("Got JSON response:\n\n```json\n{pretty_json}\n```"))
		.await
}

#[admin_command]
pub(super) async fn remote_user_in_rooms(&self, user_id: OwnedUserId) -> Result {
	if user_id.server_name() == self.services.server.name {
		return Err!(
			"User belongs to our server, please use `list-joined-rooms` user admin command \
			 instead.",
		);
	}

	if !self.services.users.exists(&user_id).await {
		return Err!("Remote user does not exist in our database.",);
	}

	let mut rooms: Vec<(OwnedRoomId, u64, String)> = self
		.services
		.rooms
		.state_cache
		.rooms_joined(&user_id)
		.then(|room_id| get_room_info(self.services, room_id))
		.collect()
		.await;

	if rooms.is_empty() {
		return Err!("User is not in any rooms.");
	}

	rooms.sort_by_key(|r| r.1);
	rooms.reverse();

	let num = rooms.len();
	let body = rooms
		.iter()
		.map(|(id, members, name)| format!("{id} | Members: {members} | Name: {name}"))
		.collect::<Vec<_>>()
		.join("\n");

	self.write_str(&format!("Rooms {user_id} shares with us ({num}):\n```\n{body}\n```"))
		.await
}

#[admin_command]
pub(super) async fn sending_queue(
	&self,
	server: Option<OwnedServerName>,
	clear: bool,
	all: bool,
) -> Result {
	self.bail_restricted()?;

	if clear {
		if let Some(ref server) = server {
			self.services.sending.clear_destination_queue(server).await;
			return self
				.write_str(&format!("Cleared sending queue for {server}"))
				.await;
		}
		if !all {
			return Err!(Request(InvalidParam(
				"Use --all to clear the entire sending queue, or specify a server."
			)));
		}
		self.services.sending.clear_all_federation_queues().await;
		return self
			.write_str("Cleared sending queue for all destinations")
			.await;
	}

	let destinations = self.services.sending.queued_destinations().await;
	let destinations: Vec<_> = destinations
		.into_iter()
		.filter(|(dest, ..)| server.as_ref().is_none_or(|filter| dest == filter.as_str()))
		.collect();

	if destinations.is_empty() {
		return match server {
			| Some(server) =>
				self.write_str(&format!("Sending queue is empty for {server}."))
					.await,
			| None => self.write_str("Sending queue is empty.").await,
		};
	}

	let mut out = String::from("Outbound Sending Queue:\n```\n");
	let mut total_queued = 0_usize;
	let mut total_active = 0_usize;

	for (dest, queued, active) in &destinations {
		total_queued = total_queued.saturating_add(*queued);
		total_active = total_active.saturating_add(*active);
		writeln!(out, "{dest:50}  queued={queued:>6}  active={active:>4}")?;
	}

	writeln!(out, "```")?;
	writeln!(
		out,
		"**Total:** {total_queued} queued, {total_active} active across {} destinations",
		destinations.len()
	)?;

	self.write_str(&out).await
}

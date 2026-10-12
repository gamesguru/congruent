use std::{fs::File, path::PathBuf};

use conduwuit::{Result, err};
use slipstream::OwnedRoomId;

use crate::admin_command;

#[admin_command]
pub(super) async fn export_mtxdb(
	&self,
	room_id: Option<OwnedRoomId>,
	path: Option<String>,
	all: bool,
) -> Result {
	if all && room_id.is_some() {
		return Err(err!("Cannot use --all with a specific room_id"));
	}
	if !all && room_id.is_none() {
		return Err(err!("Must specify a room_id or --all"));
	}

	let mut total_events = 0_u64;

	let rooms: Vec<OwnedRoomId> = if all {
		self.services.rooms.state.rooms_with_roots().await?
	} else {
		vec![room_id.unwrap()]
	};

	for room in &rooms {
		let room_path = if rooms.len() > 1 {
			let safe_room_id = room.to_string().replace('!', "").replace(':', "_");
			path.as_ref().map_or_else(
				|| std::env::temp_dir().join(format!("mtxdb-export-{safe_room_id}.jsonl")),
				|p| PathBuf::from(p).join(format!("mtxdb-export-{safe_room_id}.jsonl")),
			)
		} else {
			path.as_ref().map_or_else(
				|| {
					let safe_room_id = room.to_string().replace('!', "").replace(':', "_");
					std::env::temp_dir().join(format!("mtxdb-export-{safe_room_id}.jsonl"))
				},
				PathBuf::from,
			)
		};

		let mut output = File::create(&room_path).map_err(|error| {
			err!(Database("Failed to create mtxdb export file {room_path:?}: {error}"))
		})?;
		let counts = self
			.services
			.rooms
			.state
			.export_room_jsonl(room, &mut output)
			.await?;

		total_events = total_events.saturating_add(counts.events);

		if rooms.len() == 1 {
			self.write_str(&format!(
				"Exported {} events to {}",
				counts.events,
				room_path.display()
			))
			.await?;
		}
	}

	if rooms.len() > 1 {
		let output_dir = path
			.clone()
			.unwrap_or_else(|| std::env::temp_dir().to_string_lossy().into_owned());
		self.write_str(&format!(
			"Exported {} rooms, {} total events to {}",
			rooms.len(),
			total_events,
			output_dir
		))
		.await?;
	}

	Ok(())
}

#[admin_command]
pub(super) async fn export_mtxdb_keys(&self, path: Option<String>) -> Result {
	let path =
		path.map_or_else(|| std::env::temp_dir().join("mtxdb-export-keys.jsonl"), PathBuf::from);
	let mut output = File::create(&path).map_err(|error| {
		err!(Database("Failed to create mtxdb keys export file {path:?}: {error}"))
	})?;

	let counts = self
		.services
		.server_keys
		.export_keys_jsonl(&mut output)
		.await?;

	self.write_str(&format!(
		"Exported {} historical, {} provisional, {} corroborated, {} notary key records to {}",
		counts.historical,
		counts.provisional,
		counts.corroborated,
		counts.notary,
		path.display()
	))
	.await
}

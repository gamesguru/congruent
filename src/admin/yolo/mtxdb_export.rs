use std::{fs::File, path::PathBuf};

use conduwuit::{Result, err};
use slipstream::OwnedRoomId;

use crate::admin_command;

#[admin_command]
pub(super) async fn export_mtxdb(&self, room_id: OwnedRoomId, path: Option<String>) -> Result {
	let path = path.map_or_else(
		|| {
			let safe_room_id = room_id.to_string().replace('!', "").replace(':', "_");
			std::env::temp_dir().join(format!("mtxdb-export-{safe_room_id}.jsonl"))
		},
		PathBuf::from,
	);
	let mut output = File::create(&path).map_err(|error| {
		err!(Database("Failed to create mtxdb export file {path:?}: {error}"))
	})?;
	let counts = self
		.services
		.rooms
		.state
		.export_room_jsonl(&room_id, &mut output)
		.await?;

	self.write_str(&format!(
		"Exported {} nodes, {} roots, and {} events ({} missing nodes) to {}",
		counts.nodes,
		counts.roots,
		counts.events,
		counts.missing_nodes,
		path.display()
	))
	.await
}

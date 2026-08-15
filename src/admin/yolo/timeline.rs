use std::fmt::Write as _;

use conduwuit::{Result, err, matrix::Event};
use futures::StreamExt;
use ruma::{OwnedEventId, OwnedRoomId};

use crate::admin_command;

#[admin_command]
pub(super) async fn reorder_timeline(
	&self,
	room_id: Option<OwnedRoomId>,
	all: bool,
	no_compute_state: bool,
	force_reindex: bool,
) -> Result {
	self.bail_restricted()?;

	if all {
		let mut room_ids: Vec<OwnedRoomId> = Vec::new();
		let mut rooms = self.services.rooms.metadata.iter_ids();
		while let Some(room_id) = rooms.next().await {
			room_ids.push(room_id.to_owned());
		}
		drop(rooms);

		self.write_str(&format!("Reordering timeline for {} rooms...", room_ids.len()))
			.await?;

		let mut count = 0_usize;
		let mut failures: Vec<(OwnedRoomId, String)> = Vec::new();
		for room_id in room_ids {
			match Box::pin(self.services.rooms.timeline.reorder_timeline(
				&room_id,
				no_compute_state,
				force_reindex,
			))
			.await
			{
				| Ok(_) => count = count.saturating_add(1),
				| Err(error) => failures.push((room_id, error.to_string())),
			}
		}

		if failures.is_empty() {
			return self
				.write_str(&format!(
					"Reordered timeline for {count} rooms. Clients should re-sync."
				))
				.await;
		}

		let mut output =
			format!("Reordered timeline for {count} rooms. {} rooms failed:\n", failures.len());
		for (room_id, error) in &failures {
			let _ = writeln!(output, "- {room_id}: {error}");
		}

		return self.write_str(&output).await;
	}

	let room_id = room_id.ok_or_else(|| err!("room_id is required unless --all is specified"))?;

	self.write_str(&format!("Reordering timeline for {room_id} by topological DAG order..."))
		.await?;

	let count = Box::pin(self.services.rooms.timeline.reorder_timeline(
		&room_id,
		no_compute_state,
		force_reindex,
	))
	.await?;

	self.write_str(&format!(
		"Reordered {count} PDUs in room {room_id}. Clients should re-sync this room."
	))
	.await
}

#[admin_command]
pub(super) async fn rebuild_state(&self, room_id: OwnedRoomId) -> Result {
	self.bail_restricted()?;

	self.write_str(&format!("Incrementally rebuilding state for {room_id} from the timeline..."))
		.await?;

	Box::pin(self.services.rooms.timeline.rebuild_state(&room_id)).await?;

	self.write_str(&format!(
		"Successfully rebuilt state for {room_id}. Timeline PduCounts were unchanged."
	))
	.await
}

#[admin_command]
pub(super) async fn purge_timeline_pdu(&self, event_id: OwnedEventId) -> Result {
	self.bail_restricted()?;

	let in_timeline = self
		.services
		.rooms
		.timeline
		.non_outlier_pdu_exists(&event_id)
		.await;

	let mut room_id_opt = None;
	if in_timeline {
		if let Ok(pdu) = self.services.rooms.timeline.get_pdu(&event_id).await {
			room_id_opt = pdu.room_id().map(ToOwned::to_owned);
		}
	}

	// Remove from timeline tables (room_pducount_eventid + eventid_pduid)
	self.services
		.rooms
		.timeline
		.remove_from_timeline(&event_id)
		.await;

	// Also remove from outlier tables
	self.services.rooms.outlier.remove_outlier(&event_id).await;

	if in_timeline {
		if let Some(room_id) = room_id_opt {
			let (_, num_true) = self
				.services
				.rooms
				.timeline
				.recalculate_extremities(&room_id, true)
				.await?;
			self.write_str(&format!(
				"Purged {event_id} from timeline and outlier tables. DAG Extremities \
				 automatically recalculated (now {num_true} tips)."
			))
			.await
		} else {
			self.write_str(&format!(
				"Purged {event_id} from timeline and outlier tables. DAG Extremities \
				 automatically recalculated."
			))
			.await
		}
	} else {
		self.write_str(&format!(
			"Event {event_id} was not in the timeline (purged outlier only)."
		))
		.await
	}
}

#[admin_command]
pub(super) async fn repair_unsigned(&self, room_id: OwnedRoomId) -> Result {
	self.bail_restricted()?;

	let repaired = self
		.services
		.rooms
		.timeline
		.repair_room_unsigned(&room_id)
		.await?;

	self.write_str(&format!(
		"Repair complete for room {room_id}: {repaired} state events repaired"
	))
	.await
}

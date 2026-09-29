use std::{collections::HashSet, fmt::Write};

use conduwuit::{Err, Result, matrix::Event};
use futures::StreamExt;
use ruma::{OwnedEventId, OwnedRoomId, RoomId};

use crate::admin_command;

#[admin_command]
pub(super) async fn manage_rejected(
	&self,
	event_ids: Vec<OwnedEventId>,
	unreject: bool,
	soft_fail: bool,
) -> Result {
	let mut changed = 0_usize;
	let mut already = 0_usize;

	for event_id in &event_ids {
		let is_rejected = self
			.services
			.rooms
			.pdu_metadata
			.is_event_rejected(event_id)
			.await;
		let is_soft_failed = self
			.services
			.rooms
			.pdu_metadata
			.is_event_soft_failed(event_id)
			.await;

		if unreject {
			if is_rejected {
				self.services
					.rooms
					.pdu_metadata
					.unmark_event_rejected(event_id);
				changed = changed.saturating_add(1);
			} else {
				already = already.saturating_add(1);
			}
			if soft_fail && is_soft_failed {
				self.services
					.rooms
					.pdu_metadata
					.unmark_event_soft_failed(event_id);
			}
		} else {
			if !is_rejected {
				self.services
					.rooms
					.pdu_metadata
					.mark_event_rejected(
						event_id,
						"manually rejected via admin command manage-rejected",
					)
					.await;
				changed = changed.saturating_add(1);
			} else {
				already = already.saturating_add(1);
			}
			if soft_fail && !is_soft_failed {
				self.services.rooms.pdu_metadata.mark_event_soft_failed(
					event_id,
					conduwuit_service::rooms::pdu_metadata::SoftFailCode::Manual,
				);
			}
		}
	}

	let action = if unreject { "unrejected" } else { "marked rejected" };
	let already_desc = if unreject {
		"already not rejected"
	} else {
		"already rejected"
	};
	let sf_note = if soft_fail { " (+ soft-fail marker)" } else { "" };
	self.write_str(&format!(
		"{changed} event(s) {action}{sf_note} ({already} {already_desc}, {} total)\n",
		event_ids.len()
	))
	.await
}

#[admin_command]
#[allow(clippy::fn_params_excessive_bools)]
pub(super) async fn unreject_room(
	&self,
	room_id: Option<OwnedRoomId>,
	all: bool,
	dry_run: bool,
	soft_fail: bool,
) -> Result {
	self.bail_restricted()?;

	let room_ids = if all {
		self.services
			.rooms
			.metadata
			.iter_ids()
			.map(ToOwned::to_owned)
			.collect::<Vec<_>>()
			.await
	} else if let Some(r) = room_id {
		vec![r]
	} else {
		return Err!("Must specify a room_id or use --all.");
	};

	let mut total_unmarked = 0_usize;
	let mut total_soft_unmarked = 0_usize;
	let mut total_found = 0_usize;

	for room_id in room_ids {
		let mut unmarked = 0_usize;
		let mut soft_unmarked = 0_usize;
		let mut total = 0_usize;

		// Collect all event IDs from timeline + outlier tree
		let mut pdu_ids: HashSet<OwnedEventId> = self
			.services
			.rooms
			.timeline
			.all_pdus(&room_id)
			.map(|(_, pdu)| pdu.event_id().to_owned())
			.collect()
			.await;

		let outlier_count_before = pdu_ids.len();

		let outliers: Vec<OwnedEventId> = self
			.services
			.rooms
			.outlier
			.room_stream(&room_id)
			.map(|(event_id, _)| event_id)
			.collect()
			.await;

		pdu_ids.extend(outliers);

		if !all {
			self.write_str(&format!(
				"Scanning {} events ({} timeline, {} outliers)...\n",
				pdu_ids.len(),
				outlier_count_before,
				pdu_ids.len().saturating_sub(outlier_count_before),
			))
			.await?;
		}

		for event_id in &pdu_ids {
			if self
				.services
				.rooms
				.pdu_metadata
				.is_event_rejected(event_id)
				.await
			{
				total = total.saturating_add(1);
				if !dry_run {
					self.services
						.rooms
						.pdu_metadata
						.unmark_event_rejected(event_id);
					unmarked = unmarked.saturating_add(1);
				}
			}
			if soft_fail
				&& self
					.services
					.rooms
					.pdu_metadata
					.is_event_soft_failed(event_id)
					.await
			{
				if !dry_run {
					self.services
						.rooms
						.pdu_metadata
						.unmark_event_soft_failed(event_id);
					soft_unmarked = soft_unmarked.saturating_add(1);
				}
			}
		}

		total_unmarked = total_unmarked.saturating_add(unmarked);
		total_soft_unmarked = total_soft_unmarked.saturating_add(soft_unmarked);
		total_found = total_found.saturating_add(total);

		if !all {
			if dry_run {
				self.write_str(&format!(
					"Dry run: Found {total} rejected events in {room_id} to unmark.\n"
				))
				.await?;
			} else {
				let soft_msg = if soft_fail {
					format!(", {soft_unmarked} soft-fail markers cleared")
				} else {
					String::new()
				};
				self.write_str(&format!(
					"Unmarked {unmarked} rejected events{soft_msg} in {room_id}.\n"
				))
				.await?;
			}
		}
	}

	if all {
		if dry_run {
			self.write_str(&format!("Dry run: Found {total_found} rejected events total.\n"))
				.await?;
		} else {
			let soft_msg = if soft_fail {
				format!(", {total_soft_unmarked} soft-fail markers cleared")
			} else {
				String::new()
			};
			self.write_str(&format!(
				"Unmarked {total_unmarked} rejected events{soft_msg} total across all rooms.\n"
			))
			.await?;
		}
	}

	Ok(())
}

#[admin_command]
pub(super) async fn list_rejected(
	&self,
	room_id: OwnedRoomId,
	limit: Option<usize>,
	soft_fail: bool,
	reverse: bool,
) -> Result {
	self.bail_restricted()?;

	let limit = limit.unwrap_or(100);
	let mut count = 0;
	let mut body = String::new();

	let mut stream = if reverse {
		self.services
			.rooms
			.timeline
			.pdus_rev(&room_id, std::ops::Bound::Unbounded)
			.filter_map(|r| futures::future::ready(r.ok()))
			.boxed()
	} else {
		self.services.rooms.timeline.all_pdus(&room_id).boxed()
	};

	while let Some((_, pdu)) = stream.next().await {
		if count >= limit {
			writeln!(body, "--- Stopped after {limit} entries ---")?;
			break;
		}

		let event_id = pdu.event_id();
		let mut show = false;
		let mut is_soft = false;
		let mut is_rej = false;

		if !soft_fail {
			if self
				.services
				.rooms
				.pdu_metadata
				.is_event_rejected(event_id)
				.await
			{
				show = true;
				is_rej = true;
			}
		}

		if self
			.services
			.rooms
			.pdu_metadata
			.is_event_soft_failed(event_id)
			.await
		{
			show = true;
			is_soft = true;
		}

		if show {
			let flags = if is_soft && is_rej {
				" [rejected, soft-failed]"
			} else if is_soft {
				" [soft-failed]"
			} else if is_rej {
				" [rejected]"
			} else {
				""
			};
			let room_id_str = pdu.room_id().map_or("unknown", RoomId::as_str);
			let sender = pdu.sender();
			let kind = pdu.kind.to_string();
			let ts = pdu.origin_server_ts;
			let reason = if is_rej {
				self.services
					.rooms
					.pdu_metadata
					.get_rejection_reason(event_id)
					.await
					.filter(|r| !r.is_empty())
					.map_or(String::new(), |r| format!("\tReason: {r}"))
			} else if is_soft {
				self.services
					.rooms
					.pdu_metadata
					.get_soft_fail_reason(event_id)
					.await
					.filter(|r| !r.is_empty())
					.map_or(String::new(), |r| format!("\tReason: {r}"))
			} else {
				String::new()
			};
			writeln!(
				body,
				"{event_id}\tTS: {ts}\tRoom: {room_id_str}\tSender: {sender}\tType: \
				 {kind}{flags}{reason}"
			)?;
			count = count.saturating_add(1);
		}
	}

	if body.is_empty() {
		return Err!("No rejected events found in timeline.");
	}

	self.write_str(&format!("Found {count} rejected timeline events:\n```\n{body}\n```"))
		.await
}

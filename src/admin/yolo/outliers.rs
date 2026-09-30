use std::fmt::Write;

use conduwuit::{
	Err, Result, info,
	matrix::{Event, pdu::PduEvent},
};
use futures::{StreamExt, future::ready};
use ruma::{OwnedEventId, OwnedRoomOrAliasId, OwnedUserId};

use crate::admin_command;

#[admin_command]
pub(super) async fn list_outliers(
	&self,
	room_id: Option<OwnedRoomOrAliasId>,
	sender: Option<OwnedUserId>,
	limit: Option<usize>,
	rejected: bool,
	clear: bool,
	reverse: bool,
) -> Result {
	let limit = limit.unwrap_or(100);

	let mut outliers: Vec<(OwnedEventId, PduEvent)> = Vec::new();
	{
		let resolved_room_id = match room_id {
			| Some(room) => Some(self.services.rooms.alias.resolve(&room).await?),
			| None => None,
		};

		let mut stream = if let Some(room) = &resolved_room_id {
			self.services.rooms.outlier.room_stream(room).boxed()
		} else {
			self.services.rooms.outlier.stream().take(10_000).boxed()
		};

		let mut i = 0_usize;
		while let Some((event_id, pdu)) = stream.next().await {
			if sender.as_ref().is_none_or(|s| pdu.sender() == s) {
				outliers.push((event_id, pdu));
			}
			i = i.saturating_add(1);
			if i.is_multiple_of(10_000) {
				tokio::task::yield_now().await;
			}
		}
	}

	// Sort by origin_server_ts (or in reverse, if requested)
	outliers.sort_by(|(_, a), (_, b)| {
		if reverse {
			b.origin_server_ts.cmp(&a.origin_server_ts)
		} else {
			a.origin_server_ts.cmp(&b.origin_server_ts)
		}
	});

	let mut count = 0_usize;
	let mut cleared = 0_usize;
	let mut body = String::new();
	for (event_id, pdu) in outliers {
		if count >= limit {
			writeln!(body, "--- Stopped after {limit} entries ---")?;
			break;
		}

		let is_stuck = self
			.services
			.rooms
			.timeline
			.get_pdu_id(&event_id)
			.await
			.is_ok();
		let is_rejected = self
			.services
			.rooms
			.pdu_metadata
			.is_event_rejected(&event_id)
			.await;
		let is_soft_failed = self
			.services
			.rooms
			.pdu_metadata
			.is_event_soft_failed(&event_id)
			.await;

		let status =
			super::outlier_utils::OutlierStatus { is_stuck, is_rejected, is_soft_failed };

		let action = super::outlier_utils::classify_outlier(&status, rejected, clear);
		match action {
			| super::outlier_utils::OutlierAction::Skip => continue,
			| super::outlier_utils::OutlierAction::Show { should_clear } =>
				if should_clear {
					self.services
						.rooms
						.pdu_metadata
						.clear_pdu_markers(&event_id);
					cleared = cleared.saturating_add(1);
				},
		}

		let room_id_str = pdu.room_id().map_or_else(
			|| {
				if pdu.kind.to_string() == "m.room.create" {
					event_id.as_str().replace('$', "!")
				} else {
					"unknown".to_owned()
				}
			},
			|r| r.as_str().to_owned(),
		);
		let sender = pdu.sender();
		let kind = pdu.kind.to_string();
		let ts = pdu.origin_server_ts;
		let flags = super::outlier_utils::render_flags(&status);
		let reason = if is_rejected {
			self.services
				.rooms
				.pdu_metadata
				.get_rejection_reason(&event_id)
				.await
				.filter(|r| !r.is_empty())
				.map_or(String::new(), |r| format!("\tReason: {r}"))
		} else if is_soft_failed {
			self.services
				.rooms
				.pdu_metadata
				.get_soft_fail_reason(&event_id)
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

	if body.is_empty() {
		if rejected {
			return Err!("No rejected outliers found.");
		}
		return Err!("No outliers found.");
	}

	let header = super::outlier_utils::summary_header(rejected);
	self.write_str(&format!("{header} ({count} shown, {cleared} cleared):\n```\n{body}\n```"))
		.await
}

#[admin_command]
pub(super) async fn purge_outliers(
	&self,
	event_id: Option<OwnedEventId>,
	room_id: Option<OwnedRoomOrAliasId>,
	sender: Option<OwnedUserId>,
	all: bool,
	force: bool,
) -> Result {
	// Fast path: single event by ID
	if let Some(ref eid) = event_id {
		self.services.rooms.outlier.remove_outlier(eid).await;
		return self.write_str(&format!("Purged outlier {eid}")).await;
	}

	if room_id.is_none() && sender.is_none() && !all {
		return Err!(
			"You must specify --event-id, a room, a sender, or use --all to purge outliers."
		);
	}

	let outliers: Vec<OwnedEventId> = if let Some(room) = room_id {
		let room_id = self.services.rooms.alias.resolve(&room).await?;
		self.services
			.rooms
			.outlier
			.room_stream(&room_id)
			.filter(|(_event_id, pdu): &(OwnedEventId, PduEvent)| {
				let sender_match = sender.as_ref().is_none_or(|s| pdu.sender() == s);
				ready(sender_match)
			})
			.map(|(event_id, _)| event_id)
			.collect()
			.await
	} else if sender.is_some() {
		self.services
			.rooms
			.outlier
			.stream()
			.filter(|(_event_id, pdu): &(OwnedEventId, PduEvent)| {
				let sender_match = sender.as_ref().is_none_or(|s| pdu.sender() == s);
				ready(sender_match)
			})
			.map(|(event_id, _)| event_id)
			.collect()
			.await
	} else {
		self.services.rooms.outlier.stream_keys().collect().await
	};

	let purged = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
	let skipped = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
	let total_len = outliers.len();

	futures::stream::iter(outliers)
		.for_each_concurrent(100, |event_id| {
			let purged = std::sync::Arc::clone(&purged);
			let skipped = std::sync::Arc::clone(&skipped);
			async move {
				if force {
					self.services.rooms.outlier.remove_outlier(&event_id).await;
					purged.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
				} else if self
					.services
					.rooms
					.timeline
					.get_pdu_id(&event_id)
					.await
					.is_ok()
				{
					// Duplicate: exists in both outlier and timeline tables
					self.services.rooms.outlier.clear_outlier_flag(&event_id);
					purged.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
				} else {
					skipped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
				}

				let p = purged.load(std::sync::atomic::Ordering::Relaxed);
				let s = skipped.load(std::sync::atomic::Ordering::Relaxed);
				let total = p.saturating_add(s);
				if total.is_multiple_of(10_000) && total > 0 {
					info!("Purge progress: {p} purged, {s} skipped of {total_len} total");
				}
			}
		})
		.await;

	let p = purged.load(std::sync::atomic::Ordering::Relaxed);
	let s = skipped.load(std::sync::atomic::Ordering::Relaxed);
	self.write_str(&format!("Purged {p} outliers, skipped {s} un-rescued outliers."))
		.await
}

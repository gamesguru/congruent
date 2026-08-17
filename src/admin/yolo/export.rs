use std::collections::HashSet;

use conduwuit::{
	Result,
	matrix::{Event, pdu::PduEvent},
};
use ruma::{CanonicalJsonObject, EventId, OwnedEventId, events::TimelineEventType};
use serde_json::Value as JsonValue;
use tokio::io::AsyncWriteExt;

pub(super) struct DagExportStats {
	pub count: u64,
	pub total_prev_events: u64,
	pub state_events: u64,
	pub missing_hash: u64,
	pub unique_hashes: HashSet<u64>,
	pub last_ssh: Option<u64>,
	pub last_is_state_event: bool,
	pub last_event_id: Option<Box<EventId>>,
	pub last_event_type: Option<TimelineEventType>,
	pub last_state_key: Option<String>,
	pub max_depth: u64,
	pub min_depth: u64,
	pub all_event_ids: HashSet<OwnedEventId>,
	pub referenced_as_prev: HashSet<OwnedEventId>,
	pub all_events_prevs: std::collections::HashMap<OwnedEventId, Vec<OwnedEventId>>,
}

impl Default for DagExportStats {
	fn default() -> Self {
		Self {
			count: 0,
			total_prev_events: 0,
			state_events: 0,
			missing_hash: 0,
			unique_hashes: HashSet::new(),
			last_ssh: None,
			last_is_state_event: false,
			last_event_id: None,
			last_event_type: None,
			last_state_key: None,
			max_depth: 0,
			min_depth: u64::MAX,
			all_event_ids: HashSet::new(),
			referenced_as_prev: HashSet::new(),
			all_events_prevs: std::collections::HashMap::new(),
		}
	}
}

/// Decorates a PDU's JSON representation with metadata (soft_failed, rejected,
/// outlier, shortstatehash) Returns the updated JSON object and a boolean
/// indicating if the event should be separated from the main export.
pub(super) async fn decorate_pdu_for_export(
	ctx: &crate::context::Context<'_>,
	pdu_json: &CanonicalJsonObject,
	pdu_opt: Option<&PduEvent>,
	is_outlier: bool,
) -> Result<(serde_json::Map<String, JsonValue>, bool, Option<u64>)> {
	let mut obj: serde_json::Map<String, JsonValue> =
		serde_json::from_value(serde_json::to_value(pdu_json)?)?;

	if is_outlier {
		obj.insert("__outlier".to_owned(), JsonValue::Bool(true));
	}

	let mut is_separated = is_outlier;
	let mut shortstatehash = None;

	if let Some(pdu) = pdu_opt {
		obj.insert("event_id".to_owned(), JsonValue::String(pdu.event_id().to_string()));
		let is_soft_failed = ctx
			.services
			.rooms
			.pdu_metadata
			.is_event_soft_failed(pdu.event_id())
			.await;
		if is_soft_failed {
			obj.insert("__soft_failed".to_owned(), JsonValue::Bool(true));
			is_separated = true;
		}

		let is_rejected = ctx
			.services
			.rooms
			.pdu_metadata
			.is_event_rejected(pdu.event_id())
			.await;
		if is_rejected {
			obj.insert("__rejected".to_owned(), JsonValue::Bool(true));
			is_separated = true;
		}

		if !is_separated {
			if let Ok(ssh) = ctx
				.services
				.rooms
				.state_accessor
				.pdu_shortstatehash(pdu.event_id())
				.await
			{
				obj.insert("__shortstatehash".to_owned(), JsonValue::from(ssh));
				shortstatehash = Some(ssh);
			}
		}

		// Export full EventMetadata + PduCount for timeline diagnostics
		if let Ok(meta_bytes) = ctx
			.services
			.rooms
			.timeline
			.get_event_metadata(pdu.event_id())
			.await
		{
			obj.insert("__is_outlier".to_owned(), JsonValue::from(meta_bytes.is_outlier));
			obj.insert("__short_room_id".to_owned(), JsonValue::from(meta_bytes.short_room_id));
			obj.insert(
				"__local_topo_depth".to_owned(),
				JsonValue::from(meta_bytes.deprecated_local_topo_depth),
			);
			obj.insert(
				"__soft_failed".to_owned(),
				JsonValue::from(matches!(
					meta_bytes.status,
					conduwuit_service::rooms::pdu_metadata::EventStatus::SoftFailed(_)
				)),
			);
			obj.insert(
				"__rejected".to_owned(),
				JsonValue::from(matches!(
					meta_bytes.status,
					conduwuit_service::rooms::pdu_metadata::EventStatus::Rejected(_)
				)),
			);
		}

		// Stream ordering position (what /sync uses to iterate)
		if let Ok(count) = ctx
			.services
			.rooms
			.timeline
			.get_pdu_count(pdu.event_id())
			.await
		{
			match count {
				| conduwuit::matrix::pdu::PduCount::Normal(n) => {
					obj.insert("__pdu_count".to_owned(), JsonValue::from(n));
				},
				| conduwuit::matrix::pdu::PduCount::Backfilled(n) => {
					obj.insert("__pdu_count".to_owned(), JsonValue::from(n));
					obj.insert("__backfilled".to_owned(), JsonValue::Bool(true));
				},
			}
		}
	} else {
		is_separated = true;
	}

	Ok((obj, is_separated, shortstatehash))
}

impl DagExportStats {
	#[allow(clippy::too_many_arguments)]
	pub(super) async fn process_and_write_pdu(
		&mut self,
		ctx: &crate::context::Context<'_>,
		file: &mut tokio::fs::File,
		outliers_file: &mut tokio::fs::File,
		pdu_json: CanonicalJsonObject,
		pdu_result: Result<PduEvent>,
		is_outlier: bool,
		print: bool,
		merge_outliers: bool,
	) -> Result<()> {
		let pdu_opt = pdu_result.as_ref().ok();
		let (obj, is_separated, shortstatehash) =
			decorate_pdu_for_export(ctx, &pdu_json, pdu_opt, is_outlier).await?;
		let is_separated = is_separated && !merge_outliers;

		if let Ok(pdu) = &pdu_result {
			if !is_separated {
				if let Some(ssh) = shortstatehash {
					self.unique_hashes.insert(ssh);
					self.last_ssh = Some(ssh);
				} else {
					self.missing_hash = self.missing_hash.saturating_add(1);
				}

				if pdu.state_key.is_some() {
					self.state_events = self.state_events.saturating_add(1);
					self.last_is_state_event = true;
					self.last_event_type = Some(pdu.kind().clone());
					self.last_state_key = pdu.state_key.as_ref().map(|k| format!("{k}"));
				} else {
					self.last_is_state_event = false;
				}

				self.last_event_id = Some(pdu.event_id().into());
				let eid = pdu.event_id().to_owned();
				self.all_event_ids.insert(eid.clone());
				let mut prevs = Vec::new();
				for prev in pdu.prev_events() {
					self.referenced_as_prev.insert(prev.to_owned());
					prevs.push(prev.to_owned());
				}
				self.all_events_prevs.insert(eid, prevs);
				let d: u64 = pdu.depth.into();
				self.max_depth = self.max_depth.max(d);
				self.min_depth = self.min_depth.min(d);
			}
		}

		let json = serde_json::to_string(&obj)?;

		if is_separated {
			outliers_file.write_all(json.as_bytes()).await?;
			outliers_file.write_all(b"\n").await?;
		} else {
			file.write_all(json.as_bytes()).await?;
			file.write_all(b"\n").await?;
			self.count = self.count.saturating_add(1);
			if let Ok(pdu) = &pdu_result {
				self.total_prev_events = self
					.total_prev_events
					.saturating_add(u64::try_from(pdu.prev_events().count()).unwrap_or(0));
			}
		}

		if print {
			ctx.write_str(&format!("{json}\n")).await?;
		}

		Ok(())
	}
}

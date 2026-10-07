//! Read-only structural classification of a single room's prev/auth DAG.
//!
//! Part of `check-rooms --deep`. This pass never writes to the database and
//! decodes no PDUs; it only classifies each short-ID reference by consulting
//! the derived indexes and `eventid_metadata`. It exists to tell the
//! difference between an edge that is fine (accepted/bridge parent), an edge
//! into a known outlier, an edge that is mapped but has no stored PDU, and an
//! edge whose short id is not mapped at all.

use std::collections::{HashMap, HashSet};

use conduwuit_core::Result;
use futures::{StreamExt, stream};
use slipstream::{OwnedEventId, RoomId};

use super::Service;
use crate::rooms::short::ShortEventId;

/// How a referenced short event id relates to the room's stored event set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParentClass {
	/// A live, non-outlier, non-rejected event.
	Accepted,
	/// A rejected or soft-failed event: transparent to the accepted graph.
	Bridge,
	/// Present in `eventid_pdu` but flagged as an outlier.
	Outlier,
	/// Short id maps back to an event id, but no PDU/metadata is stored.
	Dangling,
	/// Short id has no reverse mapping at all.
	Unmapped,
}

/// Counts produced by [`Service::audit_room_dag`]. All fields are read-only
/// observations; nothing here is a repair.
#[derive(Debug, Default, Clone)]
pub struct RoomDagAudit {
	pub scanned_events: u64,
	pub accepted_events: u64,
	pub bridge_events: u64,
	pub outlier_events: u64,
	pub unknown_events: u64,
	pub prev_edges: u64,
	pub prev_accepted: u64,
	pub prev_bridge: u64,
	pub prev_outlier: u64,
	pub prev_dangling: u64,
	pub prev_unmapped: u64,
	pub prev_missing_rows: u64,
	pub auth_edges: u64,
	pub auth_accepted: u64,
	pub auth_bridge: u64,
	pub auth_outlier: u64,
	pub auth_dangling: u64,
	pub auth_unmapped: u64,
	pub auth_missing_rows: u64,
}

impl RoomDagAudit {
	#[must_use]
	pub fn summary(&self) -> String {
		format!(
			"STRUCT DAG reached={} accepted={} bridge={} outlier={} unknown={}; prev edges={} \
			 (acc={} br={} out={} dangling={} unmapped={} missing_rows={}); auth edges={} \
			 (acc={} br={} out={} dangling={} unmapped={} missing_rows={})",
			self.scanned_events,
			self.accepted_events,
			self.bridge_events,
			self.outlier_events,
			self.unknown_events,
			self.prev_edges,
			self.prev_accepted,
			self.prev_bridge,
			self.prev_outlier,
			self.prev_dangling,
			self.prev_unmapped,
			self.prev_missing_rows,
			self.auth_edges,
			self.auth_accepted,
			self.auth_bridge,
			self.auth_outlier,
			self.auth_dangling,
			self.auth_unmapped,
			self.auth_missing_rows,
		)
	}
}

impl Service {
	/// Classifies the room's prev/auth DAG without mutating anything. Iterates
	/// the room's timeline events, reads only derived indexes and metadata, and
	/// counts where each referenced parent lands.
	pub async fn audit_room_dag(&self, room_id: &RoomId) -> RoomDagAudit {
		let mut audit = RoomDagAudit::default();
		let mut classes: HashMap<ShortEventId, ParentClass> = HashMap::new();

		let room_ids = self.db.room_shorteventids_rev(room_id, None).chunks(1024);
		let mut stream = std::pin::pin!(room_ids);

		while let Some(chunk) = stream.next().await {
			let short_ids: Vec<ShortEventId> = chunk.into_iter().filter_map(Result::ok).collect();
			if short_ids.is_empty() {
				continue;
			}

			audit.scanned_events = audit
				.scanned_events
				.saturating_add(u64::try_from(short_ids.len()).unwrap_or(u64::MAX));

			let prevs: Vec<Result<Vec<ShortEventId>>> = self
				.db
				.multi_get_shortprevevents(stream::iter(short_ids.clone()))
				.collect()
				.await;
			let auths: Vec<Result<Vec<ShortEventId>>> = self
				.db
				.multi_get_shortauthevents(stream::iter(short_ids.clone()))
				.collect()
				.await;

			// Classify the reached events themselves, then every referenced id.
			self.classify_shortids(&short_ids, &mut classes).await;
			for short in &short_ids {
				audit.accepted_events = audit.accepted_events.saturating_add(u64::from(
					matches!(classes.get(short), Some(ParentClass::Accepted)),
				));
				audit.bridge_events = audit.bridge_events.saturating_add(u64::from(matches!(
					classes.get(short),
					Some(ParentClass::Bridge)
				)));
				audit.outlier_events = audit.outlier_events.saturating_add(u64::from(matches!(
					classes.get(short),
					Some(ParentClass::Outlier)
				)));
				audit.unknown_events = audit.unknown_events.saturating_add(u64::from(matches!(
					classes.get(short),
					None | Some(ParentClass::Dangling | ParentClass::Unmapped)
				)));
			}

			let mut targets: Vec<ShortEventId> = Vec::new();
			for (index, _) in short_ids.iter().enumerate() {
				match prevs.get(index) {
					| Some(Ok(parents)) => targets.extend(parents.iter().copied()),
					| Some(Err(_)) =>
						audit.prev_missing_rows = audit.prev_missing_rows.saturating_add(1),
					| None => {},
				}
				match auths.get(index) {
					| Some(Ok(auth)) => targets.extend(auth.iter().copied()),
					| Some(Err(_)) =>
						audit.auth_missing_rows = audit.auth_missing_rows.saturating_add(1),
					| None => {},
				}
			}
			self.classify_shortids(&targets, &mut classes).await;

			for (index, _) in short_ids.iter().enumerate() {
				if let Some(Ok(parents)) = prevs.get(index) {
					for target in parents {
						audit.prev_edges = audit.prev_edges.saturating_add(1);
						bump(&mut audit, false, classes.get(target));
					}
				}
				if let Some(Ok(auth)) = auths.get(index) {
					for target in auth {
						audit.auth_edges = audit.auth_edges.saturating_add(1);
						bump(&mut audit, true, classes.get(target));
					}
				}
			}
		}

		audit
	}

	/// Resolves and classifies any not-yet-cached short ids, batching the
	/// reverse lookup, metadata read, and verdict read per call.
	async fn classify_shortids(
		&self,
		shorts: &[ShortEventId],
		classes: &mut HashMap<ShortEventId, ParentClass>,
	) {
		let mut unique: Vec<ShortEventId> = Vec::new();
		let mut seen: HashSet<ShortEventId> = HashSet::new();
		for short in shorts {
			if !classes.contains_key(short) && seen.insert(*short) {
				unique.push(*short);
			}
		}
		if unique.is_empty() {
			return;
		}

		let resolved: Vec<Result<OwnedEventId>> = self
			.services
			.short
			.multi_get_eventid_from_short(stream::iter(unique.iter().copied()))
			.collect()
			.await;

		let metadata_ids: Vec<OwnedEventId> = resolved
			.iter()
			.filter_map(|result| result.as_ref().ok().cloned())
			.collect();
		let mut metadata = self
			.db
			.get_event_metadata_batch(&metadata_ids)
			.await
			.into_iter();

		let mut verdict_pairs: Vec<(ShortEventId, OwnedEventId)> = Vec::new();
		for (short, result) in unique.iter().zip(resolved) {
			let Ok(event_id) = result else {
				classes.insert(*short, ParentClass::Unmapped);
				continue;
			};

			match metadata.next() {
				| Some(Ok(meta)) if meta.is_outlier => {
					classes.insert(*short, ParentClass::Outlier);
				},
				| Some(Ok(_)) => verdict_pairs.push((*short, event_id)),
				| _ => {
					classes.insert(*short, ParentClass::Dangling);
				},
			}
		}

		if verdict_pairs.is_empty() {
			return;
		}

		let verdict_ids: Vec<OwnedEventId> = verdict_pairs
			.iter()
			.map(|(_, event_id)| event_id.clone())
			.collect();
		let flagged = self
			.services
			.pdu_metadata
			.verdict_flagged_batch(&verdict_ids)
			.await;

		for (short, event_id) in verdict_pairs {
			classes.insert(
				short,
				if flagged.contains(&event_id) {
					ParentClass::Bridge
				} else {
					ParentClass::Accepted
				},
			);
		}
	}
}

fn bump(audit: &mut RoomDagAudit, auth: bool, class: Option<&ParentClass>) {
	if auth {
		match class {
			| Some(ParentClass::Accepted) => {
				audit.auth_accepted = audit.auth_accepted.saturating_add(1);
			},
			| Some(ParentClass::Bridge) => {
				audit.auth_bridge = audit.auth_bridge.saturating_add(1);
			},
			| Some(ParentClass::Outlier) => {
				audit.auth_outlier = audit.auth_outlier.saturating_add(1);
			},
			| Some(ParentClass::Dangling) => {
				audit.auth_dangling = audit.auth_dangling.saturating_add(1);
			},
			| _ => {
				audit.auth_unmapped = audit.auth_unmapped.saturating_add(1);
			},
		}
	} else {
		match class {
			| Some(ParentClass::Accepted) => {
				audit.prev_accepted = audit.prev_accepted.saturating_add(1);
			},
			| Some(ParentClass::Bridge) => {
				audit.prev_bridge = audit.prev_bridge.saturating_add(1);
			},
			| Some(ParentClass::Outlier) => {
				audit.prev_outlier = audit.prev_outlier.saturating_add(1);
			},
			| Some(ParentClass::Dangling) => {
				audit.prev_dangling = audit.prev_dangling.saturating_add(1);
			},
			| _ => {
				audit.prev_unmapped = audit.prev_unmapped.saturating_add(1);
			},
		}
	}
}

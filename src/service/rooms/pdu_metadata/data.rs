use std::{mem::size_of, sync::Arc};

use conduwuit::{
	Result,
	arrayvec::ArrayVec,
	err,
	matrix::{Event, PduCount},
	utils::{
		ReadyExt,
		stream::{TryIgnore, WidebandExt},
		string_from_bytes, u64_from_u8,
	},
};
use database::{Interfix, Map};
use futures::{Stream, StreamExt};
use ruma::{EventId, OwnedEventId, RoomId, UserId, api::Direction};

use crate::{
	Dep,
	rooms::{
		self,
		pdu_metadata::is_retryable_rejection_reason,
		short::{ShortEventId, ShortRoomId},
		timeline::{PduId, PdusIterItem, RawPduId},
	},
};

pub(super) struct Data {
	tofrom_relation: Arc<Map>,
	referencedevents: Arc<Map>,
	eventid_metadata: Arc<Map>,
	msc2836_children: Arc<Map>,
	msc2836_reported_children: Arc<Map>,
	services: Services,
}

/// MSC2836: children counts + hash as reported by a remote server's
/// `/event_relationships` response, kept when it exceeds what we know
/// ourselves. See [`Data::msc2836_get_reported_children`].
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub(super) struct Msc2836ReportedChildren {
	pub(super) counts: std::collections::BTreeMap<String, u64>,
	pub(super) hash: String,
}

struct Services {
	timeline: Dep<rooms::timeline::Service>,
}

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let db = &args.db;
		Self {
			tofrom_relation: db["tofrom_relation"].clone(),
			referencedevents: db["referencedevents"].clone(),
			eventid_metadata: db["eventid_metadata"].clone(),
			msc2836_children: db["msc2836_children"].clone(),
			msc2836_reported_children: db["msc2836_reported_children"].clone(),
			services: Services {
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
			},
		}
	}

	pub(super) fn add_relation(&self, from: u64, to: u64) {
		const BUFSIZE: usize = size_of::<u64>() * 2;

		let key: &[u64] = &[to, from];
		self.tofrom_relation.aput_raw::<BUFSIZE, _, _>(key, []);
	}

	pub(super) fn get_relations<'a>(
		&'a self,
		user_id: &'a UserId,
		shortroomid: ShortRoomId,
		target: ShortEventId,
		from: PduCount,
		dir: Direction,
	) -> impl Stream<Item = PdusIterItem> + Send + 'a {
		// Query from exact position then filter excludes it (saturating_inc could skip
		// events at min/max boundaries).
		//
		// Relations currently only index normal timeline counts. `PduCount::min()`
		// is a backfilled sentinel whose unsigned encoding lands far beyond any
		// normal count, which would make forward pagination from the beginning
		// return an empty stream.
		let from_unsigned = match (dir, from) {
			| (Direction::Forward, PduCount::Backfilled(_)) => 0,
			| _ => from.into_unsigned(),
		};
		let mut current = ArrayVec::<u8, 16>::new();
		current.extend(target.to_be_bytes());
		current.extend(from_unsigned.to_be_bytes());
		let current = current.as_slice();
		match dir {
			| Direction::Forward => self.tofrom_relation.raw_keys_from(current).boxed(),
			| Direction::Backward => self.tofrom_relation.rev_raw_keys_from(current).boxed(),
		}
		.ignore_err()
		.ready_take_while(move |key| key.starts_with(&target.to_be_bytes()))
		.map(|to_from| u64_from_u8(&to_from[8..16]))
		.map(PduCount::from_unsigned)
		.ready_filter(move |count| {
			if from == PduCount::min() || from == PduCount::max() {
				true
			} else {
				let count_unsigned = count.into_unsigned();
				match dir {
					| Direction::Forward => count_unsigned > from_unsigned,
					| Direction::Backward => count_unsigned < from_unsigned,
				}
			}
		})
		.wide_filter_map(move |shorteventid| async move {
			let pdu_id: RawPduId = PduId { shortroomid, shorteventid }.into();

			let mut pdu = self.services.timeline.get_pdu_from_id(&pdu_id).await.ok()?;

			pdu.as_mut_pdu().set_unsigned(Some(user_id));

			Some((shorteventid, pdu))
		})
	}

	#[inline]
	pub(super) fn mark_as_referenced<'a, I>(&self, room_id: &RoomId, event_ids: I)
	where
		I: Iterator<Item = &'a EventId>,
	{
		for prev in event_ids {
			let key = (room_id, prev);
			self.referencedevents.put_raw(key, []);
		}
	}

	pub(super) async fn is_event_referenced(&self, room_id: &RoomId, event_id: &EventId) -> bool {
		let key = (room_id, event_id);
		self.referencedevents.qry(&key).await.is_ok()
	}

	pub(super) fn mark_event_soft_failed(&self, event_id: &EventId, reason: &str) {
		let mut meta = if let Ok(metadata_bytes) = self.eventid_metadata.get_blocking(event_id) {
			rooms::timeline::EventMetadata::from_bincode(&metadata_bytes).unwrap_or_default()
		} else {
			// New metadata: events reaching this path without existing metadata
			// are always outliers (not yet in the timeline).
			rooms::timeline::EventMetadata { is_outlier: true, ..Default::default() }
		};

		if !meta.soft_failed || meta.soft_fail_reason.is_empty() {
			meta.soft_failed = true;
			reason.clone_into(&mut meta.soft_fail_reason);
			if let Ok(new_bytes) = bincode::serialize(&meta) {
				self.eventid_metadata.insert(event_id, new_bytes);
			}
		}
	}

	pub(super) async fn is_event_soft_failed(&self, event_id: &EventId) -> bool {
		if let Ok(metadata_bytes) = self.eventid_metadata.get(event_id).await {
			if let Ok(meta) = rooms::timeline::EventMetadata::from_bincode(&metadata_bytes) {
				return meta.soft_failed;
			}
		}
		false
	}

	pub(super) async fn get_soft_fail_reason(&self, event_id: &EventId) -> Option<String> {
		let metadata_bytes = self.eventid_metadata.get(event_id).await.ok()?;
		let meta = rooms::timeline::EventMetadata::from_bincode(&metadata_bytes).ok()?;
		if meta.soft_failed && !meta.soft_fail_reason.is_empty() {
			Some(meta.soft_fail_reason)
		} else {
			None
		}
	}

	pub(super) fn unmark_event_soft_failed(&self, event_id: &EventId) {
		if let Ok(metadata_bytes) = self.eventid_metadata.get_blocking(event_id) {
			if let Ok(mut meta) = rooms::timeline::EventMetadata::from_bincode(&metadata_bytes) {
				if meta.soft_failed {
					meta.soft_failed = false;
					if let Ok(new_bytes) = bincode::serialize(&meta) {
						self.eventid_metadata.insert(event_id, new_bytes);
					}
				}
			}
		}
	}

	pub(super) fn mark_event_rejected(&self, event_id: &EventId, reason: &str) {
		let mut meta = if let Ok(metadata_bytes) = self.eventid_metadata.get_blocking(event_id) {
			rooms::timeline::EventMetadata::from_bincode(&metadata_bytes).unwrap_or_default()
		} else {
			// New metadata: events reaching this path without existing metadata
			// are always outliers (not yet in the timeline).
			rooms::timeline::EventMetadata { is_outlier: true, ..Default::default() }
		};

		let new_reason_retryable = is_retryable_rejection_reason(reason);
		let existing_reason_retryable = !meta.rejection_reason.is_empty()
			&& is_retryable_rejection_reason(&meta.rejection_reason);

		// Keep the first rejection by default, but allow a later *permanent*
		// rejection to replace an earlier retryable placeholder. That prevents
		// retryable sentinels like `MissingAuthEvent` from masking the real
		// reason when an intrinsic validation failure is discovered later in
		// the same attempt.
		if !meta.rejected
			|| meta.rejection_reason.is_empty()
			|| (existing_reason_retryable && !new_reason_retryable)
		{
			meta.rejected = true;
			reason.clone_into(&mut meta.rejection_reason);
			if let Ok(new_bytes) = bincode::serialize(&meta) {
				self.eventid_metadata.insert(event_id, new_bytes);
			}
		}
	}

	pub(super) async fn try_get_rejection_reason(
		&self,
		event_id: &EventId,
	) -> Result<Option<String>> {
		let metadata_bytes = match self.eventid_metadata.get(event_id).await {
			| Ok(bytes) => bytes,
			| Err(e) if e.is_not_found() => return Ok(None),
			| Err(e) => return Err(e),
		};
		let meta = rooms::timeline::EventMetadata::from_bincode(&metadata_bytes)
			.map_err(|e| err!(Database("Failed to deserialize EventMetadata: {e}")))?;
		if meta.rejected && !meta.rejection_reason.is_empty() {
			Ok(Some(meta.rejection_reason))
		} else {
			Ok(None)
		}
	}

	pub(super) async fn is_event_rejected(&self, event_id: &EventId) -> bool {
		if let Ok(metadata_bytes) = self.eventid_metadata.get(event_id).await {
			if let Ok(meta) = rooms::timeline::EventMetadata::from_bincode(&metadata_bytes) {
				return meta.rejected;
			}
		}
		false
	}

	pub(super) fn unmark_event_rejected(&self, event_id: &EventId) {
		if let Ok(metadata_bytes) = self.eventid_metadata.get_blocking(event_id) {
			if let Ok(mut meta) = rooms::timeline::EventMetadata::from_bincode(&metadata_bytes) {
				if meta.rejected {
					meta.rejected = false;
					meta.rejection_reason.clear();
					if let Ok(new_bytes) = bincode::serialize(&meta) {
						self.eventid_metadata.insert(event_id, new_bytes);
					}
				}
			}
		}
	}

	/// Removes any soft-fail or rejection markers applied to the target PDU
	pub(super) fn clear_pdu_markers(&self, event_id: &EventId) {
		self.unmark_event_rejected(event_id);
		self.unmark_event_soft_failed(event_id);
	}

	/// MSC2836: index `child` as a relationship-child of `parent` with the
	/// given `rel_type` (from `content.m.relationship`).
	pub(super) fn msc2836_add_child(&self, parent: &EventId, child: &EventId, rel_type: &str) {
		let key = (parent, child);
		self.msc2836_children.put_raw(key, rel_type.as_bytes());
	}

	/// MSC2836: all known children of `parent`, as (child event ID, rel_type)
	/// pairs.
	pub(super) async fn msc2836_get_children(
		&self,
		parent: &EventId,
	) -> Vec<(OwnedEventId, String)> {
		let prefix = (parent, Interfix);
		self.msc2836_children
			.stream_prefix_raw(&prefix)
			.ignore_err()
			.filter_map(|(key, rel_type)| async move {
				let child_bytes = key.rsplit(|&b| b == database::SEP).next()?;
				let child = string_from_bytes(child_bytes).ok()?;
				let child = EventId::parse(&child).ok()?.to_owned();
				let rel_type = string_from_bytes(rel_type).ok()?;
				Some((child, rel_type))
			})
			.collect()
			.await
	}

	/// MSC2836: remember `counts`/`hash` as reported by a remote server for
	/// `event_id`, but only if their total exceeds what's already stored
	/// (spec: "the event with the higher children count should be
	/// persisted").
	pub(super) fn msc2836_set_reported_children(
		&self,
		event_id: &EventId,
		counts: &std::collections::BTreeMap<String, u64>,
		hash: &str,
	) {
		let new_total: u64 = counts.values().sum();
		let existing = self
			.msc2836_reported_children
			.get_blocking(event_id)
			.ok()
			.and_then(|bytes| bincode::deserialize::<Msc2836ReportedChildren>(&bytes).ok());
		let should_replace = existing.as_ref().is_none_or(|existing| {
			new_total > existing.counts.values().sum::<u64>()
				|| (new_total == existing.counts.values().sum::<u64>() && existing.hash != hash)
		});

		if should_replace {
			let record = Msc2836ReportedChildren {
				counts: counts.clone(),
				hash: hash.to_owned(),
			};
			if let Ok(bytes) = bincode::serialize(&record) {
				self.msc2836_reported_children.insert(event_id, bytes);
			}
		}
	}

	/// MSC2836: the remembered reported children (if any) for `event_id`.
	pub(super) async fn msc2836_get_reported_children(
		&self,
		event_id: &EventId,
	) -> Option<(std::collections::BTreeMap<String, u64>, String)> {
		let bytes = self.msc2836_reported_children.get(event_id).await.ok()?;
		let record = bincode::deserialize::<Msc2836ReportedChildren>(&bytes).ok()?;
		Some((record.counts, record.hash))
	}
}

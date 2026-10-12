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
use slipstream::{EventId, OwnedEventId, RoomId, UserId, api::Direction};

use crate::{
	Dep,
	rooms::{
		self,
		pdu_metadata::{RejectionCode, SoftFailCode},
		short::{ShortEventId, ShortRoomId},
		timeline::{PduId, PdusIterItem, RawPduId},
	},
};

pub(super) struct Data {
	tofrom_relation: Arc<Map>,
	referencedevents: Arc<Map>,
	msc2836_children: Arc<Map>,
	msc2836_reported_children: Arc<Map>,
	mtxdb_verdicts: conduwuit_mtxdb::verdicts::VerdictAdapter,
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
			msc2836_children: db["msc2836_children"].clone(),
			msc2836_reported_children: db["msc2836_reported_children"].clone(),
			mtxdb_verdicts: conduwuit_mtxdb::verdicts::VerdictAdapter::new(args.mtxdb.clone()),
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

	/// Persist an authoritative mtxdb verdict change. A failure must stop
	/// processing rather than falling back to legacy RocksDB marker maps.
	pub(super) fn update_verdict<F>(&self, event_id: &EventId, change: F) -> Result<()>
	where
		F: FnMut(&mut conduwuit_mtxdb::verdicts::Verdict),
	{
		self.mtxdb_verdicts
			.update(event_id.as_bytes(), change)
			.map_err(|error| {
				err!(Database("Failed to persist mtxdb verdict for {event_id}: {error}"))
			})
	}

	pub(super) fn mark_event_soft_failed(
		&self,
		event_id: &EventId,
		code: SoftFailCode,
	) -> Result<()> {
		self.update_verdict(event_id, |v| v.soft_fail = Some(code.to_u8()))
	}

	pub(super) fn is_event_soft_failed(&self, event_id: &EventId) -> Result<bool> {
		Ok(self.get_soft_fail_code(event_id)?.is_some())
	}

	pub(super) fn get_soft_fail_reason(&self, event_id: &EventId) -> Result<Option<String>> {
		Ok(self
			.get_soft_fail_code(event_id)?
			.map(|code| code.tag().to_owned()))
	}

	pub(super) fn unmark_event_soft_failed(&self, event_id: &EventId) -> Result<()> {
		self.update_verdict(event_id, |v| v.soft_fail = None)
	}

	pub(super) fn mark_event_rejected(&self, event_id: &EventId, reason: &str) -> Result<()> {
		let new_code = RejectionCode::parse(reason).unwrap_or(RejectionCode::Unknown);
		self.update_verdict(event_id, |verdict| {
			if verdict.rejection.is_some_and(|old| {
				!RejectionCode::from_u8(old).is_retryable() && new_code.is_retryable()
			}) {
				return;
			}
			verdict.rejection = Some(new_code.to_u8());
		})
	}

	pub(super) fn is_event_rejected(&self, event_id: &EventId) -> Result<bool> {
		Ok(self.get_rejection_code(event_id)?.is_some())
	}

	/// Read the event's rejection marker from the authoritative mtxdb verdict.
	pub(super) fn get_rejection_code(&self, event_id: &EventId) -> Result<Option<RejectionCode>> {
		self.mtxdb_verdicts
			.get(event_id.as_bytes())
			.map(|verdict| {
				verdict
					.and_then(|value| value.rejection)
					.map(RejectionCode::from_u8)
			})
			.map_err(|error| {
				err!(Database("Failed to read mtxdb rejection for {event_id}: {error}"))
			})
	}

	/// Read the event's soft-fail marker from the authoritative mtxdb verdict.
	pub(super) fn get_soft_fail_code(&self, event_id: &EventId) -> Result<Option<SoftFailCode>> {
		self.mtxdb_verdicts
			.get(event_id.as_bytes())
			.map(|verdict| {
				verdict
					.and_then(|value| value.soft_fail)
					.map(SoftFailCode::from_u8)
			})
			.map_err(|error| {
				err!(Database("Failed to read mtxdb soft-fail for {event_id}: {error}"))
			})
	}

	/// Returns the event IDs carrying a rejection or soft-fail marker.
	pub(super) fn verdict_flagged_batch(
		&self,
		event_ids: &[OwnedEventId],
	) -> Result<std::collections::HashSet<OwnedEventId>> {
		if event_ids.is_empty() {
			return Ok(std::collections::HashSet::new());
		}

		let event_id_refs: Vec<&[u8]> = event_ids.iter().map(OwnedEventId::as_bytes).collect();
		let verdicts = self
			.mtxdb_verdicts
			.get_many(&event_id_refs)
			.map_err(|error| err!(Database("Failed to batch-read mtxdb verdicts: {error}")))?;

		let mut flagged = std::collections::HashSet::new();
		for (event_id, verdict) in event_ids.iter().zip(verdicts) {
			if verdict.is_some_and(|value| value.rejection.is_some() || value.soft_fail.is_some())
			{
				flagged.insert(event_id.clone());
			}
		}

		Ok(flagged)
	}

	pub(super) fn unmark_event_rejected(&self, event_id: &EventId) -> Result<()> {
		self.update_verdict(event_id, |v| v.rejection = None)
	}

	/// Removes any soft-fail or rejection markers applied to the target PDU
	pub(super) fn clear_pdu_markers(&self, event_id: &EventId) -> Result<()> {
		self.update_verdict(event_id, |v| {
			v.soft_fail = None;
			v.rejection = None;
		})
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
				let child = EventId::parse(&child).ok()?;
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

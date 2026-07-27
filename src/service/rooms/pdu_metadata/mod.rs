mod bundled_aggregations;
mod data;
use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use conduwuit::{Result, debug, matrix::PduCount};
use futures::{StreamExt, future::try_join};
use ruma::{EventId, OwnedEventId, RoomId, UserId, api::Direction};
use sha2::{Digest, Sha256};

use self::data::Data;
use crate::{
	Dep,
	rooms::{self, timeline::PdusIterItem},
};

pub struct Service {
	services: Services,
	db: Data,
}

struct Services {
	short: Dep<rooms::short::Service>,
	timeline: Dep<rooms::timeline::Service>,
	state_accessor: Dep<rooms::state_accessor::Service>,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			services: Services {
				short: args.depend::<rooms::short::Service>("rooms::short"),
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
				state_accessor: args
					.depend::<rooms::state_accessor::Service>("rooms::state_accessor"),
			},
			db: Data::new(&args),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	#[tracing::instrument(skip(self, from, to), level = "debug")]
	pub fn add_relation(&self, from: PduCount, to: PduCount) {
		match (from, to) {
			| (PduCount::Normal(f), PduCount::Normal(t)) => self.db.add_relation(f, t),
			| _ => {
				// TODO: Relations with backfilled pdus
			},
		}
	}

	#[allow(clippy::too_many_arguments)]
	pub async fn get_relations<'a>(
		&'a self,
		user_id: &'a UserId,
		room_id: &'a RoomId,
		target: &'a EventId,
		from: PduCount,
		limit: usize,
		max_depth: u8,
		dir: Direction,
	) -> Vec<PdusIterItem> {
		let room_id = self.services.short.get_shortroomid(room_id);

		let target = self.services.timeline.get_pdu_count(target);

		let Ok((room_id, target)) = try_join(room_id, target).await else {
			return Vec::new();
		};

		let target = match target {
			| PduCount::Normal(c) => c,
			// TODO: Support backfilled relations
			| _ => 0, // This will result in an empty iterator
		};

		let mut pdus: Vec<_> = self
			.db
			.get_relations(user_id, room_id, target, from, dir)
			.collect()
			.await;

		let mut stack: Vec<_> = pdus
			.iter()
			.filter(|_| max_depth > 0)
			.map(|pdu| (pdu.clone(), 1))
			.collect();

		'limit: while let Some(stack_pdu) = stack.pop() {
			let target = match stack_pdu.0.0 {
				| PduCount::Normal(c) => c,
				// TODO: Support backfilled relations
				| PduCount::Backfilled(_) => 0, // This will result in an empty iterator
			};

			let relations: Vec<_> = self
				.db
				.get_relations(user_id, room_id, target, from, dir)
				.collect()
				.await;

			for relation in relations {
				if stack_pdu.1 < max_depth {
					stack.push((relation.clone(), stack_pdu.1.saturating_add(1)));
				}

				pdus.push(relation);
				if pdus.len() >= limit {
					break 'limit;
				}
			}
		}

		pdus
	}

	#[tracing::instrument(skip_all, level = "debug")]
	pub fn mark_as_referenced<'a, I>(&self, room_id: &RoomId, event_ids: I)
	where
		I: Iterator<Item = &'a EventId>,
	{
		self.db.mark_as_referenced(room_id, event_ids);
	}

	#[inline]
	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn is_event_referenced(&self, room_id: &RoomId, event_id: &EventId) -> bool {
		self.db.is_event_referenced(room_id, event_id).await
	}

	#[inline]
	#[tracing::instrument(skip(self), level = "debug")]
	pub fn mark_event_soft_failed(&self, event_id: &EventId, reason: &str) {
		self.db.mark_event_soft_failed(event_id, reason);
	}

	pub async fn get_soft_fail_reason(&self, event_id: &EventId) -> Option<String> {
		self.db.get_soft_fail_reason(event_id).await
	}

	#[inline]
	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn is_event_soft_failed(&self, event_id: &EventId) -> bool {
		self.db.is_event_soft_failed(event_id).await
	}

	pub async fn is_event_rejected(&self, event_id: &EventId) -> bool {
		self.db.is_event_rejected(event_id).await
	}

	pub async fn mark_event_rejected(&self, event_id: &EventId, reason: &str) {
		// Only protect events that are truly on the timeline (have a PduCount
		// ordering entry), not outliers. The eventid_pdu table is shared by
		// both timeline and outlier paths, so pdu_exists would incorrectly
		// prevent rejection of outliers that were just added.
		if self.is_event_visible_to_clients(event_id).await {
			conduwuit::warn!(
				%event_id,
				%reason,
				"refusing to reject timeline event (already passed auth)"
			);
			return;
		}
		self.db.mark_event_rejected(event_id, reason);
	}

	pub fn unmark_event_soft_failed(&self, event_id: &EventId) {
		self.db.unmark_event_soft_failed(event_id);
	}

	pub fn unmark_event_rejected(&self, event_id: &EventId) {
		self.db.unmark_event_rejected(event_id);
	}

	/// Returns true if the event is not rejected. Soft-failed events ARE
	/// accepted for auth purposes (used in federation/state-res contexts).
	pub async fn is_event_accepted(&self, event_id: &EventId) -> bool {
		!self.db.is_event_rejected(event_id).await
	}

	/// Returns true if the event is in the timeline and should be visible
	/// to clients. Events only in the outlier store (rejected, pending,
	/// etc.) are not visible.
	pub async fn is_event_visible_to_clients(&self, event_id: &EventId) -> bool {
		self.services.timeline.get_pdu_id(event_id).await.is_ok()
	}

	pub async fn get_rejection_reason(&self, event_id: &EventId) -> Option<String> {
		self.db.get_rejection_reason(event_id).await
	}

	pub fn clear_pdu_markers(&self, event_id: &EventId) { self.db.clear_pdu_markers(event_id); }

	/// MSC2836: record that `child` relates to `parent` via `rel_type`
	/// (`content.m.relationship`).
	pub fn msc2836_add_child(&self, parent: &EventId, child: &EventId, rel_type: &str) {
		self.db.msc2836_add_child(parent, child, rel_type);
	}

	/// MSC2836: all known children of `parent`, as (child event ID, rel_type)
	/// pairs.
	pub async fn msc2836_get_children(&self, parent: &EventId) -> Vec<(OwnedEventId, String)> {
		self.db.msc2836_get_children(parent).await
	}

	/// MSC2836: purely local children counts + hash for `event_id`, from our
	/// own directly-known child edges only (no remote-reported data mixed
	/// in). See [`Self::msc2836_children_unsigned`] for the combined view.
	async fn msc2836_local_children(
		&self,
		event_id: &EventId,
	) -> (std::collections::BTreeMap<String, u64>, String) {
		let known = self.db.msc2836_get_children(event_id).await;

		let mut counts = std::collections::BTreeMap::<String, u64>::new();
		let mut ids = Vec::with_capacity(known.len());
		for (child, rel_type) in &known {
			let count = counts.entry(rel_type.clone()).or_insert(0);
			*count = count.saturating_add(1);
			ids.push(child.to_string());
		}
		ids.sort_unstable();
		ids.dedup();

		let hash = STANDARD_NO_PAD.encode(Sha256::digest(ids.concat().as_bytes()));
		debug!(%event_id, ?ids, "MSC2836 local child IDs");

		(counts, hash)
	}

	/// MSC2836: the `unsigned.children` / `unsigned.children_hash` values to
	/// report for `event_id`, combining our own directly-known child edges
	/// with whatever a remote server has reported for this event (using
	/// whichever total is higher, per the MSC).
	pub async fn msc2836_children_unsigned(
		&self,
		event_id: &EventId,
	) -> (std::collections::BTreeMap<String, u64>, String) {
		let (counts, hash) = self.msc2836_local_children(event_id).await;
		let local_total: u64 = counts.values().sum();
		debug!(%event_id, ?counts, %hash, "MSC2836 local child metadata");

		if let Some((reported_counts, reported_hash)) =
			self.db.msc2836_get_reported_children(event_id).await
		{
			let reported_total: u64 = reported_counts.values().sum();
			debug!(%event_id, ?reported_counts, %reported_hash, "MSC2836 reported child metadata");
			if reported_total > local_total
				|| (reported_total == local_total && reported_hash != hash)
			{
				return (reported_counts, reported_hash);
			}
		}

		(counts, hash)
	}

	/// MSC2836: whether `event_id` has children we know about (via a remote
	/// report) but haven't fetched/indexed ourselves yet -- i.e. it's worth
	/// asking federation for an update. Per the MSC: unexplored if the
	/// reported count exceeds the locally-known count, or the counts match
	/// but the hashes differ.
	pub async fn msc2836_needs_explore(&self, event_id: &EventId) -> bool {
		let Some((reported_counts, reported_hash)) =
			self.db.msc2836_get_reported_children(event_id).await
		else {
			return false;
		};
		let (known_counts, known_hash) = self.msc2836_local_children(event_id).await;
		let reported_total: u64 = reported_counts.values().sum();
		let known_total: u64 = known_counts.values().sum();
		reported_total > known_total
			|| (reported_total == known_total && reported_hash != known_hash)
	}

	/// MSC2836: remember children counts/hash reported by a remote server
	/// for `event_id`, if higher than what's currently known/stored.
	pub fn msc2836_set_reported_children(
		&self,
		event_id: &EventId,
		counts: &std::collections::BTreeMap<String, u64>,
		hash: &str,
	) {
		self.db
			.msc2836_set_reported_children(event_id, counts, hash);
	}
}

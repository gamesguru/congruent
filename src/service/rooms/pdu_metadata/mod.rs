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

/// Machine-checkable classification of why an event was rejected during
/// outlier/auth processing.
///
/// This exists so retry classification never has to substring-match
/// arbitrary human prose — that broke silently in practice: `"auth event
/// {mid} is rejected"` (`handle_incoming_pdu.rs`) and `"depends on rejected
/// auth event {aid}"` (`handle_outlier_pdu.rs`) are the same underlying
/// cause (a cascading rejection, worth retrying if the dependency's own
/// verdict later changes), but only one of those two wordings matched the
/// old `.contains(...)` list. `tag()` is the single string persisted via
/// `mark_event_rejected` for a given variant, `parse` recovers the variant
/// from a stored reason by matching only that leading tag (not the
/// free-text detail after it), and `is_retryable` is a plain match — so
/// renaming a detail message, or adding a new call site, can't silently
/// change retry behavior the way a fresh literal string always could.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionCode {
	/// Signature verification failed against the origin's keys. Permanent —
	/// the same bytes will never verify differently.
	SignatureVerificationFailed,
	/// The PDU didn't parse into a valid `PduEvent` at all. Permanent — the
	/// data itself is malformed.
	InvalidPduFormat,
	/// This event's own auth chain includes an event we've already
	/// rejected. Permanent — deliberately *not* retryable, even though the
	/// dependency's own rejection might itself have been resolution-related
	/// and could later get un-rejected. This is a cascading judgment about
	/// *another* event's current state, not a resolution failure of this
	/// event's own; if the dependency's rejection is ever lifted, whatever
	/// later re-processes the dependency fresh will naturally re-derive
	/// this event's verdict too. Treating this tag itself as retryable
	/// would strip the permanence guarantee from every event that
	/// legitimately, permanently cascades from an intrinsically-bad auth
	/// event (Complement's
	/// `TestInboundFederationRejectsEventsWithRejectedAuthEvents`
	/// exists specifically to check that cascade stays permanent — mirrors
	/// Synapse's `AUTH_ERROR` being terminal).
	DependsOnRejectedAuthEvent,
	/// This event's prev_events point only at rejected predecessors.
	/// Retryable — the rejection is about the event's current resolution
	/// context, not the event bytes themselves. If the predecessor rejection
	/// is later cleared, a fresh attempt may be able to resolve state and
	/// auth correctly.
	PrevEventsRejected,
	/// We could not resolve one of this event's auth events at all (not
	/// locally, not via `/event_auth`, not via `/state_ids`). Retryable —
	/// this is a resolution failure, not evidence the event is invalid; a
	/// caller with more context (a fuller state snapshot, a successful
	/// backfill) may be able to supply the missing data.
	MissingAuthEvent,
	/// Two auth events share the same type+state_key. Permanent — a
	/// structural property of the claimed auth event set that can't change
	/// on retry.
	DuplicateAuthEventKey,
	/// No `m.room.create` in the resolved auth events (room versions that
	/// require one). Permanent for the same reason as above.
	MissingCreateEvent,
	/// The event's claimed auth events were all resolved, but the auth
	/// check against them failed. Permanent — re-running the same check
	/// against the same resolved auth events can't change the outcome.
	AuthCheckFailed,
	/// A `/get_missing_events` response contained a prev_event that failed
	/// canonical-JSON validation. Retryable — a different server, or a
	/// retry of the same server, may return well-formed data.
	StructurallyInvalidInGetMissingEvents,
	/// At least one of this event's prev_events was still unknown after the
	/// `/state_ids` fetch used to recover state (and a follow-up single-hop
	/// `/event` fetch) both failed to resolve it. Retryable — a subsequent
	/// attempt (e.g. once a sibling event fills in the gap, or federation
	/// recovers) may succeed where this one didn't.
	PrevEventUnknownStateIdsFailed,
	/// Every prev_event was present/known, but `/state_ids`-based state
	/// recovery still failed to produce a resolvable state snapshot (as
	/// opposed to [`Self::PrevEventUnknownStateIdsFailed`], which is about a
	/// prev_event that never got resolved at all). Retryable — this is a
	/// resolution failure, not evidence the event is invalid; a later
	/// attempt with a fuller state snapshot may succeed.
	StateResolutionFailedWithPrevsPresent,
}

impl RejectionCode {
	/// The stable string persisted via `mark_event_rejected` for this
	/// variant. Callers that want to attach dynamic detail (an event ID,
	/// say) should use [`Self::with_detail`] instead so the detail doesn't
	/// end up as part of what [`Self::parse`] has to match against.
	#[must_use]
	pub const fn tag(self) -> &'static str {
		match self {
			| Self::SignatureVerificationFailed => "signature_verification_failed",
			| Self::InvalidPduFormat => "invalid_pdu_format",
			| Self::DependsOnRejectedAuthEvent => "depends_on_rejected_auth_event",
			| Self::PrevEventsRejected => "prev_events_rejected",
			| Self::MissingAuthEvent => "missing_auth_event",
			| Self::DuplicateAuthEventKey => "duplicate_auth_event_key",
			| Self::MissingCreateEvent => "missing_create_event",
			| Self::AuthCheckFailed => "auth_check_failed",
			| Self::StructurallyInvalidInGetMissingEvents =>
				"structurally_invalid_in_get_missing_events",
			| Self::PrevEventUnknownStateIdsFailed => "prev_event_unknown_state_ids_failed",
			| Self::StateResolutionFailedWithPrevsPresent =>
				"state_resolution_failed_with_prevs_present",
		}
	}

	/// Whether this class of rejection is worth retrying once more context
	/// is available (see the variant docs above for the reasoning behind
	/// each one).
	#[must_use]
	pub const fn is_retryable(self) -> bool {
		matches!(
			self,
			Self::MissingAuthEvent
				| Self::PrevEventsRejected
				| Self::StructurallyInvalidInGetMissingEvents
				| Self::PrevEventUnknownStateIdsFailed
				| Self::StateResolutionFailedWithPrevsPresent
		)
	}

	/// Build the reason string to persist: this variant's stable tag,
	/// followed by a free-text detail for admin/debug readability (e.g. the
	/// specific event ID involved). Only the tag is ever matched back by
	/// [`Self::parse`] — the detail can say anything without risk of
	/// breaking retry classification.
	pub fn with_detail<D: std::fmt::Display>(self, detail: D) -> String {
		format!("{}: {detail}", self.tag())
	}

	/// Recover the code from a reason string previously produced by
	/// [`Self::tag`] or [`Self::with_detail`]. Matches only the leading
	/// tag (everything before the first `:`), so it's exact even as detail
	/// wording changes. Returns `None` for anything that isn't one of our
	/// known tags (e.g. a reason string predating this enum, or a typo) —
	/// callers should treat that as "not retryable" rather than guessing.
	#[must_use]
	pub fn parse(reason: &str) -> Option<Self> {
		let tag = reason.split(':').next().unwrap_or(reason).trim();
		[
			Self::SignatureVerificationFailed,
			Self::InvalidPduFormat,
			Self::DependsOnRejectedAuthEvent,
			Self::PrevEventsRejected,
			Self::MissingAuthEvent,
			Self::DuplicateAuthEventKey,
			Self::MissingCreateEvent,
			Self::AuthCheckFailed,
			Self::StructurallyInvalidInGetMissingEvents,
			Self::PrevEventUnknownStateIdsFailed,
			Self::StateResolutionFailedWithPrevsPresent,
		]
		.into_iter()
		.find(|code| code.tag() == tag)
	}
}

/// Returns true if a persisted rejection reason (from
/// `get_rejection_reason`) is worth retrying. See [`RejectionCode`] for the
/// classification and reasoning; a reason that doesn't parse as one of our
/// known codes is treated as not retryable (permanent) rather than guessed
/// at.
#[must_use]
pub fn is_retryable_rejection_reason(reason: &str) -> bool {
	RejectionCode::parse(reason).is_some_and(RejectionCode::is_retryable)
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

	/// Returns true if the event is rejected for a reason that should
	/// permanently cascade to dependents (`DependsOnRejectedAuthEvent`,
	/// etc.).
	///
	/// A bare `is_event_rejected` conflates two very different situations: an
	/// auth event that is intrinsically bad (bad signature, failed auth
	/// check -- permanent, must cascade) and one we merely failed to
	/// *resolve* in time (missing auth events, a degraded fetch --
	/// transient, see [`RejectionCode::is_retryable`]). Callers deciding
	/// whether to hard-cascade a rejection onto a dependent event (which
	/// itself gets marked with the permanent
	/// [`RejectionCode::DependsOnRejectedAuthEvent`]) must use this instead
	/// of a bare `is_event_rejected` check, or a purely transient rejection
	/// on the dependency permanently poisons every event that depends on it.
	pub async fn is_event_permanently_rejected(&self, event_id: &EventId) -> bool {
		// A single read: `get_rejection_reason` already returns `None` for
		// anything not currently rejected (see its impl in `data.rs`), so a
		// separate `is_event_rejected` pre-check only opens a TOCTOU window --
		// a concurrent retry (`take_retry_if_rejection_retryable`) clearing the
		// rejection between the two reads would make this method fall through
		// to `!retryable == true` on a stale `None` reason, i.e. report a
		// no-longer-rejected event as *permanently* rejected.
		let Some(reason) = self.get_rejection_reason(event_id).await else {
			return false;
		};
		!is_retryable_rejection_reason(&reason)
	}

	/// Returns true if the event is marked rejected specifically because
	/// *its own* auth-event chain never finished resolving
	/// ([`RejectionCode::MissingAuthEvent`]) -- as opposed to other
	/// retryable codes that get attached at a later pipeline stage, after
	/// this event's own outlier auth check already succeeded (e.g.
	/// `StateResolutionFailedWithPrevsPresent`,
	/// `PrevEventUnknownStateIdsFailed`, both set during timeline-upgrade
	/// state resolution, not outlier auth validation).
	///
	/// Callers deciding whether it's safe to reuse a stored outlier as
	/// *auth context* for another event (rather than just deciding whether
	/// to retry the event itself) must use this narrower check instead of
	/// [`is_retryable_rejection_reason`]: an event with e.g.
	/// `StateResolutionFailedWithPrevsPresent` already passed real auth
	/// validation and its stored content is trustworthy auth context, so
	/// treating it as "unresolved" would force a needless (and likely
	/// equally unsuccessful) re-fetch of an event that was never the
	/// problem.
	pub async fn is_event_pending_auth_resolution(&self, event_id: &EventId) -> bool {
		matches!(
			self.get_rejection_reason(event_id)
				.await
				.as_deref()
				.and_then(RejectionCode::parse),
			Some(RejectionCode::MissingAuthEvent)
		)
	}

	/// Returns true if `event_id` is currently marked rejected for a reason
	/// worth retrying (see [`is_retryable_rejection_reason`]), and if so,
	/// clears the rejection so the next processing attempt starts fresh
	/// instead of being permanently short-circuited by the stale verdict.
	///
	/// Callers that hit an "already known and rejected" gate before doing
	/// their own work (e.g. `handle_outlier_pdu`'s early return) should use
	/// this instead of a bare `is_event_rejected` check, so a caller with
	/// more context than whichever attempt originally rejected the event
	/// (a fuller state snapshot, a successful backfill, a later `/send`)
	/// gets a real chance to reach a different, correct verdict rather than
	/// inheriting a resolution failure that was never about the event
	/// itself being invalid.
	pub async fn take_retry_if_rejection_retryable(&self, event_id: &EventId) -> bool {
		// Single read for the same reason as `is_event_permanently_rejected`:
		// avoid a TOCTOU window between an `is_event_rejected` check and a
		// separate `get_rejection_reason` read.
		let Some(reason) = self.get_rejection_reason(event_id).await else {
			return false;
		};
		let retryable = is_retryable_rejection_reason(&reason);
		if retryable {
			self.unmark_event_rejected(event_id);
		}
		retryable
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

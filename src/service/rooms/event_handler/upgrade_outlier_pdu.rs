use std::{
	borrow::Borrow,
	collections::{BTreeMap, HashMap},
	time::Instant,
};

use conduwuit::{
	Err, Result, debug, debug_info, err, implement, info,
	matrix::{Event, EventTypeExt, PduEvent, StateKey, state_res},
	trace,
	utils::stream::IterStream,
	warn,
};
use futures::{StreamExt, TryStreamExt, future::ready};
use slipstream::{CanonicalJsonValue, OwnedEventId, RoomId, ServerName, events::StateEventType};

use super::{get_room_version_id, to_room_version};
use crate::rooms::{pdu_metadata::RejectionCode, timeline::RawPduId};

/// Authenticates an outlier against state at the event and attempts to append
/// it, resolving room state as needed. `is_timeline_event` enables
/// current-state auth checks and state advancement;
/// `predecessors_were_recovered` also forces state resolution for non-state
/// timeline events. `state_ids_anchor` is an optional predecessor or deeper
/// event used to recover missing state.
///
/// Returns the timeline ID, if appended, and whether state or predecessors were
/// recovered. An already-present event returns its ID and `false`. Reused state
/// missing the sender's membership may trigger a remote fetch.
///
/// # Errors
/// Propagates state recovery, resolution, storage, and append errors. Failed
/// auth returns `M_FORBIDDEN` and may persist a rejection; soft failures return
/// `M_INVALID_PARAM` and keep the event outside the visible timeline. Policy
/// server errors are ignored, while an explicit spam verdict soft-fails the
/// event.
#[implement(super::Service)]
#[allow(clippy::too_many_arguments)]
pub async fn upgrade_outlier_to_timeline_pdu<Pdu>(
	&self,
	incoming_pdu: PduEvent,
	val: BTreeMap<String, CanonicalJsonValue>,
	create_event: &Pdu,
	origin: &ServerName,
	room_id: &RoomId,
	is_timeline_event: bool,
	predecessors_were_recovered: bool,
	state_ids_anchor: Option<OwnedEventId>,
) -> Result<(Option<RawPduId>, bool)>
where
	Pdu: Event + Send + Sync,
{
	// Skip the PDU if we already have it as a timeline event
	if let Ok(pduid) = self
		.services
		.timeline
		.get_pdu_id(incoming_pdu.event_id())
		.await
	{
		return Ok((Some(pduid), false));
	}

	if self
		.services
		.pdu_metadata
		.is_event_soft_failed(incoming_pdu.event_id())?
	{
		return Err!(Request(InvalidParam("Event has been soft failed")));
	}

	debug!(
		event_id = %incoming_pdu.event_id,
		"Upgrading PDU from outlier to timeline"
	);
	let timer = Instant::now();
	let room_version_id = get_room_version_id(create_event)?;

	// 10. Fetch missing state and auth chain events by calling /state_ids at
	//     backwards extremities doing all the checks in this list starting at 1.
	//     These are not timeline events.

	debug!(
		event_id = %incoming_pdu.event_id,
		"Resolving state at event"
	);
	// Lift the enclosing flush boundary around state resolution and fetch_state
	// so that federation I/O (e.g. /state_ids round-trips) doesn't suppress
	// unrelated WAL flushes across the whole server.
	let state_at_incoming_event = self
		.services
		.timeline
		.without_cork(|| async {
			let state = if incoming_pdu.prev_events().count() == 1 {
				self.state_at_incoming_degree_one(&incoming_pdu, room_id)
					.await?
			} else {
				self.state_at_incoming_resolved(&incoming_pdu, room_id, &room_version_id)
					.await?
			};

			// A snapshot reused from a predecessor can predate the room's full
			// state: a predecessor promoted from a partial-state join keeps a
			// snapshot without any members. Judging this event against it would
			// reject it for want of the sender's membership, and the rejection
			// is stored, so a later backfill could never replace it. Ask the
			// peer for the state at this event instead.
			let state = match state {
				| Some(reused)
					if self
						.lacks_sender_membership(&incoming_pdu, room_id, &reused)
						.await =>
				{
					debug!(
						event_id = %incoming_pdu.event_id,
						"reused state has no membership for the sender; fetching state"
					);
					None
				},
				| other => other,
			};

			if state.is_none() {
				// A deeper anchor (the point before a gap) can fill in missing
				// events and auth-chain data. Its snapshot can also serve directly
				// when it is this event's sole predecessor; otherwise it predates
				// the gap and we need a snapshot for this event itself.
				let mut state_at_event = None;
				if let Some(anchor) = state_ids_anchor.as_ref() {
					let anchor_is_only_prev = incoming_pdu.prev_events().count() == 1
						&& incoming_pdu.prev_events().next() == Some(anchor);
					let anchor_fetch = self
						.fetch_state(origin, create_event, room_id, anchor, false)
						.await;
					info!(
						target: "state_at_event_debug",
						event_id = %incoming_pdu.event_id,
						%anchor,
						prev_events = ?incoming_pdu.prev_events().collect::<Vec<_>>(),
						anchor_is_only_prev,
						anchor_fetch = ?anchor_fetch.as_ref().map(|s| s.as_ref().map(HashMap::len)).map_err(ToString::to_string),
						"upgrade_outlier: deeper anchor fetch"
					);
					match anchor_fetch {
						| Ok(state) if anchor_is_only_prev => state_at_event = state,
						| Ok(Some(fetched))
							if incoming_pdu.prev_events().count() > 1
								&& incoming_pdu.prev_events().any(|prev| prev == anchor) =>
						{
							// The anchor is one of several predecessors: its fetched state
							// plus the others' own give the state before this event.
							state_at_event = self
								.state_at_incoming_with_fetched_prev(
									&incoming_pdu,
									room_id,
									&room_version_id,
									anchor,
									&fetched,
								)
								.await?;
						},
						| Ok(_) => {},
						| Err(e) => debug!(%anchor, "deeper /state_ids anchor fetch failed: {e}"),
					}
				}
				// State after the sole predecessor is exactly the state before this
				// event. Reuse it when the anchor is that predecessor; otherwise the
				// deeper anchor only warmed the event/auth stores and we still need a
				// snapshot for this event itself.
				if state_at_event.is_none() {
					state_at_event = self
						.fetch_state(
							origin,
							create_event,
							room_id,
							incoming_pdu.event_id(),
							false,
						)
						.await?;
				}
				Ok::<_, conduwuit::Error>((state_at_event, true))
			} else {
				Ok::<_, conduwuit::Error>((state, false))
			}
		})
		.await?;

	let (state_at_incoming_event, was_recovered) = state_at_incoming_event;
	let state_at_incoming_event =
		state_at_incoming_event.expect("we always set this to some above");

	// Diagnostic: record which state the auth check below will see. The sender's
	// membership is looked up by its canonical short key; a state map that holds
	// the sender under some other short key reports `sender_member_present=false`.
	{
		let sender_ssk = self
			.services
			.short
			.get_shortstatekey(&StateEventType::RoomMember, incoming_pdu.sender().as_str())
			.await
			.ok();
		let sender_member_present = sender_ssk
			.as_ref()
			.is_some_and(|ssk| state_at_incoming_event.contains_key(ssk));
		info!(
			target: "state_at_event_debug",
			event_id = %incoming_pdu.event_id,
			%room_id,
			prev_events = incoming_pdu.prev_events().count(),
			was_recovered = was_recovered,
			state_size = state_at_incoming_event.len(),
			?sender_ssk,
			sender_member_present = sender_member_present,
			"upgrade_outlier: state at event"
		);
	};

	let room_version = to_room_version(&room_version_id);
	debug!(
		event_id = %incoming_pdu.event_id,
		"Performing auth check to upgrade"
	);
	// 11. Check the auth of the event passes based on the state of the event
	// A partial-state room omits member events from its state, so the sender's
	// membership is absent. The event's own auth_events carry it; fall back to
	// those (only for partial rooms) instead of failing auth.
	let partial_room = self
		.services
		.state_partial
		.partial_server(room_id)
		.await
		.is_ok_and(|s| s.is_some());
	// A user leaving for themselves grants nothing, so it is judged by its own
	// auth_events even if the provisional state says they already left (e.g. after
	// a kick accepted on partial state); everything else is judged against the
	// state at the event, with the auth_events only filling gaps.
	let claimed_first = partial_room
		&& *incoming_pdu.kind() == slipstream::events::TimelineEventType::RoomMember
		&& incoming_pdu.state_key() == Some(incoming_pdu.sender().as_str())
		&& incoming_pdu
			.get_content::<slipstream::events::room::member::RoomMemberEventContent>()
			.is_ok_and(|c| {
				c.membership == slipstream::events::room::member::MembershipState::Leave
			});
	let partial_auth_events: Vec<PduEvent> = if partial_room {
		let mut v = Vec::new();
		for id in incoming_pdu.auth_events() {
			if let Ok(pdu) = self.services.timeline.get_pdu(id).await {
				v.push(pdu);
			}
		}
		v
	} else {
		Vec::new()
	};
	info!(
		target: "state_at_event_debug",
		event_id = %incoming_pdu.event_id,
		auth_events = incoming_pdu.auth_events().count(),
		resolved = partial_auth_events.len(),
		"partial auth fallback: resolved event auth_events"
	);
	let partial_auth_events = &partial_auth_events;
	let state_fetch_state = &state_at_incoming_event;
	let state_fetch = |k: StateEventType, s: StateKey| async move {
		if claimed_first {
			let claimed = partial_auth_events.iter().find(|p| {
				*p.kind() == slipstream::events::TimelineEventType::from(k.clone())
					&& p.state_key() == Some(s.as_str())
			});
			if let Some(claimed) = claimed {
				return Some(claimed.clone());
			}
		}
		let from_state = async {
			let shortstatekey = self.services.short.get_shortstatekey(&k, &s).await.ok()?;
			let event_id = state_fetch_state.get(&shortstatekey)?;
			self.services.timeline.get_pdu(event_id).await.ok()
		}
		.await;
		from_state.or_else(|| {
			partial_auth_events
				.iter()
				.find(|p| {
					*p.kind() == slipstream::events::TimelineEventType::from(k.clone())
						&& p.state_key() == Some(s.as_str())
				})
				.cloned()
		})
	};

	debug!(
		event_id = %incoming_pdu.event_id,
		"Running initial auth check"
	);
	let auth_check = state_res::event_auth::auth_check(
		&room_version,
		&incoming_pdu,
		None, // TODO: third party invite
		|ty, sk| state_fetch(ty.clone(), sk.into()),
		create_event.as_pdu(),
	)
	.await
	.map_err(|e| err!(Request(Forbidden("Auth check failed: {e:?}"))))?;

	if !auth_check {
		// Later events that name this one as a predecessor take the state from
		// before it; that only works if it is known to be rejected.
		self.services
			.pdu_metadata
			.mark_event_rejected(incoming_pdu.event_id(), RejectionCode::AuthCheckFailed.tag())?;
		return Err!(Request(Forbidden("Event has failed auth check with state at the event.")));
	}

	// 13. Use state resolution to find new room state

	// We start looking at current room state now, so lets lock the room
	trace!(
		room_id = %room_id,
		"Locking the room"
	);
	let state_lock = self.services.state.mutex.lock(room_id).await;

	// Re-check if the PDU was added to the timeline while we were waiting for the
	// lock
	if let Ok(pduid) = self
		.services
		.timeline
		.get_pdu_id(incoming_pdu.event_id())
		.await
	{
		return Ok((Some(pduid), false));
	}

	let mut soft_fail = if is_timeline_event {
		debug!(
			event_id = %incoming_pdu.event_id,
			"Gathering auth events"
		);
		let auth_events = self
			.services
			.state
			.get_auth_events(
				room_id,
				incoming_pdu.kind(),
				incoming_pdu.sender(),
				incoming_pdu.state_key(),
				incoming_pdu.content(),
				&room_version,
				&room_version_id,
			)
			.await?;

		let state_fetch = |k: &StateEventType, s: &str| {
			let key = k.with_state_key(s);
			let claimed = || {
				partial_auth_events
					.iter()
					.find(|p| {
						*p.kind() == slipstream::events::TimelineEventType::from(k.clone())
							&& p.state_key() == Some(s)
					})
					.cloned()
			};
			// See `claimed_first` above.
			if claimed_first {
				ready(claimed().or_else(|| auth_events.get(&key).map(ToOwned::to_owned)))
			} else {
				ready(
					auth_events
						.get(&key)
						.map(ToOwned::to_owned)
						.or_else(claimed),
				)
			}
		};

		debug!(
			event_id = %incoming_pdu.event_id,
			"Running auth check with claimed state auth"
		);
		let auth_check = state_res::event_auth::auth_check(
			&room_version,
			&incoming_pdu,
			None, // third-party invite
			state_fetch,
			create_event.as_pdu(),
		)
		.await
		.map_err(|e| err!(Request(Forbidden("Auth check failed: {e:?}"))))?;

		// Soft fail check before doing state res
		debug!(
			event_id = %incoming_pdu.event_id,
			"Performing soft-fail check"
		);
		match (auth_check, incoming_pdu.redacts_id(&room_version_id)) {
			| (false, _) => true,
			| (true, None) => false,
			| (true, Some(redact_id)) => {
				// A redaction of an event we don't have yet is accepted: whether the
				// sender may redact it can only be judged once the target arrives, so
				// soft-failing it here would drop a valid redaction.
				// TODO: If pending-redaction tracking is added, bound its per-room
				// entries and age. The current path stores no additional pending index;
				// the bound is needed for the future target-to-redaction lookup.
				self.services.timeline.get_pdu(&redact_id).await.is_ok()
					&& !self
						.services
						.state_accessor
						.user_can_redact(&redact_id, incoming_pdu.sender(), room_id, true)
						.await?
			},
		}
	} else {
		false
	};

	let (previous_root_handle, new_room_state, mut pending_nodes) =
		if let Some(state_key) = incoming_pdu.state_key() {
			debug!("Event is a state-event. Deriving new room state");

			// We also add state after incoming event to the fork states.
			let mut state_after = state_at_incoming_event.clone();
			let shortstatekey = self
				.services
				.short
				.get_or_create_shortstatekey(&incoming_pdu.kind().to_string().into(), state_key)
				.await;

			let event_id = incoming_pdu.event_id();
			state_after.insert(shortstatekey, event_id.to_owned());

			// `state_at_incoming_event` is, in the single-predecessor case,
			// materialized from the predecessor's own root handle, so reuse that
			// root instead of rebuilding and re-persisting an identical HAMT.
			// Fall back to a full rebuild for fork/state-resolution inputs and for
			// predecessors whose stored root predates their own state change.
			let (prev_root, mut prev_nodes) = match self
				.reusable_predecessor_root_handle(room_id, &incoming_pdu)
				.await?
			{
				| Some(root) => (root, Vec::new()),
				| None =>
					self.state_map_to_root_handle(room_id, &state_at_incoming_event)
						.await?,
			};
			let prev_root = Some(prev_root);

			// In a partial-state room the members needed to judge conflicting
			// forks are unknown, so state resolution would reject events that were
			// accepted on the strength of their own auth events. Apply the event to
			// the current state like Synapse does; the resync re-authorises it and
			// corrects the state if it was wrong.
			let partial_current = if partial_room {
				match self.services.state.get_room_state_hamt(room_id).await {
					| Ok(root) => self
						.services
						.state_accessor
						.state_full_ids_hamt(room_id, &root)
						.try_collect::<HashMap<u64, OwnedEventId>>()
						.await
						.ok(),
					| Err(_) => None,
				}
			} else {
				None
			};
			let (new_root, pending_nodes) = if let Some(mut current) = partial_current {
				current.insert(shortstatekey, event_id.to_owned());
				self.state_map_to_root_handle(room_id, &current).await?
			} else {
				self.resolve_state(room_id, &room_version_id, state_after)
					.await?
			};

			prev_nodes.extend(pending_nodes);
			(prev_root, Some(new_root), prev_nodes)
		} else if is_timeline_event && (was_recovered || predecessors_were_recovered) {
			let prev_root = self.services.state.get_room_state_hamt(room_id).await.ok();
			let (new_root, pending_nodes) = self
				.resolve_state(room_id, &room_version_id, state_at_incoming_event)
				.await?;

			(prev_root, Some(new_root), pending_nodes)
		} else if is_timeline_event {
			// Non-state timeline events reuse the predecessor root for historical
			// queries, but never advance the live room state.
			let (root, root_nodes) = match self
				.reusable_predecessor_root_handle(room_id, &incoming_pdu)
				.await?
			{
				| Some(root) => (root, Vec::new()),
				| None =>
					self.state_map_to_root_handle(room_id, &state_at_incoming_event)
						.await?,
			};

			(None, Some(root), root_nodes)
		} else {
			// Backfilled outliers only record their historical state root and do not
			// alter current room state.
			let (new_root, nodes) = self
				.state_map_to_root_handle(room_id, &state_at_incoming_event)
				.await?;

			(None, Some(new_root), nodes)
		};

	info!(room_id = %room_id, "Applying the resolved state transition");
	if !soft_fail {
		// Don't call the below checks on events that have already soft-failed, there's
		// no reason to re-calculate that.
		// 14-pre. If the event is not a state event, ask the policy server about it
		if incoming_pdu.state_key.is_none() {
			debug!(event_id = %incoming_pdu.event_id, "Checking policy server for event");
			// Lift the cork around the policy server round-trip.
			let mut pdu_object = incoming_pdu.to_canonical_object();
			match self
				.services
				.timeline
				.without_cork(|| {
					self.ask_policy_server(&incoming_pdu, &mut pdu_object, room_id, true)
				})
				.await
			{
				| Ok(false) => {
					warn!(
						event_id = %incoming_pdu.event_id,
						"Event has been marked as spam by policy server"
					);
					soft_fail = true;
				},
				| _ => {
					debug!(
						event_id = %incoming_pdu.event_id,
						"Event has passed policy server check or the policy server was unavailable."
					);
				},
			}
		}

		// Additionally, if this is a redaction for a soft-failed event, we soft-fail it
		// also.

		// TODO: this is supposed to hide redactions from policy servers, however, for
		// full efficacy it also needs to hide redactions for unknown events. This
		// needs to be investigated at a later time.
		if let Some(redact_id) = incoming_pdu.redacts_id(&room_version_id) {
			debug!(
				redact_id = %redact_id,
				"Checking if redaction is for a soft-failed event"
			);
			if self
				.services
				.pdu_metadata
				.is_event_soft_failed(&redact_id)?
			{
				warn!(
					redact_id = %redact_id,
					"Redaction is for a soft-failed event, soft failing the redaction"
				);
				soft_fail = true;
			}
		}
	}

	// Now we calculate the set of extremities this room has after the incoming
	// event has been applied, using the final soft-fail decision. Soft-failed
	// events must not modify the DAG tip set, and an ancestor being upgraded
	// only becomes a tip when it would otherwise leave the set empty.
	trace!("Calculating extremities");
	let current_extremities: Vec<OwnedEventId> = self
		.services
		.state
		.get_forward_extremities(room_id)
		.collect()
		.await;
	let prev_events: Vec<&slipstream::EventId> = incoming_pdu.prev_events().collect();
	let room_id_owned = room_id.to_owned();
	let is_referenced = |event_id: &slipstream::EventId| {
		let eid = event_id.to_owned();
		let rid = room_id_owned.clone();
		async move {
			self.services
				.pdu_metadata
				.is_event_referenced(&rid, &eid)
				.await
		}
	};
	let extremities = super::extremities::calculate_forward_extremities(
		current_extremities,
		incoming_pdu.event_id(),
		&prev_events,
		soft_fail,
		is_referenced,
		is_timeline_event,
	)
	.await;

	if soft_fail {
		info!(
			event_id = %incoming_pdu.event_id,
			"Soft failing event"
		);
		let extremities = extremities.iter().map(Borrow::borrow);
		debug_assert!(extremities.clone().count() > 0, "extremities not empty");
		let append_ctx = crate::rooms::timeline::AppendPduContext {
			state_lock: &state_lock,
			room_id,
			state_root_handle: new_room_state.clone(),
			pending_nodes,
			prev_state_root_handle: previous_root_handle.clone(),
			advance_current_state: is_timeline_event
				&& (incoming_pdu.state_key().is_some()
					|| was_recovered
					|| predecessors_were_recovered)
				&& new_room_state.is_some(),
			was_joined_before_state_install: None,
		};

		Box::pin(self.services.timeline.append_incoming_pdu(
			&incoming_pdu,
			val,
			extremities,
			soft_fail,
			false,
			append_ctx,
		))
		.await?;

		// Soft fail, we keep the event as an outlier but don't add it to the timeline
		self.services.pdu_metadata.mark_event_soft_failed(
			incoming_pdu.event_id(),
			crate::rooms::pdu_metadata::SoftFailCode::AuthCheckFailed,
		)?;

		warn!(
			event_id = %incoming_pdu.event_id,
			"Event was soft failed"
		);
		return Err!(Request(InvalidParam("Event has been soft failed")));
	}

	// The legacy force_state updated the joined-member/servers caches
	// (`roomserverids`) on state transitions. That cache update must be
	// preserved here, otherwise remote members that join the room are
	// never registered for outbound federation fan-out and locally-sent
	// events stop being delivered to their servers.
	// This runs only once every soft-fail check has passed so rejected events
	// never mutate derived caches.
	// We only update the derived caches; the HAMT root is committed
	// separately by set_event_state_with_root in append_pdu.
	if !soft_fail
		&& (incoming_pdu.state_key().is_some() || was_recovered || predecessors_were_recovered)
		&& let (Some(prev_root), Some(new_root)) =
			(previous_root_handle.as_ref(), new_room_state.as_ref())
	{
		// The cache delta resolves both roots through mtxdb. Make any deferred
		// roots traversable before diffing; append_pdu later publishes the root
		// pointers together with the event, while these nodes may safely remain
		// unrooted if cache reconciliation fails.
		self.services
			.state_hamt
			.store
			.publish_nodes(room_id.as_bytes(), &pending_nodes)?;
		pending_nodes.clear();

		Box::pin(self.services.state.update_caches_for_state_delta_between(
			room_id,
			Some(prev_root),
			new_root,
		))
		.await?;
	}

	// Now that the event has passed all auth it is added into the timeline.
	// We use the `state_at_event` instead of `state_after` so we accurately
	// represent the state for this event.
	trace!("Appending pdu to timeline");
	let extremities = extremities.iter().map(Borrow::borrow);
	debug_assert!(extremities.clone().count() > 0, "extremities not empty");
	let append_ctx = crate::rooms::timeline::AppendPduContext {
		state_lock: &state_lock,
		room_id,
		state_root_handle: new_room_state.clone(),
		pending_nodes,
		prev_state_root_handle: previous_root_handle.clone(),
		advance_current_state: is_timeline_event
			&& (incoming_pdu.state_key().is_some()
				|| was_recovered
				|| predecessors_were_recovered)
			&& new_room_state.is_some(),
		was_joined_before_state_install: None,
	};

	let pdu_id = Box::pin(self.services.timeline.append_incoming_pdu(
		&incoming_pdu,
		val,
		extremities,
		soft_fail,
		incoming_pdu.state_key.is_some(),
		append_ctx,
	))
	.await?;

	// Event has passed all auth/stateres checks
	drop(state_lock);
	debug_info!(
		elapsed = ?timer.elapsed(),
		"Accepted",
	);

	Ok((pdu_id, was_recovered || predecessors_were_recovered))
}

/// Returns the HAMT root representing the state at `incoming_pdu` when it can
/// be recovered from the incoming event's single predecessor, avoiding a full
/// rebuild from the short-state map.
///
/// `pdu_roothandle_after_event` yields the post-event root for timeline/migrated events,
/// which already includes the predecessor's own state change. Backfilled events
/// historically store the *pre*-event root, so confirm the predecessor's own
/// slot resolves to itself before trusting it; otherwise return `None` and let
/// the caller materialize the map. Because the predecessor's root has already
/// been persisted, this also avoids re-writing nodes on the hot path.
#[implement(super::Service)]
async fn reusable_predecessor_root_handle(
	&self,
	room_id: &RoomId,
	incoming_pdu: &PduEvent,
) -> Result<Option<rezzy::hamt::RootHandle>> {
	let mut prev_events = incoming_pdu.prev_events();
	let (Some(prev_event), None) = (prev_events.next(), prev_events.next()) else {
		return Ok(None);
	};

	// A missing predecessor PDU/root or an absent state slot is a legitimate
	// reason to rebuild. Any other failure (storage/transient) must propagate
	// rather than silently forcing the expensive fallback and masking it.
	let prev_pdu = match self
		.services
		.timeline
		.get_pdu_in_room(Some(room_id), prev_event)
		.await
	{
		| Ok(pdu) => pdu,
		| Err(e) if e.is_not_found() => return Ok(None),
		| Err(e) => return Err(e),
	};

	let root = match self
		.services
		.state_accessor
		.pdu_roothandle_after_event(prev_event)
		.await
	{
		| Ok(root) => root,
		| Err(e) if e.is_not_found() => return Ok(None),
		| Err(e) => return Err(e),
	};

	// Guard against backfilled pre-event roots: if the predecessor is a state
	// event, its own slot must resolve back to itself at this root.
	if let Some(state_key) = prev_pdu.state_key() {
		let event_type: StateEventType = prev_pdu.kind().to_string().into();
		let local = match self
			.services
			.state_accessor
			.state_get_in_room_hamt(room_id, &root, &event_type, state_key)
			.await
		{
			| Ok(local) => local,
			| Err(e) if e.is_not_found() => return Ok(None),
			| Err(e) => return Err(e),
		};
		if local.event_id() != prev_event {
			return Ok(None);
		}
	}

	Ok(Some(root))
}

#[implement(super::Service)]
pub async fn state_map_to_root_handle(
	&self,
	room_id: &RoomId,
	short_state: &HashMap<u64, OwnedEventId>,
) -> Result<(rezzy::hamt::RootHandle, Vec<crate::rooms::state_hamt::store::PendingNode>)> {
	let mut lattice = rezzy::incremental::LtHash::default();
	let mut entries = Vec::with_capacity(short_state.len());
	let mut short_state_keys = Vec::with_capacity(short_state.len());
	let mut event_ids = Vec::with_capacity(short_state.len());

	for (&shortstatekey, event_id) in short_state {
		short_state_keys.push(shortstatekey);
		event_ids.push(event_id.clone());
	}

	let string_keys: Vec<Result<(StateEventType, StateKey)>> = self
		.services
		.short
		.multi_get_statekey_from_short(short_state_keys.iter().copied().stream())
		.collect()
		.await;

	for ((shortstatekey, event_id), key_result) in
		short_state_keys.into_iter().zip(event_ids).zip(string_keys)
	{
		let event_id = event_id.as_ref();
		let shorteventid = self
			.services
			.short
			.get_or_create_shorteventid(event_id)
			.await;
		entries.push((shortstatekey, shorteventid));

		if let Ok((event_type, state_key)) = key_result {
			lattice.insert(
				event_type.to_string().as_str(),
				state_key.as_str(),
				event_id.as_str(),
			);
		}
	}

	let structural_key = crate::rooms::state_hamt::room_structural_key(
		&self.services.globals.server_secret,
		room_id,
	);
	let (root_handle, root_node) =
		rezzy::hamt::build_hamt_root_handle(&structural_key, &lattice, entries)
			.map_err(|e| err!(error!("Failed to build HAMT root: {e:?}")))?;

	let nodes = self
		.services
		.state_hamt
		.store
		.persist_node_recursive_deferred(room_id.as_bytes(), root_node)?;

	Ok((root_handle, nodes))
}

/// Whether a reused snapshot is too incomplete to judge an event by. A full
/// snapshot holds the membership of anyone who can send; it is not expected
/// for create events or self-joins/self-knocks (`exempt`), or while the room is
/// `partial`.
const fn snapshot_lacks_sender(exempt: bool, partial: bool, sender_present: bool) -> bool {
	!exempt && !partial && !sender_present
}

/// Whether `state` leaves out the membership of `pdu`'s sender.
/// Returns `false` for create events, self-joins, self-knocks, and
/// partial-state rooms. A failed membership-key lookup counts as absent
/// membership; a failed partial-state lookup counts as a full-state room.
#[implement(super::Service)]
async fn lacks_sender_membership(
	&self,
	pdu: &PduEvent,
	room_id: &RoomId,
	state: &HashMap<u64, OwnedEventId>,
) -> bool {
	let exempt = *pdu.kind() == slipstream::events::TimelineEventType::RoomCreate
		|| (*pdu.kind() == slipstream::events::TimelineEventType::RoomMember
			&& pdu.state_key() == Some(pdu.sender().as_str())
			// Only joins and knocks legitimately have no prior member; a
			// leave must find the membership it ends.
			&& pdu
				.get_content::<slipstream::events::room::member::RoomMemberEventContent>()
				.is_ok_and(|c| {
					matches!(
						c.membership,
						slipstream::events::room::member::MembershipState::Join
							| slipstream::events::room::member::MembershipState::Knock
					)
				}));

	let partial = self
		.services
		.state_partial
		.partial_server(room_id)
		.await
		.is_ok_and(|server| server.is_some());

	let sender_present = match self
		.services
		.short
		.get_shortstatekey(&StateEventType::RoomMember, pdu.sender().as_str())
		.await
	{
		| Ok(ssk) => state.contains_key(&ssk),
		| Err(_) => false,
	};

	snapshot_lacks_sender(exempt, partial, sender_present)
}

#[cfg(test)]
mod tests {
	use super::snapshot_lacks_sender;

	#[test]
	fn stale_partial_join_snapshot_is_incomplete() {
		// Predecessor promoted from a partial join, room since resynced.
		assert!(snapshot_lacks_sender(false, false, false));
	}

	#[test]
	fn snapshot_with_the_sender_is_complete() {
		assert!(!snapshot_lacks_sender(false, false, true));
	}

	#[test]
	fn partial_rooms_use_the_auth_events_fallback() {
		assert!(!snapshot_lacks_sender(false, true, false));
	}

	#[test]
	fn joins_and_creates_are_judged_without_the_sender() {
		assert!(!snapshot_lacks_sender(true, false, false));
	}
}

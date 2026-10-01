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
use futures::{StreamExt, future::ready};
use ruma::{CanonicalJsonValue, OwnedEventId, RoomId, ServerName, events::StateEventType};

use super::{get_room_version_id, to_room_version};
use crate::rooms::timeline::RawPduId;

#[implement(super::Service)]
pub async fn upgrade_outlier_to_timeline_pdu<Pdu>(
	&self,
	incoming_pdu: PduEvent,
	val: BTreeMap<String, CanonicalJsonValue>,
	create_event: &Pdu,
	origin: &ServerName,
	room_id: &RoomId,
	is_timeline_event: bool,
) -> Result<Option<RawPduId>>
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
		return Ok(Some(pduid));
	}

	if self
		.services
		.pdu_metadata
		.is_event_soft_failed(incoming_pdu.event_id())
		.await
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

			if state.is_none() {
				self.fetch_state(origin, create_event, room_id, incoming_pdu.event_id(), false)
					.await
			} else {
				Ok(state)
			}
		})
		.await?;

	let state_at_incoming_event =
		state_at_incoming_event.expect("we always set this to some above");

	let room_version = to_room_version(&room_version_id);

	debug!(
		event_id = %incoming_pdu.event_id,
		"Performing auth check to upgrade"
	);
	// 11. Check the auth of the event passes based on the state of the event
	let state_fetch_state = &state_at_incoming_event;
	let state_fetch = |k: StateEventType, s: StateKey| async move {
		let shortstatekey = self.services.short.get_shortstatekey(&k, &s).await.ok()?;

		let event_id = state_fetch_state.get(&shortstatekey)?;
		self.services.timeline.get_pdu(event_id).await.ok()
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
		return Ok(Some(pduid));
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
			ready(auth_events.get(&key).map(ToOwned::to_owned))
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
			| (true, Some(redact_id)) =>
				!self
					.services
					.state_accessor
					.user_can_redact(&redact_id, incoming_pdu.sender(), room_id, true)
					.await?,
		}
	} else {
		false
	};

	let (previous_root_handle, new_room_state) = if let Some(state_key) = incoming_pdu.state_key()
	{
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
		let prev_root = Some(
			match self
				.reusable_predecessor_root_handle(room_id, &incoming_pdu)
				.await?
			{
				| Some(root) => root,
				| None =>
					self.state_map_to_root_handle(room_id, &state_at_incoming_event)
						.await?,
			},
		);
		let new_root = Some(
			self.resolve_state(room_id, &room_version_id, state_after)
				.await?,
		);

		(prev_root, new_root)
	} else if is_timeline_event {
		// Non-state timeline events may introduce recovered state (e.g. via /state_ids).
		// Resolve the state at the incoming event against current room state so that
		// newly discovered state is adopted without evicting concurrent local state.
		let prev_root = self.services.state.get_room_state_hamt(room_id).await.ok();
		let new_root = Some(
			self.resolve_state(room_id, &room_version_id, state_at_incoming_event)
				.await?,
		);

		(prev_root, new_root)
	} else {
		// Outlier non-state events (backfill, etc.) only record their historical state root.
		let new_root = Some(
			self.state_map_to_root_handle(room_id, &state_at_incoming_event)
				.await?,
		);

		(None, new_root)
	};

	info!(room_id = %room_id, "Applying the resolved state transition");
	// The legacy force_state updated the joined-member/servers caches
	// (`roomserverids`) on state transitions. That cache update must be
	// preserved here, otherwise remote members that join the room are
	// never registered for outbound federation fan-out and locally-sent
	// events stop being delivered to their servers.
	// We only update the derived caches; the HAMT root is committed
	// separately by set_event_state_with_root in append_pdu.
	if !soft_fail
		&& let (Some(prev_root), Some(new_root)) =
			(previous_root_handle.as_ref(), new_room_state.as_ref())
	{
		Box::pin(self.services.state.update_caches_for_state_delta_between(
			room_id,
			Some(prev_root),
			new_root,
		))
		.await?;
	}

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
				.is_event_soft_failed(&redact_id)
				.await
			{
				warn!(
					redact_id = %redact_id,
					"Redaction is for a soft-failed event, soft failing the redaction"
				);
				soft_fail = true;
			}
		}
	}

	// 14. Check if the event passes auth based on the "current state" of the room,
	//     if not soft fail it
	let append_ctx = crate::rooms::timeline::AppendPduContext {
		state_lock: &state_lock,
		room_id,
		state_root_handle: new_room_state.clone(),
		prev_state_root_handle: previous_root_handle.clone(),
		advance_current_state: is_timeline_event
			&& incoming_pdu.state_key().is_none()
			&& new_room_state.is_some(),
		was_joined_before_state_install: None,
	};

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
	let prev_events: Vec<&ruma::EventId> = incoming_pdu.prev_events().collect();
	let room_id_owned = room_id.to_owned();
	let is_referenced = |event_id: &ruma::EventId| {
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
		);

		warn!(
			event_id = %incoming_pdu.event_id,
			"Event was soft failed"
		);
		return Err!(Request(InvalidParam("Event has been soft failed")));
	}

	// Now that the event has passed all auth it is added into the timeline.
	// We use the `state_at_event` instead of `state_after` so we accurately
	// represent the state for this event.
	trace!("Appending pdu to timeline");
	let extremities = extremities.iter().map(Borrow::borrow);
	debug_assert!(extremities.clone().count() > 0, "extremities not empty");

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

	Ok(pdu_id)
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
#[tracing::instrument(level = "debug", skip_all)]
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
#[tracing::instrument(level = "debug", skip_all)]
pub async fn state_map_to_root_handle(
	&self,
	room_id: &RoomId,
	short_state: &HashMap<u64, OwnedEventId>,
) -> Result<rezzy::hamt::RootHandle> {
	let mut lattice = rezzy::state::LtHash::default();
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

	for ((shortstatekey, event_id), key_result) in short_state_keys
		.into_iter()
		.zip(event_ids.into_iter())
		.zip(string_keys.into_iter())
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

	self.services.globals.with_cork_and_flush(|| {
		self.services
			.state_hamt
			.store
			.persist_node_recursive(root_node);
	});

	Ok(root_handle)
}

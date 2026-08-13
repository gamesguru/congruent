use std::{
	borrow::Borrow,
	collections::{BTreeMap, HashMap},
	sync::Arc,
	time::Instant,
};

use conduwuit::{
	Err, Result, debug, debug_info, debug_warn, err, implement, info,
	matrix::{Event, PduEvent, StateKey, state_res},
	trace,
	utils::stream::ReadyExt,
	warn,
};
use futures::{FutureExt, StreamExt, future::ready};
use ruma::{
	CanonicalJsonValue, OwnedEventId, RoomId, RoomVersionId, ServerName, events::StateEventType,
};

use super::{get_room_version_id, to_room_version};
use crate::rooms::{
	pdu_metadata::RejectionCode, short::ShortStateHash,
	state_compressor::HashSetCompressStateEvent, timeline::RawPduId,
};

/// Upgrade an outlier PDU to a full timeline event.
///
/// Performs auth checks, state resolution, soft-fail evaluation, and finally
/// appends the PDU to the room timeline.  The function is deliberately kept
/// thin; the heavy lifting is delegated to the helpers below so that each
/// async state-machine stays within the stack-frame budget.
#[implement(super::Service)]
#[allow(clippy::too_many_arguments)]
pub async fn upgrade_outlier_to_timeline_pdu<Pdu>(
	&self,
	incoming_pdu: PduEvent,
	val: BTreeMap<String, CanonicalJsonValue>,
	create_event: &Pdu,
	origin: &ServerName,
	room_id: &RoomId,
	// Non-spec-compliant admin override to force-accept events.
	skip_soft_fail: bool,
	is_forward_extremity: bool,
	// True if this event's own /get_missing_events call (in fetch_prev) came
	// back with a structurally-invalid event for one of its prev_events. Lets
	// resolve_state_at_incoming_event skip a doomed /state_ids fetch instead of
	// hitting federation for state at an event we already know is unusable.
	prev_fetch_had_invalid_data: bool,
	state_ids_anchor: Option<&ruma::EventId>,
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

	let (rejected, soft_failed_early) = tokio::join!(
		self.services
			.pdu_metadata
			.is_event_rejected(incoming_pdu.event_id()),
		self.services
			.pdu_metadata
			.is_event_soft_failed(incoming_pdu.event_id())
	);
	let retryable_missing_auth_rejection = rejected
		&& !skip_soft_fail
		&& self
			.services
			.pdu_metadata
			.get_rejection_reason(incoming_pdu.event_id())
			.await
			.is_some_and(|reason| {
				crate::rooms::pdu_metadata::is_retryable_rejection_reason(&reason)
			});

	if rejected && !skip_soft_fail && !retryable_missing_auth_rejection {
		return Err!(Request(Forbidden("Event has been rejected")));
	} else if soft_failed_early && !skip_soft_fail {
		// Return Ok(None) so the remote server stops endlessly retrying
		info!("Event was previously soft-failed; acknowledging receipt");
		return Ok(None);
	}

	// If we reject/soft-fail/are missing auth events, the event is also rejected.
	if !skip_soft_fail {
		for aid in incoming_pdu.auth_events() {
			let exists = self.services.timeline.pdu_exists(aid).await;
			if !exists {
				// An absent auth event is a resolution failure, not evidence
				// it was ever rejected -- classify it with the retryable
				// `MissingAuthEvent` (mirrors handle_outlier_pdu's own
				// classification for the same situation), not the
				// deliberately-permanent `DependsOnRejectedAuthEvent`.
				// Conflating the two would permanently poison this event even
				// though a later successful backfill/state fetch could supply
				// `aid` and let a fresh attempt reach a different, correct
				// verdict.
				info!(
					"Rejecting incoming event {} which depends on missing auth event {aid}",
					incoming_pdu.event_id()
				);
				self.services
					.pdu_metadata
					.mark_event_rejected(
						incoming_pdu.event_id(),
						&RejectionCode::MissingAuthEvent.with_detail(aid),
					)
					.await;
				return Err!(Request(Forbidden("Event depends on missing auth event {aid}")));
			}

			let accepted = self.services.pdu_metadata.is_event_accepted(aid).await;
			if !accepted {
				info!(
					"Rejecting incoming event {} which depends on rejected auth event {aid}",
					incoming_pdu.event_id()
				);
				self.services
					.pdu_metadata
					.mark_event_rejected(
						incoming_pdu.event_id(),
						&RejectionCode::DependsOnRejectedAuthEvent.with_detail(aid),
					)
					.await;
				return Err!(Request(Forbidden("Event depends on rejected auth event {aid}")));
			}
		}
	}

	info!(
		event_id = %incoming_pdu.event_id,
		"Upgrading PDU from outlier to timeline"
	);
	let timer = Instant::now();
	let room_version_id = get_room_version_id(create_event)?;

	let mut state_at_incoming_event = Box::pin(self.resolve_state_at_incoming_event(
		&incoming_pdu,
		create_event,
		origin,
		room_id,
		&room_version_id,
		skip_soft_fail,
		prev_fetch_had_invalid_data,
		state_ids_anchor,
	))
	.await?;

	let room_version = to_room_version(&room_version_id);

	// Re-check if the PDU was added to the timeline while we were waiting
	if let Ok(pduid) = self
		.services
		.timeline
		.get_pdu_id(incoming_pdu.event_id())
		.await
	{
		return Ok(Some(pduid));
	}

	debug!(event_id = %incoming_pdu.event_id, "Gathering explicitly claimed auth events");
	let mut auth_events = HashMap::new();
	let mut missing_auth_events = false;

	for event_id in incoming_pdu.auth_events() {
		let is_rejected = self.services.pdu_metadata.is_event_rejected(event_id).await;
		if is_rejected && !skip_soft_fail {
			warn!(
				event_id = %incoming_pdu.event_id,
				auth_event_id = %event_id,
				"Event rejected because auth_event is rejected"
			);
			self.services
				.pdu_metadata
				.mark_event_rejected(
					incoming_pdu.event_id(),
					&RejectionCode::DependsOnRejectedAuthEvent.with_detail(event_id),
				)
				.await;
			return Err!(Request(Forbidden(
				"Event authorisation fails because it references a rejected auth_event"
			)));
		}

		if let Ok(pdu) = self.services.timeline.get_pdu(event_id).await {
			if let Some(state_key) = &pdu.state_key {
				let key = StateEventType::from(pdu.kind().clone());
				auth_events.insert((key, state_key.clone()), pdu);
			}
		} else {
			missing_auth_events = true;
		}
	}

	if missing_auth_events {
		debug!(event_id = %incoming_pdu.event_id, "Missing claimed auth events locally. Falling back to state-based auth events");
		if let Ok(state_auth_events) = self
			.services
			.state
			.get_auth_events(
				room_id,
				incoming_pdu.kind(),
				incoming_pdu.sender(),
				incoming_pdu.state_key(),
				incoming_pdu.content(),
				&room_version,
			)
			.await
		{
			for ((k, s), pdu) in state_auth_events {
				auth_events.entry((k, s)).or_insert(pdu);
			}
		}
	}

	let state_provider =
		crate::rooms::auth_adapter::PduStateProvider::from_ruma_map(&auth_events)
			.with_create_event(Some(create_event));

	// Check the auth of the event passes based on the claimed auth_events
	debug!(event_id = %incoming_pdu.event_id, "Running auth check with claimed state auth");
	let auth_check_claimed = crate::rooms::auth_adapter::rezzy_auth_check(
		&incoming_pdu,
		&state_provider,
		crate::rooms::auth_adapter::to_state_res_version(&room_version_id),
	);

	if !auth_check_claimed {
		if skip_soft_fail {
			warn!(
				event_id = %incoming_pdu.event_id,
				"Event failed auth check against claimed auth_events, but skip_soft_fail is set — continuing"
			);
		} else {
			self.services
				.pdu_metadata
				.mark_event_rejected(
					incoming_pdu.event_id(),
					RejectionCode::AuthCheckFailed.tag(),
				)
				.await;

			return Err!(Request(Forbidden(
				"Event authorisation fails based on its auth_events"
			)));
		}
	}

	// Check auth of event passes based on its state (soft-fail check)
	debug!(event_id = %incoming_pdu.event_id, "Running initial auth check against state-at-event");
	let srv = crate::rooms::auth_adapter::to_state_res_version(&room_version_id);
	let content = incoming_pdu.get_content_as_value();
	let auth_types = rezzy::auth::auth_types_for_event(
		&incoming_pdu.kind.to_string(),
		incoming_pdu.sender().as_str(),
		incoming_pdu.state_key(),
		&content,
		srv,
	);

	let mut state_auth_events = HashMap::new();
	for (ty, sk) in &auth_types {
		let state_ty: StateEventType = ty.as_str().into();
		let state_key = StateKey::from(sk.as_str());
		let pdu_opt: Option<PduEvent> = async {
			match &state_at_incoming_event {
				| StateAtEvent::Resolved(state) => {
					let shortstatekey = self
						.services
						.short
						.get_shortstatekey(&state_ty, &state_key)
						.await
						.ok()?;
					let event_id = state.get(&shortstatekey)?;
					self.services.timeline.get_pdu(event_id).await.ok()
				},
				| StateAtEvent::Compressed(compressed) =>
					self.find_pdu_in_compressed_state(&state_ty, &state_key, compressed)
						.await,
				| StateAtEvent::FastForward(shortstatehash) => {
					let shorteventid = self
						.services
						.state_accessor
						.state_get_shortid(*shortstatehash, &state_ty, &state_key)
						.await
						.ok()?;
					let event_id = self
						.services
						.short
						.get_eventid_from_short::<OwnedEventId>(shorteventid)
						.await
						.ok()?;
					self.services.timeline.get_pdu(&event_id).await.ok()
				},
			}
		}
		.await;

		if let Some(pdu) = pdu_opt {
			state_auth_events.insert((state_ty, state_key), pdu);
		}
	}
	let state_at_provider =
		crate::rooms::auth_adapter::PduStateProvider::from_ruma_map(&state_auth_events)
			.with_create_event(Some(create_event));
	let auth_check_state =
		crate::rooms::auth_adapter::rezzy_auth_check(&incoming_pdu, &state_at_provider, srv);

	if !auth_check_state {
		if skip_soft_fail {
			warn!(
				event_id = %incoming_pdu.event_id,
				"Event failed auth check against state at event, but skip_soft_fail is set — continuing"
			);
		} else {
			self.services
				.pdu_metadata
				.mark_event_rejected(
					incoming_pdu.event_id(),
					RejectionCode::AuthCheckFailed.tag(),
				)
				.await;

			return Err!(Request(Forbidden("Event authorisation fails based on state at event")));
		}
	}

	// NOTE: soft_fail is evaluated INSIDE the OCC loop (below) against the
	// committed current state to avoid a TOCTOU race. If the current state
	// changes between the initial check and the commit, a banned user's
	// message could slip through.
	let mut soft_fail = false;

	let state_ids_compressed = match &state_at_incoming_event {
		| StateAtEvent::FastForward(shortstatehash) => {
			self.services
				.state_compressor
				.load_shortstatehash_info(*shortstatehash)
				.await?
				.pop()
				.expect("top frame must have full_state")
				.full_state
				.expect("must have full_state")
				.clone() // This is Arc<CompressedState>
		},
		| StateAtEvent::Compressed(compressed) => compressed.clone(),
		| StateAtEvent::Resolved(state) =>
			self.services
				.state_compressor
				.compress_state_events(state.iter().map(|(ssk, eid)| (ssk, eid.borrow())))
				.collect()
				.map(Arc::new)
				.await,
	};

	// Finalize soft_fail before any state processing: check policy server
	// and redaction status so we can skip expensive state resolution for
	// events that will be rejected.
	if !soft_fail {
		// If the event is not a state event, ask the policy server about it
		if incoming_pdu.state_key.is_none() {
			debug!(event_id = %incoming_pdu.event_id, "Checking policy server for event");
			match self
				.ask_policy_server(
					&incoming_pdu,
					&mut incoming_pdu.to_canonical_object(),
					room_id,
					true,
				)
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

		// Additionally, if this is a redaction for a soft-failed event, we
		// soft-fail it also.
		if let Some(redact_id) = incoming_pdu.redacts_id(&room_version_id) {
			debug!(
				redact_id = %redact_id,
				"Checking if redaction is for a soft-failed or rejected event"
			);
			if !self
				.services
				.pdu_metadata
				.is_event_accepted(&redact_id)
				.await
			{
				warn!(
					redact_id = %redact_id,
					"Redaction targets a non-accepted event, soft failing"
				);
				soft_fail = true;
			}
		}
	}

	// --- Phase 2: state event handling (extracted to reduce frame) ---
	//
	// Derive new room state for all incoming state events, including
	// soft-failed ones. State resolution merges forks deterministically —
	// a soft-failed event may carry state from a fork we haven't seen,
	// and feeding it into resolve_state heals local drift.
	//
	// OCC (Optimistic Concurrency Control): We compute the state delta
	// WITHOUT holding the room lock, then acquire the lock and verify the
	// base state hash hasn't changed. If it has, we DROP the lock and
	// retry. This avoids holding the exclusive mutex during CPU-bound
	// state resolution.
	let state_delta_opt;
	let state_lock;

	if !is_forward_extremity {
		state_delta_opt = None;
		// Dummy lock to satisfy lifetimes since we aren't mutating state
		state_lock = self.services.state.mutex.lock(room_id).await;
	} else {
		loop {
			// Capture base state hash BEFORE the unlocked computation
			let base_shortstatehash = self
				.services
				.state
				.get_room_shortstatehash(room_id)
				.await
				.ok();

			if let StateAtEvent::FastForward(shortstatehash) = &state_at_incoming_event {
				if Some(*shortstatehash) != base_shortstatehash {
					info!(
						"Fast-forward state hash shift ({} -> {:?}), re-eval state @ incoming",
						shortstatehash, base_shortstatehash
					);
					state_at_incoming_event = Box::pin(self.resolve_state_at_incoming_event(
						&incoming_pdu,
						create_event,
						origin,
						room_id,
						&room_version_id,
						skip_soft_fail,
						false,
						None,
					))
					.await?;
				}
			}

			// Heavy computation WITHOUT the lock
			let delta = self
				.calculate_state_delta(
					&incoming_pdu,
					state_at_incoming_event.clone(),
					room_id,
					&room_version_id,
				)
				.await?;

			// Acquire lock for the commit phase
			trace!(room_id = %room_id, "Locking the room");
			let lock = self.services.state.mutex.lock(room_id).await;

			// Re-check if the PDU was already added while we were unlocked
			if let Ok(pduid) = self
				.services
				.timeline
				.get_pdu_id(incoming_pdu.event_id())
				.await
			{
				return Ok(Some(pduid));
			}

			// OCC verification: has the base state shifted?
			let current_shortstatehash = self
				.services
				.state
				.get_room_shortstatehash(room_id)
				.await
				.ok();

			if base_shortstatehash == current_shortstatehash {
				// State is consistent — evaluate soft_fail against the
				// COMMITTED current state while holding the lock. This
				// closes the TOCTOU race: the state we check against is
				// exactly the state that will be live when we append.
				if !skip_soft_fail {
					// Check redaction permissions
					if let Some(redact_id) = incoming_pdu.redacts_id(&room_version_id) {
						if !self
							.services
							.state_accessor
							.user_can_redact(&redact_id, incoming_pdu.sender(), room_id, true)
							.await?
						{
							soft_fail = true;
						}
					}

					// Auth against current room state (the critical check)
					if !soft_fail {
						let auth_check_current = Box::pin(self.check_current_state_auth(
							room_id,
							&room_version,
							&incoming_pdu,
						))
						.await
						.unwrap_or(false);

						if !auth_check_current {
							warn!(
								event_id = %incoming_pdu.event_id,
								"Event passed auth against state-at-event, but FAILED auth \
								against the committed current room state. Soft-failing event."
							);
							soft_fail = true;
						}
					}
				}

				// CRITICAL: If soft-failed, discard the state delta. Without
				// this, a banned user's state event (e.g. room name change,
				// power level edit) would be applied via force_state below —
				// modifying the room's active state — even though the event
				// is correctly withheld from the timeline.
				//
				// DAG integrity is preserved because append_incoming_pdu
				// still calls set_event_state using state_ids_compressed
				// (computed prior to the OCC loop).
				if soft_fail {
					state_delta_opt = None;
				} else {
					state_delta_opt = delta;
				}
				state_lock = lock;
				break;
			}

			// State changed — drop the lock and retry so we don't block the room
			info!(
				%room_id,
				?base_shortstatehash,
				?current_shortstatehash,
				"Room state changed during unlocked state-res, dropping lock and retrying"
			);
			drop(lock);
		}
	}

	// Apply the state delta (still holding state_lock from the successful break)
	trace!("Appending pdu to timeline");
	if let Some(HashSetCompressStateEvent { shortstatehash, added, removed }) = state_delta_opt {
		Box::pin(self.services.state.force_state(
			room_id,
			shortstatehash,
			added,
			removed,
			&state_lock,
		))
		.await?;
	}

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
		is_forward_extremity,
	)
	.await;

	// Recovered federation history is still new to this homeserver, so it
	// must stay in the sync-visible live stream. Use the normal append path
	// here; only explicit `/backfill` pagination inserts use backfilled counts.
	let pdu_id = Box::pin(self.services.timeline.append_incoming_pdu(
		&incoming_pdu,
		val,
		extremities.into_iter(),
		state_ids_compressed,
		None,
		soft_fail,
		true,
		&state_lock,
		room_id,
	))
	.await?;

	if soft_fail {
		self.services.pdu_metadata.mark_event_soft_failed(
			incoming_pdu.event_id(),
			"auth check failed against current room state",
		);

		debug_warn!(
			elapsed = ?timer.elapsed(),
			"Event has been soft-failed",
		);
	} else {
		debug_info!(
			elapsed = ?timer.elapsed(),
			"Accepted",
		);
	}

	// Event has passed all auth/stateres checks
	drop(state_lock);

	Ok(pdu_id)
}

#[derive(Clone)]
enum StateAtEvent {
	Resolved(HashMap<u64, OwnedEventId>),
	Compressed(Arc<crate::rooms::state_compressor::CompressedState>),
	FastForward(ShortStateHash),
}

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip_all)]
async fn check_current_state_auth(
	&self,
	room_id: &RoomId,
	_room_version: &state_res::RoomVersion,
	incoming_pdu: &PduEvent,
) -> Result<bool> {
	let room_version_id = self.services.state.get_room_version(room_id).await?;
	let srv = crate::rooms::auth_adapter::to_state_res_version(&room_version_id);
	let content = incoming_pdu.get_content_as_value();
	let mut auth_types = rezzy::auth::auth_types_for_event(
		&incoming_pdu.kind.to_string(),
		incoming_pdu.sender().as_str(),
		incoming_pdu.state_key(),
		&content,
		srv,
	);

	// Always include create event — rezzy's check_auth uses it for the
	// is_creator fallback when no membership event exists for the sender.
	if !auth_types.iter().any(|(ty, _)| ty == "m.room.create") {
		auth_types.push(("m.room.create".to_owned(), String::new()));
	}

	let mut auth_state = HashMap::new();
	for (ty, sk) in &auth_types {
		let state_ty: StateEventType = ty.as_str().into();
		if let Ok(pdu) = self
			.services
			.state_accessor
			.room_state_get(room_id, &state_ty, sk)
			.await
		{
			auth_state.insert((state_ty, StateKey::from(sk.as_str())), pdu);
		}
	}

	let state_provider = crate::rooms::auth_adapter::PduStateProvider::from_ruma_map(&auth_state);
	Ok(crate::rooms::auth_adapter::rezzy_auth_check(incoming_pdu, &state_provider, srv))
}

/// Find the state-at-event for an incoming PDU. If the PDU is a fast-forward
/// candidate we bypass full state resolution. If we are unable to resolve state
/// (e.g. auth chain fetch fails or soft-fail is active) then the room's current
/// room state is used as a best-effort fallback to avoid wiping state.
#[implement(super::Service)]
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "debug", skip_all)]
async fn resolve_state_at_incoming_event<Pdu>(
	&self,
	incoming_pdu: &PduEvent,
	create_event: &Pdu,
	origin: &ServerName,
	room_id: &RoomId,
	room_version_id: &RoomVersionId,
	skip_soft_fail: bool,
	prev_fetch_had_invalid_data: bool,
	state_ids_anchor: Option<&ruma::EventId>,
) -> Result<StateAtEvent>
where
	Pdu: Event + Send + Sync,
{
	// Fetch missing state and auth chain events by calling /state_ids at
	// backwards extremities doing all the checks in this list starting at 1.
	// These are not timeline events.
	let current_extremities: Vec<OwnedEventId> = self
		.services
		.state
		.get_forward_extremities(room_id)
		.collect()
		.await;

	let prev_events: Vec<_> = incoming_pdu.prev_events().map(ToOwned::to_owned).collect();
	let exact_match = !current_extremities.is_empty()
		&& prev_events.len() == current_extremities.len()
		&& current_extremities.iter().all(|e| prev_events.contains(e));
	let is_dag_fork = !exact_match;

	let mut state_at_event: Option<StateAtEvent> = None;

	if exact_match {
		info!(
			"Incoming PDU matches current extremities exactly (fast-forward candidate). \
			 Skipping full state lookup."
		);
		if let Ok(current_shortstatehash) =
			self.services.state.get_room_shortstatehash(room_id).await
		{
			if current_shortstatehash != 0 {
				return Ok(StateAtEvent::FastForward(current_shortstatehash));
			}
		}
	}

	if state_at_event.is_none() {
		info!(
			"State is none. Resolving state for incoming PDU (prev_events count: {})",
			incoming_pdu.prev_events().count()
		);

		// When the incoming event creates a DAG fork (its prev_events don't
		// match the current forward extremities), we MUST resolve state across
		// both the incoming event's prev_events AND the current forward
		// extremities. Without this, state resolution uses only the fork's
		// state — which may lack memberships that exist in the current state
		// (e.g., a user who joined on the other fork). `force_state` would
		// then apply the fork's state as a delta, removing those memberships.
		//
		// This matches Synapse's `compute_event_context()` which resolves
		// state groups across all prev_events (including current extremities
		// that aren't in the incoming event's prev_events).
		let resolved_state = if is_dag_fork {
			// Collect all events we need to resolve across: the incoming
			// event's prev_events PLUS the current forward extremities.
			let mut all_extremities: Vec<OwnedEventId> = prev_events.clone();
			for ext in &current_extremities {
				if !all_extremities.contains(ext) {
					all_extremities.push(ext.clone());
				}
			}

			info!(
				event_id = %incoming_pdu.event_id(),
				n_prev = prev_events.len(),
				n_extremities = current_extremities.len(),
				n_total = all_extremities.len(),
				"DAG fork detected: resolving state across prev_events + current extremities"
			);

			self.resolve_extremities(
				all_extremities.iter().map(AsRef::as_ref),
				room_id,
				room_version_id,
				None,
			)
			.await?
		} else if incoming_pdu.prev_events().count() == 1 {
			self.state_at_incoming_degree_one(incoming_pdu, room_id)
				.await?
		} else {
			self.state_at_incoming_resolved(incoming_pdu, room_id, room_version_id, None)
				.await?
		};
		if let Some(compressed) = resolved_state {
			state_at_event = Some(StateAtEvent::Compressed(compressed));
		}
		info!("State resolution completed for incoming PDU");
	}

	if state_at_event.is_none() && !skip_soft_fail {
		// Local state is unavailable — prev_events are not yet in DB or their
		// state hashes have not been computed.
		//
		// Before making any network requests, check whether state is missing
		// because prev_events are rejected. If they are, a /state_ids fetch
		// would be wasted traffic — just fall through to the current room
		// state fallback. The auth check will still reject invalid events.
		let all_prevs_rejected = futures::stream::iter(incoming_pdu.prev_events())
			.all(|prev_id| async move {
				self.services.pdu_metadata.is_event_rejected(prev_id).await
					|| self.services.timeline.get_pdu_id(prev_id).await.is_ok()
			})
			.await;

		let any_prev_rejected = futures::stream::iter(incoming_pdu.prev_events())
			.any(
				|prev_id| async move { self.services.pdu_metadata.is_event_rejected(prev_id).await },
			)
			.await;

		let all_prevs_unknown = futures::stream::iter(incoming_pdu.prev_events())
			.all(|prev_id| async move {
				self.services.timeline.get_pdu_id(prev_id).await.is_err()
					&& self
						.services
						.outlier
						.get_pdu_outlier(prev_id)
						.await
						.is_err()
			})
			.await;

		if prev_fetch_had_invalid_data && all_prevs_unknown {
			// This event's own fetch_prev call just tried /get_missing_events
			// and got back a structurally-invalid event for (one of) its
			// prev_events. That data can't be made valid by asking again via a
			// different federation endpoint, so skip the /state_ids attempt
			// below entirely and reject now.
			info!(
				event_id = %incoming_pdu.event_id,
				"Rejecting event: prev_event was structurally invalid in get_missing_events response"
			);
			self.services
				.pdu_metadata
				.mark_event_rejected(
					incoming_pdu.event_id(),
					RejectionCode::StructurallyInvalidInGetMissingEvents.tag(),
				)
				.await;
			return Err!(Request(Forbidden(
				"Cannot determine state: prev_event was structurally invalid"
			)));
		} else if any_prev_rejected && all_prevs_rejected {
			// All non-timeline prev_events are rejected — no point fetching
			// state from federation. Fall through to current room state.
			debug!(
				event_id = %incoming_pdu.event_id,
				"Skipping /state_ids fetch: not a state event or prev_events are rejected; using current room state"
			);
		} else {
			// When we have a single unresolved predecessor, ask /state_ids at
			// that predecessor first. This matches the gap-recovery path used by
			// Complement's corrupted-auth-chain test, where /get_missing_events
			// returns one event but the sender still needs to provide the state
			// snapshot that links it back into the known DAG.
			let mut prev_events = incoming_pdu.prev_events();
			let first_prev = prev_events.next();

			// Only trust a caller-supplied `state_ids_anchor` when it is actually
			// one of this event's own prev_events. It's computed upstream from a
			// gap-recovery /get_missing_events fetch and can otherwise point at
			// an unrelated event further back in the DAG; querying /state_ids
			// against that would authenticate `incoming_pdu` against state that
			// doesn't actually precede it. Re-derive a fresh iterator since
			// `prev_events` above was already partially consumed.
			let validated_anchor = state_ids_anchor
				.filter(|anchor| incoming_pdu.prev_events().any(|prev_id| prev_id == *anchor));

			// The single prev_event id is the preferred /state_ids target: its
			// resolved state is the state immediately preceding `incoming_pdu`.
			// `incoming_pdu.event_id()` below is only the last-resort fallback,
			// used when there are zero or multiple prev_events and therefore no
			// single direct predecessor to anchor the lookup on.
			let state_lookup_event_id = match validated_anchor {
				| Some(anchor) => anchor.to_owned(),
				| None => match (first_prev, prev_events.next()) {
					| (Some(first_prev), None) => {
						let nested_prev = if let Ok(prev_pdu) =
							self.services.timeline.get_pdu(first_prev).await
						{
							let mut nested = prev_pdu.prev_events();
							match (nested.next(), nested.next()) {
								| (Some(nested_prev), None) => Some(nested_prev.to_owned()),
								| _ => None,
							}
						} else if let Ok(prev_pdu) =
							self.services.timeline.get_pdu_outlier(first_prev).await
						{
							let mut nested = prev_pdu.prev_events();
							match (nested.next(), nested.next()) {
								| (Some(nested_prev), None) => Some(nested_prev.to_owned()),
								| _ => None,
							}
						} else {
							None
						};

						nested_prev.unwrap_or_else(|| first_prev.to_owned())
					},
					| _ => incoming_pdu.event_id().to_owned(),
				},
			};

			// Attempt a synchronous /state_ids fetch from the sending server
			// BEFORE queuing the async DAG healer.
			//
			// The healer fires asynchronously (after a delay), which races with
			// the sending server's lifetime: in Complement tests the fake
			// federation server shuts down when the test times out, so the
			// healer's /state_ids calls always arrive too late and "all servers
			// failed". Fetching inline here gives us a shot while the sender
			// is still alive.
			debug!(
				event_id = %incoming_pdu.event_id,
				%origin,
				"local state unavailable; attempting synchronous /state_ids fetch"
			);
			match Box::pin(self.fetch_state(
				origin,
				create_event,
				room_id,
				&state_lookup_event_id,
				false,
			))
			.await
			{
				| Ok(Some(fetched_state)) => {
					info!(
						target: "state_res_debug",
						event_id = %incoming_pdu.event_id,
						n_state = fetched_state.len(),
						"fetched state via /state_ids; proceeding with auth check"
					);
					state_at_event = Some(StateAtEvent::Resolved(fetched_state));
				},
				| Ok(None) | Err(_) => {
					// If any predecessor is still completely unknown — not in
					// the timeline and not even persisted as an outlier — we
					// still do not know the event's real state-at-event. A
					// mixed known/unknown predecessor set is not safe to
					// authorize against current room state either: the unknown
					// edge could still anchor the event to a different auth
					// context.
					let unknown_prev_ids: Vec<_> =
						futures::stream::iter(incoming_pdu.prev_events())
							.filter_map(|prev_id| async move {
								(self.services.timeline.get_pdu_id(prev_id).await.is_err()
									&& self
										.services
										.outlier
										.get_pdu_outlier(prev_id)
										.await
										.is_err())
								.then(|| prev_id.to_owned())
							})
							.collect()
							.await;

					let any_prev_unknown = !unknown_prev_ids.is_empty();

					if any_prev_unknown {
						// Last resort before rejecting: make a single,
						// non-recursive attempt to materialize each prev_event
						// via GET /event/{id}. /state_ids just failed, so we
						// have no state snapshot — but a server that doesn't
						// serve /state_ids may still serve individual events.
						// Without this, a prev_event we were never sent (e.g.
						// its own /send was rejected for an unrelated reason,
						// like a signature we can't verify) leaves us with no
						// cryptographic or structural facts about it, and the
						// only options are: reject the child outright, or
						// evaluate it against synthetic/current state — the
						// latter is the DAG-takeover hazard Synapse's
						// `on_receive_pdu` rejects for (a forged
						// single-extremity event judged against today's power
						// levels instead of its claimed ancestry).
						//
						// Fetched events go through the same signature/hash
						// checks and auth-event resolution as any other
						// outlier (`handle_outlier_pdu`) and are persisted as
						// accepted or rejected accordingly. We do NOT chase
						// their own prev_events — this is a single hop per
						// prev_event, bounded by prev_events().count()
						// (already capped at 20 in parse_incoming_pdu), fired
						// concurrently, not a backfill.
						futures::stream::iter(unknown_prev_ids.iter())
							.for_each_concurrent(4, |prev_id| async move {
								debug!(
									event_id = %incoming_pdu.event_id,
									%prev_id,
									%origin,
									"prev_event still unknown after /state_ids failure; \
									 attempting single-hop /event fetch"
								);
								let Ok(res) = self
									.services
									.sending
									.send_federation_request(
										origin,
										ruma::api::federation::event::get_event::v1::Request::new(
											prev_id.clone(),
											None,
										),
									)
									.await
								else {
									return;
								};
								let Ok((fetched_id, val)) =
									conduwuit::matrix::event::gen_event_id_canonical_json(
										&res.pdu,
										room_version_id,
									)
								else {
									return;
								};
								if fetched_id != **prev_id {
									return;
								}

								// Verified and persisted as an outlier
								// (accepted or rejected) by handle_outlier_pdu;
								// the Result doesn't matter here — either way,
								// the prev_event is now "known" below.
								drop(
									Box::pin(self.handle_outlier_pdu(
										origin,
										Some(create_event),
										&fetched_id,
										room_id,
										val,
										false,
										false,
										Some(room_version_id),
									))
									.await,
								);
							})
							.await;

						// Require EVERY prev_event to now be known (in the timeline or
						// stored as an outlier) before it's safe to fall through to
						// the current-room-state authorization below. A single-hop
						// /event fetch above is fired concurrently for each
						// still-unknown prev_event, and a partial result — some
						// prevs fetched, others still genuinely unknown — must
						// still be rejected: authorizing against current state
						// while any claimed predecessor remains unverified is the
						// DAG-takeover hazard this whole fallback exists to avoid.
						let any_prev_still_unknown =
							futures::stream::iter(incoming_pdu.prev_events())
								.any(|prev_id| async move {
									self.services.timeline.get_pdu_id(prev_id).await.is_err()
										&& self
											.services
											.outlier
											.get_pdu_outlier(prev_id)
											.await
											.is_err()
								})
								.await;

						if any_prev_still_unknown {
							info!(
								event_id = %incoming_pdu.event_id,
								"Rejecting event: at least one prev_event still unknown after \
								 /state_ids and /event fetches failed"
							);
							self.services
								.pdu_metadata
								.mark_event_rejected(
									incoming_pdu.event_id(),
									RejectionCode::PrevEventUnknownStateIdsFailed.tag(),
								)
								.await;
							return Err!(Request(Forbidden(
								"Cannot determine state: prev_events remain unknown after \
								 /state_ids fetch failed"
							)));
						}
					}

					// All prev_events exist but state hashes not computed — safe to
					// fall back to current room state for the auth check.
					info!(
						target: "state_res_debug",
						event_id = %incoming_pdu.event_id,
						"fetch_state failed but prev_events present; falling back to current room state"
					);
				},
			}
		}
	}

	if state_at_event.is_none() {
		// State could not be determined from prev_events or federation.
		// Fall back to current room state — the auth check at step 11 will
		// still reject invalid events.
		debug!(
			event_id = %incoming_pdu.event_id,
			"Could not find state at event — using current room state as fallback"
		);
		let current_shortstatehash = self
			.services
			.state
			.get_room_shortstatehash(room_id)
			.await
			.map_err(|_| err!(Database("Room has no state")))?;

		let current_state: HashMap<_, _> = self
			.services
			.state_accessor
			.state_full_shortids(current_shortstatehash)
			.ready_filter_map(Result::ok)
			.map(|(shortstatekey, shorteventid)| async move {
				let event_id = self
					.services
					.short
					.get_eventid_from_short::<Box<_>>(shorteventid)
					.await
					.ok()?;
				Some((shortstatekey, (*event_id).to_owned()))
			})
			.buffer_unordered(64)
			.filter_map(ready)
			.collect()
			.await;

		state_at_event = Some(StateAtEvent::Resolved(current_state));
	}

	Ok(state_at_event.unwrap())
}

/// For state events: build the new post-event state, run state resolution
/// against the current room state, and return the state delta for application.
///
/// Extracted from `upgrade_outlier_to_timeline_pdu` to give `save_state`
/// its own async state-machine frame and allow Optimistic Concurrency Control
/// (running without a lock).
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip_all)]
async fn calculate_state_delta(
	&self,
	incoming_pdu: &PduEvent,
	state_at_incoming_event: StateAtEvent,
	room_id: &RoomId,
	room_version_id: &RoomVersionId,
) -> Result<Option<HashSetCompressStateEvent>> {
	let current_shortstatehash = self
		.services
		.state
		.get_room_shortstatehash(room_id)
		.await
		.ok();

	if incoming_pdu.state_key().is_none() {
		// Just a normal message, state hasn't diverged: fast path out.
		let state_at_hash = match &state_at_incoming_event {
			| StateAtEvent::FastForward(ssh) => Some(*ssh),
			| StateAtEvent::Compressed(_) | StateAtEvent::Resolved(_) => None, /* We have to
			                                                                    * compress to
			                                                                    * get the hash */
		};

		if let Some(ssh) = state_at_hash {
			if Some(ssh) == current_shortstatehash {
				return Ok(None);
			}
		}
	} else {
		info!("Event is a state-event. Deriving new room state");
	}

	let new_room_state = match state_at_incoming_event {
		| StateAtEvent::FastForward(shortstatehash) => {
			info!("Fast-forward state update, skipping state resolution and map expansion");
			let mut current_state_compressed = self
				.services
				.state_compressor
				.load_shortstatehash_info(shortstatehash)
				.await?
				.pop()
				.expect("must have frame")
				.full_state
				.expect("must have full_state")
				.as_ref()
				.clone();

			if let Some(state_key) = incoming_pdu.state_key() {
				let shortstatekey = self
					.services
					.short
					.get_or_create_shortstatekey(
						&incoming_pdu.kind().to_string().into(),
						state_key,
					)
					.await;

				let shorteventid = self
					.services
					.short
					.get_or_create_shorteventid(incoming_pdu.event_id())
					.await;

				if let Ok(old_shorteventid) = self
					.services
					.state_accessor
					.state_get_shortid(
						shortstatehash,
						&incoming_pdu.kind().to_string().into(),
						state_key,
					)
					.await
				{
					let old_compressed = crate::rooms::state_compressor::compress_state_event(
						shortstatekey,
						old_shorteventid,
					);
					current_state_compressed.remove(&old_compressed);
				}

				let new_compressed = crate::rooms::state_compressor::compress_state_event(
					shortstatekey,
					shorteventid,
				);
				current_state_compressed.insert(new_compressed);
			}

			Arc::new(current_state_compressed)
		},
		| StateAtEvent::Compressed(state_after) => {
			let mut state_after = state_after.clone();
			if let Some(state_key) = incoming_pdu.state_key() {
				let shortstatekey = self
					.services
					.short
					.get_or_create_shortstatekey(
						&incoming_pdu.kind().to_string().into(),
						state_key,
					)
					.await;
				let shorteventid = self
					.services
					.short
					.get_or_create_shorteventid(incoming_pdu.event_id())
					.await;

				let state_after_mut: &mut std::collections::BTreeSet<[u8; 16]> =
					Arc::make_mut(&mut state_after);
				let old_compressed = state_after_mut
					.iter()
					.find(|bytes| bytes.starts_with(&shortstatekey.to_be_bytes()))
					.copied();
				if let Some(old) = old_compressed {
					state_after_mut.remove(&old);
				}
				state_after_mut.insert(crate::rooms::state_compressor::compress_state_event(
					shortstatekey,
					shorteventid,
				));
			}
			state_after
		},
		| StateAtEvent::Resolved(state_after) => {
			let mut state_after = state_after.clone();
			if let Some(state_key) = incoming_pdu.state_key() {
				let shortstatekey = self
					.services
					.short
					.get_or_create_shortstatekey(
						&incoming_pdu.kind().to_string().into(),
						state_key,
					)
					.await;

				let event_id = incoming_pdu.event_id();
				state_after.insert(shortstatekey, event_id.to_owned());
			}

			let t = Instant::now();
			info!(
				event_id = %incoming_pdu.event_id(),
				%room_id,
				"state_res: starting resolve_state for incoming state event"
			);
			let result = self
				.resolve_state(room_id, room_version_id, state_after)
				.await?;
			info!(
				event_id = %incoming_pdu.event_id(),
				%room_id,
				elapsed = ?t.elapsed(),
				"state_res: resolve_state complete"
			);

			result
		},
	};

	// Save the resolved state delta into the database (safe to do concurrently)
	debug!("Compressing new room state");
	let state_delta = self
		.services
		.state_compressor
		.save_state(room_id, new_room_state)
		.await?;

	// If the state delta is empty (no added/removed events), we can fast-path out
	// without taking the room state lock and churning caches, UNLESS the state hash
	// shifted (i.e., if we resolved from multiple parents to an existing hash).
	if state_delta.added.is_empty()
		&& state_delta.removed.is_empty()
		&& Some(state_delta.shortstatehash) == current_shortstatehash
	{
		return Ok(None);
	}

	Ok(Some(state_delta))
}

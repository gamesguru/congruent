use std::collections::{BTreeMap, HashMap, hash_map};

use conduwuit::{
	Err, Event, PduEvent, Result, debug, debug_info, err, implement, info, trace, warn,
};
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, EventId, OwnedEventId, RoomId, ServerName,
	events::{StateEventType, TimelineEventType},
};

use super::{check_room_id, get_room_version_id, to_room_version};
use crate::rooms::{pdu_metadata::RejectionCode, timeline::pdu_fits};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthRecoveryStage {
	BeforeStateIds,
	AfterStateIds,
}

#[implement(super::Service)]
#[allow(clippy::too_many_arguments)]
pub async fn handle_outlier_pdu<'a, Pdu>(
	&self,
	origin: &'a ServerName,
	create_event: Option<&'a Pdu>,
	event_id: &'a EventId,
	room_id: &'a RoomId,
	mut value: CanonicalJsonObject,
	_auth_events_known: bool,
	skip_sig_verify: bool,
	room_version_override: Option<&'a ruma::RoomVersionId>,
	auth_recovery_stage: AuthRecoveryStage,
) -> Result<(PduEvent, BTreeMap<String, CanonicalJsonValue>)>
where
	Pdu: Event + Send + Sync,
{
	// Skip the PDU if we already have it
	if let Ok(json) = self.services.timeline.get_outlier_pdu_json(event_id).await {
		if let Ok(pdu) = PduEvent::from_id_val(event_id, json.clone(), Some(room_id)) {
			if pdu.room_id_or_hash().as_deref() == Some(room_id) {
				// If this event was previously rejected, propagate the
				// rejection so callers treat it as invalid (e.g. when
				// checking auth chains of dependent events) -- UNLESS the
				// rejection reason indicates it was our own failure to
				// *resolve* this event's dependencies (missing auth events,
				// a degraded-context fetch, etc.) rather than the event
				// itself being invalid. Those are worth retrying: a caller
				// with better context than whoever originally rejected it
				// (a fuller state snapshot, a successful backfill, a normal
				// `/send`) may reach a different, correct verdict. Without
				// this, a single degraded attempt (e.g. the last-resort
				// `GET /event/{id}` fallback in `resolve_state_at_incoming_event`,
				// which runs with no state snapshot at all) can permanently
				// poison an event for every future caller, including ones
				// with full context -- backfill silently drops it forever,
				// and it can never be re-delivered.
				let _insert_lock = self.services.timeline.mutex_insert.lock(room_id).await;
				if self
					.services
					.pdu_metadata
					.take_retry_if_rejection_retryable(event_id)
					.await
				{
					info!(
						target: "state_res_debug",
						%event_id,
						"handle_outlier_pdu: retrying previously-rejected event (prior \
						 rejection was resolution-related, not intrinsic to the event)"
					);
					// `take_retry_if_rejection_retryable` already cleared the
					// rejection mark so this attempt starts fresh. Immediately
					// restore a placeholder "pending" mark for the duration of
					// this attempt: several exit paths below (`pdu_fits`,
					// `check_room_id`, room-version lookup, etc.) return early
					// via `?`/`return Err!` without marking anything, and the
					// stored outlier from the previous attempt is still
					// sitting in the DB. Without this, one of those bare exits
					// would leave that stale outlier unmarked again --
					// reopening the exact "un-rejected pending outlier trusted
					// by a later invocation" bug this retry path exists to
					// avoid. Any exit that explicitly marks a more specific
					// reason overwrites this placeholder; the success path at
					// the end of this function clears it.
					self.services
						.pdu_metadata
						.mark_event_rejected(event_id, RejectionCode::MissingAuthEvent.tag())
						.await;
				} else if self.services.pdu_metadata.is_event_rejected(event_id).await {
					return Err!(Request(Forbidden(
						"Event {event_id} is already known and rejected"
					)));
				} else {
					info!(
						target: "state_res_debug",
						%event_id,
						event_type = ?pdu.kind,
						"handle_outlier_pdu: early return, event already known"
					);
					return Ok((pdu, json));
				}
			}
		}
	}

	if !pdu_fits(&mut value.clone()) {
		warn!(
			"dropping incoming PDU {event_id} in room {room_id} from {origin} because it \
			 exceeds 65535 bytes or is otherwise too large."
		);
		// Persist as permanently rejected -- unlike a missing/unresolved
		// dependency, this event's size will never change on retry. Without
		// this, a recursive auth-chain caller's `is_event_permanently_rejected`
		// check sees "not rejected at all" (nothing was ever persisted) and
		// misfiles this as merely `still_missing`/retryable instead of
		// cascading a permanent rejection to dependents.
		value.insert(
			"event_id".to_owned(),
			CanonicalJsonValue::String(event_id.as_str().to_owned()),
		);
		self.services
			.outlier
			.add_pdu_outlier(event_id, &value, Some(room_id))
			.await;
		self.services
			.pdu_metadata
			.mark_event_rejected(event_id, RejectionCode::InvalidPduFormat.tag())
			.await;
		return Err!(Request(TooLarge("PDU is too large")));
	}
	// Strip unsigned before signature verification (unsigned is not signed,
	// so it must be excluded). Stash it so we can re-attach origin's
	// prev_content after verification succeeds.
	let stashed_unsigned = value.remove("unsigned");

	// TODO: For RoomVersion6 we must check that Raw<..> is canonical do we anywhere?: https://matrix.org/docs/spec/rooms/v6#canonical-json

	let room_version_id = match create_event {
		| Some(ce) => get_room_version_id(ce)?,
		| None =>
			if let Some(override_v) = room_version_override {
				override_v.clone()
			} else {
				self.services
					.state
					.get_room_version(room_id)
					.await
					.map_err(|e| {
						err!(Request(InvalidParam(
							"Room version is unknown locally and no override was provided: {e}"
						)))
					})?
			},
	};

	let mut incoming_pdu = if skip_sig_verify {
		// Caller already verified signatures (e.g. import_pdus via
		// validate_and_add_event_id). Skip redundant verification.
		value
	} else if self
		.services
		.server
		.config
		.bypassed_signature_events
		.contains(&event_id.to_owned())
	{
		// Configured exception — skip signature verification for known-bad events
		conduwuit::info!(
			"Bypassing signature verification for configured exception event: {event_id}"
		);
		value
	} else {
		// Check signatures, otherwise drop
		// check content hash, redact if doesn't match
		match self
			.services
			.server_keys
			.verify_event_at(&value, Some(&room_version_id), "handle_outlier_pdu::initial")
			.await
		{
			| Ok(ruma::signatures::Verified::All) => value,
			| Ok(ruma::signatures::Verified::Signatures) => {
				// Content hash mismatch: content may have been tampered by a relay.
				// If we already have this event locally, re-use our known-good content
				// instead of redacting or re-fetching from the origin.
				if let Ok(known_pdu) = self.services.timeline.get_pdu(event_id).await {
					info!(
						%event_id,
						"Received redacted copy, but we already have known-good content. Re-using."
					);
					check_room_id(room_id, &known_pdu)?;
					let obj = known_pdu.to_canonical_object();
					return Ok((known_pdu, obj));
				}

				// Attempt to fetch a pristine copy from the sender's server.
				let sender_server = value
					.get("sender")
					.and_then(|v| v.as_str())
					.and_then(|s| ruma::UserId::parse(s).ok())
					.map(|u| u.server_name().to_owned());

				let mut recovered = false;
				if let Some(ref server) = sender_server {
					if server.as_str() != origin.as_str() {
						debug_info!(
							%event_id,
							"Hash mismatch, fetching pristine copy from {server}"
						);
						if let Ok(res) = self
							.services
							.timeline
							.without_cork(|| {
								self.services.sending.send_federation_request(
									server,
									ruma::api::federation::event::get_event::v1::Request {
										event_id: event_id.to_owned(),
										include_unredacted_content: None,
									},
								)
							})
							.await
						{
							if let Ok((eid, clean_val)) =
								conduwuit::matrix::event::gen_event_id_canonical_json(
									&res.pdu,
									&room_version_id,
								) {
								if eid == *event_id {
									if matches!(
										self.services
											.server_keys
											.verify_event_at(
												&clean_val,
												Some(&room_version_id),
												"handle_outlier_pdu::recovered_pristine_copy",
											)
											.await,
										Ok(ruma::signatures::Verified::All)
									) {
										debug_info!(
											%event_id,
											"Recovered pristine copy from {server}"
										);
										recovered = true;
									}
								}
							}
						}
					}
				}

				if recovered {
					// Re-fetch since we can't move clean_val out of the nested scope
					if let Ok(res) = self
						.services
						.timeline
						.without_cork(|| {
							self.services.sending.send_federation_request(
								sender_server.as_ref().unwrap(),
								ruma::api::federation::event::get_event::v1::Request {
									event_id: event_id.to_owned(),
									include_unredacted_content: None,
								},
							)
						})
						.await
					{
						if let Ok((_, clean_val)) =
							conduwuit::matrix::event::gen_event_id_canonical_json(
								&res.pdu,
								&room_version_id,
							) {
							clean_val
						} else {
							debug_info!("Calculated hash does not match (redaction): {event_id}");
							match ruma::canonical_json::redact(
								value.clone(),
								&room_version_id,
								None,
							) {
								| Ok(redacted) => redacted,
								| Err(_) => {
									self.mark_redaction_failure_rejected(
										event_id, room_id, value,
									)
									.await;
									return Err!(Request(InvalidParam("Redaction failed")));
								},
							}
						}
					} else {
						debug_info!("Calculated hash does not match (redaction): {event_id}");
						match ruma::canonical_json::redact(value.clone(), &room_version_id, None)
						{
							| Ok(redacted) => redacted,
							| Err(_) => {
								self.mark_redaction_failure_rejected(event_id, room_id, value)
									.await;
								return Err!(Request(InvalidParam("Redaction failed")));
							},
						}
					}
				} else {
					debug_info!("Calculated hash does not match (redaction): {event_id}");
					match ruma::canonical_json::redact(value.clone(), &room_version_id, None) {
						| Ok(redacted) => redacted,
						| Err(_) => {
							self.mark_redaction_failure_rejected(event_id, room_id, value)
								.await;
							return Err!(Request(InvalidParam("Redaction failed")));
						},
					}
				}
			},
			| Err(e) => {
				// Persist as rejected outlier so we don't re-fetch from
				// federation on every auth chain walk
				value.insert(
					"event_id".to_owned(),
					CanonicalJsonValue::String(event_id.as_str().to_owned()),
				);
				self.services
					.outlier
					.add_pdu_outlier(event_id, &value, Some(room_id))
					.await;
				self.services
					.pdu_metadata
					.mark_event_rejected(
						event_id,
						RejectionCode::SignatureVerificationFailed.tag(),
					)
					.await;
				return Err!(Request(InvalidParam(debug_error!(
					"Signature verification failed for {event_id}: {e}"
				))));
			},
		}
	};

	// Now that we have checked the signature and hashes we can add the eventID and
	// convert to our PduEvent type
	incoming_pdu
		.insert("event_id".to_owned(), CanonicalJsonValue::String(event_id.as_str().to_owned()));

	// Re-attach the origin's unsigned field (age, etc.) after stripping
	// untrusted state metadata. append_pdu will recompute prev_content
	// from local state when a snapshot is available.
	if let Some(CanonicalJsonValue::Object(mut unsigned_obj)) = stashed_unsigned {
		unsigned_obj.remove("prev_content");
		unsigned_obj.remove("prev_sender");
		unsigned_obj.remove("replaces_state");
		if !unsigned_obj.is_empty() {
			incoming_pdu.insert("unsigned".to_owned(), CanonicalJsonValue::Object(unsigned_obj));
		}
	}

	let pdu_event = match PduEvent::from_id_val(event_id, incoming_pdu.clone(), Some(room_id)) {
		| Ok(pdu) => pdu,
		| Err(e) => {
			// Persist as a rejected outlier to preserve the DAG chain.
			// This prevents future valid events that reference this event from
			// failing with MissingAuthEvents.
			self.services
				.pdu_metadata
				.mark_event_rejected(event_id, RejectionCode::InvalidPduFormat.tag())
				.await;
			self.services
				.outlier
				.add_pdu_outlier(event_id, &incoming_pdu, Some(room_id))
				.await;
			return Err!(Request(BadJson(debug_warn!("Event is not a valid PDU: {e}"))));
		},
	};

	check_room_id(room_id, &pdu_event)?;

	// Fetch all auth events
	let mut auth_events: HashMap<OwnedEventId, PduEvent> = HashMap::new();

	for aid in pdu_event.auth_events() {
		// If any of the auth events are already marked as *permanently* rejected,
		// this event is automatically rejected. We must check this BEFORE
		// attempting to fetch the auth event to avoid deadlocks (e.g.
		// MissingAuthEvents) when an auth event is unparsable but correctly
		// marked as rejected in our database. A transiently-rejected auth event
		// (missing/unresolved, not intrinsically bad) must NOT hard-cascade here
		// -- fall through and let it be treated as missing/retryable below.
		if self
			.services
			.pdu_metadata
			.is_event_permanently_rejected(aid)
			.await
		{
			self.services
				.pdu_metadata
				.mark_event_rejected(
					event_id,
					&RejectionCode::DependsOnRejectedAuthEvent.with_detail(aid),
				)
				.await;
			self.services
				.outlier
				.add_pdu_outlier(pdu_event.event_id(), &incoming_pdu, Some(room_id))
				.await;
			self.services
				.pdu_metadata
				.mark_event_rejected(
					pdu_event.event_id(),
					&RejectionCode::DependsOnRejectedAuthEvent.with_detail(aid),
				)
				.await;
			return Err!(Request(Forbidden("Event depends on rejected auth event {aid}")));
		}

		if self
			.services
			.pdu_metadata
			.is_event_pending_auth_resolution(aid)
			.await
		{
			// This auth event's *own* outlier auth check never completed
			// (still marked `MissingAuthEvent` from a prior attempt) --
			// don't trust whatever bytes are sitting in the outlier store
			// for it as validated auth context. Treat it as unresolved so
			// it goes through the missing-auth-event recovery path below,
			// which re-runs `handle_outlier_pdu` on it and gets a fresh,
			// fully-validated verdict.
			//
			// Deliberately narrower than a bare `is_event_rejected`: other
			// retryable codes (e.g. `StateResolutionFailedWithPrevsPresent`)
			// get attached *after* this event's own auth check already
			// succeeded, at a later timeline-upgrade stage -- those are
			// valid, trustworthy auth context and must not be treated as
			// unresolved here.
			info!(
				target: "state_res_debug",
				%event_id,
				auth_event_id = %aid,
				"Auth event transiently rejected locally, treating as unresolved for outlier"
			);
			continue;
		}

		if let Ok(auth_event) = self
			.services
			.timeline
			.get_pdu_in_room(Some(room_id), aid)
			.await
		{
			check_room_id(room_id, &auth_event)?;
			if auth_event.state_key.is_none() {
				self.services
					.pdu_metadata
					.mark_event_rejected(
						pdu_event.event_id(),
						RejectionCode::InvalidPduFormat.tag(),
					)
					.await;
				self.services
					.outlier
					.add_pdu_outlier(pdu_event.event_id(), &incoming_pdu, Some(room_id))
					.await;
				return Err!(Request(InvalidParam(
					"Auth event exists locally but is not a state event"
				)));
			}
			info!(
				target: "state_res_debug",
				%event_id,
				auth_event_id = %aid,
				event_type = ?auth_event.kind,
				"Found auth event locally for outlier"
			);
			auth_events.insert(aid.to_owned(), auth_event);
		} else {
			info!(
				target: "state_res_debug",
				%event_id,
				auth_event_id = %aid,
				"Auth event NOT found locally for outlier"
			);
		}
	}

	// Check for auth events still missing after local + outlier lookup
	let missing_auth_events = pdu_event
		.auth_events()
		.filter(|id| !auth_events.contains_key(*id))
		.collect::<Vec<_>>();
	info!(
		target: "state_res_debug",
		%event_id,
		found = auth_events.len(),
		missing = missing_auth_events.len(),
		total_auth = pdu_event.auth_events().count(),
		"Auth events local lookup summary"
	);
	if !missing_auth_events.is_empty() {
		self.resolve_missing_outlier_auth_events(
			origin,
			create_event,
			event_id,
			room_id,
			&pdu_event,
			&incoming_pdu,
			&room_version_id,
			room_version_override,
			&missing_auth_events,
			&mut auth_events,
			auth_recovery_stage,
		)
		.await?;
	}
	debug!("No missing auth events for outlier event {event_id}");

	// Build map of auth events and reject if we are still missing some
	let mut auth_events_by_key: HashMap<_, _> = HashMap::with_capacity(auth_events.len());
	for id in pdu_event.auth_events() {
		// Re-check for rejected auth events. We might have fetched them via
		// /event_auth and discovered they were rejected. If they are
		// *permanently* rejected, this event must be rejected too. A
		// transiently-rejected one falls through to the missing-auth-event
		// branch below instead of hard-cascading.
		if self
			.services
			.pdu_metadata
			.is_event_permanently_rejected(id)
			.await
		{
			self.services
				.pdu_metadata
				.mark_event_rejected(
					event_id,
					&RejectionCode::DependsOnRejectedAuthEvent.with_detail(id),
				)
				.await;
			self.services
				.outlier
				.add_pdu_outlier(pdu_event.event_id(), &incoming_pdu, Some(room_id))
				.await;
			self.services
				.pdu_metadata
				.mark_event_rejected(
					pdu_event.event_id(),
					&RejectionCode::DependsOnRejectedAuthEvent.with_detail(id),
				)
				.await;
			return Err!(Request(Forbidden("Event depends on rejected auth event {id}")));
		}

		let Some(auth_event) = auth_events.get(id).map(ToOwned::to_owned) else {
			self.services
				.pdu_metadata
				.mark_event_rejected(event_id, &RejectionCode::MissingAuthEvent.with_detail(id))
				.await;
			self.services
				.outlier
				.add_pdu_outlier(pdu_event.event_id(), &incoming_pdu, Some(room_id))
				.await;
			return Err!(Request(InvalidParam(debug_error!(
				"Could not fetch all auth events for outlier {event_id}, still missing: {id}"
			))));
		};

		check_room_id(room_id, &auth_event)?;

		let Some(ref state_key) = auth_event.state_key else {
			self.services
				.pdu_metadata
				.mark_event_rejected(event_id, RejectionCode::InvalidPduFormat.tag())
				.await;
			self.services
				.outlier
				.add_pdu_outlier(pdu_event.event_id(), &incoming_pdu, Some(room_id))
				.await;
			return Err!(Request(InvalidParam(
				"Auth event is not a state event: {}",
				auth_event.event_id()
			)));
		};

		match auth_events_by_key.entry((auth_event.kind.to_string().into(), state_key.clone())) {
			| hash_map::Entry::Vacant(v) => {
				v.insert(auth_event);
			},
			| hash_map::Entry::Occupied(_) => {
				self.services
					.pdu_metadata
					.mark_event_rejected(event_id, RejectionCode::DuplicateAuthEventKey.tag())
					.await;
				self.services
					.outlier
					.add_pdu_outlier(pdu_event.event_id(), &incoming_pdu, Some(room_id))
					.await;
				return Err!(Request(InvalidParam(
					"Auth event's type and state_key combination exists multiple times: {}, {}",
					auth_event.kind,
					auth_event.state_key().unwrap_or("")
				)));
			},
		}
	}

	// MSC4242 (state DAGs): events carry `prev_state_events` instead of
	// `auth_events`, so there is nothing to auth-check here against claimed
	// auth events. Authorisation is evaluated against state-at-event (resolved
	// from the state DAG) downstream in `upgrade_outlier_to_timeline_pdu`.
	if to_room_version(&room_version_id).state_dags {
		trace!("State DAG room: skipping claimed auth_events validation for {event_id}");
	} else {
		// The original create event must be in the auth events for v11 and below.
		// The create event itself has an empty auth_events array (it's the DAG root).
		// For v12+, create is not required in auth_events.
		if pdu_event.kind != TimelineEventType::RoomCreate
			&& !to_room_version(&room_version_id).room_ids_as_hashes
			&& !auth_events_by_key
				.contains_key(&(StateEventType::RoomCreate, String::new().into()))
		{
			self.services
				.pdu_metadata
				.mark_event_rejected(event_id, RejectionCode::MissingCreateEvent.tag())
				.await;
			self.services
				.outlier
				.add_pdu_outlier(pdu_event.event_id(), &incoming_pdu, Some(room_id))
				.await;
			return Err!(Request(InvalidParam(
				"Incoming event missing m.room.create in auth events"
			)));
		}

		let state_provider =
			crate::rooms::auth_adapter::PduStateProvider::from_ruma_map(&auth_events_by_key)
				.with_create_event(create_event);
		let auth_check = crate::rooms::auth_adapter::rezzy_auth_check(
			&pdu_event,
			&state_provider,
			crate::rooms::auth_adapter::to_state_res_version(&room_version_id),
		);

		if !auth_check {
			self.services
				.pdu_metadata
				.mark_event_rejected(event_id, RejectionCode::AuthCheckFailed.tag())
				.await;
			self.services
				.outlier
				.add_pdu_outlier(pdu_event.event_id(), &incoming_pdu, Some(room_id))
				.await;
			return Err!(Request(Forbidden(
				"Event authorisation fails based on event's claimed auth events"
			)));
		}
	}

	trace!("Validation successful.");

	// 7. Persist the event as an outlier.
	self.services
		.outlier
		.add_pdu_outlier(pdu_event.event_id(), &incoming_pdu, Some(room_id))
		.await;

	// Full validation completed successfully: clear any pending/rejected
	// mark left behind by an earlier attempt (e.g. the placeholder set when
	// entering the retry branch above, or a stale mark from before this
	// function was called at all). Without this, a prior rejection --
	// retryable or not -- would keep shadowing an event that has now
	// genuinely passed auth.
	self.services.pdu_metadata.unmark_event_rejected(event_id);

	trace!("Added pdu as outlier.");

	Ok((pdu_event, incoming_pdu))
}

/// Persist a permanent rejection for an event whose content-hash mismatch
/// couldn't even be recovered via redaction (`redact()` itself failed,
/// meaning the JSON is too malformed to canonicalize at all -- not merely
/// "needs redacting"). Without this, `is_event_permanently_rejected` sees
/// nothing was ever persisted for this event, and a recursive auth-chain
/// caller misfiles this permanent, intrinsic failure as merely
/// `still_missing`/retryable.
#[implement(super::Service)]
async fn mark_redaction_failure_rejected(
	&self,
	event_id: &EventId,
	room_id: &RoomId,
	value: CanonicalJsonObject,
) {
	let mut value = value;
	value.insert("event_id".to_owned(), CanonicalJsonValue::String(event_id.as_str().to_owned()));
	self.services
		.outlier
		.add_pdu_outlier(event_id, &value, Some(room_id))
		.await;
	self.services
		.pdu_metadata
		.mark_event_rejected(event_id, RejectionCode::InvalidPduFormat.tag())
		.await;
}

#[implement(super::Service)]
#[allow(clippy::too_many_arguments)]
async fn resolve_missing_outlier_auth_events<'a, Pdu>(
	&self,
	origin: &'a ServerName,
	create_event: Option<&'a Pdu>,
	event_id: &'a EventId,
	room_id: &'a RoomId,
	pdu_event: &PduEvent,
	incoming_pdu: &CanonicalJsonObject,
	room_version_id: &ruma::RoomVersionId,
	room_version_override: Option<&'a ruma::RoomVersionId>,
	missing_auth_events: &[&EventId],
	auth_events: &mut HashMap<OwnedEventId, PduEvent>,
	auth_recovery_stage: AuthRecoveryStage,
) -> Result<()>
where
	Pdu: Event + Send + Sync,
{
	const MAX_INLINE_FETCH: usize = 5;

	for mid in missing_auth_events {
		if self
			.services
			.pdu_metadata
			.is_event_permanently_rejected(mid)
			.await
		{
			self.services
				.pdu_metadata
				.mark_event_rejected(
					event_id,
					&RejectionCode::DependsOnRejectedAuthEvent.with_detail(mid),
				)
				.await;
			self.services
				.outlier
				.add_pdu_outlier(pdu_event.event_id(), incoming_pdu, Some(room_id))
				.await;
			return Err!(Request(Forbidden("Event depends on rejected auth event {mid}")));
		}
	}

	if missing_auth_events.len() > MAX_INLINE_FETCH {
		info!(
			"Missing {} auth events for {event_id}; will be resolved via /state_ids retry",
			missing_auth_events.len()
		);
		let missing: Vec<_> = missing_auth_events
			.iter()
			.map(|id| (*id).to_owned())
			.collect();
		// Mark as pending/retryable *before* persisting the outlier: a bare
		// outlier with no rejection record would be indistinguishable from a
		// fully-validated one to the "already known" early return at the top
		// of `handle_outlier_pdu`, letting a later invocation return it
		// without ever re-running auth checks.
		self.services
			.pdu_metadata
			.mark_event_rejected(event_id, RejectionCode::MissingAuthEvent.tag())
			.await;
		self.services
			.outlier
			.add_pdu_outlier(pdu_event.event_id(), incoming_pdu, Some(room_id))
			.await;
		return Err!(MissingAuthEvents(missing));
	}

	if matches!(auth_recovery_stage, AuthRecoveryStage::BeforeStateIds) {
		info!(
			target: "state_res_debug",
			%event_id,
			count = missing_auth_events.len(),
			"Deferring /event_auth fallback until after /state_ids retry"
		);
		let missing: Vec<_> = missing_auth_events
			.iter()
			.map(|id| (*id).to_owned())
			.collect();
		// See comment above: mark pending/retryable before persisting, so a
		// later invocation doesn't treat this outlier as already-validated.
		self.services
			.pdu_metadata
			.mark_event_rejected(event_id, RejectionCode::MissingAuthEvent.tag())
			.await;
		self.services
			.outlier
			.add_pdu_outlier(pdu_event.event_id(), incoming_pdu, Some(room_id))
			.await;
		return Err!(MissingAuthEvents(missing));
	}

	// This is the last-resort fallback for missing auth events: we already
	// tried a local lookup (timeline + outlier store) above and came up
	// short. A bulk /event_auth call is efficient, but it's also the
	// federation request most likely to be unhandled or misbehave on the
	// remote (see: TestCorruptedAuthChain, where an unregistered /event_auth
	// handler turned a partial-chain scenario into a fatal 404). If this
	// still cannot resolve the chain, the caller decides whether that should
	// become a retryable rejection or a clean ACK.
	warn!(
		target: "state_res_debug",
		%event_id,
		count = missing_auth_events.len(),
		missing = ?missing_auth_events,
		"Falling back to /event_auth for missing auth events"
	);

	let mut rejected_in_chain = std::collections::BTreeSet::<OwnedEventId>::new();
	if let Ok(response) = self
		.services
		.timeline
		.without_cork(|| {
			self.services.sending.send_federation_request(
				origin,
				ruma::api::federation::authorization::get_event_authorization::v1::Request {
					room_id: room_id.to_owned(),
					event_id: event_id.to_owned(),
				},
			)
		})
		.await
	{
		let mut auth_chain_map = HashMap::new();
		info!(
			target: "state_res_debug",
			%event_id,
			chain_len = response.auth_chain.len(),
			"Processing /event_auth response"
		);
		for auth_pdu in &response.auth_chain {
			match conduwuit::matrix::event::gen_event_id_canonical_json(auth_pdu, room_version_id)
			{
				| Ok((ref auth_eid, auth_val)) => {
					match PduEvent::from_id_val(auth_eid, auth_val.clone(), Some(room_id)) {
						| Ok(parsed) =>
							if check_room_id(room_id, &parsed).is_ok() {
								info!(
									target: "state_res_debug",
									%event_id,
									auth_eid = %auth_eid,
									event_type = ?parsed.kind,
									"Parsed auth chain event from /event_auth"
								);
								auth_chain_map
									.insert(auth_eid.clone(), (auth_val.clone(), parsed));
							} else {
								warn!(%event_id, %auth_eid, "room_id mismatch in /event_auth chain");
							},
						| Err(e) => {
							warn!(%event_id, %auth_eid, "Failed to parse auth chain event as PduEvent: {e}");
						},
					}
				},
				| Err(e) => {
					warn!(%event_id, "Failed to gen_event_id from /event_auth chain: {e}");
				},
			}
		}

		let mut in_degree = HashMap::new();
		for (eid, (_, pdu)) in &auth_chain_map {
			let mut count = 0_usize;
			for auth_id in pdu.auth_events() {
				if auth_chain_map.contains_key(auth_id) {
					count = count.saturating_add(1);
				}
			}
			in_degree.insert(eid.clone(), count);
		}

		let mut sorted_auth_chain = Vec::new();
		let mut queue: Vec<_> = in_degree
			.iter()
			.filter_map(|(k, &v)| if v == 0 { Some(k.clone()) } else { None })
			.collect();

		while let Some(eid) = queue.pop() {
			sorted_auth_chain.push(eid.clone());
			for (other_eid, (_, other_pdu)) in &auth_chain_map {
				if other_pdu.auth_events().any(|aid| aid == eid) {
					if let Some(deg) = in_degree.get_mut(other_eid) {
						*deg = deg.saturating_sub(1);
						if *deg == 0 {
							queue.push(other_eid.clone());
						}
					}
				}
			}
		}

		for auth_eid in sorted_auth_chain {
			if let Some((auth_val, _)) = auth_chain_map.remove(&auth_eid) {
				if !auth_events.contains_key(&auth_eid) {
					info!(
						target: "state_res_debug",
						%event_id,
						%auth_eid,
						"Processing auth chain event recursively"
					);
					match Box::pin(self.handle_outlier_pdu(
						origin,
						create_event,
						&auth_eid,
						room_id,
						auth_val,
						true,
						false,
						room_version_override,
						auth_recovery_stage,
					))
					.await
					{
						| Ok((pdu, _)) =>
							if pdu.state_key.is_none() {
								warn!(
									target: "state_res_debug",
									%event_id,
									auth_eid = %pdu.event_id(),
									"Auth chain event from /event_auth is not a state event"
								);
								self.services
									.pdu_metadata
									.mark_event_rejected(
										event_id,
										RejectionCode::InvalidPduFormat.tag(),
									)
									.await;
								self.services
									.outlier
									.add_pdu_outlier(
										pdu_event.event_id(),
										incoming_pdu,
										Some(room_id),
									)
									.await;
								return Err!(Request(InvalidParam(
									"Auth chain event from /event_auth is not a state event"
								)));
							} else {
								info!(
									target: "state_res_debug",
									%event_id,
									%auth_eid,
									resolved_id = %pdu.event_id(),
									"Auth chain event accepted"
								);
								auth_events.insert(pdu.event_id().to_owned(), pdu);
							},
						| Err(ref e) => {
							info!(
								target: "state_res_debug",
								%event_id,
								%auth_eid,
								"Auth chain event rejected/failed: {e}"
							);
							rejected_in_chain.insert(auth_eid.clone());
						},
					}
				} else {
					info!(
						target: "state_res_debug",
						%event_id,
						%auth_eid,
						"Skipping auth chain event, already in auth_events"
					);
				}
			}
		}
	}

	info!(
		target: "state_res_debug",
		%event_id,
		auth_events_count = auth_events.len(),
		rejected_count = rejected_in_chain.len(),
		rejected = ?rejected_in_chain,
		"Re-checking auth events after /event_auth"
	);
	let mut still_missing = Vec::new();
	for id in pdu_event.auth_events() {
		let in_auth = auth_events.contains_key(id);
		let in_rejected = rejected_in_chain.contains(id);
		// Only a *permanent* rejection (bad signature, failed auth check,
		// etc.) may hard-cascade here. `rejected_in_chain` and a bare
		// `is_event_rejected` both include ids whose recursive
		// `handle_outlier_pdu` call merely couldn't resolve them in time
		// (MissingAuthEvents) -- those are pushed to `still_missing` below
		// instead, so they get a retryable verdict rather than a permanent
		// one.
		let permanently_rejected = self
			.services
			.pdu_metadata
			.is_event_permanently_rejected(id)
			.await;
		info!(
			target: "state_res_debug",
			%event_id,
			auth_event_id = %id,
			in_auth,
			in_rejected,
			permanently_rejected,
			"Auth event status"
		);
		if !in_auth {
			if permanently_rejected {
				self.services
					.pdu_metadata
					.mark_event_rejected(
						event_id,
						&RejectionCode::DependsOnRejectedAuthEvent.with_detail(id),
					)
					.await;
				self.services
					.outlier
					.add_pdu_outlier(pdu_event.event_id(), incoming_pdu, Some(room_id))
					.await;
				self.services
					.pdu_metadata
					.mark_event_rejected(
						pdu_event.event_id(),
						&RejectionCode::DependsOnRejectedAuthEvent.with_detail(id),
					)
					.await;
				return Err!(Request(Forbidden("Event depends on rejected auth event {id}")));
			}
			still_missing.push(id.to_owned());
		}
	}

	if !still_missing.is_empty() {
		debug_info!(
			"Still missing {} auth events for {event_id} after /event_auth: {:?}",
			still_missing.len(),
			still_missing
		);

		warn!(
			target: "state_res_debug",
			%event_id,
			count = still_missing.len(),
			missing = ?still_missing,
			"Falling back to /event_auth left auth events unresolved; deferring to caller retry"
		);

		// See the comment at the earlier `MissingAuthEvents` returns: mark
		// pending/retryable before persisting the outlier so a later
		// invocation re-validates instead of trusting this outlier as-is.
		self.services
			.pdu_metadata
			.mark_event_rejected(event_id, RejectionCode::MissingAuthEvent.tag())
			.await;
		self.services
			.outlier
			.add_pdu_outlier(pdu_event.event_id(), incoming_pdu, Some(room_id))
			.await;
		return Err!(MissingAuthEvents(still_missing));
	}

	Ok(())
}

use std::{
	collections::{HashMap, hash_map},
	time::{Duration, Instant},
};

use conduwuit::{
	Err, Event, PduEvent, Result, debug, debug_warn, err, implement, info,
	utils::stream::{BroadbandExt, IterStream},
	warn,
};
use futures::StreamExt;
use slipstream::{
	EventId, OwnedEventId, RoomId, ServerName, api::federation::event::get_room_state_ids,
	events::StateEventType,
};

use crate::rooms::short::ShortStateKey;

/// Redact (when verification only matched signatures), re-attach `unsigned`,
/// and parse one verified event.
///
/// Runs on a blocking worker: canonical-JSON redaction and `PduEvent` parsing
/// are CPU-bound and would otherwise serialize inside one async task.
fn prepare_verified_event(
	event_id: &OwnedEventId,
	mut val: slipstream::CanonicalJsonObject,
	stashed_unsigned: Option<slipstream::CanonicalJsonValue>,
	needs_redaction: bool,
	room_id: &slipstream::OwnedRoomId,
	room_version: &slipstream::RoomVersionId,
) -> (Result<Option<PduEvent>, String>, slipstream::CanonicalJsonObject) {
	if needs_redaction {
		if let Err(error) =
			slipstream::canonical_json::redact_in_place(&mut val, room_version, None)
		{
			val.insert(
				"event_id".to_owned(),
				slipstream::CanonicalJsonValue::String(event_id.as_str().to_owned()),
			);
			return (Err(format!("{error:?}")), val);
		}
	}

	if let Some(slipstream::CanonicalJsonValue::Object(unsigned)) = stashed_unsigned
		&& !unsigned.is_empty()
	{
		val.insert("unsigned".to_owned(), slipstream::CanonicalJsonValue::Object(unsigned));
	}

	val.insert(
		"event_id".to_owned(),
		slipstream::CanonicalJsonValue::String(event_id.as_str().to_owned()),
	);

	let pdu = PduEvent::from_id_val(event_id, val.clone(), Some(room_id))
		.ok()
		.filter(|pdu| crate::rooms::event_handler::check_room_id(room_id, pdu).is_ok());

	(Ok(pdu), val)
}

/// Call /state to find out what the state at this pdu is. We trust the
/// server's response to some extent, but we still do a lot of checks
/// on the events.
#[implement(super::Service)]
pub(crate) async fn fetch_state<Pdu>(
	&self,
	origin: &ServerName,
	create_event: &Pdu,
	room_id: &RoomId,
	event_id: &EventId,
	skip_sig_verify: bool,
) -> Result<Option<HashMap<u64, OwnedEventId>>>
where
	Pdu: Event + Send + Sync,
{
	let mut pool = self.build_server_pool(room_id, origin, 5).await;
	let mut last_err = err!(Request(NotFound("No server could provide /state")));

	let weights = &[
		("rank", 0.1),
		("latency", 1.0),
		("errors", 1000.0),
		("rate_limits", 2000.0),
		("dead_ends", 500.0),
		("consecutive_picks", 100.0),
	];

	let (state_pdu_ids, fetched_unknown_events): (
		Vec<OwnedEventId>,
		Vec<(OwnedEventId, conduwuit::matrix::pdu::RawJson)>,
	) = 'found: {
		while let Some(server) = pool.next_scored(weights) {
			info!(target: "state_ids_debug", %server, %origin, %room_id, %event_id, "fetch_state: sending /state_ids");
			let state_ids_started = Instant::now();
			let req = self.services.sending.send_federation_request(
				&server,
				get_room_state_ids::v1::Request::new(event_id.to_owned(), room_id.to_owned()),
			);

			let timeout = Duration::from_secs(self.services.server.config.federation_timeout);
			let state_ids_res = match conduwuit::timeout(timeout, req).await {
				| Ok(Ok(res)) => res,
				| Ok(Err(e)) => {
					info!(%server, "fetch_state /state_ids failed: {e}");
					if super::server_pool::ServerPool::is_rate_limit(&e.to_string()) {
						pool.record_rate_limit(&server);
					} else {
						pool.record_error(&server);
					}
					last_err = e;
					continue;
				},
				| Err(_) => {
					let e = err!(Request(Unknown("Server took too long to return /state_ids")));
					info!(%server, "fetch_state /state_ids failed: {e}");
					pool.record_dead_end(&server);
					last_err = e;
					continue;
				},
			};
			info!(
				target: "state_ids_debug",
				%server,
				%room_id,
				%event_id,
				elapsed_ms = state_ids_started.elapsed().as_millis(),
				auth_chain_count = state_ids_res.auth_chain_ids.len(),
				state_count = state_ids_res.pdu_ids.len(),
				"fetch_state: received /state_ids"
			);

			let mut missing_ids = Vec::new();
			let mut known_count: usize = 0;

			let all_ids = state_ids_res
				.auth_chain_ids
				.iter()
				.chain(state_ids_res.pdu_ids.iter())
				.filter_map(|id| match OwnedEventId::parse(id.as_str()) {
					| Ok(id) => Some(id),
					| Err(_) => {
						conduwuit::debug_warn!(
							"Skipping invalid event ID in remote state response"
						);
						None
					},
				})
				.collect::<Vec<_>>();

			// Room-scoped: an ID stored under another room is treated as missing
			// here and rejected by `check_room_id` after the fetch. A batched
			// read failure must not abort state recovery, so fall back to
			// fetching every ID.
			let existence_check_started = Instant::now();
			let present = match self.services.timeline.event_ids_present(room_id, &all_ids) {
				| Ok(present) => present,
				| Err(error) => {
					debug_warn!("fetch_state: batched event existence check failed: {error}");
					vec![false; all_ids.len()]
				},
			};
			for (id, exists) in all_ids.into_iter().zip(present) {
				if exists {
					known_count = known_count.saturating_add(1);
				} else {
					missing_ids.push(id);
				}
			}

			missing_ids.sort_unstable();
			missing_ids.dedup();
			info!(
				target: "state_ids_debug",
				%room_id,
				%event_id,
				elapsed_ms = existence_check_started.elapsed().as_millis(),
				total_ids = state_ids_res
					.auth_chain_ids
					.len()
					.saturating_add(state_ids_res.pdu_ids.len()),
				known_count,
				missing_count = missing_ids.len(),
				"fetch_state: checked local event bodies"
			);

			debug!(
				auth_chain_count = state_ids_res.auth_chain_ids.len(),
				state_count = state_ids_res.pdu_ids.len(),
				missing_count = missing_ids.len(),
				known_count = known_count,
				"Processing /state_ids response from remote server"
			);

			let event_fetch_started = Instant::now();
			let fetch_futures = missing_ids.into_iter().map(|eid| {
				let server = server.clone();
				async move {
					let req = slipstream::api::federation::event::get_event::v1::Request::new(
						eid.clone(),
						None,
					);
					match self
						.services
						.sending
						.send_federation_request(&server, req)
						.await
					{
						| Ok(res) => match res.pdus.into_iter().next() {
							| Some(pdu) =>
								Ok::<_, (OwnedEventId, conduwuit::Error)>((eid.clone(), pdu)),
							| None => Err((
								eid.clone(),
								err!(Request(NotFound("Empty pdus in get_event response"))),
							)),
						},
						| Err(e) => Err((eid.clone(), e)),
					}
				}
			});

			let mut fetch_stream = futures::stream::iter(fetch_futures).buffer_unordered(20);
			let mut fetched_events: Vec<(OwnedEventId, conduwuit::matrix::pdu::RawJson)> =
				Vec::new();
			let mut failed_ids: Vec<OwnedEventId> = Vec::new();

			// A single unfetchable event (e.g. an old member event whose
			// origin server never shared it, or a permanently corrupted
			// branch of the auth chain) must not sink the entire /state_ids
			// response. Matches Synapse's `_get_state_ids_after_missing_prev_event`,
			// which treats a partial fetch failure as a warning, not a
			// blocker: it builds the state map from whatever it *did* manage
			// to fetch, and lets normal auth-checking reject just the
			// specific events that end up depending on what's missing.
			// Aborting the whole ladder here on the first 404 previously
			// caused this exact PDU's state resolution to nondeterministically
			// hard-fail depending on unrelated fetch-completion ordering
			// (see TestCorruptedAuthChain).
			while let Some(result) = fetch_stream.next().await {
				match result {
					| Ok((eid, raw_json)) => {
						fetched_events.push((eid, raw_json));
					},
					| Err((eid, e)) => {
						info!(%server, "fetch_state /event/{eid} failed: {e}");
						failed_ids.push(eid);
					},
				}
			}
			info!(
				target: "state_ids_debug",
				%server,
				%room_id,
				%event_id,
				elapsed_ms = event_fetch_started.elapsed().as_millis(),
				fetched_count = fetched_events.len(),
				failed_count = failed_ids.len(),
				"fetch_state: completed /event fetches"
			);

			if !failed_ids.is_empty() {
				// A large /state_ids response can make `failed_ids` huge; only log a
				// bounded sample of the offending event IDs (distinct from the total
				// count) instead of serializing the whole vector into this warn.
				let failed_sample: Vec<_> = failed_ids.iter().take(10).collect();
				warn!(
					%server,
					count = failed_ids.len(),
					missing = ?failed_sample,
					"fetch_state: failed to fetch some referenced events; proceeding with \
					 partial state, dependents will be rejected by normal auth checks"
				);
			}

			pool.record_success(&server);
			if server != *origin {
				debug!(%server, "fetch_state: used fallback server for /state_ids -> /event/ ladder");
			}

			break 'found (state_ids_res.pdu_ids, fetched_events);
		}

		warn!(
			"fetch_state: all servers failed /state_ids -> /event/ for {event_id}. pool \
			 stats:\n{}",
			pool.summary()
		);
		return Err(last_err);
	};

	let room_version_id = self.services.state.get_room_version(room_id).await?;
	let key_acquisition_started = Instant::now();
	self.services
		.server_keys
		.acquire_events_pubkeys(fetched_unknown_events.iter().map(|(_, raw_json)| raw_json))
		.await;
	info!(
		target: "state_ids_debug",
		%room_id,
		%event_id,
		elapsed_ms = key_acquisition_started.elapsed().as_millis(),
		fetched_event_count = fetched_unknown_events.len(),
		"fetch_state: pre-acquired signing keys for fetched events"
	);

	// Reconstruct unknown_events
	let mut unknown_events = Vec::new();
	for (eid, raw_json) in fetched_unknown_events {
		if let Ok((parsed_eid, val)) =
			conduwuit::matrix::event::gen_event_id_canonical_json(&raw_json, &room_version_id)
		{
			if parsed_eid == eid {
				unknown_events.push((eid, val));
			} else {
				warn!("Event ID mismatch for fetched event: expected {eid}, got {parsed_eid}");
			}
		}
	}

	debug!(
		"fetch_state: {} newly missing events fetched successfully",
		unknown_events.len(),
	);
	let key_metadata_started = Instant::now();
	let key_metadata = self
		.services
		.server_keys
		.event_key_metadata_for_events(&unknown_events, &room_version_id)
		.await;
	info!(
		target: "state_ids_debug",
		%room_id,
		%event_id,
		elapsed_ms = key_metadata_started.elapsed().as_millis(),
		fetched_event_count = unknown_events.len(),
		key_origin_count = key_metadata.origin_count(),
		"fetch_state: loaded signing-key metadata for fetched events"
	);

	// Concurrently verify signatures and prepare fetched events for PDU parsing.
	// Use an explicit width: the automatic stream width follows the database
	// worker topology and can be as low as 2, which serializes this CPU-bound
	// batch badly (MSC4297 state responses can carry hundreds of auth events).
	let signature_verification_started = Instant::now();
	let signature_verification_concurrency = self
		.services
		.server
		.config
		.fetch_state_signature_verification_concurrency;
	let mut verified_events: HashMap<OwnedEventId, (PduEvent, slipstream::CanonicalJsonObject)> =
		unknown_events
			.into_iter()
			.stream()
			.broadn_filter_map(signature_verification_concurrency, {
				let room_version_id = room_version_id.clone();
				let key_metadata = key_metadata.clone();
				move |(eid, mut val): (OwnedEventId, slipstream::CanonicalJsonObject)| {
					let room_version_id = room_version_id.clone();
					let key_metadata = key_metadata.clone();
					async move {
						let stashed_unsigned = val.remove("unsigned");

						let verification_result = if skip_sig_verify
							|| self
								.services
								.server
								.config
								.bypassed_signature_events
								.contains(&eid)
						{
							Ok(slipstream::signatures::Verified::All)
						} else {
							self.services
								.server_keys
								.verify_event_at_with_metadata(
									&val,
									Some(&room_version_id),
									"fetch_state::broad_filter_map",
									Some(&key_metadata),
								)
								.await
						};

						match verification_result {
							| Ok(
								slipstream::signatures::Verified::All
								| slipstream::signatures::Verified::Signatures,
							) => {
								let needs_redaction = matches!(
									verification_result,
									Ok(slipstream::signatures::Verified::Signatures)
								);
								let event_id = eid.clone();
								let worker_room_id = room_id.to_owned();
								let worker_room_version = room_version_id.clone();
								let prepared = self
									.services
									.server
									.runtime()
									.spawn_blocking(move || {
										prepare_verified_event(
											&event_id,
											val,
											stashed_unsigned,
											needs_redaction,
											&worker_room_id,
											&worker_room_version,
										)
									})
									.await;
								let (prepared, val) = match prepared {
									| Ok(prepared) => prepared,
									| Err(error) => {
										conduwuit::warn!(%eid, "Failed to prepare verified event: {error}");
										return None;
									},
								};
								if let Err(error) = prepared {
									conduwuit::warn!("Redaction failed for {eid}: {error}");
									if let Err(error) = self
										.services
										.pdu_metadata
										.mark_event_rejected(
											&eid,
											&crate::rooms::pdu_metadata::RejectionCode::InvalidPduFormat
												.with_detail("redaction failed after hash mismatch"),
										)
									{
										conduwuit::warn!(%error, %eid, "failed to record rejection verdict");
									}
									self.services
										.outlier
										.add_pdu_outlier(&eid, &val, Some(room_id))
										.await;
									return None;
								}
								return prepared.ok().flatten().map(|pdu| (eid, (pdu, val)));
							},
							| _ => {
								// Event sig failed; persist as rejected outlier so we don't
								// re-fetch
								if let Err(error) = self
									.services
									.pdu_metadata
									.mark_event_rejected(
										&eid,
										crate::rooms::pdu_metadata::RejectionCode::SignatureVerificationFailed
											.tag(),
									)
								{
									conduwuit::warn!(%error, %eid, "failed to record rejection verdict");
								}
								val.insert(
									"event_id".to_owned(),
									slipstream::CanonicalJsonValue::String(
										eid.as_str().to_owned(),
									),
								);
								self.services
									.outlier
									.add_pdu_outlier(&eid, &val, Some(room_id))
									.await;
							},
						}
						None
					}
				}
			})
			.collect()
			.await;
	info!(
		target: "state_ids_debug",
		%room_id,
		%event_id,
		elapsed_ms = signature_verification_started.elapsed().as_millis(),
		verified_count = verified_events.len(),
		key_metadata_fallbacks = key_metadata.fallback_count(),
		"fetch_state: signature verification completed"
	);

	// Tologically sort the verified events based on auth_events
	let mut graph = HashMap::new();
	let mut entries = HashMap::new();
	for (eid, (pdu, _)) in &verified_events {
		graph.insert(eid.clone(), pdu.auth_events().map(ToOwned::to_owned).collect());
		entries.insert(eid.clone(), (0_u64.into(), pdu.depth(), pdu.origin_server_ts));
	}
	let sorted_eids = conduwuit::utils::timeline_sorter::sort_timeline_events(&entries, &graph);

	// Persist the whole verified batch in bounded mtxdb transactions. Each
	// outlier write would otherwise publish and materialize its own journal
	// group; a large /state_ids response (hundreds of auth-chain events)
	// exceeds the federation transaction deadline that way.
	//
	// These events have passed signature verification but not the auth pass
	// below. `force_auth_check` makes `handle_outlier_pdu` validate them
	// instead of taking its "already known" early return; otherwise a
	// corrupted chain (an event whose auth event could not be fetched) would
	// be trusted just because its body was pre-persisted.
	let verified_batch = sorted_eids
		.iter()
		.filter_map(|eid| verified_events.get(eid))
		.map(|(pdu, val)| (pdu, val))
		.collect::<Vec<_>>();
	if let Err(error) = self
		.services
		.timeline
		.write_events_to_mtxdb(room_id, &verified_batch)
		.await
	{
		debug_warn!("fetch_state: failed to pre-persist verified events to mtxdb: {error}");
	}

	// Auth-check them sequentially, roots first, so only accepted predecessors
	// are available when validating descendants.
	let outlier_count = sorted_eids.len();
	let outlier_processing_started = Instant::now();
	let mut accepted_outliers = 0_usize;
	let mut rejected_outliers = 0_usize;
	for eid in sorted_eids {
		if let Some((_, val)) = verified_events.remove(&eid) {
			let outlier_started = Instant::now();
			match Box::pin(self.handle_outlier_pdu(
				origin,
				Some(create_event),
				&eid,
				room_id,
				val,
				true, // is_outlier
				true, // skip_sig_verify (already done above)
				true, // force_auth_check (pre-persisted batch)
				&room_version_id,
				super::AuthRecoveryStage::AfterStateIds,
			))
			.await
			{
				| Ok(_) => {
					accepted_outliers = accepted_outliers.saturating_add(1);
					debug!(
						target: "state_ids_debug",
						%room_id,
						%event_id,
					%eid,
						elapsed_ms = outlier_started.elapsed().as_millis(),
						"fetch_state: accepted and persisted auth-chain outlier"
					);
				},
				| Err(e) => {
					rejected_outliers = rejected_outliers.saturating_add(1);
					debug!(
						target: "state_ids_debug",
						%room_id,
						%event_id,
						%eid,
					error = %e,
						elapsed_ms = outlier_started.elapsed().as_millis(),
						"fetch_state: failed to handle auth-chain outlier: {e}"
					);
					debug_warn!("fetch_state: failed to handle outlier {eid}: {e}");
				},
			}
		}
	}
	info!(
		target: "state_ids_debug",
		%room_id,
		%event_id,
		outlier_count,
		accepted_outliers,
		rejected_outliers,
		elapsed_ms = outlier_processing_started.elapsed().as_millis(),
		"fetch_state: completed ordered outlier auth and persistence"
	);

	// Construct the returned state map
	let mut state: HashMap<ShortStateKey, OwnedEventId> =
		HashMap::with_capacity(state_pdu_ids.len());
	for eid in state_pdu_ids {
		// Read from our outlier store or timeline
		let Ok(pdu) = self.services.timeline.get_pdu(&eid) else {
			continue;
		};
		let Some(state_key) = pdu.state_key() else {
			if self.services.pdu_metadata.is_event_rejected(&eid)? {
				continue;
			}
			return Err!(Database("Found non-state pdu in state events."));
		};

		// A rejected outlier (e.g. missing auth events from a corrupted auth
		// chain) must never represent resolved room state. If we have a valid
		// local prior state event for this slot, use the local event ID;
		// otherwise skip the slot completely.
		let target_eid = if self.services.pdu_metadata.is_event_rejected(&eid)?
			&& !self
				.services
				.pdu_metadata
				.is_event_visible_to_clients(&eid)
				.await
		{
			let local_eid = match self.services.state.get_room_state_hamt(room_id).await {
				| Ok(room_root) => self
					.services
					.state_accessor
					.state_get_in_room_hamt(
						room_id,
						&room_root,
						&pdu.kind().to_string().into(),
						state_key,
					)
					.await
					.ok()
					.map(|local_pdu| local_pdu.event_id().to_owned()),
				| Err(_) => None,
			};
			let Some(local_eid) = local_eid else {
				continue;
			};
			local_eid
		} else {
			eid
		};

		let shortstatekey = self
			.services
			.short
			.get_or_create_shortstatekey(&pdu.kind().to_string().into(), state_key)
			.await;

		match state.entry(shortstatekey) {
			| hash_map::Entry::Vacant(v) => {
				v.insert(target_eid);
			},
			| hash_map::Entry::Occupied(_) => {
				return Err!(Database(
					"State event's type and state_key combination exists multiple times: {}, {}",
					pdu.kind(),
					state_key
				));
			},
		}
	}

	// The original create event must still be in the state
	let create_shortstatekey = self
		.services
		.short
		.get_shortstatekey(&StateEventType::RoomCreate, "")
		.await?;

	if state.get(&create_shortstatekey).map(AsRef::as_ref) != Some(create_event.event_id()) {
		return Err!(Database("Incoming event refers to wrong create event."));
	}

	Ok(Some(state))
}

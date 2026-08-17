use std::{
	collections::{HashMap, hash_map},
	time::Duration,
};

use conduwuit::{
	Err, Event, PduEvent, Result, debug, debug_warn, err, implement, info,
	utils::stream::{BroadbandExt, IterStream},
	warn,
};
use futures::StreamExt;
use ruma::{
	EventId, OwnedEventId, RoomId, ServerName, api::federation::event::get_room_state_ids,
	events::StateEventType,
};

use crate::rooms::short::ShortStateKey;

/// Call /state to find out what the state at this pdu is. We trust the
/// server's response to some extent, but we still do a lot of checks
/// on the events.
#[implement(super::Service)]
#[tracing::instrument(
	level = "debug",
	skip_all,
	fields(%origin),
)]
pub(super) async fn fetch_state<Pdu>(
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
		Vec<(OwnedEventId, Box<serde_json::value::RawValue>)>,
	) = 'found: {
		while let Some(server) = pool.next_scored(weights) {
			let req = self.services.sending.send_federation_request(
				&server,
				get_room_state_ids::v1::Request::new(event_id.to_owned(), room_id.to_owned()),
			);

			let timeout = Duration::from_secs(self.services.server.config.federation_timeout);
			let state_ids_res = match tokio::time::timeout(timeout, req).await {
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

			let mut missing_ids = Vec::new();
			let mut known_count: usize = 0;

			let all_ids = state_ids_res
				.auth_chain_ids
				.iter()
				.chain(state_ids_res.pdu_ids.iter());

			for id in all_ids {
				if !self.services.timeline.pdu_exists(id).await {
					missing_ids.push(id.clone());
				} else {
					known_count = known_count.saturating_add(1);
				}
			}

			missing_ids.sort_unstable();
			missing_ids.dedup();

			debug!(
				auth_chain_count = state_ids_res.auth_chain_ids.len(),
				state_count = state_ids_res.pdu_ids.len(),
				missing_count = missing_ids.len(),
				known_count = known_count,
				"Processing /state_ids response from remote server"
			);

			let fetch_futures = missing_ids.into_iter().map(|eid| {
				let server = server.clone();
				async move {
					let req = ruma::api::federation::event::get_event::v1::Request::new(
						(*eid).to_owned(),
						None,
					);
					match self
						.services
						.sending
						.send_federation_request(&server, req)
						.await
					{
						| Ok(res) => Ok::<_, (OwnedEventId, conduwuit::Error)>((
							(*eid).to_owned(),
							res.pdu,
						)),
						| Err(e) => Err(((*eid).to_owned(), e)),
					}
				}
			});

			let mut fetch_stream = futures::stream::iter(fetch_futures).buffer_unordered(20);
			let mut fetched_events: Vec<(OwnedEventId, Box<serde_json::value::RawValue>)> =
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

	// Concurrently parse and verify signatures (Pure CPU and network keys fetch)
	let mut verified_events: HashMap<OwnedEventId, (PduEvent, ruma::CanonicalJsonObject)> =
		unknown_events
			.into_iter()
			.stream()
			.broad_filter_map({
				let room_version_id = room_version_id.clone();
				move |(eid, mut val): (OwnedEventId, ruma::CanonicalJsonObject)| {
					let room_version_id = room_version_id.clone();
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
							Ok(ruma::signatures::Verified::All)
						} else {
							self.services
								.server_keys
								.verify_event_at(
									&val,
									Some(&room_version_id),
									"fetch_state::broad_filter_map",
								)
								.await
						};

						match verification_result {
							| Ok(
								ruma::signatures::Verified::All
								| ruma::signatures::Verified::Signatures,
							) => {
								if matches!(
									verification_result,
									Ok(ruma::signatures::Verified::Signatures)
								) {
									if let Err(e) = ruma::canonical_json::redact_in_place(
										&mut val,
										&room_version_id,
										None,
									) {
										conduwuit::warn!("Redaction failed for {eid}: {e:?}");
										self.services
											.pdu_metadata
											.mark_event_rejected(
												&eid,
												&crate::rooms::pdu_metadata::RejectionCode::InvalidPduFormat
													.with_detail("redaction failed after hash mismatch"),
											)
											.await;
										val.insert(
											"event_id".to_owned(),
											ruma::CanonicalJsonValue::String(
												eid.as_str().to_owned(),
											),
										);
										self.services
											.outlier
											.add_pdu_outlier(&eid, &val, Some(room_id))
											.await;
										return None;
									}
								}

								// Re-attach unsigned for completeness
								if let Some(ruma::CanonicalJsonValue::Object(unsigned_obj)) =
									stashed_unsigned
								{
									if !unsigned_obj.is_empty() {
										val.insert(
											"unsigned".to_owned(),
											ruma::CanonicalJsonValue::Object(unsigned_obj),
										);
									}
								}

								val.insert(
									"event_id".to_owned(),
									ruma::CanonicalJsonValue::String(eid.as_str().to_owned()),
								);

								if let Ok(pdu) =
									PduEvent::from_id_val(&eid, val.clone(), Some(room_id))
								{
									if crate::rooms::event_handler::check_room_id(room_id, &pdu)
										.is_ok()
									{
										return Some((eid, (pdu, val)));
									}
								}
							},
							| _ => {
								// Event sig failed; persist as rejected outlier so we don't
								// re-fetch
								self.services
									.pdu_metadata
									.mark_event_rejected(
										&eid,
										crate::rooms::pdu_metadata::RejectionCode::SignatureVerificationFailed
											.tag(),
									)
									.await;
								val.insert(
									"event_id".to_owned(),
									ruma::CanonicalJsonValue::String(eid.as_str().to_owned()),
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

	// Tologically sort the verified events based on auth_events
	let mut graph = HashMap::new();
	let mut entries = HashMap::new();
	for (eid, (pdu, _)) in &verified_events {
		graph.insert(eid.clone(), pdu.auth_events().map(ToOwned::to_owned).collect());
		entries
			.insert(eid.clone(), (0_u64.into(), pdu.depth().into(), pdu.origin_server_ts.into()));
	}
	let sorted_eids = conduwuit::utils::timeline_sorter::sort_timeline_events(&entries, &graph);

	// Sequentially auth_check them (roots first so auth checks succeed since
	// handle_outlier_pdu queries the DB).
	for eid in sorted_eids {
		if let Some((_, val)) = verified_events.remove(&eid) {
			if let Err(e) = Box::pin(self.handle_outlier_pdu(
				origin,
				Some(create_event),
				&eid,
				room_id,
				val,
				true, // is_outlier
				true, // skip_sig_verify (already done above)
				Some(&room_version_id),
				super::AuthRecoveryStage::AfterStateIds,
			))
			.await
			{
				debug_warn!("fetch_state: failed to handle outlier {eid}: {e}");
			}
		}
	}

	// Construct the returned state map
	let mut state: HashMap<ShortStateKey, OwnedEventId> =
		HashMap::with_capacity(state_pdu_ids.len());
	for eid in state_pdu_ids {
		// Read from our outlier store or timeline
		let pdu = self.services.timeline.get_pdu(&eid).await;
		if let Ok(pdu) = pdu {
			let state_key = pdu
				.state_key()
				.ok_or_else(|| err!(Database("Found non-state pdu in state events.")))?;

			let shortstatekey = self
				.services
				.short
				.get_or_create_shortstatekey(&pdu.kind().to_string().into(), state_key)
				.await;

			match state.entry(shortstatekey) {
				| hash_map::Entry::Vacant(v) => {
					v.insert(eid.clone());
				},
				| hash_map::Entry::Occupied(_) => {
					return Err!(Database(
						"State event's type and state_key combination exists multiple times: \
						 {}, {}",
						pdu.kind(),
						state_key
					));
				},
			}
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

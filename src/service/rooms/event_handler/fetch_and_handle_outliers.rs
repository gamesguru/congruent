use std::{
	collections::{BTreeMap, HashMap, HashSet, hash_map},
	time::Instant,
};

use conduwuit::{
	Event, PduEvent, debug, implement, info, matrix::event::gen_event_id_canonical_json,
	state_res, trace, utils::continue_exponential_backoff_secs, warn,
};
use conduwuit_core::debug_info;
use futures::{
	FutureExt, future,
	stream::{FuturesUnordered, StreamExt},
};
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, EventId, OwnedEventId, OwnedRoomId, OwnedServerName,
	RoomId, ServerName,
	api::federation::{authorization::get_event_authorization, event::get_event},
};

enum FetchResult {
	Event(OwnedEventId, conduwuit::Result<(get_event::v1::Response, OwnedServerName)>),
	AuthChain(
		OwnedEventId,
		conduwuit::Result<(get_event_authorization::v1::Response, OwnedServerName)>,
	),
}

#[implement(super::Service)]
#[allow(clippy::too_many_arguments)]
pub async fn fetch_and_handle_outliers<'a, Pdu, Events>(
	&self,
	origin: &'a ServerName,
	events: Events,
	create_event: Option<&'a Pdu>,
	room_id: &'a RoomId,
	skip_sig_verify: bool,
	room_version_override: Option<&'a ruma::RoomVersionId>,
	explicit_routing_servers: Option<Vec<OwnedServerName>>,
) -> Vec<(PduEvent, Option<BTreeMap<String, CanonicalJsonValue>>)>
where
	Pdu: Event + Send + Sync,
	Events: Iterator<Item = &'a EventId> + Clone + Send,
{
	let back_off = |id| match self
		.services
		.globals
		.bad_event_ratelimiter
		.write()
		.entry(id)
	{
		| hash_map::Entry::Vacant(e) => {
			e.insert((Instant::now(), 1));
		},
		| hash_map::Entry::Occupied(mut e) => {
			*e.get_mut() = (Instant::now(), e.get().1.saturating_add(1));
		},
	};

	// Shared by every site that decides whether to fetch a missing *auth*
	// event (as opposed to `back_off`'s per-event write side, or the
	// differently-tuned backoff window used for top-level timeline events)
	// so the two durations below can't drift out of sync between callers.
	let auth_event_ratelimited = |id: &EventId| {
		self.services
			.globals
			.bad_event_ratelimiter
			.read()
			.get(id)
			.is_some_and(|(time, tries)| {
				const MIN_DURATION: u64 = 60 * 2;
				const MAX_DURATION: u64 = 60 * 60 * 8;
				continue_exponential_backoff_secs(
					MIN_DURATION,
					MAX_DURATION,
					time.elapsed(),
					*tries,
				)
			})
	};

	let mut routing_servers = if let Some(explicit) = explicit_routing_servers {
		explicit
	} else {
		self.build_federation_server_list(
			room_id,
			origin,
			self.services.server.config.federation_fallback_room_servers,
		)
		.await
	};

	if routing_servers.len() > 2 {
		conduwuit::utils::shuffle(&mut routing_servers[1..]);
	}
	routing_servers.truncate(4);

	debug!(
		origin = %origin,
		n_total = routing_servers.len(),
		"Built federation fallback server list for outlier fetching"
	);

	let mut fetched_info: HashMap<OwnedEventId, CanonicalJsonObject> = HashMap::new();
	let mut graph: HashMap<OwnedEventId, HashSet<OwnedEventId>> = HashMap::with_capacity(128);
	let mut auth_chain_fetched: HashSet<OwnedEventId> = HashSet::with_capacity(128);
	let mut individual_auth_fetch_attempted: HashSet<OwnedEventId> = HashSet::new();
	// `graph` membership means "known to the topological sort", not "we've
	// issued a fetch for this ID" -- tier 1 below inserts placeholder graph
	// nodes for missing auth events regardless of whether the bulk fetch
	// actually resolves them, so gating the individual-fetch fallback on
	// `!graph.contains_key(id)` would always be false by the time it runs.
	// Track fetch-attempted IDs separately.
	let mut individually_fetch_in_flight: HashSet<OwnedEventId> = HashSet::new();
	let mut active_fetches = FuturesUnordered::new();
	let fetch_concurrency = std::sync::Arc::new(tokio::sync::Semaphore::new(
		self.services.server.concurrency_scaled(2),
	));
	let limit = self.services.server.config.max_fetch_prev_events;

	macro_rules! push_generic_fetch {
		($fetches:expr, $event_id:expr, $is_retry:expr, $retry_msg:literal, $fail_msg:literal, $exhaust_msg:literal, $not_found_msg:literal, $req_closure:expr, $result_variant:path $(, $extra_capture:ident)*) => {
			let servers = routing_servers.clone();
			let sem = fetch_concurrency.clone();
			let event_id = $event_id;
			let is_retry = $is_retry;
			$( let $extra_capture = $extra_capture.clone(); )*
			$fetches.push(
				async move {
					let _permit = sem.acquire().await;
					for attempt in 0..2_u8 {
						if is_retry && attempt > 0 {
							tokio::time::sleep(std::time::Duration::from_secs(
								2_u64.pow(attempt.into()),
							))
							.await;
							debug!(%event_id, attempt, $retry_msg);
						}
						let reqs = servers.iter().enumerate().map(|(i, server)| {
							let event_id = event_id.clone();
							$( let $extra_capture = $extra_capture.clone(); )*
							let req = $req_closure(event_id.clone() $(, $extra_capture.clone() )*);
							async move {
								let start = Instant::now();
								match self
									.services
									.sending
									.send_federation_request(server, req)
									.await
								{
									| Ok(res) => {
										self.update_peer_stats(server, true, start.elapsed());
										Ok((res, server.clone()))
									},
									| Err(e) => {
										self.update_peer_stats(server, false, start.elapsed());
										if i == 0 {
											debug!(%event_id, %server, $fail_msg, e);
										}
										Err(e)
									},
								}
							}
							.boxed()
						});

						match future::select_ok(reqs).await {
							| Ok((res, _rem)) => return $result_variant(event_id, Ok(res)),
							| Err(_all_errors) =>
								if is_retry {
									info!(%event_id, n_servers = servers.len(), attempt, $exhaust_msg);
								} else {
									debug!(%event_id, n_servers = servers.len(), $exhaust_msg);
									break;
								},
						}
					}
					$result_variant(
						event_id,
						Err(conduwuit::err!(Request(NotFound($not_found_msg)))),
					)
				}
				.boxed(),
			);
		};
	}

	let push_fetch =
		|event_id: OwnedEventId, is_retry: bool, fetches: &mut FuturesUnordered<_>| {
			push_generic_fetch!(
				fetches,
				event_id,
				is_retry,
				"Retrying fetch",
				"Origin server failed fetch: {}",
				"All servers exhausted for fetch",
				"event not found after trying all servers",
				|event_id| get_event::v1::Request {
					event_id,
					include_unredacted_content: None,
				},
				FetchResult::Event
			);
		};

	let push_auth_fetch = |room_id: OwnedRoomId,
	                       event_id: OwnedEventId,
	                       is_retry: bool,
	                       fetches: &mut FuturesUnordered<_>| {
		push_generic_fetch!(
			fetches,
			event_id,
			is_retry,
			"Retrying auth chain fetch",
			"Origin server failed auth fetch: {}",
			"All servers exhausted for auth fetch",
			"auth chain not found after trying all servers",
			|event_id, room_id| get_event_authorization::v1::Request { room_id, event_id },
			FetchResult::AuthChain,
			room_id
		);
	};

	let mut requested_seeds = Vec::new();
	let mut pre_resolved_pdus = Vec::new();

	for id in events {
		requested_seeds.push(id.to_owned());

		if let Ok(local_pdu) = self.services.timeline.get_pdu(id).await {
			if self.services.pdu_metadata.is_event_soft_failed(id).await {
				info!(target: "auth_chain", "Found known soft-failed outlier locally: {id}");
			} else {
				trace!("Found {id} in main timeline or outlier tree");
			}
			pre_resolved_pdus.push((id.to_owned(), local_pdu));
			continue;
		}

		if self.services.pdu_metadata.is_event_soft_failed(id).await {
			warn!(target: "auth_chain", "Skipping unparsable soft-failed outlier: {id}");
			continue;
		}

		if let Some((time, tries)) = self.services.globals.bad_event_ratelimiter.read().get(id) {
			const MIN_DURATION: u64 = 60 * 2;
			const MAX_DURATION: u64 = 60 * 60 * 8;
			if continue_exponential_backoff_secs(
				MIN_DURATION,
				MAX_DURATION,
				time.elapsed(),
				*tries,
			) {
				info!(
					target: "auth_chain",
					tried = ?*tries,
					elapsed = ?time.elapsed(),
					"Backing off from {id} (ratelimited)"
				);
				continue;
			}
		}

		push_fetch(id.to_owned(), false, &mut active_fetches);
		graph.insert(id.to_owned(), HashSet::new());
	}

	let mut processed_pdus: HashMap<
		OwnedEventId,
		(PduEvent, Option<BTreeMap<String, CanonicalJsonValue>>),
	> = HashMap::new();

	loop {
		while let Some(fetch_result) = active_fetches.next().await {
			match fetch_result {
				| FetchResult::Event(next_id, fetch_res) => {
					individually_fetch_in_flight.remove(&next_id);
					match fetch_res {
						| Ok((res, successful_server)) => {
							debug!("Got {next_id} over federation from {successful_server}");

							let room_version_id = match create_event {
								| Some(ce) =>
									match crate::rooms::event_handler::get_room_version_id(ce) {
										| Ok(v) => v,
										| Err(_) => {
											warn!(
												"Provided create_event for {room_id} has no \
												 room version! Skipping outlier {next_id}"
											);
											back_off(next_id.clone());
											continue;
										},
									},
								| None => {
									let mut version = None;
									if let Ok(json) =
										serde_json::from_str::<serde_json::Value>(res.pdu.get())
									{
										if json.get("type").and_then(|t| t.as_str())
											== Some("m.room.create")
										{
											let v = json
												.get("content")
												.and_then(|c| c.get("room_version"))
												.and_then(|v| v.as_str())
												.unwrap_or("1");
											version = ruma::RoomVersionId::try_from(v).ok();
										}
									}
									match version {
										| Some(v) => v,
										| None =>
											if let Some(override_v) = room_version_override {
												override_v.clone()
											} else {
												match self
													.services
													.state
													.get_room_version(room_id)
													.await
												{
													| Ok(v) => v,
													| Err(e) => {
														warn!(
															"Unknown room version for \
															 {room_id}, skipping outlier \
															 {next_id}: {e}"
														);
														back_off(next_id.clone());
														continue;
													},
												}
											},
									}
								},
							};
							let (calculated_event_id, value) =
								match gen_event_id_canonical_json(&res.pdu, &room_version_id) {
									| Ok(res) => res,
									| Err(e) => {
										info!(
											"Failed to parse PDU JSON over federation from \
											 {successful_server} for event {next_id}: {e}. Raw \
											 JSON: {}",
											res.pdu.get()
										);
										back_off(next_id);
										continue;
									},
								};

							if calculated_event_id != next_id {
								warn!(
									"Server didn't return event id we requested: requested: \
									 {next_id}, we got {calculated_event_id}. Event: {:?}",
									&res.pdu
								);
							}

							let mut next_auth_events = HashSet::new();
							let mut missing_auth = false;
							if let Some(auth_events) = value
								.get("auth_events")
								.and_then(CanonicalJsonValue::as_array)
							{
								for auth_event in auth_events {
									if let Ok(auth_event) = serde_json::from_value::<OwnedEventId>(
										auth_event.clone().into(),
									) {
										if self
											.services
											.pdu_metadata
											.is_event_soft_failed(&auth_event)
											.await
										{
											info!(target: "auth_chain", "Found known soft-failed auth event locally: {auth_event}");
										}

										if !graph.contains_key(&auth_event) {
											if !self
												.services
												.timeline
												.pdu_exists(&auth_event)
												.await
											{
												if auth_event_ratelimited(&auth_event) {
													info!(target: "auth_chain", "Backing off from {auth_event} (auth event ratelimited)");
													continue;
												}

												if graph.len() >= limit.into() {
													info!(target: "auth_chain", "Max auth event limit reached! Limit: {limit}");
													continue;
												}

												trace!(
													"Found missing auth event id {auth_event} \
													 for event {next_id}"
												);
												missing_auth = true;
											}
											graph.insert(auth_event.clone(), HashSet::new());
										}

										if graph.contains_key(&auth_event) {
											next_auth_events.insert(auth_event);
										}
									}
								}
							} else {
								warn!("Auth event list invalid");
							}

							if missing_auth {
								push_auth_fetch(
									room_id.to_owned(),
									next_id.clone(),
									true,
									&mut active_fetches,
								);
							}

							graph.insert(next_id.clone(), next_auth_events);
							fetched_info.insert(next_id, value);
						},
						| Err(e) => {
							debug!(
								target: "auth_chain",
								"Failed to fetch event {next_id} from all fallback servers: {e}"
							);
							back_off(next_id);
						},
					}
				},
				| FetchResult::AuthChain(next_id, fetch_res) => match fetch_res {
					| Ok((res, successful_server)) => {
						debug!(
							"Got auth chain for {next_id} over federation from \
							 {successful_server}"
						);

						let room_version_id = match create_event {
							| Some(ce) => {
								match crate::rooms::event_handler::get_room_version_id(ce) {
									| Ok(v) => v,
									| Err(_) => {
										warn!(
											"Provided create_event for {room_id} has no room \
											 version! Skipping auth chain for {next_id}"
										);
										back_off(next_id.clone());
										continue;
									},
								}
							},
							| None =>
								if let Some(override_v) = room_version_override {
									override_v.clone()
								} else {
									match self.services.state.get_room_version(room_id).await {
										| Ok(v) => v,
										| Err(e) => {
											warn!(
												"Unknown room version for {room_id}, skipping \
												 auth chain for {next_id}: {e}"
											);
											back_off(next_id.clone());
											continue;
										},
									}
								},
						};

						for pdu_raw in res.auth_chain {
							match gen_event_id_canonical_json(&pdu_raw, &room_version_id) {
								| Ok((auth_eid, auth_val)) => {
									if !graph.contains_key(&auth_eid)
										&& !fetched_info.contains_key(&auth_eid)
										&& !self.services.timeline.pdu_exists(&auth_eid).await
									{
										let mut next_auth_events = HashSet::new();
										if let Some(auth_events) = auth_val
											.get("auth_events")
											.and_then(CanonicalJsonValue::as_array)
										{
											for auth_event in auth_events {
												if let Ok(aeid) =
													serde_json::from_value::<OwnedEventId>(
														auth_event.clone().into(),
													) {
													next_auth_events.insert(aeid);
												}
											}
										}
										graph.insert(auth_eid.clone(), next_auth_events);
										fetched_info.insert(auth_eid, auth_val);
									}
								},
								| Err(e) => {
									warn!(
										"Failed to parse auth_chain PDU JSON from \
										 {successful_server}: {e}. Raw JSON: {}",
										pdu_raw.get()
									);
								},
							}
						}
					},
					| Err(e) => {
						debug!(
							target: "auth_chain",
							"Failed to fetch auth chain for event {next_id} from all fallback servers: {e}"
						);
						back_off(next_id);
					},
				},
			}
		}

		if fetched_info.is_empty() {
			break;
		}

		let event_fetch = |event_id: OwnedEventId| {
			let origin_server_ts = fetched_info
				.get(&event_id)
				.and_then(|info| info.get("origin_server_ts"))
				.and_then(CanonicalJsonValue::as_integer)
				.map(i64::from)
				.and_then(|i| ruma::UInt::try_from(i).ok())
				.unwrap_or_else(|| ruma::uint!(0));

			future::ready(conduwuit_core::Result::Ok((
				ruma::int!(0),
				ruma::MilliSecondsSinceUnixEpoch(origin_server_ts),
			)))
		};

		let sorted = state_res::lexicographical_topological_sort(&graph, &event_fetch)
			.await
			.unwrap_or_else(|e| {
				warn!("lexicographical_topological_sort failed for batch: {e}");
				let mut ids: Vec<_> = fetched_info.keys().cloned().collect();
				ids.sort_unstable();
				ids
			});

		let events_in_order: Vec<(OwnedEventId, CanonicalJsonObject)> = sorted
			.into_iter()
			.filter_map(|id| fetched_info.remove(&id).map(|info| (id, info)))
			.collect();

		let mut suspended = false;
		let mut unprocessed = Vec::new();

		for (next_id, value) in events_in_order {
			if suspended {
				unprocessed.push((next_id, value));
				continue;
			}

			if let Some((time, tries)) = self
				.services
				.globals
				.bad_event_ratelimiter
				.read()
				.get(&*next_id)
			{
				const MIN_DURATION: u64 = 5 * 60;
				const MAX_DURATION: u64 = 60 * 60 * 24;
				if continue_exponential_backoff_secs(
					MIN_DURATION,
					MAX_DURATION,
					time.elapsed(),
					*tries,
				) {
					debug!("Backing off from {next_id}");
					continue;
				}
			}

			trace!("Handling outlier {next_id}");
			match Box::pin(self.handle_outlier_pdu(
				origin,
				create_event,
				&next_id,
				room_id,
				value.clone(),
				true,
				skip_sig_verify,
				room_version_override,
				false,
			))
			.await
			{
				| Ok((pdu, json)) => {
					processed_pdus.insert(next_id.clone(), (pdu, Some(json)));
				},
				| Err(e) =>
					if let conduwuit::Error::MissingAuthEvents(missing) = &e {
						if auth_chain_fetched.insert(next_id.clone()) {
							debug_info!(
								"Suspending outlier {next_id} to fetch {} missing auth events \
								 via /event_auth",
								missing.len()
							);
							push_auth_fetch(
								room_id.to_owned(),
								next_id.clone(),
								true,
								&mut active_fetches,
							);
							for auth_event in missing {
								if !graph.contains_key(auth_event) {
									graph.insert(auth_event.clone(), HashSet::new());
								}
							}
							suspended = true;
							unprocessed.push((next_id, value));
						} else if individual_auth_fetch_attempted.insert(next_id.clone()) {
							// Bulk /event_auth has now been tried (and retried) and still
							// left auth events missing -- either the remote doesn't
							// implement /event_auth, or it's misbehaving for this room.
							// Fall back to fetching exactly the still-missing IDs
							// individually, same as the initial fetches above: through
							// `push_fetch`, so this gets the same multi-server routing
							// list, ratelimiting, and retry-with-backoff as any other
							// event fetch, rather than a bespoke recovery path. This
							// mirrors Synapse's `_get_events_and_persist`, which resolves
							// this exact gap with per-ID `/event/{id}` fetches.
							debug_info!(
								"Bulk /event_auth left {} auth events missing for {next_id}; \
								 falling back to individual /event fetches",
								missing.len()
							);
							// Whether at least one auth event actually got a fetch
							// scheduled (either just now, or already in flight from
							// an earlier round). If every single one is presently
							// ratelimited, there is nothing to wait on this round --
							// requeuing `next_id` anyway would let it fall through to
							// the terminal `else` below on its very next pass (since
							// `individual_auth_fetch_attempted` is a one-shot flag,
							// already consumed above) and get permanently rejected
							// before any backoff window actually expired.
							let mut any_scheduled = false;
							for auth_event in missing {
								if auth_event_ratelimited(auth_event) {
									info!(target: "auth_chain", "Backing off from {auth_event} (auth event ratelimited, skipping individual fetch)");
								} else {
									any_scheduled = true;
									if individually_fetch_in_flight.insert(auth_event.clone()) {
										push_fetch(auth_event.clone(), true, &mut active_fetches);
									}
								}
								if !graph.contains_key(auth_event) {
									graph.insert(auth_event.clone(), HashSet::new());
								}
							}

							if any_scheduled {
								suspended = true;
								unprocessed.push((next_id, value));
							} else {
								debug_info!(
									"All individual auth-event fallback fetches for {next_id} \
									 are ratelimited; deferring to a future retry instead of \
									 rejecting early"
								);
							}
						} else {
							warn!(target: "auth_chain", "Backing off event {next_id} after individual auth-event fallback still left events missing");
							// `missing` can run into the hundreds (e.g. MSC4297); persist
							// only a count plus the first few IDs rather than the
							// full list, to keep the stored reason bounded.
							let preview: Vec<_> = missing.iter().take(5).collect();
							self.services
								.pdu_metadata
								.mark_event_rejected(
									&next_id,
									&crate::rooms::pdu_metadata::RejectionCode::MissingAuthEvent
										.with_detail(format!(
											"{} auth events still missing after /event_auth and \
											 individual /event retries, e.g. {preview:?}",
											missing.len()
										)),
								)
								.await;
							self.services
								.outlier
								.add_pdu_outlier(&next_id, &value, Some(room_id))
								.await;
							back_off(next_id.clone());
						}
					} else {
						warn!(target: "auth_chain", "Permanently backing off event {next_id} after general failure: {e:?}");
						back_off(next_id);
					},
			}
		}

		for (id, val) in unprocessed {
			fetched_info.insert(id, val);
		}

		if !suspended {
			break;
		}
	}

	let mut final_pdus = Vec::with_capacity(requested_seeds.len());

	for (id, pdu) in pre_resolved_pdus {
		processed_pdus.insert(id, (pdu, None));
	}

	for id in requested_seeds {
		if let Some(pdu_tuple) = processed_pdus.remove(&id) {
			final_pdus.push(pdu_tuple);
		}
	}

	trace!("Fetched and handled {} outlier pdus", final_pdus.len());
	final_pdus
}

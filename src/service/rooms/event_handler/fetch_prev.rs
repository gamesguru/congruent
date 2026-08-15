use std::{
	collections::{BTreeMap, HashMap, HashSet},
	time::{Duration, Instant},
};

use conduwuit::{
	Error, Event, PduEvent, Result, implement, info,
	utils::stream::{BroadbandExt, IterStream},
	warn,
};
use futures::{StreamExt, stream::FuturesUnordered};
use ruma::{
	CanonicalJsonValue, EventId, OwnedEventId, RoomId, ServerName,
	api::federation::event::{
		event_relationships as federation_event_relationships, get_missing_events,
	},
};

use super::check_room_id;

#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip_all, fields(%origin))]
#[allow(clippy::type_complexity)]
pub(super) async fn fetch_prev<'a, Events>(
	&self,
	origin: &ServerName,
	room_id: &RoomId,
	latest_event: &'a EventId,
	initial_set: Events,
	event_sender_server: Option<&ServerName>,
) -> Result<(
	Vec<OwnedEventId>,
	HashMap<OwnedEventId, BTreeMap<String, CanonicalJsonValue>>,
	// The single unresolved prev_event of the *last-sorted fetched
	// candidate*, when it has exactly one -- i.e. one hop further back
	// than `latest_event`'s own direct prev. `/get_missing_events`
	// commonly returns only the immediate gap-filler (one candidate)
	// whose own prev_event is still unknown; that deeper event, not the
	// candidate itself, is what the sender can actually provide a
	// `/state_ids` snapshot anchored at. `None` when the last candidate
	// has zero or multiple prev_events (nothing single to anchor on).
	Option<OwnedEventId>,
	// True if the /get_missing_events response contained at least one event
	// that failed canonical-JSON validation. Such an event can never be
	// resolved by any other federation call either (the data itself is
	// malformed), so callers can skip a doomed /state_ids fetch for a
	// prev_event we just tried and structurally rejected this round.
	bool,
)>
where
	Events: Iterator<Item = &'a EventId> + Clone + Send,
{
	let still_needed: Vec<OwnedEventId> = initial_set.map(ToOwned::to_owned).collect();
	let mut remaining = Vec::with_capacity(still_needed.len());
	for id in &still_needed {
		// `pdu_exists` also matches events persisted only as outliers, which
		// includes ones we ultimately rejected. Most rejections (failed auth
		// checks, depending on another rejected event, etc.) are permanent:
		// re-fetching the same event over federation can't change why it was
		// rejected, so treat those as satisfied like any other outlier.
		// But rejections caused by us failing to *resolve* the event's own
		// dependencies (a structurally-invalid prev_event, or /state_ids
		// simply failing) are worth retrying, since a retry can supply the
		// missing data this time.
		let exists = self.services.timeline.pdu_exists(id).await;
		let retry_worthy = if self.services.pdu_metadata.is_event_rejected(id).await {
			self.services
				.pdu_metadata
				.get_rejection_reason(id)
				.await
				.is_some_and(|reason| {
					crate::rooms::pdu_metadata::is_retryable_rejection_reason(&reason)
				})
		} else {
			false
		};
		if !exists || retry_worthy {
			remaining.push(id.clone());
		}
	}

	if remaining.is_empty() {
		return Ok((Vec::new(), HashMap::new(), None, false));
	}

	let servers = self
		.build_federation_server_list_with_sender(
			room_id,
			origin,
			event_sender_server,
			self.services.server.config.federation_fallback_room_servers,
		)
		.await;

	let earliest: Vec<OwnedEventId> = self
		.services
		.state
		.get_forward_extremities(room_id)
		.collect()
		.await;

	let server_fanout = self
		.services
		.server
		.concurrency_scaled(2)
		.min(servers.len());
	let latest_event_owned = latest_event.to_owned();
	let mut active = FuturesUnordered::new();
	for server in servers {
		if self.services.sending.server_is_dead(&server) {
			continue;
		}

		let room_id_owned = room_id.to_owned();
		let earliest = earliest.clone();
		let remaining = remaining.clone();
		let latest_event_owned = latest_event_owned.clone();
		active.push(async move {
			let t = Instant::now();
			let latest_events = vec![latest_event_owned];
			info!(
				"Asking {server} for missing events in {room_id_owned} (latest: \
				 {latest_events:?}, earliest_count: {}, missing: {remaining:?})",
				earliest.len()
			);
			let res = {
				let mut attempt = 0_usize;
				loop {
					let request = get_missing_events::v1::Request {
						room_id: room_id_owned.clone(),
						earliest_events: earliest.clone(),
						latest_events: latest_events.clone(),
						limit: 50_u32.into(),
						min_depth: 0_u32.into(),
					};
					let res = tokio::time::timeout(
						Duration::from_secs(10), // Time budget
						self.services
							.sending
							.send_federation_request(&server, request),
					)
					.await;

					match &res {
						| Ok(Err(Error::BadServerResponse(msg))) if attempt == 0 => {
							info!(
								%server,
								%msg,
								"fetch_prev /get_missing_events returned malformed response; retrying once"
							);
							attempt = attempt.saturating_add(1);
						},
						| _ => break res,
					}
				}
			};
			(server, res, t.elapsed())
		});

		if active.len() >= server_fanout {
			break;
		}
	}

	let room_version_id = self.services.state.get_room_version(room_id).await?;
	let mut missing_events = Vec::new();

	while let Some((server, res, latency)) = active.next().await {
		match res {
			| Ok(Ok(response)) => {
				self.update_peer_stats(&server, true, latency);
				missing_events = response.events;
				if missing_events.is_empty() {
					let Some(fallback_anchor) = remaining.first().cloned() else {
						break;
					};
					let room_id_owned = room_id.to_owned();
					let request = federation_event_relationships::unstable::Request {
						event_id: fallback_anchor,
						room_id: Some(room_id_owned),
						max_depth: None,
						max_breadth: None,
						limit: None,
						depth_first: None,
						recent_first: None,
						include_parent: None,
						include_children: None,
						direction: Some("up".to_owned()),
						batch: None,
					};
					match tokio::time::timeout(
						Duration::from_secs(10),
						self.services
							.sending
							.send_federation_request(&server, request),
					)
					.await
					{
						| Ok(Ok(fallback_response)) => {
							missing_events = fallback_response
								.auth_chain
								.into_iter()
								.chain(fallback_response.events)
								.collect();
							if !missing_events.is_empty() {
								break;
							}
						},
						| Ok(Err(e)) => {
							info!(%server, "fetch_prev /event_relationships fallback failed: {e}");
							self.update_peer_stats(&server, false, latency);
						},
						| Err(_) => {
							info!(
								%server,
								"fetch_prev /event_relationships fallback failed: timed out"
							);
							self.update_peer_stats(&server, false, latency);
						},
					}
				} else {
					break; // First successful server wins
				}
			},
			| _ => {
				self.update_peer_stats(&server, false, latency);
			},
		}
	}

	if missing_events.is_empty() {
		warn!("All servers failed to return /get_missing_events");
		return Ok((Vec::new(), HashMap::new(), None, false));
	}

	let mut unknown_events = Vec::new();
	let mut had_invalid_response = false;
	for raw_json in missing_events {
		match conduwuit::matrix::event::gen_event_id_canonical_json(&raw_json, &room_version_id) {
			| Ok((eid, val)) =>
				if !self.services.timeline.pdu_exists(&eid).await {
					unknown_events.push((eid, val));
				},
			| Err(_) => {
				// The remote server actually answered, but the returned event is
				// structurally invalid (e.g. contains a float, per the Matrix
				// canonical JSON rules). No amount of retrying or asking a
				// different endpoint will make this data valid, so record it
				// for callers that would otherwise waste a /state_ids fetch on
				// the same event this round.
				had_invalid_response = true;
			},
		}
	}

	// Only the fields `graph`/`entries` need are carried out of the closure;
	// the full `PduEvent` (which owns its own copy of the event content,
	// separate from `val`) is dropped here rather than being kept alive in
	// an extra map until the end of the function.
	let candidate_entries: Vec<(
		OwnedEventId,
		ruma::CanonicalJsonObject,
		HashSet<OwnedEventId>,
		ruma::UInt,
		ruma::UInt,
	)> = unknown_events
		.into_iter()
		.stream()
		.broad_filter_map({
			move |(eid, mut val): (OwnedEventId, ruma::CanonicalJsonObject)| async move {
				if let Some(CanonicalJsonValue::Object(mut unsigned_obj)) = val.remove("unsigned")
				{
					unsigned_obj.remove("prev_content");
					unsigned_obj.remove("prev_sender");
					unsigned_obj.remove("replaces_state");
					if !unsigned_obj.is_empty() {
						val.insert(
							"unsigned".to_owned(),
							CanonicalJsonValue::Object(unsigned_obj),
						);
					}
				}

				let mut parse_val = val.clone();
				parse_val.insert(
					"event_id".to_owned(),
					CanonicalJsonValue::String(eid.as_str().to_owned()),
				);

				match PduEvent::from_id_val(&eid, parse_val, Some(room_id)) {
					| Ok(pdu) =>
						if check_room_id(room_id, &pdu).is_ok() {
							let prev_events = pdu.prev_events().map(ToOwned::to_owned).collect();
							let depth = pdu.depth();
							let origin_server_ts = pdu.origin_server_ts;
							Some((eid, val, prev_events, depth, origin_server_ts))
						} else {
							None
						},
					| Err(_) => None,
				}
			}
		})
		.collect()
		.await;

	let mut candidate_events = HashMap::with_capacity(candidate_entries.len());
	let mut graph = HashMap::with_capacity(candidate_entries.len());
	let mut entries = HashMap::with_capacity(candidate_entries.len());
	for (eid, val, prev_events, depth, origin_server_ts) in candidate_entries {
		graph.insert(eid.clone(), prev_events);
		entries.insert(eid.clone(), (0_u64.into(), depth.into(), origin_server_ts.into()));
		candidate_events.insert(eid, val);
	}
	let sorted_eids = conduwuit::utils::timeline_sorter::sort_timeline_events(&entries, &graph);
	let deep_anchor = deep_state_ids_anchor(&sorted_eids, &graph);

	Ok((sorted_eids, candidate_events, deep_anchor, had_invalid_response))
}

/// One hop further back than the last fetched candidate: if it has exactly
/// one prev_event, that's the deepest still-unresolved point in this batch
/// and the most useful anchor for a caller's `/state_ids` retry (see the
/// `fetch_prev` return-type doc comment). `None` if there's no last
/// candidate, its prev_events aren't exactly one, or that single prev_event
/// is itself one of the candidates in this same batch (`/get_missing_events`
/// can return a multi-hop chain in one response) -- anchoring `/state_ids`
/// there would ask for state from *before* an event this same call is about
/// to fetch/process, silently omitting whatever state change that candidate
/// itself introduces.
fn deep_state_ids_anchor(
	sorted_eids: &[OwnedEventId],
	graph: &HashMap<OwnedEventId, HashSet<OwnedEventId>>,
) -> Option<OwnedEventId> {
	let last_id = sorted_eids.last()?;
	let parents = graph.get(last_id)?;
	let mut iter = parents.iter();
	let only = iter.next()?;
	(iter.next().is_none() && !graph.contains_key(only)).then(|| only.clone())
}

#[cfg(test)]
mod tests {
	use ruma::event_id;

	use super::*;

	#[test]
	fn deep_anchor_uses_last_candidates_single_prev() {
		let gme = event_id!("$gme:test").to_owned();
		let state_ids = event_id!("$state_ids:test").to_owned();
		let sorted = vec![gme.clone()];
		let mut graph = HashMap::new();
		graph.insert(gme, [state_ids.clone()].into_iter().collect());

		assert_eq!(deep_state_ids_anchor(&sorted, &graph), Some(state_ids));
	}

	#[test]
	fn deep_anchor_none_when_last_candidate_has_no_prevs() {
		let gme = event_id!("$gme:test").to_owned();
		let sorted = vec![gme.clone()];
		let mut graph = HashMap::new();
		graph.insert(gme, HashSet::new());

		assert_eq!(deep_state_ids_anchor(&sorted, &graph), None);
	}

	#[test]
	fn deep_anchor_none_when_last_candidate_has_multiple_prevs() {
		let gme = event_id!("$gme:test").to_owned();
		let a = event_id!("$a:test").to_owned();
		let b = event_id!("$b:test").to_owned();
		let sorted = vec![gme.clone()];
		let mut graph = HashMap::new();
		graph.insert(gme, [a, b].into_iter().collect());

		assert_eq!(deep_state_ids_anchor(&sorted, &graph), None);
	}

	#[test]
	fn deep_anchor_none_when_single_prev_is_itself_a_candidate() {
		// A multi-hop /get_missing_events response: both `gme` and its own
		// single prev `state_ids` were fetched together in this batch.
		let gme = event_id!("$gme:test").to_owned();
		let state_ids = event_id!("$state_ids:test").to_owned();
		let external = event_id!("$external:test").to_owned();
		let sorted = vec![state_ids.clone(), gme.clone()];
		let mut graph = HashMap::new();
		graph.insert(gme, [state_ids.clone()].into_iter().collect());
		graph.insert(state_ids, [external].into_iter().collect());

		assert_eq!(deep_state_ids_anchor(&sorted, &graph), None);
	}

	#[test]
	fn deep_anchor_none_when_no_candidates() {
		let graph = HashMap::new();
		assert_eq!(deep_state_ids_anchor(&[], &graph), None);
	}
}

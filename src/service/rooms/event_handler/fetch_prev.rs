use std::{
	collections::{BTreeMap, HashMap},
	time::{Duration, Instant},
};

use conduwuit::{
	Event, PduEvent, Result, implement, info,
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
			let res = tokio::time::timeout(
				Duration::from_secs(10), // Time budget
				self.services.sending.send_federation_request(
					&server,
					get_missing_events::v1::Request {
						room_id: room_id_owned,
						earliest_events: earliest,
						latest_events,
						limit: 50_u32.into(),
						min_depth: 0_u32.into(),
					},
				),
			)
			.await;
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

	let candidate_events: HashMap<OwnedEventId, ruma::CanonicalJsonObject> = unknown_events
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

				if let Ok(pdu) = PduEvent::from_id_val(&eid, parse_val, Some(room_id))
					&& check_room_id(room_id, &pdu).is_ok()
				{
					return Some((eid, val));
				}

				None
			}
		})
		.collect()
		.await;

	let mut graph = HashMap::new();
	let mut entries = HashMap::new();
	for (eid, val) in &candidate_events {
		let mut parse_val = val.clone();
		parse_val
			.insert("event_id".to_owned(), CanonicalJsonValue::String(eid.as_str().to_owned()));
		let Ok(pdu) = PduEvent::from_id_val(eid, parse_val, Some(room_id)) else {
			continue;
		};
		graph.insert(eid.clone(), pdu.prev_events().map(ToOwned::to_owned).collect());
		entries
			.insert(eid.clone(), (0_u64.into(), pdu.depth().into(), pdu.origin_server_ts.into()));
	}
	let sorted_eids = conduwuit::utils::timeline_sorter::sort_timeline_events(&entries, &graph);
	let state_ids_anchor = sorted_eids.last().and_then(|prev_id| {
		let val = candidate_events.get(prev_id)?;
		let mut parse_val = val.clone();
		parse_val.insert(
			"event_id".to_owned(),
			CanonicalJsonValue::String(prev_id.as_str().to_owned()),
		);
		let pdu = PduEvent::from_id_val(prev_id, parse_val, Some(room_id)).ok()?;
		let mut prev_events = pdu.prev_events();
		let first_prev = prev_events.next()?.to_owned();
		prev_events.next().is_none().then_some(first_prev)
	});

	Ok((sorted_eids, candidate_events, state_ids_anchor, had_invalid_response))
}

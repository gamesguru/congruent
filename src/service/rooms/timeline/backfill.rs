use std::iter::once;

use conduwuit::{Err, Error, PduEvent, RoomVersion};
use conduwuit_core::{
	Result, debug, debug_warn, err, implement, info,
	matrix::{
		event::Event,
		pdu::{PduCount, PduId, RawPduId},
	},
	utils::{IterStream, ReadyExt, stream::WidebandExt},
	validated, warn,
};
use futures::{FutureExt, StreamExt};
use ruma::{
	CanonicalJsonObject, EventId, Int, OwnedEventId, RoomId, RoomVersionId, ServerName, UInt,
	api::federation,
	events::{
		StateEventType,
		room::{create::RoomCreateEventContent, power_levels::RoomPowerLevelsEventContent},
	},
};
use serde_json::value::RawValue as RawJsonValue;

#[implement(super::Service)]
#[tracing::instrument(name = "backfill", level = "trace", skip(self))]
pub async fn backfill_if_required(
	&self,
	room_id: &RoomId,
	from: PduCount,
	limit: usize,
) -> Result<()> {
	let joined_count = self
		.services
		.state_cache
		.room_joined_count(room_id)
		.await
		.unwrap_or(0);

	let has_remote_servers = self
		.services
		.state_cache
		.room_servers(room_id)
		.ready_any(|server| !self.services.globals.server_is_ours(server))
		.await;

	info!(
		%room_id, %from, %limit, %joined_count, %has_remote_servers,
		"backfill: evaluating"
	);

	// NOTE: this is the best check for previously joined.
	if !has_remote_servers
		&& !self
			.services
			.state_accessor
			.is_world_readable(room_id)
			.await
	{
		info!("backfill: SKIPPING room {room_id} -- no remote servers in room");
		return Ok(());
	}

	let power_levels: RoomPowerLevelsEventContent = self
		.services
		.state_accessor
		.room_state_get_content(room_id, &StateEventType::RoomPowerLevels, "")
		.await
		.unwrap_or_default();
	let create_event_content: RoomCreateEventContent = self
		.services
		.state_accessor
		.room_state_get_content(room_id, &StateEventType::RoomCreate, "")
		.await?;
	let create_event = self
		.services
		.state_accessor
		.room_state_get(room_id, &StateEventType::RoomCreate, "")
		.await?;

	let room_version =
		RoomVersion::new(&create_event_content.room_version).expect("supported room version");
	let mut users = power_levels.users.clone();
	if room_version.explicitly_privilege_room_creators {
		users.insert(create_event.sender().to_owned(), Int::MAX);
		if let Some(additional_creators) = &create_event_content.additional_creators {
			for user_id in additional_creators {
				users.insert(user_id.to_owned(), Int::MAX);
			}
		}
	}

	let room_mods = users.iter().filter_map(|(user_id, level)| {
		let remote_powered =
			level > &power_levels.users_default && !self.services.globals.user_is_local(user_id);
		let creator = if room_version.explicitly_privilege_room_creators {
			create_event.sender() == user_id
				|| create_event_content
					.additional_creators
					.as_ref()
					.is_some_and(|c| c.contains(user_id))
		} else {
			false
		};

		if remote_powered || creator {
			debug!(%remote_powered, %creator, "User {user_id} can backfill in room {room_id}");
			Some(user_id.server_name())
		} else {
			debug!(%remote_powered, %creator, "User {user_id} cannot backfill in room {room_id}");
			None
		}
	});

	// Iterative backfill loop: after each successful /backfill response, re-scan
	// for new backward extremities created by the newly inserted events'
	// prev_events. This matches Synapse's behavior where backfilled events create
	// new backward extremities that are discovered on subsequent pagination calls.
	let backfill_limit: u32 = limit.clamp(100, 500).try_into().unwrap_or(100);
	let mut budget = 5_u32;

	loop {
		// Phase 1: Collect scanned PDUs into an event map. With `impl DagNode
		// for Pdu`, rezzy operates directly on PduEvent — no LeanEvent conversion.
		let mut event_map: std::collections::HashMap<OwnedEventId, PduEvent> =
			std::collections::HashMap::new();
		let mut scanned = 0_usize;
		// `pdus_rev` is exclusive of its boundary; advance once so the event at
		// `from` is included in the gap scan.
		let mut pdus = self
			.pdus_rev(room_id, Some(from.saturating_inc(ruma::api::Direction::Forward)))
			.take(limit)
			.boxed();
		while let Some(Ok((pdu_id, pdu))) = pdus.next().await {
			scanned = scanned.saturating_add(1);
			debug!(
				?pdu_id,
				event_id = %pdu.event_id,
				prev_events = ?pdu.prev_events,
				"backfill: scanned timeline PDU"
			);
			event_map.insert(pdu.event_id.clone(), pdu);
		}

		// Phase 2: Pre-collect which prev_event IDs exist in the DB so the
		// rezzy `exists` predicate is synchronous.
		let mut all_prev_ids: Vec<OwnedEventId> = Vec::new();
		for pdu in event_map.values() {
			for prev_id in &pdu.prev_events {
				if !event_map.contains_key(prev_id) {
					all_prev_ids.push(prev_id.clone());
				}
			}
		}
		let mut known_ids: std::collections::HashSet<OwnedEventId> =
			std::collections::HashSet::with_capacity(all_prev_ids.len());
		for prev_id in &all_prev_ids {
			if self.get_pdu_id(prev_id).await.is_ok() {
				known_ids.insert(prev_id.clone());
			}
		}

		// Phase 3: Call rezzy for correct backward extremity detection.
		let gaps = rezzy::find_backward_extremities(&event_map, |id| known_ids.contains(id));

		if gaps.is_empty() {
			info!("backfill: no gaps in {room_id} (scanned {scanned} events from {from})");
			return Ok(());
		}

		// Build the /backfill request: send child event IDs (events that have
		// missing parents), which is what the /backfill API expects.
		let backwards_extremities: Vec<OwnedEventId> =
			gaps.iter().map(|gap| gap.event_id.clone()).collect();
		let unique_missing: std::collections::HashSet<&EventId> = gaps
			.iter()
			.flat_map(|gap| gap.missing_prev_events.iter().map(AsRef::as_ref))
			.collect();

		if budget == 0 {
			info!(
				"backfill: budget exhausted for {room_id} with {} gaps ({} unique missing \
				 parents)",
				gaps.len(),
				unique_missing.len()
			);
			return Ok(());
		}
		budget = budget.saturating_sub(1);

		for gap in &gaps {
			info!(
				"backfill: gap at {} (missing: {:?}) in {room_id}",
				gap.event_id, gap.missing_prev_events
			);
		}
		info!(
			"backfill: {room_id} has {} gaps ({} unique missing parents, scanned {scanned}, \
			 budget {budget})",
			gaps.len(),
			unique_missing.len()
		);

		let mut servers = self
			.get_backfill_servers(room_id, room_mods.clone())
			.await
			.boxed();
		let mut federated_room = false;
		let mut got_events = false;

		while let Some(ref backfill_server) = servers.next().await {
			if !self.services.globals.server_is_ours(backfill_server) {
				federated_room = true;
			}
			info!(
				"Asking {backfill_server} for backfill in {room_id} (extremities: \
				 {backwards_extremities:?})"
			);
			let response = self
				.services
				.sending
				.send_federation_request(
					backfill_server,
					federation::backfill::get_backfill::v1::Request {
						room_id: room_id.to_owned(),
						v: backwards_extremities.clone(),
						limit: UInt::from(backfill_limit),
					},
				)
				.await;
			match response {
				| Ok(response) => {
					let pdus = response.pdus;
					info!(
						"backfill: {backfill_server} returned {} events for {room_id}",
						pdus.len()
					);
					if pdus.is_empty() {
						continue;
					}
					// Handle timeline events newest-first (maintain timeline integrity)
					for pdu in pdus {
						if let Err(e) =
							self.backfill_pdu(backfill_server, pdu, None).boxed().await
						{
							debug_warn!("Failed to add backfilled pdu in room {room_id}: {e}");
						}
					}
					got_events = true;
					break; // Got events from this server, re-scan for new gaps
				},
				| Err(ref e) => {
					// If the server explicitly forbids us, drop it from candidates
					if matches!(e, Error::Federation(_, _))
						&& e.to_string().contains("not allowed")
					{
						info!("{backfill_server} forbade backfill for {room_id}, skipping");
						continue;
					}
					warn!("{backfill_server} failed to provide backfill for room {room_id}: {e}");
				},
			}
		}

		if !got_events {
			if federated_room {
				warn!("No servers could backfill, but backfill was needed in room {room_id}");
			}
			return Ok(());
		}
		// Loop back to re-scan for new gaps created by backfilled events
	}
}

#[implement(super::Service)]
#[tracing::instrument(name = "get_remote_pdu", level = "debug", skip(self))]
pub async fn get_remote_pdu(&self, room_id: &RoomId, event_id: &EventId) -> Result<PduEvent> {
	let _mutex = self.mutex_fetch.lock(event_id).await;

	let local = self.get_pdu(event_id).await;
	if local.is_ok() {
		// We already have this PDU, no need to backfill
		debug!("We already have {event_id} in {room_id}, no need to backfill.");
		return local;
	}
	debug!("Preparing to fetch event {event_id} in room {room_id} from remote servers.");
	// Similar to backfill_if_required, but only for a single PDU
	// Fetch a list of servers to try
	let has_remote_servers = self
		.services
		.state_cache
		.room_servers(room_id)
		.ready_any(|server| !self.services.globals.server_is_ours(server))
		.await;

	if !has_remote_servers
		&& !self
			.services
			.state_accessor
			.is_world_readable(room_id)
			.await
	{
		// No remote servers in the room, there is no one that can backfill
		return Err!(Request(NotFound(
			"No one can backfill this PDU, no remote servers in room."
		)));
	}

	let power_levels: RoomPowerLevelsEventContent = self
		.services
		.state_accessor
		.room_state_get_content(room_id, &StateEventType::RoomPowerLevels, "")
		.await
		.unwrap_or_default();

	let room_mods = power_levels.users.iter().filter_map(|(user_id, level)| {
		if level > &power_levels.users_default && !self.services.globals.user_is_local(user_id) {
			Some(user_id.server_name())
		} else {
			None
		}
	});

	let mut servers = self.get_backfill_servers(room_id, room_mods).await.boxed();

	while let Some(ref backfill_server) = servers.next().await {
		info!("Asking {backfill_server} for event {}", event_id);
		let value = self
			.services
			.sending
			.send_federation_request(backfill_server, federation::event::get_event::v1::Request {
				event_id: event_id.to_owned(),
				include_unredacted_content: Some(false),
			})
			.await
			.and_then(|response| {
				serde_json::from_str::<CanonicalJsonObject>(response.pdu.get()).map_err(|e| {
					err!(BadServerResponse(debug_warn!(
						"Error parsing incoming event {e:?} from {backfill_server}"
					)))
				})
			});
		let pdu = match value {
			| Ok(value) => match self
				.services
				.event_handler
				.handle_incoming_pdu(backfill_server, room_id, event_id, value, false, None)
				.boxed()
				.await
			{
				| Ok(_) => {
					// Timestamp lookup and /event retrieval need a timeline PDU,
					// not merely the validated outlier left by handle_incoming_pdu.
					// Promote it here so /context can resolve its PduId.
					if self.get_pdu_id(event_id).await.is_err() {
						let room_version = self.services.state.get_room_version(room_id).await?;
						self.promote_outliers_sorted(
							room_id,
							&[event_id.to_owned()],
							&room_version,
						)
						.await?;
					}
					debug!("Successfully backfilled {event_id} from {backfill_server}");
					Some(self.get_pdu(event_id).await)
				},
				| Err(e) => {
					warn!(
						"{backfill_server} provided an invalid PDU or failed state resolution \
						 for {event_id}: {e}"
					);

					if e.to_string().contains("Server was denied by room ACL") {
						return Err(e);
					}

					None
				},
			},
			| Err(e) => {
				warn!("{backfill_server} failed to provide backfill for room {room_id}: {e}");
				None
			},
		};
		if let Some(pdu) = pdu {
			debug!("Fetched {event_id} from {backfill_server}");
			return pdu;
		}
	}

	Err!("No servers could be used to fetch {} in {}.", room_id, event_id)
}

/// TODO: Known gap — early state events (create, initial joins, power levels)
/// arrive via `/send_join` and are stored as **outliers**, not timeline PDUs.
/// When backfill reaches the bottom of the DAG, these outlier ancestors are
/// never promoted into the visible timeline. Users scrolling up will see
/// backfilled messages but not the room's origin events. Fix: after backfill
/// exhausts prev_events into outlier territory, promote those outliers into
/// the timeline ordered by `origin_server_ts`.
#[implement(super::Service)]
#[tracing::instrument(skip(self, pdu), level = "debug")]
pub async fn backfill_pdu(
	&self,
	origin: &ServerName,
	pdu: Box<RawJsonValue>,
	count: Option<u64>,
) -> Result<()> {
	let (room_id, event_id, value) = self.services.event_handler.parse_incoming_pdu(&pdu).await?;

	// Lock so we cannot backfill the same pdu twice at the same time
	let mutex_lock = self
		.services
		.event_handler
		.mutex_federation
		.lock(&room_id)
		.await;

	// If the PDU already exists in the timeline, skip. A concurrent
	// federation /send may have raced with /backfill and inserted the
	// event with a Normal count. Keeping Normal is correct — it sorts
	// in the live timeline where the user expects it. Demoting to
	// Backfilled would inject the event into the wrong stream position.
	if self.get_pdu_id(&event_id).await.is_ok() {
		debug!("Event {event_id} already in timeline, skipping backfill");
		return Ok(());
	}

	// Backfill events come from a trusted /backfill response. We only need
	// signature verification + basic auth, not the full federation pipeline
	// (which fails when auth chain events aren't available locally — common
	// after a new join). If outlier processing fails (e.g. missing auth
	// events), fall back to storing the raw PDU directly.
	let room_version_id = self.services.state.get_room_version(&room_id).await?;

	let (pdu_event, json_value) = match self
		.services
		.event_handler
		.handle_outlier_pdu(
			origin,
			None::<&PduEvent>,
			&event_id,
			&room_id,
			value.clone(),
			false,
			false,
			Some(&room_version_id),
		)
		.await
	{
		| Ok(result) => result,
		| Err(Error::MissingAuthEvents(_)) => {
			// Missing auth events are expected during backfill (we don't have
			// the room's full history yet). Insert the raw PDU directly.
			info!(
				target: "backfill_debug",
				"handle_outlier_pdu failed for backfill event {event_id} due to missing auth events, inserting raw"
			);
			let mut raw = value;
			raw.insert(
				"event_id".to_owned(),
				ruma::CanonicalJsonValue::String(event_id.as_str().to_owned()),
			);
			let parsed: PduEvent =
				serde_json::from_value(serde_json::to_value(&raw).expect("valid json"))
					.map_err(|e| err!(Database("Bad backfill PDU {event_id}: {e}")))?;
			(parsed, raw)
		},
		| Err(e) => {
			warn!("handle_outlier_pdu rejected backfill event {event_id}: {e}");
			return Err(e);
		},
	};

	let shortroomid = self
		.services
		.short
		.get_or_create_shortroomid(&room_id)
		.await;

	let insert_lock = self.mutex_insert.lock(&room_id).await;

	// Re-check after acquiring insert lock to prevent TOCTOU races
	// with concurrent /send transactions inserting the same event.
	if self.get_pdu_id(&event_id).await.is_ok() {
		debug!("Event {event_id} already in timeline (post-lock check), skipping backfill");
		return Ok(());
	}

	let count: i64 = match count {
		| Some(count) => count.try_into()?,
		| None => self.services.globals.next_count()?.try_into()?,
	};

	let pdu_id: RawPduId = PduId {
		shortroomid,
		shorteventid: PduCount::Backfilled(validated!(0 - count)),
	}
	.into();

	// Insert pdu
	self.db
		.prepend_backfill_pdu(&pdu_id, &event_id, &json_value, &pdu_event)
		.await;

	drop(insert_lock);

	self.index_pdu_search(shortroomid, &pdu_id, &pdu_event);
	drop(mutex_lock);

	debug!("Prepended backfill pdu");
	Ok(())
}

/// Promote an outlier event into the visible timeline as a Normal PDU.
/// This skips all auth checks — the caller is responsible for ensuring
/// the event is valid (e.g. it came from a send_join response).
#[implement(super::Service)]
pub async fn promote_outlier(&self, room_id: &RoomId, event_id: &EventId) -> Result<()> {
	// Skip if already in timeline
	if self.get_pdu_id(event_id).await.is_ok() {
		return Ok(());
	}

	let value = self.services.outlier.get_outlier_pdu_json(event_id).await?;

	let pdu: PduEvent = serde_json::from_value(
		serde_json::to_value(&value).map_err(|e| err!(Database("Bad outlier JSON: {e:?}")))?,
	)
	.map_err(|e| err!(Database("Bad outlier PDU: {e:?}")))?;

	let shortroomid = self.services.short.get_or_create_shortroomid(room_id).await;

	let insert_lock = self.mutex_insert.lock(room_id).await;

	// Use backfill (negative) PDU count — these are historical events
	// that predate the join, not new forward events.
	let count: i64 = self.services.globals.next_count()?.try_into()?;

	let pdu_id: RawPduId = PduId {
		shortroomid,
		shorteventid: PduCount::Backfilled(validated!(0 - count)),
	}
	.into();

	self.db
		.prepend_backfill_pdu(&pdu_id, event_id, &value, &pdu)
		.await;

	drop(insert_lock);

	self.index_pdu_search(shortroomid, &pdu_id, &pdu);

	// Remove from outlier room index
	self.services.outlier.clear_outlier_flag(event_id);
	self.services.pdu_metadata.clear_pdu_markers(event_id);

	Ok(())
}

/// Promote a batch of outlier events into the backfilled timeline in
/// topological order (ancestors before descendants). Uses rezzy's Kahn sort
/// to order events by their DAG structure.
///
/// This is called during `/send_join` to make auth chain + state events
/// visible when users scroll up. Events already in the timeline are skipped.
#[implement(super::Service)]
pub async fn promote_outliers_sorted(
	&self,
	room_id: &RoomId,
	event_ids: &[OwnedEventId],
	room_version: &RoomVersionId,
) -> Result<usize> {
	use conduwuit_core::debug;

	if event_ids.is_empty() {
		return Ok(0);
	}

	// Build LeanEvent map from outlier PDUs for topo sort
	let mut events_map: rezzy::HashMap<String, rezzy::LeanEvent> = rezzy::HashMap::new();

	for event_id in event_ids {
		// Skip events already in the timeline
		if self.get_pdu_id(event_id).await.is_ok() {
			continue;
		}

		let Ok(pdu) = self.services.outlier.get_pdu_outlier(event_id).await else {
			continue;
		};

		let lean = rezzy::LeanEvent {
			event_id: event_id.to_string(),
			event_type: pdu.kind.to_string(),
			sender: pdu.sender.to_string(),
			state_key: pdu.state_key.as_ref().map(ToString::to_string),
			content: serde_json::from_str(pdu.content.get()).unwrap_or(serde_json::Value::Null),
			origin_server_ts: u64::from(pdu.origin_server_ts),
			auth_events: pdu.auth_events.iter().map(ToString::to_string).collect(),
			prev_events: pdu.prev_events.iter().map(ToString::to_string).collect(),
			power_level: 0,
			depth: u64::from(pdu.depth),
			..Default::default()
		};
		events_map.insert(event_id.to_string(), lean);
	}

	if events_map.is_empty() {
		return Ok(0);
	}

	// Find the create event for the sort
	let create_ev = events_map
		.values()
		.find(|ev| ev.event_type == "m.room.create");

	// Topo sort: ancestors first (create → PL → joins → messages)
	let state_res_version = {
		use ruma::RoomVersionId::*;
		match room_version {
			| V1 | V2 | V3 | V4 | V5 | V6 | V7 | V8 | V9 | V10 | V11 =>
				rezzy::StateResVersion::V2,
			| V12 => rezzy::StateResVersion::V2_1,
			| ver => return Err!(Database("Unsupported room version for topo sort: {ver}")),
		}
	};
	let mut pl_cache = std::collections::HashMap::new();
	let sorted_ids = rezzy::resolve::sorting::lean_kahn_sort(
		&events_map,
		&events_map, // auth context is the same set
		create_ev,
		state_res_version,
		&mut pl_cache,
	);

	debug!(
		"Promoting {} outliers to timeline in room {} ({} sorted)",
		events_map.len(),
		room_id,
		sorted_ids.len(),
	);

	let mut promoted = 0_usize;
	for event_id_str in &sorted_ids {
		let Ok(event_id) = <&EventId>::try_from(event_id_str.as_str()) else {
			continue;
		};
		match self.promote_outlier(room_id, event_id).await {
			| Ok(()) => {
				promoted = promoted.saturating_add(1);
			},
			| Err(e) => {
				debug!("Could not promote {event_id} to timeline: {e}");
			},
		}
	}

	debug!("Promoted {promoted}/{} outliers in {room_id}", sorted_ids.len());
	Ok(promoted)
}

/// Force-insert a PDU directly into the timeline, bypassing all auth checks.
/// The caller provides the already-parsed PDU and its canonical JSON.
/// Returns the assigned PduId on success.
#[implement(super::Service)]
pub async fn force_insert_pdu(
	&self,
	room_id: &RoomId,
	event_id: &EventId,
	pdu: &PduEvent,
	value: &CanonicalJsonObject,
	backfill: bool,
) -> Result<RawPduId> {
	// Skip if already in timeline
	if self.get_pdu_id(event_id).await.is_ok() {
		return Err!(Database("PDU {event_id} already in timeline"));
	}

	let shortroomid = self.services.short.get_or_create_shortroomid(room_id).await;

	let insert_lock = self.mutex_insert.lock(room_id).await;

	let (pdu_count, pdu_id, value) =
		self.prepare_pdu_insert(shortroomid, event_id, value, backfill)?;

	if backfill {
		self.db
			.prepend_backfill_pdu(&pdu_id, event_id, &value, pdu)
			.await;
	} else {
		self.db.append_pdu(&pdu_id, pdu, &value, pdu_count).await;
	}

	drop(insert_lock);

	self.index_pdu_search(shortroomid, &pdu_id, pdu);

	Ok(pdu_id)
}

#[implement(super::Service)]
#[allow(clippy::too_many_arguments)]
pub async fn force_insert_pdu_batch(
	&self,
	batch: &mut database::rocksdb::WriteBatch,
	room_id: &RoomId,
	event_id: &EventId,
	pdu: &PduEvent,
	value: &CanonicalJsonObject,
	backfill: bool,
) -> Result<RawPduId> {
	if self.get_pdu_id(event_id).await.is_ok() {
		return Err!(Database("PDU {event_id} already in timeline"));
	}

	let shortroomid = self.services.short.get_or_create_shortroomid(room_id).await;

	let (pdu_count, pdu_id, value) =
		self.prepare_pdu_insert(shortroomid, event_id, value, backfill)?;

	if backfill {
		self.db
			.prepend_backfill_pdu_batch(batch, &pdu_id, event_id, &value, pdu)
			.await;
	} else {
		self.db
			.append_pdu_batch(batch, &pdu_id, pdu, &value, pdu_count)
			.await;
	}

	self.index_pdu_search(shortroomid, &pdu_id, pdu);

	Ok(pdu_id)
}

#[implement(super::Service)]
/// TODO: Integrate the multi-armed bandit `ServerPool` algorithm here for
/// optimal server selection.
pub async fn get_backfill_servers<'a, I: Iterator<Item = &'a ServerName> + Send + 'a>(
	&'a self,
	room_id: &'a RoomId,
	room_mods: I,
) -> impl futures::Stream<Item = ruma::OwnedServerName> + Send + 'a {
	let canonical_room_alias_server = once(
		self.services
			.state_accessor
			.get_canonical_alias(room_id)
			.await,
	)
	.filter_map(Result::ok)
	.map(|alias| alias.server_name().to_owned())
	.stream();

	room_mods
		.map(ToOwned::to_owned)
		.stream()
		.chain(canonical_room_alias_server)
		.chain(
			self.services
				.server
				.config
				.trusted_servers
				.iter()
				.map(ToOwned::to_owned)
				.stream(),
		)
		.chain(
			self.services
				.state_cache
				.room_servers(room_id)
				.map(ToOwned::to_owned),
		)
		.ready_filter(|server_name| {
			!self.services.globals.server_is_ours(server_name)
				&& !self
					.services
					.server
					.config
					.forbidden_remote_server_names
					.is_match(server_name.host())
		})
		.wide_filter_map(move |server_name| async move {
			self.services
				.state_cache
				.server_in_room(&server_name, room_id)
				.await
				.then_some(server_name)
		})
}

#[implement(super::Service)]
pub fn prepare_pdu_insert(
	&self,
	shortroomid: u64,
	event_id: &EventId,
	value: &CanonicalJsonObject,
	backfill: bool,
) -> Result<(PduCount, RawPduId, CanonicalJsonObject)> {
	let count: u64 = self.services.globals.next_count()?;

	let (pdu_count, pdu_id) = if backfill {
		let count_i64: i64 = count.try_into()?;
		let pcount = PduCount::Backfilled(conduwuit_core::validated!(0 - count_i64));
		(pcount, RawPduId::from(PduId { shortroomid, shorteventid: pcount }))
	} else {
		let pcount = PduCount::Normal(count);
		(pcount, RawPduId::from(PduId { shortroomid, shorteventid: pcount }))
	};

	let mut value = value.clone();
	value.insert(
		"event_id".into(),
		ruma::CanonicalJsonValue::String(event_id.as_str().to_owned()),
	);

	Ok((pdu_count, pdu_id, value))
}

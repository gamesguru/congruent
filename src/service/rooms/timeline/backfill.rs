use std::{
	collections::{HashSet, hash_map::DefaultHasher},
	hash::{Hash, Hasher},
	iter::once,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
};

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
	CanonicalJsonObject, CanonicalJsonValue, EventId, Int, OwnedEventId, RoomId, RoomVersionId,
	ServerName, UInt,
	api::federation,
	events::{
		StateEventType,
		room::{create::RoomCreateEventContent, power_levels::RoomPowerLevelsEventContent},
	},
};
use serde_json::value::RawValue as RawJsonValue;

use super::TopoToken;
use crate::rooms::{
	pdu_metadata::{RejectionCode, is_retryable_rejection_reason},
	short::ShortStateKey,
};

/// Maximum number of prev_event hops [`materialize_remote_history_limited`]
/// (and, transitively, [`get_remote_pdu_limited`]'s recursive remote
/// fetches) will walk backwards from a single [`get_remote_pdu`] call. Must
/// be threaded through every recursive/fallback call as a shrinking budget
/// rather than handed out fresh at each hop -- see
/// [`get_remote_pdu_limited`]'s doc comment.
const MAX_REMOTE_HISTORY_DEPTH: usize = 64;

enum PromotionCleanupDecision {
	ClearMarkers,
	PreserveRejection,
}

fn promotion_cleanup_decision(rejected_during_promotion: bool) -> PromotionCleanupDecision {
	if rejected_during_promotion {
		PromotionCleanupDecision::PreserveRejection
	} else {
		PromotionCleanupDecision::ClearMarkers
	}
}

struct RemoteHistoryBudget {
	remaining: AtomicUsize,
	visited: tokio::sync::Mutex<HashSet<OwnedEventId>>,
}

impl RemoteHistoryBudget {
	fn new(limit: usize, root: OwnedEventId) -> Self {
		let mut visited = HashSet::with_capacity(limit.saturating_add(1));
		visited.insert(root);

		Self {
			remaining: AtomicUsize::new(limit),
			visited: tokio::sync::Mutex::new(visited),
		}
	}

	async fn try_visit(&self, event_id: &EventId) -> bool {
		let mut visited = self.visited.lock().await;
		if !visited.insert(event_id.to_owned()) {
			return false;
		}
		drop(visited);

		let mut remaining = self.remaining.load(Ordering::Relaxed);
		while let Some(next) = remaining.checked_sub(1) {
			match self.remaining.compare_exchange_weak(
				remaining,
				next,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				| Ok(_) => return true,
				| Err(observed) => remaining = observed,
			}
		}

		let mut visited = self.visited.lock().await;
		visited.remove(event_id);
		false
	}
}

#[implement(super::Service)]
#[tracing::instrument(name = "backfill", level = "trace", skip(self))]
pub async fn backfill_if_required(
	&self,
	room_id: &RoomId,
	from: TopoToken,
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

	// Singleflight the scan-and-decide phase per room: concurrent backward
	// /messages calls on the same room would otherwise each run a full scan,
	// reach the same gap conclusion, and fire duplicate /backfill requests.
	// The eventual inserts are already race-safe (backfill_pdu takes its own
	// per-room lock with a post-lock existence recheck), so this is purely
	// about not doing the scan and the federation round-trip N times over.
	let _backfill_lock = self.mutex_backfill.lock(room_id).await;

	// Tier 2: skip the scan entirely if this exact `(state_hash, from, limit)`
	// window was already verified gap-free. A larger previously-verified
	// window covers this request too, but only while the room state hash still
	// matches the one observed during the scan.
	let scan_limit_u32: u32 = limit.clamp(100, 500).try_into().unwrap_or(100);
	let current_shortstatehash = self
		.services
		.state
		.get_room_shortstatehash(room_id)
		.await
		.unwrap_or(0);
	if Self::backfill_gap_free_cache_hit(
		self.backfill_gap_free_cache.get(&room_id.to_owned()),
		current_shortstatehash,
		from,
		usize::try_from(scan_limit_u32).unwrap_or(100),
	) {
		debug!(
			%room_id, %from, %limit,
			"backfill: skipping scan, already verified gap-free for this state window"
		);
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
	let scan_limit: usize = usize::try_from(scan_limit_u32).expect("u32 fits in usize");
	let mut saw_extra_timeline_pdu = false;
	let mut verify_after_promotion = false;
	let mut budget = 5_u32;

	loop {
		// Phase 1: Collect scanned PDUs into an event map. With `impl DagNode
		// for Pdu`, rezzy operates directly on PduEvent — no LeanEvent conversion.
		let mut event_map: std::collections::HashMap<OwnedEventId, PduEvent> =
			std::collections::HashMap::new();
		let mut scanned = 0_usize;
		let mut pdus = self
			.topo_pdus_rev(room_id, Some(from))
			.take(scan_limit.saturating_add(1))
			.boxed();
		while let Some(Ok((topo_token, pdu))) = pdus.next().await {
			if scanned == scan_limit {
				saw_extra_timeline_pdu = true;
				break;
			}
			scanned = scanned.saturating_add(1);
			debug!(
				?topo_token,
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
		let mut known_ids: HashSet<OwnedEventId> = HashSet::with_capacity(all_prev_ids.len());
		for prev_id in &all_prev_ids {
			// Outliers exist in `eventid_pdu`, but they are still gaps in the
			// timeline because `/messages` scans the non-outlier topo index. If
			// we treat an outlier parent as "known" here, pagination can advance
			// past its depth and only upgrade it later during a deeper backfill,
			// permanently stranding that event behind the cursor.
			if self.non_outlier_pdu_exists(prev_id).await {
				known_ids.insert(prev_id.clone());
			}
		}

		// Phase 3: Call rezzy for correct backward extremity detection.
		let gaps = rezzy::find_backward_extremities(&event_map, |id| known_ids.contains(id));

		if gaps.is_empty() {
			info!("backfill: no gaps in {room_id} (scanned {scanned} events from {from})");
			if !saw_extra_timeline_pdu {
				let promoted = self.promote_room_state_outliers(room_id).await?;
				if promoted > 0 {
					info!("backfill: promoted {promoted} state outliers at timeline boundary");
					verify_after_promotion = true;
					continue;
				}
			}
			if verify_after_promotion {
				// A promotion can expose a previously hidden ancestor chain. Require one
				// additional gap-free pass before caching the window as stable.
				verify_after_promotion = false;
				continue;
			}

			if current_shortstatehash != 0 {
				self.backfill_gap_free_cache
					.insert(room_id.to_owned(), (current_shortstatehash, from, scan_limit));
			}
			return Ok(());
		}

		// Build the /backfill request: send child event IDs (events that have
		// missing parents), which is what the /backfill API expects.
		// `gaps` iterates a HashMap-backed event_map, so its order is not
		// stable across runs; sort so the request (and any resulting
		// insertion order) is reproducible for debugging and testing.
		let mut backwards_extremities: Vec<OwnedEventId> =
			gaps.iter().map(|gap| gap.event_id.clone()).collect();
		backwards_extremities.sort_unstable();
		let mut unique_missing: Vec<&EventId> = gaps
			.iter()
			.flat_map(|gap| gap.missing_prev_events.iter().map(AsRef::as_ref))
			.collect();
		unique_missing.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));

		let mut repeat_hasher = DefaultHasher::new();
		room_id.as_str().hash(&mut repeat_hasher);
		from.to_string().hash(&mut repeat_hasher);
		scan_limit_u32.hash(&mut repeat_hasher);
		for extremity in &backwards_extremities {
			extremity.as_str().hash(&mut repeat_hasher);
		}
		for missing in &unique_missing {
			missing.as_str().hash(&mut repeat_hasher);
		}
		let repeat_fingerprint = repeat_hasher.finish();

		if self
			.backfill_gap_repeat_cache
			.get(&(room_id.to_owned(), repeat_fingerprint))
			.is_some()
		{
			debug!(
				%room_id, %from, %scan_limit_u32, %repeat_fingerprint,
				"backfill: suppressing repeated unresolved gap window"
			);
			return Ok(());
		}
		self.backfill_gap_repeat_cache
			.insert((room_id.to_owned(), repeat_fingerprint), ());

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
						limit: UInt::from(scan_limit_u32),
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

					// Sort the batch by actual DAG topology before assigning Backfilled
					// positions. `backfill_pdu` hands out each event's
					// `PduCount::Backfilled` slot from a bare `next_count()` call in
					// whatever order it's given the batch, so the order we hand it *is*
					// the timeline order. The wire order the spec asks servers to send
					// (newest-first) is not guaranteed to be a total order -- same-depth
					// siblings can legally arrive in either relative order, and a remote
					// server's tiebreak need not match ours. Left untreated, that drift
					// becomes a permanent wrong position for one event, which then falls
					// outside a `/messages` page boundary and looks like a dropped event.
					let pdus = topo_sort_backfill_batch(&pdus);

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
async fn promote_room_state_outliers(&self, room_id: &RoomId) -> Result<usize> {
	let room_version = self.services.state.get_room_version(room_id).await?;
	let state_pdus = self
		.services
		.state_accessor
		.room_state_full_pdus(room_id)
		.filter_map(|result| async move { result.ok() })
		.collect::<Vec<_>>()
		.await;

	let state_event_ids: Vec<OwnedEventId> = state_pdus
		.iter()
		.map(|pdu| pdu.event_id().to_owned())
		.collect();
	let state_metadata = self.db.get_event_metadata_batch(&state_event_ids).await;

	let mut outlier_state_event_ids = Vec::new();
	for (pdu, metadata) in state_pdus.into_iter().zip(state_metadata) {
		let event_id = pdu.event_id().to_owned();
		match metadata {
			| Ok(meta) if meta.rejected => {
				// Retryable rejections are recovery markers, not permanent
				// evidence that the event is invalid. Let them back through
				// promotion after clearing the retryable marker so a later
				// retry path does not treat the stored outlier as already
				// settled -- but `promote_outlier` skips all auth checks on
				// the assumption its input already passed them, so this must
				// exclude `MissingAuthEvent` specifically: that reason means
				// *this event's own* auth chain never finished resolving, so
				// there's no validated auth verdict to trust here at all
				// (contrast e.g. `StateResolutionFailedWithPrevsPresent`,
				// which is attached only after this event's own auth check
				// already succeeded -- see `is_event_pending_auth_resolution`'s
				// doc comment). Force-promoting an unvalidated event would be
				// an auth bypass, not a legitimate retry. Leave it rejected;
				// it can only be safely revalidated by a real
				// `handle_outlier_pdu` re-run, not by this promotion pass.
				let pending_auth_resolution = matches!(
					RejectionCode::parse(&meta.rejection_reason),
					Some(RejectionCode::MissingAuthEvent)
				);
				if meta.is_outlier
					&& !pending_auth_resolution
					&& is_retryable_rejection_reason(&meta.rejection_reason)
				{
					outlier_state_event_ids.push(event_id);
				}
			},
			| Ok(meta) if meta.is_outlier => outlier_state_event_ids.push(event_id),
			| Ok(_) => continue,
			| Err(_) => {
				// No batch metadata to inspect here (the batch lookup itself
				// failed) -- fall back to the narrower live check so this
				// path doesn't force-promote an event whose own auth chain
				// never finished resolving. See the comment above.
				if self
					.services
					.pdu_metadata
					.is_event_pending_auth_resolution(&event_id)
					.await
				{
					continue;
				}
				if self.non_outlier_pdu_exists(&event_id).await {
					continue;
				}
				outlier_state_event_ids.push(event_id);
			},
		}
	}

	if outlier_state_event_ids.is_empty() {
		return Ok(0);
	}

	self.promote_outliers_sorted(room_id, &outlier_state_event_ids, &room_version)
		.await
}

/// Reorder a raw `/backfill` response batch into strict newest-first DAG
/// order before insertion. See the call site comment in
/// `backfill_if_required` for why the wire order can't be trusted directly.
///
/// This deliberately does *not* use `rezzy::resolve::sorting::lean_kahn_sort`
/// (the tool `promote_outliers_sorted` uses for the auth chain): that sort
/// builds its graph from `auth_events` and tie-breaks by sender power level,
/// which is correct for ordering create/power_levels/membership during a
/// `/send_join` promotion, but wrong here -- ordinary timeline messages in
/// the same room mostly share the same three `auth_events` (create, power
/// levels, sender's join), so it produces near-empty graph edges between
/// them and falls back to ranking messages by their *sender's power level*
/// instead of DAG order. That's a worse bug than the wire-order race it
/// would be replacing.
///
/// Instead this sorts by `depth` directly, matching what Synapse's
/// `_process_pulled_events` does (`sorted(new_events, key=lambda x: x.depth)`)
/// for the exact same reason stated in its own comment: wire order isn't
/// trustworthy, but `depth` is a reliable enough total order for a single
/// fetched batch, and same-depth ties are broken by insertion order same as
/// Synapse's `stream_ordering` tiebreak -- there's no secondary signal in
/// this batch that would resolve concurrent siblings any more "correctly"
/// than that.
///
/// Events that fail to parse (missing/invalid `event_id`, malformed
/// content, etc.) are dropped from the batch and logged rather than
/// aborting the whole reorder -- `backfill_pdu` already tolerates and
/// skips individual bad events without failing the batch, so this matches
/// existing behavior.
fn topo_sort_backfill_batch(pdus: &[Box<RawJsonValue>]) -> Vec<Box<RawJsonValue>> {
	let mut keyed: Vec<(u64, Box<RawJsonValue>)> = Vec::with_capacity(pdus.len());

	for pdu in pdus {
		// Deliberately not `parse_incoming_pdu` here: this pass only ever reads
		// `depth` out of the raw JSON, but that call also resolves `room_id` --
		// which, for a v12 non-create event with no inline `room_id`, falls back
		// to sequentially awaiting up to 10 auth-event DB lookups
		// (`MAX_AUTH_EVENTS_ROOM_ID_FALLBACK` in parse_incoming_pdu.rs). Paying
		// that for every event in a batch, on top of `backfill_pdu` doing the
		// exact same full parse again a moment later for the real insertion,
		// turns a 500-event v12 backfill response into thousands of sequential
		// lookups just to sort it. `backfill_pdu` remains the sole place actual
		// validation and malformed-event dropping happens for this batch -- a
		// syntactically-broken event here just can't contribute a depth, so it
		// sorts as depth 0 and gets caught (and logged) by backfill_pdu's own
		// parse when it's inserted.
		let depth = serde_json::from_str::<CanonicalJsonObject>(pdu.get())
			.ok()
			.and_then(|value| value.get("depth").and_then(CanonicalJsonValue::as_integer))
			.and_then(|d| u64::try_from(i64::from(d)).ok())
			.unwrap_or_default();

		keyed.push((depth, pdu.clone()));
	}

	// Newest-first (descending depth): `backfill_pdu` hands out
	// increasingly-negative `PduCount::Backfilled` slots in call order, so
	// the oldest event must be *inserted last* to receive the
	// most-negative (furthest-in-past) slot. `sort_by` is stable, so
	// same-depth ties keep their relative wire order -- matching Synapse's
	// `sorted(new_events, key=lambda x: x.depth)`, which relies on the same
	// stability for its own ties. No secondary key: a hash-based (event_id)
	// tiebreak would be more deterministic across repeated runs, but it has
	// no causal meaning either, and it would throw away whatever ordering
	// signal the remote server's wire order actually carries -- which is at
	// least as likely to reflect real send order as a hash comparison is.
	keyed.sort_by(|(depth_a, _), (depth_b, _)| depth_b.cmp(depth_a));

	keyed.into_iter().map(|(_, pdu)| pdu).collect()
}

#[implement(super::Service)]
#[tracing::instrument(name = "get_remote_pdu", level = "debug", skip(self))]
pub async fn get_remote_pdu(&self, room_id: &RoomId, event_id: &EventId) -> Result<PduEvent> {
	let budget =
		Arc::new(RemoteHistoryBudget::new(MAX_REMOTE_HISTORY_DEPTH, event_id.to_owned()));
	self.get_remote_pdu_limited(room_id, event_id, &budget)
		.await
}

/// Same as [`Self::get_remote_pdu`], but threading a shared per-request
/// budget through to the predecessor-chain materialization that follows a
/// successful fetch. Called directly (recursively, via
/// [`Self::materialize_remote_history_limited`]) whenever a predecessor
/// turns out to be unknown even as an outlier and has to be fetched from
/// federation in turn -- handing each sibling branch a fresh budget would
/// let remote-history fanout multiply the total work for one request.
#[implement(super::Service)]
async fn get_remote_pdu_limited(
	&self,
	room_id: &RoomId,
	event_id: &EventId,
	budget: &Arc<RemoteHistoryBudget>,
) -> Result<PduEvent> {
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
				// `/event`, `/context` and `/timestamp_to_event` fetch arbitrary
				// historical events. Ingest them as outliers first, then
				// materialize their predecessor chain into the backfilled
				// timeline so they keep their historical position instead of
				// becoming new forward timeline/extremity events.
				.handle_incoming_pdu(backfill_server, room_id, event_id, value, false, None)
				.boxed()
				.await
			{
				| Ok(_) => {
					self.materialize_remote_history_limited(room_id, event_id, budget)
						.await?;

					if self.get_pdu_id(event_id).await.is_err() {
						self.promote_outlier(room_id, event_id).await?;
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

#[implement(super::Service)]
async fn materialize_remote_history_limited(
	&self,
	room_id: &RoomId,
	event_id: &EventId,
	budget: &Arc<RemoteHistoryBudget>,
) -> Result<()> {
	if self.get_pdu_id(event_id).await.is_ok() {
		return Ok(());
	}

	let pdu = self.get_pdu(event_id).await?;
	for prev_id in pdu.prev_events() {
		if self.get_pdu_id(prev_id).await.is_ok() {
			continue;
		}

		if !budget.try_visit(prev_id).await {
			continue;
		}

		if self.get_pdu(prev_id).await.is_err() {
			let _ = Box::pin(self.get_remote_pdu_limited(room_id, prev_id, budget)).await;
		} else {
			Box::pin(self.materialize_remote_history_limited(room_id, prev_id, budget)).await?;
		}
	}

	if self.get_pdu_id(event_id).await.is_err() {
		self.promote_outlier(room_id, event_id).await?;
	}

	Ok(())
}

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
	if self.non_outlier_pdu_exists(&event_id).await {
		info!(target: "backfill_debug", %event_id, "backfill_pdu: already in timeline (pre-lock check), skipping");
		return Ok(());
	}

	// Backfill events come from a trusted /backfill response. We still need
	// auth validation, but backfill can recover missing auth context via the
	// post-state recovery stage before giving up.
	let room_version_id = self.services.state.get_room_version(&room_id).await?;

	let (pdu_event, json_value) = match Box::pin(self.services.event_handler.handle_outlier_pdu(
		origin,
		None::<&PduEvent>,
		&event_id,
		&room_id,
		value.clone(),
		false,
		false,
		Some(&room_version_id),
		crate::rooms::event_handler::AuthRecoveryStage::AfterStateIds,
	))
	.await
	{
		| Ok(result) => result,
		| Err(e @ Error::MissingAuthEvents(_)) => {
			// `handle_outlier_pdu` already persists the event as an outlier when
			// auth recovery cannot complete. Do not bypass validation by inserting
			// the raw PDU into the timeline.
			warn!(
				target: "backfill_debug",
				"handle_outlier_pdu could not fully validate backfill event {event_id}; leaving it as an outlier"
			);
			return Err(e);
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
	if self.non_outlier_pdu_exists(&event_id).await {
		warn!(
			target: "backfill_debug",
			%event_id,
			"backfill_pdu: already in timeline (post-lock check) -- TOCTOU race caught, \
			 skipping redundant insert"
		);
		return Ok(());
	}

	let count: i64 = match count {
		| Some(count) => count.try_into()?,
		| None => self.services.globals.next_count()?.try_into()?,
	};
	let count = count
		.checked_neg()
		.expect("backfill counts are strictly positive before negation");

	let pdu_id: RawPduId = PduId {
		shortroomid,
		shorteventid: PduCount::Backfilled(count),
	}
	.into();

	info!(target: "backfill_debug", %event_id, count, "backfill_pdu: about to insert");

	// Insert pdu
	self.db
		.prepend_backfill_pdu(&pdu_id, &event_id, &json_value, &pdu_event)
		.await;
	if let Err(e) = self.associate_current_state(&room_id, &event_id).await {
		warn!(target: "backfill_debug", %event_id, count, "backfill_pdu: associate_current_state FAILED: {e}");
		return Err(e);
	}

	info!(target: "backfill_debug", %event_id, count, "backfill_pdu: insert complete");

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
	let mut batch = database::Batch::new();
	self.promote_outlier_batch(&mut batch, room_id, event_id)
		.await?;
	self.db_apply_batch(batch);
	Ok(())
}

/// Batched variant of [`Self::promote_outlier`]. The caller owns the
/// `Batch` and is responsible for applying it (typically once per chunk of
/// events, mirroring [`Self::force_insert_pdu_batch`]) so bulk promotions
/// don't fire one DB commit -- and one `/sync` wake -- per event.
#[implement(super::Service)]
pub async fn promote_outlier_batch<'a>(
	&'a self,
	batch: &mut database::Batch<'a>,
	room_id: &RoomId,
	event_id: &EventId,
) -> Result<()> {
	// Skip if already in timeline
	if self.non_outlier_pdu_exists(event_id).await {
		return Ok(());
	}

	let value = self.get_outlier_pdu_json(event_id).await?;

	let pdu: PduEvent = serde_json::from_value(
		serde_json::to_value(&value).map_err(|e| err!(Database("Bad outlier JSON: {e:?}")))?,
	)
	.map_err(|e| err!(Database("Bad outlier PDU: {e:?}")))?;

	let shortroomid = self.services.short.get_or_create_shortroomid(room_id).await;

	let insert_lock = self.mutex_insert.lock(room_id).await;

	// Re-check after acquiring insert lock to prevent TOCTOU races with a
	// concurrent backfill_pdu/append_pdu/force_insert_pdu inserting the
	// same event (see docs/development-gg/backfill-append-toctou-race.md).
	if self.non_outlier_pdu_exists(event_id).await {
		warn!(
			target: "backfill_debug",
			%event_id,
			%room_id,
			"promote_outlier: event already in timeline under the insert lock -- \
			 skipping redundant insert (TOCTOU race caught)"
		);
		drop(insert_lock);
		return Ok(());
	}

	// A concurrent retry may have rejected this event after the caller's
	// earlier metadata check but before we acquired the insertion lock.
	// Do not promote an event that is now explicitly rejected.
	if self.services.pdu_metadata.is_event_rejected(event_id).await {
		warn!(
			target: "backfill_debug",
			%event_id,
			%room_id,
			"promote_outlier: event was rejected before insert-lock promotion, skipping"
		);
		drop(insert_lock);
		return Ok(());
	}

	// Use backfill (negative) PDU count — these are historical events
	// that predate the join, not new forward events.
	let count: i64 = self.services.globals.next_count()?.try_into()?;

	let pdu_id: RawPduId = PduId {
		shortroomid,
		shorteventid: PduCount::Backfilled(validated!(0 - count)),
	}
	.into();

	self.db
		.prepend_backfill_pdu_batch(batch, &pdu_id, event_id, &value, &pdu)
		.await;
	self.associate_current_state(room_id, event_id).await?;

	// A retry can still win the race between the earlier metadata checks and
	// this insert. The room insert lock keeps the read/clear pair below
	// synchronized with retry writers that use the same lock, so a rejection
	// written after the read cannot be erased here.
	match promotion_cleanup_decision(self.services.pdu_metadata.is_event_rejected(event_id).await)
	{
		| PromotionCleanupDecision::PreserveRejection => {
			warn!(
				target: "backfill_debug",
				%event_id,
				%room_id,
				"promote_outlier: event was rejected during promotion; leaving rejection in place"
			);
		},
		| PromotionCleanupDecision::ClearMarkers => {
			// Clear any stale rejection flags now that the event is accepted into
			// the timeline.
			self.services.pdu_metadata.clear_pdu_markers(event_id);
		},
	}

	drop(insert_lock);

	self.index_pdu_search(shortroomid, &pdu_id, &pdu);

	// Remove from outlier room index
	self.clear_outlier_flag(event_id);

	Ok(())
}

#[implement(super::Service)]
async fn associate_current_state(&self, room_id: &RoomId, event_id: &EventId) -> Result<()> {
	let shortstatehash = self.services.state.get_room_shortstatehash(room_id).await?;
	let state_ids: Vec<(ShortStateKey, OwnedEventId)> = self
		.services
		.state_accessor
		.state_full_ids::<OwnedEventId>(shortstatehash)
		.collect::<Vec<_>>()
		.await;
	let compressed: crate::rooms::state_compressor::CompressedState = self
		.services
		.state_compressor
		.compress_state_events(state_ids.iter().map(|(key, id)| (key, id.as_ref())))
		.collect()
		.await;

	self.services
		.state
		.set_event_state(event_id, room_id, Arc::new(compressed))
		.await?;
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
		if self.non_outlier_pdu_exists(event_id).await {
			continue;
		}

		let Ok(pdu) = self.get_pdu_outlier(event_id).await else {
			continue;
		};

		let lean = rezzy::LeanEvent {
			event_id: event_id.to_string(),
			event_type: pdu.kind.to_string(),
			sender: pdu.sender.to_string(),
			state_key: pdu.state_key.as_ref().map(|k| format!("{k}")),
			content: serde_json::from_str(pdu.content.get()).unwrap_or(serde_json::Value::Null),
			origin_server_ts: u64::from(pdu.origin_server_ts),
			auth_events: pdu.auth_events.iter().map(|id| format!("{id}")).collect(),
			prev_events: pdu.prev_events.iter().map(|id| format!("{id}")).collect(),
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
	if self.non_outlier_pdu_exists(event_id).await {
		return Err!(Database("PDU {event_id} already in timeline"));
	}

	let shortroomid = self.services.short.get_or_create_shortroomid(room_id).await;

	let insert_lock = self.mutex_insert.lock(room_id).await;

	// Re-check after acquiring insert lock to prevent TOCTOU races with a
	// concurrent backfill_pdu/append_pdu/promote_outlier inserting the same
	// event (see docs/development-gg/backfill-append-toctou-race.md).
	if self.non_outlier_pdu_exists(event_id).await {
		warn!(
			target: "backfill_debug",
			%event_id,
			%room_id,
			"force_insert_pdu: event already in timeline under the insert lock -- \
			 skipping redundant insert (TOCTOU race caught)"
		);
		drop(insert_lock);
		return Err!(Database("PDU {event_id} already in timeline"));
	}

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

#[cfg(test)]
mod tests {
	use super::{PromotionCleanupDecision, promotion_cleanup_decision};

	#[test]
	fn promotion_cleanup_preserves_concurrent_rejection() {
		assert!(matches!(
			promotion_cleanup_decision(true),
			PromotionCleanupDecision::PreserveRejection
		));
	}

	#[test]
	fn promotion_cleanup_clears_when_no_rejection_raced() {
		assert!(matches!(
			promotion_cleanup_decision(false),
			PromotionCleanupDecision::ClearMarkers
		));
	}
}

#[implement(super::Service)]
#[allow(clippy::too_many_arguments)]
pub async fn force_insert_pdu_batch<'a>(
	&'a self,
	batch: &mut database::Batch<'a>,
	room_id: &RoomId,
	event_id: &EventId,
	pdu: &PduEvent,
	value: &CanonicalJsonObject,
	backfill: bool,
) -> Result<RawPduId> {
	if self.non_outlier_pdu_exists(event_id).await {
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
	value.insert("event_id".into(), CanonicalJsonValue::String(event_id.as_str().to_owned()));

	Ok((pdu_count, pdu_id, value))
}

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	sync::Arc,
	time::{Duration, Instant},
};

use conduwuit::{Event, PduEvent, Result, debug, error, info, pdu::PduBuilder, warn};
use conduwuit_core::{
	Err, err,
	utils::stream::{BroadbandExt, IterStream, TryIgnore},
};
use futures::{FutureExt, StreamExt, TryStreamExt};
use slipstream::{
	EventId, OwnedEventId, OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, RoomVersionId,
	ServerName, UserId,
	api::federation::event::{get_room_state, get_room_state_ids},
	events::{
		StateEventType,
		room::member::{MembershipState, RoomMemberEventContent},
	},
	sswire::RawJsonValue,
};

use super::reauth::{Reauthed, StateMap};
use crate::rooms::{short::ShortStateKey, state_partial::Service};

enum StateEventLoadError {
	InvalidResponse(String),
	Transient(conduwuit::Error),
}

/// A successful resync's fetched state and the corrected state installed from
/// it, kept so later drift can be rebuilt locally instead of refetching.
struct Resynced {
	/// The event the full state was fetched at.
	anchor: OwnedEventId,
	/// That event's full state.
	anchor_state: StateMap,
	/// The forward extremities the installed state was built from (sorted).
	extremities: Vec<OwnedEventId>,
}

/// Errors meaning no remote server gave usable state, the only failures that
/// count towards `partial_state_resync_give_up_after`.
const REMOTE_FAILURES: [&str; 2] = ["No usable state", "No reachable server"];

/// How many times the re-authorised state is rebuilt when the room changes under it.
const REAUTH_ATTEMPTS: usize = 5;

impl Service {
	/// Background worker: resyncs partial-state rooms as soon as they are
	/// joined (signalled via `mark_as_partial`), and resumes any rooms left
	/// partial across a restart.
	pub async fn spawn_resync_worker(self: Arc<Self>) {
		info!("Starting MSC3902 partial state resync worker");

		let pending: Vec<OwnedRoomId> = self
			.db
			.state_partial_rooms
			.stream()
			.ignore_err()
			.map(|(room_id, _): (OwnedRoomId, OwnedServerName)| room_id)
			.collect()
			.await;
		for room_id in pending {
			_ = self.resync_tx.send(room_id);
		}

		while let Ok(room_id) = self.resync_rx.recv_async().await {
			self.clone().start_resync(room_id);
		}
	}

	/// Send PDUs created during the partial join to servers that were only
	/// discovered by the resync. Rows that cannot be read are kept and counted
	/// in the returned value; only a scan or send failure is an error, so the
	/// caller can retry it.
	async fn send_pending(&self, room_id: &RoomId) -> Result<usize> {
		let mut prefix = room_id.as_bytes().to_vec();
		prefix.push(database::SEP);
		// A scan error must fail the drain: treating it as "nothing pending" would
		// let the caller clear the partial markers with rows still unsent.
		let rows: Vec<(Vec<u8>, Vec<u8>)> = self
			.db
			.state_partial_pending
			.raw_stream_prefix(&prefix)
			.map_ok(|(key, value): (&[u8], &[u8])| (key.to_vec(), value.to_vec()))
			.try_collect()
			.await?;
		let mut unreadable = 0_usize;
		let mut pending = Vec::new();
		for (key, value) in &rows {
			// Key and value are judged separately: a value that went bad only loses
			// the list of servers already sent to, and the PDU is then (re)sent to
			// every server, which federation deduplicates by event ID; a key that
			// went bad can never be sent at all.
			let Some(pdu_id) = key
				.get(prefix.len()..)
				.and_then(|id| <[u8; 16]>::try_from(id).ok())
			else {
				// Keep the row: it can't be sent, but it must not be discarded either.
				error!(row = ?key, "Unreadable pending row for a partial-state PDU; keeping it");
				unreadable = unreadable.saturating_add(1);
				continue;
			};
			let sent_to: HashSet<OwnedServerName> = match std::str::from_utf8(value) {
				| Ok(sent_to) => sent_to
					.split(',')
					.filter_map(|server| OwnedServerName::try_from(server).ok())
					.collect(),
				| Err(_) => {
					warn!(
						row = ?key,
						"Unreadable sent-to list for a pending partial-state PDU; \
						 sending it to every server instead"
					);
					HashSet::new()
				},
			};
			pending.push((conduwuit::matrix::RawPduId(pdu_id), sent_to));
		}
		if pending.is_empty() {
			return Ok(unreadable);
		}

		let servers: Vec<OwnedServerName> = self
			.services
			.state_cache
			.room_servers(room_id)
			.filter(|server| {
				futures::future::ready(!self.services.globals.server_is_ours(server))
			})
			.collect()
			.await;

		let mut failed = HashSet::new();
		let mut first_error = None;
		for (pdu_id, sent_to) in pending {
			let remaining: Vec<OwnedServerName> = servers
				.iter()
				.filter(|server| !sent_to.contains(*server))
				.cloned()
				.collect();
			if remaining.is_empty() {
				continue;
			}
			if let Err(e) = self
				.services
				.sending
				.send_pdu_servers(remaining.into_iter().stream(), &pdu_id)
				.await
			{
				warn!("Failed to send partial-state PDU to newly discovered servers: {e}");
				failed.insert(pdu_id.0);
				first_error.get_or_insert(e);
			}
		}
		for (key, _) in &rows {
			// Unreadable rows are kept (counted above); failed sends are retried.
			let Some(pdu_id) = key
				.get(prefix.len()..)
				.and_then(|id| <[u8; 16]>::try_from(id).ok())
			else {
				continue;
			};
			if !failed.contains(&pdu_id) {
				self.db.state_partial_pending.remove(key);
			}
		}
		first_error.map_or(Ok(unreadable), Err)
	}

	/// Events accepted while `room_id`'s state was partial that no resync has
	/// cleared yet, as (row key, event id). Rows that cannot be parsed are
	/// dropped with an error: this server wrote them with a valid event id, so
	/// an unreadable one is corruption, and keeping it would hold the room
	/// partial forever.
	async fn pending_event_marks(
		&self,
		room_id: &RoomId,
	) -> Result<Vec<(Vec<u8>, OwnedEventId)>> {
		let mut prefix = room_id.as_bytes().to_vec();
		prefix.push(database::SEP);
		let rows: Vec<(Vec<u8>, Vec<u8>)> = self
			.db
			.state_partial_pending_events
			.raw_stream_prefix(&prefix)
			.map_ok(|(key, _): (&[u8], &[u8])| (key.to_vec(), Vec::new()))
			.try_collect()
			.await?;
		let mut marks = Vec::with_capacity(rows.len());
		for (key, _) in rows {
			let Some(event_id) = key
				.get(prefix.len()..)
				.and_then(|bytes| std::str::from_utf8(bytes).ok())
				.and_then(|id| OwnedEventId::parse(id).ok())
			else {
				error!(row = ?key, "Unreadable pending-event mark for a partial-state room; discarding it");
				self.db.state_partial_pending_events.remove(&key);
				continue;
			};
			marks.push((key, event_id));
		}
		Ok(marks)
	}

	/// Drop every marker keeping `room_id` partial: the room itself, the anchor
	/// event, and the pending-event marks. Callers hold the room's state lock,
	/// so no append can record a mark in between.
	async fn clear_partial_markers(&self, room_id: &RoomId) -> Result {
		// Scan before removing any markers. If the scan fails, callers retain
		// the room marker so startup and the worker can retry this cleanup.
		let marks = self.pending_event_marks(room_id).await?;
		for (key, _) in marks {
			self.db.state_partial_pending_events.remove(&key);
		}
		self.db.state_partial_rooms.remove(room_id.as_bytes());
		self.db.state_partial_events.remove(room_id.as_bytes());
		Ok(())
	}

	/// Remote servers we send device-list updates to for this room.
	async fn device_list_servers(&self, room_id: &RoomId) -> HashSet<OwnedServerName> {
		let mut servers: HashSet<OwnedServerName> = self
			.services
			.state_cache
			.room_servers(room_id)
			.collect::<Vec<_>>()
			.await
			.into_iter()
			.collect();
		servers.extend(self.partial_servers(room_id).await);
		servers.retain(|server| !self.services.globals.server_is_ours(server));
		servers
	}

	/// Like Synapse's `handle_room_un_partial_stated`: servers that only the
	/// resynced state reveals (members present or past) missed the local device
	/// list changes made since we joined, so send those to them now.
	async fn replay_device_updates(
		&self,
		room_id: &RoomId,
		servers_before: &HashSet<OwnedServerName>,
	) -> Result {
		let mut servers = self.device_list_servers(room_id).await;
		let members = self
			.services
			.state_accessor
			.room_state_keys(room_id, &StateEventType::RoomMember)
			.await
			.unwrap_or_default();
		for member in members {
			if let Ok(user_id) = UserId::parse(member.as_str()) {
				let server = user_id.server_name().clone();
				if !self.services.globals.server_is_ours(&server) {
					servers.insert(server);
				}
			}
		}
		servers.retain(|server| !servers_before.contains(server));
		if servers.is_empty() {
			return Ok(());
		}

		let from = self.partial_since(room_id).unwrap_or(0);
		let mut all_changes = BTreeMap::<u64, HashSet<OwnedUserId>>::new();
		let mut changes = std::pin::pin!(self.services.users.room_keys_changed(
			room_id,
			Some(from),
			None
		));
		while let Some((user_id, count)) = changes.next().await {
			if self.services.globals.user_is_local(&user_id) {
				all_changes.entry(count).or_default().insert(user_id);
			}
		}

		let mut user_devices = HashMap::new();
		for users in all_changes.values() {
			for user_id in users {
				if !user_devices.contains_key(user_id) {
					let devices: Vec<slipstream::OwnedDeviceId> =
						self.services.users.all_device_ids(user_id).collect().await;
					user_devices.insert(user_id.clone(), devices);
				}
			}
		}

		let (edus, _) = crate::sending::sender::build_device_list_edus(
			all_changes,
			&user_devices,
			(from, u64::MAX),
			usize::MAX,
		);
		info!(
			%room_id, ?servers, edus = edus.len(),
			"Replaying device list updates missed by servers found by the resync"
		);
		for server in &servers {
			for edu in &edus {
				self.services
					.sending
					.send_edu_server(server, edu.clone())
					.inspect_err(|e| {
						warn!("Failed to replay device list update to {server}: {e}");
					})?;
			}
		}
		Ok(())
	}

	/// Spawn a resync task for the room unless one is already running.
	fn start_resync(self: Arc<Self>, room_id: OwnedRoomId) {
		if !self.active.lock().expect("locked").insert(room_id.clone()) {
			return;
		}

		let runtime = self.services.server.runtime().clone();
		drop(runtime.spawn(async move {
			self.clone().resync_with_backoff(&room_id).await;
			self.active.lock().expect("locked").remove(&room_id);
		}));
	}

	/// Retry the resync with exponential backoff until it succeeds or the
	/// room is no longer partial.
	async fn resync_with_backoff(self: Arc<Self>, room_id: &RoomId) {
		let initial_delay = Duration::from_millis(
			self.services
				.server
				.config
				.partial_state_resync_initial_delay_ms
				.max(1),
		);
		let mut delay = initial_delay;
		// When the failures to get usable state from any server began.
		let mut failing_since: Option<Instant> = None;
		let mut servers_before: Option<HashSet<OwnedServerName>> = None;
		// Members found by every resync so far; a repeat resync only reports new ones.
		let mut found: HashSet<OwnedUserId> = HashSet::new();
		// The installed state; kept so drift is corrected by a local rebuild
		// rather than another fetch from the network.
		let mut resynced: Option<Resynced> = None;
		let mut device_updates_replayed = false;
		loop {
			let Ok(Some(server_name)) = self.partial_server(room_id).await else {
				return;
			};

			debug!("Resyncing partial state for room {} from server {}", room_id, server_name);
			// Captured once: a retry after a failed replay must still replay to the
			// servers the earlier resync discovered.
			if servers_before.is_none() {
				servers_before = Some(self.device_list_servers(room_id).await);
			}
			let servers_before = servers_before.clone().unwrap_or_default();
			if resynced.is_none() {
				match self.clone().resync_room(room_id, &server_name).await {
					| Ok((users, installed)) => {
						info!("Successfully resynced partial state for room {}", room_id);
						found.extend(users);
						resynced = Some(installed);
						delay = initial_delay;
					},
					| Err(e) => {
						warn!("Failed to resync partial state for room {}: {}", room_id, e);
						if self.should_give_up(&e, &mut failing_since) {
							warn!(%room_id, "No server gave usable state in time; abandoning the join");
							self.abandon_until_done(room_id, delay).await;
							return;
						}
						back_off(&mut delay).await;
						continue;
					},
				}
			}

			// Keep the successful resync result and completed replay stages across
			// retries. A send_pending failure must not refetch state or re-emit the
			// device-list EDUs that were already queued.
			if !device_updates_replayed {
				if let Err(e) = self.replay_device_updates(room_id, &servers_before).await {
					warn!("Failed to replay device list updates for room {room_id}: {e}");
					back_off(&mut delay).await;
					continue;
				}
				device_updates_replayed = true;
			}

			// An append that sees the room partial does its bookkeeping while
			// holding the room's state lock: a local send records its pending row,
			// every append records its pending-event mark, and the forward
			// extremities move. Holding the lock here means each PDU either did all
			// of that before this drain or sees no marker at all; both checks below
			// rely on that, so a path that replaced current state without touching
			// either would have to be found and given a state-generation counter.
			let state_lock = self.services.state.mutex.lock(room_id).await;
			// Events accepted since the installed state was built are outside its
			// re-authorised window: rebuild before the markers can be dropped.
			let Some(installed) = resynced.as_mut() else {
				continue;
			};
			if self.has_drifted(room_id, installed).await {
				drop(state_lock);
				debug!(%room_id, "room changed since the state was installed; rebuilding it");
				// Rebuild locally against the anchor already fetched. A successful
				// rebuild is checked again at once: backing off lets a busy room append
				// before every check and never reach the marker-clear path.
				match self.rebuild_drifted(room_id, installed, &mut found).await {
					| Ok(new_members) => device_updates_replayed &= !new_members,
					| Err(e) => {
						warn!(
							"Could not rebuild partial state for room {room_id}: {e}; fetching \
							 it again"
						);
						resynced = None;
						device_updates_replayed = false;
						back_off(&mut delay).await;
					},
				}
				continue;
			}
			match self.send_pending(room_id).await {
				| Ok(0) => {},
				| Ok(unreadable) => {
					warn!(
						%room_id,
						unreadable,
						"Unreadable pending rows kept; their PDUs were not replayed"
					);
					drop(state_lock);
					back_off(&mut delay).await;
					continue;
				},
				| Err(e) => {
					warn!("Failed to replay pending PDUs for room {room_id}: {e}");
					drop(state_lock);
					back_off(&mut delay).await;
					continue;
				},
			}
			if let Err(e) = self.clear_partial_markers(room_id).await {
				warn!(%room_id, "Failed to clear partial-state markers: {e}");
				drop(state_lock);
				back_off(&mut delay).await;
				continue;
			}
			drop(state_lock);

			self.notify_resolved(room_id);
			// Use the full member set found by the successful resync, even after
			// one or more replay retries.
			let discovered = std::mem::take(&mut found);
			for user_id in self.take_deferred_device_updates(room_id, &discovered) {
				self.services
					.users
					.mark_device_key_update_in_room(&user_id, room_id)
					.await;
			}
			// With no partial room left, nothing can place the remaining deferred
			// users, so don't keep them around.
			if !self.any_partial_rooms().await {
				self.clear_deferred_device_updates();
			}
			return;
		}
	}

	async fn sorted_extremities(&self, room_id: &RoomId) -> Vec<OwnedEventId> {
		let mut extremities: Vec<OwnedEventId> = self
			.services
			.state
			.get_forward_extremities(room_id)
			.collect()
			.await;
		extremities.sort();
		extremities
	}

	/// Whether events were accepted since `installed` was built, so it no longer
	/// covers the room: the forward extremities moved or a pending mark remains.
	async fn has_drifted(&self, room_id: &RoomId, installed: &Resynced) -> bool {
		if self.sorted_extremities(room_id).await != installed.extremities {
			return true;
		}
		match self.pending_event_marks(room_id).await {
			| Ok(marks) => !marks.is_empty(),
			| Err(e) => {
				warn!("Failed to read pending-event marks for room {room_id}: {e}");
				true
			},
		}
	}

	/// Re-authorise against the anchor already fetched and install the result.
	/// Members found are added to `found`; returns whether there were new ones.
	async fn rebuild_drifted(
		&self,
		room_id: &RoomId,
		installed: &mut Resynced,
		found: &mut HashSet<OwnedUserId>,
	) -> Result<bool> {
		let (users, extremities) = self
			.reauth_and_install(room_id, &installed.anchor, &installed.anchor_state)
			.await?;
		installed.extremities = extremities;
		let new_members = !users.is_empty();
		found.extend(users);
		Ok(new_members)
	}

	/// Retry `abandon_join` until it completes.
	async fn abandon_until_done(&self, room_id: &RoomId, mut delay: Duration) {
		while let Err(e) = self.abandon_join(room_id).await {
			warn!(%room_id, "Failed to finish abandoning partial-state join: {e}");
			back_off(&mut delay).await;
		}
	}

	/// Whether the remote failures have lasted the configured budget. An
	/// unreachable server counts as much as an unusable answer, so a join whose
	/// remote has vanished is not retried forever. `failing_since` tracks when
	/// they began; any other error resets it.
	fn should_give_up(
		&self,
		error: &conduwuit::Error,
		failing_since: &mut Option<Instant>,
	) -> bool {
		let message = error.to_string();
		if !REMOTE_FAILURES.iter().any(|f| message.contains(f)) {
			*failing_since = None;
			return false;
		}

		let budget = self
			.services
			.server
			.config
			.partial_state_resync_give_up_after;
		let since = *failing_since.get_or_insert_with(Instant::now);
		budget > 0 && since.elapsed() >= Duration::from_secs(budget)
	}

	/// Give up on a partial-state join: the local users leave, and the room's
	/// partial-state tracking is dropped after its leave events have been queued.
	/// A drain failure keeps the markers so the worker can retry it.
	async fn abandon_join(&self, room_id: &RoomId) -> Result {
		let local_users: Vec<OwnedUserId> = self
			.services
			.state_cache
			.local_users_in_room(room_id)
			.collect()
			.await;
		for user_id in &local_users {
			if let Err(e) = self.leave_abandoned_room(room_id, user_id).await {
				warn!(%room_id, %user_id, "Could not send a leave event, marking as left: {e}");
				self.services
					.state_cache
					.mark_as_left(user_id, room_id, None)
					.await;
			}
		}

		// Leaves above may themselves have been recorded as pending. The lock
		// prevents a new append from recording another row or mark between this
		// drain and marker removal; any unreadable row keeps the markers for retry.
		let state_lock = self.services.state.mutex.lock(room_id).await;
		let unreadable = self.send_pending(room_id).await?;
		if unreadable != 0 {
			return Err!("{unreadable} unreadable pending partial-state PDU row(s)");
		}
		self.clear_partial_markers(room_id).await?;
		drop(state_lock);
		self.services.users.untrack_remote_users_in_room(room_id);
		self.services.state_cache.update_joined_count(room_id).await;
		// Release anything waiting on this room's state.
		self.resolved.notify_waiters();
		Ok(())
	}

	/// Append a leave event for `user_id`, as for a normal leave.
	async fn leave_abandoned_room(&self, room_id: &RoomId, user_id: &UserId) -> Result {
		let state_lock = self.services.state.mutex.lock(room_id).await;
		let content = self
			.services
			.state_accessor
			.room_state_get_content::<RoomMemberEventContent>(
				room_id,
				&StateEventType::RoomMember,
				user_id.as_str(),
			)
			.await?;
		let leave = RoomMemberEventContent {
			membership: MembershipState::Leave,
			reason: Some("Could not complete the partial state join".to_owned()),
			join_authorized_via_users_server: None,
			is_direct: None,
			..content
		};

		self.services
			.timeline
			.build_and_append_pdu(
				PduBuilder::state(user_id.to_string(), &leave),
				user_id,
				Some(room_id),
				&state_lock,
			)
			.boxed()
			.await?;
		Ok(())
	}

	/// Fetch and validate full room state, re-authorise events accepted while
	/// state was partial, and install the corrected state. Return newly
	/// discovered remote members for device-list updates after the room is
	/// resolved, together with what was installed so a later drift can be
	/// rebuilt without fetching again.
	async fn resync_room(
		self: Arc<Self>,
		room_id: &RoomId,
		server_name: &ServerName,
	) -> Result<(Vec<OwnedUserId>, Resynced)> {
		debug!("Resyncing room {} starting with server {}", room_id, server_name);

		// Need room version to validate payloads (i.e., event_handler computes hashes)
		let create_pdu = self
			.services
			.state_accessor
			.room_state_get(room_id, &StateEventType::RoomCreate, "")
			.await?;
		let room_version = crate::rooms::event_handler::get_room_version_id(&create_pdu)?;

		let room_id = room_id.to_owned();
		let mut invalid_servers = HashSet::new();
		let (new_state_ids, anchor) = loop {
			let (response, remote_server, state_ids, anchor) = self
				.fetch_remote_state(&room_id, server_name, &invalid_servers)
				.await
				.map_err(|error| {
					if invalid_servers.is_empty() {
						error
					} else {
						err!(Request(Unknown(
							"No usable state: responses for {room_id} rejected by \
							 {invalid_servers:?}: {error}"
						)))
					}
				})?;

			info!(
				"Fetched {} state events and {} auth events for partial room {} from {}",
				response.pdus.len(),
				response.auth_chain.len(),
				room_id,
				remote_server
			);

			// Process auth chain events first. The state-event pass below detects
			// missing dependencies and rejects this server's response if incomplete.
			self.clone()
				.process_auth_chain(&room_id, &room_version, &remote_server, response.auth_chain)
				.await;

			let expected = response.pdus.len();
			match self
				.clone()
				.process_state_events(
					&room_id,
					&room_version,
					&remote_server,
					response.pdus,
					&state_ids,
				)
				.await
			{
				| Ok(state) if state.len() == expected => break (state, anchor),
				| Ok(state) => {
					warn!(%room_id, %remote_server, loaded = state.len(), expected, "state response omitted state keys");
					invalid_servers.insert(remote_server);
				},
				| Err(StateEventLoadError::InvalidResponse(reason)) => {
					warn!(%room_id, %remote_server, %reason, "rejecting invalid partial-state response; trying another server");
					invalid_servers.insert(remote_server);
				},
				| Err(StateEventLoadError::Transient(error)) => return Err(error),
			}
		};

		// Events received while the state was partial were accepted without the
		// omitted members; re-authorise them against the full state, which also
		// gives each one a full snapshot (Synapse un-partial-states each event).
		let anchor_state: StateMap = new_state_ids.iter().map(|(k, v)| (*k, v.clone())).collect();
		let (users, extremities) = self
			.reauth_and_install(&room_id, &anchor, &anchor_state)
			.await?;
		Ok((users, Resynced { anchor, anchor_state, extremities }))
	}

	/// Re-authorise the events accepted since `anchor` against its full state
	/// and install the corrected state, retrying while the room changes
	/// underneath, then fix device-list tracking. Returns the remote members
	/// this pass newly discovered and the forward extremities the state was
	/// installed against.
	async fn reauth_and_install(
		&self,
		room_id: &RoomId,
		anchor: &EventId,
		anchor_state: &StateMap,
	) -> Result<(Vec<OwnedUserId>, Vec<OwnedEventId>)> {
		// Events appended while the window is re-authorised (a local leave, say)
		// would be overwritten by the older state, so retry until neither the
		// forward extremities nor the pending-event marks moved under it.
		let mut reauthed = None;
		let mut gave_up = false;
		for attempt in 1..=REAUTH_ATTEMPTS {
			let Some(Reauthed {
				current,
				left,
				extremities,
				window,
				anchor_depth,
			}) = self
				.reauth_window(room_id, anchor, anchor_state.clone())
				.await
			else {
				break;
			};
			if self
				.install_current_state(room_id, current, anchor_depth, &window, &extremities)
				.await?
			{
				reauthed = Some((left, extremities));
				break;
			}
			debug!(%room_id, "room changed during re-authorisation; retrying");
			gave_up = attempt == REAUTH_ATTEMPTS;
		}
		if gave_up {
			return Err!("room {room_id} kept changing during re-authorisation");
		}
		let Some((left, installed_extremities)) = reauthed else {
			return Err!("could not reconstruct partial-state event window for {room_id}");
		};
		// Only now that the corrected state is installed: these users missed
		// updates while out of the room, so refetch their keys.
		self.flush_users_who_left_in_window(room_id, left).await;
		let known: HashSet<OwnedUserId> = self
			.services
			.state_cache
			.room_members(room_id)
			.collect()
			.await;
		self.services
			.state_cache
			.reconcile_membership(room_id)
			.await;

		// Members discovered by the resync joined silently; tell the device list
		// tracking about the remote ones.
		let discovered: Vec<OwnedUserId> = self
			.services
			.state_cache
			.room_members(room_id)
			.collect()
			.await;
		// Returned so the caller marks them only after the room is delivered:
		// marking wakes long-polling syncs, which must see the room resolved.
		Ok((
			discovered
				.into_iter()
				.filter(|u| !known.contains(u) && !self.services.globals.user_is_local(u))
				.collect(),
			installed_extremities,
		))
	}

	/// Try the preferred server and fallback servers, skipping local and excluded
	/// servers. Fetch state IDs before full state and return the response, serving
	/// server, state event IDs, and the event anchoring both requests.
	async fn fetch_remote_state(
		&self,
		room_id: &RoomId,
		server_name: &ServerName,
		excluded: &HashSet<OwnedServerName>,
	) -> Result<(get_room_state::v1::Response, OwnedServerName, Vec<OwnedEventId>, OwnedEventId)>
	{
		// Discovery fallback: collect servers to try
		let others = self
			.services
			.state_cache
			.servers_route_via(room_id)
			.await
			.unwrap_or_default();

		// Servers the send_join response listed are as good as any to ask.
		let mut others = others;
		others.extend(self.partial_servers(room_id).await);

		let servers = sort_servers(server_name, others);

		// Whether any server answered with an error, as opposed to being unreachable.
		let mut answered = false;
		for server in servers {
			if server == self.services.globals.server_name() || excluded.contains(&server) {
				continue;
			}

			debug!("Asking {} for missing full state of {}", server, room_id);
			let event_id = match self.resync_event_id(room_id).await {
				| Some(event_id) => event_id,
				| None => self
					.services
					.state
					.get_forward_extremities(room_id)
					.next()
					.await
					.expect("room has forward extremities"),
			};
			let request = get_room_state::v1::Request {
				room_id: room_id.to_owned(),
				event_id: event_id.clone(),
			};

			// Like Synapse, ask for /state_ids first and only then pull the full
			// state: the ids response is the signal that the remote server is ready
			// to serve this state.
			let ids_request = get_room_state_ids::v1::Request::new(
				request.event_id.clone(),
				request.room_id.clone(),
			);
			let ids_response = match self
				.services
				.sending
				.send_federation_request(&server, ids_request)
				.await
			{
				| Ok(response) => response,
				| Err(e) => {
					warn!("Failed to fetch state ids from {}: {}", server, e);
					answered |= server_answered(&e);
					continue;
				},
			};

			match self
				.services
				.sending
				.send_federation_request(&server, request)
				.await
			{
				| Ok(response) => {
					return Ok((response, server, ids_response.pdu_ids, event_id));
				},
				| Err(e) => {
					warn!("Failed to fetch state from {}: {}", server, e);
					answered |= server_answered(&e);
				},
			}
		}

		// Only servers that answered without usable state count towards giving
		// up; unreachable ones are retried indefinitely.
		if answered {
			Err(err!(Request(NotFound("No usable state: every server answered with an error"))))
		} else {
			Err(err!(Request(NotFound("No reachable server available to resync state"))))
		}
	}

	async fn process_auth_chain(
		self: Arc<Self>,
		room_id: &RoomId,
		room_version: &RoomVersionId,
		remote_server: &ServerName,
		auth_chain: Vec<RawJsonValue>,
	) {
		use conduwuit::matrix::event::gen_event_id_canonical_json;

		auth_chain
			.into_iter()
			.stream()
			.broad_filter_map(move |pdu_json: RawJsonValue| {
				let room_version = room_version.clone();
				let remote_server = remote_server.to_owned();
				let room_id = room_id.to_owned();
				let self_copy = self.clone();
				async move {
					let (calculated_event_id, value) =
						gen_event_id_canonical_json(&pdu_json, &room_version).ok()?;

					let _ = self_copy
						.services
						.event_handler
						.handle_incoming_pdu(
							&remote_server,
							&room_id,
							&calculated_event_id,
							value,
							// minimize work, store as outliers
							false,
							&room_version,
						)
						.await;

					Some(())
				}
			})
			.collect::<Vec<()>>()
			.await;
	}

	async fn process_state_events(
		self: Arc<Self>,
		room_id: &RoomId,
		room_version: &RoomVersionId,
		remote_server: &ServerName,
		pdus: Vec<RawJsonValue>,
		expected_ids: &[OwnedEventId],
	) -> std::result::Result<BTreeMap<ShortStateKey, OwnedEventId>, StateEventLoadError> {
		use conduwuit::matrix::event::gen_event_id_canonical_json;

		let expected: HashSet<_> = expected_ids.iter().cloned().collect();
		let mut parsed = Vec::with_capacity(pdus.len());
		for pdu_json in pdus {
			let (event_id, value) = gen_event_id_canonical_json(&pdu_json, room_version)
				.map_err(|e| {
					StateEventLoadError::InvalidResponse(format!(
						"could not parse state event: {e}"
					))
				})?;
			if !expected.contains(&event_id) {
				return Err(StateEventLoadError::InvalidResponse(format!(
					"state response contained {event_id}, absent from /state_ids"
				)));
			}

			let pdu =
				PduEvent::from_id_val(&event_id, value.clone(), Some(room_id)).map_err(|e| {
					StateEventLoadError::InvalidResponse(format!(
						"could not decode state event {event_id}: {e}"
					))
				})?;
			let state_key = pdu.state_key.ok_or_else(|| {
				StateEventLoadError::InvalidResponse(format!(
					"state response contained non-state event {event_id}"
				))
			})?;
			parsed.push((event_id, value, pdu.kind.to_string(), state_key, pdu.auth_events));
		}

		// The response must be exactly the /state_ids set, with no duplicates.
		let mut seen = HashSet::with_capacity(parsed.len());
		for item in &parsed {
			if !seen.insert(item.0.clone()) {
				return Err(StateEventLoadError::InvalidResponse(format!(
					"state response contained duplicate event {}",
					item.0
				)));
			}
		}
		if seen != expected {
			return Err(StateEventLoadError::InvalidResponse(
				"state response did not contain every event listed by /state_ids".to_owned(),
			));
		}

		// Later state events can depend on their auth events being present, so
		// ingest in waves: an event's wave is one past the deepest of its
		// `auth_events` that are also in this response. Events within a wave
		// are independent and run concurrently. A cycle among the auth events
		// leaves nodes that never become ready, and is rejected.
		let mut pending: HashMap<OwnedEventId, usize> = HashMap::with_capacity(parsed.len());
		let mut dependents: HashMap<OwnedEventId, Vec<OwnedEventId>> = HashMap::new();
		for item in &parsed {
			let deps: HashSet<&OwnedEventId> = item
				.4
				.iter()
				.filter(|auth_id| expected.contains(*auth_id))
				.collect();
			pending.insert(item.0.clone(), deps.len());
			for dep in deps {
				dependents
					.entry(dep.clone())
					.or_default()
					.push(item.0.clone());
			}
		}
		let mut by_id: HashMap<OwnedEventId, _> = parsed
			.into_iter()
			.map(|item| (item.0.clone(), item))
			.collect();
		let mut ready: Vec<OwnedEventId> = pending
			.iter()
			.filter(|(_, count)| **count == 0)
			.map(|(id, _)| id.clone())
			.collect();
		ready.sort();
		let mut waves: Vec<Vec<_>> = Vec::new();
		while !ready.is_empty() {
			let mut next = Vec::new();
			let mut wave = Vec::with_capacity(ready.len());
			for event_id in ready {
				for child in dependents.get(&event_id).into_iter().flatten() {
					if let Some(count) = pending.get_mut(child) {
						*count = count.saturating_sub(1);
						if *count == 0 {
							next.push(child.clone());
						}
					}
				}
				if let Some(item) = by_id.remove(&event_id) {
					wave.push(item);
				}
			}
			next.sort();
			waves.push(wave);
			ready = next;
		}
		if !by_id.is_empty() {
			return Err(StateEventLoadError::InvalidResponse(
				"state response contained cyclic auth_events".to_owned(),
			));
		}

		let mut loaded = HashSet::with_capacity(expected.len());
		let mut state = BTreeMap::new();
		for wave in waves {
			let wave_self = self.clone();
			let results: Vec<
				std::result::Result<(ShortStateKey, OwnedEventId), StateEventLoadError>,
			> = wave
				.into_iter()
				.stream()
				.broad_then(move |(event_id, value, kind_str, state_key, _)| {
					let room_version = room_version.clone();
					let remote_server = remote_server.to_owned();
					let room_id = room_id.to_owned();
					let self_copy = wave_self.clone();
					async move {
						if let Err(error) = self_copy
							.services
							.event_handler
							.handle_incoming_pdu(
								&remote_server,
								&room_id,
								&event_id,
								value,
								false,
								&room_version,
							)
							.await
						{
							// Auth rejection and other client-classified validation
							// failures mean this server supplied unusable state. Missing
							// auth events are also a response completeness problem;
							// storage/key-fetch failures remain transient and use the
							// normal retry backoff.
							if error.status_code().is_client_error()
								|| matches!(&error, conduwuit::Error::MissingAuthEvents(_))
							{
								return Err(StateEventLoadError::InvalidResponse(format!(
									"state event {event_id} failed validation: {error}"
								)));
							}
							return Err(StateEventLoadError::Transient(error));
						}

						let rejected = self_copy
							.services
							.pdu_metadata
							.is_event_rejected(&event_id)
							.map_err(StateEventLoadError::Transient)?;
						let pending_auth = self_copy
							.services
							.pdu_metadata
							.is_event_pending_auth_resolution(&event_id)
							.map_err(StateEventLoadError::Transient)?;
						if rejected || pending_auth {
							return Err(StateEventLoadError::InvalidResponse(format!(
								"state event {event_id} was not accepted during ingestion"
							)));
						}

						let shortstatekey = self_copy
							.services
							.short
							.get_or_create_shortstatekey(&kind_str.into(), &state_key)
							.await;

						Ok((shortstatekey, event_id))
					}
				})
				.collect()
				.await;

			// Completion order is arbitrary, so pick the error by priority:
			// an unusable response outranks a transient failure.
			let mut transient = None;
			for item in results {
				match item {
					| Ok((shortstatekey, event_id)) => {
						state.insert(shortstatekey, event_id.clone());
						loaded.insert(event_id);
					},
					| Err(e @ StateEventLoadError::InvalidResponse(_)) => return Err(e),
					| Err(e) => {
						transient.get_or_insert(e);
					},
				}
			}
			if let Some(e) = transient {
				return Err(e);
			}
		}

		if loaded != expected {
			return Err(StateEventLoadError::InvalidResponse(
				"state response did not contain every event listed by /state_ids".to_owned(),
			));
		}

		Ok(state)
	}

	/// Whether any room is still partial.
	async fn any_partial_rooms(&self) -> bool {
		let rooms: Vec<OwnedRoomId> = self
			.db
			.state_partial_rooms
			.stream()
			.ignore_err()
			.map(|(room_id, _): (OwnedRoomId, OwnedServerName)| room_id)
			.collect()
			.await;
		!rooms.is_empty()
	}

	/// Rooms whose state is still partial that `server` is known to be in.
	pub async fn partial_rooms_via(&self, server: &ServerName) -> Vec<OwnedRoomId> {
		let rooms: Vec<OwnedRoomId> = self
			.db
			.state_partial_rooms
			.stream()
			.ignore_err()
			.map(|(room_id, _): (OwnedRoomId, OwnedServerName)| room_id)
			.collect()
			.await;

		let mut via = Vec::new();
		for room_id in rooms {
			if self
				.partial_servers(&room_id)
				.await
				.iter()
				.any(|s| s == server)
			{
				via.push(room_id);
			}
		}
		via
	}

	/// Replace the room's current state with the re-authorised one, fixing
	/// device-list tracking for members whose membership changed. Returns
	/// `false` without installing when the room moved on after `window` was
	/// walked: either its forward extremities changed, or an event the walk did
	/// not cover was accepted in the meantime.
	#[allow(clippy::too_many_arguments)]
	async fn install_current_state(
		&self,
		room_id: &RoomId,
		current: HashMap<ShortStateKey, OwnedEventId>,
		anchor_depth: Option<u64>,
		window: &HashSet<OwnedEventId>,
		expected_extremities: &[OwnedEventId],
	) -> Result<bool> {
		let _state_lock = self.services.state.mutex.lock(room_id).await;
		if self.sorted_extremities(room_id).await != expected_extremities {
			return Ok(false);
		}
		// The marks still held are events accepted since the walk began plus the
		// ones it skipped at or below the anchor. Both kinds may only be cleared
		// when this install covers them: the skipped ones by the anchor's state,
		// the walked ones by `window`. Marks are classified before any is
		// removed, so a refusal leaves everything for the next pass to re-check.
		let marks = self.pending_event_marks(room_id).await?;
		let mut covered_keys = Vec::with_capacity(marks.len());
		for (key, event_id) in &marks {
			if window.contains(event_id) {
				covered_keys.push(key);
				continue;
			}
			match self.services.timeline.get_pdu(event_id) {
				| Ok(pdu) if anchor_depth.is_some_and(|depth| pdu.depth() <= depth) => {
					// Already part of the anchor's state.
					covered_keys.push(key);
				},
				| Ok(_) => {
					debug!(%room_id, %event_id, "event accepted during re-authorisation; not installing");
					return Ok(false);
				},
				| Err(_) => {
					// The walk could not have re-authorised it either; a mark for a PDU
					// we no longer hold must not keep the room partial forever.
					warn!(%room_id, %event_id, "marked partial-state event is missing from the timeline; discarding the mark");
					covered_keys.push(key);
				},
			}
		}
		let root_handle = self.services.state.get_room_state_hamt(room_id).await?;
		let old = self
			.services
			.state_accessor
			.state_full_ids_hamt(room_id, &root_handle)
			.try_collect::<HashMap<ShortStateKey, OwnedEventId>>()
			.await?;

		let (new_root, pending_nodes) = self
			.services
			.event_handler
			.state_map_to_root_handle(room_id, &current)
			.await?;
		self.services
			.state
			.set_room_state_hamt_with_nodes(room_id, &new_root, &pending_nodes)?;
		self.reconcile_tracking(room_id, &old, &current).await;
		for key in covered_keys {
			self.db.state_partial_pending_events.remove(key);
		}
		Ok(true)
	}

	async fn flush_users_who_left_in_window(&self, room_id: &RoomId, left: HashSet<OwnedUserId>) {
		use slipstream::events::room::member::{MembershipState, RoomMemberEventContent};
		for user_id in left {
			self.services.users.invalidate_remote_keys(&user_id);
			// A user who ended up out follows the membership-leave path instead.
			let rejoined = self
				.services
				.state_accessor
				.room_state_get(room_id, &StateEventType::RoomMember, user_id.as_str())
				.await
				.is_ok_and(|pdu| {
					pdu.get_content::<RoomMemberEventContent>()
						.is_ok_and(|c| c.membership == MembershipState::Join)
				});
			if !rejoined {
				continue;
			}
			// This room, and the fully stated rooms they share with us; other
			// partial rooms are left to their own resync.
			let mut rooms = vec![room_id.to_owned()];
			let others: Vec<OwnedRoomId> = self
				.services
				.state_cache
				.rooms_joined(&user_id)
				.collect()
				.await;
			for other in others {
				if other != room_id && matches!(self.partial_server(&other).await, Ok(None)) {
					rooms.push(other);
				}
			}
			for room in rooms {
				self.services
					.users
					.mark_device_key_update_in_room(&user_id, &room)
					.await;
			}
		}
	}
}

/// Sleep for `delay`, then double it up to a minute.
async fn back_off(delay: &mut Duration) {
	async_io::Timer::after(*delay).await;
	*delay = delay.saturating_mul(2).min(Duration::from_secs(60));
}

/// Whether the error came from a server that replied (with an error status or
/// an unparsable body), as opposed to one that could not be reached.
fn server_answered(error: &conduwuit::Error) -> bool {
	matches!(error, conduwuit::Error::Federation(..) | conduwuit::Error::BadServerResponse(_))
}

fn sort_servers(suggested: &ServerName, others: Vec<OwnedServerName>) -> Vec<OwnedServerName> {
	let mut servers = vec![suggested.to_owned()];
	servers.extend(others);
	servers.sort_unstable();
	servers.dedup();

	// Put the suggested server first if it was in the list
	if let Some(pos) = servers.iter().position(|s| s == suggested) {
		servers.remove(pos);
	}
	servers.insert(0, suggested.to_owned());
	servers
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_sort_servers() {
		let suggested = OwnedServerName::parse("primary.com").unwrap();
		let others = vec![
			OwnedServerName::parse("second.com").unwrap(),
			OwnedServerName::parse("third.com").unwrap(),
			OwnedServerName::parse("primary.com").unwrap(),
		];

		let sorted = sort_servers(&suggested, others);

		assert_eq!(sorted.len(), 3);
		assert_eq!(sorted[0], "primary.com");
		assert!(sorted.contains(&OwnedServerName::parse("second.com").unwrap()));
		assert!(sorted.contains(&OwnedServerName::parse("third.com").unwrap()));
	}

	#[test]
	fn test_sort_servers_no_suggested_in_others() {
		let suggested = OwnedServerName::parse("primary.com").unwrap();
		let others = vec![
			OwnedServerName::parse("second.com").unwrap(),
			OwnedServerName::parse("third.com").unwrap(),
		];

		let sorted = sort_servers(&suggested, others);

		assert_eq!(sorted.len(), 3);
		assert_eq!(sorted[0], "primary.com");
	}

	#[test]
	fn test_pdu_parsing() {
		let room_id = RoomId::parse("!room:example.com").unwrap();
		let raw = RawJsonValue::from_json_string(
			r#"{"event_id":"$event:example.com","content":{"body":"test"},"type":"m.room.message","room_id":"!room:example.com","sender":"@user:example.com","origin_server_ts":123456789,"auth_events":[],"prev_events":[],"depth":1,"hashes":{"sha256":"hash"},"signatures":{"example.com":{"ed25519:key":"sig"}}}"#.to_owned(),
		)
		.unwrap();
		let room_version = RoomVersionId::V1;
		let (event_id, value) =
			conduwuit::matrix::event::gen_event_id_canonical_json(&raw, &room_version).unwrap();
		let pdu = PduEvent::from_id_val(&event_id, value, Some(&room_id));
		pdu.unwrap();
	}

	#[test]
	fn test_state_merging() {
		let mut current_state = BTreeMap::new();
		let ssk1 = 1;
		let ssk2 = 2;
		let eid1 = OwnedEventId::parse("$1:example.com").unwrap();
		let eid2 = OwnedEventId::parse("$2:example.com").unwrap();
		let eid3 = OwnedEventId::parse("$3:example.com").unwrap();

		current_state.insert(ssk1, eid1);
		current_state.insert(ssk2, eid2.clone());

		let mut new_state_ids = BTreeMap::new();
		new_state_ids.insert(ssk1, eid3.clone()); // Overwrite ssk1

		current_state.extend(new_state_ids);

		assert_eq!(current_state.len(), 2);
		assert_eq!(current_state.get(&ssk1), Some(&eid3));
		assert_eq!(current_state.get(&ssk2), Some(&eid2));
	}
}

#[cfg(test)]
mod marks_tests;

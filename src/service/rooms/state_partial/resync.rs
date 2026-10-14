use std::{
	collections::{BTreeMap, HashMap, HashSet},
	sync::Arc,
	time::{Duration, Instant},
};

use conduwuit::{Event, PduEvent, Result, debug, info, pdu::PduBuilder, warn};
use conduwuit_core::{
	Err, err,
	utils::stream::{BroadbandExt, IterStream, TryIgnore},
};
use futures::{FutureExt, StreamExt, TryStreamExt};
use slipstream::{
	OwnedEventId, OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, RoomVersionId, ServerName,
	UserId,
	api::federation::event::{get_room_state, get_room_state_ids},
	events::{
		StateEventType,
		room::member::{MembershipState, RoomMemberEventContent},
	},
	sswire::RawJsonValue,
};

use crate::rooms::{short::ShortStateKey, state_partial::Service};

enum StateEventLoadError {
	InvalidResponse(String),
	Transient(conduwuit::Error),
}

/// Prefix of the errors meaning no server gave usable state, the only failures
/// that count towards `partial_state_resync_give_up_after`.
const NO_USABLE_STATE: &str = "No usable state";

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
	/// discovered by the resync. Rows of PDUs that could not be queued are kept
	/// and the first error is returned, so the caller can retry.
	async fn send_pending(&self, room_id: &RoomId) -> Result {
		let mut prefix = room_id.as_bytes().to_vec();
		prefix.push(database::SEP);
		let rows: Vec<(Vec<u8>, Vec<u8>)> = self
			.db
			.state_partial_pending
			.raw_stream_prefix(&prefix)
			.ignore_err()
			.map(|(key, value): (&[u8], &[u8])| (key.to_vec(), value.to_vec()))
			.collect()
			.await;
		let pending: Vec<(conduwuit::matrix::RawPduId, HashSet<OwnedServerName>)> = rows
			.iter()
			.filter_map(|(key, value)| {
				let pdu_id = key.get(prefix.len()..)?;
				let pdu_id = <[u8; 16]>::try_from(pdu_id).ok()?;
				let sent_to = std::str::from_utf8(value)
					.ok()?
					.split(',')
					.filter_map(|server| OwnedServerName::try_from(server).ok())
					.collect();
				Some((conduwuit::matrix::RawPduId(pdu_id), sent_to))
			})
			.collect();
		if pending.is_empty() {
			return Ok(());
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
			let failed_row = key
				.get(prefix.len()..)
				.and_then(|id| <[u8; 16]>::try_from(id).ok())
				.is_some_and(|id| failed.contains(&id));
			if !failed_row {
				self.db.state_partial_pending.remove(key);
			}
		}
		first_error.map_or(Ok(()), Err)
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
		let mut delay = Duration::from_secs(1);
		// When the failures to get usable state from any server began.
		let mut unusable_since: Option<Instant> = None;
		let mut servers_before: Option<HashSet<OwnedServerName>> = None;
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
			match self.clone().resync_room(room_id, &server_name).await {
				| Ok(discovered) => {
					info!("Successfully resynced partial state for room {}", room_id);
					// Keep the partial markers until both replays are queued, so a
					// failure retries them.
					let replayed = match self.send_pending(room_id).await {
						| Ok(()) => self.replay_device_updates(room_id, &servers_before).await,
						| Err(e) => Err(e),
					};
					if let Err(e) = replayed {
						warn!(
							"Failed to replay to newly discovered servers for room {room_id}: \
							 {e}"
						);
						async_io::Timer::after(delay).await;
						delay = delay.saturating_mul(2).min(Duration::from_secs(60));
						continue;
					}
					self.db.state_partial_rooms.remove(room_id.as_bytes());
					self.db.state_partial_events.remove(room_id.as_bytes());
					self.notify_resolved(room_id);
					// Only users whose update was held back while their membership was
					// unknown are reported; other newly found members had no update.
					for user_id in self.take_deferred_device_updates(room_id, &discovered) {
						self.services
							.users
							.mark_device_key_update_in_room(&user_id, room_id)
							.await;
					}
					// With no partial room left, nothing can place the remaining
					// deferred users, so don't keep them around.
					if !self.any_partial_rooms().await {
						self.clear_deferred_device_updates();
					}
					return;
				},
				| Err(e) => {
					warn!("Failed to resync partial state for room {}: {}", room_id, e);
					if self.should_give_up(&e, &mut unusable_since) {
						warn!(%room_id, "No server gave usable state in time; abandoning the join");
						self.abandon_join(room_id).await;
						return;
					}
					async_io::Timer::after(delay).await;
					delay = delay.saturating_mul(2).min(Duration::from_secs(60));
				},
			}
		}
	}

	/// Whether the failures to obtain usable state have lasted the configured
	/// budget. `unusable_since` tracks when they began; any other error resets it.
	fn should_give_up(
		&self,
		error: &conduwuit::Error,
		unusable_since: &mut Option<Instant>,
	) -> bool {
		if !error.to_string().contains(NO_USABLE_STATE) {
			*unusable_since = None;
			return false;
		}

		let budget = self
			.services
			.server
			.config
			.partial_state_resync_give_up_after;
		let since = *unusable_since.get_or_insert_with(Instant::now);
		budget > 0 && since.elapsed() >= Duration::from_secs(budget)
	}

	/// Give up on a partial-state join: the local users leave, and the room's
	/// partial-state tracking is dropped.
	async fn abandon_join(&self, room_id: &RoomId) {
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

		self.db.state_partial_rooms.remove(room_id.as_bytes());
		self.db.state_partial_events.remove(room_id.as_bytes());
		self.services.users.untrack_remote_users_in_room(room_id);
		if let Err(e) = self.send_pending(room_id).await {
			warn!(%room_id, "Could not send pending PDUs for the abandoned join: {e}");
		}
		self.services.state_cache.update_joined_count(room_id).await;
		// Release anything waiting on this room's state.
		self.resolved.notify_waiters();
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

	async fn resync_room(
		self: Arc<Self>,
		room_id: &RoomId,
		server_name: &ServerName,
	) -> Result<Vec<OwnedUserId>> {
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
		let new_state_ids = loop {
			let (response, remote_server, state_ids) = self
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
				| Ok(state) if state.len() == expected => break state,
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
		let anchor_state: HashMap<ShortStateKey, OwnedEventId> =
			new_state_ids.iter().map(|(k, v)| (*k, v.clone())).collect();
		// Events appended while the window is re-authorised (a local leave, say)
		// would be overwritten by the older state, so retry until the room's
		// forward extremities are unchanged when the state is installed.
		let mut reauthed = None;
		let mut gave_up = false;
		for attempt in 1..=REAUTH_ATTEMPTS {
			let Some(anchor) = self.resync_event_id(&room_id).await else {
				break;
			};
			let Some((current, left, extremities)) = self
				.reauth_window(&room_id, &anchor, anchor_state.clone())
				.await
			else {
				break;
			};
			if self
				.install_current_state(&room_id, current, &extremities)
				.await?
			{
				reauthed = Some(left);
				break;
			}
			debug!(%room_id, "room changed during re-authorisation; retrying");
			gave_up = attempt == REAUTH_ATTEMPTS;
		}
		if gave_up {
			return Err!("room {room_id} kept changing during re-authorisation");
		}
		let Some(left) = reauthed else {
			return Err!("could not reconstruct partial-state event window for {room_id}");
		};
		// Only now that the corrected state is installed: these users missed
		// updates while out of the room, so refetch their keys.
		self.flush_users_who_left_in_window(&room_id, left).await;
		let known: HashSet<OwnedUserId> = self
			.services
			.state_cache
			.room_members(&room_id)
			.collect()
			.await;
		self.services
			.state_cache
			.reconcile_membership(&room_id)
			.await;

		// Members discovered by the resync joined silently; tell the device list
		// tracking about the remote ones.
		let discovered: Vec<OwnedUserId> = self
			.services
			.state_cache
			.room_members(&room_id)
			.collect()
			.await;
		// Returned so the caller marks them only after the room is delivered:
		// marking wakes long-polling syncs, which must see the room resolved.
		Ok(discovered
			.into_iter()
			.filter(|u| !known.contains(u) && !self.services.globals.user_is_local(u))
			.collect())
	}

	async fn fetch_remote_state(
		&self,
		room_id: &RoomId,
		server_name: &ServerName,
		excluded: &HashSet<OwnedServerName>,
	) -> Result<(get_room_state::v1::Response, OwnedServerName, Vec<OwnedEventId>)> {
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
			let request = get_room_state::v1::Request {
				room_id: room_id.to_owned(),
				event_id: match self.resync_event_id(room_id).await {
					| Some(event_id) => event_id,
					| None => self
						.services
						.state
						.get_forward_extremities(room_id)
						.next()
						.await
						.expect("room has forward extremities"),
				},
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
				| Ok(response) => return Ok((response, server, ids_response.pdu_ids)),
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
		let mut loaded = HashSet::with_capacity(expected.len());
		let mut state = BTreeMap::new();
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

			if let Err(error) = self
				.services
				.event_handler
				.handle_incoming_pdu(
					remote_server,
					room_id,
					&event_id,
					value.clone(),
					false,
					room_version,
				)
				.await
			{
				// Auth rejection and other client-classified validation failures mean
				// this server supplied unusable state. Missing auth events are also a
				// response completeness problem; storage/key-fetch failures remain
				// transient and use the normal retry backoff.
				if error.status_code().is_client_error()
					|| matches!(&error, conduwuit::Error::MissingAuthEvents(_))
				{
					return Err(StateEventLoadError::InvalidResponse(format!(
						"state event {event_id} failed validation: {error}"
					)));
				}
				return Err(StateEventLoadError::Transient(error));
			}

			let rejected = self
				.services
				.pdu_metadata
				.is_event_rejected(&event_id)
				.map_err(StateEventLoadError::Transient)?;
			let pending_auth = self
				.services
				.pdu_metadata
				.is_event_pending_auth_resolution(&event_id)
				.map_err(StateEventLoadError::Transient)?;
			if rejected || pending_auth {
				return Err(StateEventLoadError::InvalidResponse(format!(
					"state event {event_id} was not accepted during ingestion"
				)));
			}

			let pdu = PduEvent::from_id_val(&event_id, value, Some(room_id)).map_err(|e| {
				StateEventLoadError::InvalidResponse(format!(
					"could not decode state event {event_id}: {e}"
				))
			})?;
			let state_key = pdu.state_key.as_ref().ok_or_else(|| {
				StateEventLoadError::InvalidResponse(format!(
					"state response contained non-state event {event_id}"
				))
			})?;
			let shortstatekey = self
				.services
				.short
				.get_or_create_shortstatekey(&pdu.kind.to_string().into(), state_key)
				.await;
			state.insert(shortstatekey, event_id.clone());
			loaded.insert(event_id);
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
	/// device-list tracking for members whose membership changed.
	async fn install_current_state(
		&self,
		room_id: &RoomId,
		current: HashMap<ShortStateKey, OwnedEventId>,
		expected_extremities: &[OwnedEventId],
	) -> Result<bool> {
		let _state_lock = self.services.state.mutex.lock(room_id).await;
		let mut now: Vec<OwnedEventId> = self
			.services
			.state
			.get_forward_extremities(room_id)
			.collect()
			.await;
		now.sort();
		if now != expected_extremities {
			return Ok(false);
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

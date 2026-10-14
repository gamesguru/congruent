use std::{
	collections::{BTreeMap, HashMap},
	ops::Bound,
	sync::Arc,
	time::Duration,
};

use conduwuit::{PduEvent, Result, debug, info, warn};
use conduwuit_core::{
	err,
	utils::stream::{BroadbandExt, IterStream, TryIgnore},
};
use futures::{StreamExt, TryStreamExt};
use slipstream::{
	OwnedEventId, OwnedRoomId, OwnedServerName, RoomId, RoomVersionId, ServerName,
	api::federation::event::{get_room_state, get_room_state_ids},
	events::StateEventType,
	sswire::RawJsonValue,
};

use crate::rooms::{short::ShortStateKey, state_partial::Service};

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
	/// discovered by the resync.
	async fn send_pending(&self, room_id: &RoomId) {
		let pending = self
			.pending_sends
			.lock()
			.expect("locked")
			.remove(room_id)
			.unwrap_or_default();
		if pending.is_empty() {
			return;
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
			}
		}
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
		loop {
			let Ok(Some(server_name)) = self.partial_server(room_id).await else {
				return;
			};

			debug!("Resyncing partial state for room {} from server {}", room_id, server_name);
			match self.clone().resync_room(room_id, &server_name).await {
				| Ok(discovered) => {
					info!("Successfully resynced partial state for room {}", room_id);
					self.db.state_partial_rooms.remove(room_id.as_bytes());
					self.db.state_partial_events.remove(room_id.as_bytes());
					self.send_pending(room_id).await;
					self.notify_resolved(room_id);
					// Only users whose update was held back while their membership was
					// unknown are reported; other newly found members had no update.
					for user_id in self.take_deferred_device_updates(&discovered) {
						self.services.users.mark_device_key_update(&user_id).await;
					}
					return;
				},
				| Err(e) => {
					warn!("Failed to resync partial state for room {}: {}", room_id, e);
					async_io::Timer::after(delay).await;
					delay = delay.saturating_mul(2).min(Duration::from_secs(60));
				},
			}
		}
	}

	async fn resync_room(
		self: Arc<Self>,
		room_id: &RoomId,
		server_name: &ServerName,
	) -> Result<Vec<slipstream::OwnedUserId>> {
		debug!("Resyncing room {} starting with server {}", room_id, server_name);

		let (response, remote_server) = self.fetch_remote_state(room_id, server_name).await?;

		info!(
			"Fetched {} state events and {} auth events for partial room {} from {}",
			response.pdus.len(),
			response.auth_chain.len(),
			room_id,
			remote_server
		);

		// Need room version to validate payloads (i.e., event_handler computes hashes)
		let create_pdu = self
			.services
			.state_accessor
			.room_state_get(room_id, &StateEventType::RoomCreate, "")
			.await?;
		let room_version = crate::rooms::event_handler::get_room_version_id(&create_pdu)?;

		let room_id = room_id.to_owned();

		// Process auth chain events first.
		// event_handler natively supports pulling outliers via `handle_incoming_pdu`
		// ... which validates signature, performs auth check, and saves to DB.
		self.clone()
			.process_auth_chain(&room_id, &room_version, &remote_server, response.auth_chain)
			.await;

		// Process State events
		let new_state_ids = self
			.clone()
			.process_state_events(&room_id, &room_version, &remote_server, response.pdus)
			.await;

		// Update room state graph (amend the partial state)
		self.update_state_graph(&room_id, new_state_ids.clone())
			.await?;

		// Events received while the state was partial carry snapshots without the
		// omitted members; give them the full state too (Synapse un-partial-states
		// each event), then bring the membership caches up to date.
		self.repair_event_states(&room_id, &new_state_ids).await;
		let known: std::collections::HashSet<slipstream::OwnedUserId> = self
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
		let discovered: Vec<slipstream::OwnedUserId> = self
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
	) -> Result<(get_room_state::v1::Response, OwnedServerName)> {
		// Discovery fallback: collect servers to try
		let others = self
			.services
			.state_cache
			.servers_route_via(room_id)
			.await
			.unwrap_or_default();

		let servers = sort_servers(server_name, others);

		for server in servers {
			if server == self.services.globals.server_name() {
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
			if let Err(e) = self
				.services
				.sending
				.send_federation_request(&server, ids_request)
				.await
			{
				warn!("Failed to fetch state ids from {}: {}", server, e);
				continue;
			}

			match self
				.services
				.sending
				.send_federation_request(&server, request)
				.await
			{
				| Ok(response) => return Ok((response, server)),
				| Err(e) => warn!("Failed to fetch state from {}: {}", server, e),
			}
		}

		Err(err!(Request(NotFound("No server available to resync state"))))
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
	) -> BTreeMap<ShortStateKey, OwnedEventId> {
		use conduwuit::matrix::event::gen_event_id_canonical_json;

		pdus.into_iter()
			.stream()
			.broad_filter_map(move |pdu_json: RawJsonValue| {
				let room_version = room_version.clone();
				let remote_server = remote_server.to_owned();
				let room_id = room_id.to_owned();
				let self_copy = self.clone();
				async move {
					let (calculated_event_id, value) =
						gen_event_id_canonical_json(&pdu_json, &room_version).ok()?;

					// Validate and save
					self_copy
						.services
						.event_handler
						.handle_incoming_pdu(
							&remote_server,
							&room_id,
							&calculated_event_id,
							value.clone(),
							// minimize work, store as outliers
							false,
							&room_version,
						)
						.await
						.ok()?;

					// Collect newly received state events into Map (shortstatekey -> event_id)
					let pdu = PduEvent::from_id_val(&calculated_event_id, value, Some(&room_id))
						.ok()?;
					let state_key = pdu.state_key.as_ref()?;
					let shortstatekey = self_copy
						.services
						.short
						.get_or_create_shortstatekey(&pdu.kind.to_string().into(), state_key)
						.await;

					Some((shortstatekey, calculated_event_id))
				}
			})
			.collect()
			.await
	}

	async fn repair_event_states(
		&self,
		room_id: &RoomId,
		resync: &BTreeMap<ShortStateKey, OwnedEventId>,
	) {
		let from = match self.resync_event_id(room_id).await {
			| Some(event_id) => self.services.timeline.get_pdu_count(&event_id).await.ok(),
			| None => None,
		};
		let bound = from.map_or(Bound::Unbounded, Bound::Included);

		let mut repaired = 0_usize;
		let mut pdus = std::pin::pin!(self.services.timeline.pdus(room_id, bound));
		while let Some(Ok((_, pdu))) = pdus.next().await {
			let shorteventid = self
				.services
				.short
				.get_or_create_shorteventid(&pdu.event_id)
				.await;
			let Ok(root) = self.services.state.get_roothandle(shorteventid).await else {
				continue;
			};
			let Ok(existing) = self
				.services
				.state_accessor
				.state_full_ids_hamt(room_id, &root)
				.try_collect::<HashMap<ShortStateKey, OwnedEventId>>()
				.await
			else {
				continue;
			};

			let mut merged: HashMap<ShortStateKey, OwnedEventId> =
				resync.iter().map(|(k, v)| (*k, v.clone())).collect();
			merged.extend(existing.iter().map(|(k, v)| (*k, v.clone())));
			if merged.len() == existing.len() {
				continue;
			}

			let Ok((new_root, nodes)) = self
				.services
				.event_handler
				.state_map_to_root_handle(room_id, &merged)
				.await
			else {
				continue;
			};
			if self
				.services
				.state
				.set_event_roothandle_with_nodes(room_id, &pdu.event_id, &new_root, &nodes)
				.await
				.is_ok()
			{
				repaired = repaired.saturating_add(1);
			}
		}
		info!("Repaired state snapshots of {repaired} events in {room_id} after resync");
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

	async fn update_state_graph(
		&self,
		room_id: &RoomId,
		new_state_ids: BTreeMap<ShortStateKey, OwnedEventId>,
	) -> Result<()> {
		info!("Updating state graph for partial room {}", room_id);
		let _state_lock = self.services.state.mutex.lock(room_id).await;

		let root_handle = self.services.state.get_room_state_hamt(room_id).await?;
		let mut new_state = self
			.services
			.state_accessor
			.state_full_ids_hamt(room_id, &root_handle)
			.try_collect::<HashMap<ShortStateKey, OwnedEventId>>()
			.await?;

		// The resync state is the room at the join point; whatever we already hold
		// (including events accepted since the join, e.g. a later leave) is newer.
		for (key, event_id) in new_state_ids {
			new_state.entry(key).or_insert(event_id);
		}

		let (new_root, pending_nodes) = self
			.services
			.event_handler
			.state_map_to_root_handle(room_id, &new_state)
			.await?;
		self.services
			.state
			.set_room_state_hamt_with_nodes(room_id, &new_root, &pending_nodes)?;
		Ok(())
	}
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

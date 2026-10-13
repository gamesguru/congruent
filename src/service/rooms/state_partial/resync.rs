use std::{
	collections::{BTreeMap, HashMap},
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
	api::federation::event::get_room_state, events::StateEventType, sswire::RawJsonValue,
};

use crate::rooms::{short::ShortStateKey, state_partial::Service};

impl Service {
	/// A background worker to fully load member lists of partially joined rooms
	pub async fn spawn_resync_worker(self: Arc<Self>) {
		info!("Starting MSC3902 partial state resync worker");

		loop {
			let self_copy = self.clone();
			self.db
				.state_partial_rooms
				.stream()
				.ignore_err()
				.broadn_then(4, move |(room_id, server_name): (OwnedRoomId, OwnedServerName)| {
					self_copy.clone().resync_one_room(room_id, server_name)
				})
				.collect::<Vec<()>>()
				.await;

			async_io::Timer::after(Duration::from_secs(1)).await;
		}
	}

	async fn resync_one_room(
		self: Arc<Self>,
		room_id: OwnedRoomId,
		server_name: OwnedServerName,
	) {
		debug!("Resyncing partial state for room {} from server {}", room_id, server_name);
		if let Err(e) = self.clone().resync_room(&room_id, &server_name).await {
			warn!("Failed to resync partial state for room {}: {}", room_id, e);
			return;
		}

		info!("Successfully resynced partial state for room {}", room_id);
		self.db.state_partial_rooms.remove(room_id.as_bytes());
	}

	async fn resync_room(
		self: Arc<Self>,
		room_id: &RoomId,
		server_name: &ServerName,
	) -> Result<()> {
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
		self.update_state_graph(&room_id, new_state_ids).await?;

		Ok(())
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
				event_id: self
					.services
					.state
					.get_forward_extremities(room_id)
					.next()
					.await
					.expect("room has forward extremities"),
			};

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

		new_state.extend(new_state_ids);

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

use conduwuit::{Err, Result, err, info, matrix::pdu::PduEvent};
use futures::{StreamExt, pin_mut};
use ruma::{
	OwnedEventId, OwnedRoomId, OwnedServerName, OwnedUserId, RoomVersionId,
	api::federation::event::{get_event, get_room_state},
	events::StateEventType,
};

use crate::admin_command;

#[admin_command]
pub(super) async fn federation_request(
	&self,
	server_name: OwnedServerName,
	url_path: String,
	output: Option<String>,
) -> Result {
	use conduwuit::info;

	// This command can write arbitrary files via the `output` parameter,
	// so it must remain restricted to the server console.
	self.bail_restricted()?;

	// Parse the URL path to determine which federation endpoint to call
	// Currently supports: /_matrix/federation/v1/state/{roomId}
	if let Some(rest) = url_path.strip_prefix("/_matrix/federation/v1/state/") {
		let (room_id_str, event_id_str) = if let Some((room_part, query)) = rest.split_once('?') {
			let event_id = query.strip_prefix("event_id=").unwrap_or(query);
			(room_part, Some(event_id))
		} else {
			(rest, None)
		};

		let room_id: OwnedRoomId = room_id_str
			.parse()
			.map_err(|e| err!("Invalid room ID: {e:?}"))?;

		let event_id: OwnedEventId = event_id_str
			.ok_or_else(|| err!("event_id query parameter is required"))?
			.parse()
			.map_err(|e| err!("Invalid event ID: {e:?}"))?;

		info!("Fetching federation state for {room_id} at {event_id} from {server_name}");

		let response = self
			.services
			.sending
			.send_federation_request(&server_name, get_room_state::v1::Request {
				room_id: room_id.clone(),
				event_id: event_id.clone(),
			})
			.await?;

		let dump = serde_json::json!({
			"room_id": room_id,
			"server_name": server_name,
			"event_id": event_id.to_string(),
			"pdus": response.pdus,
			"auth_chain": response.auth_chain,
		});

		let pretty = serde_json::to_string_pretty(&dump).unwrap_or_default();

		if let Some(ref path) = output {
			std::fs::write(path, &pretty)
				.map_err(|e| err!("Failed to write output file: {e:?}"))?;
			self.write_str(&format!(
				"Saved {} state PDUs and {} auth chain events to {path}",
				response.pdus.len(),
				response.auth_chain.len()
			))
			.await
		} else {
			let truncated = pretty.get(..4096).unwrap_or(&pretty);
			self.write_str(&format!(
				"Received {} state PDUs and {} auth chain events\n\n{}",
				response.pdus.len(),
				response.auth_chain.len(),
				truncated
			))
			.await
		}
	} else if let Some(event_id_str) = url_path.strip_prefix("/_matrix/federation/v1/event/") {
		let event_id: OwnedEventId = event_id_str
			.parse()
			.map_err(|e| err!("Invalid event ID: {e:?}"))?;

		info!("Fetching federation event {event_id} from {server_name}");

		let response = self
			.services
			.sending
			.send_federation_request(&server_name, get_event::v1::Request {
				event_id: event_id.clone(),
				include_unredacted_content: None,
			})
			.await?;

		let dump = serde_json::json!({
			"server_name": server_name,
			"event_id": event_id.to_string(),
			"pdu": response.pdu,
		});

		let pretty = serde_json::to_string_pretty(&dump).unwrap_or_default();

		if let Some(ref path) = output {
			std::fs::write(path, &pretty)
				.map_err(|e| err!("Failed to write output file: {e:?}"))?;
			self.write_str(&format!("Saved event PDU to {path}")).await
		} else {
			let truncated = pretty.get(..4096).unwrap_or(&pretty);
			self.write_str(&format!("Received event PDU\n\n{truncated}"))
				.await
		}
	} else {
		Err!(
			"Unsupported federation endpoint: {url_path}\n\nSupported endpoints:\n  \
			 /_matrix/federation/v1/state/!room:server?event_id=$event\n  \
			 /_matrix/federation/v1/event/$event"
		)
	}
}

#[admin_command]
pub(super) async fn fetch_pdu(
	&self,
	room_id: OwnedRoomId,
	event_id: OwnedEventId,
	server: OwnedServerName,
	skip_auth: bool,
) -> Result {
	self.bail_restricted()?;

	if !self.services.server.config.allow_federation {
		return Err!("Federation is disabled on this homeserver.");
	}

	if server == self.services.globals.server_name() {
		return Err!(
			"Not allowed to send federation requests to ourselves. Please use `get-pdu` for \
			 fetching local PDUs.",
		);
	}

	let mut room_version = self
		.services
		.rooms
		.state
		.get_room_version(&room_id)
		.await
		.ok();

	info!("fetch_pdu: sending federation request to {server} for event...");
	let response = self
		.services
		.sending
		.send_federation_request(&server, get_event::v1::Request::new(event_id, None))
		.await?;

	info!("fetch_pdu: received response from {server}, parsing PDU...");

	// If the room's state is completely missing and we happen
	// to be fetching the `m.room.create` event to rescue it, we MUST extract the
	// real version from the PDU itself. Otherwise, canonicalization fails.
	if let Ok(val) = serde_json::from_str::<serde_json::Value>(response.pdu.get()) {
		if val.get("type").and_then(|t| t.as_str()) == Some("m.room.create") {
			if let Some(v_str) = val
				.get("content")
				.and_then(|c| c.get("room_version"))
				.and_then(|v| v.as_str())
			{
				if let Ok(v) = RoomVersionId::try_from(v_str) {
					room_version = Some(v);
				}
			} else {
				// Matrix spec: If room_version is omitted in m.room.create, it defaults to V1.
				room_version = Some(RoomVersionId::V1);
			}
		}
	}

	let room_version = room_version.ok_or_else(|| {
		err!(
			"Local room version is unknown and the fetched PDU is not an m.room.create event. \
			 You must rescue the m.room.create event first."
		)
	})?;

	info!("fetch_pdu: room version is {room_version}, validating signatures...");

	let (event_id, value) = if skip_auth {
		let (eid, mut val) =
			conduwuit::matrix::event::gen_event_id_canonical_json(&response.pdu, &room_version)?;
		val.insert("event_id".into(), ruma::CanonicalJsonValue::String(eid.as_str().into()));
		info!("fetch_pdu: skip_auth mode, generated event_id={eid}");
		(eid, val)
	} else {
		let result = self
			.services
			.server_keys
			.validate_and_add_event_id(&response.pdu, &room_version)
			.await?;
		info!("fetch_pdu: validated event_id={}", result.0);
		result
	};

	let pdu = PduEvent::from_id_val(&event_id, value.clone(), Some(room_id.as_ref()))
		.map_err(|e| err!(Database("Invalid PDU: {e:?}")))?;

	info!(
		"fetch_pdu: parsed PDU type={} state_key={:?} sender={} depth={}",
		pdu.kind, pdu.state_key, pdu.sender, pdu.depth
	);

	if skip_auth {
		// Direct insert into timeline, bypassing all auth checks.
		info!("fetch_pdu: force-inserting PDU (skip_auth) into timeline...");
		let msg = match self
			.services
			.rooms
			.timeline
			.force_insert_pdu(&room_id, &event_id, &pdu, &value, true)
			.await
		{
			| Ok(pdu_id) => {
				format!("Force-inserted PDU {event_id} into timeline (skipped auth): {pdu_id:?}")
			},
			| Err(e) => format!("PDU {event_id}: {e}"),
		};
		return self.write_str(&msg).await;
	}

	let create_event = if pdu.kind == ruma::events::TimelineEventType::RoomCreate {
		// We're fetching the create event itself -- use it as its own create_event
		// to avoid the catch-22 of needing the create event to insert the create event.
		pdu.clone()
	} else {
		self.services
			.rooms
			.state_accessor
			.room_state_get(&room_id, &StateEventType::RoomCreate, "")
			.await?
	};

	info!("fetch_pdu: upgrading outlier to timeline PDU (full auth check)...");
	let result = Box::pin(
		self.services
			.rooms
			.event_handler
			.upgrade_outlier_to_timeline_pdu(
				pdu,
				value,
				&create_event,
				&server,
				&room_id,
				false,
				true,
				false,
			),
	)
	.await?;

	match result {
		| Some(ref id) => info!("fetch_pdu: success — promoted to timeline: {id:?}"),
		| None => info!("fetch_pdu: PDU was already present or promoted (no-op)"),
	}

	match result {
		| Some(id) => write!(self, "Successfully fetched and rescued PDU: {id:?}"),
		| None => write!(self, "PDU was already present or promoted successfully."),
	}
	.await
}

#[admin_command]
pub(super) async fn resend_receipts(
	&self,
	room_id: OwnedRoomId,
	server: Option<OwnedServerName>,
) -> Result {
	use std::collections::BTreeMap;

	use ruma::{
		OwnedEventId,
		api::federation::transactions::edu::{Edu, ReceiptContent, ReceiptData, ReceiptMap},
		events::{AnySyncEphemeralRoomEvent, receipt::ReceiptType},
	};

	// Collect latest receipt per local user in this room
	let mut latest_receipts: BTreeMap<
		OwnedUserId,
		(OwnedEventId, ruma::events::receipt::Receipt),
	> = BTreeMap::new();

	let receipts = self
		.services
		.rooms
		.read_receipt
		.readreceipts_since(&room_id, None);

	pin_mut!(receipts);
	while let Some((user_id, _count, raw_receipt)) = receipts.next().await {
		// Only resend our local users' receipts
		if !self.services.globals.server_is_ours(user_id.server_name()) {
			continue;
		}

		let Ok(event) =
			serde_json::from_str::<AnySyncEphemeralRoomEvent>(raw_receipt.json().get())
		else {
			continue;
		};

		let AnySyncEphemeralRoomEvent::Receipt(r) = event else {
			continue;
		};

		let Some((event_id, mut receipt_types)) = r.content.0.into_iter().next() else {
			continue;
		};

		let Some(users) = receipt_types.remove(&ReceiptType::Read) else {
			continue;
		};

		let Some(receipt) = users.into_iter().next().map(|(_, r)| r) else {
			continue;
		};

		// Keep only the latest per user (stream is ordered by count ascending)
		latest_receipts.insert(user_id.clone(), (event_id, receipt));
	}

	if latest_receipts.is_empty() {
		return self
			.write_str("No local user receipts found for this room.")
			.await;
	}

	// Build the receipt EDU
	let mut read = BTreeMap::new();
	for (user_id, (event_id, receipt)) in &latest_receipts {
		read.insert(user_id.clone(), ReceiptData {
			data: receipt.clone(),
			event_ids: vec![event_id.clone()],
		});
	}

	let receipt_map = ReceiptMap { read };
	let receipts_content = BTreeMap::from([(room_id.clone(), receipt_map)]);
	let edu = Edu::Receipt(ReceiptContent { receipts: receipts_content });

	let mut buf = conduwuit_service::sending::EduBuf::new();
	serde_json::to_writer(&mut buf, &edu)
		.map_err(|e| err!("Failed to serialize receipt EDU: {e}"))?;

	// Send to specific server or all participating servers
	if let Some(ref target_server) = server {
		self.services.sending.send_edu_server(target_server, buf)?;
		self.write_str(&format!(
			"Resent {} receipt(s) for room {} to server {}.",
			latest_receipts.len(),
			room_id,
			target_server
		))
		.await?;
	} else {
		self.services.sending.send_edu_room(&room_id, buf).await?;
		self.write_str(&format!(
			"Resent {} receipt(s) for room {} to all participating servers.",
			latest_receipts.len(),
			room_id
		))
		.await?;
	}

	Ok(())
}

#[admin_command]
pub(super) async fn fetch_state_ids(
	&self,
	room_id: OwnedRoomId,
	server: OwnedServerName,
	event_id: OwnedEventId,
) -> Result {
	use ruma::api::federation::event::{get_event, get_room_state_ids};
	self.bail_restricted()?;

	let room_version = self
		.services
		.rooms
		.state
		.get_room_version(&room_id)
		.await
		.unwrap_or(RoomVersionId::V1);

	self.write_str(&format!("Fetching state_ids for {room_id} at {event_id} from {server}..."))
		.await?;

	let response = self
		.services
		.sending
		.send_federation_request(&server, get_room_state_ids::v1::Request {
			room_id: room_id.clone(),
			event_id: event_id.clone(),
		})
		.await?;

	let mut missing_events = Vec::new();
	let mut total_events: usize = 0;

	for id in response
		.auth_chain_ids
		.iter()
		.chain(response.pdu_ids.iter())
	{
		total_events = total_events.saturating_add(1);
		let in_timeline = self.services.rooms.timeline.get_pdu(id).await.is_ok();
		let in_outlier = self
			.services
			.rooms
			.outlier
			.get_outlier_pdu_json(id)
			.await
			.is_ok();

		if !in_timeline && !in_outlier {
			missing_events.push(id.clone());
		}
	}

	self.write_str(&format!(
		"Found {} total events in state_ids response. {} are missing locally. Starting \
		 incremental fetch...",
		total_events,
		missing_events.len()
	))
	.await?;

	let mut fetched: usize = 0;
	let mut failed: usize = 0;

	for missing_id in missing_events {
		match self
			.services
			.sending
			.send_federation_request(
				&server,
				get_event::v1::Request::new(missing_id.clone(), None),
			)
			.await
		{
			| Ok(ev_resp) => {
				if let Ok((eid, mut val)) = conduwuit::matrix::event::gen_event_id_canonical_json(
					&ev_resp.pdu,
					&room_version,
				) {
					val.insert(
						"event_id".into(),
						ruma::CanonicalJsonValue::String(eid.as_str().into()),
					);
					self.services
						.rooms
						.outlier
						.add_pdu_outlier(&eid, &val, Some(&room_id))
						.await;
					fetched = fetched.saturating_add(1);
				} else {
					failed = failed.saturating_add(1);
				}
			},
			| Err(_) => {
				failed = failed.saturating_add(1);
			},
		}

		// Print progress every 100 events to avoid silencing the command for hours
		if (fetched.saturating_add(failed)).is_multiple_of(100) {
			info!(
				"fetch_state_ids progress: {}/{} fetched",
				fetched.saturating_add(failed),
				total_events
			);
		}
	}

	self.write_str(&format!(
		"Done fetching state_ids. Fetched {fetched} missing events, {failed} failed."
	))
	.await
}

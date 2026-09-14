use std::{
	collections::{HashMap, HashSet},
	fmt::Write,
	iter::once,
	time::{Instant, SystemTime},
};

use conduwuit::{
	Err, Result, err, info,
	matrix::{
		Event,
		pdu::{PduEvent, PduId, RawPduId},
	},
	trace, utils,
	utils::{
		stream::{IterStream, ReadyExt},
		string::EMPTY,
	},
	warn,
};
use futures::{FutureExt, StreamExt, TryStreamExt};
use lettre::message::Mailbox;
use ruma::{
	CanonicalJsonObject, EventId, OwnedEventId, OwnedRoomId, OwnedRoomOrAliasId, OwnedServerName,
	RoomVersionId,
	api::federation::event::{get_event, get_room_state},
};
use service::rooms::{
	short::{ShortEventId, ShortRoomId},
	state_compressor::HashSetCompressStateEvent,
};
use tracing_subscriber::EnvFilter;

use crate::admin_command;

#[admin_command]
pub(super) async fn echo(&self, message: Vec<String>) -> Result {
	let message = message.join(" ");
	self.write_str(&message).await
}

#[admin_command]
pub(super) async fn get_auth_chain(&self, event_id: OwnedEventId) -> Result {
	let Ok(event) = self.services.rooms.timeline.get_pdu(&event_id).await else {
		return Err!("Event not found.");
	};

	let room_id = event
		.room_id_or_hash()
		.ok_or_else(|| err!(Database("Event has no room_id")))?;

	let start = Instant::now();
	let count = self
		.services
		.rooms
		.auth_chain
		.event_ids_iter(&room_id, once(event_id.as_ref()))
		.ready_filter_map(Result::ok)
		.count()
		.await;

	let elapsed = start.elapsed();
	let out = format!("Loaded auth chain with length {count} in {elapsed:?}");

	self.write_str(&out).await
}

#[admin_command]
pub(super) async fn parse_pdu(&self) -> Result {
	if self.body.len() < 2
		|| !self.body[0].trim().starts_with("```")
		|| self.body.last().unwrap_or(&EMPTY).trim() != "```"
	{
		return Err!("Expected code block in command body. Add --help for details.");
	}

	let string = self.body[1..self.body.len().saturating_sub(1)].join("\n");
	match serde_json::from_str(&string) {
		| Err(e) => return Err!("Invalid json in command body: {e}"),
		| Ok(value) => match ruma::signatures::reference_hash(&value, &RoomVersionId::V6) {
			| Err(e) => return Err!("Could not parse PDU JSON: {e:?}"),
			| Ok(hash) => {
				let event_id = OwnedEventId::parse(format!("${hash}"));
				match serde_json::from_value::<PduEvent>(serde_json::to_value(value)?) {
					| Err(e) => return Err!("EventId: {event_id:?}\nCould not parse event: {e}"),
					| Ok(pdu) => write!(self, "EventId: {event_id:?}\n{pdu:#?}"),
				}
			},
		},
	}
	.await
}

#[admin_command]
pub(super) async fn get_pdu(&self, event_id: OwnedEventId, verbose: bool) -> Result {
	let in_timeline = self
		.services
		.rooms
		.timeline
		.get_pdu_id(&event_id)
		.await
		.is_ok();
	let in_outlier = self
		.services
		.rooms
		.timeline
		.outlier_pdu_exists(&event_id)
		.await
		.is_ok();
	let in_db = self.services.rooms.timeline.pdu_exists(&event_id).await;

	if !in_db {
		return Err!("PDU not found locally.");
	}

	let pdu_json = if in_timeline {
		self.services.rooms.timeline.get_pdu_json(&event_id).await?
	} else if in_outlier || in_db {
		self.services
			.rooms
			.outlier
			.get_outlier_pdu_json(&event_id)
			.await?
	} else {
		return Err!("PDU not found locally.");
	};
	let text = serde_json::to_string_pretty(&pdu_json)?;

	let mut status = String::new();
	if in_timeline && in_outlier {
		status.push_str("STUCK STATE (Both Timeline and Outlier tables)");
	} else if in_timeline {
		status.push_str("Timeline PDU");
	} else if in_outlier {
		status.push_str("Outlier PDU");
	} else {
		status.push_str("Legacy Outlier PDU (No metadata found)");
	}

	let soft_failed = self
		.services
		.rooms
		.pdu_metadata
		.is_event_soft_failed(&event_id)
		.await;
	if soft_failed {
		status.push_str(" [SOFT-FAIL]");
	}

	let rejected = self
		.services
		.rooms
		.pdu_metadata
		.is_event_rejected(&event_id)
		.await;
	if rejected {
		status.push_str(" [REJECTED]");
	}

	let mut out = format!("Status: {status}\n\n```json\n{text}\n```");

	if verbose {
		match self
			.services
			.rooms
			.timeline
			.get_event_metadata(&event_id)
			.await
		{
			| Ok(meta) => {
				use std::fmt::Write;
				writeln!(out)?;
				writeln!(out, "## Metadata")?;
				writeln!(out, "- **pdu_count**:        {:?}", meta.pdu_count)?;
				writeln!(out, "- **origin_server_ts**: {}", meta.origin_server_ts)?;
				writeln!(out, "- **depth**:            {}", meta.depth)?;
				writeln!(out, "- **short_room_id**:    {}", meta.short_room_id)?;
				writeln!(out, "- **is_outlier**:       {}", meta.is_outlier)?;
				writeln!(out, "- **soft_failed**:      {soft_failed}")?;
				writeln!(out, "- **rejected**:         {rejected}")?;
				writeln!(
					out,
					"- **redacted_by**:      {}",
					meta.redacted_by
						.as_ref()
						.map_or_else(|| "None".to_owned(), |id| format!("{id}"))
				)?;
				writeln!(
					out,
					"- **short_state_hash**: {}",
					meta.short_state_hash
						.map_or_else(|| "None".to_owned(), |h| h.to_string())
				)?;
			},
			| Err(e) => {
				use std::fmt::Write;
				writeln!(out, "\n⚠ No metadata found: {e}")?;
			},
		}
	}

	self.write_str(&out).await
}

#[admin_command]
pub(super) async fn get_short_pdu(
	&self,
	shortroomid: ShortRoomId,
	shorteventid: ShortEventId,
) -> Result {
	let pdu_id: RawPduId = PduId {
		shortroomid,
		shorteventid: shorteventid.into(),
	}
	.into();

	let pdu_json = self
		.services
		.rooms
		.timeline
		.get_pdu_json_from_id(&pdu_id)
		.await;

	match pdu_json {
		| Err(_) => return Err!("PDU not found locally."),
		| Ok(json) => {
			let json_text = serde_json::to_string_pretty(&json)?;
			write!(self, "```json\n{json_text}\n```")
		},
	}
	.await
}

#[admin_command]
pub(super) async fn get_remote_pdu_list(&self, server: OwnedServerName, force: bool) -> Result {
	if !self.services.server.config.allow_federation {
		return Err!("Federation is disabled on this homeserver.",);
	}

	if server == self.services.globals.server_name() {
		return Err!(
			"Not allowed to send federation requests to ourselves. Please use `get-pdu` for \
			 fetching local PDUs from the database.",
		);
	}

	if self.body.len() < 2
		|| !self.body[0].trim().starts_with("```")
		|| self.body.last().unwrap_or(&EMPTY).trim() != "```"
	{
		return Err!("Expected code block in command body. Add --help for details.",);
	}

	let list = self
		.body
		.iter()
		.collect::<Vec<_>>()
		.drain(1..self.body.len().saturating_sub(1))
		.filter_map(|pdu| EventId::parse(pdu).ok())
		.collect::<Vec<_>>();

	let mut failed_count: usize = 0;
	let mut success_count: usize = 0;

	for event_id in list {
		if force {
			match self
				.get_remote_pdu(event_id.to_owned(), server.clone())
				.await
			{
				| Err(e) => {
					failed_count = failed_count.saturating_add(1);
					self.services
						.admin
						.send_text(&format!("Failed to get remote PDU, ignoring error: {e}"))
						.await;

					warn!("Failed to get remote PDU, ignoring error: {e}");
				},
				| _ => {
					success_count = success_count.saturating_add(1);
				},
			}
		} else {
			self.get_remote_pdu(event_id.to_owned(), server.clone())
				.await?;
			success_count = success_count.saturating_add(1);
		}
	}

	let out =
		format!("Fetched {success_count} remote PDUs successfully with {failed_count} failures");

	self.write_str(&out).await
}

#[admin_command]
pub(super) async fn get_remote_pdu(
	&self,
	event_id: OwnedEventId,
	server: OwnedServerName,
) -> Result {
	if !self.services.server.config.allow_federation {
		return Err!("Federation is disabled on this homeserver.");
	}

	if server == self.services.globals.server_name() {
		return Err!(
			"Not allowed to send federation requests to ourselves. Please use `get-pdu` for \
			 fetching local PDUs.",
		);
	}

	match self
		.services
		.sending
		.send_federation_request(&server, get_event::v1::Request::new(event_id.clone(), None))
		.await
	{
		| Err(e) => {
			return Err!(
				"Remote server did not have PDU or failed sending request to remote server: {e}"
			);
		},
		| Ok(response) => {
			let json: CanonicalJsonObject =
				serde_json::from_str(response.pdu.get()).map_err(|e| {
					warn!(
						"Requested event ID {event_id} from server but failed to convert from \
						 RawValue to CanonicalJsonObject (malformed event/response?): {e}"
					);
					err!(Request(Unknown(
						"Received response from server but failed to parse PDU"
					)))
				})?;

			trace!("Attempting to parse PDU: {:?}", &response.pdu);
			let _parsed_pdu = {
				let parsed_result = self
					.services
					.rooms
					.event_handler
					.parse_incoming_pdu(&response.pdu)
					.boxed()
					.await;

				let (event_id, value, room_id) = match parsed_result {
					| Ok(t) => t,
					| Err(e) => {
						warn!("Failed to parse PDU: {e}");
						info!("Full PDU: {:?}", &response.pdu);
						return Err!("Failed to parse PDU remote server {server} sent us: {e}");
					},
				};

				vec![(event_id, value, room_id)]
			};

			let text = serde_json::to_string_pretty(&json)?;
			let msg = "Got PDU from specified server:";
			write!(self, "{msg}. Event body:\n```json\n{text}\n```")
		},
	}
	.await
}

#[admin_command]
pub(super) async fn get_room_state(&self, room: OwnedRoomOrAliasId) -> Result {
	use std::fmt::Write;

	self.bail_restricted()?;

	let room_id = self.services.rooms.alias.resolve(&room).await?;
	let room_state: Vec<_> = self
		.services
		.rooms
		.state_accessor
		.room_state_full_pdus(&room_id)
		.try_collect()
		.await?;

	if room_state.is_empty() {
		return Err!("Unable to find room state in our database (vector is empty)",);
	}

	let mut out = format!("{} state events in {}:\n", room_state.len(), room_id);
	for pdu in &room_state {
		writeln!(
			out,
			"  {} {} {} {}",
			pdu.kind(),
			pdu.state_key().unwrap_or(""),
			pdu.sender(),
			pdu.event_id(),
		)?;
	}

	self.write_str(&out).await
}

#[admin_command]
pub(super) async fn ping(&self, server: OwnedServerName) -> Result {
	if server == self.services.globals.server_name() {
		return Err!("Not allowed to send federation requests to ourselves.");
	}

	let timer = tokio::time::Instant::now();

	match self
		.services
		.sending
		.send_federation_request(
			&server,
			ruma::api::federation::discovery::get_server_version::v1::Request {},
		)
		.await
	{
		| Err(e) => {
			return Err!("Failed sending federation request to specified server:\n\n{e}");
		},
		| Ok(response) => {
			let ping_time = timer.elapsed();
			let json_text_res = serde_json::to_string_pretty(&response.server);

			let out = if let Ok(json) = json_text_res {
				format!("Got response which took {ping_time:?} time:\n```json\n{json}\n```")
			} else {
				format!("Got non-JSON response which took {ping_time:?} time:\n{response:?}")
			};

			write!(self, "{out}")
		},
	}
	.await
}

#[admin_command]
pub(super) async fn force_device_list_updates(&self) -> Result {
	// Force E2EE device list updates for all users
	self.services
		.users
		.stream()
		.for_each(|user_id| self.services.users.mark_device_key_update(user_id))
		.await;

	write!(self, "Marked all devices for all users as having new keys to update").await
}

#[admin_command]
pub(super) async fn change_log_level(&self, filter: Option<String>, reset: bool) -> Result {
	let handles = &["console"];

	if reset {
		let old_filter_layer = match EnvFilter::try_new(&self.services.server.config.log) {
			| Ok(s) => s,
			| Err(e) => return Err!("Log level from config appears to be invalid now: {e}"),
		};

		match self
			.services
			.server
			.log
			.reload
			.reload(&old_filter_layer, Some(handles))
		{
			| Err(e) => {
				return Err!("Failed to modify and reload the global tracing log level: {e}");
			},
			| Ok(()) => {
				let value = &self.services.server.config.log;
				let out = format!("Successfully changed log level back to config value {value}");
				return self.write_str(&out).await;
			},
		}
	}

	if let Some(filter) = filter {
		let new_filter_layer = match EnvFilter::try_new(filter) {
			| Ok(s) => s,
			| Err(e) => return Err!("Invalid log level filter specified: {e}"),
		};

		match self
			.services
			.server
			.log
			.reload
			.reload(&new_filter_layer, Some(handles))
		{
			| Ok(()) => {
				return self.write_str("Successfully changed log level").await;
			},
			| Err(e) => {
				return Err!("Failed to modify and reload the global tracing log level: {e}");
			},
		}
	}

	Err!("No log level was specified.")
}

#[admin_command]
pub(super) async fn verify_json(&self) -> Result {
	if self.body.len() < 2
		|| !self.body[0].trim().starts_with("```")
		|| self.body.last().unwrap_or(&EMPTY).trim() != "```"
	{
		return Err!("Expected code block in command body. Add --help for details.");
	}

	let string = self.body[1..self.body.len().checked_sub(1).unwrap()].join("\n");
	match serde_json::from_str::<CanonicalJsonObject>(&string) {
		| Err(e) => return Err!("Invalid json: {e}"),
		| Ok(value) => match self.services.server_keys.verify_json(&value, None).await {
			| Err(e) => return Err!("Signature verification failed: {e}"),
			| Ok(()) => write!(self, "Signature correct"),
		},
	}
	.await
}

#[admin_command]
pub(super) async fn verify_pdu(&self, event_id: OwnedEventId) -> Result {
	use std::fmt::Write;

	use conduwuit::matrix::{Event, EventTypeExt, RoomVersion};
	use ruma::signatures::Verified;

	let pdu = self.services.rooms.timeline.get_pdu(&event_id).await?;
	let mut event = self.services.rooms.timeline.get_pdu_json(&event_id).await?;
	utils::pdu_json_canonical_strip(&mut event);

	// Status flags
	let (is_rejected, is_soft_failed) = tokio::join!(
		self.services
			.rooms
			.pdu_metadata
			.is_event_rejected(&event_id),
		self.services
			.rooms
			.pdu_metadata
			.is_event_soft_failed(&event_id),
	);
	let is_outlier = self
		.services
		.rooms
		.timeline
		.outlier_pdu_exists(&event_id)
		.await
		.is_ok();
	let is_timeline = self
		.services
		.rooms
		.timeline
		.get_pdu_id(&event_id)
		.await
		.is_ok();

	let room_version_id = if let Some(room_id) = pdu.room_id_or_hash() {
		self.services
			.rooms
			.state
			.get_room_version(&room_id)
			.await
			.unwrap_or(RoomVersionId::V12)
	} else {
		RoomVersionId::V12
	};
	let _room_version = RoomVersion::new(&room_version_id).expect("room version is supported");

	// Signature verification
	let sig_msg = match self
		.services
		.server_keys
		.verify_event(&event, Some(&room_version_id))
		.await
	{
		| Err(e) => format!("SIGNATURE FAILED: {e:?}"),
		| Ok(Verified::Signatures) => "signatures OK, content hash FAILED (redaction)".to_owned(),
		| Ok(Verified::All) => "signatures and hashes OK".to_owned(),
	};

	// Auth check against current room state
	let auth_msg = if let Some(ref room_id) = pdu.room_id_or_hash() {
		{
			let create_event = self
				.services
				.rooms
				.state_accessor
				.room_state_get(room_id, &ruma::events::StateEventType::RoomCreate, "")
				.await
				.ok();

			// Gather auth events from the PDU's own declared auth_events
			let mut auth_events = HashMap::new();
			for auth_event_id in pdu.auth_events() {
				if let Ok(auth_pdu) = self.services.rooms.timeline.get_pdu(auth_event_id).await {
					let key = auth_pdu
						.kind()
						.with_state_key(auth_pdu.state_key().unwrap_or(""));
					auth_events.insert(key, auth_pdu);
				}
			}

			let state_provider =
				conduwuit_service::rooms::auth_adapter::PduStateProvider::from_ruma_map(
					&auth_events,
				)
				.with_create_event(create_event.as_ref());
			if conduwuit_service::rooms::auth_adapter::rezzy_auth_check(
				&pdu,
				&state_provider,
				conduwuit_service::rooms::auth_adapter::to_state_res_version(&room_version_id),
			) {
				"PASS".to_owned()
			} else {
				"FAIL (not authorized)".to_owned()
			}
		}
	} else {
		"SKIP (no room_id)".to_owned()
	};

	let mut out = String::new();
	writeln!(out, "Event: {event_id}")?;
	if let Some(room_id) = pdu.room_id_or_hash() {
		writeln!(out, "Room: {room_id}")?;
	}
	writeln!(out, "Type: {}", pdu.kind())?;
	if pdu.kind() == &ruma::events::TimelineEventType::RoomMember {
		if let Ok(content) = serde_json::from_str::<serde_json::Value>(pdu.content().get()) {
			if let Some(membership) = content.get("membership").and_then(|m| m.as_str()) {
				writeln!(out, "Membership: {membership}")?;
			}
		}
	}
	writeln!(out, "State key: {}", pdu.state_key().unwrap_or("<none (not a state event)>"))?;
	writeln!(out, "Sender: {}", pdu.sender())?;
	writeln!(out, "Room Version: {room_version_id}")?;
	writeln!(out, "Verify: {sig_msg}")?;
	writeln!(out, "Auth check: {auth_msg}")?;
	writeln!(
		out,
		"Status: timeline={is_timeline} outlier={is_outlier} rejected={is_rejected} \
		 soft_failed={is_soft_failed}"
	)?;
	if is_rejected {
		let reason = self
			.services
			.rooms
			.pdu_metadata
			.get_rejection_reason(&event_id)
			.await
			.unwrap_or_default();
		if reason.is_empty() {
			writeln!(out, "Rejection reason: <none stored (legacy)>")?;
		} else {
			writeln!(out, "Rejection reason: {reason}")?;
		}
	}
	if is_soft_failed {
		let reason = self
			.services
			.rooms
			.pdu_metadata
			.get_soft_fail_reason(&event_id)
			.await
			.unwrap_or_default();
		if reason.is_empty() {
			writeln!(out, "Soft-fail reason: <none stored (legacy)>")?;
		} else {
			writeln!(out, "Soft-fail reason: {reason}")?;
		}
	}

	self.write_str(&out).await
}

#[admin_command]
#[tracing::instrument(skip(self), level = "info")]
pub(super) async fn first_pdu_in_room(&self, room_id: OwnedRoomId) -> Result {
	self.bail_restricted()?;

	if !self
		.services
		.rooms
		.state_cache
		.server_is_participant(&self.services.server.name, &room_id)
		.await
	{
		return Err!("We are not participating in the room / we don't know about the room ID.",);
	}

	let first_pdu = self
		.services
		.rooms
		.timeline
		.first_pdu_in_room(&room_id)
		.await
		.map_err(|_| err!(Database("Failed to find the first PDU in database")))?;

	let out = format!("{first_pdu:?}");
	self.write_str(&out).await
}

#[admin_command]
#[tracing::instrument(skip(self), level = "info")]
pub(super) async fn latest_pdu_in_room(&self, room_id: OwnedRoomId) -> Result {
	self.bail_restricted()?;

	if !self
		.services
		.rooms
		.state_cache
		.server_is_participant(&self.services.server.name, &room_id)
		.await
	{
		return Err!("We are not participating in the room / we don't know about the room ID.");
	}

	let latest_pdu = self
		.services
		.rooms
		.timeline
		.latest_pdu_in_room(&room_id)
		.await
		.map_err(|_| err!(Database("Failed to find the latest PDU in database")))?;

	let out = format!("{latest_pdu:?}");
	self.write_str(&out).await
}

#[admin_command]
#[tracing::instrument(skip(self), level = "info")]
#[allow(clippy::fn_params_excessive_bools)]
pub(crate) async fn force_set_state(
	&self,
	room_id: OwnedRoomId,
	server_names: Vec<OwnedServerName>,
	at_event: Option<OwnedEventId>,
	overwrite: bool,
	skip_sig_verify: bool,
	absolute: bool,
	output: Option<String>,
	input: Option<String>,
	dry_run: bool,
	#[allow(unused_variables)] skip_membership_rebuild: bool,
) -> Result {
	self.bail_restricted()?;

	// --overwrite is shorthand for both flags
	let skip_sig_verify = skip_sig_verify || overwrite;
	let absolute = absolute || overwrite;

	let at_event_id = match at_event {
		| Some(event_id) => event_id,
		| None => self
			.services
			.rooms
			.timeline
			.latest_pdu_in_room(&room_id)
			.await
			.map_err(|_| {
				err!(Request(InvalidParam(
					"No PDUs found in room. Provide an event_id to bootstrap using the \
					 --at-event flag."
				)))
			})?
			.event_id()
			.to_owned(),
	};

	let db_room_version = self
		.services
		.rooms
		.state
		.get_room_version(&room_id)
		.await
		.ok();

	let at_event_id_clone = at_event_id.clone();

	// Load state from file, federation, or local database
	let (pdus, auth_chain, mut state) = Box::pin(self.fetch_and_load_state(
		&room_id,
		&server_names,
		&at_event_id,
		input.as_ref(),
		output.as_ref(),
	))
	.await?;

	// Resolve room version: prefer DB, fall back to create event in
	// federation response. Error out if neither works — using the wrong
	// version produces wrong reference hashes (e.g. V11 rules on a V6 room).
	let room_version = if let Some(v) = db_room_version {
		v
	} else {
		let mut found = None;
		for pdu in &pdus {
			if let Ok(val) = serde_json::from_str::<serde_json::Value>(pdu.get()) {
				if val.get("type").and_then(|v| v.as_str()) == Some("m.room.create") {
					if let Some(ver) = val
						.get("content")
						.and_then(|c| c.get("room_version"))
						.and_then(|v| v.as_str())
					{
						found = ver.parse::<RoomVersionId>().ok();
					}
					break;
				}
			}
		}
		let v = found.ok_or_else(|| {
			err!(Request(Unknown(
				"Cannot determine room version from DB or federation response. Refusing to \
				 proceed — wrong version produces wrong event hashes."
			)))
		})?;
		info!("Extracted room version {v} from create event (DB had no state)");
		v
	};

	let (validated, dropped) = Box::pin(self.validate_and_extract_state(
		&room_id,
		&room_version,
		&pdus,
		skip_sig_verify,
		&mut state,
	))
	.await?;

	info!("State PDUs: {validated} validated, {dropped} dropped (failed signature check)");
	if dropped > 0 {
		warn!(
			"{dropped} state PDUs were silently dropped due to signature validation failure. \
			 Consider re-running with --skip-sig-verify to skip validation."
		);
	}

	// Federation /state returns state BEFORE the queried event. When the
	// at_event is a state event, inject it into the state map so force-set
	// includes its own state change (e.g. a join event for the local user).
	if let Ok(at_pdu) = self
		.services
		.rooms
		.timeline
		.get_pdu(&at_event_id_clone)
		.await
	{
		if let Some(state_key) = &at_pdu.state_key {
			let shortstatekey = self
				.services
				.rooms
				.short
				.get_or_create_shortstatekey(&at_pdu.kind.to_string().into(), state_key)
				.await;
			info!("Injecting at_event {at_event_id_clone} into state (state-after)");
			state.insert(shortstatekey, at_event_id_clone);
		}
	}

	let (auth_added, auth_existing, auth_dropped) = Box::pin(self.validate_and_add_auth_chain(
		&room_id,
		&room_version,
		&auth_chain,
		skip_sig_verify,
	))
	.await?;

	info!("Auth chain: {auth_added} added, {auth_existing} existing, {auth_dropped} dropped");
	let auth_total = auth_added.saturating_add(auth_existing);
	if auth_total > 10_000 {
		warn!("Auth chain exceeds 10k events ({auth_total}) — possible DAG bloat");
	}

	if dry_run {
		Box::pin(self.dry_run_comparison(
			&room_id,
			&state,
			validated,
			dropped,
			auth_added,
			auth_existing,
			auth_dropped,
		))
		.await?;

		return Ok(());
	}

	// Collect remote event IDs (both state and auth chain) before state is consumed
	// by compress/resolve
	let mut remote_eids: HashSet<OwnedEventId> = state.values().cloned().collect();
	for pdu in &auth_chain {
		if let Ok((event_id, _)) =
			conduwuit::matrix::event::gen_event_id_canonical_json(pdu, &room_version)
		{
			remote_eids.insert(event_id);
		}
	}
	info!("Clearing PDU markers for {} remote events", remote_eids.len());

	// Un-reject/un-soft-fail the authoritative remote events so they
	//    can participate in state resolution
	for eid in &remote_eids {
		self.services.rooms.pdu_metadata.clear_pdu_markers(eid);
	}

	// Neutralize DAG poison BEFORE state resolution evaluates them
	info!("Rejecting conflicting local state");
	Box::pin(self.reject_conflicting_state(&room_id, &at_event_id, &remote_eids)).await;

	let new_room_state = if absolute {
		info!("Resolving new room state (ABSOLUTE OVERRIDE)");
		let compressed: conduwuit_service::rooms::state_compressor::CompressedState = self
			.services
			.rooms
			.state_compressor
			.compress_state_events(state.iter().map(|(ssk, eid)| (ssk, (*eid).as_ref())))
			.collect()
			.await;
		std::sync::Arc::new(compressed)
	} else {
		// Only attempt state resolution if the room has prior state.
		// If there's no shortstatehash, this is a genuine cold bootstrap —
		// use remote state directly. Real resolve_state errors (auth chain
		// failures, resolution bugs) must NOT silently fall through here.
		if self
			.services
			.rooms
			.state
			.get_room_shortstatehash(&room_id)
			.await
			.is_err()
		{
			info!("No prior state for room — using remote state directly (cold bootstrap)");
			let compressed: conduwuit_service::rooms::state_compressor::CompressedState = self
				.services
				.rooms
				.state_compressor
				.compress_state_events(state.iter().map(|(ssk, eid)| (ssk, (*eid).as_ref())))
				.collect()
				.await;
			std::sync::Arc::new(compressed)
		} else {
			info!("Resolving new room state (state-res)");
			Box::pin(self.services.rooms.event_handler.resolve_state(
				&room_id,
				&room_version,
				state,
			))
			.await?
		}
	};

	info!("Compressing new room state");
	let HashSetCompressStateEvent {
		shortstatehash: short_state_hash,
		added,
		removed,
	} = self
		.services
		.rooms
		.state_compressor
		// Use save_state_as_root instead of save_state: the normal save_state
		// must traverse the entire O(depth) ancestor diff chain via
		// load_shortstatehash_info, which hangs on rooms with deep history.
		// save_state_as_root checks the stateinfo_cache (O(1)) and falls back to
		// writing the full state as a fresh root, completing in O(state_size).
		.save_state_as_root(room_id.clone().as_ref(), new_room_state)
		.await?;

	let state_lock = self.services.rooms.state.mutex.lock(&*room_id).await;

	if skip_membership_rebuild {
		// Fast path: just set the state hash directly, skip per-member iteration
		info!("Fast-setting room state (skipping membership rebuild)");
		self.services
			.rooms
			.state
			.set_room_state(room_id.as_ref(), short_state_hash, &state_lock);

		// Update joined count from state snapshot
		self.services
			.rooms
			.state_cache
			.update_joined_count(room_id.as_ref())
			.await;
	} else {
		info!(
			"Forcing new room state (quiet mode): {} added, {} removed",
			added.len(),
			removed.len()
		);
		Box::pin(self.services.rooms.state.force_state_quiet(
			room_id.clone().as_ref(),
			short_state_hash,
			added,
			removed,
			&state_lock,
		))
		.await?;
	}

	// Set the tip event as the sole forward extremity. Previous behavior
	// scattered extremities across all state events, fracturing the DAG.
	// The state is already corrected by force_state above; extremities
	// should just point at the timeline tip.
	let tip_event_id = self
		.services
		.rooms
		.timeline
		.latest_pdu_in_room(&room_id)
		.await;
	if let Ok(tip_pdu) = tip_event_id {
		self.services
			.rooms
			.state
			.set_forward_extremities(
				room_id.as_ref(),
				once(tip_pdu.event_id().to_owned()),
				None,
				&state_lock,
			)
			.await;

		// Update the tip event's shortstatehash so that state_at_incoming
		// inherits the forced state when the next event arrives. Without this,
		// the forced state is ephemeral — the first incoming event re-resolves
		// from the tip's old shortstatehash and undoes the force.
		let tip_shorteventid = self
			.services
			.rooms
			.short
			.get_or_create_shorteventid(tip_pdu.event_id())
			.await;
		self.services
			.rooms
			.state
			.set_pdu_shortstatehash(tip_shorteventid, short_state_hash);
		info!("Set tip {} as sole extremity (room SSH {short_state_hash})", tip_pdu.event_id());
	} else {
		// No timeline events — /sync won't deliver this room.
		// Promote the most recent state event as a timeline anchor.
		Box::pin(self.promote_sync_anchor(&room_id, short_state_hash, &state_lock)).await;
	}

	drop(state_lock);
	if !skip_membership_rebuild {
		info!("Rebuilding membership cache");
		Box::pin(self.rebuild_membership_cache_inner(room_id.clone(), short_state_hash)).await;
	}

	self.write_str("Successfully forced the room state from the requested remote server.")
		.await
}

#[admin_command]
async fn fetch_and_load_state(
	&self,
	room_id: &ruma::RoomId,
	server_names: &[OwnedServerName],
	at_event_id: &EventId,
	input: Option<&String>,
	output: Option<&String>,
) -> Result<(
	Vec<Box<serde_json::value::RawValue>>,
	Vec<Box<serde_json::value::RawValue>>,
	HashMap<u64, OwnedEventId>,
)> {
	let mut state: HashMap<u64, OwnedEventId> = HashMap::new();
	let at_event_id_str = at_event_id.to_string();

	// Load state from file, federation, or local database
	let (pdus, auth_chain): (
		Vec<Box<serde_json::value::RawValue>>,
		Vec<Box<serde_json::value::RawValue>>,
	) = if let Some(path) = input {
		info!("Loading state from file: {path}");
		let data = std::fs::read_to_string(path)
			.map_err(|e| err!(Database("Failed to read input file: {e:?}")))?;
		let parsed: serde_json::Value = serde_json::from_str(&data)
			.map_err(|e| err!(Database("Failed to parse input file: {e:?}")))?;
		let pdus_val = parsed
			.get("pdus")
			.ok_or(err!(Database("Missing 'pdus' key in input file")))?;
		let auth_val = parsed
			.get("auth_chain")
			.ok_or(err!(Database("Missing 'auth_chain' key in input file")))?;
		let pdus: Vec<Box<serde_json::value::RawValue>> =
			serde_json::from_value(pdus_val.clone())
				.map_err(|e| err!(Database("Failed to parse PDUs: {e:?}")))?;
		let auth_chain: Vec<Box<serde_json::value::RawValue>> =
			serde_json::from_value(auth_val.clone())
				.map_err(|e| err!(Database("Failed to parse auth chain: {e:?}")))?;
		info!(
			"Loaded {} state PDUs and {} auth chain events from file",
			pdus.len(),
			auth_chain.len()
		);
		(pdus, auth_chain)
	} else if !server_names.is_empty() {
		let mut all_pdus: Vec<Box<serde_json::value::RawValue>> = Vec::new();
		let mut all_auth: Vec<Box<serde_json::value::RawValue>> = Vec::new();

		for server_name in server_names {
			info!("Fetching room state from {server_name} at event {at_event_id_str}...");
			match self
				.services
				.sending
				.send_federation_request(server_name, get_room_state::v1::Request {
					room_id: room_id.to_owned(),
					event_id: at_event_id.to_owned(),
				})
				.await
			{
				| Ok(resp) => {
					info!(
						"Received {} state PDUs and {} auth chain events from {server_name}",
						resp.pdus.len(),
						resp.auth_chain.len()
					);

					if let Some(path) = output {
						let suffix = if server_names.len() > 1 {
							format!("-{server_name}")
						} else {
							String::new()
						};
						let dump_path = format!("{path}{suffix}");
						info!("Dumping federation state response to {dump_path}");
						let dump = serde_json::json!({
							"room_id": room_id,
							"server_name": server_name,
							"event_id": at_event_id_str,
							"pdus": resp.pdus,
							"auth_chain": resp.auth_chain,
						});
						if let Err(e) = std::fs::write(
							&dump_path,
							serde_json::to_string_pretty(&dump).unwrap_or_default(),
						) {
							warn!("Failed to write output file {dump_path}: {e}");
						}
					}

					all_pdus.extend(resp.pdus);
					all_auth.extend(resp.auth_chain);
				},
				| Err(e) => {
					warn!("Failed to fetch state from {server_name}: {e}");
					self.write_str(&format!("⚠ Failed to fetch state from {server_name}: {e}\n"))
						.await?;
					continue;
				},
			}
		}

		if all_pdus.is_empty() {
			return Err!(Request(Unknown("All servers failed to respond")));
		}

		info!(
			"Merged state from {} server(s): {} PDUs, {} auth chain events",
			server_names.len(),
			all_pdus.len(),
			all_auth.len()
		);
		(all_pdus, all_auth)
	} else {
		// Local-only: rebuild state from existing database without federation
		info!("Rebuilding room state from local DAG (no federation)...");
		let ssh = self
			.services
			.rooms
			.state
			.get_room_shortstatehash(room_id)
			.await
			.map_err(|_| {
				err!("No existing state for room — provide a server to bootstrap from")
			})?;

		let local_state: HashMap<u64, OwnedEventId> = self
			.services
			.rooms
			.state_accessor
			.state_full_ids(ssh)
			.collect()
			.await;

		info!("Local state has {} entries, re-resolving...", local_state.len());
		state = local_state;
		// No PDUs or auth_chain to process — state is already populated
		(Vec::new(), Vec::new())
	};

	Ok((pdus, auth_chain, state))
}

#[admin_command]
async fn validate_and_extract_state(
	&self,
	room_id: &ruma::RoomId,
	room_version: &RoomVersionId,
	pdus: &[Box<serde_json::value::RawValue>],
	skip_sig_verify: bool,
	state: &mut HashMap<u64, OwnedEventId>,
) -> Result<(usize, usize)> {
	info!(
		"Validating signatures for {} room_state PDUs (skip_sig_verify={skip_sig_verify})",
		pdus.len()
	);
	let mut validated = 0_usize;
	let mut dropped = 0_usize;
	for pdu in pdus {
		// Always compute event_id cheaply first (hash, no crypto)
		let Ok((event_id, _)) =
			conduwuit::matrix::event::gen_event_id_canonical_json(pdu, room_version)
		else {
			dropped = dropped.saturating_add(1);
			continue;
		};

		// Skip expensive Ed25519 sig verification for events already stored locally
		let has_timeline = self
			.services
			.rooms
			.timeline
			.get_pdu_id(&event_id)
			.await
			.is_ok();
		let has_outlier = self
			.services
			.rooms
			.outlier
			.get_outlier_pdu_json(&event_id)
			.await
			.is_ok();
		let already_exists = has_timeline || has_outlier;

		let result = if skip_sig_verify || already_exists {
			// Skip signature validation — just derive event_id from content hash
			conduwuit::matrix::event::gen_event_id_canonical_json(pdu, room_version).map(
				|(event_id, mut value)| {
					value.insert(
						"event_id".into(),
						ruma::CanonicalJsonValue::String(event_id.as_str().into()),
					);
					(event_id, value)
				},
			)
		} else {
			self.services
				.server_keys
				.validate_and_add_event_id(pdu, room_version)
				.await
		};
		let Ok((event_id, value)) = result else {
			dropped = dropped.saturating_add(1);
			continue;
		};
		validated = validated.saturating_add(1);

		// Clear any rejection/soft-fail markers for state PDUs we accept from
		// federation. Without this, previously rejected events stay in the
		// rejectedeventids table and poison all subsequent state_res operations.
		// (Auth chain events already get this treatment at line ~967 below.)
		self.services
			.rooms
			.pdu_metadata
			.clear_pdu_markers(&event_id);

		let total = validated.saturating_add(dropped);
		if total.is_multiple_of(100) {
			info!(
				"Sig verify progress: {validated} ok, {dropped} dropped of {} total",
				pdus.len()
			);
		}

		// Extract fields directly from canonical JSON — avoids PduEvent
		// deserialization failures for events with oversized IDs (>255 bytes).
		let event_type_str = value.get("type").and_then(|v| match v {
			| ruma::CanonicalJsonValue::String(s) => Some(s.clone()),
			| _ => None,
		});
		let state_key_opt = value.get("state_key").and_then(|v| match v {
			| ruma::CanonicalJsonValue::String(s) => Some(s.clone()),
			| _ => None,
		});
		let pdu_room_id = value.get("room_id").and_then(|v| match v {
			| ruma::CanonicalJsonValue::String(s) => Some(s.clone()),
			| _ => None,
		});

		let Some(ref event_type) = event_type_str else {
			warn!("Skipping PDU {event_id} with no type field");
			dropped = dropped.saturating_add(1);
			continue;
		};

		// Validate room_id matches (v11+ events may omit room_id)
		if let Some(ref rid) = pdu_room_id {
			if rid != room_id.as_str() {
				return Err!(BadServerResponse(
					"Remote room_state PDU belongs to a different room"
				));
			}
		}

		if !has_timeline {
			if !has_outlier {
				info!("PDU {event_id} NOT in timeline, adding as outlier");
			} else {
				info!("PDU {event_id} is outlier, ensuring room association is present");
			}
			self.services
				.rooms
				.outlier
				.add_pdu_outlier(&event_id, &value, Some(room_id))
				.await;
		}

		if let Some(state_key) = &state_key_opt {
			let shortstatekey = self
				.services
				.rooms
				.short
				.get_or_create_shortstatekey(&event_type.clone().into(), state_key)
				.await;

			state.insert(shortstatekey, event_id.clone());
		}
	}
	Ok((validated, dropped))
}

#[admin_command]
async fn validate_and_add_auth_chain(
	&self,
	room_id: &ruma::RoomId,
	room_version: &RoomVersionId,
	auth_chain: &[Box<serde_json::value::RawValue>],
	skip_sig_verify: bool,
) -> Result<(usize, usize, usize)> {
	info!("Going through auth_chain response");
	let mut auth_existing = 0_usize;
	let mut auth_added = 0_usize;
	let mut auth_dropped = 0_usize;
	let auth_chain_total = auth_chain.len();
	for pdu in auth_chain {
		// Compute event_id first (cheap hash, no crypto) to check existence
		// before doing expensive sig verification.
		let Ok((event_id, _)) =
			conduwuit::matrix::event::gen_event_id_canonical_json(pdu, room_version)
		else {
			auth_dropped = auth_dropped.saturating_add(1);
			continue;
		};

		// Skip events we already have (timeline or outlier store)
		let has_timeline = self
			.services
			.rooms
			.timeline
			.get_pdu_id(&event_id)
			.await
			.is_ok();
		let outlier_json = self
			.services
			.rooms
			.outlier
			.get_outlier_pdu_json(&event_id)
			.await;
		let has_outlier = outlier_json.is_ok();

		if has_timeline || has_outlier {
			if let Ok(json) = outlier_json {
				// Ensure the outlier is associated with this room.
				// For v12 hashed rooms, previous corrupt reorder_timeline runs might have
				// omitted the room association, leaving it in a broken state.
				self.services
					.rooms
					.outlier
					.add_pdu_outlier(&event_id, &json, Some(room_id))
					.await;
			}
			// Clear markers for existing auth events to heal any previous
			// soft-fails/rejections
			self.services
				.rooms
				.pdu_metadata
				.clear_pdu_markers(&event_id);
			auth_existing = auth_existing.saturating_add(1);
		} else {
			// Only sig-verify events we actually need to store
			let result = if skip_sig_verify {
				conduwuit::matrix::event::gen_event_id_canonical_json(pdu, room_version).map(
					|(event_id, mut value)| {
						value.insert(
							"event_id".into(),
							ruma::CanonicalJsonValue::String(event_id.as_str().into()),
						);
						(event_id, value)
					},
				)
			} else {
				self.services
					.server_keys
					.validate_and_add_event_id(pdu, room_version)
					.await
			};

			let Ok((event_id, value)) = result else {
				auth_dropped = auth_dropped.saturating_add(1);
				continue;
			};

			// Clear markers for incoming auth events from the backbone
			self.services
				.rooms
				.pdu_metadata
				.clear_pdu_markers(&event_id);

			self.services
				.rooms
				.outlier
				.add_pdu_outlier(&event_id, &value, Some(room_id))
				.await;
			auth_added = auth_added.saturating_add(1);
		}

		let processed = auth_existing
			.saturating_add(auth_added)
			.saturating_add(auth_dropped);
		if processed.is_multiple_of(1000) {
			info!(
				"Auth chain progress: {auth_added} added, {auth_existing} existing, \
				 {auth_dropped} dropped of {auth_chain_total} total"
			);
		}
	}
	Ok((auth_added, auth_existing, auth_dropped))
}

#[admin_command]
async fn dry_run_comparison(
	&self,
	room_id: &ruma::RoomId,
	state: &HashMap<u64, OwnedEventId>,
	validated: usize,
	dropped: usize,
	auth_added: usize,
	auth_existing: usize,
	auth_dropped: usize,
) -> Result {
	// Compare remote state against local state without modifying anything
	let local_state: HashMap<u64, OwnedEventId> = if let Ok(ssh) = self
		.services
		.rooms
		.state
		.get_room_shortstatehash(room_id)
		.await
	{
		self.services
			.rooms
			.state_accessor
			.state_full_ids(ssh)
			.collect()
			.await
	} else {
		HashMap::new()
	};

	let mut would_add = Vec::new();
	let mut would_remove = Vec::new();
	let mut would_replace = Vec::new();

	// Events in remote state but not in local
	for (ssk, remote_eid) in state {
		match local_state.get(ssk) {
			| None => would_add.push(remote_eid.clone()),
			| Some(local_eid) if local_eid != remote_eid => {
				would_replace.push((local_eid.clone(), remote_eid.clone()));
			},
			| Some(_) => {}, // Same event, no change
		}
	}

	// Events in local state but not in remote
	for (ssk, local_eid) in &local_state {
		if !state.contains_key(ssk) {
			would_remove.push(local_eid.clone());
		}
	}

	self.write_str(&format!(
		"**Dry run** — no changes applied.\n\nRemote state events: {}\nLocal state events: \
		 {}\nWould add: {}\nWould remove: {}\nWould replace: {}\nValidated: {validated}, \
		 Dropped: {dropped}\nAuth chain: {auth_added} new, {auth_existing} existing, \
		 {auth_dropped} dropped",
		state.len(),
		local_state.len(),
		would_add.len(),
		would_remove.len(),
		would_replace.len(),
	))
	.await?;

	if !would_replace.is_empty() {
		let mut details = String::from("\nReplacements:\n");
		for (old, new) in &would_replace {
			let _ = writeln!(details, "  {old} → {new}");
		}
		self.write_str(&details).await?;
	}

	if !would_add.is_empty() {
		let mut details = String::from("\nAdditions:\n");
		for eid in &would_add {
			let _ = writeln!(details, "  + {eid}");
		}
		self.write_str(&details).await?;
	}

	if !would_remove.is_empty() {
		let mut details = String::from("\nRemovals:\n");
		for eid in &would_remove {
			let _ = writeln!(details, "  - {eid}");
		}
		self.write_str(&details).await?;
	}

	Ok(())
}

/// Mark conflicting local state events as rejected. Without this, stale
/// unrejected "join" events win over authoritative "ban" events during
/// future state resolution, causing the state to reset in a loop.
#[admin_command]
async fn reject_conflicting_state(
	&self,
	room_id: &ruma::RoomId,
	at_event_id: &EventId,
	remote_eids: &HashSet<OwnedEventId>,
) {
	let local_ssh: Result<u64> = match self
		.services
		.rooms
		.state_accessor
		.pdu_shortstatehash(at_event_id)
		.await
	{
		| Ok(ssh) => Ok(ssh),
		| Err(_) =>
			self.services
				.rooms
				.state
				.get_room_shortstatehash(room_id)
				.await,
	};

	let Ok(local_ssh) = local_ssh else {
		return;
	};

	// Collect into Vec FIRST to drop the zero-copy RocksDB iterator
	// before the write phase. Holding an iterator across .await points
	// risks SEGV if compaction invalidates the underlying memory.
	let local_eids: HashSet<OwnedEventId> = self
		.services
		.rooms
		.state_accessor
		.state_full(local_ssh)
		.map(|(_, pdu)| pdu.event_id().to_owned())
		.collect()
		.await;

	let extra: Vec<OwnedEventId> = local_eids.difference(remote_eids).cloned().collect();
	if !extra.is_empty() {
		let mut rejected = 0_usize;
		for eid in &extra {
			if !self
				.services
				.rooms
				.pdu_metadata
				.is_event_rejected(eid)
				.await
			{
				self.services
					.rooms
					.pdu_metadata
					.mark_event_rejected(eid, "conflicting state replaced by remote")
					.await;
				rejected = rejected.saturating_add(1);
			}
		}
		info!(
			"Marked {rejected}/{} conflicting events as rejected (we will not revisit them 🪦)",
			extra.len()
		);
	}
}

/// Rebuild membership cache from a state snapshot. Extracted to keep
/// `force_set_room_state_from_server` below the stack-frame limit.
#[admin_command]
async fn rebuild_membership_cache_inner(&self, room_id: OwnedRoomId, _short_state_hash: u64) {
	let _state_lock = self.services.rooms.state.mutex.lock(&room_id).await;
	self.services
		.rooms
		.state_cache
		.reconcile_membership(&room_id)
		.await;
}

/// Promote the most recent state event to the timeline as a Normal PDU,
/// giving `/sync` a positional cursor to deliver the room to clients.
#[admin_command]
async fn promote_sync_anchor(
	&self,
	room_id: &ruma::RoomId,
	short_state_hash: u64,
	state_lock: &conduwuit_service::rooms::state::RoomMutexGuard,
) {
	use conduwuit::matrix::Event;
	use futures::StreamExt;

	info!("No timeline events found; promoting anchor event for /sync visibility");

	let mut best: Option<(u64, OwnedEventId, PduEvent, CanonicalJsonObject)> = None;

	// Collect candidate IDs first to avoid holding zero-copy RocksDB
	// iterator across .await points (SEGV prevention)
	let anchor_candidates: Vec<_> = self
		.services
		.rooms
		.state_accessor
		.state_full_pdus(short_state_hash)
		.map(|pdu| {
			let ts: u64 = pdu.origin_server_ts().0.into();
			let eid = pdu.event_id().to_owned();
			(ts, eid)
		})
		.collect()
		.await;

	for (ts, eid) in anchor_candidates {
		if best.as_ref().is_none_or(|(best_ts, ..)| ts > *best_ts) {
			// Check both timeline AND outlier tables — force-set imports
			// state events into the outlier table, not timeline.
			let json_result = match self.services.rooms.timeline.get_pdu_json(&eid).await {
				| Ok(json) => Ok(json),
				| Err(_) => self.services.rooms.outlier.get_outlier_pdu_json(&eid).await,
			};
			if let Ok(json) = json_result {
				// Use from_id_val to inject the room_id into V3+ events
				// which strip it from the raw JSON to save space.
				let pdu_result = PduEvent::from_id_val(&eid, json.clone(), Some(room_id));
				match pdu_result {
					| Ok(pdu_owned) => {
						best = Some((ts, eid, pdu_owned, json));
					},
					| Err(e) => {
						warn!("Skipping anchor candidate {eid}: bad PDU JSON: {e}");
					},
				}
			}
		}
	}

	if let Some((_ts, anchor_id, anchor_pdu, anchor_json)) = best {
		match self
			.services
			.rooms
			.timeline
			.force_insert_pdu(room_id, &anchor_id, &anchor_pdu, &anchor_json, true)
			.await
		{
			| Ok(_pdu_id) => {
				self.services
					.rooms
					.state
					.set_forward_extremities(room_id, once(anchor_id.clone()), None, state_lock)
					.await;
				info!("Promoted {anchor_id} as timeline anchor for /sync");
			},
			| Err(e) => {
				warn!("Failed to promote anchor event {anchor_id}: {e}");
			},
		}
	} else {
		warn!("No state events found to promote as timeline anchor");
	}
}

#[admin_command]
pub(super) async fn get_signing_keys(
	&self,
	server_name: Option<OwnedServerName>,
	notary: Option<OwnedServerName>,
	query: bool,
) -> Result {
	let server_name = server_name.unwrap_or_else(|| self.services.server.name.clone());

	if let Some(notary) = notary {
		let signing_keys = self
			.services
			.server_keys
			.notary_request(&notary, &server_name)
			.await?;

		let out = format!("```rs\n{signing_keys:#?}\n```");
		return self.write_str(&out).await;
	}

	let out = if query {
		let signing_keys = self
			.services
			.server_keys
			.server_request(&server_name)
			.await?;
		format!("```json\n{}\n```", signing_keys.json().get())
	} else {
		let signing_keys = self
			.services
			.server_keys
			.signing_keys_for(&server_name)
			.await?;
		format!("```rs\n{signing_keys:#?}\n```")
	};

	self.write_str(&out).await
}

#[admin_command]
pub(super) async fn get_verify_keys(&self, server_name: Option<OwnedServerName>) -> Result {
	let server_name = server_name.unwrap_or_else(|| self.services.server.name.clone());

	let keys = self
		.services
		.server_keys
		.verify_keys_for(&server_name)
		.await;

	let mut out = String::new();
	writeln!(out, "| Key ID | Public Key |")?;
	writeln!(out, "| --- | --- |")?;
	for (key_id, key) in keys {
		writeln!(out, "| {key_id} | {key:?} |")?;
	}

	self.write_str(&out).await
}

#[admin_command]
pub(super) async fn resolve_true_destination(
	&self,
	server_name: OwnedServerName,
	no_cache: bool,
) -> Result {
	if !self.services.server.config.allow_federation {
		return Err!("Federation is disabled on this homeserver.",);
	}

	if server_name == self.services.server.name {
		return Err!(
			"Not allowed to send federation requests to ourselves. Please use `get-pdu` for \
			 fetching local PDUs.",
		);
	}

	let actual = self
		.services
		.resolver
		.resolve_actual_dest(&server_name, !no_cache)
		.await?;

	let msg = format!("Destination: {}\nHostname URI: {}", actual.dest, actual.host);
	self.write_str(&msg).await
}

#[admin_command]
pub(super) async fn memory_stats(&self, opts: Option<String>) -> Result {
	const OPTS: &str = "abcdefghijklmnopqrstuvwxyz";

	let opts: String = OPTS
		.chars()
		.filter(|&c| {
			let allow_any = opts.as_ref().is_some_and(|opts| opts == "*");

			let allow = allow_any || opts.as_ref().is_some_and(|opts| opts.contains(c));

			!allow
		})
		.collect();

	let stats = conduwuit::alloc::memory_stats(&opts).unwrap_or_default();

	self.write_str("```\n").await?;
	self.write_str(&stats).await?;
	self.write_str("\n```").await?;
	Ok(())
}

#[cfg(tokio_unstable)]
#[admin_command]
pub(super) async fn runtime_metrics(&self) -> Result {
	let out = self.services.server.metrics.runtime_metrics().map_or_else(
		|| "Runtime metrics are not available.".to_owned(),
		|metrics| {
			format!(
				"```rs\nnum_workers: {}\nnum_alive_tasks: {}\nglobal_queue_depth: {}\n```",
				metrics.num_workers(),
				metrics.num_alive_tasks(),
				metrics.global_queue_depth()
			)
		},
	);

	self.write_str(&out).await
}

#[cfg(not(tokio_unstable))]
#[admin_command]
pub(super) async fn runtime_metrics(&self) -> Result {
	self.write_str("Runtime metrics require building with `tokio_unstable`.")
		.await
}

#[cfg(all(tokio_unstable, feature = "tokio_metrics"))]
#[admin_command]
pub(super) async fn runtime_interval(&self) -> Result {
	let out = self.services.server.metrics.runtime_interval().map_or_else(
		|| "Runtime metrics are not available.".to_owned(),
		|metrics| format!("```rs\n{metrics:#?}\n```"),
	);

	self.write_str(&out).await
}

#[cfg(not(all(tokio_unstable, feature = "tokio_metrics")))]
#[admin_command]
pub(super) async fn runtime_interval(&self) -> Result {
	self.write_str("Runtime metrics require building with `tokio_unstable` and `tokio_metrics`.")
		.await
}

#[admin_command]
pub(super) async fn time(&self) -> Result {
	let now = SystemTime::now();
	let now = utils::time::format(now, "%+");

	self.write_str(&now).await
}

#[admin_command]
pub(super) async fn database_stats(
	&self,
	property: Option<String>,
	map: Option<String>,
	verbose: bool,
) -> Result {
	let map_name = map.as_ref().map_or(EMPTY, String::as_str);

	if !verbose && property.is_none() {
		writeln!(self, "| Map | Est. Keys | Est. Live Size |").await?;
		writeln!(self, "| --- | ---: | ---: |").await?;
		self.services
			.db
			.iter()
			.filter(|&(&name, _)| map_name.is_empty() || map_name == name)
			.try_stream()
			.try_for_each(|(&name, map)| {
				let keys = map
					.property("rocksdb.estimate-num-keys")
					.unwrap_or_default();
				let size = map
					.property("rocksdb.estimate-live-data-size")
					.unwrap_or_default();
				writeln!(self, "| {name} | {keys} | {size} |")
			})
			.await
	} else {
		let property = property.unwrap_or_else(|| "rocksdb.stats".to_owned());
		self.services
			.db
			.iter()
			.filter(|&(&name, _)| map_name.is_empty() || map_name == name)
			.try_stream()
			.try_for_each(|(&name, map)| {
				let res = map.property(&property).expect("invalid property");
				writeln!(self, "##### {name}:\n```\n{}\n```", res.trim())
			})
			.await
	}
}

#[admin_command]
pub(super) async fn database_files(&self, map: Option<String>, level: Option<i32>) -> Result {
	let mut files: Vec<_> = self.services.db.db.file_list().collect::<Result<_>>()?;

	files.sort_by_key(|f| f.name.clone());

	writeln!(self, "| lev  | sst  | keys | dels | size | column |").await?;
	writeln!(self, "| ---: | :--- | ---: | ---: | ---: | :---   |").await?;
	files
		.into_iter()
		.filter(|file| {
			map.as_deref()
				.is_none_or(|map| map == file.column_family_name)
		})
		.filter(|file| level.as_ref().is_none_or(|&level| level == file.level))
		.try_stream()
		.try_for_each(|file| {
			writeln!(
				self,
				"| {} | {:<13} | {:7}+ | {:4}- | {:9} | {} |",
				file.level,
				file.name,
				file.num_entries,
				file.num_deletions,
				file.size,
				file.column_family_name,
			)
		})
		.await
}

#[admin_command]
pub(super) async fn trim_memory(&self) -> Result {
	conduwuit::alloc::trim(None)?;

	writeln!(self, "done").await
}

#[admin_command]
pub(super) async fn send_test_email(&self) -> Result {
	self.bail_restricted()?;

	let mailer = self.services.mailer.expect_mailer()?;
	let Some(sender) = self.sender else {
		return Err!("No sender user provided in context");
	};

	let Some(email) = self
		.services
		.threepid
		.get_email_for_localpart(sender.localpart())
		.await
	else {
		return Err!("{} has no associated email address", sender);
	};

	mailer
		.send(Mailbox::new(None, email.clone()), service::mailer::messages::Test)
		.await?;

	self.write_str(&format!("Test email successfully sent to {email}"))
		.await?;

	Ok(())
}

#[admin_command]
pub(super) async fn fix_pdu_event_ids(&self) -> Result {
	let timeline_fixed = self.services.rooms.timeline.fix_pdu_event_ids().await?;
	let outlier_fixed = self.services.rooms.outlier.fix_pdu_event_ids()?;

	let out = format!(
		"Fixed {timeline_fixed} missing event_ids in timeline PDUs.\nFixed {outlier_fixed} \
		 missing event_ids in outlier PDUs.\n"
	);
	self.write_str(&out).await?;

	Ok(())
}

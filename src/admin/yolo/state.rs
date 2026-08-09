use std::{
	collections::{HashMap, HashSet},
	fmt::Write,
	time::Instant,
};

use conduwuit::{
	Err, PduCount, Result, err, info,
	matrix::{Event, pdu::PduEvent},
	warn,
};
use futures::{StreamExt, pin_mut};
use ruma::{
	OwnedEventId, OwnedRoomId, OwnedServerName, OwnedUserId,
	api::federation::event::get_room_state,
	events::{StateEventType, TimelineEventType},
};
use serde_json::Value as JsonValue;

use super::dag::format_ts;
use crate::admin_command;

#[admin_command]
pub(super) async fn compare_room_state(
	&self,
	room_id: OwnedRoomId,
	servers: Vec<OwnedServerName>,
	at_event: Option<OwnedEventId>,
	conflict: Option<OwnedUserId>,
	summary: bool,
	skip_sig_verify: bool,
) -> Result {
	use std::fmt::Write;

	use ruma::api::federation::event::get_room_state;

	if servers.is_empty() {
		return Err!(Request(InvalidParam("Provide at least one server to compare against.")));
	}
	let server = &servers[0];
	let room_version = self.services.rooms.state.get_room_version(&room_id).await?;
	let at_event_id = match at_event {
		| Some(event_id) => event_id,
		| None => self
			.services
			.rooms
			.timeline
			.latest_pdu_in_room(&room_id)
			.await?
			.event_id()
			.to_owned(),
	};

	// Fetch tip once — used for state-event detection and injection
	let tip_pdu_opt = self
		.services
		.rooms
		.timeline
		.get_pdu(&at_event_id)
		.await
		.ok();
	let tip_is_state_event = tip_pdu_opt
		.as_ref()
		.is_some_and(|pdu| pdu.state_key.is_some());

	let response = match self
		.services
		.sending
		.send_federation_request(server, get_room_state::v1::Request {
			room_id: room_id.clone(),
			event_id: at_event_id.clone(),
		})
		.await
	{
		| Ok(r) => r,
		| Err(e) => {
			return self
				.write_str(&format!(
					"Failed to fetch state from {server} at event {at_event_id}: {e}\n\nThe \
					 remote server may not have this event. Try specifying a known-shared event \
					 with --at-event, or compare with a different server.",
				))
				.await;
		},
	};

	// Single pass: build remote state map + count remote members
	let mut remote_state = HashMap::new();
	let mut event_timestamps: HashMap<OwnedEventId, u64> = HashMap::new();
	// (membership_or_empty, sender)
	let mut event_meta: HashMap<OwnedEventId, (String, String)> = HashMap::new();
	let mut skipped = 0_usize;
	let mut remote_joined: HashSet<String> = HashSet::new();
	let mut remote_invited: HashSet<String> = HashSet::new();
	let mut remote_left: HashSet<String> = HashSet::new();

	// Conflict tracking: (server_label, event_id, ts, membership, displayname,
	// avatar_url)
	let conflict_key = conflict
		.as_ref()
		.map(|u| ("m.room.member".to_owned(), u.to_string()));
	let mut conflict_entries: Vec<(String, String, u64, String, String, String)> = Vec::new();

	for pdu_raw in &response.pdus {
		let (event_id, value) = if skip_sig_verify {
			match conduwuit::matrix::event::gen_event_id_canonical_json(pdu_raw, &room_version) {
				| Ok((eid, val)) => (eid, val),
				| Err(e) => {
					warn!("Skipping PDU, canonicalization failed: {e}");
					skipped = skipped.saturating_add(1);
					continue;
				},
			}
		} else {
			match self
				.services
				.server_keys
				.validate_and_add_event_id(pdu_raw, &room_version)
				.await
			{
				| Ok(r) => r,
				| Err(e) => {
					// Persist as rejected outlier so the event is available for
					// auth chain lookups and state resolution context
					match conduwuit::matrix::event::gen_event_id_canonical_json(
						pdu_raw,
						&room_version,
					) {
						| Ok((eid, val)) => {
							warn!(
								"PDU {eid} failed signature verification, storing as rejected \
								 outlier: {e}"
							);
							self.services.rooms.outlier.add_pdu_outlier(
								&eid,
								&val,
								Some(&room_id),
							);
							self.services
								.rooms
								.pdu_metadata
								.mark_event_rejected(
									&eid,
									"signature verification failed in compare-room-state",
								)
								.await;
							// Still count membership for the remote's totals —
							// the remote sent this as part of their state.
							if let Ok(pdu) =
								PduEvent::from_id_val(&eid, val, Some(room_id.as_ref()))
							{
								if pdu.kind == TimelineEventType::RoomMember {
									if let Some(state_key) = &pdu.state_key {
										let content: JsonValue = pdu.get_content_as_value();
										let membership = content
											.get("membership")
											.and_then(|v| v.as_str())
											.unwrap_or("unknown");
										match membership {
											| "join" => {
												remote_joined.insert(state_key.to_string());
												remote_invited.remove(state_key.as_str());
												remote_left.remove(state_key.as_str());
											},
											| "invite" => {
												remote_invited.insert(state_key.to_string());
												remote_joined.remove(state_key.as_str());
												remote_left.remove(state_key.as_str());
											},
											| "leave" => {
												remote_left.insert(state_key.to_string());
												remote_joined.remove(state_key.as_str());
												remote_invited.remove(state_key.as_str());
											},
											| _ => {
												remote_joined.remove(state_key.as_str());
												remote_invited.remove(state_key.as_str());
												remote_left.remove(state_key.as_str());
											},
										}
									}
								}
								if let Some(state_key) = &pdu.state_key {
									remote_state.insert(
										(pdu.kind.to_string(), state_key.to_string()),
										eid.clone(),
									);
								}
								event_timestamps
									.insert(eid.clone(), u64::from(pdu.origin_server_ts));
								let content: JsonValue = pdu.get_content_as_value();
								let membership = content
									.get("membership")
									.and_then(|v| v.as_str())
									.unwrap_or("")
									.to_owned();
								event_meta
									.insert(eid.clone(), (membership, pdu.sender().to_string()));
							}
							skipped = skipped.saturating_add(1);
							continue;
						},
						| Err(e2) => {
							warn!("Skipping PDU, canonicalization failed: {e2}");
							skipped = skipped.saturating_add(1);
							continue;
						},
					}
				},
			}
		};

		let pdu = match PduEvent::from_id_val(&event_id, value, Some(room_id.as_ref())) {
			| Ok(pdu) => pdu,
			| Err(e) => {
				warn!(
					"Skipping PDU {event_id}, deserialization failed (likely oversized ID): {e}"
				);
				skipped = skipped.saturating_add(1);
				continue;
			},
		};
		event_timestamps.insert(event_id.clone(), u64::from(pdu.origin_server_ts));
		if let Some(state_key) = &pdu.state_key {
			remote_state.insert((pdu.kind.to_string(), state_key.to_string()), event_id.clone());
		}
		// Store metadata for richer diff output
		{
			let content: JsonValue = pdu.get_content_as_value();
			let membership = content
				.get("membership")
				.and_then(|v| v.as_str())
				.unwrap_or("")
				.to_owned();
			event_meta.insert(event_id.clone(), (membership, pdu.sender().to_string()))
		};

		if pdu.kind == TimelineEventType::RoomMember {
			if let Some(state_key) = &pdu.state_key {
				let content: JsonValue = pdu.get_content_as_value();
				let membership = content
					.get("membership")
					.and_then(|v| v.as_str())
					.unwrap_or("unknown");
				match membership {
					| "join" => {
						remote_joined.insert(state_key.to_string());
						remote_invited.remove(state_key.as_str());
						remote_left.remove(state_key.as_str());
					},
					| "invite" => {
						remote_invited.insert(state_key.to_string());
						remote_joined.remove(state_key.as_str());
						remote_left.remove(state_key.as_str());
					},
					| "leave" => {
						remote_left.insert(state_key.to_string());
						remote_joined.remove(state_key.as_str());
						remote_invited.remove(state_key.as_str());
					},
					| _ => {
						remote_joined.remove(state_key.as_str());
						remote_invited.remove(state_key.as_str());
						remote_left.remove(state_key.as_str());
					},
				}

				if let Some(ref ck) = conflict_key {
					if *state_key == ck.1 {
						let displayname = content
							.get("displayname")
							.and_then(|v| v.as_str())
							.unwrap_or("(none)")
							.to_owned();
						let avatar = content
							.get("avatar_url")
							.and_then(|v| v.as_str())
							.unwrap_or("(none)")
							.to_owned();
						let ts = u64::from(pdu.origin_server_ts);
						conflict_entries.push((
							server.to_string(),
							event_id.to_string(),
							ts,
							membership.to_owned(),
							displayname,
							avatar,
						));
					}
				}
			}
		}
	}

	let local_state_hash = self
		.services
		.rooms
		.state
		.get_room_shortstatehash(&room_id)
		.await?;

	// Inject tip event into remote state (uses cached tip_pdu_opt)
	if tip_is_state_event {
		if let Some(ref tip_pdu) = tip_pdu_opt {
			if let Some(state_key) = &tip_pdu.state_key {
				remote_state.insert(
					(tip_pdu.kind.to_string(), state_key.to_string()),
					at_event_id.clone(),
				);

				if tip_pdu.kind == TimelineEventType::RoomMember {
					let content: JsonValue = tip_pdu.get_content_as_value();
					match content.get("membership").and_then(|v| v.as_str()) {
						| Some("join") => {
							remote_joined.insert(state_key.to_string());
							remote_invited.remove(state_key.as_str());
							remote_left.remove(state_key.as_str());
						},
						| Some("invite") => {
							remote_invited.insert(state_key.to_string());
							remote_joined.remove(state_key.as_str());
							remote_left.remove(state_key.as_str());
						},
						| Some("leave") => {
							remote_left.insert(state_key.to_string());
							remote_joined.remove(state_key.as_str());
							remote_invited.remove(state_key.as_str());
						},
						| _ => {
							remote_joined.remove(state_key.as_str());
							remote_invited.remove(state_key.as_str());
							remote_left.remove(state_key.as_str());
						},
					}
				}
			}
		}
	}

	// Single pass: build local state map + count local members
	let mut local_state: HashMap<(String, String), OwnedEventId> = HashMap::new();
	let mut local_state_joined = 0_usize;
	let mut local_state_invited = 0_usize;
	let mut local_state_left = 0_usize;
	{
		let state_full = self
			.services
			.rooms
			.state_accessor
			.state_full(local_state_hash);
		pin_mut!(state_full);
		while let Some(((event_type, state_key), pdu)) = state_full.next().await {
			let eid = pdu.event_id().to_owned();
			event_timestamps.insert(eid.clone(), pdu.origin_server_ts().0.into());
			local_state.insert((event_type.to_string(), state_key.to_string()), eid.clone());
			// Store metadata for richer diff output
			{
				let content: JsonValue = pdu.get_content_as_value();
				let membership = content
					.get("membership")
					.and_then(|v| v.as_str())
					.unwrap_or("")
					.to_owned();
				event_meta.insert(eid.clone(), (membership, pdu.sender().to_string()))
			};

			if event_type == StateEventType::RoomMember {
				let content: JsonValue = pdu.get_content_as_value();
				let membership = content
					.get("membership")
					.and_then(|v| v.as_str())
					.unwrap_or("unknown");
				match membership {
					| "join" => local_state_joined = local_state_joined.saturating_add(1),
					| "invite" => {
						local_state_invited = local_state_invited.saturating_add(1);
					},
					| "leave" => {
						local_state_left = local_state_left.saturating_add(1);
					},
					| _ => {},
				}

				if let Some(ref ck) = conflict_key {
					if &*state_key == ck.1.as_str() {
						let displayname = content
							.get("displayname")
							.and_then(|v| v.as_str())
							.unwrap_or("(none)")
							.to_owned();
						let avatar = content
							.get("avatar_url")
							.and_then(|v| v.as_str())
							.unwrap_or("(none)")
							.to_owned();
						let ts: u64 = pdu.origin_server_ts().0.into();
						conflict_entries.push((
							"local".to_owned(),
							eid.to_string(),
							ts,
							membership.to_owned(),
							displayname,
							avatar,
						));
					}
				}
			}
		}
	}

	let mut missing_locally = Vec::new();
	for (key, event_id) in &remote_state {
		if local_state.get(key) != Some(event_id) {
			let ts = event_timestamps.get(event_id).copied().unwrap_or_else(|| {
				tip_pdu_opt
					.as_ref()
					.filter(|tip| tip.event_id() == event_id)
					.map_or(0, |tip| u64::from(tip.origin_server_ts))
			});
			let extra = fmt_event_meta(&key.0, event_id, &event_meta);
			missing_locally
				.push((ts, format!("{event_id} ({} {}) {}{extra}", key.0, key.1, format_ts(ts))));
		}
	}
	missing_locally.sort_by_key(|(ts, _)| *ts);

	let mut extra_locally = Vec::new();
	for (key, event_id) in &local_state {
		if remote_state.get(key) != Some(event_id) {
			let ts = event_timestamps.get(event_id).copied().unwrap_or_else(|| {
				tip_pdu_opt
					.as_ref()
					.filter(|tip| tip.event_id() == event_id)
					.map_or(0, |tip| u64::from(tip.origin_server_ts))
			});
			let extra = fmt_event_meta(&key.0, event_id, &event_meta);
			extra_locally
				.push((ts, format!("{event_id} ({} {}) {}{extra}", key.0, key.1, format_ts(ts))));
		}
	}
	extra_locally.sort_by_key(|(ts, _)| *ts);

	let cached_joined = self
		.services
		.rooms
		.state_cache
		.room_joined_count(&room_id)
		.await
		.unwrap_or(0);

	let latest_local = self
		.services
		.rooms
		.timeline
		.latest_pdu_in_room(&room_id)
		.await?;
	let latest_local_id = latest_local.event_id().to_owned();

	let extremity_count = self
		.services
		.rooms
		.state
		.get_forward_extremities(&room_id)
		.count()
		.await;

	let cache_status = if u64::try_from(local_state_joined).unwrap_or(0) == cached_joined {
		"✓"
	} else {
		"✗ MISMATCH"
	};

	let mut out = String::from("```\n");
	writeln!(
		out,
		"Room State Comparison for {room_id} vs {server}\nat_event (sent to remote): \
		 {at_event_id}\nlocal tip: {latest_local_id}\nMissing locally: {}\nExtra locally: \
		 {}\nSkipped (bad sig): {skipped}",
		missing_locally.len(),
		extra_locally.len()
	)?;
	writeln!(out)?;
	writeln!(out, "Room SSH:        {local_state_hash}")?;
	writeln!(out, "Extremities:     {extremity_count}")?;
	writeln!(
		out,
		"Local joined:    state={local_state_joined}, cache={cached_joined} {cache_status}"
	)?;
	writeln!(out, "Local invited:   state={local_state_invited}")?;
	writeln!(out, "Local left:      state={local_state_left}")?;
	writeln!(out, "Remote joined:   {}", remote_joined.len())?;
	writeln!(out, "Remote invited:  {}", remote_invited.len())?;
	writeln!(out, "Remote left:     {}", remote_left.len())?;
	if tip_is_state_event {
		writeln!(
			out,
			"NOTE: Tip is a state event — injected into remote state for state-after comparison"
		)?;
	}
	if !summary {
		writeln!(out)?;
		fmt_list(&mut out, "Missing IDs", &missing_locally)?;
		fmt_list(&mut out, "Extra IDs", &extra_locally)?;
	}
	writeln!(out, "```")?;
	self.write_str(&out).await?;

	// If additional servers provided, compare first server against each
	if servers.len() > 1 {
		let tip_key: Option<(String, String)> = tip_pdu_opt.as_ref().and_then(|pdu| {
			pdu.state_key
				.as_ref()
				.map(|sk| (pdu.kind.to_string(), sk.to_string()))
		});

		for cmp_server in &servers[1..] {
			let response = match self
				.services
				.sending
				.send_federation_request(cmp_server, get_room_state::v1::Request {
					room_id: room_id.clone(),
					event_id: at_event_id.clone(),
				})
				.await
			{
				| Ok(r) => r,
				| Err(e) => {
					self.write_str(&format!("\n--- vs {cmp_server}: ERROR: {e}\n"))
						.await?;
					continue;
				},
			};

			let mut server_state = HashMap::new();
			let mut verify_errors = 0_usize;
			let mut cmp_joined: HashSet<String> = HashSet::new();
			let mut cmp_invited: HashSet<String> = HashSet::new();
			let mut cmp_left: HashSet<String> = HashSet::new();
			for pdu_raw in &response.pdus {
				let (event_id, value) = match if skip_sig_verify {
					conduwuit::matrix::event::gen_event_id_canonical_json(pdu_raw, &room_version)
				} else {
					self.services
						.server_keys
						.validate_and_add_event_id(pdu_raw, &room_version)
						.await
				} {
					| Ok(r) => r,
					| Err(e) => {
						if let Ok((eid, val)) =
							conduwuit::matrix::event::gen_event_id_canonical_json(
								pdu_raw,
								&room_version,
							) {
							warn!(
								"compare_room_state: PDU {eid} failed verification, storing as \
								 rejected outlier: {e}"
							);
							self.services.rooms.outlier.add_pdu_outlier(
								&eid,
								&val,
								Some(&room_id),
							);
							self.services
								.rooms
								.pdu_metadata
								.mark_event_rejected(
									&eid,
									"signature verification failed in compare-room-state",
								)
								.await;
							// Still count membership — remote sent this as
							// part of their state.
							if let Ok(pdu) =
								PduEvent::from_id_val(&eid, val, Some(room_id.as_ref()))
							{
								event_timestamps
									.insert(eid.clone(), u64::from(pdu.origin_server_ts));
								if let Some(state_key) = &pdu.state_key {
									server_state.insert(
										(pdu.kind.to_string(), state_key.to_string()),
										eid.clone(),
									);
									if !event_meta.contains_key(&eid) {
										let content: JsonValue = pdu.get_content_as_value();
										let membership = content
											.get("membership")
											.and_then(|v| v.as_str())
											.unwrap_or("")
											.to_owned();
										event_meta.insert(
											eid.clone(),
											(membership, pdu.sender().to_string()),
										);
									}
								}
								if pdu.kind == TimelineEventType::RoomMember {
									if let Some(state_key) = &pdu.state_key {
										let content: JsonValue = pdu.get_content_as_value();
										let membership = content
											.get("membership")
											.and_then(|v| v.as_str())
											.unwrap_or("unknown");
										match membership {
											| "join" => {
												cmp_joined.insert(state_key.to_string());
												cmp_invited.remove(state_key.as_str());
												cmp_left.remove(state_key.as_str());
											},
											| "invite" => {
												cmp_invited.insert(state_key.to_string());
												cmp_joined.remove(state_key.as_str());
												cmp_left.remove(state_key.as_str());
											},
											| "leave" => {
												cmp_left.insert(state_key.to_string());
												cmp_joined.remove(state_key.as_str());
												cmp_invited.remove(state_key.as_str());
											},
											| _ => {
												cmp_joined.remove(state_key.as_str());
												cmp_invited.remove(state_key.as_str());
												cmp_left.remove(state_key.as_str());
											},
										}
									}
								}
							}
						}
						verify_errors = verify_errors.saturating_add(1);
						continue;
					},
				};
				let Ok(pdu) = PduEvent::from_id_val(&event_id, value, Some(room_id.as_ref()))
				else {
					continue;
				};
				event_timestamps.insert(event_id.clone(), u64::from(pdu.origin_server_ts));
				if let Some(state_key) = &pdu.state_key {
					server_state
						.insert((pdu.kind.to_string(), state_key.to_string()), event_id.clone());

					// Store metadata for richer diff output
					if !event_meta.contains_key(&event_id) {
						let content: JsonValue = pdu.get_content_as_value();
						let membership = content
							.get("membership")
							.and_then(|v| v.as_str())
							.unwrap_or("")
							.to_owned();
						event_meta
							.insert(event_id.clone(), (membership, pdu.sender().to_string()));
					}

					if pdu.kind == TimelineEventType::RoomMember {
						let content: JsonValue = pdu.get_content_as_value();
						let membership = content
							.get("membership")
							.and_then(|v| v.as_str())
							.unwrap_or("unknown");
						match membership {
							| "join" => {
								cmp_joined.insert(state_key.to_string());
								cmp_invited.remove(state_key.as_str());
								cmp_left.remove(state_key.as_str());
							},
							| "invite" => {
								cmp_invited.insert(state_key.to_string());
								cmp_joined.remove(state_key.as_str());
								cmp_left.remove(state_key.as_str());
							},
							| "leave" => {
								cmp_left.insert(state_key.to_string());
								cmp_joined.remove(state_key.as_str());
								cmp_invited.remove(state_key.as_str());
							},
							| _ => {
								cmp_joined.remove(state_key.as_str());
								cmp_invited.remove(state_key.as_str());
								cmp_left.remove(state_key.as_str());
							},
						}

						if let Some(ref ck) = conflict_key {
							if *state_key == ck.1 {
								let displayname = content
									.get("displayname")
									.and_then(|v| v.as_str())
									.unwrap_or("(none)")
									.to_owned();
								let avatar = content
									.get("avatar_url")
									.and_then(|v| v.as_str())
									.unwrap_or("(none)")
									.to_owned();
								let ts = u64::from(pdu.origin_server_ts);
								conflict_entries.push((
									cmp_server.to_string(),
									event_id.to_string(),
									ts,
									membership.to_owned(),
									displayname,
									avatar,
								));
							}
						}
					}
				}
			}

			if let Some(ref key) = tip_key {
				server_state.insert(key.clone(), at_event_id.clone());
				if let Some(ref tip_pdu) = tip_pdu_opt {
					if tip_pdu.kind == TimelineEventType::RoomMember {
						let content: JsonValue = tip_pdu.get_content_as_value();
						match content.get("membership").and_then(|v| v.as_str()) {
							| Some("join") => {
								cmp_joined.insert(key.1.clone());
								cmp_invited.remove(&key.1);
								cmp_left.remove(&key.1);
							},
							| Some("invite") => {
								cmp_invited.insert(key.1.clone());
								cmp_joined.remove(&key.1);
								cmp_left.remove(&key.1);
							},
							| Some("leave") => {
								cmp_left.insert(key.1.clone());
								cmp_joined.remove(&key.1);
								cmp_invited.remove(&key.1);
							},
							| _ => {
								cmp_joined.remove(&key.1);
								cmp_invited.remove(&key.1);
								cmp_left.remove(&key.1);
							},
						}
					}
				}
			}

			let mut only_on_first = Vec::new();
			for (key, event_id) in &remote_state {
				if server_state.get(key) != Some(event_id) {
					let ts = event_timestamps.get(event_id).copied().unwrap_or_else(|| {
						tip_pdu_opt
							.as_ref()
							.filter(|tip| tip.event_id() == event_id)
							.map_or(0, |tip| u64::from(tip.origin_server_ts))
					});
					let extra = fmt_event_meta(&key.0, event_id, &event_meta);
					only_on_first.push((
						ts,
						format!("{event_id} ({} {}) {}{extra}", key.0, key.1, format_ts(ts)),
					));
				}
			}
			only_on_first.sort_by_key(|(ts, _)| *ts);

			let mut only_on_cmp = Vec::new();
			for (key, event_id) in &server_state {
				if remote_state.get(key) != Some(event_id) {
					let ts = event_timestamps.get(event_id).copied().unwrap_or_else(|| {
						tip_pdu_opt
							.as_ref()
							.filter(|tip| tip.event_id() == event_id)
							.map_or(0, |tip| u64::from(tip.origin_server_ts))
					});
					let extra = fmt_event_meta(&key.0, event_id, &event_meta);
					only_on_cmp.push((
						ts,
						format!("{event_id} ({} {}) {}{extra}", key.0, key.1, format_ts(ts)),
					));
				}
			}
			only_on_cmp.sort_by_key(|(ts, _)| *ts);

			let mut section = format!(
				"```\n--- {server} vs {cmp_server}:\nOnly on {server}: {}  Only on \
				 {cmp_server}: {}\n{cmp_server} joined: {}, invited: {}, left: {}\n",
				only_on_first.len(),
				only_on_cmp.len(),
				cmp_joined.len(),
				cmp_invited.len(),
				cmp_left.len()
			);
			if verify_errors > 0 {
				writeln!(section, "Skipped (bad sig): {verify_errors}")?;
			}
			if !summary {
				fmt_list(&mut section, &format!("IDs only on {server}"), &only_on_first)?;
				fmt_list(&mut section, &format!("IDs only on {cmp_server}"), &only_on_cmp)?;
			}
			writeln!(section, "```")?;
			self.write_str(&section).await?;
		}
	}
	// Output conflict summary if requested
	if let Some(ref user) = conflict {
		if !conflict_entries.is_empty() {
			use std::fmt::Write;

			let mut out = format!("\n--- Conflict detail for {user}:\n```\n");
			for (srv, eid, ts, membership, displayname, avatar) in &conflict_entries {
				writeln!(out, "{srv}:")?;
				writeln!(out, "  event:       {eid}")?;
				writeln!(out, "  timestamp:   {}", format_ts(*ts))?;
				writeln!(out, "  membership:  {membership}")?;
				writeln!(out, "  displayname: {displayname}")?;
				writeln!(out, "  avatar_url:  {avatar}")?;
			}
			writeln!(out, "```")?;
			self.write_str(&out).await?;
		} else {
			self.write_str(&format!("\n--- Conflict: {user} not found in any server state\n"))
				.await?;
		}
	}

	Ok(())
}

fn fmt_list(out: &mut String, label: &str, items: &[(u64, String)]) -> std::fmt::Result {
	use std::fmt::Write;

	write!(out, "{label}: [")?;
	for (_, item) in items {
		write!(out, "\n  {item}")?;
	}
	writeln!(out, "{}]", if items.is_empty() { "" } else { "\n" })
}

/// Returns a suffix like " [join]" or " [by @admin:hs]" for richer diff output.
fn fmt_event_meta(
	event_type: &str,
	event_id: &OwnedEventId,
	meta: &HashMap<OwnedEventId, (String, String)>,
) -> String {
	let Some((membership, sender)) = meta.get(event_id) else {
		return String::new();
	};
	match event_type {
		| "m.room.member" if !membership.is_empty() => format!(" [{membership}]"),
		| "m.room.power_levels" => format!(" [by {sender}]"),
		| _ => String::new(),
	}
}

#[admin_command]
pub(super) async fn set_state_event(
	&self,
	room_id: OwnedRoomId,
	event_type: String,
	event_id: OwnedEventId,
	state_key: String,
) -> Result {
	use conduwuit_service::rooms::state_compressor::CompressedState;

	self.bail_restricted()?;

	// TODO: transactionalize this whole function for safety & idempotency
	let event_type: StateEventType = event_type.into();

	// Verify the event exists locally (timeline or outlier)
	let pdu = match self.services.rooms.timeline.get_pdu(&event_id).await {
		| Ok(pdu) => pdu,
		| Err(_) => {
			// Try outlier: get the JSON and parse it
			let json = self
				.services
				.rooms
				.outlier
				.get_outlier_pdu_json(&event_id)
				.await
				.map_err(|_| err!(Request(NotFound("Event {event_id} not found locally"))))?;
			serde_json::from_value::<PduEvent>(
				serde_json::to_value(&json).map_err(|e| err!(Request(InvalidParam("{e}"))))?,
			)
			.map_err(|e| err!(Request(InvalidParam("Failed to parse outlier: {e}"))))?
		},
	};

	// Verify it matches the claimed type/state_key
	if pdu.kind.to_string() != event_type.to_string() {
		return Err!(Request(InvalidParam(
			"Event type mismatch: expected {event_type}, got {}",
			pdu.kind
		)));
	}
	if pdu.state_key.as_deref() != Some(&*state_key) {
		return Err!(Request(InvalidParam(
			"State key mismatch: expected {state_key:?}, got {:?}",
			pdu.state_key
		)));
	}

	// Automatically clear rejected/soft-failed flags — if we're explicitly
	// promoting this event to current state, these flags are contradictory.
	self.services
		.rooms
		.pdu_metadata
		.unmark_event_rejected(&event_id);
	self.services
		.rooms
		.pdu_metadata
		.unmark_event_soft_failed(&event_id);

	let state_lock = self.services.rooms.state.mutex.lock(&room_id).await;

	// Get current state
	let current_shortstatehash = self
		.services
		.rooms
		.state
		.get_room_shortstatehash(&room_id)
		.await
		.map_err(|_| err!(Request(NotFound("Room has no state"))))?;

	// Get the full state as (shortstatekey, event_id) pairs
	let current_state: HashMap<u64, OwnedEventId> = self
		.services
		.rooms
		.state_accessor
		.state_full_ids(current_shortstatehash)
		.collect()
		.await;

	// Build new compressed state
	let target_shortstatekey = self
		.services
		.rooms
		.short
		.get_or_create_shortstatekey(&event_type, &state_key)
		.await;

	let mut new_state = CompressedState::new();

	for (shortstatekey, eid) in &current_state {
		if *shortstatekey == target_shortstatekey {
			// Replace with our target event
			let compressed = self
				.services
				.rooms
				.state_compressor
				.compress_state_event(*shortstatekey, &event_id)
				.await;
			new_state.insert(compressed);
		} else {
			let compressed = self
				.services
				.rooms
				.state_compressor
				.compress_state_event(*shortstatekey, eid)
				.await;
			new_state.insert(compressed);
		}
	}

	// If the (type, state_key) wasn't in the current state, add it
	if !current_state.contains_key(&target_shortstatekey) {
		let compressed = self
			.services
			.rooms
			.state_compressor
			.compress_state_event(target_shortstatekey, &event_id)
			.await;
		new_state.insert(compressed);
	}

	// Save the new state
	let new_state = std::sync::Arc::new(new_state);
	let new_shortstatehash = self
		.services
		.rooms
		.state
		.set_event_state(&event_id, &room_id, new_state)
		.await?;

	self.services
		.rooms
		.state
		.set_room_state(&room_id, new_shortstatehash, &state_lock);

	// Rebuild membership cache if this is a member event
	if event_type == StateEventType::RoomMember {
		if let Ok(user_id) = ruma::UserId::parse(&state_key) {
			self.services
				.rooms
				.state_cache
				.update_membership(&room_id, user_id, &pdu, false)
				.await?;
		}
		self.services
			.rooms
			.state_cache
			.update_joined_count(&room_id)
			.await;
	}

	let membership = if event_type == StateEventType::RoomMember {
		pdu.content
			.get()
			.parse::<serde_json::Value>()
			.ok()
			.and_then(|c: serde_json::Value| {
				c.get("membership")
					.and_then(|m| m.as_str().map(String::from))
			})
			.unwrap_or_default()
	} else {
		String::new()
	};

	let out = format!(
		"Set ({event_type}, {state_key:?}) => {event_id}{}\n",
		if membership.is_empty() {
			String::new()
		} else {
			format!(" (membership: {membership})")
		}
	);
	info!("{out}");
	self.write_str(&out).await
}

#[admin_command]
pub(super) async fn rebuild_membership_cache(&self, room_id: OwnedRoomId) -> Result {
	let _state_lock = self.services.rooms.state.mutex.lock(&room_id).await;
	self.services
		.rooms
		.state_cache
		.reconcile_membership(&room_id)
		.await;

	let out = format!("Rebuilt membership cache for {room_id}");
	self.write_str(&out).await
}

#[admin_command]
pub(super) async fn audit_membership(
	&self,
	room_id: OwnedRoomId,
	server: Option<OwnedServerName>,
	at_event: Option<OwnedEventId>,
	clean: bool,
) -> Result {
	let audit_started = Instant::now();
	info!("audit-membership: start room_id={room_id} clean={clean} remote={server:?}");

	// ── Phase 1: Timeline vs State Snapshot ──────────────────────────────
	self.write_str("**Phase 1: Timeline vs State Snapshot**\n")
		.await?;

	let timeline_scan_started = Instant::now();
	let mut timeline_membership: HashMap<OwnedUserId, (String, String)> = HashMap::new();

	let pdus = self
		.services
		.rooms
		.timeline
		.pdus(&room_id, std::ops::Bound::Excluded(PduCount::min()));

	pin_mut!(pdus);
	let mut timeline_count = 0_usize;

	while let Some(Ok((_count, pdu))) = pdus.next().await {
		if pdu.kind != TimelineEventType::RoomMember {
			continue;
		}

		let Some(state_key) = pdu.state_key() else {
			continue;
		};

		let content: JsonValue = pdu.get_content_as_value();
		let membership = content
			.get("membership")
			.and_then(|v| v.as_str())
			.unwrap_or("unknown")
			.to_owned();

		let event_id = pdu.event_id().to_string();

		if let Ok(user_id) = OwnedUserId::try_from(state_key) {
			timeline_membership.insert(user_id, (membership, event_id));
		}

		timeline_count = timeline_count.saturating_add(1);
	}
	info!(
		"audit-membership: timeline scan room_id={room_id} events={timeline_count} \
		 unique_users={} elapsed={:?}",
		timeline_membership.len(),
		timeline_scan_started.elapsed()
	);

	let state_hash = self
		.services
		.rooms
		.state
		.get_room_shortstatehash(&room_id)
		.await?;

	let state = self.services.rooms.state_accessor.state_full(state_hash);

	let state_scan_started = Instant::now();
	pin_mut!(state);
	let mut state_membership: HashMap<OwnedUserId, (String, String)> = HashMap::new();

	while let Some(((event_type, state_key), pdu)) = state.next().await {
		if event_type != StateEventType::RoomMember {
			continue;
		}

		let content: JsonValue = pdu.get_content_as_value();
		let membership = content
			.get("membership")
			.and_then(|v| v.as_str())
			.unwrap_or("unknown")
			.to_owned();

		let event_id = pdu.event_id().to_string();

		if let Ok(user_id) = OwnedUserId::try_from(state_key.as_str()) {
			state_membership.insert(user_id, (membership, event_id));
		}
	}
	info!(
		"audit-membership: state snapshot room_id={room_id} users={} elapsed={:?}",
		state_membership.len(),
		state_scan_started.elapsed()
	);

	let mut divergences = Vec::new();
	let mut total_purged = 0_usize;
	let max_clean_passes = 50_usize;
	let mut pass_num = 0_usize;

	loop {
		pass_num = pass_num.saturating_add(1);
		let mut pass_purged = 0_usize;
		let clean_pass_started = Instant::now();

		// Rebuild timeline membership for this pass
		let mut tl_membership_pass: HashMap<OwnedUserId, (String, String)> = HashMap::new();
		let pdus_pass = self
			.services
			.rooms
			.timeline
			.pdus(&room_id, std::ops::Bound::Excluded(PduCount::min()));

		pin_mut!(pdus_pass);
		while let Some(Ok((_count, pdu))) = pdus_pass.next().await {
			if pdu.kind != TimelineEventType::RoomMember {
				continue;
			}
			let Some(state_key) = pdu.state_key() else {
				continue;
			};
			let membership = pdu
				.get_content_as_value()
				.get("membership")
				.and_then(|v| v.as_str())
				.unwrap_or("leave")
				.to_owned();
			let event_id = pdu.event_id().to_string();
			if let Ok(user_id) = OwnedUserId::try_from(state_key) {
				tl_membership_pass.insert(user_id, (membership, event_id));
			}
		}

		for (user_id, (tl_membership, tl_event)) in &tl_membership_pass {
			let is_divergent = match state_membership.get(user_id) {
				// Membership type differs (join vs leave, etc) — always divergent
				| Some((st_membership, _)) if st_membership != tl_membership => true,
				// Same membership but different event ID — only divergent for leave/ban
				// (multiple join events with different IDs are just renames)
				| Some((st_membership, st_event))
					if st_event != tl_event
						&& (st_membership == "leave" || st_membership == "ban") =>
					true,
				// User in timeline but absent from state
				| None if tl_membership == "join" || tl_membership == "invite" => true,
				| _ => false,
			};

			if is_divergent && clean {
				if let Ok(event_id) = OwnedEventId::try_from(tl_event.as_str()) {
					if let Ok(pdu_json) =
						self.services.rooms.timeline.get_pdu_json(&event_id).await
					{
						self.services.rooms.outlier.add_pdu_outlier(
							&event_id,
							&pdu_json,
							Some(&room_id),
						);
					}
					// remove_from_timeline demotes back to outlier.
					// no additional flag needed; rescue-room will re-evaluate.
					self.services
						.rooms
						.timeline
						.remove_from_timeline(&event_id)
						.await;

					pass_purged = pass_purged.saturating_add(1);
					total_purged = total_purged.saturating_add(1);

					if total_purged <= 100 {
						divergences.push(format!(
							"PURGED {user_id}: demoted `{tl_membership}` (via {tl_event}) to \
							 outlier",
						));
					}
					continue;
				}
			}

			if !clean {
				// Original diagnostic output (only on non-clean runs)
				match state_membership.get(user_id) {
					| Some((st_membership, st_event)) if st_membership != tl_membership => {
						divergences.push(format!(
							"WARN {user_id}: timeline says `{tl_membership}` (via {tl_event}) \
							 but state says `{st_membership}` (via {st_event})"
						));
					},
					| Some((_, st_event)) if st_event != tl_event => {
						divergences.push(format!(
							"DIFF {user_id}: `{tl_membership}` {tl_event} {st_event}"
						));
					},
					| None if tl_membership == "join" || tl_membership == "invite" => {
						divergences.push(format!(
							"MISSING {user_id}: timeline says `{tl_membership}` (via \
							 {tl_event}) but user is ABSENT from state snapshot"
						));
					},
					| _ => {},
				}
			}
		}

		if !clean || pass_purged == 0 || pass_num >= max_clean_passes {
			// Update timeline_membership from the final pass for ghost counting
			timeline_membership = tl_membership_pass;
			if clean && pass_num >= max_clean_passes && pass_purged > 0 {
				divergences.push(format!(
					"WARN: hit max {max_clean_passes} clean passes, {total_purged} total purged \
					 — remaining divergences may need manual inspection"
				));
			}
			break;
		}

		info!(
			"audit-membership: clean pass room_id={room_id} pass={} purged={} elapsed={:?}",
			pass_num,
			pass_purged,
			clean_pass_started.elapsed()
		);
	}

	if clean && total_purged > 100 {
		divergences.push(format!(
			"... and {} more purged (truncated)",
			total_purged.saturating_sub(100)
		));
	}

	// Count ghosts (federation imports with no local timeline events) by
	// membership — these are expected, not actionable anomalies.
	let mut ghost_count = 0_usize;
	let mut ghost_joined = 0_usize;
	let mut ghost_left = 0_usize;
	let mut ghost_banned = 0_usize;
	for (user_id, (st_membership, _)) in &state_membership {
		if !timeline_membership.contains_key(user_id) {
			ghost_count = ghost_count.saturating_add(1);
			match st_membership.as_str() {
				| "join" => ghost_joined = ghost_joined.saturating_add(1),
				| "leave" => ghost_left = ghost_left.saturating_add(1),
				| "ban" => ghost_banned = ghost_banned.saturating_add(1),
				| _ => {},
			}
		}
	}

	let mut out = format!(
		"Phase 1 for {room_id}:\n- Timeline membership events: {timeline_count}\n- Unique users \
		 in timeline: {}\n- Users in state snapshot: {}\n- Ghosts (federation imports, no \
		 timeline): {ghost_count} (joined={ghost_joined}, left={ghost_left}, \
		 banned={ghost_banned})\n",
		timeline_membership.len(),
		state_membership.len(),
	);

	if divergences.is_empty() {
		writeln!(out, "\nNo actionable divergences.").expect("fmt");
	} else {
		writeln!(out, "\n**{} actionable divergence(s):**", divergences.len()).expect("fmt");
		for d in &divergences {
			writeln!(out, "- {d}").expect("fmt");
		}
	}

	self.write_str(&out).await?;
	info!(
		"audit-membership: phase1 complete room_id={room_id} divergences={} total_purged={} \
		 elapsed={:?}",
		divergences.len(),
		total_purged,
		audit_started.elapsed()
	);

	// ── Phase 2: State Snapshot vs Cache ─────────────────────────────────
	self.write_str("\n**Phase 2: State Snapshot vs Cache**\n")
		.await?;

	let cache_phase_started = Instant::now();
	let mut state_joined: HashSet<OwnedUserId> = HashSet::new();
	let mut state_invited: HashSet<OwnedUserId> = HashSet::new();
	let mut state_left = 0_usize;
	let mut state_banned = 0_usize;
	let mut state_knocked = 0_usize;

	for (user_id, (membership, _)) in &state_membership {
		match membership.as_str() {
			| "join" => {
				state_joined.insert(user_id.clone());
			},
			| "invite" => {
				state_invited.insert(user_id.clone());
			},
			| "leave" => state_left = state_left.saturating_add(1),
			| "ban" => state_banned = state_banned.saturating_add(1),
			| "knock" => state_knocked = state_knocked.saturating_add(1),
			| _ => {},
		}
	}

	let cached_joined = self
		.services
		.rooms
		.state_cache
		.room_joined_count(&room_id)
		.await
		.unwrap_or(0);

	let cached_invited = self
		.services
		.rooms
		.state_cache
		.room_invited_count(&room_id)
		.await
		.unwrap_or(0);

	// Collect actual cache members for bidirectional comparison
	let cached_joined_members: HashSet<OwnedUserId> = self
		.services
		.rooms
		.state_cache
		.room_members(&room_id)
		.map(ToOwned::to_owned)
		.collect()
		.await;
	info!(
		"audit-membership: cache joined members room_id={room_id} count={} elapsed={:?}",
		cached_joined_members.len(),
		cache_phase_started.elapsed()
	);

	let cached_invited_members: HashSet<OwnedUserId> = self
		.services
		.rooms
		.state_cache
		.room_members_invited(&room_id)
		.map(ToOwned::to_owned)
		.collect()
		.await;
	info!(
		"audit-membership: cache invited members room_id={room_id} count={} elapsed={:?}",
		cached_invited_members.len(),
		cache_phase_started.elapsed()
	);

	let mut cache_mismatches = Vec::new();

	// Check state → cache (MISSING: in state but not cache)
	for user_id in &state_joined {
		if !cached_joined_members.contains(user_id) {
			cache_mismatches
				.push(format!("MISSING {user_id}: state says JOINED but cache says NOT joined"));
		}
	}

	for user_id in &state_invited {
		if !cached_invited_members.contains(user_id) {
			cache_mismatches.push(format!(
				"MISSING {user_id}: state says INVITED but cache says NOT invited"
			));
		}
	}

	// Check cache → state (EXTRA: in cache but not state)
	for user_id in &cached_joined_members {
		if !state_joined.contains(user_id) {
			cache_mismatches
				.push(format!("EXTRA {user_id}: cache says JOINED but state says NOT joined"));
		}
	}

	for user_id in &cached_invited_members {
		if !state_invited.contains(user_id) {
			cache_mismatches
				.push(format!("EXTRA {user_id}: cache says INVITED but state says NOT invited"));
		}
	}

	let total_members = state_joined
		.len()
		.saturating_add(state_invited.len())
		.saturating_add(state_left)
		.saturating_add(state_banned)
		.saturating_add(state_knocked);

	let counts_line = format!(
		"- Total members in state: {total_members}\n- Joined: state={}, \
		 cache={cached_joined}\n- Invited: state={}, cache={cached_invited}\n- Left: \
		 state={state_left}\n- Banned: state={state_banned}\n- Knocked: state={state_knocked}",
		state_joined.len(),
		state_invited.len()
	);

	if cache_mismatches.is_empty() {
		self.write_str(&format!(
			"OK: Membership cache is consistent for {room_id}\n{counts_line}"
		))
		.await?;
	} else {
		let mut out = format!(
			"Membership cache divergences for {room_id}:\n{counts_line}\n\n**{} \
			 mismatch(es):**\n",
			cache_mismatches.len()
		);

		for m in cache_mismatches.iter().take(100) {
			writeln!(out, "- {m}").expect("fmt");
		}
		if cache_mismatches.len() > 100 {
			writeln!(out, "- ... and {} more", cache_mismatches.len().saturating_sub(100))
				.expect("fmt");
		}

		self.write_str(&out).await?;
	}
	info!(
		"audit-membership: phase2 compare room_id={room_id} mismatches={} state_joined={} \
		 state_invited={} elapsed={:?}",
		cache_mismatches.len(),
		state_joined.len(),
		state_invited.len(),
		cache_phase_started.elapsed()
	);

	// ── Phase 2.5: Aggregate count cross-check + active healing ──────────
	let state_joined_count: u64 = state_joined
		.len()
		.try_into()
		.expect("joined count overflow");
	let state_invited_count: u64 = state_invited
		.len()
		.try_into()
		.expect("invited count overflow");
	let cached_joined_u64 = self
		.services
		.rooms
		.state_cache
		.room_joined_count(&room_id)
		.await
		.unwrap_or(0);
	let cached_invited_u64 = cached_invited;

	if cached_joined_u64 != state_joined_count
		|| cached_invited_u64 != state_invited_count
		|| !cache_mismatches.is_empty()
	{
		let heal_started = Instant::now();
		let mut healed_extra_joined = 0_usize;
		let mut healed_extra_invited = 0_usize;
		let mut healed_missing_joined = 0_usize;
		let mut healed_missing_invited = 0_usize;
		self.write_str(&format!(
			"\n✗ CACHE INCONSISTENCY (joined state: {state_joined_count}, joined cache: \
			 {cached_joined_u64}, invited state: {state_invited_count}, invited cache: \
			 {cached_invited_u64}, mismatches: {}). Healing…",
			cache_mismatches.len(),
		))
		.await?;

		if cache_mismatches.is_empty() {
			self.services.db["roomid_joinedcount"].raw_put(&room_id, state_joined_count);
			self.services.db["roomid_invitedcount"].raw_put(&room_id, state_invited_count);
			self.write_str("\n✓ Cache repaired.\n").await?;
			info!(
				"audit-membership: count-only heal room_id={room_id} joined={} invited={} elapsed={:?}",
				state_joined_count,
				state_invited_count,
				heal_started.elapsed()
			);
			return Ok(());
		}

		// Heal EXTRA users (in cache but not state)
		for user_id in &cached_joined_members {
			if !state_joined.contains(user_id) {
				healed_extra_joined = healed_extra_joined.saturating_add(1);
				self.services
					.rooms
					.state_cache
					.mark_as_left_silent(user_id, &room_id)
					.await;
			}
		}
		for user_id in &cached_invited_members {
			if !state_invited.contains(user_id) {
				healed_extra_invited = healed_extra_invited.saturating_add(1);
				self.services
					.rooms
					.state_cache
					.mark_as_left_silent(user_id, &room_id)
					.await;
			}
		}

		// Heal MISSING users (in state but not cache)
		for user_id in &state_joined {
			if !cached_joined_members.contains(user_id) {
				healed_missing_joined = healed_missing_joined.saturating_add(1);
				self.services
					.rooms
					.state_cache
					.mark_as_joined_silent(user_id, &room_id)
					.await;
			}
		}

		for user_id in &state_invited {
			if !cached_invited_members.contains(user_id) {
				healed_missing_invited = healed_missing_invited.saturating_add(1);
				// Heal invite by fetching the actual PDU from the authoritative state
				if let Ok(pdu) = self
					.services
					.rooms
					.state_accessor
					.state_get(state_hash, &StateEventType::RoomMember, user_id.as_str())
					.await
				{
					let _ = self
						.services
						.rooms
						.state_cache
						.update_membership(&room_id, user_id, &pdu, false)
						.await;
				}
			}
		}

		self.services
			.rooms
			.state_cache
			.update_joined_count(&room_id)
			.await;
		self.write_str("\n✓ Cache repaired.\n").await?;
		info!(
			"audit-membership: cache heal room_id={room_id} extra_joined={} extra_invited={} \
			 missing_joined={} missing_invited={} elapsed={:?}",
			healed_extra_joined,
			healed_extra_invited,
			healed_missing_joined,
			healed_missing_invited,
			heal_started.elapsed()
		);
	}

	// ── Phase 3: Remote comparison (optional) ────────────────────────────
	if let Some(ref server) = server {
		self.write_str(&format!("\n**Phase 3: Local vs Remote ({server})**\n"))
			.await?;

		let latest_event_id = match at_event {
			| Some(ref eid) => eid.clone(),
			| None => self
				.services
				.rooms
				.timeline
				.latest_pdu_in_room(&room_id)
				.await?
				.event_id()
				.to_owned(),
		};

		match self
			.services
			.sending
			.send_federation_request(server, get_room_state::v1::Request {
				room_id: room_id.clone(),
				event_id: latest_event_id.clone(),
			})
			.await
		{
			| Ok(response) => {
				let room_version = self.services.rooms.state.get_room_version(&room_id).await?;

				let mut remote_members: HashMap<String, String> = HashMap::new();
				let mut sig_failed: usize = 0;
				let mut parse_failed: usize = 0;

				for pdu_raw in &response.pdus {
					let (event_id, value) = match self
						.services
						.server_keys
						.validate_and_add_event_id(pdu_raw, &room_version)
						.await
					{
						| Ok(r) => r,
						| Err(e) => {
							sig_failed = sig_failed.saturating_add(1);
							if let Ok((eid, val)) =
								conduwuit::matrix::event::gen_event_id_canonical_json(
									pdu_raw,
									&room_version,
								) {
								warn!(
									"audit_membership: PDU {eid} failed sig verify, storing as \
									 rejected outlier: {e}"
								);
								self.services.rooms.outlier.add_pdu_outlier(
									&eid,
									&val,
									Some(&room_id),
								);
								self.services
									.rooms
									.pdu_metadata
									.mark_event_rejected(
										&eid,
										"signature verification failed in rescue-room",
									)
									.await;
							}
							continue;
						},
					};

					let Ok(pdu) = PduEvent::from_id_val(&event_id, value, Some(room_id.as_ref()))
					else {
						parse_failed = parse_failed.saturating_add(1);
						continue;
					};

					if pdu.kind != TimelineEventType::RoomMember {
						continue;
					}

					if let Some(state_key) = pdu.state_key() {
						let content: JsonValue = pdu.get_content_as_value();
						let membership = content
							.get("membership")
							.and_then(|v| v.as_str())
							.unwrap_or("unknown")
							.to_owned();

						remote_members.insert(state_key.to_owned(), membership);
					}
				}

				// Inject the tip event into remote_members, if it is a member event!

				if let Ok(tip_pdu) = self.services.rooms.timeline.get_pdu(&latest_event_id).await
				{
					if tip_pdu.kind == TimelineEventType::RoomMember {
						if let Some(state_key) = tip_pdu.state_key() {
							let content: JsonValue = tip_pdu.get_content_as_value();

							let membership = content
								.get("membership")
								.and_then(|v| v.as_str())
								.unwrap_or("unknown")
								.to_owned();

							remote_members.insert(state_key.to_owned(), membership);
						}
					}
				}

				let mut local_members: HashMap<String, (String, String)> = HashMap::new();

				for (user_id, (membership, eid)) in &state_membership {
					local_members.insert(user_id.to_string(), (membership.clone(), eid.clone()));
				}

				let remote_joined = remote_members.values().filter(|m| *m == "join").count();
				let remote_invited = remote_members.values().filter(|m| *m == "invite").count();
				let remote_left = remote_members.values().filter(|m| *m == "leave").count();
				let remote_banned = remote_members.values().filter(|m| *m == "ban").count();
				self.write_str(&format!(
					"Remote {server}: {} total member events, joined={remote_joined}, \
					 invited={remote_invited}, left={remote_left}, banned={remote_banned}\n",
					remote_members.len()
				))
				.await?;

				let mut remote_diffs: Vec<(u64, String)> = Vec::new();

				let now_secs = std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap_or_default()
					.as_secs();

				let format_age = |age_secs: u64| -> String {
					let days = age_secs / 86400;
					let hours = age_secs / 3600;
					if age_secs > 86400 {
						format!("{days}d ago")
					} else if age_secs > 3600 {
						format!("{hours}h ago")
					} else {
						format!("{age_secs}s ago")
					}
				};

				for (user, remote_ms) in &remote_members {
					match local_members.get(user) {
						| Some((local_ms, eid)) if local_ms != remote_ms => {
							let age_secs = if let Ok(eid) = OwnedEventId::parse(eid) {
								self.services
									.rooms
									.timeline
									.get_pdu(&eid)
									.await
									.ok()
									.map_or(u64::MAX, |p| {
										let ms = u64::from(p.origin_server_ts);
										now_secs.saturating_sub(ms / 1000)
									})
							} else {
								u64::MAX
							};
							let age = if age_secs < u64::MAX {
								format_age(age_secs)
							} else {
								String::from("unknown")
							};
							remote_diffs.push((
								age_secs,
								format!(
									"WARN {user}: local=`{local_ms}`, {server}=`{remote_ms}` \
									 (event: {eid}, {age})"
								),
							));
						},
						| None if remote_ms == "join" || remote_ms == "invite" => {
							remote_diffs.push((
								u64::MAX,
								format!(
									"MISSING {user}: ABSENT locally but {server} says \
									 `{remote_ms}`"
								),
							));
						},
						| _ => {},
					}
				}

				for (user, (local_ms, eid)) in &local_members {
					if !remote_members.contains_key(user)
						&& (local_ms == "join" || local_ms == "invite")
					{
						let age_secs = if let Ok(eid) = OwnedEventId::parse(eid) {
							self.services
								.rooms
								.timeline
								.get_pdu(&eid)
								.await
								.ok()
								.map_or(u64::MAX, |p| {
									let ms = u64::from(p.origin_server_ts);
									now_secs.saturating_sub(ms / 1000)
								})
						} else {
							u64::MAX
						};
						let age = if age_secs < u64::MAX {
							format_age(age_secs)
						} else {
							String::from("unknown")
						};
						remote_diffs.push((
							age_secs,
							format!(
								"GHOST {user}: local says `{local_ms}` but ABSENT on {server} \
								 (event: {eid}, {age})"
							),
						));
					}
				}

				// Sort newest first (smallest age_secs first)
				remote_diffs.sort_by_key(|(age, _)| *age);

				let failure_summary = if sig_failed > 0 || parse_failed > 0 {
					format!(" (sig_failed={sig_failed}, parse_failed={parse_failed})")
				} else {
					String::new()
				};

				if remote_diffs.is_empty() {
					self.write_str(&format!(
						"OK: Local and {server} agree on membership ({} members, \
						 joined={remote_joined}, invited={remote_invited}, left={remote_left}, \
						 banned={remote_banned}){failure_summary}",
						remote_members.len()
					))
					.await?;
				} else {
					let mut out = format!(
						"Remote membership diffs vs {server} ({} diff(s)){failure_summary}:\n",
						remote_diffs.len()
					);
					for (_, d) in &remote_diffs {
						writeln!(out, "- {d}").expect("fmt");
					}
					self.write_str(&out).await?;
				}
			},
			| Err(e) => {
				self.write_str(&format!("Failed to fetch state from {server}: {e}"))
					.await?;
			},
		}
	}

	info!(
		"audit-membership: complete room_id={room_id} total_elapsed={:?}",
		audit_started.elapsed()
	);

	Ok(())
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use ruma::event_id;

	use super::*;

	#[test]
	fn test_fmt_list_empty() {
		let mut out = String::new();
		fmt_list(&mut out, "Missing", &[]).unwrap();
		assert_eq!(out, "Missing: []\n");
	}

	#[test]
	fn test_fmt_list_items() {
		let mut out = String::new();
		fmt_list(&mut out, "Extra", &[(123, "item1".to_owned()), (456, "item2".to_owned())])
			.unwrap();
		assert_eq!(out, "Extra: [\n  item1\n  item2\n]\n");
	}

	#[test]
	fn test_fmt_event_meta_empty() {
		let meta = HashMap::new();
		let eid = event_id!("$abc:test.org").to_owned();
		assert_eq!(fmt_event_meta("m.room.member", &eid, &meta), "");
	}

	#[test]
	fn test_fmt_event_meta_member() {
		let mut meta = HashMap::new();
		let eid = event_id!("$abc:test.org").to_owned();
		meta.insert(eid.clone(), ("join".to_owned(), "@user:test.org".to_owned()));
		assert_eq!(fmt_event_meta("m.room.member", &eid, &meta), " [join]");
	}

	#[test]
	fn test_fmt_event_meta_power_levels() {
		let mut meta = HashMap::new();
		let eid = event_id!("$abc:test.org").to_owned();
		meta.insert(eid.clone(), (String::new(), "@user:test.org".to_owned()));
		assert_eq!(fmt_event_meta("m.room.power_levels", &eid, &meta), " [by @user:test.org]");
	}
}

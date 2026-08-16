use std::{
	collections::{HashMap, HashSet},
	fmt::Write,
};

use conduwuit::{
	Result, err, info,
	matrix::{Event, pdu::PduEvent},
};
use conduwuit_core::utils::stream::TryIgnore;
use futures::{StreamExt, future::ready};
use ruma::{OwnedEventId, OwnedRoomId, OwnedServerName, RoomId, events::StateEventType};

use crate::admin_command;

#[admin_command]
#[allow(clippy::fn_params_excessive_bools)]
pub(super) async fn rescue_room(
	&self,
	room_id: OwnedRoomId,
	force: bool,
	nuclear: bool,
	all: bool,
	timeline_limit: Option<usize>,
	reorder: bool,
	heal_from: Vec<OwnedServerName>,
) -> Result {
	self.bail_restricted()?;

	// --heal-from implies --force (no point doing full state_res per
	// outlier when we're about to overwrite state from the backbone)
	let force = force || !heal_from.is_empty();

	if all {
		let mut room_ids: HashSet<OwnedRoomId> = HashSet::new();
		let mut outliers = self.services.rooms.outlier.stream();

		while let Some((_event_id, pdu)) = outliers.next().await {
			if let Some(room_id) = pdu.room_id() {
				room_ids.insert(room_id.to_owned());
			} else {
				// V3+ rooms: PDU JSON doesn't contain room_id.
				// We need a way to find the room association.
				// For --all, we scan eventid_metadata for outliers.
				// But we can also just try to find it from the event_id if it's
				// a create event, or just ignore for now as it's expensive.
				if let Some(room_id) = pdu.room_id_or_hash() {
					room_ids.insert(room_id);
				}
			}
		}
		drop(outliers);

		if room_ids.is_empty() {
			return self.write_str("No outliers found in any room.").await;
		}

		self.write_str(&format!(
			"Found outliers in {} rooms. Starting rescue...",
			room_ids.len()
		))
		.await?;

		let mut total_rescued = 0_usize;
		for room_id in room_ids {
			if Box::pin(self.rescue_room(room_id, force, nuclear, false, None, false, vec![]))
				.await
				.is_ok()
			{
				total_rescued = total_rescued.saturating_add(1);
			}
		}

		return self
			.write_str(&format!("Finished rescue attempt for {total_rescued} rooms."))
			.await;
	}

	let mut events: HashMap<OwnedEventId, PduEvent> = self
		.services
		.rooms
		.outlier
		.room_stream(&room_id)
		.collect()
		.await;

	if let Some(limit) = timeline_limit {
		self.write_str(&format!("Including last {limit} timeline PDUs for re-processing..."))
			.await?;
		let timeline_pdus: Vec<(OwnedEventId, PduEvent)> = self
			.services
			.rooms
			.timeline
			.pdus_rev(&room_id, std::ops::Bound::Unbounded)
			.filter_map(|item| ready(item.ok()))
			.take(limit)
			.map(|(_, pdu)| (pdu.event_id().to_owned(), pdu))
			.collect()
			.await;

		for (event_id, pdu) in timeline_pdus {
			events.entry(event_id).or_insert(pdu);
		}
	}

	if events.is_empty() {
		return self.write_str("No outliers found in this room.").await;
	}

	self.write_str(&format!(
		"Healing {} events in room {room_id} via heal_room()...",
		events.len()
	))
	.await?;
	let result = Box::pin(self.services.rooms.timeline.heal_room(
		&room_id,
		events,
		None,
		&conduwuit_service::rooms::timeline::HealOptions {
			clear_markers: force,
			compute_state: false,
			rebuild_membership: false,
			is_reorder: reorder,
		},
	))
	.await?;

	let msg = format!(
		"Healed room {room_id}: {} inserted, {} skipped, {} failed, {} extremities.",
		result.inserted,
		result.skipped,
		result.failed,
		result.extremities.len()
	);
	self.write_str(&msg).await?;

	if reorder {
		self.write_str(&format!("Reordering timeline for {room_id} after rescue..."))
			.await?;
		Box::pin(
			self.services
				.rooms
				.timeline
				.reorder_timeline(&room_id, false, false),
		)
		.await?;
	} else {
		self.write_str(&format!("Rebuilding state for {room_id} using rezzy..."))
			.await?;
		Box::pin(self.services.rooms.timeline.rebuild_state(&room_id)).await?;

		self.write_str(&format!("Rebuilding membership cache for {room_id}..."))
			.await?;
		self.services
			.rooms
			.state_cache
			.reconcile_membership(&room_id)
			.await;
	}

	if !heal_from.is_empty() {
		// Find the latest local event to use as at_event for bootstrapping
		let at_event = self
			.services
			.rooms
			.timeline
			.latest_pdu_in_room(&room_id)
			.await
			.ok()
			.map(|pdu| pdu.event_id().to_owned());

		self.write_str(&format!(
			"\nHealing state from {:?} (force-set-state --overwrite)...",
			heal_from.iter().map(|s| s.as_str()).collect::<Vec<_>>()
		))
		.await?;

		Box::pin(self.force_set_state(
			room_id.clone(),
			heal_from,
			at_event,
			true,  // overwrite
			true,  // skip_sig_verify
			true,  // absolute
			None,  // output
			None,  // input
			false, // dry_run
			false, // skip_membership_rebuild
		))
		.await?;

		self.write_str("Heal complete. Room state synchronized with backbone.")
			.await?;
	}

	Ok(())
}

#[admin_command]
pub(super) async fn rescue_pdu(&self, event_id: OwnedEventId, force: bool) -> Result {
	self.bail_restricted()?;

	let pdu_json = self
		.services
		.rooms
		.timeline
		.get_pdu_json(&event_id)
		.await
		.map_err(|_| err!("PDU not found in database."))?;

	let pdu: PduEvent = serde_json::from_value(serde_json::to_value(&pdu_json)?)?;
	let room_id = pdu
		.room_id()
		.ok_or_else(|| err!("PDU has no room_id."))?
		.to_owned();

	// Clear all soft-fail and rejection markers when rescuing unconditionally
	// (if an admin is rescuing a PDU, they definitely want it un-rejected)
	self.services
		.rooms
		.pdu_metadata
		.clear_pdu_markers(&event_id);

	// --force fast path: bypass state resolution and auth entirely.
	// Required for ancient events where remote servers have pruned historical
	// state (404 Pdu state not found) or the origin is gone.
	if force {
		self.services
			.rooms
			.timeline
			.promote_outlier(&room_id, &event_id)
			.await?;
		return self
			.write_str(&format!(
				"Force-promoted {event_id} into the timeline (bypassed state resolution)."
			))
			.await;
	}

	let create_event = self
		.services
		.rooms
		.state_accessor
		.room_state_get(&room_id, &StateEventType::RoomCreate, "")
		.await
		.map_err(|_| err!("Failed to find create event for room."))?;

	let origin = pdu
		.origin
		.clone()
		.unwrap_or_else(|| pdu.sender.server_name().to_owned());

	// Lenient path: falls back to current room state when no server can
	// provide /state_ids for this historical event.
	Box::pin(
		self.services
			.rooms
			.event_handler
			.upgrade_outlier_to_timeline_pdu(
				pdu,
				pdu_json,
				&create_event,
				&origin,
				&room_id,
				true,  // skip_soft_fail: always lenient for admin rescue
				true,  // is_forward_extremity
				false, // prev_fetch_had_invalid_data: no fresh fetch_prev here
				None,
				// No outer `with_cork_and_flush` on this admin path -- the
				// timeline insert must flush itself.
				false,
			),
	)
	.await?;

	self.write_str("Successfully rescued PDU.").await
}

#[admin_command]
pub(super) async fn clean_corrupt_rooms(&self, execute: bool) -> Result {
	use futures::StreamExt;
	use ruma::RoomId;

	let ours = self.services.globals.server_name();
	let mut corrupt = Vec::new();
	let mut total = 0_usize;

	let mut raw_stream = self.services.rooms.metadata.iter_ids();

	while let Some(room_id) = raw_stream.next().await {
		total = total.saturating_add(1);
		let s = room_id.as_str();

		let valid = s.starts_with('!') && s.len() <= 255 && <&RoomId>::try_from(s).is_ok();
		if !valid && s.starts_with('!') {
			corrupt.push(room_id.to_owned());
		}
	}

	self.write_str(&format!("Scanned {total} rooms, found {} corrupt entries\n", corrupt.len()))
		.await?;

	for room_id in &corrupt {
		self.write_str(&format!("  corrupt: {} ({} bytes)\n", room_id, room_id.as_str().len()))
			.await?;
		if execute {
			let prefix = (ours, conduwuit_database::Interfix, &**room_id);
			let _ =
				self.services.rooms.state_cache.server_rooms_remove_raw(
					&conduwuit_database::serialize_to_vec(prefix).unwrap(),
				);
			self.services.rooms.metadata.remove_room_raw(room_id);
		}
	}

	if !execute {
		self.write_str(
			"\nDry run — corrupt entries are found using raw bytes. Use --execute to remove \
			 individual entries.\n",
		)
		.await
	} else {
		self.write_str("\nNote: Removed malformed room IDs from the serverroomids tree.\n")
			.await
	}
}

#[admin_command]
pub(super) async fn check_rooms(&self, problems_only: bool, fix: bool) -> Result {
	use conduwuit_core::debug_info;
	let ours = self.services.globals.server_name();

	let room_ids: Vec<_> = self
		.services
		.rooms
		.metadata
		.iter_ids()
		.map(ToOwned::to_owned)
		.collect()
		.await;

	let n_rooms = room_ids.len();
	self.write_str(&format!("Scanning {n_rooms} rooms...\n"))
		.await?;

	let mut total_rooms = 0_usize;
	let mut problem_rooms = 0_usize;
	let mut fixed_rooms = 0_usize;
	let mut output = String::new();

	for room_id in &room_ids {
		if !self.services.server.running() {
			self.write_str("Shutting down... aborted check-rooms early.\n")
				.await?;
			break;
		}

		total_rooms = total_rooms.saturating_add(1);
		info!(room_id = %room_id, scanned = total_rooms, total = n_rooms, "check-rooms scanning room");
		let mut issues: Vec<String> = Vec::new();
		let room_str = room_id.as_str();

		// Corrupt room ID check
		if <&RoomId>::try_from(room_str).is_err() || !room_str.is_ascii() {
			issues.push(format!("CORRUPT_ID ({} bytes, non-parseable)", room_str.len()));
			// Can't do further checks on a corrupt ID
			problem_rooms = problem_rooms.saturating_add(1);
			writeln!(output, "FAIL {} -- {}", room_str, issues.join(", ")).ok();
			continue;
		}

		debug_info!("checking create event...");
		// Create event check
		let create_state = self
			.services
			.rooms
			.state_accessor
			.room_state_get(room_id, &StateEventType::RoomCreate, "")
			.await;

		debug_info!("create event check done.");

		match &create_state {
			| Ok(create_pdu) => {
				let create_id = create_pdu.event_id();
				let soft_failed = self
					.services
					.rooms
					.pdu_metadata
					.is_event_soft_failed(create_id)
					.await;
				if soft_failed {
					issues.push("SOFT_FAILED_CREATE".to_owned());
				}
			},
			| Err(_) => {
				issues.push("MISSING_CREATE".to_owned());
			},
		}

		debug_info!("checking local user cache...");
		// Local user check
		let has_local = self
			.services
			.rooms
			.state_cache
			.active_local_users_in_room(room_id)
			.boxed()
			.next()
			.await
			.is_some();

		debug_info!("local user cache done.");

		if !has_local {
			let we_participate = self
				.services
				.rooms
				.state_cache
				.server_in_room(ours, room_id)
				.await;
			if we_participate {
				issues.push("ORPHANED (server listed, 0 local users)".to_owned());
			}
		}

		debug_info!("recalculating extremities...");
		// Forward extremities check
		let (would_change, num_true) = self
			.services
			.rooms
			.timeline
			.recalculate_extremities(room_id, fix)
			.await
			.unwrap_or((false, 0));

		debug_info!("recalculated extremities.");

		if would_change {
			if fix {
				issues.push(format!("EXTREMITIES_DRIFT (Fixed, true tips: {num_true})"));
			} else {
				issues.push(format!(
					"EXTREMITIES_DRIFT (DAG tips silently broken, true tips: {num_true})"
				));
			}
		}

		let ext_count = self
			.services
			.rooms
			.state
			.get_forward_extremities(room_id)
			.count()
			.await;

		if ext_count == 0 {
			issues.push("ZERO_EXTREMITIES (stuck DAG)".to_owned());
		} else if ext_count > 1 {
			issues.push(format!("MULTIPLE_EXTREMITIES ({ext_count} tips)"));
		}

		debug_info!("checking chronological timeline...");
		// Chronological timeline check (detecting hidden fragmentation/breaks)
		let mut timeline_breaks = 0_usize;
		let mut timeline_segments = 1_usize;
		let mut has_timeline_issue = false;
		let pdus = self.services.rooms.timeline.all_pdus(room_id).take(1000);
		futures::pin_mut!(pdus);
		let mut prev_ts = None;
		while let Some((_count, pdu)) = pdus.next().await {
			let ts: u64 = pdu.origin_server_ts().0.into();
			if let Some(pts) = prev_ts {
				if ts < pts {
					timeline_breaks = timeline_breaks.saturating_add(1);
					timeline_segments = timeline_segments.saturating_add(1);
					has_timeline_issue = true;
				}
			}
			prev_ts = Some(ts);
		}
		debug_info!("chronological timeline check done.");

		if has_timeline_issue {
			if fix {
				if Box::pin(
					self.services
						.rooms
						.timeline
						.reorder_timeline(room_id, false, false),
				)
				.await
				.is_ok()
				{
					issues.push(format!(
						"CHRONOLOGICAL_BREAKS (Fixed, breaks={timeline_breaks}, \
						 segments={timeline_segments})"
					));
					fixed_rooms = fixed_rooms.saturating_add(1);
				} else {
					issues.push(format!(
						"CHRONOLOGICAL_BREAKS (Failed to fix, breaks={timeline_breaks}, \
						 segments={timeline_segments})"
					));
				}
			} else {
				issues.push(format!(
					"CHRONOLOGICAL_BREAKS (breaks={timeline_breaks}, \
					 segments={timeline_segments})"
				));
			}
		}

		// Membership cache drift check
		let cache_joined = self
			.services
			.rooms
			.state_cache
			.room_joined_count(room_id)
			.await
			.unwrap_or(0);

		// Get actual state member count (joined)
		let state_joined: u64 = self
			.services
			.rooms
			.state_cache
			.room_members(room_id)
			.count()
			.await
			.try_into()
			.unwrap_or(0);

		if cache_joined != state_joined {
			issues.push(format!("MEMBERSHIP_DRIFT (cache={cache_joined}, state={state_joined})"));

			if fix {
				self.services
					.rooms
					.state_cache
					.update_joined_count(room_id)
					.await;
				issues.push("FIXED".to_owned());
				fixed_rooms = fixed_rooms.saturating_add(1);
			}
		}

		if issues.is_empty() {
			if !problems_only {
				writeln!(output, "OK   {room_id} (ext={ext_count}, joined={cache_joined})").ok();
			}
		} else {
			problem_rooms = problem_rooms.saturating_add(1);
			writeln!(output, "FAIL {room_id} -- {}", issues.join(", ")).ok();
		}

		// Flush every 25 rooms to show live progress
		if total_rooms.is_multiple_of(25) {
			if !output.is_empty() {
				self.write_str(&output).await?;
				output.clear();
			}
			info!(
				scanned = total_rooms,
				total = n_rooms,
				problems = problem_rooms,
				"check-rooms progress"
			);
		}
	}

	if !output.is_empty() {
		self.write_str(&output).await?;
	}

	let mut summary =
		format!("\n**Scan complete:** {total_rooms} rooms checked, {problem_rooms} with issues.");
	if fix && fixed_rooms > 0 {
		write!(summary, " {fixed_rooms} rooms repaired.").ok();
	}
	summary.push('\n');

	self.write_str(&summary).await
}

#[admin_command]
pub(super) async fn heal_receipts(&self) -> Result {
	use std::collections::HashSet;

	use ruma::events::receipt::ReceiptEvent;

	self.write_str("Starting read receipt heal. This may take a moment...")
		.await?;

	let mut stream = self.services.db["readreceiptid_readreceipt"]
		.rev_raw_stream()
		.ignore_err();

	// seen: (room_id, user_id, receipt_type, thread)
	let mut seen = HashSet::new();
	let mut deleted = 0_usize;
	let mut kept = 0_usize;

	while let Some((key, value)) = stream.next().await {
		let parts: Vec<&[u8]> = key.split(|&b| b == conduwuit_database::SEP).collect();
		if parts.len() < 3 {
			continue;
		}

		let room_id_bytes = parts[0];
		let room_id_str = String::from_utf8_lossy(room_id_bytes).to_string();

		let Ok(receipt) = serde_json::from_slice::<ReceiptEvent>(value) else {
			continue;
		};

		let mut all_obsolete = true;

		for receipts in receipt.content.0.values() {
			for (receipt_type, users) in receipts {
				for (user_id, receipt_data) in users {
					let thread = receipt_data.thread.as_str().unwrap_or("").to_owned();
					let sig = (
						room_id_str.clone(),
						user_id.to_string(),
						receipt_type.to_string(),
						thread,
					);

					if seen.insert(sig) {
						all_obsolete = false;
					}
				}
			}
		}

		if all_obsolete {
			self.services.db["readreceiptid_readreceipt"].remove(&key);
			deleted = deleted.saturating_add(1);
		} else {
			kept = kept.saturating_add(1);
		}
	}

	self.write_str(&format!(
		"Heal complete! Kept: {kept}, Deleted: {deleted} duplicate/obsolete receipts."
	))
	.await?;
	Ok(())
}

#[admin_command]
pub(super) async fn reindex_short(
	&self,
	room_id: Option<OwnedRoomId>,
	all: bool,
	skip_topo: bool,
) -> Result {
	self.bail_restricted()?;
	let rebuild_topo = !skip_topo;

	if all {
		let rooms: Vec<OwnedRoomId> = self
			.services
			.rooms
			.metadata
			.iter_ids()
			.map(ToOwned::to_owned)
			.collect()
			.await;

		self.write_str(&format!("Reindexing derived data for {} rooms...\n", rooms.len()))
			.await?;

		let mut total_stats =
			conduwuit_service::rooms::timeline::reindex::ReindexStats::default();
		for (i, rid) in rooms.iter().enumerate() {
			match self
				.services
				.rooms
				.timeline
				.reindex_short(rid, rebuild_topo)
				.await
			{
				| Ok(stats) => {
					if stats.repaired_prev_events > 0
						|| stats.repaired_metadata > 0
						|| stats.repaired_auth_events > 0
						|| stats.missing_pdu > 0
					{
						self.write_str(&format!(
							"[{}/{}] {rid}: {stats}\n",
							i.saturating_add(1),
							rooms.len()
						))
						.await?;
					}
					total_stats.total_events =
						total_stats.total_events.saturating_add(stats.total_events);
					total_stats.missing_pdu =
						total_stats.missing_pdu.saturating_add(stats.missing_pdu);
					total_stats.hash_mismatches = total_stats
						.hash_mismatches
						.saturating_add(stats.hash_mismatches);
					total_stats.repaired_short_ids = total_stats
						.repaired_short_ids
						.saturating_add(stats.repaired_short_ids);
					total_stats.repaired_metadata = total_stats
						.repaired_metadata
						.saturating_add(stats.repaired_metadata);
					total_stats.repaired_prev_events = total_stats
						.repaired_prev_events
						.saturating_add(stats.repaired_prev_events);
					total_stats.repaired_auth_events = total_stats
						.repaired_auth_events
						.saturating_add(stats.repaired_auth_events);
					total_stats.repaired_auth_chains = total_stats
						.repaired_auth_chains
						.saturating_add(stats.repaired_auth_chains);
					total_stats.repaired_topo_index = total_stats
						.repaired_topo_index
						.saturating_add(stats.repaired_topo_index);
					total_stats.repaired_relations = total_stats
						.repaired_relations
						.saturating_add(stats.repaired_relations);
					total_stats.repaired_references = total_stats
						.repaired_references
						.saturating_add(stats.repaired_references);
					total_stats.repaired_search_index = total_stats
						.repaired_search_index
						.saturating_add(stats.repaired_search_index);
				},
				| Err(e) => {
					self.write_str(&format!(
						"[{}/{}] {rid}: ERROR: {e}\n",
						i.saturating_add(1),
						rooms.len()
					))
					.await?;
				},
			}
		}

		self.write_str(&format!("\nAll rooms complete: {total_stats}"))
			.await?;
	} else {
		let rid = room_id.expect("room_id required when --all not set");
		let stats = self
			.services
			.rooms
			.timeline
			.reindex_short(&rid, rebuild_topo)
			.await?;
		self.write_str(&format!("Reindex complete for {rid}: {stats}"))
			.await?;
	}

	Ok(())
}

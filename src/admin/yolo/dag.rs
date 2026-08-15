use std::{
	collections::{HashMap, HashSet, VecDeque},
	fmt::Write,
};

use conduwuit::{
	Err, PduCount, Result, err, info,
	matrix::{Event, pdu::PduEvent},
	warn,
};
use futures::StreamExt;
use ruma::{
	CanonicalJsonObject, EventId, OwnedEventId, OwnedRoomId, OwnedRoomOrAliasId, OwnedServerName,
	RoomVersionId,
	api::federation::event::{get_event, get_missing_events},
};
use tokio::io::AsyncWriteExt;

use super::export::DagExportStats;
use crate::admin_command;

#[admin_command]
#[allow(clippy::fn_params_excessive_bools)]
pub(super) async fn get_room_dag(
	&self,
	room_id: OwnedRoomOrAliasId,
	start: i64,
	end: i64,
	print: bool,
	outliers: bool,
	segments: bool,
	merge_outliers: bool,
) -> Result {
	let room_id = self.services.rooms.alias.resolve(&room_id).await?;
	let pdu_ids: Vec<OwnedEventId> = self
		.services
		.rooms
		.timeline
		.all_pdus(&room_id)
		.map(|(_, pdu)| pdu.event_id().to_owned())
		.collect()
		.await;

	let actual_start = if start < 0 {
		let offset = usize::try_from(start.unsigned_abs()).unwrap_or(usize::MAX);
		u64::try_from(pdu_ids.len().saturating_sub(offset)).unwrap_or(u64::MAX)
	} else {
		start.unsigned_abs()
	};

	let mut i = 0_u64;
	let mut stats = DagExportStats::default();
	let server = self.services.globals.server_name();
	let room_version_str = self
		.services
		.rooms
		.state
		.get_room_version(&room_id)
		.await
		.map_or_else(|_| "unknown".to_owned(), |v| v.to_string());
	let safe_room_id = room_id.to_string().replace('!', "").replace(':', "_");
	let path = format!("/tmp/local-dag-{safe_room_id}-v{room_version_str}-{server}.jsonl");
	let mut file = tokio::fs::File::create(&path)
		.await
		.map_err(|e| err!(Database("Failed to create file {path}: {e:?}")))?;

	let outliers_path =
		format!("/tmp/local-dag-{safe_room_id}-v{room_version_str}-{server}-outliers.jsonl");
	let mut outliers_file = tokio::fs::File::create(&outliers_path)
		.await
		.map_err(|e| err!(Database("Failed to create outliers file {outliers_path}: {e:?}")))?;

	let mut segment_start_idx = actual_start;
	let mut segment_start_ts = None;
	let mut segment_end_ts = None;
	let mut segment_start_depth = 0;
	let mut segment_end_depth = 0;
	let mut prev_ts = None;
	let mut segment_count = 0_usize;
	let mut segment_reports = Vec::new();
	let mut chronological_breaks = Vec::new();

	for event_id in pdu_ids {
		if let Ok(end) = u64::try_from(end) {
			if i > end {
				break;
			}
		}
		if i >= actual_start {
			if let Ok(pdu_json) = self.services.rooms.timeline.get_pdu_json(&event_id).await {
				let pdu_result = self.services.rooms.timeline.get_pdu(&event_id).await;
				if let Ok(ref pdu) = pdu_result {
					let ts: u64 = pdu.origin_server_ts().0.into();
					let depth: u64 = pdu.depth.into();
					if let Some(pts) = prev_ts {
						if ts < pts {
							chronological_breaks.push((i, event_id.clone(), ts, pts));
							if segments {
								if let (Some(sts), Some(ets)) = (segment_start_ts, segment_end_ts)
								{
									segment_reports.push(format!(
										"Segment {}: events {}..{} (len {}), depth {}..{}, time \
										 {}..{}",
										segment_count.saturating_add(1),
										segment_start_idx,
										i.saturating_sub(1),
										i.saturating_sub(segment_start_idx),
										segment_start_depth,
										segment_end_depth,
										format_ts(sts),
										format_ts(ets),
									));
									segment_count = segment_count.saturating_add(1);
									segment_start_idx = i;
									segment_start_ts = Some(ts);
									segment_start_depth = depth;
								}
							}
						}
					}
					if segment_start_ts.is_none() {
						segment_start_ts = Some(ts);
						segment_start_depth = depth;
					}
					segment_end_ts = Some(ts);
					segment_end_depth = depth;
					prev_ts = Some(ts);
				}
				if let Err(e) = stats
					.process_and_write_pdu(
						self,
						&mut file,
						&mut outliers_file,
						pdu_json,
						pdu_result,
						false,
						print,
						merge_outliers,
					)
					.await
				{
					warn!("Failed to process PDU {event_id}: {e}");
				}
			}
		}
		i = i.saturating_add(1);
	}

	if segments && segment_start_ts.is_some() {
		if let (Some(sts), Some(ets)) = (segment_start_ts, segment_end_ts) {
			let end_idx = i.saturating_sub(1);
			let len = i.saturating_sub(segment_start_idx);
			segment_reports.push(format!(
				"Segment {}: events {}..{} (len {}), depth {}..{}, time {}..{}",
				segment_count.saturating_add(1),
				segment_start_idx,
				end_idx,
				len,
				segment_start_depth,
				segment_end_depth,
				format_ts(sts),
				format_ts(ets),
			));
		}
	}

	if outliers {
		let outlier_ids: Vec<OwnedEventId> = self
			.services
			.rooms
			.outlier
			.room_stream(&room_id)
			.map(|(id, _)| id)
			.collect()
			.await;

		for event_id in outlier_ids {
			if let Ok(pdu_json) = self
				.services
				.rooms
				.outlier
				.get_outlier_pdu_json(&event_id)
				.await
			{
				let pdu_result = self.services.rooms.outlier.get_pdu_outlier(&event_id).await;
				if let Err(e) = stats
					.process_and_write_pdu(
						self,
						&mut file,
						&mut outliers_file,
						pdu_json,
						pdu_result,
						true,
						print,
						merge_outliers,
					)
					.await
				{
					warn!("Failed to process outlier PDU {event_id}: {e}");
				}
			}
		}
	}

	// Forward extremities: events not referenced as prev_events by any other event
	let heads_count = stats
		.all_event_ids
		.difference(&stats.referenced_as_prev)
		.count();

	let (bf_whole, bf_frac) = if stats.count > 0 {
		let scaled = stats
			.total_prev_events
			.saturating_mul(1000)
			.checked_div(stats.count)
			.unwrap_or(0);
		(scaled.checked_div(1000).unwrap_or(0), scaled % 1000)
	} else {
		(0, 0)
	};

	let room_ssh = self
		.services
		.rooms
		.state
		.get_room_shortstatehash(&room_id)
		.await
		.ok();

	let tip_match = match (stats.last_ssh, room_ssh) {
		| (Some(tip), Some(room)) if tip == room => "✓ tip matches room state".to_owned(),
		| (Some(tip), Some(room)) if stats.last_is_state_event => {
			// pdu_shortstatehash is the state BEFORE the tip event; room SSH is
			// the state AFTER. Verify the room state actually contains the tip
			// event's state change — look up (type, state_key) in room state.
			if let (Some(last_eid), Some(last_type), Some(last_sk)) =
				(&stats.last_event_id, &stats.last_event_type, &stats.last_state_key)
			{
				let room_has_tip = self
					.services
					.rooms
					.state_accessor
					.state_get_id::<Box<EventId>>(room, &last_type.to_string().into(), last_sk)
					.await
					.is_ok_and(|eid| *eid == **last_eid);

				if room_has_tip {
					format!(
						"✓ tip is state event — room state includes tip (pre={tip} post={room})"
					)
				} else {
					format!(
						"✗ tip DIVERGES — room state at ({last_type}, {last_sk}) does not point \
						 to tip event {last_eid}"
					)
				}
			} else {
				"✗ tip DIVERGES from room state (state event but missing metadata)".to_owned()
			}
		},
		| (Some(_tip), Some(_room)) => "✗ tip DIVERGES from room state".to_owned(),
		| _ => "? unknown".to_owned(),
	};

	// Rename to include depth range so successive runs don't overwrite
	let min_d = if stats.min_depth == u64::MAX {
		0
	} else {
		stats.min_depth
	};
	let final_path = format!(
		"/tmp/local-dag-{safe_room_id}-v{room_version_str}-{server}-d{min_d}-{max_depth}.jsonl",
		max_depth = stats.max_depth
	);
	if let Err(e) = tokio::fs::rename(&path, &final_path).await {
		warn!("Failed to rename {path} -> {final_path}: {e}");
	}
	let display_path = if tokio::fs::metadata(&final_path).await.is_ok() {
		&final_path
	} else {
		&path
	};

	let mut out = format!("Wrote {count} PDUs to {display_path}\n", count = stats.count);
	writeln!(out, "```").expect("fmt");
	writeln!(out, "PDUs:           {count}", count = stats.count).expect("fmt");
	writeln!(out, "State events:   {state_events}", state_events = stats.state_events)
		.expect("fmt");
	writeln!(out, "Branching:      {bf_whole}.{bf_frac:03} avg prev_events/PDU").expect("fmt");
	let (frag_whole, frag_frac) = if stats.max_depth > 0 {
		let scaled = stats
			.count
			.saturating_mul(1000)
			.checked_div(stats.max_depth)
			.unwrap_or(0);
		(scaled.checked_div(1000).unwrap_or(0), scaled % 1000)
	} else {
		(0, 0)
	};
	let mut roots = Vec::new();
	for (id, prevs) in &stats.all_events_prevs {
		if prevs.iter().all(|p| !stats.all_event_ids.contains(p)) {
			roots.push(id.clone());
		}
	}
	let roots_count = roots.len();
	let isolated_count = stats
		.all_event_ids
		.difference(&stats.referenced_as_prev)
		.filter(|id| roots.contains(id))
		.count();

	writeln!(
		out,
		"Frag factor:    {frag_whole}.{frag_frac:03} ({count} events / {max_depth} depth, \
		 {heads_count} heads, {roots_count} roots, {isolated_count} isolated)",
		count = stats.count,
		max_depth = stats.max_depth
	)
	.expect("fmt");
	writeln!(out, "Unique states:  {}", stats.unique_hashes.len()).expect("fmt");
	writeln!(out, "Missing hash:   {missing_hash}", missing_hash = stats.missing_hash)
		.expect("fmt");
	if let Some(tip) = stats.last_ssh {
		writeln!(out, "Tip SSH:        {tip}").expect("fmt");
	}
	if let Some(room) = room_ssh {
		writeln!(out, "Room SSH:       {room}").expect("fmt");
	}
	writeln!(out, "Status:         {tip_match}").expect("fmt");
	writeln!(out, "```").expect("fmt");

	if segments {
		writeln!(out, "\n--- Chronological Segments ({}) ---", segment_reports.len())
			.expect("fmt");
		for report in &segment_reports {
			writeln!(out, "{report}").expect("fmt");
		}
		writeln!(out, "\n--- Chronological Breaks ({}) ---", chronological_breaks.len())
			.expect("fmt");
		for (idx, eid, ts, pts) in &chronological_breaks {
			let ts_dt = format_ts(*ts);
			let pts_dt = format_ts(*pts);
			let diff = std::time::Duration::from_millis(pts.saturating_sub(*ts)).as_secs_f64();
			writeln!(
				out,
				"Break at index {idx}: event {eid} went BACKWARDS by {diff:.1}s\n  Prev: \
				 {pts_dt}\n  Curr: {ts_dt}"
			)
			.expect("fmt");
		}
	}

	self.write_str(&out).await
}

#[admin_command]
#[allow(clippy::too_many_arguments)]
#[allow(clippy::fn_params_excessive_bools)]
pub(super) async fn get_remote_dag(
	&self,
	room_id: OwnedRoomId,
	server: Option<OwnedServerName>,
	limit: i64,
	from: Option<OwnedEventId>,
	print: bool,
	verbose: bool,
	room_version: Option<RoomVersionId>,
	extra_servers: Vec<OwnedServerName>,
	gap_fill: bool,
	import: bool,
	_skip_auth: bool,
	reorder: bool,
) -> Result {
	use futures::StreamExt;

	if !self.services.server.config.allow_federation {
		return Err!("Federation is disabled on this homeserver.");
	}

	if let Some(ref s) = server {
		if *s == self.services.globals.server_name() {
			return Err!("Cannot fetch from ourselves. Use get-room-dag instead.");
		}
	}

	let start_event_id: OwnedEventId = match from {
		| Some(eid) => eid,
		| None => self
			.services
			.rooms
			.timeline
			.latest_pdu_in_room(&room_id)
			.await?
			.event_id()
			.to_owned(),
	};

	let room_version = match self.services.rooms.state.get_room_version(&room_id).await {
		| Ok(v) => v,
		| Err(_e) =>
			if let Some(v) = room_version {
				v
			} else {
				return Err!(Request(InvalidParam(
					"Local room version missing. You must specify --room-version explicitly."
				)));
			},
	};

	// Build server pool: primary + auto-discovered EMA-ranked room servers
	let mut pool = if !gap_fill {
		if let Some(ref s) = server {
			service::rooms::event_handler::server_pool::ServerPool::from_servers(vec![s.clone()])
		} else if !extra_servers.is_empty() {
			service::rooms::event_handler::server_pool::ServerPool::from_servers(vec![])
		} else {
			return Err!(
				"You must specify a server, --also, or use --gap-fill for auto-discovery."
			);
		}
	} else {
		let preferred = server
			.clone()
			.unwrap_or_else(|| self.services.globals.server_name().to_owned());
		self.services
			.rooms
			.event_handler
			.build_server_pool(
				&room_id,
				&preferred,
				self.services.server.config.federation_fallback_room_servers,
			)
			.await
	};

	// Add any explicit --also servers
	if !extra_servers.is_empty() {
		let mut servers = if let Some(ref s) = server {
			vec![s.clone()]
		} else {
			vec![]
		};
		for s in &extra_servers {
			if !servers.contains(s) {
				servers.push(s.clone());
			}
		}
		// Rebuild pool with explicit servers first
		let ranked = pool.server_names().to_vec();
		for s in ranked {
			if !servers.contains(&s) {
				servers.push(s);
			}
		}
		pool = service::rooms::event_handler::server_pool::ServerPool::from_servers(servers);
	}

	let safe_room_id = room_id.to_string().replace('!', "").replace(':', "_");
	let server_str = server.as_ref().map_or("auto", |s| s.as_str());
	let path = format!("/tmp/remote-dag-{safe_room_id}-v{room_version}-{server_str}.jsonl");
	let file = tokio::fs::File::create(&path)
		.await
		.map_err(|e| err!(Database("Failed to create file {path}: {e:?}")))?;
	let mut writer = tokio::io::BufWriter::new(file);

	let mut seen = HashSet::<OwnedEventId>::new();
	let mut queued = HashSet::<OwnedEventId>::new();
	queued.insert(start_event_id.clone());
	let mut queue = VecDeque::from(vec![start_event_id]);
	let mut total = 0_usize;
	let mut total_prev_events = 0_u64;
	let mut batches = 0_usize;
	let mut min_depth = u64::MAX;
	let mut max_depth = 0_u64;
	let mut consecutive_errors = 0_usize;
	let mut last_fetched_event: Option<OwnedEventId> = None;
	let batch_size = ruma::uint!(500);
	let start_time = tokio::time::Instant::now();

	let server_list_str = pool.display();

	info!("get-remote-dag: starting crawl from {server_list_str} for {room_id} (limit: {limit})");
	self.write_str(&format!(
		"Fetching DAG from {server_list_str} for {room_id} (limit: {limit})...\n"
	))
	.await?;

	let unlimited = limit < 0;
	let limit = if unlimited {
		usize::MAX
	} else {
		usize::try_from(limit).unwrap_or(usize::MAX)
	};

	while !queue.is_empty() && total < limit {
		// Cap request queue to avoid 414 URI Too Long from reverse proxies.
		// Drain items from front so we don't lose unsent frontier items.
		let current_batch_size = 50.min(queue.len());
		let request_v: Vec<_> = queue.drain(..current_batch_size).collect();

		// Pick next available server from the pool
		let Some(active_server) = pool.next_available() else {
			// All servers in cooldown, wait briefly
			for id in request_v.into_iter().rev() {
				queue.push_front(id);
			}
			tokio::time::sleep(std::time::Duration::from_secs(2)).await;
			continue;
		};

		let request = ruma::api::federation::backfill::get_backfill::v1::Request {
			room_id: room_id.clone(),
			v: request_v.clone(),
			limit: batch_size,
		};

		batches = batches.saturating_add(1);
		let mut response = match self
			.services
			.sending
			.send_federation_request(&active_server, request)
			.await
		{
			| Ok(r) => {
				consecutive_errors = 0;
				pool.record_success(&active_server);
				r
			},
			| Err(e) => {
				let err_str = e.to_string();

				// 414 URI Too Long -- re-add only half the items to shrink next request
				if err_str.contains("414") {
					let keep = request_v.len() / 2;
					for id in request_v.into_iter().take(keep) {
						queue.push_front(id);
					}
					continue;
				}

				// Re-add items for next server attempt
				for id in request_v.clone().into_iter().rev() {
					queue.push_front(id);
				}

				// 429 rate limit — pool handles cooldown + backoff
				if service::rooms::event_handler::server_pool::ServerPool::is_rate_limit(&err_str)
				{
					pool.record_rate_limit(&active_server);
					info!("get-remote-dag: {active_server} rate-limited, rotating");
					continue;
				}

				// Other errors
				pool.record_error(&active_server);
				consecutive_errors = consecutive_errors.saturating_add(1);
				info!(
					"get-remote-dag: {active_server} request failed after {total} PDUs in \
					 {batches} batches (attempt {consecutive_errors}/3): {e}"
				);
				if verbose {
					self.write_str(&format!(
						"Federation request to {active_server} failed (batch {batches}, \
						 queue={}, attempt {consecutive_errors}/3):\n```\n{e:?}\n```\n",
						queue.len()
					))
					.await?;
				} else {
					self.write_str(&format!(
						"Federation request to {active_server} failed (attempt \
						 {consecutive_errors}/3): {e}"
					))
					.await?;
				}

				// With multiple servers, rotate instead of giving up
				if pool.is_multi() {
					consecutive_errors = 0;
					continue;
				}

				if consecutive_errors >= 3 {
					self.write_str("Giving up after 3 consecutive failures.\n")
						.await?;
					break;
				}
				continue;
			},
		};

		if response.pdus.is_empty() {
			// Dead-end — pool puts this server in short cooldown
			pool.record_dead_end(&active_server);
			for id in request_v.into_iter().rev() {
				queue.push_front(id);
			}

			if !pool.all_exhausted() {
				continue; // Try another server
			}

			// All servers exhausted, fall back to /event/
			info!("get-remote-dag: all servers exhausted, falling back to /event/");
			let batch_size_fb = 50.min(queue.len());
			let request_v_fallback: Vec<_> = queue.drain(..batch_size_fb).collect();
			let mut fallback_pdus = Vec::new();
			for event_id in &request_v_fallback {
				for fallback_server in pool.server_names() {
					if let Ok(res) = self
						.services
						.sending
						.send_federation_request(fallback_server, get_event::v1::Request {
							event_id: event_id.clone(),
							include_unredacted_content: None,
						})
						.await
					{
						if Some(fallback_server) != server.as_ref() {
							info!(
								"get-remote-dag: {fallback_server} filled gap {event_id} that \
								 primary server didn't have"
							);
						}
						fallback_pdus.push(res.pdu);
						break;
					}
				}
			}
			if fallback_pdus.is_empty() {
				info!(
					"get-remote-dag: /event/ fallback also returned empty; giving up on this \
					 batch."
				);
				continue;
			}
			info!("get-remote-dag: recovered {} PDUs via /event/ fallback!", fallback_pdus.len());
			response = ruma::api::federation::backfill::get_backfill::v1::Response {
				origin: active_server.clone(),
				origin_server_ts: ruma::MilliSecondsSinceUnixEpoch::now(),
				pdus: fallback_pdus,
			};
		}

		let mut verifications = futures::stream::iter(response.pdus)
			.map(|raw_pdu| {
				let rv = room_version.clone();
				async move {
					// BYPASS signature verification to make get-remote-dag BLAZING fast!
					// Just generate the ID and canonical JSON without fetching keys over network.
					let res =
						conduwuit::matrix::event::gen_event_id_canonical_json(&raw_pdu, &rv);
					(raw_pdu, res)
				}
			})
			.buffered(500);

		// Collect batch results to identify true frontier
		let mut batch_pdus = Vec::new();
		let mut batch_event_ids = HashSet::new();

		while let Some((raw_pdu, validation_res)) = verifications.next().await {
			let (event_id, mut value) = match validation_res {
				| Ok((eid, val)) => (eid, val),
				| Err(e) => {
					warn!("get_remote_dag: Failed to canonicalize PDU: {e}");
					continue;
				},
			};

			batch_event_ids.insert(event_id.clone());

			value.insert(
				"event_id".to_owned(),
				ruma::CanonicalJsonValue::String(event_id.as_str().to_owned()),
			);

			batch_pdus.push((event_id, value, raw_pdu));
		}

		for (event_id, value, raw_pdu) in batch_pdus {
			if seen.contains(&event_id) {
				continue;
			}
			seen.insert(event_id.clone());

			let Ok(pdu) = PduEvent::from_id_val(&event_id, value.clone(), Some(room_id.as_ref()))
			else {
				continue;
			};

			let mut export_val: serde_json::Map<String, serde_json::Value> =
				serde_json::from_str(raw_pdu.get()).unwrap_or_default();
			if !export_val.contains_key("event_id") {
				export_val.insert(
					"event_id".to_owned(),
					serde_json::Value::String(event_id.to_string()),
				);
			}
			if let Ok(json) = serde_json::to_string(&export_val) {
				if writer.write_all(json.as_bytes()).await.is_ok() {
					let _ = writer.write_all(b"\n").await;
				}
				if print {
					let _ = self.write_str(&format!("{json}\n")).await;
				}
			}

			total_prev_events = total_prev_events
				.saturating_add(u64::try_from(pdu.prev_events().count()).unwrap_or(0));
			let depth: u64 = pdu.depth.into();
			min_depth = min_depth.min(depth);
			max_depth = max_depth.max(depth);
			total = total.saturating_add(1);
			last_fetched_event = Some(event_id.clone());

			if total.is_multiple_of(1000) {
				let elapsed = start_time.elapsed();
				info!(
					"get-remote-dag: {total} PDUs fetched from {server_list_str} in {elapsed:?} \
					 ({batches} batches, queue={})",
					queue.len()
				);
			}

			// Add prev_events to the queue for next iteration ONLY if they are frontier
			for prev in pdu.prev_events() {
				if !seen.contains(prev) && !batch_event_ids.contains(prev) {
					if queued.insert(prev.to_owned()) {
						queue.push_back(prev.to_owned());
					}
				}
			}

			if total >= limit {
				break;
			}
		}

		// Yield to avoid blocking
		tokio::task::yield_now().await;
	}

	writer
		.flush()
		.await
		.map_err(|e| err!(Database("Failed to flush writer: {e:?}")))?;

	let elapsed = start_time.elapsed();
	let (bf_whole, bf_frac) = if total > 0 {
		let divisor = u64::try_from(total).unwrap_or(1);
		let scaled = total_prev_events
			.saturating_mul(1000)
			.checked_div(divisor)
			.unwrap_or(0);
		(scaled.checked_div(1000).unwrap_or(0), scaled % 1000)
	} else {
		(0, 0)
	};

	let finish_reason = if consecutive_errors >= 3 {
		"aborted (federation errors)"
	} else if total >= limit && !unlimited {
		"hit requested limit"
	} else if queue.is_empty() {
		"queue empty (reached genesis or all prev_events known locally)"
	} else {
		"unknown"
	};

	info!(
		"get-remote-dag: complete — {total} PDUs from {server_list_str} in {elapsed:?} \
		 ({batches} batches, bf={bf_whole}.{bf_frac:03}, depth={min_depth}..{max_depth}, \
		 reason: {finish_reason})"
	);

	// Rename to include depth range so successive runs don't overwrite
	let final_path = format!(
		"/tmp/remote-dag-{safe_room_id}-v{room_version}-{server_str}-d{min_depth}-{max_depth}.\
		 jsonl"
	);
	if let Err(e) = tokio::fs::rename(&path, &final_path).await {
		warn!("Failed to rename {path} -> {final_path}: {e}");
	}
	let display_path = if tokio::fs::metadata(&final_path).await.is_ok() {
		&final_path
	} else {
		&path
	};

	let tail_hint = last_fetched_event
		.map(|e| format!("\nLast fetched event (tail): {e}"))
		.unwrap_or_default();

	self.write_str(&format!(
		"\nSuccessfully fetched {total} PDUs from {server_list_str} to {display_path} (depth: \
		 {min_depth}..{max_depth}, branching factor: {bf_whole}.{bf_frac:03})\nReason: \
		 {finish_reason}{tail_hint}\n"
	))
	.await?;

	// Pipeline hint
	if import {
		self.write_str(&format!(
			"\nTo import: `yolo import-pdus {display_path} --skip-sig-verify`\n"
		))
		.await?;
	}

	if reorder {
		self.write_str(&format!("To reorder: `yolo reorder-timeline {room_id}`\n"))
			.await?;
	}

	Ok(())
}

#[admin_command]
pub(super) async fn dag_merge_base(
	&self,
	room_id: OwnedRoomId,
	server: Option<OwnedServerName>,
	event_a: Option<OwnedEventId>,
	event_b: Option<OwnedEventId>,
	max_depth: usize,
	federate: bool,
) -> Result {
	// Server is required when event_b is not provided (need to probe remote tip)
	if event_b.is_none() && server.is_none() {
		return Err!("--server is required unless both --event-a and --event-b are provided");
	}

	if let Some(ref server) = server {
		if !self.services.server.config.allow_federation {
			return Err!("Federation is disabled on this homeserver.");
		}
		if *server == self.services.globals.server_name()
			&& !self.services.server.config.federation_loopback
		{
			return Err!(
				"Cannot compare against ourselves (enable federation_loopback to allow)."
			);
		}
	}

	let room_version = self.services.rooms.state.get_room_version(&room_id).await?;

	/// Look up a PDU from timeline first, then outlier table.
	macro_rules! get_pdu_any {
		($event_id:expr) => {{
			let eid: &EventId = $event_id;
			if let Ok(pdu) = self.services.rooms.timeline.get_pdu(eid).await {
				Some(pdu)
			} else if let Ok(pdu) = self.services.rooms.outlier.get_pdu_outlier(eid).await {
				Some(pdu)
			} else {
				None
			}
		}};
	}

	// Fetch a PDU from the remote server and store as outlier.
	macro_rules! fed_fetch {
		($event_id:expr) => {{
			let eid: OwnedEventId = $event_id;
			let result: Option<PduEvent> = async {
				let server = server.as_ref()?;
				let response = self
					.services
					.sending
					.send_federation_request(
						server,
						get_event::v1::Request::new(eid.clone(), None),
					)
					.await
					.ok()?;
				let (validated_id, value) = self
					.services
					.server_keys
					.validate_and_add_event_id(&response.pdu, &room_version)
					.await
					.ok()?;
				let pdu =
					PduEvent::from_id_val(&validated_id, value.clone(), Some(room_id.as_ref()))
						.ok()?;
				self.services
					.rooms
					.outlier
					.add_pdu_outlier(&validated_id, &value, Some(room_id.as_ref()))
					.await;
				Some(pdu)
			}
			.await;
			result
		}};
	}

	// Resolve local tip (event A)
	let event_a = match event_a {
		| Some(id) => id,
		| None => {
			let latest = self
				.services
				.rooms
				.timeline
				.latest_pdu_in_room(&room_id)
				.await?;
			self.write_str(&format!("Local tip: {}\n", latest.event_id()))
				.await?;
			latest.event_id().to_owned()
		},
	};

	// Resolve remote tip (event B) — only needed when --event-b is not provided.
	// The early guard ensures server.is_some() in this path.
	let event_b = match event_b {
		| Some(id) => id,
		| None => {
			let server = server.as_ref().expect("guarded above");
			self.write_str(&format!("Probing {server} for remote tip via make_join...\n"))
				.await?;

			let user_id = self
				.services
				.rooms
				.state_cache
				.active_local_users_in_room(&room_id)
				.boxed()
				.next()
				.await
				.ok_or_else(|| err!("No active local users in room {room_id}"))?
				.to_owned();

			let make_join_request =
				ruma::api::federation::membership::prepare_join_event::v1::Request {
					room_id: room_id.clone(),
					user_id,
					ver: self.services.server.supported_room_versions().collect(),
				};

			let response = self
				.services
				.sending
				.send_federation_request(server, make_join_request)
				.await
				.map_err(|e| err!("make_join to {server} failed: {e}"))?;

			let event_stub_raw = response.event;

			let event_stub: CanonicalJsonObject = serde_json::from_str(event_stub_raw.get())
				.map_err(|e| err!("Invalid make_join template from {server}: {e}"))?;

			let remote_tips: Vec<OwnedEventId> = event_stub
				.get("prev_events")
				.and_then(|v| v.as_array())
				.map(|arr| {
					arr.iter()
						.filter_map(|v| {
							v.as_str()
								.and_then(|s| <&EventId>::try_from(s).ok().map(ToOwned::to_owned))
						})
						.collect()
				})
				.unwrap_or_default();

			if remote_tips.is_empty() {
				return Err!(
					"make_join from {server} returned no prev_events (forward extremities)"
				);
			}

			let remote_tip_id = remote_tips.into_iter().next().expect("checked non-empty");
			self.write_str(&format!("Remote tip (via make_join): {remote_tip_id}\n"))
				.await?;
			remote_tip_id
		},
	};

	let pdu_a = match get_pdu_any!(&event_a) {
		| Some(pdu) => pdu,
		| None => fed_fetch!(event_a.clone())
			.ok_or_else(|| err!("Event A not found locally or via federation: {event_a}"))?,
	};
	let pdu_b = match get_pdu_any!(&event_b) {
		| Some(pdu) => pdu,
		| None => fed_fetch!(event_b.clone())
			.ok_or_else(|| err!("Event B not found locally or via federation: {event_b}"))?,
	};

	self.write_str(&format!(
		"Walking DAG backwards from:\n  A (local):  {event_a} (ts {ta_ts}, type {ta})\n  B \
		 (remote): {event_b} (ts {tb_ts}, type {tb})\n\nMax depth: {max_depth}\n",
		ta_ts = pdu_a.origin_server_ts,
		ta = pdu_a.kind,
		tb_ts = pdu_b.origin_server_ts,
		tb = pdu_b.kind,
	))
	.await?;

	// Bidirectional BFS via prev_events
	// ancestors_a/b: event_id -> (depth_from_start, parent_event_id)
	let mut ancestors_a: HashMap<OwnedEventId, (usize, Option<OwnedEventId>)> = HashMap::new();
	let mut ancestors_b: HashMap<OwnedEventId, (usize, Option<OwnedEventId>)> = HashMap::new();
	let mut queue_a: VecDeque<(OwnedEventId, usize)> = VecDeque::new();
	let mut queue_b: VecDeque<(OwnedEventId, usize)> = VecDeque::new();

	ancestors_a.insert(event_a.clone(), (0, None));
	ancestors_b.insert(event_b.clone(), (0, None));
	queue_a.push_back((event_a.clone(), 0));
	queue_b.push_back((event_b.clone(), 0));

	let mut merge_bases: Vec<OwnedEventId> = Vec::new();
	let mut steps = 0_usize;
	let mut missing_events = 0_usize;
	let mut fetched_events = 0_usize;

	while (!queue_a.is_empty() || !queue_b.is_empty()) && steps < max_depth {
		// Expand one step from A
		if let Some((current, depth)) = queue_a.pop_front() {
			if ancestors_b.contains_key(&current) {
				if !merge_bases.contains(&current) {
					merge_bases.push(current.clone());
				}
				// Don't stop — find all merge bases at this depth
			}

			let pdu = match get_pdu_any!(&current) {
				| Some(p) => Some(p),
				| None if federate => {
					fetched_events = fetched_events.saturating_add(1);
					let srv = server.as_deref().map_or("local", |s| s.as_str());
					info!(
						"dag-merge-base: fetching {current} from {srv} (A-side, \
						 #{fetched_events})"
					);
					fed_fetch!(current.clone())
				},
				| None => None,
			};
			if let Some(pdu) = pdu {
				for prev in pdu.prev_events() {
					let prev = prev.to_owned();
					if !ancestors_a.contains_key(&prev) {
						let next_depth = depth.saturating_add(1);
						ancestors_a.insert(prev.clone(), (next_depth, Some(current.clone())));
						if next_depth < max_depth {
							queue_a.push_back((prev, next_depth));
						}
					}
				}
			} else {
				missing_events = missing_events.saturating_add(1);
			}
		}

		// Expand one step from B
		if let Some((current, depth)) = queue_b.pop_front() {
			if ancestors_a.contains_key(&current) {
				if !merge_bases.contains(&current) {
					merge_bases.push(current.clone());
				}
			}

			let pdu = match get_pdu_any!(&current) {
				| Some(p) => Some(p),
				| None if federate => {
					fetched_events = fetched_events.saturating_add(1);
					let srv = server.as_deref().map_or("local", |s| s.as_str());
					info!(
						"dag-merge-base: fetching {current} from {srv} (B-side, \
						 #{fetched_events})"
					);
					fed_fetch!(current.clone())
				},
				| None => None,
			};
			if let Some(pdu) = pdu {
				for prev in pdu.prev_events() {
					let prev = prev.to_owned();
					if !ancestors_b.contains_key(&prev) {
						let next_depth = depth.saturating_add(1);
						ancestors_b.insert(prev.clone(), (next_depth, Some(current.clone())));
						if next_depth < max_depth {
							queue_b.push_back((prev, next_depth));
						}
					}
				}
			} else {
				missing_events = missing_events.saturating_add(1);
			}
		}

		steps = steps.saturating_add(1);

		// If we found merge bases and both queues are past the merge base depth, stop
		if !merge_bases.is_empty() && queue_a.is_empty() && queue_b.is_empty() {
			break;
		}
		// Early stop if we found merge bases and current depth exceeds merge base depth
		// by a margin
		if let Some(first_mb) = merge_bases.first() {
			let mb_depth_a = ancestors_a.get(first_mb).map_or(0, |(d, _)| *d);
			let mb_depth_b = ancestors_b.get(first_mb).map_or(0, |(d, _)| *d);
			let mb_max = mb_depth_a.max(mb_depth_b);
			let current_min_a = queue_a.front().map_or(usize::MAX, |(_, d)| *d);
			let current_min_b = queue_b.front().map_or(usize::MAX, |(_, d)| *d);
			if current_min_a > mb_max && current_min_b > mb_max {
				break;
			}
		}
	}

	self.write_str(&format!(
		"Walked {} steps, visited {} (A) + {} (B) events, {} missing, {} fetched\n",
		steps,
		ancestors_a.len(),
		ancestors_b.len(),
		missing_events,
		fetched_events,
	))
	.await?;

	if merge_bases.is_empty() {
		return self
			.write_str(&format!(
				"**No common ancestor found** within {max_depth} steps.\n\nThe events may be on \
				 completely disjoint DAG branches, or the common ancestor is deeper than the \
				 limit."
			))
			.await;
	}

	// For each merge base, trace back the path from both events
	for mb in &merge_bases {
		let mb_pdu = get_pdu_any!(mb);
		let mb_info = mb_pdu.as_ref().map_or_else(
			|| "unknown".to_owned(),
			|p| format!("ts {}, type {}", p.origin_server_ts, p.kind),
		);

		// BFS distances from each starting event
		let dist_a = ancestors_a
			.get(mb)
			.map_or_else(|| "?".to_owned(), |(d, _)| d.to_string());
		let dist_b = ancestors_b
			.get(mb)
			.map_or_else(|| "?".to_owned(), |(d, _)| d.to_string());

		self.write_str(&format!(
			"\n### Merge base: `{mb}` ({mb_info})\nA is {dist_a} step(s) away, B is {dist_b} \
			 step(s) away\n"
		))
		.await?;

		// Trace path A -> merge base
		let path_a = trace_path(&ancestors_a, &event_a, mb);
		let path_b = trace_path(&ancestors_b, &event_b, mb);

		// Render ASCII DAG
		let short = |id: &EventId| -> String {
			let s = id.as_str();
			let truncated: String = s.chars().take(12).collect();
			if s.len() > 12 {
				format!("{truncated}…")
			} else {
				s.to_owned()
			}
		};

		let mut graph = String::new();

		// Header
		writeln!(graph, "```").ok();
		writeln!(
			graph,
			"  A ({} steps)          B ({} steps)",
			path_a.len().saturating_sub(1),
			path_b.len().saturating_sub(1)
		)
		.ok();
		writeln!(graph, "  │                     │").ok();

		let max_len = path_a.len().max(path_b.len());
		for i in 0..max_len {
			let left = path_a.get(i).map(|id| short(id)).unwrap_or_default();
			let right = path_b.get(i).map(|id| short(id)).unwrap_or_default();

			// Check if this is the merge base
			let is_mb_left = path_a.get(i).is_some_and(|id| id == mb);
			let is_mb_right = path_b.get(i).is_some_and(|id| id == mb);

			if is_mb_left || is_mb_right {
				writeln!(graph, "  └──────────┬──────────┘").ok();
				writeln!(graph, "             │").ok();
				writeln!(graph, "      {left} ◄── MERGE BASE").ok();

				// Get the merge base PDU info
				if let Some(ref p) = mb_pdu {
					writeln!(graph, "      ts={}", p.origin_server_ts).ok();
				}
				break;
			}

			if !left.is_empty() && !right.is_empty() {
				writeln!(graph, "  {left:<20}  {right}").ok();
				writeln!(graph, "  │                     │").ok();
			} else if !left.is_empty() {
				writeln!(graph, "  {left:<20}  ·").ok();
				writeln!(graph, "  │                     ·").ok();
			} else {
				writeln!(graph, "  ·                     {right}").ok();
				writeln!(graph, "  ·                     │").ok();
			}
		}

		// If we never printed a merge base line (both paths end at merge base on
		// different iterations)
		if max_len == 0 {
			writeln!(graph, "  (same event)").ok();
		}

		writeln!(graph, "```").ok();
		self.write_str(&graph).await?;
	}

	Ok(())
}

/// Trace the path from a starting event back to the target event using the
/// ancestor map.
fn trace_path(
	ancestors: &HashMap<OwnedEventId, (usize, Option<OwnedEventId>)>,
	from: &EventId,
	to: &EventId,
) -> Vec<OwnedEventId> {
	let mut path = Vec::new();
	let mut current = from.to_owned();
	let mut visited = HashSet::new();

	loop {
		if !visited.insert(current.clone()) {
			break; // cycle guard
		}
		path.push(current.clone());
		if current == to {
			break;
		}
		match ancestors.get(&current) {
			| Some((_, Some(parent))) => current = parent.clone(),
			| _ => break,
		}
	}

	path
}

/// Format an origin_server_ts (millis since epoch) as a human-readable UTC
/// string.
pub(super) fn format_ts(ts_millis: u64) -> String {
	let ts_secs = ts_millis.checked_div(1000).unwrap_or(0);
	let days = ts_secs.checked_div(86_400).unwrap_or(0);
	let time_of_day = ts_secs.checked_rem(86_400).unwrap_or(0);
	let hours = time_of_day.checked_div(3600).unwrap_or(0);
	let minutes = time_of_day
		.checked_rem(3600)
		.unwrap_or(0)
		.checked_div(60)
		.unwrap_or(0);
	let seconds = time_of_day.checked_rem(60).unwrap_or(0);
	let (year, month, day) = civil_from_days(days.cast_signed());
	format!("{year:04}-{month:02}-{day:02} {hours:02}:{minutes:02}:{seconds:02} UTC")
}

/// Convert days since 1970-01-01 to (year, month, day).
/// Based on Howard Hinnant's civil_from_days algorithm.
pub(super) fn civil_from_days(days: i64) -> (i64, u64, u64) {
	let z = days.saturating_add(719_468);
	let era = if z >= 0 { z } else { z.saturating_sub(146_096) }.saturating_div(146_097);
	let doe = z
		.saturating_sub(era.saturating_mul(146_097))
		.cast_unsigned();
	let yoe = doe
		.saturating_sub(doe.saturating_div(1460))
		.saturating_add(doe.saturating_div(36_524))
		.saturating_sub(doe.saturating_div(146_096))
		.saturating_div(365);
	let y = yoe.cast_signed().saturating_add(era.saturating_mul(400));
	let doy = doe.saturating_sub(
		yoe.saturating_mul(365)
			.saturating_add(yoe.saturating_div(4))
			.saturating_sub(yoe.saturating_div(100)),
	);
	let mp = doy.saturating_mul(5).saturating_add(2).saturating_div(153);
	let d = doy
		.saturating_sub(mp.saturating_mul(153).saturating_add(2).saturating_div(5))
		.saturating_add(1);
	let m = if mp < 10 {
		mp.saturating_add(3)
	} else {
		mp.saturating_sub(9)
	};
	let y = if m <= 2 { y.saturating_add(1) } else { y };
	(y, m, d)
}

#[admin_command]
pub(super) async fn audit_auth_chain(
	&self,
	room_id: OwnedRoomId,
	fetch: bool,
	verbose: bool,
	servers: Vec<OwnedServerName>,
	event_ids: Vec<OwnedEventId>,
	outliers: bool,
) -> Result {
	// Resolve room and get current state hash, fallback to extremities if no state
	let mut state_ids: Vec<OwnedEventId> = if !event_ids.is_empty() {
		event_ids
	} else {
		match self
			.services
			.rooms
			.state
			.get_room_shortstatehash(&room_id)
			.await
		{
			| Ok(sstatehash) =>
				self.services
					.rooms
					.state_accessor
					.state_full_ids(sstatehash)
					.map(|(_, id)| id)
					.collect()
					.await,
			| Err(_) =>
				self.services
					.rooms
					.state
					.get_forward_extremities(&room_id)
					.collect()
					.await,
		}
	};

	if outliers {
		let outlier_ids: Vec<OwnedEventId> = self
			.services
			.rooms
			.outlier
			.room_stream(&room_id)
			.map(|(id, _)| id)
			.collect()
			.await;
		state_ids.extend(outlier_ids);
	}

	let mut state_ids = state_ids;
	if state_ids.is_empty() {
		if let Ok(latest) = self
			.services
			.rooms
			.timeline
			.latest_pdu_in_room(&room_id)
			.await
		{
			state_ids.push(Event::event_id(&latest).to_owned());
		}
	}

	if state_ids.is_empty() {
		return Err!(
			"Room {room_id} has no state and no forward extremities — completely empty?"
		);
	}

	self.write_str(&format!(
		"Auditing auth chain for {room_id} ({} seed events)...\n",
		state_ids.len()
	))
	.await?;

	// Deep walk the auth chain by parsing the actual PDUs
	// This finds true DAG holes that the DB index might miss if the events were
	// forcefully injected
	let fetcher = |event_id: OwnedEventId| {
		Box::pin(async move {
			let is_rejected = self
				.services
				.rooms
				.pdu_metadata
				.is_event_rejected(&event_id)
				.await;
			let is_soft_failed = self
				.services
				.rooms
				.pdu_metadata
				.is_event_soft_failed(&event_id)
				.await;

			if let Ok(pdu) = self.services.rooms.timeline.get_pdu(&event_id).await {
				conduwuit::utils::dag_walker::FetchResult::Timeline(
					pdu,
					is_rejected,
					is_soft_failed,
				)
			} else if let Ok(pdu) = self.services.rooms.outlier.get_pdu_outlier(&event_id).await {
				if verbose {
					let _ = self.write_str(&format!("  OUTLIER: {event_id}\n")).await;
				}
				conduwuit::utils::dag_walker::FetchResult::Outlier(
					pdu,
					is_rejected,
					is_soft_failed,
				)
			} else {
				if verbose {
					let _ = self.write_str(&format!("  MISSING: {event_id}\n")).await;
				}
				conduwuit::utils::dag_walker::FetchResult::Missing
			}
		})
	};

	let result = conduwuit::utils::dag_walker::walk_dag(state_ids.clone(), fetcher).await;

	let in_timeline = result.in_timeline;
	let in_outlier = result.in_outlier;
	let missing = result.missing;
	let rejected = result.rejected;
	let soft_failed = result.soft_failed;

	self.write_str(&format!(
		"Results: {in_timeline} timeline, {in_outlier} outlier-only, {} missing, {rejected} \
		 rejected, {soft_failed} soft-failed\n",
		missing.len()
	))
	.await?;

	if missing.is_empty() || !fetch {
		if !missing.is_empty() {
			use std::fmt::Write;
			let mut out = String::from("Missing event IDs:\n");
			for eid in &missing {
				let _ = writeln!(out, "  {eid}");
			}
			out.push_str("Hint: rerun with --fetch to attempt recovery from room servers.\n");
			self.write_str(&out).await?;
		}
		return Ok(());
	}

	// --fetch: reuse the battle-tested outlier fetch pipeline (32-server EMA
	// fallback, backoff, full signature validation, rate-limit tracking)

	let mut out = format!(
		"Fetching {} missing events via fetch_and_handle_outliers pipeline...\n",
		missing.len(),
	);
	for eid in &missing {
		let _ = writeln!(out, "  -> {eid}");
	}
	self.write_str(&out).await?;

	let fetched = self
		.services
		.rooms
		.event_handler
		.fetch_and_handle_outliers(
			self.services.globals.server_name(),
			missing.iter().map(AsRef::as_ref),
			None::<&PduEvent>,
			&room_id,
			false, // TODO: fetch doesn't skip signature verification currently
			None,
			if servers.is_empty() { None } else { Some(servers) },
		)
		.await;

	let mut result_out = format!(
		"Fetched {}/{} missing auth events (now in outlier store).\n",
		fetched.len(),
		missing.len()
	);
	let fetched_ids: HashSet<OwnedEventId> = fetched
		.iter()
		.map(|(pdu, _)| pdu.event_id().to_owned())
		.collect();
	for eid in &fetched_ids {
		let _ = writeln!(result_out, "  ✓ {eid}");
	}
	for eid in &missing {
		if !fetched_ids.iter().any(|fid| fid == eid) {
			let _ = writeln!(
				result_out,
				"  ✗ {eid} (fetch failed, was rejected, or was not found on any server)"
			);
		}
	}
	self.write_str(&result_out).await
}

#[admin_command]
pub(super) async fn fetch_missing_events(
	&self,
	room_id: OwnedRoomId,
	event_ids: Vec<OwnedEventId>,
	rounds: usize,
	override_limit: bool,
) -> Result {
	use std::time::Instant;

	let room_version = self.services.rooms.state.get_room_version(&room_id).await?;

	// Build EMA-sorted server list
	let servers = self
		.services
		.rooms
		.event_handler
		.build_federation_server_list(
			&room_id,
			self.services.globals.server_name(),
			self.services.server.config.federation_fallback_room_servers,
		)
		.await;

	// Use current forward extremities if no explicit targets are given
	let extremities: Vec<OwnedEventId> = self
		.services
		.rooms
		.state
		.get_forward_extremities(&room_id)
		.collect()
		.await;

	let mut current_targets = if event_ids.is_empty() {
		extremities.clone()
	} else {
		event_ids
	};

	if current_targets.len() > 50 && !override_limit {
		return Err!(
			"Refusing to trace backwards from more than 50 extremities/roots simultaneously \
			 (provided: {}). This can cause severe server load. Pass --override to bypass this \
			 safety check.",
			current_targets.len()
		);
	}

	self.write_str(&format!(
		"fetch-missing-events (Chunked Crawler): room={room_id} servers={} roots={} rounds={}\n",
		servers.len(),
		current_targets.len(),
		rounds,
	))
	.await?;

	let mut total_filled: usize = 0;

	for round in 1..=rounds {
		if current_targets.is_empty() {
			self.write_str(&format!("Round {round}: no targets left to trace backwards from.\n"))
				.await?;
			break;
		}

		// Ensure we don't exceed the Matrix specification limit of 100 latest_events
		if current_targets.len() > 100 {
			current_targets.truncate(100);
		}

		self.write_str(&format!(
			"Round {round}/{rounds}: Tracing backwards from {} roots...\n",
			current_targets.len()
		))
		.await?;

		let mut round_filled: usize = 0;
		let mut next_targets = HashSet::new();
		let mut success = false;

		// Try servers sequentially to avoid hammering the federation
		for server in &servers {
			self.write_str(&format!("  -> Querying {server}...\n"))
				.await?;
			let t = Instant::now();
			let res = self
				.services
				.sending
				.send_federation_request(server, get_missing_events::v1::Request {
					room_id: room_id.clone(),
					earliest_events: vec![], // Walk as far back as limit allows
					latest_events: current_targets.clone(),
					limit: 100_u32.into(),
					min_depth: 0_u32.into(),
				})
				.await;

			self.services
				.rooms
				.event_handler
				.update_peer_stats(server, res.is_ok(), t.elapsed());

			match res {
				| Ok(response) => {
					let events = response.events;
					if events.is_empty() {
						continue; // Try next server
					}

					for raw in events {
						if let Ok((event_id, value)) = self
							.services
							.server_keys
							.validate_and_add_event_id(raw.as_ref(), &room_version)
							.await
						{
							if self
								.services
								.rooms
								.outlier
								.get_pdu_outlier(&event_id)
								.await
								.is_err() && !self
								.services
								.rooms
								.timeline
								.pdu_exists(&event_id)
								.await
							{
								self.services
									.rooms
									.outlier
									.add_pdu_outlier(&event_id, &value, Some(&room_id))
									.await;
								round_filled = round_filled.saturating_add(1);

								// Collect prev_events of the newly fetched events as potential
								// next targets
								if let Ok(pdu) = PduEvent::from_id_val(
									&event_id,
									value.clone(),
									Some(room_id.as_ref()),
								) {
									for prev in pdu.prev_events() {
										if self
											.services
											.rooms
											.outlier
											.get_pdu_outlier(prev)
											.await
											.is_err() && !self
											.services
											.rooms
											.timeline
											.pdu_exists(prev)
											.await
										{
											next_targets.insert(prev.to_owned());
										}
									}
								}
							}
						}
					}

					success = true;
					self.write_str(&format!(
						"  [Success] Fetched {round_filled} new missing ancestors from \
						 {server}.\n"
					))
					.await?;
					break; // Found a server that has the events, stop checking others this round
				},
				| Err(_) => {},
			}
		}

		total_filled = total_filled.saturating_add(round_filled);

		if !success || round_filled == 0 {
			self.write_str("No new events found from any server this round, stopping early.\n")
				.await?;
			break;
		}

		current_targets = next_targets.into_iter().collect();
	}

	self.write_str(&format!(
		"Done. Total ancestors securely traced and stored as outliers: {total_filled}\n"
	))
	.await
}

#[admin_command]
pub(super) async fn dedup_room(&self, room_id: OwnedRoomId, dry_run: bool) -> Result {
	self.bail_restricted()?;

	let room_version = self.services.rooms.state.get_room_version(&room_id).await?;

	let shortroomid = self.services.rooms.short.get_shortroomid(&room_id).await?;

	let pdus: Vec<(PduCount, PduEvent)> = self
		.services
		.rooms
		.timeline
		.all_pdus(&room_id)
		.collect()
		.await;

	let total = pdus.len();
	info!("Scanning {total} timeline PDUs in {room_id} for wrong-hash and exact duplicates");

	let mut removed_wrong_hash = 0_usize;
	let mut removed_exact = 0_usize;
	let mut kept = 0_usize;
	let mut seen: HashSet<OwnedEventId> = HashSet::new();

	for (pdu_count, pdu) in &pdus {
		let stored_event_id = pdu.event_id();

		// Check for exact duplicates in the timeline
		if seen.contains(stored_event_id) {
			if dry_run {
				info!("Would remove exact duplicate: {stored_event_id}");
			} else {
				let pdu_id: conduwuit::matrix::pdu::RawPduId =
					conduwuit::matrix::pdu::PduId { shortroomid, shorteventid: *pdu_count }
						.into();
				self.services
					.rooms
					.timeline
					.drop_duplicate_pdu(&pdu_id)
					.await;
			}
			removed_exact = removed_exact.saturating_add(1);
			continue;
		}

		// Load the raw JSON to recompute the correct content-hash event_id
		let Ok(json) = self
			.services
			.rooms
			.timeline
			.get_pdu_json(stored_event_id)
			.await
		else {
			continue;
		};

		// Only strip event_id (DB events include it, federation events don't).
		// ruma's reference_hash handles redaction + stripping signatures/unsigned
		// per the room version spec.
		let mut hashable = json.clone();
		hashable.remove("event_id");

		let Ok(correct_event_id) =
			conduwuit::matrix::event::gen_event_id(&hashable, &room_version)
		else {
			continue;
		};

		if *stored_event_id != *correct_event_id {
			if dry_run {
				info!("Would remove wrong-hash: {stored_event_id} (correct: {correct_event_id})");
			} else {
				self.services
					.rooms
					.timeline
					.remove_from_timeline(stored_event_id)
					.await;
				self.services
					.rooms
					.outlier
					.remove_outlier(stored_event_id)
					.await;
			}
			removed_wrong_hash = removed_wrong_hash.saturating_add(1);
		} else {
			seen.insert(stored_event_id.to_owned());
			kept = kept.saturating_add(1);
		}

		let processed = kept
			.saturating_add(removed_wrong_hash)
			.saturating_add(removed_exact);
		if processed.is_multiple_of(1000) && processed > 0 {
			info!(
				"Dedup progress: {kept} kept, {removed_wrong_hash} wrong-hash, {removed_exact} \
				 exact duplicates of {total} total"
			);
		}
	}

	let action = if dry_run { "Would remove" } else { "Removed" };
	self.write_str(&format!(
		"{action} {removed_wrong_hash} wrong-hash duplicates and {removed_exact} exact \
		 duplicates out of {total} timeline PDUs. {kept} kept."
	))
	.await
}

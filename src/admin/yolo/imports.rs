use conduwuit::{Err, Result, err, info, warn};
use conduwuit_service::rooms::event_handler::AuthRecoveryStage;
use ruma::{
	CanonicalJsonObject, OwnedEventId, OwnedRoomId, RoomVersionId, events::StateEventType,
};

use crate::admin_command;

#[admin_command]
pub(super) async fn import_pdus(
	&self,
	path: String,
	room_id: Option<OwnedRoomId>,
	skip_auth: bool,
	skip_sig_verify: bool,
	force: bool,
	room_version: Option<RoomVersionId>,
) -> Result {
	use futures::StreamExt;
	use tokio::io::AsyncBufReadExt;
	self.bail_restricted()?;

	let inferred_room_id = match room_id {
		| Some(r) => r,
		| None => {
			let file = tokio::fs::File::open(&path)
				.await
				.map_err(|e| err!("Failed to open file {path}: {e}"))?;
			let mut reader = tokio::io::BufReader::new(file);
			let mut first_line = String::new();
			reader
				.read_line(&mut first_line)
				.await
				.map_err(|e| err!("Failed to read line: {e}"))?;

			if first_line.trim().is_empty() {
				return Err!(Request(InvalidParam("File is empty or first line is invalid")));
			}

			let first_pdu: CanonicalJsonObject = serde_json::from_str(&first_line)
				.map_err(|e| err!(Request(InvalidParam("Failed to parse first PDU: {e}"))))?;
			let r_id = first_pdu
				.get("room_id")
				.and_then(|v| v.as_str())
				.and_then(|s| ruma::RoomId::parse(s).ok());
			r_id.map(ToOwned::to_owned)
				.or_else(|| {
					let is_create =
						first_pdu.get("type").and_then(|v| v.as_str()) == Some("m.room.create");
					if is_create {
						let eid = first_pdu.get("event_id").and_then(|v| v.as_str())?;
						OwnedRoomId::parse(eid.replace('$', "!")).ok()
					} else {
						None
					}
				})
				.ok_or_else(|| {
					err!(Request(InvalidParam(
						"Could not infer room_id from first PDU. Please specify --room-id \
						 manually."
					)))
				})?
		},
	};
	let room_id = inferred_room_id;

	let room_version = match room_version {
		| Some(v) => {
			self.services.rooms.short.set_room_version(&room_id, &v);
			v
		},
		| None => match self.services.rooms.state.get_room_version(&room_id).await {
			| Ok(v) => v,
			| Err(_) => {
				return Err!(Request(InvalidParam(
					"Local room version unknown. You must specify --room-version explicitly \
					 when importing to an empty room."
				)));
			},
		},
	};

	let origin = room_id
		.server_name()
		.filter(|s| !self.services.globals.server_is_ours(s))
		.unwrap_or_else(|| self.services.globals.server_name())
		.to_owned();

	let mode = match (skip_auth, skip_sig_verify) {
		| (true, _) => "force-insert (skip-auth)",
		| (_, true) => "auth-checked (skip-sig-verify)",
		| _ => "full pipeline",
	};

	self.write_str(&format!(
		"Importing PDUs from {path} into {room_id} [{mode}] (in-memory)..\n"
	))
	.await?;

	let room_version_ref = room_version.clone();

	let room_id_ref = room_id.clone();
	let parsed_pdus: Vec<_> = tokio::task::spawn_blocking(move || {
		use std::io::BufRead;
		let file = std::fs::File::open(&path).expect("Failed to open file for parsing");
		let reader = std::io::BufReader::new(file);

		reader
			.lines()
			.map_while(Result::ok)
			.filter(|line| !line.trim().is_empty())
			.filter_map(|line| {
				let value: CanonicalJsonObject = match serde_json::from_str(&line) {
					| Ok(v) => v,
					| Err(e) => {
						warn!("Failed to parse JSON: {e}");
						return None;
					},
				};

				let is_outlier = value
					.get("__outlier")
					.and_then(ruma::CanonicalJsonValue::as_bool)
					.unwrap_or(false);
				let is_soft_failed = value
					.get("__soft_failed")
					.and_then(ruma::CanonicalJsonValue::as_bool)
					.unwrap_or(false);
				let is_rejected = value
					.get("__rejected")
					.and_then(ruma::CanonicalJsonValue::as_bool)
					.unwrap_or(false);

				let (eid, value, pdu_event) =
					match conduwuit::utils::pdu_parser::parse_and_clean_pdu(
						value,
						room_id_ref.as_ref(),
						&room_version_ref,
					) {
						| Ok(v) => v,
						| Err(e) => {
							warn!("Failed to parse_and_clean_pdu: {e}");
							return None;
						},
					};

				Some((eid, value, pdu_event, is_outlier, is_soft_failed, is_rejected))
			})
			.collect()
	})
	.await
	.unwrap();

	let total = parsed_pdus.len();

	// Cork database writes to batch and sync efficiently on drop
	let _cork = self.services.db.cork();

	let create_event = std::sync::Arc::new(
		self.services
			.rooms
			.state_accessor
			.room_state_get(&room_id, &StateEventType::RoomCreate, "")
			.await
			.ok(),
	);

	let chunks: Vec<Vec<_>> = parsed_pdus
		.chunks(5000)
		.map(
			<[(
				OwnedEventId,
				std::collections::BTreeMap<String, ruma::CanonicalJsonValue>,
				conduwuit::Pdu,
				bool,
				bool,
				bool,
			)]>::to_vec,
		)
		.collect();

	let inserted = std::sync::atomic::AtomicUsize::new(0);
	let rejected = std::sync::atomic::AtomicUsize::new(0);
	let failed = std::sync::atomic::AtomicUsize::new(0);

	futures::stream::iter(chunks)
		.for_each_concurrent(16, |chunk| async {
			let mut chunk_inserted: usize = 0;
			let mut chunk_rejected: usize = 0;
			let mut chunk_failed: usize = 0;
			let mut batch = self.services.rooms.timeline.db_batch();
			// promote_outlier_batch's writes land only once `batch` is applied
			// below; finalize each one's rejection/soft-fail markers afterward
			// via finish_promote_outlier (see that method's doc comment).
			let mut queued_promotions: Vec<OwnedEventId> = Vec::new();

			for (eid, value, pdu, is_outlier, is_soft_failed, is_rejected) in chunk {
				let is_outlier = is_outlier || force;

				let (skip_further, eid, value, pdu) = if is_outlier {
					(true, eid, value, pdu)
				} else if skip_auth {
					(false, eid, value, pdu)
				} else {
					let (eid, val) = if skip_sig_verify {
						(eid, value.clone())
					} else {
						let mut raw_val = value.clone();
						if room_version != RoomVersionId::V1 && room_version != RoomVersionId::V2
						{
							raw_val.remove("event_id");
						}
						let raw = match serde_json::value::RawValue::from_string(
							serde_json::to_string(&raw_val)
								.map_err(|e| e.to_string())
								.unwrap_or_default(),
						) {
							| Ok(r) => r,
							| Err(e) => {
								warn!("import_pdus insert err: {e}");
								chunk_failed = chunk_failed.saturating_add(1);
								continue;
							},
						};

						match self
							.services
							.server_keys
							.validate_and_add_event_id(&raw, &room_version)
							.await
						{
							| Ok(result) => result,
							| Err(_) => {
								(eid, value.clone()) // Will handle as outlier due to failure
							},
						}
					};

					let mut pdu_val = val;
					if room_version != RoomVersionId::V1 && room_version != RoomVersionId::V2 {
						pdu_val.remove("event_id");
					}

					let handled = Box::pin(self.services.rooms.event_handler.handle_outlier_pdu(
						&origin,
						create_event.as_ref().as_ref(),
						&eid,
						&room_id,
						pdu_val,
						true,
						skip_sig_verify,
						Some(&room_version),
						AuthRecoveryStage::BeforeStateIds,
					))
					.await;

					match handled {
						| Ok((new_pdu, _)) => {
							let new_pdu_event = new_pdu.clone();
							(false, eid, value, new_pdu_event)
						},
						| Err(_) => {
							// If failed, treat as outlier
							(true, eid, value, pdu)
						},
					}
				};

				let insert_result: Result<(OwnedEventId, bool)> = async {
					if skip_further && is_outlier {
						self.services
							.rooms
							.outlier
							.add_pdu_outlier(&eid, &value, Some(&room_id))
							.await;
						return Ok((eid.clone(), true));
					}
					if force {
						if let Ok(pdu_id) = self.services.rooms.timeline.get_pdu_id(&eid).await {
							self.services
								.rooms
								.timeline
								.replace_pdu(&pdu_id, &value, &eid)
								.await?;
							return Ok((eid.clone(), true));
						}
					}
					if skip_auth {
						self.services
							.rooms
							.timeline
							.force_insert_pdu_batch(
								&mut batch, &room_id, &eid, &pdu, &value, true,
							)
							.await?;
					} else {
						// Batched like the skip_auth branch above -- one DB commit (and
						// one /sync wake) per chunk instead of per event. Finalizing
						// (releasing the reservation, resolving rejection markers)
						// happens after `batch` is applied below -- see
						// finish_promote_outlier's doc comment.
						let outcome = self
							.services
							.rooms
							.timeline
							.promote_outlier_batch(&mut batch, &room_id, &eid)
							.await?;
						if matches!(
							outcome,
							conduwuit_service::rooms::timeline::PromoteOutlierOutcome::Queued
						) {
							queued_promotions.push(eid.clone());
						}
					}

					Ok((eid.clone(), true))
				}
				.await;

				match insert_result {
					| Ok((eid, true)) => {
						chunk_inserted = chunk_inserted.saturating_add(1);
						if is_soft_failed {
							self.services
								.rooms
								.pdu_metadata
								.mark_event_soft_failed(&eid, "imported as soft-failed");
						}
						if is_rejected {
							self.services
								.rooms
								.pdu_metadata
								.mark_event_rejected(&eid, "imported as rejected")
								.await;
						}
					},
					| Ok((_eid, false)) => {
						chunk_rejected = chunk_rejected.saturating_add(1);
					},
					| Err(e) => {
						warn!("import_pdus insert err: {e}");
						chunk_failed = chunk_failed.saturating_add(1);
					},
				}
			}

			self.services.rooms.timeline.db_apply_batch(batch);
			for eid in &queued_promotions {
				self.services
					.rooms
					.timeline
					.finish_promote_outlier(&room_id, eid)
					.await;
			}
			info!(
				"Finished a chunk: {chunk_inserted} inserted, {chunk_rejected} rejected, \
				 {chunk_failed} failed"
			);
			inserted.fetch_add(chunk_inserted, std::sync::atomic::Ordering::Relaxed);
			rejected.fetch_add(chunk_rejected, std::sync::atomic::Ordering::Relaxed);
			failed.fetch_add(chunk_failed, std::sync::atomic::Ordering::Relaxed);
		})
		.await;

	let inserted = inserted.load(std::sync::atomic::Ordering::Relaxed);
	let rejected = rejected.load(std::sync::atomic::Ordering::Relaxed);
	let failed = failed.load(std::sync::atomic::Ordering::Relaxed);

	let (_, num_true) = self
		.services
		.rooms
		.timeline
		.recalculate_extremities(&room_id, true)
		.await?;

	self.write_str(&format!(
		"\nImported {inserted} PDUs, {rejected} stored as rejected outliers, {failed} errors \
		 out of {total} total for 		 {room_id}. DAG Extremities recalculated (now {num_true} \
		 tips). Run `yolo rebuild-state 		 {room_id}` if you are finished importing."
	))
	.await
}

#[admin_command]
pub(super) async fn import_outliers(&self, jsonl: String) -> Result {
	self.bail_restricted()?;
	let mut count = 0_usize;

	for line in jsonl.lines() {
		if line.trim().is_empty() {
			continue;
		}

		let pdu: CanonicalJsonObject = serde_json::from_str(line).map_err(|e| {
			err!(
				"Failed to parse PDU JSON: {e:?}. Make sure it's valid JSON on each line of the \
				 code block."
			)
		})?;

		let event_id = pdu
			.get("event_id")
			.and_then(ruma::CanonicalJsonValue::as_str)
			.and_then(|id| OwnedEventId::parse(id).ok())
			.ok_or_else(|| err!("Missing or invalid event_id in PDU JSON"))?;

		self.services
			.rooms
			.outlier
			.add_pdu_outlier(&event_id, &pdu, None)
			.await;
		count = count.saturating_add(1);
	}

	self.write_str(&format!("Successfully imported {count} outliers."))
		.await
}

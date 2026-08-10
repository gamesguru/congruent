use std::{collections::HashMap, sync::Arc};

use conduwuit_core::{
	Result, info,
	matrix::{
		Event,
		pdu::{PduCount, PduId, RawPduId},
	},
	warn,
};
use futures::StreamExt;
use roaring::RoaringTreemap;
use ruma::{OwnedEventId, RoomId};

use super::{Service, metadata::EventMetadata};
use crate::rooms::short::ShortEventId;

/// Statistics returned from `reindex_short`.
#[derive(Debug, Default)]
pub struct ReindexStats {
	pub total_events: usize,
	pub missing_pdu: usize,
	pub hash_mismatches: usize,
	pub repaired_short_ids: usize,
	pub repaired_metadata: usize,
	pub repaired_prev_events: usize,
	pub repaired_auth_events: usize,
	pub repaired_auth_chains: usize,
	pub repaired_topo_index: usize,
	pub repaired_relations: usize,
	pub repaired_references: usize,
	pub repaired_search_index: usize,
	pub extremities_updated: bool,
	pub extremities_count: usize,
}

impl std::fmt::Display for ReindexStats {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			f,
			"total={}, missing_pdu={}, hash_mismatches={}, short_ids={}, metadata={}, \
			 prev_events={}, auth_events={}, auth_chains={}, topo_index={}, relations={}, \
			 references={}, search={}, extremities={} (updated={})",
			self.total_events,
			self.missing_pdu,
			self.hash_mismatches,
			self.repaired_short_ids,
			self.repaired_metadata,
			self.repaired_prev_events,
			self.repaired_auth_events,
			self.repaired_auth_chains,
			self.repaired_topo_index,
			self.repaired_relations,
			self.repaired_references,
			self.repaired_search_index,
			self.extremities_count,
			self.extremities_updated,
		)
	}
}

impl Service {
	/// Sweep all events in a room and repopulate any missing derived data
	/// from the `eventid_pdu` source of truth.
	///
	/// This is safe to run at any time. It preserves canonical stream order and
	/// existing local topo depths, while rebuilding the room topo index from
	/// the stream source of truth.
	pub async fn reindex_short(
		&self,
		room_id: &RoomId,
		rebuild_topo: bool,
	) -> Result<ReindexStats> {
		let shortroomid = self.services.short.get_or_create_shortroomid(room_id).await;
		let room_version = self.services.state.get_room_version(room_id).await?;
		let mut stats = ReindexStats::default();

		info!("reindex_short: collecting all events for {room_id}...");

		// Phase 1: Collect all events from the stream index
		let mut events: Vec<(PduCount, OwnedEventId)> = Vec::new();
		{
			let mut stream =
				std::pin::pin!(self.db.room_event_ids_rev(room_id, None).chunks(1024));
			while let Some(chunk) = stream.next().await {
				let chunk_futures =
					chunk
						.into_iter()
						.filter_map(Result::ok)
						.map(|eid| async move {
							if let Ok(count) = self.get_pdu_count(&eid).await {
								Some((count, eid))
							} else {
								None
							}
						});
				let res = futures::future::join_all(chunk_futures).await;
				events.extend(res.into_iter().flatten());
			}
		}
		events.reverse(); // Forward order (oldest first)
		stats.total_events = events.len();
		info!("reindex_short: found {} events in stream index", events.len());

		// Phase 2: For each event, read PDU JSON and repair derived data
		let cork = self.db.db.cork();
		let cleared_topo = if rebuild_topo {
			self.db.clear_room_topo_index(room_id).await?
		} else {
			0
		};
		let mut topo_batch = self.db.db_batch();
		let mut topo_batch_len = 0_usize;
		if rebuild_topo {
			info!("reindex_short: cleared {cleared_topo} topo index rows for {room_id}");
		}

		// Auth chain cache for incremental computation (roaring bitmaps)
		let mut auth_chain_cache: HashMap<ShortEventId, Arc<RoaringTreemap>> = HashMap::new();

		for (i, (count, event_id)) in events.iter().enumerate() {
			if i.is_multiple_of(1000) {
				info!(
					"reindex_short: room={room_id} progress: {i}/{} events (hash_mismatches={}, \
					 metadata={}, prev_events={}, auth_events={}, auth_chains={}, search={})",
					stats.total_events,
					stats.hash_mismatches,
					stats.repaired_metadata,
					stats.repaired_prev_events,
					stats.repaired_auth_events,
					stats.repaired_auth_chains,
					stats.repaired_search_index,
				);
			}

			let Ok((pdu, raw_bytes)) = self.db.get_pdu_and_raw_bytes(event_id).await else {
				stats.missing_pdu = stats.missing_pdu.saturating_add(1);
				continue;
			};

			// --- Event ID hash validation ---
			match conduwuit_core::matrix::event::gen_event_id_from_bytes(
				&raw_bytes,
				&room_version,
			) {
				| Ok(expected_id) =>
					if expected_id != *event_id {
						warn!(
							"HASH_MISMATCH: room={room_id}, event={event_id}, \
							 expected={expected_id}. Stored JSON does not match event ID hash."
						);
						stats.hash_mismatches = stats.hash_mismatches.saturating_add(1);
					},
				| Err(e) => {
					warn!(
						"HASH_ERROR: room={room_id}, event={event_id}, error={e:?}. Could not \
						 generate event ID from stored bytes."
					);
				},
			}

			// --- Short ID mappings ---
			let was_missing = self
				.services
				.short
				.get_shorteventid(event_id)
				.await
				.is_err();
			let short_eid = self
				.services
				.short
				.get_or_create_shorteventid(event_id)
				.await;

			if was_missing {
				stats.repaired_short_ids = stats.repaired_short_ids.saturating_add(1);
			}

			let pdu_id: RawPduId = PduId { shortroomid, shorteventid: *count }.into();

			// --- eventid_metadata ---
			let metadata = match self.db.get_event_metadata(event_id).await {
				| Ok(meta) => meta,
				| Err(_) => {
					let meta = EventMetadata {
						short_room_id: shortroomid,
						is_outlier: false,
						origin_server_ts: pdu.origin_server_ts().0,
						depth: pdu.depth(),
						soft_failed: false,
						rejected: pdu.rejected(),
						redacted_by: pdu.redacts().map(ToOwned::to_owned),
						short_state_hash: None,
						deprecated_local_topo_depth: pdu.depth().into(),
						pdu_count: match count {
							| PduCount::Normal(x) => Some(*x),
							| PduCount::Backfilled(_) => None,
						},
						soft_fail_reason: String::new(),
						rejection_reason: String::new(),
					};
					if let Ok(bytes) = bincode::serialize(&meta) {
						self.db.store_eventid_metadata(event_id.as_bytes(), bytes);
						stats.repaired_metadata = stats.repaired_metadata.saturating_add(1);
					}
					meta
				},
			};

			if rebuild_topo {
				// --- roomid_topologicalorder_pducount ---
				self.db.insert_topo_pducount_into_batch(
					&mut topo_batch,
					&pdu_id,
					event_id,
					metadata.deprecated_local_topo_depth,
				);
				stats.repaired_topo_index = stats.repaired_topo_index.saturating_add(1);
				topo_batch_len = topo_batch_len.saturating_add(1);
				if topo_batch_len >= 1000 {
					self.db.db_apply_batch(topo_batch);
					topo_batch = self.db.db_batch();
					topo_batch_len = 0;
				}
			}

			// --- shorteventid_shortprevevents ---
			let prev_event_ids: Vec<OwnedEventId> =
				pdu.prev_events().map(ToOwned::to_owned).collect();

			let mut prev_shorts = Vec::with_capacity(prev_event_ids.len());
			for prev_id in &prev_event_ids {
				prev_shorts.push(
					self.services
						.short
						.get_or_create_shorteventid(prev_id)
						.await,
				);
			}

			let stored_prev_shorts = self.db.get_shortprevevents(short_eid).await.ok();
			if stored_prev_shorts.as_ref() != Some(&prev_shorts) {
				self.db.store_shortprevevents(short_eid, &prev_shorts);
				stats.repaired_prev_events = stats.repaired_prev_events.saturating_add(1);
			}

			// --- shorteventid_shortauthevents ---
			let auth_event_ids: Vec<OwnedEventId> =
				pdu.auth_events().map(ToOwned::to_owned).collect();

			let auth_shorts: Vec<ShortEventId> = {
				let mut shorts = Vec::with_capacity(auth_event_ids.len());
				for auth_id in &auth_event_ids {
					shorts.push(
						self.services
							.short
							.get_or_create_shorteventid(auth_id)
							.await,
					);
				}
				shorts
			};

			let stored_auth_shorts = self.db.get_shortauthevents(short_eid).await.ok();
			let auth_events_repaired = stored_auth_shorts.as_ref() != Some(&auth_shorts);
			if auth_events_repaired {
				self.db.store_shortauthevents(short_eid, &auth_shorts);
				stats.repaired_auth_events = stats.repaired_auth_events.saturating_add(1);
			}

			// --- shorteventid_authchain (incremental) ---
			// auth_chain[e] = auth_events(e) ∪ ⋃(auth_chain[ae])
			if auth_events_repaired
				|| self
					.services
					.auth_chain
					.get_cached_eventid_authchain(&[short_eid])
					.await
					.is_err()
			{
				let mut full_chain = RoaringTreemap::new();
				for &auth_short in &auth_shorts {
					full_chain.insert(auth_short);
					// Use our local cache (built during this sweep) for ancestors
					if let Some(ancestor_chain) = auth_chain_cache.get(&auth_short) {
						full_chain |= ancestor_chain.as_ref();
					}
				}

				let chain_arc = Arc::new(full_chain);
				self.services
					.auth_chain
					.cache_auth_chain_bitmap(vec![short_eid], &chain_arc);
				auth_chain_cache.insert(short_eid, chain_arc);
				stats.repaired_auth_chains = stats.repaired_auth_chains.saturating_add(1);
			} else if let Ok(existing) = self
				.services
				.auth_chain
				.get_cached_eventid_authchain(&[short_eid])
				.await
			{
				// Populate local cache for descendants
				auth_chain_cache.insert(short_eid, existing);
			}

			// --- tofrom_relation ---
			if let Ok(content) = pdu.get_content::<super::ExtractRelatesToEventId>() {
				if let Ok(related_count) = self.get_pdu_count(&content.relates_to.event_id).await
				{
					self.services
						.pdu_metadata
						.add_relation(*count, related_count);
					stats.repaired_relations = stats.repaired_relations.saturating_add(1);
				}
			}

			// --- referencedevents ---
			if !prev_event_ids.is_empty() {
				self.services
					.pdu_metadata
					.mark_as_referenced(room_id, prev_event_ids.iter().map(AsRef::as_ref));
				stats.repaired_references = stats.repaired_references.saturating_add(1);
			}

			// --- tokenids (search index) ---
			self.index_pdu_search(shortroomid, &pdu_id, &pdu);
			stats.repaired_search_index = stats.repaired_search_index.saturating_add(1);
		}

		if rebuild_topo && topo_batch_len > 0 {
			self.db.db_apply_batch(topo_batch);
		}

		// --- Forward extremities (roomid_pduleaves) ---
		let (extremities_updated, extremities_count) =
			self.recalculate_extremities(room_id, true).await?;
		stats.extremities_count = extremities_count;
		stats.extremities_updated = extremities_updated;

		drop(cork);

		info!("reindex_short: completed for {room_id}: {stats}");
		Ok(stats)
	}
}

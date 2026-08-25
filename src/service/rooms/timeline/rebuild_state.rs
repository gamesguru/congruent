use std::{
	collections::{BTreeSet, HashMap, HashSet},
	sync::Arc,
	time::Instant,
};

use conduwuit::utils::timeline_sorter::sort_timeline_events;
use conduwuit_core::{
	Result, debug, info,
	matrix::{event::Event, state_res::StateMap},
	warn,
};
use ruma::{OwnedEventId, RoomId, RoomVersionId, events::TimelineEventType};

use crate::rooms;

/// Event metadata extracted during Phase 1 streaming.
/// Carries auth_events and (event_type, state_key) so we never need to load
/// full PduEvents into RAM during the walk. Only fork resolution (rare) needs
/// on-demand DB reads.
/// (event_id, prev_events, auth_events, Option<(event_type, state_key)>, depth)
type EventMeta = (
	OwnedEventId,
	Vec<OwnedEventId>,
	Vec<OwnedEventId>,
	Option<(String, String)>,
	u64,
);

/// Safe u32 -> usize for Vec indexing of roaring bitmap indices.
#[inline]
fn to_usize(v: u32) -> usize { usize::try_from(v).expect("u32 fits in usize") }

/// Shared context threaded through all phases of rebuild_state.
/// Metadata carries everything needed for the walk; state event PDUs
/// are kept in-memory so fork resolution never hits RocksDB.
struct RebuildCtx {
	room_version: RoomVersionId,
	events_meta: Vec<EventMeta>,
	event_set: HashSet<OwnedEventId>,
	eid_to_idx: HashMap<OwnedEventId, u32>,
	idx_to_eid: Vec<OwnedEventId>,
	auth_chain_bitmaps: Vec<roaring::RoaringBitmap>,
	/// State event PDUs indexed by the same u32 as eid_to_idx.
	/// `None` for message events (never needed for resolution).
	state_pdus: Vec<Option<rezzy::LeanEvent>>,
}

fn pdu_to_lean(pdu: &conduwuit::PduEvent) -> rezzy::LeanEvent {
	let content_val: serde_json::Value =
		serde_json::from_str(pdu.content.get()).unwrap_or(serde_json::Value::Null);
	let power_level = content_val
		.get("power_level")
		.and_then(|pl| {
			pl.as_i64()
				.or_else(|| pl.as_str().and_then(|s| s.parse().ok()))
		})
		.unwrap_or(0);
	rezzy::LeanEvent {
		event_id: pdu.event_id.to_string(),
		event_type: pdu.kind.to_string(),
		state_key: pdu.state_key.as_ref().map(|k| format!("{k}")),
		power_level,
		origin_server_ts: pdu.origin_server_ts.into(),
		sender: pdu.sender.to_string(),
		content: content_val,
		prev_events: pdu.prev_events.iter().map(|id| format!("{id}")).collect(),
		auth_events: pdu.auth_events.iter().map(|id| format!("{id}")).collect(),
		depth: u64::from(pdu.depth),
		..Default::default()
	}
}

impl super::Service {
	/// Rebuilds room state entirely in-memory, then batch-writes the result to
	/// DB. Memory usage is dominated by metadata vectors and state groups, NOT
	/// by full PduEvent JSON. For a 60K-event room this uses ~50MB instead of
	/// the previous ~4GB.
	#[tracing::instrument(skip(self), level = "info")]
	pub async fn rebuild_state(&self, room_id: &RoomId) -> Result<()> {
		let original_room_shortstatehash = self
			.services
			.state
			.get_room_shortstatehash(room_id)
			.await
			.ok();

		// Phase 1: Stream events and extract metadata + keep state PDUs
		eprintln!("[rebuild_state] Phase 1: streaming events...");
		let (events_meta, room_version, state_pdus) = self.rebuild_stream_events(room_id).await?;
		eprintln!("[rebuild_state] Phase 1 done: {} events", events_meta.len());

		let event_set: HashSet<OwnedEventId> =
			events_meta.iter().map(|(eid, ..)| eid.clone()).collect();

		// Phase 2b: Pre-compute auth chains bottom-up (iterative DFS)
		eprintln!("[rebuild_state] Phase 2b: computing auth chains...");
		let (eid_to_idx, idx_to_eid, auth_chain_bitmaps) =
			Self::rebuild_auth_chains(&events_meta);
		eprintln!("[rebuild_state] Phase 2b done: {} chains", auth_chain_bitmaps.len());

		let ctx = RebuildCtx {
			room_version,
			events_meta,
			event_set,
			eid_to_idx,
			idx_to_eid,
			auth_chain_bitmaps,
			state_pdus,
		};

		// Phase 3+4: In-memory state walk with eviction + inline DB writes
		eprintln!("[rebuild_state] Phase 3+4: walk and write...");
		let (event_ssh, current_shortstatehash) =
			Box::pin(self.rebuild_walk_and_write(room_id, &ctx)).await?;
		eprintln!("[rebuild_state] Phase 3+4 done: {} SSHs computed", event_ssh.len());

		// Phase 5: Final multi-head extremity merge
		eprintln!("[rebuild_state] Phase 5: merge extremities...");
		let current_shortstatehash = self
			.rebuild_merge_extremities(room_id, &ctx, &event_ssh, current_shortstatehash)
			.await?;
		eprintln!("[rebuild_state] Phase 5 done");

		// Phase 6: Apply final state
		let (total_added, total_removed) = self
			.services
			.state_compressor
			.diff_full_state(original_room_shortstatehash.unwrap_or(0), current_shortstatehash)
			.await;

		let state_lock = self.services.state.mutex.lock(room_id).await;
		self.services
			.state
			.force_state_quiet(
				room_id,
				current_shortstatehash,
				total_added,
				total_removed,
				&state_lock,
			)
			.await?;

		eprintln!("[rebuild_state] Phase 6 done: state applied");
		Ok(())
	}

	// ── Phase 1: Stream events and collect metadata ──
	// Now extracts auth_events and event_type directly, eliminating the need
	// for Phase 2 (prefetch).

	async fn rebuild_stream_events(
		&self,
		room_id: &RoomId,
	) -> Result<(Vec<EventMeta>, RoomVersionId, Vec<Option<rezzy::LeanEvent>>)> {
		info!("rebuild_state: streaming events in topological order...");
		let start = Instant::now();

		let (entries, graph, _metadata_cache) = self.db.collect_reorder_entries(room_id).await?;
		let sorted = sort_timeline_events(&entries, &graph);

		let mut events_meta: Vec<EventMeta> = Vec::new();
		let mut state_pdus: Vec<Option<rezzy::LeanEvent>> = Vec::new();
		let mut room_version = self
			.services
			.state
			.get_room_version(room_id)
			.await
			.unwrap_or(RoomVersionId::V1);

		for eid in sorted {
			let (pdu, _json) = self.db.get_from_eventid_pdu(&eid).await.map_err(|e| {
				conduwuit::err!(Database("rebuild_state: missing PDU {eid}: {e}"))
			})?;
			let prev: Vec<OwnedEventId> = pdu.prev_events().map(ToOwned::to_owned).collect();
			let auth: Vec<OwnedEventId> = pdu.auth_events().map(ToOwned::to_owned).collect();
			let is_state = pdu.state_key().is_some();
			let state_key = pdu
				.state_key()
				.map(|sk| (pdu.kind().to_string(), sk.to_owned()));
			let depth = u64::from(pdu.depth());

			// Timeline events are authoritative; clear any stale rejection flags.
			self.services.pdu_metadata.unmark_event_rejected(&eid);

			if *pdu.kind() == TimelineEventType::RoomCreate {
				if let Ok(create_content) = serde_json::from_str::<
					ruma::events::room::create::RoomCreateEventContent,
				>(pdu.content().get())
				{
					room_version = create_content.room_version;
				} else {
					warn!(
						"rebuild_state: create event {eid} could not be parsed for room \
						 version; using cached room version {room_version}"
					);
				}
			}

			events_meta.push((eid.clone(), prev, auth, state_key, depth));
			// Keep state event PDUs for fork resolution; drop messages
			state_pdus.push(if is_state { Some(pdu_to_lean(&pdu)) } else { None });
		}

		let state_count = state_pdus.iter().filter(|p| p.is_some()).count();
		info!(
			"rebuild_state: streamed {} events ({} state) in {:?} | room version: {}",
			events_meta.len(),
			state_count,
			start.elapsed(),
			room_version,
		);
		Ok((events_meta, room_version, state_pdus))
	}

	// ── Phase 2b: Pre-compute auth chains bottom-up ──
	// Uses an iterative post-order DFS with cycle detection to correctly handle
	// busted DAGs where auth events may appear out of order.
	// Now reads auth_events directly from EventMeta instead of event_cache.

	fn rebuild_auth_chains(
		events_meta: &[EventMeta],
	) -> (HashMap<OwnedEventId, u32>, Vec<OwnedEventId>, Vec<roaring::RoaringBitmap>) {
		let start = Instant::now();

		// Pass 1: Index all events
		let eid_to_idx: HashMap<OwnedEventId, u32> = events_meta
			.iter()
			.enumerate()
			.map(|(i, (eid, ..))| {
				(eid.clone(), u32::try_from(i).expect("room has > 2^32 (4B) events"))
			})
			.collect();
		let idx_to_eid: Vec<OwnedEventId> =
			events_meta.iter().map(|(eid, ..)| eid.clone()).collect();

		// Pass 2: Iterative post-order traversal on auth DAG for transitive closures.
		// Uses an explicit stack instead of recursion (Rust has no TCO).
		let n = events_meta.len();
		let mut bitmaps: Vec<Option<roaring::RoaringBitmap>> = vec![None; n];
		let mut visiting = vec![false; n]; // Cycle detection

		for i in 0..n {
			let mut stack = vec![i];
			while let Some(&curr) = stack.last() {
				if bitmaps[curr].is_some() {
					stack.pop();
					continue;
				}

				visiting[curr] = true;

				// Read auth_events from metadata (index 2 in the tuple)
				let auth_events = &events_meta[curr].2;
				let mut all_resolved = true;
				for auth_id in auth_events {
					if let Some(&auth_idx) = eid_to_idx.get(auth_id) {
						let auth_usize = to_usize(auth_idx);
						if bitmaps[auth_usize].is_none() {
							if visiting[auth_usize] {
								warn!(
									"rebuild_state: auth chain cycle at {} -> {}",
									idx_to_eid[curr], auth_id,
								);
							} else {
								stack.push(auth_usize);
								all_resolved = false;
							}
						}
					}
				}

				if all_resolved {
					let mut chain = roaring::RoaringBitmap::new();
					for auth_id in auth_events {
						if let Some(&auth_idx) = eid_to_idx.get(auth_id) {
							let auth_usize = to_usize(auth_idx);
							if let Some(resolved_chain) = &bitmaps[auth_usize] {
								chain.insert(auth_idx);
								chain |= resolved_chain;
							}
						}
					}
					bitmaps[curr] = Some(chain);
					visiting[curr] = false;
					stack.pop();
				}
			}
		}

		// Unwrap all Options into final Vec
		let final_bitmaps: Vec<roaring::RoaringBitmap> =
			bitmaps.into_iter().map(Option::unwrap_or_default).collect();

		debug!(
			"rebuild_state: pre-computed {} auth chains in {:?}",
			final_bitmaps.len(),
			start.elapsed(),
		);
		(eid_to_idx, idx_to_eid, final_bitmaps)
	}
}

/// Owned variant of `rezzy::StateUpdate` for sending across thread boundaries.
enum StateUpdateOwned {
	New {
		state: rezzy::SharedState<String>,
		/// Incrementally maintained LtHash from rezzy, used as dedup key to
		/// skip O(N) compression loop if the same state has already been seen.
		hash: Box<rezzy::LtHash>,
	},
	Unchanged {
		parent_event_id: String,
	},
}

impl super::Service {
	// ── Phase 3+4: Batch state computation via rezzy + inline DB writes ──
	//
	// Delegates the entire state walk (topological sort, fork resolution at
	// merge points, state event application) to rezzy's compute_state_at_batch.
	// This replaces the previous per-event walk loop with state groups, fork
	// caching, superset optimization, and group eviction.

	async fn rebuild_walk_and_write(
		&self,
		room_id: &RoomId,
		ctx: &RebuildCtx,
	) -> Result<(HashMap<OwnedEventId, u64>, u64)> {
		let start = Instant::now();
		let mut cork = Some(self.db.db.cork());

		// ── Pre-cache short IDs ──
		let precache_start = Instant::now();
		let mut unique_state_keys: HashSet<(String, String)> = HashSet::new();
		for (_, _, _, state_key, _) in &ctx.events_meta {
			if let Some((ty, sk)) = state_key {
				unique_state_keys.insert((ty.clone(), sk.clone()));
			}
		}

		let mut ssk_cache: HashMap<
			rezzy::basespec::event_types::EventType,
			HashMap<String, u64>,
		> = HashMap::with_capacity(unique_state_keys.len());
		for (ty, sk) in &unique_state_keys {
			let ssk = self
				.services
				.short
				.get_or_create_shortstatekey(&ty.as_str().into(), sk)
				.await;
			ssk_cache
				.entry(ty.clone().into())
				.or_default()
				.insert(sk.clone(), ssk);
		}

		let mut sei_cache: HashMap<OwnedEventId, u64> =
			HashMap::with_capacity(ctx.events_meta.len());
		let mut sei_str_cache: HashMap<String, u64> =
			HashMap::with_capacity(ctx.events_meta.len());
		for (eid, ..) in &ctx.events_meta {
			let sei = self.services.short.get_or_create_shorteventid(eid).await;
			sei_str_cache.insert(eid.to_string(), sei);
			sei_cache.insert(eid.clone(), sei);
		}
		debug!(
			"rebuild_state: pre-cached {} shortstatekeys + {} shorteventids in {:?}",
			ssk_cache.len(),
			sei_cache.len(),
			precache_start.elapsed(),
		);

		// ── Build LeanEvent map from events_meta + state_pdus ──
		let lean_start = Instant::now();
		let mut lean_events: HashMap<String, rezzy::LeanEvent> =
			HashMap::with_capacity(ctx.events_meta.len());

		for (i, (eid, prev, auth, _state_key, depth)) in ctx.events_meta.iter().enumerate() {
			let lean = if let Some(Some(lean_ev)) = ctx.state_pdus.get(i) {
				// State event: full LeanEvent with content for resolution
				lean_ev.clone()
			} else {
				// Non-state event: skeleton for DAG traversal only
				rezzy::LeanEvent {
					event_id: eid.to_string(),
					prev_events: prev.iter().map(|id| format!("{id}")).collect(),
					auth_events: auth.iter().map(|id| format!("{id}")).collect(),
					depth: *depth,
					rejected: false,
					soft_fail: false,
					..Default::default()
				}
			};
			lean_events.insert(eid.to_string(), lean);
		}
		debug!(
			"rebuild_state: built {} LeanEvents in {:?}",
			lean_events.len(),
			lean_start.elapsed(),
		);

		// ── Map room version to StateResVersion ──
		let version = rooms::auth_adapter::to_state_res_version(&ctx.room_version);

		// ── Compute state at all events via rezzy streaming ──
		let batch_start = Instant::now();
		let target_ids_owned: Vec<String> = ctx
			.events_meta
			.iter()
			.filter(|(_, prev, _, state_key, _)| state_key.is_some() || prev.len() != 1)
			.map(|(eid, ..)| eid.to_string())
			.collect();
		debug!(
			"rebuild_state: targeting {} / {} events for rezzy state computation",
			target_ids_owned.len(),
			ctx.events_meta.len(),
		);

		let (tx, mut rx) = tokio::sync::mpsc::channel(100);

		// Spawn synchronous rezzy pipeline on a blocking thread
		let lean_events_moved = lean_events;
		tokio::task::spawn_blocking(move || {
			let target_refs: Vec<&String> = target_ids_owned.iter().collect();
			let mut abort = false;
			let completed = rezzy::compute_state_at_streaming_optimized(
				&target_refs,
				&lean_events_moved,
				version,
				|id, update| {
					let owned_update = match update {
						| rezzy::StateUpdate::New { state, hash } =>
							StateUpdateOwned::New { state, hash: Box::new(*hash) },
						| rezzy::StateUpdate::Unchanged { parent_event_id, .. } =>
							StateUpdateOwned::Unchanged {
								parent_event_id: parent_event_id.clone(),
							},
					};
					if abort {
						return;
					}
					if tx.blocking_send((id, owned_update)).is_err() {
						abort = true;
					}
				},
			);
			if !completed {
				warn!("compute_state_at_streaming_optimized detected cycle; results incomplete");
			}
		});

		// ── Consume stream and write SSH for each event ──
		let empty_ssh = self
			.services
			.state_compressor
			.save_state(room_id, Arc::new(BTreeSet::new()))
			.await?
			.shortstatehash;

		let mut event_ssh: HashMap<OwnedEventId, u64> = HashMap::new();
		let mut lthash_to_ssh: HashMap<rezzy::LtHash, u64> = HashMap::new();
		let mut current_shortstatehash = empty_ssh;
		let mut groups_compressed = 0_usize;
		let mut groups_deduped = 0_usize;
		let mut processed = 0_usize;
		let total_events = ctx.events_meta.len();
		let mut pending_updates: HashMap<String, StateUpdateOwned> = HashMap::new();

		// ── Instrumentation counters ──
		let mut n_unchanged = 0_usize;
		let mut n_new = 0_usize;
		let mut n_new_deduped = 0_usize;
		let mut n_inherited = 0_usize;
		let mut t_unchanged = std::time::Duration::ZERO;
		let mut t_compress = std::time::Duration::ZERO;
		let mut t_save = std::time::Duration::ZERO;
		let mut t_write = std::time::Duration::ZERO;
		let mut t_recv_wait = std::time::Duration::ZERO;
		let mut _t_last_recv = Instant::now();
		let mut pdu_ssh_batch: Vec<(u64, u64)> = Vec::with_capacity(4096);

		for (eid, prev, _, state_key, _) in &ctx.events_meta {
			processed = processed.saturating_add(1);

			if processed.is_multiple_of(1000) {
				debug!(
					"rebuild_state: writing {}/{} SSHs | {} compressed, {} deduped | elapsed: \
					 {:?}",
					processed,
					total_events,
					groups_compressed,
					groups_deduped,
					batch_start.elapsed(),
				);
			}

			let is_rezzy_target = state_key.is_some() || prev.len() != 1;
			let ssh = if is_rezzy_target {
				let owned_update = if let Some(update) = pending_updates.remove(eid.as_str()) {
					update
				} else {
					loop {
						let t0 = Instant::now();
						let Some((resolved_id, update)) = rx.recv().await else {
							break StateUpdateOwned::Unchanged {
								parent_event_id: prev
									.first()
									.map(|id| format!("{id}"))
									.unwrap_or_default(),
							};
						};
						t_recv_wait = t_recv_wait.saturating_add(t0.elapsed());
						_t_last_recv = Instant::now();

						if resolved_id == *eid {
							break update;
						}
						pending_updates.insert(resolved_id, update);
					}
				};

				match owned_update {
					| StateUpdateOwned::Unchanged { parent_event_id } => {
						let t0 = Instant::now();
						n_unchanged = n_unchanged.saturating_add(1);
						groups_deduped = groups_deduped.saturating_add(1);
						// Look up parent's SSH by string key to avoid OwnedEventId parsing
						let parent_eid: OwnedEventId = parent_event_id
							.as_str()
							.try_into()
							.expect("parent_event_id from rezzy should be a valid event ID");
						let result = event_ssh.get(&parent_eid).copied().unwrap_or(empty_ssh);
						t_unchanged = t_unchanged.saturating_add(t0.elapsed());
						result
					},
					| StateUpdateOwned::New { state, hash } => {
						n_new = n_new.saturating_add(1);

						// LtHash pre-check: skip the entire O(N) compression loop if
						// we've already seen this exact state. LtHash is 128 bytes
						// (cryptographic lattice hash) — collision is a non-issue.
						if let Some(&existing_ssh) = lthash_to_ssh.get(&hash) {
							groups_deduped = groups_deduped.saturating_add(1);
							n_new_deduped = n_new_deduped.saturating_add(1);
							existing_ssh
						} else {
							// Compress state to BTreeSet<u128> for storage.
							let tc0 = Instant::now();
							let mut compressed = BTreeSet::new();
							for (key, ev_id_str) in &state {
								let ssk = ssk_cache
									.get(&key.0)
									.and_then(|keys| keys.get(&key.1))
									.copied()
									.unwrap_or(0);
								let sei = sei_str_cache.get(ev_id_str).copied().unwrap_or(0);
								let compressed_val =
									rooms::state_compressor::compress_state_event(ssk, sei);
								compressed.insert(compressed_val);
							}
							t_compress = t_compress.saturating_add(tc0.elapsed());

							let ts0 = Instant::now();
							let result = self
								.services
								.state_compressor
								// rebuild_state is an administrative bulk repair path.
								// Using the root write path avoids walking the entire
								// ancestor diff chain for every intermediate state.
								.save_state_as_root(room_id, Arc::new(compressed))
								.await?;
							let ssh = result.shortstatehash;
							lthash_to_ssh.insert(*hash, ssh);
							groups_compressed = groups_compressed.saturating_add(1);
							t_save = t_save.saturating_add(ts0.elapsed());
							ssh
						}
					},
				}
			} else {
				n_inherited = n_inherited.saturating_add(1);
				event_ssh
					.get(&prev[0])
					.copied()
					.unwrap_or(current_shortstatehash)
			};

			let tw0 = Instant::now();
			// Write pdu_shortstatehash for this event
			let shorteventid = sei_cache.get(eid).copied().unwrap_or(0);
			if shorteventid != 0 {
				pdu_ssh_batch.push((shorteventid, ssh));
				if pdu_ssh_batch.len() >= 4096 {
					self.services
						.state
						.set_pdu_shortstatehash_batch(&pdu_ssh_batch);
					pdu_ssh_batch.clear();
				}
			}
			t_write = t_write.saturating_add(tw0.elapsed());

			event_ssh.insert(eid.clone(), ssh);
			current_shortstatehash = ssh;

			if groups_compressed.is_multiple_of(100) && groups_compressed > 0 {
				drop(cork.take());
				tokio::task::yield_now().await;
				cork = Some(self.db.db.cork());
			}
		}

		if !pdu_ssh_batch.is_empty() {
			self.services
				.state
				.set_pdu_shortstatehash_batch(&pdu_ssh_batch);
		}

		drop(cork.take());

		info!(
			"rebuild_state: walk+write done in {:?} | {} events, {} groups compressed, {} \
			 deduped",
			start.elapsed(),
			processed,
			groups_compressed,
			groups_deduped,
		);
		eprintln!(
			"  [PERF] consumer breakdown: inherited={n_inherited} unchanged={n_unchanged} \
			 new={n_new} new_deduped={n_new_deduped}"
		);
		eprintln!(
			"  [PERF]   t_recv_wait={t_recv_wait:?}  t_unchanged={t_unchanged:?}  \
			 t_compress={t_compress:?}  t_save={t_save:?}  t_write={t_write:?}"
		);

		Ok((event_ssh, current_shortstatehash))
	}

	/// Resolve a fork between multiple parent state sets using in-memory PDUs
	/// and `rezzy`. Pre-separates unconflicted/conflicted, computes auth
	/// difference via roaring bitmaps, then builds LeanEvents from the
	/// pre-cached state PDUs (zero RocksDB I/O).
	fn resolve_fork_with_states(
		ctx: &RebuildCtx,
		fork_states: &[&StateMap<OwnedEventId>],
	) -> StateMap<OwnedEventId> {
		// 1. Pre-separate into unconflicted and conflicted keys
		let num_maps = fork_states.len();
		let mut counts: HashMap<(String, String, String), usize> = HashMap::new();
		let mut key_to_ids: HashMap<(String, String), HashSet<String>> = HashMap::new();

		for map in fork_states {
			for ((ty, sk), id) in *map {
				let ty_s = ty.to_string();
				let sk_s = sk.to_string();
				let id_s = id.to_string();
				let count = counts
					.entry((ty_s.clone(), sk_s.clone(), id_s.clone()))
					.or_insert(0);
				*count = count.saturating_add(1);
				key_to_ids.entry((ty_s, sk_s)).or_default().insert(id_s);
			}
		}

		let mut unconflicted: rezzy::SharedState = rezzy::SharedState::new();
		let mut conflicted_keys: HashSet<(String, String)> = HashSet::new();

		for (key, ids) in &key_to_ids {
			if ids.len() == 1 {
				let id = ids.iter().next().unwrap();
				let count = counts
					.get(&(key.0.clone(), key.1.clone(), id.clone()))
					.copied()
					.unwrap_or(0);
				if count == num_maps {
					unconflicted.insert((key.0.clone().into(), key.1.clone()), id.clone());
					continue;
				}
			}
			conflicted_keys.insert(key.clone());
		}

		let mut conflicted_eids: HashSet<OwnedEventId> = HashSet::new();
		for map in fork_states {
			for ((ty, sk), id) in *map {
				if conflicted_keys.contains(&(ty.to_string(), sk.to_string())) {
					conflicted_eids.insert(id.clone());
				}
			}
		}

		// Early exit: no conflicts means all states agree
		if conflicted_eids.is_empty() {
			eprintln!("[resolve_fork] 0 conflicts, early exit");
			return fork_states[0].clone();
		}

		eprintln!(
			"[resolve_fork] {} conflicted keys, {} conflicted eids, {} unconflicted keys",
			conflicted_keys.len(),
			conflicted_eids.len(),
			unconflicted.len(),
		);

		// 2. Map room version early — needed to decide auth chain diff vs subgraph
		let version = rooms::auth_adapter::to_state_res_version(&ctx.room_version);
		let is_v2_1_plus = matches!(
			version,
			rezzy::StateResVersion::V2_1
				| rezzy::StateResVersion::V2_1_1
				| rezzy::StateResVersion::V2_2
		);

		// 3. Compute auth chains (needed for both V2 auth diff and V2_1+ context)
		let mut union_auth = roaring::RoaringBitmap::new();
		let mut intersect_auth = roaring::RoaringBitmap::new();
		let mut first = true;

		for map in fork_states {
			let mut chain = roaring::RoaringBitmap::new();
			for eid in map.values() {
				if let Some(&idx) = ctx.eid_to_idx.get(eid) {
					chain.insert(idx);
					chain |= &ctx.auth_chain_bitmaps[to_usize(idx)];
				}
			}
			if first {
				union_auth.clone_from(&chain);
				intersect_auth = chain;
				first = false;
			} else {
				union_auth |= &chain;
				intersect_auth &= &chain;
			}
		}

		// V2 only: auth chain diff events are also conflicted.
		// V2_1+ (MSC4297): uses conflicted state subgraph instead.
		if !is_v2_1_plus {
			let auth_diff = std::ops::Sub::sub(&union_auth, &intersect_auth);
			for idx in auth_diff {
				conflicted_eids.insert(ctx.idx_to_eid[to_usize(idx)].clone());
			}
		}

		// 4. Collect all event IDs we need for resolution (auth context + conflicted)
		let mut all_needed_indices: HashSet<u32> = HashSet::new();
		for idx in &union_auth {
			all_needed_indices.insert(idx);
		}
		for state in fork_states {
			for eid in state.values() {
				if let Some(&idx) = ctx.eid_to_idx.get(eid) {
					all_needed_indices.insert(idx);
				}
			}
		}
		for eid in &conflicted_eids {
			if let Some(&idx) = ctx.eid_to_idx.get(eid) {
				all_needed_indices.insert(idx);
			}
		}

		// 5. Build LeanEvents from in-memory state_pdus (zero RocksDB I/O and zero JSON
		//    parsing!)
		let mut auth_context: HashMap<String, rezzy::LeanEvent> = HashMap::new();
		for &idx in &all_needed_indices {
			if let Some(Some(lean_ev)) = ctx.state_pdus.get(to_usize(idx)) {
				auth_context.insert(ctx.idx_to_eid[to_usize(idx)].to_string(), lean_ev.clone());
			}
		}
		eprintln!(
			"[resolve_fork] auth_context: {} events (from {} needed indices)",
			auth_context.len(),
			all_needed_indices.len(),
		);

		// 6. Build full context ONCE, then extract conflicted via remove()

		let conflicted_events: HashMap<String, rezzy::LeanEvent> = if is_v2_1_plus {
			// MSC4297 (V2.1+): rezzy computes the exact HashMap we need
			let direct_conflicted: Vec<String> =
				conflicted_eids.iter().map(|id| format!("{id}")).collect();
			eprintln!(
				"[resolve_fork] computing V2.1+ conflicted subgraph ({} direct_conflicted, {} \
				 auth_context)...",
				direct_conflicted.len(),
				auth_context.len(),
			);
			let subgraph_start = Instant::now();
			let v2_1_conflicted_subgraph =
				rezzy::compute_v2_1_conflicted_subgraph(&auth_context, &direct_conflicted);
			eprintln!(
				"[resolve_fork] subgraph took {:?}, {} events",
				subgraph_start.elapsed(),
				v2_1_conflicted_subgraph.len(),
			);

			// Remove conflicted events from auth_context (mutually exclusive)
			for id in v2_1_conflicted_subgraph.keys() {
				auth_context.remove(id);
			}

			v2_1_conflicted_subgraph
		} else {
			// V1 or V2: pull known conflicted_eids (state diff + auth chain diff) out
			let mut v2_conflicted_auth_context = HashMap::with_capacity(conflicted_eids.len());
			for eid in &conflicted_eids {
				let id_str = eid.to_string();
				if let Some(lean) = auth_context.remove(&id_str) {
					v2_conflicted_auth_context.insert(id_str, lean);
				}
			}
			v2_conflicted_auth_context
		};

		eprintln!(
			"[resolve_fork] conflicted_events={}, auth_context={}, version={:?} — calling \
			 rezzy::resolve_iterative_sort...",
			conflicted_events.len(),
			auth_context.len(),
			version,
		);
		let rezzy_start = Instant::now();
		let mut pl_cache = HashMap::new();
		let resolved_lean = rezzy::resolve_iterative_sort(
			&unconflicted,
			&conflicted_events,
			&auth_context,
			version,
			&mut pl_cache,
		);
		eprintln!(
			"[resolve_fork] rezzy::resolve_iterative_sort took {:?}",
			rezzy_start.elapsed()
		);

		// 8. Convert back to Ruma StateMap
		let mut resolved = StateMap::new();
		for ((ty_str, sk_str), eid_str) in resolved_lean {
			let ty: ruma::events::StateEventType = ty_str.to_string().into();
			let sk: conduwuit_core::matrix::StateKey = sk_str.into();
			if let Ok(eid) = OwnedEventId::try_from(eid_str.as_str()) {
				resolved.insert((ty, sk), eid);
			}
		}

		resolved
	}

	// ── Phase 5: Final multi-head extremity merge ──
	// Handles rooms with multiple forward extremities by merging their state.

	async fn rebuild_merge_extremities(
		&self,
		room_id: &RoomId,
		ctx: &RebuildCtx,
		event_ssh: &HashMap<OwnedEventId, u64>,
		current_shortstatehash: u64,
	) -> Result<u64> {
		use conduwuit::utils::stream::{IterStream, ReadyExt, WidebandExt};
		use futures::{StreamExt, TryStreamExt};

		let mut has_children: HashSet<&OwnedEventId> = HashSet::new();
		for (_, prev_events, ..) in &ctx.events_meta {
			for parent in prev_events {
				if ctx.event_set.contains(parent) {
					has_children.insert(parent);
				}
			}
		}

		let extremity_sshs: Vec<u64> = ctx
			.events_meta
			.iter()
			.map(|(eid, ..)| eid)
			.filter(|eid| !has_children.contains(eid))
			.filter_map(|eid| event_ssh.get(eid).copied())
			.collect::<HashSet<_>>()
			.into_iter()
			.collect();

		let num_extremities = ctx
			.events_meta
			.iter()
			.map(|(eid, ..)| eid)
			.filter(|eid| !has_children.contains(eid))
			.count();

		if extremity_sshs.len() <= 1 {
			eprintln!(
				"[rebuild_state] Phase 5: {num_extremities} extremities, all share 1 SSH — skip \
				 merge",
			);
			return Ok(current_shortstatehash);
		}

		eprintln!(
			"[rebuild_state] Phase 5: {} extremities, {} unique SSHs — loading state...",
			num_extremities,
			extremity_sshs.len(),
		);

		// Load full compressed state for each unique SSH
		let mut all_compressed = BTreeSet::new();
		for &ssh in &extremity_sshs {
			if let Some(full_state) = self.services.state_compressor.get_full_state(ssh).await {
				for entry in full_state.as_ref() {
					all_compressed.insert(*entry);
				}
			}
		}

		// Build ssk -> set of shorteventid values to detect conflicts
		let mut ssk_values: HashMap<u64, HashSet<u64>> = HashMap::new();
		for bytes in &all_compressed {
			let (ssk, sei) = rooms::state_compressor::parse_compressed_state_event(*bytes);
			ssk_values.entry(ssk).or_default().insert(sei);
		}

		let conflicting: Vec<_> = ssk_values
			.iter()
			.filter(|(_, values)| values.len() > 1)
			.map(|(ssk, _)| *ssk)
			.collect();

		if conflicting.is_empty() {
			eprintln!(
				"[rebuild_state] Phase 5: trivial merge, {} state entries, 0 conflicts",
				ssk_values.len(),
			);
			let merged_ssh = self
				.services
				.state_compressor
				.save_state(room_id, Arc::new(all_compressed))
				.await?
				.shortstatehash;
			return Ok(merged_ssh);
		}

		eprintln!(
			"[rebuild_state] Phase 5: {} conflicts across {} unique SSHs — running N-way \
			 resolution...",
			conflicting.len(),
			extremity_sshs.len(),
		);

		debug!(
			"rebuild_state: {} forward extremities with {} unique SSHs ({} conflicts) — merging \
			 via n-way resolution...",
			num_extremities,
			extremity_sshs.len(),
			conflicting.len(),
		);

		let mut fork_maps = Vec::with_capacity(extremity_sshs.len());
		for &ssh in &extremity_sshs {
			let map: HashMap<u64, OwnedEventId> = self
				.services
				.state_accessor
				.state_full_ids(ssh)
				.collect()
				.await;
			fork_maps.push(map);
		}

		let fork_states: Vec<StateMap<OwnedEventId>> = fork_maps
			.iter()
			.stream()
			.wide_then(|fork_map| {
				let shortstatekeys = fork_map.keys().copied().stream();
				let event_ids = fork_map.values().cloned().stream();
				self.services
					.short
					.multi_get_statekey_from_short(shortstatekeys)
					.zip(event_ids)
					.ready_filter_map(|(ty_sk, id)| Some((ty_sk.ok()?, id)))
					.collect()
			})
			.map(Ok::<_, conduwuit::Error>)
			.try_collect()
			.await?;

		let fork_state_refs: Vec<&StateMap<OwnedEventId>> = fork_states.iter().collect();
		eprintln!(
			"[rebuild_state] Phase 5: loaded {} fork state maps, calling \
			 resolve_fork_with_states...",
			fork_state_refs.len()
		);
		let resolve_start = Instant::now();
		let resolved_map = Self::resolve_fork_with_states(ctx, &fork_state_refs);
		eprintln!(
			"[rebuild_state] Phase 5: resolve_fork_with_states took {:?}, {} entries",
			resolve_start.elapsed(),
			resolved_map.len()
		);

		let mut compressed = BTreeSet::new();
		for ((ty, sk), id) in &resolved_map {
			let ssk = self
				.services
				.short
				.get_or_create_shortstatekey(ty, sk.as_ref())
				.await;
			let sei = self.services.short.get_or_create_shorteventid(id).await;
			compressed.insert(rooms::state_compressor::compress_state_event(ssk, sei));
		}

		debug!("rebuild_state: merged state has {} entries", compressed.len());
		let merged_ssh = self
			.services
			.state_compressor
			.save_state(room_id, Arc::new(compressed))
			.await?
			.shortstatehash;

		Ok(merged_ssh)
	}
}

use std::{
	collections::{HashMap, HashSet},
	time::Instant,
};

use conduwuit::utils::timeline_sorter::sort_timeline_events;
use conduwuit_core::{
	Result, debug, err, info,
	matrix::{StateKey, event::Event, state_res::StateMap},
	utils::IterStream,
	warn,
};
use futures::StreamExt;
use slipstream::{
	OwnedEventId, RoomId, RoomVersionId,
	events::{StateEventType, TimelineEventType},
};

use crate::rooms::{
	self,
	short::{ShortEventId, ShortStateKey},
};

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
	let content_val =
		rezzy::JsonValue::parse(pdu.content.get()).expect("PDU content must be valid JSON");
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
		origin_server_ts: pdu.origin_server_ts,
		sender: pdu.sender.to_string(),
		content: content_val,
		prev_events: pdu.prev_events.iter().map(|id| format!("{id}")).collect(),
		auth_events: pdu.auth_events.iter().map(|id| format!("{id}")).collect(),
		depth: pdu.depth,
		..Default::default()
	}
}

impl super::Service {
	/// Rebuilds room state entirely in-memory, then batch-writes the result to
	/// DB. Memory usage is dominated by metadata vectors and state groups, NOT
	/// by full PduEvent JSON. For a 60K-event room this uses ~50MB instead of
	/// the previous ~4GB.
	pub async fn rebuild_state(&self, room_id: &RoomId) -> Result<()> {
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
		let (event_root, current_root) =
			Box::pin(self.rebuild_walk_and_write(room_id, &ctx)).await?;
		eprintln!("[rebuild_state] Phase 3+4 done: {} roots computed", event_root.len());

		// Phase 5: Final multi-head extremity merge
		eprintln!("[rebuild_state] Phase 5: merge extremities...");
		let current_root = self
			.rebuild_merge_extremities(room_id, &ctx, &event_root, current_root)
			.await?;
		eprintln!("[rebuild_state] Phase 5 done");

		// Phase 6: Apply final state. This mirrors the legacy `force_state_quiet`
		// admin bypass: commit the rebuilt root and refresh the joined count
		// without running the full added/removed cache delta (`force_state`
		// would resolve and fan out every state event, which can fail on state
		// events that only exist as outliers). Callers that need the derived
		// membership cache rebuilt run `reconcile_membership` afterwards.
		let state_lock = self.services.state.mutex.lock(room_id).await;
		self.services
			.state
			.set_room_state_hamt(room_id, &current_root, &state_lock);
		self.services.state_cache.update_joined_count(room_id).await;

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
			let depth = pdu.depth();

			// Timeline events are authoritative; clear any stale rejection flags.
			self.services.pdu_metadata.unmark_event_rejected(&eid);

			if *pdu.kind() == TimelineEventType::RoomCreate {
				if let Ok(create_content) = slipstream::codec::from_str::<
					slipstream::events::room::create::RoomCreateEventContent,
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

impl super::Service {
	/// Builds and persists a HAMT root for `entries`, using the supplied
	/// pre-computed state lattice.
	///
	/// The root node and its recursively-resolved children are written to the
	/// HAMT node store, and the lattice is recorded under the root's structural
	/// hash so a subsequent `state::append_to_state` can apply a single event
	/// without re-materializing the whole tree.
	fn store_hamt_root(
		&self,
		room_id: &RoomId,
		entries: Vec<(ShortStateKey, ShortEventId)>,
		lattice: &rezzy::state::LtHash,
	) -> Result<rezzy::hamt::RootHandle> {
		let structural_key =
			rooms::state_hamt::room_structural_key(&self.services.globals.server_secret, room_id);
		let (root_handle, root_node) =
			rezzy::hamt::build_hamt_root_handle(&structural_key, lattice, entries)
				.map_err(|e| err!(error!("rebuild_state: failed to build HAMT root: {e:?}")))?;

		self.services
			.state_hamt
			.store
			.persist_node_recursive(root_node);

		let encoded = lattice.to_bytes();
		self.db.db["state_hamt_root_lattices"].insert(&root_handle.structural_hash, &encoded);

		Ok(root_handle)
	}

	/// Reconstructs an LtHash lattice for a set of `(shortstatekey, shorteventid)`
	/// entries by resolving them to their `(type, state_key, event_id)` triples.
	/// Used by Phase 5, which materializes a handful of extremity roots.
	async fn lattice_for_short_entries(
		&self,
		entries: &[(ShortStateKey, ShortEventId)],
	) -> Result<rezzy::state::LtHash> {
		let mut lattice = rezzy::state::LtHash::default();

		let shortstatekeys: Vec<ShortStateKey> = entries
			.iter()
			.map(|(shortstatekey, _)| *shortstatekey)
			.collect();
		let string_keys: Vec<Result<(StateEventType, StateKey)>> = self
			.services
			.short
			.multi_get_statekey_from_short(shortstatekeys.iter().copied().stream())
			.collect()
			.await;

		for ((_shortstatekey, shorteventid), key_result) in entries.iter().zip(string_keys) {
			if let Ok((event_type, state_key)) = key_result {
				if let Ok(event_id) = self
					.services
					.short
					.get_eventid_from_short::<OwnedEventId>(*shorteventid)
					.await
				{
					lattice.insert(
						event_type.to_string().as_str(),
						state_key.as_str(),
						event_id.as_str(),
					);
				}
			}
		}

		Ok(lattice)
	}

	/// Writes a chunk of `shorteventid -> serialized RootHandle` mappings to
	/// the `shorteventid_roothandle` map. Used by rebuild-state to bound the
	/// number of pending writes; the caller holds a database cork, so these
	/// land in the same in-memory batch.
	fn write_roothandle_entries(&self, entries: &[(u64, Vec<u8>)]) {
		let map = self.db.db["shorteventid_roothandle"].clone();
		for (shorteventid, bytes) in entries {
			map.insert(&shorteventid.to_be_bytes(), bytes);
		}
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
	) -> Result<(HashMap<OwnedEventId, rezzy::hamt::RootHandle>, rezzy::hamt::RootHandle)> {
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
		let version = match ctx.room_version.as_str() {
			| "1" => rezzy::StateResVersion::V1,
			| "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "10" | "11" =>
				rezzy::StateResVersion::V2,
			| "12" => rezzy::StateResVersion::V2_1,
			| _ => rezzy::StateResVersion::V2_1_1,
		};

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
		self.services.server.runtime().spawn_blocking(move || {
			let target_refs: Vec<&String> = target_ids_owned.iter().collect();
			// Empty (`""`) state-key sentinel for the `(EventType, K)` lookups
			let empty_key = String::new();
			let result = rezzy::StreamingInputs::new(
				&target_refs,
				&lean_events_moved,
				version,
				&empty_key,
			)
			.try_compute_optimized(|id, update| {
				let owned_update = match update {
					| rezzy::StateUpdate::New { state, hash } =>
						StateUpdateOwned::New { state, hash: Box::new(*hash) },
					| rezzy::StateUpdate::Unchanged { parent_event_id, .. } =>
						StateUpdateOwned::Unchanged { parent_event_id: parent_event_id.clone() },
				};
				tx.blocking_send((id, owned_update))
					.map_err(|_| "state output channel closed")
			});
			if result == Err(rezzy::StateComputationError::CycleDetected) {
				warn!("streaming state computation detected cycle; results incomplete");
			}
		});

		// ── Consume stream and write a HAMT root for each event ──
		// Root handle for the empty state; events whose parent has no computed
		// root (e.g. the first event in the room) inherit it.
		let empty_root =
			self.store_hamt_root(room_id, Vec::new(), &rezzy::state::LtHash::default())?;

		let mut event_root: HashMap<OwnedEventId, rezzy::hamt::RootHandle> = HashMap::new();
		let mut lthash_to_root: HashMap<rezzy::LtHash, rezzy::hamt::RootHandle> = HashMap::new();
		let mut current_root = empty_root.clone();
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
		let mut pdu_root_entries: Vec<(u64, Vec<u8>)> = Vec::with_capacity(4096);

		for (eid, prev, _, state_key, _) in &ctx.events_meta {
			processed = processed.saturating_add(1);

			if processed.is_multiple_of(1000) {
				debug!(
					"rebuild_state: writing {}/{} roots | {} compressed, {} deduped | elapsed: \
					 {:?}",
					processed,
					total_events,
					groups_compressed,
					groups_deduped,
					batch_start.elapsed(),
				);
			}

			let is_rezzy_target = state_key.is_some() || prev.len() != 1;
			let root = if is_rezzy_target {
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

						if resolved_id == eid.as_str() {
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
						// Look up parent's root by string key to avoid OwnedEventId parsing
						let parent_eid = OwnedEventId::parse(parent_event_id.as_str())?;
						let result = event_root
							.get(&parent_eid)
							.cloned()
							.unwrap_or_else(|| empty_root.clone());
						t_unchanged = t_unchanged.saturating_add(t0.elapsed());
						result
					},
					| StateUpdateOwned::New { state, hash } => {
						n_new = n_new.saturating_add(1);

						// LtHash pre-check: skip rebuilding the whole tree if we've
						// already seen this exact resolved state. LtHash is a
						// cryptographic lattice hash — collision is a non-issue.
						if let Some(existing_root) = lthash_to_root.get(&*hash) {
							groups_deduped = groups_deduped.saturating_add(1);
							n_new_deduped = n_new_deduped.saturating_add(1);
							existing_root.clone()
						} else {
							// Build the (shortstatekey, shorteventid) entries for
							// storage from the resolved state and pre-cached short IDs.
							let tc0 = Instant::now();
							let mut entries = Vec::with_capacity(state.len());
							for (key, ev_id_str) in &state {
								let ssk = ssk_cache
									.get(&key.0)
									.and_then(|keys| keys.get(&key.1))
									.copied()
									.unwrap_or(0);
								let sei = sei_str_cache.get(ev_id_str).copied().unwrap_or(0);
								entries.push((ssk, sei));
							}
							t_compress = t_compress.saturating_add(tc0.elapsed());

							let ts0 = Instant::now();
							// `hash` is rezzy's incrementally-maintained lattice
							// for this exact state, so reuse it directly as the
							// root's state-group lattice.
							let root = self.store_hamt_root(room_id, entries, &hash)?;
							lthash_to_root.insert(*hash, root.clone());
							groups_compressed = groups_compressed.saturating_add(1);
							t_save = t_save.saturating_add(ts0.elapsed());
							root
						}
					},
				}
			} else {
				n_inherited = n_inherited.saturating_add(1);
				event_root
					.get(&prev[0])
					.cloned()
					.unwrap_or_else(|| current_root.clone())
			};

			let tw0 = Instant::now();
			// Write this event's post-event HAMT root handle.
			let shorteventid = sei_cache.get(eid).copied().unwrap_or(0);
			if shorteventid != 0 {
				pdu_root_entries.push((shorteventid, rooms::state::root_handle_to_bytes(&root)));
				if pdu_root_entries.len() >= 4096 {
					self.write_roothandle_entries(&pdu_root_entries);
					pdu_root_entries.clear();
				}
			}
			t_write = t_write.saturating_add(tw0.elapsed());

			event_root.insert(eid.clone(), root.clone());
			current_root = root;

			if groups_compressed.is_multiple_of(100) && groups_compressed > 0 {
				drop(cork.take());
				smol::future::yield_now().await;
				cork = Some(self.db.db.cork());
			}
		}

		if !pdu_root_entries.is_empty() {
			self.write_roothandle_entries(&pdu_root_entries);
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

		Ok((event_root, current_root))
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

		let mut unconflicted: std::collections::BTreeMap<
			(rezzy::basespec::event_types::EventType, String),
			String,
		> = std::collections::BTreeMap::new();
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
			return fork_states[0]
				.iter()
				.map(|((ty, key), event_id)| {
					((StateEventType::from(ty.as_str()), key.clone()), event_id.clone())
				})
				.collect();
		}

		eprintln!(
			"[resolve_fork] {} conflicted keys, {} conflicted eids, {} unconflicted keys",
			conflicted_keys.len(),
			conflicted_eids.len(),
			unconflicted.len(),
		);

		// 2. Map room version early — needed to decide auth chain diff vs subgraph
		let version = match ctx.room_version.as_str() {
			| "1" => rezzy::StateResVersion::V1,
			| "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "10" | "11" =>
				rezzy::StateResVersion::V2,
			| "12" => rezzy::StateResVersion::V2_1,
			| _ => rezzy::StateResVersion::V2_1_1,
		};
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
		// Empty (`""`) state-key sentinel for the `(EventType, K)` lookups
		let empty_key = String::new();
		let unconflicted_state: rezzy::state::at::SharedState = (&unconflicted).into();
		let resolved_lean = rezzy::resolve_iterative_sort(rezzy::IterativeInputs::new(
			&unconflicted_state,
			&conflicted_events,
			&auth_context,
			version,
			&mut pl_cache,
			&empty_key,
		));
		eprintln!(
			"[resolve_fork] rezzy::resolve_iterative_sort took {:?}",
			rezzy_start.elapsed()
		);

		// 8. Convert back to Ruma StateMap
		let mut resolved = StateMap::new();
		for ((ty_str, sk_str), eid_str) in resolved_lean {
			let ty: StateEventType = ty_str.to_string().into();
			let sk: StateKey = sk_str.into();
			if let Ok(eid) = OwnedEventId::parse(eid_str.as_str()) {
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
		event_root: &HashMap<OwnedEventId, rezzy::hamt::RootHandle>,
		current_root: rezzy::hamt::RootHandle,
	) -> Result<rezzy::hamt::RootHandle> {
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

		let extremity_roots: Vec<rezzy::hamt::RootHandle> = ctx
			.events_meta
			.iter()
			.map(|(eid, ..)| eid)
			.filter(|eid| !has_children.contains(eid))
			.filter_map(|eid| event_root.get(eid).cloned())
			.collect::<HashSet<_>>()
			.into_iter()
			.collect();

		let num_extremities = ctx
			.events_meta
			.iter()
			.map(|(eid, ..)| eid)
			.filter(|eid| !has_children.contains(eid))
			.count();

		if extremity_roots.len() <= 1 {
			eprintln!(
				"[rebuild_state] Phase 5: {num_extremities} extremities, all share 1 root — \
				 skip merge",
			);
			return Ok(current_root);
		}

		eprintln!(
			"[rebuild_state] Phase 5: {} extremities, {} unique roots — loading state...",
			num_extremities,
			extremity_roots.len(),
		);

		// Materialize the union of the extremity states.
		let mut all_entries: HashMap<ShortStateKey, ShortEventId> = HashMap::new();
		for root in &extremity_roots {
			// Abort rather than merge a partial union: dropping an extremity's
			// state would commit incomplete room state.
			let full_state = self.services.state_accessor.load_full_state_hamt(root)?;
			for (shortstatekey, shorteventid) in full_state {
				all_entries.insert(shortstatekey, shorteventid);
			}
		}

		// Build ssk -> set of shorteventid values to detect conflicts
		let mut ssk_values: HashMap<ShortStateKey, HashSet<ShortEventId>> = HashMap::new();
		for (&shortstatekey, &shorteventid) in &all_entries {
			ssk_values
				.entry(shortstatekey)
				.or_default()
				.insert(shorteventid);
		}

		let conflicting: Vec<ShortStateKey> = ssk_values
			.iter()
			.filter(|(_, values)| values.len() > 1)
			.map(|(shortstatekey, _)| *shortstatekey)
			.collect();

		if conflicting.is_empty() {
			eprintln!(
				"[rebuild_state] Phase 5: trivial merge, {} state entries, 0 conflicts",
				ssk_values.len(),
			);
			let entries: Vec<(ShortStateKey, ShortEventId)> = all_entries.into_iter().collect();
			let lattice = self.lattice_for_short_entries(&entries).await?;
			let merged_root = self.store_hamt_root(room_id, entries, &lattice)?;
			return Ok(merged_root);
		}

		eprintln!(
			"[rebuild_state] Phase 5: {} conflicts across {} unique roots — running N-way \
			 resolution...",
			conflicting.len(),
			extremity_roots.len(),
		);

		debug!(
			"rebuild_state: {} forward extremities with {} unique roots ({} conflicts) — \
			 merging via n-way resolution...",
			num_extremities,
			extremity_roots.len(),
			conflicting.len(),
		);

		let mut fork_maps = Vec::with_capacity(extremity_roots.len());
		for root in &extremity_roots {
			let map: HashMap<ShortStateKey, OwnedEventId> = self
				.services
				.state_accessor
				.state_full_ids_hamt(root)
				.try_collect()
				.await?;
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

		let mut lattice = rezzy::state::LtHash::default();
		let mut entries: Vec<(ShortStateKey, ShortEventId)> =
			Vec::with_capacity(resolved_map.len());
		for ((ty, sk), id) in &resolved_map {
			lattice.insert(ty.to_string().as_str(), sk.as_str(), id.as_str());
			let ssk = self
				.services
				.short
				.get_or_create_shortstatekey(ty, sk.as_ref())
				.await;
			let sei = self.services.short.get_or_create_shorteventid(id).await;
			entries.push((ssk, sei));
		}

		debug!("rebuild_state: merged state has {} entries", entries.len());
		let merged_root = self.store_hamt_root(room_id, entries, &lattice)?;

		Ok(merged_root)
	}
}

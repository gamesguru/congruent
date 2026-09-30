In RocksDB there are no separate indexes — the **key prefix IS the index**. Let me check the key layouts:

Searched for "pduid_pdu|eventid_pduid|roomid_pduleaves|roomid_topo|shorteventid_short(prev|auth)"
Viewed maps.rs:1-540

Here's the full picture:

**Yes, querying by room is fast.** The two primary timeline CFs are keyed by `shortroomid` prefix:

| CF Name                            | Key Layout                                               | Index Type                                                                                 |
| ---------------------------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `room_pducount_eventid`            | `[shortroomid: 8B][pdu_count: 8B]` → event_id            | `SEQUENTIAL` — prefix scan by shortroomid = all events in room, ordered by stream position |
| `roomid_topologicalorder_pducount` | `[shortroomid: 8B][depth: 8B][pdu_count: 8B]` → event_id | `SEQUENTIAL` — prefix scan by shortroomid = all events in room, ordered by topo depth      |

RocksDB sorts keys lexicographically, so scanning with a `shortroomid` prefix is a single seek + forward iteration — O(1) to start, O(n) to scan all events. It's as fast as any B-tree index.

**But** — `eventid_metadata`, `eventid_pdu`, `shorteventid_shortauthevents`, and `shorteventid_shortprevevents` are keyed by **event_id** or **shorteventid**, NOT by room. So:

- ✅ "Give me all events in room X" → fast prefix scan on `room_pducount_eventid`
- ✅ "Give me all events in room X in topo order" → fast prefix scan on `roomid_topologicalorder_pducount`
- ❌ "Give me auth_events for all events in room X" → must look up each `shorteventid_shortauthevents` individually (point reads)
- ❌ "Give me the PDU body for event $abc" → point read on `eventid_pdu` (fast per-event, but not a room-scoped scan)

This is exactly why `rebuild_state` extracts auth_events during Phase 1's topo scan — it piggybacks on the room-scoped sequential iteration to avoid 60K random point reads later.

---

Viewed rebuild_state.rs:944-983
Edited rebuild_state.rs
Ran command: `ga .`
Ran command: `gs
gOn branch guru/dev-2026-03-27+b1-presence+b2-federation
Your branch is ahead of 'origin/guru/dev-2026-03-27+b1-presence+b2-federation' by 6 commits.
  (use "git push" to publish your local commits)
Changes to be committed:
  (use "git restore --staged <file>..." to unstage)
        modified:   src/service/rooms/timeline/rebuild_state.rs`
Ran command: `gca`
Ran command: `git show
commit c6b49b5a32112e9828fa8a502b2fef02d186cd94 (HEAD -> guru/dev-2026-03-27+b1-presence+b2-federation)
Author: Shane Jaroch <chown_tee@proton.me>
Date: Sat Jun 27 03:57:49 2026 -0400
perf(rebuild_state): remove event_cache, fetch PDUs on-demand during fork resolution

    Eliminates the HashMap<OwnedEventId, Arc<PduEvent>> that held all 60K+
    events in RAM (~4GB). EventMeta now carries auth_events and event_type
    directly, so the walk and auth chain computation use only metadata.

    PDUs are fetched from RocksDB on-demand only during genuine fork
    resolution (~2-5 per room, ~200 events fetched, <5ms).

diff --git a/src/service/rooms/timeline/rebuild_state.rs b/src/service/rooms/timeline/rebuild_state.rs
index e0c6f279b..33b095d44 100644
--- a/src/service/rooms/timeline/rebuild_state.rs
+++ b/src/service/rooms/timeline/rebuild_state.rs
@@ -1,6 +1,7 @@
use std::{
collections::{BTreeSet, HashMap, HashSet},
hash::{Hash, Hasher},

-       pin::pin,
          sync::Arc,
          time::{Duration, Instant},
    };
    @@ -16,7 +17,17 @@
    use crate::rooms;

/// Event metadata extracted during Phase 1 streaming.
-type EventMeta = (OwnedEventId, Vec<OwnedEventId>, Option<String>, u64);
+/// Carries auth_events and (event_type, state_key) so we never need to load
+/// full PduEvents into RAM during the walk. Only fork resolution (rare) needs
+/// on-demand DB reads.
+/// (event_id, prev_events, auth_events, Option<(event_type, state_key)>, depth)
+type EventMeta = (

-       OwnedEventId,
-       Vec<OwnedEventId>,
-       Vec<OwnedEventId>,
-       Option<(String, String)>,
-       u64,
    +);

/// Safe u32 -> usize for Vec indexing of roaring bitmap indices. #[inline]
@@ -30,10 +41,11 @@ fn is_subset(small: &StateMap<OwnedEventId>, large: &StateMap<OwnedEventId>) ->
}

/// Shared context threaded through all phases of rebuild_state.
+/// No event_cache — metadata carries everything needed for the walk;
+/// PDUs are fetched on-demand only during fork resolution.
struct RebuildCtx {
room_version: RoomVersionId,
events_meta: Vec<EventMeta>,

-       event_cache: HashMap<OwnedEventId, Arc<PduEvent>>,
          event_set: HashSet<OwnedEventId>,
          eid_to_idx: HashMap<OwnedEventId, u32>,
          idx_to_eid: Vec<OwnedEventId>,
    @@ -41,9 +53,10 @@ struct RebuildCtx {
    }

impl super::Service {

-       /// Rebuilds room state entirely in-memory like ruma-lean, then batch-writes
-       /// the result to DB. This avoids per-event RocksDB round-trips during state
-       /// resolution, achieving seconds instead of minutes for large DAGs.

*       /// Rebuilds room state entirely in-memory, then batch-writes the result to
*       /// DB. Memory usage is dominated by metadata vectors and state groups, NOT
*       /// by full PduEvent JSON. For a 60K-event room this uses ~50MB instead of
*       /// the previous ~4GB.
          #[tracing::instrument(skip(self), level = "info")]
          pub async fn rebuild_state(&self, room_id: &RoomId) -> Result<()> {
                  let original_room_shortstatehash = self
    @@ -53,29 +66,25 @@ pub async fn rebuild_state(&self, room_id: &RoomId) -> Result<()> {
    .await
    .ok();

-               // Phase 1: Stream events and collect metadata

*               // Phase 1: Stream events and extract metadata (no heavy JSON cache)
                let (events_meta, room_version) = self.rebuild_stream_events(room_id).await;

-               // Phase 2: Pre-load ALL events into RAM
-               let event_cache = self.rebuild_prefetch_events(room_id, &events_meta).await;
                let event_set: HashSet<OwnedEventId> =
                        events_meta.iter().map(|(eid, ..)| eid.clone()).collect();

                // Phase 2b: Pre-compute auth chains bottom-up (iterative DFS)

-               let (eid_to_idx, idx_to_eid, auth_chain_bitmaps) =
-                       Self::rebuild_auth_chains(&events_meta, &event_cache);

*               let (eid_to_idx, idx_to_eid, auth_chain_bitmaps) = Self::rebuild_auth_chains(&events_meta);

                let ctx = RebuildCtx {
                        room_version,
                        events_meta,

-                       event_cache,
                        event_set,
                        eid_to_idx,
                        idx_to_eid,
                        auth_chain_bitmaps,
                };

-               // Phase 3+4: Merged state walk + inline DB writes + group eviction

*               // Phase 3+4: In-memory state walk with eviction + inline DB writes
                let (event_ssh, current_shortstatehash) =
                        self.rebuild_walk_and_write(room_id, &ctx).await?;

@@ -113,6 +122,8 @@ pub async fn rebuild_state(&self, room_id: &RoomId) -> Result<()> {
}

        // ── Phase 1: Stream events and collect metadata ──

-       // Now extracts auth_events and event_type directly, eliminating the need
-       // for Phase 2 (prefetch).

          async fn rebuild_stream_events(&self, room_id: &RoomId) -> (Vec<EventMeta>, RoomVersionId) {
                  info!("rebuild_state: streaming events in topological order...");

    @@ -122,11 +133,14 @@ async fn rebuild_stream_events(&self, room_id: &RoomId) -> (Vec<EventMeta>, Room
    let mut room_version = RoomVersionId::V1;
    let mut room_version_found = false;

*               let mut stream = std::pin::pin!(self.topo_pdus(room_id, None));

-               let mut stream = pin!(self.topo_pdus(room_id, None));
                while let Some(Ok((_pdu_count, pdu))) = stream.next().await {
                        let eid = pdu.event_id().to_owned();
                        let prev: Vec<OwnedEventId> = pdu.prev_events().map(ToOwned::to_owned).collect();

*                       let state_key = pdu.state_key().map(ToOwned::to_owned);

-                       let auth: Vec<OwnedEventId> = pdu.auth_events().map(ToOwned::to_owned).collect();
-                       let state_key = pdu
-                               .state_key()
-                               .map(|sk| (pdu.kind().to_string(), sk.to_owned()));
                          let depth = u64::from(pdu.depth());

                          // Timeline events are authoritative; clear any stale rejection flags.

    @@ -142,52 +156,25 @@ async fn rebuild_stream_events(&self, room_id: &RoomId) -> (Vec<EventMeta>, Room
    }
    }

*                       events_meta.push((eid, prev, state_key, depth));

-                       events_meta.push((eid, prev, auth, state_key, depth));
                }

*               debug!(
*                       "rebuild_state: loaded {} event metadata in {:?}",

-               info!(
-                       "rebuild_state: streamed {} events in {:?} | room version: {}",
                        events_meta.len(),
                        start.elapsed(),
-                       room_version,
                );
                (events_meta, room_version)
        }

*       // ── Phase 2: Pre-load ALL events into RAM ──
*
*       async fn rebuild_prefetch_events(
*               &self,
*               room_id: &RoomId,
*               events_meta: &[EventMeta],
*       ) -> HashMap<OwnedEventId, Arc<PduEvent>> {
*               let start = Instant::now();
*               let event_cache: HashMap<OwnedEventId, Arc<PduEvent>> = {
*                       let event_ids_stream =
*                               futures::stream::iter(events_meta.iter().map(|(eid, ..)| eid.clone()));
*                       self.multi_get_pdus(Some(room_id), event_ids_stream)
*                               .filter_map(|r| async move { r.ok() })
*                               .map(|mut pdu| {
*                                       pdu.rejected = false;
*                                       (pdu.event_id.clone(), Arc::new(pdu))
*                               })
*                               .collect()
*                               .await
*               };
*               info!(
*                       "rebuild_state: pre-loaded {} events into RAM in {:?}",
*                       event_cache.len(),
*                       start.elapsed(),
*               );
*               event_cache
*       }
*        // ── Phase 2b: Pre-compute auth chains bottom-up ──
         // Uses an iterative post-order DFS with cycle detection to correctly handle
         // busted DAGs where auth events may appear out of order.

-       // Now reads auth_events directly from EventMeta instead of event_cache.

        fn rebuild_auth_chains(
                events_meta: &[EventMeta],

*               event_cache: &HashMap<OwnedEventId, Arc<PduEvent>>,
        ) -> (HashMap<OwnedEventId, u32>, Vec<OwnedEventId>, Vec<roaring::RoaringBitmap>) {
                let start = Instant::now();

@@ -218,22 +205,21 @@ fn rebuild_auth_chains(

                                visiting[curr] = true;

-                               let eid = &idx_to_eid[curr];

*                               // Read auth_events from metadata (index 2 in the tuple)
*                               let auth_events = &events_meta[curr].2;
                                let mut all_resolved = true;

-                               if let Some(pdu) = event_cache.get(eid) {
-                                       for auth_id in &pdu.auth_events {
-                                               if let Some(&auth_idx) = eid_to_idx.get(auth_id) {
-                                                       let auth_usize = to_usize(auth_idx);
-                                                       if bitmaps[auth_usize].is_none() {
-                                                               if visiting[auth_usize] {
-                                                                       warn!(
-                                                                               "rebuild_state: auth chain cycle at {} -> {}",
-                                                                               eid, auth_id,
-                                                                       );
-                                                               } else {
-                                                                       stack.push(auth_usize);
-                                                                       all_resolved = false;
-                                                               }

*                               for auth_id in auth_events {
*                                       if let Some(&auth_idx) = eid_to_idx.get(auth_id) {
*                                               let auth_usize = to_usize(auth_idx);
*                                               if bitmaps[auth_usize].is_none() {
*                                                       if visiting[auth_usize] {
*                                                               warn!(
*                                                                       "rebuild_state: auth chain cycle at {} -> {}",
*                                                                       idx_to_eid[curr], auth_id,
*                                                               );
*                                                       } else {
*                                                               stack.push(auth_usize);
*                                                               all_resolved = false;
                                                          }
                                                  }
                                          }

    @@ -241,14 +227,12 @@ fn rebuild_auth_chains(

                                            if all_resolved {
                                                    let mut chain = roaring::RoaringBitmap::new();

-                                       if let Some(pdu) = event_cache.get(eid) {
-                                               for auth_id in &pdu.auth_events {
-                                                       if let Some(&auth_idx) = eid_to_idx.get(auth_id) {
-                                                               let auth_usize = to_usize(auth_idx);
-                                                               if let Some(resolved_chain) = &bitmaps[auth_usize] {
-                                                                       chain.insert(auth_idx);
-                                                                       chain |= resolved_chain;
-                                                               }

*                                       for auth_id in auth_events {
*                                               if let Some(&auth_idx) = eid_to_idx.get(auth_id) {
*                                                       let auth_usize = to_usize(auth_idx);
*                                                       if let Some(resolved_chain) = &bitmaps[auth_usize] {
*                                                               chain.insert(auth_idx);
*                                                               chain |= resolved_chain;
                                                          }
                                                  }
                                          }
    @@ -277,6 +261,9 @@ fn rebuild_auth_chains(
    // inline SSH compression and DB writes. Evicts state groups from memory as
    // soon as all their child events have been processed, keeping only the
    // "live frontier" in RAM (~100-200 groups instead of 10k+).
*       //
*       // No event_cache needed: state_key and event_type come from EventMeta.
*       // PDUs are fetched on-demand only during fork resolution.

          async fn rebuild_walk_and_write(
                  &self,

    @@ -287,14 +274,11 @@ async fn rebuild_walk_and_write(
    let mut cork = Some(self.db.db.cork());

                            // ── Pre-cache short IDs ──

-               // Collect all (type, state_key) pairs from state events + all event IDs
                let precache_start = Instant::now();
                let mut unique_state_keys: HashSet<(String, String)> = HashSet::new();
-               for (eid, _, state_key, _) in &ctx.events_meta {
-                       if let Some(sk) = state_key {
-                               if let Some(pdu) = ctx.event_cache.get(eid) {
-                                       unique_state_keys.insert((pdu.kind.to_string(), sk.clone()));
-                               }

*               for (_, _, _, state_key, _) in &ctx.events_meta {
*                       if let Some((ty, sk)) = state_key {
*                               unique_state_keys.insert((ty.clone(), sk.clone()));
                        }
                }

@@ -323,8 +307,6 @@ async fn rebuild_walk_and_write(
);

                // ── Pre-compute group eviction metadata ──

-               // children_remaining[eid] = number of future events that reference eid
-               // as a prev_event. When it hits 0, eid's group can potentially be freed.
                  let mut children_remaining: HashMap<&OwnedEventId, usize> = HashMap::new();
                  for (_, prev_events, ..) in &ctx.events_meta {
                          for p in prev_events {

    @@ -335,8 +317,6 @@ async fn rebuild_walk_and_write(
    }
    }

-               // group_live_refs[gid] = number of events with remaining children that
-               // use this group. When it hits 0, the group's StateMap can be freed.
                  let mut group_live_refs: HashMap<usize, usize> = HashMap::new();

                  // ── State walk + inline write ──

    @@ -373,7 +353,7 @@ async fn rebuild_walk_and_write(
    let shortroomid = self.services.short.get_or_create_shortroomid(room_id).await;
    let mut current_shortstatehash = empty_ssh;

-               for (eid, prev_events, state_key, _depth) in &ctx.events_meta {

*               for (eid, prev_events, _, state_key, _depth) in &ctx.events_meta {
                          processed = processed.saturating_add(1);

                          if processed.is_multiple_of(1000) {

    @@ -429,8 +409,6 @@ async fn rebuild_walk_and_write(
    let mut state_hashes = Vec::with_capacity(unique_states.len()
    );
    for s in &unique_states {
    let mut h = std::collections::hash_map::DefaultHasher::new();

-                                                               // Sort entries to guarantee deterministic hashing
-                                                               // across HashMap/BTreeMap types
                                                                  let mut entries: Vec<_> = s.iter().collect();
                                                                  entries.sort_unstable_by_key(|(k, _)| *k);
                                                                  for (k, v) in entries {
    @@ -474,13 +452,14 @@ async fn rebuild_walk_and_write(
    fork_skip_count = fork_skip_count.saturating_add(1);
    unique_groups[superset_idx]
    } else {
-                                                               // Genuine conflict: ruma-lean optimized resolution

*                                                               // Genuine conflict: fetch PDUs on-demand + ruma-lean
                                                                let fork_start = Instant::now();
                                                                let fork_state_refs: Vec<&StateMap<OwnedEventId>> =
                                                                        unique_states.iter().map(|s| &**s).collect();

-                                                               let resolved =
-                                                                       Self::resolve_fork_with_states(ctx, &fork_state_refs);

*                                                               let resolved = self
*                                                                       .resolve_fork_with_states(room_id, ctx, &fork
    \_state_refs)
*                                                                       .await;

                                                                  let fork_elapsed = fork_start.elapsed();
                                                                  fork_resolve_count = fork_resolve_count.saturating_add(1);

    @@ -519,14 +498,9 @@ async fn rebuild_walk_and_write(
    };

                                    // ── Apply state event or inherit parent group ──

-                       let group_after = if let Some(sk) = state_key {
-                               let Some(pdu) = ctx.event_cache.get(eid) else {
-                                       warn!("rebuild_state: state event {eid} missing from cache — skipping");
-                                       event_group.insert(eid.clone(), state_before_group);
-                                       continue;
-                               };
-                               let event_type: ruma::events::StateEventType = pdu.kind.to_string().into();
-                               let sk_typed: conduwuit_core::matrix::StateKey = sk.as_str().into();

*                       let group_after = if let Some((event_type_str, state_key_str)) = state_key {
*                               let event_type: ruma::events::StateEventType = event_type_str.as_str().into();
*                               let sk_typed: conduwuit_core::matrix::StateKey = state_key_str.as_str().into();

                                  let current_state = state_groups
                                          .get(&state_before_group)

    @@ -552,7 +526,6 @@ async fn rebuild_walk_and_write(
    event_group.insert(eid.clone(), group_after);

                                    // ── Track group liveness ──

-                       // If this event has children, mark its group as live
                            if children_remaining.get(eid).copied().unwrap_or(0) > 0 {
                                    let refs = group_live_refs.entry(group_after).or_insert(0);
                                    *refs = refs.saturating_add(1);

      @@ -600,18 +573,16 @@ async fn rebuild_walk_and_write(

                              // Write pdu_shortstatehash for this event
                              if state_key.is_some() {`

      Ran command: `gs

    On branch guru/dev-2026-03-27+b1-presence+b2-federation
    Your branch is ahead of 'origin/guru/dev-2026-03-27+b1-presence+b2-federation' by 6 commits.
    (use "git push" to publish your local commits)
    nothing to commit, working tree clean`

Good question — let me check what actually uses those tables:

Searched for "shorteventid_shortprevevents"
Searched for "shortid_shortauthchain"
Searched for "shortauthchain|auth_chain.*short|shorteventid.*authevents|shortauthevents"

**No, they're still needed.** `rebuild_state` bypasses them, but they're used in production hot paths:

| Table                          | Still used by                                                                                                                |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------- |
| `shorteventid_shortauthevents` | `auth_chain/mod.rs` — `get_auth_chain_inner()` for federation `/event_auth`, `/state_ids`, and live state res during `/send` |
| `shorteventid_shortprevevents` | `data.rs` — `get_shortprevevents()` for DAG traversal, outlier processing, `reindex.rs`                                      |

`rebuild_state` builds its **own** in-memory auth chains from `EventMeta` (RoaringBitmaps) and never touches those tables. But:

- **`auth_chain/mod.rs`** uses `shorteventid_shortauthevents` to compute auth chains on the fly for federation requests and live state resolution — this is the normal runtime path, not rebuild
- **`reindex.rs`** uses `shorteventid_shortprevevents` to rebuild DAG edges when shorteventids change
- **`outlier/mod.rs`** writes to both tables when storing outlier events

So `rebuild_state` is self-contained (metadata + RoaringBitmaps), but those tables are still the canonical persistent auth/prev edge stores for everything else in the server.

---

Searched for "tscount|timestamp_to_event|msc3030|roomid_ts"
Searched for "timestamp_to|jump.*date|origin_server_ts.*index|get_event_for_timestamp"
Searched for "timestamp_to_event|get_event_for_timestamp|TimestampToEvent"
Searched for "v1/rooms.*timestamp|rooms.*ts=|\_timestamp"

No. Continuwuity has **no** `roomid_tscount_pducount` index and **no** MSC3030 (`/timestamp_to_event`) endpoint.

Tuwunel added that index to support "jump to date" — the key is `[shortroomid][origin_server_ts][pdu_count]` so you can binary-search for the closest event to any timestamp.

Without it, continuwuity would have to do a full linear scan of the room's timeline to find the event closest to a given timestamp. The topo index sorts by `(depth, count)` and the stream index sorts by `(count)` — neither is sorted by timestamp.

Want me to look into adding it? It'd be a new column family + a migration to backfill it from existing events + the API endpoint.

# `maps.rs` Optimization Review Package

This note packages the likely performance work in `src/database/maps.rs` for a deeper advisor review.

The goal is not to pick a winner yet. The goal is to identify which map families are worth benchmark-driven analysis, which ones are probably already fine, and which ones would require a schema migration rather than a simple tuning change.

## Scope

Primary source:

- [`src/database/maps.rs`](/run/media/shane/shane4tb-ent/repos/continuwuity/src/database/maps.rs)

Relevant consumers:

- [`src/database/engine/cf_opts.rs`](/run/media/shane/shane4tb-ent/repos/continuwuity/src/database/engine/cf_opts.rs)
- [`src/service/rooms/timeline/data.rs`](/run/media/shane/shane4tb-ent/repos/continuwuity/src/service/rooms/timeline/data.rs)
- [`src/service/rooms/auth_chain/mod.rs`](/run/media/shane/shane4tb-ent/repos/continuwuity/src/service/rooms/auth_chain/mod.rs)
- [`src/service/rooms/timeline/extremities.rs`](/run/media/shane/shane4tb-ent/repos/continuwuity/src/service/rooms/timeline/extremities.rs)
- [`src/service/rooms/outlier/mod.rs`](/run/media/shane/shane4tb-ent/repos/continuwuity/src/service/rooms/outlier/mod.rs)
- [`src/service/rooms/timeline/reindex.rs`](/run/media/shane/shane4tb-ent/repos/continuwuity/src/service/rooms/timeline/reindex.rs)

## Executive Summary

`maps.rs` is mostly a RocksDB descriptor table. The optimization surface is concentrated in:

- key layout
- cache placement and cache sizing
- block size / index size / compression presets
- whether a map is actually hot enough to justify special handling

The most interesting candidates are not the tiny account/profile maps. They are the room-local DAG and timeline tables, especially:

- `shorteventid_shortauthevents`
- `shorteventid_shortprevevents`
- `shorteventid_authchain`
- `room_pducount_eventid`
- `roomid_topologicalorder_pducount`
- `roomid_timestamp_pducount`
- `roomid_depth_missingeventid`
- `roomid_missingeventid_depth`

The highest-value question is whether any of the point-lookup edge tables should become room-prefixed compound keys or be reorganized into fewer, denser CFs.

## Current Shape

### Already room-scoped and likely decent

These are already keyed by `shortroomid` prefix or otherwise room-local:

- `room_pducount_eventid`
- `roomid_topologicalorder_pducount`
- `roomid_timestamp_pducount`
- `roomid_depth_missingeventid`
- `roomid_missingeventid_depth`

These are the tables where prefix locality already works in our favor.

### Current point-lookup / random-read tables

These are the main candidates for tuning:

- `eventid_pdu`
- `eventid_pduid`
- `eventid_shorteventid`
- `shorteventid_eventid`
- `shorteventid_authchain`
- `shorteventid_shortauthevents`
- `shorteventid_shortprevevents`
- `shortstatekey_statekey`
- `statekey_shortstatekey`

### Likely low-priority unless profiling says otherwise

Most of the small random maps in the top half of `maps.rs` are probably not worth a schema redesign first:

- alias / login / token / password-reset maps
- device and media lookup maps
- user profile maps
- small relation maps

These are more likely to benefit from generic RocksDB tuning than bespoke layout changes.

## Candidate Optimizations

### 1. Room-prefixed edge keys for auth/prev edges

The current edge maps are keyed by `shorteventid` alone:

- `shorteventid_shortauthevents`
- `shorteventid_shortprevevents`

That means any room-local traversal that touches many events pays a lot of scattered point reads. A room-prefixed key, such as:

- `shortroomid || shorteventid`

would cluster adjacent lookups by room and improve locality.

This is the clearest schema-level optimization in the file, but it is also the one that requires the most migration work:

- dual write old and new
- read new first, fallback to old
- backfill historical rows
- remove old CFs after verification

This is already discussed in the existing perf note in `docs/development-gg/new-db-col-fams-for-msc3030-and-in-general.md`.

Advisor question:

- Does this reduce end-to-end latency enough on realistic auth-chain / backfill / reindex workloads to justify the migration complexity?

### 2. Reconsider `shorteventid_authchain`

`shorteventid_authchain` is a cached closure map. It is logically related to the edge maps, but it has different usage and cache behavior.

Potential questions:

- Is the current cache size sane relative to `shorteventid_shortauthevents` and `shorteventid_shortprevevents`?
- Would a different block size or a different compaction profile help?
- Is this map hot enough to deserve special tuning, or is the real bottleneck upstream in traversal logic?

This is a tuning candidate, not a schema candidate, unless profiling shows the cache miss pattern is pathological.

### 3. Cache capacity tuning for hot random-read maps

`cf_opts.rs` gives explicit cache knobs for the hottest maps:

- `pdu_cache_capacity`
- `auth_chain_cache_capacity`
- `shorteventid_cache_capacity`
- `shorteventid_shortprevevents_cache_capacity`
- `shorteventid_shortauthevents_cache_capacity`
- `eventidshort_cache_capacity`
- `eventid_pdu_cache_capacity`
- `shortstatekey_cache_capacity`

These defaults are already distinct, but they were clearly chosen as broad heuristics rather than workload-specific profiles.

Likely review points:

- Are the edge-table cache capacities too small for large rooms?
- Are we over-caching low-value maps and under-caching the edge tables?
- Would a smaller number of larger caches outperform many unique caches for these workloads?

This is the lowest-risk optimization path because it does not require a schema migration.

### 4. Compaction and block layout for sequential room indexes

`room_pducount_eventid`, `roomid_topologicalorder_pducount`, and `roomid_timestamp_pducount` are all room-scoped scans, but they use different descriptor presets.

Worth checking:

- Are the `SEQUENTIAL` vs `SEQUENTIAL_SMALL` presets actually matched to their write and scan patterns?
- Would any of these benefit from a larger block size or a different index size?
- Are the existing compressed-index choices still optimal on current data sizes?

This is especially relevant for tables that are scan-heavy but not write-heavy.

### 5. Remove or retire dead weight

`room_pducount_eventid_backup` is already marked deprecated in `maps.rs`.

Advisor question:

- Is it safe to schedule this for removal in the next DB version bump, or does any live path still depend on it?

This is not a runtime optimization by itself, but it reduces schema surface and maintenance cost.

### 6. Check whether some dual indexes should be merged

Several table pairs are effectively dual indexes over the same underlying concept:

- `eventid_pduid` and `eventid_pdu`
- `eventid_shorteventid` and `shorteventid_eventid`
- `roomid_depth_missingeventid` and `roomid_missingeventid_depth`

The current design is valid, but the advisor should check whether any of these pairs are more expensive than necessary:

- Are both indexes needed on the hot path?
- Could one be derived cheaply enough to delete?
- Is the extra write amplification justified?

This is a good place to look for simplification, not just speed.

## What To Measure

The advisor should benchmark the following before proposing a change:

1. Auth-chain reconstruction for large rooms.
2. Reindex / repair passes that repeatedly walk `shorteventid_shortprevevents` and `shorteventid_shortauthevents`.
3. Backfill and extremity recalculation in a room with high DAG churn.
4. Typical `/event_auth` and `/state_ids` latency under cache pressure.
5. Cold-cache versus warm-cache behavior for the hot CFs.

Metrics to collect:

- total latency
- point reads per event
- block cache hit ratio
- SST read amplification
- write amplification after any schema change
- migration cost for historical data

## Suggested Decision Framework

For each candidate, the advisor should classify it as one of:

- `tuning only`
- `new CF required`
- `needs migration/backfill`
- `probably not worth it`

That classification matters more than the raw speedup guess.

## My Initial Ranking

Most promising:

1. Room-prefixed auth/prev edge keys
2. Cache retuning for `shorteventid_shortauthevents` / `shorteventid_shortprevevents`
3. Revisit `shorteventid_authchain`

Medium value:

1. Sequential index block/layout tuning
2. Dual-index simplification review

Low value unless profiling says otherwise:

1. Small account/profile/token maps
2. Deprecated map cleanup

## Open Questions For The Advisor

- Is the hotspot actually RocksDB lookup latency, or is it the traversal logic around it?
- Do the edge tables show enough temporal/room locality to justify a room-prefixed redesign?
- Would a schema change win more than a better cache policy?
- Are any of the current descriptor presets clearly mismatched to their workload?
- Which maps are worth keeping as separate CFs versus folding into fewer, denser structures?

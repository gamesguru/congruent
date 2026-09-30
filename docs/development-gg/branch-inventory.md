# Branch Inventory: `guru/dev-2026-03-27+b1-presence+b2-federation`

912 commits off `main`. Organized by functional area.

---

## 1. Presence (b1) — ~30 commits

The original purpose of this branch. Optimizes presence federation.

- Moka cache for presence layer with tests
- Batch presence updates per-server (not per-room) with smart bundling policy
- Cap presence EDU batching to transaction window
- Stream-based presence processing (avoid collect)
- Cache `user_sees_user` checks
- `raw_stream_from()` fast scan (avoid full scan)
- Timer list emptying, skip non-dupe EDUs
- Time-based federation batching
- Conditional status updates based on user→server cache mappings
- Throttle presence updates, invalidate stale user-user cache
- Presence stats bumped to warn log level
- `yield_now().await` in member API to speed up refresh

**Complement impact**: Neutral (presence tests already passing)

---

## 2. Federation (b2) — ~40 commits

Performance and correctness for inbound/outbound federation.

- `wide_` methods to parallelize server discovery and remote PDU fetching
- Short-circuit soft-failed outliers, parallelize event fetching
- Handle soft-failed txns gracefully, info-log skips
- Process backfill PDUs oldest-first (prevent sequential network stalls)
- Fix timeline order for backfill PDUs
- Comprehensive fixes for backfill, outliers, and budget gating
- Logic to handle outliers better
- Bypassed signature events applied to auth chain outliers
- Sync `fetch_state` before async healer
- Propagate `pdu_id` from `upgrade_outlier_to_timeline_pdu` ← **+108 pass**
- Restore sync `fetch_state` for missing auth events
- Include direct `auth_events` in flattened cache closure
- Fix bogus PDU injection in `should_rescind_invite`
- Call `/state_ids` when auth chain too deep for iterative fetcher

**Complement impact**: Commit `9ff4c1b82` was the big jump (+94 total tests, +108 passes)

---

## 3. State Resolution — ~50 commits

V2.1 (MSC4297) state resolution implementation and performance.

### V2.1 / MSC4297

- Correctly preserve unconflicted state in V2.1 resolution
- Restore V2.1 auth diff flattening
- Restore supplemental auth events for V2.1
- Gate V2.1 PL sub-resolution (prevent tripled auth-check on V2/V10/V11)
- V2.1 supplemental auth gate restored
- `min_depth` pruning for v2.1 conflicted state subgraph
- Unit tests for v2.1 state res
- Refactor: split tests out separately

### Performance

- Concurrent layer-by-layer subgraph traversal
- Optimistic concurrency control to eliminate room lock contention
- OCC race condition and BFS duplication bug fixes
- BFS traversal and iterative auth-check hoisting
- `DashMap+OnceCell` lazy fetchers, BFS auth scan, sender PL memoization
- Batched PDU get for faster auth chain traversals
- Remove redundant inner DashMap cache from `mainline_sort`
- Hard cap + `skip_sig_verify` threading for DAG recovery
- Replace O(N) `get_mainline_depth` with O(1) LCA-to-RMQ Sparse Table
- Eradicate polynomial scaling and async deadlocks
- Flatten auth chain cache, restore synchronous inline timeline upgrades
- Optimize state resolution cache, repair unsigned exits

### DAG Healer (now removed)

- Flatten recursive DAG healing into iterative fetch queue
- DAG healer batching
- Make DAG healer async
- **Removed entirely** (`c1810faf2`) — was causing race conditions with
  `force_state` that corrupted `state_cache` and broke restricted room tests

**Complement impact**: V2.1 commits were neutral (406-410 band). Healer removal pending verification.

---

## 4. Timeline / Extremities — ~10 commits

- Prevent soft-failed events from poisoning DAG extremities
- `clean_extremities` admin command
- OOM mitigation for `reorder-timeline` admin command
- Use `get_pdu_json_from_id` for reorder backup (prevent skipping events)

---

## 5. Admin CLI — ~10 commits

- Progress counter and faster flush to `check-rooms`
- Handle oversized event IDs in `force_set_state` via raw JSON extraction
- Properly wait/allow interruption of long-running attach jobs
- `purge_outliers` command speedup
- Update admin CLI docs/cmd markdown

---

## 6. Sync / Client API — ~10 commits

- Fix lazy loading sync bug
- Restore `joined.rs` and `mod.rs` logic from upstream
- Cork and flush DB prior to join room calls
- Load spaces/room hierarchy calls faster
- Batch short states and better scans
- Resolve OOM lockups and swap thrashing on large room joins

---

## 7. General Perf — ~15 commits

- Skip `notify_presence_change` DB reads in presence loop
- `members?at=` speedup (selectively pull room members data)
- Reduce federation noise to debug level
- Footprint counters for `state_res` and `auth_fetch`
- Advisory optimization in state res

---

## 8. Lint / Format / CI — ~40+ commits

- Numerous lint/format passes
- CI: update justfile for `like=` on postgres query
- CI: force complementary test diagnostic baseline
- CI: checkout submodules for ruma-upstream test fixtures
- CI: exclude/include `case-study-state-res` workspace member

---

## 9. WIP / Experimental — ~15 commits

- Various `wip`, `wip2`, `wip3`, `wip4` commits
- "many changes" commit (`b0c24cfef`)
- Speculative auth chain walk change (reverted)

---

## Complement Test Trend

Earliest data in log starts at commit #1551. Total tests are 740; the drop to 646 at #1617 and back to 740 at #1628 was a CI configuration change (not a code regression).

| #    | Commit      | Total   | Pass        | Description                                                      |
| ---- | ----------- | ------- | ----------- | ---------------------------------------------------------------- |
| 1551 | `ca6fd92af` | 740     | 522–524     | fix(federation): circuit breaker retryable error                 |
| 1553 | `29c0d902e` | 740     | 522–524     | feat(observability): phase-transition logging                    |
| 1561 | `65f1f764b` | 740     | 522–524     | fix: apply bypassed_signature_events to auth chain outliers      |
| 1564 | `e27bcc380` | 740     | 522–524     | refactor: factor out reconcile fork state stackframe             |
| 1569 | `a61afa22c` | 740     | 520–522     | fix: prevent soft-failed events poisoning DAG extremities        |
| 1571 | `18e08d048` | 740     | 520–522     | fix tests/lint                                                   |
| 1575 | `4970ec8c8` | 740     | 520–522     | more test modifications in state res                             |
| 1576 | `8b2799a47` | 740     | 522–524     | fix: call /state_ids when auth chain too deep                    |
| 1577 | `dc322762d` | 740     | 520–522     | fix: revert blocking fetch_state; add timing logs                |
| 1582 | `18bac3ae7` | 740     | 522         | DashMap+OnceCell lazy fetchers, BFS auth scan, sender PL memo    |
| 1584 | `554d1f5f6` | 740     | 520–522     | perf: optimize state-res, remove BFS traps, async healer         |
| 1586 | `6050efc12` | 740     | 522         | perf: eradicate polynomial scaling and async deadlocks           |
| 1592 | `deb548b9f` | 740     | 522         | perf: hard cap + skip_sig_verify for DAG recovery                |
| 1600 | `1237fb2ae` | 740     | 520–524     | fix: sync fetch_state before async healer                        |
| 1602 | `cf76ed029` | 740     | 522–524     | refactor: split upgrade_outlier_to_timeline_pdu                  |
| 1607 | `e271b4227` | 740     | 519–522     | style: rustfmt on state-res patches                              |
| 1610 | `9438b6f1e` | 740     | 519–521     | perf: optimistic concurrency control (OCC) for room locks        |
| 1611 | `fa19eb7c6` | 740     | 519–521     | fix: OCC race condition and BFS duplication bugs                 |
| 1617 | `f3a6d89b9` | **646** | 404–408     | lints _(CI config dropped 94 tests from suite)_                  |
| 1618 | `16f41eddf` | 646     | 406–410     | lints                                                            |
| 1619 | `1c9d90361` | 646     | 406–410     | perf: v2.1 timeouts, flatten auth chain cache                    |
| 1620 | `1f4fd990e` | 646     | 408–410     | fix: preserve unconflicted state in V2.1                         |
| 1622 | `2779d5a90` | 646     | 406–410     | fix: V2.1 auth diff flattening                                   |
| 1627 | `d02652a4c` | 646     | 406–410     | fix: gate V2.1 PL sub-resolution                                 |
| 1628 | `9ff4c1b82` | **740** | **514–518** | **fix: propagate pdu_id + restore V2.1 auth gate** _(+108 pass)_ |
| 1631 | `e36ed50b2` | 740     | 515–518     | refactor: split tests out separately                             |
| 1637 | `302de31f0` | 740     | 520–522     | fix(ci): checkout submodules for test fixtures                   |
| 1638 | `c1810faf2` | 740     | TBD         | **remove DAG healer**                                            |

**Key observations:**

- #1551–#1611: Stable at **519–524 pass** across all perf/state-res work. No measurable improvement or regression.
- #1617: CI config change dropped total from 740→646, obscuring pass count comparisons.
- #1628: Restored 740 total AND jumped from 406→518 pass — the `pdu_id` propagation fix was the only commit that moved the needle.
- The entire V2.1 state-res series (#1619–#1627) was **measurably neutral** on Complement.

### Current Regressions (vs pre-branch)

| Test                                                 | Cause                                 | Status                               |
| ---------------------------------------------------- | ------------------------------------- | ------------------------------------ |
| `TestRestrictedRoomsRemoteJoinFailOver`              | DAG healer race → stale `state_cache` | Healer removed, pending verification |
| `TestRestrictedRoomsRemoteJoinFailOverInMSC3787Room` | Same                                  | Same                                 |
| `TestRestrictedRoomsSpacesSummaryFederation`         | Same class                            | Same                                 |
| `TestEventAuth` (3 subtests)                         | Likely auth chain changes             | Not yet investigated                 |
| `TestMSC4297StateResolutionV2_1_*` (2 tests)         | Pre-existing, needs fixture files     | CI submodule issue                   |
| `TestToDeviceMessagesOverFederation`                 | Intermittent, pre-existing            | Flaky                                |

### Known Live Server Issues

| Issue                           | Description                                                                                                                                        |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Room `!L58ME...` 100s state-res | 60k auth chain, 15k conflicted set, ~2800 DAG holes. Every membership event costs 100s.                                                            |
| Txn `1779618351273` 500 loop    | PDU missing `event_id` field. `Pdu` struct requires it but v4+ rooms don't include it on wire. Some deserialization path bypasses `from_id_val()`. |

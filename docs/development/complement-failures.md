# Complement Test Failures

Last updated: 2026-05-06
Branch: `guru/dev-2026-03-27+b1-presence+b2-federation`
Results: **391 pass / 138 fail** (subtests)

## Summary

Current branch fixes **46 tests** vs the dev branch (362 pass / 167 fail),
with **zero regressions**. The remaining 138 failing subtests are categorized
below.

---

## Federation Reliability (Power Outage / Kill / Restart)

These tests directly exercise message delivery after server stops, restarts,
and network interruptions — the scenario most relevant to power outages and
killed processes.

### `TestToDeviceMessagesOverFederation`

| Subtest | Status | Notes |
|---------|--------|-------|
| `good_connectivity` | ✅ pass | |
| `interrupted_connectivity` | ✅ pass | Fixed by `reschedule_flush` |
| `stopped_server` | ✅ pass | Fixed by exponential backoff |

**Root cause (fixed)**: Two issues combined to cause lost to-device EDUs:
1. **Timer jitter**: `handle_response_err` schedules a retry Flush via
   `tokio::spawn(sleep → send)`. OS timer jitter could cause the Flush to
   arrive before the backoff expired, silently dropping it. Fixed by
   `reschedule_flush` which re-queues rejected Flush messages after 1s.
2. **Quadratic backoff**: The backoff formula was `base × tries²` (5, 20, 45s)
   instead of proper exponential `base × 2^(tries-1)` (5, 10, 20s). After a
   server restart, `startup_netburst` would fail twice (hs2 still down), and
   the 20s second retry pushed total delivery past the 30s test timeout.
   Fixed by switching to standard exponential backoff.

**Files**: `src/service/sending/sender.rs` (`handle_request`, `reschedule_flush`,
`handle_response_err`)

### `TestDeviceListsUpdateOverFederation`

| Subtest | Status | Notes |
|---------|--------|-------|
| `good_connectivity` | ❌ fail | Basic device list EDU delivery broken |
| `interrupted_connectivity` | ❌ fail | |
| `stopped_server` | ❌ fail | |

**Root cause**: All three subtests fail, including `good_connectivity`. This is
NOT a restart race — device list update EDUs are fundamentally not being
delivered over federation. Likely a separate bug in device list change
tracking or EDU composition, not the sender retry logic.

---

## PDU Delivery & Backfill

These tests exercise receiving PDUs over federation, including historical
event retrieval (backfill) and forward timeline construction.

### `TestMessagesOverFederation`

| Subtest | Status | Notes |
|---------|--------|-------|
| `Visible shared history after re-joining room (backfill)` | ❌ fail | |
| ↳ `messagesRequestLimit is lower than the number of messages backfilled` | ❌ fail | |

**Impact**: After leaving and re-joining a room, historical messages from the
period of absence are not correctly backfilled from the remote server. This
affects users who rejoin rooms after a gap (including power outage scenarios
where the server misses events while offline).

### `TestOutboundFederationEventSizeGetMissingEvents`

❌ **fail** — Federation `/get_missing_events` handler may not correctly
handle large event payloads.

### `TestOutboundFederationIgnoresMissingEventWithBadJSONForRoomVersion6`

❌ **fail** — Missing events with invalid JSON in room version 6 should be
silently ignored rather than causing errors.

### `TestInboundCanReturnMissingEvents`

| Subtest | Status |
|---------|--------|
| `invited visibility` | ❌ fail |
| `joined visibility` | ❌ fail |
| `shared visibility` | ❌ fail |
| `world_readable visibility` | ❌ fail |

**Impact**: The `/get_missing_events` endpoint fails across all visibility
levels. This directly affects forwardfill — when a server comes back online
after a power outage, remote servers request missing events via this endpoint
to fill gaps in the DAG.

### `TestSendJoinPartialStateResponse`

❌ **fail** — Partial state join responses may not be correctly handled,
affecting lazy-loading federation joins.

---

## Timeline & Sync

### `TestSync`

| Subtest | Status | Notes |
|---------|--------|-------|
| `Newly joined room has correct timeline in incremental sync` | ❌ fail | |
| `Newly joined room includes presence in incremental sync` | ❌ fail | |
| `Get presence for newly joined members in incremental sync` | ❌ fail | |
| `Device list tracking / User correctly listed when they leave` | ❌ fail | |

### `TestRoomCreationReportsEventsToMyself`

| Subtest | Status |
|---------|--------|
| `Room creation reports m.room.create to myself` | ❌ fail |
| `Setting state twice is idempotent` | ❌ fail |

### `TestArchivedRoomsHistory`

| Subtest | Status |
|---------|--------|
| `timeline_has_events/incremental_sync` | ❌ fail |
| `timeline_has_events/initial_sync` | ❌ fail |

---

## MSC3030 / Timestamp-to-Event

### `TestJumpToDateEndpoint`

All subtests fail (local and federated). This is the MSC3030 implementation.
The `roomid_timestamp_pducount` column needs to be properly registered and
the endpoints need route registration for both stable and unstable paths.

---

## Other Notable Failures

| Test | Category | Notes |
|------|----------|-------|
| `TestRestrictedRooms*Join` | Auth | Restricted room joins fail (local + remote + MSC3787) |
| `TestFederationKeyUploadQuery` | E2EE | Remote key claim/query broken |
| `TestMembershipOnEvents` | State | |
| `TestRelationsPagination` | Relations | |
| `TestSearch` | Search | Back-pagination and context around results |
| `TestRoomForget` | Forget | Forgotten room message pagination |
| `TestThreadSubscriptions` | Threads | All 7 subtests fail |
| `TestDelayedEvents` | MSC | All subtests fail |
| `TestPushRuleRoomUpgrade` | Push | Push rules not carried over on room upgrade |
| `TestAsyncUpload` | Media | Async upload flow broken |
| `TestRemovingAccountData` | Account | DELETE/PUT account data removal |

---

## Tests Fixed by Current Branch (vs dev)

The current branch fixes 45 subtests including:

- `TestToDeviceMessagesOverFederation/interrupted_connectivity` — federation retry
- `TestPresence` / `TestRemotePresence` — presence tracking
- `TestSendAndFetchMessage` — basic message send/receive
- `TestMediaFilenames` — media download with custom filenames
- `TestRoomCanonicalAlias` — alias validation
- `TestRoomMembers` — join with custom content
- `TestRoomState` — joined members fetch
- `TestMSC4297StateResolutionV2_1` — state resolution v2.1
- `TestArchivedRoomsHistory/timeline_is_empty` — archived room sync
- `TestThreadReceiptsInSyncMSC4102` — threaded receipts
- `TestSync/sync_should_succeed_even_if_...redaction_of_unknown_event`

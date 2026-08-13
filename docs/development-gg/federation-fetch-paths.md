# Federation DAG Fetch Paths

Continuwuity has **4 independent code paths** that fetch events from remote
servers to fill DAG gaps. Each has its own server selection logic, concurrency
model, error handling, and retry behavior. This fragmentation makes it
difficult to reason about DAG completeness guarantees.

## Path 1: `fetch_and_handle_outliers`

**File:** `src/service/rooms/event_handler/fetch_and_handle_outliers.rs`

**Used by:** DAG healer, state resolution dependency fetching

**Method:** `GET /_matrix/federation/v1/event/{eventId}` (per-event)

**Server selection:** Origin server + up to 4 routing servers from
`trusted_servers` / room members. Truncated to 4 total.

**Concurrency:** Sequential per-event, but called in parallel batches by the
healer.

**Error handling:** Returns the first successful fetch; logs failures at debug
level. "All fallback servers exhausted" was previously at warn (now debug).

**Weakness:** Per-event fetching is O(n) round-trips for n missing events.
No batch endpoint used.

---

## Path 2: `monitor.rs` (forward-fill sweep)

**File:** `src/service/rooms/monitor.rs`

**Used by:** Background periodic sweep (every 4 hours) and startup scan.

**Method:** Two-phase:

1. `PUT /_matrix/federation/v1/make_join` — probe for remote extremities via
   `prev_events` in the join template
2. `POST /_matrix/federation/v1/get_missing_events` — fetch gap between local
   and remote extremities

**Server selection:** Trusted servers first (up to 5), then room homeserver,
then random sample from remaining participants. Avoids alphabetical bias.

**Concurrency:** 10 rooms concurrently (periodic), 1 room at a time (startup).
50ms sleep between rooms.

**Error handling:** Tries each candidate server sequentially. First successful
probe wins. Failures logged at warn.

**Weakness:** Only probes rooms idle for >12 hours (`PERIODIC_STALE_THRESHOLD_MS`).
Active rooms with excessive extremities are never swept. The `make_join` probe
is a hack — it reveals remote extremities as a side effect but isn't designed
for DAG healing. Limited to 50 events per `/get_missing_events` call.

---

## Path 3: `fetch_prev` (incoming PDU processing)

**File:** `src/service/rooms/event_handler/fetch_prev.rs`

**Used by:** `handle_incoming_pdu` when processing federation `/send` transactions.

**Method:** `GET /_matrix/federation/v1/event/{eventId}` for each missing
`prev_event` in the incoming PDU.

**Server selection:** Origin server of the incoming transaction.

**Concurrency:** Sequential per missing prev_event.

**Error handling:** Missing prev_events are fetched and processed recursively.
Depth-limited to prevent infinite recursion.

**Weakness:** Only fetches from the sending server. If that server doesn't
have the event (e.g., it was a different server's event), the fetch fails and
creates a DAG hole. No fallback to other servers.

---

## Path 4: `pre_fetch_state_res_deps`

**File:** `src/service/rooms/event_handler/pre_fetch_state_res_deps.rs`

**Used by:** State resolution for incoming state events (called before
acquiring the room lock).

**Method:** Two-phase:

1. `GET /_matrix/federation/v1/state_ids/{roomId}` — get full state ID set
2. `GET /_matrix/federation/v1/event/{eventId}` — batch fetch missing events

**Server selection:** Origin server + trusted servers + room member servers.
Multi-server with 300s budget.

**Concurrency:** 32 concurrent event fetches.

**Error handling:** Best-effort. Missing events are logged but don't block
state resolution (it proceeds with whatever it has).

**Weakness:** Only triggered for state events, not regular messages. The
`/state_ids` response can be very large for big rooms.

---

## Consolidation Opportunity

All 4 paths should share a common helper:

```rust
async fn fetch_events_from_federation(
    &self,
    room_id: &RoomId,
    event_ids: &[&EventId],
    candidate_servers: &[OwnedServerName],
    concurrency: usize,
    timeout: Duration,
) -> Vec<(OwnedEventId, Result<PduEvent>)>
```

This would unify:

- Server selection and prioritization
- Concurrency control
- Error handling and logging
- Rate limiting and backoff
- Deduplication of in-flight requests

### Impact

Without consolidation, each path makes independent decisions about which
servers to contact, leading to:

- **DAG holes** when `fetch_prev` only tries the origin server
- **Wasted bandwidth** when multiple paths fetch the same event concurrently
- **Inconsistent resilience** — some paths retry across servers, others don't
- **Stuck outliers** when an event's prev_events can't be fetched by the
  single path that tries

## Auth-Chain Resolution Note

`handle_outlier_pdu` currently prefers a bulk `/event_auth` fetch for
missing auth events, then falls back to single-hop `/event/{id}` fetches
for whatever remains missing.

That is a valid optimization, but it is not the same strategy Synapse
uses. Synapse resolves missing auth events by fetching the missing event
IDs individually via `/event/{id}`; it does not have a bulk `/event_auth`
size cliff in this path. The tradeoff is straightforward:

- Bulk `/event_auth` is cheaper when the remote can satisfy the whole
  chain in one response.
- Individual `/event` fetches are more uniform and degrade more
  gracefully when the remote only partially supports the auth-chain path
  or when the total auth chain is large but the local gap is small.

This is a design note, not a root-cause claim for any specific failing
test. The code keeps the bulk path as the first attempt and adds a
bounded per-event fallback for the remaining missing auth events.

### Related Issues

- Stuck outlier `$gpwWnqDYqCLRDVHXE8S45KVSJnyVBOJoRgBIWlz4Zuc` in
  `!tgmfqAWaBc978M80V9:nutra.tk` — local event became an outlier because
  `fetch_prev` couldn't resolve the second `prev_event` (`$a3T_...`) from
  the origin server alone.
- Excessive extremities in `t2l.io` rooms — concurrent fork accumulation
  from many servers, compounded by inconsistent extremity caps (now fixed
  with unified `MAX_FORWARD_EXTREMITIES=10` at the DB writer level).

# Refactor: Narrow `handle_incoming_pdu` State Fetches

## Problem

`handle_incoming_pdu` is a broad federation-ingestion entry point. Depending on
the event and its local dependencies, it can:

- validate and store an incoming PDU;
- fetch missing predecessor events;
- fetch missing auth/state events;
- call federation `/state_ids`;
- resolve state at the event;
- promote the event into the timeline.

This makes the callsite's intent implicit. A caller that only wants to ingest an
event can unexpectedly trigger an expensive full-state federation request.

`/state_ids` is particularly expensive: it asks the remote server for the full
state and auth-chain IDs at an event. It should not be used as a generic retry
for every kind of event ingestion.

## Observed Failure

Complement `TestFederatedEventRelationships` currently reports five unexpected
requests to the remote test server:

```text
GET /_matrix/federation/v1/state_ids/<room_id>
```

The sequence is:

1. A remote join receives `send_join` state and remote DAG extremities.
2. A missing extremity is fetched through MSC2836 `/event_relationships`.
3. The returned event is passed to `handle_incoming_pdu` as a timeline event.
4. Generic state resolution cannot resolve the event locally.
5. The handler synchronously falls back to `/state_ids` against Complement.
6. Complement marks the calls as unexpected and fails the test.

The MSC2836 client endpoint itself returns `200 OK`; the failure is caused by
the join/extremity ingestion path.

Relevant callsites:

- `src/api/client/membership/join.rs`: first-join and rejoin extremity fetches;
- `src/api/server/send.rs`: normal live federation transactions;
- `src/api/server/utils.rs`: send-join, send-leave, and send-knock ingestion;
- `src/api/client/membership/invite.rs`: invite event ingestion;
- `src/service/rooms/monitor.rs`: background federation repair;
- `src/service/rooms/timeline/backfill.rs`: historical backfill.

The `fetch_state` callsites are concentrated in the incoming-PDU state
resolution path and missing-auth retry path:

- `src/service/rooms/event_handler/upgrade_outlier_pdu.rs`;
- `src/service/rooms/event_handler/handle_incoming_pdu.rs`.

## Correctness Risk

Suppressing `/state_ids` globally would be unsafe. If an event is accepted using
the current room state instead of the state at its predecessor, the server may:

- incorrectly pass or fail membership and power-level auth checks;
- attach an event to the wrong state snapshot;
- create incorrect forward extremities;
- accept or reject later events differently from other servers;
- propagate a state divergence through federation.

The fix must therefore narrow the operation's behavior by ingestion intent, not
skip state/auth validation indiscriminately.

## Proposed Design

Replace the implicit behavior with explicit ingestion modes, for example:

```rust
enum IncomingPduMode {
	LiveTimeline,
	JoinBootstrap,
	Backfill,
	Repair,
}
```

### `LiveTimeline`

Used for `/send` and other normal live federation events.

- Perform full auth and state resolution.
- Fetch missing predecessors and auth events as required.
- Permit `/state_ids` when local state is genuinely unavailable.
- Do not silently fall back to current state when correctness depends on
  state-at-event.

### `JoinBootstrap`

Used while processing events associated with a `send_join` response.

- Treat the state and auth chain returned by `send_join` as the authoritative
  bootstrap snapshot.
- Do not independently call `/state_ids` for an extremity whose state is already
  represented by that snapshot.
- Validate the event against the supplied snapshot before storing/promoting it.
- Preserve the event as an outlier or defer promotion if the supplied snapshot
  does not contain enough dependencies.

This is the mode needed to fix `TestFederatedEventRelationships`. The correct
solution is not to accept the fetched extremity against arbitrary current room
state.

### `Backfill`

Used by historical backfill.

- Store fetched events as outliers first.
- Avoid synchronous full-state federation requests in the pagination path.
- Promote only after predecessor/auth dependencies and state are available.

### `Repair`

Used by the monitor and background healing workers.

- Permit bounded retries and `/state_ids` fetches.
- Apply per-room/per-server rate limits and deduplication.
- Never let repair traffic block normal federation intake.

## Implementation Plan

1. Introduce an explicit mode/options parameter for the internal ingestion path;
   keep the public `handle_incoming_pdu` wrapper temporarily as the strict live
   timeline default.
2. Split validation/storage from timeline promotion so bootstrap and backfill
   can persist outliers without entering full timeline state resolution.
3. Pass the `send_join` state snapshot into the join-bootstrap path.
4. Change `join.rs` extremity handling to use join-bootstrap mode on first join.
5. Keep rejoin/live extremity handling on the full path only when the local room
   state cannot prove the event's context.
6. Make `/state_ids` calls observable with a reason/mode field in logs and
   metrics.
7. Add tests covering:
    - no `/state_ids` request during a first join with complete `send_join` state;
    - `/state_ids` still used for a live event with missing state;
    - incomplete join state defers or rejects promotion rather than using current
      state incorrectly;
    - backfill does not synchronously block on `/state_ids`.

## Temporary Diagnostic Rule

Do not fix this by adding a broad `skip_state_fetch` flag or by changing every
`handle_incoming_pdu(..., true, ...)` call to `false`. Those changes can hide the
network symptom while weakening auth/state correctness and producing divergent
rooms.

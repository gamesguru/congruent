# MSC4521 outbound reconciliation

## Current boundary

Congruent serves `room_digest`, `room_digest/.../strata`, and `room_diff`.
`handle_incoming_pdu` already fetches missing predecessors inline through
`fetch_prev` and `/get_missing_events`. There is no outbound MSC4521 client,
reconciliation queue, or worker. A missing predecessor is evidence of a DAG gap,
but does not prove the complete event sets differ. Conversely, two rooms can
have different event sets without an incoming PDU exposing the difference.

## Triggers

1. After an incoming PDU reveals missing predecessors, enqueue a comparison with
   the PDU's origin **after** the existing inline recovery completes. Do not add
   network work to the transaction response or hold the room's federation mutex
   while reconciling.
2. When federation communication with a previously unreachable peer succeeds,
   enqueue shared rooms that were active during the outage. Bound the number of
   rooms per recovery and coalesce repeated triggers.
3. Periodically compare recently active shared rooms, with a low rate and
   jitter. This catches silent divergence when no PDU arrives and no connection
   transition is observable.
4. Provide an explicit room/peer trigger for diagnostics and Complement. The
   mock's `claim()` changes only its own in-memory responses; it sends no PDU or
   EDU, so the server cannot discover it without this trigger or polling.

All triggers enter one `(room_id, peer)` queue. Admit only joined rooms and
peers with a current room member; check ACLs before enqueue and again before
network I/O. Feature-gate the queue with `msc4521_enabled`.

## Queue and backoff

Keep one in-flight exchange per `(room_id, peer)`, a global concurrency limit,
and a per-peer limit. Merge duplicate triggers; retain a dirty bit so an event
arriving during an exchange schedules one subsequent comparison. Apply
exponential backoff with jitter to transport failures and unsupported peers;
cache a 404/unknown-feature verdict for longer than a transient failure. Do not
retry malformed peer responses immediately. A successful equal digest clears the
dirty bit and backoff. Put hard limits on requests, bytes, fetched events, and
wall time per round. Queue entries must expire or be evicted so a dead peer
cannot accumulate unbounded work.

## First outbound exchange

Fetch the peer's digest and frame. Compare with a fresh local snapshot. If
equal, stop. If different, negotiate the frame and obtain strata, then use the
existing reconciliation library to build bounded `room_diff` sketches. Treat
returned IDs as untrusted hints: sample before a bulk fetch, enforce per-peer
fetch limits, verify hashes/signatures/auth/DAG context through the existing
event pipeline, and remember rejected IDs for a bounded period. Never insert an
event merely because a sketch decoded its short ID. After accepted events
advance the timeline, take a new snapshot and compare again within the round
budget.

## Implementation order

1. Add the bounded, coalescing queue and explicit trigger, with no outbound
   fetches yet. Instrument trigger, enqueue, skip, and outcome reasons.
2. Add digest comparison and capability/backoff handling. An equal-set check
   should complete without sketch or event fetch work.
3. Add strata and `room_diff` rounds, followed by bounded, validated event
   fetches. Wire the incoming-PDU and reconnect triggers only after the worker
   has limits and peer checks.
4. Make Complement's adversarial tests explicitly trigger an exchange after
   `claim()`. Keep a separate test for the passive triggers. This avoids waiting
   for an unspecified polling interval in every adversarial case.

No normal read receipt or device-list update should be used as a trigger: those
EDUs describe user activity and device keys, respectively. A dedicated digest
EDU could be an optional acceleration hint later, but it cannot be the only
recovery path because it can be lost during an outage.

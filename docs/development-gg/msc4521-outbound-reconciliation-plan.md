# MSC4521 outbound reconciliation

## Current boundary

Congruent serves `room_digest`, `room_digest/.../strata`, and `room_diff`, and
runs the outbound side: a bounded `(room_id, peer)` queue, a worker, and an
exchange that compares digests, runs strata and sketch rounds, and fetches the
offered events through the existing event pipeline. Explicit, activity
(local writes and inbound transactions carrying PDUs or room-scoped EDUs), and
periodic-sweep triggers feed the queue; all are gated on `msc4521_enabled`.
Peer `/version` advertisement is checked before reconciliation requests.
Reconnect-driven triggers and extremity transfer (`ExtremityDiff`) remain
planned. The sections below record the design and the remaining work.

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

## Planned follow-up

1. Wire incoming-gap and reconnect triggers into the existing bounded queue.
2. Add extremity transfer for `ExtremityDiff`, with the same admission checks
   and per-round limits as sketch reconciliation.
3. Keep Complement's adversarial tests explicit about the activity that starts
   an exchange, while retaining coverage for passive triggers.

Room-scoped typing and receipt EDUs currently act only as bounded activity
hints; they do not carry reconciliation data. They may schedule the same
out-of-band exchange as a PDU or local write, subject to the room-wide and
per-peer activity intervals. A dedicated digest EDU could reduce that extra
round trip later, but it cannot be the only recovery path because it can be
lost during an outage.

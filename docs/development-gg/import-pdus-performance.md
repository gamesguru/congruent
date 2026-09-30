# import-pdus Performance Analysis

## Overview

`import-pdus` processes events **sequentially**, one at a time through the full
federation pipeline. For large rooms (100k+ events), this can take minutes to
hours depending on the mode used.

## Architecture

The import pipeline in [`imports.rs`](../src/admin/yolo/imports.rs) reads a JSONL
file line-by-line:

```
for each line in JSONL file:
    parse_and_clean_pdu()
    if --skip-auth:
        force_insert_pdu()          # fast path — direct DB insert
    else:
        handle_outlier_pdu()        # slow path — full pipeline
        promote_outlier()           # move from outlier to timeline
```

## Per-Event Cost Breakdown (default mode)

| Step                   | Cost/event | Notes                                                       |
| ---------------------- | ---------- | ----------------------------------------------------------- |
| Signature verification | ~1–5ms     | Ed25519/RSA crypto per event                                |
| Auth event lookups     | ~0.5–2ms   | 3–5 DB reads (timeline + outlier stores)                    |
| Auth chain fetch       | 50–500ms   | Only if missing → HTTP `/event_auth` + recursive processing |
| `auth_check`           | ~0.1ms     | State resolution auth against auth events                   |
| `promote_outlier`      | ~0.2ms     | Mutex + DB insert + search indexing                         |

### Throughput estimates

| Mode                    | Cost/event | 100k events  |
| ----------------------- | ---------- | ------------ |
| Default (full pipeline) | ~3–8ms     | 5–13 minutes |
| `--skip-sig-verify`     | ~1–3ms     | 2–5 minutes  |
| `--skip-auth`           | ~0.1ms     | ~10 seconds  |

## Why Each Mode Is Slower

### Default mode

`handle_outlier_pdu` runs the complete federation verification pipeline:

1. **Signature verification** — calls `verify_event()` which does Ed25519
   verification against the signing server's keys. Keys may need to be fetched
   over federation if not cached.

2. **Auth event resolution** — for each of the event's 3–5 `auth_events`:
    - Check timeline DB (`get_pdu_in_room`)
    - Check outlier store (`get_pdu_outlier`)
    - If missing: fetch via `/event_auth` federation endpoint, topologically
      sort the response, and recursively call `handle_outlier_pdu` for each
      fetched auth event

3. **Auth check** — `event_auth::auth_check()` validates the event against its
   auth events (power levels, membership, etc.)

4. **Persist as outlier** — write to outlier store

5. **Promote to timeline** — `promote_outlier()` moves from outlier to timeline
   DB with a backfill PDU count, indexes message bodies for search

### `--skip-sig-verify` mode

Skips step 1 (signature verification) but still runs the full auth pipeline
(steps 2–5). The crypto savings (~1–5ms/event) help but the DB lookups and
auth_check still dominate.

### `--skip-auth` mode

Uses `force_insert_pdu()` which bypasses everything: no signatures, no auth
event lookups, no auth_check. Direct DB insert. ~0.1ms/event.

## Potential Optimizations (not yet implemented)

### 1. Batch parallelism

Process N events concurrently using a bounded semaphore. Signature verification
is CPU-bound and embarrassingly parallel. Auth event lookups can be pipelined
since imported events are processed in order.

**Constraint:** Events in the same room that depend on each other (via
`auth_events` or `prev_events`) must be processed in DAG order.

### 2. In-memory auth event cache

During a bulk import, the auth events for event N+1 are almost always events
that were just inserted for events 1..N. Maintaining a `HashMap<EventId, PduEvent>`
during the import would eliminate most DB reads.

This could reduce auth event lookup cost from ~0.5–2ms to ~0.01ms per event.

### 3. Batch key lookup

Events from the same server share signing keys. Instead of looking up keys per
event, fetch the server's signing keys once and verify all events from that
server in a batch.

### 4. Topological pre-sort

If the JSONL file is sorted in DAG order (parents before children), auth events
are guaranteed to be available locally when processing each event. This
eliminates all federation fetches during import.

The `parse_and_clean_pdu` step could be extended to do a topological sort of the
input before processing, or the export tool could guarantee DAG order.

## Relationship to State Resolution

`import-pdus` does NOT run holistic state resolution. Each event is individually
auth-checked against its own `auth_events`. This is fundamentally different from
the (now-removed) `reconcile_fork_states` in `reorder-timeline`, which ran V2.0
state_res across all fork tip states holistically.

After import completes, `recalculate_extremities` updates the DAG tips but does
not run state_res. The user is instructed to run `force-set-room-state` to
finalize the room state.

# Room Healing & DAG Recovery Guide

## Symptoms & Diagnosis

| Symptom                                          | Cause                                             | Fix                              |
| ------------------------------------------------ | ------------------------------------------------- | -------------------------------- |
| Messages out of order / fragmented segments      | Timeline PDUs stored in wrong order               | `reorder-timeline`               |
| Missing chunks of history                        | Events never fetched from federation              | `get-remote-dag` → `rescue-room` |
| Events stuck as outliers (fetched but invisible) | Failed state-res or soft-fail during ingest       | `rescue-room`                    |
| Multiple forward extremities (forked DAG)        | Normal federation concurrency; stale if excessive | `check-rooms --fix`              |
| Broken room state (wrong members, permissions)   | State corruption or missed state events           | `rescue-room --heal-from`        |
| Jumbled `unsigned.prev_content`                  | Missing `replaces_state` metadata                 | `repair-unsigned`                |

## Diagnostic Commands

```
# Show rooms with multiple extremities
yolo view-extremities --all --verbose

# Full health scan (dry run)
yolo check-rooms --problems-only

# Full health scan + auto-fix extremity drift & membership caches
yolo check-rooms --fix
```

## Recovery Pipelines

### Timeline Out of Order (most common)

Nheko/clients show messages jumbled, fragmented segments, wrong chronological order.

```bash
# Single room
yolo reorder-timeline <room_id>

# All rooms (deploy-wide heal)
yolo reorder-timeline --all

# Only last N events (fast path)
yolo reorder-timeline <room_id> --tail 500
```

After reorder, clients must clear cache and re-sync.

### Missing Event Chunks (gaps in history)

Room has large holes where events were never fetched from federation.

```bash
# Step 1: Backfill missing events from federation into outlier store
yolo get-remote-dag <room_id>

# Step 2: Promote outliers to timeline + reorder
yolo rescue-room <room_id> --force --reorder
```

### Full Room Recovery (nuclear option)

Room is severely broken — missing events, wrong state, corrupted extremities.

```bash
# Step 1: Backfill from federation
yolo get-remote-dag <room_id>

# Step 2: Rescue + reorder + heal state from trusted server
yolo rescue-room <room_id> --force --reorder --heal-from matrix.org
```

### Outliers Only (no missing events, just stuck)

Events were fetched but failed state-res and got stuck as outliers.

```bash
# With state resolution (safe, skips superseded events)
yolo rescue-room <room_id>

# Force-promote all outliers (bypass state-res)
yolo rescue-room <room_id> --force

# Rescue a single event
yolo rescue-pdu <event_id> --force
```

### Extremity Drift

Stale/phantom forward extremities that should have been superseded.

```bash
yolo check-rooms --fix
```

For genuinely forked extremities (concurrent senders), sending any message in the
room naturally merges all tips — your event's `prev_events` references all current
extremities.

## Command Reference

### `reorder-timeline`

Re-sorts all timeline PDUs by `origin_server_ts`. Does NOT fetch missing events or
fix extremities. Optionally rebuilds state snapshots (unless `--no-compute-state`).

### `rescue-room`

Promotes outliers into the timeline via topological sort + `upgrade_outlier_to_timeline_pdu`.

| Flag                   | Effect                                                                    |
| ---------------------- | ------------------------------------------------------------------------- |
| `--force`              | Bypass state-res supersession checks; force-promote all outliers          |
| `--nuclear`            | (reserved)                                                                |
| `--reorder`            | Run `reorder-timeline` after rescue                                       |
| `--heal-from <server>` | After rescue, force-set room state from remote server (implies `--force`) |
| `--timeline-limit <N>` | Also re-process last N timeline PDUs                                      |

### `get-remote-dag`

Walks the remote DAG via federation (`/get_missing_events`, `/backfill`, `/event`)
using the `ServerPool` multi-armed bandit for server selection. Fetched events are
stored as outliers — must run `rescue-room` after to promote them.

### `check-rooms`

Scans all rooms for: corrupt IDs, missing create events, orphaned memberships,
extremity drift, membership cache drift. With `--fix`, auto-repairs drift issues.

### `repair-unsigned`

Rebuilds `unsigned.prev_content` for state events by looking up prior state from
snapshots or `replaces_state` references.

---

## Architecture: Current State & Refactoring Roadmap

### What Uses `ServerPool` (refactored)

| Component        | File                    | Status                                |
| ---------------- | ----------------------- | ------------------------------------- |
| `get-remote-dag` | `src/admin/yolo/dag.rs` | ✅ Uses `ServerPool` with MAB scoring |

### Code Silos (not yet refactored)

These components do their own ad-hoc server selection and retry logic instead of
using the shared `ServerPool` abstraction:

| Component                   | File                                                           | What It Does                         |
| --------------------------- | -------------------------------------------------------------- | ------------------------------------ |
| `rescue-room`               | `src/admin/yolo/heal.rs`                                       | Promotes outliers, heals state       |
| `reorder-timeline`          | `src/admin/yolo/timeline.rs`                                   | Re-sorts PDUs by timestamp           |
| `repair-unsigned`           | `src/admin/yolo/timeline.rs`                                   | Fixes unsigned metadata              |
| `fetch_prev`                | `src/service/rooms/event_handler/fetch_prev.rs`                | Fetches prev_events during ingest    |
| `fetch_state`               | `src/service/rooms/event_handler/fetch_state.rs`               | Fetches state for auth during ingest |
| `fetch_and_handle_outliers` | `src/service/rooms/event_handler/fetch_and_handle_outliers.rs` | Recursive outlier resolution         |

### Planned Shared Helpers (not yet implemented)

- **`compute_dag_stats()`** — Shared DAG statistics (depth, breadth, missing events,
  extremity count) for use in `get-room-dag`, `get-remote-dag`, and diagnostics.
- **`ServerPool` migration** — Move `fetch_prev.rs`, `fetch_state.rs`, and
  `fetch_and_handle_outliers.rs` to use the abstract `ServerPool` instead of
  hand-rolled server iteration.
- **Shared outlier promotion** — Extract the topo-sort + promote loop from
  `rescue-room` into a reusable service method.

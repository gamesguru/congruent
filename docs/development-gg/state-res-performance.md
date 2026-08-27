# State Resolution Performance & Congestion Remediation

## Problem Statement

On resource-constrained environments (2–4 GB RAM, 2–4 CPU threads), federated
state resolution can monopolize the executor and saturate the DB pool, causing:

- Federation transactions blocking for 600+ seconds
- DB commands hanging (admin queries queued behind long-running auth chain reads)
- Presence EDU processing starving client request handling
- Nheko phantom notifications from unstable shortstatehash recalculations

### Root Cause

The `event_rejected` callback in `resolve_state.rs` was bypassed (always
returned `false`), forcing re-evaluation of every rejected/soft-failed event in
auth chains of 8,000+ events. Each re-evaluation performs full signature
verification and recursive auth chain walks, triggering thousands of DB reads
per state resolution call.

## Configuration Options

All options default to `false` for safety. **Enable them for performance:**

```toml
# Skip hard-rejected events (auth failures, signature failures)
state_res_ignore_rejected = true

# Skip soft-failed events (failed current-state auth, persisted as outliers)
# This is the most impactful setting — prevents 600s+ blocking on rooms with
# thousands of soft-failed auth events.
state_res_ignore_soft_failed = true

# Skip admin-rejected events (manually rejected via admin commands)
state_res_ignore_admin_rejected = true

# Disable the background DAG healer (default: false)
# The healer fires federation requests for missing events discovered during
# state resolution. On constrained boxes this causes excessive CPU/IO load.
allow_dag_healer = false
```

## Concurrency Scaling

Federation concurrency limits scale dynamically via `Server::concurrency_scaled()`:

```rust
let num_workers = self.services.server.config.worker_threads
    .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |n| n.get()));

// multiplier * (num_workers / 2), minimum 2
pub fn concurrency_scaled(&self, multiplier: usize) -> usize {
    let base = (num_workers / 2).max(1);
    (multiplier * base).max(2)
}
```

Applied to:

- **Outlier fetching**: `concurrency_scaled(2)` (was hardcoded 32)
- **Pre-fetch fan-out**: `concurrency_scaled(1)` per server
- **fetch_prev**: `concurrency_scaled(2)` (was hardcoded 16)
- **DAG healer**: 8 fallback servers (was 32), 100ms delay between requests
- **Monitor sweeps**: 4h intervals with yield points

## Memoization: `iterative_auth_check`

The `m.room.create` event never changes for a room, but `iterative_auth_check`
previously re-discovered it from `auth_state` on every loop iteration. For a
52K room with 8,645-event auth chains, this caused millions of redundant
BTreeMap lookups.

**Fix**: `cached_create_event: Option<E>` is populated on first discovery and
reused for all subsequent iterations. Function-scoped, no leak risk.

## Architecture Comparison: Tuwunel

Tuwunel moved `state_res` from `src/core/matrix/state_res/` to
`src/service/rooms/state_res/` and significantly refactored:

### Module Split

| **Continuwuity**              | **Tuwunel**                                                                   |
| ----------------------------- | ----------------------------------------------------------------------------- |
| `mod.rs` (1770 lines)         | `mod.rs` (76) + `resolve.rs` + 6 `resolve/*.rs` files + `topological_sort.rs` |
| `event_auth.rs` (1914 lines)  | `event_auth.rs` (707) + `auth_types.rs` + `room_member.rs`                    |
| `power_levels.rs` (256 lines) | `events/*.rs` (5 files, 930 lines)                                            |
| `test_utils.rs` (572 lines)   | `test_utils.rs` (921) + 4 test modules (~5k lines)                            |

### Key Architectural Differences

1. **Rejection filtering**: Tuwunel uses `auth_event.rejected()` — a trait
   method on the `Event` type — instead of an `event_rejected` callback closure.
   No DB call per rejection check; the flag is already on the deserialized event.

2. **Create event handling**: Tuwunel derives the create event ID from
   `event.room_id().as_event_id()` for v12 rooms (room ID == create event ID).
   No special-casing, no memoization needed.

3. **SmallVec<[_; 4]>** for auth events: stack-allocated for the common case
   (≤4 auth events), avoiding heap allocation per event.

4. **Binary search** over sorted SmallVec for `fetch_state`, instead of
   rebuilding a `StateMap<E>` BTreeMap per iteration.

5. **`try_fold`** streaming: events are processed lazily via `try_fold` instead
   of collecting into a `Vec` upfront.

### Migration Path

Our memoization and config-gated rejection filtering bridge the gap. For full
convergence with tuwunel's approach, the long-term plan is:

1. Add a `rejected()` method to the `Event` trait (populated at deserialization)
2. Replace the `event_rejected` closure with inline trait checks
3. Adopt `SmallVec` + binary search for auth event collection
4. Split `mod.rs` into submodules matching tuwunel's layout

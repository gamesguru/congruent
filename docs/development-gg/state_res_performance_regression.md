# State Resolution $O(N \cdot E)$ Performance Regression

## Overview

A severe performance regression was identified in the Matrix state resolution v2.1 algorithm inside `continuwuity`, causing deep auth chain evaluations (like those triggered by `test_busted_dag_resolution` and large conflicted sets) to stall for well over 60 seconds.

The core issue stems from an accidental $O(N \cdot E)$ algorithmic complexity introduced during the resolution phase when attempting to authorize events and traverse their auth chains.

## Bottlenecks Discovered

### 1. The `get_power_level_for_sender` Strict Scan

In earlier commits (e.g., around `48d682de5`), the algorithm was modified to alter how privileged room creators in v12 rooms were handled. However, this left behind an extremely slow lookup path.

When validating events during state resolution, the algorithm iterates through the `auth_events` of every event to find `m.room.power_levels` and `m.room.create`. Because this occurs inside the core topological sort loop, `fetch_event()` was being sequentially `await`ed on **every single auth event**, for **every single event in the conflicted set**. For a room with 20,000 conflicted events, this resulted in roughly ~100,000 sequential cache/database lookups.

**Fix:** Introduced an aggressive `FAST PATH` using `parsed_pl_cache` to short-circuit the scan if we've already parsed the power level event in memory, dropping the lookups down to a fraction of a percent.

### 2. The `mainline_sort` Type Checking

A secondary, nearly identical bottleneck existed in `mainline_sort`. When finding the 1-hop power level event to attach a node to the mainline, the code iterated over `event.auth_events()` and blindly called `fetch_event(aid).await` simply to check if `is_type_and_key(&aev, &TimelineEventType::RoomPowerLevels, "")`.

**Fix:** Injected an `is_pl_cache` directly into the `mainline_sort` loop to remember the power-level status of those shared auth events, preventing ~100,000 redundant event deserializations.

### 3. The `mainline_sort` Depth Memoization Bug

While debugging the performance stalls, a critical topological depth tracking bug was discovered in `mainline_sort`:

```rust
// BUG: Assigns the exact same depth to all nodes in the path
if let Some(depth) = found_depth {
    for id in path {
        mainline_depth.insert(id, depth);
    }
}
```

This violates the Matrix specification (§6.6.3.3), which mandates that a power level event's position is its distance from the resolved power level. By giving the entire path the _same_ depth, events were falsely tying in the mainline sort and falling back to timestamp ordering, breaking the deterministic state resolution semantics.

**Fix:** Restored the proper `into_iter().rev()` traversal and `.saturating_add(1)` increment so path depths are memoized correctly.

## Conclusion

These compounding inefficiencies triggered extreme stalls in DAG resolution. By enforcing strict early-exits and aggressive memoization, the algorithm safely sidesteps the $O(N \cdot E)$ tarpit without altering the deterministic outputs of state resolution.

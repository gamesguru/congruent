# Resolving the `get_forward_extremities` UAF and Insights into State Resolution

This document summarizes a critical bug fix regarding how `conduwuit` handled RocksDB stream cursors, which previously led to catastrophic Use-After-Free (UAF) memory corruption causing cross-room DAG bleeding. It also details insights gained during testing of Matrix State Resolution Algorithms (V2.1 vs V2.2).

## The `get_forward_extremities` Use-After-Free Bug

### The Problem

In `conduwuit`'s database implementation, the standard `futures::Stream` API was used to iterate over values fetched from RocksDB. However, because standard Rust `Stream` traits lack a Lending mechanism (where items yielded borrow from the stream itself), the author utilized an `unsafe` block (`slice_longevity`) to artificially extend the lifetimes of references (`&Slice`) pointing to the RocksDB cursor's internal transient buffer.

The `get_forward_extremities` function returned an Iterator of `&EventId` references originating from this transient buffer. Downstream callers would then chain `.map(ToOwned::to_owned)` to lazily convert these references into owned values.

Because `get_forward_extremities` yielded references rather than owned values, and the `ToOwned::to_owned` step happened asynchronously out in the caller's scope, the RocksDB cursor could advance or be dropped _before_ the caller actually cloned the data. This allowed callers to read dangling pointers, leading to memory corruption. When generating `prev_events` under high load, this corruption occasionally caused Room A to mistakenly adopt Room B's leaf nodes, grafting the server's global timeline together and causing massive graph conflicts.

### The Fix

To eliminate this window of vulnerability, we refactored `get_forward_extremities` (and its entire call chain) to eagerly allocate and return fully owned `OwnedEventId` values natively.

By removing the `&EventId` references at the database boundary, the `ToOwned::to_owned` cloning operation now occurs _inside_ the safe database boundary before the cursor ever moves. All downstream callers (like `append_pdu`, `set_forward_extremities`, etc.) were updated to accept iterators of `OwnedEventId` instead of `&EventId`. This completely neutralized the danger of the `slice_longevity` hack for forward extremities.

## State Resolution V2.1 vs V2.2: Edge Cases & Flaws

In the process of debugging the DAG bleeding, we tested the boundaries of Matrix Room Version State Resolution algorithms.

### 1. Power Level Tie-Breakers (Kahn's Sort)

When resolving concurrent conflicting events, State Resolution uses Kahn's algorithm for topological sorting. If two events tie on all metrics (same depth, same timestamp), they are sorted by `event_id`.

- **Power Events are Special:** If two conflicting Power Level events are sorted, whichever is evaluated _first_ immediately sets the authorization rules for the rest of the loop.
- **Mutual Destruction in V2.1:** If two moderators (PL 50) concurrently ban each other, both bans are accepted. Because V2.1 strictly isolates auth checks to an event's local auth chain, neither ban's auth chain contains the other's ban. Therefore, both bans pass `iterative_auth_ok` and are inserted into the final resolved state.

### 2. The "Phantom State" Flaw in V2.1

V2.1's strict isolation of auth chains introduces a severe vulnerability. If an Admin bans Eve on Fork A, but Eve concurrently submits a malicious state change on Fork B, Eve's state change will **not** see the ban during resolution. Because her event's local auth chain only includes the state prior to the ban, V2.1 evaluates her event against the old state. Eve's malicious event is successfully accepted into the resolved state despite her being banned on the mainline.

### 3. V2.2 (MSC4297 / MSC4242) and Deep BFS

V2.2 attempts to fix V2.1's flaws by searching for authorization events across the entire graph using Breadth-First Search (BFS) (`auth_chain_distance`).

- **Performance:** Iterative BFS can efficiently traverse massive graphs (e.g., 1,000,000 hops deep) without stack overflows.
- **Security Regressions:** V2.2's BFS is vulnerable to Demotion Evasion. If an Admin demotes Eve, Eve can submit a malicious state event and simply _omit_ the demotion from her 1-hop auth chain. V2.2's BFS will crawl backward through her `eve_join` event until it finds the ancient Power Level event where she was an Admin. Because V2.2 relies purely on DAG connectivity rather than state consensus overlays, it incorrectly authorizes the attack, allowing Eve to bypass her demotion.

V2.1, despite its flaws, securely overlays the consensus Power Levels during validation, meaning it rightfully rejects Demotion Evasion attacks.

## 4. The "V3" Solution & Invite Locks

The original Matrix State Resolution algorithms oscillated between two extremes:

- **V2.0 (The Shotgun):** Supplemented _all_ state events (including `m.room.join_rules`) into the auth chain overlay during resolution. This caused global "Invite Locks", where an Admin changing the room to Invite Only accidentally overwrote the local "Public" auth chain of historical joins, permanently locking out historical users.
- **V2.1 (The Scalpel):** To fix Invite Locks, MSC4297 strictly isolated the supplemental merge to _only_ `m.room.power_levels`. While this successfully protected `join_rules`, it accidentally isolated Bans and Kicks, creating the "Concurrent Ban Evasion" flaw (Point 2 above).

**The V3 Sweet Spot:** The mathematically flawless solution is to expand the V2.1 Supplemental Merge to include **Authoritative Memberships** (Bans and Kicks). By supplementing `m.room.power_levels` AND `m.room.member` (when `membership == ban|leave` and `sender != state_key`), the consensus Ban successfully overlays onto concurrent malicious events, forcing `iterative_auth_check` to rightfully reject them. Meanwhile, `m.room.join_rules` remains cleanly isolated, ensuring the DAG can heal from splits without suffering Invite Locks.

### The `continuwuity` Vulnerability

Fascinatingly, the `continuwuity` codebase intuitively understood the V3 solution—its `is_power_event()` function correctly included Bans and Kicks. However, it accidentally also included `TimelineEventType::RoomJoinRules`. Because `RoomJoinRules` were being supplemented into the auth overlay, `continuwuity` was vulnerable to the exact V2.0 Invite Lock anomaly that MSC4297 was designed to prevent!

We patched this by cleanly removing `TimelineEventType::RoomJoinRules` from `is_power_event()`, securing `continuwuity` against both Concurrent Ban Evasions and Invite Locks.

## Summary

The cross-room DAG bleeding on live servers was a local UAF memory corruption bug, not a protocol-level graph theory failure. The refactor to `OwnedEventId` natively enforces memory safety across the codebase, while our discoveries in State Resolution algorithms harden the network against future split-brain exploits.

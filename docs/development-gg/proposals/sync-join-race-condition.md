# Sync/Join Race Condition: Missing Room Until Refresh

## Problem

When a user joins a room, the room intermittently fails to appear in the
Cinny (or other client) UI until a full page refresh. The join succeeds
server-side but the client never learns about it through `/sync`.

## Root Cause

The failure window is between the join write committing to RocksDB and
the next `/sync` long-poll cycle reading it:

```
Timeline:
  T0: Client sends /join
  T1: /sync long-poll is in-flight (blocking, waiting for new events)
  T2: Server writes join PDU to RocksDB WAL
  T3: /sync returns (either timeout or unrelated event wakes it)
  T4: Client sends next /sync with `since=T3_token`
  T5: Join PDU at T2 is BEHIND the T3 token → never returned
```

The `since` token advances past the join event's PDU count, so it falls
into a gap between two sync responses. The client never sees the join
event in any `/sync` response and doesn't know the room exists.

### Why It's Intermittent

- **Works** when the `/sync` long-poll hasn't returned yet and the join
  event wakes the watcher, causing it to be included in the current
  response with `limited: true` and full state.
- **Fails** when the `/sync` returns (timeout or other event) just
  before the join is visible, advancing the token past it.

### Contributing Factors

1. **RocksDB write visibility** — WAL writes are not instantly visible
   to concurrent reads. A sync handler may snapshot the DB before the
   join's WAL entry is flushed.
2. **Non-atomic membership flow** — The join involves multiple writes
   (PDU append, state update, membership cache, joined count) that are
   not batched. A sync read can observe partial state.
3. **Watcher race** — The DB watcher that wakes `/sync` may fire before
   the PDU is readable, or may not fire at all if the write lands in a
   batch that doesn't trigger notification.

## Observed Behavior

- **Working case**: Sync returns the room in `rooms.join` with
  `"limited": true`, full `state` block, and the join event in
  `timeline.events`. Client renders the room immediately.
- **Failing case**: Sync returns without the room. Next sync's `since`
  token is past the join. Room never appears. Page refresh triggers a
  fresh initial sync which discovers the room.

## Solution: PR #13 (RocksDB Transactional Wrappers)

[PR #13](https://github.com/continuwuity/continuwuity/pull/13)
(`guru/experiment/rocksdb-transactional-wrappers`) directly addresses
this with three mechanisms:

### 1. Atomic Writes via `Database::transaction`

Joins, knocks, and leaves are wrapped in a single RocksDB `WriteBatch`.
All writes (PDU, state, membership cache, counts) commit atomically —
no partial state is ever visible to sync readers.

### 2. In-Flight Isolation via `current_count_in_flight()`

`/sync` reads are bounded by a snapshot count taken _before_ any
in-flight transaction. If a join is being written, sync won't read past
the pre-join watermark. The join will appear in the _next_ sync cycle
after the transaction commits.

### 3. Recently-Joined Cache

A `recently_joined` cache ensures new rooms appear in sync even if the
RocksDB prefix iterator hasn't caught up. This is the safety net for
the race window — if the DB snapshot misses the join, the cache catches
it.

### 4. Watcher Fires on Commit Only

DB watcher callbacks only fire after the transaction commits, ensuring
that when `/sync` is woken up, the data is actually readable.

## Workaround (Current)

Until PR #13 lands, the only workaround is a client-side page refresh
(forces initial sync) or waiting for the next sync cycle that happens
to include the room's events.

## Related Issues

- Fixes #1142 (empty timelines after join)
- Addresses #779 (sync consistency)
- Related to MSC3030 timestamp index write-visibility race

# State Resolution Membership Bugs

Three interconnected bugs caused federated room membership state to silently
inflate, adding "ghost" joined users to the state snapshot while the
membership cache remained correct.

Discovered May 2026 via `yolo audit-membership` diagnostics.

## Command Quick Reference

There are many admin commands across `yolo` and `debug`. Here's what each
does, grouped by workflow.

### Diagnostics (read-only, safe to run anytime)

| Command                                       | What it does                                         |
| --------------------------------------------- | ---------------------------------------------------- |
| `yolo audit-membership !room_id --server srv` | Full audit: timeline vs state vs cache vs remote     |
| `yolo compare-room-state !room_id srv1 srv2`  | Compare member counts/extremities across servers     |
| `yolo view-extremities !room_id`              | Show forward extremities (DAG tips)                  |
| `yolo list-outliers !room_id`                 | List all outlier PDUs in a room                      |
| `yolo get-room-dag !room_id`                  | Export local DAG as PDU list                         |
| `yolo get-remote-dag !room_id --server srv`   | Fetch remote DAG via federation backfill             |
| `yolo dag-merge-base !room_id`                | Find merge base between local DAG branches           |
| `debug get-pdu $event_id`                     | Inspect single event (shows timeline/outlier status) |
| `debug get-room-state !room_id`               | Dump full room state snapshot                        |
| `debug first-pdu-in-room !room_id`            | Show earliest timeline event                         |
| `debug latest-pdu-in-room !room_id`           | Show latest timeline event                           |

### Healing (writes data — use carefully)

| Command                                   | What it does                                           | When to use                             |
| ----------------------------------------- | ------------------------------------------------------ | --------------------------------------- |
| `yolo force-set-state !room_id srv1 srv2` | Fetch state from remotes, **overwrite** local snapshot | State snapshot is wrong (ghost members) |
| `yolo heal-room !room_id`                 | Combined: force-set + rescue + reorder                 | Full room repair (does everything)      |

### Outlier Management (writes data)

| Command                             | What it does                                    | When to use                     |
| ----------------------------------- | ----------------------------------------------- | ------------------------------- |
| `yolo rescue-pdu $event_id`         | Upgrade one outlier to timeline (with auth)     | Single stuck outlier            |
| `yolo rescue-room !room_id`         | Rescue all outliers in a room (with auth)       | Many outliers need promoting    |
| `yolo promote-outliers !room_id`    | Force-insert outliers to timeline (**no auth**) | Bootstrapping from send_join    |
| `yolo purge-outlier $event_id`      | Delete one outlier                              | Remove bad outlier              |
| `yolo purge-outliers !room_id`      | Delete outliers that already exist in timeline  | Clean up historical STUCK state |
| `yolo purge-timeline-pdu $event_id` | Delete from timeline AND outlier tables         | Remove bad event entirely       |

### Timeline Repair (writes data)

| Command                          | What it does                        | When to use                       |
| -------------------------------- | ----------------------------------- | --------------------------------- |
| `yolo reorder-timeline !room_id` | Re-sort by `origin_server_ts`       | Events out of chronological order |
| `yolo repair-unsigned !room_id`  | Rebuild `unsigned` fields           | Corrupted unsigned metadata       |
| `yolo resend-receipts !room_id`  | Re-send read receipts to federation | Missing receipts on remote        |

### Federation (network requests)

| Command                                      | What it does                             |
| -------------------------------------------- | ---------------------------------------- |
| `yolo fetch-pdu $event_id !room_id`          | Fetch specific PDU from remote + persist |
| `yolo import-outliers !room_id --input file` | Import PDUs from JSONL as outliers       |
| `yolo import-pdus !room_id --input file`     | Import PDUs from JSONL to timeline       |
| `yolo federation-request srv /_matrix/...`   | Raw federation API call                  |

### Typical Workflows

**"My room has ghost members" (state inflation)**:

```
1. yolo audit-membership !room_id --server trusted.org    # diagnose
2. yolo force-set-state !room_id trusted1.org trusted2.org # heal
3. yolo audit-membership !room_id --server trusted.org    # verify
```

**"Events are out of order"**:

```
1. yolo reorder-timeline !room_id
```

**"Room is totally broken" (full repair)**:

```
1. yolo heal-room !room_id
```

**"Historical outliers stuck in both tables"**:

```
1. yolo purge-outliers !room_id    # safe — only removes duplicates
```

## Summary

| Bug                                        | File                 | Severity | Status    |
| ------------------------------------------ | -------------------- | -------- | --------- |
| Hotel California state-res regression      | `resolve_state.rs`   | HIGH     | Fixed     |
| Outlier table leak on federation promotion | `timeline/append.rs` | MEDIUM   | Fixed     |
| force_state silent PDU lookup failure      | `state/mod.rs`       | LOW      | Fixed     |
| Membership cache count drift               | `state_cache/update` | MEDIUM   | Diagnosed |
| audit-membership false "OK"                | `yolo/commands.rs`   | LOW      | TODO      |
| Rejected events in state-res (root cause)  | `state_res/mod.rs`   | HIGH     | Fixed     |
| MSC4297 version gate (smoking gun)         | `state_res/mod.rs`   | CRITICAL | Fixed     |
| repair_unsigned OOM risk                   | `yolo/commands.rs`   | MEDIUM   | TODO      |
| N+1 DB queries in event_rejected           | `state_res/mod.rs`   | LOW      | TODO      |
| force_state PDU-free cache update          | `state/mod.rs`       | MEDIUM   | TODO      |

## Bug 1: Hotel California State-Res Regression

### What happens

When state-res merges a fork branch against the current room state and both
branches contain a membership event for the same user (e.g., `join` on the
fork branch, `leave` on the current branch), state-res v2 can incorrectly
pick the older `join` event over the newer `leave`.

### Why it happens (Matrix state-res v2 algorithm)

1. State-res builds a "base state" from the **intersection** of the two
   forking state sets.
2. If the fork diverged **before** the user originally joined the room, the
   base state does NOT contain the user's join event.
3. State-res then auth-checks each conflicting event against this base state.
4. The `leave` event **fails auth** because the user isn't joined in the
   base state (you can't leave a room you're not in).
5. The `leave` event is dropped from the conflict set.
6. The stale `join` event wins by default — the user is "resurrected" as
   joined.

This is sometimes called the "Hotel California" bug: you can check out any
time you like, but state-res may never let you leave.

### Observable symptoms

- State snapshot membership count slowly increases over time
- `yolo audit-membership` shows "ghosts" — users in state but not timeline
- `--conflict <user_id>` shows no remote server has the user as joined
- The state-winning event is often an outlier-only PDU (e.g., a profile
  update at a high depth that was never part of the local timeline)

### The fix (post-filter)

After state-res produces its result, we post-filter `m.room.member` state
keys. For each membership event where state-res picked a different event
than our current state, we compare `origin_server_ts`. If state-res picked
an **older** event, we override it to keep our current (newer) event.

This is restricted to `m.room.member` only — other state types (power levels,
room names) may legitimately have older events win due to power-level
tie-breaking rules in the spec.

```
File: src/service/rooms/event_handler/resolve_state.rs
```

### Why not skip resolve_state entirely for old events?

An older incoming state event might carry `state_at_incoming_event` containing
other perfectly valid, newer state events (power levels, room names) from its
fork branch. Skipping `resolve_state` entirely would drop those valid updates.
The post-filter approach allows the full merge to happen while enforcing a
strict invariant: state-res cannot regress a member's state to an older event.

## Bug 2: Outlier Table Leak on Federation Promotion

### What happens

When an outlier PDU is upgraded to a timeline event via the federation path
(`upgrade_outlier_to_timeline_pdu` → `append_incoming_pdu` → `append_pdu`),
the event is inserted into the timeline table but **never removed** from the
outlier table. The event exists in both tables indefinitely.

### Why it matters

- Events stuck in both tables are discoverable by both `get_pdu` (timeline)
  and `get_pdu_outlier` (outlier). State-res can find these "phantom" outliers
  and treat them as valid fork-branch candidates, amplifying Bug 1.
- Silent database bloat — every federated event that was first seen as an
  outlier (most of them) leaks ~500 bytes of duplicate storage.
- DAG diagnostic tools may report confusing results when the same event
  appears in multiple tables.

### The fix

Call `remove_outlier(event_id, Some(room_id))` after `append_pdu` succeeds
in `append_incoming_pdu`. Placement is after the timeline insert but before
admin command processing.

Soft-failed events correctly remain as outliers because they return early
(line ~59) before reaching `append_pdu`, so this cleanup only fires for
fully authenticated, timeline-promoted events.

```
File: src/service/rooms/timeline/append.rs
```

### Note: promote_outlier was already correct

The backfill path (`promote_outlier` in `backfill.rs`) already called
`remove_outlier`. Only the federation forward-fill path was missing it.

## Bug 3: force_state Silent PDU Lookup Failure

### What happens

`force_state` iterates state diff events and updates the membership cache
for `m.room.member` events. It uses `get_pdu_in_room(Some(room_id))` to
fetch each PDU. When this lookup fails (e.g., for outlier events with room_id
indexing edge cases), it silently skips the membership cache update.

### The fix

Fall back to `get_pdu_in_room(None)` (unfiltered lookup) before skipping.
Log a warning when the fallback is used.

```
File: src/service/rooms/state/mod.rs
```

---

## Cross-Ecosystem Comparison

### tuwunel (conduwuit sibling)

**Status: VULNERABLE to all three bugs.**

tuwunel's `resolve_state` in
`src/service/rooms/event_handler/resolve_state.rs` is structurally identical
to our pre-fix code. No post-filter, no `current_state_ids` clone, blind
`compress_state_events` pipeline. Their `hydra_backports` flag changes some
resolution rules but does not address the base-state auth check failure.

Their `append_incoming_pdu` also lacks the `remove_outlier` cleanup.

Any tuwunel instance in large federated rooms with fork branches will
accumulate the same ghost membership inflation over time.

### Synapse (reference implementation)

**Status: Architecturally protected, but uses the same vulnerable algorithm.**

Synapse runs state-res v2 through `resolve_events_with_store` — the same
algorithm with the same base-state auth check failure mode. However, two
structural differences prevent the damage:

1. **State computation model**: Synapse's `compute_event_context` resolves
   state across the `prev_events` state groups (the state _before_ the new
   event), not by directly merging incoming state against the room's current
   state. The result becomes state context, not a direct overwrite.

2. **Persistence layer firewall**: Synapse's `update_current_state` in
   `persist_events.py` recalculates current state dynamically from forward
   extremities. Even if state-res produces bad results, the persistence layer
   reconciles them against existing extremities rather than blindly committing.

In conduwuit, `resolve_state` directly merges `[current_state_ids,
incoming_state]` and the result is immediately compressed and committed as
the new room state. Our post-filter provides the equivalent safety net
without requiring an architectural rewrite.

---

## Self-Audit Procedure

Use these steps to verify membership state consistency after deploying fixes
or if drift is suspected.

### Step 1: Audit local state vs cache

```
yolo audit-membership !room_id --server trusted_server.org
```

Check for:

- **State vs cache count mismatch**: State snapshot count should equal cache
  count. If state > cache, Hotel California may be active.
- **DIFF entries**: Same membership but different event IDs between timeline
  and state. Indicates state-res picked a fork-branch event over the
  timeline event.
- **WARN entries**: Different membership between timeline and state.
  Strongest signal of Hotel California — e.g., timeline says `leave` but
  state says `join`.
- **Ghost count**: Users in state but with no timeline event. High ghost
  count (relative to room activity) suggests federation state import
  without corresponding timeline events.

### Step 2: Cross-check with remote servers

```
yolo compare-room-state !room_id server1.org server2.org server3.org
```

If local joined count exceeds all remote servers, the local state is
inflated. The remote consensus is the ground truth.

### Step 3: Inspect specific conflicts

```
yolo audit-membership !room_id --server trusted.org
```

For each WARN/DIFF entry:

```
debug get-pdu $state_event_id
```

Check:

- Is the state-winning event an **outlier**? (Status: "Outlier PDU")
- Is its `origin_server_ts` **older** than the timeline event?
- Does any remote server have this user as joined?

If all three: classic Hotel California.

### Step 4: Heal (if needed)

```
yolo force-set-state !room_id server1.org server2.org
```

This fetches authoritative state from remote servers and overwrites the
local state snapshot. The multi-server variant merges state from multiple
sources for better coverage.

After healing, re-run Step 1 to verify counts match.

### Step 5: Monitor

After deploying the Hotel California post-filter fix, watch logs for:

```
State-res sought to resurrect older membership event
```

These `info!` messages indicate the post-filter is actively intercepting
resurrection attempts. Frequency should decrease over time as fork branches
are resolved.

### Expected healthy state

- `audit-membership` shows 0 actionable divergences
- State snapshot count = cache count = remote server consensus
- No WARN entries (timeline/state membership disagreements)
- DIFF entries may still exist for profile updates (same membership,
  different event ID) — these are cosmetic, not harmful

## Bug 4: Membership Cache Count Drift After Leave Events

### What happens

A user leaves a room (visible in the timeline). The state snapshot correctly
reflects the leave (`state=615`), but the **aggregate count cache** is stale
(`cache=616`). The `audit-membership` tool incorrectly reports "OK:
Membership cache is consistent" despite the count mismatch.

### Example

```
Phase 2: State Snapshot vs Cache
OK: Membership cache is consistent for !c10y-...
- Joined: state=615, cache=616    ← MISMATCH IGNORED
```

### Why it happens

The leave event arrives through the normal timeline path:

1. `handle_incoming_pdu` → `append_pdu` (timeline/append.rs:363)
2. `update_membership(room_id, user_id, pdu, true)` — calls `mark_as_left`
   AND `update_joined_count`
3. Cache correctly shows 615

But then a subsequent **state-res** triggers (e.g., a competing fork branch
merges). The state-res flow:

4. `resolve_state` runs, Hotel California post-filter fires (35 overrides)
5. `force_state` processes the delta: `1 new, 1 removed`
6. During "new" processing, a stale `join` event for a different user may
   call `update_membership(room_id, user_id, &pdu, false)` — note `false`
   for `update_joined_count`
7. At the end of `force_state`, `update_joined_count` recalculates from
   `room_members()` — but `room_members()` reads from the individual
   user cache entries (`userroomid_joined`), which may not yet reflect
   all the changes

The root cause is a **TOCTOU race**: `mark_as_left` removes the user from
`userroomid_joined`, but a concurrent or subsequent `force_state` can
re-insert them via `mark_as_joined` if state-res produces a stale join.
The Hotel California post-filter catches MOST of these, but cannot catch
events where `origin_server_ts` is equal or very close.

### Additional factor: rejected events in state-res

The deeper root cause is that **soft-failed events participate in state
resolution**. Line 710-711 of `src/core/matrix/state_res/mod.rs`:

```rust
//TODO: synapse checks "rejected_reason" which is most likely related to soft-failing
```

Synapse skips rejected events at three points during `iterative_auth_check`:

1. Skip the event itself if previously rejected
2. Skip any auth event that was previously rejected
3. Don't include rejected events in the final resolved state

Without this filter, soft-failed events can win state-res battles and
produce stale membership state that the Hotel California post-filter must
then clean up. This is the source of the 35 overrides per transaction.

### The fix (multi-part)

| Component                  | Fix                                                                    | Status  |
| -------------------------- | ---------------------------------------------------------------------- | ------- |
| `audit-membership` Phase 2 | Compare `state_joined.len()` vs `cached_joined` for aggregate mismatch | TODO    |
| `audit-membership` Phase 2 | Call `update_joined_count` when mismatch detected                      | TODO    |
| `state_res::resolve`       | Add `event_rejected` closure parameter                                 | PLANNED |
| `iterative_auth_check`     | Skip rejected events and their auth events                             | PLANNED |
| `resolve_state.rs`         | Wire `is_event_soft_failed` as the reject closure                      | PLANNED |

### Workaround

Until the `event_rejected` firewall is implemented:

```
yolo audit-membership !room_id --server trusted.org   # see the drift
debug force-joined-count !room_id                     # (does not exist yet)
```

Current workaround is to use `force-set-state` to re-sync from trusted
remote servers, which recalculates the count as a side effect.

---

## Bug 5: `audit-membership` Phase 2 False "OK"

### What happens

The audit tool checks per-user `is_joined`/`is_invited` cache entries against
the state snapshot. If all individual entries match, it reports "OK". However,
it does NOT compare the **aggregate count** (`room_joined_count`) against the
state-derived count.

This means `state=615, cache=616` is reported as "OK" because every user
that is in the state snapshot IS correctly marked in the cache — but there's
one extra user in the cache who is NOT in the state snapshot.

### The fix

Add an aggregate count comparison to Phase 2:

```rust
if state_joined.len() as u64 != cached_joined || state_invited.len() as u64 != cached_invited {
    // Report MISMATCH and call update_joined_count to fix
}
```

```
File: src/admin/yolo/commands.rs (audit_membership, Phase 2)
```

---

## Planned: Event Rejected Firewall (`state_res::resolve`)

### Overview

Add an `event_rejected` closure to `state_res::resolve()` to skip
soft-failed events during state resolution, achieving Synapse parity.

### API change

```rust
pub async fn resolve<..., Reject, RejectFut>(
    ...,
    event_rejected: &Reject,
) -> Result<StateMap<OwnedEventId>>
where
    Reject: Fn(OwnedEventId) -> RejectFut + Sync,
    RejectFut: Future<Output = bool> + Send,
```

### Filtering points in `iterative_auth_check`

1. **Main event loop** (line 675): `if event_rejected(event.event_id()).await { continue; }`
2. **Auth events** (line 708): skip auth event if rejected
3. **Resolved state** (post-loop): optionally filter rejected events from output

### Service wiring

```rust
// resolve_state.rs
let event_rejected = |event_id| self.services.pdu_metadata.is_event_soft_failed(&event_id);
state_res::resolve(..., &event_rejected)
```

### Impact

- Eliminates the source of the 35+ Hotel California overrides per transaction
- Reduces state-res divergence in high-activity rooms
- Makes the post-filter a safety net rather than the primary defense

---

## Bug 6: MSC4297 Version Gate (SMOKING GUN)

### What happens

`iterative_auth_check` in `state_res/mod.rs` had an unconditional block
that injected `resolved_state` events into `auth_state` for ALL room
versions. The comment said "MSC4297: for V2.1" but there was **no version
gate**.

### Why it causes divergence

In standard State-Res V2 (rooms V1-V11, including Matrix HQ), an event
is evaluated **strictly** against its own `auth_events`. By injecting
`resolved_state` into `auth_state`, Conduwuit overwrites the event's
auth chain with previously-resolved events from the same iterative loop.

Example scenario:

1. Two conflicting power level events: PL_old (March 19) and PL_new (March 23)
2. PL_old is processed first and enters `resolved_state`
3. When PL_new is processed, Conduwuit injects PL_old from `resolved_state`
   into PL_new's `auth_state`
4. PL_new's auth check sees conflicting permissions from PL_old
5. PL_new **fails auth** and is dropped
6. Synapse evaluates PL_new cleanly (no injection), PL_new passes auth and wins

This single bug causes persistent divergence from Synapse on any V2 room
with conflicting power level events.

### The fix

Gate the block to only run for V2.1 rooms:

```rust
if room_version.state_res == StateResolutionVersion::V2_1 {
    for key in &auth_types {
        // ... only inject resolved_state for V2.1 rooms
    }
}
```

```
File: src/core/matrix/state_res/mod.rs (iterative_auth_check)
```

---

## TODO: OOM Risk in `repair_unsigned`

### What happens

`repair_unsigned` in `yolo/commands.rs` uses `.collect().await` to load
every state event in the room's history into RAM simultaneously. For
large rooms (Matrix HQ has 88K+ state PDUs), this will OOM-kill the
process.

### Why it's dangerous

The command loads ALL state events into a `Vec<_>` before processing them
in chunks. The chunking happens on the already-collected vector, so the
entire dataset is resident in memory.

### The fix

Chunk the _stream_ instead of the collected vector:

```rust
let mut pdus_stream = self
    .services
    .rooms
    .timeline
    .pdus(&room_id, Some(PduCount::min()))
    .filter_map(|r| ready(r.ok()))
    .filter(|(_, pdu)| ready(pdu.state_key().is_some()))
    .chunks(100); // StreamExt::chunks

while let Some(chunk) = pdus_stream.next().await {
    // process chunk with FuturesUnordered
}
```

This trades the total-count progress log for bounded memory usage.

```
File: src/admin/yolo/commands.rs (repair_unsigned)
```

---

## TODO: N+1 Database Queries in `event_rejected`

### What happens

The `event_rejected` closure calls `is_event_soft_failed(&event_id).await`
inside the inner loops of `iterative_auth_check`. For each event being
checked, it queries RocksDB for the event itself, then again for each of
its auth events, producing O(N × M) sequential database lookups.

### Impact

For 50 conflicting control events with 10 auth events each: 500+
sequential RocksDB point lookups per state resolution.

### The fix (pre-filtering)

Filter rejected events from the `auth_events` HashMap during the initial
concurrent fetch, before the sequential auth check loop:

```rust
let auth_events: HashMap<OwnedEventId, E> = auth_event_ids
    .into_iter()
    .stream()
    .broad_filter_map(fetch_event)
    .broad_filter_map(|auth_event| async move {
        if event_rejected(auth_event.event_id().to_owned()).await {
            None
        } else {
            Some((auth_event.event_id().to_owned(), auth_event))
        }
    })
    .collect()
    .boxed()
    .await;
```

This converts O(N × M) sequential lookups into O(N + M) concurrent
lookups.

```
File: src/core/matrix/state_res/mod.rs (iterative_auth_check)
```

---

## TODO: `force_state` PDU-Free Cache Update

### What happens (Bug 4 root cause refinement)

The cache count drift documented in Bug 4 is not a TOCTOU race — the
room `state_lock` prevents concurrent access. The real cause is the
silent `continue` when PDU lookup fails in `force_state`:

```rust
while let Some(event_id) = removed_event_ids.next().await {
    let Ok(pdu) = self.services.timeline
        .get_pdu_in_room(Some(room_id), &event_id).await
        .or_else(|_| { ... })
    else {
        continue; // ← silently skips mark_as_left!
    };
```

When a user is removed from the state snapshot by state-res but the
historical PDU is missing/pruned/unfetchable, `force_state` silently
skips `mark_as_left`, leaving the user permanently stuck in the cache.

### The fix (PDU-free approach)

The `statediffremoved` compressed state entries contain `shortstatekey`
which maps to `(StateEventType, state_key)` via the `short` service.
We don't need the PDU at all:

```rust
let removed_events = statediffremoved
    .iter()
    .stream()
    .map(|&old| parse_compressed_state_event(old));

while let Some((shortstatekey, _shorteventid)) = removed_events.next().await {
    let Ok((event_type, state_key)) = self.services.short
        .get_statekey_from_short(shortstatekey).await
    else { continue; };

    if event_type == StateEventType::RoomMember {
        if let Ok(user_id) = ruma::UserId::parse(&state_key) {
            self.services.state_cache
                .mark_as_left(&user_id, room_id, None).await;
        }
    }
}
```

This eliminates the PDU lookup entirely and guarantees `mark_as_left`
fires for every removed membership event.

```
File: src/service/rooms/state/mod.rs (force_state)
```

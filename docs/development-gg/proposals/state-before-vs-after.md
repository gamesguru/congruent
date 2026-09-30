# State Before vs State After: The Off-by-One Semantic

## Problem

Three layers of the Matrix stack use "state before the event" semantics,
which causes diagnostic confusion and client sync issues when the tip
event is itself a state event.

## Where It Manifests

### 1. Federation `/state/{roomId}`

The spec defines this endpoint as returning the room state **before**
the queried event. When `compare-room-state` queries a remote server
with the tip event and that tip is a state event (e.g. a join), the
remote response excludes the tip's own state change. This makes the
tip appear as an "extra" event locally even though the room is in
perfect sync.

**Example:** Tip is `@shane:wombatx.me` joining. The remote `/state`
returns state without that join → compare shows 1 "extra locally."

### 2. `pdu_shortstatehash` vs `roomid_shortstatehash`

Internally, each PDU stores its `pdu_shortstatehash` — the state
snapshot at which the event was **authorized** (state before). The
room's `roomid_shortstatehash` is updated to the state **after** the
latest state event. When the tip is a state event:

```
tip SSH  = state BEFORE tip event
room SSH = state AFTER tip event
→ SSH status: ✗ tip DIVERGES from room
```

This divergence is correct behavior, not corruption. The two SSHs
serve different purposes:

- `pdu_shortstatehash`: "what state authorized this event?"
- `roomid_shortstatehash`: "what is the room's current state?"

### 3. Client `/sync` (solved by MSC4222)

Clients had the same ambiguity — `/sync` returned state events in an
order that made it unclear whether the state block represented state
before or after the timeline events. MSC4222 (Matrix v1.16, Sept 2025)
solved this by adding `state_after` to `/sync` responses.

## Code Paths

### Federation intake (`append_incoming_pdu`)

```
set_event_state(event_id, room_id, state_ids_compressed)
→ stores state_ids_compressed as pdu_shortstatehash
→ this is the PRE-EVENT state from the sending server
```

### Local events (`append_to_state`)

```
shorteventid_shortstatehash.aput(shorteventid, previous_shortstatehash)
→ stores the PREVIOUS room SSH as the PDU's SSH
→ then computes NEW SSH including this event's state change
→ caller sets roomid_shortstatehash to the NEW SSH
```

Both paths store **pre-event** state on the PDU. The room SSH advances
to **post-event** state. The gap is by design.

## Diagnostic Impact

The `compare-room-state` command shows two false positives when the
tip is a state event:

1. **SSH DIVERGES** — tip SSH ≠ room SSH (expected for state events)
2. **Extra locally** — the tip's own state change appears as a phantom
   diff because the remote `/state` excludes it

### Mitigation (implemented)

The command emits a warning when `at_event` is a state event:

```
⚠ at_event is a state event — remote state excludes its own change
```

### Future improvement (advisor-reviewed)

The SSH comparison could check if the divergence is explained by a
single state event (the tip itself). If applying the tip's state key
change to the tip SSH produces the room SSH, report:

```
SSH status: ✓ tip is consistent (state-event off-by-one)
```

### `reorder-timeline` state walk fix (TODO)

The root cause of the self-reference bug is that `reorder-timeline`
starts its state walk from the room's current (full) SSH. This gives
early backfilled events a state snapshot that includes future events.

**⚠ DO NOT reset to empty.** In federated rooms, the timeline starts
at the user's join — `m.room.create`, `m.room.power_levels`, etc. are
outliers not in the timeline. Resetting to empty would produce state
snapshots that lack the room's foundational state.

**Correct approach:** Seed the walk from the `pdu_shortstatehash` of
the oldest event in the sorted timeline:

```rust
let oldest_event_id = sorted.first().unwrap();
let initial_ssh = self.services.rooms.state_accessor
    .pdu_shortstatehash(oldest_event_id).await;
// Reset room SSH to initial_ssh before starting the walk
```

This preserves outlier state while ensuring the walk starts from the
correct historical point instead of the future tip.

## Why Not Store Post-Event State?

Storing the post-event state as `pdu_shortstatehash` would:

1. **Break federation `/state` semantics** — the spec says state is
   returned _before_ the event. If we store post-event state, we'd
   serve wrong state to other servers.
2. **Break auth chain reasoning** — state-res needs to know what state
   authorized an event, not what state resulted from it.
3. **Break backward compatibility** — existing PDUs in the DB would
   have pre-event SSH; new ones would have post-event SSH.

The current behavior is spec-correct. Only the diagnostics need
improvement.

## `force-set` Hazard: State Rollback on Tip State Events

**WARNING:** `force-set-room-state-from-server --overwrite` queries
the remote's `/state` at the tip event. Because federation returns
state **before** the event, this silently rolls back the tip's own
state change.

**Example:** If the tip is a `m.room.power_levels` update:

- Remote returns the OLD power_levels (state before the tip)
- `force-set --overwrite` adopts the old power_levels
- The power level change is lost

This affects any state event type at the tip: joins, bans, topic
changes, server ACLs, encryption settings, etc.

### Mitigation options

1. **Warn in force-set**: Before executing, check if the tip is a
   state event. If so, warn the operator that the tip's state change
   will be rolled back.
2. **Post-apply the tip**: After adopting remote state, re-apply the
   tip event's state change on top. This preserves the remote's auth
   chain while keeping the tip's effect.
3. **Use a non-tip event**: Allow `--at-event` to specify a message
   event (non-state) as the query point, avoiding the off-by-one.

## Related

- [MSC4222](https://github.com/matrix-org/matrix-spec-proposals/pull/4222):
  `state_after` for `/sync` (Matrix v1.16)
- `sync-join-race-condition.md`: related sync timing issue
- `force-set-room-state-from-server`: updates both tip SSH and room
  SSH to the same value, eliminating the divergence

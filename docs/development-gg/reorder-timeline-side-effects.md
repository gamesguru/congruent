# Timeline Reorder Side Effects

`yolo reorder-timeline` reassigns PDU counts (positions) for all events in a room,
sorting by `origin_server_ts` with topological tiebreaking. This has cascading
effects because many subsystems index by PDU count.

## What Breaks

### 1. Client Sync Tokens (HIGH impact)

Sync tokens encode a PDU count. After reorder, all existing `since` tokens become
stale — they reference old PDU counts that now point to different events (or nothing).

**Effect**: Clients that do an incremental sync will either:

- Get duplicate events (token points to earlier than expected)
- Miss events (token points to later than expected)
- Trigger a full re-sync (token not found)

**Mitigation**: Clients must clear cache and do a full initial sync after reorder.
The command output already says "Clients should re-sync this room."

### 2. `timeline_start_shortstatehash` in Sync (HIGH impact)

The sync endpoint determines state at the start of the timeline window by looking up
`pdu_shortstatehash` for the first PDU in the window. After reorder, a different event
is now "first" in the window — its shortstatehash reflects a different state epoch.

**Effect**: Members who joined after the new "first event" appear as invited/missing
in the sync state. This is the root cause of the "nex shows as invited" bug.

**Root cause**: `pdu_shortstatehash` is stored per `event_id` (not per position), so it
survives reorder correctly. But the _selection_ of which event is "first" changes.

**Mitigation**: After reorder, a full initial sync should use `current_shortstatehash`
(line 522 fallback in `joined.rs`). But lazy loading and timeline limits mean the
fallback may not trigger.

### 3. `token_shortstatehash` Table (MEDIUM impact)

`associate_token_shortstatehash(room_id, count, shortstatehash)` maps sync token
counts to state snapshots. After reorder, old mappings reference invalid counts.

**Effect**: Incremental syncs using stale tokens compute wrong state deltas.

**Mitigation**: Old entries become unreachable after client re-sync. No cleanup needed
but the table accumulates garbage.

### 4. Notification/Highlight Counts (LOW impact)

`userroomid_notificationcount` and `userroomid_highlightcount` are aggregate counters
per (user, room). They are NOT indexed by PDU count, so they survive reorder.

**Effect**: None expected. Counts remain accurate.

### 5. Read Receipts (LOW impact)

Read receipts are stored by `event_id`, not by PDU count. The receipt itself survives.
However, the "last read" position (used to compute unread counts) may shift.

**Effect**: Unread count may temporarily be wrong until the user reads a new message.

### 6. MSC3030 Timestamp Index (MEDIUM impact)

The timestamp-to-PDU index maps `origin_server_ts` → PDU position. After reorder,
PDU positions change but the index is NOT rebuilt.

**Effect**: "Go to date" / timestamp navigation may return wrong positions.

**Mitigation**: Rebuild the timestamp index after reorder. Currently not automated.

### 7. Forward Extremities (NONE)

Stored by `event_id`, not PDU count. Unaffected.

### 8. State Snapshots / shortstatehash (NONE)

`pdu_shortstatehash` maps `event_id` → `shortstatehash`. Since event IDs don't change,
these mappings survive. However, the _selection_ of which event's shortstatehash is
used (per issue #2) changes.

### 9. Outlier/PDU Store Consistency (LOW impact)

`reorder_timeline` calls `reindex_timeline` which writes events to both
`eventid_outlierpdu` and `roomid_outliereventid`. Then `append_pdu` removes from
`eventid_outlierpdu` but NOT `roomid_outliereventid`, potentially leaving stale
index entries.

**Effect**: `list-outliers` may show phantom entries that no longer exist in the PDU store.

## Related: Membership Semantics Difference vs Synapse

Synapse tracks `invite → leave` transitions as "left" members (showing them in the
"People who left" list). conduwuit may only track `join → leave` transitions in
`left_state`, meaning users who were invited but rejected/kicked without ever joining
won't appear as "left" members.

**Effect**: Clients connected to conduwuit won't show rejected-invite users under
"left members", while clients on Synapse will. This is most visible in Cinny's
member list panel.

**Spec**: `membership: "leave"` covers both cases per spec. conduwuit's state_cache
should track both transitions for full compatibility.

## Recommendations

1. **Always tell clients to re-sync** after reorder (already done)
2. **Run `repair-unsigned`** after reorder to fix `prev_content` metadata
3. **Consider rebuilding timestamp index** after reorder
4. **Run `audit-membership`** to verify state consistency
5. **Future**: Add a `yolo audit-state-snapshots` command to detect timeline PDUs
   missing `pdu_shortstatehash` entries

# Timeline Reorder Side Effects

`yolo reorder-timeline` rebuilds a room's local topological index
(`roomid_topologicalorder_pducount`) using a Kahn DAG sort over `prev_events`
(parents before children), tie-breaking concurrent events on
`origin_server_ts`, then the event's Matrix `depth`, then `event_id`. By default
the immutable stream order (PDU count / receive order) is preserved; only
`--force-reindex` reassigns it. This has cascading effects because many
subsystems index by PDU count or by the topological position.

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

### 6. MSC3030 Timestamp Index (LOW by default, HIGH with `--force-reindex`)

`roomid_timestamp_pducount` maps `origin_server_ts` → stream PDU count (the count
is the last component of the key; the value is empty). The default reorder
preserves stream PDU counts, so this index stays valid. Only `--force-reindex`
renumbers PDU counts, and it does NOT rebuild this index.

**Effect**: "Go to date" / timestamp navigation may return wrong positions after
a `--force-reindex` run. The default path is unaffected.

**Mitigation**: Avoid `--force-reindex` unless necessary; if used, rebuild the
timestamp index. Currently not automated.

### 7. Forward Extremities (INTENTIONAL)

Extremities are stored by `event_id`, so reordering does not corrupt them. But
reorder deliberately recomputes them: `recalculate_extremities(room_id, true)`
resets the room's forward extremities to the true DAG tips. This is a fix rather
than a side effect, but it changes what federation/backfill see as our latest
events.

### 8. State Snapshots / shortstatehash (NONE)

`pdu_shortstatehash` maps `event_id` → `shortstatehash`. Since event IDs don't change,
these mappings survive. However, the _selection_ of which event's shortstatehash is
used (per issue #2) changes.

### 9. Outlier/PDU Store Consistency (NONE)

This entry is historical. Current `reorder_timeline` does not call
`reindex_timeline`, so it never writes the outlier maps (`eventid_outlierpdu` /
`roomid_outliereventid`). Reorder only rebuilds the topological index
(`roomid_topologicalorder_pducount`) and the cached
`eventid_metadata.deprecated_local_topo_depth`; the PDU store and event JSON are
untouched.

### 10. Search Inverted Index (LOW by default, HIGH with `--force-reindex`)

The search index (`tokenids`) is keyed by PDU id (shortroomid + shorteventid,
where shorteventid is the stream PDU count). The default reorder preserves PDU
counts, so entries stay valid. `--force-reindex` renumbers counts and does NOT
rebuild the search index.

**Effect**: full-text search may return stale/missing results after
`--force-reindex`.

**Mitigation**: Rebuild search (`reindex-short` or the reindex path) after a
`--force-reindex` run. Not automated by the reorder command itself.

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
3. **Rebuild the timestamp and search indexes** after a `--force-reindex` run
   (the default path preserves PDU counts and needs neither)
4. **Run `audit-membership`** to verify state consistency
5. **Future**: Add a `yolo audit-state-snapshots` command to detect timeline PDUs
   missing `pdu_shortstatehash` entries

# Topo Depth Fix: Recompute depth when parent events arrive

## Problem

When events arrive via federation during a network partition or backfill,
their `prev_events` may not yet be in the database. The current
`append_pdu` code computes `local_topological_depth` by looking up
`prev_events` metadata — if those parents are missing, `max_depth = 0`
and the event gets `depth = 1`.

This causes:

- All partition-recovery events to have the same topo depth (1)
- `seek_topo_key` to fail finding topo entries for these events
- `/messages` to return empty chunks (fixed with raw stream fallback)

## Proposed Fix

After storing an event, check if any already-stored events reference
this event as a `prev_event`. If so, recompute their
`local_topological_depth` and update the topo index entry:

1. Remove old topo key: `[room | old_depth | count]`
2. Compute new depth: `max(parent_depths) + 1`
3. Insert new topo key: `[room | new_depth | count]`
4. Update `eventid_metadata` with new depth

### Risks

- **Recursive propagation**: If event A's depth changes, all events
  referencing A as a parent also need updating. This can cascade.
- **Performance**: Each incoming event could trigger N updates.
- **ABA race**: Concurrent updates to the same topo key.

### Alternative: Lazy recomputation

Instead of eager propagation, mark events as "depth_dirty" and
recompute on next `topo_pdus_rev` read. Simpler but adds read latency.

## Files to modify

- `src/service/rooms/timeline/data.rs`: `append_pdu`, `replace_pdu`
- `src/service/migrations.rs`: Migration to fix existing dirty depths

## Current workaround

`src/api/client/message.rs` falls back to raw `pdus_rev` stream
when `topo_pdus_rev` returns empty results.

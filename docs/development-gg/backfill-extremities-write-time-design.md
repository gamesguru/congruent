# Design: persist backward extremities at write time (tier 3)

Status: part 1 of 3 landed (`8d7951099`) -- schema (two new column families)
+ write-time bookkeeping + pure-function unit tests, per the "Write-time
bookkeeping" section below. **Not yet done:** the migration for existing
rooms, and the `backfill_if_required` read-path swap (still the old scan) --
see "Migration for existing rooms" and "Read-time replacement" below, both
still accurate as written. The new index is being populated on every insert
starting now but nothing reads it yet. Tracked in
`docs/development-gg/room-issues.csv` (row: `backfill_if_required scans the
full window...`). Tiers 1 (singleflight, `89b49fe07`) and 2 (exact-window
cache, `b525c5d61`) are landed and independent of this tier's remaining work.

## Problem recap

`backfill_if_required` (`src/service/rooms/timeline/backfill.rs`) answers
"does this room have a gap near `from`?" by scanning up to `limit` PDUs
backward from `from` on every backward `/messages` call, building a
`HashMap<OwnedEventId, PduEvent>`, and running `rezzy::find_backward_extremities`
over it. Tiers 1/2 stop the *redundant* work (concurrent/repeat identical
scans); they don't stop the *first* scan for a given window, which still
costs a full `limit`-sized range read plus existence probes for every
`prev_event` not already in the scanned map. On a large, active room this is
read amplification on the hot path of every backward pagination.

## How Synapse does it

Verified directly against `../synapse` (this machine has a real checkout),
not from memory:

**Schema** (`synapse/storage/databases/main/events.py`,
`event_federation.py`): a table `event_backward_extremities(room_id,
event_id)` where `event_id` is the *missing* event — the backward extremity
itself — plus the pre-existing `event_edges(event_id, prev_event_id)` DAG
table, and a newer `timeline_gaps(room_id, instance_name, stream_ordering)`
table for MSC3871-style gap signaling independent of backfill points.

**Write time** — `_update_backward_extremities` (`events.py:3645`), called
for every newly-persisted non-outlier event and every outlier promotion
(`events.py:2691`):
1. Collect the event's `prev_event_ids()`.
2. Query which of those are *not* already known as non-outlier local events.
3. `upsert` the remainder into `event_backward_extremities`.
4. Record the stream position into `timeline_gaps`.
5. **Delete** `event_backward_extremities` rows for any event that is
   *itself* now being persisted (`events.py:3712`,
   `DELETE FROM event_backward_extremities WHERE event_id = ? AND room_id = ?`)
   — a formerly-missing parent stops being an extremity the moment it
   arrives.

**Read time** — `get_backfill_points_in_room` (`event_federation.py:1199`):
one SQL join, `event_backward_extremities` ⋈ `event_edges` ⋈ `events`,
filtered by `event.depth <= nearby_depth` (current scroll depth + planned
backfill count, so a backfill point slightly ahead of the visible window
still gets found), ordered by depth descending, `LIMIT`ed. No scan, no
per-request DAG walk — the write-time bookkeeping already did the work.

The core idea we should take: **make insert time pay for gap-bookkeeping
once, so read time is an indexed lookup.** The exact schema doesn't
transplant directly, because we don't have SQL joins — see below.

## Proposed schema for us

RocksDB has no joins, but it does have cheap prefix range scans over sorted
keys — the same trick `room_pducount_eventid` and
`roomid_topologicalorder_pducount` already use for exactly this reason (two
CFs holding the same facts in two different sort orders). Do the same thing
here instead of Synapse's join:

**CF 1 — `roomid_depth_missingeventid`** (read path)
`[shortroomid: 8B][depth: 8B][event_id]` → `()`
Sorted so a backward `/messages` call can prefix-scan `shortroomid` and
range-bound on `depth` near its current position, exactly like
`roomid_topologicalorder_pducount` already does for the main timeline. This
replaces the entire scan-and-decide loop with one bounded range read.

**CF 2 — `roomid_missingeventid_depth`** (delete path)
`[shortroomid: 8B][event_id]` → `depth: 8B`
Synapse deletes by primary key (`event_id`) directly because their table is
naturally keyed that way; ours is keyed by depth for the read path, so we
need this second index to know *which* CF-1 key to delete when a missing
event finally arrives — an O(1) point lookup instead of a scan.

This is a direct copy of the existing dual-index pattern in this codebase,
not a new idea — same justification as `room_pducount_eventid` /
`roomid_topologicalorder_pducount` already coexisting.

(Bikeshed: CF/field names above are placeholders for the actual PR, not
final.)

## Write-time bookkeeping

Every insert path currently touched by tiers 1/2's cache-invalidation calls
needs the equivalent extremity bookkeeping, in the same places:
`append.rs`'s `append_pdu`, and `backfill.rs`'s `backfill_pdu`,
`promote_outlier`, `force_insert_pdu`, `force_insert_pdu_batch`.

On inserting event `E` (depth `E.depth`):
1. For each `prev_id` in `E.prev_events`: if `get_pdu_id(prev_id)` fails
   (not known locally), write `roomid_depth_missingeventid[shortroomid,
   E.depth, prev_id] = ()` and `roomid_missingeventid_depth[shortroomid,
   prev_id] = E.depth`.
2. Resolve: look up `roomid_missingeventid_depth[shortroomid, E.event_id]`.
   If present, `E` was itself a recorded extremity — delete both index
   entries (using the stored depth to construct the CF-1 key).

Both steps are pure point reads/writes keyed by data already in hand at
insert time (no extra PDU fetches), and should go in the same `WriteBatch`
as the PDU insert itself so a crash can't leave the extremity index
inconsistent with the timeline it describes — this is the part that needs
the most care in review, since a missed delete leaks a phantom extremity
forever (harmless but wasteful) while a missed insert silently recreates
tier 1/2's original bug class (a gap that's never found). Given this
codebase's history with exactly that failure mode, this bookkeeping should
ship with the same kind of pure/DB-free unit tests as the `Bound<PduCount>`
work (`data.rs`'s `boundary_tests` module) — the insert/resolve logic above
is expressible as a pure function over `(known_locally: impl Fn(&EventId) ->
bool, prev_events, event_id, depth)` and is testable without a real DB, the
same way `pdus_rev_exclusive_until`/`pdus_exclusive_from` were.

## Read-time replacement

`backfill_if_required` becomes: range-scan `roomid_depth_missingeventid` for
`shortroomid` bounded near the caller's position (mirroring Synapse's
`nearby_depth` padding — current depth plus planned backfill count, so nearby
but not-yet-visible extremities are still found), and if the range is empty,
return immediately. If not empty, fire the `/backfill` federation request as
today. The `HashMap`/`rezzy::find_backward_extremities` scan-and-detect logic
is deleted entirely — the index already *is* the detection result.

Tiers 1 and 2's caching becomes mostly redundant once this lands (a range
scan is cheap enough that memoizing it is unlikely to be worth the
complexity), but there's no urgency to rip them out — they're correct and
harmless either way; that cleanup can ride along with this change or be its
own small followup.

## Migration for existing rooms

This codebase already has a migration framework for exactly this shape of
problem (`src/service/migrations.rs`): a `db["global"]` marker key checked
at startup, `fresh()` sets markers immediately for brand-new databases
(nothing to backfill), `migrate()` runs the real work once for existing
databases. Recent precedent doing a full one-time walk over every event to
populate a new derived index: `POPULATE_TOPOLOGICAL_INDEX_MARKER` /
`populate_topological_index` and `POPULATE_SHORTPREVEVENTS_MARKER`
(`migrations.rs:743-749` and surrounding).

Plan: add `POPULATE_BACKWARD_EXTREMITIES_MARKER`, walk every room's known
events once (same shape as the existing `populate_topological_index`
migration), and for each event apply the same "is each prev_event known
locally" check as the write-time path above to populate CF 1/2 from
scratch. Until that migration has run, `backfill_if_required` needs to keep
the old scan path as a fallback (gated on the marker, same pattern as other
conditional migrations already do) rather than assume the new index is
populated — this is the main reason tier 3 can't just replace tier
1's/2's code path outright on day one.

## Open questions for whoever picks this up

- Exact CF names/`key_size_hint`/`val_size_hint` (`src/database/maps.rs`
  descriptor conventions) — not decided here, cosmetic.
- Whether `roomid_missingeventid_depth`'s value should carry anything beyond
  depth (e.g. which specific child(ren) reference it, for debugging) —
  Synapse doesn't need this because their join reconstructs it live; we'd be
  discarding it unless we store it.
- Redactions/history purge interaction: Synapse's `purge_events.py` has to
  special-case `event_backward_extremities` during room purge
  (`purge_events.py:56,147,283,418`) — our `heal`/`reorder-timeline`/
  admin `yolo` tooling touches the timeline directly in several places
  (`room-issues.csv` has several rows about exactly that tooling) and each
  of those will need the same bookkeeping treatment or a documented reason
  they don't (e.g. if they only ever add data, never remove/rewrite it).
- Whether to keep tiers 1/2's caches after this lands, or clean them up in
  the same change.

## Sizing

Not a single sitting. Rough shape: schema + write-time bookkeeping + its own
unit tests is the first PR-sized chunk; the migration is a second,
independently reviewable chunk (it can be written and tested against the
first chunk's schema without touching the read path yet); swapping
`backfill_if_required`'s read path and deleting the old scan is the third,
smallest chunk, and should be the very last thing merged, gated on the
migration marker so it's provably safe to remove the fallback.

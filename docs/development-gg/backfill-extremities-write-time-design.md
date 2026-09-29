# Design: persist backward extremities at write time (tier 3)

Status: part 1 (`8d7951099`, schema + write-time bookkeeping) is landed and
stable. Parts 2+3 (`969cb1528`, migration + read-path swap) were landed,
then **reverted** (`2239f27ce`) after complement caught a real, 100%
reproducible regression: `TestMessagesPaginationStress/NoDuplicates/Messy_room_activity`
(limit=1 and limit=3) failed on every single room_version=12 run across all
four OS/arch combinations once `969cb1528` was live, while room_version=11
stayed clean. This was not a flake -- confirmed by the `complement-results.log`
run history and caught before it merited more trust than that. My own
"safe by construction" claim below (in the "Deviated from this doc" note)
was wrong, or at least incomplete: whatever the actual defect is, it produced
duplicate/incorrect pagination results, not merely an occasionally-too-eager
backfill trigger. Root cause not yet identified -- see "Open question: what
actually broke" below before attempting parts 2+3 again.

Currently on: part 1 only. `backfill_if_required` uses the old scan
exclusively; the new column families are being populated at write time but
nothing reads them. This is the same safe state part 1 was designed to
leave things in.

**Deviated from this doc in one place before the revert:** the "Read-time
replacement" section below describes bounding the index scan near the
caller's position via Synapse's `nearby_depth` padding heuristic. The
reverted implementation skipped that (existence check across the whole
room instead) to avoid tuning a heuristic under time pressure. Given the
regression, it's worth reconsidering whether _that specific simplification_
is implicated -- an unbounded-by-position scan returning a very large or
oddly-ordered extremities set is a plausible source of exactly the kind of
"pagination integrity" failure complement caught, though this is a
hypothesis to verify, not a confirmed cause.

**Open question: what actually broke.** Not yet root-caused. Candidates
worth checking first, in rough order of suspicion:

1. The child-vs-missing-parent event_id fix (see below) itself -- verify
   the value written by `record_backward_extremities_into_batch` and read
   by `nearby_backward_extremities` actually agree in all cases, including
   whatever `promote_outlier`/`force_insert_pdu` write.
2. Whether `nearby_backward_extremities`'s unbounded-by-position result set
   can include extremities whose child event isn't actually near `from`,
   causing the budget loop to fire `/backfill` requests whose returned
   PDUs land somewhere that confuses `assertPaginationIntegrity`'s dedup
   check -- possibly interacting with tier 1/2's caches in a way not
   accounted for.
3. Whether the migration path and the write-path can disagree for a room
   populated partly before and partly after `8d7951099` within the same
   complement test run (i.e. a race between migration-driven population
   and live write-path population for the _same_ room during the test).

**Previously landed (and still landed) as part of the reverted commit's
write-side correctness work, independent of the read-path bug:**
`record_backward_extremities_into_batch`'s CF1 value was originally `()`,
matching Synapse's schema literally. But this codebase's `/backfill` call
wants _child_ event IDs (events with a missing parent), not the missing
parent's own ID. This fix was reverted along with everything else in
`969cb1528` and will need to be re-applied (and re-verified) when parts 2+3
are attempted again.

Tracked in `docs/development-gg/room-issues.csv` (row: `backfill_if_required
scans the full window...`, back to `PARTIAL`). Tiers 1 (singleflight,
`89b49fe07`) and 2 (exact-window cache, `b525c5d61`) remain in place
alongside tier 3; there's no urgency to remove them (see "Read-time
replacement" below).

## Problem recap

`backfill_if_required` (`src/service/rooms/timeline/backfill.rs`) answers
"does this room have a gap near `from`?" by scanning up to `limit` PDUs
backward from `from` on every backward `/messages` call, building a
`HashMap<OwnedEventId, PduEvent>`, and running `rezzy::find_backward_extremities`
over it. Tiers 1/2 stop the _redundant_ work (concurrent/repeat identical
scans); they don't stop the _first_ scan for a given window, which still
costs a full `limit`-sized range read plus existence probes for every
`prev_event` not already in the scanned map. On a large, active room this is
read amplification on the hot path of every backward pagination.

## How Synapse does it

Verified directly against `../synapse` (this machine has a real checkout),
not from memory:

**Schema** (`synapse/storage/databases/main/events.py`,
`event_federation.py`): a table `event_backward_extremities(room_id,
event_id)` where `event_id` is the _missing_ event — the backward extremity
itself — plus the pre-existing `event_edges(event_id, prev_event_id)` DAG
table, and a newer `timeline_gaps(room_id, instance_name, stream_ordering)`
table for MSC3871-style gap signaling independent of backfill points.

**Write time** — `_update_backward_extremities` (`events.py:3645`), called
for every newly-persisted non-outlier event and every outlier promotion
(`events.py:2691`):

1. Collect the event's `prev_event_ids()`.
2. Query which of those are _not_ already known as non-outlier local events.
3. `upsert` the remainder into `event_backward_extremities`.
4. Record the stream position into `timeline_gaps`.
5. **Delete** `event_backward_extremities` rows for any event that is
   _itself_ now being persisted (`events.py:3712`,
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
need this second index to know _which_ CF-1 key to delete when a missing
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
is deleted entirely — the index already _is_ the detection result.

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

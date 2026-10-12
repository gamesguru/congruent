# Room DAG repair triage

This documents the validated workflow for distinguishing stale extremity indexes
from incomplete backfilled room history.

## Core rule

Do not repair a room merely because `check-rooms --deep` reports many tips.
First classify the room's parent closure.

### Safe extremity-repair candidate

A room is a candidate for `recalculate-extremities` when all of these hold:

- the `prev` DAG has no dangling or unmapped parents;
- mtxdb reports `0 missing prev_events`;
- the room has no relevant outlier population;
- the finding is `EXTREMITIES_DRIFT` only;
- there are no chronology breaks requiring timeline reordering.

For this class, the stored extremity index is stale, while the canonical event
DAG is locally complete.

### Do not blindly repair

Rooms with dangling or unmapped `prev` parents are incomplete event subsets,
often because they were backfilled without the full historical graph. Their
large root/head/isolated counts are not automatically corruption. Fetch the
missing events first if closure is required.

Missing Conduwuit auth-cache rows are not equivalent to missing `auth_events`:
the auth-chain reader falls back to the event PDU. Do not mass-backfill the
auth cache merely because its column family is sparse.

## Independent mtxdb check

Export a room from the admin console without forcing topological order:

```text
yolo get-room-dag <ROOM_ID> 0 -1 --outliers --merge-outliers
```

Import the generated JSONL into a fresh mtxdb database:

```bash
mtxdb --dir /tmp/mtxdb-room init
mtxdb --dir /tmp/mtxdb-room import /tmp/local-dag-....jsonl
mtxdb --dir /tmp/mtxdb-room info \
  --collection '<ROOM_ID>' \
  --stats
```

The built-in `matrix-event-v1` template is used by default. Passing
`--template ../mtxdb/templates/matrix-event-v1.json` is optional.

Compare:

```text
Conduwuit STRUCT DAG dangling/unmapped prevs
mtxdb missing prev_events
```

The counts may differ slightly because Conduwuit counts edge occurrences while
mtxdb reports distinct missing event IDs. They should identify the same
underlying missing-parent population.

mtxdb does not inspect Conduwuit short-ID tables, stale mappings, rejection
metadata, or room extremity indexes. It is an independent canonical-event DAG
check.

## Validated example

Room:

```text
!D1J4GsCJBfrgJ0aXT0:nutra.tk
```

The exported room contained 1,617 events. mtxdb reported:

```text
2 forward extremities
0 missing prev_events
0 missing auth_events
```

Conduwuit independently computed:

```text
scanned_events=1617
accepted_events=1617
outlier_events=0
current_tips=1
calculated_tips=2
```

`view-extremities` initially showed one stored tip. This confirmed stale
extremity state rather than missing event data.

The targeted repair was:

```text
yolo recalculate-extremities !D1J4GsCJBfrgJ0aXT0:nutra.tk
```

Conduwit's result was:

```text
SUCCESS: DAG Extremities were silently broken and have now been
recalculated and permanently healed! Set to 2 true DAG tips.
```

Afterward, `view-extremities` showed both canonical tips. No timeline reorder
was needed because the room had no chronology breaks.

## Operational sequence

1. Export one representative room.
2. Run mtxdb `info --stats` on the export.
3. If parent closure is incomplete, do not recalculate or reorder; fetch or
   accept the missing historical events.
4. If parent closure is complete and only extremities drift, run targeted
   `recalculate-extremities` on a disposable copy first.
5. Verify with `count-extremities` and `view-extremities`.
6. Do not run `reorder-timeline` unless chronology is independently broken.

The export and mtxdb import are read-only with respect to the Conduwuit room;
`recalculate-extremities` is the mutation step.

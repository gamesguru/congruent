# Diagnosis: v12 `TestMessagesOverFederation` re-join failure (message 18/20 stranded)

Status: root-caused to a specific line, not yet fixed. Do not attempt the
obvious patch (gating `upgrade_outlier_to_timeline_pdu`'s early-return on the
topo index, or making `get_pdu_id` authoritative) without first reading
`backfill-extremities-write-time-design.md` — every candidate fix here
touches "what does 'this event is in the timeline' mean", which is exactly
the surface `969cb1528` got wrong and `2239f27ce` reverted, with a **100%
reproducible** `TestMessagesPaginationStress/NoDuplicates` regression on
v12. That regression's root cause was never identified either. Don't stack
a second unverified change onto the same surface.

## Symptom

`TestMessagesOverFederation/Visible_shared_history_after_re-joining_room_(backfill)`,
the `messagesRequestLimit`-lower-than-backfilled case (20 messages sent,
limit=10). Consistently and only on **room_version=12**. Message index 18 of
20 (the second-to-last message, direct `prev_event` of the last message) is
never returned by any `/messages` page across a full backward traversal to
the start of the room. Confirmed across three independent runs, three
different rooms, same shape every time:

- Run `31240375066` (`amd64 ubuntu-24.04-v12`): `$wMeUhZ6Xb8kLyVNb80Vi9YE9DLS4rCXskzovLkBpuUg` missing.
- Run `31241023834` (`amd64 ubuntu-24.04-v12`): same test, different room, same index missing.
- Run `31267318970` (`amd64 ubuntu-22.04-v12`): `$gtk-MtPypI_2PSZ9fgl7lTywDdq6eMj5c1LzAlLw4Nk` missing, child `$vXF1rN3KSbYS--fVVKoMN4jno2sUgGGiiQ_HAc-5AMA` present.

This is **not** the mechanism `3e218c4ab` fixed (see below) — that fix
cleared all `ubuntu-24.04` configs (both arches, both room versions) on run
`31267318970`. This failure is specific to `ubuntu-22.04` / v12 and survives
that fix untouched, because the trigger below diverts control before
`3e218c4ab`'s code path is ever reached.

## Trigger: a real, intermittent signature-verification failure, v12-only

The **last** message in the room (message 20/20, e.g. `$vXF1rN3KSbYS...`,
`depth: 27`) fails cryptographic signature verification on its *first*
fetch route, every single time this failure has been observed:

```
WARN conduwuit_service::server_keys::verify: Signature verification failed
for event $vXF1rN3KSbYS--fVVKoMN4jno2sUgGGiiQ_HAc-5AMA. Error:
Verification(Signature(signature::Error { source: Some(Verification
equation was not satisfied) })). ...
```

That event is fetched via **two independent routes** in the same join:

1. `fetch_prev`'s own `/get_missing_events` call (resolving some other
   event's `prev_events`) — does its own manual `verify_event` call inline
   (`fetch_prev.rs`'s `broad_filter_map`). **Fails.** The event gets
   `mark_event_rejected("signature verification failed")` +
   `add_pdu_outlier`, and is dropped from that call's `eventid_info`.
2. `join_remote_process`'s explicit "fetching join extremity" fetch
   (`join.rs`, `conduwuit_api::client::membership::join: fetching join
   extremity`) — goes through `handle_outlier_pdu` again independently.
   **Succeeds** this time, and the event gets promoted normally
   (`"Upgrading PDU from outlier to timeline"`).

Same event, same signature, two fetch/verify call sites, one fails one
succeeds. That is a canonicalization instability, not an actual bad
signature — `verify.rs:224`'s comment (`"Remove once that's resolved; this
is deliberately temporary"`) is existing instrumentation someone added
specifically to chase this class of bug; it has not been resolved yet. This
has not been root-caused. Candidates: `isolate_origin_signatures`'s
behavior on v12 events specifically, or a difference in what's stripped
(`unsigned`, etc.) between the two call sites before the bytes reach
`ruma::signatures::verify_event`. Two independent v12-only regressions in
this same area (this one, and `969cb1528`'s) is worth treating as a signal,
not a coincidence.

## Consequence: the parent gets silently stranded, twice

Because the *first* verify attempt on the child failed, that first
`fetch_prev` call never got to process the child's own `prev_events` — the
child wasn't in its `eventid_info`, so nothing walked further back from it
that round.

Once the *second* fetch succeeds and the child is promoted, its own
`process_timeline_upgrade` runs, calls `fetch_prev` again for the child's
`prev_events`, correctly discovers the parent (message 18) as missing,
fetches it, and it passes both `fetch_prev`'s sig check and
`handle_outlier_pdu`'s auth-chain check cleanly (`found=2 missing=0`, no
warnings). It lands in `eventid_info`. `process_timeline_upgrade` then calls
`handle_prev_pdu(..., Some((pdu, json)), ...)` for it — the *normal*, non-`None`
branch. `3e218c4ab`'s fix (which only changed the `None` branch) never
engages here; this is a different mechanism.

`handle_prev_pdu` calls `upgrade_outlier_to_timeline_pdu`. That function's
very first lines:

```rust
// upgrade_outlier_pdu.rs:53-61
if let Ok(pduid) = self.services.timeline.get_pdu_id(incoming_pdu.event_id()).await {
    return Ok(Some(pduid));
}
```

Log evidence this branch is the one taken, for the parent (`$gtk-MtPyp`, run
`31267318970`):

```
16:51:45.221018Z  state_res_debug: Found auth event locally for outlier event_id=$gtk-MtPyp ...
16:51:45.221028Z  state_res_debug: Auth events local lookup summary event_id=$gtk-MtPyp found=2 missing=0 total_auth=2
16:51:45.221540Z  state_res_debug: fetched state via /state_ids; proceeding with auth check event_id=$vXF1rN ...
```

512 microseconds between the parent's successful auth-chain check and the
*child's own* state resolution resuming — with **no** `"Upgrading PDU from
outlier to timeline"` log (the only log between `get_pdu_id`'s early return
and the real upgrade path), no `"Event was previously soft-failed"`, no
`"Prev $gtk-MtPyp failed"` (which would appear if `handle_prev_pdu`
propagated an `Err`, and would have aborted the child's own promotion too —
it didn't). Every other branch in `upgrade_outlier_to_timeline_pdu` before
the real upgrade logs something or returns `Err`; this is the only silent
`Ok`. `get_pdu_id` said the parent was already a timeline PDU. It wasn't —
`/messages` never returned it, through the full backward traversal to the
start of the room.

**Second skip, same phantom mapping:** `backfill_if_required`'s own gap-scan
independently rediscovers this exact gap 100ms later (a *third* fetch/attempt,
via `/backfill` this time) and calls `backfill_pdu`, whose own guard —
`non_outlier_pdu_exists` — presumably reads the same
`eventid_metadata`/`eventid_pduid` mapping `get_pdu_id` does and also
declines to insert (`handle_outlier_pdu: early return, event already
known` — the outlier cache hit, not a timeline hit, but downstream of that
the insert is skipped the same way). One phantom "already in timeline"
signal, two independent skip paths, one message permanently missing from
`/messages`.

## Where the phantom mapping likely comes from

`get_pdu_id` (`data.rs:697`) has two sources of truth, checked in order:

1. **Fast path**: `eventid_metadata` — if `EventMetadata.pdu_count` is
   `Some`, trust it.
2. **Legacy fallback**: `eventid_pduid`.

Neither of these *is* `roomid_topologicalorder_pducount`, the index
`/messages` actually reads (`topo_pdus_rev`). The normal insert paths
(`prepend_backfill_pdu_batch`, `append_pdu_batch`) write all of these
together in one `WriteBatch`, so they can't disagree by construction. But
`eventid_metadata` and `eventid_pduid` are no longer written that way by the
reorder helpers on current HEAD: `replace_stream_and_topo_pducount_batch` /
`set_event_metadata_depth_and_count_into_batch` now land
`room_pducount_eventid`, `eventid_pduid`, `eventid_metadata`, and the topo
index in a single RocksDB `WriteBatch`, so those four mappings are atomic
with respect to each other. `reorder.rs` is still worth checking for other
consistency bugs (for example room-wide index rebuild races or oversized
batches), but it is **not** the remaining example of split, unbatched writes
to these mappings.

**Not yet checked:** whether some part of the join/state-resolution path
(distinct from the normal insert functions) writes `eventid_metadata` for an
event as a side effect of using its `depth` for state-res bookkeeping,
without that event ever going through a real insert. This is the next thing
to check, with a real DB to inspect (see "Next step").

## What NOT to do without Docker access

Two candidate fixes come to mind immediately and both are traps:

- **Gate `upgrade_outlier_to_timeline_pdu`'s early-return on the topo
  index instead of `get_pdu_id`.** Changes what "already in the timeline"
  means for this one call site only — inconsistent with `non_outlier_pdu_exists`
  and every other caller of `get_pdu_id`, a worse split than the current one.
- **Make `get_pdu_id` check the topo index too / instead.** This is
  materially the same shape of change as `969cb1528`'s reverted read-path
  swap — different index, same idea: change which structure is authoritative
  for "does this event have a timeline position". That PR's regression
  (`TestMessagesPaginationStress/NoDuplicates`, 100% reproducible on v12,
  never root-caused) is the exact failure class this kind of change can
  introduce. `2239f27ce`'s author's own "safe by construction" reasoning
  about a smaller version of this same swap was wrong. Don't repeat it
  without a passing run of that test in hand, which requires Docker.

The verify-instability trigger is upstream of all of this and should be
fixed first regardless — if the child's signature verifies cleanly on the
first attempt, the parent is never stranded, and this whole failure mode
doesn't fire. Whether the phantom-mapping issue also needs its own fix once
that's done is a separate, open question.

## Next step for whoever picks this up (needs Docker)

1. Reproduce a v12 `TestMessagesOverFederation` re-join run locally.
2. Diff `verify.rs:224`'s canonical-JSON dump (logged on every verify
   failure) against what hs1 actually signed for the message-20-class
   event, across both fetch routes (`fetch_prev`'s manual check vs.
   `handle_outlier_pdu`'s). That's where the instability starts.
3. Once verification is stable, re-run the same test. If message 18 still
   goes missing, inspect `eventid_metadata` / `eventid_pduid` /
   `roomid_topologicalorder_pducount` directly for that event via the admin
   `heal`/inspection tooling to confirm or rule out the phantom-mapping
   theory before writing a fix.

## What `3e218c4ab` already fixed (separate, real, unaffected by the above)

`handle_prev_pdu`'s `None`-`eventid_info` branch (a *different* silent-skip:
`fetch_prev` omits a `prev_id` from its result when `pdu_exists` — which
matches outlier-only events — already considered it "not needed", and the
old code read `None` as "nothing to do" instead of "might be outlier-only,
never promoted"). That fix is real, compiles and clippies clean, and
cleared `ubuntu-24.04` on both arches and both room versions on run
`31267318970`. It just doesn't touch this bug, since this bug's control
flow never reaches that branch.

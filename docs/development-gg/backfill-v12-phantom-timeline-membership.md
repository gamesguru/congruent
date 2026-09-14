# Diagnosis: v12 `TestMessagesOverFederation` re-join failure (message 18/20 stranded)

Status: root-caused to a specific line, not yet fixed and verified. One
candidate fix has been applied to `append.rs`'s soft-fail branch (see
"Candidate fix applied, unverified" near the end) — real invariant
violation, plausible connection to this bug, **not proven** to be the cause,
**not yet run against Complement**. Do not attempt the other obvious patches
(gating `upgrade_outlier_to_timeline_pdu`'s early-return on the topo index,
or making `get_pdu_id` authoritative) without first reading
`backfill-extremities-write-time-design.md` — every candidate fix here
touches "what does 'this event is in the timeline' mean", which is exactly
the surface `969cb1528` got wrong and `2239f27ce` reverted, with a **100%
reproducible** `TestMessagesPaginationStress/NoDuplicates` regression on
v12. That regression's root cause was never identified either. Don't stack
a second unverified change onto the same surface.

## Symptom

`TestMessagesOverFederation/Visible_shared_history_after_re-joining_room_(backfill)`,
the `messagesRequestLimit`-lower-than-backfilled case (20 messages sent,
limit=10). Message index 18 of 20 (the second-to-last message, direct
`prev_event` of the last message) is never returned by any `/messages` page
across a full backward traversal to the start of the room. Confirmed across
five additional independent runs since the original three below, same shape
every time:

- Run `31240375066` (`amd64 ubuntu-24.04-v12`): `$wMeUhZ6Xb8kLyVNb80Vi9YE9DLS4rCXskzovLkBpuUg` missing.
- Run `31241023834` (`amd64 ubuntu-24.04-v12`): same test, different room, same index missing.
- Run `31267318970` (`amd64 ubuntu-22.04-v12`): `$gtk-MtPypI_2PSZ9fgl7lTywDdq6eMj5c1LzAlLw4Nk` missing, child `$vXF1rN3KSbYS--fVVKoMN4jno2sUgGGiiQ_HAc-5AMA` present.
- Run `31666441322` (arm64 ubuntu-24.04): two adjacent events missing this time
  (indices 17 and 18) instead of one — `$O9bjGHIb09FCBCs0HDYvylfDNlnWa0hELiZKvcb8fv0`
  and `$I1OqqPmmqCtRh7XNWEaju2W3q3OPTT8LJHQO0n-AoNo`. Both confirmed present in
  `eventid_pdu`/`append_pdu: insert complete` on the serving homeserver (topo
  writes completed, per timestamp, ~6-10ms _before_ the `/messages` call that
  should have returned them — ruling out a simple write-after-read race for
  this instance).
- Run `31668463227` / commit `0ca548647`: **two different stranded events in
  the same run**, one per arm64 job:
    - arm64-v11: `$Hd735Ac1tD65KI1I_tKVByAD5NzVIkhkhlFZG4kZ6Bw`, index 18,
      inserted via `backfill_pdu` (`prepend_backfill_pdu_batch`,
      `Backfilled(-862)`) after `backfill_if_required`'s gap-scan
      rediscovered it 100ms+ after an earlier outlier-only fetch.
    - arm64-v12: `$1id4YyJezjz0hslPQmnZE8LNXSqSlFgvq-HxZl8Wrh4`, index 18,
      inserted via `append_pdu` (`append_pdu_batch`, `Normal(862)`) from the
      live federation-transaction path, no backfill involved.

  **Different insertion function, different count region (Backfilled vs.
  Normal), same run, same symptom.** That weakens "which insert function
  ran" as the discriminator. What both share, identically, immediately
  before the write: a `"DAG fork detected: resolving state across
  prev_events + current extremities ... n_prev=1 n_extremities=1 n_total=2"`
  step, followed by `"State resolution completed for incoming PDU"`. Every
  stranded event traced so far (this run and the `31267318970`/
  `31240375066` runs above) went through this DAG-fork-merge branch
  specifically, never the plain fast-forward branch (`"Fast-forward state
  update, skipping state resolution"` — seen in the same logs for other,
  non-stranded events). Not proof, but a real correlation across every
  instance traced — worth checking whether the `depth` value this branch
  assigns/uses differs from what fast-forward would compute, since a wrong
  depth would misplace the topo-index entry without any write being "missing"
  in the sense audited below.

- PR #47 CI run `#1984` (5-way job matrix): **4 of 5 jobs fail with this exact
  signature** (856 pass / 3 fail / 11 skip, identical failing-test list) —
  amd64-22.04-v12, amd64-24.04-v12, arm64-24.04-v12, **and arm64-24.04-v11**.
  Only amd64-24.04-v11 passes clean (859/0/11).

### Correction: this is not "v12 only"

The original three runs were all v12, which is why the title and this
section originally said "consistently and only on room_version=12." That
does not hold up against the larger sample: **arm64-ubuntu-24.04-v11 fails
with the identical signature**, in two separate runs now (`31668463227` and
PR #47 run `#1984`), while **amd64-ubuntu-24.04-v11 passes clean** in both.
The pattern that actually fits the data is arch/timing-sensitivity, not a
room-version gate — v12's extra auth/creator-gate bookkeeping plausibly
widens the same timing window that arm64 already hits more reliably on its
own, rather than v12 being a distinct trigger. Title kept for continuity
(existing cross-references), but don't read "v12" as a hard precondition
when reproducing this.

This is **not** the mechanism `3e218c4ab` fixed (see below) — that fix
cleared all `ubuntu-24.04` configs (both arches, both room versions) on run
`31267318970`. This failure is specific to `ubuntu-22.04` / v12 and survives
that fix untouched, because the trigger below diverts control before
`3e218c4ab`'s code path is ever reached.

## Trigger: a real, intermittent signature-verification failure

(Originally described as "v12-only" below — see the correction above. The
three original repro runs were all v12, so references to "v12" in this
section reflect that original sample, not a confirmed precondition.)

The **last** message in the room (message 20/20, e.g. `$vXF1rN3KSbYS...`,
`depth: 27`) fails cryptographic signature verification on its _first_
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

Because the _first_ verify attempt on the child failed, that first
`fetch_prev` call never got to process the child's own `prev_events` — the
child wasn't in its `eventid_info`, so nothing walked further back from it
that round.

Once the _second_ fetch succeeds and the child is promoted, its own
`process_timeline_upgrade` runs, calls `fetch_prev` again for the child's
`prev_events`, correctly discovers the parent (message 18) as missing,
fetches it, and it passes both `fetch_prev`'s sig check and
`handle_outlier_pdu`'s auth-chain check cleanly (`found=2 missing=0`, no
warnings). It lands in `eventid_info`. `process_timeline_upgrade` then calls
`handle_prev_pdu(..., Some((pdu, json)), ...)` for it — the _normal_, non-`None`
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
_child's own_ state resolution resuming — with **no** `"Upgrading PDU from
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
independently rediscovers this exact gap 100ms later (a _third_ fetch/attempt,
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

Neither of these _is_ `roomid_topologicalorder_pducount`, the index
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

## Write-path audit (static only, no Docker) — what's confirmed and what's ruled out

A second pass (this session, plus an independently-run parallel trace that
converged on the same `get_pdu_id`/data.rs:854 mechanism from a different
angle — good cross-check) read every writer of `eventid_metadata` end to
end. Line numbers are current HEAD, not the `data.rs:697` cited above (the
file has moved since); the mechanism is unchanged.

**Confirmed correct, by direct code reading — not just inference:**

- `append_pdu_batch` and `prepend_backfill_pdu_batch` (`timeline/data.rs`)
  both write `roomid_topologicalorder_pducount`, `eventid_pduid`,
  `room_pducount_eventid`, and `eventid_metadata` (with `pdu_count: Some(..)`)
  into the _same_ `database::Batch` object, applied as one unit. No `.await`
  splits the topo write from the metadata write within either function. This
  matches the doc's original claim; verified directly, not assumed.
- Both functions are called from call sites that hold the room's
  `mutex_insert` for the duration of the write (`timeline/append.rs:247`,
  `timeline/backfill.rs:747`) — so live-federation delivery and backfill
  gap-fill can't race each other's writes for the same room.
- `append_pdu`'s own duplicate-insert guard
  (`self.non_outlier_pdu_exists(...)` at `append.rs:248`) runs **after**
  acquiring `mutex_insert`, so two concurrent callers converging on the same
  event self-heal correctly: the second one sees the first's real entry
  under the lock and reuses it rather than double-inserting or skipping.
- `add_pdu_outlier_batch` (`rooms/outlier/mod.rs:~225`, flagged as a
  suspect by the parallel trace above) is **not** the hazard it looks like.
  It always constructs a fresh `EventMetadata` with `pdu_count: None`
  explicitly, and guards itself: if the existing stored metadata shows
  `is_outlier: false` (i.e. already a real timeline entry), it logs and
  returns without writing at all. This closes out that specific guess —
  don't re-check this file next time without new evidence.
- The `TimelineKey` encoding (`core/matrix/pdu/count.rs`) — depth as the
  primary axis, big-endian, unsigned-comparable; `stream_ordering` as
  offset-binary-encoded secondary axis (XOR sign bit, the standard technique
  for sorting signed ints under unsigned byte comparison) — is structurally
  correct. A high-depth event with a deeply negative (`Backfilled`) count
  sorts in its correct depth bucket regardless of the count's sign or
  magnitude; it does not get shunted next to unrelated low-depth entries
  that happen to share the Backfilled region. Ruled out as an independent
  explanation.
- `topo_pdus_rev`'s boundary logic (`timeline/data.rs:1894-1929`) reseeks
  from `depth = u64::MAX` on _every_ page (not incrementally continuing from
  the prior page), specifically so a backfill gap-filler landing after a
  page was already served can still be picked up on the next page — per its
  own comment, built for exactly this failure class. Traced by hand against
  the run-`31668463227` log: for the first page the boundary token has
  `until.depth = u64::MAX` too (the literal sentinel), so the depth-based
  cutoff barely constrains anything and the missing event's real depth
  (26, one less than its child's logged `depth: 27`) should pass it fine.
  Doesn't explain the miss on its own.
- `count_to_id` (`timeline/data.rs:2087`) is a pure deterministic encode —
  `PduId { shortroomid, shorteventid }` — no DB read at all. Can't be
  "stale." Ruled out.

**A latent race on marker fields — a separate, real bug, not a proven cause
of this failure. Flagging so it isn't re-discovered and folded into the
causal chain for this bug without new evidence:**

`rooms/pdu_metadata/data.rs`'s `mark_event_rejected` (`:188`),
`mark_event_soft_failed` (`:138`), `unmark_event_rejected`, and
`unmark_event_soft_failed` (`:225`) each do an unbatched, unsynchronized
read-`eventid_metadata`-mutate-one-field-`.insert()`-the-whole-struct-back
cycle, entirely outside `mutex_insert` and outside any `database::Batch`.
**None of them ever set `pdu_count`** — they only toggle the
soft-fail/rejected markers, copying `pdu_count` forward unchanged from
whatever they read. By themselves they cannot manufacture a fake timeline
membership; only the pre-existing value survives. Independently confirmed
by a second static pass, so treat this as settled, not just asserted.

The theoretical race they're still capable of is narrower than originally
framed here: they _can_ race against an in-flight `append_pdu_batch`/
`prepend_backfill_pdu_batch` for the same event — if one of these reads the
pre-append metadata (`pdu_count: None`) and its write lands _after_ the real
insert's batch commits, it silently reverts `eventid_metadata.pdu_count`
back to `None` — a real, verified inconsistency bug, worth its own fix.

But it can only produce a **false negative** (a real timeline event's
`pdu_count` field reverted to looking unset), not the **false positive**
(`get_pdu_id` returning `Some` for an event with no real entry) this bug
needs. And even the false-negative direction self-heals for both consumers
in the doc's trace: `get_pdu_id` falls back to `eventid_pduid` (untouched by
this race — only `append_pdu_batch`/`prepend_backfill_pdu_batch` write it,
same atomic batch as everything else), and `non_outlier_pdu_exists`
additionally cross-checks `room_pducount_eventid.exists(&pduid)` (same
atomicity guarantee) rather than trusting `get_pdu_id`'s answer alone. So
this race doesn't currently look load-bearing for the actual stranding —
noted here mainly so the next person doesn't spend the same hour on it.

**Net result of this pass:** I did not find a writer-side static path in the
audited code (the four writers listed above) that obviously explains the
false positive `get_pdu_id`/`non_outlier_pdu_exists` would need to produce.
That is narrower than "ruled out" — the state-resolution/bookkeeping paths
this doc already flagged as unchecked (previous section) remain unchecked,
and an absence of an obvious mechanism in what's been read is not proof
none exists elsewhere. The log evidence (silent `Ok` where every other
branch logs or errors, sub-millisecond timing) still points at a phantom
mapping of some kind; this pass only narrows _where it isn't_, not where it
is. Candidates _not yet ruled out_, in priority order given the evidence
above:

1. **The DAG-fork-merge depth correlation** (see the `31668463227` entry in
   "Symptom" above) — every stranded event traced across every run shares
   this one step immediately before the write, regardless of which insert
   function ultimately runs. Check what `pdu.depth()` actually resolves to
   for an event processed through this branch vs. what fast-forward would
   give it, and whether that depth is consistent with the depth the child
   event (`n8pHPQnL6...`/`$6rFEpnkP...`-class, always the direct child of
   the stranded event) expects its parent to have. This is the most
   concrete, narrowly-scoped lead so far — check it first.
2. Whether `eventid_pduid` or `room_pducount_eventid` can end up out of sync
   with the topo index through some path outside the four writers audited
   here (state-resolution bookkeeping is still explicitly unchecked, per the
   paragraph above this section).
3. Whether the two concurrent fetch/verify routes in the trigger mechanism
   can each derive a _different_ `RawPduId` for the same event via two
   independent `next_count()` calls before either acquires `mutex_insert` —
   worth confirming `upgrade_outlier_to_timeline_pdu`'s early-return check
   (`upgrade_outlier_pdu.rs:53-61`) itself runs unlocked, and whether that
   specific gap (not the write side, the _decision_ side) is where two
   racing callers can diverge.

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

## Ruled out: `add_pdu_outlier` synchronization (not part of the causal chain)

A lead raised mid-investigation: ~20 of the ~22 call sites of
`add_pdu_outlier` use the plain (non-`_locked`) variant, which looked at
first glance like an unsynchronized write racing `append_pdu`'s
`mutex_insert`-protected write. **This is wrong — checked directly against
the implementation, not assumed:**

```rust
// rooms/outlier/mod.rs:151
pub async fn add_pdu_outlier(&self, event_id: &EventId, pdu: &CanonicalJsonObject, room_id: Option<&RoomId>) {
    let room_id_for_lock = derive_room_id(pdu, room_id, event_id);
    let _guard = match room_id_for_lock.as_deref() {
        | Some(room_id) => Some(self.services.timeline.mutex_insert.lock(room_id).await),
        | None => None,
    };
    self.add_pdu_outlier_inner(event_id, pdu, room_id);
}
```

`add_pdu_outlier` acquires `mutex_insert` **internally** before writing.
`add_pdu_outlier_locked` isn't "the synchronized variant" — its own doc
comment says it's for callers that **already hold** the lock (e.g.
`force_state` invoked from inside `append_pdu`), since the mutex isn't
reentrant and re-locking from the same call stack would deadlock. Both
variants funnel into the same `add_pdu_outlier_inner` → `add_pdu_outlier_batch`
under the same lock either way. There is no TOCTOU gap on this surface.
Ruled out — don't re-raise this lead without new evidence.

## Candidate fix applied, unverified: `clear_outlier_flag` in the soft-fail branch

Applied to `src/service/rooms/timeline/append.rs`'s `append_incoming_pdu`,
staged, **not yet run against Complement**:

```diff
 	if soft_fail {
-		self.clear_outlier_flag(pdu.event_id());
 		self.services
 			.pdu_metadata
 			.unmark_event_rejected(pdu.event_id());
```

**Why it's a real bug independent of whether it's this bug's cause:**
`clear_outlier_flag`'s own doc comment: "This is used when an event is
promoted to or already exists in the timeline." The soft-fail branch does
neither — it returns `Ok(None)` without ever calling `append_pdu`. Calling
it there produces a third, undefined state: `is_outlier: false` but
`pdu_count: None` — not a real outlier, not a real timeline entry. Checked
every consumer of `EventMetadata.is_outlier` in the service layer;
`outlier_pdu_exists` (`data.rs:1153`) and `add_pdu_outlier_batch`'s "never
overwrite a timeline event" guard both trust `is_outlier: false` as a
reliable proxy for "safely in the timeline," which becomes false for
exactly this limbo state.

**What's NOT verified:** the exact causal link to the `/messages` stranding.
`get_pdu_id`'s early-return check (this doc's traced mechanism) keys off
`pdu_count`, not `is_outlier` — so on paper a retry of a soft-failed event
should still correctly fail that check and reprocess normally through
`append_pdu`, which unconditionally overwrites `is_outlier` with a fresh
correct value regardless of what a prior soft-fail left behind. Tracing
through every downstream consumer of the limbo state (`extremities.rs`'s
outlier/bridge classification turned out to self-heal via the `soft_failed`
flag, which `mark_event_soft_failed` sets correctly _before_ this branch
runs — checked, not an issue) did not turn up a confirmed path from this
bug to the exact symptom. It's applied because it's correct on its own
merits and low-risk (doesn't touch the get_pdu_id/topo-index authority
question the "what NOT to do" section warns about), not because the causal
chain is proven. **Needs a Complement run before treating this as fixing
the actual bug** — if `TestMessagesOverFederation`'s re-join case still
fails after this, the two remaining candidates (DAG-fork/depth correlation;
decision-side early-return race) are next.

## What `3e218c4ab` already fixed (separate, real, unaffected by the above)

`handle_prev_pdu`'s `None`-`eventid_info` branch (a _different_ silent-skip:
`fetch_prev` omits a `prev_id` from its result when `pdu_exists` — which
matches outlier-only events — already considered it "not needed", and the
old code read `None` as "nothing to do" instead of "might be outlier-only,
never promoted"). That fix is real, compiles and clippies clean, and
cleared `ubuntu-24.04` on both arches and both room versions on run
`31267318970`. It just doesn't touch this bug, since this bug's control
flow never reaches that branch.

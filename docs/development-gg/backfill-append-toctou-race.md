# `TestMessagesOverFederation` backfill gap-fill: investigation notes

Status: **unconfirmed, in progress**. This is a working log, not a fix
writeup — see `complement-failures.md` for the tracked failure entry this
supports.

> **Update:** hypothesis #2 below (`append_pdu` has no TOCTOU recheck) was
> the initial finding, written before a fix was drafted. It has since been
> addressed — `append_pdu` now takes `mutex_insert` and rechecks
> `non_outlier_pdu_exists` under the lock (`append.rs` around the
> `insert_lock` acquisition), mirroring `backfill_pdu`'s existing recheck.
> See "Audit of every `mutex_insert` call site" and "Draft fix status"
> below, which already reflect this; the hypothesis section itself is left
> as originally written for the investigation history, but should be read
> as superseded by those two sections, not as the current state of the
> code.

## Symptom

`TestMessagesOverFederation/Visible shared history after re-joining room
(backfill)/messagesRequestLimit is lower than the number of messages
backfilled` fails consistently (reproduced twice, same shape both times):
the timeline is short exactly one event — always the second-to-last
(oldest-but-one) message, always the one event that required a genuine
backfill gap-fill rather than being already known locally.

Both repros show the identical mechanism:

```
backfill: gap at $sn32CAG... (missing: ["$Jgq5bqxt..."])
backfill: Asking hs1 for backfill (extremities: ["$sn32CAG..."])
backfill: hs1 returned 27 events
state_res_debug: handle_outlier_pdu: early return, event already known event_id=$Jgq5bqxt...
backfill: no gaps (scanned 10 events from 807)
```

`$Jgq5bqxt...` (the missing gap-filler) is the *only* event in the 27-event
batch that logs the "already known" early-return branch — implying it was
already present as a bare **outlier** (not yet promoted to the timeline)
before this backfill call, most likely because Complement's join/state-res
flow independently discovered it while walking auth chains. Every other
event in the batch is silently skipped (debug-level only) as already fully
in the timeline. The batch-processing loop completes in ~1.5ms total, and
by the time `/messages` returns its next page, `$Jgq5bqxt...` is absent.

## Two live hypotheses, neither confirmed

### 1. `associate_current_state` failing silently

`backfill_pdu` (`src/service/rooms/timeline/backfill.rs`) does:

```rust
self.db.prepend_backfill_pdu(&pdu_id, &event_id, &json_value, &pdu_event).await;
self.associate_current_state(&room_id, &event_id).await?;
```

If `associate_current_state` errors, the `?` propagates up through
`backfill_pdu`'s `Result`, and the *only* caller
(`backfill_if_required`'s loop) does:

```rust
if let Err(e) = self.backfill_pdu(backfill_server, pdu, None).boxed().await {
    debug_warn!("Failed to add backfilled pdu in room {room_id}: {e}");
}
```

`debug_warn!` is invisible at INFO level — exactly why nothing in the
pasted Complement logs (INFO level) shows an error for this event even
though it never appears. Whether the raw `prepend_backfill_pdu` row alone
(without `associate_current_state` completing) is sufficient for
`/messages` to surface the event is untested.

**Temporary instrumentation added** (commit `e41dbee4c`, NOT meant to
ship as-is): raises the insert + `associate_current_state` steps in
`backfill_pdu` to `info!`/`warn!` so a Complement run will show directly
whether this specific event reaches the insert call and whether
`associate_current_state` errors.

### 2. `append_pdu` / `backfill_pdu` TOCTOU asymmetry

`backfill_pdu` defends against a concurrent normal `/send` racing the same
event: it checks `non_outlier_pdu_exists`, acquires the shared per-room
`mutex_insert` lock, then **rechecks** `non_outlier_pdu_exists` again
under the lock before inserting (`backfill.rs:598`, "Re-check after
acquiring insert lock to prevent TOCTOU races with concurrent /send
transactions").

`append_pdu`/`append_incoming_pdu` (`src/service/rooms/timeline/append.rs`)
— the normal `/send` insertion path used by `upgrade_outlier_pdu.rs` — has
**no existence check anywhere**, before or after acquiring that same
`mutex_insert` lock (grepped the whole file for
`pdu_exists`/`non_outlier_pdu_exists`: zero hits). It just takes the lock
and unconditionally calls `next_count()` + `db.append_pdu(...)`.

The protection is asymmetric. If `backfill_pdu` wins the lock race and
inserts an event as `Backfilled`, a concurrent normal `/send` delivering
that same event (plausible here — Complement's join/state-res flow can
independently discover and later re-deliver the same event) queues up
behind it, acquires the lock second, and — with no recheck — inserts it
*again* under a fresh `Normal` count. This is a confirmed *code* gap; it
is not yet confirmed to be *this* symptom's cause. It's unclear from
reading alone whether a duplicate insert under two different `PduCount`s
would manifest as a missing event (vs. a duplicate) without tracing the
`shorteventid`/`eventid_pduid` allocation path further.

## Ruled out

- **In-memory cache staleness**: `backfill_gap_free_cache` (the only
  cache touching this path) holds a boolean "is this room's tail
  gap-free", not event data, and is correctly invalidated right after
  every successful insert. `/messages`' read path
  (`all_pdus`/`pdus_rev`) iterates RocksDB directly with no cache layer.
  Ruled out as the mechanism.
- **WAL/flush-durability timing**: the failure is permanent within the
  same test run, confirmed seconds after the insert with no process
  restart — far longer than any plausible RocksDB WAL flush delay, and
  RocksDB reads are immediately consistent with prior writes on the same
  process handle regardless of WAL fsync state. Doesn't fit the timing.
- **Rejection-cache poisoning** (the originally-suspected mechanism,
  see below): disproven for this specific trace. The pre-fix
  `"early return, event already known"` log line is only reachable
  *after* `is_event_rejected` has already returned `false` — meaning
  `$Jgq5bqxt...` was accepted, not rejected, when backfill reached it.

## Related, independently-verified fixes made along the way (not the same bug)

While investigating, found and fixed a genuinely permanent, unrelated
defect: `handle_outlier_pdu`'s early-return gate treated every rejection
as permanent, including ones caused purely by insufficient context at
processing time (e.g. the MSC4499 single-hop `GET /event/{id}` fallback,
which runs with no state snapshot). Added
`take_retry_if_rejection_retryable` plus a typed `RejectionCode`
classification (`src/service/rooms/pdu_metadata/mod.rs`) so
resolution-failure rejections can be retried by a later, better-informed
caller while intrinsic ones (bad signature, failed auth check, cascading
from a permanently-rejected auth event) stay permanent. Commits
`68b44a9de`, `baec3f5a6`.

The second commit (`baec3f5a6`) fixed a real regression the first one
introduced: `DependsOnRejectedAuthEvent` was initially marked retryable,
which broke Complement's
`TestInboundFederationRejectsEventsWithRejectedAuthEvents` (a regression
test for matrix-org/synapse#9595) — cascading rejections from a
permanently-bad auth event must stay permanent, not be re-derived on
every touch. Synapse's own classification
(`synapse/api/constants.py::RejectedReason`) has no retry concept at all;
its two reasons (`AUTH_ERROR`, `OVERSIZED_EVENT`) are both terminal.

This work is unrelated to the `TestMessagesOverFederation` mechanism
above — confirmed by tracing the actual log line through the pre-fix
branch logic, not assumed.

## Audit of every `mutex_insert` call site

Grepped every acquisition of the shared per-room `mutex_insert` lock across
`src/service/rooms/timeline/` to check for the same asymmetry:

| Site | Recheck under lock? | Status |
| --- | --- | --- |
| `backfill_pdu` (`backfill.rs:596`) | Yes (pre-existing) | OK |
| `append_pdu` (`append.rs:247`) | **No** | **Fixed** (draft, commit `b2cc8fde0`) |
| `promote_outlier` (`backfill.rs:647`) | **No** | **Fixed** (this commit) |
| `force_insert_pdu` (`backfill.rs:822`) | **No** | **Fixed** (this commit) |
| `reorder_timeline` (`reorder.rs:36`) | N/A | Not vulnerable — rebuilds the topo index over an already-collected entry set, not a check-then-insert of one incoming event |

All three real gaps now get the same recheck-after-lock treatment
`backfill_pdu` already had. None of these fixes are validated against the
actual failing Complement test yet — see "Next steps" below.

## Draft fix status

- `append_pdu`: recheck added (commit `b2cc8fde0`, updated in working tree). Initially, this draft skipped the rest of the function on collision. A newer iteration resolves this: it skips the redundant DB insert but correctly continues execution to process `force_state`, push-rule evaluation, and auth-chain-caching, mitigating the risk of skipped state changes.
- `promote_outlier`, `force_insert_pdu`: recheck added, simple no-op /
  error return on collision — lower risk than `append_pdu`'s case since
  neither of these has the force_state/push-rule side-effect concern.

## Next steps

1. Debug-profile build (`make build PROFILE=dev` or equivalent) with the
   `backfill_pdu` instrumentation from `e41dbee4c`.
2. `just complement TestMessagesOverFederation` (or the wrapper script),
   capture whether `$Jgq5bqxt...`-equivalent reaches
   `"backfill_pdu: about to insert"` / `"insert complete"` /
   `"associate_current_state FAILED"`.
3. ~~If it inserts cleanly, add an existence check (or at minimum a
   duplicate-detection log) to `append_pdu` mirroring `backfill_pdu`'s
   TOCTOU recheck~~ — done (see "Draft fix status" above); still worth
   looking for a concurrent `/send` racing the same event_id in the logs
   around the same timestamp to confirm it was actually this mechanism.
4. Revert or downgrade the temporary `info!`/`warn!` instrumentation in
   `backfill.rs` back to `debug!` once root-caused.

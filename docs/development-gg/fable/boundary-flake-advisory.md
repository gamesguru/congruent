# Advisory: `pdus_rev` boundary regression — flakiness explained, hardening plan

Answering the four questions in `writeup.md`, in the order that matters. Short form:

1. The revert is correct; the wrapper is adequate **only as a stopgap** — the structural
   problem is that `pdus` and `pdus_rev` have **opposite** inclusion conventions, and the
   wrapper fixes one of them at two call sites. Bound-style API: yes. (§2)
2. The intermittency explanation is **right at the top level, imprecise in its mechanism** —
   and the imprecision matters, because the true mechanism (global-counter interleaving
   between parallel subtests) is checkable and the stated one ("tokens one PduCount apart")
   is not literally true. No race in the budget loop is needed to explain the flip. (§1)
3. The scan-and-rebuild loop is a genuine load problem under real federation, independent of
   the correctness bug. The long-term fix is the one Synapse landed on years ago: persist
   backward extremities at insert time instead of rediscovering them by scan at read time. (§3)
4. Yes to unit tests, with a specific list — including two boundary cases the wrapper itself
   introduces. (§4)

One meta-observation first, because it reframes question 1: **this is the third flip of the
same line.** `94d3c8558` (July 26) removed the increment; it was reverted after log analysis
showed the tree didn't match any commit; `f1415e22a` (July 27) removed it again, with a comment
asserting the opposite of the docstring three lines away; `d3c802f6f` reverted it again. Two
independent authors have now made the identical mistake in the identical spot within 48 hours.
That is no longer evidence about the authors; it is evidence about the API.

---

## 1. Why it flaked: the mechanism, corrected

### What the writeup gets right

The bug is deterministic; the _visible symptom_ is run-dependent; therefore CI triage saw
"flaky" where the code was consistently wrong. That conclusion is correct and is the important
one. The evidence is strong: three different test families, one shared root cause, and
`TestMessagesPaginationStress` failing with the **same event ID at every limit** (1, 3, 7, 50)
is exactly the signature of a structural off-by-one and exactly not the signature of a race.

### Where the stated mechanism is imprecise

> "they request `/context` tokens exactly one `PduCount` apart"

Not literally true, on two counts, and the correct version is _stronger_ for your case:

**(a) The tokens are event tokens, not arithmetic neighbors.** `context.rs` derives them as
`start = events_before.last() token` and `end = events_after.last() token` (falling back to
`base_token`). They are the counts of two _different real events_, not `base ± 1`.

**(b) With a global counter, no two events in one room are reliably adjacent.**
`prepare_pdu_insert` and `append_pdu` draw from `globals.next_count()` — shared across every
room on the server. `TestJumpToDateEndpoint` runs its subtests under `t.Parallel()`, each in
its own room, against one homeserver, while other test families run concurrently. So the
counts assigned inside any one room are interleaved with every other room's traffic, and the
numeric gaps between consecutive events _in the same room_ vary run to run.

### The corrected mechanism

The exclusive boundary only bites when **an event of this room sits exactly at the requested
`from` count**. Pagination tokens are derived from real events (point (a)), so the event at
`from` always exists — the question is _which_ event it is, and whether that event is the one
carrying the dangling `prev_events` pointer that the gap scan must see.

Per-run global-counter interleaving (point (b)), plus the insertion order of the join/backfill
sequence (each prepend draws a fresh global count, so backfilled events' relative counts depend
on when they were inserted relative to the other rooms' traffic), determines the count layout
of each subtest's room. Same topology, different layout each run. The off-by-one always drops
exactly one event from the scan window; which _assertion_ notices depends on whether the
dropped event is eventA's child or eventB's child in that run's layout. Hence `(start)` and
`(end)` trading places across the 22:08 and 01:17 runs, with different missing event IDs —
both observations are exactly what this predicts.

**No nondeterminism in `backfill_if_required` is required to explain the flip.** The
sensitivity lives entirely in token/count assignment upstream of it.

### But you asked whether the loop is _also_ nondeterministic — it is, in three benign-until-they-aren't ways

Since the question was explicitly "is the model explaining this away too neatly," here is what
is genuinely nondeterministic in the loop, and why none of it explains these failures but two
of them deserve fixes anyway:

1. **`event_map` is a `HashMap`; `gaps` ordering follows its iteration order.** The
   `/backfill` request's `extremities` vec therefore varies per run. Remotes walk their own
   DAG from those extremities, so the _set_ of returned events is stable, but the insertion
   order of the response can vary → different prepend counts → different backfilled layout.
   This feeds mechanism (b) above; it does not independently break anything, but it makes runs
   non-reproducible. Sort `backwards_extremities` before building the request. One line,
   and your runs get more deterministic for free.
2. **No singleflight on `backfill_if_required`.** Two concurrent backward `/messages` on the
   same room both scan, both detect the same gap, both fire `/backfill` at the remote, and
   both insert the response. Whether the second insert is safely idempotent depends on the
   existence check inside the prepend path — verify it, because a double prepend would assign
   the same event **two different Backfilled counts**, which is a duplicate-event bug that
   would look _exactly_ like this class of flake. Complement's `t.Parallel()` makes this
   window real, not theoretical. A per-room async mutex or singleflight around the whole
   gap-scan-and-fill closes it.
3. **The budget loop's stopping point** depends on how many gaps each `/backfill` response
   resolves, which depends on the remote's walk. Bounded and fine, but it means "how much
   history exists after one `/messages` call" is not a constant — worth remembering when a
   test asserts exact counts (the `TestMessagesOverFederation` 300-event assertion is
   implicitly sensitive to this; it only passes because the loop re-scans until no gaps within
   budget 5).

### Verifying the explanation instead of trusting it

The writeup is candid that the explanation "has not been independently verified." It can be,
cheaply, without forcing runs deterministic: write the unit test in §4.3 that constructs a
room where the dangling-prev event sits exactly at `from`, run it against the pre-revert code
(one `git stash` of the fix), and watch it fail; run against post-revert and watch it pass.
That converts the explanation from "consistent with the logs" to "reproduced and killed."
Fifteen minutes, and it is also the regression guard for flip number four.

---

## 2. Wrapper vs. structural enforcement — the writeup undersells its own case

`pdus_rev_inclusive` is correct as far as it goes. Here is why it does not go far enough, from
the `data.rs` excerpt you attached:

```
pdus_rev:  EXCLUSIVE of `until`. To include it, pass until.saturating_inc(Forward).   // +1
pdus:      EXCLUSIVE of `from`.  To include it, pass from.saturating_inc(Backward).   // −1
```

**The two primitives require opposite-sign adjustments for the same intent.** A developer who
correctly learns "inclusive means +1" from the `pdus_rev` fix will then write a latent bug at
any `pdus` call site — silently, skipping one event, in whatever feature they're building. The
wrapper fixed one convention at two call sites; the trap is the _pair_.

And the wrapper is advisory: `pdus_rev` remains callable directly (members.rs was migrated,
but nothing stops the next call site), and the comment-based warning regime has now failed
twice with the docstring _three lines from the edit_.

### Recommended shape

```rust
// core/src/matrix/pdu/mod.rs (or wherever PduCount lives)
#[derive(Clone, Copy, Debug)]
pub enum From {
    Inclusive(PduCount),
    Exclusive(PduCount),
}

impl From {
    /// Resolve to the raw seek count for a reverse (older-first) scan.
    fn seek_rev(self) -> PduCount {
        match self {
            Self::Exclusive(c) => c,
            Self::Inclusive(c) => c.saturating_inc(Direction::Forward),
        }
    }
    /// Resolve for a forward scan — note the opposite adjustment.
    fn seek_fwd(self) -> PduCount {
        match self {
            Self::Exclusive(c) => c,
            Self::Inclusive(c) => c.saturating_inc(Direction::Backward),
        }
    }
}
```

Then:

- `pdus_rev(room_id, from: From)` and `pdus(room_id, from: From)` — the adjustment lives in
  **one** place per direction, next to the seek it modifies, and every call site is forced to
  state its intent in the signature. `grep 'Inclusive('` finds every boundary-including caller
  forever.
- Delete `pdus_rev_inclusive` once migrated — it was the right 30-minute fix and is the wrong
  permanent one; keeping both means two ways to say the same thing.
- Make the raw exclusive-seek variants `pub(super)` (they already are at the data layer — the
  service-layer re-exports in `timeline/mod.rs` are the leak; narrow those).
- `topo_pdus_rev` takes the same treatment. Its exclusive boundary is _correct_ for its two
  current callers (`context.rs` events_before/events_after and `/messages` pagination — the
  Matrix spec makes pagination tokens exclusive), which is exactly why the parameter should say
  `From::Exclusive(base_token)` out loud: the next reader learns the spec constraint from the
  call site instead of re-deriving it.

Mirroring `std::ops::Bound` (per the writeup's own suggestion) is also fine; a domain-specific
two-variant enum is marginally better here because `Bound::Unbounded` is not meaningful for
these seeks and you don't want to handle it.

**Do not stop at the wrapper.** The recurrence rate — twice in 48 hours, two authors, plus the
pre-existing defensive comment in `members.rs` proving a third near-miss — is the empirical
answer to "is a well-named method enough."

### One latent bug inside the wrapper itself

`saturating_inc(Direction::Forward)` on a `PduCount` has an edge the wrapper inherits: what is
`Backfilled(-1) + 1`? If the answer is `Backfilled(0)` and `Normal(0)` sorts elsewhere, an
inclusive scan anchored on the newest backfilled event may skip across the class boundary
incorrectly. Your failing run's tokens (`t7_-114`, `end t1_-78`) show these scans routinely
operate in the backfilled range, so this is not hypothetical territory. I cannot resolve it
from the excerpt — `PduCount`'s increment impl isn't attached — but §4's test list includes the
case that answers it. If the increment does cross classes wrongly, the `From` enum's
`seek_rev` is the single place to fix it, which is one more argument for centralizing.

---

## 3. Performance: the scan is a read-amplification bug waiting for a big room

Per backward `/messages` call, `backfill_if_required` currently does, per loop iteration
(budget 5):

- `pdus_rev_inclusive(...).take(limit)` — up to `limit.clamp(100,500)` full PDU resolves
  (RocksDB point reads + JSON deserialization each);
- `get_pdu_id` existence probes for every distinct `prev_event` not in the scanned map —
  roughly 1–2× the scan size in additional point reads;
- a fresh `HashMap` build and a rezzy extremity computation.

Steady state (no gaps — the overwhelmingly common case) costs one full iteration of that
before the request is served, **added to first-byte latency of every backward pagination**.
A Matrix client opening a room scrollback issues these in bursts; N concurrent clients on one
large room means N independent scans of the same window with zero sharing (see §1.2 — no
singleflight either). This is fine in Complement-sized rooms and will not be fine in a 50k-event
room with an active user base.

Tiering the fix:

1. **Now (bundled with the singleflight from §1.2):** per-room dedup so concurrent paginations
   share one scan. Also skip the scan entirely when `from` is `PduCount::max()`/fresh-token and
   the room has no remote servers — the cheap checks already at the top of the function are
   good; make them cover more of the common path.
2. **Soon:** a per-room negative-result cache — "scanned window (from, from+limit) at
   generation G, no gaps" — invalidated by any timeline insert in the room (you already have
   an insert mutex per room to hang the generation counter on). Turns the steady state into a
   map lookup.
3. **Right:** persist backward extremities at **write** time. When inserting any timeline
   event, if a `prev_event` is absent from the timeline, record `(room_id, missing_id,
child_id)` in a backward-extremities table; remove entries when the missing event arrives.
   `backfill_if_required` becomes "any extremities in range? if not, return" — one indexed
   read. This is Synapse's design and it exists because Synapse hit exactly this
   read-amplification wall. It also _deletes the boundary-scan code entirely_, which — given
   this file's history — is the most durable fix of all: the third regression cannot happen in
   code that no longer exists.

Option 3 is a schema change and belongs in its own commit series with a migration; don't
bundle it with the correctness work. But put it on the roadmap now, because §1's analysis
shows the scan is also the _fragile_ part, not just the slow part.

---

## 4. Tests: yes, and here is the exact list

The writeup's framing is right: the only detector for this bug class today is a slow
integration suite whose failures _look_ environmental (§1 explains why). That is the worst
possible detector for a deterministic off-by-one. The unit tests are small because the
primitives are small:

1. **`pdus_rev` boundary:** three events at known counts; `From::Exclusive(mid)` yields
   exactly the older set; `From::Inclusive(mid)` yields mid plus the older set. Same pair for
   `pdus` in the forward direction — this single test is the one that would have caught both
   `94d3c8558` and `f1415e22a` in milliseconds, and documents the opposite-sign trap
   permanently.
2. **Sparse counts:** events at counts {10, 17, 40} (simulating global-counter interleaving);
   `Inclusive(17)` and `Exclusive(17)` behave; then the same anchored at a count that does
   **not** exist in the room (e.g. 20) — asserting the seek rounds in the documented
   direction. This encodes §1's mechanism as a test.
3. **The regression:** minimal room where the event at exactly `from` carries a `prev_events`
   pointer to an absent event; assert `backfill_if_required`'s gap detection reports it. Run
   once against the broken revision to prove it bites (see §1's verification note), then keep
   it. This is the test that makes flip number four impossible to land.
4. **Class-boundary increments:** `Inclusive(Backfilled(-1))`, `Inclusive(Normal(0))`,
   `Exclusive(Backfilled(min))`, `Inclusive(PduCount::max())` — pin the saturation and
   class-crossing behavior of the adjustment itself (§2's latent-bug question, answered in
   CI forever).
5. **`topo_pdus_rev` legacy-token path:** the excerpt shows it has a separate "old buggy
   behavior" branch for legacy tokens — that branch has the same shape of seek arithmetic and
   currently zero direct coverage. One test per branch.
6. **Idempotent backfill insert:** insert the same event through the prepend path twice;
   assert one timeline entry and one count (§1.2's double-insert question, pinned).

All of these run without federation, without containers, in milliseconds. Gate merges on them.
Keep Complement as the acceptance layer it is good at, and stop using it as the unit-test
layer it is bad at — that mismatch, more than any individual mistake, is what turned a
one-line off-by-one into three test families flaking across two days and (counting this
conversation's earlier rounds) five separate fix attempts.

---

## 5. Answers in one line each, for the commit message

1. Revert correct; wrapper adequate only until the `From::Inclusive/Exclusive` parameter lands
   on both `pdus` and `pdus_rev` (opposite-sign conventions are the real trap); raw variants go
   private.
2. Explanation directionally right; true mechanism is global-counter interleaving across
   parallel rooms deciding _which_ event sits at the excluded boundary — no budget-loop race
   needed, but sort the extremities vec, add per-room singleflight, and verify prepend
   idempotency anyway.
3. Real problem; short-term singleflight + skip-fast paths, long-term persist backward
   extremities at write time and delete the scan (which also permanently retires this bug
   class).
4. Yes; six tests listed, one of which (`#3`) should be demonstrated failing on the broken
   revision before it is merged as the guard.

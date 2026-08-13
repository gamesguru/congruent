# Gap-recovery coordinator: design sketch (not implemented)

Status: **design sketch only**. Nothing in this doc is implemented or
scheduled. It exists so the shape of a future consolidation is written down
before anyone starts it, per the sequencing discussion in this session: land
test coverage for the current three paths and a real Complement baseline
first, then use this as the starting point for the actual refactor — not the
other way around. See `federation-fetch-paths.md` for the original diagnosis
this responds to, and `event-auth-fallback-vs-synapse.md` for the specific
auth-gap fix that prompted it.

## Current state: three independent policies

Four call sites make their own, independently-arrived-at decisions about how
to recover from a federation gap. None of them agree with each other on all
axes:

|                          | `fetch_and_handle_outliers.rs`                                                                                                                                     | `fetch_prev.rs`                                                          | `fetch_state.rs`                                                                                                                                                                    | `resolve_missing_outlier_auth_events` (`handle_outlier_pdu.rs`) |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| Server selection         | `build_federation_server_list` + manual shuffle/truncate(4)                                                                                                        | `build_federation_server_list_with_sender` (adds event-sender server)    | `build_server_pool` — the function's own doc comment calls this **"the preferred entry point for any federation operation that needs multi-server rotation with cooldown/backoff"** | none — queries `origin` only                                    |
| Bulk vs individual fetch | bulk `/event_auth` first, individual `/event/{id}` fallback (tier 2, fixed this session)                                                                           | individual only (no fallback on failure — reverted to that this session) | individual `/event/{id}` only, via `/state_ids` ladder                                                                                                                              | bulk `/event_auth` only, size-gated at `MAX_INLINE_FETCH<=5`    |
| Retry/backoff            | per-request retry-with-backoff inside `push_generic_fetch!`, plus a bounded 3-tier state machine (`auth_chain_fetched` → `individually_fetch_requested` → give up) | none                                                                     | scored server pool with cooldown, but no tiered retry on the gap itself                                                                                                             | none — one shot, defers via `Err(MissingAuthEvents)`            |
| Ratelimit integration    | `bad_event_ratelimiter`, checked before initial fetch only                                                                                                         | none                                                                     | none visible in the excerpt reviewed                                                                                                                                                | none                                                            |
| Rejection bookkeeping    | `mark_event_rejected` with `RejectionCode::MissingAuthEvent`, bounded-size detail string                                                                           | none — caller (`fetch_prev`) just logs and drops                         | not reviewed this session                                                                                                                                                           | `add_pdu_outlier`, no rejection code set                        |

That table is the actual argument for a coordinator: the same conceptual
problem (an event references IDs we don't have) gets four different answers
depending on which code path happened to hit it first. `fetch_state.rs`
already has the "preferred" primitive (`build_server_pool`) that the other
two fetch-heavy paths don't use.

## Proposed shape

A single async fn/struct that all four sites call into instead of hand-rolling
their own fetch loops:

```rust
/// Owns the *policy* for recovering from a federation gap: which servers to
/// ask, bulk-vs-individual, retry budget, ratelimit checks, and what to do
/// when recovery is exhausted. Callers describe *what* is missing; this
/// decides *how hard* to try and *in what order*.
pub struct GapRecovery<'a> {
    room_id: &'a RoomId,
    origin: &'a ServerName,
    event_sender: Option<&'a ServerName>,
    create_event: Option<&'a dyn Event>,
    room_version: &'a RoomVersionId,
}

pub enum GapKind {
    /// Missing `auth_events` for an already-fetched PDU. Bulk `/event_auth`
    /// is viable (the full chain is well-defined); size-gate is a policy
    /// decision the coordinator owns, not each caller.
    AuthEvents { for_event: OwnedEventId, missing: Vec<OwnedEventId> },
    /// Missing `prev_events` — no bulk primitive exists for this in the
    /// federation API; individual fetch (or `/get_missing_events`) only.
    PrevEvents { missing: Vec<OwnedEventId> },
    /// Missing state constituents surfaced by a `/state_ids` response.
    StateConstituents { missing: Vec<OwnedEventId> },
}

pub enum GapOutcome {
    Resolved(Vec<(PduEvent, CanonicalJsonObject)>),
    Partial { resolved: Vec<...>, still_missing: Vec<OwnedEventId> },
    Exhausted { still_missing: Vec<OwnedEventId> },
}

impl GapRecovery<'_> {
    pub async fn recover(&self, gap: GapKind) -> GapOutcome { ... }
}
```

Internally `recover()` owns exactly the things the table above shows
diverging today:

1. Server list construction — always `build_server_pool`, so every caller
   gets cooldown/rotation, not just `fetch_state`.
2. Bulk-vs-individual choice, including the `MAX_INLINE_FETCH` cutoff — one
   constant, one place, instead of duplicated/absent per call site. (Whether
   that cutoff should exist at all is the Synapse-divergence question from
   `event-auth-fallback-vs-synapse.md` — this doc doesn't relitigate that; it
   just says there should be one place to change it if that's ever decided.)
3. The tiered retry state machine currently living only in
   `fetch_and_handle_outliers.rs` (`auth_chain_fetched` /
   `individually_fetch_requested`), generalized to all `GapKind`s.
4. Ratelimit checks (`bad_event_ratelimiter`) before every attempt, not just
   the first one in one of four call sites.
5. One `mark_event_rejected` / `add_pdu_outlier` exit path, so rejection
   reasons are consistently recorded regardless of which caller hit the gap.

Callers become adapters: `fetch_prev` still does its GME-response fetch and
topological sort, but when `handle_outlier_pdu` returns
`Err(MissingAuthEvents)`, it calls `GapRecovery::recover(AuthEvents { .. })`
instead of giving up (or instead of hand-rolling the
`fetch_and_handle_outliers` call the reverted `fetch_prev` patch did this
session). `fetch_and_handle_outliers`'s own fetch loop becomes largely
`GapRecovery` plus the outer topological-sort/suspend bookkeeping it already
needs for its own initially-requested events. `fetch_state`'s `/state_ids`
ladder becomes a `StateConstituents` gap handed to the same coordinator.

## Migration order, safest first

Doing this in one shot means changing every gap-recovery code path at once —
worst possible blast radius for a change nobody has regression tests for yet.
Suggested order, each step shippable and independently revertable:

1. **Characterization tests first** (already proposed, not yet done): one
   Complement test per path that forces it through its current recovery
   behavior, so there's a baseline to refactor against. This is the
   prerequisite, not step 1 of the refactor itself.
2. **Extract, don't yet merge.** Pull `fetch_and_handle_outliers.rs`'s
   3-tier state machine into a standalone `GapRecovery`-shaped function,
   called from exactly the one site it's called from today. No behavior
   change, just a boundary — proves the API shape works before anyone else
   depends on it.
3. **Migrate `fetch_state.rs` second** — it already uses `build_server_pool`
   and already does individual fetch, so its migration is closer to
   "rename the call" than "change the policy." Lowest-risk second mover.
4. **Migrate `fetch_prev.rs` last** — it's the one with zero fallback today,
   the one two sessions already collided on this evening, and the one whose
   correct behavior on `TestCorruptedAuthChain` depends on _not_ trying
   harder in that specific case. Needs the most care and the most explicit
   test coverage before it changes.
5. **Only then** revisit whether `MAX_INLINE_FETCH` should still exist, now
   that there's one place to change it instead of four.

## Explicit non-goals

- This is not a proposal to change the bulk-`/event_auth`-first policy,
  the `TestCorruptedAuthChain` outcome, or anything behavioral. Extracting a
  shared coordinator should be behavior-preserving by construction (each
  migrated call site keeps its current policy as the coordinator's default
  for that `GapKind`, verified against the characterization tests from step
    1. — policy changes are a separate, later decision.
- Not a redesign of `resolve_missing_outlier_auth_events`'s inline bulk
  fetch — that one stays as-is per the earlier decision in
  `event-auth-fallback-vs-synapse.md`; it's listed in the table for
  completeness, not as a near-term migration target.

# `/event_auth` fallback: `TestCorruptedAuthChain` fix + Synapse design-divergence note

Status: `TestCorruptedAuthChain` fix applied (complement-src mock only, unverified —
no Complement run performed). Logging change applied and low-risk (main repo).
Synapse comparison below is recorded as a **design-divergence / tradeoff note,
not a proven root cause of any bug**. Decision: keep the current resolution
algorithm as-is; do not change it on the back of this comparison alone.

## What broke

`TestCorruptedAuthChain` (`complement-src/tests/federation_room_get_missing_events_test.go`)
failed locally with:

```
Server.UnexpectedRequestsAreErrors=false received unexpected request to server:
GET /_matrix/federation/v1/event_auth/!1HWBnYMn0mDTgasYfl:hs1/$il_W700BYZbhDRS1VNwqhZVgU0Uca_Za899slNm6OzI
- sending 404 which may cause the HS to backoff from Complement
...
fetch_prev: Failed to handle outlier: $il_W700BYZbhDRS1VNwqhZVgU0Uca_Za899slNm6OzI
Prev $il_W700BYZbhDRS1VNwqhZVgU0Uca_Za899slNm6OzI failed: M_FORBIDDEN:
Event depends on missing auth event $NToqqmd83UH8M4nrxagnqMuw4N_i_OJe_fnwFHW99GY
--- FAIL: TestCorruptedAuthChain (11.45s)
```

Live log confirms our server's own request:

```
state_res_debug: Fetching missing auth events via /event_auth event_id=$il_W700... count=1
...
Still missing 1 auth events for $il_W700... after /event_auth: ["$NToqqmd83..."]
```

Root cause: the test's mock federation server never registered a handler for
`/event_auth` at all (`srv := federation.NewServer(t, deployment, federation.HandleKeyRequests(), federation.HandleMakeSendJoinRequests(), federation.HandleTransactionRequests(nil, nil), federation.HandleInviteRequests(nil))`
— no `HandleEventAuthRequests()`). Any request to that endpoint hit Complement's
generic "unexpected request" 404 path, which our server treats as a hard
failure rather than "the endpoint isn't available, fall back."

Traced via `complement-src` history: `HandleEventAuthRequests()` support for
this test was added in `5f12dce`, then reverted in `89d28cf`. Re-applied as
`9debb8c` ("Reapply 'add `/event_auth` support for corrupted auth chain
case'"), which:

- registers `federation.HandleEventAuthRequests()` on the mock server, and
- adds `eventB` to `srvRoom.Timeline` so the mock's `/event_auth` handler
  (which resolves the requested event's auth chain by looking it up in
  `room.Timeline` — `federation/handle.go:334`) can serve it.

Caveat, unverified: the mock handler looks up the _requested_ event ID (per
the log, whichever event maps to `$il_W700...`) in `Timeline`, not just its
auth events. `MustCreateEvent` never auto-adds events to `Timeline` — only
explicit `AddEvent`/manual-append calls do. This patch only explicitly adds
`eventB`. Whether the event actually named in our `/event_auth` request is
itself reachable via `Timeline` has not been traced end-to-end in a live run.
Flagging this rather than claiming the fix is complete.

## Logging change (main repo, applied)

`src/service/rooms/event_handler/handle_outlier_pdu.rs:566` — bumped the
`/event_auth` fallback log from `info!` to `warn!`, added the list of missing
auth event IDs, and a comment explaining why: this is the last-resort fallback
after a local (timeline + outlier store) lookup came up short, and it's the
single federation request most likely to be unhandled or behave differently
across implementations. Observability-only change, no behavior change.

## How our resolution algorithm works

`resolve_missing_outlier_auth_events` (`handle_outlier_pdu.rs`), reached from
`handle_outlier_pdu` whenever an outlier PDU's claimed `auth_events` aren't
all found locally:

```rust
const MAX_INLINE_FETCH: usize = 5;
```

- `missing_auth_events.len() <= 5` → one bulk `GET /event_auth` call, inline,
  synchronously, for the event's full auth chain.
- `missing_auth_events.len() > 5` → skip the fetch entirely, return
  `Err(MissingAuthEvents)`, defer resolution to the async `/state_ids`-driven
  retry ("healer") path.

Neither branch does per-event `GET /event/{id}` fetches for missing auth
events. (A different mechanism — a single-hop per-ID `/event` fetch — exists
in `upgrade_outlier_pdu.rs` for missing `prev_events`, not `auth_events`.)

## Synapse comparison (design-divergence note, not a proven root cause)

Synapse's direct analog, `_load_or_fetch_auth_events_for_event` →
`_get_remote_auth_chain_for_event` (`synapse/handlers/federation_event.py:2130+`),
has **no size-based cutoff**: it always issues one inline `/event_auth` call
whenever any auth events are missing, regardless of count. If the request
itself fails (`RequestSendFailed`), it logs at `info` and lets the still-missing
set flow through to an `AuthError(FORBIDDEN, "Auth events could not be found")`
a few lines later — no async retry queue for this specific gap.

Separately, Synapse's `/state_ids`-triggered gap-recovery path
(`_get_state_ids_after_missing_prev_event`, `federation_event.py:1252`) computes
a combined `missing_event_ids = missing_desired_event_ids | missing_auth_event_ids`
from the `/state_ids` response and fetches them via `_get_events_and_persist`
— **individual `GET /event/{id}` calls per missing ID** (or, above a 10%-missing
heuristic, a full `/state` fetch instead). This is the path actually exercised
by `TestCorruptedAuthChain`: Synapse asks for A, B, C, D individually, B 404s,
and the failure is precisely attributable, so C/D/E are correctly not
persisted — matching the test's intent. `/event_auth` is never invoked for this
test on the Synapse side.

Tradeoffs, for the record:

- **Round trips**: bulk `/event_auth` (ours) is O(1) requests vs. Synapse's
  O(N) individual fetches for the same gap — a real win when N is more than a
  couple and network latency dominates.
- **Payload size**: `/event_auth` returns the _entire_ auth chain
  unconditionally, not just what's missing. `MAX_INLINE_FETCH=5` assumes "few
  missing → small total chain," which doesn't hold for an established room
  with a large total auth chain and only a small local gap (Synapse's own
  comment on this code cites a real example: ~200k events for
  `#matrix:matrix.org`). In that shape, individual fetches pay only for what's
  missing; ours pays for the whole chain.
- **Fragility**: individual fetches degrade gracefully — each ID is
  independently retryable and a failure is precisely attributable. Bulk
  `/event_auth` is all-or-nothing at the transport level: an unimplemented,
  erroring, or timed-out endpoint loses the entire batch, with no partial
  credit. This test file itself documents `/event_auth` as an interop-risk
  endpoint — it explicitly skips Dendrite because _"Dendrite doesn't make
  exactly the same requests as it seems to fallback to /event_auth."_
- **Determinism**: our `>5` branch introduces a second, timing-sensitive
  resolution path (the async `/state_ids` healer, which elsewhere in this
  codebase is noted to race against a remote server's lifetime in Complement
  tests). Synapse has one deterministic mechanism per context and doesn't
  bail-and-retry-later for this gap class.

## Decision

Keep the current inline `/event_auth` fast path in
`resolve_missing_outlier_auth_events` as the first attempt — this is a
design divergence worth knowing about, not a demonstrated bug.
`TestCorruptedAuthChain` only has 1 missing auth event, so it always takes
our inline `<=5` path regardless of this comparison; the actual failure was
the missing mock handler registration, not the choice of resolution
strategy.

**Update:** the additive per-event fallback was implemented, but _not_
inline inside `resolve_missing_outlier_auth_events` (an earlier attempt at
that, commit `aa22c268a`, was reverted — it recursed back into
`handle_outlier_pdu` per missing event with no ratelimiting and only
`origin` as a source, duplicating and bypassing the retry/backoff
machinery `fetch_and_handle_outliers` already has, on a function that was
specifically split three days prior to reduce stack use). Instead, the
fallback now lives in `fetch_and_handle_outliers`'s existing suspend/retry
loop, as a second tier after the bulk `/event_auth` retry: individual
`GET /event/{id}` fetches for whatever is still missing, via the same
`push_fetch` used for every other event fetch (multi-server routing list,
ratelimiting, retry-with-backoff already built in). See
`federation-fetch-paths.md` for the tiering. This still did not have a
reproducible failing case backing it — it remains unverified against a
live Complement run — but it's structured additively (bulk stays the fast
path) and reuses existing, already-tuned recovery infrastructure rather
than adding a new one.

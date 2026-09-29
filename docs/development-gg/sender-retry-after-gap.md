# Federation Sender: Missing `retry_after`/`M_LIMIT_EXCEEDED` Handling

## The gap

When a federation destination responds to a transaction with `429 Too Many
Requests` (`M_LIMIT_EXCEEDED`), the Matrix spec lets that response optionally
include how long we should wait before retrying (`retry_after_ms` in the
response body, or a `Retry-After` HTTP header — ruma surfaces both as
`ErrorKind::LimitExceeded { retry_after: Option<RetryAfter> }` on
`ruma::api::client::error::Error`).

No branch we've checked actually reads this. Every 429 gets the same generic
`base * 2^tries` capped-exponential backoff as any other transient failure,
computed in `handle_response_err` (`src/service/sending/sender.rs`). If the
remote's requested wait exceeds what our formula happens to produce for the
current `tries` count (e.g. remote asks for 60s, our formula only waits 2s at
`tries=1`), we retry sooner than the remote permitted — which can trigger
another 429, or worse, get us treated as abusive.

Checked and confirmed absent (searched sender.rs on each for
`retry_after`/`RetryAfter`/`LimitExceeded`/`error_kind`, zero matches unless
noted):

- `guru/hotfix/new-features-tests-complement-fails` (this branch) — absent.
  A `retry_after_delay()` helper was written and committed here
  (`1bbb06111`) that respects `retry_after` for the _scheduled_ `Flush` delay,
  but it doesn't gate `select_events_current`'s independent backoff check, so
  a new PDU/EDU arriving for the same destination before the remote's
  requested wait elapses can still trigger a premature send. Flagged by
  `cubic-dev-ai` PR review; left unresolved by explicit decision (see below).
- `guru/dev-2026-03-27+b1-presence+b2-federation` (PR #47) — absent entirely.
  This branch instead bounds retries generally via `sender_retry_max_attempts`
  (default 32) + a `dead_servers` blacklist (cleared on next success), which
  solves "don't retry a permanently broken destination forever" but says
  nothing about respecting a temporarily-rate-limited destination's requested
  wait.
- `guru/fix/complement-exp-backoff-logic` (PR #60, open since 2026-05-06,
  unmerged, has an unresolved merge conflict per its own checklist) — absent.
  This is the origin of the `base * 2^tries` exponential-backoff +
  `reschedule_flush` mechanism itself (predates the 429-retryable and
  `retry_after` work by ~2 months); it doesn't special-case any status code.

## Why it's still open

Doing this properly means storing the server-provided minimum wait somewhere
`select_events_current` can see it — `TransactionStatus::Failed` currently
only carries `(tries, Instant)`. The fix discussed but not implemented:
extend it to `Failed(u32, Instant, Option<Duration>)` (tries, last-failure
time, server-provided floor), and have `select_events_current`'s gate check
`elapsed < max(formula_backoff, floor)` instead of only the formula.

Explicitly deferred on 2026-07-13: the narrower, already-committed fix
(respecting `retry_after` for the proactively-scheduled `Flush`) was judged
enough for now, with the full `TransactionStatus` threading left as a known,
low-probability race (only matters if a _new_ event for the same destination
arrives mid-backoff) rather than a functional break.

## If picking this up later

1. Extend `TransactionStatus::Failed` to carry the optional server-provided
   floor duration, set from `retry_after_delay(e)` in `handle_response_err`.
2. In `select_events_current`, gate on `max(continue_exponential_backoff_secs(...), floor)`
   instead of the formula alone.
3. Update every match arm that constructs/destructures `Failed(...)` in
   `src/service/sending/sender.rs` (as of this writing: `handle_response_err`
   and `select_events_current` are the only two).

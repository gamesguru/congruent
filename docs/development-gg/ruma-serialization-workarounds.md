# Ruma Serialization Workarounds

Catalog of places where we patch a response's JSON body _after_ it's built,
instead of relying on ruma's (or the vendored `ruwuma` fork's) derived
`Serialize` impl, because that impl drops or reshapes data in ways that
don't match spec-test expectations. In every case here the vendored crate
was checked and found to match upstream `ruma-client-api` — these are not
fork-introduced bugs, just places where the generic derive doesn't produce
what the endpoint needs, and patching the vendored crate wasn't worth the
maintenance burden of a fork-local patch. If several of these accumulate
around the same ruma type, that's a signal it may be worth revisiting.

## Pattern

Two variants used depending on where the route is registered:

1. **`axum::middleware::map_response`, scoped via a nested `Router`** — for
   routes registered with `.ruma_route(...)`, where the handler must return
   the exact `Req::OutgoingResponse` type (see `src/api/router/handler.rs`),
   so the handler itself can't deviate from ruma's type. Wrap just that
   route in its own `Router::new().ruma_route(...).layer(map_response(...))`
   and `.merge()` it into the main router — see `src/api/router.rs`.
2. **Inline `serde_json::Value` patch after `try_into_http_response`** — for
   handlers that already build the ruma response manually and can
   post-process before returning (e.g. `/sync`, which serializes once,
   patches, and returns the patched `Value` directly).

Both variants: parse the serialized body into `serde_json::Value`, patch the
specific keys, re-serialize. Fall back to the original bytes if parsing or
re-serializing fails, so a shape we didn't anticipate degrades to ruma's
default output rather than 500ing.

There's also a **request-side** version of this problem: a ruma `Request`
field that's required (no `Option`, no `#[serde(default)]`) but the actual
reference implementation (Synapse) treats as optional. There's no response
body to patch after the fact here — ruma's deserializer rejects the request
before our handler ever runs — so the fix is `axum::middleware::from_fn`
(request-side, not `map_response`) mutating the query string ahead of
extraction, scoped the same way via a nested `Router`. See `/messages`'s
`dir` below.

## Instances

### `/search` — empty `results` dropped instead of `results: []`

`ruma::api::client::search::search_events::v3::ResultRoomEvents::results` is
`#[serde(default, skip_serializing_if = "Vec::is_empty")]`. When a search
page has zero results (e.g. paginating past the end), the `results` key is
omitted entirely instead of serializing as `[]`. Complement's
`TestSearch/parallel/Can_back-paginate_search_results` asserts the key is
always present once `room_events` was requested.

- **Fix:** `ensure_search_results_present` in `src/api/router.rs`, wired via
  `map_response` scoped to `client::search_events_route`.
- Verified identical `skip_serializing_if` in both the vendored `ruwuma`
  fork and upstream `ruma-client-api` before patching here — not a
  fork-specific bug.

### `/messages` — `dir` required by ruma, optional in practice

`ruma::api::client::message::get_message_events::v3::Request::dir` is a
required `Direction` field, matching the spec's literal text ("dir
(Required)"). Synapse doesn't actually enforce that —
`PaginationConfig.from_request` defaults it to `Direction::FORWARDS` when
absent, the same kind of spec-vs-reference gap MSC3567 already documented
for `from` ("Synapse already implements this, but it is not
spec-compliant"). Complement tests against Synapse's actual behavior (e.g.
`TestRoomForget`'s "Forgotten room messages cannot be paginated" omits
`dir` entirely), so a request missing it 400ed with `M_BAD_JSON` before our
handler could even run its own checks.

- **Fix:** `default_messages_dir` in `src/api/router.rs` — the request-side
  variant described above — injects `dir=f` into the query string when
  absent, wired via `from_fn` scoped to `client::get_message_events_route`.

### `/directory/list` (public rooms) — missing `join_rule`

`inject_public_join_rule` in `src/api/router.rs`, wired the same way onto
`get_public_rooms_route` / `get_public_rooms_filtered_route` (both the
client and federation variants). Backfills `join_rule: "public"` onto
directory chunk entries that don't have one set, for older rooms/clients
that predate the field.

### `/sync` (v3) — three separate patches in one place

`src/api/client/sync/v3/mod.rs` builds the ruma `sync_events::v3::Response`
normally, serializes it once via `try_into_http_response`, then patches the
resulting `serde_json::Value` directly (rather than using `map_response`,
since this handler already owns the serialize step):

1. **`rooms.knock` dropped when it's the only non-empty membership
   section.** Ruma's `Rooms::is_empty()` doesn't count `knock` when
   deciding whether to omit the whole `rooms` object, so a sync response
   with only knock updates (no join/leave/invite) loses `rooms` entirely.
   Manually re-inserts `rooms.knock` when this happens.
2. **`ephemeral` missing on joined rooms.** Injects
   `ephemeral: { events: [] }` on joined-room entries that don't have the
   key, to satisfy Complement's expectations about the shape being always
   present.
3. **`device_lists` occasionally dropped.** Comment in code: "Ruma may omit
   non-empty device_lists during serialization in some edge cases." The
   computed `device_lists_json` is unconditionally re-inserted into the
   response object as a belt-and-suspenders measure so a one-shot
   device-list update can't silently vanish between internal assembly and
   the final body. (This one is the least precisely diagnosed of the
   three — worth a closer look if `device_lists` issues resurface, since
   "may omit... in some edge cases" suggests the original root cause
   wasn't fully pinned down before the workaround was added.)

Also present in this same file: MSC4222 `state_after` / `org.matrix.msc4222.state_after`
injection for joined and left rooms — not a ruma limitation, just a field
ruma doesn't know about yet, added the same way for convenience since the
patch machinery was already there.

## When adding a new one

- Confirm it's actually a ruma/vendored-crate serialization gap and not a
  bug in our own response construction first (check what we pass into the
  ruma type before assuming the derive is at fault).
- Check whether the vendored `ruwuma` fork diverges from upstream
  `ruma-client-api` for the type in question — if the fork already deviates
  intentionally, a patch there may be more appropriate than a body-patch
  here.
- Prefer `map_response` + scoped nested `Router` if the route goes through
  `.ruma_route(...)`; only patch inline post-serialize if the handler
  already owns that step (like `/sync`).
- Add an entry to this file.

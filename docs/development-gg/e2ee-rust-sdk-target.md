# Adding a Rust-SDK target to `just e2ee`

Last updated: 2026-08-18 Status: not started (scoping notes only)

## Why

`just e2ee` (and the `complement-crypto-tests` CI job) only ever exercises
**matrix-js-sdk** against conduwuit. complement-crypto's `tests/` package is
written to be SDK-agnostic — it drives whichever bindings are compiled in via
the `COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX` combinations (`jj`, `jr`, `rj`, `rr`
— js/rust on each side of Alice/Bob) — but we currently build with `-tags jssdk`
only, so the matrix is hardcoded to `jj` and the Rust bindings are never linked
in.

Two things fall out of fixing that:

1. **More coverage of the same tests** — every existing `tests/*_test.go` case
   (key backup, verification, to-device retries, room-key cycling, the
   spoofed-sender race, etc.) gets exercised against the Rust SDK too, not just
   JS. Real client behavior differs between the two SDKs (see the
   `TestSpoofedEventSenderHandling` investigation in
   [`complement-failures.md`](./complement-failures.md) — that failure mode was
   JS-SDK-specific timeline-commit timing; the Rust SDK may or may not hit the
   same race).
2. **A `tests/rust/` package that only runs today if you build with
   `-tags rust`**, containing NSE (iOS Notification Service Extension)
   multiprocess tests with no JS analog — `TestMultiprocessNSE`,
   `TestMultiprocessNSEBackupKeyMacError`, `TestMultiprocessNSEOlmSessionWedge`,
   `TestNotificationClientDupeOTKUpload`,
   `TestMultiprocessInitialE2EESyncDoesntDropDeviceListUpdates`. These never run
   in our CI or local `e2ee` recipe today.

## What's already there

The harness side of this is not a from-scratch job — `justfile` already has the
plumbing to rebuild the Rust bindings, it's just never wired into `e2ee`:

```just
# Rebuild the version of matrix-rust-sdk used and regenerate its Go bindings.
rebuild-rust-sdk rust-sdk-path:
    just _build-rust-sdk {{ rust-sdk-path }}
    just _patch-ldflags

_build-rust-sdk dir:
    cd {{ dir }}
    cargo build -p matrix-sdk-ffi --features 'sentry, _only-for-testing-disable-megolm-minimum-rotation-period-ms'
    uniffi-bindgen-go -o <complement-crypto>/internal/api/rust \
        --config <complement-crypto>/uniffi.toml \
        --library ./target/debug/libmatrix_sdk_ffi.a

_patch-ldflags:
    # adds `#cgo LDFLAGS: -lmatrix_sdk_ffi` to the generated bindings
```

And `complement-crypto-src/internal/api/langs/lang_rust.go` is gated on
`//go:build rust` (note: the tag is `rust`, **not** `rustsdk` — don't confuse it
with the JS tag `jssdk`).

## What's missing

1. **A `matrix-rust-sdk` checkout.** `rebuild-rust-sdk` takes a `rust-sdk-path`
   argument — there is no vendored/submoduled copy of `matrix-rust-sdk` in this
   repo today (unlike `complement-crypto-src` and `ruwuma`, which are
   submodules). First decision: add it as a submodule pinned to a commit
   (mirrors how `complement-crypto-src` and `ruwuma` are already handled), or
   require a `--rust-sdk-path` env var pointing at a local checkout for now and
   defer pinning until this is CI-bound.

2. **`e2ee` recipe changes** (`justfile`):
    - Build tag: `go test -tags jssdk` → `-tags jssdk,rust`.
    - `COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX` default is hardcoded to `jj`
      because the rust binding isn't compiled in; once it is, this can default
      to the full `jj,jr,rj,rr` (or stay overridable via env var for faster
      iteration on just one pairing).
    - Prereq check: mirror the existing JS-bundle check (`dist/index.html` must
      exist or the recipe errors with a build hint) with an equivalent check
      that `libmatrix_sdk_ffi.a` / the generated
      `internal/api/rust/matrix_sdk_ffi/*.go` bindings exist.
    - Link flags: needs `CGO_ENABLED=1` and `CGO_LDFLAGS`/`-L` pointed at
      wherever `libmatrix_sdk_ffi.a` ends up (`<rust-sdk-path>/target/debug`
      today; would need to be a stable location if we vendor the SDK).
    - Test enumeration: `ALL_TESTS` currently only greps `tests/*_test.go`.
      `tests/rust/*_test.go` (the NSE tests) is a **separate Go package** with
      its own `TestMain`, so it can't just be folded into the existing shard
      loop — it needs its own `go test ./tests/rust` invocation, most likely as
      a second target (e.g. `just e2ee-rust-nse`) rather than a flag on `e2ee`.

3. **CI wiring** (`.github/workflows/complement.yml`). The
   `complement-crypto-tests` job currently only has a "Build JS SDK bundle"
   step. Adding Rust would mean a real `cargo build -p matrix-sdk-ffi` in CI —
   non-trivial build time (Rust FFI + uniffi codegen), so this should be proven
   out locally first and only added to CI once we know it's not adding 10+
   minutes to every crypto run for marginal coverage.

## Suggested order of work

1. Get `just e2ee` running `jj,jr,rj,rr` locally against a local
   `matrix-rust-sdk` checkout (steps 1–2 above, no CI change). Confirm the
   existing `tests/*_test.go` suite actually passes against the Rust SDK
   pairings — don't assume it will just because JS does.
2. Decide submodule-pin vs. local-path based on how stable that turns out to be.
3. Stand up `tests/rust/` (the NSE suite) as its own recipe/target and see which
   of those tests are meaningful against conduwuit specifically (some NSE
   behavior may be entirely client-side and not exercise the server at all).
4. Only then consider CI — as its own job/matrix leg, not folded into the
   existing JS-only `complement-crypto-tests` job, given the extra build cost.

## Open questions

- Is there an appetite for vendoring `matrix-rust-sdk` as a submodule (like
  `ruwuma`/`complement-crypto-src`), given it's a large repo with its own slow
  Rust build? Might be worth checking whether upstream complement-crypto
  publishes prebuilt bindings/artifacts we could pull instead of building from
  source every time.
- Has upstream `complement-crypto` already discussed running `tests/rust/` in
  CI, or hit the same "separate package, own TestMain" friction we would? Worth
  a quick look before reinventing the sharding approach for it.

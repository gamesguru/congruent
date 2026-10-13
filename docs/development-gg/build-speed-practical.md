# Build speed: what is already on, what is left

Companion to `build_time_optimization.md` (structural proposal). This note is
the practical, measured side. Source: cargo timing report of a workspace
`clippy --all-targets` run, 430 units, 185s wall, 6 cores.

## Where the time goes

The build is a serial chain; dependency pruning barely moves wall time.

| Crate | Time |
|---|---|
| `conduwuit_core` | ~16s |
| `conduwuit_service` | ~61s (test target ~69s) |
| `conduwuit_api` | ~64s (test target ~78s) |
| `conduwuit_admin` | ~36s (test target ~45s) |
| `conduwuit_router`, `conduwuit`, `conduwuit_web` | 11-14s each |

Third-party crates (`tokio`, `h2`, `hickory-proto`, `mtxdb`, `rezzy`) each
finish in under 20s, in parallel with that chain.

## Already in place (no action needed)

- **`sccache` + `mold`**: `.cargo/cargo-wrapper.sh` uses them when installed.
  Anything that runs cargo with `RUSTC_WRAPPER=` (empty) bypasses both; do not
  do that in agent or CI-less local runs.
- **Light debug info**: `[profile.dev-quick]` (`Cargo.custom.toml`) uses
  `debug = "line-tables-only"`, dependencies at `debug = 0`, and
  `codegen-units = 256`. Only plain `[profile.dev]` has `debug = "full"`; keep
  it for real debugging.
- **Incremental**: enabled. Note `sccache` does not cache incremental crates,
  so it only helps third-party dependencies; workspace crates rely on
  incremental compilation.

## Not available

- **`-Zthreads=N`**: `rust-toolchain.toml` pins stable `1.98.1`; `-Z` flags
  need nightly. Nightly is only used for rustfmt. Not worth switching the
  build toolchain, and `RUSTC_BOOTSTRAP=1` is an unsupported hack.
- **Cranelift backend**: nightly-only as well.

## What actually helps

1. **Do not use `--all-targets` while iterating.** It compiles `service`,
   `api` and `admin` twice (lib and test). Use
   `cargo clippy -p <crate> --profile dev-quick`, and run the full
   `--workspace --all-targets` only before pushing.
2. Always pass `--profile dev-quick` for check, clippy and tests.
3. Keep `RUSTC_WRAPPER` unset (default) so the wrapper script applies.
4. Structural: split `conduwuit_service` into smaller crates so they compile
   in parallel (see `build_time_optimization.md`). Biggest win, biggest
   refactor.

## Dependency pruning status

Removed: `reqwest`, `axum`, `tower-http`, `opentelemetry`,
`tracing-opentelemetry`, `aws-lc-rs`, and the `hyper-util`, `event-listener`
and `async-channel` Forgejo forks. Kept: `core_affinity` fork (the code uses
fork-only `set_each_for_current`).

Still present: `hyper`, `hyper-util`, `h2`, `tower`, `tokio`, `tokio-rustls`,
`http-body-util` (router and transitive), `smol`/`async-io`/`async-process`/
`blocking`, `async-net`, `futures-rustls`, `url`, `hickory-resolver`.
Next step: `cargo tree -i <crate>` on `tokio-rustls`, `tower`, `h2` and the
`smol` family to see which are still pulled in only transitively.

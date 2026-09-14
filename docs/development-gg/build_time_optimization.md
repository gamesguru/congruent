# Proposal: Build Time Optimization

## Problem

Incremental builds take ~30-60s even for single-file changes because the workspace has a linear crate dependency chain:

```
conduwuit_core → conduwuit_database → conduwuit_service → conduwuit_api → conduwuit_admin → conduwuit_router → conduwuit
```

Cargo can't parallelize across crates when each depends on the previous. A change in `conduwuit_api` still triggers sequential recompilation of `conduwuit_admin → conduwuit_router → conduwuit`.

### Root Causes (Beyond the Chain)

1. **ruma** — the Matrix SDK is one of the slowest Rust libraries to compile. Massive macro expansion for every API endpoint type, hundreds of serde derives, deeply nested generics. ~40%+ of clean build time. Even on incremental builds, rustc must resolve ruma's generic types in our crates.
2. **async everywhere** — every `async fn` generates a hidden state machine enum. Hundreds of async functions chained through streams means heavy type inference and borrow checking.
3. **`#[implement]` proc macro** — custom proc macro on nearly every service method. Each invocation triggers full macro expansion.
4. **Streams + generics** — `impl Stream<Item = impl Event>` chains with `broad_filter_map`, `ready_filter_map` etc. Each combinator creates a new monomorphized type. A 5-step stream pipeline generates 5 nested generic types.
5. **serde derives** — on every PDU, event content, API request/response. Each `#[derive(Serialize, Deserialize)]` expands to hundreds of lines.

## Proposed Changes

### 1. Break the Dependency Chain (High Impact)

Extract service trait definitions into a thin `conduwuit_interfaces` crate containing only:

- Trait definitions for each service (rooms, users, globals, etc.)
- Shared types and error enums
- No implementation code

This lets `conduwuit_api` and `conduwuit_admin` compile against interfaces in parallel, rather than waiting for the full `conduwuit_service` implementation:

```
                    conduwuit_interfaces
                   /        |           \
    conduwuit_service   conduwuit_api   conduwuit_admin
                   \        |           /
                    conduwuit_router
                          |
                      conduwuit
```

**Estimated impact**: 2-3× faster incremental builds for API-layer changes.

**Effort**: Significant refactor — requires defining trait boundaries for every service. Could be done incrementally, one service at a time.

### 2. Use `mold` Linker (Low Effort, Medium Impact)

Linking is often the bottleneck on incremental debug builds. `mold` is 5-10× faster than the default `ld`.

```toml
# .cargo/config.toml
[target.x86_64-unknown-linux-gnu]
linker = "clang"
rustflags = ["-C", "link-arg=-fuse-ld=mold"]
```

**Estimated impact**: 2-5s saved per build on link-heavy changes.

### 3. Cranelift Backend for Dev Builds (Low Effort, Medium Impact)

Skip LLVM entirely for debug/check builds:

```sh
cargo +nightly -Zcodegen-backend=cranelift check
```

**Estimated impact**: ~30-40% faster codegen, but nightly-only and may produce slower runtime code (dev-only).

### 4. `sccache` for Cross-Branch Caching (Low Effort, Low Impact)

Caches compiled artifacts across git branches. Switching between feature branches won't trigger full rebuilds:

```toml
# .cargo/config.toml
[build]
rustc-wrapper = "sccache"
```

**Estimated impact**: Eliminates redundant rebuilds when switching branches.

## Speeding Up `cargo check` / `cargo clippy`

These commands skip codegen entirely (no linking, no LLVM), so mold and cranelift don't help. The bottleneck is the sequential crate dependency chain and rustc's type checking / macro expansion.

### Immediate: Target a Single Crate

If you only changed files in one crate, skip downstream recompilation:

```sh
# Only check the crate you touched (skips admin → router → main)
cargo check -p conduwuit_api

# Same for clippy
cargo clippy -p conduwuit_api
```

Saves ~10s on a typical 30s incremental check by skipping 3 downstream crates that didn't change.

### Longer-Term Code Changes

- **Reduce proc macro usage** — `#[implement]`, `#[async_trait]`, heavy derive macros all add parse/expand time
- **Use `dyn Trait` in non-hot paths** — reduces monomorphization work during type checking
- **Crate split** (see above) — the only real fix for the sequential chain

## Priority

1. **`cargo check -p`** — use today, no setup needed
2. **mold linker** — already configured ✅
3. **sccache** — already installed ✅, add `RUSTC_WRAPPER=sccache` to env
4. **Cranelift** — for `cargo build` only, not check/clippy
5. **Interface crate split** — plan for a future major refactor cycle

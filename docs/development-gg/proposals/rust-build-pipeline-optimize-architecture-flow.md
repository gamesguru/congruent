# Rust Build Pipeline: Optimize Architecture & Flow

## Problem

The continuwuity workspace has a **linear dependency chain**:

```
core → database → service → api → admin → binary
```

Any change to a leaf crate (e.g., `rezzy`, `ruma`, or even a test string in `admin/tests.rs`) cascades a full rebuild through every downstream crate. With `--release` (no incremental compilation), this costs **~5 minutes per iteration** regardless of change size.

Even `#[cfg(test)]` code isn't isolated — tests compiled inside a crate (`mod tests`) are part of that crate's compilation unit, so changing a test assertion recompiles the entire crate.

## Quick Wins

- **Cranelift backend** for debug builds (`-Zcodegen-backend=cranelift`) — ~2x faster compilation, skips LLVM
- **`cargo-nextest`** — parallelizes test execution across cores
- **`sccache`** — caches compiled crates across git rev changes and CI runs
- **Faster linker** — use `mold` (Linux) or `lld` to speed up the final linking stage
- **`cargo build --timings`** — generates an HTML report showing which crates are build bottlenecks
- **Move heavy tests to integration test crates** — standalone `tests/` binaries compile independently and only rebuild themselves

## Architectural: Interface Crate Pattern

### How It Works

Instead of downstream crates depending on full implementation crates, they depend on thin **interface crates** containing only trait definitions and DTOs:

**Before (linear chain):**

```
admin ──→ api ──→ service ──→ database ──→ core
                     ↑
                   rezzy, ruma, rocksdb
```

Change anything in `service` → `api`, `admin`, and binary all recompile.

**After (diamond with trait boundary):**

```
admin ──→ service_traits (tiny, stable)
api   ──→ service_traits
service_impl ──→ service_traits + database + rezzy + ruma
binary ──→ service_impl + admin + api  (wiring only)
```

Change anything in `service_impl` → **only `service_impl` recompiles**. `admin` and `api` are untouched because they only see the trait signatures, which rarely change.

### Significance

This is **the most impactful architectural change** for Rust build times in large workspaces:

- **Granular caching**: Cargo compiles crates as independent units. Unchanged crates are fully cached.
- **Parallel compilation**: Independent crates build simultaneously. A diamond graph enables more parallelism than a linear chain.
- **Reduced rebuild scope**: Implementation changes (bug fixes, dependency bumps) don't cascade through the entire project.
- **Estimated impact**: For a change to `service_impl` (state resolution, federation), rebuild time drops from ~5 min (full chain) to ~1 min (single crate).

### Trade-offs

- **Monomorphization vs `dyn Trait`**: Using generics (`<T: Trait>`) for DI causes monomorphization (code generated per concrete type), which can increase compile times. Use `dyn Trait` where runtime cost is negligible.
- **Over-segmentation**: Too many tiny crates adds coordination overhead and can increase build times from graph complexity. Target 8-15 crates, not 50.
- **Circular dependencies**: Rust forbids them. The interface crate pattern naturally avoids this since traits flow one direction.

## Feature-Gating Heavy Dependencies

`rezzy` only matters for state resolution; `rocksdb` only matters for storage. If these are behind Cargo features:

```toml
[features]
default = ["state-res", "rocksdb"]
state-res = ["dep:rezzy"]
```

Then changes to `rezzy` only rebuild crates that enable `state-res`, not the entire workspace.

## Next Steps

1. Run `cargo build --timings` to generate the build bottleneck report
2. Map the dependency graph with `cargo tree` to identify cascade hotspots
3. Measure baseline incremental rebuild times for changes at each layer (`core`, `database`, `service`, `api`, `admin`)
4. Identify which crates pull in heavy dependencies (`ruma`, `rocksdb`) and whether those can be isolated behind feature gates or interface boundaries
5. Prototype a `conduwuit_service_traits` crate with the most-used trait definitions

## References

- [Rust Performance Book — Compile Times](https://nnethercote.github.io/perf-book/compile-times.html) — `cargo build --timings`, `cargo-llvm-lines`, linker tips
- [Tips for Faster Rust Compile Times (corrode.dev)](https://corrode.dev/blog/tips-for-faster-rust-compile-times/) — crate splitting, sccache, mold linker
- [Reducing Compile Times with Workspace Crates (PingCAP)](https://www.pingcap.com/blog/rust-compilation-model-calamity/) — real-world experience from TiKV
- [Dependency Inversion in Rust (40tude.fr)](https://www.40tude.fr/rust-how-to-do-dependency-injection/) — interface crate pattern with worked examples

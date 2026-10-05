# Lean HTTP stack and dependency audit

## Proposal

Replace Axum with a small HTTP layer built from:

- `tokio` for the runtime
- `hyper` for HTTP
- `hyper-util` for Tokio connection serving
- `http-body-util` for common request and response bodies
- `matchit` for path matching

Keep routing, extraction, errors, middleware, and response conversion in
project-owned modules. Handlers should use `http::Request`/`http::Response` and
project-owned request context types, rather than Axum extractors or
`IntoResponse`.

The absolute-minimum variant can omit `matchit` and use a small route table, but
`hyper + matchit` is the better production balance: small, explicit, and still
capable of parameterized routes.

## Migration shape

1. Inventory Axum usage: routers, nesting, state, extractors, middleware,
   rejections, WebSockets, streaming, and tests.
2. Introduce framework-neutral `RequestContext`, path/query/body helpers,
   `WebError`, and response builders.
3. Add a temporary Axum adapter that converts Axum requests into those
   project-owned types.
4. Port middleware into framework-neutral functions: tracing, request IDs, auth,
   body limits, compression, security headers, rate limiting, and error
   handling.
5. Build the Hyper dispatcher beside Axum and migrate health checks, static
   files, and simple pages first.
6. Migrate API subsystems in groups, preserving status codes, headers, JSON
   shapes, redirects, streaming, and upgrade behavior with integration tests.
7. Migrate WebSocket/upgrade and body-streaming paths after the basic dispatcher
   is proven.
8. Delete the Axum adapter, Axum-specific tests, feature flags, and dependencies
   only after the full test suite passes through Hyper.

The business and service layers should not be rewritten as part of this effort.
The migration is successful when only the server adapter knows which HTTP
implementation is in use.

## Dependency and feature audit

The goal is not merely to remove Axum. Cargo features and transitive
dependencies should be audited so the replacement does not recreate the same
bloat under different names.

### Establish a baseline

Record before and after:

```bash
cargo tree --workspace --all-features --edges normal,build,dev > /tmp/continuwuity-tree.txt
cargo tree --workspace --all-features --duplicates
cargo metadata --format-version 1 --no-deps > /tmp/continuwuity-metadata.json
cargo bloat --release --crates
cargo bloat --release --filter conduwuit
```

Repeat the measurements with default features, the production feature set, and
test/dev features. Compare release binary size, compile time, dependency count,
and enabled native libraries.

### Inspect feature activation

For every workspace package, record:

- default features and whether they are actually needed;
- features enabled by workspace-wide `full` or `all` feature bundles;
- optional dependencies that become mandatory through umbrella features;
- platform-specific features activated on targets that do not need them;
- build dependencies and proc macros pulled in only for development;
- duplicate versions of foundational crates such as `serde`, `bytes`, `http`,
  `tokio`, crypto, hashing, and compression crates.

Useful commands:

```bash
cargo tree -e features -i axum
cargo tree -e features -i tokio
cargo tree -e features -i hyper
cargo tree -e features -i openssl
cargo tree -e features -i ring
cargo tree --duplicates
```

Use `cargo tree -e features` to identify who activates a feature; do not infer
feature ownership from the final dependency list alone.

### Likely bloat candidates

Audit these areas first:

- Axum extras that are only used for a small number of extractors or middleware
  layers;
- `tower-http` layers enabled globally when only one route needs them;
- JSON, form, multipart, and query parsing features enabled together despite
  limited use;
- WebSocket support enabled in binaries that do not serve upgrades;
- TLS backends and native crypto libraries duplicated across clients and
  servers;
- compression libraries enabled for development or internal endpoints;
- database backends, RocksDB, or native bindings enabled through broad `full`
  features;
- image, media, HTML/template, and static-file support compiled into workers
  that do not serve web pages;
- tracing subscribers, console integrations, and pretty printers enabled in
  production binaries;
- test utilities and mock servers leaking through normal or build dependencies;
- proc macros and derive crates duplicated across packages;
- broad workspace feature forwarding that activates optional dependencies
  everywhere;
- duplicate versions caused by loose version requirements or incompatible
  feature choices.

### Feature policy

Prefer narrowly named feature bundles over a universal `full` feature. Each
optional dependency should have one owning feature and a documented consumer.
Production binaries should select an explicit feature set; tests may opt into
heavier tooling separately.

Avoid enabling a feature merely because a dependency offers it. Every feature
should have a measured use, a target package, and a reason it belongs in that
binary.

### Review criteria

For each candidate removal or change, verify:

1. No source or generated code requires the feature.
2. No supported target relies on the dependency indirectly.
3. Tests and benchmarks still compile under their intended feature sets.
4. Release binaries and startup behavior remain correct.
5. Compile time, dependency count, and binary size improve or the tradeoff is
   documented.

Make dependency reductions in small commits. A useful sequence is: remove unused
direct features, deduplicate versions, narrow workspace feature forwarding,
remove unused optional dependencies, then remove Axum and its adapter.

## Definition of done

- Hyper serves every production route.
- No application handler imports Axum.
- Axum, Axum extractors, and Axum response traits are absent from production
  dependencies.
- Production features are explicit and documented.
- `cargo tree --duplicates` has been reviewed and justified.
- Release size and compile-time measurements are recorded before and after.
- Full unit, integration, federation, WebSocket, streaming, and deployment
  checks pass.

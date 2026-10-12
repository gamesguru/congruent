# mtx-slipstream migration

## Goal

Make `mtx-slipstream` the lightweight Matrix compatibility and serialization
layer used by Continuwuity, replacing the current Ruma/Serde/Tokio-heavy
paths over time.

The end state should use:

- `rezzy-json` as the canonical JSON value model;
- `simd-json` with minimal features for parsing and writing;
- explicit Matrix codecs for hot event, PDU, sync, and federation paths;
- small, local Matrix identifiers, event types, API models, and errors;
- no dependency on Ruma, Serde derives, or Tokio inside slipstream’s core.

## Transitional rule

`mtx-slipstream` remains wired under the `ruma` compatibility namespace while
the migration is in progress. This is an adapter boundary, not the desired
long-term architecture. Core may continue using Serde-backed configuration and
compatibility models temporarily, but new hot-path code must not add Ruma or
Serde requirements.

## Order of work

1. Split the compatibility surface into modules matching the Matrix domains:
   identifiers, room versions, canonical JSON, raw JSON, events, APIs,
   signatures, and headers.
2. Implement primitive identifiers and scalar types with validation and the
   conversions required at core boundaries.
3. Implement canonical JSON operations and redaction over `rezzy-json`.
4. Implement explicit raw-event and event-content codecs over `simd-json`.
5. Replace event and PDU call sites in core from generic Serde operations to
   those codecs.
6. Remove the compatibility namespace and delete the remaining Ruma imports.
7. Remove Serde from migrated hot paths, then remove Tokio from slipstream’s
   core where asynchronous orchestration is not required.
8. Remove obsolete workspace dependencies only after all consumers are gone.

## Constraints

- Do not add Ruma as a dependency of slipstream.
- Do not add Serde derives to slipstream types.
- Keep compatibility code modular; do not grow `src/lib.rs` into a monolith.
- Use compiler errors and focused tests to drive each migration layer.
- Preserve Matrix wire compatibility and validate behavior before deleting the
  old implementation.

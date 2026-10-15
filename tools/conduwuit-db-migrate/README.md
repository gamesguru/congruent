# conduwuit-db-migrate

This migration utility is an isolated Cargo workspace, excluded from the
production workspace so its RocksDB dependency is never resolved by normal
server builds. It exports the legacy metadata maps that remain in redb after
room DAGs and signing keys are imported through mtxdb's existing
`mtxdb-export/v1` JSONL format.

The metadata archive is UTF-8 JSONL (`conduwuit-metadata/v1`). Keys and values
are hex encoded so arbitrary RocksDB bytes round-trip exactly. The final
`end` record includes the row count; import rejects a truncated or malformed
archive. redb records use the production table name and composite-key encoding
from `src/database/engine/redb.rs`.

Export defaults to every source column family except `default` and the
mtxdb-owned event/state/DAG/signing-key families. The `list` command shows
that classification before export. Explicit map names can select a subset,
but mtxdb-owned families are refused. The importer refuses to overwrite an
existing redb destination and publishes the completed temporary database by
rename. Stop the source homeserver before exporting so RocksDB is a stable
read-only snapshot; the tool opens it read-only and never writes to it.

```text
cargo run --manifest-path tools/conduwuit-db-migrate/Cargo.toml -- \
  list /run/media/shane/shane4tb-ent/vps/wombatx/var/lib/conduwuit

cargo run --manifest-path tools/conduwuit-db-migrate/Cargo.toml -- \
  export /run/media/shane/shane4tb-ent/vps/wombatx/var/lib/conduwuit \
  /tmp/wombatx-metadata.jsonl

cargo run --manifest-path tools/conduwuit-db-migrate/Cargo.toml -- \
  import /tmp/wombatx-metadata.jsonl \
  /tmp/wombatx-redb/metadata.redb
```

Room DAGs and remote signing keys are exported separately with Congruent's
`!admin yolo export-mtxdb --all` and `!admin yolo export-mtxdb-keys`, then
imported with `mtxdb import`. This tool does not mutate the RocksDB source or
perform the mtxdb import.

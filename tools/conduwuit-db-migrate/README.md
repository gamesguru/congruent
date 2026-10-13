# conduwuit-db-migrate

This is an intentionally isolated migration utility. It is not a member of
the production workspace and has its own `Cargo.lock`, so the server does not
resolve or compile RocksDB.

The tool copies legacy metadata column families from RocksDB into one redb
`metadata` table. Keys are namespaced by their source column-family name, so
records from different legacy maps cannot collide. Re-running the import is
safe: the same source record deterministically overwrites the same redb key.

Events, state, and DAG data are intentionally excluded. Those datasets should
be exported/imported with mtxdb's native tooling, preserving its shard and
transaction semantics.

```text
# Export the conservative metadata set
cargo run --manifest-path tools/conduwuit-db-migrate/Cargo.toml -- \
  export /var/lib/conduwuit /tmp/conduwuit-metadata.cdb

# Import it into the redb metadata database
cargo run --manifest-path tools/conduwuit-db-migrate/Cargo.toml -- \
  import /tmp/conduwuit-metadata.cdb /var/lib/conduwuit-redb/metadata.redb
```

Pass legacy column-family names after the bundle path to export an explicit
subset. Unknown names are ignored; event/state families are never selected by
the default list.

# conduwuit-db-migrate

This migration utility is an isolated Cargo workspace, excluded from the
production workspace so its RocksDB dependency is never resolved by normal
server builds. It exports legacy metadata maps for redb and converts legacy
RocksDB room events and cached signing keys into the `mtxdb-export/v1` JSONL
format consumed by `mtxdb import`.

The metadata archive is UTF-8 JSONL (`conduwuit-metadata/v1`). Keys and values
are hex encoded so arbitrary RocksDB bytes round-trip exactly. The final `end`
record includes the row count; import rejects a truncated or malformed archive.
redb records use the production table name and composite-key encoding from
`src/database/engine/redb.rs`.

Export defaults to every source column family except `default` and the
mtxdb-owned event/DAG/signing-key families. Legacy state maps stay in the
metadata archive: `mtxdb import` does not yet build state groups for
`mtxdb-export/v1` inputs, so the server's v20 migration uses these maps to
build its HAMT roots. The root-handle and HAMT-node maps remain excluded
because v20 rewrites them. The `list` command shows the classification before
export. Explicit map names can select a subset, but mtxdb-owned families are
refused. The importer refuses to overwrite an existing redb destination and
publishes the completed temporary database by rename. Stop the source
homeserver before exporting so RocksDB is a stable read-only snapshot; the
tool opens it read-only and never writes to it.

`export-mtxdb` writes one `room-<hex room id>.jsonl` per room plus
`server-keys.jsonl`. It understands both the legacy `pduid_pdu` /
`eventid_outlierpdu` layout and the newer `eventid_pdu` layout. It infers the
homeserver name from the server part of IDs in `userid_password`, and requires
those IDs to agree. Federated user rows can make that ambiguous; pass the
configured server name as the optional third argument in that case. The PDU
body's own `event_id` is authoritative if a legacy index disagrees.

Room event records contain the PDU and its graph references. Rejection,
soft-fail, and outlier flags are not exported; the importer derives its stored
verdicts from the event data and available checks. Legacy cached keys are
exported as permanent keys; no notary payload is synthesized.

```text
cargo run --manifest-path tools/conduwuit-db-migrate/Cargo.toml -- \
  list /run/media/shane/shane4tb-ent/vps/wombatx/var/lib/conduwuit

cargo run --manifest-path tools/conduwuit-db-migrate/Cargo.toml -- \
  export-mtxdb /run/media/shane/shane4tb-ent/vps/wombatx/var/lib/conduwuit \
  /tmp/wombatx-mtxdb-export

cargo run --manifest-path tools/conduwuit-db-migrate/Cargo.toml -- \
  export /run/media/shane/shane4tb-ent/vps/wombatx/var/lib/conduwuit \
  /tmp/wombatx-metadata.jsonl

cargo run --manifest-path tools/conduwuit-db-migrate/Cargo.toml -- \
  import /tmp/wombatx-metadata.jsonl \
  /tmp/wombatx-redb/metadata.redb

# Import every room file and the keys file in one invocation. The import
# marker is only written when the complete set succeeds. `--no-state-hamts`
# leaves state to the server: congruent's v20 migration builds its short-ID
# HAMTs from the legacy state maps in metadata.redb.
mtxdb import --no-state-hamts --dir /path/to/new-database/mtxdb /tmp/wombatx-mtxdb-export/*.jsonl
```

The exporter refuses an existing output directory and never writes to the
RocksDB source. Do not start the new server until both the redb metadata import
and the complete mtxdb import have succeeded.

Archives made by an older build that excluded the legacy state maps cannot
build room state: v20 sees no `roomid_shortstatehash`, falls back to empty HAMT
roots, and leaves `roomid_roothandle` unwritten, so room-state reads fail with
`Missing state for ...`. Re-export the metadata with a current build.

## Rooms that cannot be imported

`mtxdb import` refuses a room file with no valid `m.room.create` event, and
without a create event the run ends with no completion marker. Exclude those
rooms (`MIGRATE_SKIP_ROOMS` for `make migrate/import`).

The `wombatx.me` export contained three such rooms. Each holds only `leave`
events for `@shane:wombatx.me` (outliers from invites that were declined or
never joined), and none has a `roomid_shortroomid` entry:

| Room                                     | Events   | Status  |
| ---------------------------------------- | -------- | ------- |
| `!GmViaerQkRWufXAOKV:matrix.koolant.net` | 1 leave  | skipped |
| `!KJCvzPiZICmWXQywiv:matrix.org`         | 1 leave  | skipped |
| `!mefQhZzgTaxNCNzAeK:kde.org`            | 2 leaves | skipped |

Their create and auth events were not stored in the source database or in the
`vps16-dev`, `vps16-dev-2026-02-06` and `vps151-nightly` dumps. Fetching them
over federation was tried and failed:

- As `nutra.tk` (signed request accepted): `matrix.koolant.net`, `matrix.org`
  and `kde.org` each answered `403 M_FORBIDDEN "Host not in room."` to both
  `/backfill` and `/event/{id}`.
- As `wombatx.me`: `matrix.org` answered `401 M_UNAUTHORIZED` because the old
  server was offline and its signing key could not be fetched; `kde.org` timed
  out.

Servers only release a room's events to a server that is in the room, so these
can be recovered only from a server or account that is currently joined. Losing
them changes no room state.

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
mtxdb-owned event/state/DAG/signing-key families. The `list` command shows that
classification before export. Explicit map names can select a subset, but
mtxdb-owned families are refused. The importer refuses to overwrite an existing
redb destination and publishes the completed temporary database by rename. Stop
the source homeserver before exporting so RocksDB is a stable read-only
snapshot; the tool opens it read-only and never writes to it.

`export-mtxdb` writes one `room-<hex room id>.jsonl` per room plus
`server-keys.jsonl`. It understands both the legacy `pduid_pdu` /
`eventid_outlierpdu` layout and the newer `eventid_pdu` layout. It infers the
homeserver name from the server part of IDs in `userid_password`, and requires
those IDs to agree. Federated user rows can make that ambiguous; pass the
configured server name as the optional third argument in that case. The PDU
body's own `event_id` is authoritative if a legacy index disagrees.

Verdicts are unioned across every store a source version may use:
`eventid_rejections` / `eventid_softfailed` (one-byte codes), the legacy
`eventid_status` map, the retired `rejectedeventids`, `adminrejectedeventids`,
and `softfailedeventids` sets, and the v18-v20 `eventid_metadata` layouts. More
specific codes win over a retired set; a retired-set-only verdict uses the
Unknown rejection/soft-fail code. The exporter prints per-source counts and
warns about metadata rows that match no known layout. Verdicts for event IDs
with no exported PDU are reported and omitted from the room files;
`orphan-verdicts.audit` lists each omitted ID, code, and source map. It is an
audit sidecar, not importer input. Legacy cached keys are exported as permanent
keys with `ts_added_ms=0`; no notary payload is synthesized.

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
# marker is only written when the complete set succeeds.
mtxdb import --dir /path/to/new-database/mtxdb /tmp/wombatx-mtxdb-export/*.jsonl
```

The exporter refuses an existing output directory and never writes to the
RocksDB source. Do not start the new server until both the redb metadata import
and the complete mtxdb import have succeeded.

## Verdicts from older copies

Older database copies can hold `rejectedeventids` and `softfailedeventids`
entries that later upgrades dropped. List the event IDs one per line (for
example from `ldb ... scan --no_value`) and pass them to `export-mtxdb`:

```text
export-mtxdb <rocksdb> <output-dir> wombatx.me \
  --extra-rejected  rejected-ids.txt \
  --extra-soft-failed soft-failed-ids.txt
```

Verdicts the primary database already has win. IDs with no exported PDU are
dropped with the other orphan verdicts and listed in `orphan-verdicts.audit`.
`make migrate/export` takes the same files through `MIGRATE_REJECTED` and
`MIGRATE_SOFT_FAILED`.

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

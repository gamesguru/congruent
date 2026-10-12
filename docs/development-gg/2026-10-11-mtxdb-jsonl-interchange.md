# mtxdb JSONL interchange

The server export is the interchange contract. A full migration consists of
one room export for every room, plus the global signing-key export. mtxdb
rebuilds event locators, edges, state roots, HAMT nodes, and auth closures from
the imported events; those derived structures are not serialized.

The importer must finish the complete import and write the import marker as
the final transaction. The server refuses to start against an existing
RocksDB event or signing-key store without a compatible marker.

## Room export

The UTF-8 JSONL room format is `mtxdb-export/v1`. Each line is a compact JSON
object terminated by LF, tagged by the `t` field. The stream contains one
`header`, zero or more `event` records, and one final `end` record. A stream
without `end` is incomplete. The current format does not include a checksum.

Example header:

```json
{"t":"header","format":"mtxdb-export/v1","server":"example.org","room_id":"!room:example.org"}
```

Each `event` record contains:

- `event_id`: the Matrix event ID.
- `pdu`: the stored event JSON object.
- `prev_events` and `auth_events`: copied from the PDU for direct edge
  rebuilding.
- `outlier`, `soft_failed`, and `rejected`: disposition flags.
- Optional `soft_fail_code` and `rejection_code`: numeric marker codes.
- Optional `redacted_by`: the event ID of the redacting event.

Example event:

```json
{"t":"event","event_id":"$event:example.org","prev_events":[],"auth_events":[],"outlier":false,"soft_failed":false,"rejected":false,"pdu":{}}
```

The `end` record carries the event count:

```json
{"t":"end","events":20}
```

Importers validate that every event belongs to the room in the header, that
the PDU identity agrees with `event_id`, and that edge fields agree with the
PDU. They persist verdict codes and redaction metadata alongside the original
PDU, then rebuild derived state through mtxdb's native APIs.

## Derived state (not exported)

`mtxdb-export/v1` carries no room state: no state roots, HAMT nodes, state
entries, short ids, locators, or auth closures. They are derived from the
events, and the on-disk layout belongs to mtxdb, so the format does not
serialize it.

How an importer reproduces the server's state from `event` records is
deliberately deferred. Open questions for a later revision:

- Whether the importer re-runs state resolution over the imported DAG, or the
  export gains an explicit per-room state record (type, state key, event ID
  entries for each current-state tip) that the importer only has to load.
- How the importer checks that its derived state agrees with the exporting
  server's, for example by comparing a per-room digest.
- How events whose verdicts affect resolution (`rejected`, `soft_failed`,
  `outlier`) are treated while deriving state.

Until that is specified, `v1` consumers must not assume any state is present,
and must not infer one from the order of `event` records.

## Global signing-key export

Signing keys are global, so they use a separate JSONL file produced by
`!admin yolo export-mtxdb-keys`. Each line has `t: "server_keys"`, an
`origin`, a `kind`, and a base64 `payload`. The supported kinds are
`historical`, `provisional`, `corroborated`, and `notary`. The first three
payloads are the serialized record values; the notary payload is the original
self-signed JSON document. Importers decode each payload and store it in the
matching global mtxdb key record.

The room exports are produced by `!admin yolo export-mtxdb --all`. Import all
room files and the global key file before starting the server. Importing only
some rooms or omitting the key file is not a complete migration.

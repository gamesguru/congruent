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

### Verdicts and the outlier flag

`outlier` is an implementation concept, not a spec one: an event stored without
known state at that event. It is independent of the verdicts and the importer
must not derive one from the other wholesale. The Matrix server-server spec
(checks on receipt of a PDU, "Soft failure", "Rejection") decides it:

- A soft-failed event is a valid member of the DAG. It participates in state
  resolution, can appear in the current state, and is served over federation.
  It is only withheld from clients and kept out of forward extremities. The
  importer must preserve its `outlier` flag as exported and must never set
  `outlier` because of `soft_failed`.
- A rejected event never updates state, is not relayed to clients, and is not
  used as a `prev_event` by new events. It may still be part of the DAG. The
  importer may store a rejected event as an outlier, but in either case it
  must record the rejection verdict.
- An event with a missing auth event cannot pass the auth-events check, so it
  is rejected (see "Auth closures"). It is stored as an outlier.

Hiding rejected and soft-failed events from clients is a read-time filter on
the verdicts. It does not require changing where the event is stored.

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

## Auth closures

Auth closures are not exported. They are a pure function of the `auth_events`
edges already carried by every `event` record, so the importer rebuilds them.
Shipping them would add a second source of truth that has to be validated
against the edges, and their size grows with chain length for every event.

The importer builds closures in one pass in the order the events arrive,
which is dependency order: an event's closure is the union of each auth
event's closure plus that auth event itself. This is the same construction as
rezzy's `AuthGraph::build`, which keeps a transitive-ancestor bitmap per event
over a dense index.

A missing auth event never fails the whole import, and the affected event is
not dropped. The importer stores it as an outlier with a rejection verdict, the
same way the server records an event it could not authorize:

- An event with an `auth_events` id that is not in the room's export is stored
  with `outlier: true` and rejection code `MissingAuthEvent` (4). That code is
  retryable: the event is unjudged, not known bad, and a later backfill that
  supplies the auth event can clear it.
- An event that cites an auth event the importer has itself rejected is stored
  with `outlier: true` and `DependsOnRejectedAuthEvent` (2), which is permanent.
  The importer applies this in dependency order, so the rejection cascades to
  every descendant through `auth_events`. This mirrors rezzy's
  `check_auth_chain`, which rejects events that cite a rejected auth event.
- A missing `prev_events` id is not an auth problem and does not change the
  event's verdict. It is reported as a gap, like rezzy's backward extremities.
- An event that already carries a stronger verdict in the export keeps it. The
  importer only adds a verdict to an event that has none.
- Such an event is excluded from forward extremities and from derived state,
  like any other outlier or rejected event. Its closure, if stored, covers only
  the auth events that were present; no consumer may treat it as complete.

If the `m.room.create` event is the one that is missing or rejected, nothing in
the room can be authorized, so the importer fails that room as a whole. Other
rooms are unaffected.

The import returns a warning list and does not abort. Each entry names the
event, the class, and the cause: missing prev, missing auth, or cascaded from a
rejected auth event, so a lossy import is distinguishable from a clean one. The
final marker transaction records the per-class counts.

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

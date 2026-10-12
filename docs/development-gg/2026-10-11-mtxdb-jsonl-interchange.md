# mtxdb JSONL interchange

The server export is the interchange contract. A full migration consists of one
room export for every room, plus the global signing-key export. The offline
importer rebuilds event locators, edges, state roots, HAMT nodes, and auth
closures from the imported events; those derived structures are not serialized.

The importer must finish the complete import, including all derived state, and
write the import marker as the final transaction. The server refuses to start
without a compatible marker when it detects legacy or imported event data.
Startup does not derive or backfill imported state.

## Room export

The UTF-8 JSONL room format is `mtxdb-export/v1`. Each line is a compact JSON
object terminated by LF, tagged by the `t` field. The stream contains one
`header`, zero or more `event` records, and one final `end` record. A stream
without `end` is incomplete. See "Integrity" below for checksums.

Example header:

```json
{
    "t": "header",
    "format": "mtxdb-export/v1",
    "server": "example.org",
    "room_id": "!room:example.org"
}
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
{
    "t": "event",
    "event_id": "$event:example.org",
    "prev_events": [],
    "auth_events": [],
    "outlier": false,
    "soft_failed": false,
    "rejected": false,
    "pdu": {}
}
```

The `end` record carries the event count:

```json
{ "t": "end", "events": 20 }
```

Importers validate that every event belongs to the room in the header, that the
PDU identity agrees with `event_id`, and that edge fields agree with the PDU.
They persist verdict codes and redaction metadata alongside the original PDU,
then rebuild all derived state through mtxdb's native APIs before committing the
marker.

### Verdicts and the outlier flag

`outlier` is an implementation concept, not a spec one: the event is stored
without a timeline position. The Matrix server-server spec ("Soft failure",
"Rejection") decides what a server does with a flagged event; where it stores
the event is the server's choice.

- A soft-failed event is a valid member of the DAG. Per the spec it takes part
  in state resolution, can appear in the current state, and is served over
  federation. It is only withheld from clients and kept out of forward
  extremities. This server (like Tuwunel) stores it as an outlier: it keeps its
  state at the event but is never given a timeline position. Synapse instead
  persists it as a normal event with a `soft_failed` flag. The exporter sends
  `outlier: true` for such an event because that is where it is stored. The
  importer does not derive `outlier` from `soft_failed`; it keeps the exported
  flag.
- A rejected event never updates state, is not relayed to clients, and is not
  used as a `prev_event` by new events. It may still be part of the DAG. The
  importer stores it as an outlier (`outlier` is set whenever `rejected` is) and
  records the rejection verdict.
- An event with a missing auth event cannot pass the auth-events check, so it is
  rejected (see "Auth closures") and stored as an outlier.

The importer therefore sets `outlier` in exactly three cases: the export says
so, the event is rejected, or the importer forces a rejection because of a
missing or rejected auth event. A verdict record is written for every outlier,
even one with no other verdict, because the flag is part of the verdict record.

Soft-failed outliers remain inputs to offline state derivation. The importer
must not skip an event only because it is stored as an outlier or carries a
soft-fail verdict. Rejected events are excluded from resolved state.

Hiding rejected and soft-failed events from clients is a read-time filter on the
verdicts.

## Derived state (not exported)

`mtxdb-export/v1` carries no room state: no state roots, HAMT nodes, state
entries, short ids, locators, or auth closures. They are derived from the
events, and the on-disk layout belongs to mtxdb, so the format does not
serialize it.

For each room, the offline importer computes state resolution from the complete
event DAG and verdicts, then writes the resulting roots and HAMT nodes together
with the per-event roots the server reads. It also builds the auth closures from
the imported `auth_events` graph. The importer must finish these writes before
it writes the marker. It must not infer state from the order of `event` records.

### Server-ready import checklist

The importer may write the completion marker only after every item below is
persisted. These are the records the server's current read paths need to serve a
room without startup derivation or a background repair pass.

| Data                           | Current server read path                                                                                           | Current backing store                                                                | Required import output                                                                                                                                                                                       |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| HAMT nodes                     | `rooms::state_hamt::Store::get_node` / `read_node_verified`                                                        | mtxdb `State` shard                                                                  | Every node reachable from each imported root, encoded with the server's rezzy HAMT codec and structural key.                                                                                                 |
| Current room root              | `rooms::state::get_room_state_hamt`                                                                                | RocksDB `roomid_roothandle` (although writes also publish to mtxdb)                  | The final current-state `RootHandle` for the room in mtxdb's room-roots collection.                                                                                                                          |
| Per-event state root           | `rooms::state::get_roothandle`, after resolving short event ID in `rooms::short`                                   | RocksDB `shorteventid_roothandle` (although writes also publish to mtxdb)            | A `RootHandle` for every imported event whose state snapshot the server may query, keyed so the server can find it from the event ID.                                                                        |
| Short IDs used by state leaves | `rooms::short::get_shorteventid`, `get_shortstatekey`, and their reverse lookups                                   | RocksDB short-ID maps and counters                                                   | Stable event and state-key IDs plus the reverse mappings required to resolve every HAMT leaf.                                                                                                                |
| Direct auth edges              | `rooms::auth_chain::get_auth_chain_inner` via `timeline::multi_get_shortauthevents`; also state-event auth lookups | RocksDB `shorteventid_shortauthevents`; mtxdb has shadow-written full event-ID edges | Auth-event IDs for every imported event, in the mtxdb edge layout.                                                                                                                                           |
| Transitive auth closure        | `rooms::auth_chain::Data::get_cached_eventid_authchain`                                                            | RocksDB `shorteventid_authchain`, with an in-memory cache                            | The complete closure bitmap for every event in the representation used by the server, or a server change to read the imported mtxdb closure directly. A cache miss must not trigger a rebuild after startup. |

`mtxdb::events::EventAdapter` already has persisted-closure reads, but the live
`rooms::auth_chain` service does not call them. Its `get_auth_chain` path walks
the RocksDB short-auth edge table on a cache miss. Therefore writing mtxdb
closures alone does not currently make the server consume them; the server-side
cutover must precede the importer work that targets those records.

The root and closure rows above are derived from the complete event DAG and
verdicts during the offline import. The marker is written only after they and
the reachable HAMT nodes are committed. Round-trip validation must query the
server adapters and compare the imported room's current state and per-event
state against the source room before this checklist is considered satisfied.

## Auth closures

Auth closures are not exported. They are a pure function of the `auth_events`
edges already carried by every `event` record, so the importer rebuilds them.
Shipping them would add a second source of truth that has to be validated
against the edges, and their size grows with chain length for every event.

The importer builds closures from the complete auth graph, independent of input
record order: an event's closure is the union of each auth event's closure plus
that auth event itself. This is the same construction as rezzy's
`AuthGraph::build`, which keeps a transitive-ancestor bitmap per event over a
dense index.

A missing auth event never fails the whole import, and the affected event is not
dropped. The importer stores it as an outlier with a rejection verdict, the same
way the server records an event it could not authorize:

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
- Such an event is excluded from forward extremities and resolved state. Its
  closure covers only auth events present in the export; no consumer may treat
  it as complete.

If the `m.room.create` event is the one that is missing or rejected, nothing in
the room can be authorized, so the importer fails that room as a whole. Other
rooms are unaffected.

The import returns a warning list and does not abort. Each entry names the
event, the class, and the cause: missing prev, missing auth, or cascaded from a
rejected auth event, so a lossy import is distinguishable from a clean one. The
final marker transaction records the per-class counts.

## Integrity

The format carries no checksum. Integrity rests on three things:

- Each `pdu` is covered by its event ID, which is a hash of the PDU in room
  versions 3 and later. Importers verify `pdu` against `event_id`.
- The `end` record's event count detects a truncated file.
- Transport corruption is detected outside the format. Compress the file with
  gzip or zstd (both checksum their content) or ship a `SHA256SUMS` sidecar, as
  the test fixtures do.

The metadata around the PDU (verdict flags, codes, `redacted_by`, `outlier`) is
not individually checksummed.

## Global signing-key export

Signing keys are global, so they use a separate JSONL file produced by
`!admin yolo export-mtxdb-keys`. Each line has `t: "server_keys"`, an `origin`,
a `kind`, and a base64 `payload`. The supported kinds are `historical`,
`provisional`, `corroborated`, and `notary`. The first three payloads are the
serialized record values; the notary payload is the original self-signed JSON
document. Importers decode each payload and store it in the matching global
mtxdb key record.

The room exports are produced by `!admin yolo export-mtxdb --all`. Import all
room files and the global key file before starting the server. Importing only
some rooms or omitting the key file is not a complete migration.

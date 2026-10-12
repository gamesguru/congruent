# mtxdb room JSONL interchange format

This document proposes the versioned JSONL contract for moving a complete
Matrix room from a conduwuit database into mtxdb. The producer reads the
source database; `mtxdb import` consumes the stream and derives mtxdb-owned
indexes, locators, short IDs, and auth closures.

In the current conduwuit tree, the HAMT `Map` used by this export is backed by
the redb metadata engine. The interchange format is independent of that source
backend; it describes logical room data and portable HAMT payloads, not a
redb file or a RocksDB column-family dump.

The existing `mtxdb import` JSONL mode accepts one raw Matrix event object per
line. That format remains supported for event-only imports. It is not the
complete room interchange format: it carries no HAMT nodes or state roots.
Complete room exports use the tagged format below, and the CLI must explicitly
support it before an export is described as importable.

## Framing

The file is UTF-8. Each non-empty line is exactly one compact JSON object
followed by LF. Every record has the same `format` value and a `type` tag.
Record order is significant:

1. One `header` record.
2. Zero or more `state_node` records.
3. Zero or more `state_root` records.
4. Zero or more `event` records.
5. Exactly one `end` record, last.

An importer rejects unknown format versions, duplicate headers or trailers,
records after `end`, malformed records, and a stream without `end`. This makes
a truncated stream detectable. Import is all-or-nothing at the room level:
the importer validates the complete stream and its references before
publishing its transaction.

The format identifier is `mtxdb.room-jsonl/v1`. A future incompatible change
uses a new identifier. Fields not defined here are not silently interpreted by
version 1 importers.

## Records

### Header

```json
{"format":"mtxdb.room-jsonl/v1","type":"header","source":"conduwuit","server":"example.org","rooms":["!room:example.org"]}
```

`rooms` lists the room IDs represented by the stream. It permits a multi-room
export while keeping each data record independently attributable. `server` is
the exporting server name and is informational; it does not affect record
identity.

### State node

```json
{"format":"mtxdb.room-jsonl/v1","type":"state_node","room_id":"!room:example.org","structural_hash":"<64 lowercase hex digits>","bytes":"<base64>"}
```

`bytes` is the exact encoded HAMT node payload from the source database.
`structural_hash` is the full 32-byte hash, not a truncated mtxdb record ID.
The importer verifies that the payload decodes and hashes to this value before
storing it. A node appears once per room and must precede every root that
references it. Implementations may deduplicate identical nodes internally.

### State root

```json
{"format":"mtxdb.room-jsonl/v1","type":"state_root","room_id":"!room:example.org","kind":"room","event_id":null,"handle":{"codec_version":1,"routing_version":1,"routing_params":"<8 lowercase hex digits>","structural_hash":"<64 lowercase hex digits>","state_group_id":"<64 lowercase hex digits>"}}
```

`kind` is `room` for the room's current-state root or `event` for an event's
post-event state root. `event_id` is null for `room` and the Matrix event ID
for `event`. A room may have at most one `room` root and at most one `event`
root for a given event ID. The handle fields preserve the complete source
`RootHandle`; they are explicit so consumers do not have to infer versions
from the current software build. The root's structural hash must name a
previously supplied node.

### Event

```json
{"format":"mtxdb.room-jsonl/v1","type":"event","room_id":"!room:example.org","event_id":"$event:example.org","pdu":{},"outlier":false,"soft_failed":false,"rejected":false}
```

`pdu` is the complete stored Matrix event JSON object. Its event ID and room ID
must match the record's `event_id` and `room_id`. The importer extracts
`prev_events` and `auth_events` from the PDU; they are not copied into parallel
fields. The three disposition flags preserve source metadata and default to
false only in formats that explicitly permit their omission. Event records
follow all state nodes and roots. Their order does not imply DAG order; the
importer derives graph relationships from the PDU.

### End

```json
{"format":"mtxdb.room-jsonl/v1","type":"end","counts":{"state_nodes":12,"state_roots":4,"events":20},"sha256":"<64 lowercase hex digits>"}
```

Counts are exact record counts, excluding `header` and `end`. `sha256` is the
SHA-256 digest of the exact UTF-8 bytes of every preceding line, including each
line's terminating LF and excluding the `end` line. The importer verifies both
counts and digest before committing. A producer writes `end` only after all
records have been emitted successfully.

## Import behavior

`mtxdb import` should retain its current raw-event JSONL behavior and add
explicit detection or a format option for `mtxdb.room-jsonl/v1`. It must not
feed tagged records to the existing Matrix-event parser. For the versioned
format, it validates the framing, checksum, counts, room membership, PDU
identity, node hashes, and root references; then writes events, state nodes,
and roots through mtxdb's native APIs in one atomic import operation. Derived
indexes and auth closures are rebuilt by mtxdb and are not part of this file.

An import must fail clearly if the target mtxdb version cannot preserve the
source root-handle/HAMT versions. It must not silently discard roots or
substitute newly resolved state for the supplied historical state.

## Compatibility note

The mtxdb CLI also has an inspection envelope named `mtxdb.export/v1`. That
envelope describes individual records already stored in mtxdb and is explicitly
not accepted by `mtxdb import`; it is not this room interchange format.

The current draft server exporter uses the tag `mtxdb-export/1`, emits
`header`/`node`/`root`/`event`/`end` records, and encodes the root handle as an
opaque base64 byte string. That is a draft, not this contract: it does not use
the format identifier, record names, explicit root fields, or integrity rules
defined here. The exporter and CLI importer must be aligned to this document
before the output is called a universal or importable format.

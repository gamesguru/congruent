# PDU reference edges

An _edge_ is any field of a PDU that names another event (or room) by id. For
locally constructed events (`create_event`), every edge that should stay inside
the room must be checked before the event is signed. Remote events are out of
scope: they may legitimately cite events we cannot resolve yet.

Rule for local construction:

- **Structural** edges (`prev_events`, `auth_events`): a missing or other-room
  target is an error.
- **Semantic** edges (relations, replies, redactions, pins): a target found in
  another room is an error; a target we do not have is allowed (a local user may
  react to an event not yet backfilled).
- **Cross-room by design**: never enforce room locality.

Statuses below are `yes` (enforced in `create_event`), `no` (not yet enforced),
and `n/a` (not applicable: server-populated, to-device, account data, or
cross-room by design).

## How this was derived

- **Spec** (`../spec`, `data/event-schemas/schema/*.yaml` and
  `content/**/*.md`): every schema field typed as an event id, plus the module
  docs for relations, replies, threads, replacements, annotations, references,
  redactions (room v11) and pinned events.
- **Real PDUs** (`/run/media/shane/shane4tb-ent/dags/merged`): 456,982 event
  objects across 25 PDU JSONL files (23 merged or remote DAG inputs and two
  local-DAG fixtures). One `*.rezzy-meta.jsonl` file has 82,235 metadata records
  and is excluded. The 539,217 total counts PDU and metadata lines together, not
  just events. The scan checks event-id-shaped strings under `prev_events`,
  `auth_events`, `redacts`, `content` and `unsigned`, grouped by event type and
  normalised JSON path. Counts below are occurrences, not distinct events. The
  scan scripts are throwaway, not in-tree.
- Not measured: whether any edge in that data actually crosses rooms. This is an
  inventory of where ids appear, not of which are violated.

## 1. Structural edges (top-level PDU fields)

| Edge             | Field                    | Rule       | Status |
| ---------------- | ------------------------ | ---------- | ------ |
| Previous events  | `prev_events[]`          | structural | yes    |
| Auth events      | `auth_events[]`          | structural | yes    |
| Redaction target | top-level `redacts`      | semantic   | yes    |
| Redaction target | `content.redacts` (v11+) | semantic   | no     |
| Event identity   | `event_id`, `room_id`    | n/a        | n/a    |

Details:

- **`prev_events[]` - structural, yes.** A missing or other-room target is an
  error in the depth loop.
- **`auth_events[]` - structural, yes.** `get_auth_events` is room-scoped; the
  fork path re-reads from the room's own state HAMT.
- **Top-level `redacts` - semantic, yes.** This is the builder field, used by
  pre-v11 rooms (4,640 occurrences in the data).
- **`content.redacts` - semantic, yes.** This is the canonical field in room v11
  and later (16,448 occurrences). The client redact API sets both fields
  (`src/api/client/redact.rs:36-38`); `create_event` checks each independently.
- **`event_id` and `room_id` - n/a.** These are the event's own identity, not
  references.
- **Legacy `prev_state[]` - not emitted by the local PDU model.** It appears in
  older room-version redaction allow-lists, but is absent from the local `Pdu`
  type. The corpus has 55,487 instances, all empty arrays; no event IDs were
  found in them.

## 2. Relations, enforced

All of these are under `content.m.relates_to` and are covered by the generic
`m.relates_to.event_id` check, whatever the `rel_type`:

| Relation                                       | Spec                              | Data                |
| ---------------------------------------------- | --------------------------------- | ------------------- |
| `m.annotation` (`m.reaction`)                  | event_annotations                 | 25,259              |
| `m.replace` (edits)                            | event_replacements                | in `m.room.message` |
| `m.thread`                                     | threading                         | in `m.room.message` |
| `m.reference`                                  | reference_relations               | in `m.room.message` |
| in-room verification (`m.key.verification.*`)  | `m.key.verification.m.relates_to` | 4                   |
| polls (`org.matrix.msc3381.poll.response/end`) | MSC3381                           | 205                 |
| encrypted wrappers (`m.room.encrypted`)        | relation stays in clear           | 113                 |
| stickers                                       | `m.relates_to`                    | 1                   |

| Field                                         | Rule     | Status |
| --------------------------------------------- | -------- | ------ |
| `content.m.relates_to.event_id`               | semantic | yes    |
| `content.m.relates_to.m.in_reply_to.event_id` | semantic | yes    |

Details:

- `m.relates_to.event_id` covers annotations, edits, threads, references, poll
  responses/ends and other `rel_type`s.
- `m.in_reply_to.event_id` covers rich replies and thread fallback replies. It
  appears 74,190 times in `m.room.message`, 534 in `m.room.encrypted`, 40 in
  `m.sticker` and 7 in `m.room.member`.
- A reply's `m.in_reply_to` may stand alone (no `rel_type` and no `event_id` in
  `m.relates_to`), so both paths are read independently.

## 3. Event-id fields in spec event types

| Field                           | Event                            | Rule         | Status |
| ------------------------------- | -------------------------------- | ------------ | ------ |
| `content.pinned[]`              | `m.room.pinned_events`           | semantic     | yes    |
| `content.predecessor.event_id`  | `m.room.create` (removed in v12) | cross-room   | n/a    |
| `content.event_id`              | `m.fully_read`                   | account data | n/a    |
| event-id keys                   | `m.receipt`                      | EDU          | n/a    |
| `content.m.relates_to.event_id` | `m.key.verification.*`           | to-device    | n/a    |

Details:

- `content.pinned[]`: 1,149 occurrences. A pin of a foreign-room event is
  exactly the poisoning this inventory is meant to catch.
- `content.predecessor.event_id` points at the old room's last event; never
  enforce room locality.
- `m.fully_read` is account data and `m.receipt` is an EDU, so neither is a PDU.
- `m.key.verification.*` events are to-device messages; their `m.relates_to`
  does not name a room event.

No other current spec event schema names an event id. The remaining spec ids are
room ids, covered in section 6.

## 4. Edges in real PDUs that the spec does not define

Vendor and unstable fields. Only the Nheko mirror is checked today; the rest
are not. For each, decide whether the id is a same-room edge (check it), an
origin-room reference (forwards, cross-room by design) or an opaque external id
(ignore).

- **Nheko relations**: `content.im.nheko.relations.v1.relations[].event_id`
  Appears on `m.room.message` (6,372), `m.reaction` (531) and `m.room.encrypted`
  (122). It mirrors `m.relates_to` for encrypted rooms. Semantic, yes:
  a cross-room target is rejected; malformed entries are skipped.
- **Nested replacement relation**: `content.m.new_content.m.relates_to...`
  Appears on edits (8 + 8). Not an edge on its own: the spec says any
  `m.relates_to` inside `m.new_content` is ignored, and the outer `m.relates_to`
  is the edge.
- **Double nesting**: `content.content.m.relates_to.event_id` Appears on
  `m.room.message` (2) and `m.reaction` (1). Malformed client output; ignore.
- **Bot pointers**: `content.net.31a05b.triggered_by.event_id` (689),
  `content.xyz.maubot.reminder.reply_to` (4) and `content.pong.ping` (160), all
  on `m.room.message`. Proposed: optional semantic checks.
- **Forwards**: `content.com.famedly.app.forwarded.event_id` and `.room_id` One
  occurrence. Also `content.moe.sable.message.forward.original_event_id` and
  `.original_room_id` (2). The id belongs to the origin room: cross-room by
  design.
- **Bridge ids**: `content.origin_id` (1) and `content.remote_id` (11) are
  opaque external ids that happen to look like event ids; ignore.
- **Message text**: `content.body` (2) just contains an event id typed into the
  message; not an edge.
- **MSC2836**: `content.m.relationship.event_id` Did not appear in the data. It
  is a real gap in the spec/MSC sense but has no sample here. Proposed:
  semantic, not enforced.

## 5. Server-populated, not client-settable

All of these live under `unsigned`, which we populate from our own state:

- `unsigned.replaces_state` (63,379): event id of the previous state event.
- `unsigned.prev_content` (for example `.pinned[]`): a copy of the replaced
  state's content, which may itself contain event ids.
- `unsigned.redacted_by` (180) and `unsigned.redacted_because` (about 9,800):
  embed the redaction event, including its `prev_events`, `auth_events` and
  `redacts`.
- `unsigned.m.relations.<rel_type>.latest_event` (about 270): a bundled
  aggregation containing an embedded event and its relation ids. The spec
  defines bundles for `m.replace`, `m.thread` and `m.reference`.
- `unsigned.invite_room_state` and `unsigned.knock_room_state` (26): stripped
  state of the room being joined or invited to; ids refer to _that_ room.

Not validated at construction. Revisit if any becomes client-settable, since
`unsigned` is stripped from client-supplied content before signing.

## 6. Room-id edges (not event ids, but room locality matters)

| Field                         | Event                | Status     |
| ----------------------------- | -------------------- | ---------- |
| state key                     | `m.space.child`      | cross-room |
| state key                     | `m.space.parent`     | cross-room |
| `content.allow[].room_id`     | `m.room.join_rules`  | cross-room |
| `content.predecessor.room_id` | `m.room.create`      | cross-room |
| `content.replacement_room`    | `m.room.tombstone`   | cross-room |
| `content.entity`              | `m.policy.rule.room` | cross-room |

Details:

- `m.space.child` and `m.space.parent` use a room id as the state key (198 and
  29 occurrences in the data).
- `content.allow[].room_id` is for restricted joins (room v8+). A policy rule's
  `content.entity` may also be a room id or alias. Both refer to other rooms by
  design.
- Room ids also appear in to-device and account-data events (`m.room_key*`,
  `m.forwarded_room_key`, `m.room_key.withheld`, `m.direct`,
  `m.image_pack.rooms`). Those are not PDUs and are outside this inventory.

## Summary of what is missing

Enforce next (same-room or reject), in priority order by volume and risk:

1. MSC2836 `m.relationship.event_id` (spec/MSC only, no sample).

Optional: vendor `triggered_by`, `reply_to` and `pong.ping`.

Never enforce: forwards, predecessor, space and restricted-join room ids.

## Adding an edge

1. Decide structural vs semantic vs cross-room and add a row above.
2. Resolve the target with `get_pdu_in_room(room_id, id)`. A not-found result is
   allowed for semantic edges; a room mismatch never is.
3. Add a test next to `create_event_rejects_cross_room_*` in
   `src/service/rooms/state/tests.rs` that points the edge at an event in
   another room and asserts the mismatch error.

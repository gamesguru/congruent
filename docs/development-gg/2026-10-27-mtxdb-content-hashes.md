# mtxdb migration: content hashes, redactions, and count reconciliation

Date: 2026-10-27

This note records the final `wombatx.me` RocksDB → mtxdb export/import behavior
for content hashes and redactions. It is a companion to
`2026-10-11-mtxdb-jsonl-interchange.md` and the
`tools/conduwuit-db-migrate/README.md` runbook.

## What the exporter must emit

A legacy PDU is not necessarily the wire event. The old store serializes its
in-memory `Pdu`, which adds local fields that are not part of the hashed event.
The content hash is defined over the event with `unsigned`, `signatures`, and
`hashes` removed; it does not know about room-version-specific local fields.
`parse_pdu` therefore normalizes each exported PDU:

| Field                             | Export rule                                                           | Why                                                                                                     |
| --------------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `event_id`                        | Remove for v3+ IDs (bare `$hash`). Keep v1/v2 IDs (`$opaque:server`). | v3+ event IDs are derived and are not in the event. v1/v2 IDs are opaque, server-assigned event fields. |
| `age_ts`                          | Remove.                                                               | Local timestamp added by the legacy store; never hashed.                                                |
| `room_id` on v12+ `m.room.create` | Remove.                                                               | v12 derives the room ID from the create event; the create event omits it.                               |
| `room_id` on every other event    | Restore the inferred room ID if missing.                              | The hashed event includes `room_id`; conduwuit dropped it from some stored PDUs.                        |

The event ID and room ID remain in the record/header for the importer. The
exporter uses the `:` in the event ID to distinguish v1/v2 from v3+ without
needing the room version at parse time. A future v1/v2 migration must preserve
that distinction.

After these fixes, the remaining content-hash mismatches are not caused by
exporter-added fields. A direct scan of the final export found:

- 1,036,611 event records
- 25,679 content-hash mismatches, matching the import log
- 0 occurrences of `age_ts`
- 56 events without `room_id`, all v12+ creates (the only correct omissions)

## Content-hash mismatches are handled by redaction

The Matrix spec's received-PDU checks say:

> Passes hash checks, otherwise it is redacted before being processed further.

The Signing Events section is more explicit:

> If the hash check fails, then it is assumed that this is because we have only
> been given a redacted version of the event. To enforce this, the receiving
> server should use the redacted copy it calculated rather than the full copy it
> received.

Source:
<https://spec.matrix.org/latest/server-server-api/#validating-hashes-and-signatures-on-received-events>

The redaction algorithm preserves `hashes` and `signatures` while replacing
`content` with the room-version-specific redaction result. A stored event that
has already been redacted therefore no longer matches its original content hash.
The importer's behavior follows the spec:

1. Recompute the content hash.
2. On mismatch, build the canonical redacted PDU for the room version.
3. Store that redacted PDU and run the auth checks against it.
4. Leave the original hash in the export as evidence of the mismatch.

Recomputing and re-redacting an already-redacted event is idempotent for the
content that has already been removed. It does not make the mismatch disappear,
and it does not prove why the mismatch exists.

A mismatch is therefore **evidence**, not proof, that the event was redacted by
a room redaction. The redaction-shaped contents seen in this export (`{}`,
`{"membership":"join"}`, and preserved redaction keys) are consistent with
already-redacted events, but only an audit of each mismatch plus the original
source can establish the cause. The importer must not skip the redaction step
just because the content looks already redacted.

## Final run

The corrected export and clean import produced:

| Metric                                              |                                     Value |
| --------------------------------------------------- | ----------------------------------------: |
| Exported events                                     |                                 1,036,611 |
| Imported events                                     |                                 1,036,607 |
| Auth closures                                       |                                 1,036,537 |
| Signing keys                                        | 12,938 permanent, 0 provisional, 0 notary |
| Content-hash mismatches                             |                                    25,679 |
| Exported accepted but auth-failed (stored rejected) |                                         1 |
| Exported rejected but auth-passed                   |                                         0 |
| Cascaded rejections                                 |                                        57 |
| Missing auth events                                 |                                        13 |
| Missing prev events                                 |                                    67,316 |
| Failed input files                                  |                                         0 |

The 25,679 mismatches are the events for which the importer stored the canonical
redacted form. The sample contents are dominated by membership-only joins, empty
contents, and redaction-preserved fields. This is consistent with the source
rewriting stored events when they are redacted, but the count alone does not
prove that every mismatch is a legitimate room redaction.

The 57 cascades are downstream of the 13 missing auth events in `!UbCm…` (1 + 5)
and `!iMZE…` (9 + 52). The single accepted-but-failed event is `$zzjanv…` in
`!L58…`, where an `m.room.name` exceeds the sender's power level; it is stored
rejected.

## The four-event difference

The import stores four fewer events than the export. This is not a duplicate or
collision problem:

- The export contains 1,036,611 event records.
- There are 1,036,611 unique event IDs globally; no event ID occurs twice.
- The three rooms excluded by `MIGRATE_SKIP_ROOMS` contain exactly four events.
- The unique IDs in all non-skipped room files total 1,036,607, exactly the
  number stored.

The four events are:

| Room                                     | Events | Event IDs                                                                                      |
| ---------------------------------------- | -----: | ---------------------------------------------------------------------------------------------- |
| `!GmViaerQkRWufXAOKV:matrix.koolant.net` |      1 | `$ZPMW_o0VY1YG2VP5aWHzGwoXh6dkyZotbGLa6nSESzU`                                                 |
| `!KJCvzPiZICmWXQywiv:matrix.org`         |      1 | `$zZp2szc063P57TW75zinoGwi713olsNSB5IgUG7GiRY`                                                 |
| `!mefQhZzgTaxNCNzAeK:kde.org`            |      2 | `$cR9X1qL-h4UwoVMmC61owKcu4hvjMYRr1xM7xCHR5rk`, `$yeVXBC-SajxErgYzige3822BJbTETJZ0Np4ydH4StQc` |

These rooms have no stored `m.room.create` or auth events; they hold only leave
events for `@shane:wombatx.me` from declined or never-joined invites. They are
skipped intentionally; losing them changes no room state. See the exporter
README for the attempted federation recovery.

## Remaining audit work

The import currently emits a warning with up to ten mismatched event IDs per
room, but it does not yet persist a per-event mismatch audit. To make the
"evidence, not proof" distinction reviewable, the audit row should record:

- `event_id`
- `room_id`
- stored `hashes.sha256`
- recomputed content hash
- whether the incoming PDU was already redaction-shaped
- the room version used for redaction

That audit, plus the signature verification/key-timestamp work, remains open.

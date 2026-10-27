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
| Keys corroborated by verified events                |                                    11,917 |
| Events with no usable in-window key                 |                                   201,944 |
| Events with no signature material                   |                                        27 |
| Events with invalid signatures (stored rejected)    |                                       148 |
| Content-hash mismatches                             |                                    25,679 |
| Content-differing mismatches                        |                                        17 |
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

## Signature verification

Rooms are imported after the signing-key export, so every event can be checked
against the key catalog without a live key scan. A key is usable only when the
event's `origin_server_ts` falls inside the key's validity window
(`valid_until_ts` and `expired_ts`), following MSC4499. The outcomes are kept
separate:

| Outcome                                        | Events |
| ---------------------------------------------- | -----: |
| No usable in-window key (stored as accepted)   | 201,944 |
| No signature material to check (stored as accepted) | 27 |
| Invalid signature (stored as rejected)         |    148 |
| Verified by an in-window key                   | 834,488 |

The verified total is the remainder of the 1,036,607 imported events after the
three unverified outcomes. The no-key total reflects coverage, not a decision: an event with no usable key
is stored accepted because there is nothing to verify against, and the key
remains uncorroborated. The 148 invalid signatures are derived refusals, not
exported verdicts, and use `m.signature_verification_failed`.

Verified observations replace the key's candidate times: `first_seen_ms` and
`last_active_ms` come only from events that actually verified,
`ts_added_ms` remains the export time, and the key binding is unchanged. Keys
with no verified event keep no candidate times and stay uncorroborated. In this
run 11,917 of the 12,938 keys were corroborated; the remaining 1,021 are not an
error under MSC4499.

The content-differing events were checked the same way:
16 of the 17 verified. One, `$DtItYlMAnhae0wiVHnfCEGKUdnhPRVVrO5mhHiqxqdE` in
`!iMZE…` (room version 6), had no usable in-window key; none of the 17 had an
invalid signature. A content-hash mismatch and a signature failure are
independent outcomes.

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

## Content-hash audit

Each run writes `content-hash-mismatches.jsonl` next to the mtxdb root. The
sidecar holds one row per mismatch with:

- `event_id`
- `room_id`
- stored `hashes.sha256`
- recomputed content hash
- `content_matches_redaction`
- the room version used for redaction
- `signature_status` (`verified`, `no_usable_key`, `no_signature_material`, or
  `invalid`) when signature checking was available

The human-readable log lists only the 17 events whose content actually differs
from the canonical redaction, plus the total. The sidecar retains all 25,679
rows, so the "evidence, not proof" distinction remains reviewable without
flooding the log. A same-content mismatch stays in the sidecar as evidence but
does not imply data loss: the stored event is already the canonical redaction.

# nutra.tk drift in unredacted longue

```shell
uwu> yolo compare-room-state !sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE unredacted.org
| level | span | message |
| ------: | :-----: | :------- |
|  WARN |   command    | Signature verification failed for event unknown. Error: Verification(Signature(signature::Error { source: Some(Verification equation was not satisfied) })). Available keys: {"mfrisch.com": {"ed25519:1": "dH20wi8vHM55vgKAdf9Dbziq+mzVsvZYqxYuhFL/K10"}}. Event signatures: {"mfrisch.com":{"ed25519:1":"k5+1oU+WABNOXeWbQtccJUr3cQ8dwUzXIPYWmRz2rr7PjkGVrh5cixF4RnvpKUjokzf2lxSI4AJjnodAWnh4BA"}} |
|  WARN |   command    | PDU $XmQC-8bf9mIEXy4NkoBwe0FE4tbbakKc_0CH_85WRyY failed signature verification, storing as rejected outlier: Event $XmQC-8bf9mIEXy4NkoBwe0FE4tbbakKc_0CH_85WRyY failed verification: Verification error: Could not verify signature: signature error: Verification equation was not satisfied |

Room State Comparison for !sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE vs unredacted.org
at_event (sent to remote): $OymLvYkMmv0IVdxOpaJS7-ke-kbgKr3mty-hyS03sKM
local tip: $OymLvYkMmv0IVdxOpaJS7-ke-kbgKr3mty-hyS03sKM
Missing locally: 2
Extra locally: 2
Skipped (bad sig): 1

Room SSH:        48850154
Extremities:     7
Local joined:    state=1138, cache=1138 ✓
Local invited:   state=1
Remote joined:   1137
Remote invited:  1
NOTE: Tip is a state event — injected into remote state for state-after comparison

Missing IDs: [
  $HQq7a_ms8hIEuXKT7gur3D-q4vU7Gz79QJT98SCg3qE (m.room.join_rules ) 2026-02-07 01:03:01 UTC
  $7OGzQH5IUP9l58M1es6lLi1cXkQD5dO24eXa3GGbhNw (m.room.member @rfk:unredacted.org) 2026-07-10 16:35:12 UTC [leave]
]
Extra IDs: [
  $sRaLqf_I5C0HtxIscqoTCmpSr1_kXmNVGbNRcLhLOn8 (m.room.member @rfk:unredacted.org) 2026-01-26 08:36:25 UTC [join]
  $_d9he0TxctxW5jJR2cH3QegtDo10vUsFg_iDkdEEHAI (m.room.join_rules ) 2026-03-02 16:24:34 UTC
]

uwu> debug verify-pdu $7OGzQH5IUP9l58M1es6lLi1cXkQD5dO24eXa3GGbhNw
| level | span | message |
| ------: | :-----: | :------- |
|  WARN |   command    | Signature verification failed for event $7OGzQH5IUP9l58M1es6lLi1cXkQD5dO24eXa3GGbhNw. Error: Verification(Signature(signature::Error { source: Some(Verification equation was not satisfied) })). Available keys: {"unredacted.org": {"ed25519:a_wLAi": "mz+O9Qx7wplpk9qxDYOt6ed+x0kaRg+K/Zm7DEknUnk"}}. Event signatures: {"unredacted.org":{"ed25519:a_wLAi":"g2vskkQXtGn+jiQV9ha6P6mfLJ6K8Nvyitfocl/2v/mNmJ5Rusyg1cI23jt1BeDMi3Bq5qW6FpGgIQxs27NJBw"}} |

Event: $7OGzQH5IUP9l58M1es6lLi1cXkQD5dO24eXa3GGbhNw
Room: !sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE
Type: m.room.member
Membership: leave
State key: @rfk:unredacted.org
Sender: @rfk:unredacted.org
Room Version: 12
Verify: SIGNATURE FAILED: Verification error: Could not verify signature: signature error: Verification equation was not satisfied
Auth check: PASS
Status: timeline=false outlier=true rejected=true soft_failed=false

uwu> debug get-signing-keys unredacted.org
ServerSigningKeys {
    server_name: "unredacted.org",
    verify_keys: {
        "ed25519:a_wLAi": VerifyKey {
            key: "mz+O9Qx7wplpk9qxDYOt6ed+x0kaRg+K/Zm7DEknUnk",
        },
    },
    old_verify_keys: {},
    signatures: Signatures(
        {},
    ),
    valid_until_ts: 2026-01-28T16:23:28.729,
}

uwu> debug get-pdu $7OGzQH5IUP9l58M1es6lLi1cXkQD5dO24eXa3GGbhNw
Status: Outlier PDU [REJECTED]

{
  "auth_events": [
    "$2n2ZYGPaV-lWAKt1RTyUqt03BZOuQn5Qm1qybDgEQmU",
    "$sRaLqf_I5C0HtxIscqoTCmpSr1_kXmNVGbNRcLhLOn8"
  ],
  "content": {
    "membership": "leave"
  },
  "depth": 110995,
  "event_id": "$7OGzQH5IUP9l58M1es6lLi1cXkQD5dO24eXa3GGbhNw",
  "hashes": {
    "sha256": "EAZJLmEmJgtYdauISNuADJ1dO9w/dwD4cmTrdSrOSN4"
  },
  "origin_server_ts": 1783701312894,
  "prev_events": [
    "$IsFSuGyX1HcwaC8ads0mIqjSCnQD2zEyZ4LmmziIcBU"
  ],
  "room_id": "!sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE",
  "sender": "@rfk:unredacted.org",
  "signatures": {
    "unredacted.org": {
      "ed25519:a_wLAi": "g2vskkQXtGn+jiQV9ha6P6mfLJ6K8Nvyitfocl/2v/mNmJ5Rusyg1cI23jt1BeDMi3Bq5qW6FpGgIQxs27NJBw"
    }
  },
  "state_key": "@rfk:unredacted.org",
  "type": "m.room.member",
  "unsigned": {
    "age_ts": 1783701312894
  }
}

uwu> yolo view-extremities !sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE
Room !sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE has 7 extremities:
$IsFSuGyX1HcwaC8ads0mIqjSCnQD2zEyZ4LmmziIcBU    TS: 1783699833638       Sender: @bot.draupnir:unredacted.org
$OymLvYkMmv0IVdxOpaJS7-ke-kbgKr3mty-hyS03sKM    TS: 1783907562557       Sender: @dark:nether.im
$lo2cr4dWPqZsVHfHMxr-sqCCAmFyFDmfvRXhtWmnyZU    TS: 1782291851517       Sender: @lunar:unredacted.org
$o0Bjdwqz0SCvKHdhcaEgmge3KFJZLw6A5VEYwfJi1Ag    TS: 1782275852605       Sender: @gaphag:unredacted.org
$oRXWAiMZTZp2sikl4enauIHZOX6GYVxfi3dv86qzzZ4    TS: 1782291697432       Sender: @lunar:unredacted.org
$qWT_unVE1UO0ITLZj5_5HG7T4V1IuzV2tPhT_-x1WZw    TS: 1782290185062       Sender: @viparr:unredacted.org
$vpI_IAystTKym0FA6vlJRaUg9rGhAMnkXPme6Z3pTww    TS: 1782291532059       Sender: @lunar:unredacted.org

uwu> server build-info
Build Information

Version: 0.5.9
Package: conduwuit_admin
Description: A Matrix homeserver written in Rust, the official continuation of the conduwuit homeserver.

Git Information

Commit Hash: b9a3f9b776b4a4570b730162a3ad028048574257
Commit Hash (short): b9a3f9b77
```

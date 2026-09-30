# State res V2.1 profound divergence case study (~30 members, ~3000 events)

Note that my server performed some manual intervention fairly on due to a glitch/annoyance in how Cinny combines use of the timeline membership cache and the S2C resolved state.

## Timeline only

```log
uwu> yolo get-room-dag !ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk 0 -1

Wrote 3779 PDUs to /tmp/local-dag-ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk-v12-nutra.tk-d1-3509.jsonl
PDUs:           3779
State events:   180
Branching:      1.072 avg prev_events/PDU
Frag factor:    1.076 (3779 events / 3509 depth, 4 heads)
Unique states:  191
Missing hash:   0
Tip SSH:        38267102
Room SSH:       38303242
Status:         ✓ tip is state event — room state includes tip (pre=38267102 post=38303242)
```

## Trace Gaps (Rejected Outliers)

We investigated several "phantom" nodes in the DAG visualizer. These were events that were referenced by other events in the timeline as `prev_events`, but were missing from the `get-room-dag` timeline export because they were rejected/soft-failed and resided only in the outlier tree:

1. **`$kLs29-CLnbEecpMy4txN3etFCL9PzOZSaicorfnFGNE`**
    - Depth: ~3153
    - Type: `m.room.member` (invite)
    - Status: Rejected (Outlier)

2. **`$fPVTIbPz09MM3dd6C90uxa2YadiIVqtRITwbP4IVlwU`**
    - Depth: ~661
    - Status: Rejected (Outlier)
    - Parent to: `$qhAFddgPWi4Yjg0OHnvO-qmO5xIkVpsbfDUWtV0XWLI` (d=662)

_Note: With the updated `get-room-dag` command, these are now included in the dump with the `__outlier: true` flag, allowing the DAG visualizer to properly connect and highlight them rather than rendering them as dangling external edges._

## Heads (Forward Extremities)

_(Analysis pending: need to run the head-finding script to identify all 4 timeline heads)_

## Final state (again from `nutra.tk`'s perspective)

```log
uwu> yolo compare-room-state !ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk wombatx.me nexy7574.co.uk uwu.zirco.dev zirco.dev matrix.org starstruck.systems feline.support unredacted.org
Room State Comparison for !ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk vs wombatx.me
at_event (sent to remote): $-ErrVEjC_pxCOHKt5w2nZz479Jj5CH3-LvSyiFChJOI
local tip: $-ErrVEjC_pxCOHKt5w2nZz479Jj5CH3-LvSyiFChJOI
Missing locally: 1
Extra locally: 1
Skipped (bad sig): 0

Room SSH:        38303242
Extremities:     1
Local joined:    state=18, cache=18 ✓
Local invited:   state=1
Remote joined:   18
Remote invited:  1
NOTE: Tip is a state event — injected into remote state for state-after comparison

Missing IDs: [
  $Xk604YNwDRXNYzv4AHSDrPuvp13BqEJcV8IIJ52eZ3M (m.room.member @caufa:muoi.me) 2026-05-27 07:02:25 UTC [leave]
]
Extra IDs: [
  $5DmDHnaUCObGInq5BR6kx6fxL7h6fOe7QQBrH24qwIM (m.room.member @caufa:muoi.me) 2026-05-27 17:51:52 UTC [leave]
]

--- vs nexy7574.co.uk: ERROR: Remote server nexy7574.co.uk responded with: [404 / M_NOT_FOUND] NotFound: Event not found.
--- wombatx.me vs uwu.zirco.dev:
Only on wombatx.me: 1  Only on uwu.zirco.dev: 1
uwu.zirco.dev joined: 19, invited: 1
IDs only on wombatx.me: [
  $Xk604YNwDRXNYzv4AHSDrPuvp13BqEJcV8IIJ52eZ3M (m.room.member @caufa:muoi.me) 2026-05-27 07:02:25 UTC [leave]
]
IDs only on uwu.zirco.dev: [
  $5DmDHnaUCObGInq5BR6kx6fxL7h6fOe7QQBrH24qwIM (m.room.member @caufa:muoi.me) 2026-05-27 17:51:52 UTC [leave]
]
--- wombatx.me vs zirco.dev:
Only on wombatx.me: 1  Only on zirco.dev: 4
zirco.dev joined: 22, invited: 1
IDs only on wombatx.me: [
  $Xk604YNwDRXNYzv4AHSDrPuvp13BqEJcV8IIJ52eZ3M (m.room.member @caufa:muoi.me) 2026-05-27 07:02:25 UTC [leave]
]
IDs only on zirco.dev: [
  $5DmDHnaUCObGInq5BR6kx6fxL7h6fOe7QQBrH24qwIM (m.room.member @caufa:muoi.me) 2026-05-27 17:51:52 UTC [leave]
  $Dgsv-oP7vncQbFB6JSc0sP-O1euEAAW-BTurVdyD8jA (m.room.member @lveneris:kludgecs.com) 2026-05-27 19:59:02 UTC [join]
  $ryFdP1Gm9O40O9SohLKZN0MBh-4RFYMe72SVIc08fxQ (m.room.member @kim:sosnowkadub.de) 2026-05-28 05:46:40 UTC [join]
  $umjE2hcCdUO6qAWjNg66p5EOubhtRHHsPAjsJ3w85P8 (m.room.member @tobiasfella:kde.org) 2026-05-28 09:02:31 UTC [join]
]

--- vs matrix.org: ERROR: Remote server matrix.org responded with: [404 / M_NOT_FOUND] Could not find event

--- vs starstruck.systems: ERROR: Remote server starstruck.systems responded with: [404 / M_NOT_FOUND] Could not find event $-ErrVEjC_pxCOHKt5w2nZz479Jj5CH3-LvSyiFChJOI

--- vs feline.support: ERROR: Remote server feline.support responded with: [404 / M_NOT_FOUND] Could not find event $-ErrVEjC_pxCOHKt5w2nZz479Jj5CH3-LvSyiFChJOI

--- vs unredacted.org: ERROR: Remote server unredacted.org responded with: [403 / M_FORBIDDEN] Host not in room.

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# another example...
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

uwu> debug verify-pdu $OesjhwrT56xTNofVAV4i8JWngqR5ykT5UzBZxl66zGw
| level | span | message |
| ------: | :-----: | :------- |
|  WARN |   command    | Signature verification failed for event $OesjhwrT56xTNofVAV4i8JWngqR5ykT5UzBZxl66zGw. Error: Verification(Signature(signature::Error { source: Some(Verification equation was not satisfied) })). Available keys: {"uwu.zirco.dev": {"ed25519:E01XVFwT": "8hvlcSTvSsJ90AOAHDuXRNt7kwi3oCoGQUocs2wvohA"}}. Event signatures: {"uwu.zirco.dev":{"ed25519:E01XVFwT":"CnJxZ2vlUEHjDnFBZKN8tKgXE0rcASG2VwP6bHD6du51QOYLb1H54iS7PbXXVoVsU7ycYQr3WSsb8kKsb+FxCg"}} |
|  WARN |   command    | cannot invite a user who is banned or already joined |

Event: $OesjhwrT56xTNofVAV4i8JWngqR5ykT5UzBZxl66zGw
Room: !ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk
Type: m.room.member
Membership: invite
State key: @logn:zirco.dev
Sender: @logn:uwu.zirco.dev
Room Version: 12
Verify: SIGNATURE FAILED: Verification error: Could not verify signature: signature error: Verification equation was not satisfied
Auth check: FAIL (not authorized)
Status: timeline=true outlier=true rejected=false soft_failed=false
```

## Misc PDUs

```log
uwu> debug get-pdu $OesjhwrT56xTNofVAV4i8JWngqR5ykT5UzBZxl66zGw
Status: STUCK STATE (Both Timeline and Outlier tables)

{
  "auth_events": [
    "$qTzF9lmqmLf1Z8lLma0eJaZ65hhiF4br3B9KCKf5vx8",
    "$C2M-j-XNs-9gseH6bxCVtYxVT_ntFcF3vfsX3ctVsvM",
    "$54aywZw-1VkMtGjipmJkVCLmLzpzv9rTPHsIEXovI2I",
    "$yOdx2qkGaTX40MraQFvhux9kB7WrP_Euw5NSWeAshFE"
  ],
  "content": {
    "avatar_url": "mxc://zirco.dev/pNBPDNiHJzKmRrMRXZrIVUXZ",
    "is_direct": false,
    "membership": "invite"
  },
  "depth": 3119,
  "event_id": "$OesjhwrT56xTNofVAV4i8JWngqR5ykT5UzBZxl66zGw",
  "hashes": {
    "sha256": "Da6Hi8DqZI62uFOMvffUMeKA6hEyo32eEZEOmCn05kI"
  },
  "origin_server_ts": 1780087000209,
  "prev_events": [
    "$4d9u1P1nzsOSCds8j72VaHpbTVa57rGy_5DKV43djoQ"
  ],
  "room_id": "!ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk",
  "sender": "@logn:uwu.zirco.dev",
  "signatures": {
    "uwu.zirco.dev": {
      "ed25519:E01XVFwT": "CnJxZ2vlUEHjDnFBZKN8tKgXE0rcASG2VwP6bHD6du51QOYLb1H54iS7PbXXVoVsU7ycYQr3WSsb8kKsb+FxCg"
    },
    "zirco.dev": {
      "ed25519:a_mkrB": "yuwWGyW/3Z3N/0Wfrua3ehaKhoxSH+Paq/zqIM90sTGEFZvmwL0YXRKWH26RwlrILKntJjOTR7sqW+bkf1Z4Aw"
    }
  },
  "state_key": "@logn:zirco.dev",
  "type": "m.room.member",
  "unsigned": {
    "prev_content": {
      "avatar_url": "mxc://zirco.dev/pNBPDNiHJzKmRrMRXZrIVUXZ",
      "displayname": "LogN",
      "membership": "knock",
      "reason": "a"
    },
    "prev_sender": "@logn:zirco.dev",
    "replaces_state": "$54aywZw-1VkMtGjipmJkVCLmLzpzv9rTPHsIEXovI2I"
  }
}
```

## Independently resolve state of my graph

```shell
shane@coffeelake:/run/media/shane/shane4tb-ent/dags$ make PREFIX=local-dag ROOM=ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk-v12
Merging: local-dag-ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk-v12-nutra.tk-d1-3236.jsonl local-dag-ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk-v12-nutra.tk-d1-3509.jsonl
ruma-lean -i local-dag-ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk-v12-nutra.tk-d1-3236.jsonl -i local-dag-ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk-v12-nutra.tk-d1-3509.jsonl --state-res v2-1 -f default
[merge] local-dag-ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk-v12-nutra.tk-d1-3236.jsonl: 3360 events (3360 new, 0 shared)
[merge] local-dag-ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk-v12-nutra.tk-d1-3509.jsonl: 3779 events (419 new, 3360 shared)
[merge] merge-base: 3360 shared events across 2 inputs
[merge] total: 3779 unique events
{
  "auth_chain_size": 72,
  "duration_ms": 0,
  "resolved_state_size": 44,
  "state_event_ids": [
    "$niAckwtntGI1DrfHyWVUW3zkwvZcQmo_UwpPUCe_Lx0",
    "$ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk",
    "$TJkwY8Z9AuNf7aGW3_7iNd1KkccDeU-4nCKZ2S7gu5Y",
    "$qhKQhKpR10UkfCpLPtTBRgB0XsHuNH7H7Y6JJ6pt9EA",
    "$C2M-j-XNs-9gseH6bxCVtYxVT_ntFcF3vfsX3ctVsvM",
    "$7rBzVS2lkPBTKYvb7U0aW7mTDPKwJ6FyGBk4DI69-ZY",
    "$1xKH_lH6EsFdJyhuERkiLP43bK2ScC7kHb1gRQdoTmA",
    "$q0Pngvkj_pKkJenWeXlV6ioUoXuSAVLQXFZICu-sd0Y",
    "$MhHtA_I3XQn_aVHjbwusQEmw5lqE3rXhT3bTO1XUmqY",
    "$5DmDHnaUCObGInq5BR6kx6fxL7h6fOe7QQBrH24qwIM",
    "$lQzDw9VjXPC8tNaOWt4GQv_zDa1CKGnclJGpmCT4XDc",
    "$_j9ncnbfXXRUQ1Y-cPJU1O4tO7s7SW5v9O6D-7wwY4U",
    "$gtz9BB6Aip9uYtPN03D69RpCBB8rmwpLsIghrO0RRBY",
    "$xd0g8DWGSejDGYBAMImWFC5_byW-YNpUw4kCr2YtAYk",
    "$-ErrVEjC_pxCOHKt5w2nZz479Jj5CH3-LvSyiFChJOI",
    "$h1mlcMwKDS-cycwJr1SRfxWO_6n5cuzhEbuJfd6yf8w",
    "$ryFdP1Gm9O40O9SohLKZN0MBh-4RFYMe72SVIc08fxQ",
    "$76TMM42K6fgAy5Sr_sf5YzW6VvYlTAjSK0FTQFEv9so",
    "$yOdx2qkGaTX40MraQFvhux9kB7WrP_Euw5NSWeAshFE",
    "$awpfvCpzzuYIN4gdf5HU82Pzh8gO3_5qFxkAuaiDHjY",
    "$Dgsv-oP7vncQbFB6JSc0sP-O1euEAAW-BTurVdyD8jA",
    "$Ua3c_wpE4686VvBnWv9lLpmc0G1TbAk3x-myINQ2Qj4",
    "$DpfofWp18SBC6WXFFUHJub3dSrM_k9uWvBdwyPcdK4c",
    "$kcvvMbqajfxqkFbRxJDQMSu_JGzkp8rPRGRY8vvdCtE",
    "$WzWLWCoswAzhiMQV7Kj5AxW5BdrJTD9QPvnf-9ef4cA",
    "$bX3weYwBAy5QZMbktsczHPO1p0SgkFhHxYf2lk1WOeg",
    "$cM8Sx1DUu-Ioz6gZ56bBMAI7xAwGGMIdLcVreHxBHMs",
    "$odqBbCBa7J371wN6iA-_ITV5isWNF6UMXnXkYqS5arI",
    "$6PcQA6QgpEmbtoz8Uw7xwqXfMleTwM5aBCOIuJ48UQM",
    "$uoKHyZAjXR-K03aujkNUpn7x_OIibIsSiXO4ibgYtz4",
    "$TN_r2amxtcHiHRjPPsVIrDOXhyRFJFy-0QGVT85rzss",
    "$TXkydSgsknKY0e9eFO0SyuDHOkCkEKvx2Oyk7Vkk4RM",
    "$tgSLBGShVfNEpjNxTo1hMPiv1T-RtDz42MO9Zal9TY4",
    "$xgt3Xl05iZrTkPx6VsfCKFAcRtqm9ZEzmYyK__QoOZI",
    "$Aq_DgSCw0Q4bvsslozzkRHQ12AWRB68BAWkj39wf5TQ",
    "$umjE2hcCdUO6qAWjNg66p5EOubhtRHHsPAjsJ3w85P8",
    "$tcDr_86qkXwG4ECZq-oyWYrKSYqlSj6ALNeffiTAh1k",
    "$Xt3LozdiPyiy6stqT2y7RlvQP3JUt4G0Lp4MQse8ZYw",
    "$_Q3S8YFD90o8QNCYH0jqKHEI3R4ZGUQtBvXmleEayts",
    "$HK8IK22MxZT42iHvwVjGDae0G2L_ugUJYLbFaE7h3LI",
    "$Irpf2AAiTf8HpukkYlSo30owzLM4RrZ2576rt9dddn4",
    "$LRP_8cG0-Lmu4rxbl0uNFnPCNX4ZRsUdpagUCSdx-yw",
    "$gVJ1GyA8UP-314t4_NHMpTyXZdrbyLfaRkZKPh7niZk",
    "$4hpB0CEHXz_8P2_qnFeTNU5efOfOg-xZQfWryyNfCQ4"
  ],
  "status": "success",
  "version": "V2_1"
}
```

## Servers in the room (21)

```shell
shane@coffeelake:/run/media/shane/shane4tb-ent/dags$ make servers PREFIX=local-dag ROOM=ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk-v12
zirco.dev
wombatx.me
muoi.me
starstruck.systems
nhjkl.com
feline.support
nexy7574.co.uk
matrix.org
hnvn.de
unredacted.org
federated.nexus
dapperepoging.nl
mangotcf.ru
codestorm.net
nutra.tk
uwu.zirco.dev
explode.org
kludgecs.com
sosnowkadub.de
kde.org
maunium.net
```

## Timeline queries

```log
uwu> yolo get-remote-dag --limit -1 --from  $-ErrVEjC_pxCOHKt5w2nZz479Jj5CH3-LvSyiFChJOI !ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk zirco.dev
| level | span | message |
| ------: | :-----: | :------- |
|  INFO |   command    | get-remote-dag: starting crawl from zirco.dev for !ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk (limit: -1) |
|  INFO |   command    | get-remote-dag: complete — 230 PDUs from zirco.dev in 1.818984706s (9 batches, bf=1.539, depth=3319..3509) |

Fetching DAG from zirco.dev for !ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk (limit: -1)...

Successfully fetched 230 PDUs from zirco.dev to /tmp/remote-dag-ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk-v12-zirco.dev-d3319-3509.jsonl (depth: 3319..3509, branching factor: 1.539)
```

# Dev Branch Contributions to Main

**Branch:** `guru/dev-2026-03-27+b1-presence+b2-federation`
**Merge base:** `8719ff83` (late March 2026)
**Branch-only commits:** ~190 (excluding merges from main)
**Branch-only files touched:** 109 Rust files

> [!NOTE]
> All line counts below reflect **only branch-specific commits** (non-merge). Upstream changes from main that entered via merge commits are excluded.

---

## [IN-SCOPE] In-Scope: Presence Performance

| File                                                     | +/-           | What it does                                                                                                         |
| -------------------------------------------------------- | ------------- | -------------------------------------------------------------------------------------------------------------------- |
| [presence/mod.rs](src/service/presence/mod.rs)           | +466/-268     | Moka cache for presence, debounced updates (1s), task yielding to prevent CPU starvation, conditional status updates |
| [presence/data.rs](src/service/presence/data.rs)         | +256/-275     | Presence data layer refactoring for cache integration                                                                |
| [presence/presence.rs](src/service/presence/presence.rs) | +4/-4         | Minor adjustments                                                                                                    |
| **Subtotal**                                             | **+726/-547** | **net +179**                                                                                                         |

---

## [IN-SCOPE] In-Scope: Federation Send Performance

| File                                               | +/-           | What it does                                                            |
| -------------------------------------------------- | ------------- | ----------------------------------------------------------------------- |
| [sending/sender.rs](src/service/sending/sender.rs) | +314/-161     | Per-server EDU batching (not per-room), queue ordering fix, task yields |
| [sending/mod.rs](src/service/sending/mod.rs)       | +48/-15       | Sending service interface additions                                     |
| [sending/stats.rs](src/service/sending/stats.rs)   | +2/-1         | Stats tweak                                                             |
| **Subtotal**                                       | **+364/-177** | **net +187**                                                            |

---

## [IN-SCOPE] In-Scope: Federation Receive / Backfill

| File                                                                                                       | +/-             | What it does                                                   |
| ---------------------------------------------------------------------------------------------------------- | --------------- | -------------------------------------------------------------- |
| [event_handler/fetch_and_handle_outliers.rs](src/service/rooms/event_handler/fetch_and_handle_outliers.rs) | +261/-158       | Parallelize event fetching, short-circuit soft-failed outliers |
| [event_handler/fetch_prev.rs](src/service/rooms/event_handler/fetch_prev.rs)                               | +150/-112       | Budget gating for prev_event fetching                          |
| [event_handler/upgrade_outlier_pdu.rs](src/service/rooms/event_handler/upgrade_outlier_pdu.rs)             | +244/-244       | Refactored outlier upgrade path                                |
| [event_handler/handle_incoming_pdu.rs](src/service/rooms/event_handler/handle_incoming_pdu.rs)             | +83/-78         | Incoming PDU handling improvements                             |
| [event_handler/handle_outlier_pdu.rs](src/service/rooms/event_handler/handle_outlier_pdu.rs)               | +49/-59         | Outlier handling refinements                                   |
| [event_handler/handle_prev_pdu.rs](src/service/rooms/event_handler/handle_prev_pdu.rs)                     | +25/-25         | Prev PDU handling                                              |
| [event_handler/parse_incoming_pdu.rs](src/service/rooms/event_handler/parse_incoming_pdu.rs)               | +32/-33         | Parsing changes                                                |
| [event_handler/state_at_incoming.rs](src/service/rooms/event_handler/state_at_incoming.rs)                 | +5/-3           | Minor                                                          |
| [event_handler/mod.rs](src/service/rooms/event_handler/mod.rs)                                             | +2/-5           | Module adjustments                                             |
| [timeline/backfill.rs](src/service/rooms/timeline/backfill.rs)                                             | +84/-74         | Backfill ordering (oldest-first), logging                      |
| [server/send.rs](src/api/server/send.rs)                                                                   | +101/-37        | Incoming transaction processing, soft-fail handling            |
| [server/send_join.rs](src/api/server/send_join.rs)                                                         | +13/-0          | Send join improvements                                         |
| [federation/execute.rs](src/service/federation/execute.rs)                                                 | +2/-2           | Minor                                                          |
| **Subtotal**                                                                                               | **+1,051/-830** | **net +221**                                                   |

---

## [IN-SCOPE] Related: Monitor / Forwardfill

| File                                                       | +/-             | What it does                                                                                                                                                                                              |
| ---------------------------------------------------------- | --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [rooms/monitor.rs](src/service/rooms/monitor.rs)           | +906/-564       | **NEW** — Background forwardfill daemon. On startup scans all federated rooms idle >5min, then hourly for rooms idle >4h. Probes remote servers via `make_join` for DAG tips, fetches missing extremities |
| [timeline/bumper.rs](src/service/rooms/timeline/bumper.rs) | +99/-99         | **REVERTED** — Dummy event creator (added then fully reverted, net zero)                                                                                                                                  |
| **Subtotal**                                               | **+1,005/-663** | **net +342**                                                                                                                                                                                              |

---

## [OUT-OF-SCOPE] Out-of-Scope: Admin / Debug Commands

| File                                                             | +/-             | What it does                                                                                                                 |
| ---------------------------------------------------------------- | --------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| [admin/debug/commands.rs](src/admin/debug/commands.rs)           | +1,513/-521     | `compare-remote-state`, `rescue-outliers`, `reorder-timeline`, `reindex-timeline`, `at_event` filter, state comparison tools |
| [admin/debug/mod.rs](src/admin/debug/mod.rs)                     | +176/-15        | Command registrations for above                                                                                              |
| [admin/room/commands.rs](src/admin/room/commands.rs)             | +154/-80        | Room admin commands                                                                                                          |
| [admin/server/commands.rs](src/admin/server/commands.rs)         | +105/-0         | New server admin commands                                                                                                    |
| [admin/room/mod.rs](src/admin/room/mod.rs)                       | +20/-10         | Module registrations                                                                                                         |
| [admin/federation/commands.rs](src/admin/federation/commands.rs) | +26/-5          | Federation debug commands                                                                                                    |
| [admin/server/mod.rs](src/admin/server/mod.rs)                   | +3/-0           | Module entry                                                                                                                 |
| **Subtotal**                                                     | **+1,997/-631** | **net +1,366**                                                                                                               |

---

## [OUT-OF-SCOPE] Out-of-Scope: Sync Fixes

| File                                                  | +/-           | What it does                                                            |
| ----------------------------------------------------- | ------------- | ----------------------------------------------------------------------- |
| [sync/v3/joined.rs](src/api/client/sync/v3/joined.rs) | +484/-415     | Federated lazy loading fix, PDU vanishing race, timeline flickering fix |
| [sync/v3/state.rs](src/api/client/sync/v3/state.rs)   | +116/-30      | State sync improvements                                                 |
| [sync/v3/mod.rs](src/api/client/sync/v3/mod.rs)       | +78/-71       | Sync module refactoring                                                 |
| [sync/v3/left.rs](src/api/client/sync/v3/left.rs)     | +29/-2        | Left room sync fix                                                      |
| [sync/mod.rs](src/service/sync/mod.rs)                | +168/-5       | Sync service — notification watcher, global counter consolidation       |
| [sync/v5.rs](src/api/client/sync/v5.rs)               | +1/-1         | Trivial                                                                 |
| **Subtotal**                                          | **+876/-524** | **net +352**                                                            |

---

## [OUT-OF-SCOPE] Out-of-Scope: Timeline

| File                                                       | +/-           | What it does                                                 |
| ---------------------------------------------------------- | ------------- | ------------------------------------------------------------ |
| [timeline/data.rs](src/service/rooms/timeline/data.rs)     | +183/-103     | pdus_rev fix, cross-room boundary checks, room_id validation |
| [timeline/mod.rs](src/service/rooms/timeline/mod.rs)       | +139/-22      | `reorder_timeline` (topological sort), `reindex_timeline`    |
| [timeline/append.rs](src/service/rooms/timeline/append.rs) | +75/-59       | Append improvements                                          |
| [timeline/build.rs](src/service/rooms/timeline/build.rs)   | +9/-7         | Build adjustments                                            |
| **Subtotal**                                               | **+406/-191** | **net +215**                                                 |

---

## [OUT-OF-SCOPE] Out-of-Scope: Outlier Rescue System

| File                                                     | +/-           | What it does                                                                    |
| -------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------- |
| [rooms/outlier/mod.rs](src/service/rooms/outlier/mod.rs) | +271/-139     | Outlier rescue — upgrades outlier PDUs to timeline events, un-soft-fails events |
| **Subtotal**                                             | **+271/-139** | **net +132**                                                                    |

---

## [OUT-OF-SCOPE] Out-of-Scope: Spaces

| File                                       | +/-           | What it does                                |
| ------------------------------------------ | ------------- | ------------------------------------------- |
| [client/space.rs](src/api/client/space.rs) | +376/-349     | Space hierarchy pagination, traversal fixes |
| [server/utils.rs](src/api/server/utils.rs) | +47/-43       | Server-side space utilities                 |
| **Subtotal**                               | **+423/-392** | **net +31**                                 |

---

## [OUT-OF-SCOPE] Out-of-Scope: Other Significant Changes

| File                                                                           | +/-       | What it does                                                |
| ------------------------------------------------------------------------------ | --------- | ----------------------------------------------------------- |
| [users/mod.rs](src/service/users/mod.rs)                                       | +343/-100 | User service additions (profile, presence-related caching)  |
| [main/attach.rs](src/main/attach.rs)                                           | +246/-208 | CLI attach — proper wait/interruption for long-running jobs |
| [state_accessor/server_can.rs](src/service/rooms/state_accessor/server_can.rs) | +212/-153 | Server permission checks                                    |
| [globals/mod.rs](src/service/globals/mod.rs)                                   | +171/-11  | Global service additions                                    |
| [membership/join.rs](src/api/client/membership/join.rs)                        | +167/-78  | Join improvements, `is_direct` fix                          |
| [rooms/state/mod.rs](src/service/rooms/state/mod.rs)                           | +166/-61  | Room state service changes                                  |
| [rooms/state_cache/mod.rs](src/service/rooms/state_cache/mod.rs)               | +145/-26  | State cache additions (`active_local_users_in_room`, etc.)  |
| [resolver/dns.rs](src/service/resolver/dns.rs)                                 | +101/-7   | DNS resolver enhancements                                   |
| [state_cache/update.rs](src/service/rooms/state_cache/update.rs)               | +92/-54   | State cache update logic                                    |
| [router/run.rs](src/router/run.rs)                                             | +82/-80   | Router startup/shutdown                                     |
| [client/keys.rs](src/api/client/keys.rs)                                       | +64/-22   | E2EE cross-signing fault tolerance                          |
| [rooms/short/mod.rs](src/service/rooms/short/mod.rs)                           | +59/-22   | Short ID batching                                           |
| [state_res/mod.rs](src/core/matrix/state_res/mod.rs)                           | +65/-35   | State resolution tweaks                                     |
| [state_res/event_auth.rs](src/core/matrix/state_res/event_auth.rs)             | +42/-16   | Auth chain visibility fixes                                 |
| [config/mod.rs](src/core/config/mod.rs)                                        | +47/-12   | Config additions                                            |
| [core/metrics/mod.rs](src/core/metrics/mod.rs)                                 | +40/-1    | Metrics additions                                           |
| [message.rs](src/api/client/message.rs)                                        | +35/-33   | Message filtering (dummy_event, ignored users)              |
| [membership/members.rs](src/api/client/membership/members.rs)                  | +34/-27   | Members endpoint — `at=` speedup                            |
| [router/request.rs](src/router/request.rs)                                     | +32/-2    | Request logging                                             |
| [read_receipt/data.rs](src/service/rooms/read_receipt/data.rs)                 | +44/-28   | Read receipt improvements                                   |

---

## Summary

```
IN-SCOPE (presence + federation perf):
  36 files, +3,146/-2,217 (net +929)

OUT-OF-SCOPE:
  73 files, +7,539/-5,374 (net +2,165)

  Largest out-of-scope areas:
    Admin/debug commands:  net +1,366 lines
    Sync fixes:            net +352 lines
    Timeline:              net +215 lines
    Outlier rescue:        net +132 lines
    CLI attach:            net +38 lines
    Spaces:                net +31 lines
```

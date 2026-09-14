# Server Reliability Adjacency Matrix

Design proposal for a persistent homeserver reliability and interconnectedness
cache. The goal is to make intelligent fan-out decisions during federation
requests (hierarchy walks, forwardfill, state fetches) by tracking which
servers are reachable, fast, and authoritative.

Created May 2026.

## Problem

Today, federation fan-out is naive:

- `m.space.child` `via` lists are static and may contain dead servers
- Forwardfill picks target servers without knowing which are responsive
- `force-set-state` tries servers in arbitrary order
- Hierarchy walking fans out to up to 3 `via` servers with no preference

This causes:

1. **Wasted time** waiting for timeouts from dead servers
2. **Incomplete data** when all `via` servers for a room are unreachable but
   other servers in the room could have answered
3. **No learning** — the same dead server gets retried every sweep cycle

## Proposed Solution: Adjacency Matrix DB Table

### Schema

```
Table: server_reliability
┌──────────────────┬──────────────────┬─────────┬────────────┬──────────────┐
│ origin_server    │ target_server    │ score   │ last_ok_ts │ last_fail_ts │
├──────────────────┼──────────────────┼─────────┼────────────┼──────────────┤
│ wombatx.me       │ matrix.org       │ 0.95    │ 1715550000 │ 1715540000   │
│ wombatx.me       │ disko.media      │ 0.00    │ 0          │ 1715550000   │
│ wombatx.me       │ codestorm.net    │ 0.87    │ 1715549000 │ 1715530000   │
└──────────────────┴──────────────────┴─────────┴────────────┴──────────────┘
```

- **origin_server**: always the local server (but schema supports multi-server
  for future clustering)
- **target_server**: the remote homeserver
- **score**: rolling reliability score (0.0 = always fails, 1.0 = always
  succeeds), computed as exponential moving average
- **last_ok_ts / last_fail_ts**: timestamps of last success/failure for
  staleness detection

### Score Computation

```
new_score = (alpha * outcome) + ((1 - alpha) * old_score)
```

Where `outcome` is 1.0 for success, 0.0 for failure, and `alpha` controls
recency weight (suggest `alpha = 0.3`).

### Interconnectedness Index

For rooms with many participating servers, the adjacency matrix enables
selecting the **best-connected** server — one that is likely to have complete
state because it successfully communicates with the most other servers in the
room.

```
interconnectedness(server, room) = Σ reliability(local, peer)
                                   for peer in room_servers
                                   where peer ≠ server
```

This score can be used to prefer well-connected servers for:

- `force-set-state` source selection
- Forwardfill target selection
- Hierarchy federation fallback ordering

## Integration Points

### 1. Sending Service (`src/service/sending/`)

After every federation request completes (success or failure), update the
reliability score:

```rust
// On success:
self.services.server_reliability.record_success(target_server).await;

// On failure:
self.services.server_reliability.record_failure(target_server).await;
```

### 2. Forwardfill (`src/service/rooms/monitor.rs`)

Replace static server selection with reliability-sorted ordering:

```rust
let candidates = self.services.server_reliability
    .best_servers_for_room(room_id, 5)
    .await;
```

### 3. Space Hierarchy (`src/service/rooms/spaces/mod.rs`)

When building the `via` fan-out list, prefer reliable servers:

```rust
let sorted_via = self.services.server_reliability
    .sort_by_reliability(via)
    .await;
// Then take top 3
```

### 4. Force-Set-State (`src/admin/debug/commands.rs`)

When the user doesn't specify a source server, auto-select the most reliable
and well-connected server in the room.

### 5. Admin Diagnostics

New admin commands:

- `debug server-reliability <server>` — show score and history
- `debug server-reliability-matrix` — dump full adjacency matrix
- `debug server-interconnectedness <room_id>` — rank servers by
  interconnectedness for a specific room

## Backoff & Decay

- Servers with score < 0.1 should be backed off (skip for 1 hour)
- Servers with score < 0.01 backed off for 24 hours
- Scores decay toward 0.5 over time if no requests are made (prevents
  permanent blacklisting of temporarily-down servers)
- `last_ok_ts` older than 7 days resets score to 0.5 (stale data)

## Database Implementation

Use a new RocksDB column family `server_reliability` keyed by
`(origin_server, target_server)`. Value is a packed struct:

```rust
#[derive(Serialize, Deserialize)]
struct ServerReliability {
    score: f32,
    last_ok_ts: u64,
    last_fail_ts: u64,
    success_count: u32,
    failure_count: u32,
}
```

## Migration Path

1. Add the DB table and `ServerReliability` service
2. Wire up `record_success`/`record_failure` in the sending service
3. Add `sort_by_reliability` helper and use in forwardfill
4. Extend to hierarchy and force-set-state
5. Add admin diagnostic commands

## Open Questions

- Should we track per-endpoint reliability (e.g. `/state` vs `/event` vs
  `/hierarchy`)? Or is a single score per server sufficient?
- Should the interconnectedness index weight by room membership count?
- How should we handle servers behind Cloudflare/CDN that return 200 but with
  error bodies?
- Should we share reliability data with other continuwuity instances via a
  custom federation extension?

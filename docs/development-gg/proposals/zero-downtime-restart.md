# Zero-Downtime Restart & EDU Recovery

## Problem

During binary restarts, the server is unreachable for 1-5 seconds. Inbound federation transactions containing ephemeral EDUs (read receipts, typing notifications, presence) are lost. Remote servers may retry the transaction, but:

- Most senders back off after 1-2 failures
- EDUs are bundled into transactions — a failed transaction drops ALL contained EDUs
- The Matrix spec defines no mechanism to request missed ephemeral data

Read receipts are the most visible casualty: after a restart, unread counts on remote users' messages appear stale until those users read something new.

---

## Proposal 2: Socket Activation (Zero-Downtime Restart)

### Concept

Use systemd socket activation to keep the TCP listener open during the binary swap. The kernel buffers incoming connections in the socket's backlog queue while the old process exits and the new one starts.

### How It Works

```
1. systemd owns the listening socket (e.g., 0.0.0.0:6167)
2. Old binary receives SIGTERM, begins graceful shutdown
3. Incoming TCP connections queue in kernel backlog (default 128)
4. New binary starts, inherits the socket fd from systemd
5. New binary accepts queued connections, processes buffered requests
6. Zero dropped connections if restart < backlog drain time
```

### Implementation

#### systemd socket unit (`conduwuit.socket`)

```ini
[Unit]
Description=Conduwuit Matrix Server Socket

[Socket]
ListenStream=0.0.0.0:6167
# Increase backlog for busy servers
Backlog=512
# Keep socket alive during restart
FreeBind=true

[Install]
WantedBy=sockets.target
```

#### systemd service unit changes (`conduwuit.service`)

```ini
[Unit]
Description=Conduwuit Matrix Server
Requires=conduwuit.socket
After=conduwuit.socket

[Service]
# Accept socket from systemd
Type=notify
ExecStart=/usr/local/bin/conduwuit
Restart=on-failure
# Give time for graceful shutdown
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

#### Rust code changes

Conduwuit needs to detect and accept a systemd-passed file descriptor instead of binding its own socket:

```rust
// In the server startup code (src/router/mod.rs or similar)
use std::os::unix::io::FromRawFd;

fn get_listener() -> TcpListener {
    // Check for systemd socket activation (LISTEN_FDS env var)
    if let Ok(fds) = std::env::var("LISTEN_FDS") {
        if fds.parse::<i32>().unwrap_or(0) > 0 {
            // fd 3 is the first passed fd by convention
            return unsafe { TcpListener::from_raw_fd(3) };
        }
    }
    // Fallback: bind normally when no socket activation
    TcpListener::bind(&config.address).unwrap()
}
```

The `listenfd` or `sd-notify` crate handles this more robustly.

#### Restart command

```bash
# Instead of: systemctl restart conduwuit
# Use:
systemctl restart conduwuit.service
# Socket stays open, connections buffer during restart
```

### Complexity

- **Low-Medium**: ~50 lines of Rust, 2 systemd unit files
- **Risk**: Low — falls back to normal binding if no socket passed
- **Limitation**: Only buffers TCP connections. If restart takes >10s, kernel drops connections. Backlog=512 handles ~500 concurrent inbound federation requests.

---

## Proposal 3: Receipt Reconciliation on Startup

### Concept

After the forwardfill scan completes, iterate rooms with recent activity and request the latest receipt state from a joined remote server. This fills in any read receipts missed during downtime.

### How It Works

```
1. Startup completes, forwardfill scan finishes
2. For each room with activity in the last N hours:
   a. Pick a remote server that is joined to the room
   b. GET /_matrix/federation/v1/state/{roomId} (or a lighter endpoint)
   c. Extract m.receipt EDUs from the response
   d. Merge into local receipt store (latest receipt per user wins)
3. Sync delivers updated receipts to local clients
```

### Design Considerations

#### Endpoint choice

The Matrix federation spec doesn't have a dedicated "get current receipts" endpoint. Options:

1. **`/state/{roomId}`** — Returns full room state. Receipts are NOT in room state (they're ephemeral). **Won't work.**

2. **`/event/{eventId}`** — Returns a single event. Not useful for receipts.

3. **Custom extension** — Implement a vendor-specific endpoint like `/_matrix/federation/unstable/org.conduwuit/receipts/{roomId}` that returns the latest receipt per user. Requires both servers to support it. **Only works between conduwuit instances.**

4. **Piggyback on `/send`** — When receiving the next transaction from a remote server, request it to include a full receipt snapshot. Non-standard but transparent to other implementations.

5. **Client-side reconciliation** — After restart, local clients re-send their own read receipts via `/receipt`. This only fixes the LOCAL user's receipts, not remote users'. But it's the most impactful fix since the local user's unread counts are what matter most.

#### Practical approach: local client nudge

The simplest high-impact fix doesn't require federation changes:

```rust
// After startup, for each local user:
// 1. Find their latest receipt in each room (from DB)
// 2. Find the latest event they've seen (from sync)
// 3. If latest_event > latest_receipt, synthesize a receipt update
//    to trigger the client to re-send its read position
```

This doesn't recover REMOTE users' receipts, but it fixes the local user's experience immediately.

#### Full federation approach

For a complete solution, implement a post-startup task:

```rust
async fn reconcile_receipts(&self) {
    let cutoff = utils::millis_since_unix_epoch() - (6 * 60 * 60 * 1000); // 6 hours

    for room_id in self.rooms.active_rooms_since(cutoff) {
        // Get latest event_id we have
        let latest = self.timeline.latest_pdu_id(&room_id)?;

        // Ask a remote server for receipts via /send exchange
        // (piggyback on next outbound transaction)
        self.sending.request_receipt_sync(&room_id, latest).await;
    }
}
```

### Complexity

- **Client nudge**: Low (~30 lines), high impact for local users
- **Federation extension**: Medium-High (~200 lines + protocol design), requires bilateral support
- **Piggyback approach**: Medium (~100 lines), transparent to remote servers

---

## Recommendation

**Phase 1**: Implement socket activation (Proposal 2). Eliminates EDU loss during planned restarts entirely. Low risk, low effort, high reward.

**Phase 2**: Implement client-side receipt nudge (Proposal 3, simplified). After startup, trigger local clients to re-send their read positions. Fixes the most visible symptom (local user's stale unread counts) without federation changes.

**Phase 3** (future): Design a federation receipt reconciliation protocol if cross-server receipt accuracy becomes critical.

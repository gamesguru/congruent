# Federation E2EE Reliability

## Problem Statement

Continuwuity-to-continuwuity encrypted message delivery is less reliable than
Synapse-to-continuwuity. Messages sent from continuwuity homeservers
(e.g. `nutra.tk`, `mdev.nutra.tk`) to another continuwuity instance
(e.g. `wombatx.me`) frequently result in "Unable to decrypt" errors, while
messages from Synapse servers (e.g. `matrix.org`, `unredacted.org`) to the
same continuwuity instance decrypt fine.

## Root Causes

### 1. To-Device EDU Delivery (Key Shares)

Megolm session keys are shared via **to-device messages**, which are
federation EDUs. The E2EE pipeline is:

1. Client encrypts message with Megolm session key
2. Client sends Megolm session key to each recipient device via
   `PUT /_matrix/client/v3/sendToDevice/m.room_key/{txnId}`
3. Server packages this as an `m.direct_to_device` EDU
4. Server delivers EDU to recipient's homeserver in a federation transaction

**Failure mode:** If the recipient server is unreachable (even briefly), the
EDU enters the federation sender's exponential backoff queue. The key share
may never be delivered if:

- The backoff outlasts the sender's queue retention
- The server restarts before delivery (see netburst below)
- The queue fills with other events for that destination

**Code path:** `src/api/client/to_device.rs` → `send_edu_server()` →
federation sender queue.

### 2. Startup Netburst Queue Truncation

On server restart, the federation sender replays queued events via the
"netburst" mechanism. However, the queue is **truncated** per destination:

```rust
// src/service/sending/sender.rs
let keep = self.server.config.startup_netburst_keep; // default: 50
if entry.len() >= keep {
    warn!("Dropping unsent event {dest:?}");
    self.db.delete_active_request(&key);
}
```

**Impact:** If more than 50 events (PDUs + EDUs combined) are queued for a
destination when the server restarts, to-device key shares beyond the limit
are **permanently dropped**. Unlike PDUs which can be fetched later via
`/event`, to-device messages have no recovery mechanism.

**Mitigation:** Increase `startup_netburst_keep` to `500` or higher on
servers with frequent E2EE traffic.

### 3. Device List Update Propagation

Before encrypting, a client queries device lists for all room members. The
server must gossip device list changes via `m.device_list_update` EDUs.

Continuwuity sends **placeholder** device list updates that force remote
servers to re-sync:

```rust
// src/service/sending/sender.rs, select_edus_device_changes()
let edu = Edu::DeviceListUpdate(DeviceListUpdateContent {
    user_id: user_id.into(),
    device_id: device_id!("placeholder").to_owned(),
    device_display_name: Some("Placeholder".to_owned()),
    stream_id: uint!(1),
    prev_id: Vec::new(), // Empty prev_id forces resync
    ..
});
```

If these placeholder updates don't reach the remote server (due to backoff
or queue truncation), the remote client may encrypt for stale/missing
devices, resulting in undecryptable messages.

## Comparison with Synapse

Synapse handles to-device messages more reliably because:

1. **Dedicated to-device queue** — Synapse separates to-device messages from
   the general federation transaction sender, with independent retry logic
2. **Persistent retry** — To-device messages are retried with their own
   backoff schedule, not mixed with presence/typing/PDU backoff
3. **No queue truncation** — Synapse does not drop to-device messages on
   restart

## Diagnostic Commands

### Existing

- `uwu> federation incoming-federation` — Show active incoming transactions
- `uwu> rooms bump --all` — Probe stagnant rooms for missing events

### Needed (TODO)

- `federation sender-status <server>` — Show backoff state, queue depth,
  last successful transaction timestamp for a destination
- `federation clear-backoff <server>` — Force-clear exponential backoff for
  a destination to allow immediate retry
- `federation queue-depth [server]` — Show pending PDU/EDU counts per
  destination

## Recovery Options

Once keys are lost, server-side recovery is not possible. Options:

1. **Client-side key request** — Long-press undecryptable message → "Request
   keys" (if the sender's client is online and supports key forwarding)
2. **Key backup** — If the sender has server-side key backup enabled,
   recipients with cross-signing can request keys from backup
3. **Re-share keys** — Sender can manually re-share room keys from client
   settings (client-dependent)

## Configuration Recommendations

For continuwuity instances with E2EE rooms:

```toml
# Increase queue retention on restart (default: 50)
startup_netburst_keep = 500

# Ensure outgoing federation is enabled
allow_federation = true

# Ensure device list updates are sent
# (no explicit config — always enabled when federation is on)
```

## Related Files

- `src/api/client/to_device.rs` — Client-to-server to-device endpoint
- `src/service/sending/sender.rs` — Federation sender (backoff, netburst, EDU selection)
- `src/service/sending/mod.rs` — `send_edu_server()`, `send_edu_room()`
- `src/api/server/send.rs` — Incoming federation transaction handler (to-device receipt)

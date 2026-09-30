# MSC4500: State Accumulator Optimizations

## Internal optimizations

Mostly internal state tracking and storage!

For example, when appending a new state event in `append_state_pdu`, the homeserver creates a new `ShortStateHash`. If multiple forks independently converge to the exact same state, they might currently generate _different_ `ShortStateHash` identifiers because checking if that massive `BTreeSet` state already exists in the DB is computationally expensive.

If we index `ShortStateHash` by their `LtHash` digest, we can instantly `O(1)` deduplicate state groups across the entire server. When a new state is computed, we calculate its `LtHash`. If that digest already exists in the database, we completely skip writing a new state-diff chain and simply reuse the existing `ShortStateHash`! That would drastically reduce database size and State Group fragmentation.

---

## Endpoint optimizations

There are massive opportunities to leverage `LtHash` beyond just short-circuiting local state resolution:

### 1. `/state_ids` and `/state`

When a server requests `GET /_matrix/federation/v1/state_ids/{roomId}?event_id={eventId}`, the responding server currently sends back a massive array of every single state event ID in the room. If we include the `LtHash` digest in the response (e.g. `{"pdu_ids": [...], "state_hash": "..."}`), the requesting server can instantly compare the digest to its own local state. If they match, the requesting server doesn't even need to process the massive array of IDs because it mathematically proves their state is perfectly synced!

### 2. Fast `/get_missing_events`

When finding missing events, servers often have to do heavy graph traversal. With `LtHash`, a server could send its current state digest in the request. The remote server can instantly verify if the requesting server's state diverges, and if so, exactly where, drastically cutting down on graph reconstruction.

### 3. `/make_join` & `/send_join` validation

When joining a room, you are handed the state by a remote server. By computing the `LtHash` of the handed state and checking it against a known good digest (if we have one from another server in the room), we can immediately detect if a malicious server is lying about the room state (e.g. omitting bans or fabricating power levels) without having to perform full state resolution or event auth on the entire tree first.

### 4. MSC4500 `/reconcile` Endpoint

As mentioned in the MSC4500 spec, `LtHash` is the foundation of the `/reconcile` endpoint, allowing out-of-sync servers to request a "state bisect" path from a healthy peer to quickly fast-forward missing room state without a heavy `make_join` process.

### 5. Internal: State Groups & Deltas

Currently, homeservers use complex delta-chains or massive BTreeSets to track incremental state changes (State Groups). By pairing state groups with their `LtHash` digest, we can perform O(1) identity checks when branching or merging state groups in the database, avoiding the need to reconstruct delta chains just to verify if two groups converged to the same state.

### 6. Internal: Fast State Resolution (Implemented)

As recently merged, we can short-circuit the Matrix State Resolution V2 algorithm entirely. During a fork, we calculate the `LtHash` of each extremity. If they match, we immediately return the state without doing any graph traversal or diffing. Furthermore, we use `LtHash` to deduplicate deep forks before materializing them, saving massive amounts of RAM and DB overhead.

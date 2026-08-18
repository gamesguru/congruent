# HAMT-based state reads: runtime trade-offs

This note covers the runtime/efficiency trade-off of completing the migration
from the legacy `shortstatehash` state-accessor (delta‑chained statediffs) to
HAMT‑rooted reads (`rezzy::hamt`), and where the code currently stands.

## Migration status (as of PR #89)

| Layer                                                                                                     | Status          | Notes                                                                                                                                                                                     |
| --------------------------------------------------------------------------------------------------------- | --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Write / state transition**                                                                              | ✅ Done         | `append_to_state` builds a HAMT root + node and `persist_node_recursive`s nodes + root handle. No code writes `shortstatehash_statediff` anymore; the `state_compressor` service is gone. |
| **Transition-time "read current state"**                                                                  | ✅ Done         | Inside `append_to_state`, `load_state_map_from_root_handle(root_handle)` traverses the HAMT via `state_hamt.store.get_node` + `visit_entries`, not the legacy delta chain.                |
| **Public/external reads** (sync, spaces, send_join, context, room_state, resolve_state, member summaries) | ✅ Migrated     | All public consumers obtain a `RootHandle` via `get_room_state_hamt` / timeline root handles and read through the `*_hamt` accessors. The legacy `get_room_shortstatehash`, `state_full_shortids`, and `load_full_state` read paths are gone. |

So this PR cut over both the _writer_ and the cloud-facing _readers_ to the HAMT

## The two read models

### Legacy: `shortstatehash` → delta chain

A room's latest state is a `ShortStateHash` (a version number). To materialize
state you follow `statehash_shortstatehash` (parent pointer) backwards through
`shortstatehash_statediff` records — each record is a small (add/rem) delta —
and splice deltas into an accumulator:

```
shortstatehash (current) ─●──parent──●──parent──● ── … ──> empty
                           │           │           │
                        statediff   statediff   statediff   (each = small add/rem set)
```

**Costs:**

- **N dB lookups** where N = number of deltas since the accumulator's last
  baseline (O(state history depth) per read).
- Each intermediate `(shortstatekey, shorteventid)` must be _materialized_ into
  an event id anyway (that part is shared).
- Reads **amortize well for recent state** (few deltas) but degrade with long
  change history unless periodically compacted.

### HAMT: `RootHandle` → one root node, persistent trie

A room's current state is `roomid_roothandle → RootHandle` (48 bytes: 16-byte
structural hash + 32-byte state-group id). The node graph lives in
`state_hamt_nodes`, keyed by structural hash. Materializing state is a single
root-node fetch plus `visit_entries`, which lazily resolves child nodes only
along the trie path you touch:

```
roomid_roothandle ─ root_handle ─ get_node(structural_hash) ─ root node
                                              │ visit_entries (resolves child nodes lazily)
                                              ▼
                          full (shortstatekey, shorteventid) entries
```

**Costs:**

- **O(1) “$N$ lookups” for structural identity** — you always talk in
  `RootHandle`s (content-addressable), so two handles can be compared/hashed
  without materializing anything.
- **Structural sharing**: unchanged state subtrees are deduplicated by content
  hash, so two similar versions share node storage; materializing a version only
  pays for the nodes that differ from what you already hold.
- **One root fetch + trie walk** instead of replaying N deltas. Walk cost is
  O(entries + touched path length × fanout), independent of room change history.

## Trade-off summary

| Concern                                | Legacy `shortstatehash`                  | HAMT `RootHandle`                                    |
| -------------------------------------- | ---------------------------------------- | ---------------------------------------------------- |
| Read a room's full state               | O(history depth) delta lookups           | O(1) root fetch + trie walk                          |
| Read a single `(type,key)` state event | via full materialization (shared)        | can be routed toward the relevant trie path (shared) |
| Structural identity / hash             | recompute from materialized set          | `RootHandle.structural_hash` is the identity         |
| Storage of similar versions            | copies full set per delta chain baseline | content-addressable sharing                          |
| Memory during read                     | accumulator grows to full state          | visits lazily; holds only what you materialize       |
| Write                                  | build + compact deltas (gone)            | build trie + persist changed nodes                   |

### When HAMT is strictly better

- Rooms with **long change history** (legacy pays N lookups per read; HAMT
  ignores history).
- **Many rooms / many versions** shared in memory — structural sharing and
  content addressing reduce duplicate materialization and let you compare or
  cache versions without re-walking.
- **Frequent state reads** of the same room (each read is cheap and stable).

### When the legacy model had an edge / what to watch

- **Tiny delta updates**: the legacy model's per-read cost only pays the
  _incremental_ change since baseline; a naive HAMT full-walk per read re-visits
  the whole trie each time. Mitigation: keep the trie _shallow and wide_ (the
  `rezzy` fanout), and prefer reads of a single (type,key) over full
  materialization where the consumer only needs one field.
- **Point lookups** can be a regression if implemented as "materialize all +
  filter". The existing benches (`hamt_point_lookups`) already exercise the
  dedicated search path; consumers should use it rather than
  `load_full_state_hamt`.
- **`state_is_empty_hamt`** currently materializes the whole map
  (`load_full_state_hamt`) just to check `.is_empty()`. A structural check on
  the root hash would be O(1). Flagged as a future optimization in the code
  (`state_accessor/state.rs`).

## Public-consumer status (as of PR #89 head)

The cloud-facing readers all resolve a `RootHandle` (via `get_room_state_hamt`
or a timeline root handle) and read through the `*_hamt` accessors:

- `src/api/client/sync/v3/joined.rs:452/961` and `sync/v5.rs:1889` — sync room
  state via `get_room_state_hamt` / `state_full_shortids_hamt`.
- `src/api/client/sync/v3/left.rs` — left-room boundary via per-event root
  handles.
- `src/api/client/context.rs:172` — context around an event.
- `src/api/server/send_join.rs:51` and `state.rs`/`state_ids.rs` — room state
  / event ids sent to joining or querying servers.
- `src/service/rooms/spaces/mod.rs:243` — space summary.
- `src/service/rooms/state_accessor/room_state.rs` — state resolution.
- `src/service/rooms/event_handler/resolve_state.rs:27` and
  `state_at_incoming.rs` — incoming event resolution.

The legacy `get_room_shortstatehash`, `state_full_shortids`, and
`load_full_state` read paths no longer exist on these hot paths; the only
remaining uses of the `roomid_shortstatehash` /
`shorteventid_shortstatehash` / `statehash_shortstatehash` maps are as
migration inputs in `migrations.rs`.

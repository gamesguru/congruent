# HAMT read-path cutover: rezzy tool gaps assessment

Status of the MSC4511 / augmented-HAMT read-path refactor with respect to what
the upstream `rezzy` HAMT crate needs to expose.

## Summary

The remaining read-path work (sync v3/v5, state-at-incoming, state resolution)
can be completed with the current `rezzy` API — no new rezzy methods are
required, and no pending rezzy changes block the cutover. The one derive change
identified earlier (making `RootHandle` usable as a map key) has **landed
upstream**: `rezzy::hamt::RootHandle` now derives `Hash` (see below), so a
resolved root can be used directly as a hash-map/hash-set key in fork-dedup
logic without keying on the lossier `structural_hash`.

## What already exists (no rezzy change needed)

These are all available and in use:

| Primitive                                                                                               | Location                 | Used for                                                                                    |
| ------------------------------------------------------------------------------------------------------- | ------------------------ | ------------------------------------------------------------------------------------------- |
| `RootHandle` (`structural_hash: [u8;16]`, `state_group_id: [u8;32]`)                                    | rezzy                    | resolved-root identity                                                                      |
| `store.get_node(&structural_hash)`                                                                      | rezzy store              | traversal root                                                                              |
| `root_node.search(&structural_key, &shortstatekey, ...)`                                                | rezzy                    | point lookup                                                                                |
| `root_node.visit_entries(...)`                                                                          | rezzy                    | full-state materialization                                                                  |
| `load_full_state_hamt(&RootHandle)`                                                                     | conduwuit accessor       | build `(ShortStateKey, ShortEventId)` map / `state_added_hamt`/`state_removed_hamt` diffing |
| `state_full_shortids_hamt(RootHandle)`                                                                  | conduwuit accessor       | owned-handle stream                                                                         |
| `state_full_ids_hamt(&RootHandle)`                                                                      | conduwuit accessor       | `(ShortStateKey, OwnedEventId)` stream                                                      |
| `state_full_pdus_hamt` / `state_full_hamt`                                                              | conduwuit accessor       | full PDUs / typed PDUs                                                                      |
| `state_get_shortid_hamt` / `state_get_in_room_hamt` / `state_get_content_hamt` / `user_membership_hamt` | conduwuit accessor       | point reads                                                                                 |
| `state_contains_shortstatekey_hamt`                                                                     | conduwuit accessor       | membership-of-key test                                                                      |
| `get_room_state_hamt(room_id)`                                                                          | conduwuit `rooms::state` | current room root                                                                           |
| timeline `prev/next/get_root_handle`                                                                    | conduwuit timeline       | sync per-event root handles                                                                 |

## The rezzy `RootHandle: Hash` change — landed upstream

`rezzy::hamt::RootHandle` now derives (rezzy dev branch, rev `03fb50f`):

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct RootHandle {
    pub structural_hash: StructuralHash, // [u8; 16]
    pub state_group_id: StateGroupId,    // [u8; 32]
}
```

It is `Eq`, `PartialEq`, and now `Hash`, so roots can be used directly as
`HashMap`/`HashSet` keys in the two places the remaining cutover needs:

1. `event_handler/state_at_incoming.rs` — forks are collected via
   `try_collect::<HashMap<_,_>>()` keyed by the per-prev-event state identity
   and deduplicated by digest.
2. sync-v3/v5 join logic that compares/aggregates per-event roots.

Combining both fields (local `structural_hash` + global `state_group_id`) makes
root handles first-class map keys in the same spirit as the old
`ShortStateHash: u64`. No `Ord` is derived and none is needed; the codebase does
not order roots.

## Conduwuit-side accessor add (not a rezzy change) — landed

To mirror the legacy `state_contains_type` (used for room summary / heroes in
sync v3), conduwuit added
`state_contains_type_hamt(&RoomId, &RootHandle, &StateEventType) -> bool`
(`src/service/rooms/state_accessor/state.rs`). It iterates
`state_full_shortids_hamt` and stops at the first state-key of the requested
type; it requires no rezzy addition. It is already in use by sync v3 room
summary / heroes (`src/api/client/sync/v3/joined.rs`).

## Non-goals / deliberately settled

- `get_extremity_lthash` (state-at-incoming fast path) stays a `NotImplemented`
  stub for now; the resolve-path always run. Implementing it is separate
  MSC00DC/lattice work and does not block the cutover.
- Timeline root-handle lookups are deliberately uncached for the moment; the
  existing u64 `ShortStateHash` caches don't fit `RootHandle`. Adding
  `RootHandle` value caches for `(ShortRoomId, PduCount)` is a later
  optimization, not a correctness blocker.

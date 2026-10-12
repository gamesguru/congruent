# Device List Updates: Distinguish Kicked from Left

## Context

In `src/api/client/sync/v3/joined.rs` (`build_device_list_updates`), we handle
membership transitions for `device_lists.changed` and `device_lists.left`. Currently
`Leave` and `Ban` are handled together:

```rust
| Leave | Ban => {
    // User left/was kicked/banned from this room.
    // Only add to `left` if they don't share any
    // OTHER encrypted rooms.
    ...
}
```

## Problem

The Matrix spec represents "kicked" as `membership: leave` where `sender != state_key`
(i.e., someone else removed the user). There is no distinct `MembershipState::Kicked`
variant. This means we can't distinguish voluntary leave from involuntary kick at the
type level.

## Proposed Improvement

Consider distinguishing kicked users by checking `sender != state_key` on the
membership event. This could be useful for:

- More accurate device list tracking (a kicked user may rejoin; a voluntarily-left
  user is less likely to)
- Logging and audit trails
- Potential future UX in admin tools

## Current Behavior

All of `Leave`, kicked (`Leave` where sender ≠ state_key), and `Ban` are treated
identically for `device_lists.left`.

## Files

- `src/api/client/sync/v3/joined.rs` — `build_device_list_updates()` around line 956

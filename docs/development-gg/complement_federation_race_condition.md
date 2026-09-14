# Complement Test Flakiness: Federation Race Condition

## Overview

A consistent flakiness was observed in `TestKnockRestrictedRoomsLocalJoinNoCreatorsUsesPowerLevelsV12` inside the Complement test suite. The test occasionally fails when a user on `hs2` attempts to join a restricted room immediately after a user on `hs1` updates the power levels.

## The Race Condition

The Complement test executes the following sequence:

1. `alice` (on `hs1`) sends an `m.room.power_levels` event granting `bob` (also on `hs1`) authorization powers.
2. The test harness calls `SendEventSynced`, which blocks until `hs1` processes the event and it appears in `alice`'s local sync stream.
3. Immediately after, `charlie` (on `hs2`) attempts to join the room using `hs2` as the authorization server.

Matrix federation is asynchronous. `SendEventSynced` only guarantees local consistency on `hs1`. It does **not** guarantee that `hs1` has transmitted the event to `hs2` over federation, nor that `hs2` has finished processing the incoming transaction.

When `charlie` initiates the local join on `hs2`, `hs2` checks its local state to authorize the join. If the power level event has not arrived yet, `hs2` falls back to a remote join (or rejects the join if testing strict local authorization), causing the test to fail intermittently depending on CPU load and network latency.

## Proposed Solution

To eliminate the flakiness, the test harness must explicitly wait for the state to propagate across the federation boundary before initiating the dependent action.

By inserting a `MustSyncUntil` block for a user on `hs2` (e.g., waiting for `hs2` to see the new `m.room.power_levels` event in the sync stream) before `charlie` joins, we ensure `hs2`'s local state is fully caught up with `hs1`.

### Example Fix

```go
// Wait for federation: Ensure hs2 has received the new power levels before Charlie joins
bob.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHas(room, func(ev gjson.Result) bool {
    return ev.Get("type").Str == "m.room.power_levels" && ev.Get("sender").Str == alice.UserID
}))

// Now Charlie can safely join
charlie.JoinRoom(t, allowed_room, []spec.ServerName{
    deployment.GetFullyQualifiedHomeserverName(t, "hs1"),
})
```

While this was reverted from the `continuwuity` submodule to avoid drifting from upstream, it should be proposed as a patch to the official matrix-org Complement repository to resolve the upstream CI flakiness.

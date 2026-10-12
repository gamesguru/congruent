# TestUnrejectRejectedEvents

## Status: FAIL (consistent)

## What the test does

1. Complement creates a room on its mock server
2. hs1 joins via federation
3. Complement sends an event whose auth chain references an event that Complement deliberately withholds (returns 404 for `/event/` and `/state_ids/`)
4. hs1 **rejects** the event because it can't verify the auth chain
5. Complement later sends the missing auth event (via `/send` transaction)
6. Test expects hs1 to **un-reject** the previously rejected event and show it in `/sync` timeline

## Why it fails

We reject the event and never reconsider it. When the missing auth event later arrives, we don't go back and re-evaluate previously rejected events that depended on it.

Per the Matrix spec (server-server API, "Handling failures"):

> "Subsequent events from other servers that reference rejected events should be allowed if they still pass the auth rules."

This implies rejected events should be re-evaluated when new auth events arrive.

## Observed behavior

- `/sync` timeline never contains the previously-rejected event (5 attempts over 5s, all show `timeline.events does not exist`)
- Server makes unexpected requests to Complement (`GET /event/`, `GET /state_ids/`, `PUT /send/`) which get 404'd, causing backoff

## What's needed to fix

An "unreject" mechanism in the event handler:

- When a new event arrives that was previously missing from an auth chain, scan for rejected outliers that reference it
- Re-run auth checks on those rejected events with the now-complete auth chain
- If they pass, clear the rejected marker and promote to timeline

This is a non-trivial feature requiring:

1. A reverse index: "which rejected events reference this event_id as an auth event?"
2. Re-evaluation logic in the event handler pipeline
3. Proper cascading (un-rejecting event A might un-reject event B that depends on A)

## Spec references

- [Server-Server API: Handling failures](https://spec.matrix.org/v1.13/server-server-api/#checks-performed-on-receipt-of-a-pdu)
- [Server-Server API: Soft failure](https://spec.matrix.org/v1.13/server-server-api/#soft-failure)

## Distinction from soft-fail

- **Rejected**: Auth fails against the state _before_ the event (definitive failure — bad signatures, missing auth chain). Per the spec, a homeserver cannot trust an event if its authorization chain cannot be verified. Therefore, a missing auth chain _must_ result in rejection rather than a soft-fail.
- **Soft-failed**: Auth passes basic validation (signatures, hashes, and auth chain) but fails checks against the _current_ state of the room (e.g. trying to join or send a message when the server's current state believes the sender is banned). Soft-failed events participate in state resolution and do not trigger cascading rejections of their descendants.

### Why we cannot simply soft-fail missing auth chains:

1. **Security / Trust Boundaries**: Accepting events with unverified auth chains as "soft-failed" would allow a rogue server to inject unauthorized events, which we would blindly ingest and allow to participate in state resolution.
2. **Sync Promotability**: Both rejected and soft-failed events are hidden from the `/sync` timeline. Even if we soft-failed the event initially, the client would still not see it when the missing auth events arrived. We would still require the exact same reactive re-evaluation mechanism to clear the soft-fail marker and promote it to the timeline.
3. **Synapse Parity**: Synapse strictly rejects events with missing auth chains, stashing them as rejected outliers. When the missing auth event later arrives in a subsequent transaction, Synapse triggers a re-evaluation of dependent rejected events, promoting them to the timeline if they now pass.

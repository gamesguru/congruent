# Dehydrated Device Verification Bug (MSC3814)

## Symptom

Dehydrated devices show as **"Unverified"** in Element Web after creation.
Element prompts to "Verify other device" but dehydrated devices can't accept
interactive verification (they're not active sessions).

## Root Cause Analysis

### Flow observed (Element Web console)

1. `GET /_matrix/client/unstable/org.matrix.msc3814.v1/dehydrated_device` -> 404
2. `PUT /_matrix/client/unstable/org.matrix.msc3814.v1/dehydrated_device` -> 200
3. Element does **NOT** call `/keys/signatures/upload` afterward
4. Element expects the cross-signing signature to already be inside the
   `device_keys` payload of the PUT request

### Server-side issues found

#### 1. `fallback_keys` silently dropped

The Ruma `Request` struct includes `fallback_keys: BTreeMap<OwnedOneTimeKeyId, Raw<OneTimeKey>>`
but the handler in `set_dehydrated_device()` never processes them.
Without fallback keys, the dehydrated device cannot receive pre-key encrypted messages.

**File:** `src/service/users/dehydrated_device.rs` (line 75-79 handles `one_time_keys` but
`fallback_keys` is completely absent)

#### 2. Cross-signing signature in `device_keys` may not propagate correctly

`add_device_keys()` stores the raw `DeviceKeys` JSON (which includes cross-signing
signatures from Element) and calls `mark_device_key_update()`. The signatures are
stored, but the question is whether they're returned correctly in `/keys/query`
responses so that other devices recognize the dehydrated device as verified.

#### 3. Device may not persist

In one test, the dehydrated device ID `muakKuv46S9ixFt4XmUWPZGnnByNUDvILTPdpQB0mEA`
was **not in the device list** (`list-devices`) despite being created. Possible causes:

- `create_device()` is called with empty token `""` — may cause issues
- Another codepath is cleaning up the device
- The dehydrated device data persists in `userid_dehydrateddevice` but the device
  entry in `userdeviceid_metadata` is lost

### Admin debug commands

```
!admin query users list-devices @user:server
!admin query users get-device-keys @user:server DEVICE_ID
!admin query users get-master-key @user:server
!admin query users get-user-signing-key @user:server
```

No admin command exists for `get_dehydrated_device_id` yet.

## Fix plan

1. **Add `fallback_keys` handling** in `set_dehydrated_device()` — similar to
   `one_time_keys` loop but using the fallback key storage
2. **Add `info!` logging** to `set_dehydrated_device()` to trace the full flow
   (device creation, key upload, one-time keys, fallback keys)
3. **Investigate device persistence** — ensure the device created for dehydration
   survives and appears in `/keys/query` responses
4. **Verify cross-signing signatures** — ensure the signatures embedded in
   `device_keys` from the PUT request are returned in `/keys/query` so other
   devices see the dehydrated device as verified

## References

- MSC3814: https://github.com/matrix-org/matrix-spec-proposals/pull/3814
- Ruma request struct: `ruwuma/crates/ruma-client-api/src/dehydrated_device/put_dehydrated_device.rs`
- Server handler: `src/api/client/dehydrated_device.rs`
- Service impl: `src/service/users/dehydrated_device.rs`

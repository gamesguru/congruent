# Ed25519 Point Decompression Interop Issue

## Problem

Some Matrix homeservers publish ed25519 public keys with non-canonical
compressed Edwards point encodings. These keys are accepted by libsodium
(used by Synapse via PyNaCl) but rejected by `ed25519-dalek` (used by
continuwuity via `ruma-signatures`).

### Error

```text
Cannot decompress Edwards point
```

This causes continuwuity to reject events from affected servers during both
inbound federation and admin `compare-room-state` operations.

## Verification Chain

```text
continuwuity
  └─ ruma::signatures::verify_json()
       └─ ruma-signatures 0.15.0 (ruwuma fork)
            └─ ed25519-dalek 2.2.0
                 └─ curve25519-dalek (CompressedEdwardsY::decompress)
```

The rejection happens at `curve25519-dalek`'s `CompressedEdwardsY::decompress()`
which returns `None` for non-canonical point encodings.

## Why Synapse Accepts These

Synapse uses PyNaCl → libsodium → `crypto_sign_verify_detached()`. libsodium
performs verification using `ge25519_frombytes_negate_vartime()` which is more
permissive with point encoding. It successfully verifies signatures against
keys that `curve25519-dalek` refuses to even parse.

## Potential Fixes

### Option 1: Patch ruma-signatures (recommended)

Add a fallback in ruma-signatures that catches the point decompression error
and retries with a lenient parser. This would require:

1. Catching the `SignatureError` from `ed25519-dalek`
2. Manually decompressing the point with a tolerant implementation
3. Performing verification with the recovered public key

**Pros**: Minimal blast radius, only affects signature verification
**Cons**: Requires forking/patching ruma-signatures further

### Option 2: Enable `legacy_compatibility` in ed25519-dalek

The `legacy_compatibility` feature relaxes **signature scalar** checks but
does NOT fix point decompression. This alone is insufficient.

### Option 3: Use libsodium for verification fallback

Add `libsodium-sys` as an optional dependency and fall back to it when
`ed25519-dalek` rejects a key. This matches Synapse's behavior exactly.

**Pros**: Exact compatibility with Synapse
**Cons**: Additional C dependency, FFI complexity

### Option 4: Accept unverified events from trusted servers

When fetching state from a trusted server (e.g., during `force-set`),
skip signature verification for events that fail point decompression.
The trusted server has already verified the event.

**Pros**: No crypto changes needed
**Cons**: Weakens security model for those specific events

## Impact Assessment

- Events affected: Rare — only from servers with malformed keys
- State divergence: Minor — typically 1-2 membership events per room
- Security risk of lenient parsing: Low — the keys are valid ed25519 keys
  with non-canonical encoding, not forged keys

## Spec Gap

The Matrix specification (room versions 1-11) mandates ed25519 for PDU
signatures but does not specify:

- Required encoding strictness for public keys
- Whether implementations must accept non-canonical point encodings
- Behavior when crypto libraries disagree on key validity

A spec clarification on canonical encoding requirements would resolve this
class of interop issue across all implementations.

## References

- [ed25519-dalek legacy_compatibility](https://docs.rs/ed25519-dalek/latest/ed25519_dalek/#features)
- [curve25519-dalek point decompression](https://docs.rs/curve25519-dalek/latest/curve25519_dalek/edwards/struct.CompressedEdwardsY.html)
- [libsodium ed25519 verify](https://doc.libsodium.org/public-key_cryptography/public-key_signatures)
- Observed in rooms: `!sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE`,
  `!aPQx8hPCm0vu6PiwsZbVSsVPlugBWOyanle4y5-8p7Q`

# DNS 0x20 Case Randomization & ServFail Caching Bug

## Problem

Federation connections fail for servers whose authoritative nameservers do not
support DNS 0x20 case randomization (RFC draft). The failure is silent — it
appears as a normal "no records found" negative response, not an error.

### Observed Behavior

```
uwu> debug ping furfy.dev
NoRecordsFound {
    query: Query { name: Name("fuRfY.deV."), query_type: A, ... },
    response_code: ServFail,
    ...
}
```

The server `furfy.dev` is independently confirmed reachable (`testmatrix`
reports Tuwunel 1.7.1 on port 443 via .well-known), but continuwuity cannot
federate with it.

### Root Cause

Two compounding bugs:

**1. DNS 0x20 hardcoded to `true`**

In `src/service/resolver/dns.rs`:

```rust
opts.case_randomization = true;
```

DNS 0x20 randomizes query name casing (e.g. `furfy.dev` → `fuRfY.deV`) as a
cache-poisoning defense. However, many nameservers in the wild do not handle
mixed-case queries correctly and return `ServFail` instead of echoing the query
name verbatim. This breaks resolution entirely for those domains.

**2. `is_no_records_found` treats ServFail as benign**

hickory-resolver wraps `ServFail` responses in the same `NoRecordsFound` enum
variant used for `NXDomain`:

```rust
ProtoErrorKind::NoRecordsFound {
    response_code: ServFail,  // ← NOT NXDomain
    ...
}
```

The `is_no_records_found` function matched on `NoRecordsFound { .. }` without
inspecting `response_code`, causing two problems:

- **Metrics**: ServFail not counted as a DNS failure
- **Error handling**: `handle_resolve_error` in `actual.rs` silently swallows
  the error (returns `Ok(())`) instead of propagating it

### Caching Amplification

hickory-resolver's internal in-memory LRU cache stores negative responses using
`negative_min_ttl` / `negative_max_ttl`, which are both set to
`dns_min_ttl_nxdomain` — **3 days** by default.

hickory does not distinguish between `NXDomain` and `ServFail` for caching
purposes. A single ServFail response gets cached for up to 3 days. During that
window, all federation to that server is broken.

Flushing the continuwuity resolver cache (`query resolver flush-cache --all`)
does clear hickory's internal cache via `resolver.clear_cache()`. However, the
very next lookup immediately re-poisons the cache if 0x20 is still enabled.

**Cache flush does NOT fix this** — it was confirmed that flushing and
retrying produced a different 0x20 randomization (`fUrfY.DEV.`) that also
failed with ServFail.

## Fix

### 1. New config option: `dns_case_randomization`

**File**: `src/core/config/mod.rs`

```rust
/// Enable DNS 0x20 case randomization for cache-poisoning protection.
/// This randomizes the case of query names (e.g. `example.com` becomes
/// `eXaMpLe.CoM`) as a defense against DNS cache-poisoning attacks.
///
/// Some nameservers do not properly handle mixed-case queries and will
/// return ServFail, breaking federation with those servers entirely.
/// This is disabled by default for maximum compatibility.
#[serde(default)]
pub dns_case_randomization: bool,
```

Default: `false`. Operators can opt-in if their upstream resolvers and the
federation ecosystem they interact with support it.

**File**: `src/service/resolver/dns.rs`

```diff
-opts.case_randomization = true;
+opts.case_randomization = config.dns_case_randomization;
```

### 2. Tighten `is_no_records_found` to reject ServFail

**File**: `src/service/resolver/dns.rs`

```diff
-/// Check if a DNS resolve error is a NoRecordsFound (NXDOMAIN) response.
-/// These are valid negative responses, not actual failures.
-fn is_no_records_found(e: &hickory_resolver::ResolveError) -> bool {
-    use hickory_resolver::{ResolveErrorKind::Proto, proto::ProtoErrorKind};
-
-    matches!(
-        e.kind(),
-        Proto(e) if matches!(e.kind(), ProtoErrorKind::NoRecordsFound { .. })
-    )
-}
+/// Check if a DNS resolve error is a NoRecordsFound (NXDOMAIN/NoError)
+/// response. These are valid negative responses, not actual failures.
+/// ServFail is explicitly excluded as it indicates a transient server error.
+fn is_no_records_found(e: &hickory_resolver::ResolveError) -> bool {
+    use hickory_resolver::{
+        ResolveErrorKind::Proto,
+        proto::{ProtoErrorKind, op::ResponseCode},
+    };
+
+    matches!(
+        e.kind(),
+        Proto(e) if matches!(
+            e.kind(),
+            ProtoErrorKind::NoRecordsFound {
+                response_code: ResponseCode::NXDomain | ResponseCode::NoError,
+                ..
+            }
+        )
+    )
+}
```

This ensures ServFail is:

- Counted as a DNS failure in metrics
- Propagated as an error in `handle_resolve_error` (not silently swallowed)

## Known Remaining Issue

hickory-resolver's internal cache does not distinguish `ServFail` from
`NXDomain` for negative TTL purposes. If a ServFail occurs (for any reason,
not just 0x20), it will be cached for `dns_min_ttl_nxdomain` seconds (default
3 days). Splitting the negative TTL by response code would require changes
to hickory-resolver itself or a wrapper that intercepts caching behavior.

With `dns_case_randomization = false` (the new default), this is largely
mitigated since the primary source of spurious ServFail responses is
eliminated.

## Context

- **Port 8448 default**: mandated by [Matrix Server-Server spec §3.1](https://spec.matrix.org/latest/server-server-api/#resolving-server-names) step 5
- **DNS 0x20**: described in [draft-vixie-dnsext-dns0x20](https://datatracker.ietf.org/doc/html/draft-vixie-dnsext-dns0x20-00) — adds entropy to DNS queries by randomizing case as a cache-poisoning defense
- **hickory-resolver version**: 0.25.2
- Synapse does not use 0x20 (uses system `getaddrinfo` via Twisted's `GAIResolver`)

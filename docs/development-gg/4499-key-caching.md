# MSC4499: Strict server signing key caching and key ID uniqueness

Because the specification lacks a strict caching contract, new homeserver
implementations often attempt to be "helpful." Without explicit guidance,
developers may design flexible caches that store multiple key bodies for a
single key ID and perform verification either with the most recently observed
key (last-wins) or the first one which works (trial verification).

While existing implementations such as Synapse effectively enforce a unique
`(server_name, key_id)` constraint at the storage layer, this only guarantees
one stored row per key ID, not which key body wins when a new observation
conflicts with an existing one: Synapse's storage layer resolves such conflicts
by last-write-wins (an upsert keyed on `(server_name, key_id)`), the opposite of
the First Seen Wins rule this MSC introduces. The protocol itself remains
underspecified and does not give clear guidance on this matter.

This ambiguity leads to an annoying loophole where key collisions in the wild
can cause room state divergence between servers, and introduces possible risks
or undefined behaviors if attempting to gracefully handle them (by trial).

This MSC standardizes signing key caching requirements, introduces a strict
**First Seen Wins** rule for key IDs, and lays the groundwork for future work.

This proposal deliberately rejects trial verification in favor of deterministic
binding, even though that causes more visible failures when a server publishes
conflicting material. Deterministic failure is preferable to receiver-specific
verification behavior.

## Proposal

<!-- TODO: indicate Synapse-derived subset of spec via comment fences. -->

### Relationship to existing specification

This MSC strengthens and supersedes the existing key caching and verification
rules defined in the Matrix specification (specifically the
[Server-Server API § Retrieving server keys](https://spec.matrix.org/v1.18/server-server-api/#retrieving-server-keys)
and the notary query endpoint). In particular, this proposal upgrades the
existing `SHOULD` caching guidance to `MUST`, formalizes the `valid_until_ts`
7-day validity clamp as a cache constraint, and replaces ambiguous logic with a
strict 1:1 key ID uniqueness paradigm and accompanying caching guidance.

### Key caching requirements

Servers MUST cache federated server signing keys procured from
`/_matrix/key/v2/server` responses and `/_matrix/key/v2/query` notary responses.
The following requirements apply to all signing algorithm types (`ed25519`, and
any potential future signing algorithms defined by later proposals).

<!-- Read marker. -->

**Cache refresh lifetime.** Servers MUST cache key responses and SHOULD
proactively refresh cached keys before their clamped `valid_until_ts` expiry
(restricted to _at most_ 7 days from fetch) to avoid verification failures
during key rotation windows. When a server re-fetches a key and receives the
exact same key body it already has, this is a normal refresh; the server MUST
simply update its cached `valid_until_ts` and `expired_ts` timestamps.
Furthermore, servers MUST rely on their cache. They MUST NOT fetch origin keys
for every inbound message or request if a valid key is already cached locally.

**Negative caching and backoff.** Servers MUST cache fetch failures. A dead or
unreachable remote server can cause fetch storms if every inbound event or
reference triggers a fresh network request. Servers MUST implement exponential
backoff (e.g., starting at 1 minute, capping at 1 hour) per remote server for
failed key fetches. Inbound federation demand whose authentication _requires_ a
key fetch for a backoff-listed server SHOULD permit at most one immediate
(rate-limited) fetch attempt per remote server per backoff interval, where the
rate limit MUST NOT be looser than the current backoff interval itself (i.e.
this escape valve MUST NOT be used to reconstruct a fetch frequency above what
the backoff schedule already allows); all further demand arriving within that
interval MUST fail fast against the negative cache rather than triggering its
own probe. Without this per-interval limit, an attacker can relay junk
purportedly signed by a dead server's name to induce one outbound probe per
inbound request, defeating the backoff entirely.

**Fetch coalescing.** When multiple local codepaths concurrently need key
material for the same remote server, implementations SHOULD coalesce them into a
single active fetch attempt for that server: at any given time, there SHOULD be
at most one in-flight outbound key-fetch HTTP transaction per remote
`server_name`, and all concurrent waiters for that same fetch SHOULD observe the
same terminal result (success, malformed response rejection, or transport/notary
failure) rather than each spawning its own retry sequence. Once that shared
attempt completes, any later fetch is governed normally by the resulting cache
state and backoff state; coalescing is only a duplicate-suppression rule for
overlapping local demand, not a bypass around the negative-cache policy above.
If that fetch succeeds and the request authenticates, servers SHOULD clear the
backoff state.

Implementations SHOULD allow the minimum backoff floor to be shortened or
otherwise overridden (e.g. via a test-only configuration hook) in test
configurations, so conformance tests do not need to sleep for a full minute in
order to observe backoff being enforced and later cleared.

**Cache persistence.** Key caches SHOULD be persisted to durable storage (e.g.,
database) rather than held only in memory. A server restart should not require
re-fetching every remote server's keys from the network.

**Active key ceiling.** A single server-key response MUST NOT contain more than
50 active keys in `verify_keys`. Such a payload MUST be rejected as malformed.
Large historical key sets belong in `old_verify_keys`.

**Retired key ceiling (per response).** A single server-key response MUST NOT
contain more than 3,000 entries in `old_verify_keys`. Such a payload MUST be
rejected as malformed. This mirrors the 3,000-entry storage ceiling defined
under [Storage considerations](#storage-considerations) so that a conformant
origin can always publish its full retainable retired-key set in one response,
and so that a receiving server can bound parsing and hashing cost before
allocating any database records, independent of the storage-layer eviction rule.

**Notary internal indexing.** Notary servers act as massive aggregation points
for federation keys. To prevent them from becoming distribution vectors for
collisions, notaries MUST also enforce the First Seen Wins rule internally.
However, to preserve a forensic trail of misconfigurations and anomalous event
rejections, notary implementations SHOULD internally index observed key bodies
by their full SHA-256 digest. This allows the notary to safely store historical
collisions without database constraint violations, even if it only serves the
"first seen" key via the active API. Because `/_matrix/key/v2/server` and
`/_matrix/key/v2/query` responses are self-signed by the origin over the entire
payload, a notary MUST NOT satisfy this by locally patching a colliding response
to substitute the first-seen key body: mutating `verify_keys` or
`old_verify_keys` invalidates the origin's `signatures` entry for that payload,
so a patched response fails verification for any downstream client checking the
origin's own signature, regardless of any additional signature the notary itself
appends. Implementations instead satisfy this requirement by declining to update
their served cache entry for that origin when a fetch contains a rejected
collision, continuing to serve the last self-signed payload consistent with the
bindings they actually accepted. This also familiarizes developers with the
inescapable future where key _bodies_ (values as opposed to IDs) become close to
~1 KB (prohibitively large for a "unique identifier" in a relational database).
This forensic index is an implementation-private log of rejected material; it is
not part of the notary's served binding set and is therefore outside the scope
of, and not bounded by, the 3,000-key retention ceiling described under
[Storage considerations](#storage-considerations), which governs only the
bindings a notary actively serves.

**Notary fallback (two-tier binding).** When a required signing key is not
present in the local cache, servers typically query a configured notary server
(`/_matrix/key/v2/query`). Because a notary is a relay, a direct fetch over
validated TLS to the actual server name (`/_matrix/key/v2/server`) provides
strictly stronger cryptographic proof of ownership.

To prevent a malicious or compromised notary from permanently calcifying a
poisoned key binding, bindings first observed via a notary are **provisional**.
They are used normally for verification, but if a subsequent _direct_ fetch from
the origin server yields a key body that conflicts with the provisional binding,
the direct fetch MUST override the provisional one, unless the provisional key
has already expired or been retired. The server updates its cache to the
direct-observed key body and MUST log the collision loudly. The server SHOULD
log which events (or at minimum which rooms/time window) were verified under the
displaced binding, and MAY re-verify recent events. Bindings observed directly
from the origin server are **permanent** (see below). Servers MUST NOT treat
notary unavailability as a verification success. A provisional binding MUST NOT
be overridden if its cached `valid_until_ts` has passed, or if it was learned
from `old_verify_keys` with a past `expired_ts`.

**Binding promotion.** A provisional (notary-observed) binding becomes permanent
the first time a direct fetch from the origin confirms the same key body.
Servers SHOULD attempt a prompt direct fetch after learning any binding via a
notary, to promote the binding and close the provisional window. Once permanent,
the binding is subject to the standard First Seen Wins rule: a later direct
fetch presenting a different key body for the same key ID is a collision and
MUST be rejected and logged. Direct-versus-direct conflicts are always resolved
by First Seen Wins; the two-tier rule applies only to the notary-versus-direct
case. Notary-versus-notary conflicts (or the same notary at two different times)
are also resolved by First Seen Wins among provisional observations. A
freshness-driven re-fetch MUST NOT become a side channel for overriding First
Seen Wins: if a server queries a notary with `minimum_valid_until_ts` to force
an upstream refresh and the notary's re-fetch of the origin yields key material
for the same key ID that conflicts with a binding the notary already holds, the
notary MUST reject the new key as a collision rather than serving it as an
update. Symmetrically, from the querying client's perspective, a notary response
returned to satisfy `minimum_valid_until_ts` is still an ordinary provisional
observation subject to the rules above; requesting fresher validity confers no
override authority over an existing binding.

### Key ID uniqueness requirement

A key ID (`algorithm:key_id`) MUST map to exactly one public key body for a
given remote server. This is a strict, permanent 1:1 binding. The purpose of a
key ID is to provide an unambiguous reference from a signature entry to a
specific cryptographic key; allowing multiple key bodies under the same ID
defeats this purpose.

**Permanent binding.** The cryptographic binding between a key ID and its public
key body is a **permanent record**, not a cache entry. This permanence governs
_key-body identity_ only; it does not alter the validity-window semantics (e.g.,
event signatures are still verified against the key's validity at the event's
`origin_server_ts`, and federation requests still require a currently valid
key). While `valid_until_ts` dictates when a server should refresh the
`/_matrix/key/v2/server` endpoint, the observed association between a key ID and
its key body MUST NOT be purged from the server's key database when
`valid_until_ts` expires. Purging this binding would leave the server naive to
future collisions and blindly accepting colliding key bodies.

**Collision detection.** If a server observes a key response (whether fetched
directly via `/_matrix/key/v2/server` or via a `/_matrix/key/v2/query` notary)
from a remote server where a key ID that was previously associated with public
key `A` is now associated with a different public key `B`, the receiving server:

1. **MUST retain the previously observed key.** The original key body remains
   authoritative for that key ID, unless the existing binding is provisional and
   the new observation is a direct fetch, in which case the two-tier override
   rule applies (see Notary fallback). In all other cases, the conflicting
   response MUST NOT replace it.
2. **SHOULD log the collision.** It helps forensically to log the key ID
   collision at warning level, including the remote server name, the key ID, and
   the SHA-256 fingerprints of both the cached and conflicting public keys. This
   alerts the operator to a potential misconfiguration or compromise on the
   remote server and may aid in community forensic or reconciliation efforts.
3. **MUST NOT perform trial verification.** The server SHOULD NOT cache multiple
   key bodies under the same key ID and MUST NOT attempt extra signature
   verification other than against the single bound key body. Notaries are the
   exception: they may index historical bodies internally for forensics, as
   described above. See [Security considerations](#security-considerations) for
   the vulnerabilities and general annoyances this would introduce.

**Intra-payload rejection.** A single key response payload MUST NOT contain
multiple different public key bodies for the same key ID (e.g., across
`verify_keys` and `old_verify_keys`, or duplicated within the same dictionary).
The same key body appearing under one key ID in both `verify_keys` and
`old_verify_keys` is legal. If a receiving server detects a key ID collision
within a single HTTP response, the entire response MUST be rejected as
malformed.

If a notary rejects an upstream key response as malformed, it MUST omit that
response from the `server_keys` array but MAY continue serving other valid
entries in the batch. Furthermore, implementations MUST reject key response
payloads containing duplicate keys within a single JSON object, at any depth,
anywhere in the response document (not only within `verify_keys` or
`old_verify_keys`). This rejection applies to the raw received bytes before any
canonicalization: the Matrix specification's Canonical JSON appendix defines
canonical form for JSON a server itself produces, but per RFC 8259, JSON
documents received over the wire may legally contain duplicate object members
with implementation-defined (and commonly silently-deduplicating) parser
behavior. A duplicate key ID across `verify_keys` and `old_verify_keys` — or
duplicated within the same dictionary — is exactly this ambiguity, which is why
it must be checked against the raw response rather than assumed already illegal
by the wire format.

**First Seen Wins.** The collision detection rule follows a strict **First Seen
Wins** policy. The first public key body observed for a given
`(server_name, algorithm, key_id)` tuple (whether found in `verify_keys` or
`old_verify_keys`) is the permanent binding. This rule becomes less relevant in
the future, once key IDs are reduced to collision-resistant canonical checksums
of the key body (rather than admin-supplied near arbitrary strings).

**Local impact.** The First Seen Wins rule causes a **localized DAG divergence**
for the misconfigured server: peers that cached the original key will reject new
events from the server (signature verification fails against the wrong key
body), while peers that never cached the original key will accept them. This is
an unavoidable consequence of out-of-band key resolution — different servers
observe different key states at different times. This MSC does not and _cannot_
eliminate this divergence, because key fetching is not part of the room DAG
mainline. What this MSC does is make the divergence **deterministic, documented,
and intentional**: it prioritizes strict cryptographic integrity over silently
corrupting historical verification. While this unavoidably leaves affected peers
with a split-brain view of the room (requiring manual cache eviction or state
resets to recover) if the origin server is not fixed, it creates an immediate,
visible failure that forces the misconfigured administrator to correct their
setup. Eliminating this collateral damage entirely requires a new room version
mandating Content-Addressed Key IDs, which is deferred to a future MSC (see
[Future considerations](#future-considerations)).

### Key rotation procedure

When a server rotates its signing key, the administrator MUST:

1. **Generate a new key with a new, unique key ID.** For example, rotating from
   `ed25519:1` to `ed25519:2`, or from `foobar:old_key_id` to
   `foobar:new_key_id`.
2. **Retire the old key.** The old key MUST appear in the `old_verify_keys`
   section of the `/_matrix/key/v2/server` response with an appropriate
   `expired_ts` timestamp.
3. **Publish the new key.** The new key appears in `verify_keys` with the new
   key ID.

Reusing a key ID with a different key body is a **protocol violation**. This
most commonly occurs when an administrator wipes a server's database,
regenerates signing keys, but leaves the server configuration set to the same
key ID (e.g., the default `ed25519:auto`). This is not a hypothetical: Synapse
defaulted every new installation to the literal key ID `auto` until a 2016 fix
introduced a randomized suffix, and any deployment whose signing key file was
generated before then, or was later restored from a backup or template predating
the fix, still carries it today — a real, non-trivial population of servers in
current federation, not merely an illustrative example.

If this happens, administrators must rotate to a fresh key ID immediately. They
should further take efforts to correct membership or state drifts that occurred
during the period when an invalid, duplicated key was used to sign PDUs.

### Admin startup guardrails

Homeserver implementations SHOULD detect key ID reuse at startup. If the
server's configured signing key has a different key body than what was
previously persisted for that key ID, the server MUST refuse to start and emit a
clear error message instructing the administrator to either restore the original
key or assign a new key ID. This prevents the misconfiguration from propagating
to the federation in the first place. Ideally the server should also check for
pre-existing keys under that ID with its configured notaries (but if they abide
by the paragraph below, this is a largely unnecessary precaution).

Because local startup guardrails cannot detect collisions if the server's
database has been entirely wiped (the most common cause of key ID reuse),
homeserver implementations SHOULD ensure that default key ID generation
incorporates a timestamp or high-entropy component (e.g., `ed25519:a7B_93k`
rather than the default `ed25519:auto` or `ed25519:1`). This ensures that if an
administrator regenerates keys after a total state loss, a novel key ID is
structurally guaranteed. It also protects against a new server owner unwittingly
re-registering under a domain which formerly ran a Conduit server.

This is the most effective mitigation because it eliminates the root cause: it
all but certainly stops the bad key from ever being published and sidesteps the
federation-wide collision detection and localized divergence entirely.

### Recovery from key loss

If a remote server has irrecoverably lost its private signing key (e.g.,
unrecoverable database failure without backup):

1. **The administrator MUST generate a new key with a new key ID.**
2. **If the public key material is still known** (e.g., from backups, logs, or
   cached by peers), the lost key SHOULD be published in `old_verify_keys` with
   `expired_ts` set to the approximate time of loss. Peers that never
   independently observed this key as active will treat the entry as
   **uncorroborated** (see [Storage considerations](#storage-considerations)):
   it is still retained for historical PDU verification, but sits at the bottom
   of the retention order under the eviction ceiling. A peer's binding becomes
   corroborated only through that peer's own prior observation of the key as
   active (for example, if its own notary fallback happened to relay this key
   while it was still genuinely active elsewhere, before the loss), or through
   an explicit local operator action grounded in independently verified evidence
   — never by asking a notary to vouch for the retirement after the fact, which
   no implementation may treat as corroboration.
3. **If the public key material is completely lost**, the administrator must
   accept that historical events signed by the lost key may fail verification on
   servers that never cached it. By design there is no protocol-level recovery
   for this scenario.

The protocol does not provide an automated recovery mechanism for key ID
collisions. Under the current constraints, it is best for the federation to
surface the misconfiguration as a visible failure — forcing the administrator to
discover and fix the error — than to bake dangerous trial verification logic or
other accommodations into homeservers to quietly allow administrative mistakes.

**Manual cache eviction.** Because the First Seen Wins policy permanently binds
a key ID, a successful TOFU poisoning attack (or serious remote
misconfiguration) will result in permanent federation failure with that server.
To allow recovery, homeserver implementations MUST provide an administrative
mechanism (e.g., an admin API or CLI interface) to manually evict cached
key-body bindings for a specific remote server name, allowing a human operator
to break the binding and re-initiate TOFU.

This manual eviction MUST be logged loudly by the homeserver, including both the
server name and the fingerprints of the evicted keys. This is an intentionally
manual, operator-gated ability to perform cache merges or manual overrides. It
must not be automated or triggered via inbound/outbound federation traffic; room
ACLs and other federation-visible mechanisms MUST NOT be able to force eviction
or bypass First Seen Wins. This includes any third-party forensic or attestation
evidence about a key binding, however cryptographically strong — for example, a
cross-server equivocation proof a future proposal might define. Such evidence
remains advisory and MUST NOT automatically trigger eviction, rebinding, or any
other deviation from First Seen Wins on a receiving server. It may inform the
human operator who decides whether to invoke this manual mechanism; it must
never invoke it by itself.

### Historical event verification

Cached keys, including keys retired to `old_verify_keys`, MUST be retained for
historical PDU verification. An event signed by `algorithm:key_id` at time `T`
(where `T` is the event's `origin_server_ts`) is valid if and only if: (1) `T`
falls within the key's validity window (i.e., `T` is less than the key's
`expired_ts` if present, and `T` is less than the `valid_until_ts` asserted when
the key was active), and (2) the event signature cryptographically validates.
The 7-day cache validity clamp restricts the window in which the key is
authorized to sign new events, but does not invalidate historically signed
events when verifying them years later.

Servers MUST sanity-check `expired_ts` values in `old_verify_keys`. A future
`expired_ts` (beyond a 5-minute clock-skew allowance) MUST be treated as
malformed for that specific key entry, but MUST NOT poison the rest of the
response payload. This should be uncommon, but servers must not use the key in
this case.

**`expired_ts` is first-assignment-wins, like the key body it retires.** A key
observed active with no `expired_ts` yet, later republished in `old_verify_keys`
with its first `expired_ts`, is ordinary retirement — this is the expected
lifecycle (see [Cache refresh lifetime](#key-caching-requirements)) and MUST be
applied prospectively without comment. The rule applies only once an
`expired_ts` has actually been observed: that first-observed value is then the
binding's retirement timestamp, permanently, the same way the key body itself is
permanent. A later observation asserting a _different_ `expired_ts` for the same
key ID — earlier **or** later — MUST be logged as suspicious and MUST NOT
replace the first-observed value, for eviction ordering or for any future
verification. Earlier values would retroactively fail already-accepted PDUs,
forcing a state reset over pure metadata with no dispute about the event or the
key's ownership; later values would widen the window a holder of that
compromised retired key can backdate forgeries into (see
[Stolen retired keys and backdated forgeries](#security-considerations)), so
neither direction is benign. This is distinct from the provisional-binding
override above, where a direct fetch replacing a _conflicting key body_ MAY
prompt re-verification of recent events — that path corrects which key was ever
legitimate; this rule instead governs metadata churn on a key body that was
never in question, and requires no per-PDU reliance bookkeeping beyond simply
never re-verifying an already-accepted PDU against a later-observed
`expired_ts`. Because this binding is per-receiver and local, a peer that first
observes the changed `expired_ts` (e.g., one joining or refreshing after the
change) may still reach a different verdict than one that locked in the original
value earlier — the same cross-peer divergence already accepted for key-body
First Seen Wins (see
[Localized DAG divergence is unavoidable](#potential-issues)), just triggered by
validity-window metadata instead of key-body identity.

The strict key ID uniqueness requirement ensures that this lookup is always
unambiguous: for any `(server_name, algorithm, key_id)` tuple, there is at most
one public key body, and its validity window is well-defined. This permanent
binding also acts as a forensic asset post-compromise: you can definitively
prove which specific key body signed what event, and when.

This MSC deliberately does not put collision handling into room version auth
rules. Key ID collisions are local observations from out-of-band HTTP key
fetching, not facts carried by the event JSON. Encoding them into auth rules
would make the split-brain permanent: old peers with the old key cached would
reject, new peers seeing only the replacement key would accept, and both would
believe they were following the room version.

## Potential issues

- **Misconfigured servers will experience local isolation.** An administrator
  who wipes their database and regenerates keys under the same key ID will find
  their server unable to federate with peers that cached the original key. This
  is intentional — the protocol prioritizes correctness and security over
  convenience. The fix is straightforward: change the key ID in the server
  configuration and remediate any membership or state divergences.

- **No automated key ID collision recovery.** Unlike some protocols that provide
  key-reset ceremonies or trusted-third-party recovery, Matrix provides no
  automated mechanism, since it conflicts with the zero-trust federation model.

- **Permanent key-body storage.** The permanent binding requirement means
  servers must retain key-body records indefinitely, proportional to the number
  of remote servers encountered. For a typical homeserver federating with a few
  thousand servers, this is negligible (a few megabytes of public key material).

- **Two-tier binding and the TOFU window.** Allowing a direct fetch to override
  a provisional notary binding means an attacker who can serve a direct
  `/_matrix/key/v2/server` response (IP hijack, DNS spoofing) can displace a
  notary-learned key. While this extends the window of vulnerability beyond the
  initial TOFU race, requiring servers to attempt a prompt direct fetch upon
  learning a notary binding bounds this window. The override primarily removes
  the ability of a compromised _notary_ to permanently calcify a poisoned
  binding. Security limitations or concerns here hint at the need for follow-up
  work (e.g., a Global Settings Lock).

- **Localized DAG divergence is unavoidable.** The First Seen Wins rule means
  that peers with different cache histories may disagree on events from a
  misconfigured server. This is an inherent property of out-of-band key
  resolution and cannot be solved at the protocol level. This MSC makes the
  behavior deterministic rather than implementation-dependent, which is an
  improvement over the status quo. A solution to this concern is deferred to
  content-addressable keys or to Member Keys; see
  [Future considerations](#future-considerations).

## Alternatives

- **Trial verification (try all cached keys for a key ID).** Explicitly
  rejected. Trial verification introduces a CPU-exhaustion DoS vector, breaks
  historical DAG verification (which key was active when?), needlessly
  complicates the spec and homeserver requirements, while violating the
  cryptographic identity contract implicitly specified by the key ID.

- **Soft failure on key ID collision (warn but accept the new key).** This
  silently breaks historical verification. Events signed under the old key body
  would fail verification using the new key, corrupting state resolution for any
  room involving the affected host and any other pre-MSC4499 server.

- **Key ID collision resolution via notary consensus.** Peers could query
  multiple notary servers and accept the key body attested by a majority. This
  introduces a trusted-third-party assumption that Matrix's federation model
  explicitly avoids. Notary servers may themselves have stale caches,
  complicating efforts at gossip or consensus.

## Security considerations

- **CPU-exhaustion.** The strict "1:1 key ID to key body mapping" eliminates the
  trial verification attack vector. Signature verification is performed against
  exactly one key per key ID, bounding the computational cost of event
  verification.

- **TOFU cache poisoning.** Under Matrix's Trust-On-First-Use model, a
  `/_matrix/key/v2/server` response is self-signed by the private key associated
  with the payload. An attacker who briefly hijacks a server's IP (DNS spoofing,
  BGP hijacking) can generate a new keypair and re-publish it under the target's
  key ID — with valid self-signature. The First Seen Wins policy protects
  against this: if the legitimate key was cached first, the attacker's key is
  rejected as a collision. If the attacker's key is cached first (the server was
  never contacted before), TOFU provides no protection regardless of this MSC —
  an inherent limitation of TOFU, not a flaw in the proposal. Currently
  mitigating this is an admin effort.

- **Bounded key revocation lag (inherited limitation).** Matrix key resolution
  is strictly pull-based; an origin server cannot push a rotation or an
  emergency revocation to the federation. Because this MSC requires servers to
  rely on their local cache and not probe the network while a cached key remains
  within its `valid_until_ts`, worst-case revocation propagation is bounded only
  by the 7-day ceiling inherited from the base Server-Server specification (see
  [Cache refresh lifetime](#key-caching-requirements)), not by how quickly the
  origin publishes the fix. A peer that fetched an origin's keys shortly before
  a compromise will continue to trust the compromised key for up to 7 days from
  that peer's own last fetch — counted from when _that peer_ last checked, not
  from when the origin rotated or discovery occurred — before its cache
  naturally expires and forces a re-fetch. This MSC accepts that ceiling as a
  deliberate trade-off rather than tightening it: mandating faster mandatory
  refreshes would trade this lag for federation-wide fetch storms, and this
  MSC's own negative-caching and backoff requirements exist precisely to bound
  that opposite failure mode (see
  [Negative caching and backoff](#key-caching-requirements)). An operator who
  learns of a compromise out-of-band before the 7-day window naturally lapses
  can use the operator-gated [manual cache eviction](#recovery-from-key-loss)
  mechanism to clear the stale binding immediately; the next signature check
  against that key_id then triggers a fresh fetch, rather than the eviction
  itself reaching out to the origin.

- **Origin spoofing.** While allowing direct fetches to override provisional
  notary-learned keys prevents notary-enforced lock-in, it temporarily exposes
  the server to DNS/BGP spoofing on direct connections. This is an acceptable
  TOFU trade-off because (1) direct connections use WebPKI TLS certificate
  validation (bringing in standard internet-grade security), (2) the window of
  vulnerability is bounded to the brief provisional period before the server
  performs a confirming direct fetch, and (3) future MSCs such as a Global
  Settings Lock would effectively mitigate this concern.

- **DAG integrity.** The key ID uniqueness requirement protects abiding servers
  by guaranteeing that historical signature verification is locally
  deterministic. For any event at any point in time, the key that signed it is
  unambiguously identified by the `(server_name, algorithm, key_id)` tuple in
  the `signatures` dictionary.

- **Compromise monitoring.** Key ID collisions are a potential indicator of
  server compromise (an attacker generating a new key and attempting to publish
  it under an existing ID). Hard rejection with operator alerting provides an
  early warning mechanism. They can also be a sign of outdated, legacy servers.

- **Stolen retired keys and backdated forgeries.** Enforcing `expired_ts` stops
  an attacker with a compromised retired key from signing current events. It
  does not prevent them from backdating `origin_server_ts` to forge historical
  events, though the reach of such events is limited by `prev_events` and depth.

- **Cache expiration is not binding expiration.** The `valid_until_ts` field
  governs when to _refresh_ the key endpoint, not when to _forget_ the key body.
  Servers that purge key-body bindings on `valid_until_ts` expiry create a
  window where collision detection is blind. This MSC explicitly requires
  permanent retention of key-body bindings to close this gap.

### Storage considerations

Mandating indefinite storage of key-body bindings introduces a theoretical
storage exhaustion vector if an attacker forces a server to fetch and
permanently store millions of unique key IDs. Homeserver implementations MUST
enforce a maximum limit of 3,000 retired key IDs (`old_verify_keys` entries) per
remote server name, matching the per-response ceiling above; a server's current
`verify_keys` (bounded separately at 50) are exempt from and not counted against
this ceiling, so the two 3,000-entry limits always refer to the same retired-key
capacity. If a remote server reaches this quota, receiving servers MUST NOT
ignore new key IDs permanently. Instead, they MUST evict retired keys according
to the deterministic ordering defined below — not by recency or
least-recently-used heuristics, which would make eviction
implementation-dependent rather than the deterministic behavior this MSC
requires. Keys currently published in the `verify_keys` section of a direct
fetch MUST always be prioritized and exempt from eviction. This exemption is
bounded by the 50-key active ceiling on any single response
([Key caching requirements](#key-caching-requirements)); it is not a license for
a `verify_keys` set to grow without bound across many legitimate rotations over
time. A remote server whose cumulative set of currently-active key IDs, observed
across successive responses, grows far beyond the single-digit counts typical of
legitimate operation is itself the signal described as "unambiguously hostile"
below, independent of whether any individual response stays under the 50-key
cap.

**Corroboration tier.** This tier answers a narrower question than the
provisional/permanent split above. It does not decide which key body is correct
— First Seen Wins already settles that, permanently, regardless of
corroboration. It only decides which permanently-retained retired-key bindings
get deleted first if the 3,000-entry ceiling above is ever reached. That
question matters because `old_verify_keys` entries are plain claims inside a
self-signed response — the origin asserts "this key used to be active," but
nothing separately signed by the retired key itself backs that claim up, making
retired-key claims cheaper to fabricate in bulk than current `verify_keys`
entries (see [Other considerations](#other-considerations)).

Before applying the eviction ordering below, implementations MUST sort retired
bindings into two tiers:

- **Corroborated:** the receiving server itself independently observed that
  `(server_name, algorithm, key_id)` as a currently-published `verify_keys`
  entry in some prior response — via a direct fetch, or via a notary relaying
  the origin's genuinely-active state at that earlier time — before this
  retirement claim arrived. A local operator may also mark a binding
  corroborated based on independently verified historical evidence.
- **Uncorroborated:** everything else — a retired-key entry that arrives
  already-retired, with no independent record anywhere that the key was ever
  genuinely active.

Corroboration MUST be grounded only in the receiver's own accumulated
observation history or explicit operator action, never in a live attestation
solicited at retirement time: a notary MUST NOT be queried at retirement time to
simply vouch that it once saw a key active. Without the "grounded in a prior,
organic observation" requirement, nothing would stop a single compromised or
colluding notary from making that claim, on demand, about any key for any
domain, turning one bad notary into a universal corroboration-forging oracle and
defeating this tier's purpose entirely. This corroboration path requires nothing
beyond the plain self-signed response data every implementation already relies
on for First Seen Wins — the same baseline `/_matrix/key/v2/server` and
`/_matrix/key/v2/query` self-signature this MSC assumes throughout — and it MUST
NOT be strengthened, weakened, or otherwise gated by any advisory provenance
signal a future proposal might define (for example, TLS transcript evidence or a
notary-side anti-spam proof artifact): such signals are advisory-only wherever
they are defined, and this MSC has no dependency on them.

Uncorroborated bindings MUST still be accepted and retained for historical PDU
verification — rejecting them outright would break legitimate first-contact
backfill (a server that joins federation late and has never talked to an origin
before its most recent rotation) and the lost-key recovery case in
[Recovery from key loss](#recovery-from-key-loss), where a peer may legitimately
be the first to ever see a given historical key. Corroboration changes exactly
one thing — eviction order under the ceiling — and nothing else: an
uncorroborated binding is accepted the same way, stored the same way, and blocks
a later conflicting key body under First Seen Wins exactly as permanently as a
corroborated one does.

If MSC00E4 `trusted_notary_keys` is present, a listed full content-addressed key
identifier permits a notary to return the corresponding retained historical key
body without the origin embedding that body in `old_verify_keys`. This does not
create a new corroboration source by itself. The receiver still recomputes the
returned key body's full content-addressed `key_id`, verifies any required
proof-of-work, signatures, and expiry claims, and then sorts the binding into
the corroborated or uncorroborated tier using the same local observation-history
rules above. A notary-supplied body whose recomputed full ID does not exactly
match the origin-signed `trusted_notary_keys` entry MUST be rejected.

Implementations MUST apply this ceiling deterministically: always retain all
current `verify_keys`; then retain corroborated retired keys in descending order
of an _effective retirement timestamp_ (defined below); then, in whatever slots
remain, retain uncorroborated retired keys under the same ordering. When a new
valid retired-key binding is learned while the local retired-key set for that
remote server is already at the 3,000-entry ceiling, implementations MUST
recompute the full retained set by applying this ordering across the union of
the previously retained retired keys and the newly learned candidate. If the new
candidate sorts above the retention floor, it MUST be stored and whichever
existing binding now falls below the floor MUST be evicted; if the new candidate
sorts below the floor, the implementation MUST discard that new candidate
instead. Hitting the storage ceiling therefore MUST degrade into this
deterministic prune-and-retain behavior, not into fetch failure, not into
dropping all newly learned historical bindings unconditionally, and not into
eviction of currently-active `verify_keys`. Uncorroborated bindings are
therefore always evicted before any corroborated binding, regardless of their
respective `expired_ts` values. For a key published in `old_verify_keys`, the
effective retirement timestamp is its `expired_ts`. For a key that was
previously observed active (in `verify_keys` or `old_verify_keys`) but has since
disappeared from the origin's responses without ever being given an `expired_ts`
(a lazy or misbehaving origin simply dropping it), the effective retirement
timestamp is the local timestamp of the last observation in which the key was
still present. This makes every retained-or-evictable binding sortable,
including vanished keys that never received a formal retirement. Ties in the
effective retirement timestamp are broken by bytewise lexicographic comparison
of the full `algorithm:key_id` string as UTF-8, ascending; the lexicographically
smaller identifier is retained first. Any keys ordered below the retention floor
by this rule may be evicted. Eviction of a _corroborated_ binding SHOULD be
logged at warning level: reaching the ceiling deeply enough to displace
corroborated history is itself the anomaly signal for the flood scenario in
[Other considerations](#other-considerations), and costs nothing beyond the
logging this MSC already requires elsewhere for collisions. Because both the
corroboration tier (which may rely on local observation history) and the
effective retirement timestamp for vanished keys are local determinations rather
than origin-asserted values, this part of the ordering is local to each
implementation; this is consistent with, and does not strengthen, the
cross-server convergence limits described below. When new valid historical key
material is learned, notaries and receiving servers MAY re-evaluate the retained
retired-key set — including re-evaluating corroboration as new observations
arrive — but such re-evaluation MUST apply the same deterministic pruning rule
over the full locally known candidate set. This improves eventual convergence
after observation gaps or network partitions, but does not guarantee identical
real-time results across notaries. Implementations MUST rely on existing
federation rate-limiting to discard junk traffic before allocating database
records. In practice, legitimate servers publish single-digit numbers of active
keys at any given time; a server claiming tens of thousands of key IDs is
unambiguously hostile. A future Proof-of-Work gated proposal may mitigate the
spurious bulk generation of keys behind Equihash or Cuckoo Cycle.

### Other considerations

- **Eviction reopens a TOFU window on the permanent-binding guarantee —
  mitigated by the corroboration tier.** `expired_ts` is asserted by the origin
  server itself, and `old_verify_keys` entries are plain historical claims
  within that self-signed response — they are not separately signed by the
  retired key they describe. A malicious or compromised origin could therefore
  attempt to fabricate synthetic retired-key entries to flood a peer's
  3,000-entry quota and push a legitimate historical key binding below the
  retention floor. The corroboration tier above closes the one-shot version of
  this: a freshly fabricated `key_id` that this receiver never independently
  observed as active — through its own direct fetches or its own past
  notary-relayed fetches — lands in the uncorroborated tier, where it can only
  evict other uncorroborated entries; it cannot push out a corroborated,
  legitimately-retired binding. Because corroboration is deliberately never
  grantable by asking a notary to vouch after the fact (see
  [Storage considerations](#storage-considerations)), a compromised notary
  cannot shortcut this either. To evict a corroborated target, the attacker must
  first get up to 3,000 fabricated `key_id`s independently observed as genuinely
  active. This is bounded only by the existing 50-key active-key ceiling per
  response and round-trip latency: a compromised or rogue origin can force up to
  50 newly-corroborated `key_ids` per burst simply by rotating its `verify_keys`
  and getting the receiver to authenticate against each new key ID in turn (e.g.
  via signed federation traffic), accumulating the 3,000 entries needed in as
  little as tens of such bursts — on the order of minutes to hours. This MSC
  accepts that timeline rather than mandating a dedicated rate limiter to slow
  it: because the 3,000-entry ceiling is enforced per remote `server_name`, this
  flood can only accelerate eviction of that _same_ origin's own historical
  retired-key bindings on a given receiver — it cannot be used to evict a
  different domain's history. The prerequisite remains control of the origin's
  current signing capability — as the legitimate operator gone rogue, or via a
  full compromise — the same prerequisite as TOFU cache poisoning above, not the
  narrower "possession of one historical private key" scenario that stolen
  retired keys describes. An attacker who already controls an origin's current
  signing capability gains only the ability to erase that same origin's own
  historical record faster; this MSC treats that self-scoped outcome as the
  accepted residual risk, and leaves any rate-limiting of novel key-ID discovery
  to individual implementations to apply at their own discretion.

- **The provisional-binding freeze is a deliberate trade, not an oversight.** A
  provisional binding that has expired or been retired MUST NOT be overridden by
  a later direct fetch (see Notary fallback). This is intentional: a direct
  fetch cannot attest anything about a key the origin no longer serves, and
  allowing post-expiry rewrites would let an attacker rewrite historical
  verification after the fact. The consequence is that a notary-poisoned binding
  that expires or is retired before any direct confirmation is frozen in that
  poisoned state permanently, recoverable only through the manual eviction
  mechanism described under [Recovery from key loss](#recovery-from-key-loss).
  This MSC accepts that trade — auditability of historical verification over
  automated self-healing — as the safer default.

## Implementation and rollout notes

Because strict collision rejection can break federation with misconfigured
servers already in the wild, implementations SHOULD ship an initial
collision-observation phase: log detected collisions as warnings without
rejecting the new key, gather real-world breakage data, then enable strict
enforcement. A configuration flag (e.g., `org.matrix.msc4499_strict_caching`) is
the obvious gate. This is rollout guidance only; the normative rules above are
unchanged by it.

## Unstable prefix

This MSC does not introduce new protocol identifiers and does not require an
unstable prefix. The behavior changes (mandatory caching, permanent key-body
binding, collision detection, trial verification prohibition) are implementation
requirements that can be readily adopted. No API endpoints substantially change.

## Dependencies

- None. This MSC is independent of other proposals. It applies to `ed25519` keys
  today. It will apply equally to future server-signing algorithms if accepted
  into the spec and if this document is not superseded by a refined or more
  encompassing MSC.

## Open questions

- How should moderation tooling (community ban lists, Draunir) treat a server
  locally isolated by a key collision — as a temporary outage, or as a signal
  worth surfacing to operators?

## Backwards compatibility

This proposal introduces no wire-format changes, but does add stricter
receiver-side validation:

- **No protocol wire changes.** No new fields, endpoints, or response formats.
- **Stricter receiver-side validation.** The active-key ceiling, retired-key
  ceiling, and raw-byte duplicate-key rejection mean a payload a pre-MSC
  receiver would have silently tolerated (or handled ambiguously) is now a
  MUST-reject; no conformant, well-behaved origin produces such a payload today.
- **No room version changes.** No changes in auth or state resolution rules.
- **Existing well-configured servers are unaffected.** Servers that already use
  unique key IDs on rotation (the newly-defined behavior) experience no change.
- **Misconfigured servers experience a clarified failure mode.** Servers that
  reuse key IDs with different key bodies will be rejected by peers implementing
  this MSC. This failure already occurs unpredictably today (depending on cache
  state and timing); this MSC makes the behavior expected and codified.

## Future considerations

### Content-addressed key IDs (stricter protocol requirements)

The root cause of key ID collisions is that the `key_id` is currently an
arbitrary, administrator-defined string (e.g., `ed25519:auto`). A future room
version could eliminate this entire class of vulnerabilities by mandating that
the `key_id` must be deterministically derived from the public key body
itself—for example, `ed25519:<base64(SHA256(KeyBody))[:16]>`.

Under this paradigm, a key ID collision becomes exceedingly difficult. If an
administrator regenerates their keys, the new key body structurally enforces a
novel key ID. This entirely mitigates the TOFU poisoning vulnerability (an
attacker cannot assert a new key under an old ID without conducting a
computationally intractable search). It would eliminate the need for out-of-band
collision detection heuristics, allowing us to enforce strict key uniqueness
directly within room version auth rules.

Because this requires changing how PDU signatures are verified and supplants
legacy key formats thoroughly entrenched in the wild, it requires a new room
version and is deferred to a future MSC. Until then, protection must remain
strictly at the local server caching layer as outlined in this proposal.

### Member Keys [MSC4430]

The Member Keys proposal caps these concerns to a future room version by moving
the key body in-band (and reducing the complications inherent in today's
out-of-band notary model, while freeing up notary capacity to serve future
functions such as aiding in EDU reconciliation or corroborating correct room
state accumulation for a given epoch).

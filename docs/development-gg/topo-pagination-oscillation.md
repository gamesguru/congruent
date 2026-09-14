# Topological Pagination: The Fix/Break Oscillation

Post-mortem of the backward pagination regression cycle on `dev`, July 2026.
Traces ~10 commits over 2 weeks where fixing one mechanism kept breaking the other.

## The Two Safety Mechanisms

The topo index is keyed as `[shortroomid:8][depth:8][pducount:8]`.
Backward pagination needs two things to work correctly:

| Mechanism        | Purpose                                                                        |
| ---------------- | ------------------------------------------------------------------------------ |
| **Seek depth**   | Where the RocksDB reverse iterator _starts_ in the depth dimension             |
| **Stream guard** | Filter that stops the iterator from returning events that shouldn't be visible |

Every regression in this cycle can be traced to **removing or misconfiguring
one of these two mechanisms**.

## The Cycle

### Phase 0: Working State (pre-`1c4673212`)

- **Depth source**: `local_topological_depth` — computed incrementally from
  `max(prev_events.depth) + 1`
- **Stream guard**: `.ready_try_take_while(move |(count, _)| Ok(*count <= until))`
  — monotonicity guard preventing reverse iterator wraparound

**Status: NPO passes, MOF passes — 50+ CI runs, zero failures.**

---

### Juncture 1: `1c4673212` — "use global depth for topo index" — BROKE IT

**Added:**

- Use `pdu.depth()` (federation global depth) for topo key encoding
- Sign-bit flipping for PduCount so backfilled counts sort correctly
- `deprecated_local_topo_depth` field rename

**Removed:**

- `local_topological_depth` computation (the `max(prev.depth) + 1` loop)
- Monotonicity guard in `topo_pdus_rev` (the `ready_try_take_while`)
- Monotonicity guard in `topo_pdus` (forward direction too)
- `depth_cache` parameter from `prepend_backfill_pdu_batch`

**What broke:**
NPO 100%, MOF 100%. The reverse iterator scans without any guard, and global
federation depths land seek positions in wrong places.

**Lesson**: You cannot remove BOTH the depth computation AND the stream guard
simultaneously. They were compensating for each other. The monotonicity guard
was specifically there to handle iterator wraparound caused by depth encoding.

---

### Juncture 2: `c7885232e` — "resolve infinite pagination loops" — PARTIAL FIX

**Added:**

- `pdu_id_to_depth()` helper (replaces `pdu_id_to_topo_key()`)
- Exact depth lookup: "We MUST NOT use the depth of a nearby 1D count, as
  it might be higher"
- Safe fallback: `if dir == Forward { u64::MAX } else { 0 }`

**Removed:**

- The old 1D nearest-neighbor stream scanning fallback

**What it fixed:**
Infinite pagination loops caused by seeking to wrong depths.

**What it did NOT fix:**
The stream guard was still missing. Relied entirely on accurate seek positions.

**Lesson**: Fixing the seek without restoring the guard works... until you
encounter a DAG fork where the seek depth is inherently ambiguous.

---

### Juncture 3: `7ffebce75` — "account for DAG forks in seek depth" — OVER-CORRECTION

**Added:**

```rust
let current_depth = self.pdu_id_to_depth(current).await.unwrap_or(token_depth);
let target_depth = match dir {
    Direction::Backward => token_depth.max(current_depth),
    Direction::Forward => token_depth.min(current_depth),
};
```

**What it fixed:**
Missing events from high-depth parallel branches during backward pagination.

**What it broke (subtly):**
`max(token_depth, current_depth)` _inflates_ the seek position above where the
previous page ended. This causes duplicate events at page boundaries — the
start of page N+1 overlaps with events already returned on page N.

**Lesson**: `max()` is a blunt instrument. It captures more events but violates
the invariant that page N+1 starts exactly where page N ended. The correct
approach is to seek high (like `u64::MAX`) but _filter_ the stream.

---

### Juncture 4: `250e12817` — "remove depth inflation" — RE-BROKE BRANCHES

**Added:**

- Reverted to exact `token_depth` (no inflation)
- Regression test `backward_seek_uses_exact_token_depth_no_inflation()`
- Comment citing Synapse's SQL: `WHERE (topo < ?) OR (topo = ? AND stream < ?)`

**Removed:**

- The `max(token_depth, current_depth)` logic
- The entire `current_depth` lookup and nearest-neighbor fallback

**What it fixed:**
Duplicate events at page boundaries.

**What it re-broke:**
By using exact `token_depth`, events on high-depth parallel branches (the
partition scenario) are invisible again. NPO starts failing.

**Lesson**: This is the fundamental tension. Exact depth → no duplicates but
misses branches. Inflated depth → captures branches but causes duplicates. The
resolution MUST be: **seek high, filter the stream**.

---

### Juncture 5: `2edda0ce5` — "u64::MAX depth for backward seek" — HALF FIX

**Added:**

```rust
let seek_depth = match dir {
    Direction::Backward => u64::MAX,
    Direction::Forward => token_depth,
};
```

Plus first-page detection: `if pages.is_empty() && start_from.is_some() { u64::MAX }`

**What it fixed:**
By seeking from `u64::MAX`, backward pagination captures events at ALL depths
including remote partition branches.

**What was still missing:**
No stream guard. Events that arrived AFTER the sync position (high depth, high
pdu_count) are incorrectly included in backward results.

---

### Juncture 6: `6d202e76a` — "add stream count filter" — WRONG COMBINATION

**Added:**

- Stream count filter to exclude post-sync events

**Removed:**

- The `u64::MAX` seek depth! Reverted back to `token_depth`

**What broke:**
Went backward to exact `token_depth`, re-losing the partition branch coverage.
The filter was correct in concept but the seek was wrong again.

**Lesson**: You need BOTH. Adding one while removing the other just shifts the
bug.

---

### Juncture 7: `ae7aae516` — "u64::MAX seek + stream count filter" — FIXED

**Added:**

- `u64::MAX` seek depth (restored)
- `u64::MAX` first-page seek (restored)
- Stream count ceiling filter in `topo_pdus_rev`:
    ```rust
    .ready_try_filter_map(move |item| {
        if item.0.pdu_count <= count_ceiling {
            Ok(Some(item))
        } else {
            Ok(None)
        }
    })
    ```
- Changed filter boundary from `> ceil` to `>= ceil` (exclusive)

**CI result: 628 pass / 113 fail / 0 new failures — 9 runs, all matrix entries.
NPO and MOF fixed.**

---

## The Pattern

```
Phase 0: local_depth + monotonicity guard              → ✅ works
   ↓ 1c4673212: removed BOTH
Phase 1: global_depth, no guard                        → ❌ NPO/MOF 100% fail
   ↓ c7885232e: fixed seek
Phase 2: exact depth, no guard                         → 🟡 fragile
   ↓ 7ffebce75: max() inflation
Phase 3: inflated depth, no guard                      → ❌ duplicates
   ↓ 250e12817: exact depth again
Phase 4: exact depth, no guard                         → ❌ misses branches
   ↓ 2edda0ce5: u64::MAX seek
Phase 5: MAX seek, no guard                            → ❌ post-sync leaks
   ↓ 6d202e76a: added guard, removed MAX
Phase 6: exact depth + guard                           → ❌ misses branches
   ↓ ae7aae516: MAX + guard
Phase 7: MAX seek + guard                              → ✅ fixed
```

## Core Takeaway

The backward pagination correctness invariant requires **two orthogonal
mechanisms**:

| #   | Mechanism              | Responsibility                                              | Analogy                 |
| --- | ---------------------- | ----------------------------------------------------------- | ----------------------- |
| 1   | **Seek = `u64::MAX`**  | Cast the widest net — start from the highest possible depth | "Open the aperture"     |
| 2   | **Stream count guard** | Filter out events that arrived after the sync position      | "Apply the lens filter" |

Every single regression came from having only one of the two:

- **Seek too low, no guard** → misses partition branches (NPO fails)
- **Seek high, no guard** → includes post-sync events (ordering wrong)
- **Seek too low, with guard** → guard correct but never sees events to filter
- **Seek high, with guard** → ✅ correct

The original `local_topological_depth` accidentally avoided this problem because
locally-computed depths were always close to the correct seek position.
Switching to global federation depth made the depth dimension unreliable, which
means the seek MUST be `u64::MAX` and the guard MUST compensate.

## Risk Note

The original monotonicity guard (`*count <= until`) compared PduCounts.
The new guard (`item.0.pdu_count <= count_ceiling`) compares unsigned pdu_count
values. These are semantically equivalent only if the offset-binary encoding is
correctly handled. This is the remaining area to watch.

## Related

- `topo_depth_fix.md` — the original design doc for the depth problem
- `src/service/rooms/timeline/data.rs` — all changes live here

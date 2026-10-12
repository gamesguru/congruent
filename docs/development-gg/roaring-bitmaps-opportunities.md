# Roaring Bitmap Optimization Opportunities

This document outlines high-frequency, CPU-intensive areas in the Conduwuit codebase where utilizing Roaring Bitmaps (`roaring::RoaringBitmap` or `roaring::RoaringTreemap`) could provide significant performance improvements in both execution time and memory usage.

## 1. Auth Chains (`auth_chain/mod.rs`)

**Current State**:
Auth chains are manipulated and cached as `Vec<ShortEventId>` and built recursively using `HashSet<ShortEventId>`. During the recursive walk in `get_auth_chain_outer` and `get_auth_chain`, the code merges chains by pushing them into a `Vec`, then calling `.sort_unstable()` and `.dedup()`.

**The Opportunity**:
Auth chains are essentially massive sets of clustered integers (`ShortEventId` is a `u64`). If we replace the `HashSet` and `Vec` logic with `roaring::RoaringTreemap` (the 64-bit extension of `RoaringBitmap`), we unlock the following benefits:

- **SIMD Merging**: Merging chunked auth chains becomes a native bitwise OR (`chain_a | chain_b`), completely eliminating the CPU overhead of sorting and deduping vectors in Rust.
- **Massive Memory Savings**: The `shorteventid_authchain` cache footprint would shrink drastically compared to storing arrays of raw 64-bit integers, as Roaring Bitmaps compress contiguous sequences (which are highly common in DAGs) into run-length encodings.

## 2. State Cache & Room Memberships (`state_cache/mod.rs`)

**Current State**:
The `state_cache` operates on `OwnedUserId` strings. Determining if a user is joined, or computing the intersection of two rooms (e.g., to find shared users for presence or profile sharing checks), involves iterating and hashing string IDs.

**The Opportunity**:
If we introduce a `ShortUserId` type (analogous to `ShortEventId` and `ShortRoomId`), room memberships could be represented internally as a single Roaring Bitmap per room.

- **Fast Intersection**: Checking if two users share a room, or calculating room intersection for federation queries, becomes a single, ultra-fast bitwise `AND` between two bitmaps. Synapse heavily utilizes this pattern for state groups and memberships.

## 3. State Compressor Diffs (`state_compressor/mod.rs`)

**Current State**:
The `CompressedState` type is currently defined as a `BTreeSet<[u8; 16]>` (a concatenation of `ShortStateKey` and `ShortEventId`). When computing the diff between two states (e.g., when a state resolution or new event occurs), the code calls `.difference()` on the `BTreeSet`.

**The Opportunity**:
While `BTreeSet` is decent, if the state compressor could track state keys and event IDs in a way that maps neatly to integer sets (perhaps by assigning a unique integer ID to every valid `(ShortStateKey, ShortEventId)` pair), computing added and removed state between thousands of events would become an instantaneous bitwise `XOR` or `AND NOT` operation.

## 4. Read Receipts (`read_receipt/mod.rs`)

**Current State**:
`readreceipts_since` streams over the `readreceiptid_readreceipt` RocksDB index. If a room has heavy read receipt churn, iterating over the DB tree can become a latency bottleneck.

**The Opportunity**:
If we maintained a Roaring Bitmap of `PduCount` integers that currently _have_ active read receipts, `readreceipts_since(since)` could just execute `bitmap.remove_range(0..since)` in memory and yield exactly the subset of PDU Counts that we need to fetch. This would completely bypass the need for RocksDB index scanning.
_Note: Because read receipts are often dense and queried sequentially via bounds (`since`), a standard integer sequence or a single watermark `PduCount` is usually fine. A bitmap here should only be introduced if RocksDB iteration profiling shows it as a genuine bottleneck in large rooms._

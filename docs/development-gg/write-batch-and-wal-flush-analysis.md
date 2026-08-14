# WriteBatch vs. WriteBatchWithIndex and WAL Flush Mechanics

## Context
We evaluated merging the `force_state_inner` persistence phases into a single atomic batch to save 1-2 WAL flushes per event on the hot path (outlier demotion / DAG ingestion). The goal was to thread `Option<&mut Batch>` to unify the writes.

## The Read-After-Write Hazard
Our database writes currently rely on `WriteBatch`. A plain `WriteBatch` is a write-only buffer; it does not merge unapplied writes into subsequent reads (e.g., `db.get()`).

If we defer the PDU insertion into a shared batch, the immediate downstream read (`get_pdu_in_room`) in `force_state_insert_locked` will 404. Fixing this requires injecting a custom in-memory `self-event` cache bypass into the most mathematically unforgiving path in the server (DAG demotion).

## Why WriteBatchWithIndex Isn't a Silver Bullet
RocksDB offers `WriteBatchWithIndex` to support read-your-own-batch semantics via `get_from_batch_and_db`. However, this is not a drop-in fix:
* It requires explicitly telling every read operation which batch to consult.
* `get_pdu_in_room` (and anything else `force_state_inner` reads) would need an `Option<&Batch>`-aware variant plumbed through the entire call stack.
* The plumbing complexity is identical to the in-memory bypass, just shifted to the read layer.

## WAL Flush Reality Check (write vs. fsync)
The assumed performance penalty of the redundant flushes was based on the idea that every flush forces a mechanical HDD seek. This is incorrect for our hot path:
* `Engine::flush()` calls `flush_wal(false)`.
* `flush_wal(false)` pushes the WAL buffer to the OS via a `write()` syscall. It **does not** force a real `fsync`.
* Only `Engine::sync()` (`flush_wal(true)`) forces a hardware sync/seek.

Because the hot path is already non-syncing, the `corked()` mechanism is saving redundant `write()` syscalls, not disk seeks. The actual I/O penalty is significantly smaller than "HDD thrashing" implies.

## Conclusion
**Deferred.** The risk of introducing a read-after-write caching bug into the hardened outlier-demotion path vastly outweighs the benefit of saving 1-2 `write()` syscalls per event. The current architecture (where `append_pdu_batch` applies immediately, ensuring safe subsequent reads) remains the optimal trade-off between atomicity, code simplicity, and performance.

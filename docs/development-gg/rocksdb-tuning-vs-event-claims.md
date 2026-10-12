# RocksDB Tuning vs. Event Claims

This note exists to separate two very different statements that tend to get
blurred together in casual discussion:

1. **Real room deletion support**
   Tuwunel has explicit admin and internal deletion paths for rooms. Deleting a
   room purges local membership, aliases, directory publication, PDUs,
   forward extremities, receipts, search tokens, relation indexes, and the
   internal short room ID.

2. **Generic database tuning**
   The performance and storage wins come from RocksDB configuration, not from a
   special event-layer compression format. The relevant knobs are:
    - Zstd / other SST compression
    - bottommost compression
    - WAL compression
    - direct I/O
    - compaction and background thread tuning
    - cache sizing and block-table layout

## What "highly compressed" actually means

When someone says the database is "extremely compressed," the concrete meaning
is:

- RocksDB SST files can be compressed per column family.
- The default build enables `zstd_compression`, so Zstd is the intended
  database compression path.
- Bottommost SST levels can be compressed again for older data.
- WALs can also be compressed.

That is all database-file compression. It is not an event-specific storage
feature, and it does not imply that events themselves are stored in some special
format beyond normal RocksDB key/value persistence.

## What it does not mean

- It does not mean event payloads have a bespoke compression pipeline.
- It does not mean room deletion is a compression feature.
- It does not mean filesystem compression is beneficial; on some filesystems it
  can actively hurt performance by defeating Direct I/O.

## Relevant code paths

- [Room deletion and purge flow](src/admin/room/commands.rs)
- [Database-wide RocksDB options](src/database/engine/db_opts.rs)
- [Per-column compression and table options](src/database/engine/cf_opts.rs)
- [RocksDB maintenance notes](docs/maintenance.mdx)

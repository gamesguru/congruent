# RocksDB Directory Lock Design

## Problem Statement

RocksDB natively uses a `LOCK` file inside the database directory to ensure that only a single process can write to the database at any given time. However, this relies on standard OS advisory locking (e.g., `flock` on Unix-like systems).

Because advisory locks are tied to the file descriptor and the underlying inode rather than the filename itself, this mechanism is easily bypassed if the `LOCK` file is manually deleted (`rm DB/LOCK`) while the original process is still running. If a second process starts, it will create a _new_ `LOCK` file with a _new_ inode. Since the first process holds a lock on the old (unlinked) inode, the second process will successfully acquire a lock on the new file, leading to catastrophic and permanent data corruption as both processes write to the database simultaneously.

## Objective

We want to prevent multiple instances of conduwuit from opening the RocksDB database and corrupting data, even if the RocksDB `LOCK` file is maliciously or accidentally removed.

## Proposed Solution

Instead of relying solely on the RocksDB `LOCK` file, conduwuit should acquire an exclusive, OS-level lock on either the **database directory itself** or an **adjacent lock file** located _outside_ the database directory (e.g., `conduwuit.lock`).

### Implementation Details

1. **Dependency:**
   Add a cross-platform file locking crate (such as `fs2` or `fd-lock`) to `Cargo.toml`, or utilize the existing `nix` dependency to call `flock` directly if targeting only Unix-like systems.

2. **Lock Acquisition:**
   In `src/database/engine/open.rs`, right before the call to `Db::open_cf_descriptors`, open the database path directory (or the adjacent lock file) using standard `std::fs::File`.

    Apply a non-blocking, exclusive lock to this file descriptor.

    ```rust
    // Example using fs2
    use fs2::FileExt;

    let lock_file = std::fs::File::open(&config.database_path)?;
    if let Err(e) = lock_file.try_lock_exclusive() {
        // If locking fails (e.g., WouldBlock), another instance is running.
        // Log a critical error and abort startup.
    }
    ```

3. **Lock Retention:**
   Add a field (e.g., `_dir_lock: std::fs::File`) to the `Engine` struct in `src/database/engine.rs`.

    This ensures that the locked file descriptor is kept alive for the entire lifetime of the `Engine` and, by extension, the conduwuit process. The OS will automatically release the lock when the file descriptor is closed upon shutdown or process crash.

## Verification

To test that the lock functions correctly:

1. Start an instance of conduwuit.
2. Manually delete the `LOCK` file inside the database directory (`rm -f path/to/db/LOCK`).
3. Attempt to start a second instance of conduwuit pointing to the exact same database.
4. The second instance should fail to start immediately, reporting that the database directory is locked.

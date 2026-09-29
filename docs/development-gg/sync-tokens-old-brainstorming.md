# Ginger's Kill-Sync-Tokens Work

This document tracks the ongoing work by Ginger in the `kill-sync-tokens` branch and identify synergistic opportunities.

## Integrated Improvements

The following items from Ginger's branch have been integrated into `main`/`guru/sync-tokens-2nd-try`:

1.  **Panic Fixes for Missing SSH**:
    - Gracefully handle cases where a Short State Hash (SSH) is missing during sync (specifically sliding sync).
    - Prevents server panics when encountering events with missing or reset state.
    - Commits: `624bd3796`, `bf4c716c7`.

2.  **Improved `last_sync_end` Calculation**:
    - Implemented a more accurate forward-scanning approach to determine the state at the end of the last sync.
    - Uses the PDU immediately following the last sync point to ensure precision.
    - Commit: `4facaa444`.

## Documentation of Future Items

The following items are planned for future integration once the `kill-sync-tokens` branch is stabilized:

1.  **Type-Safe MSC4222 Support**:
    - Integrates `use_state_after` directly into the `SyncContext` and the type system.
    - Eliminates the need for manual JSON patching of `state_after`.
    - Ensures better long-term maintainability and compatibility with Ruma.
    - Commit: `af8e28559`.

2.  **Removing the Sync Token Table**:
    - The ultimate goal: entirely remove the `roomsynctoken_shortstatehash` database table.
    - Moves the server to a "stateless" sync token model, relying exclusively on timeline lookups.
    - Completely eliminates database bloat and OOM risks associated with the large sync token table.
    - Commit: `6a2480774`.

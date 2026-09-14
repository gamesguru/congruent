# Federated State Resolution Case Study

Room: `!sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE` (unredacted.org general)

## Background

State divergence was observed between `unredacted.org` (continuwuity) and
`matrix.org` (Synapse) for 8 disputed membership events. Both servers run
Matrix State Resolution V2 but disagree on the winning event for the same
state keys.

## Methodology

1. Dumped full room DAG from both servers via `yolo get-remote-dag`
2. Fed the unredacted.org DAG (56,418 events) into
   [ruma-lean](https://github.com/gamesguru/ruma-lean), an independent V2
   state-res implementation with no shared code with either server
3. Compared ruma-lean's resolved state against each server's disputed events

## Results

| Server         | Wins | Out of |
| -------------- | ---- | ------ |
| unredacted.org | 7    | 8      |
| matrix.org     | 1    | 8      |

**Conclusion**: unredacted.org resolves state correctly for 7/8 disputed
events. matrix.org's incorrect results are caused by missing auth chain
events — the same deterministic algorithm produces different outputs when
given incomplete inputs.

## Root Cause

State Resolution V2 is **deterministic but input-dependent**. Both servers
run the same algorithm, but matrix.org has an incomplete auth chain for
these users, causing it to pick older/incorrect membership events. This is
not a bug in either server's state-res implementation — it is a consequence
of federated DAG fragmentation where not all servers receive all events.

## Ed25519 Point Decompression Interop Issue

During `compare-room-state` operations, some events from remote servers
fail signature verification with:

```
Cannot decompress Edwards point
```

### Cause

- **Synapse** uses PyNaCl (wrapping libsodium), which accepts non-canonical
  ed25519 point encodings
- **Continuwuity** uses `ed25519-dalek` (Rust), which strictly rejects
  points that are not in canonical compressed form

Some origin servers publish ed25519 public keys with technically-malformed
but functional encodings. These keys pass libsodium's lenient parser but
fail dalek's strict decompression.

### Impact

Events signed by these servers are:

- ✅ Accepted by Synapse-based homeservers (matrix.org, unredacted.org, etc.)
- ❌ Rejected by continuwuity and other strict implementations
- ❌ Skipped during `compare-room-state` verification

This can cause minor state divergence where continuwuity is missing events
that Synapse servers accepted. The affected events are typically membership
events from small/misconfigured homeservers.

### Spec Gap

The Matrix specification mandates ed25519 for signing but does not specify
encoding strictness for public keys. This allows interop differences between
crypto libraries. A spec clarification on canonical encoding requirements
would resolve this class of issue.

## Tools

- `verify.sh` — Runs ruma-lean V2 state-res on raw DAG dumps and checks
  disputed events between unredacted.org and matrix.org
- `sort_jsonl.py` — Sorts JSONL DAG dumps by `origin_server_ts` for
  consistent processing

## Files

All data files are gitignored. To reproduce, fetch DAGs with:

```bash
yolo get-remote-dag !sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE unredacted.org --limit -1
yolo get-remote-dag !sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE matrix.org --limit -1
```

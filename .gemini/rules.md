# Agent Rules for Continuwuity

## Behavioral Rules

1. **When the user says "tell me" a command — TELL them, don't run it.**
2. **Never push code without explicit user approval.**
3. **Never claim a fix works until complement data confirms it.** Check the actual CI results, don't speculate.
4. **Don't propose background heuristics or "healer" patterns.** The user has explicitly rejected this approach.
5. **When analyzing test regressions, check the FULL historical complement data first** — don't just look at the last few commits.
6. **When the user reports complement test failures, follow this SOP:**
    1. Run `just ci-query-failures baseline=ec5844630 like=dev` to get the current failure list
    2. Find the complement workflow run: `gh run list -w "Complement Tests" --limit 5 --json databaseId,headBranch,status,conclusion --jq '...'`
    3. Download the log artifact: `gh run download <run-id> -n complement-logs-<arch>-<os>-v<room_version> -D .complement-logs/`
    4. Extract the failure output: `jq -r 'select(.Test == "<TestName>" and .Action == "output") | .Output' .complement-logs/test_logs.jsonl`
    5. Filter for assertions: `grep -i "MatchResponse\|did not see\|FAIL\|Error\|expected\|got\|assert"`
    6. **Then** look at the relevant server code based on what the assertion actually says — don't guess.
7. **Always prioritize shared, modular, and reusable code.** Do not write ugly 200-line custom-built monolithic functions; aggressively extract shared logic into `utils` or service methods so it can be reused across components.

## CI & Testing

### Complement Tests (External CI)

- Complement tests run in GitHub Actions (workflow: `complement.yml`), NOT a separate system.
- Results are ingested into a PostgreSQL database after each run.
- Query complement results:
    ```bash
    just ci-query-failures baseline=<commit> like=<branch-pattern>
    ```
- Example:
    ```bash
    just ci-query-failures baseline=ec5844630 like=dev
    ```

### Downloading and Analyzing Complement Logs

- Complement test logs are uploaded as GitHub Actions artifacts.
- Artifact naming pattern: `complement-logs-{arch}-{os}-v{room_version}`
    - Examples: `complement-logs-amd64-ubuntu-24.04-v12`, `complement-logs-arm64-ubuntu-24.04-v11`
- **IMPORTANT**: The `gh run view <id> --log-failed` command shows the GH Actions _job_ logs (build output), NOT the complement test output. Always download the artifact instead.

#### Step 1: Find the complement workflow run ID

```bash
gh run list -w "Complement Tests" --limit 5 --json databaseId,headBranch,status,conclusion \
  --jq '.[] | "\(.databaseId) \(.status) \(.conclusion) \(.headBranch)"'
```

#### Step 2: Download the log artifact

```bash
gh run download <run-id> -n complement-logs-amd64-ubuntu-24.04-v12 -D .complement-logs/
```

This downloads two files:

- `test_results.jsonl` — one JSON line per test with `Action: "pass"|"fail"|"skip"`
- `test_logs.jsonl` — one JSON line per output line with `Test`, `Action: "output"`, and `Output` fields

#### Step 3: List all failing tests

```bash
jq -r 'select(.Action == "fail") | .Test' .complement-logs/test_results.jsonl
```

#### Step 4: Extract the failure output for a specific test

```bash
jq -r 'select(.Test == "<TestName>" and .Action == "output") | .Output' \
  .complement-logs/test_logs.jsonl
```

#### Step 5: Filter for just the assertion / error lines

```bash
jq -r 'select(.Test == "<TestName>" and .Action == "output") | .Output' \
  .complement-logs/test_logs.jsonl \
  | grep -i "MatchResponse\|did not see\|FAIL\|Error\|expected\|got\|assert"
```

### GitHub Actions CI

- List recent CI runs:
    ```bash
    make download/list
    ```
- Download CI binary artifact:
    ```bash
    make download RUN=<run-id>
    ```
- Download by commit hash:
    ```bash
    make download/hash HASH=<short-hash>
    ```
- View failed GH Actions logs:
    ```bash
    gh run view <run-id> --log-failed
    ```

### Running Complement Locally

- Build for complement:
    ```bash
    make complement/build complement/docker
    ```
- Requires Docker daemon running.

## Build & Lint

- Format and lint:
    ```bash
    make format lint
    ```
- **NEVER run plain `cargo fmt`.** It uses stable rustfmt which mass-reformats 196+ files and breaks the build. Use ONLY:
    - `make format` (preferred)
    - `cargo +nightly fmt` (manual equivalent)
- The project uses `pre-commit` hooks.
- Custom macros like `err!()` do NOT support tracing-style `%field` syntax — use `warn!()` or `tracing::warn!()` for structured logging.

## Architecture Notes

### Room Versions

- Room versions ≥ v4: `event_id` is computed from content hash, NOT present in wire JSON.
- Room versions v1–v3: `event_id` is an explicit field in the PDU JSON.
- The `Pdu` struct requires `event_id` for deserialization. Use `Pdu::from_id_val()` which injects `event_id` before deserializing. Direct `serde_json::from_value` on raw federation PDUs will fail for v4+ rooms.

### State Resolution

- V2.1 (MSC4297) state resolution is implemented in this codebase.
- Large public rooms (e.g., Matrix HQ) can have 60k+ auth chains and 15k+ conflicted sets — this is expected, not a bug.
- `state_res_ignore_rejected` and `state_res_ignore_soft_failed` config flags can reduce auth chain sizes.

### Federation

- `parse_incoming_pdu` in `src/service/rooms/event_handler/parse_incoming_pdu.rs` handles both v4+ (computed event_id) and pre-v4 (explicit event_id) PDUs.
- The `handle_incoming_pdu` pipeline is the main federation ingestion path.
- Room-level federation locks are held via `mutex_federation`.

## Repository

- GitHub org/repo for `gh` commands: resolved automatically by `gh` from git remotes.
- Main development branch under test: `guru/dev-2026-03-27+b1-presence+b2-federation`
- Complement baseline commit: `ec5844630`

## Test Matrix Targets

Use to determine the currently deployed revision. Update it in the `deployments.csv` if out of date.

- `testmatrix nutra.tk`
- `testmatrix wombatx.me`

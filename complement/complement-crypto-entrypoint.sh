#!/usr/bin/env bash
# Complement-Crypto test runner entrypoint.
#
# Runs the complement-crypto `go test` suite (JS SDK flavour) against a
# homeserver image supplied either as the first argument or via
# $COMPLEMENT_BASE_IMAGE. Output is normalised into the same jsonl layout that
# continuwuity's `bin/complement` produces for the regular complement suite
# (results.jsonl + full_output.jsonl), so the same CI result-parsing tooling
# applies.
set -euo pipefail

COMPLEMENT_BASE_IMAGE="${1:-${COMPLEMENT_BASE_IMAGE:?COMPLEMENT_BASE_IMAGE not set}}"
export COMPLEMENT_BASE_IMAGE

# Defaults mirror the upstream complement-crypto "single SDK" CI run.
COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX="${COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX:-jj}"
export COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX
COMPLEMENT_ENABLE_DIRTY_RUNS="${COMPLEMENT_ENABLE_DIRTY_RUNS:-1}"
export COMPLEMENT_ENABLE_DIRTY_RUNS
TESTCONTAINERS_RYUK_DISABLED="${TESTCONTAINERS_RYUK_DISABLED:-true}"
export TESTCONTAINERS_RYUK_DISABLED

# Skip-list of tests known to be flaky/nondeterministic against continuwuity,
# or that need features continuwuity does not (yet) implement. Overridable.
DEFAULT_SKIP='TestOnRejoinBobCanSeeButNotDecryptHistoryInPublicRoom'
SKIP="${COMPLEMENT_CRYPTO_SKIP:-$DEFAULT_SKIP}"

# Which tests to run (regex). Defaults to everything.
RUN="${COMPLEMENT_CRYPTO_RUN:-.*}"

# Parallelism / timeout.
PARALLEL="${COMPLEMENT_CRYPTO_PARALLEL:-2}"
TIMEOUT="${COMPLEMENT_CRYPTO_TIMEOUT:-30m}"

# The source checkout. `go test -json` is run from here; each package runs from
# its own package directory so the relative `./mitmproxy_addons` path in
# tests/main_test.go resolves correctly.
SRC="${COMPLEMENT_CRYPTO_SRC:-/usr/src/complement-crypto}"
cd "$SRC" || { echo "cannot cd to $SRC"; exit 1; }

echo "=== Complement-Crypto (JS SDK) against $COMPLEMENT_BASE_IMAGE ==="
echo "source=$SRC matrix=$COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX run=$RUN skip=$SKIP"

jq_res='{Action: .Action, Test: .Test, Package: .Package, Elapsed: .Elapsed}'
jq_sel='select((.Action == "pass" or .Action == "fail" or .Action == "skip") and .Test != null)'
jq_tab='[.Action, .Test] | @tsv'

# Results are written to the directory CI bind-mounts (mirrors the layout used
# by the regular complement suite). Overridable via env.
RESULTS_DIR="${COMPLEMENT_CRYPTO_RESULTS_DIR:-/var/lib/complement-crypto}"
mkdir -p "$RESULTS_DIR"

go test \
    -tags=jssdk \
    -json \
    -shuffle=1337 \
    -parallel="$PARALLEL" \
    -timeout="$TIMEOUT" \
    -count=1 \
    -skip="$SKIP" \
    -run="$RUN" \
    ./tests ./tests/js \
    | tee "$RESULTS_DIR/full_output.jsonl" \
    | jq -c "$jq_sel | $jq_res" \
    | tee "$RESULTS_DIR/results.jsonl" \
    | jq -r "$jq_tab"

# Fail the run (non-zero) if any test failed. `set -o pipefail` above would
# already catch a failing `go test`, but keep an explicit check so the summary
# also detects failures via the parsed jsonl.
if grep -q '"Action":"fail"' "$RESULTS_DIR/results.jsonl" 2>/dev/null; then
    echo "!!! some complement-crypto tests FAILED !!!" >&2
    exit 1
fi
exit 0

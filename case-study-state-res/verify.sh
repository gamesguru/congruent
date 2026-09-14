#!/usr/bin/env bash
# State Resolution Verification: unredacted.org vs matrix.org
# Room: !sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE
#
# Uses rezzy's independent state-res output (state-res.json) to determine
# which server resolved state correctly for disputed membership events.
#   cargo install rezzy --features cli

set -euo pipefail
cd "$(dirname "$0")"

STATE_RES="state-res-unredacted-dag.json"

# Generate state-res output from raw DAG
rezzy -i ./remote-dag-sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE-v12-unredacted.org-formatted.jsonl | tee "$STATE_RES"

echo "=== State Resolution Verification ==="
echo "Room: !sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE"
echo "Resolver: rezzy (independent V2 state-res)"
echo

# Disputed events — same state_key, different event_id on each server
echo "--- Disputed events only on matrix.org ---"
MATRIX_IDS=(
  '$AJsK9SExNlblHbfse7eDhSNISk9E871gJzbkqoTA9Ds'
  '$Hk-xXbs52DhNQI_Ca1E2DkyNMazBITKkepo8IuqC7EI'
  '$TtQ6QYSjCphiJuzNiwfINI-ylQQTkBSkWaMydae_nCc'
  '$YlZG-G6Ak3fdjf4TIHEA8oD7C_FHX8EwmwFYL6jXNtg'
  '$heDtrL6Z-AVUZkzEsqtIKLxIQpzhMwcEU4JZ1bRyXSE'
  '$kUBfA5z53UYwkouV54Wq_UgK_8vnszbTp8gflvF3qns'
  '$mK__qhCzbLBUyb4IjkIxXKQpmdBwr8vxWwd40sXn1U4'
  '$rmb6V2Nb_UScP9htYUTPOy9LhbWgxb5wxgMEIfj8aFM'
)

matrix_wins=0
for id in "${MATRIX_IDS[@]}"; do
  if rg -qF "$id" "$STATE_RES"; then
    echo "  ✓ $id (in resolved state)"
    matrix_wins=$((matrix_wins + 1))
  else
    echo "  ✗ $id"
  fi
done

echo
echo "--- Disputed events only on unredacted.org ---"
UNREDACTED_IDS=(
  '$0-Rwh5ycT6Hwr9jkoiSsOSKW7HK_xiSrNyCvzh2Whcs'
  '$EhAnh9S3GYGd3tHSsoVhZAGbQt9fPgV_ketRNIQDc0s'
  '$TN3aSG4dg-NueYfa8FNgOg154yVJlB_g102cf5eQiFY'
  '$CITU5ramZfoRbG5NuEBd_kMm6f9a1UJB5TKRhMpVT6E'
  '$DT2PAjF5OtuocQGMV_ekKgN68M6XaYYsO2TGQPGEZ_c'
  '$4sXgVhE2a85_i94Ul_TvfwKVfpjIHUQKWcuzdw0W8as'
  '$xqrfEc0vwvpDFN4laAkpvtniqlv1oV7kb-RfdT7mXCI'
  '$x49Eu0L3xnLbMJ1sAJIk8wtj0moDiZyjya_rNh3U2UQ'
)

unredacted_wins=0
for id in "${UNREDACTED_IDS[@]}"; do
  if rg -qF "$id" "$STATE_RES"; then
    echo "  ✓ $id (in resolved state)"
    unredacted_wins=$((unredacted_wins + 1))
  else
    echo "  ✗ $id"
  fi
done

echo
echo "=== Result ==="
echo "matrix.org winners:     $matrix_wins / ${#MATRIX_IDS[@]}"
echo "unredacted.org winners: $unredacted_wins / ${#UNREDACTED_IDS[@]}"
echo

if [ "$unredacted_wins" -gt "$matrix_wins" ]; then
  echo "✓ unredacted.org has the correct state resolution."
elif [ "$matrix_wins" -gt "$unredacted_wins" ]; then
  echo "✓ matrix.org has the correct state resolution."
else
  echo "⚠ Tie — inconclusive."
fi

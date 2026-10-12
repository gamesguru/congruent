#!/usr/bin/env bash
# Check the mtxdb-export fixtures against their SHA256SUMS manifest.
#
# Continuwuity's exporter tests and the mtxdb CLI importer tests each keep a
# copy of these files (the exporter writes them, the importer reads them). The
# manifest is identical in both repos, so a copy that drifts fails here.
#
# usage: check-export-fixtures.sh [PEER]
#   PEER  a peer checkout directory, or an http(s) URL of the peer's
#         SHA256SUMS; when given (or set in EXPORT_FIXTURES_PEER), the two
#         manifests must match byte for byte.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
dir=$(find "$root" -type d -path '*/tests/fixtures/mtxdb-export' -not -path '*/target/*' | head -n 1)
[ -n "$dir" ] || { echo "no tests/fixtures/mtxdb-export under $root" >&2; exit 1; }

(cd "$dir" && sha256sum --check --strict SHA256SUMS)

peer=${1:-${EXPORT_FIXTURES_PEER:-}}
[ -n "$peer" ] || { echo "no peer given; skipping cross-repo comparison"; exit 0; }

case "$peer" in
  http://*|https://*) other=$(curl --fail --silent --show-error --location "$peer") ;;
  *) other=$(cat "$(find "$peer" -path '*/tests/fixtures/mtxdb-export/SHA256SUMS' -not -path '*/target/*' | head -n 1)") ;;
esac
if [ "$other" != "$(cat "$dir/SHA256SUMS")" ]; then
  echo "export fixtures differ from the peer's; regenerate and copy both files and SHA256SUMS" >&2
  diff <(cat "$dir/SHA256SUMS") <(printf '%s\n' "$other") >&2 || true
  exit 1
fi
echo "export fixtures match the peer"

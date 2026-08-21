#!/usr/bin/env bash
set -euo pipefail

# import_history.sh (Simplified Bulk Ingest - Fresh Rebuild)
# Ingests historical JSONL data directly into PostgreSQL with a clean slate.

LEDGER_DIR=${1:-}
TEMP_DIR=""
DB_TARGET=${DATABASE_URL:-c10y}

if [ -z "$LEDGER_DIR" ]; then
	TEMP_DIR=$(mktemp -d -t c10y-import-XXXXXX)
	# The _metadata/badges branch is on GitHub
	REPO_URL="https://github.com/gamesguru/continuwuity.git"
	echo "✓ No ledger directory provided. Cloning _metadata/badges branch from $REPO_URL..."
	if ! git clone --depth 1 --branch _metadata/badges "$REPO_URL" "$TEMP_DIR" >/dev/null 2>&1; then
		echo "❌ Failed to clone _metadata/badges from $REPO_URL"
		exit 1
	fi
	LEDGER_DIR="$TEMP_DIR"
fi

# Ensure tables and views exist (Fresh Schema)
SQL_FILE="$(dirname "$0")/tables.sql"
if [ -f "$SQL_FILE" ]; then
	echo "✓ Applying fresh schema from $SQL_FILE..."
	psql "$DB_TARGET" -c "DROP TABLE IF EXISTS run_details, runs, ever_passed CASCADE;" >/dev/null
	psql "$DB_TARGET" -f "$SQL_FILE" >/dev/null
fi

psql_local() {
	psql "${DATABASE_URL:-$DB_TARGET}"
}

PSQL_SINK=psql_local
source "$(dirname "$0")/sync_recent.sh"

echo "✓ Starting bulk historical JSON import into '$DB_TARGET'..."

# 1. Bulk Ingest Run Summaries
echo "→ Ingesting run summaries..."
psql "$DB_TARGET" <<EOF
BEGIN;
SET LOCAL synchronous_commit = OFF;
CREATE TEMP TABLE b (j jsonb);
\copy b FROM '$LEDGER_DIR/runs.jsonl' csv quote e'\x01' delimiter e'\x02';

INSERT INTO runs (run_date, commit_hash, upstream_commit, branch, author_name, actor, provider, arch, os, version_string, features, profile, binary_sha256, n_pass, n_skip, n_fail, room_version)
SELECT
    (j->>'run_date')::timestamptz, (j->>'commit_hash'), (j->>'upstream_commit'), (j->>'branch'),
    (j->>'author_name'), (j->>'actor'), (j->>'provider'), NULLIF(j->>'arch', ''), NULLIF(j->>'os', ''),
    (j->>'version_string'), COALESCE(regexp_replace(btrim(j->>'features', ' ,'), '[,\s]+', ' ', 'g'), ''), NULLIF(j->>'profile', ''), (j->>'binary_sha256'),
    (j->'passed_count')::int, (j->'skipped_count')::int, (j->'failed_count')::int, COALESCE(NULLIF(j->>'room_version', ''), '11')
FROM b
ON CONFLICT (commit_hash, arch, os, profile, room_version, features) DO NOTHING;
COMMIT;
EOF

# 2. Bulk Ingest Test Details (Injecting metadata from summaries / filenames)
echo "→ Consolidating and ingesting test details..."
(
	jq -r --arg sep "$MANIFEST_SEP" '[.commit_hash, (.arch // ""), (.os // ""), (.profile // ""), ((.room_version // "") | if length == 0 then "11" else . end), ((.features // "") | gsub("[,\\s]+"; " ") | gsub("^ | $"; ""))] | join($sep)' \
		"$LEDGER_DIR/runs.jsonl" |
	while IFS="$MANIFEST_SEP" read -r COMMIT ARCH OS PROFILE ROOM_VERSION FEATURES; do
		SAFE_ARCH=${ARCH//[!a-zA-Z0-9._-]/_}
		SAFE_OS=${OS//[!a-zA-Z0-9._-]/_}
		SAFE_PROFILE=${PROFILE//[!a-zA-Z0-9._-]/_}
		SAFE_ROOM_VERSION=${ROOM_VERSION//[!a-zA-Z0-9._-]/_}
		FILE="$LEDGER_DIR/runs_data/${COMMIT}-${SAFE_ARCH}-${SAFE_OS}-${SAFE_PROFILE}-${SAFE_ROOM_VERSION}.jsonl"

		if [ ! -f "$FILE" ]; then
			LEGACY_FILE="$LEDGER_DIR/runs_data/${COMMIT}-${SAFE_ARCH}-${SAFE_OS}-${SAFE_PROFILE}.jsonl"
			if [ -f "$LEGACY_FILE" ]; then
				FILE="$LEGACY_FILE"
			else
				FILE="$LEDGER_DIR/runs_data/${COMMIT}.jsonl"
			fi
		fi

		[ -f "$FILE" ] || continue
		jq -c --arg h "$COMMIT" --arg a "$ARCH" --arg o "$OS" --arg p "$PROFILE" --arg rv "$ROOM_VERSION" --arg f "$FEATURES" \
			'. + {commit: (if ((.commit // "") | length) > 0 then .commit else $h end),
             arch: (if ((.arch // "") | length) > 0 then .arch else $a end),
             os: (if ((.os // "") | length) > 0 then .os else $o end),
             profile: (if ((.profile // "") | length) > 0 then .profile else $p end),
             room_version: (if ((.room_version // "") | length) > 0 then .room_version else $rv end),
             features: (if ((.features // "") | length) > 0 then (.features | gsub("[,\\s]+"; " ") | gsub("^ | $"; "")) else $f end)}' "$FILE"
	done
) | ingest_details
[ -n "$TEMP_DIR" ] && rm -rf "$TEMP_DIR"
echo "✓ Bulk import complete."

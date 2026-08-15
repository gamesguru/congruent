#!/usr/bin/env bash
set -euo pipefail

# sync_recent.sh
# Single ingest path for CI run data -> PostgreSQL. Two modes:
#   sync_recent.sh [LIMIT]                  bulk: catch up the last LIMIT runs from the orphan branch
#   sync_recent.sh --direct FILE META_JSON  direct: ingest one local run immediately, no git fetch
#
# Both modes share the same run_details/ever_passed upsert logic below, so there is exactly
# one place that knows how a run gets written -- avoids the two write paths drifting (e.g.
# computing run_date independently) and silently duplicating "runs" rows.
#
# import_history.sh also sources this file (for psql_remote/PSQL_SINK/ingest_details) to
# reuse the same ingest logic for its full-rebuild path, just pointed at a direct local
# connection instead of the SSH-tunneled one used here.

TARGET_BRANCH=${TARGET_BRANCH:-"_metadata/badges"}
export DB_TARGET=${DATABASE_URL:-"c10y"}
SSH_TARGET=${SSH_TARGET:-"git@nutra.tk"}

psql_remote() {
	ssh -C -o StrictHostKeyChecking=no -o ServerAliveInterval=30 "$SSH_TARGET" "psql -U git c10y"
}

# Escapes a value for safe use inside a single-quoted SQL string literal (doubles embedded
# single quotes, per the SQL standard). Required anywhere external data -- e.g. a git branch
# name, which is attacker-influenced on any push to the repo -- is interpolated into SQL text
# piped over the SSH/stdin transport below, since that transport carries plain SQL text rather
# than parameterized psql variables.
sql_quote() {
	printf '%s' "$1" | sed "s/'/''/g"
}

# Command (function or binary name) that ingest_details() pipes assembled SQL into.
# Defaults to the SSH-tunneled remote psql; import_history.sh overrides this to a direct
# local connection before sourcing this file.
PSQL_SINK=${PSQL_SINK:-psql_remote}
MANIFEST_SEP=$'\x1f'

# Reads NDJSON on stdin (each line already tagged with commit/arch/os/profile/room_version/
# features), streams it into a temp table, then upserts run_details + ever_passed for exactly
# the run rows those lines belong to.
ingest_details() {
	(
		echo "BEGIN;"
		echo "SET LOCAL synchronous_commit = OFF;"
		echo "CREATE TEMP TABLE t (j jsonb);"
		printf '%s\n' "\copy t FROM STDIN csv quote e'\x01' delimiter e'\x02';"
		cat
		echo "\."
		cat "$(dirname "${BASH_SOURCE[0]}")/ingest_details.sql"
		echo "COMMIT;"
	) | "$PSQL_SINK"
}

# Everything below only runs when this file is executed directly, not when sourced
# (import_history.sh sources it just for the ingest_details() function above).
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
	return 0
fi

if [[ "${1:-}" == "--direct" ]]; then
	RESULTS_FILE=$2
	RUN_META=$3

	COMMIT=$(jq -r '.commit_hash' <<<"$RUN_META")
	BRANCH=$(jq -r '.branch // ""' <<<"$RUN_META")
	ARCH=$(jq -r '.arch // ""' <<<"$RUN_META")
	OS=$(jq -r '.os // ""' <<<"$RUN_META")
	PROFILE=$(jq -r '.profile // ""' <<<"$RUN_META")
	ROOM_VERSION=$(jq -r '.room_version // ""' <<<"$RUN_META")
	VERSION=$(jq -r '.version_string // ""' <<<"$RUN_META")
	FEATURES=$(jq -r '.features // ""' <<<"$RUN_META")
	PASS=$(jq -r '.pass // 0' <<<"$RUN_META")
	FAIL=$(jq -r '.fail // 0' <<<"$RUN_META")
	SKIP=$(jq -r '.skip // 0' <<<"$RUN_META")
	RUN_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

	# pass/fail/skip must be plain integers -- reject anything else rather than splice it
	# into SQL unquoted (they come from workflow step outputs, but treat them as untrusted).
	[[ "$PASS" =~ ^[0-9]+$ ]] || PASS=0
	[[ "$FAIL" =~ ^[0-9]+$ ]] || FAIL=0
	[[ "$SKIP" =~ ^[0-9]+$ ]] || SKIP=0
	ROOM_VERSION=${ROOM_VERSION:-11}

	if [[ -z "$COMMIT" || ! -f "$RESULTS_FILE" ]]; then
		echo "⚠ Direct ingest skipped: missing commit_hash or results_file"
		exit 1
	fi

	echo "→ Direct ingest for $COMMIT ($ARCH/$OS/v$ROOM_VERSION)..."
	(
		echo "BEGIN;"
		echo "SET LOCAL synchronous_commit = OFF;"
		# sql_quote() only doubles single quotes, so force standard string semantics for
		# this transaction before interpolating any external values into string literals.
		echo "SET LOCAL standard_conforming_strings = on;"
		echo "INSERT INTO runs (run_date, commit_hash, branch, arch, os, profile, n_pass, n_skip, n_fail, room_version, features, version_string)
        SELECT '$(sql_quote "$RUN_DATE")'::timestamptz, '$(sql_quote "$COMMIT")', '$(sql_quote "$BRANCH")', NULLIF('$(sql_quote "$ARCH")', ''), NULLIF('$(sql_quote "$OS")', ''), NULLIF('$(sql_quote "$PROFILE")', ''), ${PASS}, ${SKIP}, ${FAIL},
          COALESCE(NULLIF('$(sql_quote "$ROOM_VERSION")', ''), '11'),
          COALESCE(regexp_replace(btrim('$(sql_quote "$FEATURES")', ' ,'), '[,\s]+', ' ', 'g'), ''), '$(sql_quote "$VERSION")'
        ON CONFLICT (commit_hash, arch, os, profile, room_version, features) DO NOTHING;"
		echo "COMMIT;"
	) | psql_remote

	jq -c --arg c "$COMMIT" --arg a "$ARCH" --arg o "$OS" --arg p "$PROFILE" --arg rv "$ROOM_VERSION" --arg f "$FEATURES" \
		'. + {commit: $c, arch: $a, os: $o, profile: $p, room_version: $rv, features: ($f | gsub("[,\\s]+"; " ") | gsub("^ | $"; ""))}' "$RESULTS_FILE" |
		ingest_details
	echo "✓ Direct ingest complete."
	exit 0
fi

LIMIT=${1:-100}

echo "→ Fetching latest metadata from origin/$TARGET_BRANCH..."
git fetch origin "$TARGET_BRANCH" --depth 1 --filter=blob:none >/dev/null 2>&1

# Ingest Recent Summaries (fast — ON CONFLICT DO NOTHING skips existing)
echo "→ Streaming last $LIMIT run summaries..."
(
	echo "BEGIN;"
	echo "SET LOCAL synchronous_commit = OFF;"
	echo "CREATE TEMP TABLE b (j jsonb);"
	printf '%s\n' "\copy b FROM STDIN csv quote e'\x01' delimiter e'\x02';"
	git show "FETCH_HEAD:runs.jsonl" | tail -n "$LIMIT"
	echo "\."
	echo "INSERT INTO runs (run_date, commit_hash, upstream_commit, branch, author_name, actor, provider, arch, os, version_string, features, profile, binary_sha256, n_pass, n_skip, n_fail, room_version)
        SELECT
          (j->>'run_date')::timestamptz, (j->>'commit_hash'), (j->>'upstream_commit'), (j->>'branch'),
          (j->>'author_name'), (j->>'actor'), (j->>'provider'), NULLIF(j->>'arch', ''), NULLIF(j->>'os', ''),
          (j->>'version_string'), COALESCE(btrim(regexp_replace(j->>'features', '[,\s]+', ' ', 'g'), ' ,'), ''), NULLIF(j->>'profile', ''), (j->>'binary_sha256'),
          (j->'passed_count')::int, (j->'skipped_count')::int, (j->'failed_count')::int, COALESCE(NULLIF(j->>'room_version', ''), '11')
        FROM b ON CONFLICT (commit_hash, arch, os, profile, room_version, features) DO NOTHING;"
	echo "COMMIT;"
) | psql_remote

# Pre-cache git tree for fast existence checks
ALL_FILES=$(git ls-tree -r FETCH_HEAD:runs_data --name-only || true)

# Build manifest with single jq pass (instead of 5× jq calls per line)
TMPMANIFEST=$(mktemp)
trap 'rm -f "$TMPMANIFEST"' EXIT
git show "FETCH_HEAD:runs.jsonl" | tail -n "$LIMIT" |
	jq -r --arg sep "$MANIFEST_SEP" '[.commit_hash, (.arch // ""), (.os // ""), (.profile // ""), ((.room_version // "") | if length > 0 then . else "11" end), ((.features // "") | gsub("[,\\s]+"; " ") | gsub("^ | $"; ""))] | join($sep)' \
		>"$TMPMANIFEST"

# Find which of these exact $LIMIT runs already have details (to skip re-ingesting).
# Scoped to the commit hashes we're actually loading, not an id-range guess -- id order
# doesn't track runs.jsonl's chronological order (e.g. after a full historical rebuild).
echo "→ Checking which of the loaded runs already have details..."
EXISTING_KEYS=$(
	(
		echo "CREATE TEMP TABLE m (commit_hash text);"
		printf '%s\n' "\copy m FROM STDIN csv quote e'\x01' delimiter e'\x02';"
		while IFS="$MANIFEST_SEP" read -r COMMIT _; do
			printf '%s\n' "$COMMIT"
		done <"$TMPMANIFEST" | sort -u
		echo "\."
		cat <<'SQL'
SELECT r.commit_hash || '|' || COALESCE(r.arch,'') || '|' || COALESCE(r.os,'') || '|' || COALESCE(r.profile,'') || '|' || COALESCE(NULLIF(r.room_version, ''), '11') || '|' || COALESCE(regexp_replace(btrim(r.features, ' ,'), '[,\s]+', ' ', 'g'), '')
FROM runs r
JOIN m ON m.commit_hash = r.commit_hash
WHERE EXISTS (SELECT 1 FROM run_details rd WHERE rd.run_id = r.id LIMIT 1);
SQL
	) | ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=30 "$SSH_TARGET" "psql -U git c10y -t -A"
)

declare -A HAS_DETAILS=()
while IFS= read -r key; do
	[[ -n "$key" ]] && HAS_DETAILS["$key"]=1
done <<<"$EXISTING_KEYS"

NEED=0
SKIP=0
declare -a PENDING_FILES=()
declare -a PENDING_META=()

while IFS="$MANIFEST_SEP" read -r COMMIT ARCH OS PROFILE ROOM_VERSION FEATURES; do
	KEY="${COMMIT}|${ARCH}|${OS}|${PROFILE}|${ROOM_VERSION}|${FEATURES}"
	if [[ -n "${HAS_DETAILS[$KEY]+x}" ]]; then
		((SKIP++)) || true
		continue
	fi

	SAFE_ARCH=${ARCH//[!a-zA-Z0-9._-]/_}
	SAFE_OS=${OS//[!a-zA-Z0-9._-]/_}
	SAFE_PROFILE=${PROFILE//[!a-zA-Z0-9._-]/_}
	SAFE_ROOM_VERSION=${ROOM_VERSION//[!a-zA-Z0-9._-]/_}
	BASENAME="${COMMIT}-${SAFE_ARCH}-${SAFE_OS}-${SAFE_PROFILE}-${SAFE_ROOM_VERSION}.jsonl"

	if grep -Fqx "$BASENAME" <<<"$ALL_FILES" 2>/dev/null; then
		PENDING_FILES+=("runs_data/${BASENAME}")
		PENDING_META+=("${COMMIT}${MANIFEST_SEP}${ARCH}${MANIFEST_SEP}${OS}${MANIFEST_SEP}${PROFILE}${MANIFEST_SEP}${ROOM_VERSION}${MANIFEST_SEP}${FEATURES}")
		((NEED++)) || true
	fi
done <"$TMPMANIFEST"

echo "→ Skipped $SKIP already-ingested runs, $NEED need details."

if [[ $NEED -eq 0 ]]; then
	echo "✓ All runs already have details. Nothing to do."
	exit 0
fi

# Stream only the NEW run detail files
echo "→ Streaming $NEED new run detail files..."
(
	for i in "${!PENDING_FILES[@]}"; do
		IFS="$MANIFEST_SEP" read -r COMMIT ARCH OS PROFILE ROOM_VERSION FEATURES <<<"${PENDING_META[$i]}"
		git show "FETCH_HEAD:${PENDING_FILES[$i]}" |
			jq -c --arg c "$COMMIT" --arg a "$ARCH" --arg o "$OS" --arg p "$PROFILE" --arg rv "$ROOM_VERSION" --arg f "$FEATURES" \
				'. + {commit: $c, arch: $a, os: $o, profile: $p, room_version: $rv, features: $f}'
	done
) | ingest_details

echo "✓ Incremental sync of last $LIMIT runs complete."

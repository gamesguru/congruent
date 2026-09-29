#!/bin/bash
set -e

BINARY="${1:-./target/debug/conduwuit}"

if [ ! -f "$BINARY" ]; then
	echo "Error: Binary not found at $BINARY. Please build first."
	exit 1
fi

DB_PATH="/tmp/conduwuit-reorder-test-db"
CONFIG_PATH="/tmp/conduwuit-reorder.toml"
DAG_FILE="/run/media/shane/shane4tb-ent/dags/local-dag-L58ME6ufiP49v97UIOBIpvWKEgj4912JmECPuDzlvCI-v12-wombatx.me-d1-68018.jsonl"
ROOM_ID="!L58ME6ufiP49v97UIOBIpvWKEgj4912JmECPuDzlvCI"

# Extract a tiny sample of 100 events from the problematic DAG
TEST_DAG="/tmp/test_dag.jsonl"
head -n 100 "$DAG_FILE" > "$TEST_DAG"

echo "Creating fresh DB..."
rm -rf "$DB_PATH"
cat <<EOF >"$CONFIG_PATH"
[global]
server_name = "localhost"
database_path = "$DB_PATH"
port = 6168
EOF

export LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"

echo "Importing test DAG..."
"$BINARY" -c "$CONFIG_PATH" --execute "yolo import-pdus $ROOM_ID $TEST_DAG --skip-auth --skip-sig-verify --room-version 12" --test smoke

echo "Running reorder-timeline..."
"$BINARY" -c "$CONFIG_PATH" --execute "yolo reorder-timeline $ROOM_ID" --test smoke

echo "Test passed!"

#!/bin/bash
set -e

BINARY="${1:-./target/latest/conduwuit}"

if [ ! -f "$BINARY" ]; then
	echo "Error: Binary not found at $BINARY"
	exit 1
fi

echo "Running basic version checks..."
"$BINARY" --version
"$BINARY" --version-verbose

echo "Creating dummy config for DB/startup test..."
cat <<'EOF' >/tmp/conduwuit-sanity.toml
[global]
server_name = "localhost"
database_path = "/tmp/conduwuit-sanity-db"
port = 6167
EOF

echo "Starting conduwuit in the background..."
export LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"
"$BINARY" -c /tmp/conduwuit-sanity.toml &
PID=$!

timeout=15
echo "Polling for ${timeout} seconds to ensure successful startup..."
elapsed=0
success=false

while [ $elapsed -lt $timeout ]; do
	# Check if process is still alive
	if ! kill -0 $PID 2>/dev/null; then
		echo "x conduwuit process crashed during startup!"
		wait $PID || true
		exit 1
	fi

	# Check if the HTTP server is listening and responding
	if curl -s http://localhost:6167/_matrix/client/versions >/dev/null; then
		echo "✓ conduwuit successfully started and is serving HTTP requests."
		success=true
		break
	fi

	sleep 2
	elapsed=$((elapsed + 2))
done

# Clean up the background process
echo "Shutting down conduwuit..."
kill -QUIT $PID 2>/dev/null || true

# Wait up to 30s for graceful shutdown, then force-kill
shutdown_timeout=30
shutdown_elapsed=0
while kill -0 $PID 2>/dev/null && [ $shutdown_elapsed -lt $shutdown_timeout ]; do
	sleep 1
	shutdown_elapsed=$((shutdown_elapsed + 1))
done

if kill -0 $PID 2>/dev/null; then
	echo "⚠ conduwuit did not exit within ${shutdown_timeout}s, sending SIGKILL..."
	kill -9 $PID 2>/dev/null || true
	wait $PID 2>/dev/null || true
else
	wait $PID 2>/dev/null || true
fi

if [ "$success" = true ]; then
	echo "Sanity check passed!"
	exit 0
else
	echo "x conduwuit failed to start HTTP server within $timeout seconds."
	exit 1
fi

#!/usr/bin/env bash
export COMPLEMENT_TEST=1
set -xe
# If we have no $SERVER_NAME set, abort
if [ -z "$SERVER_NAME" ]; then
	echo "SERVER_NAME is not set, aborting"
	exit 1
fi

# If /complement/ca/ca.crt or /complement/ca/ca.key are missing, abort
if [ ! -f /complement/ca/ca.crt ] || [ ! -f /complement/ca/ca.key ]; then
	echo "/complement/ca/ca.crt or /complement/ca/ca.key is missing, aborting"
	exit 1
fi

# Add the root cert to the local trust store
echo 'Installing Complement CA certificate to local trust store'
cp /complement/ca/ca.crt /usr/local/share/ca-certificates/complement-ca.crt
update-ca-certificates

# Sign a certificate for our $SERVER_NAME
echo "Generating and signing certificate for $SERVER_NAME"
openssl genrsa -out "/$SERVER_NAME.key" 2048

echo "Generating CSR for $SERVER_NAME"
openssl req -new -sha256 \
	-key "/$SERVER_NAME.key" \
	-out "/$SERVER_NAME.csr" \
	-subj "/C=US/ST=CA/O=Continuwuity, Inc./CN=$SERVER_NAME" \
	-addext "subjectAltName=DNS:$SERVER_NAME"
openssl req -in "$SERVER_NAME.csr" -noout -text

echo "Signing certificate for $SERVER_NAME with Complement CA"
cat <<EOF >./cert.ext
authorityKeyIdentifier=keyid,issuer
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment, dataEncipherment, nonRepudiation
extendedKeyUsage = serverAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = *.docker.internal
DNS.2 = hs1
DNS.3 = hs2
DNS.4 = hs3
DNS.5 = hs4
DNS.6 = $SERVER_NAME
IP.1 = 127.0.0.1
EOF
openssl x509 \
	-req \
	-in "/$SERVER_NAME.csr" \
	-CA /complement/ca/ca.crt \
	-CAkey /complement/ca/ca.key \
	-CAserial /tmp/ca.srl -CAcreateserial \
	-out "/$SERVER_NAME.crt" \
	-days 1 \
	-sha256 \
	-extfile ./cert.ext

# Tell continuwuity where to find the certs
export CONTINUWUITY_TLS__KEY="/$SERVER_NAME.key"
export CONTINUWUITY_TLS__CERTS="/$SERVER_NAME.crt"
# And who it is
export CONTINUWUITY_SERVER_NAME="$SERVER_NAME"
# Ensure each test node gets an isolated database to prevent reuse errors
export CONDUWUIT_DATABASE_PATH="${CONDUWUIT_DATABASE_PATH:-/var/lib/continuwuity/$SERVER_NAME}"
echo "rocksdb: $CONDUWUIT_DATABASE_PATH/rocksdb"
echo "mtxdb: $CONDUWUIT_DATABASE_PATH/mtxdb"

echo "Starting Continuwuity with SERVER_NAME=$SERVER_NAME"
mkdir -p "$CONDUWUIT_DATABASE_PATH"
chown -R "${CONDUWUIT_UID}:${CONDUWUIT_GID}" "/$SERVER_NAME.key" "/$SERVER_NAME.crt" "$CONDUWUIT_DATABASE_PATH"

# Drop root privileges and start continuwuity as the host UID
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

# Verify all dynamic libraries are resolvable before starting
MISSING_LIBS=$(ldd /usr/local/bin/conduwuit 2>&1 | grep "not found" || true)
if [ -n "$MISSING_LIBS" ]; then
	echo "FATAL: Missing dynamic libraries (check COMPLEMENT_HOST_MOUNTS):"
	echo "$MISSING_LIBS"
	echo ""
	echo "Mounted paths visible in /usr/local/lib:"
	find /usr/local/lib/ -maxdepth 1 -mindepth 1 -printf '%f\n' 2>/dev/null | head -20
	exit 1
fi

# Supervisor loop: Complement's dirty-run mode reuses this container across
# tests. A SIGUSR1 (sent via `docker kill --signal=SIGUSR1`) makes the
# entrypoint wipe the database and restart conduwuit, giving the next test a
# clean slate without Docker ever recreating the container. Ordinary process
# exits, including crash recovery, restart on the existing database.
#
# The wipe is gated on NEEDS_DB_WIPE rather than being unconditional: Complement
# also drives StopServer/StartServer as a plain `docker stop` / `docker start`
# round-trip, which re-executes this whole script. Tests such as
# TestDelayedEvents/delayed_state_events_are_kept_on_server_restart assert that
# the database survives that round-trip, so a re-executed entrypoint must start
# the server on whatever data is already on disk.
CONDUWUIT_PID=""
NEEDS_DB_WIPE=0

stop_conduwuit() {
	if [ -n "$CONDUWUIT_PID" ] && kill -0 "$CONDUWUIT_PID" 2>/dev/null; then
		kill -TERM "$CONDUWUIT_PID" 2>/dev/null || true
		for _ in $(seq 1 100); do
			kill -0 "$CONDUWUIT_PID" 2>/dev/null || break
			sleep 0.1
		done
		kill -KILL "$CONDUWUIT_PID" 2>/dev/null || true
		wait "$CONDUWUIT_PID" 2>/dev/null || true
	fi
}

reset_server() {
	echo "Reset triggered: stopping conduwuit, database wiped before the next start"
	NEEDS_DB_WIPE=1
	stop_conduwuit
}

terminate_server() {
	echo "Terminate triggered: stopping conduwuit"
	stop_conduwuit
	exit 0
}

trap reset_server SIGUSR1
trap terminate_server SIGHUP
trap terminate_server SIGTERM SIGINT

while true; do
	if [ "$NEEDS_DB_WIPE" -eq 1 ]; then
		echo "Resetting database and starting Continuwuity (SERVER_NAME=$SERVER_NAME)"
		rm -rf "${CONDUWUIT_DATABASE_PATH:?}"
		mkdir -p "$CONDUWUIT_DATABASE_PATH"
		chown -R "${CONDUWUIT_UID}:${CONDUWUIT_GID}" "$CONDUWUIT_DATABASE_PATH"
		NEEDS_DB_WIPE=0
	fi
	# Test containers are disposable: turn fsync into a no-op (libeatmydata) so
	# every redb commit does not wait on the disk. Set COMPLEMENT_FSYNC=1 to keep
	# real fsyncs, e.g. when measuring durability cost.
	EATMYDATA=""
	if [ "${COMPLEMENT_FSYNC:-0}" != "1" ] && command -v eatmydata >/dev/null 2>&1; then
		EATMYDATA="eatmydata"
	fi
	setpriv --reuid="${CONDUWUIT_UID}" --regid="${CONDUWUIT_GID}" --clear-groups $EATMYDATA /usr/local/bin/conduwuit --config /etc/continuwuity/config.toml &
	CONDUWUIT_PID=$!
	rc=0
	wait "$CONDUWUIT_PID" || rc=$?
	if [ "$NEEDS_DB_WIPE" -eq 1 ]; then
		continue
	fi
	echo "Conduwuit exited unexpectedly (status $rc)"
	exit "$rc"
done

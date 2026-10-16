#!/usr/bin/env bash
# Complement federation-abort hook for Continuwuity homeserver containers.
#
# Enable with:
#   export COMPLEMENT_FEDERATION_ABORT_SCRIPT="$PWD/complement/complement-abort-federation.sh"
#
# Complement runs this for every homeserver container when a test's mock
# federation server stops listening, passing: <container id> <server name>.
# The homeserver then drops that destination's queue, retry state, scheduled
# flushes and in-flight result, so nothing is sent to a retired host:port that a
# later test may reuse.
set -euo pipefail

container_id=${1:?container id}
server_name=${2:?server name}

# Headless attach: no TTY or stdin, sends the command to the running server's
# console socket and exits once the command has replied; `timeout` only guards
# against a missing reply stalling the test. The entrypoint exports the server
# name and database path inside its own shell, which `docker exec` does not
# inherit, so derive them from SERVER_NAME exactly as the entrypoint does.
timeout 30 docker exec "$container_id" sh -c '
	export CONTINUWUITY_SERVER_NAME="$SERVER_NAME"
	export CONDUWUIT_DATABASE_PATH="${CONDUWUIT_DATABASE_PATH:-/var/lib/continuwuity/$SERVER_NAME}"
	exec /usr/local/bin/conduwuit --config /etc/continuwuity/config.toml \
		--attach --execute "$1"
' sh "federation sending-queue ${server_name} --abort"

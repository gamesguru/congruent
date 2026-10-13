#!/usr/bin/env bash
# Complement post-test diagnostics for Continuwuity homeserver containers.
#
# Enable with:
#   export COMPLEMENT_POST_TEST_SCRIPT="$PWD/complement/complement-post-test.sh"
#
# Complement runs this after each test and before the container is removed,
# passing: <container id> <test name> <failed true|false>. One report is written
# per container to ${COMPLEMENT_STATS_DIR:-.tmp/complement/stats}.
#
# This measures where a run spends its time (CPU vs I/O vs memory pressure,
# per-process I/O, thread states, on-disk database size). It does not attempt
# to attribute time to individual requests; use the server's own phase timers
# for that. Aggregates here are totals over the container's lifetime, not
# wall-clock savings.
set -uo pipefail

container_id=${1:?container id}
test_name=${2:-unknown}
failed=${3:-unknown}
stats_dir=${COMPLEMENT_STATS_DIR:-.tmp/complement/stats}
safe_name=$(printf '%s' "$test_name" | tr -c 'A-Za-z0-9_.-' '_' | cut -c1-120)
out="$stats_dir/${safe_name}-${container_id:0:12}.txt"

mkdir -p "$stats_dir"

dexec() { docker exec "$container_id" sh -c "$1" 2>&1 || true; }

{
	echo "Complement Continuwuity diagnostics"
	echo "test: $test_name"
	echo "failed: $failed"
	echo "container: $container_id"
	echo "timestamp: $(date --iso-8601=seconds)"

	if ! docker inspect -f '{{.State.Running}}' "$container_id" 2>/dev/null | grep -q true; then
		echo
		echo "container is not running; only inspect data is available"
		docker inspect -f '{{.State.Status}} exit={{.State.ExitCode}} started={{.State.StartedAt}} finished={{.State.FinishedAt}}' "$container_id" 2>&1 || true
		exit 0
	fi

	echo
	echo "== started / uptime =="
	docker inspect -f 'started={{.State.StartedAt}}' "$container_id" 2>&1 || true

	echo
	echo "== pressure stall (cumulative since container start; total is microseconds) =="
	for resource in cpu io memory; do
		echo "-- $resource"
		dexec "cat /proc/pressure/$resource"
	done

	echo
	echo "== cgroup cpu.stat (throttling shows CPU limits biting) =="
	dexec "cat /sys/fs/cgroup/cpu.stat"

	echo
	echo "== cgroup io.stat (bytes and ios per device) =="
	dexec "cat /sys/fs/cgroup/io.stat"

	echo
	echo "== cgroup memory =="
	dexec "for f in memory.current memory.peak memory.max; do printf '%s: ' \$f; cat /sys/fs/cgroup/\$f; done"

	echo
	echo "== conduwuit process =="
	pid=$(dexec "pidof conduwuit | cut -d' ' -f1" | tr -d '\r\n')
	echo "pid: ${pid:-not found}"
	if [ -n "${pid:-}" ] && [ "$pid" -eq "$pid" ] 2>/dev/null; then
		echo "-- /proc/$pid/io (write_bytes = bytes sent to storage; cancelled_write_bytes = never written)"
		dexec "cat /proc/$pid/io"
		echo "-- /proc/$pid/status (selected)"
		dexec "grep -E '^(VmRSS|VmHWM|Threads|voluntary_ctxt_switches|nonvoluntary_ctxt_switches):' /proc/$pid/status"
		echo "-- /proc/$pid/schedstat (cpu_ns, runqueue_wait_ns, timeslices)"
		dexec "cat /proc/$pid/schedstat"
		echo "-- thread states (R running, S sleeping, D uninterruptible I/O wait)"
		dexec "for t in /proc/$pid/task/*/stat; do awk '{print \$3}' \$t; done | sort | uniq -c"
		echo "-- thread names"
		dexec "for t in /proc/$pid/task/*/comm; do cat \$t; done | sed 's/[0-9]*\$//' | sort | uniq -c | sort -rn | head -15"
	fi

	echo
	echo "== database directories (KiB) =="
	dexec "du -sk /var/lib/continuwuity/*/* 2>/dev/null"

	echo
	echo "== open file counts =="
	[ -n "${pid:-}" ] && dexec "ls /proc/$pid/fd | wc -l"
} >"$out" 2>&1

exit 0

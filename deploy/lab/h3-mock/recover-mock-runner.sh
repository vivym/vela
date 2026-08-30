#!/bin/sh

set -eu

image=${1:-}
root=${VELA_LAB_RUNNER_ROOT:-/var/lib/vela-lab/mock-runner}
container=${VELA_LAB_RUNNER_CONTAINER:-vela-h3-mock-runner}

fail() {
	printf 'recover-mock-runner: %s\n' "$*" >&2
	exit 1
}

[ "$(id -u)" -eq 0 ] || fail "run as root"
[ -n "$image" ] || fail "usage: $0 <registry/repository@sha256:digest>"
printf '%s\n' "$image" | grep -Eq '@sha256:[0-9a-f]{64}$' ||
	fail "image must use an immutable sha256 digest"
[ "$root" = /var/lib/vela-lab/mock-runner ] ||
	fail "VELA_LAB_RUNNER_ROOT must remain /var/lib/vela-lab/mock-runner"
[ -f "$root/config/recovery_mock_runner.py" ] || fail "installed recovery client is missing"
[ "$(sed -n '1p' "$root/config/mock-mode" 2>/dev/null)" = success ] ||
	fail "mock Runner must be in success mode before recovery rehearsal"
[ "$(docker container inspect "$container" --format '{{index .Config.Labels "vela.ai.component"}}' 2>/dev/null)" = h3-mock-runner ] ||
	fail "container $container is not managed by this deployment"
[ "$(docker container inspect "$container" --format '{{.State.Health.Status}}')" = healthy ] ||
	fail "container $container is not healthy"
[ "$(docker container inspect "$container" --format '{{.Config.Image}}')" = "$image" ] ||
	fail "container image does not match the requested digest"
command -v python3 >/dev/null 2>&1 || fail "python3 with pidfd support is required"

identity_digest=$(printf '%s:%s' "$(hostname)" "$(cat /etc/machine-id)" |
	sha256sum | awk '{print $1}')
worker_id=$(printf '%s' "$identity_digest" | cut -c1-32 |
	sed -E 's/(.{8})(.{4})(.{4})(.{4})(.{12})/\1-\2-\3-\4-\5/')

started=$(docker run --rm \
	--network none \
	--user 10001:10001 \
	--read-only \
	--cap-drop ALL \
	--security-opt no-new-privileges \
	--pids-limit 128 \
	--mount "type=bind,src=$root/run,dst=/run/vela-runner" \
	--mount "type=bind,src=$root/scratch,dst=/var/lib/vela/worker/scratch,readonly" \
	--mount "type=bind,src=$root/config,dst=/etc/vela-runner,readonly" \
	--env VELA_LAB_WORKER_ID="$worker_id" \
	--entrypoint /opt/vela/venv/bin/python \
	"$image" /etc/vela-runner/recovery_mock_runner.py start)
printf '%s\n' "$started" | jq -e '.schema_version == 1 and .phase == "started"' >/dev/null ||
	fail "recovery client did not start an Attempt"

before_started_at=$(docker container inspect "$container" --format '{{.State.StartedAt}}')
before_restart_count=$(docker container inspect "$container" --format '{{.RestartCount}}')
before_pid=$(docker container inspect "$container" --format '{{.State.Pid}}')
before_container_id=$(docker container inspect "$container" --format '{{.Id}}')
case "$before_pid" in
	''|*[!0-9]*) fail "container PID is invalid" ;;
esac
[ "$before_pid" -gt 1 ] || fail "container PID is invalid"
python3 - "$container" "$before_container_id" "$before_pid" "$before_started_at" <<'PY'
import os
import signal
import subprocess
import sys

container, expected_id, expected_pid_raw, expected_started_at = sys.argv[1:]
expected_pid = int(expected_pid_raw)
if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
    raise RuntimeError("host Python does not support pidfd signaling")

pidfd = os.pidfd_open(expected_pid)
try:
    observed = subprocess.check_output(
        [
            "docker",
            "container",
            "inspect",
            container,
            "--format",
            "{{.Id}}\t{{.State.Pid}}\t{{.State.StartedAt}}\t{{.State.Status}}",
        ],
        text=True,
    ).strip()
    observed_id, observed_pid, observed_started_at, observed_status = observed.split("\t")
    if (
        observed_id != expected_id
        or observed_pid != expected_pid_raw
        or observed_started_at != expected_started_at
        or observed_status != "running"
    ):
        raise RuntimeError("container process identity changed before fault injection")
    cgroup = open(f"/proc/{expected_pid}/cgroup", encoding="utf-8").read()
    if expected_id not in cgroup and expected_id[:12] not in cgroup:
        raise RuntimeError("target PID does not belong to the managed container cgroup")
    signal.pidfd_send_signal(pidfd, signal.SIGKILL)
finally:
    os.close(pidfd)
PY

attempt=0
status=starting
after_started_at=$before_started_at
after_pid=$before_pid
while [ "$attempt" -lt 30 ]; do
	status=$(docker container inspect "$container" --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}')
	after_started_at=$(docker container inspect "$container" --format '{{.State.StartedAt}}')
	after_pid=$(docker container inspect "$container" --format '{{.State.Pid}}')
	if [ "$status" = healthy ] && [ "$after_started_at" != "$before_started_at" ] && [ "$after_pid" != "$before_pid" ]; then
		break
	fi
	attempt=$((attempt + 1))
	sleep 1
done
[ "$status" = healthy ] || fail "container did not become healthy after SIGKILL"
[ "$after_started_at" != "$before_started_at" ] || fail "container start identity did not change"
[ "$after_pid" != "$before_pid" ] || fail "container PID did not change"
after_restart_count=$(docker container inspect "$container" --format '{{.RestartCount}}')
[ "$after_restart_count" -gt "$before_restart_count" ] || fail "automatic restart count did not advance"

recovered=$(docker run --rm \
	--network none \
	--user 10001:10001 \
	--read-only \
	--cap-drop ALL \
	--security-opt no-new-privileges \
	--pids-limit 128 \
	--mount "type=bind,src=$root/run,dst=/run/vela-runner" \
	--mount "type=bind,src=$root/scratch,dst=/var/lib/vela/worker/scratch,readonly" \
	--mount "type=bind,src=$root/config,dst=/etc/vela-runner,readonly" \
	--env VELA_LAB_WORKER_ID="$worker_id" \
	--env VELA_LAB_RECOVERY_IDENTITY_JSON="$started" \
	--entrypoint /opt/vela/venv/bin/python \
	"$image" /etc/vela-runner/recovery_mock_runner.py resume)
printf '%s\n' "$recovered" |
	jq -e '.schema_version == 1 and .phase == "recovered" and .resumed_local_state == true and .execution_state == "SUCCEEDED" and (.outputs | length) == 2' >/dev/null ||
	fail "same-authority local recovery did not complete"

printf '%s\n' "$recovered" |
	jq -c --arg before "$before_started_at" --arg after "$after_started_at" \
		--argjson pid_before "$before_pid" --argjson pid_after "$after_pid" \
		--argjson restart_count "$after_restart_count" \
		'. + {fault: "HOST_SIGKILL_CONTAINER_PID", pid_before: $pid_before, pid_after: $pid_after, started_at_before: $before, started_at_after: $after, restart_count: $restart_count}'

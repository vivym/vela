#!/bin/sh

set -eu

image=${1:-}
attempt_id=${2:-}
root=${VELA_LAB_RUNNER_ROOT:-/var/lib/vela-lab/mock-runner}
container=${VELA_LAB_RUNNER_CONTAINER:-vela-h3-mock-runner}

fail() {
	printf 'replay-terminal-mock-runner: %s\n' "$*" >&2
	exit 1
}

[ "$(id -u)" -eq 0 ] || fail "run as root"
printf '%s\n' "$image" | grep -Eq '^10\.1\.200\.17:5443/vela-lab/vela-h3-runner@sha256:[0-9a-f]{64}$' || fail "image must use the fixed lab repository and an immutable digest"
printf '%s\n' "$attempt_id" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' || fail "Attempt ID is invalid"
[ "$root" = /var/lib/vela-lab/mock-runner ] || fail "VELA_LAB_RUNNER_ROOT must remain /var/lib/vela-lab/mock-runner"
[ -f "$root/config/replay_terminal_mock_runner.py" ] && [ ! -L "$root/config/replay_terminal_mock_runner.py" ] || fail "installed terminal replay client is missing or unsafe"
[ -f "$root/scratch/runner-state/$attempt_id/state.json" ] || fail "terminal state file is absent"
[ ! -e "$root/scratch/outputs/$attempt_id" ] || fail "terminal Attempt output directory was not completely cleaned"
[ "$(docker container inspect "$container" --format '{{.State.Health.Status}}' 2>/dev/null)" = healthy ] || fail "managed Runner is not healthy"
[ "$(docker container inspect "$container" --format '{{.Config.Image}}')" = "$image" ] || fail "managed Runner image does not match the requested digest"

docker run --rm \
	--network none \
	--user 10001:10001 \
	--read-only \
	--cap-drop ALL \
	--security-opt no-new-privileges \
	--pids-limit 128 \
	--mount "type=bind,src=$root/run,dst=/run/vela-runner" \
	--mount "type=bind,src=$root/scratch,dst=/var/lib/vela/worker/scratch,readonly" \
	--mount "type=bind,src=$root/config,dst=/etc/vela-runner,readonly" \
	--env "VELA_LAB_TERMINAL_ATTEMPT_ID=$attempt_id" \
	--entrypoint /opt/vela/venv/bin/python \
	"$image" /etc/vela-runner/replay_terminal_mock_runner.py

#!/bin/sh

set -eu

image=${1:-}
root=${VELA_LAB_RUNNER_ROOT:-/var/lib/vela-lab/mock-runner}
container=${VELA_LAB_RUNNER_CONTAINER:-vela-h3-mock-runner}

fail() {
	printf 'smoke-mock-runner: %s\n' "$*" >&2
	exit 1
}

[ "$(id -u)" -eq 0 ] || fail "run as root"
[ -n "$image" ] || fail "usage: $0 <registry/repository@sha256:digest>"
printf '%s\n' "$image" | grep -Eq '@sha256:[0-9a-f]{64}$' ||
	fail "image must use an immutable sha256 digest"
[ "$root" = /var/lib/vela-lab/mock-runner ] ||
	fail "VELA_LAB_RUNNER_ROOT must remain /var/lib/vela-lab/mock-runner"
[ -f "$root/config/smoke_mock_runner.py" ] || fail "installed smoke client is missing"
[ "$(sed -n '1p' "$root/config/mock-mode" 2>/dev/null)" = success ] ||
	fail "mock Runner must be in success mode before smoke"
[ "$(docker container inspect "$container" --format '{{.State.Health.Status}}' 2>/dev/null)" = healthy ] ||
	fail "container $container is not healthy"
[ "$(docker container inspect "$container" --format '{{.Config.Image}}')" = "$image" ] ||
	fail "container image does not match the requested digest"

identity_digest=$(printf '%s:%s' "$(hostname)" "$(cat /etc/machine-id)" |
	sha256sum | awk '{print $1}')
worker_id=$(printf '%s' "$identity_digest" | cut -c1-32 |
	sed -E 's/(.{8})(.{4})(.{4})(.{4})(.{12})/\1-\2-\3-\4-\5/')
node_identity=$(printf '%s-%s' "$(hostname)" "$(printf '%s' "$identity_digest" | cut -c1-12)")

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
	--env VELA_LAB_WORKER_ID="$worker_id" \
	--env VELA_LAB_NODE_IDENTITY="$node_identity" \
	--entrypoint /opt/vela/venv/bin/python \
	"$image" /etc/vela-runner/smoke_mock_runner.py

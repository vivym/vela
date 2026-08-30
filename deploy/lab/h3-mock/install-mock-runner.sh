#!/bin/sh

set -eu

image=${1:-}
root=${VELA_LAB_RUNNER_ROOT:-/var/lib/vela-lab/mock-runner}
container=${VELA_LAB_RUNNER_CONTAINER:-vela-h3-mock-runner}
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)

fail() {
	printf 'install-mock-runner: %s\n' "$*" >&2
	exit 1
}

[ "$(id -u)" -eq 0 ] || fail "run as root"
[ -n "$image" ] || fail "usage: $0 <registry/repository@sha256:digest>"
printf '%s\n' "$image" | grep -Eq '@sha256:[0-9a-f]{64}$' ||
	fail "image must use an immutable sha256 digest"
[ "$root" = /var/lib/vela-lab/mock-runner ] ||
	fail "VELA_LAB_RUNNER_ROOT must remain /var/lib/vela-lab/mock-runner"
[ -f "$script_dir/profiles.json" ] || fail "profiles.json is missing"
[ -f "$script_dir/mock-backend-wrapper.sh" ] || fail "mock-backend-wrapper.sh is missing"
[ -f "$script_dir/set-mock-runner-mode.sh" ] || fail "set-mock-runner-mode.sh is missing"
[ -f "$script_dir/start-mock-runner-container.sh" ] || fail "start-mock-runner-container.sh is missing"
[ -f "$script_dir/upgrade-mock-runner-catalog-profile.sh" ] || fail "upgrade-mock-runner-catalog-profile.sh is missing"
[ -f "$script_dir/validate-runner-restart-state.sh" ] || fail "validate-runner-restart-state.sh is missing"
[ -f "$script_dir/smoke_mock_runner.py" ] || fail "smoke_mock_runner.py is missing"
[ -f "$script_dir/smoke-mock-runner.sh" ] || fail "smoke-mock-runner.sh is missing"
[ -f "$script_dir/recovery_mock_runner.py" ] || fail "recovery_mock_runner.py is missing"
[ -f "$script_dir/recover-mock-runner.sh" ] || fail "recover-mock-runner.sh is missing"
[ -f "$script_dir/remove-mock-runner.sh" ] || fail "remove-mock-runner.sh is missing"
command -v docker >/dev/null 2>&1 || fail "docker is required"
command -v ip >/dev/null 2>&1 || fail "ip is required"
command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v nvidia-smi >/dev/null 2>&1 || fail "nvidia-smi is required"

[ "$(hostname)" = ubuntu ] || fail "mock Runner installation is restricted to the two lab Workers"
worker_ip=$(ip -j -4 address show dev eno1 2>/dev/null |
	jq -r '.[0].addr_info[] | select(.family == "inet" and .scope == "global") | .local' |
	head -n 1)
case "$worker_ip" in
	10.1.200.19 | 10.1.200.16) ;;
	*) fail "mock Runner installation is restricted to Worker LAN addresses 10.1.200.19 and 10.1.200.16" ;;
esac
[ "$(ip -j -4 route show default | jq -r '.[0].dev // ""')" = eno1 ] ||
	fail "Worker default route must use eno1"

[ ! -e "$root" ] || fail "$root already exists; preserve or explicitly remove it before reinstalling"
if docker container inspect "$container" >/dev/null 2>&1; then
	fail "container $container already exists"
fi

backend_revision=$(jq -er '.backend_revision' "$script_dir/profiles.json") ||
	fail "profiles.json has no backend revision"
printf '%s\n' "$backend_revision" |
	grep -Eq '^mock-h3-backend@sha256:[0-9a-f]{64}$' ||
	fail "profiles.json backend revision is not a mock sha256 identity"
backend_sha256=${backend_revision#mock-h3-backend@sha256:}
image_metadata=$(docker image inspect "$image" --format '{{.Os}} {{.Architecture}} {{index .Config.Labels "vela.ai.build-kind"}} {{index .Config.Labels "vela.ai.h3-backend.sha256"}}' 2>/dev/null) ||
	fail "digest-pinned image is not present locally"
[ "$image_metadata" = "linux amd64 noncanonical-lab $backend_sha256" ] ||
	fail "image platform, lab marker, or embedded backend digest is unexpected"

gpu_records=$(nvidia-smi --query-gpu=index,uuid --format=csv,noheader,nounits | sort -t, -k1,1n)
gpu_count=$(printf '%s\n' "$gpu_records" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')
[ "$gpu_count" -eq 8 ] || fail "exactly eight GPUs are required"
gpu_uuids=$(printf '%s\n' "$gpu_records" | awk -F, '{gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}')
unique_gpu_count=$(printf '%s\n' "$gpu_uuids" | sort -u | wc -l | tr -d ' ')
[ "$unique_gpu_count" -eq 8 ] || fail "GPU UUIDs must be unique"
if printf '%s\n' "$gpu_uuids" | grep -Evq '^GPU-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'; then
	fail "GPU UUID format is invalid"
fi

active_gpu_processes=$(nvidia-smi --query-compute-apps=gpu_uuid --format=csv,noheader,nounits 2>/dev/null | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')
[ "$active_gpu_processes" -eq 0 ] || fail "Worker has active GPU compute processes"

install -d -m 0755 /var/lib/vela-lab
install -d -m 0755 "$root"
install -d -m 0750 "$root/admin"
install -d -m 0755 "$root/config"
install -d -m 0700 -o 10001 -g 10001 "$root/run"
install -d -m 0700 -o 10001 -g 10001 "$root/scratch"
install -d -m 0700 -o 10001 -g 10001 "$root/scratch/runner-state"
install -d -m 0700 -o 10001 -g 10001 "$root/scratch/outputs"
install -m 0444 -o 0 -g 0 "$script_dir/profiles.json" "$root/config/profiles.json"
install -m 0555 -o 0 -g 0 "$script_dir/mock-backend-wrapper.sh" "$root/config/mock-backend-wrapper.sh"
printf 'success\n' >"$root/config/mock-mode"
chown 0:0 "$root/config/mock-mode"
chmod 0444 "$root/config/mock-mode"
install -m 0444 -o 0 -g 0 "$script_dir/smoke_mock_runner.py" "$root/config/smoke_mock_runner.py"
install -m 0444 -o 0 -g 0 "$script_dir/recovery_mock_runner.py" "$root/config/recovery_mock_runner.py"
install -m 0550 -o 0 -g 0 "$script_dir/smoke-mock-runner.sh" "$root/admin/smoke-mock-runner.sh"
install -m 0550 -o 0 -g 0 "$script_dir/recover-mock-runner.sh" "$root/admin/recover-mock-runner.sh"
install -m 0550 -o 0 -g 0 "$script_dir/remove-mock-runner.sh" "$root/admin/remove-mock-runner.sh"
install -m 0550 -o 0 -g 0 "$script_dir/set-mock-runner-mode.sh" "$root/admin/set-mock-runner-mode.sh"
install -m 0550 -o 0 -g 0 "$script_dir/start-mock-runner-container.sh" "$root/admin/start-mock-runner-container.sh"
install -m 0550 -o 0 -g 0 "$script_dir/upgrade-mock-runner-catalog-profile.sh" "$root/admin/upgrade-mock-runner-catalog-profile.sh"
install -m 0550 -o 0 -g 0 "$script_dir/validate-runner-restart-state.sh" "$root/admin/validate-runner-restart-state.sh"

roles_tmp=$(mktemp "$root/config/.gpu-roles.XXXXXX")
trap 'rm -f "$roles_tmp"' EXIT HUP INT TERM
printf '%s\n' "$gpu_uuids" |
	jq -Rn '[inputs] | {schema_version: 1, encoder_vae: .[0], dit: .[1:8]}' >"$roles_tmp"
chown 0:0 "$roles_tmp"
chmod 0444 "$roles_tmp"
mv "$roles_tmp" "$root/config/gpu-roles.json"
trap - EXIT HUP INT TERM

"$script_dir/start-mock-runner-container.sh" "$image" mode-control

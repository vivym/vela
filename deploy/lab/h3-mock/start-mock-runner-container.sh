#!/bin/sh

set -eu

image=${1:-}
layout=${2:-}
root=${VELA_LAB_RUNNER_ROOT:-/var/lib/vela-lab/mock-runner}
container=${VELA_LAB_RUNNER_CONTAINER:-vela-h3-mock-runner}
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)

fail() {
	printf 'start-mock-runner-container: %s\n' "$*" >&2
	exit 1
}

[ "$(id -u)" -eq 0 ] || fail "run as root"
[ -n "$image" ] || fail "usage: $0 <registry/repository@sha256:digest> <mode-control|legacy-success>"
printf '%s\n' "$image" | grep -Eq '@sha256:[0-9a-f]{64}$' ||
	fail "image must use an immutable sha256 digest"
case "$layout" in mode-control | legacy-success) ;; *) fail "layout must be mode-control or legacy-success" ;; esac
[ "$root" = /var/lib/vela-lab/mock-runner ] ||
	fail "VELA_LAB_RUNNER_ROOT must remain /var/lib/vela-lab/mock-runner"
command -v docker >/dev/null 2>&1 || fail "docker is required"
command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v nvidia-smi >/dev/null 2>&1 || fail "nvidia-smi is required"
[ -x "$script_dir/validate-runner-restart-state.sh" ] || fail "restart-state validator is missing"
[ -f "$root/config/profiles.json" ] || fail "installed profiles.json is missing"
[ -f "$root/config/gpu-roles.json" ] || fail "installed gpu-roles.json is missing"
if docker container inspect "$container" >/dev/null 2>&1; then
	fail "container $container already exists"
fi
"$script_dir/validate-runner-restart-state.sh" >/dev/null

backend_revision=$(jq -er '.backend_revision' "$root/config/profiles.json") ||
	fail "profiles.json has no backend revision"
printf '%s\n' "$backend_revision" | grep -Eq '^mock-h3-backend@sha256:[0-9a-f]{64}$' ||
	fail "profiles.json backend revision is not a mock sha256 identity"
backend_sha256=${backend_revision#mock-h3-backend@sha256:}
image_metadata=$(docker image inspect "$image" --format '{{.Os}} {{.Architecture}} {{index .Config.Labels "vela.ai.build-kind"}} {{index .Config.Labels "vela.ai.h3-backend.sha256"}}' 2>/dev/null) ||
	fail "digest-pinned image is not present locally"
[ "$image_metadata" = "linux amd64 noncanonical-lab $backend_sha256" ] ||
	fail "image platform, lab marker, or embedded backend digest is unexpected"

gpu_uuids=$(nvidia-smi --query-gpu=index,uuid --format=csv,noheader,nounits |
	sort -t, -k1,1n |
	awk -F, '{gsub(/^[ \t]+|[ \t]+$/, "", $2); print $2}')
[ "$(printf '%s\n' "$gpu_uuids" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')" -eq 8 ] ||
	fail "exactly eight GPUs are required"
expected_uuids=$(jq -er '[.encoder_vae] + .dit | .[]' "$root/config/gpu-roles.json") ||
	fail "installed GPU role map is invalid"
[ "$gpu_uuids" = "$expected_uuids" ] || fail "installed GPU role map does not match the host"
cuda_visible_devices=$(printf '%s\n' "$gpu_uuids" | paste -sd, -)
output_spec_id=$(jq -er '.profiles[0].output_spec_id' "$root/config/profiles.json") ||
	fail "profiles.json has no OutputSpec"

case "$layout" in
	mode-control)
		[ -f "$root/config/mock-backend-wrapper.sh" ] && [ ! -L "$root/config/mock-backend-wrapper.sh" ] ||
			fail "guarded mock backend wrapper is missing or unsafe"
		[ -f "$root/config/mock-mode" ] && [ ! -L "$root/config/mock-mode" ] ||
			fail "mock mode file is missing or unsafe"
		[ "$(sed -n '1p' "$root/config/mock-mode")" = success ] || fail "new mode-controlled Runner must start in success mode"
		backend_command=/etc/vela-runner/mock-backend-wrapper.sh
		backend_arguments="[\"--mock-output-spec-id\",\"$output_spec_id\",\"--mock-stage-delay\",\"250ms\"]"
		config_revision=$(sha256sum \
			"$root/config/profiles.json" \
			"$root/config/gpu-roles.json" \
			"$root/config/mock-backend-wrapper.sh" |
			sha256sum | awk '{print $1}')
		;;
	legacy-success)
		backend_command=/opt/vela/bin/h3-backend
		backend_arguments="[\"--mock-output-spec-id\",\"$output_spec_id\",\"--mock-mode\",\"success\",\"--mock-stage-delay\",\"250ms\"]"
		config_revision=$(sha256sum "$root/config/profiles.json" "$root/config/gpu-roles.json" |
			sha256sum | awk '{print $1}')
		;;
esac

docker run --detach \
	--name "$container" \
	--restart unless-stopped \
	--network none \
	--user 10001:10001 \
	--gpus all \
	--cpus 4 \
	--memory 4g \
	--memory-swap 4g \
	--pids-limit 256 \
	--read-only \
	--cap-drop ALL \
	--security-opt no-new-privileges \
	--tmpfs /tmp:rw,noexec,nosuid,nodev,size=64m,uid=10001,gid=10001,mode=0700 \
	--mount "type=bind,src=$root/run,dst=/run/vela-runner" \
	--mount "type=bind,src=$root/scratch,dst=/var/lib/vela/worker/scratch" \
	--mount "type=bind,src=$root/config,dst=/etc/vela-runner,readonly" \
	--env "CUDA_VISIBLE_DEVICES=$cuda_visible_devices" \
	--env VELA_RUNNER_SOCKET=/run/vela-runner/runner.sock \
	--env VELA_RUNNER_SCRATCH_ROOT=/var/lib/vela/worker/scratch \
	--env VELA_RUNNER_STATE_ROOT=/var/lib/vela/worker/scratch/runner-state \
	--env VELA_RUNNER_OUTPUT_ROOT=/var/lib/vela/worker/scratch/outputs \
	--env "VELA_RUNNER_BACKEND_REVISION=$backend_revision" \
	--env "VELA_RUNNER_BACKEND_COMMAND=$backend_command" \
	--env "VELA_RUNNER_BACKEND_ARGS_JSON=$backend_arguments" \
	--env VELA_RUNNER_PROFILES_FILE=/etc/vela-runner/profiles.json \
	--env VELA_RUNNER_GPU_ROLES_FILE=/etc/vela-runner/gpu-roles.json \
	--env VELA_RUNNER_STOP_TIMEOUT=15 \
	--env VELA_RUNNER_MAX_OUTPUT_BYTES=1073741824 \
	--health-cmd 'test -S /run/vela-runner/runner.sock' \
	--health-interval 5s \
	--health-timeout 2s \
	--health-retries 6 \
	--health-start-period 5s \
	--log-driver json-file \
	--log-opt max-size=10m \
	--log-opt max-file=3 \
	--label vela.ai.environment=non-production-lab \
	--label vela.ai.component=h3-mock-runner \
	--label "vela.ai.config-revision=sha256:$config_revision" \
	"$image" >/dev/null

attempt=0
status=unknown
while [ "$attempt" -lt 12 ]; do
	status=$(docker container inspect "$container" --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}')
	[ "$status" = healthy ] && break
	[ "$status" = exited ] && break
	attempt=$((attempt + 1))
	sleep 1
done
[ "$status" = healthy ] || {
	docker logs --tail 100 "$container" >&2 || true
	fail "container health is $status"
}
observed_gpus=$(docker exec "$container" nvidia-smi --query-gpu=uuid --format=csv,noheader,nounits |
	sed '/^[[:space:]]*$/d' | sort -u | wc -l | tr -d ' ')
[ "$observed_gpus" -eq 8 ] || fail "container does not observe exactly eight GPUs"

printf 'container=%s\nimage=%s\nlayout=%s\nconfig_revision=sha256:%s\nhealth=%s\ngpus=%s\n' \
	"$container" "$image" "$layout" "$config_revision" "$status" "$observed_gpus"

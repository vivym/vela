#!/bin/sh

set -eu

new_image=${1:-}
expected_container_id=${2:-}
expected_attempt_id=${3:-}
output=${4:-}
apply=${5:-}
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
root=/var/lib/vela-lab/mock-runner
admin=$root/admin
config=$root/config
receipts=$root/receipts
container=vela-h3-mock-runner
repository=10.1.200.17:5443/vela-lab/vela-h3-runner
old_image=$repository@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3
runner_revision=dfd504e99b043ca0397294cc60ee8941d70306bb
runtime_sha256=71a2a4b086db11f71c81369ed4044d452d5f85ef30e82644764d5b4680be0baf
backend_sha256=765077057011f16f852886601235f066dff7a89d3127719a5ae3c38206c7aee6
runtime_path=/opt/vela/venv/lib/python3.13/site-packages/vela_h3_runner/runtime.py
temporary=
committed=false
phase=preflight

fail() {
	printf 'rollout-fixed-runner: %s\n' "$*" >&2
	exit 1
}

gpu_process_count() {
	nvidia-smi --query-compute-apps=pid --format=csv,noheader,nounits 2>/dev/null |
		sed '/^[[:space:]]*$/d' | wc -l | tr -d ' '
}

cleanup() {
	if [ "$committed" = false ] && [ -n "$temporary" ] && [ -d "$temporary" ]; then
		printf 'schema=vela-lab-fixed-runner-rollout-v1\nresult=FAIL\nphase=%s\nfailed_at=%s\nstate_preserved=true\nworker_must_remain_draining=true\nproduction_gates=0/9\n' \
			"$phase" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$temporary/result.txt"
		printf 'rollout-fixed-runner: diagnostic receipt retained at %s\n' "$temporary" >&2
	fi
}

install_helper() {
	source=$1
	destination=$2
	mode=$3
	[ -f "$source" ] && [ ! -L "$source" ] || fail "helper source $source is missing or unsafe"
	if [ -e "$destination" ]; then
		[ -f "$destination" ] && [ ! -L "$destination" ] || fail "installed helper $destination is unsafe"
		cp -p "$destination" "$temporary/helper-backups/$(basename "$destination")"
	fi
	install -m "$mode" -o 0 -g 0 "$source" "$destination"
}

[ "$(id -u)" -eq 0 ] || fail "run as root"
[ "$apply" = --apply ] || fail "usage: $0 <new-image@sha256:digest> <expected-container-id> <expected-attempt-id> <absolute-receipt-directory> --apply"
printf '%s\n' "$new_image" | grep -Eq "^$repository@sha256:[0-9a-f]{64}$" || fail "new image must use the fixed lab repository and an immutable digest"
printf '%s\n' "$expected_container_id" | grep -Eq '^[0-9a-f]{64}$' || fail "expected container ID is invalid"
printf '%s\n' "$expected_attempt_id" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' || fail "expected Attempt ID is invalid"
case "$output" in "$receipts"/fixed-runner-rollout-*) ;; *) fail "receipt path is outside the fixed Runner receipt root" ;; esac
[ ! -e "$output" ] || fail "receipt path already exists"
[ -d "$root" ] && [ ! -L "$root" ] || fail "Runner root is missing or unsafe"
for command in docker jq sha256sum nvidia-smi; do command -v "$command" >/dev/null 2>&1 || fail "$command is required"; done
[ "$(gpu_process_count)" -eq 0 ] || fail "Worker has active GPU compute processes"
[ "$(sed -n '1p' "$config/mock-mode")" = success ] || fail "mock Runner is not in success mode"

container_json=$(docker container inspect "$container" 2>/dev/null) || fail "managed Runner container is absent"
observed_container_id=$(printf '%s\n' "$container_json" | jq -er '.[0].Id')
[ "$observed_container_id" = "$expected_container_id" ] || fail "managed Runner container ID changed"
[ "$(printf '%s\n' "$container_json" | jq -er '.[0].Config.Image')" = "$old_image" ] || fail "managed Runner is not on the expected old digest"
[ "$(printf '%s\n' "$container_json" | jq -er '.[0].Config.Labels["vela.ai.component"]')" = h3-mock-runner ] || fail "managed Runner label changed"

state_dir=$root/scratch/runner-state/$expected_attempt_id
state_file=$state_dir/state.json
[ -f "$state_file" ] && [ ! -L "$state_file" ] || fail "expected retained state file is missing or unsafe"
[ "$(find "$root/scratch/runner-state" -mindepth 1 -maxdepth 1 -type d ! -name "$expected_attempt_id" -print | wc -l | tr -d ' ')" -eq 0 ] || fail "Runner state root contains an unexpected Attempt"
[ ! -e "$root/scratch/outputs/$expected_attempt_id" ] || fail "retained terminal output directory is not completely absent"
[ "$(jq -r '.identity.attempt_id' "$state_file")" = "$expected_attempt_id" ] || fail "retained state Attempt identity changed"
[ "$(jq -r '.state' "$state_file")" = 4 ] || fail "retained state is not SUCCEEDED"
[ "$(jq -r '.resume' "$state_file")" = false ] || fail "retained state is unexpectedly resumable"
[ "$(jq -r '.outputs | length' "$state_file")" = 2 ] || fail "retained state output receipt count changed"

image_metadata=$(docker image inspect "$new_image" --format '{{.Os}}|{{.Architecture}}|{{index .Config.Labels "vela.ai.build-kind"}}|{{index .Config.Labels "vela.ai.h3-backend.sha256"}}|{{index .Config.Labels "org.opencontainers.image.revision"}}|{{index .Config.Labels "vela.ai.lab-parent-digest"}}|{{index .Config.Labels "vela.ai.lab-runtime-sha256"}}' 2>/dev/null) || fail "new digest-pinned Runner image is absent"
[ "$image_metadata" = "linux|amd64|noncanonical-lab|$backend_sha256|$runner_revision|sha256:${old_image##*@sha256:}|$runtime_sha256" ] || fail "new Runner image metadata is invalid"
observed_runtime=$(docker run --rm --network none --entrypoint /usr/bin/sha256sum "$new_image" "$runtime_path" | awk '{print $1}')
[ "$observed_runtime" = "$runtime_sha256" ] || fail "new Runner image contains the wrong runtime.py"
validator_result=$("$script_dir"/validate-runner-restart-state.sh)
printf '%s\n' "$validator_result" | grep -F 'result=READY checked_states=1 retired_success_states=1 production_gates=0/9' >/dev/null || fail "restart-state validator did not accept exactly one cleaned terminal state"

install -d -m 0700 "$receipts"
temporary=$(mktemp -d "$receipts/.fixed-runner-rollout.XXXXXX")
chmod 0700 "$temporary"
install -d -m 0700 "$temporary/helper-backups"
trap cleanup EXIT HUP INT TERM
printf 'schema=vela-lab-fixed-runner-rollout-v1\nstarted_at=%s\nhostname=%s\nold_image=%s\nnew_image=%s\nexpected_container_id=%s\nexpected_attempt_id=%s\nproduction_gates=0/9\n' \
	"$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$(hostname)" "$old_image" "$new_image" "$expected_container_id" "$expected_attempt_id" >"$temporary/input.txt"
printf '%s\n' "$container_json" >"$temporary/container-before.json"
printf '%s\n' "$validator_result" >"$temporary/validator-before.txt"
sha256sum "$state_file" >"$temporary/terminal-state-before.sha256"
find "$state_dir" -xdev -maxdepth 1 -type f -printf '%f|%s|%m|%u|%g\n' | sort >"$temporary/terminal-state-inventory-before.txt"
sha256sum "$script_dir"/start-mock-runner-container.sh \
	"$script_dir"/remove-mock-runner.sh \
	"$script_dir"/write-mock-runner-container-identity.sh \
	"$script_dir"/validate-runner-restart-state.sh \
	"$script_dir"/smoke-mock-runner.sh \
	"$script_dir"/smoke_mock_runner.py \
	"$script_dir"/replay-terminal-mock-runner.sh \
	"$script_dir"/replay_terminal_mock_runner.py \
	"$0" >"$temporary/source-sha256.txt"

phase=install-helpers
install_helper "$script_dir/start-mock-runner-container.sh" "$admin/start-mock-runner-container.sh" 0550
install_helper "$script_dir/remove-mock-runner.sh" "$admin/remove-mock-runner.sh" 0550
install_helper "$script_dir/write-mock-runner-container-identity.sh" "$admin/write-mock-runner-container-identity.sh" 0550
install_helper "$script_dir/validate-runner-restart-state.sh" "$admin/validate-runner-restart-state.sh" 0550
install_helper "$script_dir/smoke-mock-runner.sh" "$admin/smoke-mock-runner.sh" 0550
install_helper "$script_dir/smoke_mock_runner.py" "$config/smoke_mock_runner.py" 0444
install_helper "$script_dir/replay-terminal-mock-runner.sh" "$admin/replay-terminal-mock-runner.sh" 0550
install_helper "$script_dir/replay_terminal_mock_runner.py" "$config/replay_terminal_mock_runner.py" 0444

phase=remove-old-container
[ "$(docker container inspect "$container" --format '{{.Id}}')" = "$expected_container_id" ] || fail "managed Runner identity changed immediately before removal"
"$admin/remove-mock-runner.sh" >"$temporary/remove-old-container.txt"
[ -d "$state_dir" ] && [ ! -e "$root/scratch/outputs/$expected_attempt_id" ] || fail "retained state changed during container removal"

phase=start-fixed-container
"$admin/start-mock-runner-container.sh" "$new_image" mode-control >"$temporary/start-fixed-container.txt"
new_container_id=$(docker container inspect "$container" --format '{{.Id}}')
[ "$new_container_id" != "$expected_container_id" ] || fail "container replacement did not create a new identity"
[ "$(docker container inspect "$container" --format '{{.State.Health.Status}}')" = healthy ] || fail "fixed Runner container is not healthy"

phase=terminal-replay
"$admin/replay-terminal-mock-runner.sh" "$new_image" "$expected_attempt_id" >"$temporary/terminal-replay.json"
sha256sum --check --strict "$temporary/terminal-state-before.sha256" >"$temporary/terminal-state-replay-check.txt"
[ ! -e "$root/scratch/outputs/$expected_attempt_id" ] || fail "terminal replay recreated the cleaned output directory"
[ "$(gpu_process_count)" -eq 0 ] || fail "terminal replay started a GPU compute process"

phase=fresh-smoke
"$admin/smoke-mock-runner.sh" "$new_image" >"$temporary/fresh-smoke.json"
[ ! -e "$state_dir" ] || fail "fresh smoke did not retire the cleaned terminal state"
[ "$(find "$root/scratch/runner-state" -mindepth 1 -maxdepth 1 -type d -print | wc -l | tr -d ' ')" -eq 1 ] || fail "fresh smoke did not leave exactly one Runner state"
validator_after=$($admin/validate-runner-restart-state.sh)
printf '%s\n' "$validator_after" | grep -F 'result=READY checked_states=1 retired_success_states=0 production_gates=0/9' >/dev/null || fail "fresh smoke did not leave one complete successful state"
printf '%s\n' "$validator_after" >"$temporary/validator-after.txt"
[ "$(gpu_process_count)" -eq 0 ] || fail "fresh smoke left a GPU compute process"
docker container inspect "$container" >"$temporary/container-after.json"
nvidia-smi --query-compute-apps=pid,process_name --format=csv,noheader >"$temporary/gpu-processes-after.txt"
find "$root/scratch/runner-state" -xdev -mindepth 1 -maxdepth 2 -printf '%P|%y|%s|%m|%u|%g\n' | sort >"$temporary/state-after.txt"

phase=finalize
printf 'schema=vela-lab-fixed-runner-rollout-v1\nresult=PASS\nold_container_id=%s\nnew_container_id=%s\nnew_image=%s\nterminal_replay=PASS\nfresh_smoke=PASS\nstate_preserved=true\ncompleted_at=%s\nproduction_gates=0/9\n' \
	"$expected_container_id" "$new_container_id" "$new_image" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$temporary/result.txt"
find "$temporary" -maxdepth 1 -type f ! -name SHA256SUMS -print | sort | xargs sha256sum >"$temporary/SHA256SUMS"
mv "$temporary" "$output"
temporary=
committed=true
printf 'schema=vela-lab-fixed-runner-rollout-v1 result=PASS new_container_id=%s new_image=%s receipt=%s production_gates=0/9\n' \
	"$new_container_id" "$new_image" "$output"

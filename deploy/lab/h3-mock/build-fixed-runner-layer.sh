#!/bin/sh

set -eu

runtime_source=${1:-}
output=${2:-}
apply=${3:-}
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
root=/var/lib/vela-lab/mock-runner
receipts=$root/receipts
registry=10.1.200.17:5443
repository=$registry/vela-lab/vela-h3-runner
old_digest=71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3
old_image=$repository@sha256:$old_digest
runner_revision=dfd504e99b043ca0397294cc60ee8941d70306bb
runtime_sha256=71a2a4b086db11f71c81369ed4044d452d5f85ef30e82644764d5b4680be0baf
old_runtime_sha256=a5bd2706b2ba019d2d032cd2ec018390aafef5a0174ee8703c960cd7418f003b
backend_sha256=765077057011f16f852886601235f066dff7a89d3127719a5ae3c38206c7aee6
target_tag=$repository:runtime-$runtime_sha256
runtime_path=/opt/vela/venv/lib/python3.13/site-packages/vela_h3_runner/runtime.py
registry_config=${VELA_LAB_REGISTRY_CONFIG:-/etc/rancher/rke2/registries.yaml}
temporary=
context=
auth_dir=
committed=false

fail() {
	printf 'build-fixed-runner-layer: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	if [ -n "$auth_dir" ] && [ -d "$auth_dir" ]; then
		find "$auth_dir" -xdev -mindepth 1 -delete
		rmdir "$auth_dir"
	fi
	if [ -n "$context" ] && [ -d "$context" ]; then
		find "$context" -xdev -mindepth 1 -delete
		rmdir "$context"
	fi
	if [ "$committed" = false ] && [ -n "$temporary" ] && [ -d "$temporary" ]; then
		printf 'phase=failed\nfailed_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >>"$temporary/result.txt"
		printf 'build-fixed-runner-layer: diagnostic receipt retained at %s\n' "$temporary" >&2
	fi
}

image_metadata() {
	image=$1
	docker image inspect "$image" --format '{{.Os}}|{{.Architecture}}|{{index .Config.Labels "vela.ai.build-kind"}}|{{index .Config.Labels "vela.ai.h3-backend.sha256"}}|{{index .Config.Labels "org.opencontainers.image.revision"}}|{{index .Config.Labels "vela.ai.lab-parent-digest"}}|{{index .Config.Labels "vela.ai.lab-runtime-sha256"}}'
}

resolve_digest() {
	docker image inspect "$target_tag" --format '{{range .RepoDigests}}{{println .}}{{end}}' |
		awk -v prefix="$repository@sha256:" 'index($0, prefix) == 1 {print}'
}

[ "$(id -u)" -eq 0 ] || fail "run as root"
[ "$apply" = --apply ] || fail "usage: $0 <runtime.py> <absolute-receipt-directory> --apply"
[ -f "$runtime_source" ] && [ ! -L "$runtime_source" ] || fail "runtime.py is missing or unsafe"
[ -f "$script_dir/Dockerfile.runtime-fix" ] && [ ! -L "$script_dir/Dockerfile.runtime-fix" ] ||
	fail "runtime-fix Dockerfile is missing or unsafe"
case "$output" in "$receipts"/fixed-runner-image-*) ;; *) fail "receipt path is outside the fixed Runner receipt root" ;; esac
[ ! -e "$output" ] || fail "receipt path already exists"
[ -d "$root" ] && [ ! -L "$root" ] || fail "Runner root is missing or unsafe"
[ -r "$registry_config" ] && [ ! -L "$registry_config" ] || fail "RKE2 registry configuration is missing or unsafe"
for command in docker jq sha256sum; do command -v "$command" >/dev/null 2>&1 || fail "$command is required"; done
[ "$(sha256sum "$runtime_source" | awk '{print $1}')" = "$runtime_sha256" ] || fail "runtime.py SHA-256 is unexpected"

old_metadata=$(docker image inspect "$old_image" --format '{{.Os}}|{{.Architecture}}|{{index .Config.Labels "vela.ai.build-kind"}}|{{index .Config.Labels "vela.ai.h3-backend.sha256"}}' 2>/dev/null) ||
	fail "old digest-pinned Runner image is absent"
[ "$old_metadata" = "linux|amd64|noncanonical-lab|$backend_sha256" ] || fail "old Runner image metadata changed"
observed_old_runtime=$(docker run --rm --network none --entrypoint /usr/bin/sha256sum "$old_image" "$runtime_path" |
	awk '{print $1}')
[ "$observed_old_runtime" = "$old_runtime_sha256" ] || fail "old Runner runtime SHA-256 changed"

install -d -m 0700 "$receipts"
temporary=$(mktemp -d "$receipts/.fixed-runner-image.XXXXXX")
chmod 0700 "$temporary"
trap cleanup EXIT HUP INT TERM
printf 'schema=vela-lab-fixed-runner-image-v1\nstarted_at=%s\nold_image=%s\nruntime_sha256=%s\nrunner_revision=%s\ntarget_tag=%s\nproduction_gates=0/9\n' \
	"$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$old_image" "$runtime_sha256" "$runner_revision" "$target_tag" >"$temporary/input.txt"
sha256sum "$runtime_source" "$script_dir/Dockerfile.runtime-fix" "$0" >"$temporary/source-sha256.txt"

username=$(jq -er --arg registry "$registry" '.configs[$registry].auth.username | select(type == "string")' "$registry_config") ||
	fail "registry username is missing or invalid"
password=$(jq -er --arg registry "$registry" '.configs[$registry].auth.password | select(type == "string")' "$registry_config") ||
	fail "registry password is missing or invalid"
printf '%s\n' "$username" | grep -Eq '^[A-Za-z0-9._-]{1,128}$' || fail "registry username is invalid"
[ "${#password}" -le 512 ] || fail "registry password is too long"
auth_dir=$(mktemp -d "$receipts/.fixed-runner-auth.XXXXXX")
chmod 0700 "$auth_dir"
if ! printf '%s' "$password" | docker --config "$auth_dir" login "$registry" --username "$username" --password-stdin >"$temporary/registry-login.txt" 2>&1; then
	fail "registry login failed"
fi
unset password

published=false
if docker --config "$auth_dir" pull "$target_tag" >"$temporary/preexisting-pull.txt" 2>&1; then
	published=true
else
	context=$(mktemp -d "$receipts/.fixed-runner-context.XXXXXX")
	chmod 0700 "$context"
	install -m 0444 "$runtime_source" "$context/runtime.py"
	DOCKER_BUILDKIT=1 docker build \
		--network none \
		--build-arg "BASE_IMAGE=$old_image" \
		--build-arg "RUNNER_REVISION=$runner_revision" \
		--build-arg "RUNTIME_SHA256=$runtime_sha256" \
		--tag "$target_tag" \
		--file "$script_dir/Dockerfile.runtime-fix" \
		"$context" >"$temporary/build.txt" 2>&1 || fail "fixed Runner layer build failed"
	docker --config "$auth_dir" push "$target_tag" >"$temporary/push.txt" 2>&1 || fail "fixed Runner layer push failed"
	docker --config "$auth_dir" pull "$target_tag" >"$temporary/post-push-pull.txt" 2>&1 || fail "published tag pull verification failed"
fi

expected_metadata="linux|amd64|noncanonical-lab|$backend_sha256|$runner_revision|sha256:$old_digest|$runtime_sha256"
[ "$(image_metadata "$target_tag")" = "$expected_metadata" ] || fail "fixed Runner image metadata is invalid"
observed_runtime=$(docker run --rm --network none --entrypoint /usr/bin/sha256sum "$target_tag" "$runtime_path" |
	awk '{print $1}')
[ "$observed_runtime" = "$runtime_sha256" ] || fail "fixed Runner image contains the wrong runtime.py"
digest=$(resolve_digest)
[ "$(printf '%s\n' "$digest" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')" -eq 1 ] || fail "published digest is ambiguous"
printf '%s\n' "$digest" | grep -Eq "^$repository@sha256:[0-9a-f]{64}$" || fail "published digest is invalid"
docker --config "$auth_dir" pull "$digest" >"$temporary/digest-pull.txt" 2>&1 || fail "digest pull verification failed"
[ "$(image_metadata "$digest")" = "$expected_metadata" ] || fail "digest-pulled Runner image metadata changed"
docker image inspect "$digest" >"$temporary/image-inspect.json"
printf 'schema=vela-lab-fixed-runner-image-v1\nresult=PASS\npreexisting=%s\nimage=%s\nruntime_sha256=%s\ncompleted_at=%s\nproduction_gates=0/9\n' \
	"$published" "$digest" "$runtime_sha256" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$temporary/result.txt"
find "$temporary" -maxdepth 1 -type f ! -name SHA256SUMS -print | sort | xargs sha256sum >"$temporary/SHA256SUMS"
mv "$temporary" "$output"
temporary=
committed=true
printf 'schema=vela-lab-fixed-runner-image-v1 result=PASS image=%s receipt=%s production_gates=0/9\n' "$digest" "$output"

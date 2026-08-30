#!/bin/sh

set -eu
umask 077

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
# shellcheck disable=SC1091
. "$script_dir/gpu-operator.env"

state_dir=${1:-}
username_file=${2:-}
password_file=${3:-}
apply=${4:-}
resume=${5:-}
registry=10.1.200.17:5443
registry_host=10.1.200.17
registry_ca=/etc/docker/certs.d/10.1.200.17:5443/ca.crt
expected_image_count=4
docker_config=
netrc_file=
state_file=
state_tmp=

fail() {
	printf 'mirror-gpu-operator-images-worker: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	if [ -n "$state_tmp" ] && [ -e "$state_tmp" ]; then
		find "$state_tmp" -delete
	fi
	if [ -n "$docker_config" ] && [ -d "$docker_config" ]; then
		find "$docker_config" -xdev -mindepth 1 -delete
		rmdir "$docker_config"
	fi
}

check_root_secret() {
	path=$1
	label=$2
	[ -f "$path" ] && [ ! -L "$path" ] || fail "$label must be a regular file"
	[ "$(stat -c '%u:%g:%a' "$path")" = 0:0:600 ] ||
		fail "$label must be owned by root:root with mode 0600"
	[ "$(wc -c <"$path" | tr -d '[:space:]')" -le 4096 ] ||
		fail "$label is unexpectedly large"
	awk 'NR > 1 || index($0, "\r") { exit 1 } END { exit !(NR == 1 && length($0) > 0) }' "$path" ||
		fail "$label must contain exactly one non-empty line"
}

registry_request() {
	repository=$1
	reference=$2
	header_file=$3
	curl --silent --show-error \
		--head \
		--cacert "$registry_ca" \
		--netrc-file "$netrc_file" \
		--header 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json' \
		--dump-header "$header_file" \
		--output /dev/null \
		--write-out '%{http_code}' \
		"https://$registry/v2/$repository/manifests/$reference"
}

write_state() {
	state_status=$1
	state_tmp=$(mktemp "$state_dir/.mirror-gpu-operator.state.XXXXXX")
	printf 'schema=vela-gpu-operator-worker-mirror-v1\nversion=%s\nregistry=%s\nstatus=%s\n' \
		"$GPU_OPERATOR_VERSION" "$registry" "$state_status" >"$state_tmp"
	chown 0:0 "$state_tmp"
	chmod 0600 "$state_tmp"
	mv "$state_tmp" "$state_file"
	state_tmp=
}

check_resume_state() {
	[ -f "$state_file" ] && [ ! -L "$state_file" ] || fail "mirror state file is missing or invalid"
	[ "$(stat -c '%u:%g:%a' "$state_file")" = 0:0:600 ] ||
		fail "mirror state file must be owned by root:root with mode 0600"
	[ "$(wc -l <"$state_file" | tr -d '[:space:]')" -eq 4 ] || fail "mirror state file has an unexpected shape"
	grep -Fxq 'schema=vela-gpu-operator-worker-mirror-v1' "$state_file" || fail "mirror state schema is invalid"
	grep -Fxq "version=$GPU_OPERATOR_VERSION" "$state_file" || fail "mirror state version is invalid"
	grep -Fxq "registry=$registry" "$state_file" || fail "mirror state Registry is invalid"
	grep -Fxq 'status=in_progress' "$state_file" || fail "mirror state is not resumable"
}

target_parts() {
	target=$1
	target_path=${target#"$registry/"}
	target_reference=${target_path##*:}
	target_repository=${target_path%:*}
	[ "$target_repository" != "$target_path" ] || fail "target image has no tag: $target"
}

preflight_target() {
	target_parts "$1"
	header_file=$docker_config/preflight.headers
	status=$(registry_request "$target_repository" "$target_reference" "$header_file") ||
		fail "Registry preflight request failed for $1"
	case "$status" in
		404) ;;
		200) fail "Registry target already exists: $1" ;;
		*) fail "Registry preflight for $1 returned HTTP $status" ;;
	esac
}

verify_existing_target() {
	expected_digest=$1
	target=$2
	target_parts "$target"
	tag_headers=$docker_config/existing-tag.headers
	tag_status=$(registry_request "$target_repository" "$target_reference" "$tag_headers") ||
		fail "Registry tag verification request failed for $target"
	case "$tag_status" in
		404) return 1 ;;
		200) ;;
		*) fail "Registry tag verification for $target returned HTTP $tag_status" ;;
	esac
	tag_digest=$(awk 'tolower($1) == "docker-content-digest:" { sub("\r$", "", $2); print $2 }' "$tag_headers" | tail -n 1)
	[ "$tag_digest" = "$expected_digest" ] ||
		fail "Registry tag $target resolves to $tag_digest, expected $expected_digest"
	digest_headers=$docker_config/existing-digest.headers
	digest_status=$(registry_request "$target_repository" "$expected_digest" "$digest_headers") ||
		fail "Registry digest verification request failed for $target@$expected_digest"
	[ "$digest_status" = 200 ] ||
		fail "Registry digest verification returned HTTP $digest_status for $target@$expected_digest"
	remote_digest=$(awk 'tolower($1) == "docker-content-digest:" { sub("\r$", "", $2); print $2 }' "$digest_headers" | tail -n 1)
	[ "$remote_digest" = "$expected_digest" ] ||
		fail "Registry returned digest $remote_digest for $target@$expected_digest"
	published=$((published + 1))
	printf 'verified-existing=%s digest=%s\n' "$target" "$expected_digest"
	return 0
}

publish_image() {
	source=$1
	source_digest=$2
	target=$3
	if [ "$mode" = resume ] && verify_existing_target "$source_digest" "$target"; then
		return
	fi
	docker pull --platform linux/amd64 "$source@$source_digest" >/dev/null ||
		fail "pull failed for $source@$source_digest"
	platform=$(docker image inspect "$source@$source_digest" --format '{{.Os}}/{{.Architecture}}' 2>/dev/null) ||
		fail "pulled image is unavailable: $source@$source_digest"
	[ "$platform" = linux/amd64 ] || fail "$source@$source_digest has unexpected platform $platform"
	docker tag "$source@$source_digest" "$target"
	push_output=$(docker push "$target") || fail "push failed for $target"
	digest=$(printf '%s\n' "$push_output" |
		awk '/digest: sha256:[0-9a-f]{64}/ { for (i = 1; i <= NF; i++) if ($i == "digest:") print $(i + 1) }' |
		tail -n 1)
	[ "$digest" = "$source_digest" ] ||
		fail "push returned digest $digest for $target, expected $source_digest"
	target_parts "$target"
	header_file=$docker_config/digest.headers
	status=$(registry_request "$target_repository" "$digest" "$header_file") ||
		fail "Registry digest verification request failed for $target@$digest"
	[ "$status" = 200 ] || fail "Registry digest verification returned HTTP $status for $target@$digest"
	remote_digest=$(awk 'tolower($1) == "docker-content-digest:" { sub("\r$", "", $2); print $2 }' "$header_file" | tail -n 1)
	[ "$remote_digest" = "$digest" ] ||
		fail "Registry returned digest $remote_digest for $target@$digest"
	published=$((published + 1))
	printf 'published=%s digest=%s\n' "$target" "$digest"
}

case "$#:$apply:$resume" in
	4:--apply:) mode=initial ;;
	5:--apply:--resume) mode=resume ;;
	*) fail "usage: $0 <state-directory> <registry-username-file> <registry-password-file> --apply [--resume]" ;;
esac
[ "$(id -u)" -eq 0 ] || fail "run as root"
[ "$(hostname)" = ubuntu ] || fail "this command is restricted to the selected Worker 1 host"
for command_name in base64 curl docker grep ip jq nvidia-smi sha256sum stat systemctl zpool; do
	command -v "$command_name" >/dev/null 2>&1 || fail "$command_name is required"
done
[ "$(ip -j -4 address show dev eno1 | jq -r '.[0].addr_info[] | select(.family == "inet" and .scope == "global") | .local' | head -n 1)" = 10.1.200.19 ] ||
	fail "Worker 1 LAN identity is not 10.1.200.19 on eno1"
[ "$(systemctl is-active rke2-agent 2>/dev/null)" = active ] || fail "rke2-agent is not active"
[ "$(docker inspect --format '{{.State.Status}}/{{.State.Health.Status}}' vela-h3-mock-runner 2>/dev/null)" = running/healthy ] ||
	fail "persistent mock Runner is not healthy"
[ "$(zpool list -H -o health data)" = ONLINE ] || fail "Worker ZFS data pool is not ONLINE"
[ "$(nvidia-smi --query-compute-apps=pid --format=csv,noheader,nounits | sed '/^[[:space:]]*$/d' | wc -l | tr -d '[:space:]')" -eq 0 ] ||
	fail "Worker has active GPU compute processes"
[ -f "$registry_ca" ] && [ ! -L "$registry_ca" ] || fail "Registry CA is missing"

[ -d "$state_dir" ] && [ ! -L "$state_dir" ] || fail "state directory is invalid"
[ "$(stat -c '%u:%g:%a' "$state_dir")" = 0:0:700 ] ||
	fail "state directory must be owned by root:root with mode 0700"
state_file=$state_dir/.mirror-gpu-operator.state
check_root_secret "$username_file" registry-username-file
check_root_secret "$password_file" registry-password-file
username=$(sed -n '1p' "$username_file")
password=$(sed -n '1p' "$password_file")
printf '%s\n' "$username" | grep -Eq '^[A-Za-z0-9._-]+$' || fail "registry username contains unsupported characters"
printf '%s\n' "$password" | grep -Eq '^[A-Za-z0-9._~!$%&()*+,/:;<=>?@^{}|-]+$' || fail "registry password contains unsupported characters"

docker_config=$(mktemp -d /run/vela-gpu-operator-mirror.XXXXXX)
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
export DOCKER_CONFIG="$docker_config"
auth_file=$docker_config/registry.auth
printf '%s:%s' "$username" "$password" | base64 -w 0 >"$auth_file"
jq -n --arg registry "$registry" --rawfile auth "$auth_file" \
	'{auths: {($registry): {auth: $auth}}}' >"$docker_config/config.json"
netrc_file=$docker_config/registry.netrc
printf 'machine %s\nlogin %s\npassword %s\n' \
	"$registry_host" "$username" "$password" >"$netrc_file"
chmod 0600 "$docker_config/config.json" "$netrc_file"
probe_status=$(curl --silent --show-error --cacert "$registry_ca" --netrc-file "$netrc_file" \
	--output /dev/null --write-out '%{http_code}' "https://$registry/v2/") ||
	fail "authenticated Registry probe failed"
[ "$probe_status" = 200 ] || fail "authenticated Registry probe returned HTTP $probe_status"

if [ "$mode" = initial ]; then
	[ ! -e "$state_file" ] && [ ! -L "$state_file" ] || fail "mirror state already exists; inspect it before using --resume"
	preflight_target "$registry/nvidia/gpu-operator:$GPU_OPERATOR_VERSION"
	preflight_target "$registry/nvidia/k8s-device-plugin:${GPU_DEVICE_PLUGIN_IMAGE##*:}"
	preflight_target "$registry/nvidia/k8s/dcgm-exporter:${GPU_DCGM_EXPORTER_IMAGE##*:}"
	preflight_target "$registry/nfd/node-feature-discovery:${GPU_NFD_IMAGE##*:}"
	write_state in_progress
else
	check_resume_state
fi

published=0
publish_image "$GPU_OPERATOR_IMAGE" "$GPU_OPERATOR_AMD64_MANIFEST" \
	"$registry/nvidia/gpu-operator:$GPU_OPERATOR_VERSION"
publish_image "$GPU_DEVICE_PLUGIN_IMAGE" "$GPU_DEVICE_PLUGIN_AMD64_MANIFEST" \
	"$registry/nvidia/k8s-device-plugin:${GPU_DEVICE_PLUGIN_IMAGE##*:}"
publish_image "$GPU_DCGM_EXPORTER_IMAGE" "$GPU_DCGM_EXPORTER_AMD64_MANIFEST" \
	"$registry/nvidia/k8s/dcgm-exporter:${GPU_DCGM_EXPORTER_IMAGE##*:}"
publish_image "$GPU_NFD_IMAGE" "$GPU_NFD_AMD64_MANIFEST" \
	"$registry/nfd/node-feature-discovery:${GPU_NFD_IMAGE##*:}"

[ "$published" -eq "$expected_image_count" ] || fail "published only $published images"
write_state complete
printf 'result=PASS role=worker-1 version=%s published=%s registry=%s mode=%s source_images=retained prune=none\n' \
	"$GPU_OPERATOR_VERSION" "$published" "$registry" "$mode"

#!/bin/sh

set -eu
umask 077

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
# shellcheck disable=SC1091
. "$script_dir/images.env"

state_dir=${1:-}
username_file=${2:-}
password_file=${3:-}
apply=${4:-}
resume=${5:-}
registry=10.1.200.17:5443
registry_host=10.1.200.17
registry_ca=/etc/vela-registry/tls/ca.crt
docker_registry_ca=/etc/docker/certs.d/10.1.200.17:5443/ca.crt
docker_config=
netrc_file=
state_tmp=
state_file=

fail() {
	printf 'publish-vela-lab-observability-images: %s\n' "$*" >&2
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
	[ "$(stat -c '%u:%g:%a' "$path")" = 0:0:600 ] || fail "$label must be owned by root:root with mode 0600"
	[ "$(wc -c <"$path" | tr -d '[:space:]')" -le 4096 ] || fail "$label is unexpectedly large"
	awk 'NR > 1 || index($0, "\r") { exit 1 } END { exit !(NR == 1 && length($0) > 0) }' "$path" ||
		fail "$label must contain exactly one non-empty line"
}

target_parts() {
	target=$1
	target_path=${target#"$registry/"}
	target_reference=${target_path##*:}
	target_repository=${target_path%:*}
	[ "$target_repository" != "$target_path" ] || fail "target image has no tag: $target"
}

registry_request() {
	repository=$1
	reference=$2
	header_file=$3
	curl --silent --show-error --head --cacert "$registry_ca" --netrc-file "$netrc_file" \
		--header 'Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
		--dump-header "$header_file" --output /dev/null --write-out '%{http_code}' \
		"https://$registry/v2/$repository/manifests/$reference"
}

remote_digest() {
	awk 'tolower($1) == "docker-content-digest:" { sub("\r$", "", $2); print $2 }' "$1" | tail -n 1
}

write_state() {
	status=$1
	state_tmp=$(mktemp "$state_dir/.publish-observability.state.XXXXXX")
	printf 'schema=vela-lab-observability-publish-v1\nregistry=%s\nimage_count=3\ncompressed_source_bytes=624172137\nstatus=%s\n' \
		"$registry" "$status" >"$state_tmp"
	chown 0:0 "$state_tmp"
	chmod 0600 "$state_tmp"
	mv "$state_tmp" "$state_file"
	state_tmp=
}

check_resume_state() {
	[ -f "$state_file" ] && [ ! -L "$state_file" ] || fail "publication state is missing or invalid"
	[ "$(stat -c '%u:%g:%a' "$state_file")" = 0:0:600 ] || fail "publication state mode or owner drifted"
	grep -Fxq 'schema=vela-lab-observability-publish-v1' "$state_file" || fail "publication state schema drifted"
	grep -Fxq "registry=$registry" "$state_file" || fail "publication state Registry drifted"
	grep -Fxq 'image_count=3' "$state_file" || fail "publication image count drifted"
	grep -Fxq 'compressed_source_bytes=624172137' "$state_file" || fail "publication size inventory drifted"
	grep -Fxq 'status=in_progress' "$state_file" || fail "publication state is not resumable"
}

verify_existing_target() {
	expected_digest=$1
	target=$2
	target_parts "$target"
	headers=$docker_config/existing.headers
	status=$(registry_request "$target_repository" "$target_reference" "$headers") || fail "Registry request failed for $target"
	[ "$status" = 200 ] || return 1
	[ "$(remote_digest "$headers")" = "$expected_digest" ] || fail "$target resolves to an unexpected digest"
	printf 'verified-existing=%s digest=%s\n' "$target" "$expected_digest"
	published=$((published + 1))
	return 0
}

preflight_target() {
	target=$1
	target_parts "$target"
	headers=$docker_config/preflight.headers
	status=$(registry_request "$target_repository" "$target_reference" "$headers") || fail "Registry preflight failed for $target"
	case "$status" in
		404) ;;
		200) fail "Registry target already exists: $target" ;;
		*) fail "Registry preflight for $target returned HTTP $status" ;;
	esac
}

publish_image() {
	source=$1
	digest=$2
	target=$3
	if [ "$mode" = resume ] && verify_existing_target "$digest" "$target"; then
		return
	fi
	docker pull --platform linux/amd64 "$source@$digest" >/dev/null || fail "pull failed for $source@$digest"
	platform=$(docker image inspect "$source@$digest" --format '{{.Os}}/{{.Architecture}}') || fail "pulled image is unavailable"
	[ "$platform" = linux/amd64 ] || fail "$source@$digest has unexpected platform $platform"
	docker tag "$source@$digest" "$target"
	push_output=$(docker push "$target") || fail "push failed for $target"
	push_digest=$(printf '%s\n' "$push_output" | awk '/digest: sha256:/ { for (i = 1; i <= NF; i++) if ($i == "digest:") print $(i + 1) }' | tail -n 1)
	[ "$push_digest" = "$digest" ] || fail "push returned digest $push_digest for $target, expected $digest"
	target_parts "$target"
	headers=$docker_config/published.headers
	status=$(registry_request "$target_repository" "$digest" "$headers") || fail "digest verification failed for $target"
	[ "$status" = 200 ] && [ "$(remote_digest "$headers")" = "$digest" ] || fail "private digest verification failed for $target"
	printf 'published=%s digest=%s\n' "$target" "$digest"
	published=$((published + 1))
}

case "$#:$apply:$resume" in
	4:--apply:) mode=initial ;;
	5:--apply:--resume) mode=resume ;;
	*) fail "usage: $0 <state-directory> <registry-username-file> <registry-password-file> --apply [--resume]" ;;
esac
[ "$(id -u)" -eq 0 ] || fail "run as root"
[ "$(hostname)" = marslab-server ] || fail "run only on marslab-server"
for command_name in base64 curl docker grep ip jq sha256sum stat; do
	command -v "$command_name" >/dev/null 2>&1 || fail "$command_name is required"
done
[ "$(ip -j -4 address show dev enp34s0f0 | jq -r '.[0].addr_info[] | select(.family == "inet" and .scope == "global") | .local' | head -n 1)" = 10.1.200.17 ] || fail "control LAN identity drifted"
[ "$(docker inspect --format '{{.Id}}' fchip-4591d89ff18127a74b8a25a0 2>/dev/null)" = b0a653da3926e90d88a6d3329fab8a927456e23ddfd6acb7d7d40cf6f9db0c94 ] || fail "shared experiment container identity changed"
[ "$(docker inspect --format '{{.State.Status}}' fchip-4591d89ff18127a74b8a25a0 2>/dev/null)" = running ] || fail "shared experiment container is not running"
[ "$(docker inspect --format '{{.Id}}' vela-registry 2>/dev/null)" = 2bd86fd8f7db91609a430dd8e12402bb5eb5def9454f297994f51ab9c1571d68 ] || fail "Registry container identity changed"
[ "$(docker inspect --format '{{.State.Status}}' vela-registry 2>/dev/null)" = running ] || fail "Registry is not running"
[ -f "$registry_ca" ] && [ -f "$docker_registry_ca" ] || fail "Registry CA trust is missing"
[ "$(sha256sum "$registry_ca" | cut -d ' ' -f 1)" = "$(sha256sum "$docker_registry_ca" | cut -d ' ' -f 1)" ] || fail "Docker Registry CA trust drifted"
[ -d "$state_dir" ] && [ ! -L "$state_dir" ] || fail "state directory is invalid"
[ "$(stat -c '%u:%g:%a' "$state_dir")" = 0:0:700 ] || fail "state directory must be root:root mode 0700"
state_file=$state_dir/.publish-observability.state

check_root_secret "$username_file" registry-username-file
check_root_secret "$password_file" registry-password-file
username=$(sed -n '1p' "$username_file")
password=$(sed -n '1p' "$password_file")
printf '%s\n' "$username" | grep -Eq '^[A-Za-z0-9._-]+$' || fail "registry username contains unsupported characters"
printf '%s\n' "$password" | grep -Eq '^[A-Za-z0-9._~!$%&()*+,/:;<=>?@^{}|-]+$' || fail "registry password contains unsupported characters"

docker_config=$(mktemp -d /run/vela-observability-publish.XXXXXX)
trap cleanup EXIT HUP INT TERM
export DOCKER_CONFIG="$docker_config"
auth_file=$docker_config/registry.auth
printf '%s:%s' "$username" "$password" | base64 -w 0 >"$auth_file"
jq -n --arg registry "$registry" --rawfile auth "$auth_file" '{auths: {($registry): {auth: $auth}}}' >"$docker_config/config.json"
netrc_file=$docker_config/registry.netrc
printf 'machine %s\nlogin %s\npassword %s\n' "$registry_host" "$username" "$password" >"$netrc_file"
chmod 0600 "$docker_config/config.json" "$netrc_file"

probe_status=$(curl --silent --show-error --cacert "$registry_ca" --netrc-file "$netrc_file" --output /dev/null --write-out '%{http_code}' "https://$registry/v2/") || fail "authenticated Registry probe failed"
[ "$probe_status" = 200 ] || fail "authenticated Registry probe returned HTTP $probe_status"

if [ "$mode" = initial ]; then
	[ ! -e "$state_file" ] && [ ! -L "$state_file" ] || fail "publication state already exists; inspect before resuming"
	preflight_target "$PROMETHEUS_TARGET"
	preflight_target "$ALERTMANAGER_TARGET"
	preflight_target "$GRAFANA_TARGET"
	write_state in_progress
else
	check_resume_state
fi

published=0
publish_image "$PROMETHEUS_SOURCE" "$PROMETHEUS_AMD64_MANIFEST" "$PROMETHEUS_TARGET"
publish_image "$ALERTMANAGER_SOURCE" "$ALERTMANAGER_AMD64_MANIFEST" "$ALERTMANAGER_TARGET"
publish_image "$GRAFANA_SOURCE" "$GRAFANA_AMD64_MANIFEST" "$GRAFANA_TARGET"
[ "$published" -eq 3 ] || fail "published only $published images"
write_state complete
printf 'schema=vela-lab-observability-publish-v1 result=PASS images=3 compressed_source_bytes=624172137 registry=%s mode=%s source_images=retained prune=none\n' "$registry" "$mode"

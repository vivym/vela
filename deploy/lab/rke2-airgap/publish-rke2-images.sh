#!/bin/sh

set -eu
umask 077

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
# shellcheck disable=SC1091
. "$script_dir/release.env"

artifact_dir=${1:-}
username_file=${2:-}
password_file=${3:-}
apply=${4:-}
resume=${5:-}
registry=10.1.200.17:5443
registry_host=10.1.200.17
registry_ca=/etc/vela-registry/tls/ca.crt
docker_registry_ca=/etc/docker/certs.d/10.1.200.17:5443/ca.crt
registry_probe_repository=vela-lab/vela-h3-runner
registry_probe_digest=sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3
daemon_probe=10.1.200.17:5443/vela-lab/busybox@sha256:7a3ebe5bfd1a4a19797d20b0c0bb39d44393e9a03fd852c0865b0f540d868df0
expected_image_count=17
docker_config=
netrc_file=
state_file=
state_tmp=

fail() {
	printf 'publish-rke2-images: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	if [ -n "$state_tmp" ] && [ -e "$state_tmp" ]; then
		rm -f "$state_tmp"
	fi
	if [ -n "$docker_config" ] && [ -d "$docker_config" ]; then
		find "$docker_config" -xdev -mindepth 1 -delete
		rmdir "$docker_config"
	fi
}

check_file() {
	path=$1
	expected_size=$2
	expected_sha256=$3
	[ -f "$path" ] && [ ! -L "$path" ] || fail "$path must be a regular file"
	actual_size=$(wc -c <"$path" | tr -d '[:space:]')
	[ "$actual_size" = "$expected_size" ] ||
		fail "$path size is $actual_size, expected $expected_size"
	actual_sha256=$(sha256sum "$path" | awk '{print $1}')
	[ "$actual_sha256" = "$expected_sha256" ] ||
		fail "$path sha256 is $actual_sha256, expected $expected_sha256"
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
	method=$1
	repository=$2
	reference=$3
	header_file=$4
	[ "$method" = HEAD ] || fail "unsupported Registry request method: $method"
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
	state_tmp=$(mktemp "$artifact_dir/.publish-rke2-images.state.XXXXXX")
	printf 'schema=vela-rke2-publish-v1\nrelease=%s\narchive_sha256=%s\nregistry=%s\nstatus=%s\n' \
		"$RKE2_VERSION" "$RKE2_IMAGES_SHA256" "$registry" "$state_status" >"$state_tmp"
	chown 0:0 "$state_tmp"
	chmod 0600 "$state_tmp"
	mv "$state_tmp" "$state_file"
	state_tmp=
}

check_resume_state() {
	[ -f "$state_file" ] && [ ! -L "$state_file" ] || fail "publication state file is missing or invalid"
	[ "$(stat -c '%u:%g:%a' "$state_file")" = 0:0:600 ] ||
		fail "publication state file must be owned by root:root with mode 0600"
	[ "$(wc -l <"$state_file" | tr -d '[:space:]')" -eq 5 ] || fail "publication state file has an unexpected shape"
	grep -Fxq 'schema=vela-rke2-publish-v1' "$state_file" || fail "publication state schema is invalid"
	grep -Fxq "release=$RKE2_VERSION" "$state_file" || fail "publication state release is invalid"
	grep -Fxq "archive_sha256=$RKE2_IMAGES_SHA256" "$state_file" || fail "publication state archive is invalid"
	grep -Fxq "registry=$registry" "$state_file" || fail "publication state Registry is invalid"
	grep -Fxq 'status=in_progress' "$state_file" || fail "publication state is not resumable"
}

case "$#:$apply:$resume" in
	4:--apply:) mode=initial ;;
	5:--apply:--resume) mode=resume ;;
	*) fail "usage: $0 <artifact-directory> <registry-username-file> <registry-password-file> --apply [--resume]" ;;
esac
[ "$(id -u)" -eq 0 ] || fail "run as root"
[ "$(hostname)" = marslab-server ] || fail "this command is restricted to marslab-server"
for command_name in base64 curl docker grep ip jq sha256sum stat; do
	command -v "$command_name" >/dev/null 2>&1 || fail "$command_name is required"
done
[ "$(ip -j -4 address show dev enp34s0f0 | jq -r '.[0].addr_info[] | select(.family == "inet" and .scope == "global") | .local' | head -n 1)" = 10.1.200.17 ] ||
	fail "marslab LAN identity is not 10.1.200.17 on enp34s0f0"
[ "$(docker inspect --format '{{.Id}}' fchip-4591d89ff18127a74b8a25a0 2>/dev/null)" = b0a653da3926e90d88a6d3329fab8a927456e23ddfd6acb7d7d40cf6f9db0c94 ] ||
	fail "shared experiment container identity changed"
[ "$(docker inspect --format '{{.State.Status}}' vela-registry 2>/dev/null)" = running ] ||
	fail "vela-registry is not running"
[ -f "$registry_ca" ] && [ ! -L "$registry_ca" ] || fail "Registry CA is missing"
[ -f "$docker_registry_ca" ] && [ ! -L "$docker_registry_ca" ] ||
	fail "Docker Registry CA trust is missing at $docker_registry_ca"
[ "$(sha256sum "$registry_ca" | cut -d ' ' -f 1)" = "$(sha256sum "$docker_registry_ca" | cut -d ' ' -f 1)" ] ||
	fail "Docker Registry CA trust does not match the Registry CA"

[ -d "$artifact_dir" ] && [ ! -L "$artifact_dir" ] || fail "artifact directory is invalid"
state_file=$artifact_dir/.publish-rke2-images.state
archive=$artifact_dir/$RKE2_IMAGES_FILE
image_list=$artifact_dir/$RKE2_IMAGE_LIST_FILE
checksum_file=$artifact_dir/$RKE2_CHECKSUM_FILE
check_file "$archive" "$RKE2_IMAGES_SIZE" "$RKE2_IMAGES_SHA256"
check_file "$image_list" "$RKE2_IMAGE_LIST_SIZE" "$RKE2_IMAGE_LIST_SHA256"
check_file "$checksum_file" "$RKE2_CHECKSUM_SIZE" "$RKE2_CHECKSUM_SHA256"
awk -v file="$RKE2_IMAGES_FILE" -v sha="$RKE2_IMAGES_SHA256" \
	'$1 == sha && $2 == file { found = 1 } END { exit !found }' "$checksum_file" ||
	fail "$checksum_file does not authenticate $RKE2_IMAGES_FILE"

image_count=$(wc -l <"$image_list" | tr -d '[:space:]')
[ "$image_count" -eq "$expected_image_count" ] ||
	fail "image inventory contains $image_count lines, expected $expected_image_count"
[ "$(sort -u "$image_list" | wc -l | tr -d '[:space:]')" -eq "$expected_image_count" ] ||
	fail "image inventory contains duplicate references"
if grep -Evq '^docker\.io/rancher/[a-z0-9._/-]+:[A-Za-z0-9._+-]+$' "$image_list"; then
	fail "image inventory contains an unexpected reference"
fi

check_root_secret "$username_file" registry-username-file
check_root_secret "$password_file" registry-password-file
username=$(sed -n '1p' "$username_file")
password=$(sed -n '1p' "$password_file")
printf '%s\n' "$username" | grep -Eq '^[A-Za-z0-9._-]+$' ||
	fail "registry username contains unsupported characters"
printf '%s\n' "$password" | grep -Eq '^[A-Za-z0-9._~!$%&()*+,/:;<=>?@^{}|-]+$' ||
	fail "registry password contains unsupported characters"

docker_config=$(mktemp -d /run/vela-rke2-publish.XXXXXX)
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
probe_headers=$docker_config/probe.headers
probe_status=$(registry_request HEAD "$registry_probe_repository" "$registry_probe_digest" "$probe_headers") ||
	fail "authenticated Registry probe failed"
[ "$probe_status" = 200 ] || fail "authenticated Registry probe returned HTTP $probe_status"
docker pull "$daemon_probe" >/dev/null || fail "Docker daemon Registry trust/authentication probe failed"
[ "$(docker image inspect "$daemon_probe" --format '{{.Os}}/{{.Architecture}}' 2>/dev/null)" = linux/amd64 ] ||
	fail "Docker daemon Registry probe has an unexpected platform"

if [ "$mode" = initial ]; then
	[ ! -e "$state_file" ] && [ ! -L "$state_file" ] || fail "publication state already exists; inspect it before using --resume"
	while IFS= read -r source; do
		[ -n "$source" ] || fail "image inventory contains an empty reference"
		target=$registry/${source#docker.io/}
		if docker image inspect "$source" >/dev/null 2>&1; then
			fail "source image already exists locally: $source"
		fi
		if docker image inspect "$target" >/dev/null 2>&1; then
			fail "target image already exists locally: $target"
		fi
		repository=${target#"$registry/"}
		reference=${repository##*:}
		repository=${repository%:*}
		preflight_headers=$docker_config/preflight.headers
		preflight_status=$(registry_request HEAD "$repository" "$reference" "$preflight_headers") ||
			fail "Registry preflight request failed for $target"
		case "$preflight_status" in
			404) ;;
			200) fail "Registry target already exists: $target" ;;
			*) fail "Registry preflight for $target returned HTTP $preflight_status" ;;
		esac
	done <"$image_list"
	write_state in_progress
else
	check_resume_state
fi

docker image load --input "$archive"

published=0
while IFS= read -r source; do
	target=$registry/${source#docker.io/}
	platform=$(docker image inspect "$source" --format '{{.Os}}/{{.Architecture}}' 2>/dev/null) ||
		fail "archive did not load expected image: $source"
	[ "$platform" = linux/amd64 ] || fail "$source has unexpected platform $platform"
	docker tag "$source" "$target"
	push_output=$(docker push "$target") || fail "push failed for $target"
	digest=$(printf '%s\n' "$push_output" |
		awk '/digest: sha256:[0-9a-f]{64}/ { for (i = 1; i <= NF; i++) if ($i == "digest:") print $(i + 1) }' |
		tail -n 1)
	printf '%s\n' "$digest" | grep -Eq '^sha256:[0-9a-f]{64}$' ||
		fail "push did not return a manifest digest for $target"
	repository=${target#"$registry/"}
	repository=${repository%:*}
	digest_headers=$docker_config/digest.headers
	digest_status=$(registry_request HEAD "$repository" "$digest" "$digest_headers") ||
		fail "Registry digest verification request failed for $target@$digest"
	[ "$digest_status" = 200 ] ||
		fail "Registry digest verification returned HTTP $digest_status for $target@$digest"
	remote_digest=$(awk 'tolower($1) == "docker-content-digest:" { sub("\\r$", "", $2); print $2 }' "$digest_headers" | tail -n 1)
	[ "$remote_digest" = "$digest" ] ||
		fail "Registry returned digest $remote_digest for $target@$digest"
	published=$((published + 1))
	printf 'published=%s digest=%s\n' "$target" "$digest"
done <"$image_list"

[ "$published" -eq "$expected_image_count" ] || fail "published only $published images"
write_state complete
printf 'result=PASS release=%s published=%s registry=%s mode=%s source_images=retained daemon_probe=retained state=%s prune=none\n' \
	"$RKE2_VERSION" "$published" "$registry" "$mode" "$state_file"

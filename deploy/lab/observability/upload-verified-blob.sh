#!/bin/sh

set -eu
umask 077

repository=${1:-}
blob=${2:-}
expected_bytes=${3:-}
expected_digest=${4:-}
username_file=${5:-}
password_file=${6:-}
apply=${7:-}
registry=10.1.200.17:5443
registry_host=10.1.200.17
registry_ca=/etc/vela-registry/tls/ca.crt
temporary=
netrc_file=

fail() {
	printf 'upload-vela-lab-verified-blob: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	if [ -n "$temporary" ] && [ -d "$temporary" ]; then
		find "$temporary" -xdev -mindepth 1 -delete
		rmdir "$temporary"
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

registry_head() {
	header_file=$1
	curl --silent --show-error --head --cacert "$registry_ca" --netrc-file "$netrc_file" \
		--dump-header "$header_file" --output /dev/null --write-out '%{http_code}' \
		"https://$registry/v2/$repository/blobs/$expected_digest"
}

[ "$#" -eq 7 ] && [ "$apply" = --apply ] || fail "usage: $0 <repository> <blob-file> <expected-bytes> <sha256-digest> <registry-username-file> <registry-password-file> --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root"
[ "$(hostname)" = marslab-server ] || fail "run only on marslab-server"
case "$repository" in
	observability/prometheus | observability/alertmanager | observability/grafana) ;;
	*) fail "repository is outside the fixed observability namespace" ;;
esac
printf '%s\n' "$expected_bytes" | grep -Eq '^[1-9][0-9]*$' || fail "expected byte count is invalid"
printf '%s\n' "$expected_digest" | grep -Eq '^sha256:[0-9a-f]{64}$' || fail "expected digest is invalid"
[ -f "$blob" ] && [ ! -L "$blob" ] || fail "blob must be a regular file"
[ "$(stat -c '%u:%g:%a' "$blob")" = 0:0:600 ] || fail "blob must be root:root mode 0600"
[ "$(wc -c <"$blob" | tr -d '[:space:]')" = "$expected_bytes" ] || fail "blob size does not match $expected_bytes"
[ "sha256:$(sha256sum "$blob" | awk '{print $1}')" = "$expected_digest" ] || fail "blob digest does not match $expected_digest"
check_root_secret "$username_file" registry-username-file
check_root_secret "$password_file" registry-password-file
for command_name in curl docker grep ip jq sha256sum stat; do
	command -v "$command_name" >/dev/null 2>&1 || fail "$command_name is required"
done
[ "$(ip -j -4 address show dev enp34s0f0 | jq -r '.[0].addr_info[] | select(.family == "inet" and .scope == "global") | .local' | head -n 1)" = 10.1.200.17 ] || fail "control LAN identity drifted"
[ "$(docker inspect --format '{{.Id}}' vela-registry 2>/dev/null)" = 2bd86fd8f7db91609a430dd8e12402bb5eb5def9454f297994f51ab9c1571d68 ] || fail "Registry container identity changed"
[ "$(docker inspect --format '{{.State.Status}}' vela-registry 2>/dev/null)" = running ] || fail "Registry is not running"
[ "$(docker inspect --format '{{.Id}}' fchip-4591d89ff18127a74b8a25a0 2>/dev/null)" = b0a653da3926e90d88a6d3329fab8a927456e23ddfd6acb7d7d40cf6f9db0c94 ] || fail "shared experiment container identity changed"
[ "$(docker inspect --format '{{.State.Status}}' fchip-4591d89ff18127a74b8a25a0 2>/dev/null)" = running ] || fail "shared experiment container is not running"

temporary=$(mktemp -d /run/vela-observability-blob.XXXXXX)
trap cleanup EXIT HUP INT TERM
netrc_file=$temporary/registry.netrc
username=$(sed -n '1p' "$username_file")
password=$(sed -n '1p' "$password_file")
printf 'machine %s\nlogin %s\npassword %s\n' "$registry_host" "$username" "$password" >"$netrc_file"
chmod 0600 "$netrc_file"

before_headers=$temporary/before.headers
before_status=$(registry_head "$before_headers") || fail "Registry blob preflight failed"
case "$before_status" in
	200)
		observed=$(awk 'tolower($1) == "docker-content-digest:" { sub("\r$", "", $2); print $2 }' "$before_headers" | tail -n 1)
		[ "$observed" = "$expected_digest" ] || fail "existing blob returned digest $observed"
		printf 'schema=vela-lab-observability-blob-upload-v1 result=PASS action=verified-existing repository=%s digest=%s bytes=%s\n' "$repository" "$expected_digest" "$expected_bytes"
		exit 0
		;;
	404) ;;
	*) fail "Registry blob preflight returned HTTP $before_status" ;;
esac

post_headers=$temporary/post.headers
post_status=$(curl --silent --show-error --request POST --cacert "$registry_ca" --netrc-file "$netrc_file" \
	--dump-header "$post_headers" --output /dev/null --write-out '%{http_code}' \
	"https://$registry/v2/$repository/blobs/uploads/") || fail "Registry upload initialization failed"
[ "$post_status" = 202 ] || fail "Registry upload initialization returned HTTP $post_status"
location=$(awk 'tolower($1) == "location:" { sub("\r$", "", $2); print $2 }' "$post_headers" | tail -n 1)
case "$location" in
	https://10.1.200.17:5443/v2/observability/*/blobs/uploads/*) ;;
	/v2/observability/*/blobs/uploads/*) location="https://$registry$location" ;;
	*) fail "Registry returned an unexpected upload location" ;;
esac
case "$location" in *\?*) separator='&' ;; *) separator='?' ;; esac

put_headers=$temporary/put.headers
put_status=$(curl --silent --show-error --request PUT --cacert "$registry_ca" --netrc-file "$netrc_file" \
	--header 'Content-Type: application/octet-stream' --data-binary "@$blob" \
	--dump-header "$put_headers" --output /dev/null --write-out '%{http_code}' \
	"$location${separator}digest=$expected_digest") || fail "Registry blob PUT failed"
[ "$put_status" = 201 ] || fail "Registry blob PUT returned HTTP $put_status"
put_digest=$(awk 'tolower($1) == "docker-content-digest:" { sub("\r$", "", $2); print $2 }' "$put_headers" | tail -n 1)
[ "$put_digest" = "$expected_digest" ] || fail "Registry PUT returned digest $put_digest"

after_headers=$temporary/after.headers
after_status=$(registry_head "$after_headers") || fail "Registry blob postflight failed"
[ "$after_status" = 200 ] || fail "Registry blob postflight returned HTTP $after_status"
after_digest=$(awk 'tolower($1) == "docker-content-digest:" { sub("\r$", "", $2); print $2 }' "$after_headers" | tail -n 1)
[ "$after_digest" = "$expected_digest" ] || fail "Registry postflight returned digest $after_digest"
printf 'schema=vela-lab-observability-blob-upload-v1 result=PASS action=uploaded repository=%s digest=%s bytes=%s\n' "$repository" "$expected_digest" "$expected_bytes"

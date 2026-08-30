#!/bin/sh

set -eu
umask 077

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
# shellcheck disable=SC1091
. "$script_dir/release.env"

artifact_dir=${1:-}
mode=${2:---metadata-only}
download_prefix=${RKE2_DOWNLOAD_PREFIX:-}

fail() {
	printf 'fetch-rke2-airgap: %s\n' "$*" >&2
	exit 1
}

sha256_file() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | awk '{print $1}'
	elif command -v shasum >/dev/null 2>&1; then
		shasum -a 256 "$1" | awk '{print $1}'
	else
		fail "sha256sum or shasum is required"
	fi
}

check_file() {
	path=$1
	expected_size=$2
	expected_sha256=$3
	actual_size=$(wc -c <"$path" | tr -d '[:space:]')
	[ "$actual_size" = "$expected_size" ] ||
		fail "$path size is $actual_size, expected $expected_size"
	actual_sha256=$(sha256_file "$path")
	[ "$actual_sha256" = "$expected_sha256" ] ||
		fail "$path sha256 is $actual_sha256, expected $expected_sha256"
}

fetch_file() {
	name=$1
	canonical_url=$2
	expected_size=$3
	expected_sha256=$4
	destination=$artifact_dir/$name
	partial=$destination.part
	url=${download_prefix}${canonical_url}

	if [ -f "$destination" ]; then
		[ ! -L "$destination" ] || fail "$destination must not be a symbolic link"
		check_file "$destination" "$expected_size" "$expected_sha256"
		chmod 0600 "$destination"
		printf 'verified=%s bytes=%s\n' "$destination" "$expected_size"
		return
	fi
	[ ! -e "$destination" ] || fail "$destination exists and is not a regular file"
	[ ! -e "$partial" ] || [ -f "$partial" ] ||
		fail "$partial exists and is not a regular file"
	[ ! -L "$partial" ] || fail "$partial must not be a symbolic link"
	if [ -f "$partial" ]; then
		chmod 0600 "$partial"
	fi

	curl --fail --location --retry 5 --retry-all-errors \
		--connect-timeout 15 --speed-limit 1024 --speed-time 120 \
		--continue-at - --output "$partial" "$url"
	chmod 0600 "$partial"
	check_file "$partial" "$expected_size" "$expected_sha256"
	mv "$partial" "$destination"
	chmod 0600 "$destination"
	printf 'downloaded=%s bytes=%s\n' "$destination" "$expected_size"
}

[ -n "$artifact_dir" ] ||
	fail "usage: $0 <artifact-directory> [--metadata-only|--all]"
case "$mode" in
	--metadata-only | --all) ;;
	*) fail "mode must be --metadata-only or --all" ;;
esac
case "$download_prefix" in
	"") transport=direct ;;
	https://*/)
		case "$download_prefix" in
			*"@"* | *"?"* | *"#"*) fail "download prefix must not contain credentials, a query, or a fragment" ;;
		esac
		transport=$download_prefix
		;;
	*) fail "RKE2_DOWNLOAD_PREFIX must be empty or an HTTPS URL ending in /" ;;
esac
command -v curl >/dev/null 2>&1 || fail "curl is required"
[ "$artifact_dir" != / ] || fail "artifact directory must not be /"
[ ! -L "$artifact_dir" ] || fail "artifact directory must not be a symbolic link"
mkdir -p "$artifact_dir"
[ -d "$artifact_dir" ] || fail "$artifact_dir is not a directory"
chmod 0700 "$artifact_dir"

fetch_file "$RKE2_CHECKSUM_FILE" \
	"$RKE2_RELEASE_BASE_URL/$RKE2_CHECKSUM_FILE" \
	"$RKE2_CHECKSUM_SIZE" "$RKE2_CHECKSUM_SHA256"
fetch_file "$RKE2_IMAGE_LIST_FILE" \
	"$RKE2_RELEASE_BASE_URL/$RKE2_IMAGE_LIST_FILE" \
	"$RKE2_IMAGE_LIST_SIZE" "$RKE2_IMAGE_LIST_SHA256"
fetch_file "$RKE2_INSTALL_FILE" "$RKE2_INSTALL_URL" \
	"$RKE2_INSTALL_SIZE" "$RKE2_INSTALL_SHA256"

if [ "$mode" = --all ]; then
	fetch_file "$RKE2_BINARY_FILE" \
		"$RKE2_RELEASE_BASE_URL/$RKE2_BINARY_FILE" \
		"$RKE2_BINARY_SIZE" "$RKE2_BINARY_SHA256"
	fetch_file "$RKE2_IMAGES_FILE" \
		"$RKE2_RELEASE_BASE_URL/$RKE2_IMAGES_FILE" \
		"$RKE2_IMAGES_SIZE" "$RKE2_IMAGES_SHA256"
fi

awk -v file="$RKE2_BINARY_FILE" -v sha="$RKE2_BINARY_SHA256" \
	'$1 == sha && $2 == file { found = 1 } END { exit !found }' \
	"$artifact_dir/$RKE2_CHECKSUM_FILE" ||
	fail "$RKE2_CHECKSUM_FILE does not authenticate $RKE2_BINARY_FILE"
awk -v file="$RKE2_IMAGES_FILE" -v sha="$RKE2_IMAGES_SHA256" \
	'$1 == sha && $2 == file { found = 1 } END { exit !found }' \
	"$artifact_dir/$RKE2_CHECKSUM_FILE" ||
	fail "$RKE2_CHECKSUM_FILE does not authenticate $RKE2_IMAGES_FILE"

if [ "$mode" = --all ]; then
	printf 'release=%s commit=%s complete=true bytes=%s transport=%s\n' \
		"$RKE2_VERSION" "$RKE2_TAG_COMMIT" "$RKE2_ALL_TRANSFER_BYTES" "$transport"
else
	printf 'release=%s commit=%s complete=false mode=metadata-only transport=%s\n' \
		"$RKE2_VERSION" "$RKE2_TAG_COMMIT" "$transport"
fi

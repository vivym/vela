#!/bin/sh

set -eu
umask 077

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
# shellcheck disable=SC1091
. "$script_dir/release.env"

arg_count=$#
role=${1:-}
artifact_dir=${2:-}
registry_ca_source=${3:-}
username_file=${4:-}
password_file=${5:-}
token_file=
address_confirmation=
address_policy=
swap_confirmation=
apply=
registry=10.1.200.17:5443
target_dir=/etc/rancher/rke2

fail() {
	printf 'install-rke2-node: %s\n' "$*" >&2
	exit 1
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

case "$role" in
	server)
		service=rke2-server
		install_type=server
		config_source=$script_dir/config/server.yaml
		[ "$arg_count" -eq 8 ] ||
			fail "usage: $0 server <binary-artifact-directory> <registry-ca> <registry-username-file> <registry-password-file> (--dhcp-reservation-proven|--dynamic-ip-risk-approved) --swap-exception-approved --apply"
		address_confirmation=${6:-}
		swap_confirmation=${7:-}
		apply=${8:-}
		;;
	worker-1 | worker-2)
		service=rke2-agent
		install_type=agent
		config_source=$script_dir/config/$role.yaml
		[ "$arg_count" -eq 9 ] ||
			fail "usage: $0 $role <binary-artifact-directory> <registry-ca> <registry-username-file> <registry-password-file> <agent-token-file> (--dhcp-reservation-proven|--dynamic-ip-risk-approved) --swap-exception-approved --apply"
		token_file=${6:-}
		address_confirmation=${7:-}
		swap_confirmation=${8:-}
		apply=${9:-}
		;;
	*)
		fail "usage: $0 <server|worker-1|worker-2> <binary-artifact-directory> <registry-ca> <registry-username-file> <registry-password-file> [agent-token-file] (--dhcp-reservation-proven|--dynamic-ip-risk-approved) --swap-exception-approved --apply"
		;;
esac
case "$address_confirmation" in
	--dhcp-reservation-proven) address_policy=dhcp-reservation-proven ;;
	--dynamic-ip-risk-approved) address_policy=dynamic-ip-risk-approved ;;
	*) fail "literal --dhcp-reservation-proven or --dynamic-ip-risk-approved confirmation is required" ;;
esac
[ "$swap_confirmation" = --swap-exception-approved ] ||
	fail "literal --swap-exception-approved confirmation is required"
[ "$apply" = --apply ] || fail "literal --apply confirmation is required"
[ "$(id -u)" -eq 0 ] || fail "run as root"
for command_name in awk find grep install jq openssl sha256sum stat systemctl; do
	command -v "$command_name" >/dev/null 2>&1 || fail "$command_name is required"
done

[ -d "$artifact_dir" ] && [ ! -L "$artifact_dir" ] || fail "binary artifact directory is invalid"
artifact_entries=$(find "$artifact_dir" -mindepth 1 -maxdepth 1 -printf '%f\n' | sort)
expected_entries=$(printf '%s\n' "$RKE2_INSTALL_FILE" "$RKE2_BINARY_FILE" "$RKE2_CHECKSUM_FILE" | sort)
[ "$artifact_entries" = "$expected_entries" ] ||
	fail "binary artifact directory must contain exactly $RKE2_INSTALL_FILE, $RKE2_BINARY_FILE, and $RKE2_CHECKSUM_FILE"
binary=$artifact_dir/$RKE2_BINARY_FILE
installer=$artifact_dir/$RKE2_INSTALL_FILE
checksum_file=$artifact_dir/$RKE2_CHECKSUM_FILE
check_file "$binary" "$RKE2_BINARY_SIZE" "$RKE2_BINARY_SHA256"
check_file "$installer" "$RKE2_INSTALL_SIZE" "$RKE2_INSTALL_SHA256"
check_file "$checksum_file" "$RKE2_CHECKSUM_SIZE" "$RKE2_CHECKSUM_SHA256"
awk -v file="$RKE2_BINARY_FILE" -v sha="$RKE2_BINARY_SHA256" \
	'$1 == sha && $2 == file { found = 1 } END { exit !found }' "$checksum_file" ||
	fail "$checksum_file does not authenticate $RKE2_BINARY_FILE"
[ -f "$config_source" ] && [ ! -L "$config_source" ] || fail "$config_source is missing"

[ -f "$registry_ca_source" ] && [ ! -L "$registry_ca_source" ] ||
	fail "registry CA must be a regular file"
[ "$(stat -c '%u:%g' "$registry_ca_source")" = 0:0 ] ||
	fail "registry CA must be owned by root:root"
openssl x509 -in "$registry_ca_source" -noout -checkend 86400 >/dev/null ||
	fail "registry CA is invalid or expires within 24 hours"
check_root_secret "$username_file" registry-username-file
check_root_secret "$password_file" registry-password-file
if [ -n "$token_file" ]; then
	check_root_secret "$token_file" agent-token-file
fi

"$script_dir/preflight-node.sh" "$role" \
	"$address_confirmation" "$swap_confirmation"

install -d -o 0 -g 0 -m 0700 "$target_dir"
install -o 0 -g 0 -m 0600 "$config_source" "$target_dir/config.yaml"
install -o 0 -g 0 -m 0600 "$registry_ca_source" "$target_dir/registry-ca.crt"
registries_tmp=$(mktemp "$target_dir/.registries.yaml.XXXXXX")
trap 'rm -f "$registries_tmp"' EXIT HUP INT TERM
jq -n \
	--rawfile username "$username_file" \
	--rawfile password "$password_file" \
	--arg registry "$registry" \
	'{
	  mirrors: {($registry): {endpoint: [("https://" + $registry)]}},
	  configs: {($registry): {
	    auth: {
	      username: ($username | sub("\\n$"; "")),
	      password: ($password | sub("\\n$"; ""))
	    },
	    tls: {ca_file: "/etc/rancher/rke2/registry-ca.crt"}
	  }}
	}' >"$registries_tmp"
chown 0:0 "$registries_tmp"
chmod 0600 "$registries_tmp"
mv "$registries_tmp" "$target_dir/registries.yaml"
trap - EXIT HUP INT TERM
if [ -n "$token_file" ]; then
	install -o 0 -g 0 -m 0600 "$token_file" "$target_dir/agent-token"
fi

INSTALL_RKE2_ARTIFACT_PATH="$artifact_dir" \
	INSTALL_RKE2_METHOD=tar \
	INSTALL_RKE2_TAR_PREFIX=/usr/local \
	INSTALL_RKE2_TYPE="$install_type" \
	INSTALL_RKE2_VERSION="$RKE2_VERSION" \
	sh "$installer"

[ -x /usr/local/bin/rke2 ] || fail "RKE2 binary was not installed"
[ -f "/usr/local/lib/systemd/system/$service.service" ] ||
	fail "$service systemd unit was not installed"
if systemctl is-enabled --quiet "$service" 2>/dev/null; then
	fail "$service unexpectedly became enabled"
fi
service_status=$(systemctl is-active "$service" 2>/dev/null || true)
[ "${service_status:-inactive}" = inactive ] || fail "$service is unexpectedly $service_status"
[ "$(stat -c '%u:%g:%a' "$target_dir/config.yaml")" = 0:0:600 ] || fail "config.yaml permissions changed"
[ "$(stat -c '%u:%g:%a' "$target_dir/registries.yaml")" = 0:0:600 ] || fail "registries.yaml permissions changed"
[ "$(stat -c '%u:%g:%a' "$target_dir/registry-ca.crt")" = 0:0:600 ] || fail "registry CA permissions changed"
if [ -n "$token_file" ]; then
	[ "$(stat -c '%u:%g:%a' "$target_dir/agent-token")" = 0:0:600 ] || fail "agent token permissions changed"
fi
printf 'result=PASS role=%s release=%s service=%s address_policy=%s enabled=false active=false images_copied=false\n' \
	"$role" "$RKE2_VERSION" "$service" "$address_policy"

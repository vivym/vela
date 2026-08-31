#!/bin/sh

set -eu
umask 077

role=${1:-}
receipt_dir=${2:-}
swap_confirmation=${3:-}
restart_confirmation=${4:-}
apply=${5:-}
test_mode=${VELA_RKE2_NOSWAP_TESTING:-}
test_root=${VELA_RKE2_NOSWAP_TEST_ROOT:-}
target_name=99-vela-noswap.conf
base_name=00-rke2-defaults.conf
target_tmp=
desired_tmp=

fail() {
	printf 'configure-kubelet-noswap: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	[ -z "$target_tmp" ] || rm -f -- "$target_tmp"
	[ -z "$desired_tmp" ] || rm -f -- "$desired_tmp"
}

write_manifest() {
	manifest_tmp=$(mktemp "$receipt_dir/.SHA256SUMS.XXXXXX")
	(
		cd "$receipt_dir"
		for file in *; do
			[ "$file" = SHA256SUMS ] && continue
			[ -f "$file" ] || continue
			sha256sum "$file"
		done | LC_ALL=C sort
	) >"$manifest_tmp"
	chmod 0600 "$manifest_tmp"
	mv "$manifest_tmp" "$receipt_dir/SHA256SUMS"
}

fail_after_write() {
	reason=$1
	printf 'status=FAIL role=%s service=%s changed=%s reason=%s\n' \
		"$role" "$service" "$changed" "$reason" >"$receipt_dir/STATUS"
	"$systemctl_cmd" show "$service" \
		--property=Id,ActiveState,SubState,UnitFileState,MainPID,NRestarts \
		--no-pager >"$receipt_dir/service.after.txt" 2>&1 || true
	write_manifest
	fail "$reason; preserve $receipt_dir and follow its separately approved rollback instructions"
}

case "$role" in
	server | worker-1 | worker-2) ;;
	*) fail "usage: $0 <server|worker-1|worker-2> <new-receipt-directory> --swap-exception-approved --restart-approved --apply" ;;
esac
[ -n "$receipt_dir" ] || fail "new receipt directory is required"
[ "$swap_confirmation" = --swap-exception-approved ] ||
	fail "literal --swap-exception-approved confirmation is required"
[ "$restart_confirmation" = --restart-approved ] ||
	fail "literal --restart-approved confirmation is required"
[ "$apply" = --apply ] || fail "literal --apply confirmation is required"
[ "$#" -eq 5 ] ||
	fail "usage: $0 <server|worker-1|worker-2> <new-receipt-directory> --swap-exception-approved --restart-approved --apply"

case "$role" in
	server)
		expected_hostname=marslab-server
		expected_interface=enp34s0f0
		expected_ip=10.1.200.17
		service=rke2-server
		;;
	worker-1)
		expected_hostname=ubuntu
		expected_interface=eno1
		expected_ip=10.1.200.19
		service=rke2-agent
		;;
	worker-2)
		expected_hostname=ubuntu
		expected_interface=eno1
		expected_ip=10.1.200.16
		service=rke2-agent
		;;
esac

if [ -n "$test_mode" ] || [ -n "$test_root" ]; then
	[ "$test_mode" = 1 ] && [ -n "$test_root" ] ||
		fail "test overrides require literal VELA_RKE2_NOSWAP_TESTING=1 and VELA_RKE2_NOSWAP_TEST_ROOT"
	case "$test_root" in
		/*) ;;
		*) fail "test root must be absolute" ;;
	esac
	[ -d "$test_root" ] && [ ! -L "$test_root" ] || fail "test root must be a real directory"
	[ "$(realpath "$test_root")" = "$test_root" ] || fail "test root must be canonical"
	config_dir=$test_root/var/lib/rancher/rke2/agent/etc/kubelet.conf.d
	stat_cmd=${VELA_RKE2_NOSWAP_TEST_STAT:-stat}
	install_cmd=${VELA_RKE2_NOSWAP_TEST_INSTALL:-install}
	owner_uid=$(id -u)
	owner_gid=$(id -g)
	case "$receipt_dir" in
		"$test_root"/*) ;;
		*) fail "test receipt must be beneath the test root" ;;
	esac
else
	PATH=/usr/sbin:/usr/bin:/sbin:/bin
	export PATH
	[ "$(id -u)" -eq 0 ] || fail "run as root"
	config_dir=/var/lib/rancher/rke2/agent/etc/kubelet.conf.d
	stat_cmd=stat
	install_cmd=install
	owner_uid=0
	owner_gid=0
fi
systemctl_cmd=systemctl

for command_name in awk basename cat chmod cp cut date dirname find hostname ip jq ln mkdir mktemp mv realpath rm sha256sum sleep sort swapon systemctl; do
	command -v "$command_name" >/dev/null 2>&1 || fail "$command_name is required"
done
command -v "$stat_cmd" >/dev/null 2>&1 || fail "$stat_cmd is required"
command -v "$install_cmd" >/dev/null 2>&1 || fail "$install_cmd is required"

[ "$(hostname)" = "$expected_hostname" ] ||
	fail "hostname does not match role $role"
address_json=$(ip -j -4 address show dev "$expected_interface")
observed_ip=$(printf '%s' "$address_json" |
	jq -r '.[0].addr_info[] | select(.family == "inet" and .scope == "global") | .local' |
	awk 'NR == 1 { print }')
[ "$observed_ip" = "$expected_ip" ] ||
	fail "$expected_interface address is $observed_ip, expected $expected_ip"

[ -d "$config_dir" ] && [ ! -L "$config_dir" ] ||
	fail "$config_dir must be a real directory"
[ "$(realpath "$config_dir")" = "$config_dir" ] ||
	fail "$config_dir must be canonical and contain no symlink component"
[ "$($stat_cmd -c '%u:%g:%a' "$config_dir")" = "$owner_uid:$owner_gid:700" ] ||
	fail "$config_dir must be owned by $owner_uid:$owner_gid with mode 0700"
base=$config_dir/$base_name
[ -f "$base" ] && [ ! -L "$base" ] || fail "$base must be a regular file"
[ "$($stat_cmd -c '%u:%g:%a' "$base")" = "$owner_uid:$owner_gid:600" ] ||
	fail "$base must be owned by $owner_uid:$owner_gid with mode 0600"

target=$config_dir/$target_name
desired_tmp=$(mktemp "${TMPDIR:-/tmp}/vela-noswap-desired.XXXXXX")
trap cleanup EXIT HUP INT TERM
printf '%s\n' \
	'apiVersion: kubelet.config.k8s.io/v1beta1' \
	'kind: KubeletConfiguration' \
	'memorySwap:' \
	'  swapBehavior: NoSwap' >"$desired_tmp"
desired_sha256=$(sha256sum "$desired_tmp" | awk '{print $1}')

entries=$(find "$config_dir" -mindepth 1 -maxdepth 1 -exec basename {} \; | LC_ALL=C sort)
if [ -e "$target" ] || [ -L "$target" ]; then
	[ -f "$target" ] && [ ! -L "$target" ] ||
		fail "$target exists but is not a regular non-symlink file"
	[ "$($stat_cmd -c '%u:%g:%a' "$target")" = "$owner_uid:$owner_gid:600" ] ||
		fail "$target has unexpected ownership or mode"
	[ "$(sha256sum "$target" | awk '{print $1}')" = "$desired_sha256" ] ||
		fail "$target already exists with different content"
	expected_entries=$(printf '%s\n%s\n' "$base_name" "$target_name" | LC_ALL=C sort)
	[ "$entries" = "$expected_entries" ] ||
		fail "$config_dir contains entries outside $base_name and $target_name"
	target_before=present-identical
	changed=false
else
	[ "$entries" = "$base_name" ] ||
		fail "$config_dir must contain only $base_name before the first apply"
	target_before=absent
	changed=true
fi

active_state=$("$systemctl_cmd" is-active "$service" 2>/dev/null || true)
[ "$active_state" = active ] || fail "$service must be active before restart"
enabled_state=$("$systemctl_cmd" is-enabled "$service" 2>/dev/null || true)
[ "$enabled_state" = enabled ] || fail "$service must be enabled before restart"
swap_before=$(swapon --show --bytes --noheadings --output NAME,TYPE,SIZE,USED,PRIO)
[ -n "$swap_before" ] || fail "active host swap is absent; this helper only implements the approved swap-exception topology"
[ "$(printf '%s\n' "$swap_before" | awk 'NF { count++; name=$1 } END { if (count == 1) print name }')" = /swap.img ] ||
	fail "expected exactly one active /swap.img entry"

case "$receipt_dir" in
	/*) ;;
	*) fail "receipt directory must be absolute" ;;
esac
receipt_parent=$(dirname "$receipt_dir")
receipt_name=$(basename "$receipt_dir")
[ "$receipt_dir" = "$receipt_parent/$receipt_name" ] ||
	fail "receipt directory must be canonical"
case "$receipt_name" in
	'' | . | ..) fail "receipt directory has an unsafe final component" ;;
esac
[ -d "$receipt_parent" ] && [ ! -L "$receipt_parent" ] ||
	fail "receipt parent must be an existing real directory"
[ "$(realpath "$receipt_parent")" = "$receipt_parent" ] ||
	fail "receipt parent must be canonical and contain no symlink component"
[ "$($stat_cmd -c '%u:%g' "$receipt_parent")" = "$owner_uid:$owner_gid" ] ||
	fail "receipt parent must be owned by $owner_uid:$owner_gid"
parent_permissions=$($stat_cmd -c '%A' "$receipt_parent")
[ "$(printf '%s' "$parent_permissions" | cut -c6)" != w ] &&
	[ "$(printf '%s' "$parent_permissions" | cut -c9)" != w ] ||
	fail "receipt parent must not be group- or world-writable"
[ ! -e "$receipt_dir" ] && [ ! -L "$receipt_dir" ] ||
	fail "receipt directory already exists"

mkdir -m 0700 "$receipt_dir"
printf 'vela-rke2-kubelet-noswap-receipt-v1\n' >"$receipt_dir/SCHEMA"
printf 'status=in_progress role=%s service=%s changed=%s\n' \
	"$role" "$service" "$changed" >"$receipt_dir/STATUS"
{
	printf 'captured_at=%s\n' "$(date -u +%FT%TZ)"
	printf 'role=%s\n' "$role"
	printf 'hostname=%s\n' "$(hostname)"
	printf 'interface=%s\n' "$expected_interface"
	printf 'address=%s\n' "$observed_ip"
	printf 'service=%s\n' "$service"
	printf 'config_dir=%s\n' "$config_dir"
	printf 'target=%s\n' "$target"
	printf 'target_before=%s\n' "$target_before"
} >"$receipt_dir/node.before.txt"
"$systemctl_cmd" show "$service" \
	--property=Id,ActiveState,SubState,UnitFileState,MainPID,NRestarts \
	--no-pager >"$receipt_dir/service.before.txt"
printf '%s\n' "$swap_before" >"$receipt_dir/swap.before.txt"
printf '%s  %s\n' "$(sha256sum "$base" | awk '{print $1}')" "$base" >"$receipt_dir/base.before.sha256"
printf '%s\n' "$target_before" >"$receipt_dir/target.before.state"
cp "$desired_tmp" "$receipt_dir/drop-in.applied.conf"
if [ "$target_before" = present-identical ]; then
	cp "$target" "$receipt_dir/target.before.conf"
	cat >"$receipt_dir/ROLLBACK.txt" <<EOF
This run found the exact NoSwap drop-in already present and did not rewrite it.
Do not remove $target as rollback for this run.
Any additional restart or configuration change requires separate approval.
After an approved restart, run the strict cluster verifier from the control node.
EOF
else
	cat >"$receipt_dir/ROLLBACK.txt" <<EOF
Rollback is not automatic and requires separate approval plus a fresh node/cluster preflight.
Verify this receipt and confirm that $target still matches drop-in.applied.conf.
Then remove only the helper-owned target and restart only the matching service:
rm -- $target
systemctl restart $service
After restart, require the node, GPU inventory, workloads, and strict cluster verifier to recover.
EOF
fi

if [ "$changed" = true ]; then
	target_tmp=$(mktemp "$config_dir/.$target_name.XXXXXX")
	"$install_cmd" -m 0600 "$desired_tmp" "$target_tmp"
	[ "$($stat_cmd -c '%u:%g:%a' "$target_tmp")" = "$owner_uid:$owner_gid:600" ] ||
		fail_after_write "temporary drop-in ownership or mode changed"
	ln "$target_tmp" "$target" || fail_after_write "target appeared during atomic publication"
	rm -f -- "$target_tmp"
	target_tmp=
fi
[ -f "$target" ] && [ ! -L "$target" ] || fail_after_write "published target is unsafe"
[ "$(sha256sum "$target" | awk '{print $1}')" = "$desired_sha256" ] ||
	fail_after_write "published target digest mismatch"
[ "$($stat_cmd -c '%u:%g:%a' "$target")" = "$owner_uid:$owner_gid:600" ] ||
	fail_after_write "published target ownership or mode mismatch"

if ! "$systemctl_cmd" restart "$service"; then
	fail_after_write "$service restart command failed"
fi
attempt=0
while :; do
	active_state=$("$systemctl_cmd" is-active "$service" 2>/dev/null || true)
	[ "$active_state" = active ] && break
	attempt=$((attempt + 1))
	[ "$attempt" -lt 30 ] || fail_after_write "$service did not return to active"
	sleep 2
done
enabled_state=$("$systemctl_cmd" is-enabled "$service" 2>/dev/null || true)
[ "$enabled_state" = enabled ] || fail_after_write "$service is no longer enabled"
"$systemctl_cmd" show "$service" \
	--property=Id,ActiveState,SubState,UnitFileState,MainPID,NRestarts \
	--no-pager >"$receipt_dir/service.after.txt"
printf '%s  %s\n' "$(sha256sum "$target" | awk '{print $1}')" "$target" >"$receipt_dir/target.after.sha256"
printf 'status=PASS role=%s service=%s changed=%s\n' \
	"$role" "$service" "$changed" >"$receipt_dir/STATUS"
write_manifest
trap - EXIT HUP INT TERM
cleanup

printf 'result=PASS role=%s service=%s changed=%s receipt=%s follow_up=strict-cluster-verification-required\n' \
	"$role" "$service" "$changed" "$receipt_dir"

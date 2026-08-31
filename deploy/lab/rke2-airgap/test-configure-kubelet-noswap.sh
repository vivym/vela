#!/bin/sh

set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
helper=$script_dir/configure-kubelet-noswap.sh
temporary=$(mktemp -d "${TMPDIR:-/tmp}/vela-noswap-test.XXXXXX")
trap 'rm -rf -- "$temporary"' EXIT HUP INT TERM

fail() {
	printf 'test-configure-kubelet-noswap: %s\n' "$*" >&2
	exit 1
}

if stat -c '%a' "$temporary" >/dev/null 2>&1; then
	test_stat=$(command -v stat)
else
	test_stat=$(command -v gstat || true)
	[ -n "$test_stat" ] || fail "GNU stat or gstat is required"
fi
test_install=$(command -v install)
real_ln=$(command -v ln)

test_root=$temporary/root
mkdir -p "$test_root/receipts"
test_root=$(realpath "$test_root")
chmod 0700 "$test_root/receipts"

assert_confirmation_rejected() {
	name=$1
	expected_error=$2
	shift 2
	receipt=$test_root/receipts/$name
	stderr_file=$temporary/$name.stderr
	if VELA_RKE2_NOSWAP_TEST_ROOT=$test_root \
		"$helper" worker-1 "$receipt" "$@" 2>"$stderr_file"; then
		fail "helper accepted $name"
	fi
	grep -Fq "$expected_error" "$stderr_file" ||
		fail "helper did not explain $name"
	[ ! -e "$receipt" ] || fail "helper created a receipt for $name"
	[ ! -e "$test_root/var/lib/rancher/rke2/agent/etc/kubelet.conf.d/99-vela-noswap.conf" ] ||
		fail "helper wrote the drop-in for $name"
	[ ! -e "$test_root/systemctl.log" ] || fail "helper invoked systemctl for $name"
}

assert_confirmation_rejected \
	missing-swap-confirmation \
	'literal --swap-exception-approved confirmation is required'
assert_confirmation_rejected \
	missing-restart-confirmation \
	'literal --restart-approved confirmation is required' \
	--swap-exception-approved
assert_confirmation_rejected \
	missing-apply-confirmation \
	'literal --apply confirmation is required' \
	--swap-exception-approved --restart-approved

stub_dir=$temporary/stubs
systemctl_log=$test_root/systemctl.log
config_dir=$test_root/var/lib/rancher/rke2/agent/etc/kubelet.conf.d
target=$config_dir/99-vela-noswap.conf
positive_receipt=$test_root/receipts/worker-1-positive
mkdir -p "$stub_dir" "$config_dir"
chmod 0700 "$config_dir"
printf '%s\n' \
	'apiVersion: kubelet.config.k8s.io/v1beta1' \
	'kind: KubeletConfiguration' \
	'memorySwap: {}' >"$config_dir/00-rke2-defaults.conf"
chmod 0600 "$config_dir/00-rke2-defaults.conf"

cat >"$stub_dir/hostname" <<'EOF'
#!/bin/sh
printf '%s\n' "${VELA_NOSWAP_STUB_HOSTNAME:-ubuntu}"
EOF
cat >"$stub_dir/ip" <<'EOF'
#!/bin/sh
printf '[{"addr_info":[{"family":"inet","scope":"global","local":"%s"}]}]\n' \
	"${VELA_NOSWAP_STUB_IP:-10.1.200.19}"
EOF
cat >"$stub_dir/swapon" <<'EOF'
#!/bin/sh
printf '/swap.img file 8589930496 0 -2\n'
EOF
cat >"$stub_dir/systemctl" <<'EOF'
#!/bin/sh
case "$1" in
	is-active)
		if [ -e "$VELA_RKE2_NOSWAP_TEST_RESTART_MARKER" ]; then
			printf '%s\n' "${VELA_NOSWAP_STUB_AFTER_STATE:-active}"
		else
			printf '%s\n' "${VELA_NOSWAP_STUB_BEFORE_STATE:-active}"
		fi
		;;
	is-enabled) printf 'enabled\n' ;;
	show)
		printf '%s\n' \
			'Id=rke2-agent.service' \
			'ActiveState=active' \
			'SubState=running' \
			'UnitFileState=enabled' \
			'MainPID=4242' \
			'NRestarts=0'
		;;
	restart)
		printf 'restart %s\n' "$2" >>"$VELA_RKE2_NOSWAP_TEST_SYSTEMCTL_LOG"
		: >"$VELA_RKE2_NOSWAP_TEST_RESTART_MARKER"
		;;
	*) exit 64 ;;
esac
EOF
cat >"$stub_dir/sleep" <<'EOF'
#!/bin/sh
exit 0
EOF
cat >"$stub_dir/ln" <<'EOF'
#!/bin/sh
if [ "${VELA_NOSWAP_STUB_LN_COLLISION:-0}" = 1 ]; then
	printf 'concurrent-owner\n' >"$2"
	chmod 0600 "$2"
	exit 1
fi
exec "$VELA_NOSWAP_REAL_LN" "$@"
EOF
chmod +x "$stub_dir/hostname" "$stub_dir/ip" "$stub_dir/swapon" "$stub_dir/systemctl" "$stub_dir/sleep" "$stub_dir/ln"

run_helper() {
	root=$1
	shift
	PATH=$stub_dir:$PATH \
		VELA_RKE2_NOSWAP_TESTING=1 \
		VELA_RKE2_NOSWAP_TEST_ROOT=$root \
		VELA_RKE2_NOSWAP_TEST_STAT=$test_stat \
		VELA_RKE2_NOSWAP_TEST_INSTALL=$test_install \
		VELA_RKE2_NOSWAP_TEST_SYSTEMCTL_LOG=$root/systemctl.log \
		VELA_RKE2_NOSWAP_TEST_RESTART_MARKER=$root/restarted \
		VELA_NOSWAP_STUB_HOSTNAME=${VELA_NOSWAP_STUB_HOSTNAME:-ubuntu} \
		VELA_NOSWAP_STUB_IP=${VELA_NOSWAP_STUB_IP:-10.1.200.19} \
		VELA_NOSWAP_STUB_BEFORE_STATE=${VELA_NOSWAP_STUB_BEFORE_STATE:-active} \
		VELA_NOSWAP_STUB_AFTER_STATE=${VELA_NOSWAP_STUB_AFTER_STATE:-active} \
		VELA_NOSWAP_STUB_LN_COLLISION=${VELA_NOSWAP_STUB_LN_COLLISION:-0} \
		VELA_NOSWAP_REAL_LN=$real_ln \
		"$helper" "$@"
}

prepare_root() {
	case_name=$1
	case_root=$temporary/$case_name/root
	mkdir -p "$case_root/receipts" "$case_root/var/lib/rancher/rke2/agent/etc/kubelet.conf.d"
	case_root=$(realpath "$case_root")
	case_config_dir=$case_root/var/lib/rancher/rke2/agent/etc/kubelet.conf.d
	chmod 0700 "$case_root/receipts" "$case_config_dir"
	printf '%s\n' \
		'apiVersion: kubelet.config.k8s.io/v1beta1' \
		'kind: KubeletConfiguration' \
		'memorySwap: {}' >"$case_config_dir/00-rke2-defaults.conf"
	chmod 0600 "$case_config_dir/00-rke2-defaults.conf"
}

run_helper "$test_root" worker-1 "$positive_receipt" \
	--swap-exception-approved --restart-approved --apply

expected=$temporary/expected.conf
printf '%s\n' \
	'apiVersion: kubelet.config.k8s.io/v1beta1' \
	'kind: KubeletConfiguration' \
	'memorySwap:' \
	'  swapBehavior: NoSwap' >"$expected"
cmp -s "$expected" "$target" || fail "helper wrote unexpected drop-in content"
[ "$($test_stat -c '%a' "$target")" = 600 ] || fail "helper wrote the drop-in with unsafe mode"
[ "$(cat "$systemctl_log")" = 'restart rke2-agent' ] || fail "helper restarted an unexpected service"
grep -Fxq 'status=PASS role=worker-1 service=rke2-agent changed=true' "$positive_receipt/STATUS" ||
	fail "helper did not complete the receipt"
grep -Fq "rm -- $target" "$positive_receipt/ROLLBACK.txt" ||
	fail "helper did not record the exact rollback target"
grep -Fxq 'state=regular' "$positive_receipt/target.after.txt" ||
	fail "helper did not record the successful target state"
grep -Fxq "sha256=$(sha256sum "$expected" | awk '{print $1}')" "$positive_receipt/target.after.txt" ||
	fail "helper did not record the successful target digest"
(cd "$positive_receipt" && sha256sum --check --strict SHA256SUMS >/dev/null) ||
	fail "helper receipt manifest did not verify"

rm -f "$systemctl_log" "$test_root/restarted"
idempotent_receipt=$test_root/receipts/worker-1-idempotent
run_helper "$test_root" worker-1 "$idempotent_receipt" \
	--swap-exception-approved --restart-approved --apply
grep -Fxq 'status=PASS role=worker-1 service=rke2-agent changed=false' "$idempotent_receipt/STATUS" ||
	fail "helper did not record the idempotent path"
[ "$(cat "$systemctl_log")" = 'restart rke2-agent' ] ||
	fail "idempotent path did not restart only rke2-agent"
(cd "$idempotent_receipt" && sha256sum --check --strict SHA256SUMS >/dev/null) ||
	fail "idempotent receipt manifest did not verify"

prepare_root wrong-hostname
wrong_hostname_receipt=$case_root/receipts/wrong-hostname
wrong_hostname_stderr=$temporary/wrong-hostname.stderr
VELA_NOSWAP_STUB_HOSTNAME=unexpected-host
if run_helper "$case_root" worker-1 "$wrong_hostname_receipt" \
	--swap-exception-approved --restart-approved --apply 2>"$wrong_hostname_stderr"; then
	fail "helper accepted the wrong hostname"
fi
unset VELA_NOSWAP_STUB_HOSTNAME
grep -Fq 'hostname does not match role worker-1' "$wrong_hostname_stderr" ||
	fail "helper did not explain the hostname mismatch"
[ ! -e "$wrong_hostname_receipt" ] || fail "hostname mismatch created a receipt"
[ ! -e "$case_root/systemctl.log" ] || fail "hostname mismatch restarted a service"

prepare_root symlink-target
symlink_target=$case_config_dir/99-vela-noswap.conf
ln -s "$temporary/untrusted.conf" "$symlink_target"
symlink_receipt=$case_root/receipts/symlink-target
symlink_stderr=$temporary/symlink-target.stderr
if run_helper "$case_root" worker-1 "$symlink_receipt" \
	--swap-exception-approved --restart-approved --apply 2>"$symlink_stderr"; then
	fail "helper accepted a symlink target"
fi
grep -Fq 'exists but is not a regular non-symlink file' "$symlink_stderr" ||
	fail "helper did not explain the unsafe target"
[ ! -e "$symlink_receipt" ] || fail "unsafe target created a receipt"
[ ! -e "$case_root/systemctl.log" ] || fail "unsafe target restarted a service"

prepare_root publication-collision
collision_target=$case_config_dir/99-vela-noswap.conf
collision_receipt=$case_root/receipts/publication-collision
collision_stderr=$temporary/publication-collision.stderr
VELA_NOSWAP_STUB_LN_COLLISION=1
if run_helper "$case_root" worker-1 "$collision_receipt" \
	--swap-exception-approved --restart-approved --apply 2>"$collision_stderr"; then
	fail "helper reported success after a publication collision"
fi
unset VELA_NOSWAP_STUB_LN_COLLISION
[ "$(cat "$collision_target")" = concurrent-owner ] ||
	fail "collision test did not preserve the concurrent actor's target"
grep -Fq 'status=FAIL role=worker-1 service=rke2-agent changed=false reason=target appeared during atomic publication' \
	"$collision_receipt/STATUS" || fail "collision receipt claimed target ownership"
grep -Fq 'Do not remove any file' "$collision_receipt/ROLLBACK.txt" ||
	fail "collision receipt did not prohibit unsafe rollback"
if grep -Fq "rm -- $collision_target" "$collision_receipt/ROLLBACK.txt"; then
	fail "collision receipt instructed removal of another actor's target"
fi
grep -Fxq 'state=regular' "$collision_receipt/target.after.txt" ||
	fail "collision receipt omitted resulting target state"
[ ! -e "$case_root/systemctl.log" ] || fail "publication collision restarted a service"
(cd "$collision_receipt" && sha256sum --check --strict SHA256SUMS >/dev/null) ||
	fail "publication collision receipt manifest did not verify"

prepare_root inactive-service
inactive_receipt=$case_root/receipts/inactive-service
inactive_stderr=$temporary/inactive-service.stderr
VELA_NOSWAP_STUB_BEFORE_STATE=inactive
if run_helper "$case_root" worker-1 "$inactive_receipt" \
	--swap-exception-approved --restart-approved --apply 2>"$inactive_stderr"; then
	fail "helper accepted an inactive service"
fi
unset VELA_NOSWAP_STUB_BEFORE_STATE
grep -Fq 'rke2-agent must be active before restart' "$inactive_stderr" ||
	fail "helper did not explain the inactive service"
[ ! -e "$inactive_receipt" ] || fail "inactive service created a receipt"
[ ! -e "$case_config_dir/99-vela-noswap.conf" ] || fail "inactive service wrote the target"
[ ! -e "$case_root/systemctl.log" ] || fail "inactive service triggered restart"

prepare_root unhealthy-restart
unhealthy_target=$case_config_dir/99-vela-noswap.conf
unhealthy_receipt=$case_root/receipts/unhealthy-restart
unhealthy_stderr=$temporary/unhealthy-restart.stderr
VELA_NOSWAP_STUB_AFTER_STATE=failed
if run_helper "$case_root" worker-1 "$unhealthy_receipt" \
	--swap-exception-approved --restart-approved --apply 2>"$unhealthy_stderr"; then
	fail "helper reported success after an unhealthy restart"
fi
unset VELA_NOSWAP_STUB_AFTER_STATE
[ -f "$unhealthy_target" ] || fail "unhealthy restart did not preserve the applied target for diagnosis"
grep -Fq 'status=FAIL role=worker-1 service=rke2-agent changed=true reason=rke2-agent did not return to active' \
	"$unhealthy_receipt/STATUS" || fail "unhealthy restart did not leave a FAIL receipt"
[ "$(cat "$case_root/systemctl.log")" = 'restart rke2-agent' ] ||
	fail "unhealthy restart called an unexpected service"
grep -Fxq 'state=regular' "$unhealthy_receipt/target.after.txt" ||
	fail "unhealthy restart receipt omitted resulting target state"
grep -Eq '^sha256=[0-9a-f]{64}$' "$unhealthy_receipt/target.after.txt" ||
	fail "unhealthy restart receipt omitted resulting target digest"
(cd "$unhealthy_receipt" && sha256sum --check --strict SHA256SUMS >/dev/null) ||
	fail "unhealthy restart receipt manifest did not verify"

printf 'result=PASS tests=9\n'

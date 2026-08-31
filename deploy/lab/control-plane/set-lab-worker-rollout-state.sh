#!/bin/sh

set -eu

worker_id=${1:-}
action=${2:-}
output=${3:-}
apply=${4:-}
namespace=vela-lab
worker1_id=84000000-0000-0000-0000-000000000101
worker2_id=84000000-0000-0000-0000-000000000102
receipt_root=/root/vela-lab-deploy-bc590e20/receipts
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}
temporary=
committed=false

fail() {
	printf 'set-lab-worker-rollout-state: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	if [ "$committed" = false ] && [ -n "$temporary" ] && [ -d "$temporary" ]; then
		printf 'result=FAIL\nfailed_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >>"$temporary/result.txt"
		printf 'set-lab-worker-rollout-state: diagnostic receipt retained at %s\n' "$temporary" >&2
	fi
}

query_database() {
	sql=$1
	# Credentials are expanded only inside the PostgreSQL container.
	# shellcheck disable=SC2016
	printf '%s\n' "$sql" | "$kubectl_bin" exec --stdin --namespace "$namespace" \
		statefulset/vela-lab-postgres -- sh -ec \
		'PGPASSWORD=$POSTGRES_PASSWORD exec psql --no-psqlrc --set=ON_ERROR_STOP=1 --tuples-only --no-align --field-separator="|" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"'
}

global_boundary() {
	query_database "
SELECT
  (SELECT count(*) FROM attempt_leases WHERE revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts),
  (SELECT count(*) FROM jobs WHERE state IN ('QUEUED','ASSIGNED','RUNNING','FINALIZING','RETRY_WAIT','CANCELING')),
  (SELECT count(*) FROM workers WHERE lifecycle_state='READY' AND reachability_condition='HEALTHY');"
}

worker_state() {
	query_database "SELECT lifecycle_state, reachability_condition, epoch FROM workers WHERE id='$worker_id'::uuid;"
}

[ "$(id -u)" -eq 0 ] || fail "run as root"
[ "$apply" = --apply ] || fail "usage: $0 <worker-id> --drain|--restore <absolute-receipt-directory> --apply"
case "$worker_id" in "$worker1_id" | "$worker2_id") ;; *) fail "Worker ID is outside the fixed lab inventory" ;; esac
case "$action" in --drain | --restore) ;; *) fail "action must be --drain or --restore" ;; esac
case "$output" in "$receipt_root"/fixed-runner-worker-*) ;; *) fail "receipt path is outside the fixed lab receipt root" ;; esac
[ ! -e "$output" ] || fail "receipt path already exists"
[ -x "$kubectl_bin" ] || fail "$kubectl_bin is missing"
[ -r "$kubeconfig" ] || fail "$kubeconfig is unreadable"
command -v sha256sum >/dev/null 2>&1 || fail "sha256sum is required"
export KUBECONFIG="$kubeconfig"

temporary=$(mktemp -d "$receipt_root/.fixed-runner-worker-state.XXXXXX")
chmod 0700 "$temporary"
trap cleanup EXIT HUP INT TERM
printf 'schema=vela-lab-worker-rollout-state-v1\nworker_id=%s\naction=%s\nstarted_at=%s\nproduction_gates=0/9\n' \
	"$worker_id" "$action" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$temporary/input.txt"
global_before=$(global_boundary)
state_before=$(worker_state)
printf '%s\n' "$global_before" >"$temporary/global-before.txt"
printf '%s\n' "$state_before" >"$temporary/worker-before.txt"

case "$action" in
	--drain)
		[ "$global_before" = '0|0|0|2' ] || fail "drain preflight is not 0 leases, 0 receipts, 0 active Jobs, 2 READY/HEALTHY Workers"
		[ "$state_before" = 'READY|HEALTHY|1' ] || fail "target Worker is not READY/HEALTHY at epoch 1"
		changed=$(query_database "
WITH changed AS (
  UPDATE workers AS worker SET lifecycle_state='DRAINING', updated_at=clock_timestamp()
  WHERE worker.id='$worker_id'::uuid AND worker.epoch=1
    AND worker.lifecycle_state='READY' AND worker.reachability_condition='HEALTHY'
    AND NOT EXISTS (SELECT 1 FROM attempt_leases AS lease WHERE lease.worker_id=worker.id AND lease.revoked_at IS NULL)
  RETURNING worker.id
)
SELECT count(*) FROM changed;")
		[ "$changed" = 1 ] || fail "guarded Worker drain changed an unexpected row count"
		expected_global='0|0|0|1'
		expected_state='DRAINING|HEALTHY|1'
		;;
	--restore)
		[ "$global_before" = '0|0|0|1' ] || fail "restore preflight is not 0 leases, 0 receipts, 0 active Jobs, 1 READY/HEALTHY Worker"
		[ "$state_before" = 'DRAINING|HEALTHY|1' ] || fail "target Worker is not DRAINING/HEALTHY at epoch 1"
		changed=$(query_database "
WITH changed AS (
  UPDATE workers AS worker SET lifecycle_state='READY', updated_at=clock_timestamp()
  WHERE worker.id='$worker_id'::uuid AND worker.epoch=1
    AND worker.lifecycle_state='DRAINING' AND worker.reachability_condition='HEALTHY'
    AND NOT EXISTS (SELECT 1 FROM attempt_leases AS lease WHERE lease.worker_id=worker.id AND lease.revoked_at IS NULL)
  RETURNING worker.id
)
SELECT count(*) FROM changed;")
		[ "$changed" = 1 ] || fail "guarded Worker restore changed an unexpected row count"
		expected_global='0|0|0|2'
		expected_state='READY|HEALTHY|1'
		;;
esac

global_after=$(global_boundary)
state_after=$(worker_state)
printf '%s\n' "$global_after" >"$temporary/global-after.txt"
printf '%s\n' "$state_after" >"$temporary/worker-after.txt"
[ "$global_after" = "$expected_global" ] || fail "global authority boundary did not reach the expected state"
[ "$state_after" = "$expected_state" ] || fail "target Worker did not reach the expected state"
printf 'schema=vela-lab-worker-rollout-state-v1\nresult=PASS\nworker_id=%s\naction=%s\ncompleted_at=%s\nproduction_gates=0/9\n' \
	"$worker_id" "$action" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$temporary/result.txt"
(
	cd "$temporary"
	checksum_files=$(find . -maxdepth 1 -type f ! -name SHA256SUMS -printf '%P\n' | sort)
	# Receipt file names are fixed and contain no whitespace.
	# shellcheck disable=SC2086
	sha256sum $checksum_files >SHA256SUMS
)
mv "$temporary" "$output"
temporary=
committed=true
printf 'schema=vela-lab-worker-rollout-state-v1 result=PASS worker_id=%s action=%s receipt=%s production_gates=0/9\n' \
	"$worker_id" "$action" "$output"

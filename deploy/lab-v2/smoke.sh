#!/bin/sh

set -eu

manifests=${1:-}
apply=${2:-}
namespace=vela-lab-v2
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}

fail() {
	printf 'smoke-vela-lab-control-plane: %s\n' "$*" >&2
	exit 1
}

[ "$apply" = --apply ] || fail "usage: $0 <rendered-manifest-directory> --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root to read the RKE2 kubeconfig"
[ -x "$kubectl_bin" ] || fail "$kubectl_bin is missing"
[ -r "$kubeconfig" ] || fail "$kubeconfig is unreadable"
case "$manifests" in
	/*) ;;
	*) fail "manifest directory must be absolute" ;;
esac
[ -f "$manifests/60-smoke.yaml" ] || fail "60-smoke.yaml is absent"
command -v jq >/dev/null 2>&1 || fail "jq is required"
export KUBECONFIG="$kubeconfig"

query_database() {
	# The variables are expanded inside the PostgreSQL container.
	# shellcheck disable=SC2016
	printf '%s\n' "$1" | "$kubectl_bin" exec --stdin --namespace "$namespace" \
		statefulset/vela-lab-postgres -- sh -ec \
		'PGPASSWORD=$POSTGRES_PASSWORD exec psql --no-psqlrc --tuples-only --no-align --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"'
}

$kubectl_bin rollout status deployment/vela-lab-control --namespace "$namespace" --timeout=60s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-stage-worker-1 --namespace "$namespace" --timeout=60s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-stage-worker-2 --namespace "$namespace" --timeout=60s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-stage-worker-thumbnail --namespace "$namespace" --timeout=60s >/dev/null

schedulable_pools=$(query_database 'SELECT count(*) FROM vela_list_schedulable_worker_pools();')
[ "$schedulable_pools" -gt 0 ] ||
	fail "no schedulable Worker pool has fresh capacity evidence"

job=$($kubectl_bin create -f "$manifests/60-smoke.yaml" -o name)
case "$job" in
	job.batch/vela-lab-smoke-*) ;;
	*) fail "unexpected smoke Job identity $job" ;;
esac
if ! $kubectl_bin wait "$job" --namespace "$namespace" --for=condition=complete --timeout=420s; then
	$kubectl_bin describe "$job" --namespace "$namespace" >&2 || true
	$kubectl_bin logs "$job" --namespace "$namespace" --all-containers >&2 || true
	fail "end-to-end smoke Job failed; Job is preserved for diagnosis"
fi
receipt=$($kubectl_bin logs "$job" --namespace "$namespace")
printf '%s\n' "$receipt" | jq -e '
  .status == "LAB VERIFIED" and
  .final_state == "SUCCEEDED" and
  .artifact_count == 2 and
  (.artifact_kinds | sort) == ["THUMBNAIL", "VIDEO"]
' >/dev/null || fail "smoke receipt is invalid"

production_gate_receipts=$(query_database 'SELECT count(*) FROM production_gate_receipts;')
[ "$production_gate_receipts" = 0 ] || fail "lab created Production Gate receipts"

printf '%s\n' "$receipt"
printf 'schema=vela-lab-smoke-wrapper-v1 job=%s result=PASS production_gates=0/9\n' "$job"

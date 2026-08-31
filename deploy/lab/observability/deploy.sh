#!/bin/sh

set -eu
umask 077

manifests=${1:-}
output=${2:-}
apply=${3:-}
namespace=vela-observability
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}
temporary=
mutated=false

fail() {
	printf 'deploy-vela-lab-observability: %s\n' "$*" >&2
	exit 1
}

query_database() {
	sql=$1
	# shellcheck disable=SC2016
	printf '%s\n' "$sql" | "$kubectl_bin" exec --stdin --namespace vela-lab \
		statefulset/vela-lab-postgres -- sh -ec \
		'PGPASSWORD=$POSTGRES_PASSWORD exec psql --no-psqlrc --set=ON_ERROR_STOP=1 --tuples-only --no-align --field-separator="|" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"'
}

finish() {
	status=$?
	trap - EXIT HUP INT TERM
	if [ "$status" -ne 0 ] && [ -n "$temporary" ] && [ -d "$temporary" ]; then
		rollback_result=not-required
		if [ "$mutated" = true ]; then
			rollback_result=PASS
			"$kubectl_bin" delete namespace "$namespace" --ignore-not-found --wait=true --timeout=180s \
				>"$temporary/rollback-namespace.log" 2>&1 || rollback_result=FAIL
			"$kubectl_bin" apply -f "$temporary/control-ingress-before.json" \
				>"$temporary/rollback-control-ingress.log" 2>&1 || rollback_result=FAIL
		fi
		printf 'status=INCOMPLETE rollback=%s production_gates=0/9\n' "$rollback_result" >"$temporary/STATUS"
		(cd "$temporary" && find . -type f ! -name SHA256SUMS -print | LC_ALL=C sort | xargs sha256sum) >"$temporary/SHA256SUMS"
		if [ ! -e "$output" ] && [ ! -L "$output" ]; then
			mv "$temporary" "$output"
		fi
	fi
	exit "$status"
}

[ "$#" -eq 3 ] && [ "$apply" = --apply ] || fail "usage: $0 <rendered-manifest-directory> <new-output-directory> --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root"
[ "$(hostname)" = marslab-server ] || fail "run only on marslab-server"
case "$manifests" in /*) ;; *) fail "manifest directory must be absolute" ;; esac
case "$output" in /*) ;; *) fail "output directory must be absolute" ;; esac
[ -d "$manifests" ] && [ ! -L "$manifests" ] || fail "manifest directory is invalid"
[ ! -e "$output" ] && [ ! -L "$output" ] || fail "output path already exists"
for file in 00-namespace.yaml 10-configmaps.yaml 20-stack.yaml 30-network-policies.yaml 40-control-ingress.yaml images.lock; do
	[ -f "$manifests/$file" ] && [ ! -L "$manifests/$file" ] || fail "$file is absent or invalid"
done
[ -x "$script_dir/verify.sh" ] || fail "verify.sh is not executable"
[ -x "$kubectl_bin" ] || fail "$kubectl_bin is missing"
[ -r "$kubeconfig" ] || fail "$kubeconfig is unreadable"
for command_name in docker jq sha256sum; do command -v "$command_name" >/dev/null 2>&1 || fail "$command_name is required"; done
export KUBECONFIG="$kubeconfig"
temporary=$(mktemp -d "$(dirname "$output")/.vela-observability-deploy.XXXXXX")
chmod 0700 "$temporary"
trap finish EXIT HUP INT TERM

sha256sum "$manifests"/* >"$temporary/rendered-manifests-sha256.txt"
"$kubectl_bin" get nodes -o json >"$temporary/nodes-before.json"
jq -e '
  (.items | length) == 3
  and all(.items[]; any(.status.conditions[]; .type == "Ready" and .status == "True"))
  and any(.items[]; .metadata.name == "vela-lab-control-1" and .metadata.labels["vela.ai/node-role"] == "control-storage" and (.status.allocatable["nvidia.com/gpu"] // "0") == "0")
  and ([.items[] | select(.metadata.name == "vela-lab-worker-1" or .metadata.name == "vela-lab-worker-2") | (.status.allocatable["nvidia.com/gpu"] // "0")] | sort) == ["8", "8"]
' "$temporary/nodes-before.json" >/dev/null || fail "three-node Ready GPU boundary drifted"
[ -z "$("$kubectl_bin" get namespace "$namespace" --ignore-not-found -o name)" ] || fail "$namespace already exists; verify or roll it back explicitly"
"$kubectl_bin" get networkpolicy control-ingress --namespace vela-lab -o json | jq '
  del(.metadata.annotations, .metadata.creationTimestamp, .metadata.generation,
      .metadata.managedFields, .metadata.resourceVersion, .metadata.uid, .status)
' >"$temporary/control-ingress-before.json"

[ "$(docker inspect --format '{{.Id}}' vela-registry 2>/dev/null)" = 2bd86fd8f7db91609a430dd8e12402bb5eb5def9454f297994f51ab9c1571d68 ] || fail "Registry container identity changed"
[ "$(docker inspect --format '{{.State.Status}}' vela-registry 2>/dev/null)" = running ] || fail "Registry is not running"
[ "$(docker inspect --format '{{.Id}}' fchip-4591d89ff18127a74b8a25a0 2>/dev/null)" = b0a653da3926e90d88a6d3329fab8a927456e23ddfd6acb7d7d40cf6f9db0c94 ] || fail "shared experiment container identity changed"
[ "$(docker inspect --format '{{.State.Status}}' fchip-4591d89ff18127a74b8a25a0 2>/dev/null)" = running ] || fail "shared experiment container is not running"

global_before=$(query_database '
SELECT
  (SELECT count(*) FROM attempt_leases WHERE revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts),
  (SELECT count(*) FROM jobs WHERE state IN ('\''QUEUED'\'','\''ASSIGNED'\'','\''RUNNING'\'','\''FINALIZING'\'','\''RETRY_WAIT'\'','\''CANCELING'\'')),
  (SELECT count(*) FROM workers WHERE lifecycle_state = '\''READY'\'' AND reachability_condition = '\''HEALTHY'\'');')
printf '%s\n' "$global_before" >"$temporary/database-before.txt"
[ "$global_before" = '0|0|0|2' ] || fail "database authority boundary is $global_before, expected 0|0|0|2"

"$kubectl_bin" apply --dry-run=server -f "$manifests/00-namespace.yaml" >"$temporary/server-dry-run.log"
"$kubectl_bin" apply -f "$manifests/00-namespace.yaml" >"$temporary/apply-namespace.log"
mutated=true
for file in 10-configmaps.yaml 20-stack.yaml 30-network-policies.yaml 40-control-ingress.yaml; do
	"$kubectl_bin" apply --dry-run=server -f "$manifests/$file" >>"$temporary/server-dry-run.log"
done
"$kubectl_bin" apply -f "$manifests/10-configmaps.yaml" >"$temporary/apply-configmaps.log"
"$kubectl_bin" apply -f "$manifests/30-network-policies.yaml" >"$temporary/apply-observability-network.log"
"$kubectl_bin" apply -f "$manifests/40-control-ingress.yaml" >"$temporary/apply-control-ingress.log"
"$kubectl_bin" apply -f "$manifests/20-stack.yaml" >"$temporary/apply-stack.log"
"$kubectl_bin" rollout status deployment/vela-lab-prometheus --namespace "$namespace" --timeout=300s >"$temporary/prometheus-rollout.log"
"$kubectl_bin" rollout status deployment/vela-lab-alertmanager --namespace "$namespace" --timeout=300s >"$temporary/alertmanager-rollout.log"
"$kubectl_bin" rollout status deployment/vela-lab-grafana --namespace "$namespace" --timeout=300s >"$temporary/grafana-rollout.log"

"$script_dir/verify.sh" "$temporary/postflight" >"$temporary/verify.log"
printf 'status=LAB_REHEARSAL_PASS rollback=available production_gates=0/9\n' >"$temporary/STATUS"
(cd "$temporary" && find . -type f ! -name SHA256SUMS -print | LC_ALL=C sort | xargs sha256sum) >"$temporary/SHA256SUMS"
mv "$temporary" "$output"
trap - EXIT HUP INT TERM
printf 'schema=vela-lab-observability-deployment-v1 output=%s result=LAB_REHEARSAL_PASS production_gates=0/9\n' "$output"

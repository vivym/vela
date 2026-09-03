#!/bin/sh

set -eu

apply=${1:-}
namespace=vela-lab-v2
identity=vela-lab-v2
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}

fail() {
	printf 'rollback-vela-lab-v2: %s\n' "$*" >&2
	exit 1
}

[ "$apply" = --apply ] || fail "usage: $0 --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root to read the RKE2 kubeconfig"
[ -x "$kubectl_bin" ] || fail "$kubectl_bin is missing"
[ -r "$kubeconfig" ] || fail "$kubeconfig is unreadable"
export KUBECONFIG="$kubeconfig"

observed_environment=$($kubectl_bin get namespace "$namespace" -o jsonpath='{.metadata.labels.vela\.ai/environment}')
observed_identity=$($kubectl_bin get namespace "$namespace" -o jsonpath='{.metadata.labels.vela\.ai/deployment-identity}')
[ "$observed_environment" = non-production-lab ] || fail "namespace environment label is invalid"
[ "$observed_identity" = "$identity" ] || fail "namespace deployment identity is invalid"

selector="vela.ai/deployment-identity=$identity"
resource_types='deployment,statefulset,job,service,configmap,secret,serviceaccount,role,rolebinding,networkpolicy'
before=$($kubectl_bin get "$resource_types" --namespace "$namespace" --selector "$selector" \
	--ignore-not-found --no-headers 2>/dev/null | wc -l | tr -d ' ')
[ "$before" -gt 0 ] || fail "no identity-bound resources exist; refusing an ambiguous rollback"

workload_types='deployment,statefulset,job'
$kubectl_bin delete "$workload_types" --namespace "$namespace" --selector "$selector" \
	--ignore-not-found --cascade=foreground --wait=true --timeout=300s

remaining_dependents=$($kubectl_bin get 'pod,replicaset' --namespace "$namespace" --selector "$selector" \
	--ignore-not-found --no-headers 2>/dev/null | wc -l | tr -d ' ')
[ "$remaining_dependents" -eq 0 ] ||
	fail "$remaining_dependents identity-bound Pods or ReplicaSets remain; preserving policies and support resources"

$kubectl_bin delete networkpolicy --namespace "$namespace" --selector "$selector" \
	--ignore-not-found --wait=true --timeout=60s
$kubectl_bin delete 'service,configmap,secret,serviceaccount,role,rolebinding' \
	--namespace "$namespace" --selector "$selector" --ignore-not-found --wait=true --timeout=60s

remaining=$($kubectl_bin get "$resource_types" --namespace "$namespace" --selector "$selector" \
	--ignore-not-found --no-headers 2>/dev/null | wc -l | tr -d ' ')
[ "$remaining" -eq 0 ] || fail "$remaining identity-bound resources remain"

printf 'schema=vela-lab-rollback-v1 namespace=%s identity=%s removed=%s result=PASS host_data=preserved production_gates=0/9\n' \
	"$namespace" "$identity" "$before"

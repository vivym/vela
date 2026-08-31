#!/bin/sh

set -eu
umask 077

deployment_receipt=${1:-}
output=${2:-}
apply=${3:-}
namespace=vela-observability
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}

fail() {
	printf 'rollback-vela-lab-observability: %s\n' "$*" >&2
	exit 1
}

[ "$#" -eq 3 ] && [ "$apply" = --apply ] || fail "usage: $0 <deployment-receipt-directory> <new-output-directory> --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root"
[ "$(hostname)" = marslab-server ] || fail "run only on marslab-server"
case "$deployment_receipt" in /*) ;; *) fail "deployment receipt must be absolute" ;; esac
case "$output" in /*) ;; *) fail "output directory must be absolute" ;; esac
[ -d "$deployment_receipt" ] && [ ! -L "$deployment_receipt" ] || fail "deployment receipt is invalid"
[ -f "$deployment_receipt/control-ingress-before.json" ] || fail "control ingress prestate is absent"
grep -Fxq 'status=LAB_REHEARSAL_PASS rollback=available production_gates=0/9' "$deployment_receipt/STATUS" || fail "deployment receipt is not rollback-authorized"
[ ! -e "$output" ] && [ ! -L "$output" ] || fail "output path already exists"
[ -x "$kubectl_bin" ] && [ -r "$kubeconfig" ] || fail "RKE2 kubectl or kubeconfig is unavailable"
command -v jq >/dev/null 2>&1 || fail "jq is required"
export KUBECONFIG="$kubeconfig"
temporary=$(mktemp -d "$(dirname "$output")/.vela-observability-rollback.XXXXXX")
chmod 0700 "$temporary"

"$kubectl_bin" get namespace "$namespace" -o json >"$temporary/namespace-before.json"
jq -e '.metadata.labels["vela.ai/environment"] == "non-production-lab" and .metadata.labels["vela.ai/network-role"] == "observability"' \
	"$temporary/namespace-before.json" >/dev/null || fail "refusing to delete a namespace outside the exact lab boundary"
"$kubectl_bin" get networkpolicy control-ingress --namespace vela-lab -o json >"$temporary/control-ingress-before-rollback.json"
"$kubectl_bin" delete namespace "$namespace" --wait=true --timeout=180s >"$temporary/delete-namespace.log"
"$kubectl_bin" apply -f "$deployment_receipt/control-ingress-before.json" >"$temporary/restore-control-ingress.log"
[ -z "$("$kubectl_bin" get namespace "$namespace" --ignore-not-found -o name)" ] || fail "$namespace still exists"
"$kubectl_bin" rollout status deployment/vela-lab-control --namespace vela-lab --timeout=60s >/dev/null
"$kubectl_bin" rollout status deployment/vela-lab-worker-agent-1 --namespace vela-lab --timeout=60s >/dev/null
"$kubectl_bin" rollout status deployment/vela-lab-worker-agent-2 --namespace vela-lab --timeout=60s >/dev/null
"$kubectl_bin" get networkpolicy control-ingress --namespace vela-lab -o json >"$temporary/control-ingress-after.json"
printf 'schema=vela-lab-observability-rollback-v1\ncaptured_at=%s\nresult=PASS\nproduction_gates=0/9\nregistry_images=retained\ndocker_images=retained\nprune=none\n' \
	"$(date -u +%FT%TZ)" >"$temporary/STATUS"
(cd "$temporary" && find . -type f ! -name SHA256SUMS -print | LC_ALL=C sort | xargs sha256sum) >"$temporary/SHA256SUMS"
chmod 0600 "$temporary"/*
mv "$temporary" "$output"
printf 'schema=vela-lab-observability-rollback-v1 output=%s result=PASS production_gates=0/9\n' "$output"

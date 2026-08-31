#!/bin/sh

set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH='' cd -- "$script_dir/../../.." && pwd)
temporary=

fail() {
	printf 'test-vela-lab-observability: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	if [ -n "$temporary" ] && [ -d "$temporary" ]; then
		find "$temporary" -xdev -mindepth 1 -delete
		rmdir "$temporary"
	fi
}

for command_name in jq kubectl promtool shellcheck; do
	command -v "$command_name" >/dev/null 2>&1 || fail "$command_name is required"
done
trap cleanup EXIT HUP INT TERM

shellcheck "$script_dir"/*.sh
promtool check config --syntax-only "$script_dir/prometheus.yaml" >/dev/null
promtool check rules "$repository_root/deploy/observability/rules.yaml" "$script_dir/lab-rules.yaml" >/dev/null
(cd "$repository_root/deploy/observability" && promtool test rules rule-tests.yaml) >/dev/null
jq -e '.uid == "vela-statistical-slos" and .title == "Vela Statistical SLOs"' \
	"$repository_root/deploy/observability/dashboard.json" >/dev/null

temporary=$(mktemp -d /tmp/vela-observability-test.XXXXXX)
rendered=$temporary/rendered
"$script_dir/render-manifests.sh" "$script_dir/images.env" "$rendered" >/dev/null

[ "$(awk '$1 == "image:" {print $2}' "$rendered/20-stack.yaml" | wc -l | tr -d '[:space:]')" -eq 3 ] || fail "rendered image count drifted"
[ "$(grep -Ec '^10\.1\.200\.17:5443/observability/.+@sha256:[0-9a-f]{64}$' "$rendered/images.lock")" -eq 3 ] || fail "private digest lock drifted"
grep -Fxq 'compressed_source_bytes=624172137' "$rendered/images.lock" || fail "compressed size inventory drifted"
grep -Fxq 'production_gates=0/9' "$rendered/images.lock" || fail "Production Gate boundary is absent"
grep -q 'vela.ai/node-role: control-storage' "$rendered/20-stack.yaml" || fail "control-only placement is absent"
[ "$(grep -c 'automountServiceAccountToken: false' "$rendered/20-stack.yaml")" -eq 3 ] || fail "ServiceAccount token boundary drifted"
[ "$(grep -c 'readOnlyRootFilesystem: true' "$rendered/20-stack.yaml")" -eq 3 ] || fail "read-only root boundary drifted"
[ "$(grep -c 'type: ClusterIP' "$rendered/20-stack.yaml")" -eq 3 ] || fail "ClusterIP boundary drifted"
if grep -Eq 'hostPath:|persistentVolumeClaim:|nvidia.com/gpu|type: (NodePort|LoadBalancer)' "$rendered"/*.yaml; then
	fail "rendered manifests escaped the ephemeral non-GPU lab boundary"
fi
grep -q 'vela.ai/network-role: observability' "$rendered/40-control-ingress.yaml" || fail "observability namespace selector is absent"
grep -q 'vela.ai/client-role: otel-collector' "$rendered/40-control-ingress.yaml" || fail "collector Pod selector is absent"

printf 'schema=vela-lab-observability-test-v1 result=PASS images=3 compressed_source_bytes=624172137 production_gates=0/9\n'

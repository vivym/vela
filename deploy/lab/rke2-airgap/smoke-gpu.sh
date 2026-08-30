#!/bin/sh

set -eu

apply=${1:-}
namespace=vela-gpu-smoke
image='10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3'
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}

fail() {
	printf 'smoke-rke2-gpu: %s\n' "$*" >&2
	exit 1
}

[ "$apply" = --apply ] || fail "usage: $0 --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root to read the RKE2 kubeconfig"
[ -x "$kubectl_bin" ] || fail "$kubectl_bin is missing"
[ -r "$kubeconfig" ] || fail "$kubeconfig is unreadable"
export KUBECONFIG="$kubeconfig"

if $kubectl_bin get namespace "$namespace" >/dev/null 2>&1; then
	fail "namespace $namespace already exists; inspect it instead of replacing it"
fi

allocated=$($kubectl_bin get pods --all-namespaces -o json |
	jq '[.items[].spec.containers[].resources.limits["nvidia.com/gpu"]? // 0 | tonumber] | add // 0')
[ "$allocated" -eq 0 ] || fail "cluster already has $allocated requested GPUs"

$kubectl_bin apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: $namespace
  labels:
    vela.ai/managed-by: rke2-gpu-smoke
EOF

run_probe() {
	node=$1
	count=$2
	name=gpu-${count}-${node#vela-lab-}

	$kubectl_bin apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $name
  namespace: $namespace
  labels:
    vela.ai/managed-by: rke2-gpu-smoke
spec:
  nodeName: $node
  restartPolicy: Never
  activeDeadlineSeconds: 180
  automountServiceAccountToken: false
  tolerations:
    - key: vela.ai/h3
      operator: Equal
      value: "true"
      effect: NoSchedule
  containers:
    - name: probe
      image: $image
      imagePullPolicy: IfNotPresent
      command: [/bin/sh, -ec]
      args:
        - |
          observed=\$(nvidia-smi --query-gpu=uuid --format=csv,noheader,nounits | sed '/^[[:space:]]*\$/d' | sort -u | wc -l)
          test "\$observed" -eq "$count"
          printf 'node=%s expected_gpus=%s observed_gpus=%s\\n' "$node" "$count" "\$observed"
      resources:
        requests:
          nvidia.com/gpu: "$count"
        limits:
          nvidia.com/gpu: "$count"
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: [ALL]
EOF

	if ! $kubectl_bin wait --namespace "$namespace" \
		--for=jsonpath='{.status.phase}'=Succeeded "pod/$name" --timeout=180s; then
		$kubectl_bin describe --namespace "$namespace" "pod/$name" >&2 || true
		$kubectl_bin logs --namespace "$namespace" "$name" >&2 || true
		fail "probe $name failed; namespace is preserved for inspection"
	fi
	probe_output=$($kubectl_bin logs --namespace "$namespace" "$name")
	expected_output="node=$node expected_gpus=$count observed_gpus=$count"
	[ "$probe_output" = "$expected_output" ] ||
		fail "probe $name returned unexpected output: $probe_output"
	printf '%s\n' "$probe_output"
	$kubectl_bin delete --namespace "$namespace" "pod/$name" --wait=true
}

for node in vela-lab-worker-1 vela-lab-worker-2; do
	run_probe "$node" 1
	run_probe "$node" 8
done

namespace_owner=$($kubectl_bin get namespace "$namespace" \
	-o jsonpath='{.metadata.labels.vela\.ai/managed-by}')
[ "$namespace_owner" = rke2-gpu-smoke ] ||
	fail "namespace ownership label changed; refusing cleanup"
$kubectl_bin delete namespace "$namespace" --wait=true
printf 'result=PASS workers=2 probes=4 namespace=%s cleanup=complete\n' "$namespace"

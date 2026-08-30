#!/bin/sh

set -eu

mode=${1:-base}
failures=0
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}

fail() {
	printf 'verify-rke2-cluster: %s\n' "$*" >&2
	exit 1
}

pass_check() {
	printf 'check=%s status=PASS detail=%s\n' "$1" "$2"
}

fail_check() {
	printf 'check=%s status=FAIL detail=%s\n' "$1" "$2"
	failures=$((failures + 1))
}

check_equal() {
	if [ "$2" = "$3" ]; then
		pass_check "$1" "$2"
	else
		fail_check "$1" "observed=$2 expected=$3"
	fi
}

case "$mode" in
	base | gpu) ;;
	*) fail "usage: $0 [base|gpu]" ;;
esac
[ "$(id -u)" -eq 0 ] || fail "run as root to read the RKE2 kubeconfig"
command -v jq >/dev/null 2>&1 || fail "jq is required"
[ -x "$kubectl_bin" ] || fail "$kubectl_bin is missing"
[ -r "$kubeconfig" ] || fail "$kubeconfig is unreadable"
export KUBECONFIG="$kubeconfig"

printf 'schema=vela-rke2-cluster-postflight-v1\n'
printf 'captured_at=%s\n' "$(date -u +%FT%TZ)"
printf 'mode=%s\n' "$mode"

readyz=$($kubectl_bin get --raw='/readyz' 2>/dev/null || true)
check_equal api-readyz "$readyz" ok

nodes=$($kubectl_bin get nodes -o json)
check_equal node-count "$(printf '%s' "$nodes" | jq '.items | length')" 3

check_node() {
	name=$1
	ip=$2
	role=$3
	node=$(printf '%s' "$nodes" | jq -c --arg name "$name" '.items[] | select(.metadata.name == $name)')
	if [ -z "$node" ]; then
		fail_check "node-$name" absent
		return
	fi
	check_equal "node-$name-ready" \
		"$(printf '%s' "$node" | jq -r '.status.conditions[] | select(.type == "Ready") | .status')" True
	check_equal "node-$name-ip" \
		"$(printf '%s' "$node" | jq -r '.status.addresses[] | select(.type == "InternalIP") | .address')" "$ip"
	check_equal "node-$name-version" \
		"$(printf '%s' "$node" | jq -r '.status.nodeInfo.kubeletVersion')" v1.35.7+rke2r1
	if [ "$role" = control ]; then
		check_equal "node-$name-control-label" \
			"$(printf '%s' "$node" | jq -r '.metadata.labels["vela.ai/node-role"] // ""')" control-storage
		check_equal "node-$name-gpu-operands-disabled" \
			"$(printf '%s' "$node" | jq -r '.metadata.labels["nvidia.com/gpu.deploy.operands"] // ""')" false
		check_equal "node-$name-worker-label" \
			"$(printf '%s' "$node" | jq -r '.metadata.labels["vela.ai/worker-profile"] // ""')" ""
	else
		check_equal "node-$name-worker-profile" \
			"$(printf '%s' "$node" | jq -r '.metadata.labels["vela.ai/worker-profile"] // ""')" h3
		check_equal "node-$name-worker-pool" \
			"$(printf '%s' "$node" | jq -r '.metadata.labels["vela.ai/worker-pool"] // ""')" launch
		check_equal "node-$name-worker-taint" \
			"$(printf '%s' "$node" | jq -r 'any(.spec.taints[]?; .key == "vela.ai/h3" and .value == "true" and .effect == "NoSchedule")')" true
	fi
}

check_node vela-lab-control-1 10.1.200.17 control
check_node vela-lab-worker-1 10.1.200.19 worker
check_node vela-lab-worker-2 10.1.200.16 worker

for node_name in vela-lab-control-1 vela-lab-worker-1 vela-lab-worker-2; do
	kubelet_config=$($kubectl_bin get --raw="/api/v1/nodes/$node_name/proxy/configz" 2>/dev/null || true)
	if [ -z "$kubelet_config" ]; then
		fail_check "node-$node_name-kubelet-config" unavailable
		continue
	fi
	check_equal "node-$node_name-fail-swap-on" \
		"$(printf '%s' "$kubelet_config" | jq -r '.kubeletconfig.failSwapOn')" false
	check_equal "node-$node_name-pod-swap-policy" \
		"$(printf '%s' "$kubelet_config" | jq -r '.kubeletconfig.memorySwap.swapBehavior // "__MISSING__"')" NoSwap
done

daemonsets=$($kubectl_bin get daemonsets --all-namespaces -o json)
daemonset_failures=$(printf '%s' "$daemonsets" |
	jq --arg mode "$mode" '[.items[] | select(
		if ($mode == "gpu" and
			.metadata.namespace == "gpu-operator" and
			.metadata.name == "nvidia-device-plugin-mps-control-daemon")
		then
			(.status.desiredNumberScheduled // 0) != 0 or
			(.status.currentNumberScheduled // 0) != 0 or
			(.status.numberReady // 0) != 0
		else
			(.status.desiredNumberScheduled // 0) == 0 or
			(.status.numberReady // 0) != (.status.desiredNumberScheduled // 0)
		end
	)] | length')
check_equal daemonsets-ready "$daemonset_failures" 0

if [ "$mode" = gpu ]; then
	mps_daemonset=$(printf '%s' "$daemonsets" |
		jq -c '.items[] | select(.metadata.namespace == "gpu-operator" and .metadata.name == "nvidia-device-plugin-mps-control-daemon")')
	if [ -n "$mps_daemonset" ]; then
		check_equal gpu-mps-control-daemon-disabled \
			"$(printf '%s' "$mps_daemonset" | jq -r '[.status.desiredNumberScheduled // 0, .status.currentNumberScheduled // 0, .status.numberReady // 0] | join("/")')" \
			0/0/0
	else
		fail_check gpu-mps-control-daemon-disabled daemonset-absent
	fi
fi

deployments=$($kubectl_bin get deployments --all-namespaces -o json)
deployment_failures=$(printf '%s' "$deployments" |
	jq '[.items[] | select((.spec.replicas // 1) != (.status.availableReplicas // 0))] | length')
check_equal deployments-ready "$deployment_failures" 0

pods=$($kubectl_bin get pods --all-namespaces -o json)
pod_failures=$(printf '%s' "$pods" | jq '[.items[] |
	select(.status.phase != "Succeeded") |
	select(.status.phase != "Running" or any(.status.containerStatuses[]?; .ready != true))] | length')
check_equal pods-ready "$pod_failures" 0

canal=$(printf '%s' "$daemonsets" | jq -c '.items[] | select(.metadata.namespace == "kube-system" and .metadata.name == "rke2-canal")')
if [ -n "$canal" ]; then
	check_equal canal-ready \
		"$(printf '%s' "$canal" | jq -r '(.status.numberReady // 0) == (.status.desiredNumberScheduled // 0)')" true
else
	fail_check canal-ready daemonset-absent
fi

if [ "$mode" = gpu ]; then
	for node_name in vela-lab-worker-1 vela-lab-worker-2; do
		node=$(printf '%s' "$nodes" | jq -c --arg name "$node_name" '.items[] | select(.metadata.name == $name)')
		check_equal "node-$node_name-gpu-capacity" \
			"$(printf '%s' "$node" | jq -r '.status.capacity["nvidia.com/gpu"] // "0"')" 8
		check_equal "node-$node_name-gpu-allocatable" \
			"$(printf '%s' "$node" | jq -r '.status.allocatable["nvidia.com/gpu"] // "0"')" 8
	done
	control_node=$(printf '%s' "$nodes" | jq -c '.items[] | select(.metadata.name == "vela-lab-control-1")')
	check_equal control-gpu-allocatable \
		"$(printf '%s' "$control_node" | jq -r '.status.allocatable["nvidia.com/gpu"] // "0"')" 0
fi

if [ "$failures" -eq 0 ]; then
	printf 'result=PASS failures=0\n'
else
	printf 'result=FAIL failures=%s\n' "$failures"
	exit 1
fi

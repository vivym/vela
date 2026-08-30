#!/bin/sh

set -eu

manifests=${1:-}
phase=${2:-}
apply=${3:-}
namespace=vela-lab
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}

fail() {
	printf 'deploy-vela-lab-control-plane: %s\n' "$*" >&2
	exit 1
}

case "$phase:$apply" in
	namespace:--apply | dependencies:--apply | bootstrap:--apply | control:--apply | workers:--apply | network:--apply | all:--apply) ;;
	*) fail "usage: $0 <rendered-manifest-directory> <namespace|dependencies|bootstrap|control|workers|network|all> --apply" ;;
esac
[ "$(id -u)" -eq 0 ] || fail "run as root to read the RKE2 kubeconfig"
[ -x "$kubectl_bin" ] || fail "$kubectl_bin is missing"
[ -r "$kubeconfig" ] || fail "$kubeconfig is unreadable"
case "$manifests" in
	/*) ;;
	*) fail "manifest directory must be absolute" ;;
esac
[ ! -L "$manifests" ] || fail "manifest directory must not be a symlink"
[ -d "$manifests" ] || fail "manifest directory is absent"
for file in 00-namespace.yaml 10-dependencies.yaml 20-bootstrap.yaml 30-control.yaml 40-workers.yaml 50-network-policies.yaml; do
	[ -f "$manifests/$file" ] || fail "$file is absent"
done
image_list=$(awk '$1 == "image:" {print $2}' "$manifests"/*.yaml)
[ -n "$image_list" ] || fail "rendered manifests contain no images"
for image in $image_list; do
	printf '%s\n' "$image" | grep -Eq '^10\.1\.200\.17:5443/[a-z0-9][a-z0-9._/-]*@sha256:[0-9a-f]{64}$' ||
		fail "rendered manifests contain a mutable, unresolved, or external image"
done

export KUBECONFIG="$kubeconfig"
command -v jq >/dev/null 2>&1 || fail "jq is required"
nodes=$($kubectl_bin get nodes -o json)
[ "$(printf '%s' "$nodes" | jq '.items | length')" -eq 3 ] || fail "cluster must contain exactly three nodes"
check_node() {
	name=$1
	ip=$2
	role=$3
	node=$(printf '%s' "$nodes" | jq -c --arg name "$name" '.items[] | select(.metadata.name == $name)')
	[ -n "$node" ] || fail "node $name is absent"
	[ "$(printf '%s' "$node" | jq -r '.status.conditions[] | select(.type == "Ready") | .status')" = True ] || fail "node $name is not Ready"
	[ "$(printf '%s' "$node" | jq -r '.status.addresses[] | select(.type == "InternalIP") | .address')" = "$ip" ] || fail "node $name address drifted"
	if [ "$role" = control ]; then
		[ "$(printf '%s' "$node" | jq -r '.metadata.labels["vela.ai/node-role"] // ""')" = control-storage ] || fail "control-storage label is absent"
		[ "$(printf '%s' "$node" | jq -r '.status.allocatable["nvidia.com/gpu"] // "0"')" = 0 ] || fail "control node exposes allocatable GPUs"
	else
		[ "$(printf '%s' "$node" | jq -r '.metadata.labels["vela.ai/worker-profile"] // ""')" = h3 ] || fail "$name is not an H3 Worker"
		[ "$(printf '%s' "$node" | jq -r '.status.allocatable["nvidia.com/gpu"] // "0"')" = 8 ] || fail "$name does not expose eight GPUs"
	fi
}
check_node vela-lab-control-1 10.1.200.17 control
check_node vela-lab-worker-1 10.1.200.19 worker
check_node vela-lab-worker-2 10.1.200.16 worker

ensure_namespace() {
	$kubectl_bin apply -f "$manifests/00-namespace.yaml" >/dev/null
	environment=$($kubectl_bin get namespace "$namespace" -o jsonpath='{.metadata.labels.vela\.ai/environment}')
	[ "$environment" = non-production-lab ] || fail "namespace environment label is invalid"
}

ensure_network() {
	ensure_namespace
	$kubectl_bin apply -f "$manifests/50-network-policies.yaml" >/dev/null
}

load_asset_identity() {
	asset_manifest_sha256=$($kubectl_bin get configmap vela-lab-asset-identity --namespace "$namespace" \
		-o jsonpath='{.data.manifest-sha256}')
	printf '%s\n' "$asset_manifest_sha256" | grep -Eq '^[0-9a-f]{64}$' ||
		fail "installed asset identity is missing or invalid"
	for resource in \
		secret/vela-lab-bootstrap-env \
		secret/vela-lab-control-database-env \
		secret/vela-lab-control-secret-env \
		secret/vela-lab-bootstrap-files; do
		observed=$($kubectl_bin get "$resource" --namespace "$namespace" \
			-o jsonpath='{.metadata.annotations.vela\.ai/asset-manifest-sha256}')
		[ "$observed" = "$asset_manifest_sha256" ] ||
			fail "$resource does not match the installed asset identity"
	done
}

deploy_dependencies() {
	ensure_network
	for secret in vela-lab-postgres-env vela-lab-minio-env vela-lab-nats-config vela-lab-nats-tls; do
		$kubectl_bin get secret "$secret" --namespace "$namespace" >/dev/null || fail "secret $secret is absent"
	done
	$kubectl_bin apply -f "$manifests/10-dependencies.yaml" >/dev/null
	$kubectl_bin rollout status statefulset/vela-lab-postgres --namespace "$namespace" --timeout=300s
	$kubectl_bin rollout status statefulset/vela-lab-nats --namespace "$namespace" --timeout=300s
	$kubectl_bin rollout status deployment/vela-lab-minio --namespace "$namespace" --timeout=300s
}

deploy_bootstrap() {
	ensure_network
	load_asset_identity
	desired_job=$($kubectl_bin create --dry-run=client -f "$manifests/20-bootstrap.yaml" -o json |
		jq --arg digest "$asset_manifest_sha256" '
		  .metadata.annotations["vela.ai/asset-manifest-sha256"] = $digest
		  | .spec.template.metadata.annotations["vela.ai/asset-manifest-sha256"] = $digest')
	bootstrap_image=$(printf '%s\n' "$desired_job" |
		jq -er '.spec.template.spec.containers | map(select(.name == "bootstrap")) | .[0].image')
	if $kubectl_bin get job vela-lab-bootstrap --namespace "$namespace" >/dev/null 2>&1; then
		complete=$($kubectl_bin get job vela-lab-bootstrap --namespace "$namespace" -o jsonpath='{.status.succeeded}')
		[ "$complete" = 1 ] || fail "existing bootstrap Job is not successful; preserve it for diagnosis"
		observed_image=$($kubectl_bin get job vela-lab-bootstrap --namespace "$namespace" \
			-o jsonpath='{.spec.template.spec.containers[?(@.name=="bootstrap")].image}')
		observed_job_assets=$($kubectl_bin get job vela-lab-bootstrap --namespace "$namespace" \
			-o jsonpath='{.metadata.annotations.vela\.ai/asset-manifest-sha256}')
		observed_pod_assets=$($kubectl_bin get job vela-lab-bootstrap --namespace "$namespace" \
			-o jsonpath='{.spec.template.metadata.annotations.vela\.ai/asset-manifest-sha256}')
		[ "$observed_image" = "$bootstrap_image" ] || fail "existing bootstrap Job image does not match the rendered revision"
		[ "$observed_job_assets" = "$asset_manifest_sha256" ] && [ "$observed_pod_assets" = "$asset_manifest_sha256" ] ||
			fail "existing bootstrap Job does not match the installed asset identity"
		$kubectl_bin logs job/vela-lab-bootstrap --namespace "$namespace" | grep -Fx \
			'LAB_BOOTSTRAP=complete production_gate_receipts=0' >/dev/null || fail "existing bootstrap receipt is invalid"
		return
	fi
	printf '%s\n' "$desired_job" | $kubectl_bin apply -f - >/dev/null
	if ! $kubectl_bin wait job/vela-lab-bootstrap --namespace "$namespace" --for=condition=complete --timeout=900s; then
		$kubectl_bin describe job/vela-lab-bootstrap --namespace "$namespace" >&2 || true
		$kubectl_bin logs job/vela-lab-bootstrap --namespace "$namespace" --all-containers >&2 || true
		fail "bootstrap Job failed"
	fi
	$kubectl_bin logs job/vela-lab-bootstrap --namespace "$namespace" | grep -Fx \
		'LAB_BOOTSTRAP=complete production_gate_receipts=0' >/dev/null || fail "bootstrap receipt is invalid"
}

deploy_control() {
	ensure_network
	$kubectl_bin apply -f "$manifests/30-control.yaml" >/dev/null
	$kubectl_bin rollout status deployment/vela-lab-control --namespace "$namespace" --timeout=300s
}

deploy_workers() {
	ensure_network
	$kubectl_bin apply -f "$manifests/40-workers.yaml" >/dev/null
	$kubectl_bin rollout status deployment/vela-lab-worker-agent-1 --namespace "$namespace" --timeout=300s
	$kubectl_bin rollout status deployment/vela-lab-worker-agent-2 --namespace "$namespace" --timeout=300s
	for index in 1 2; do
		deployment=vela-lab-worker-agent-$index
		node=vela-lab-worker-$index
		observed=$($kubectl_bin get pods --namespace "$namespace" -l "app.kubernetes.io/name=$deployment" \
			-o jsonpath='{.items[0].spec.nodeName}')
		[ "$observed" = "$node" ] || fail "$deployment scheduled on $observed instead of $node"
	done
	requested_gpus=$($kubectl_bin get pods --namespace "$namespace" -o json |
		jq '[.items[].spec.containers[].resources.requests["nvidia.com/gpu"]? // 0 | tonumber] | add // 0')
	[ "$requested_gpus" -eq 0 ] || fail "Vela lab application Pods unexpectedly request GPUs"
}

case "$phase" in
	namespace) ensure_namespace ;;
	dependencies) deploy_dependencies ;;
	bootstrap) deploy_bootstrap ;;
	control) deploy_control ;;
	workers) deploy_workers ;;
	network) ensure_network ;;
	all)
		deploy_dependencies
		deploy_bootstrap
		deploy_control
		deploy_workers
		;;
esac

printf 'schema=vela-lab-deployment-v1 phase=%s namespace=%s result=PASS production_gates=0/9\n' \
	"$phase" "$namespace"

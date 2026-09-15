#!/usr/bin/env bash
set -euo pipefail

# Install cluster prerequisites without embedding credentials or Secret values.
# KUBECONFIG and the internal registry CA must already be configured.

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT
kube=${KUBECTL:-kubectl}

download_verified() {
  local url=$1 sha=$2 output=$3
  curl --fail --silent --show-error --location "$url" -o "$output"
  printf '%s  %s\n' "$sha" "$output" | sha256sum --check --status
}

download_verified \
  https://github.com/cert-manager/cert-manager/releases/download/v1.21.1/cert-manager.yaml \
  5f6a499b8c1857d57f560f536e0dcc830914b45c420899fe7ad0692c8624e408 \
  "$work_dir/cert-manager.yaml"
download_verified \
  https://github.com/cloudnative-pg/cloudnative-pg/releases/download/v1.30.0/cnpg-1.30.0.yaml \
  f8bede43fe4ee0d478c2355b204a36876b2ae4faac60f2a9452280b293da3b88 \
  "$work_dir/cnpg.yaml"

$kube label node llmpool01 vela.ai/control-plane-tier=cpu --overwrite
$kube label node llmpool02 vela.ai/control-plane-tier=cpu --overwrite
$kube label node marslab-gpu-01 vela.ai/control-plane-tier=gpu-shared --overwrite
$kube apply --server-side -f "$work_dir/cert-manager.yaml"
$kube apply --server-side -f "$work_dir/cnpg.yaml"
$kube apply --server-side -k "$repo_root/deploy/control-storage/barman-cloud-plugin-install"
$kube apply --server-side -f "$repo_root/deploy/management-cluster/local-path-storage.yaml"
$kube apply --server-side -f "$repo_root/deploy/management-cluster/operator-pdbs.yaml"

$kube -n cert-manager set image deployment/cert-manager \
  cert-manager-controller=quay.io/jetstack/cert-manager-controller@sha256:416a2d76870d996460e62bd7f521bf14fa017be9e3e904aab92163a331fcb61a
$kube -n cert-manager set image deployment/cert-manager-cainjector \
  cert-manager-cainjector=quay.io/jetstack/cert-manager-cainjector@sha256:ccf6b919ec0500745a47a910118f834f9636d0aac1ff221245cd2557ed8c7c98
$kube -n cert-manager set image deployment/cert-manager-webhook \
  cert-manager-webhook=quay.io/jetstack/cert-manager-webhook@sha256:d8b3961b51c8c7320633f8208dc46bf88aa13804d0f7cbe48a096b2c523cee42
$kube -n cnpg-system set image deployment/cnpg-controller-manager \
  manager=ghcr.io/cloudnative-pg/cloudnative-pg@sha256:a2701eb97cdd2a34b1fdb2cb51987f544b706e40bec72ae7146cd8580efefebb
$kube -n cnpg-system set env deployment/cnpg-controller-manager \
  OPERATOR_IMAGE_NAME=ghcr.io/cloudnative-pg/cloudnative-pg@sha256:a2701eb97cdd2a34b1fdb2cb51987f544b706e40bec72ae7146cd8580efefebb

for deployment in cert-manager cert-manager-cainjector cert-manager-webhook; do
  case "$deployment" in
    cert-manager) pod_name=cert-manager ;;
    cert-manager-cainjector) pod_name=cainjector ;;
    cert-manager-webhook) pod_name=webhook ;;
  esac
  $kube -n cert-manager patch deployment "$deployment" --type=merge -p \
    '{"spec":{"replicas":2,"strategy":{"type":"RollingUpdate","rollingUpdate":{"maxSurge":0,"maxUnavailable":1}},"template":{"spec":{"nodeSelector":{"vela.ai/control-plane-tier":"cpu"},"tolerations":[{"key":"node-role.kubernetes.io/control-plane","operator":"Exists","effect":"NoSchedule"}],"affinity":{"podAntiAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":[{"labelSelector":{"matchLabels":{"app.kubernetes.io/name":"'"$pod_name"'"}},"topologyKey":"kubernetes.io/hostname"}]}}}}}}'
done

$kube -n cnpg-system patch deployment cnpg-controller-manager --type=merge -p \
  '{"spec":{"replicas":2,"strategy":{"type":"RollingUpdate","rollingUpdate":{"maxSurge":0,"maxUnavailable":1}},"template":{"spec":{"nodeSelector":{"vela.ai/control-plane-tier":"cpu"},"tolerations":[{"key":"node-role.kubernetes.io/control-plane","operator":"Exists","effect":"NoSchedule"}],"affinity":{"podAntiAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":[{"labelSelector":{"matchLabels":{"app.kubernetes.io/name":"cloudnative-pg"}},"topologyKey":"kubernetes.io/hostname"}]}}}}}}'

$kube -n cnpg-system patch deployment barman-cloud --type=merge -p \
  '{"spec":{"replicas":2,"strategy":{"type":"RollingUpdate","rollingUpdate":{"maxSurge":0,"maxUnavailable":1}},"template":{"spec":{"nodeSelector":{"vela.ai/control-plane-tier":"cpu"},"tolerations":[{"key":"node-role.kubernetes.io/control-plane","operator":"Exists","effect":"NoSchedule"}],"affinity":{"podAntiAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":[{"labelSelector":{"matchLabels":{"app":"barman-cloud"}},"topologyKey":"kubernetes.io/hostname"}]}}}}}}'

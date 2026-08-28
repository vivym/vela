#!/bin/sh
set -eu

repository_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cluster_name=vela-cnpg-failover
kind_binary="$repository_root/bin/kind"
kind_version=v0.32.0
kind_node_image='kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5'
crane_binary="$repository_root/bin/crane"
crane_version=v0.20.6
image_platform=${VELA_CNPG_IMAGE_PLATFORM:-}
cnpg_version=v1.30.0
cnpg_sha256=f8bede43fe4ee0d478c2355b204a36876b2ae4faac60f2a9452280b293da3b88
cnpg_operator_image=ghcr.io/cloudnative-pg/cloudnative-pg:1.30.0
postgres_image=ghcr.io/cloudnative-pg/postgresql:16.4
test_directory=$(mktemp -d)
kubeconfig="$test_directory/kubeconfig"
created=false

if [ -z "$image_platform" ]; then
	image_arch=$(docker info --format '{{.Architecture}}' 2>/dev/null || go env GOARCH)
	case "$image_arch" in
	amd64 | x86_64)
		image_arch=amd64
		;;
	arm64 | aarch64)
		image_arch=arm64
		;;
	*)
		echo "unsupported Docker server architecture: $image_arch" >&2
		exit 1
		;;
	esac
	image_platform="linux/$image_arch"
fi
case "$image_platform" in
linux/amd64 | linux/arm64) ;;
*)
	echo "VELA_CNPG_IMAGE_PLATFORM must be linux/amd64 or linux/arm64, got $image_platform" >&2
	exit 1
	;;
esac

cleanup() {
	if [ "$created" = true ]; then
		"$kind_binary" delete cluster --name "$cluster_name" --kubeconfig "$kubeconfig" >/dev/null 2>&1 || true
	fi
	find "$test_directory" -xdev -type f -delete
	rmdir "$test_directory" 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM

mkdir -p "$repository_root/bin"
if [ ! -x "$kind_binary" ]; then
	GOBIN="$repository_root/bin" go install "sigs.k8s.io/kind@$kind_version"
fi
if [ ! -x "$crane_binary" ]; then
	GOBIN="$repository_root/bin" go install "github.com/google/go-containerregistry/cmd/crane@$crane_version"
fi
if "$kind_binary" get clusters | awk -v name="$cluster_name" '$0 == name { found = 1 } END { exit !found }'; then
	echo "refusing to reuse existing kind cluster $cluster_name" >&2
	exit 1
fi

created=true
"$kind_binary" create cluster \
	--name "$cluster_name" \
	--image "$kind_node_image" \
	--config "$repository_root/internal/integration/testdata/cnpg-kind.yaml" \
	--kubeconfig "$kubeconfig" \
	--wait 5m

pull_image() {
	image=$1
	archive_name=$2
	local_platform=$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image" 2>/dev/null || true)
	if [ "$local_platform" = "$image_platform" ]; then
		return
	fi

	archive="$test_directory/$archive_name"
	"$crane_binary" pull --platform "$image_platform" "$image" "$archive"
	docker load -i "$archive"
	rm "$archive"
	loaded_platform=$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image")
	if [ "$loaded_platform" != "$image_platform" ]; then
		echo "loaded $image for $loaded_platform, want $image_platform" >&2
		exit 1
	fi
}

pull_image "$cnpg_operator_image" cnpg-operator.tar
pull_image "$postgres_image" postgres.tar
"$kind_binary" load docker-image \
	--name "$cluster_name" \
	"$cnpg_operator_image" "$postgres_image"

cnpg_manifest="$test_directory/cnpg.yaml"
curl -fsSL --retry 5 --retry-all-errors --connect-timeout 15 --max-time 120 \
	-o "$cnpg_manifest" \
	"https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/$cnpg_version/releases/cnpg-${cnpg_version#v}.yaml"
printf '%s  %s\n' "$cnpg_sha256" "$cnpg_manifest" | shasum -a 256 -c -

kubectl --kubeconfig "$kubeconfig" apply \
	-f "$repository_root/internal/integration/testdata/cnpg-storage-class.yaml"
kubectl --kubeconfig "$kubeconfig" apply --server-side -f "$cnpg_manifest"
kubectl --kubeconfig "$kubeconfig" -n cnpg-system patch \
	deployment cnpg-controller-manager --type=strategic \
	-p '{"spec":{"template":{"spec":{"nodeSelector":{"node-role.kubernetes.io/control-plane":""},"tolerations":[{"key":"node-role.kubernetes.io/control-plane","operator":"Exists","effect":"NoSchedule"}],"containers":[{"name":"manager","imagePullPolicy":"IfNotPresent"}]}}}}'
kubectl --kubeconfig "$kubeconfig" -n cnpg-system rollout status \
	deployment/cnpg-controller-manager --timeout=5m

kubectl --kubeconfig "$kubeconfig" apply \
	-f "$repository_root/deploy/control-storage/namespace.yaml"
kubectl --kubeconfig "$kubeconfig" -n vela-system create secret generic vela-backup-s3 \
	--from-literal=ACCESS_KEY_ID=cnpg-conformance-only \
	--from-literal=SECRET_ACCESS_KEY=cnpg-conformance-only
apply_attempt=0
until kubectl --kubeconfig "$kubeconfig" -n vela-system apply \
	-f "$repository_root/deploy/control-storage/recovery-contract.yaml" \
	-f "$repository_root/deploy/control-storage/disruption-budgets.yaml" \
	-f "$repository_root/deploy/control-storage/postgres-cluster.yaml"
do
	apply_attempt=$((apply_attempt + 1))
	if [ "$apply_attempt" -ge 60 ]; then
		echo "CloudNativePG admission webhook did not accept the release Cluster" >&2
		exit 1
	fi
	sleep 1
done
patch_attempt=0
until kubectl --kubeconfig "$kubeconfig" -n vela-system patch \
	clusters.postgresql.cnpg.io vela-postgres \
	--type=json -p '[{"op":"add","path":"/spec/enableSuperuserAccess","value":true},{"op":"add","path":"/spec/imagePullPolicy","value":"IfNotPresent"},{"op":"replace","path":"/spec/imageName","value":"ghcr.io/cloudnative-pg/postgresql:16.4"},{"op":"remove","path":"/spec/plugins"}]'
do
	patch_attempt=$((patch_attempt + 1))
	if [ "$patch_attempt" -ge 60 ]; then
		echo "CloudNativePG Cluster did not become patchable" >&2
		exit 1
	fi
	sleep 1
done
kubectl --kubeconfig "$kubeconfig" -n vela-system wait \
	--for=condition=Ready clusters.postgresql.cnpg.io/vela-postgres --timeout=10m

VELA_CNPG_KUBECONFIG="$kubeconfig" \
VELA_CNPG_KIND_CLUSTER="$cluster_name" \
	go test -tags=integration,cnpg ./internal/integration \
		-run '^TestCloudNativePGSingleNodeFailoverPreservesAuthorityAndNoQuorumFailsClosed$' \
		-count=1 -timeout=20m -v

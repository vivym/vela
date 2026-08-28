#!/bin/sh
set -eu

repository_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
contract="$repository_root/deploy/control-storage/barman-cloud-plugin-contract.json"
cluster_name=vela-cnpg-pitr
kind_binary="$repository_root/bin/kind"
kind_version=v0.32.0
kind_node_image='kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5'
crane_binary="$repository_root/bin/crane"
crane_version=v0.20.6
image_platform=${VELA_CNPG_IMAGE_PLATFORM:-}
test_directory=$(mktemp -d)
kubeconfig="$test_directory/kubeconfig"
created=false
namespace=vela-system
source_cluster=vela-postgres
restore_cluster=vela-postgres-restored
backup_name=vela-pitr-base-backup
minio_access_key=vela-cnpg-pitr
minio_secret_key=vela-cnpg-pitr-secret

require_command() {
	if ! command -v "$1" >/dev/null 2>&1; then
		echo "$1 is required" >&2
		exit 1
	fi
}

require_command docker
require_command jq
require_command kubectl
require_command curl

if [ ! -s "$contract" ]; then
	echo "Barman Cloud Plugin contract is missing: $contract" >&2
	exit 1
fi

contract_value() {
	value=$(jq -er "$1" "$contract") || {
		echo "missing contract value $1" >&2
		exit 1
	}
	printf '%s\n' "$value"
}

cnpg_manifest_url=$(contract_value '.cloudnative_pg.manifest_url')
cnpg_manifest_sha256=$(contract_value '.cloudnative_pg.manifest_sha256')
cnpg_operator_identity=$(contract_value '.cloudnative_pg.operator_image')
postgres_identity=$(contract_value '.cloudnative_pg.postgres_image')
cert_manager_manifest_url=$(contract_value '.cert_manager.manifest_url')
cert_manager_manifest_sha256=$(contract_value '.cert_manager.manifest_sha256')
cert_manager_cainjector_identity=$(contract_value '.cert_manager.images.cainjector')
cert_manager_controller_identity=$(contract_value '.cert_manager.images.controller')
cert_manager_webhook_identity=$(contract_value '.cert_manager.images.webhook')
barman_manifest_url=$(contract_value '.barman_cloud_plugin.manifest_url')
barman_manifest_sha256=$(contract_value '.barman_cloud_plugin.manifest_sha256')
barman_operator_identity=$(contract_value '.barman_cloud_plugin.operator_image')
barman_sidecar_identity=$(contract_value '.barman_cloud_plugin.sidecar_image')
barman_plugin_name=$(contract_value '.barman_cloud_plugin.name')
minio_identity=$(contract_value '.local_conformance.minio_image')

image_tag() {
	case "$1" in
	*@sha256:*) printf '%s\n' "${1%@sha256:*}" ;;
	*)
		echo "image identity lacks an immutable digest: $1" >&2
		exit 1
		;;
	esac
}

cnpg_operator_image=$(image_tag "$cnpg_operator_identity")
postgres_image=$(image_tag "$postgres_identity")
cert_manager_cainjector_image=$(image_tag "$cert_manager_cainjector_identity")
cert_manager_controller_image=$(image_tag "$cert_manager_controller_identity")
cert_manager_webhook_image=$(image_tag "$cert_manager_webhook_identity")
barman_operator_image=$(image_tag "$barman_operator_identity")
barman_sidecar_image=$(image_tag "$barman_sidecar_identity")
minio_image=$(image_tag "$minio_identity")

if [ -z "$image_platform" ]; then
	image_arch=$(docker info --format '{{.Architecture}}' 2>/dev/null || go env GOARCH)
	case "$image_arch" in
	amd64 | x86_64) image_arch=amd64 ;;
	arm64 | aarch64) image_arch=arm64 ;;
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
	find "$test_directory" -xdev -depth -type d -empty -delete 2>/dev/null || true
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

download_verified() {
	url=$1
	want_sha256=$2
	target=$3
	curl -fsSL --retry 5 --retry-all-errors --connect-timeout 15 --max-time 120 \
		-o "$target" "$url"
	printf '%s  %s\n' "$want_sha256" "$target" | shasum -a 256 -c -
}

pull_image() {
	identity=$1
	archive_name=$2
	tag=$(image_tag "$identity")
	archive="$test_directory/$archive_name"
	"$crane_binary" pull --platform "$image_platform" "$identity" "$archive"
	load_output=$(docker load -i "$archive")
	find "$archive" -xdev -type f -delete
	loaded_reference=$(printf '%s\n' "$load_output" | sed -n 's/^Loaded image: //p' | tail -1)
	if [ -z "$loaded_reference" ]; then
		loaded_reference=$(printf '%s\n' "$load_output" | sed -n 's/^Loaded image ID: //p' | tail -1)
	fi
	if [ -z "$loaded_reference" ]; then
		echo "docker load did not report a loaded image for $identity" >&2
		exit 1
	fi
	docker tag "$loaded_reference" "$tag"
	if [ "$loaded_reference" != "$tag" ]; then
		docker image rm "$loaded_reference" >/dev/null 2>&1 || true
	fi
	loaded_platform=$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$tag")
	if [ "$loaded_platform" != "$image_platform" ]; then
		echo "loaded $tag for $loaded_platform, want $image_platform" >&2
		exit 1
	fi
}

wait_for_jsonpath() {
	resource_namespace=$1
	resource=$2
	path=$3
	want=$4
	attempts=$5
	attempt=0
	while [ "$attempt" -lt "$attempts" ]; do
		got=$(kubectl --kubeconfig "$kubeconfig" -n "$resource_namespace" get "$resource" \
			-o "jsonpath={$path}" 2>/dev/null || true)
		if [ "$got" = "$want" ]; then
			return
		fi
		attempt=$((attempt + 1))
		sleep 2
	done
	echo "$resource $path did not become $want; last value: $got" >&2
	kubectl --kubeconfig "$kubeconfig" -n "$resource_namespace" get "$resource" -o yaml >&2 || true
	exit 1
}

created=true
"$kind_binary" create cluster \
	--name "$cluster_name" \
	--image "$kind_node_image" \
	--config "$repository_root/internal/integration/testdata/cnpg-kind.yaml" \
	--kubeconfig "$kubeconfig" \
	--wait 5m

pull_image "$cert_manager_cainjector_identity" cert-manager-cainjector.tar
pull_image "$cert_manager_controller_identity" cert-manager-controller.tar
pull_image "$cert_manager_webhook_identity" cert-manager-webhook.tar
pull_image "$cnpg_operator_identity" cnpg-operator.tar
pull_image "$postgres_identity" postgres.tar
pull_image "$barman_operator_identity" barman-cloud-operator.tar
pull_image "$barman_sidecar_identity" barman-cloud-sidecar.tar
pull_image "$minio_identity" minio.tar

"$kind_binary" load docker-image --name "$cluster_name" \
	"$cert_manager_cainjector_image" \
	"$cert_manager_controller_image" \
	"$cert_manager_webhook_image" \
	"$cnpg_operator_image" \
	"$postgres_image" \
	"$barman_operator_image" \
	"$barman_sidecar_image" \
	"$minio_image"

cert_manager_manifest="$test_directory/cert-manager.yaml"
cnpg_manifest="$test_directory/cnpg.yaml"
barman_manifest="$test_directory/barman-cloud.yaml"
download_verified "$cert_manager_manifest_url" "$cert_manager_manifest_sha256" "$cert_manager_manifest"
download_verified "$cnpg_manifest_url" "$cnpg_manifest_sha256" "$cnpg_manifest"
download_verified "$barman_manifest_url" "$barman_manifest_sha256" "$barman_manifest"
for manifest_image in \
	"$cert_manager_cainjector_image" \
	"$cert_manager_controller_image" \
	"$cert_manager_webhook_image"
do
	if ! grep -F "$manifest_image" "$cert_manager_manifest" >/dev/null; then
		echo "cert-manager manifest does not reference $manifest_image" >&2
		exit 1
	fi
done
if ! grep -F "$cnpg_operator_image" "$cnpg_manifest" >/dev/null; then
	echo "CloudNativePG manifest does not reference $cnpg_operator_image" >&2
	exit 1
fi
if ! grep -F "$barman_operator_image" "$barman_manifest" >/dev/null; then
	echo "Barman Cloud manifest does not reference $barman_operator_image" >&2
	exit 1
fi

kubectl --kubeconfig "$kubeconfig" apply --server-side -f "$cert_manager_manifest"
kubectl --kubeconfig "$kubeconfig" -n cert-manager rollout status \
	deployment/cert-manager --timeout=5m
kubectl --kubeconfig "$kubeconfig" -n cert-manager rollout status \
	deployment/cert-manager-cainjector --timeout=5m
kubectl --kubeconfig "$kubeconfig" -n cert-manager rollout status \
	deployment/cert-manager-webhook --timeout=5m

kubectl --kubeconfig "$kubeconfig" apply --server-side -f "$cnpg_manifest"
kubectl --kubeconfig "$kubeconfig" -n cnpg-system patch \
	deployment cnpg-controller-manager --type=strategic \
	-p '{"spec":{"template":{"spec":{"nodeSelector":{"node-role.kubernetes.io/control-plane":""},"tolerations":[{"key":"node-role.kubernetes.io/control-plane","operator":"Exists","effect":"NoSchedule"}],"containers":[{"name":"manager","imagePullPolicy":"IfNotPresent"}]}}}}'
kubectl --kubeconfig "$kubeconfig" -n cnpg-system rollout status \
	deployment/cnpg-controller-manager --timeout=5m

kubectl --kubeconfig "$kubeconfig" apply --server-side -f "$barman_manifest"
kubectl --kubeconfig "$kubeconfig" -n cnpg-system patch \
	deployment barman-cloud --type=strategic \
	-p '{"spec":{"template":{"spec":{"nodeSelector":{"node-role.kubernetes.io/control-plane":""},"tolerations":[{"key":"node-role.kubernetes.io/control-plane","operator":"Exists","effect":"NoSchedule"}],"containers":[{"name":"barman-cloud","imagePullPolicy":"IfNotPresent"}]}}}}'
kubectl --kubeconfig "$kubeconfig" -n cnpg-system rollout status \
	deployment/barman-cloud --timeout=5m

kubectl --kubeconfig "$kubeconfig" apply \
	-f "$repository_root/internal/integration/testdata/cnpg-storage-class.yaml"
kubectl --kubeconfig "$kubeconfig" apply \
	-f "$repository_root/deploy/control-storage/namespace.yaml"
kubectl --kubeconfig "$kubeconfig" -n "$namespace" apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vela-pitr-minio
spec:
  replicas: 1
  selector:
    matchLabels:
      app: vela-pitr-minio
  template:
    metadata:
      labels:
        app: vela-pitr-minio
    spec:
      nodeSelector:
        node-role.kubernetes.io/control-plane: ""
      tolerations:
        - key: node-role.kubernetes.io/control-plane
          operator: Exists
          effect: NoSchedule
      containers:
        - name: minio
          image: $minio_image
          imagePullPolicy: IfNotPresent
          args: ["server", "/data", "--address", ":9000"]
          env:
            - name: MINIO_ROOT_USER
              value: $minio_access_key
            - name: MINIO_ROOT_PASSWORD
              value: $minio_secret_key
          ports:
            - name: s3
              containerPort: 9000
          readinessProbe:
            httpGet:
              path: /minio/health/ready
              port: s3
---
apiVersion: v1
kind: Service
metadata:
  name: vela-pitr-minio
spec:
  selector:
    app: vela-pitr-minio
  ports:
    - name: s3
      port: 9000
      targetPort: s3
EOF
kubectl --kubeconfig "$kubeconfig" -n "$namespace" rollout status \
	deployment/vela-pitr-minio --timeout=3m
kubectl --kubeconfig "$kubeconfig" -n "$namespace" exec deployment/vela-pitr-minio -- \
	mc alias set local http://127.0.0.1:9000 "$minio_access_key" "$minio_secret_key"
kubectl --kubeconfig "$kubeconfig" -n "$namespace" exec deployment/vela-pitr-minio -- \
	mc mb --ignore-existing local/vela-postgres-backup
kubectl --kubeconfig "$kubeconfig" -n "$namespace" exec deployment/vela-pitr-minio -- \
	mc version enable local/vela-postgres-backup

kubectl --kubeconfig "$kubeconfig" -n "$namespace" create secret generic vela-backup-s3 \
	--from-literal=ACCESS_KEY_ID="$minio_access_key" \
	--from-literal=SECRET_ACCESS_KEY="$minio_secret_key"
kubectl --kubeconfig "$kubeconfig" -n "$namespace" apply \
	-f "$repository_root/deploy/control-storage/postgres-object-store.yaml"
kubectl --kubeconfig "$kubeconfig" -n "$namespace" patch \
	objectstores.barmancloud.cnpg.io vela-postgres-backup --type=merge \
	-p '{"spec":{"configuration":{"endpointURL":"http://vela-pitr-minio:9000"}}}'
kubectl --kubeconfig "$kubeconfig" -n "$namespace" apply \
	-f "$repository_root/deploy/control-storage/postgres-cluster.yaml"
kubectl --kubeconfig "$kubeconfig" -n "$namespace" patch \
	clusters.postgresql.cnpg.io "$source_cluster" --type=merge \
	-p "{\"spec\":{\"enableSuperuserAccess\":true,\"imageName\":\"$postgres_image\",\"imagePullPolicy\":\"IfNotPresent\",\"resources\":{\"requests\":{\"cpu\":\"100m\",\"memory\":\"256Mi\"},\"limits\":{\"cpu\":\"2\",\"memory\":\"2Gi\"}}}}"
kubectl --kubeconfig "$kubeconfig" -n "$namespace" apply \
	-f "$repository_root/deploy/control-storage/postgres-scheduled-backup.yaml"
kubectl --kubeconfig "$kubeconfig" -n "$namespace" wait \
	--for=condition=Ready "clusters.postgresql.cnpg.io/$source_cluster" --timeout=10m

source_primary=$(kubectl --kubeconfig "$kubeconfig" -n "$namespace" get \
	"clusters.postgresql.cnpg.io/$source_cluster" -o jsonpath='{.status.currentPrimary}')
sidecar_images=$(kubectl --kubeconfig "$kubeconfig" -n "$namespace" get pod "$source_primary" \
	-o jsonpath='{.spec.initContainers[*].image} {.spec.containers[*].image}')
case "$sidecar_images" in
*"$barman_sidecar_image"*) ;;
*)
	echo "Barman Cloud sidecar was not injected: $sidecar_images" >&2
	exit 1
	;;
esac
sidecar_restart_policy=$(kubectl --kubeconfig "$kubeconfig" -n "$namespace" get pod "$source_primary" \
	-o "jsonpath={.spec.initContainers[?(@.image=='$barman_sidecar_image')].restartPolicy}")
if [ "$sidecar_restart_policy" != "Always" ]; then
	echo "Barman Cloud native sidecar restart policy = $sidecar_restart_policy, want Always" >&2
	exit 1
fi

source_sql() {
	kubectl --kubeconfig "$kubeconfig" -n "$namespace" exec "$source_primary" \
		-c postgres -- psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres -Atqc "$1"
}

source_sql "CREATE TABLE pitr_markers (name text PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT clock_timestamp()); INSERT INTO pitr_markers(name) VALUES ('before-target'); CHECKPOINT;"
kubectl --kubeconfig "$kubeconfig" -n "$namespace" apply -f - <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata:
  name: $backup_name
spec:
  method: plugin
  cluster:
    name: $source_cluster
  pluginConfiguration:
    name: $barman_plugin_name
EOF
wait_for_jsonpath "$namespace" "backup/$backup_name" '.status.phase' completed 300

recovery_target=$(source_sql "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US') || 'Z';")
case "$recovery_target" in
????-??-??T??:??:??.??????Z) ;;
*)
	echo "PostgreSQL produced a non-RFC3339 recovery target: $recovery_target" >&2
	exit 1
	;;
esac
source_sql "SELECT pg_sleep(1); INSERT INTO pitr_markers(name) VALUES ('after-target');"
archived_wal=$(source_sql "SELECT pg_walfile_name(pg_current_wal_lsn());")
source_sql "SELECT pg_switch_wal();" >/dev/null
archive_attempt=0
while [ "$archive_attempt" -lt 150 ]; do
	last_archived_wal=$(source_sql "SELECT COALESCE(last_archived_wal, '') FROM pg_stat_archiver;")
	if [ "$last_archived_wal" = "$archived_wal" ]; then
		break
	fi
	archive_attempt=$((archive_attempt + 1))
	sleep 2
done
if [ "$last_archived_wal" != "$archived_wal" ]; then
	echo "WAL $archived_wal was not archived; last archived WAL: $last_archived_wal" >&2
	exit 1
fi

kubectl --kubeconfig "$kubeconfig" -n "$namespace" apply -f - <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: $restore_cluster
spec:
  instances: 1
  enableSuperuserAccess: true
  imageName: $postgres_image
  imagePullPolicy: IfNotPresent
  bootstrap:
    recovery:
      source: source
      recoveryTarget:
        targetTime: "$recovery_target"
  externalClusters:
    - name: source
      plugin:
        name: $barman_plugin_name
        parameters:
          barmanObjectName: vela-postgres-backup
          serverName: $source_cluster
  storage:
    size: 1Gi
    storageClass: local-path
  walStorage:
    size: 1Gi
    storageClass: local-path
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
    limits:
      cpu: "2"
      memory: 2Gi
EOF
kubectl --kubeconfig "$kubeconfig" -n "$namespace" wait \
	--for=condition=Ready "clusters.postgresql.cnpg.io/$restore_cluster" --timeout=10m
restore_primary=$(kubectl --kubeconfig "$kubeconfig" -n "$namespace" get \
	"clusters.postgresql.cnpg.io/$restore_cluster" -o jsonpath='{.status.currentPrimary}')
restore_sql() {
	kubectl --kubeconfig "$kubeconfig" -n "$namespace" exec "$restore_primary" \
		-c postgres -- psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres -Atqc "$1"
}
before_count=$(restore_sql "SELECT count(*) FROM pitr_markers WHERE name = 'before-target';")
after_count=$(restore_sql "SELECT count(*) FROM pitr_markers WHERE name = 'after-target';")
if [ "$before_count" != 1 ] || [ "$after_count" != 0 ]; then
	echo "restored marker counts before=$before_count after=$after_count, want 1/0" >&2
	exit 1
fi

printf '%s\n' \
	"[VERIFIED] Barman Cloud Plugin PITR conformance" \
	"- platform: $image_platform" \
	"- source cluster: $source_cluster" \
	"- restore cluster: $restore_cluster" \
	"- backup: $backup_name (completed)" \
	"- recovery target: $recovery_target" \
	"- archived WAL: $archived_wal" \
	"- restored markers: before=$before_count after=$after_count"

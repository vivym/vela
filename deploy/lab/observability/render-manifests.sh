#!/bin/sh

set -eu

images=${1:-}
output=${2:-}
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH='' cd -- "$script_dir/../../.." && pwd)
kubectl_bin=${KUBECTL_BIN:-kubectl}

fail() {
	printf 'render-vela-lab-observability: %s\n' "$*" >&2
	exit 1
}

[ -f "$images" ] || fail "usage: $0 <images.env> <new-output-directory>"
case "$output" in
	/*) ;;
	*) fail "output directory must be absolute" ;;
esac
[ ! -e "$output" ] && [ ! -L "$output" ] || fail "output path already exists"
command -v "$kubectl_bin" >/dev/null 2>&1 || fail "$kubectl_bin is required"

# shellcheck disable=SC1090
. "$images"

validate_image() {
	name=$1
	target=$2
	digest=$3
	printf '%s\n' "$target" | grep -Eq '^10\.1\.200\.17:5443/observability/[a-z0-9._/-]+:[A-Za-z0-9._+-]+$' ||
		fail "$name target is not in the fixed private Registry namespace"
	printf '%s\n' "$digest" | grep -Eq '^sha256:[0-9a-f]{64}$' ||
		fail "$name digest is invalid"
}

validate_image prometheus "$PROMETHEUS_TARGET" "$PROMETHEUS_AMD64_MANIFEST"
validate_image alertmanager "$ALERTMANAGER_TARGET" "$ALERTMANAGER_AMD64_MANIFEST"
validate_image grafana "$GRAFANA_TARGET" "$GRAFANA_AMD64_MANIFEST"
[ "$((PROMETHEUS_COMPRESSED_BYTES + ALERTMANAGER_COMPRESSED_BYTES + GRAFANA_COMPRESSED_BYTES))" -eq 624172137 ] ||
	fail "compressed image inventory drifted"

temporary=$(mktemp -d "$(dirname "$output")/.vela-observability-render.XXXXXX")
committed=false
cleanup() {
	if [ "$committed" != true ]; then
		find "$temporary" -xdev -mindepth 1 -delete
		rmdir "$temporary"
	fi
}
trap cleanup EXIT HUP INT TERM
chmod 0750 "$temporary"

install -m 0640 "$script_dir/00-namespace.yaml" "$temporary/00-namespace.yaml"
install -m 0640 "$script_dir/20-network-policies.yaml" "$temporary/30-network-policies.yaml"
install -m 0640 "$script_dir/30-control-ingress.yaml" "$temporary/40-control-ingress.yaml"

{
	"$kubectl_bin" create configmap vela-lab-prometheus-config \
		--namespace vela-observability \
		--from-file=prometheus.yml="$script_dir/prometheus.yaml" \
		--dry-run=client -o yaml
	printf '%s\n' '---'
	"$kubectl_bin" create configmap vela-lab-prometheus-rules \
		--namespace vela-observability \
		--from-file=rules.yaml="$repository_root/deploy/observability/rules.yaml" \
		--from-file=lab-rules.yaml="$script_dir/lab-rules.yaml" \
		--dry-run=client -o yaml
	printf '%s\n' '---'
	"$kubectl_bin" create configmap vela-lab-alertmanager-config \
		--namespace vela-observability \
		--from-file=alertmanager.yml="$script_dir/alertmanager.yaml" \
		--dry-run=client -o yaml
	printf '%s\n' '---'
	"$kubectl_bin" create configmap vela-lab-grafana-provisioning \
		--namespace vela-observability \
		--from-file=datasource.yaml="$script_dir/grafana-datasource.yaml" \
		--from-file=dashboard-provider.yaml="$script_dir/grafana-dashboard-provider.yaml" \
		--dry-run=client -o yaml
	printf '%s\n' '---'
	"$kubectl_bin" create configmap vela-lab-grafana-dashboard \
		--namespace vela-observability \
		--from-file=dashboard.json="$repository_root/deploy/observability/dashboard.json" \
		--dry-run=client -o yaml
} >"$temporary/10-configmaps.yaml"
chmod 0640 "$temporary/10-configmaps.yaml"

sed \
	-e "s|__PROMETHEUS_IMAGE__|$PROMETHEUS_TARGET@$PROMETHEUS_AMD64_MANIFEST|g" \
	-e "s|__ALERTMANAGER_IMAGE__|$ALERTMANAGER_TARGET@$ALERTMANAGER_AMD64_MANIFEST|g" \
	-e "s|__GRAFANA_IMAGE__|$GRAFANA_TARGET@$GRAFANA_AMD64_MANIFEST|g" \
	"$script_dir/10-stack.yaml.tmpl" >"$temporary/20-stack.yaml"
chmod 0640 "$temporary/20-stack.yaml"

grep -Eq '__[A-Z0-9_]+__' "$temporary/20-stack.yaml" && fail "unresolved image placeholder"
image_count=$(awk '$1 == "image:" {print $2}' "$temporary/20-stack.yaml" | tee "$temporary/images.lock" | wc -l | tr -d '[:space:]')
[ "$image_count" -eq 3 ] || fail "rendered stack has $image_count images, expected 3"
if grep -Evq '^10\.1\.200\.17:5443/observability/[a-z0-9._/-]+:[A-Za-z0-9._+-]+@sha256:[0-9a-f]{64}$' "$temporary/images.lock"; then
	fail "rendered stack contains an external, mutable, or unresolved image"
fi
grep -Eq 'hostPath:|persistentVolumeClaim:|type: (NodePort|LoadBalancer)' "$temporary"/*.yaml &&
	fail "rendered stack escaped the ephemeral ClusterIP lab boundary"
printf 'compressed_source_bytes=%s\nproduction_gates=0/9\n' 624172137 >>"$temporary/images.lock"
chmod 0640 "$temporary/images.lock"

mv "$temporary" "$output"
committed=true
printf 'schema=vela-lab-observability-render-v1 output=%s images=3 compressed_source_bytes=624172137 result=PASS production_gates=0/9\n' "$output"

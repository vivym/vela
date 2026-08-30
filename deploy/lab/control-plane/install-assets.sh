#!/bin/sh

set -eu

assets=${1:-}
apply=${2:-}
namespace=vela-lab
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}
manifest_checks=

fail() {
	printf 'install-vela-lab-assets: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	[ -z "$manifest_checks" ] || rm -f -- "$manifest_checks"
}
trap cleanup EXIT HUP INT TERM

[ "$apply" = --apply ] || fail "usage: $0 <absolute-asset-directory> --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root to read the RKE2 kubeconfig"
[ -x "$kubectl_bin" ] || fail "$kubectl_bin is missing"
[ -r "$kubeconfig" ] || fail "$kubeconfig is unreadable"
case "$assets" in
	/*) ;;
	*) fail "asset directory must be absolute" ;;
esac
[ ! -L "$assets" ] || fail "asset directory must not be a symlink"
[ -d "$assets" ] || fail "asset directory is absent"
[ "$(stat -c %a "$assets")" = 700 ] || fail "asset directory mode must be 0700"
[ "$(stat -c %u "$assets")" -eq 0 ] || fail "asset directory must be root-owned"
if find "$assets" -type l -print -quit | grep -q .; then
	fail "asset directory contains a symlink"
fi
if find "$assets" -type f ! -perm 0600 -print -quit | grep -q .; then
	fail "every asset file must have mode 0600"
fi
for path in \
	manifest.json \
	env/postgres.env env/minio.env env/bootstrap.env env/database.env \
	env/control-secret.env env/control-public.env \
	nats/nats.conf nats/outbox.creds nats/scheduler.creds nats/bootstrap.creds \
	pki/ca.crt pki/nats-server.crt pki/nats-server.key pki/nats-client.crt pki/nats-client.key \
	pki/control-worker.crt pki/control-worker.key pki/control-fleet.crt pki/control-fleet.key \
	pki/control-finance.crt pki/control-finance.key pki/control-compliance.crt pki/control-compliance.key \
	pki/control-remediation.crt pki/control-remediation.key \
	pki/worker-1.crt pki/worker-1.key pki/worker-2.crt pki/worker-2.key \
	control/lease.json control/webhook.json control/minio-access-key control/minio-secret-key \
	control/invoice-bearer-token control/node-agents.json \
	bootstrap/smoke-secret smoke/bearer-credential; do
	[ -f "$assets/$path" ] || fail "required asset $path is absent"
done
for command in jq sha256sum; do command -v "$command" >/dev/null 2>&1 || fail "$command is required"; done
jq -e '
  .schema_version == 1
  and .environment == "non-production-lab"
  and .production_gate_evidence == false
  and (.files | type == "object" and length > 0)
  and all(.files | to_entries[];
    (.key | test("^[A-Za-z0-9._/-]+$") and (startswith("/") | not) and (contains("../") | not))
    and (.value | test("^[0-9a-f]{64}$")))
' \
	"$assets/manifest.json" >/dev/null || fail "asset manifest is invalid"
asset_manifest_sha256=$(sha256sum "$assets/manifest.json" | awk '{print $1}')
manifest_checks=$(mktemp /run/vela-lab-asset-manifest.XXXXXX)
chmod 0600 "$manifest_checks"
jq -r '.files | to_entries | sort_by(.key)[] | "\(.value)  \(.key)"' \
	"$assets/manifest.json" >"$manifest_checks"
manifested_count=$(wc -l <"$manifest_checks" | tr -d ' ')
observed_count=$(find "$assets" -type f ! -path "$assets/manifest.json" | wc -l | tr -d ' ')
[ "$manifested_count" -eq "$observed_count" ] || fail "asset manifest does not cover the exact file set"
(
	cd "$assets"
	sha256sum --check --strict "$manifest_checks" >/dev/null
) || fail "asset manifest file digests do not verify"

export KUBECONFIG="$kubeconfig"
$kubectl_bin apply -f - >/dev/null <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: vela-lab
  labels:
    app.kubernetes.io/part-of: vela
    vela.ai/environment: non-production-lab
EOF

managed_resources='secret/vela-lab-postgres-env
secret/vela-lab-minio-env
secret/vela-lab-bootstrap-env
secret/vela-lab-control-database-env
secret/vela-lab-control-secret-env
secret/vela-lab-nats-config
secret/vela-lab-nats-tls
secret/vela-lab-bootstrap-files
secret/vela-lab-control-files
secret/vela-lab-worker-1-tls
secret/vela-lab-worker-2-tls
secret/vela-lab-smoke-credential
configmap/vela-lab-control-generated-public
configmap/vela-lab-asset-identity'
managed_count=0
existing_count=0
for resource in $managed_resources; do
	managed_count=$((managed_count + 1))
	if $kubectl_bin get "$resource" --namespace "$namespace" >/dev/null 2>&1; then
		existing_count=$((existing_count + 1))
		observed=$($kubectl_bin get "$resource" --namespace "$namespace" \
			-o jsonpath='{.metadata.annotations.vela\.ai/asset-manifest-sha256}')
		[ "$observed" = "$asset_manifest_sha256" ] ||
			fail "existing $resource belongs to a different or unbound asset set"
	fi
done
[ "$existing_count" -eq 0 ] || [ "$existing_count" -eq "$managed_count" ] ||
	fail "managed asset resources are only partially installed"

apply_secret() {
	name=$1
	shift
	$kubectl_bin create secret generic "$name" --namespace "$namespace" "$@" \
		--dry-run=client -o yaml | $kubectl_bin apply -f - >/dev/null
	$kubectl_bin label secret "$name" --namespace "$namespace" \
		vela.ai/environment=non-production-lab --overwrite >/dev/null
	$kubectl_bin annotate secret "$name" --namespace "$namespace" \
		vela.ai/asset-manifest-sha256="$asset_manifest_sha256" --overwrite >/dev/null
}

apply_secret vela-lab-postgres-env --from-env-file="$assets/env/postgres.env"
apply_secret vela-lab-minio-env --from-env-file="$assets/env/minio.env"
apply_secret vela-lab-bootstrap-env --from-env-file="$assets/env/bootstrap.env"
apply_secret vela-lab-control-database-env --from-env-file="$assets/env/database.env"
apply_secret vela-lab-control-secret-env --from-env-file="$assets/env/control-secret.env"

$kubectl_bin create configmap vela-lab-control-generated-public --namespace "$namespace" \
	--from-env-file="$assets/env/control-public.env" --dry-run=client -o yaml |
	$kubectl_bin apply -f - >/dev/null
$kubectl_bin label configmap vela-lab-control-generated-public --namespace "$namespace" \
	vela.ai/environment=non-production-lab --overwrite >/dev/null
$kubectl_bin annotate configmap vela-lab-control-generated-public --namespace "$namespace" \
	vela.ai/asset-manifest-sha256="$asset_manifest_sha256" --overwrite >/dev/null

apply_secret vela-lab-nats-config --from-file=nats.conf="$assets/nats/nats.conf"
apply_secret vela-lab-nats-tls \
	--from-file=ca.crt="$assets/pki/ca.crt" \
	--from-file=tls.crt="$assets/pki/nats-server.crt" \
	--from-file=tls.key="$assets/pki/nats-server.key"
apply_secret vela-lab-bootstrap-files \
	--from-file=bootstrap.creds="$assets/nats/bootstrap.creds" \
	--from-file=nats-ca.crt="$assets/pki/ca.crt" \
	--from-file=nats-tls.crt="$assets/pki/nats-client.crt" \
	--from-file=nats-tls.key="$assets/pki/nats-client.key" \
	--from-file=smoke-secret="$assets/bootstrap/smoke-secret"
apply_secret vela-lab-control-files \
	--from-file=ca.crt="$assets/pki/ca.crt" \
	--from-file=worker-tls.crt="$assets/pki/control-worker.crt" \
	--from-file=worker-tls.key="$assets/pki/control-worker.key" \
	--from-file=fleet-tls.crt="$assets/pki/control-fleet.crt" \
	--from-file=fleet-tls.key="$assets/pki/control-fleet.key" \
	--from-file=finance-tls.crt="$assets/pki/control-finance.crt" \
	--from-file=finance-tls.key="$assets/pki/control-finance.key" \
	--from-file=compliance-tls.crt="$assets/pki/control-compliance.crt" \
	--from-file=compliance-tls.key="$assets/pki/control-compliance.key" \
	--from-file=remediation-tls.crt="$assets/pki/control-remediation.crt" \
	--from-file=remediation-tls.key="$assets/pki/control-remediation.key" \
	--from-file=nats-ca.crt="$assets/pki/ca.crt" \
	--from-file=nats-tls.crt="$assets/pki/nats-client.crt" \
	--from-file=nats-tls.key="$assets/pki/nats-client.key" \
	--from-file=outbox.creds="$assets/nats/outbox.creds" \
	--from-file=scheduler.creds="$assets/nats/scheduler.creds" \
	--from-file=minio-access-key="$assets/control/minio-access-key" \
	--from-file=minio-secret-key="$assets/control/minio-secret-key" \
	--from-file=lease.json="$assets/control/lease.json" \
	--from-file=webhook.json="$assets/control/webhook.json" \
	--from-file=invoice-bearer-token="$assets/control/invoice-bearer-token" \
	--from-file=node-agents.json="$assets/control/node-agents.json"
apply_secret vela-lab-worker-1-tls \
	--from-file=ca.crt="$assets/pki/ca.crt" \
	--from-file=tls.crt="$assets/pki/worker-1.crt" \
	--from-file=tls.key="$assets/pki/worker-1.key"
apply_secret vela-lab-worker-2-tls \
	--from-file=ca.crt="$assets/pki/ca.crt" \
	--from-file=tls.crt="$assets/pki/worker-2.crt" \
	--from-file=tls.key="$assets/pki/worker-2.key"
apply_secret vela-lab-smoke-credential \
	--from-file=bearer-credential="$assets/smoke/bearer-credential"

$kubectl_bin create configmap vela-lab-asset-identity --namespace "$namespace" \
	--from-literal=manifest-sha256="$asset_manifest_sha256" --dry-run=client -o yaml |
	$kubectl_bin apply -f - >/dev/null
$kubectl_bin label configmap vela-lab-asset-identity --namespace "$namespace" \
	vela.ai/environment=non-production-lab --overwrite >/dev/null
$kubectl_bin annotate configmap vela-lab-asset-identity --namespace "$namespace" \
	vela.ai/asset-manifest-sha256="$asset_manifest_sha256" --overwrite >/dev/null

secret_count=$($kubectl_bin get secrets --namespace "$namespace" \
	-l 'vela.ai/environment=non-production-lab' --no-headers 2>/dev/null | wc -l | tr -d ' ')
printf 'schema=vela-lab-asset-install-v1 namespace=%s result=PASS assets=%s asset_manifest_sha256=%s managed_secret_label_count=%s\n' \
	"$namespace" "$assets" "$asset_manifest_sha256" "$secret_count"

#!/bin/sh

set -eu

manifests=${1:-}
apply=${2:-}
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
namespace=vela-lab-v2
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}

fail() {
	printf 'vela-lab-exact-cache: %s\n' "$*" >&2
	exit 1
}

[ "$apply" = --apply ] || fail "usage: $0 <rendered-manifest-directory> --apply [seed]"
command -v jq >/dev/null 2>&1 || fail "jq is required"
seed=${3:-$(od -An -N4 -tu4 /dev/urandom | tr -d ' ')}
case "$seed" in ''|*[!0-9]*) fail "seed must be a non-negative integer" ;; esac
export KUBECONFIG="$kubeconfig"

query_evidence() {
	{
		printf 'BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;\nPREPARE lab_cache_evidence(uuid, uuid) AS\n'
		cat "$script_dir/exact-cache-evidence.sql"
		printf "EXECUTE lab_cache_evidence(:'source_job_id'::uuid, :'target_job_id'::uuid);\nCOMMIT;\n"
	} | "$kubectl_bin" exec --stdin --namespace "$namespace" statefulset/vela-lab-postgres -- sh -ec \
		'PGPASSWORD=$POSTGRES_PASSWORD exec psql --no-psqlrc --quiet --tuples-only --no-align --set ON_ERROR_STOP=1 --set source_job_id="$1" --set target_job_id="$2" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' \
		sh "$source_job_id" "$target_job_id"
}

source_receipt=$(sh "$script_dir/smoke.sh" "$manifests" --apply "$seed")
source_job_id=$(printf '%s\n' "$source_receipt" | jq -er '.job_id')
target_job_id=$source_job_id
deadline=$(($(date +%s) + 60))
while :; do
	evidence=$(query_evidence)
	if printf '%s\n' "$evidence" | jq -e '.source_ready == true' >/dev/null; then
		break
	fi
	[ "$(date +%s)" -lt "$deadline" ] || fail "source did not execute and admit both cache stages; preserve Jobs and use a fresh seed"
	sleep 1
done

target_receipt=$(sh "$script_dir/smoke.sh" "$manifests" --apply "$seed")
target_job_id=$(printf '%s\n' "$target_receipt" | jq -er '.job_id')
[ "$source_job_id" != "$target_job_id" ] || fail "source and target Job identities must differ"
evidence=$(query_evidence)
printf '%s\n' "$evidence" | jq -e '
  .source_ready == true and .equivalent_requests == true and .target_reused == true
  and .production_gate_evidence == false
' >/dev/null || fail "database authority did not prove exact-cache reuse"
printf '%s\n' "$evidence" | jq --arg seed "$seed" \
	'. + {status: "LAB CACHE VERIFIED", seed: $seed, usage_cost_validation: "PENDING"}'

#!/bin/sh

set -eu

manifests=${1:-}
waves=${2:-}
output=${3:-}
apply=${4:-}
namespace=vela-lab
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}
temporary=
committed=false
preserve=false

fail() {
	if [ -n "$temporary" ] && [ -d "$temporary" ]; then
		preserve=true
		printf 'mock-endurance-vela-lab: diagnostic receipt preserved at %s\n' "$temporary" >&2
	fi
	printf 'mock-endurance-vela-lab: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	if [ "$committed" != true ] && [ "$preserve" != true ] && [ -n "$temporary" ]; then
		rm -rf "$temporary"
	fi
}
trap cleanup EXIT HUP INT TERM

[ "$apply" = --apply ] ||
	fail "usage: $0 <rendered-manifest-directory> <waves:1-25> <new-output-directory> --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root to read the RKE2 kubeconfig"
case "$waves" in
	'' | *[!0-9]*) fail "waves must be an integer in [1,25]" ;;
esac
[ "$waves" -ge 1 ] && [ "$waves" -le 25 ] || fail "waves must be an integer in [1,25]"
case "$manifests" in
	/*) ;;
	*) fail "manifest directory must be absolute" ;;
esac
case "$output" in
	/*) ;;
	*) fail "output directory must be absolute" ;;
esac
[ ! -e "$output" ] && [ ! -L "$output" ] || fail "output path already exists"
[ -x "$script_dir/smoke.sh" ] || fail "$script_dir/smoke.sh is missing or not executable"
[ -f "$manifests/60-smoke.yaml" ] || fail "60-smoke.yaml is absent"
[ -x "$kubectl_bin" ] || fail "$kubectl_bin is missing"
[ -r "$kubeconfig" ] || fail "$kubeconfig is unreadable"
command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v sha256sum >/dev/null 2>&1 || fail "sha256sum is required"

temporary=$(mktemp -d "$(dirname "$output")/.vela-lab-mock-endurance.XXXXXX")
chmod 0700 "$temporary"
: >"$temporary/job-ids.txt"
: >"$temporary/worker-nodes.txt"
export KUBECONFIG="$kubeconfig"

query_database() {
	sql=$1
	# The variables are expanded inside the PostgreSQL container.
	# shellcheck disable=SC2016
	printf '%s\n' "$sql" | "$kubectl_bin" exec --stdin --namespace "$namespace" \
		statefulset/vela-lab-postgres -- sh -ec \
		'PGPASSWORD=$POSTGRES_PASSWORD exec psql --no-psqlrc --tuples-only --no-align --field-separator="|" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"'
}

verify_global_state() {
	phase=$1
	row=$(query_database "
SELECT
  (SELECT count(*) FROM attempt_leases WHERE revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts),
  (SELECT count(*) FROM workers
     WHERE lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY');")
	printf '%s\n' "$row" >"$temporary/global-$phase.txt"
	old_ifs=$IFS
	IFS='|' read -r active_leases production_receipts ready_workers <"$temporary/global-$phase.txt"
	IFS=$old_ifs
	[ "$active_leases" = 0 ] || fail "$phase found active Attempt Leases"
	[ "$production_receipts" = 0 ] || fail "$phase found a lab-created Production Gate receipt"
	[ "$ready_workers" = 2 ] || fail "$phase did not find exactly two READY/HEALTHY Workers"
}

run_smoke() {
	index=$1
	padded=$(printf '%03d' "$index")
	"$script_dir/smoke.sh" "$manifests" --apply >"$temporary/iteration-$padded.log" 2>&1
}

verify_smoke() {
	index=$1
	padded=$(printf '%03d' "$index")
	log=$temporary/iteration-$padded.log
	receipt_file=$temporary/iteration-$padded.json
	receipt_count=$(jq -Rrc 'fromjson? | select(.status == "LAB VERIFIED")' "$log" | wc -l | tr -d ' ')
	[ "$receipt_count" -eq 1 ] || fail "iteration $padded did not emit exactly one smoke receipt"
	jq -Rrc 'fromjson? | select(.status == "LAB VERIFIED")' "$log" >"$receipt_file"
	jq -e '
      .status == "LAB VERIFIED" and
      .final_state == "SUCCEEDED" and
      .artifact_count == 2 and
      (.artifact_kinds | sort) == ["THUMBNAIL", "VIDEO"]
    ' "$receipt_file" >/dev/null || fail "iteration $padded smoke receipt is invalid"
	job_id=$(jq -r '.job_id' "$receipt_file")
	printf '%s\n' "$job_id" | grep -Eq \
		'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' ||
		fail "iteration $padded Job ID is invalid"
	if grep -Fqx "$job_id" "$temporary/job-ids.txt"; then
		fail "iteration $padded reused Job ID $job_id"
	fi
	printf '%s\n' "$job_id" >>"$temporary/job-ids.txt"

	database_file=$temporary/iteration-$padded.database.txt
	query_database "
SELECT
  job.state,
  (SELECT count(*) FROM visible_completions WHERE job_id = job.id),
  (SELECT count(*) FROM charges
     WHERE job_id = job.id AND reason = 'VISIBLE_COMPLETION' AND state = 'POSTED'),
  (SELECT count(*) FROM artifacts WHERE job_id = job.id AND state = 'COMMITTED'),
  (SELECT count(*) FROM artifacts WHERE job_id = job.id AND kind = 'VIDEO'),
  (SELECT count(*) FROM artifacts WHERE job_id = job.id AND kind = 'THUMBNAIL'),
  (SELECT count(*) FROM artifacts
     WHERE job_id = job.id AND (
       object_version_id IS NULL OR size_bytes IS NULL OR sha256 IS NULL OR
       validation_receipt->>'validator_revision' IS DISTINCT FROM 'ffprobe-8.0.1-sandbox-v1'
     )),
  (SELECT count(*) FROM attempt_leases AS lease
     JOIN attempts AS attempt ON attempt.id = lease.attempt_id
     WHERE attempt.job_id = job.id AND lease.revoked_at IS NULL),
  COALESCE((
    SELECT worker.node_identity
    FROM visible_completions AS completion
    JOIN attempts AS attempt ON attempt.id = completion.attempt_id
    JOIN workers AS worker ON worker.id = attempt.worker_id
    WHERE completion.job_id = job.id
  ), '')
FROM jobs AS job
WHERE job.id = '$job_id'::uuid;" >"$database_file"
	[ "$(wc -l <"$database_file" | tr -d ' ')" -eq 1 ] ||
		fail "iteration $padded database receipt is missing or ambiguous"
	old_ifs=$IFS
	IFS='|' read -r state completions charges artifacts videos thumbnails invalid_artifacts active_leases worker <"$database_file"
	IFS=$old_ifs
	[ "$state" = SUCCEEDED ] || fail "iteration $padded Job is not SUCCEEDED"
	[ "$completions" = 1 ] || fail "iteration $padded Visible Completion count is not one"
	[ "$charges" = 1 ] || fail "iteration $padded Charge count is not one"
	[ "$artifacts" = 2 ] && [ "$videos" = 1 ] && [ "$thumbnails" = 1 ] ||
		fail "iteration $padded committed Artifact inventory is invalid"
	[ "$invalid_artifacts" = 0 ] || fail "iteration $padded Artifact metadata is invalid"
	[ "$active_leases" = 0 ] || fail "iteration $padded retained an active Lease"
	case "$worker" in
		vela-lab-worker-1 | vela-lab-worker-2) ;;
		*) fail "iteration $padded completed on unexpected Worker $worker" ;;
	esac
	printf '%s\n' "$worker" >>"$temporary/worker-nodes.txt"
}

verify_global_state before
started_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
total=$((waves * 2))
wave=1
while [ "$wave" -le "$waves" ]; do
	first=$((wave * 2 - 1))
	second=$((wave * 2))
	run_smoke "$first" &
	first_pid=$!
	run_smoke "$second" &
	second_pid=$!
	first_rc=0
	second_rc=0
	wait "$first_pid" || first_rc=$?
	wait "$second_pid" || second_rc=$?
	if [ "$first_rc" -ne 0 ] || [ "$second_rc" -ne 0 ]; then
		fail "wave $wave smoke failed with results $first_rc/$second_rc"
	fi
	verify_smoke "$first"
	verify_smoke "$second"
	wave=$((wave + 1))
done
verify_global_state after

[ "$(wc -l <"$temporary/job-ids.txt" | tr -d ' ')" -eq "$total" ] ||
	fail "Job receipt count does not match the requested workload"
sort -u "$temporary/worker-nodes.txt" >"$temporary/worker-nodes.unique.txt"
[ "$(wc -l <"$temporary/worker-nodes.unique.txt" | tr -d ' ')" -eq 2 ] ||
	fail "the concurrent workload did not complete on both Workers"
completed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
workers_json=$(jq -Rsc 'split("\n") | map(select(length > 0))' "$temporary/worker-nodes.unique.txt")
harness_sha256=$(sha256sum "$0" | awk '{print $1}')
jq -n \
	--arg started_at "$started_at" \
	--arg completed_at "$completed_at" \
	--arg harness_sha256 "$harness_sha256" \
	--argjson waves "$waves" \
	--argjson jobs "$total" \
	--argjson workers "$workers_json" \
	'{
      schema: "vela-lab-mock-endurance-v1",
      status: "LAB VERIFIED",
      evidence_boundary: "NON_PRODUCTION_MOCK_REHEARSAL",
      production_gates: "0/9",
      started_at: $started_at,
      completed_at: $completed_at,
      harness_sha256: $harness_sha256,
      concurrent_waves: $waves,
      jobs_succeeded: $jobs,
      visible_completions: $jobs,
      charges: $jobs,
      committed_artifacts: ($jobs * 2),
      workers_observed: $workers
    }' >"$temporary/summary.json"
(
	cd "$temporary"
	# SHA256SUMS is explicitly excluded from find.
	# shellcheck disable=SC2094
	find . -maxdepth 1 -type f ! -name SHA256SUMS -print0 |
		LC_ALL=C sort -z | xargs -0 sha256sum >SHA256SUMS
	sha256sum --check --strict SHA256SUMS >/dev/null
)
mv "$temporary" "$output"
committed=true
printf 'schema=vela-lab-mock-endurance-wrapper-v1 output=%s jobs=%s result=PASS production_gates=0/9\n' \
	"$output" "$total"

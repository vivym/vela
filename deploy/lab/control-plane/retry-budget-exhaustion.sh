#!/bin/sh

set -eu

namespace=vela-lab
worker1_node=vela-lab-worker-1
worker2_node=vela-lab-worker-2
replacement_preset_revision_id=84000000-0000-0000-0000-000000000201
replacement_certification_id=84000000-0000-0000-0000-000000000202
service_class_id=84000000-0000-0000-0000-000000000009
expected_wrapper_sha256=7786408cf1219e9c2304cebc5c3d7f772a6c7455be63043d8ea2958c19543433
expected_helper_sha256=37b6b80a1fc1c7b7b56f724ce9bc7bfd29dbea384b5d538d3fe91e284bd8079e
previous_scenario_job_id=748f2624-cefc-4ebc-8331-13aeb3ad2b5e
previous_scenario_receipt_sha256=3e123d4c29ddec87e8fb1437096266edfed046cd78a37f853cdcb5489d1f4950
previous_scenario_receipt=/root/vela-lab-deploy-bc590e20/receipts/worker-control-network-partition-v4
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}
temporary=
committed=false
recovering=false
watchdog_marker=
watchdog_pid=
tool_image=
pod_sequence=0

fail() {
	printf 'retry-budget-exhaustion: %s\n' "$*" >&2
	exit 1
}

query_database() {
	sql=$1
	# PostgreSQL credentials are expanded only inside the database container.
	# shellcheck disable=SC2016
	printf '%s\n' "$sql" | "$kubectl_bin" exec --stdin --namespace "$namespace" \
		statefulset/vela-lab-postgres -- sh -ec \
		'PGPASSWORD=$POSTGRES_PASSWORD exec psql --no-psqlrc --set=ON_ERROR_STOP=1 --tuples-only --no-align --field-separator="|" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"'
}

load_tool_identity() {
	smoke_json=$($kubectl_bin create --dry-run=client -f "$manifests/60-smoke.yaml" -o json)
	tool_image=$(printf '%s\n' "$smoke_json" | jq -er '.spec.template.spec.containers | map(select(.name == "smoke")) | .[0].image')
	printf '%s\n' "$tool_image" | grep -Eq '^10\.1\.200\.17:5443/.+@sha256:[0-9a-f]{64}$' ||
		fail "Runner tool image is not pinned to the private Registry"
}

validate_previous_scenario_receipt() {
	[ -d "$previous_scenario_receipt" ] && [ ! -L "$previous_scenario_receipt" ] ||
		fail "previous fixed-scenario receipt is missing or unsafe"
	[ "$(stat -c %u:%g:%a "$previous_scenario_receipt")" = 0:0:700 ] ||
		fail "previous fixed-scenario receipt permissions are unsafe"
	for file in SHA256SUMS STATUS summary.json scenario-matrix.json; do
		[ -f "$previous_scenario_receipt/$file" ] && [ ! -L "$previous_scenario_receipt/$file" ] ||
			fail "previous fixed-scenario receipt lacks $file"
	done
	(
		cd "$previous_scenario_receipt"
		sha256sum --check --strict SHA256SUMS >/dev/null
	) || fail "previous fixed-scenario receipt manifest does not verify"
	[ "$(sha256sum "$previous_scenario_receipt/SHA256SUMS" | awk '{print $1}')" = "$previous_scenario_receipt_sha256" ] ||
		fail "previous fixed-scenario receipt digest changed"
	[ "$(cat "$previous_scenario_receipt/STATUS")" = 'status=LAB_REHEARSAL_PASS production_gates=0/9 fixed_scenarios=1/10' ] ||
		fail "previous fixed-scenario STATUS is not a lab pass"
	jq -e --arg job_id "$previous_scenario_job_id" '
	  .schema == "vela-lab-worker-control-network-partition-v1"
	  and .status == "LAB_REHEARSAL_PASS"
	  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
	  and .production_gates == "0/9"
	  and .fixed_scenarios_completed == 1
	  and .fixed_scenarios_required == 10
	  and .job_id == $job_id
	' "$previous_scenario_receipt/summary.json" >/dev/null ||
		fail "previous fixed-scenario summary does not match the pinned evidence"
	jq -e --arg job_id "$previous_scenario_job_id" '
	  .schema == "vela-lab-fault-scenario-matrix-v1"
	  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
	  and .production_gates == "0/9"
	  and ([.scenarios[] | select(.status == "LAB_REHEARSAL_PASS")] | length) == 1
	  and any(.scenarios[];
	    .id == "worker-control-network-partition"
	    and .status == "LAB_REHEARSAL_PASS"
	    and .job_id == $job_id)
	' "$previous_scenario_receipt/scenario-matrix.json" >/dev/null ||
		fail "previous fixed-scenario matrix does not match the pinned evidence"
}

runner_pod_json() {
	name=$1
	node=$2
	read_only=$3
	script=$4
	expected=${5:-}
	requested=${6:-}
	jq -n \
		--arg name "$name" \
		--arg namespace "$namespace" \
		--arg node "$node" \
		--arg image "$tool_image" \
		--arg script "$script" \
		--arg expected "$expected" \
		--arg requested "$requested" \
		--argjson read_only "$read_only" \
		'{
          apiVersion: "v1",
          kind: "Pod",
          metadata: {
            name: $name,
            namespace: $namespace,
            labels: {
              "app.kubernetes.io/name": "vela-lab-retry-budget-mode",
              "app.kubernetes.io/component": "lab-rehearsal",
              "vela.ai/environment": "non-production-lab"
            }
          },
          spec: {
            automountServiceAccountToken: false,
            nodeName: $node,
            restartPolicy: "Never",
            containers: [{
              name: "runner-control",
              image: $image,
              imagePullPolicy: "IfNotPresent",
              command: ["/bin/sh", "-ec"],
              args: [$script],
              env: [
                {name: "EXPECTED_MODE", value: $expected},
                {name: "REQUESTED_MODE", value: $requested}
              ],
              securityContext: {
                runAsUser: 0,
                runAsGroup: 0,
                allowPrivilegeEscalation: false,
                readOnlyRootFilesystem: true,
                capabilities: {drop: ["ALL"]},
                seccompProfile: {type: "RuntimeDefault"}
              },
              volumeMounts: [
                {name: "runner-root", mountPath: "/runner-root", readOnly: $read_only},
                {name: "tmp", mountPath: "/tmp"}
              ]
            }],
            volumes: [
              {name: "runner-root", hostPath: {path: "/var/lib/vela-lab/mock-runner", type: "Directory"}},
              {name: "tmp", emptyDir: {}}
            ]
          }
        }'
}

run_runner_pod() {
	node=$1
	read_only=$2
	script=$3
	expected=$4
	requested=$5
	log_file=$6
	pod_sequence=$((pod_sequence + 1))
	pod_name=vela-lab-retry-mode-$(date +%s)-$$-$pod_sequence
	runner_pod_json "$pod_name" "$node" "$read_only" "$script" "$expected" "$requested" |
		$kubectl_bin create -f - >/dev/null
	if ! $kubectl_bin wait --namespace "$namespace" "pod/$pod_name" \
		--for=jsonpath='{.status.phase}'=Succeeded --timeout=60s >/dev/null; then
		$kubectl_bin describe pod --namespace "$namespace" "$pod_name" >>"$log_file" 2>&1 || true
		$kubectl_bin logs --namespace "$namespace" "$pod_name" >>"$log_file" 2>&1 || true
		$kubectl_bin delete pod --namespace "$namespace" "$pod_name" --ignore-not-found=true --wait=true >/dev/null 2>&1 || true
		return 1
	fi
	$kubectl_bin logs --namespace "$namespace" "$pod_name" >"$log_file"
	$kubectl_bin delete pod --namespace "$namespace" "$pod_name" --wait=true >/dev/null
}

probe_runner() {
	node=$1
	label=$2
	log_file=$temporary/runner-$label-probe.log
	profile_file=$temporary/runner-profiles-$label.json
	# The Runner paths and substitutions expand only inside the short-lived Pod.
	# shellcheck disable=SC2016
	probe_script='root=/runner-root
wrapper=$root/config/mock-backend-wrapper.sh
helper=$root/admin/set-mock-runner-mode.sh
mode=$root/config/mock-mode
profiles=$root/config/profiles.json
for file in "$wrapper" "$helper" "$mode" "$profiles"; do test -f "$file" && test ! -L "$file"; done
test "$(stat -c %u:%g:%a "$wrapper")" = 0:0:555
test "$(stat -c %u:%g:%a "$helper")" = 0:0:550
test "$(stat -c %u:%g:%a "$mode")" = 0:0:444
test "$(stat -c %u:%g:%a "$profiles")" = 0:0:444
printf "mode=%s wrapper_sha256=%s helper_sha256=%s\n" "$(sed -n 1p "$mode")" "$(sha256sum "$wrapper" | awk '\''{print $1}'\'')" "$(sha256sum "$helper" | awk '\''{print $1}'\'')"
cat "$profiles"'
	run_runner_pod "$node" true "$probe_script" '' '' "$log_file" || return 1
	header=$(sed -n '1p' "$log_file")
	[ "$header" = "mode=success wrapper_sha256=$expected_wrapper_sha256 helper_sha256=$expected_helper_sha256" ] || return 1
	sed '1d' "$log_file" >"$profile_file"
	jq -e --arg replacement "$replacement_preset_revision_id" '
      type == "object"
      and .schema_version == 1
      and (.backend_revision | test("^mock-h3-backend@sha256:[0-9a-f]{64}$"))
      and (.profiles | type == "array" and length == 2)
      and ([.profiles[].generation_preset_revision_id] | index($replacement) != null)
    ' "$profile_file" >/dev/null
}

current_mock_mode() {
	node=$1
	mode_log=$temporary/mode-read-$(printf '%s' "$node" | tr -c 'A-Za-z0-9' '-').log
	# The Runner paths and substitutions expand only inside the short-lived Pod.
	# shellcheck disable=SC2016
	read_script='mode=/runner-root/config/mock-mode
test -f "$mode" && test ! -L "$mode"
test "$(stat -c %u:%g:%a "$mode")" = 0:0:444
test "$(wc -l <"$mode" | tr -d " ")" -eq 1
sed -n 1p "$mode"'
	run_runner_pod "$node" true "$read_script" '' '' "$mode_log" || return 1
	sed -n '1p' "$mode_log"
}

switch_mock_mode() {
	node=$1
	expected=$2
	requested=$3
	log_file=$4
	# Runner substitutions expand in the Pod; only the pinned wrapper digest expands here.
	# shellcheck disable=SC2016
	switch_script='mode=/runner-root/config/mock-mode
wrapper=/runner-root/config/mock-backend-wrapper.sh
test -f "$mode" && test ! -L "$mode"
test -f "$wrapper" && test ! -L "$wrapper"
test "$(stat -c %u:%g:%a "$mode")" = 0:0:444
test "$(stat -c %u:%g:%a "$wrapper")" = 0:0:555
test "$(sha256sum "$wrapper" | awk '\''{print $1}'\'')" = "'$expected_wrapper_sha256'"
current=$(sed -n 1p "$mode")
test "$current" = "$EXPECTED_MODE"
case "$REQUESTED_MODE" in success|failure) ;; *) exit 1 ;; esac
temporary=$(mktemp /runner-root/config/.mock-mode.XXXXXX)
cleanup_mode() { rm -f -- "$temporary"; }
trap cleanup_mode EXIT HUP INT TERM
printf "%s\n" "$REQUESTED_MODE" >"$temporary"
chown 0:0 "$temporary"
chmod 0444 "$temporary"
mv -f -- "$temporary" "$mode"
temporary=
trap - EXIT HUP INT TERM
test "$(sed -n 1p "$mode")" = "$REQUESTED_MODE"
printf "schema=vela-lab-retry-budget-mode-v1 before=%s after=%s production_gates=0/9\n" "$current" "$REQUESTED_MODE"'
	run_runner_pod "$node" false "$switch_script" "$expected" "$requested" "$log_file"
}

recover_modes() {
	log_file=$1
	[ "$recovering" = false ] || return 0
	recovering=true
	result=0
	for pair in "$worker1_node:worker-1" "$worker2_node:worker-2"; do
		node=${pair%%:*}
		label=${pair#*:}
		mode=$(current_mock_mode "$node" 2>>"$log_file" || true)
		case "$mode" in
			failure) switch_mock_mode "$node" failure success "$temporary/mode-$label-recovery.log" >>"$log_file" 2>&1 || result=1 ;;
			success) ;;
			*) printf 'unexpected %s mode during recovery: %s\n' "$label" "$mode" >>"$log_file"; result=1 ;;
		esac
	done
	recovering=false
	return "$result"
}

disarm_watchdog() {
	[ -z "$watchdog_marker" ] || rm -f -- "$watchdog_marker"
	if [ -n "$watchdog_pid" ]; then
		kill "$watchdog_pid" >/dev/null 2>&1 || true
		wait "$watchdog_pid" 2>/dev/null || true
		watchdog_pid=
	fi
}

cleanup() {
	result=$?
	trap - EXIT HUP INT TERM
	if [ "$committed" != true ] && [ -n "$temporary" ] && [ -d "$temporary" ]; then
		printf 'status=INCOMPLETE production_gates=0/9\n' >"$temporary/STATUS"
		if recover_modes "$temporary/cleanup.log"; then
			disarm_watchdog
		else
			printf 'retry-budget-exhaustion: immediate mode recovery incomplete; armed watchdog retained\n' >&2
		fi
		printf 'retry-budget-exhaustion: diagnostic receipt preserved at %s\n' "$temporary" >&2
	fi
	exit "$result"
}
trap cleanup EXIT HUP INT TERM

if [ "${1:-}" = --watchdog ]; then
	trap - EXIT HUP INT TERM
	manifests=${2:-}
	temporary=${3:-}
	watchdog_marker=${4:-}
	[ -n "$manifests" ] && [ -d "$temporary" ] && [ -f "$watchdog_marker" ] || exit 0
	export KUBECONFIG="$kubeconfig"
	load_tool_identity
	sleep 300
	[ -f "$watchdog_marker" ] || exit 0
	recover_modes "$temporary/watchdog-recovery.log"
	printf 'status=RECOVERED_BY_WATCHDOG production_gates=0/9\n' >"$temporary/WATCHDOG_STATUS"
	exit 0
fi

manifests=${1:-}
output=${2:-}
apply=${3:-}
[ "$apply" = --apply ] || fail "usage: $0 <rendered-manifest-directory> <new-output-directory> --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root to read the RKE2 kubeconfig"
[ "$(hostname)" = marslab-server ] || fail "run only on the lab control host"
case "$manifests" in /*) ;; *) fail "manifest directory must be absolute" ;; esac
case "$output" in /*) ;; *) fail "output directory must be absolute" ;; esac
[ ! -e "$output" ] && [ ! -L "$output" ] || fail "output path already exists"
[ -f "$manifests/60-smoke.yaml" ] || fail "60-smoke.yaml is absent"
[ -x "$kubectl_bin" ] || fail "$kubectl_bin is missing"
[ -r "$kubeconfig" ] || fail "$kubeconfig is unreadable"
for command in jq sha256sum; do command -v "$command" >/dev/null 2>&1 || fail "$command is required"; done
export KUBECONFIG="$kubeconfig"
temporary=$(mktemp -d "$(dirname "$output")/.vela-lab-retry-budget-exhaustion.XXXXXX")
chmod 0700 "$temporary"
load_tool_identity
validate_previous_scenario_receipt

$kubectl_bin rollout status deployment/vela-lab-control --namespace "$namespace" --timeout=60s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-worker-agent-1 --namespace "$namespace" --timeout=60s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-worker-agent-2 --namespace "$namespace" --timeout=60s >/dev/null
[ "$($kubectl_bin get pods --namespace "$namespace" -l app.kubernetes.io/name=vela-lab-retry-budget-mode --no-headers 2>/dev/null | wc -l | tr -d ' ')" -eq 0 ] ||
	fail "a retry-budget Runner-control Pod already exists"

global_before=$(query_database "
SELECT
  (SELECT count(*) FROM attempt_leases WHERE revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts),
  (SELECT count(*) FROM jobs WHERE state IN ('QUEUED','ASSIGNED','RUNNING','FINALIZING','RETRY_WAIT','CANCELING')),
  (SELECT count(*) FROM workers WHERE lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY'),
  (SELECT count(*) FROM service_class_revisions WHERE id = '$service_class_id'::uuid AND state = 'ACTIVE'
     AND max_attempts = 2 AND retryable_failure_classes @> ARRAY['TRANSIENT_BACKEND']::text[]),
  (SELECT count(*) FROM generation_preset_revisions WHERE id = '$replacement_preset_revision_id'::uuid
     AND state = 'ACTIVE' AND certified_p95_compute_seconds = 120),
  (SELECT count(*) FROM profile_certifications WHERE id = '$replacement_certification_id'::uuid
     AND state = 'ACTIVE' AND invalidated_at IS NULL),
  (SELECT require_circuit_aggregation::int || '|' || protocol_version FROM profile_circuit_protocol_state WHERE singleton);")
printf '%s\n' "$global_before" >"$temporary/global-before.txt"
[ "$global_before" = '0|0|0|2|1|1|1|0|1' ] ||
	fail "global preflight does not match the bounded retry rehearsal contract"
probe_runner "$worker1_node" worker-1 || fail "Worker 1 Runner is not failure-ready"
probe_runner "$worker2_node" worker-2 || fail "Worker 2 Runner is not failure-ready"

watchdog_marker=$temporary/WATCHDOG_ARMED
printf 'armed_at=%s timeout_seconds=300\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$watchdog_marker"
nohup "$0" --watchdog "$manifests" "$temporary" "$watchdog_marker" \
	>"$temporary/watchdog.log" 2>&1 &
watchdog_pid=$!
printf '%s\n' "$watchdog_pid" >"$temporary/watchdog.pid"

switch_mock_mode "$worker1_node" success failure "$temporary/mode-worker-1-success-to-failure.log" ||
	fail "could not switch Worker 1 to failure mode"
switch_mock_mode "$worker2_node" success failure "$temporary/mode-worker-2-success-to-failure.log" ||
	fail "could not switch Worker 2 to failure mode"

database_started_at=$(query_database 'SELECT clock_timestamp();')
printf '%s\n' "$database_started_at" >"$temporary/database-started-at.txt"
started_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
job_resource=$($kubectl_bin create -f "$manifests/60-smoke.yaml" -o name)
case "$job_resource" in job.batch/vela-lab-smoke-*) ;; *) fail "unexpected smoke Job identity $job_resource" ;; esac
printf '%s\n' "$job_resource" >"$temporary/kubernetes-job.txt"

job_id=
iteration=0
while [ "$iteration" -lt 30 ]; do
	rows=$(query_database "SELECT id FROM jobs WHERE created_at >= '$database_started_at'::timestamptz ORDER BY created_at;")
	count=$(printf '%s\n' "$rows" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')
	if [ "$count" -eq 1 ]; then job_id=$rows; break; fi
	[ "$count" -eq 0 ] || fail "more than one application Job appeared during the rehearsal"
	iteration=$((iteration + 1))
	sleep 1
done
printf '%s\n' "$job_id" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' ||
	fail "application Job ID was not observed"
printf '%s\n' "$job_id" >"$temporary/job-id.txt"

terminal=
iteration=0
while [ "$iteration" -lt 90 ]; do
	terminal=$(query_database "
SELECT
  job.state,
  retry.attempts_started,
  (SELECT count(*) FROM attempts WHERE job_id = job.id),
  (SELECT count(*) FROM attempts WHERE job_id = job.id AND state = 'FAILED'),
  (SELECT count(DISTINCT worker_id) FROM attempts WHERE job_id = job.id),
  (SELECT count(*) FROM execution_failure_decisions WHERE job_id = job.id AND disposition = 'RETRY_WAIT'),
  (SELECT count(*) FROM execution_failure_decisions WHERE job_id = job.id AND disposition = 'FAILED'),
  (SELECT state FROM credit_reservations WHERE job_id = job.id),
  (SELECT count(*) FROM visible_completions WHERE job_id = job.id),
  (SELECT count(*) FROM charges WHERE job_id = job.id),
  (SELECT count(*) FROM artifacts WHERE job_id = job.id),
  (SELECT count(*) FROM artifacts WHERE job_id = job.id AND state = 'COMMITTED'),
  (SELECT count(*) FROM attempt_leases AS lease JOIN attempts AS attempt ON attempt.id = lease.attempt_id
     WHERE attempt.job_id = job.id AND lease.revoked_at IS NULL),
  (SELECT count(*) FROM workers WHERE lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY'),
  (SELECT count(*) FROM production_gate_receipts)
FROM jobs AS job JOIN retry_runtime_states AS retry ON retry.job_id = job.id
WHERE job.id = '$job_id'::uuid;")
	printf '%s\n' "$terminal" >>"$temporary/authority-timeline.txt"
	case "$terminal" in
		'FAILED|2|2|2|2|1|1|RELEASED|0|0|0|0|0|2|0') break ;;
		FAILED'|'*) fail "application Job failed with an unexpected retry authority shape" ;;
		SUCCEEDED'|'*|CANCELED'|'*) fail "application Job reached unexpected terminal state" ;;
	esac
	iteration=$((iteration + 1))
	sleep 1
done
[ "$terminal" = 'FAILED|2|2|2|2|1|1|RELEASED|0|0|0|0|0|2|0' ] ||
	fail "retry budget did not converge to the exact terminal shape"

switch_mock_mode "$worker1_node" failure success "$temporary/mode-worker-1-failure-to-success.log" ||
	fail "could not restore Worker 1 success mode"
switch_mock_mode "$worker2_node" failure success "$temporary/mode-worker-2-failure-to-success.log" ||
	fail "could not restore Worker 2 success mode"

if ! $kubectl_bin wait --namespace "$namespace" "$job_resource" --for=condition=failed --timeout=60s >/dev/null; then
	$kubectl_bin describe --namespace "$namespace" "$job_resource" >"$temporary/smoke-job-describe.txt" 2>&1 || true
	$kubectl_bin logs --namespace "$namespace" "$job_resource" >"$temporary/smoke-job.log" 2>&1 || true
	fail "smoke wrapper did not report the expected application failure"
fi
$kubectl_bin logs --namespace "$namespace" "$job_resource" >"$temporary/smoke-job.log" 2>&1 || true
grep -F "mock Job entered terminal state FAILED" "$temporary/smoke-job.log" >/dev/null ||
	fail "smoke wrapper log does not preserve the expected terminal failure"

query_database "
SELECT jsonb_build_object(
  'job_id', job.id, 'job_state', job.state, 'current_fence', job.current_fence,
  'attempts_started', retry.attempts_started,
  'compute_seconds_consumed', retry.compute_seconds_consumed,
  'last_failure_class', retry.last_failure_class,
  'next_retry_at', retry.next_retry_at,
  'credit_reservation_state', credit.state,
  'attempts', COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'attempt_id', attempt.id, 'attempt_number', attempt.attempt_number,
    'worker_id', attempt.worker_id, 'state', attempt.state, 'fence', attempt.fence,
    'started_at', attempt.started_at, 'ended_at', attempt.ended_at,
    'lease_expires_at', lease.expires_at, 'lease_revoked_at', lease.revoked_at
  ) ORDER BY attempt.attempt_number)
  FROM attempts AS attempt LEFT JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id
  WHERE attempt.job_id = job.id), '[]'::jsonb),
  'visible_completion_count', (SELECT count(*) FROM visible_completions WHERE job_id = job.id),
  'charge_count', (SELECT count(*) FROM charges WHERE job_id = job.id),
  'artifact_count', (SELECT count(*) FROM artifacts WHERE job_id = job.id),
  'committed_artifact_count', (SELECT count(*) FROM artifacts WHERE job_id = job.id AND state = 'COMMITTED')
)::text
FROM jobs AS job
JOIN retry_runtime_states AS retry ON retry.job_id = job.id
JOIN credit_reservations AS credit ON credit.job_id = job.id
WHERE job.id = '$job_id'::uuid;" >"$temporary/authority-after.json"

query_database "
SELECT jsonb_build_object(
  'decision_id', decision.id, 'attempt_number', attempt.attempt_number,
  'attempt_id', decision.attempt_id, 'worker_id', decision.worker_id,
  'source', decision.source, 'disposition', decision.disposition,
  'attempt_state', decision.attempt_state, 'failure_class', decision.failure_class,
  'failure_fingerprint', decision.failure_fingerprint,
  'retry_recommended', decision.retry_recommended, 'worker_reusable', decision.worker_reusable,
  'attempt_compute_seconds', decision.attempt_compute_seconds,
  'total_compute_seconds', decision.total_compute_seconds,
  'next_retry_at', decision.next_retry_at, 'job_fence', decision.job_fence,
  'job_version', decision.job_version, 'decided_at', decision.decided_at
)::text
FROM execution_failure_decisions AS decision
JOIN attempts AS attempt ON attempt.id = decision.attempt_id
WHERE decision.job_id = '$job_id'::uuid
ORDER BY attempt.attempt_number;" >"$temporary/retry-decisions.jsonl"
[ "$(wc -l <"$temporary/retry-decisions.jsonl" | tr -d ' ')" -eq 2 ] || fail "RetryDecision receipt count is not two"
jq -s -e '
  length == 2
  and .[0].attempt_number == 1 and .[0].disposition == "RETRY_WAIT"
  and .[1].attempt_number == 2 and .[1].disposition == "FAILED"
  and all(.[];
    .source == "WORKER_REPORTED" and .attempt_state == "FAILED"
    and .failure_class == "TRANSIENT_BACKEND"
    and .failure_fingerprint == "mock/transient/backend"
    and .retry_recommended == true and .worker_reusable == true)
' "$temporary/retry-decisions.jsonl" >/dev/null || fail "RetryDecision receipts do not match the injected failure contract"

query_database "
SELECT jsonb_build_object(
  'event_id', event_id, 'aggregate_version', aggregate_version,
  'event_type', event_type, 'schema_version', schema_version,
  'occurred_at', occurred_at, 'published_at', published_at,
  'broker_stream', broker_stream, 'broker_sequence', broker_sequence,
  'payload_encoding', 'base64-protobuf', 'payload_base64', encode(payload, 'base64')
)::text
FROM outbox_events WHERE aggregate_id = '$job_id'::uuid
ORDER BY aggregate_version, event_type;" >"$temporary/raw-event-payloads.jsonl"
[ -s "$temporary/raw-event-payloads.jsonl" ] || fail "raw event payload receipt is empty"

postflight=$(query_database "
SELECT
  (SELECT count(*) FROM attempt_leases WHERE revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts),
  (SELECT count(*) FROM jobs WHERE state IN ('QUEUED','ASSIGNED','RUNNING','FINALIZING','RETRY_WAIT','CANCELING')),
  (SELECT count(*) FROM workers WHERE lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY'),
  (SELECT count(*) FROM profile_certifications WHERE id = '$replacement_certification_id'::uuid
     AND state = 'ACTIVE' AND invalidated_at IS NULL),
  (SELECT count(*) FROM profile_certification_circuit_openings WHERE profile_certification_id = '$replacement_certification_id'::uuid);")
printf '%s\n' "$postflight" >"$temporary/global-after.txt"
[ "$postflight" = '0|0|0|2|1|0' ] || fail "global postflight does not preserve the lab authority boundary"
[ "$(current_mock_mode "$worker1_node")" = success ] || fail "Worker 1 did not remain in success mode"
[ "$(current_mock_mode "$worker2_node")" = success ] || fail "Worker 2 did not remain in success mode"

completed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
jq -n \
	--arg job_id "$job_id" \
	--arg started_at "$started_at" \
	--arg completed_at "$completed_at" \
	--arg previous_job_id "$previous_scenario_job_id" \
	--arg previous_receipt_sha256 "$previous_scenario_receipt_sha256" \
	'{schema: "vela-lab-fault-scenario-matrix-v1", evidence_boundary: "NON_PRODUCTION_MOCK_REHEARSAL", production_gates: "0/9", scenarios: [
      {id:"process-kill", status:"NOT_RUN"},
      {id:"worker-control-network-partition", status:"LAB_REHEARSAL_PASS", job_id:$previous_job_id, receipt_sha256:$previous_receipt_sha256},
      {id:"node-reboot", status:"NOT_RUN"},
      {id:"outbox-post-commit-crash", status:"NOT_RUN"},
      {id:"publisher-pre-puback-crash", status:"NOT_RUN"},
      {id:"publisher-post-puback-pre-mark-crash", status:"NOT_RUN"},
      {id:"consumer-post-db-pre-ack-crash", status:"NOT_RUN"},
      {id:"assignment-post-commit-pre-response-crash", status:"NOT_RUN"},
      {id:"retry-budget-exhaustion", status:"LAB_REHEARSAL_PASS", job_id:$job_id, started_at:$started_at, completed_at:$completed_at},
      {id:"stale-fence-late-completion", status:"NOT_RUN"}
    ]}' >"$temporary/scenario-matrix.json"
harness_sha256=$(sha256sum "$0" | awk '{print $1}')
jq -n \
	--arg job_id "$job_id" \
	--arg started_at "$started_at" \
	--arg completed_at "$completed_at" \
	--arg harness_sha256 "$harness_sha256" \
	'{
      schema: "vela-lab-retry-budget-exhaustion-v1",
      status: "LAB_REHEARSAL_PASS",
      evidence_boundary: "NON_PRODUCTION_MOCK_REHEARSAL",
      production_gates: "0/9",
      fixed_scenarios_completed: 2,
      fixed_scenarios_required: 10,
      job_id: $job_id,
      started_at: $started_at,
      completed_at: $completed_at,
      harness_sha256: $harness_sha256,
      attempts_started: 2,
      attempt_states: ["FAILED", "FAILED"],
      retry_dispositions: ["RETRY_WAIT", "FAILED"],
      credit_reservation_state: "RELEASED",
      visible_completions: 0,
      charges: 0,
      artifact_rows: 0,
      committed_artifacts: 0,
      artifacts: ["scenario-matrix.json", "runner-profiles-worker-1.json", "runner-profiles-worker-2.json", "authority-after.json", "retry-decisions.jsonl", "raw-event-payloads.jsonl"]
    }' >"$temporary/summary.json"
printf 'status=LAB_REHEARSAL_PASS production_gates=0/9 fixed_scenarios=2/10\n' >"$temporary/STATUS"

disarm_watchdog
rm -f -- "$temporary/watchdog.pid" "$temporary/watchdog.log"
(
	cd "$temporary"
	# SHA256SUMS is explicitly excluded from find.
	# shellcheck disable=SC2094
	find . -maxdepth 1 -type f ! -name SHA256SUMS -print0 |
		LC_ALL=C sort -z | xargs -0 sha256sum >SHA256SUMS
	sha256sum --check --strict SHA256SUMS >/dev/null
)
mv "$temporary" "$output"
temporary=
committed=true
printf 'schema=vela-lab-retry-budget-exhaustion-wrapper-v1 output=%s job=%s result=LAB_REHEARSAL_PASS fixed_scenarios=2/10 production_gates=0/9\n' \
	"$output" "$job_id"

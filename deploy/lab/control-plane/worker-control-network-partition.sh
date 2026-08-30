#!/bin/sh

set -eu

namespace=vela-lab
worker1_id=84000000-0000-0000-0000-000000000101
worker2_id=84000000-0000-0000-0000-000000000102
model_revision_id=84000000-0000-0000-0000-000000000004
old_preset_revision_id=84000000-0000-0000-0000-000000000005
execution_profile_id=84000000-0000-0000-0000-000000000006
output_spec_id=84000000-0000-0000-0000-000000000007
service_class_id=84000000-0000-0000-0000-000000000009
old_certification_id=84000000-0000-0000-0000-00000000000a
old_rate_card_revision_id=84000000-0000-0000-0000-00000000000b
old_rate_card_line_id=84000000-0000-0000-0000-00000000000c
replacement_preset_revision_id=84000000-0000-0000-0000-000000000201
replacement_certification_id=84000000-0000-0000-0000-000000000202
replacement_rate_card_revision_id=84000000-0000-0000-0000-000000000203
replacement_rate_card_line_id=84000000-0000-0000-0000-000000000204
worker1_node=vela-lab-worker-1
worker2_node=vela-lab-worker-2
application_policy=application-egress
worker2_policy=worker-2-control-egress-rehearsal
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}
temporary=
output=
manifests=
committed=false
recovering=false
watchdog_pid=
watchdog_marker=
mode_sequence=0
tool_image=

fail() {
	printf 'worker-control-network-partition: %s\n' "$*" >&2
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

active_lease_count() {
	query_database 'SELECT count(*) FROM attempt_leases WHERE revoked_at IS NULL;'
}

catalog_snapshot() {
	query_database "
SELECT jsonb_build_object(
  'protocol_mode', (SELECT mode FROM catalog_evidence_protocol_state WHERE singleton),
  'production_gate_receipts', (SELECT count(*) FROM production_gate_receipts),
  'generation_presets', (SELECT jsonb_agg(jsonb_build_object(
    'id', id, 'revision', revision, 'state', state,
    'certified_p95_compute_seconds', certified_p95_compute_seconds
  ) ORDER BY revision) FROM generation_preset_revisions
    WHERE model_revision_id = '$model_revision_id'::uuid AND stable_id = 'balanced'),
  'profile_certifications', (SELECT jsonb_agg(jsonb_build_object(
    'id', id, 'generation_preset_revision_id', generation_preset_revision_id,
    'state', state, 'evidence_digest', evidence_digest
  ) ORDER BY certified_at) FROM profile_certifications
    WHERE id IN ('$old_certification_id'::uuid, '$replacement_certification_id'::uuid)),
  'rate_cards', (SELECT jsonb_agg(jsonb_build_object(
    'id', rate_card.id, 'revision', rate_card.revision, 'state', rate_card.state,
    'line_id', line.id, 'generation_preset_revision_id', line.generation_preset_revision_id,
    'unit_amount_minor', line.unit_amount_minor, 'currency', line.currency
  ) ORDER BY rate_card.revision) FROM rate_card_revisions AS rate_card
    JOIN rate_card_lines AS line ON line.rate_card_revision_id = rate_card.id
    WHERE rate_card.id IN ('$old_rate_card_revision_id'::uuid, '$replacement_rate_card_revision_id'::uuid))
)::text;"
}

prepare_replacement_catalog() {
	query_database "
BEGIN;
LOCK TABLE generation_preset_revisions, profile_certifications,
  rate_card_revisions, rate_card_lines IN ACCESS EXCLUSIVE MODE;
SELECT 1 / CASE WHEN
  (SELECT mode = 'LEGACY' FROM catalog_evidence_protocol_state WHERE singleton)
  AND (SELECT count(*) = 0 FROM production_gate_receipts)
  AND (SELECT count(*) = 0 FROM attempt_leases WHERE revoked_at IS NULL)
  AND (SELECT count(*) = 0 FROM jobs
       WHERE state IN ('QUEUED','ASSIGNED','RUNNING','FINALIZING','RETRY_WAIT','CANCELING'))
  AND (
    (
      (SELECT count(*) = 1 FROM generation_preset_revisions
       WHERE id = '$old_preset_revision_id'::uuid
         AND model_revision_id = '$model_revision_id'::uuid
         AND stable_id = 'balanced' AND revision = 1 AND state = 'ACTIVE'
         AND certified_p95_compute_seconds = 30)
      AND (SELECT count(*) = 1 FROM profile_certifications
       WHERE id = '$old_certification_id'::uuid
         AND model_revision_id = '$model_revision_id'::uuid
         AND generation_preset_revision_id = '$old_preset_revision_id'::uuid
         AND output_spec_id = '$output_spec_id'::uuid
         AND execution_profile_revision_id = '$execution_profile_id'::uuid
         AND state = 'ACTIVE' AND invalidated_at IS NULL)
      AND (SELECT count(*) = 1 FROM rate_card_revisions
       WHERE id = '$old_rate_card_revision_id'::uuid
         AND revision = 840000001 AND state = 'ACTIVE')
      AND (SELECT count(*) = 1 FROM rate_card_lines
       WHERE id = '$old_rate_card_line_id'::uuid
         AND rate_card_revision_id = '$old_rate_card_revision_id'::uuid
         AND model_revision_id = '$model_revision_id'::uuid
         AND generation_preset_revision_id = '$old_preset_revision_id'::uuid
         AND service_class_revision_id = '$service_class_id'::uuid
         AND output_spec_id = '$output_spec_id'::uuid
         AND unit_amount_minor = 1 AND currency = 'CNY')
      AND NOT EXISTS (SELECT 1 FROM generation_preset_revisions
        WHERE id = '$replacement_preset_revision_id'::uuid
           OR (model_revision_id = '$model_revision_id'::uuid AND stable_id = 'balanced' AND revision = 2))
      AND NOT EXISTS (SELECT 1 FROM profile_certifications
        WHERE id = '$replacement_certification_id'::uuid)
      AND NOT EXISTS (SELECT 1 FROM rate_card_revisions
        WHERE id = '$replacement_rate_card_revision_id'::uuid OR revision = 840000002)
      AND NOT EXISTS (SELECT 1 FROM rate_card_lines
        WHERE id = '$replacement_rate_card_line_id'::uuid)
    )
    OR (
      (SELECT count(*) = 1 FROM generation_preset_revisions
       WHERE id = '$old_preset_revision_id'::uuid AND state = 'DRAINING')
      AND (SELECT count(*) = 1 FROM profile_certifications
       WHERE id = '$old_certification_id'::uuid AND state = 'DRAINING' AND invalidated_at IS NULL)
      AND (SELECT count(*) = 1 FROM rate_card_revisions
       WHERE id = '$old_rate_card_revision_id'::uuid AND state = 'DRAINING')
      AND (SELECT count(*) = 1 FROM generation_preset_revisions
       WHERE id = '$replacement_preset_revision_id'::uuid
         AND model_revision_id = '$model_revision_id'::uuid
         AND stable_id = 'balanced' AND revision = 2 AND state = 'ACTIVE'
         AND certified_p95_compute_seconds = 120)
      AND (SELECT count(*) = 1 FROM profile_certifications
       WHERE id = '$replacement_certification_id'::uuid
         AND model_revision_id = '$model_revision_id'::uuid
         AND generation_preset_revision_id = '$replacement_preset_revision_id'::uuid
         AND output_spec_id = '$output_spec_id'::uuid
         AND execution_profile_revision_id = '$execution_profile_id'::uuid
         AND state = 'ACTIVE' AND invalidated_at IS NULL
         AND evidence_digest = 'mock-only-lab-worker-control-partition-budget-v2')
      AND (SELECT count(*) = 1 FROM rate_card_revisions
       WHERE id = '$replacement_rate_card_revision_id'::uuid
         AND revision = 840000002 AND state = 'ACTIVE')
      AND (SELECT count(*) = 1 FROM rate_card_lines
       WHERE id = '$replacement_rate_card_line_id'::uuid
         AND rate_card_revision_id = '$replacement_rate_card_revision_id'::uuid
         AND model_revision_id = '$model_revision_id'::uuid
         AND generation_preset_revision_id = '$replacement_preset_revision_id'::uuid
         AND service_class_revision_id = '$service_class_id'::uuid
         AND output_spec_id = '$output_spec_id'::uuid
         AND unit_amount_minor = 1 AND currency = 'CNY')
    )
  ) THEN 1 ELSE 0 END AS catalog_precondition;

INSERT INTO generation_preset_revisions (
  id, model_revision_id, stable_id, revision, state, certified_p95_compute_seconds
)
SELECT '$replacement_preset_revision_id'::uuid, '$model_revision_id'::uuid,
  'balanced', 2, 'REGISTERED', 120
WHERE NOT EXISTS (SELECT 1 FROM generation_preset_revisions
  WHERE id = '$replacement_preset_revision_id'::uuid);
INSERT INTO profile_certifications (
  id, model_revision_id, generation_preset_revision_id, output_spec_id,
  execution_profile_revision_id, state, evidence_digest, certified_at
)
SELECT '$replacement_certification_id'::uuid, '$model_revision_id'::uuid,
  '$replacement_preset_revision_id'::uuid, '$output_spec_id'::uuid,
  '$execution_profile_id'::uuid, 'REGISTERED',
  'mock-only-lab-worker-control-partition-budget-v2', clock_timestamp()
WHERE NOT EXISTS (SELECT 1 FROM profile_certifications
  WHERE id = '$replacement_certification_id'::uuid);
INSERT INTO rate_card_revisions (id, revision, state, effective_at)
SELECT '$replacement_rate_card_revision_id'::uuid, 840000002, 'REGISTERED',
  clock_timestamp() - interval '1 second'
WHERE NOT EXISTS (SELECT 1 FROM rate_card_revisions
  WHERE id = '$replacement_rate_card_revision_id'::uuid);
INSERT INTO rate_card_lines (
  id, rate_card_revision_id, model_revision_id, generation_preset_revision_id,
  service_class_revision_id, output_spec_id, unit_amount_minor, currency
)
SELECT '$replacement_rate_card_line_id'::uuid,
  '$replacement_rate_card_revision_id'::uuid, '$model_revision_id'::uuid,
  '$replacement_preset_revision_id'::uuid, '$service_class_id'::uuid,
  '$output_spec_id'::uuid, 1, 'CNY'
WHERE NOT EXISTS (SELECT 1 FROM rate_card_lines
  WHERE id = '$replacement_rate_card_line_id'::uuid);

UPDATE generation_preset_revisions SET state = 'DRAINING'
WHERE id = '$old_preset_revision_id'::uuid AND state = 'ACTIVE';
UPDATE profile_certifications SET state = 'DRAINING'
WHERE id = '$old_certification_id'::uuid AND state = 'ACTIVE' AND invalidated_at IS NULL;
UPDATE rate_card_revisions SET state = 'DRAINING'
WHERE id = '$old_rate_card_revision_id'::uuid AND state = 'ACTIVE';
UPDATE generation_preset_revisions SET state = 'ACTIVE'
WHERE id = '$replacement_preset_revision_id'::uuid AND state = 'REGISTERED';
UPDATE profile_certifications SET state = 'ACTIVE'
WHERE id = '$replacement_certification_id'::uuid AND state = 'REGISTERED';
UPDATE rate_card_revisions SET state = 'ACTIVE'
WHERE id = '$replacement_rate_card_revision_id'::uuid AND state = 'REGISTERED';

SELECT 1 / CASE WHEN
  (SELECT count(*) = 1 FROM generation_preset_revisions
   WHERE id = '$old_preset_revision_id'::uuid AND state = 'DRAINING')
  AND (SELECT count(*) = 1 FROM profile_certifications
   WHERE id = '$old_certification_id'::uuid AND state = 'DRAINING')
  AND (SELECT count(*) = 1 FROM rate_card_revisions
   WHERE id = '$old_rate_card_revision_id'::uuid AND state = 'DRAINING')
  AND (SELECT count(*) = 1 FROM generation_preset_revisions
   WHERE id = '$replacement_preset_revision_id'::uuid AND state = 'ACTIVE'
     AND certified_p95_compute_seconds = 120)
  AND (SELECT count(*) = 1 FROM profile_certifications
   WHERE id = '$replacement_certification_id'::uuid AND state = 'ACTIVE')
  AND (SELECT count(*) = 1 FROM rate_card_revisions
   WHERE id = '$replacement_rate_card_revision_id'::uuid AND state = 'ACTIVE')
  AND (SELECT count(*) = 1 FROM rate_card_lines
   WHERE id = '$replacement_rate_card_line_id'::uuid)
  THEN 1 ELSE 0 END AS catalog_postcondition;
COMMIT;" >/dev/null
}

load_tool_identity() {
	smoke_json=$($kubectl_bin create --dry-run=client -f "$manifests/60-smoke.yaml" -o json)
	tool_image=$(printf '%s\n' "$smoke_json" | jq -er '.spec.template.spec.containers | map(select(.name == "smoke")) | .[0].image')
	printf '%s\n' "$tool_image" | grep -Eq '^10\.1\.200\.17:5443/.+@sha256:[0-9a-f]{64}$' ||
		fail "mode tool image is not pinned to the private Registry"
}

mode_pod_json() {
	pod_name=$1
	expected=$2
	requested=$3
	node=${4:-$worker1_node}
	jq -n \
		--arg name "$pod_name" \
		--arg namespace "$namespace" \
		--arg node "$node" \
		--arg image "$tool_image" \
		--arg expected "$expected" \
		--arg requested "$requested" \
		'{
		  apiVersion: "v1",
		  kind: "Pod",
		  metadata: {
		    name: $name,
		    namespace: $namespace,
		    labels: {
		      "app.kubernetes.io/name": "vela-lab-mock-mode-switch",
		      "app.kubernetes.io/component": "lab-rehearsal",
		      "vela.ai/environment": "non-production-lab"
		    }
		  },
		  spec: {
		    automountServiceAccountToken: false,
		    nodeName: $node,
		    restartPolicy: "Never",
		    containers: [{
		      name: "mode-switch",
		      image: $image,
		      imagePullPolicy: "IfNotPresent",
		      command: ["/bin/sh", "-ec"],
		      args: ["mode=/runner-config/mock-mode\nwrapper=/runner-config/mock-backend-wrapper.sh\ntest -f \"$mode\" && test ! -L \"$mode\"\ntest -f \"$wrapper\" && test ! -L \"$wrapper\"\ntest \"$(stat -c %u:%g:%a \"$mode\")\" = 0:0:444\ntest \"$(stat -c %u:%g:%a \"$wrapper\")\" = 0:0:555\ncurrent=$(sed -n 1p \"$mode\")\ntest \"$current\" = \"$EXPECTED_MODE\"\ntemporary=$(mktemp /runner-config/.mock-mode.XXXXXX)\ncleanup_mode() { rm -f -- \"$temporary\"; }\ntrap cleanup_mode EXIT HUP INT TERM\nprintf \"%s\\n\" \"$REQUESTED_MODE\" >\"$temporary\"\nchown 0:0 \"$temporary\"\nchmod 0444 \"$temporary\"\nmv -f -- \"$temporary\" \"$mode\"\ntemporary=\ntrap - EXIT HUP INT TERM\nobserved=$(sed -n 1p \"$mode\")\ntest \"$observed\" = \"$REQUESTED_MODE\"\nprintf \"schema=vela-lab-mock-runner-mode-pod-v1 before=%s after=%s production_gates=0/9\\n\" \"$current\" \"$observed\""],
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
		        {name: "runner-config", mountPath: "/runner-config"},
		        {name: "tmp", mountPath: "/tmp"}
		      ]
		    }],
		    volumes: [
		      {name: "runner-config", hostPath: {path: "/var/lib/vela-lab/mock-runner/config", type: "Directory"}},
		      {name: "tmp", emptyDir: {}}
		    ]
		  }
		}'
}

current_runner_profiles() {
	node=$1
	pod_name=vela-lab-profiles-read-$(date +%s)-$$
	mode_sequence=$((mode_sequence + 1))
	pod_name=$pod_name-$mode_sequence
	mode_pod_json "$pod_name" success success "$node" |
		jq '.metadata.labels["app.kubernetes.io/name"] = "vela-lab-mock-profile-read"
		  | .spec.containers[0].args[0] = "profiles=/runner-config/profiles.json\ntest -f \"$profiles\" && test ! -L \"$profiles\"\ntest \"$(stat -c %u:%g:%a \"$profiles\")\" = 0:0:444\ncat \"$profiles\""' |
		$kubectl_bin create -f - >/dev/null
	if ! $kubectl_bin wait --namespace "$namespace" "pod/$pod_name" --for=jsonpath='{.status.phase}'=Succeeded --timeout=60s >/dev/null; then
		$kubectl_bin delete pod --namespace "$namespace" "$pod_name" --ignore-not-found=true --wait=true >/dev/null 2>&1 || true
		return 1
	fi
	profiles=$($kubectl_bin logs --namespace "$namespace" "$pod_name")
	$kubectl_bin delete pod --namespace "$namespace" "$pod_name" --wait=true >/dev/null
	printf '%s\n' "$profiles"
}

validate_replacement_profile_allowlist() {
	profile_file=$1
	jq -e --arg old "$old_preset_revision_id" --arg replacement "$replacement_preset_revision_id" '
	  type == "object"
	  and keys == ["backend_revision","profiles","schema_version"]
	  and .schema_version == 1
	  and (.backend_revision | test("^mock-h3-backend@sha256:[0-9a-f]{64}$"))
	  and (.profiles | type == "array" and length == 2)
	  and all(.profiles[];
	    type == "object"
	    and keys == ["execution_profile_revision_id","generation_preset_revision_id","model_revision_id","output_spec_id"]
	    and .model_revision_id == "84000000-0000-0000-0000-000000000004"
	    and .execution_profile_revision_id == "84000000-0000-0000-0000-000000000006"
	    and .output_spec_id == "84000000-0000-0000-0000-000000000007")
	  and ([.profiles[].generation_preset_revision_id] | sort) == [$old,$replacement]
	' "$profile_file" >/dev/null
}

current_mock_mode() {
	pod_name=vela-lab-mode-read-$(date +%s)-$$
	mode_sequence=$((mode_sequence + 1))
	pod_name=$pod_name-$mode_sequence
	mode_pod_json "$pod_name" success success |
		jq '.spec.containers[0].args[0] = "mode=/runner-config/mock-mode\ntest -f \"$mode\" && test ! -L \"$mode\"\nsed -n 1p \"$mode\""' |
		$kubectl_bin create -f - >/dev/null
	if ! $kubectl_bin wait --namespace "$namespace" "pod/$pod_name" --for=jsonpath='{.status.phase}'=Succeeded --timeout=60s >/dev/null; then
		$kubectl_bin delete pod --namespace "$namespace" "$pod_name" --ignore-not-found=true --wait=true >/dev/null 2>&1 || true
		return 1
	fi
	mode=$($kubectl_bin logs --namespace "$namespace" "$pod_name")
	$kubectl_bin delete pod --namespace "$namespace" "$pod_name" --wait=true >/dev/null
	printf '%s\n' "$mode"
}

switch_mock_mode() {
	expected=$1
	requested=$2
	log_file=$3
	mode_sequence=$((mode_sequence + 1))
	pod_name=vela-lab-mode-$(date +%s)-$$-$mode_sequence
	mode_pod_json "$pod_name" "$expected" "$requested" >"$temporary/$pod_name.json"
	$kubectl_bin create -f "$temporary/$pod_name.json" >/dev/null
	if ! $kubectl_bin wait --namespace "$namespace" "pod/$pod_name" --for=jsonpath='{.status.phase}'=Succeeded --timeout=60s >/dev/null; then
		$kubectl_bin describe --namespace "$namespace" "pod/$pod_name" >>"$log_file" 2>&1 || true
		$kubectl_bin logs --namespace "$namespace" "$pod_name" >>"$log_file" 2>&1 || true
		$kubectl_bin delete pod --namespace "$namespace" "$pod_name" --ignore-not-found=true --wait=true >>"$log_file" 2>&1 || true
		return 1
	fi
	$kubectl_bin logs --namespace "$namespace" "$pod_name" >"$log_file"
	$kubectl_bin delete pod --namespace "$namespace" "$pod_name" --wait=true >/dev/null
}

policy_values() {
	$kubectl_bin get networkpolicy --namespace "$namespace" "$application_policy" -o json |
		jq -c '.spec.podSelector.matchExpressions[0].values'
}

apply_partition() {
	baseline='["bootstrap","control-plane","worker-agent","smoke"]'
	restricted='["bootstrap","control-plane","smoke"]'
	[ "$(policy_values)" = "$baseline" ] || return 1
	$kubectl_bin get networkpolicy --namespace "$namespace" "$application_policy" -o json |
		jq --arg name "$worker2_policy" '
		  del(.metadata.annotations, .metadata.creationTimestamp, .metadata.generation,
		      .metadata.managedFields, .metadata.resourceVersion, .metadata.uid, .status) |
		  .metadata.name = $name |
		  .metadata.labels = {
		    "app.kubernetes.io/component": "lab-rehearsal",
		    "vela.ai/environment": "non-production-lab"
		  } |
		  .spec.podSelector = {matchLabels: {"app.kubernetes.io/name": "vela-lab-worker-agent-2"}}
		' >"$temporary/worker-2-egress-policy.json"
	$kubectl_bin create -f "$temporary/worker-2-egress-policy.json" >/dev/null
	$kubectl_bin patch networkpolicy --namespace "$namespace" "$application_policy" --type=json \
		-p '[{"op":"test","path":"/spec/podSelector/matchExpressions/0/values","value":["bootstrap","control-plane","worker-agent","smoke"]},{"op":"replace","path":"/spec/podSelector/matchExpressions/0/values","value":["bootstrap","control-plane","smoke"]}]' >/dev/null
	[ "$(policy_values)" = "$restricted" ]
}

restore_policy() {
	baseline='["bootstrap","control-plane","worker-agent","smoke"]'
	restricted='["bootstrap","control-plane","smoke"]'
	current=$(policy_values 2>/dev/null || true)
	if [ "$current" = "$restricted" ]; then
		$kubectl_bin patch networkpolicy --namespace "$namespace" "$application_policy" --type=json \
			-p '[{"op":"test","path":"/spec/podSelector/matchExpressions/0/values","value":["bootstrap","control-plane","smoke"]},{"op":"replace","path":"/spec/podSelector/matchExpressions/0/values","value":["bootstrap","control-plane","worker-agent","smoke"]}]' >/dev/null || return 1
	elif [ "$current" != "$baseline" ]; then
		return 1
	fi
	$kubectl_bin delete networkpolicy --namespace "$namespace" "$worker2_policy" --ignore-not-found=true --wait=true >/dev/null
}

restore_rehearsal_worker() {
	worker_id=$1
	state=$(query_database "SELECT lifecycle_state || '|' || reachability_condition FROM workers WHERE id = '$worker_id'::uuid AND epoch = 1;")
	case "$state" in
		"READY|HEALTHY") return 0 ;;
		"DRAINING|HEALTHY"|"DRAINING|OFFLINE") ;;
		*) return 1 ;;
	esac
	reachability=$(printf '%s\n' "$state" | cut -d '|' -f 2)
	changed=$(query_database "
WITH changed AS (
  UPDATE workers AS worker
  SET lifecycle_state = 'READY', reachability_condition = 'HEALTHY',
      updated_at = clock_timestamp()
  WHERE worker.id = '$worker_id'::uuid
    AND worker.epoch = 1
    AND worker.lifecycle_state = 'DRAINING'
    AND worker.reachability_condition = '$reachability'
    AND NOT EXISTS (
      SELECT 1 FROM attempt_leases AS lease
      WHERE lease.worker_id = worker.id AND lease.revoked_at IS NULL
    )
  RETURNING worker.id
)
SELECT count(*) FROM changed;")
	[ "$changed" = 1 ]
}

recover_environment() {
	log_file=$1
	[ "$recovering" = false ] || return 0
	recovering=true
	result=0
	mode=$(current_mock_mode 2>>"$log_file" || true)
	case "$mode" in
		hang) switch_mock_mode hang success "$temporary/mode-recovery.log" >>"$log_file" 2>&1 || result=1 ;;
		success) ;;
		*) printf 'unexpected mock mode during recovery: %s\n' "$mode" >>"$log_file"; result=1 ;;
	esac
	restore_policy >>"$log_file" 2>&1 || result=1
	$kubectl_bin rollout status deployment/vela-lab-worker-agent-1 --namespace "$namespace" --timeout=90s >>"$log_file" 2>&1 || result=1
	$kubectl_bin rollout status deployment/vela-lab-worker-agent-2 --namespace "$namespace" --timeout=90s >>"$log_file" 2>&1 || result=1
	[ "$(active_lease_count)" = 0 ] >>"$log_file" 2>&1 || result=1
	restore_rehearsal_worker "$worker1_id" >>"$log_file" 2>&1 || result=1
	restore_rehearsal_worker "$worker2_id" >>"$log_file" 2>&1 || result=1
	[ "$(query_database "SELECT count(*) FROM workers WHERE id IN ('$worker1_id'::uuid, '$worker2_id'::uuid) AND epoch = 1 AND lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY';")" = 2 ] >>"$log_file" 2>&1 || result=1
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
		if recover_environment "$temporary/cleanup.log"; then
			disarm_watchdog
		else
			printf 'worker-control-network-partition: immediate recovery incomplete; armed watchdog retained\n' >&2
		fi
		printf 'worker-control-network-partition: diagnostic receipt preserved at %s\n' "$temporary" >&2
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
	sleep 480
	[ -f "$watchdog_marker" ] || exit 0
	recover_environment "$temporary/watchdog-recovery.log"
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
[ -f "$manifests/50-network-policies.yaml" ] || fail "50-network-policies.yaml is absent"
[ -f "$manifests/60-smoke.yaml" ] || fail "60-smoke.yaml is absent"
[ -x "$kubectl_bin" ] || fail "$kubectl_bin is missing"
[ -r "$kubeconfig" ] || fail "$kubeconfig is unreadable"
for command in jq sha256sum; do command -v "$command" >/dev/null 2>&1 || fail "$command is required"; done
export KUBECONFIG="$kubeconfig"
temporary=$(mktemp -d "$(dirname "$output")/.vela-lab-worker-control-partition.XXXXXX")
chmod 0700 "$temporary"
load_tool_identity

$kubectl_bin get nodes -o json >"$temporary/nodes-before.json"
$kubectl_bin get networkpolicy --namespace "$namespace" "$application_policy" -o json >"$temporary/network-policy-before.json"
[ "$(policy_values)" = '["bootstrap","control-plane","worker-agent","smoke"]' ] || fail "application egress policy is not at baseline"
if $kubectl_bin get networkpolicy --namespace "$namespace" "$worker2_policy" >/dev/null 2>&1; then
	fail "temporary Worker 2 policy already exists"
fi
[ "$($kubectl_bin get pods --namespace "$namespace" -l app.kubernetes.io/name=vela-lab-mock-mode-switch --no-headers 2>/dev/null | wc -l | tr -d ' ')" -eq 0 ] ||
	fail "a mock mode-switch Pod already exists"

global_before=$(query_database "
SELECT
  (SELECT count(*) FROM attempt_leases WHERE revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts),
  (SELECT count(*) FROM jobs WHERE state IN ('QUEUED','ASSIGNED','RUNNING','FINALIZING','RETRY_WAIT','CANCELING')),
  (SELECT count(*) FROM workers WHERE lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY');")
printf '%s\n' "$global_before" >"$temporary/global-before.txt"
[ "$global_before" = '0|0|0|2' ] || fail "global preflight is not 0 active Leases, 0 receipts, 0 active Jobs, 2 READY/HEALTHY Workers"
[ "$(current_mock_mode)" = success ] || fail "Worker 1 mock Runner is not in success mode"
current_runner_profiles "$worker1_node" >"$temporary/runner-profiles-worker-1.json"
validate_replacement_profile_allowlist "$temporary/runner-profiles-worker-1.json" ||
	fail "Worker 1 Runner profile allowlist is not replacement-ready"
current_runner_profiles "$worker2_node" >"$temporary/runner-profiles-worker-2.json"
validate_replacement_profile_allowlist "$temporary/runner-profiles-worker-2.json" ||
	fail "Worker 2 Runner profile allowlist is not replacement-ready"
catalog_snapshot >"$temporary/catalog-before.json"
prepare_replacement_catalog || fail "replacement-budget lab Catalog revision could not be prepared"
catalog_snapshot >"$temporary/catalog-after.json"

watchdog_marker=$temporary/WATCHDOG_ARMED
printf 'armed_at=%s timeout_seconds=480\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$watchdog_marker"
nohup "$0" --watchdog "$manifests" "$temporary" "$watchdog_marker" \
	>"$temporary/watchdog.log" 2>&1 &
watchdog_pid=$!
printf '%s\n' "$watchdog_pid" >"$temporary/watchdog.pid"

switch_mock_mode success hang "$temporary/mode-success-to-hang.log" || fail "could not switch Worker 1 to hang mode"
drained=$(query_database "
WITH changed AS (
  UPDATE workers AS worker
  SET lifecycle_state = 'DRAINING', updated_at = clock_timestamp()
  WHERE worker.id = '$worker2_id'::uuid
    AND worker.epoch = 1
    AND worker.lifecycle_state = 'READY'
    AND worker.reachability_condition = 'HEALTHY'
    AND NOT EXISTS (
      SELECT 1 FROM attempt_leases AS lease
      WHERE lease.worker_id = worker.id AND lease.revoked_at IS NULL
    )
  RETURNING worker.id
)
SELECT count(*) FROM changed;")
[ "$drained" = 1 ] || fail "Worker 2 could not be guarded into DRAINING"

database_started_at=$(query_database 'SELECT clock_timestamp();')
printf '%s\n' "$database_started_at" >"$temporary/database-started-at.txt"
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

original=
iteration=0
while [ "$iteration" -lt 60 ]; do
	original=$(query_database "
SELECT attempt.id, attempt.state, attempt.fence, lease.expires_at
FROM attempts AS attempt
JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id AND lease.revoked_at IS NULL
WHERE attempt.job_id = '$job_id'::uuid AND attempt.worker_id = '$worker1_id'::uuid;")
	case "$original" in *'|RUNNING|'*) break ;; esac
	job_state=$(query_database "SELECT state FROM jobs WHERE id = '$job_id'::uuid;")
	case "$job_state" in
		FAILED|CANCELED|SUCCEEDED) fail "application Job reached terminal state $job_state before Worker 1 entered RUNNING" ;;
	esac
	iteration=$((iteration + 1))
	sleep 1
done
case "$original" in *'|RUNNING|'*) ;; *) fail "Worker 1 did not enter RUNNING" ;; esac
printf '%s\n' "$original" >"$temporary/original-attempt-running.txt"
original_attempt=$(printf '%s\n' "$original" | cut -d '|' -f 1)
original_fence=$(printf '%s\n' "$original" | cut -d '|' -f 3)
query_database "
SELECT jsonb_build_object(
  'job_id', job.id, 'job_state', job.state, 'current_fence', job.current_fence,
  'attempts', COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'attempt_id', attempt.id, 'attempt_number', attempt.attempt_number,
    'worker_id', attempt.worker_id, 'state', attempt.state, 'fence', attempt.fence,
    'lease_expires_at', lease.expires_at, 'lease_revoked_at', lease.revoked_at
  ) ORDER BY attempt.attempt_number)
  FROM attempts AS attempt LEFT JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id
  WHERE attempt.job_id = job.id), '[]'::jsonb)
)::text FROM jobs AS job WHERE job.id = '$job_id'::uuid;" >"$temporary/authority-before.json"

restore_rehearsal_worker "$worker2_id"
[ "$(query_database "SELECT lifecycle_state || '|' || reachability_condition FROM workers WHERE id = '$worker2_id'::uuid;")" = 'READY|HEALTHY' ] ||
	fail "Worker 2 did not return to READY/HEALTHY"
apply_partition || fail "could not apply the bounded Worker 1 control partition"
$kubectl_bin get networkpolicy --namespace "$namespace" "$application_policy" -o json >"$temporary/network-policy-partition.json"
old_pod=$($kubectl_bin get pods --namespace "$namespace" -l app.kubernetes.io/name=vela-lab-worker-agent-1 -o jsonpath='{.items[0].metadata.name}')
old_uid=$($kubectl_bin get pod --namespace "$namespace" "$old_pod" -o jsonpath='{.metadata.uid}')
printf '%s|%s\n' "$old_pod" "$old_uid" >"$temporary/worker-1-pod-before-delete.txt"
$kubectl_bin delete pod --namespace "$namespace" "$old_pod" --wait=false >/dev/null

partition_started_at=$(query_database 'SELECT clock_timestamp();')
printf '%s\n' "$partition_started_at" >"$temporary/partition-started-at.txt"
: >"$temporary/authority-timeline.txt"
final=
iteration=0
while [ "$iteration" -lt 180 ]; do
	final=$(query_database "
SELECT
  job.state,
  (SELECT count(*) FROM attempts WHERE job_id = job.id),
  (SELECT count(*) FROM attempts WHERE job_id = job.id AND state = 'LOST'),
  (SELECT count(*) FROM attempts WHERE job_id = job.id AND state = 'SUCCEEDED'),
  (SELECT count(*) FROM visible_completions WHERE job_id = job.id),
  (SELECT count(*) FROM charges WHERE job_id = job.id AND reason = 'VISIBLE_COMPLETION' AND state = 'POSTED'),
  (SELECT count(*) FROM artifacts WHERE job_id = job.id AND state = 'COMMITTED'),
  job.current_fence,
  clock_timestamp()
FROM jobs AS job WHERE job.id = '$job_id'::uuid;")
	printf '%s\n' "$final" >>"$temporary/authority-timeline.txt"
	case "$final" in
		SUCCEEDED'|2|1|1|1|1|2|'*) break ;;
		SUCCEEDED'|'*) fail "application Job succeeded with an unexpected authority shape" ;;
		FAILED'|'*|CANCELED'|'*) fail "application Job reached a terminal failure before replacement completion" ;;
	esac
	iteration=$((iteration + 1))
	sleep 2
done
case "$final" in SUCCEEDED'|2|1|1|1|1|2|'*) ;; *) fail "replacement Attempt did not reach the exact terminal authority shape" ;; esac

switch_mock_mode hang success "$temporary/mode-hang-to-success.log" || fail "could not restore Worker 1 success mode"
restore_policy || fail "could not restore the baseline NetworkPolicy"
$kubectl_bin get networkpolicy --namespace "$namespace" "$application_policy" -o json >"$temporary/network-policy-after.json"
$kubectl_bin rollout status deployment/vela-lab-worker-agent-1 --namespace "$namespace" --timeout=90s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-worker-agent-2 --namespace "$namespace" --timeout=90s >/dev/null
[ "$(active_lease_count)" = 0 ] || fail "active Lease remained after terminal application state"
restore_rehearsal_worker "$worker1_id" || fail "Worker 1 could not be restored after partition recovery"
restore_rehearsal_worker "$worker2_id" || fail "Worker 2 could not be restored after partition recovery"

if ! $kubectl_bin wait --namespace "$namespace" "$job_resource" --for=condition=complete --timeout=60s >/dev/null; then
	$kubectl_bin describe --namespace "$namespace" "$job_resource" >"$temporary/smoke-job-describe.txt" 2>&1 || true
	$kubectl_bin logs --namespace "$namespace" "$job_resource" >"$temporary/smoke-job.log" 2>&1 || true
	fail "smoke wrapper did not complete after application success"
fi
$kubectl_bin logs --namespace "$namespace" "$job_resource" >"$temporary/smoke-job.log"
jq -Rrc 'fromjson? | select(.status == "LAB VERIFIED")' "$temporary/smoke-job.log" >"$temporary/smoke-receipt.json"
jq -e --arg job_id "$job_id" '.job_id == $job_id and .final_state == "SUCCEEDED" and .artifact_count == 2' \
	"$temporary/smoke-receipt.json" >/dev/null || fail "smoke receipt does not match the rehearsed Job"

query_database "
SELECT jsonb_build_object(
  'job_id', job.id, 'job_state', job.state, 'current_fence', job.current_fence,
  'attempts_started', retry.attempts_started,
  'attempts', COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'attempt_id', attempt.id, 'attempt_number', attempt.attempt_number,
    'worker_id', attempt.worker_id, 'state', attempt.state, 'fence', attempt.fence,
    'started_at', attempt.started_at, 'ended_at', attempt.ended_at,
    'lease_expires_at', lease.expires_at, 'lease_revoked_at', lease.revoked_at
  ) ORDER BY attempt.attempt_number)
  FROM attempts AS attempt LEFT JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id
  WHERE attempt.job_id = job.id), '[]'::jsonb),
  'visible_completion_count', (SELECT count(*) FROM visible_completions WHERE job_id = job.id),
  'posted_charge_count', (SELECT count(*) FROM charges WHERE job_id = job.id AND reason = 'VISIBLE_COMPLETION' AND state = 'POSTED'),
  'committed_artifact_count', (SELECT count(*) FROM artifacts WHERE job_id = job.id AND state = 'COMMITTED')
)::text
FROM jobs AS job JOIN retry_runtime_states AS retry ON retry.job_id = job.id
WHERE job.id = '$job_id'::uuid;" >"$temporary/authority-after.json"

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

measurements=$(query_database "
SELECT
  CASE WHEN job.state = 'SUCCEEDED' THEN 0 ELSE 1 END,
  GREATEST((SELECT count(*) FROM visible_completions WHERE job_id = job.id) - 1, 0),
  GREATEST((SELECT count(*) FROM charges WHERE job_id = job.id AND reason = 'VISIBLE_COMPLETION' AND state = 'POSTED') - 1, 0),
  (SELECT count(*) FROM visible_completions AS completion
   JOIN attempts AS attempt ON attempt.id = completion.attempt_id
   WHERE completion.job_id = job.id AND (attempt.state <> 'SUCCEEDED' OR attempt.fence <> job.current_fence)),
  (SELECT count(*) FROM attempts WHERE job_id = job.id AND id = '$original_attempt'::uuid AND state = 'LOST' AND fence = $original_fence),
  (SELECT count(*) FROM attempts WHERE job_id = job.id AND worker_id = '$worker2_id'::uuid AND state = 'SUCCEEDED' AND fence > $original_fence),
  (SELECT count(*) FROM attempt_leases AS lease JOIN attempts AS attempt ON attempt.id = lease.attempt_id WHERE attempt.job_id = job.id AND lease.revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts),
  (SELECT count(*) FROM workers WHERE lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY')
FROM jobs AS job WHERE job.id = '$job_id'::uuid;")
printf '%s\n' "$measurements" >"$temporary/measurements.txt"
[ "$measurements" = '0|0|0|0|1|1|0|0|2' ] || fail "final measurements do not satisfy the lab rehearsal contract"

completed_at=$(query_database 'SELECT clock_timestamp();')
jq -n \
	--arg job_id "$job_id" \
	--arg started_at "$partition_started_at" \
	--arg completed_at "$completed_at" \
	'{schema: "vela-lab-fault-scenario-matrix-v1", evidence_boundary: "NON_PRODUCTION_MOCK_REHEARSAL", production_gates: "0/9", scenarios: [
	  {id:"process-kill", status:"NOT_RUN"},
	  {id:"worker-control-network-partition", status:"LAB_REHEARSAL_PASS", job_id:$job_id, started_at:$started_at, completed_at:$completed_at},
	  {id:"node-reboot", status:"NOT_RUN"},
	  {id:"outbox-post-commit-crash", status:"NOT_RUN"},
	  {id:"publisher-pre-puback-crash", status:"NOT_RUN"},
	  {id:"publisher-post-puback-pre-mark-crash", status:"NOT_RUN"},
	  {id:"consumer-post-db-pre-ack-crash", status:"NOT_RUN"},
	  {id:"assignment-post-commit-pre-response-crash", status:"NOT_RUN"},
	  {id:"retry-budget-exhaustion", status:"NOT_RUN"},
	  {id:"stale-fence-late-completion", status:"NOT_RUN"}
	]}' >"$temporary/scenario-matrix.json"
jq -n \
	--arg job_id "$job_id" \
	--arg started_at "$partition_started_at" \
	--arg completed_at "$completed_at" \
	'{
	  schema: "vela-lab-worker-control-network-partition-v1",
	  status: "LAB_REHEARSAL_PASS",
	  evidence_boundary: "NON_PRODUCTION_MOCK_REHEARSAL",
	  production_gates: "0/9",
	  fixed_scenarios_completed: 1,
	  fixed_scenarios_required: 10,
	  job_id: $job_id,
	  started_at: $started_at,
	  completed_at: $completed_at,
	  measurements: {
	    "lost-accepted-job-count": 0,
	    "duplicate-visible-completion-count": 0,
	    "duplicate-charge-count": 0,
	    "stale-authority-acceptance-count": 0
	  },
	  artifacts: ["scenario-matrix.json", "runner-profiles-worker-1.json", "runner-profiles-worker-2.json", "catalog-before.json", "catalog-after.json", "authority-before.json", "authority-after.json", "raw-event-payloads.jsonl"]
	}' >"$temporary/summary.json"
printf 'status=LAB_REHEARSAL_PASS production_gates=0/9 fixed_scenarios=1/10\n' >"$temporary/STATUS"

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
printf 'schema=vela-lab-worker-control-network-partition-wrapper-v1 output=%s job=%s result=LAB_REHEARSAL_PASS fixed_scenarios=1/10 production_gates=0/9\n' \
	"$output" "$job_id"

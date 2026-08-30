#!/bin/sh

set -eu
umask 077

namespace=vela-lab
worker1_id=84000000-0000-0000-0000-000000000101
worker2_id=84000000-0000-0000-0000-000000000102
control_node=vela-lab-control-1
worker1_node=vela-lab-worker-1
worker2_node=vela-lab-worker-2
base_service_class_id=84000000-0000-0000-0000-000000000009
finalization_service_class_id=84000000-0000-0000-0000-000000000203
finalization_rate_line_id=84000000-0000-0000-0000-000000000205
expected_control_image=10.1.200.17:5443/vela-lab/vela-control@sha256:257fefb1207a19e7023171d2cc73773fa2d0e4e03d30f337abeda70c78bc5985
expected_runner_image=10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3
expected_tool_image=10.1.200.17:5443/vela-lab/vela-lab-bootstrap@sha256:93a5be4c1ba6a8b81e3d8367672a6963d5a5b5d1d1c04d6e050efbfd2f93aa42
expected_probe_binary_sha256=96b6a0cac0017b33b13c631095bcba1e54d4b238001aaff43d2b41474308d341
previous_scenario_receipt=/root/vela-lab-deploy-bc590e20/receipts/assignment-post-commit-pre-response-crash-v1
previous_scenario_receipt_sha256=f0c1a320b396579359921a51f0a1d9d97d6f305dc331d1bed288767818bf6b30
previous_scenario_job_id=16a9a421-4fce-4416-860e-49e3fa5e3d34
previous_scenario_harness_sha256=333cb27a399fc03da909a187ae1690b06a50292e8e9fbc317533fc6a1ef1f393
application_policy=application-egress
worker2_policy=worker-2-control-egress-rehearsal
probe_policy=stale-completion-probe-egress
probe_pod=vela-lab-stale-completion-probe
warm_probe_pod=vela-lab-stale-completion-probe-warm
partition_canary_pod=vela-lab-worker-1-control-block-canary
signal_configmap=vela-lab-stale-completion-signal
probe_loaded_status='schema=vela-lab-stale-completion-probe-v1 phase=AUTHORITY_LOADED production_gates=0/9'
visible_completion_fault_phase=visible-completion-pre-coordinator-hang
visible_completion_fault_phase_env=VELA_LAB_VISIBLE_COMPLETION_FAULT_PHASE
visible_completion_fault_worker_env=VELA_LAB_VISIBLE_COMPLETION_FAULT_WORKER_ID
visible_completion_fault_marker_arg=--lab-read-visible-completion-fault-marker
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}
probe_image=
expected_worker_agent_image=
manifests=
output=
temporary=
tool_image=
job_id=
job_resource=
probe_uid=
watchdog_marker=
watchdog_heartbeat=
watchdog_pid=
mode_sequence=0
committed=false
recovering=false

fail() {
	printf 'stale-fence-late-completion: %s\n' "$*" >&2
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

heartbeat() {
	[ -z "$watchdog_heartbeat" ] || touch "$watchdog_heartbeat"
}

load_tool_identity() {
	smoke_json=$($kubectl_bin create --dry-run=client -f "$manifests/60-smoke.yaml" -o json)
	tool_image=$(printf '%s\n' "$smoke_json" | jq -er \
		'.spec.template.spec.containers | map(select(.name == "smoke")) | .[0].image')
	[ "$tool_image" = "$expected_tool_image" ] ||
		fail "lab tool image does not match the fixed private Registry digest"
}

capture_worker_agent_identity() {
	destination=$1
	deployments=$destination.deployments
	pods=$destination.pods
	$kubectl_bin get deployments --namespace "$namespace" \
		vela-lab-worker-agent-1 vela-lab-worker-agent-2 -o json >"$deployments"
	$kubectl_bin get pods --namespace "$namespace" \
		-l app.kubernetes.io/component=worker-agent -o json >"$pods"
	jq -n \
		--arg expected_image "$expected_worker_agent_image" \
		--slurpfile deployments "$deployments" \
		--slurpfile pods "$pods" '
	  {
	    schema:"vela-lab-worker-agent-image-identity-v1",
	    expected_image:$expected_image,
	    deployments:[$deployments[0].items[] | {
	      name:.metadata.name,
	      generation:.metadata.generation,
	      observed_generation:.status.observedGeneration,
	      spec_image:(.spec.template.spec.containers[] | select(.name == "worker-agent") | .image)}],
	    pods:[$pods[0].items[] | {
	      name:.metadata.name,uid:.metadata.uid,node:.spec.nodeName,
	      deployment:.metadata.labels["app.kubernetes.io/name"],
	      spec_image:(.spec.containers[] | select(.name == "worker-agent") | .image),
	      runtime_image_id:(.status.containerStatuses[] | select(.name == "worker-agent") | .imageID),
	      ready:(.status.containerStatuses[] | select(.name == "worker-agent") | .ready),
	      started_at:(.status.containerStatuses[] | select(.name == "worker-agent") | .state.running.startedAt)}]
	  }
	' >"$destination"
	rm -f -- "$deployments" "$pods"
	digest=${expected_worker_agent_image##*@}
	jq -e \
		--arg expected_image "$expected_worker_agent_image" \
		--arg digest "$digest" '
	  .schema == "vela-lab-worker-agent-image-identity-v1"
	  and .expected_image == $expected_image
	  and ([.deployments[].name] | sort) == ["vela-lab-worker-agent-1","vela-lab-worker-agent-2"]
	  and all(.deployments[];
	    .generation == .observed_generation and .spec_image == $expected_image)
	  and ([.pods[].deployment] | sort) == ["vela-lab-worker-agent-1","vela-lab-worker-agent-2"]
	  and all(.pods[];
	    .spec_image == $expected_image and .ready == true and .started_at != null
	    and (.runtime_image_id == $expected_image
	      or .runtime_image_id == $digest
	      or (.runtime_image_id | endswith("@" + $digest))))
	  and any(.pods[]; .deployment == "vela-lab-worker-agent-1" and .node == "vela-lab-worker-1")
	  and any(.pods[]; .deployment == "vela-lab-worker-agent-2" and .node == "vela-lab-worker-2")
	' "$destination" >/dev/null || return 1
}

capture_rendered_worker_agent_identity() {
	destination=$1
	rendered=$destination.rendered
	$kubectl_bin create --dry-run=client -f "$manifests/40-workers.yaml" -o json >"$rendered" ||
		return 1
	jq -s --arg expected_image "$expected_worker_agent_image" '
			  {
		    schema:"vela-lab-rendered-worker-agent-image-identity-v1",
		    expected_image:$expected_image,
		    deployments:[.[] | select(.kind == "Deployment") | {
		      name:.metadata.name,
		      spec_image:(.spec.template.spec.containers[] | select(.name == "worker-agent") | .image)}]
		  }
			' "$rendered" >"$destination" || return 1
	rm -f -- "$rendered"
	jq -e --arg expected_image "$expected_worker_agent_image" '
	  .schema == "vela-lab-rendered-worker-agent-image-identity-v1"
	  and .expected_image == $expected_image
	  and ([.deployments[].name] | sort) == ["vela-lab-worker-agent-1","vela-lab-worker-agent-2"]
	  and all(.deployments[]; .spec_image == $expected_image)
		' "$destination" >/dev/null || return 1
}

capture_probe_pod_identity() {
	name=$1
	destination=$2
	expected_phase=$3
	$kubectl_bin get pod --namespace "$namespace" "$name" -o json >"$destination" || return 1
	digest=${probe_image##*@}
	jq -e \
		--arg name "$name" \
		--arg node "$worker1_node" \
		--arg image "$probe_image" \
		--arg digest "$digest" \
		--arg phase "$expected_phase" '
	  .metadata.name == $name
	  and .metadata.namespace == "vela-lab"
	  and .spec.nodeName == $node
	  and .status.phase == $phase
	  and (.spec.containers | map(select(.name == "probe" and .image == $image)) | length) == 1
	  and ((.status.containerStatuses // []) | map(select(
	    .name == "probe"
	    and (.imageID == $image or .imageID == $digest or (.imageID | endswith("@" + $digest)))
	    and (if $phase == "Succeeded" then .state.terminated.exitCode == 0 else true end)
	  )) | length) == 1
	' "$destination" >/dev/null
}

validate_previous_scenario_receipt() {
	[ -d "$previous_scenario_receipt" ] && [ ! -L "$previous_scenario_receipt" ] ||
		fail "previous fixed-scenario receipt is missing or unsafe"
	[ "$(stat -c %U:%G:%a "$previous_scenario_receipt")" = root:root:700 ] ||
		fail "previous fixed-scenario receipt permissions are unsafe"
	for file in SHA256SUMS STATUS summary.json scenario-matrix.json; do
		[ -f "$previous_scenario_receipt/$file" ] && [ ! -L "$previous_scenario_receipt/$file" ] ||
			fail "previous fixed-scenario receipt lacks $file"
	done
	(
		cd "$previous_scenario_receipt"
		sha256sum --check --strict SHA256SUMS >/dev/null
	) || fail "previous fixed-scenario receipt manifest does not verify"
	[ "$(sha256sum "$previous_scenario_receipt/SHA256SUMS" | awk '{print $1}')" = \
		"$previous_scenario_receipt_sha256" ] || fail "previous fixed-scenario receipt digest changed"
	[ "$(cat "$previous_scenario_receipt/STATUS")" = \
		'status=LAB_REHEARSAL_PASS production_gates=0/9 fixed_scenarios=9/10' ] ||
		fail "previous fixed-scenario STATUS is not a nine-scenario lab pass"
	jq -e \
		--arg job_id "$previous_scenario_job_id" \
		--arg harness "$previous_scenario_harness_sha256" '
	  .schema == "vela-lab-assignment-post-commit-pre-response-crash-v1"
	  and .status == "LAB_REHEARSAL_PASS"
	  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
	  and .production_gates == "0/9"
	  and .fixed_scenarios_completed == 9
	  and .fixed_scenarios_required == 10
	  and .job_id == $job_id
	  and .harness_sha256 == $harness
	  and .visible_completions == 1
	  and .posted_charges == 1
	  and .artifact_rows == 2
	  and .committed_artifacts == 2
	' "$previous_scenario_receipt/summary.json" >/dev/null ||
		fail "previous fixed-scenario summary does not match the pinned evidence"
	jq -e --arg job_id "$previous_scenario_job_id" '
	  .schema == "vela-lab-fault-scenario-matrix-v1"
	  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
	  and .production_gates == "0/9"
	  and (.scenarios | type == "array" and length == 10)
	  and ([.scenarios[] | select(.status == "LAB_REHEARSAL_PASS")] | length) == 9
	  and any(.scenarios[];
	    .id == "assignment-post-commit-pre-response-crash"
	    and .status == "LAB_REHEARSAL_PASS" and .job_id == $job_id)
	  and any(.scenarios[];
	    .id == "stale-fence-late-completion" and .status == "NOT_RUN")
	' "$previous_scenario_receipt/scenario-matrix.json" >/dev/null ||
		fail "previous fixed-scenario matrix does not match the pinned evidence"
}

finalization_retry_policy_state() {
	query_database "
SELECT
  (SELECT state FROM service_class_revisions WHERE id='$base_service_class_id'::uuid),
  COALESCE((SELECT state::text FROM service_class_revisions WHERE id='$finalization_service_class_id'::uuid),'ABSENT'),
  (SELECT mode FROM catalog_evidence_protocol_state WHERE singleton),
  (SELECT count(*) FROM service_class_revisions
   WHERE id='$finalization_service_class_id'::uuid AND stable_id='standard' AND revision=2
     AND queue_retry_allowance_seconds=600 AND max_attempts=2
     AND max_total_compute_multiplier_milli=2000 AND max_finalization_seconds_per_attempt=60
     AND retry_backoff_policy='{\"kind\":\"exponential\",\"initial_seconds\":2,\"max_seconds\":10}'::jsonb
     AND retryable_failure_classes=ARRAY['WORKER_LOST','TRANSIENT_BACKEND','FINALIZATION_TIMEOUT']::text[]
     AND circuit_breaker_policy='{\"policy_revision\":\"mock-lab-stale-finalization-v1\"}'::jsonb),
  (SELECT count(*) FROM rate_card_lines
   WHERE id='$finalization_rate_line_id'::uuid),
  (SELECT count(*) FROM rate_card_lines line
   JOIN rate_card_revisions revision ON revision.id=line.rate_card_revision_id
   WHERE line.id='$finalization_rate_line_id'::uuid
     AND line.service_class_revision_id='$finalization_service_class_id'::uuid
     AND revision.state='ACTIVE' AND line.unit_amount_minor=1 AND line.currency='CNY');"
}

active_base_rate_line_count() {
	query_database "
SELECT count(*)
FROM rate_card_lines line
JOIN rate_card_revisions revision ON revision.id=line.rate_card_revision_id
WHERE line.service_class_revision_id='$base_service_class_id'::uuid
  AND revision.state='ACTIVE';"
}

prepare_finalization_retry_policy() {
	log_file=$1
	state=$(finalization_retry_policy_state 2>>"$log_file" || printf INVALID)
	case "$state" in
		'ACTIVE|ABSENT|LEGACY|0|0|0')
			[ "$(active_base_rate_line_count 2>>"$log_file" || printf INVALID)" = 1 ] || {
				printf 'ACTIVE baseline RateCard line is not unique\n' >>"$log_file"
				return 1
			}
			query_database "
BEGIN;
INSERT INTO service_class_revisions (
  id,stable_id,revision,state,queue_retry_allowance_seconds,max_attempts,
  max_total_compute_multiplier_milli,max_finalization_seconds_per_attempt,
  retry_backoff_policy,retryable_failure_classes,circuit_breaker_policy)
VALUES (
  '$finalization_service_class_id'::uuid,'standard',2,'DRAINING',600,2,2000,60,
  '{\"kind\":\"exponential\",\"initial_seconds\":2,\"max_seconds\":10}'::jsonb,
  ARRAY['WORKER_LOST','TRANSIENT_BACKEND','FINALIZATION_TIMEOUT']::text[],
  '{\"policy_revision\":\"mock-lab-stale-finalization-v1\"}'::jsonb);
INSERT INTO rate_card_lines (
  id,rate_card_revision_id,model_revision_id,generation_preset_revision_id,
  service_class_revision_id,output_spec_id,unit_amount_minor,currency)
SELECT '$finalization_rate_line_id'::uuid,line.rate_card_revision_id,line.model_revision_id,
  line.generation_preset_revision_id,'$finalization_service_class_id'::uuid,
  line.output_spec_id,line.unit_amount_minor,line.currency
FROM rate_card_lines line
JOIN rate_card_revisions revision ON revision.id=line.rate_card_revision_id
WHERE line.service_class_revision_id='$base_service_class_id'::uuid
  AND revision.state='ACTIVE';
COMMIT;" >>"$log_file" 2>&1 || return 1
			state=$(finalization_retry_policy_state 2>>"$log_file" || printf INVALID)
			;;
	esac
	case "$state" in
		'ACTIVE|DRAINING|LEGACY|1|1|1')
			query_database "
BEGIN;
UPDATE service_class_revisions SET state='DRAINING'
WHERE id='$base_service_class_id'::uuid AND state='ACTIVE';
UPDATE service_class_revisions SET state='ACTIVE'
WHERE id='$finalization_service_class_id'::uuid AND state='DRAINING';
COMMIT;" >>"$log_file" 2>&1 || return 1
			;;
		'DRAINING|ACTIVE|LEGACY|1|1|1') ;;
		*) printf 'unexpected finalization retry policy state before activation: %s\n' "$state" >>"$log_file"; return 1 ;;
	esac
	[ "$(finalization_retry_policy_state 2>>"$log_file" || printf INVALID)" = \
		'DRAINING|ACTIVE|LEGACY|1|1|1' ]
}

restore_finalization_retry_policy() {
	log_file=$1
	state=$(finalization_retry_policy_state 2>>"$log_file" || printf INVALID)
	case "$state" in
		'ACTIVE|ABSENT|LEGACY|0|0|0'|'ACTIVE|DRAINING|LEGACY|1|1|1') return 0 ;;
		'DRAINING|ACTIVE|LEGACY|1|1|1') ;;
		*) printf 'unexpected finalization retry policy state during recovery: %s\n' "$state" >>"$log_file"; return 1 ;;
	esac
	query_database "
BEGIN;
UPDATE service_class_revisions SET state='DRAINING'
WHERE id='$finalization_service_class_id'::uuid AND state='ACTIVE';
UPDATE service_class_revisions SET state='ACTIVE'
WHERE id='$base_service_class_id'::uuid AND state='DRAINING';
COMMIT;" >>"$log_file" 2>&1 || return 1
	[ "$(finalization_retry_policy_state 2>>"$log_file" || printf INVALID)" = \
		'ACTIVE|DRAINING|LEGACY|1|1|1' ]
}

mode_pod_json() {
	pod_name=$1
	expected=$2
	requested=$3
	node=$4
	jq -n \
		--arg name "$pod_name" \
		--arg namespace "$namespace" \
		--arg node "$node" \
		--arg image "$tool_image" \
		--arg expected "$expected" \
		--arg requested "$requested" '
	{
	  apiVersion:"v1", kind:"Pod",
	  metadata:{name:$name,namespace:$namespace,labels:{
	    "app.kubernetes.io/name":"vela-lab-mock-mode-switch",
	    "app.kubernetes.io/component":"lab-rehearsal",
	    "vela.ai/environment":"non-production-lab"}},
	  spec:{automountServiceAccountToken:false,nodeName:$node,restartPolicy:"Never",
	    containers:[{name:"mode-switch",image:$image,imagePullPolicy:"IfNotPresent",
	      command:["/bin/sh","-ec"],
	      args:["mode=/runner-config/mock-mode\ntest -f \"$mode\" && test ! -L \"$mode\"\ntest \"$(stat -c %u:%g:%a \"$mode\")\" = 0:0:444\ncurrent=$(sed -n 1p \"$mode\")\ntest \"$current\" = \"$EXPECTED_MODE\"\nif test \"$EXPECTED_MODE\" != \"$REQUESTED_MODE\"; then\n  temporary=$(mktemp /runner-config/.mock-mode.XXXXXX)\n  cleanup_mode() { rm -f -- \"$temporary\"; }\n  trap cleanup_mode EXIT HUP INT TERM\n  printf \"%s\\n\" \"$REQUESTED_MODE\" >\"$temporary\"\n  chown 0:0 \"$temporary\"\n  chmod 0444 \"$temporary\"\n  mv -f -- \"$temporary\" \"$mode\"\n  temporary=\n  trap - EXIT HUP INT TERM\nfi\nobserved=$(sed -n 1p \"$mode\")\ntest \"$observed\" = \"$REQUESTED_MODE\"\nprintf \"schema=vela-lab-mock-runner-mode-pod-v1 before=%s after=%s production_gates=0/9\\n\" \"$current\" \"$observed\""],
	      env:[{name:"EXPECTED_MODE",value:$expected},{name:"REQUESTED_MODE",value:$requested}],
	      securityContext:{runAsUser:0,runAsGroup:0,allowPrivilegeEscalation:false,
	        readOnlyRootFilesystem:true,capabilities:{drop:["ALL"]},seccompProfile:{type:"RuntimeDefault"}},
	      volumeMounts:[{name:"runner-config",mountPath:"/runner-config"},{name:"tmp",mountPath:"/tmp"}]}],
	    volumes:[{name:"runner-config",hostPath:{path:"/var/lib/vela-lab/mock-runner/config",type:"Directory"}},
	      {name:"tmp",emptyDir:{}}]}}
	'
}

current_mock_mode() {
	node=$1
	mode_sequence=$((mode_sequence + 1))
	pod_name=vela-lab-mode-read-$(date +%s)-$$-$mode_sequence
	mode_pod_json "$pod_name" success success "$node" |
		jq '.spec.containers[0].args[0] = "mode=/runner-config/mock-mode\ntest -f \"$mode\" && test ! -L \"$mode\"\ntest \"$(stat -c %u:%g:%a \"$mode\")\" = 0:0:444\nsed -n 1p \"$mode\""' |
		$kubectl_bin create -f - >/dev/null
	if ! $kubectl_bin wait --namespace "$namespace" "pod/$pod_name" \
		--for=jsonpath='{.status.phase}'=Succeeded --timeout=60s >/dev/null; then
		$kubectl_bin delete pod --namespace "$namespace" "$pod_name" \
			--ignore-not-found=true --wait=true >/dev/null 2>&1 || true
		return 1
	fi
	mode=$($kubectl_bin logs --namespace "$namespace" "$pod_name")
	$kubectl_bin delete pod --namespace "$namespace" "$pod_name" --wait=true >/dev/null
	printf '%s\n' "$mode"
}

current_runner_image() {
	node=$1
	mode_sequence=$((mode_sequence + 1))
	pod_name=vela-lab-runner-image-read-$(date +%s)-$$-$mode_sequence
	mode_pod_json "$pod_name" success success "$node" |
		jq '.metadata.labels["app.kubernetes.io/name"] = "vela-lab-runner-image-read"
		  | .spec.containers[0].args[0] = "identity=/runner-config/container-identity\ntest -f \"$identity\" && test ! -L \"$identity\"\ntest \"$(stat -c %u:%g:%a \"$identity\")\" = 0:0:444\nsed -n \"s/^image=//p\" \"$identity\""' |
		$kubectl_bin create -f - >/dev/null
	if ! $kubectl_bin wait --namespace "$namespace" "pod/$pod_name" \
		--for=jsonpath='{.status.phase}'=Succeeded --timeout=60s >/dev/null; then
		$kubectl_bin delete pod --namespace "$namespace" "$pod_name" \
			--ignore-not-found=true --wait=true >/dev/null 2>&1 || true
		return 1
	fi
	image=$($kubectl_bin logs --namespace "$namespace" "$pod_name")
	$kubectl_bin delete pod --namespace "$namespace" "$pod_name" --wait=true >/dev/null
	printf '%s\n' "$image"
}

switch_mock_mode() {
	node=$1
	expected=$2
	requested=$3
	log_file=$4
	mode_sequence=$((mode_sequence + 1))
	pod_name=vela-lab-mode-$(date +%s)-$$-$mode_sequence
	mode_pod_json "$pod_name" "$expected" "$requested" "$node" >"$temporary/$pod_name.json"
	$kubectl_bin create -f "$temporary/$pod_name.json" >/dev/null
	if ! $kubectl_bin wait --namespace "$namespace" "pod/$pod_name" \
		--for=jsonpath='{.status.phase}'=Succeeded --timeout=60s >/dev/null; then
		$kubectl_bin describe --namespace "$namespace" "pod/$pod_name" >>"$log_file" 2>&1 || true
		$kubectl_bin logs --namespace "$namespace" "$pod_name" >>"$log_file" 2>&1 || true
		$kubectl_bin delete pod --namespace "$namespace" "$pod_name" \
			--ignore-not-found=true --wait=true >>"$log_file" 2>&1 || true
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
		  del(.metadata.annotations,.metadata.creationTimestamp,.metadata.generation,
		      .metadata.managedFields,.metadata.resourceVersion,.metadata.uid,.status) |
		  .metadata.name=$name |
		  .metadata.labels={"app.kubernetes.io/component":"lab-rehearsal","vela.ai/environment":"non-production-lab"} |
		  .spec.podSelector={matchLabels:{"app.kubernetes.io/name":"vela-lab-worker-agent-2"}}
		' >"$temporary/worker-2-egress-policy.json"
	$kubectl_bin create -f "$temporary/worker-2-egress-policy.json" >/dev/null
	jq -n --arg namespace "$namespace" --arg name "$probe_policy" '
	{
	  apiVersion:"networking.k8s.io/v1",kind:"NetworkPolicy",
	  metadata:{name:$name,namespace:$namespace,labels:{
	    "app.kubernetes.io/component":"lab-rehearsal","vela.ai/environment":"non-production-lab"}},
	  spec:{podSelector:{matchLabels:{"app.kubernetes.io/name":"vela-lab-stale-completion-probe"}},
	    policyTypes:["Egress"],egress:[{to:[{podSelector:{matchLabels:{"app.kubernetes.io/component":"control-plane"}}}],
	      ports:[{protocol:"TCP",port:8443}]}]}
	}' >"$temporary/probe-egress-policy.json"
	$kubectl_bin create -f "$temporary/probe-egress-policy.json" >/dev/null
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
	$kubectl_bin delete networkpolicy --namespace "$namespace" "$worker2_policy" "$probe_policy" \
		--ignore-not-found=true --wait=true >/dev/null
}

probe_pod_json() {
	job=$1
	attempt=$2
	worker_epoch=$3
	fence=$4
	jq -n \
		--arg namespace "$namespace" \
		--arg name "$probe_pod" \
		--arg node "$worker1_node" \
		--arg image "$probe_image" \
		--arg signal "$signal_configmap" \
		--arg job "$job" \
		--arg attempt "$attempt" \
		--arg worker "$worker1_id" \
		--arg epoch "$worker_epoch" \
		--arg fence "$fence" '
	{
	  apiVersion:"v1",kind:"Pod",
	  metadata:{name:$name,namespace:$namespace,labels:{
	    "app.kubernetes.io/name":"vela-lab-stale-completion-probe",
	    "app.kubernetes.io/component":"worker-agent",
	    "vela.ai/environment":"non-production-lab"}},
	  spec:{automountServiceAccountToken:false,nodeName:$node,restartPolicy:"Never",
	    terminationGracePeriodSeconds:5,
	    securityContext:{runAsNonRoot:true,runAsUser:10001,runAsGroup:10001,fsGroup:10001,
	      seccompProfile:{type:"RuntimeDefault"}},
	    tolerations:[{key:"vela.ai/h3",operator:"Equal",value:"true",effect:"NoSchedule"}],
	    containers:[{name:"probe",image:$image,imagePullPolicy:"IfNotPresent",
	      command:["/usr/local/bin/vela-control"],
	      args:["--lab-probe-stale-completion",$job,$attempt,$worker,$epoch,$fence],
	      resources:{requests:{cpu:"50m",memory:"64Mi"},limits:{cpu:"500m",memory:"256Mi"}},
	      securityContext:{allowPrivilegeEscalation:false,readOnlyRootFilesystem:true,
	        capabilities:{drop:["ALL"]}},
	      volumeMounts:[
	        {name:"recovery",mountPath:"/recovery",readOnly:true},
	        {name:"signal",mountPath:"/signal",readOnly:true},
	        {name:"tls",mountPath:"/tls",readOnly:true}]}],
	    volumes:[
	      {name:"recovery",hostPath:{path:"/var/lib/vela-lab/mock-runner/scratch/agent-recovery",type:"Directory"}},
	      {name:"signal",configMap:{name:$signal,optional:true,defaultMode:292}},
	      {name:"tls",secret:{secretName:"vela-lab-worker-1-tls",defaultMode:256}}]}}
	'
}

warm_probe_pod_json() {
	jq -n \
		--arg namespace "$namespace" \
		--arg name "$warm_probe_pod" \
		--arg node "$worker1_node" \
		--arg image "$probe_image" '
	{
	  apiVersion:"v1",kind:"Pod",
	  metadata:{name:$name,namespace:$namespace,labels:{
	    "app.kubernetes.io/name":$name,
	    "app.kubernetes.io/component":"control-plane",
	    "vela.ai/environment":"non-production-lab"}},
	  spec:{automountServiceAccountToken:false,nodeName:$node,restartPolicy:"Never",
	    tolerations:[{key:"vela.ai/h3",operator:"Equal",value:"true",effect:"NoSchedule"}],
	    securityContext:{runAsNonRoot:true,runAsUser:10001,runAsGroup:10001,seccompProfile:{type:"RuntimeDefault"}},
	    containers:[{name:"probe",image:$image,imagePullPolicy:"IfNotPresent",
	      command:["/usr/local/bin/vela-control"],args:["--lab-probe-stale-completion","--validate-only"],
	      resources:{requests:{cpu:"25m",memory:"32Mi"},limits:{cpu:"250m",memory:"128Mi"}},
	      securityContext:{allowPrivilegeEscalation:false,readOnlyRootFilesystem:true,capabilities:{drop:["ALL"]}}}]}}
	'
}

partition_canary_pod_json() {
	jq -n \
		--arg namespace "$namespace" \
		--arg name "$partition_canary_pod" \
		--arg node "$worker1_node" \
		--arg image "$tool_image" '
	{
	  apiVersion:"v1",kind:"Pod",
	  metadata:{name:$name,namespace:$namespace,labels:{
	    "app.kubernetes.io/name":$name,
	    "app.kubernetes.io/component":"worker-agent",
	    "vela.ai/environment":"non-production-lab"}},
	  spec:{automountServiceAccountToken:false,nodeName:$node,restartPolicy:"Never",
	    tolerations:[{key:"vela.ai/h3",operator:"Equal",value:"true",effect:"NoSchedule"}],
	    securityContext:{runAsNonRoot:true,runAsUser:10001,runAsGroup:10001,seccompProfile:{type:"RuntimeDefault"}},
	    containers:[{name:"canary",image:$image,imagePullPolicy:"IfNotPresent",
	      command:["/usr/bin/bash","-c"],
	      args:["/usr/bin/timeout 5 /usr/bin/bash -c \"exec 3<>/dev/tcp/vela-lab-control.vela-lab.svc/8443\"\ncode=$?\ncase $code in\n  124) printf \"schema=vela-lab-control-egress-canary-v1 result=CONTROL_EGRESS_BLOCKED production_gates=0/9\\n\" ;;\n  0) printf \"control path remained reachable\\n\" >&2; exit 1 ;;\n  *) printf \"control path failed without a policy timeout: %s\\n\" \"$code\" >&2; exit 1 ;;\nesac"],
	      resources:{requests:{cpu:"10m",memory:"16Mi"},limits:{cpu:"100m",memory:"64Mi"}},
	      securityContext:{allowPrivilegeEscalation:false,readOnlyRootFilesystem:true,
	        capabilities:{drop:["ALL"]}}}]}}
	'
}

delete_pod_by_uid() {
	name=$1
	uid=$2
	grace=$3
	printf '%s\n' "$uid" | grep -Eq \
		'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' || return 1
	current=$($kubectl_bin get pod --namespace "$namespace" "$name" --ignore-not-found -o json) || return 1
	[ -n "$current" ] || return 0
	[ "$(printf '%s\n' "$current" | jq -er '.metadata.uid')" = "$uid" ] || return 1
	jq -n --arg uid "$uid" --argjson grace "$grace" \
		'{apiVersion:"v1",kind:"DeleteOptions",gracePeriodSeconds:$grace,preconditions:{uid:$uid},propagationPolicy:"Background"}' |
		$kubectl_bin delete --raw="/api/v1/namespaces/$namespace/pods/$name" -f - >/dev/null || return 1
	iteration=0
	while [ "$iteration" -lt 90 ]; do
		current=$($kubectl_bin get pod --namespace "$namespace" "$name" --ignore-not-found -o json) || return 1
		[ -n "$current" ] || return 0
		[ "$(printf '%s\n' "$current" | jq -er '.metadata.uid')" = "$uid" ] || return 0
		iteration=$((iteration + 1))
		sleep 1
	done
	return 1
}

ready_control_pod() {
	destination=$1
	excluded_uid=${2:-}
	pods=$($kubectl_bin get pods --namespace "$namespace" \
		-l app.kubernetes.io/name=vela-lab-control -o json) || return 1
	printf '%s\n' "$pods" | jq -e \
		--arg image "$expected_control_image" \
		--arg node "$control_node" \
		--arg excluded_uid "$excluded_uid" '
	  [.items[] | select(.metadata.deletionTimestamp == null)] as $active
	  | select(($active | length) == 1)
	  | $active[0]
	  | select(
	      .metadata.uid != $excluded_uid
	      and .spec.nodeName == $node
	      and .status.phase == "Running"
	      and (.spec.containers | map(select(.name == "control" and .image == $image)) | length) == 1
	      and ((.status.containerStatuses // []) | map(select(
	        .name == "control"
	        and .ready == true
	        and .state.running.startedAt != null
	        and (.containerID | test("^containerd://[0-9a-f]{64}$")))) | length) == 1
	    )
	' >"$destination"
}

wait_ready_control_pod() {
	destination=$1
	excluded_uid=${2:-}
	error_file=$destination.error
	iteration=0
	while [ "$iteration" -lt 120 ]; do
		if ready_control_pod "$destination" "$excluded_uid" 2>"$error_file"; then
			rm -f -- "$error_file"
			return 0
		fi
		[ ! -s "$error_file" ] || return 1
		rm -f -- "$error_file"
		iteration=$((iteration + 1))
		sleep 1
	done
	return 1
}

control_fault_state() {
	deployment=$($kubectl_bin get deployment --namespace "$namespace" vela-lab-control -o json) ||
		return 1
	printf '%s\n' "$deployment" | jq -er \
		--arg image "$expected_control_image" \
		--arg phase_name "$visible_completion_fault_phase_env" \
		--arg phase_value "$visible_completion_fault_phase" \
		--arg worker_name "$visible_completion_fault_worker_env" \
		--arg worker_value "$worker1_id" '
	  [.spec.template.spec.containers[] | select(.name == "control")] as $controls
	  | select(($controls | length) == 1 and $controls[0].image == $image)
	  | ($controls[0].env // []) as $env
	  | [$env[] | select(.name == $phase_name)] as $phases
	  | [$env[] | select(.name == $worker_name)] as $workers
	  | if (($phases | length) == 0 and ($workers | length) == 0) then "DISABLED"
	    elif (($phases | length) == 1 and ($workers | length) == 1
	      and $phases[0].value == $phase_value and $workers[0].value == $worker_value) then "ENABLED"
	    else error("invalid lab Visible Completion fault configuration")
	    end
	'
}

enable_visible_completion_fault() {
	[ "$(control_fault_state)" = DISABLED ] || return 1
	$kubectl_bin set env deployment/vela-lab-control --namespace "$namespace" \
		"$visible_completion_fault_phase_env=$visible_completion_fault_phase" \
		"$visible_completion_fault_worker_env=$worker1_id" >/dev/null || return 1
	$kubectl_bin rollout status deployment/vela-lab-control --namespace "$namespace" \
		--timeout=120s >/dev/null || return 1
	[ "$(control_fault_state)" = ENABLED ]
}

disable_visible_completion_fault() {
	log_file=$1
	state=$(control_fault_state 2>>"$log_file" || printf INVALID)
	case "$state" in
		DISABLED) return 0 ;;
		ENABLED) ;;
		*) printf 'invalid lab Visible Completion fault state during recovery: %s\n' "$state" >>"$log_file"; return 1 ;;
	esac
	$kubectl_bin set env deployment/vela-lab-control --namespace "$namespace" \
		"$visible_completion_fault_phase_env-" "$visible_completion_fault_worker_env-" \
		>>"$log_file" 2>&1 || return 1
	$kubectl_bin rollout status deployment/vela-lab-control --namespace "$namespace" \
		--timeout=120s >>"$log_file" 2>&1 || return 1
	[ "$(control_fault_state 2>>"$log_file" || printf INVALID)" = DISABLED ]
}

disable_visible_completion_fault_after_partition() {
	before=$1
	after=$2
	receipt=$3
	wait_ready_control_pod "$before" || return 1
	name=$(jq -er '.metadata.name' "$before") || return 1
	old_uid=$(jq -er '.metadata.uid' "$before") || return 1
	[ "$(control_fault_state)" = ENABLED ] || return 1
	$kubectl_bin set env deployment/vela-lab-control --namespace "$namespace" \
		"$visible_completion_fault_phase_env-" "$visible_completion_fault_worker_env-" >/dev/null || return 1
	$kubectl_bin rollout status deployment/vela-lab-control --namespace "$namespace" \
		--timeout=120s >/dev/null || return 1
	wait_ready_control_pod "$after" "$old_uid" || return 1
	[ "$(control_fault_state)" = DISABLED ] || return 1
	jq -n \
		--arg old_uid "$old_uid" \
		--arg new_uid "$(jq -er '.metadata.uid' "$after")" \
		--arg old_image "$(jq -er '.spec.containers[] | select(.name == "control") | .image' "$before")" \
		--arg new_image "$(jq -er '.spec.containers[] | select(.name == "control") | .image' "$after")" \
		--arg node "$control_node" '
			{schema:"vela-lab-control-reconnect-v1",reason:"DISABLE_VISIBLE_COMPLETION_FAULT_AND_DRAIN_EXISTING_WORKER_GRPC_CONNECTION",
			 node:$node,old_pod_uid:$old_uid,new_pod_uid:$new_uid,
		 old_image:$old_image,new_image:$new_image,
		 identity_changed:($old_uid != $new_uid),image_unchanged:($old_image == $new_image)}
		| select(.identity_changed and .image_unchanged)' >"$receipt"
		[ -s "$receipt" ]
}

wait_visible_completion_fault_marker() {
	stale_marker_control_pod_file=$1
	stale_marker_destination=$2
	wait_ready_control_pod "$stale_marker_control_pod_file" || return 1
	stale_marker_control_pod=$(jq -er '.metadata.name' "$stale_marker_control_pod_file") || return 1
	stale_marker_control_uid=$(jq -er '.metadata.uid' "$stale_marker_control_pod_file") || return 1
	stale_marker_error_file=$stale_marker_destination.error
	stale_marker_iteration=0
	while [ "$stale_marker_iteration" -lt 420 ]; do
		heartbeat
		stale_marker_current_uid=$($kubectl_bin get pod --namespace "$namespace" \
			"$stale_marker_control_pod" -o jsonpath='{.metadata.uid}' \
			2>"$stale_marker_error_file") || return 1
		[ "$stale_marker_current_uid" = "$stale_marker_control_uid" ] || return 1
		if $kubectl_bin exec --namespace "$namespace" "$stale_marker_control_pod" -c control -- \
			/usr/local/bin/vela-control "$visible_completion_fault_marker_arg" \
			>"$stale_marker_destination" 2>"$stale_marker_error_file"; then
			rm -f -- "$stale_marker_error_file"
			jq -e --arg worker_id "$worker1_id" '
			  type == "object"
			  and keys == ["artifact_ids","attempt_id","blocked_before_service","candidate_sha256",
			    "completion_id","expected_job_version","lease_fence","phase","schema","worker_epoch","worker_id"]
			  and .schema == "vela-lab-visible-completion-fault-marker-v1"
			  and .phase == "visible-completion-pre-coordinator-hang"
			  and .worker_id == $worker_id
			  and (.attempt_id | test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"))
			  and (.completion_id | test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"))
			  and .worker_epoch > 0 and .lease_fence > 0 and .expected_job_version > 0
			  and (.artifact_ids | type) == "array"
			  and (.artifact_ids | length) == 2
			  and (.artifact_ids | unique | length) == 2
			  and all(.artifact_ids[];
			    test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"))
			  and (.candidate_sha256 | test("^[0-9a-f]{64}$"))
			  and .blocked_before_service == true
			' "$stale_marker_destination" >/dev/null || return 1
			grep -Eqi 'lease[_ -]?token|bearer|authorization' \
				"$stale_marker_destination" && return 1
			return 0
		fi
		stale_marker_iteration=$((stale_marker_iteration + 1))
		sleep 1
	done
	return 1
}

neutralize_partition_canary() {
	log_file=$1
	pod=$($kubectl_bin get pod --namespace "$namespace" "$partition_canary_pod" \
		--ignore-not-found -o json 2>>"$log_file") || return 1
	[ -n "$pod" ] || return 0
	printf '%s\n' "$pod" | jq -e --arg image "$tool_image" --arg name "$partition_canary_pod" '
	  .metadata.labels["app.kubernetes.io/name"] == $name
	  and .metadata.labels["app.kubernetes.io/component"] == "worker-agent"
	  and .metadata.labels["vela.ai/environment"] == "non-production-lab"
	  and (.spec.containers | length) == 1
	  and .spec.containers[0].name == "canary"
	  and .spec.containers[0].image == $image
	' >/dev/null || return 1
	uid=$(printf '%s\n' "$pod" | jq -er '.metadata.uid') || return 1
	delete_pod_by_uid "$partition_canary_pod" "$uid" 0 >>"$log_file" 2>&1
}

prove_partition_active() {
	partition_canary_pod_json >"$temporary/partition-canary-pod.json"
	$kubectl_bin create --dry-run=server -f "$temporary/partition-canary-pod.json" -o json \
		>"$temporary/partition-canary-server-dry-run.json" || return 1
	$kubectl_bin create -f "$temporary/partition-canary-pod.json" >/dev/null || return 1
	uid=$($kubectl_bin get pod --namespace "$namespace" "$partition_canary_pod" \
		-o jsonpath='{.metadata.uid}') || return 1
	printf '%s\n' "$uid" >"$temporary/partition-canary-uid.txt"
	if ! $kubectl_bin wait --namespace "$namespace" "pod/$partition_canary_pod" \
		--for=jsonpath='{.status.phase}'=Succeeded --timeout=30s >/dev/null; then
		$kubectl_bin describe --namespace "$namespace" "pod/$partition_canary_pod" \
			>"$temporary/partition-canary-describe.txt" 2>&1 || true
		$kubectl_bin logs --namespace "$namespace" "$partition_canary_pod" \
			>"$temporary/partition-canary.log" 2>&1 || true
		return 1
	fi
	$kubectl_bin logs --namespace "$namespace" "$partition_canary_pod" \
		>"$temporary/partition-canary.log" || return 1
	[ "$(cat "$temporary/partition-canary.log")" = \
		'schema=vela-lab-control-egress-canary-v1 result=CONTROL_EGRESS_BLOCKED production_gates=0/9' ] || return 1
	delete_pod_by_uid "$partition_canary_pod" "$uid" 0
}

neutralize_probe() {
	log_file=$1
	for probe_cleanup_name in "$probe_pod" "$warm_probe_pod"; do
		probe_cleanup_json=$($kubectl_bin get pod --namespace "$namespace" \
			"$probe_cleanup_name" --ignore-not-found -o json 2>>"$log_file") || return 1
		[ -n "$probe_cleanup_json" ] || continue
		printf '%s\n' "$probe_cleanup_json" | jq -e \
			--arg image "$probe_image" --arg name "$probe_cleanup_name" '
		  .metadata.labels["app.kubernetes.io/name"] == $name
		  and .metadata.labels["vela.ai/environment"] == "non-production-lab"
		  and (.spec.containers | length) == 1
		  and .spec.containers[0].image == $image
		' >/dev/null || return 1
		probe_cleanup_uid=$(printf '%s\n' "$probe_cleanup_json" | jq -er '.metadata.uid') || return 1
		delete_pod_by_uid "$probe_cleanup_name" "$probe_cleanup_uid" 0 \
			>>"$log_file" 2>&1 || return 1
	done
	config=$($kubectl_bin get configmap --namespace "$namespace" "$signal_configmap" \
		--ignore-not-found -o json 2>>"$log_file") || return 1
	if [ -n "$config" ]; then
		printf '%s\n' "$config" | jq -e '
		  .metadata.labels["app.kubernetes.io/name"] == "vela-lab-stale-completion-signal"
		  and .metadata.labels["vela.ai/environment"] == "non-production-lab"
		  and (.data | keys) == ["go.json"]
		' >/dev/null || return 1
		$kubectl_bin delete configmap --namespace "$namespace" "$signal_configmap" \
			--wait=true >>"$log_file" 2>&1 || return 1
	fi
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
  SET lifecycle_state='READY', reachability_condition='HEALTHY', updated_at=clock_timestamp()
  WHERE worker.id='$worker_id'::uuid AND worker.epoch=1
    AND worker.lifecycle_state='DRAINING' AND worker.reachability_condition='$reachability'
    AND NOT EXISTS (SELECT 1 FROM attempt_leases lease
      WHERE lease.worker_id=worker.id AND lease.revoked_at IS NULL)
  RETURNING worker.id
)
SELECT count(*) FROM changed;")
	[ "$changed" = 1 ]
}

make_rehearsal_worker_available() {
	worker_id=$1
	state=$(query_database "SELECT lifecycle_state || '|' || reachability_condition FROM workers WHERE id = '$worker_id'::uuid AND epoch = 1;")
	case "$state" in
		"READY|HEALTHY"|"BUSY|HEALTHY") return 0 ;;
		"DRAINING|HEALTHY"|"DRAINING|OFFLINE") restore_rehearsal_worker "$worker_id" ;;
		*) return 1 ;;
	esac
}

recover_environment() {
	log_file=$1
	[ "$recovering" = false ] || return 0
	recovering=true
	stale_recovery_result=0
	exec 9>"$temporary/recovery.lock"
	if ! flock -n 9; then
		printf 'another recovery owner holds the environment lock\n' >>"$log_file"
		recovering=false
		exec 9>&-
		return 1
	fi
	disable_visible_completion_fault "$log_file" || stale_recovery_result=1
	neutralize_partition_canary "$log_file" || stale_recovery_result=1
	neutralize_probe "$log_file" || stale_recovery_result=1
	restore_policy >>"$log_file" 2>&1 || stale_recovery_result=1
	for pair in "$worker1_node:worker-1" "$worker2_node:worker-2"; do
		node=${pair%%:*}
		label=${pair#*:}
		mode=$(current_mock_mode "$node" 2>>"$log_file" || true)
		case "$mode" in
			slow-success) switch_mock_mode "$node" slow-success success "$temporary/mode-$label-recovery.log" >>"$log_file" 2>&1 || stale_recovery_result=1 ;;
			success) ;;
			*) printf 'unexpected %s mode during recovery: %s\n' "$label" "$mode" >>"$log_file"; stale_recovery_result=1 ;;
		esac
	done
	active=$(query_database "SELECT count(*) FROM jobs WHERE state IN ('QUEUED','ASSIGNED','RUNNING','FINALIZING','RETRY_WAIT','CANCELING');" 2>>"$log_file" || printf unknown)
	if [ "$active" != 0 ]; then
		prepare_finalization_retry_policy "$log_file" || stale_recovery_result=1
		make_rehearsal_worker_available "$worker2_id" >>"$log_file" 2>&1 || stale_recovery_result=1
	fi
	$kubectl_bin rollout status deployment/vela-lab-worker-agent-1 --namespace "$namespace" \
		--timeout=120s >>"$log_file" 2>&1 || stale_recovery_result=1
	$kubectl_bin rollout status deployment/vela-lab-worker-agent-2 --namespace "$namespace" \
		--timeout=120s >>"$log_file" 2>&1 || stale_recovery_result=1
	iteration=0
	while [ "$iteration" -lt 180 ]; do
		active=$(query_database "SELECT count(*) FROM jobs WHERE state IN ('QUEUED','ASSIGNED','RUNNING','FINALIZING','RETRY_WAIT','CANCELING');" 2>>"$log_file" || printf unknown)
		[ "$active" = 0 ] && break
		iteration=$((iteration + 1))
		sleep 1
	done
	[ "$active" = 0 ] || stale_recovery_result=1
	[ "$(active_lease_count 2>>"$log_file" || printf unknown)" = 0 ] || stale_recovery_result=1
	restore_rehearsal_worker "$worker1_id" >>"$log_file" 2>&1 || stale_recovery_result=1
	restore_rehearsal_worker "$worker2_id" >>"$log_file" 2>&1 || stale_recovery_result=1
	if [ "$active" = 0 ]; then
		restore_finalization_retry_policy "$log_file" || stale_recovery_result=1
	fi
	flock -u 9
	exec 9>&-
	recovering=false
	return "$stale_recovery_result"
}

disarm_watchdog() {
	[ -z "$watchdog_marker" ] || rm -f -- "$watchdog_marker"
	[ -z "$watchdog_heartbeat" ] || rm -f -- "$watchdog_heartbeat"
	if [ -n "$watchdog_pid" ]; then
		kill "$watchdog_pid" >/dev/null 2>&1 || true
		wait "$watchdog_pid" 2>/dev/null || true
		watchdog_pid=
	fi
}

cleanup() {
	stale_cleanup_status=$?
	trap - EXIT HUP INT TERM
	if [ "$committed" != true ] && [ -n "$temporary" ] && [ -d "$temporary" ]; then
		printf 'status=INCOMPLETE production_gates=0/9\n' >"$temporary/STATUS"
		if recover_environment "$temporary/cleanup.log"; then
			disarm_watchdog
		else
			printf 'stale-fence-late-completion: immediate recovery incomplete; armed watchdog retained\n' >&2
		fi
		printf 'stale-fence-late-completion: diagnostic receipt preserved at %s\n' "$temporary" >&2
	fi
	exit "$stale_cleanup_status"
}
trap cleanup EXIT HUP INT TERM

if [ "${1:-}" = --watchdog ]; then
	trap - EXIT HUP INT TERM
	probe_image=${2:-}
	manifests=${3:-}
	temporary=${4:-}
	watchdog_marker=${5:-}
	[ -n "$probe_image" ] && [ -n "$manifests" ] && [ -d "$temporary" ] &&
		[ -f "$watchdog_marker" ] || exit 0
	export KUBECONFIG="$kubeconfig"
	watchdog_heartbeat=$temporary/WATCHDOG_HEARTBEAT
	load_tool_identity
	sleep 300
	[ -f "$watchdog_marker" ] || exit 0
	while [ -f "$watchdog_marker" ]; do
		now=$(date +%s)
		updated=$(stat -c %Y "$watchdog_heartbeat" 2>/dev/null || printf 0)
		[ $((now - updated)) -lt 240 ] || break
		sleep 60
	done
	[ -f "$watchdog_marker" ] || exit 0
	printf 'started_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$temporary/WATCHDOG_RECOVERY_STARTED"
	recover_environment "$temporary/watchdog-recovery.log"
	printf 'status=RECOVERED_BY_WATCHDOG production_gates=0/9\n' >"$temporary/WATCHDOG_STATUS"
	exit 0
fi

probe_image=${1:-}
expected_worker_agent_image=${2:-}
manifests=${3:-}
output=${4:-}
apply=${5:-}
[ "$apply" = --apply ] ||
	fail "usage: $0 <probe-image@sha256:digest> <worker-agent-image@sha256:digest> <rendered-manifest-directory> <new-output-directory> --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root to read the RKE2 kubeconfig"
[ "$(hostname)" = marslab-server ] || fail "run only on the lab control host"
printf '%s\n' "$probe_image" | grep -Eq \
	'^10\.1\.200\.17:5443/vela-lab/vela-control@sha256:[0-9a-f]{64}$' ||
	fail "probe image must use the fixed private control repository and an immutable digest"
[ "$probe_image" = "$expected_control_image" ] ||
	fail "probe image must equal the harness-pinned control digest"
printf '%s\n' "$expected_worker_agent_image" | grep -Eq \
	'^10\.1\.200\.17:5443/vela-lab/vela-worker-agent@sha256:[0-9a-f]{64}$' ||
	fail "Worker Agent image must use the fixed private repository and an immutable digest"
case "$manifests" in /*) ;; *) fail "manifest directory must be absolute" ;; esac
case "$output" in /*) ;; *) fail "output directory must be absolute" ;; esac
[ ! -e "$output" ] && [ ! -L "$output" ] || fail "output path already exists"
[ -f "$manifests/50-network-policies.yaml" ] || fail "50-network-policies.yaml is absent"
[ -f "$manifests/60-smoke.yaml" ] || fail "60-smoke.yaml is absent"
[ -f "$manifests/40-workers.yaml" ] || fail "40-workers.yaml is absent"
[ -x "$kubectl_bin" ] || fail "$kubectl_bin is missing"
[ -r "$kubeconfig" ] || fail "$kubeconfig is unreadable"
for command in flock jq sha256sum; do
	command -v "$command" >/dev/null 2>&1 || fail "$command is required"
done
export KUBECONFIG="$kubeconfig"
temporary=$(mktemp -d "$(dirname "$output")/.vela-lab-stale-completion.XXXXXX")
chmod 0700 "$temporary"
load_tool_identity
validate_previous_scenario_receipt
capture_rendered_worker_agent_identity "$temporary/worker-agent-rendered-image.json" ||
	fail "rendered Worker Agent image identity does not match the expected digest"

$kubectl_bin rollout status deployment/vela-lab-control --namespace "$namespace" --timeout=60s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-worker-agent-1 --namespace "$namespace" --timeout=60s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-worker-agent-2 --namespace "$namespace" --timeout=60s >/dev/null
capture_worker_agent_identity "$temporary/worker-agent-image-before.json" ||
	fail "live Worker Agent image identity does not match the expected digest"
for resource in "pod/$probe_pod" "pod/$warm_probe_pod" \
	"pod/$partition_canary_pod" \
	"configmap/$signal_configmap" "networkpolicy/$worker2_policy" "networkpolicy/$probe_policy"; do
	if $kubectl_bin get --namespace "$namespace" "$resource" >/dev/null 2>&1; then
		fail "temporary resource $resource already exists"
	fi
done
[ "$(policy_values)" = '["bootstrap","control-plane","worker-agent","smoke"]' ] ||
	fail "application egress policy is not at baseline"

global_before=$(query_database "
SELECT
  (SELECT count(*) FROM attempt_leases WHERE revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts),
  (SELECT count(*) FROM jobs WHERE state IN ('QUEUED','ASSIGNED','RUNNING','FINALIZING','RETRY_WAIT','CANCELING')),
  (SELECT count(*) FROM workers WHERE id IN ('$worker1_id'::uuid,'$worker2_id'::uuid)
   AND lifecycle_state='READY' AND reachability_condition='HEALTHY');")
printf '%s\n' "$global_before" >"$temporary/global-before.txt"
[ "$global_before" = '0|0|0|2' ] ||
	fail "global preflight does not preserve the idle two-Worker lab authority boundary"
[ "$(current_mock_mode "$worker1_node")" = success ] || fail "Worker 1 Runner is not in success mode"
[ "$(current_mock_mode "$worker2_node")" = success ] || fail "Worker 2 Runner is not in success mode"
[ "$(current_runner_image "$worker1_node")" = "$expected_runner_image" ] ||
	fail "Worker 1 Runner image identity is not the fixed digest"
[ "$(current_runner_image "$worker2_node")" = "$expected_runner_image" ] ||
	fail "Worker 2 Runner image identity is not the fixed digest"
[ "$(control_fault_state)" = DISABLED ] ||
	fail "lab Visible Completion fault must be disabled before rehearsal"
initial_retry_policy_state=$(finalization_retry_policy_state)
case "$initial_retry_policy_state" in
	'ACTIVE|ABSENT|LEGACY|0|0|0'|'ACTIVE|DRAINING|LEGACY|1|1|1') ;;
	*) fail "lab finalization retry policy is not at a recoverable baseline" ;;
esac
printf '%s\n' "$initial_retry_policy_state" >"$temporary/finalization-retry-policy-before.txt"

$kubectl_bin get nodes -o json >"$temporary/nodes-before.json"
jq -e '
  (.items | length) == 3
  and all(.items[]; any(.status.conditions[]; .type == "Ready" and .status == "True"))
  and any(.items[]; .metadata.name == "vela-lab-control-1"
    and (.status.allocatable["nvidia.com/gpu"] // "0") == "0")
  and ([.items[] | select(.metadata.name == "vela-lab-worker-1" or .metadata.name == "vela-lab-worker-2")
    | (.status.allocatable["nvidia.com/gpu"] // "0")] | sort) == ["8","8"]
' "$temporary/nodes-before.json" >/dev/null || fail "three-node Ready GPU boundary is absent"

warm_probe_pod_json >"$temporary/probe-warm-pod.json"
$kubectl_bin create --dry-run=server -f "$temporary/probe-warm-pod.json" -o json \
	>"$temporary/probe-warm-server-dry-run.json"
$kubectl_bin create -f "$temporary/probe-warm-pod.json" >/dev/null
warm_uid=$($kubectl_bin get pod --namespace "$namespace" "$warm_probe_pod" \
	-o jsonpath='{.metadata.uid}')
if ! $kubectl_bin wait --namespace "$namespace" "pod/$warm_probe_pod" \
	--for=jsonpath='{.status.phase}'=Succeeded --timeout=120s >/dev/null; then
	$kubectl_bin describe --namespace "$namespace" "pod/$warm_probe_pod" \
		>"$temporary/probe-warm-describe.txt" 2>&1 || true
	$kubectl_bin logs --namespace "$namespace" "pod/$warm_probe_pod" \
		>"$temporary/probe-warm.log" 2>&1 || true
	fail "probe image did not warm on Worker 1"
fi
$kubectl_bin logs --namespace "$namespace" "pod/$warm_probe_pod" \
	>"$temporary/probe-warm.log"
warm_probe_receipt=$(grep -E '^schema=vela-lab-stale-completion-probe-v1 validation=PASS binary_sha256=[0-9a-f]{64} production_gates=0/9$' \
	"$temporary/probe-warm.log" || true)
[ "$(printf '%s\n' "$warm_probe_receipt" | sed '/^$/d' | wc -l | tr -d ' ')" = 1 ] ||
	fail "probe image validation output is malformed or non-unique"
observed_probe_binary_sha256=$(printf '%s\n' "$warm_probe_receipt" |
	sed -n 's/.* binary_sha256=\([0-9a-f]\{64\}\) production_gates=.*/\1/p')
[ "$observed_probe_binary_sha256" = "$expected_probe_binary_sha256" ] ||
	fail "probe executable SHA-256 does not match the harness-pinned binary"
capture_probe_pod_identity "$warm_probe_pod" \
	"$temporary/probe-warm-runtime-identity.json" Succeeded ||
	fail "warm probe runtime image identity does not match the fixed digest"
delete_pod_by_uid "$warm_probe_pod" "$warm_uid" 0 ||
	fail "probe warm Pod could not be deleted by UID"

watchdog_marker=$temporary/WATCHDOG_ARMED
watchdog_heartbeat=$temporary/WATCHDOG_HEARTBEAT
printf 'armed_at=%s timeout_seconds=300 heartbeat_stale_seconds=240\n' \
	"$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$watchdog_marker"
heartbeat
nohup "$0" --watchdog "$probe_image" "$manifests" "$temporary" "$watchdog_marker" \
	>"$temporary/watchdog.log" 2>&1 &
watchdog_pid=$!
printf '%s\n' "$watchdog_pid" >"$temporary/watchdog.pid"

switch_mock_mode "$worker1_node" success slow-success "$temporary/mode-worker-1-success-to-slow.log" ||
	fail "Worker 1 could not enter slow-success mode"
switch_mock_mode "$worker2_node" success slow-success "$temporary/mode-worker-2-success-to-slow.log" ||
	fail "Worker 2 could not enter slow-success mode"

drained=$(query_database "
WITH changed AS (
  UPDATE workers worker SET lifecycle_state='DRAINING',updated_at=clock_timestamp()
  WHERE worker.id='$worker2_id'::uuid AND worker.epoch=1
    AND worker.lifecycle_state='READY' AND worker.reachability_condition='HEALTHY'
    AND NOT EXISTS (SELECT 1 FROM attempt_leases lease
      WHERE lease.worker_id=worker.id AND lease.revoked_at IS NULL)
  RETURNING worker.id
)
SELECT count(*) FROM changed;")
[ "$drained" = 1 ] || fail "Worker 2 could not be guarded into DRAINING"

prepare_finalization_retry_policy "$temporary/finalization-retry-policy-activation.log" ||
	fail "could not activate the lab-only FINALIZATION retry policy"
finalization_retry_policy_state >"$temporary/finalization-retry-policy-active.txt"
$kubectl_bin get deployment --namespace "$namespace" vela-lab-control -o json \
	>"$temporary/control-fault-deployment-before.json"
enable_visible_completion_fault || fail "could not enable the bounded Visible Completion fault"
$kubectl_bin get deployment --namespace "$namespace" vela-lab-control -o json \
	>"$temporary/control-fault-deployment-enabled.json"
[ "$(control_fault_state)" = ENABLED ] || fail "Visible Completion fault did not become active"

database_started_at=$(query_database 'SELECT clock_timestamp();')
printf '%s\n' "$database_started_at" >"$temporary/database-started-at.txt"
job_resource=$($kubectl_bin create -f "$manifests/60-smoke.yaml" -o name)
case "$job_resource" in job.batch/vela-lab-smoke-*) ;; *) fail "unexpected smoke Job identity $job_resource" ;; esac
printf '%s\n' "$job_resource" >"$temporary/kubernetes-job.txt"

iteration=0
while [ "$iteration" -lt 30 ]; do
	rows=$(query_database "SELECT id FROM jobs WHERE created_at >= '$database_started_at'::timestamptz ORDER BY created_at;")
	count=$(printf '%s\n' "$rows" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')
	if [ "$count" -eq 1 ]; then job_id=$rows; break; fi
	[ "$count" -eq 0 ] || fail "more than one application Job appeared during the rehearsal"
	iteration=$((iteration + 1))
	sleep 1
done
printf '%s\n' "$job_id" | grep -Eq \
	'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' ||
	fail "application Job ID was not observed"
printf '%s\n' "$job_id" >"$temporary/job-id.txt"
query_database "
SELECT service_class_revision_id,execution_max_attempts,
  execution_max_finalization_seconds_per_attempt,
  array_to_string(execution_retryable_failure_classes,',')
FROM jobs WHERE id='$job_id'::uuid;" >"$temporary/job-finalization-retry-snapshot.txt"
[ "$(cat "$temporary/job-finalization-retry-snapshot.txt")" = \
	"$finalization_service_class_id|2|60|WORKER_LOST,TRANSIENT_BACKEND,FINALIZATION_TIMEOUT" ] ||
	fail "application Job did not bind the lab-only FINALIZATION retry snapshot"
finalization_retry_policy_state >"$temporary/finalization-retry-policy-during-job.txt"
[ "$(cat "$temporary/finalization-retry-policy-during-job.txt")" = \
	'DRAINING|ACTIVE|LEGACY|1|1|1' ] ||
	fail "lab-only FINALIZATION retry policy did not remain active for replacement Assignment"

wait_visible_completion_fault_marker \
	"$temporary/control-pod-with-visible-completion-fault.json" \
	"$temporary/visible-completion-fault-marker.json" ||
	fail "control did not block an exact Worker 1 Visible Completion candidate"
original_attempt=$(jq -er '.attempt_id' "$temporary/visible-completion-fault-marker.json")
original_fence=$(jq -er '.lease_fence' "$temporary/visible-completion-fault-marker.json")
original_epoch=$(jq -er '.worker_epoch' "$temporary/visible-completion-fault-marker.json")
original_job_version=$(jq -er '.expected_job_version' "$temporary/visible-completion-fault-marker.json")

query_database "
SELECT jsonb_build_object(
  'job_id',job.id,'job_state',job.state,'job_version',job.version,
  'attempt_id',attempt.id,'attempt_state',attempt.state,
  'worker_id',attempt.worker_id,'worker_epoch',attempt.worker_epoch,
  'attempt_fence',attempt.fence,'finalization_deadline_at',attempt.finalization_deadline_at,
  'lease_id',lease.id,'lease_phase',lease.phase,'lease_owner_kind',lease.owner_kind,
  'lease_revoked',lease.revoked_at IS NOT NULL,
  'artifacts',COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'artifact_id',artifact.id,'state',artifact.state,'verification_id',artifact.verification_id,
    'verified_at',artifact.verified_at,'kind',artifact.kind,'ordinal',artifact.ordinal)
    ORDER BY artifact.kind,artifact.ordinal)
    FROM artifacts artifact WHERE artifact.job_id=job.id AND artifact.attempt_id=attempt.id),'[]'::jsonb)
)::text
FROM jobs job
JOIN attempts attempt ON attempt.job_id=job.id
JOIN attempt_leases lease ON lease.attempt_id=attempt.id AND lease.revoked_at IS NULL
WHERE job.id='$job_id'::uuid AND attempt.id='$original_attempt'::uuid;" \
	>"$temporary/original-finalization-authority.json"
jq -e \
	--arg job_id "$job_id" \
	--arg worker_id "$worker1_id" \
	--argjson worker_epoch "$original_epoch" \
	--argjson fence "$original_fence" \
	--argjson job_version "$original_job_version" \
	--slurpfile marker "$temporary/visible-completion-fault-marker.json" '
	  .job_id == $job_id
	  and .job_state == "FINALIZING" and .job_version == $job_version
	  and .attempt_id == $marker[0].attempt_id and .attempt_state == "FINALIZING"
	  and .worker_id == $worker_id and .worker_epoch == $worker_epoch and .attempt_fence == $fence
	  and .lease_phase == "FINALIZATION" and .lease_owner_kind == "WORKER"
	  and .lease_revoked == false and .finalization_deadline_at != null
	  and (.artifacts | length) == 2
	  and all(.artifacts[];
	    .state == "VERIFIED" and .verification_id != null and .verified_at != null)
	  and ([.artifacts[].artifact_id] | sort) == ([$marker[0].artifact_ids[]] | sort)
	' "$temporary/original-finalization-authority.json" >/dev/null ||
	fail "blocked candidate is not bound to exact FINALIZATION Lease and verified Artifacts"

probe_pod_json "$job_id" "$original_attempt" "$original_epoch" "$original_fence" \
	>"$temporary/probe-pod.json"
$kubectl_bin create --dry-run=server -f "$temporary/probe-pod.json" -o json \
	>"$temporary/probe-pod-server-dry-run.json"
$kubectl_bin create -f "$temporary/probe-pod.json" >/dev/null
probe_uid=$($kubectl_bin get pod --namespace "$namespace" "$probe_pod" -o jsonpath='{.metadata.uid}')
printf '%s\n' "$probe_uid" >"$temporary/probe-pod-uid.txt"

iteration=0
while [ "$iteration" -lt 120 ]; do
	heartbeat
	probe_phase=$($kubectl_bin get pod --namespace "$namespace" "$probe_pod" -o jsonpath='{.status.phase}')
	$kubectl_bin logs --namespace "$namespace" "$probe_pod" >"$temporary/probe-live.log" 2>/dev/null || true
	if grep -Fqx "$probe_loaded_status" "$temporary/probe-live.log"; then break; fi
	case "$probe_phase" in Failed|Succeeded) fail "probe terminated before loading old authority" ;; esac
	iteration=$((iteration + 1))
	sleep 1
done
grep -Fqx "$probe_loaded_status" "$temporary/probe-live.log" ||
	fail "probe did not confirm in-memory old authority"
capture_probe_pod_identity "$probe_pod" "$temporary/probe-runtime-identity-before-replay.json" Running ||
	fail "live probe runtime image identity does not match the fixed digest"

apply_partition || fail "could not apply the bounded Worker 1 control partition"
$kubectl_bin get networkpolicy --namespace "$namespace" "$application_policy" -o json \
	>"$temporary/network-policy-partition.json"
prove_partition_active || fail "Worker 1 control partition did not pass the TCP block canary"
disable_visible_completion_fault_after_partition \
	"$temporary/control-pod-before-reconnect.json" \
	"$temporary/control-pod-after-reconnect.json" \
	"$temporary/control-reconnect.json" ||
	fail "control fault could not be disabled with a new fixed-image Pod"
$kubectl_bin get deployment --namespace "$namespace" vela-lab-control -o json \
	>"$temporary/control-fault-deployment-disabled.json"

restore_rehearsal_worker "$worker2_id" || fail "Worker 2 could not return to READY/HEALTHY"
old_worker_pod=$($kubectl_bin get pods --namespace "$namespace" \
	-l app.kubernetes.io/name=vela-lab-worker-agent-1 -o jsonpath='{.items[0].metadata.name}')
old_worker_uid=$($kubectl_bin get pod --namespace "$namespace" "$old_worker_pod" \
	-o jsonpath='{.metadata.uid}')
printf '%s|%s\n' "$old_worker_pod" "$old_worker_uid" >"$temporary/worker-1-pod-before-delete.txt"
delete_pod_by_uid "$old_worker_pod" "$old_worker_uid" 0 ||
	fail "Worker 1 Agent Pod could not be deleted by UID"

replacement=
iteration=0
while [ "$iteration" -lt 420 ]; do
	heartbeat
	replacement=$(query_database "
SELECT attempt.id,attempt.state,attempt.fence,job.version
FROM attempts attempt JOIN jobs job ON job.id=attempt.job_id
WHERE attempt.job_id='$job_id'::uuid AND attempt.worker_id='$worker2_id'::uuid
  AND attempt.fence > $original_fence
ORDER BY attempt.attempt_number DESC LIMIT 1;")
	case "$replacement" in *'|RUNNING|'*) break ;; esac
	job_state=$(query_database "SELECT state FROM jobs WHERE id='$job_id'::uuid;")
	case "$job_state" in FAILED|CANCELED|SUCCEEDED) fail "Job reached $job_state before replacement entered RUNNING" ;; esac
	iteration=$((iteration + 1))
	sleep 1
done
case "$replacement" in *'|RUNNING|'*) ;; *) fail "Worker 2 replacement did not enter RUNNING" ;; esac
printf '%s\n' "$replacement" >"$temporary/replacement-attempt-running.txt"
replacement_attempt=$(printf '%s\n' "$replacement" | cut -d '|' -f 1)
replacement_fence=$(printf '%s\n' "$replacement" | cut -d '|' -f 3)
replacement_job_version=$(printf '%s\n' "$replacement" | cut -d '|' -f 4)

retry_decision=$(query_database "
SELECT id,source,disposition,attempt_state,failure_class,attempt_fence,job_fence,job_version
FROM execution_failure_decisions
WHERE job_id='$job_id'::uuid AND attempt_id='$original_attempt'::uuid;")
printf '%s\n' "$retry_decision" >"$temporary/retry-decision.txt"
[ "$(printf '%s\n' "$retry_decision" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')" = 1 ] ||
	fail "original Attempt does not have exactly one durable failure decision"
decision_id=$(printf '%s\n' "$retry_decision" | cut -d '|' -f 1)
decision_source=$(printf '%s\n' "$retry_decision" | cut -d '|' -f 2)
decision_disposition=$(printf '%s\n' "$retry_decision" | cut -d '|' -f 3)
decision_attempt_state=$(printf '%s\n' "$retry_decision" | cut -d '|' -f 4)
decision_failure_class=$(printf '%s\n' "$retry_decision" | cut -d '|' -f 5)
decision_attempt_fence=$(printf '%s\n' "$retry_decision" | cut -d '|' -f 6)
decision_job_fence=$(printf '%s\n' "$retry_decision" | cut -d '|' -f 7)
decision_job_version=$(printf '%s\n' "$retry_decision" | cut -d '|' -f 8)
[ "$decision_source" = FINALIZATION_DEADLINE_EXPIRED ] &&
	[ "$decision_disposition" = RETRY_WAIT ] &&
	[ "$decision_attempt_state" = FAILED ] &&
	[ "$decision_failure_class" = FINALIZATION_TIMEOUT ] &&
	[ "$decision_attempt_fence" = "$original_fence" ] ||
	fail "original FINALIZATION authority did not produce the expected retry decision"
[ "$decision_job_fence" -gt "$original_fence" ] &&
	[ "$replacement_fence" -gt "$decision_job_fence" ] &&
	[ "$decision_job_version" -gt "$original_job_version" ] &&
	[ "$replacement_job_version" -gt "$decision_job_version" ] ||
	fail "replacement authority is not strictly newer than the failure decision"
jq -n \
	--arg decision_id "$decision_id" \
	--arg source "$decision_source" \
	--arg disposition "$decision_disposition" \
	--arg attempt_state "$decision_attempt_state" \
	--arg failure_class "$decision_failure_class" \
	--argjson attempt_fence "$decision_attempt_fence" \
	--argjson job_fence "$decision_job_fence" \
	--argjson job_version "$decision_job_version" '
	{schema:"vela-lab-finalization-retry-decision-v1",decision_id:$decision_id,
	 source:$source,disposition:$disposition,attempt_state:$attempt_state,
	 failure_class:$failure_class,attempt_fence:$attempt_fence,
	 job_fence:$job_fence,job_version:$job_version}
	' >"$temporary/retry-decision.json"

jq -nc \
	--arg job_id "$job_id" \
	--arg original_attempt_id "$original_attempt" \
	--arg replacement_attempt_id "$replacement_attempt" \
	--argjson original_fence "$original_fence" \
	--argjson replacement_fence "$replacement_fence" \
	--argjson original_job_version "$original_job_version" \
	--argjson replacement_job_version "$replacement_job_version" '
	{schema:"vela-lab-stale-completion-signal-v1",job_id:$job_id,
	 original_attempt_id:$original_attempt_id,original_fence:$original_fence,
	 replacement_attempt_id:$replacement_attempt_id,replacement_fence:$replacement_fence,
	 original_job_version:$original_job_version,replacement_job_version:$replacement_job_version}' \
	>"$temporary/signal.json"
$kubectl_bin create configmap "$signal_configmap" --namespace "$namespace" \
	--from-file=go.json="$temporary/signal.json" --dry-run=client -o json |
	jq '.metadata.labels={"app.kubernetes.io/name":"vela-lab-stale-completion-signal",
	  "app.kubernetes.io/component":"lab-rehearsal","vela.ai/environment":"non-production-lab"}' \
	>"$temporary/signal-configmap.json"
$kubectl_bin create --dry-run=server -f "$temporary/signal-configmap.json" -o json \
	>"$temporary/signal-configmap-server-dry-run.json"
$kubectl_bin create -f "$temporary/signal-configmap.json" >/dev/null

if ! $kubectl_bin wait --namespace "$namespace" "pod/$probe_pod" \
	--for=jsonpath='{.status.phase}'=Succeeded --timeout=180s >/dev/null; then
	$kubectl_bin describe --namespace "$namespace" "pod/$probe_pod" \
		>"$temporary/probe-describe.txt" 2>&1 || true
	$kubectl_bin logs --namespace "$namespace" "$probe_pod" >"$temporary/probe.log" 2>&1 || true
	fail "stale Completion probe did not succeed"
fi
$kubectl_bin logs --namespace "$namespace" "$probe_pod" >"$temporary/probe.log"
capture_probe_pod_identity "$probe_pod" "$temporary/probe-runtime-identity-after-replay.json" Succeeded ||
	fail "completed probe runtime image identity does not match the fixed digest"
[ "$(sed -n '1p' "$temporary/probe.log")" = "$probe_loaded_status" ] ||
	fail "probe authority-loaded status is invalid"
[ "$(wc -l <"$temporary/probe.log" | tr -d ' ')" -eq 2 ] ||
	fail "probe output must contain exactly two payload-free receipt lines"
sed -n '2p' "$temporary/probe.log" >"$temporary/probe-receipt.json"
jq -e \
	--arg job_id "$job_id" \
	--arg original_attempt_id "$original_attempt" \
	--arg worker_id "$worker1_id" \
	--arg replacement_attempt_id "$replacement_attempt" \
	--arg binary_sha256 "$expected_probe_binary_sha256" \
	--argjson worker_epoch "$original_epoch" \
	--argjson original_fence "$original_fence" \
	--argjson replacement_fence "$replacement_fence" \
	--argjson original_job_version "$original_job_version" \
	--argjson replacement_job_version "$replacement_job_version" \
	--slurpfile marker "$temporary/visible-completion-fault-marker.json" '
	  type == "object"
	  and keys == ["artifact_ids","authority_file_sha256","binary_sha256","candidate_sha256",
	    "completion_id","decision","job_id","original_attempt_id","original_fence",
	    "original_job_version","original_worker_epoch","original_worker_id","production_gates",
	    "replacement_attempt_id","replacement_fence","replacement_job_version","schema"]
	  and .schema == "vela-lab-stale-completion-probe-v1"
	  and .job_id == $job_id
	  and .original_attempt_id == $original_attempt_id
	  and .original_worker_id == $worker_id
	  and .original_worker_epoch == $worker_epoch
	  and .original_fence == $original_fence
	  and .replacement_attempt_id == $replacement_attempt_id
	  and .replacement_fence == $replacement_fence
	  and .original_job_version == $original_job_version
	  and .replacement_job_version == $replacement_job_version
	  and .completion_id == $marker[0].completion_id
	  and .artifact_ids == $marker[0].artifact_ids
	  and .candidate_sha256 == $marker[0].candidate_sha256
	  and (.authority_file_sha256 | test("^[0-9a-f]{64}$"))
	  and .binary_sha256 == $binary_sha256
	  and .decision == "REJECTED_STALE_LEASE"
	  and .production_gates == "0/9"
	' "$temporary/probe-receipt.json" >/dev/null || fail "probe receipt is invalid"
if grep -Eqi 'lease[_ -]?token|bearer|authorization' "$temporary/probe.log"; then
	fail "probe output contains a forbidden authority label"
fi

final=
iteration=0
while [ "$iteration" -lt 120 ]; do
	heartbeat
	final=$(query_database "
SELECT
  job.state,
  (SELECT count(*) FROM attempts WHERE job_id=job.id),
  (SELECT count(*) FROM attempts WHERE job_id=job.id AND state='LOST'),
  (SELECT count(*) FROM attempts WHERE job_id=job.id AND state='FAILED'),
  (SELECT count(*) FROM attempts WHERE job_id=job.id AND state='SUCCEEDED'),
  (SELECT count(*) FROM visible_completions WHERE job_id=job.id),
  (SELECT count(*) FROM charges WHERE job_id=job.id AND reason='VISIBLE_COMPLETION' AND state='POSTED'),
  (SELECT count(*) FROM artifacts WHERE job_id=job.id AND state='COMMITTED'),
  job.current_fence
FROM jobs job WHERE job.id='$job_id'::uuid;")
	printf '%s\n' "$final" >>"$temporary/authority-timeline.txt"
	case "$final" in
		SUCCEEDED'|2|0|1|1|1|1|2|'*) break ;;
		SUCCEEDED'|'*) fail "Job succeeded with an unexpected authority shape" ;;
		FAILED'|'*|CANCELED'|'*) fail "Job reached terminal failure before replacement completion" ;;
	esac
	iteration=$((iteration + 1))
	sleep 1
done
case "$final" in SUCCEEDED'|2|0|1|1|1|1|2|'*) ;; *) fail "replacement Attempt did not converge" ;; esac

switch_mock_mode "$worker1_node" slow-success success "$temporary/mode-worker-1-slow-to-success.log" ||
	fail "Worker 1 could not return to success mode"
switch_mock_mode "$worker2_node" slow-success success "$temporary/mode-worker-2-slow-to-success.log" ||
	fail "Worker 2 could not return to success mode"
restore_policy || fail "baseline NetworkPolicy could not be restored"
$kubectl_bin rollout status deployment/vela-lab-worker-agent-1 --namespace "$namespace" --timeout=120s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-worker-agent-2 --namespace "$namespace" --timeout=120s >/dev/null
[ "$(active_lease_count)" = 0 ] || fail "active Lease remained after terminal application state"
restore_rehearsal_worker "$worker1_id" || fail "Worker 1 could not be restored"
restore_rehearsal_worker "$worker2_id" || fail "Worker 2 could not be restored"
restore_finalization_retry_policy "$temporary/finalization-retry-policy-restoration.log" ||
	fail "could not restore the baseline ServiceClassRevision after application terminal state"
finalization_retry_policy_state >"$temporary/finalization-retry-policy-after.txt"

if ! $kubectl_bin wait --namespace "$namespace" "$job_resource" \
	--for=condition=complete --timeout=60s >/dev/null; then
	$kubectl_bin describe --namespace "$namespace" "$job_resource" \
		>"$temporary/smoke-job-describe.txt" 2>&1 || true
	$kubectl_bin logs --namespace "$namespace" "$job_resource" >"$temporary/smoke-job.log" 2>&1 || true
	fail "smoke wrapper did not complete after application success"
fi
$kubectl_bin logs --namespace "$namespace" "$job_resource" >"$temporary/smoke-job.log"
jq -Rrc 'fromjson? | select(.status == "LAB VERIFIED")' "$temporary/smoke-job.log" \
	>"$temporary/smoke-receipt-candidates.jsonl"
[ "$(sed '/^[[:space:]]*$/d' "$temporary/smoke-receipt-candidates.jsonl" | wc -l | tr -d ' ')" = 1 ] ||
	fail "smoke receipt is missing or non-unique"
jq -s --arg job_id "$job_id" '
	  select(length == 1 and .[0].job_id == $job_id
	    and .[0].final_state == "SUCCEEDED" and .[0].artifact_count == 2)
	  | .[0]
	' "$temporary/smoke-receipt-candidates.jsonl" >"$temporary/smoke-receipt.json"
[ -s "$temporary/smoke-receipt.json" ] || fail "smoke receipt does not match the rehearsed Job"
rm -f -- "$temporary/smoke-receipt-candidates.jsonl"

query_database "
SELECT jsonb_build_object(
  'job_id',job.id,'job_state',job.state,'current_fence',job.current_fence,
  'job_version',job.version,'result_artifact_set_id',job.result_artifact_set_id,
  'attempts_started',retry.attempts_started,
  'attempts',COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'attempt_id',attempt.id,'attempt_number',attempt.attempt_number,
    'worker_id',attempt.worker_id,'state',attempt.state,'fence',attempt.fence,
    'started_at',attempt.started_at,'ended_at',attempt.ended_at,
    'lease_expires_at',lease.expires_at,'lease_revoked_at',lease.revoked_at)
    ORDER BY attempt.attempt_number)
    FROM attempts attempt LEFT JOIN attempt_leases lease ON lease.attempt_id=attempt.id
    WHERE attempt.job_id=job.id),'[]'::jsonb),
  'retry_decisions',COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'decision_id',decision.id,'attempt_id',decision.attempt_id,'source',decision.source,
    'disposition',decision.disposition,'attempt_state',decision.attempt_state,
    'failure_class',decision.failure_class,'attempt_fence',decision.attempt_fence,
    'job_fence',decision.job_fence,'job_version',decision.job_version))
    FROM execution_failure_decisions decision WHERE decision.job_id=job.id),'[]'::jsonb),
  'artifacts',COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'artifact_id',artifact.id,'attempt_id',artifact.attempt_id,'attempt_fence',artifact.attempt_fence,
    'kind',artifact.kind,'ordinal',artifact.ordinal,'state',artifact.state,
    'verification_id',artifact.verification_id,'verified_at',artifact.verified_at)
    ORDER BY artifact.attempt_id,artifact.kind,artifact.ordinal)
    FROM artifacts artifact WHERE artifact.job_id=job.id),'[]'::jsonb),
  'artifact_sets',COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'artifact_set_id',artifact_set.id,'attempt_id',artifact_set.attempt_id,
    'attempt_fence',artifact_set.attempt_fence))
    FROM artifact_sets artifact_set WHERE artifact_set.job_id=job.id),'[]'::jsonb),
  'artifact_set_items',COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'artifact_set_id',item.artifact_set_id,'artifact_id',item.artifact_id,
    'attempt_id',item.attempt_id,'attempt_fence',item.attempt_fence,
    'kind',item.kind,'ordinal',item.ordinal) ORDER BY item.kind,item.ordinal)
    FROM artifact_set_items item WHERE item.job_id=job.id),'[]'::jsonb),
  'visible_completions',COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'completion_id',completion.id,'attempt_id',completion.attempt_id,
    'attempt_fence',completion.attempt_fence,'artifact_set_id',completion.artifact_set_id,
    'charge_id',completion.charge_id,'candidate_sha256',encode(completion.candidate_sha256,'hex'),
    'completed_at',completion.completed_at))
    FROM visible_completions completion WHERE completion.job_id=job.id),'[]'::jsonb),
  'charges',COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'charge_id',charge.id,'reason',charge.reason,'state',charge.state,
    'artifact_set_id',charge.artifact_set_id))
    FROM charges charge WHERE charge.job_id=job.id),'[]'::jsonb),
  'access_grants',COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'grant_id',grant_row.id,'artifact_set_id',grant_row.artifact_set_id,
    'revoked_at',grant_row.revoked_at))
    FROM artifact_access_grants grant_row WHERE grant_row.job_id=job.id),'[]'::jsonb)
)::text
FROM jobs job JOIN retry_runtime_states retry ON retry.job_id=job.id
WHERE job.id='$job_id'::uuid;" >"$temporary/authority-after.json"
jq -e \
	--arg original_attempt "$original_attempt" \
	--arg replacement_attempt "$replacement_attempt" \
	--arg candidate_sha256 "$(jq -er '.candidate_sha256' "$temporary/visible-completion-fault-marker.json")" \
	--argjson original_fence "$original_fence" \
	--argjson replacement_fence "$replacement_fence" \
	--argjson decision_job_fence "$decision_job_fence" '
	  . as $root
	  | .job_state == "SUCCEEDED" and .attempts_started == 2
	  and .result_artifact_set_id != null
	  and (.attempts | length) == 2
	  and any(.attempts[]; .attempt_id == $original_attempt and .state == "FAILED" and .fence == $original_fence)
	  and any(.attempts[]; .attempt_id == $replacement_attempt and .state == "SUCCEEDED" and .fence == $replacement_fence)
	  and (.retry_decisions | length) == 1
	  and .retry_decisions[0].attempt_id == $original_attempt
	  and .retry_decisions[0].source == "FINALIZATION_DEADLINE_EXPIRED"
	  and .retry_decisions[0].failure_class == "FINALIZATION_TIMEOUT"
	  and .retry_decisions[0].job_fence == $decision_job_fence
	  and (.artifacts | length) == 4
	  and ([.artifacts[] | select(.attempt_id == $original_attempt and .state == "VERIFIED")] | length) == 2
	  and ([.artifacts[] | select(.attempt_id == $replacement_attempt and .state == "COMMITTED")] | length) == 2
	  and (.artifact_sets | length) == 1
	  and .artifact_sets[0].artifact_set_id == .result_artifact_set_id
	  and .artifact_sets[0].attempt_id == $replacement_attempt
	  and .artifact_sets[0].attempt_fence == $replacement_fence
	  and (.artifact_set_items | length) == 2
	  and all(.artifact_set_items[];
	    .artifact_set_id == $root.result_artifact_set_id and .attempt_id == $replacement_attempt
	    and .attempt_fence == $replacement_fence)
	  and ([.artifact_set_items[].artifact_id] | sort) ==
	    ([.artifacts[] | select(.attempt_id == $replacement_attempt and .state == "COMMITTED") | .artifact_id] | sort)
	  and (.visible_completions | length) == 1
	  and .visible_completions[0].attempt_id == $replacement_attempt
	  and .visible_completions[0].attempt_fence == $replacement_fence
	  and .visible_completions[0].artifact_set_id == .result_artifact_set_id
	  and .visible_completions[0].candidate_sha256 != $candidate_sha256
	  and (.charges | length) == 1
	  and .charges[0].reason == "VISIBLE_COMPLETION" and .charges[0].state == "POSTED"
	  and .charges[0].artifact_set_id == .result_artifact_set_id
	  and .charges[0].charge_id == .visible_completions[0].charge_id
	  and (.access_grants | length) == 1
	  and .access_grants[0].artifact_set_id == .result_artifact_set_id
	  and .access_grants[0].revoked_at == null
	' "$temporary/authority-after.json" >/dev/null ||
	fail "final ArtifactSet, Artifact, Charge, and result-pointer ledger is inconsistent"

query_database "
SELECT jsonb_build_object(
  'event_id',event_id,'aggregate_version',aggregate_version,'event_type',event_type,
  'schema_version',schema_version,'occurred_at',occurred_at,'published_at',published_at,
  'broker_stream',broker_stream,'broker_sequence',broker_sequence,
  'payload_encoding','base64-protobuf','payload_base64',encode(payload,'base64'))::text
FROM outbox_events WHERE aggregate_id='$job_id'::uuid
ORDER BY aggregate_version,event_type;" >"$temporary/raw-event-payloads.jsonl"
[ -s "$temporary/raw-event-payloads.jsonl" ] || fail "raw event payload receipt is empty"

measurements=$(query_database "
SELECT
  CASE WHEN job.state='SUCCEEDED' THEN 0 ELSE 1 END,
  GREATEST((SELECT count(*) FROM visible_completions WHERE job_id=job.id)-1,0),
  GREATEST((SELECT count(*) FROM charges WHERE job_id=job.id
    AND reason='VISIBLE_COMPLETION' AND state='POSTED')-1,0),
  (SELECT count(*) FROM visible_completions completion
   WHERE completion.job_id=job.id AND completion.attempt_id='$original_attempt'::uuid),
  (SELECT count(*) FROM attempts WHERE job_id=job.id AND id='$original_attempt'::uuid
    AND worker_id='$worker1_id'::uuid AND state='FAILED' AND fence=$original_fence),
  (SELECT count(*) FROM attempts WHERE job_id=job.id AND id='$replacement_attempt'::uuid
    AND worker_id='$worker2_id'::uuid AND state='SUCCEEDED' AND fence=$replacement_fence),
  (SELECT count(*) FROM attempt_leases lease JOIN attempts attempt ON attempt.id=lease.attempt_id
    WHERE attempt.job_id=job.id AND lease.revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts),
  (SELECT count(*) FROM workers WHERE id IN ('$worker1_id'::uuid,'$worker2_id'::uuid)
    AND lifecycle_state='READY' AND reachability_condition='HEALTHY'),
  (SELECT count(*) FROM artifacts WHERE job_id=job.id),
  (SELECT count(*) FROM artifacts WHERE job_id=job.id
    AND attempt_id='$original_attempt'::uuid AND state='VERIFIED'),
  (SELECT count(*) FROM artifacts WHERE job_id=job.id
    AND attempt_id='$replacement_attempt'::uuid AND state='COMMITTED'),
  (SELECT count(*) FROM artifact_sets WHERE job_id=job.id
    AND attempt_id='$replacement_attempt'::uuid AND attempt_fence=$replacement_fence
    AND id=job.result_artifact_set_id),
  (SELECT count(*) FROM artifact_set_items WHERE job_id=job.id
    AND attempt_id='$replacement_attempt'::uuid AND attempt_fence=$replacement_fence
    AND artifact_set_id=job.result_artifact_set_id),
  (SELECT count(*) FROM charges WHERE job_id=job.id),
  (SELECT count(*) FROM charges WHERE job_id=job.id AND reason='VISIBLE_COMPLETION'
    AND state='POSTED' AND artifact_set_id=job.result_artifact_set_id),
  (SELECT count(*) FROM visible_completions WHERE job_id=job.id
    AND attempt_id='$replacement_attempt'::uuid AND attempt_fence=$replacement_fence
    AND artifact_set_id=job.result_artifact_set_id),
  (SELECT count(*) FROM artifact_access_grants WHERE job_id=job.id
    AND artifact_set_id=job.result_artifact_set_id AND revoked_at IS NULL)
FROM jobs job WHERE job.id='$job_id'::uuid;")
printf '%s\n' "$measurements" >"$temporary/measurements.txt"
[ "$measurements" = '0|0|0|0|1|1|0|0|2|4|2|2|1|2|1|1|1|1' ] ||
	fail "final measurements do not satisfy the stale-fence contract"

neutralize_probe "$temporary/probe-cleanup.log" || fail "probe resources could not be removed"
[ "$(policy_values)" = '["bootstrap","control-plane","worker-agent","smoke"]' ] ||
	fail "application egress policy did not remain at baseline"
[ "$(current_mock_mode "$worker1_node")" = success ] || fail "Worker 1 mode did not remain success"
[ "$(current_mock_mode "$worker2_node")" = success ] || fail "Worker 2 mode did not remain success"
[ "$(control_fault_state)" = DISABLED ] || fail "Visible Completion fault remained enabled after rehearsal"
[ "$(finalization_retry_policy_state)" = 'ACTIVE|DRAINING|LEGACY|1|1|1' ] ||
	fail "baseline ServiceClassRevision was not restored after rehearsal"
capture_worker_agent_identity "$temporary/worker-agent-image-after.json" ||
	fail "final Worker Agent image identity does not match the expected digest"

exec 8>"$temporary/recovery.lock"
flock 8
disarm_watchdog
if [ -f "$temporary/WATCHDOG_RECOVERY_STARTED" ]; then
	flock -u 8
	exec 8>&-
	fail "watchdog recovery started; refusing to publish a PASS receipt"
fi
flock -u 8
exec 8>&-
rm -f -- "$temporary/recovery.lock"

completed_at=$(query_database 'SELECT clock_timestamp();')
jq \
	--arg job_id "$job_id" \
	--arg started_at "$database_started_at" \
	--arg completed_at "$completed_at" \
	--arg previous_receipt_sha256 "$previous_scenario_receipt_sha256" '
	(.scenarios[] | select(.id == "stale-fence-late-completion")) = {
	  id:"stale-fence-late-completion",status:"LAB_REHEARSAL_PASS",job_id:$job_id,
	  started_at:$started_at,completed_at:$completed_at,
	  fault:"OLD_WORKER_MTLS_COMPLETION_AFTER_HIGHER_FENCE_ASSIGNMENT",
	  decision:"REJECTED_STALE_LEASE",previous_receipt_sha256:$previous_receipt_sha256}
	' "$previous_scenario_receipt/scenario-matrix.json" >"$temporary/scenario-matrix.json"
jq -e --arg job_id "$job_id" '
	  .schema == "vela-lab-fault-scenario-matrix-v1"
	  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
	  and .production_gates == "0/9"
	  and (.scenarios | length == 10)
	  and ([.scenarios[].id] | sort) == ([
	    "process-kill","worker-control-network-partition","node-reboot",
	    "outbox-post-commit-crash","publisher-pre-puback-crash",
	    "publisher-post-puback-pre-mark-crash","consumer-post-db-pre-ack-crash",
	    "assignment-post-commit-pre-response-crash","retry-budget-exhaustion",
	    "stale-fence-late-completion"] | sort)
	  and ([.scenarios[] | select(.status == "LAB_REHEARSAL_PASS")] | length) == 10
	  and any(.scenarios[]; .id == "stale-fence-late-completion"
	    and .job_id == $job_id and .status == "LAB_REHEARSAL_PASS"
	    and .decision == "REJECTED_STALE_LEASE")
	' "$temporary/scenario-matrix.json" >/dev/null || fail "ten-scenario matrix is invalid"

harness_sha256=$(sha256sum "$0" | awk '{print $1}')
authority_file_sha256=$(jq -er '.authority_file_sha256' "$temporary/probe-receipt.json")
probe_binary_sha256=$(jq -er '.binary_sha256' "$temporary/probe-receipt.json")
visible_completion_count=$(jq -er '.visible_completions | length' "$temporary/authority-after.json")
posted_charge_count=$(jq -er '[.charges[] | select(.reason == "VISIBLE_COMPLETION" and .state == "POSTED")] | length' \
	"$temporary/authority-after.json")
artifact_row_count=$(jq -er '.artifacts | length' "$temporary/authority-after.json")
committed_artifact_count=$(jq -er '[.artifacts[] | select(.state == "COMMITTED")] | length' \
	"$temporary/authority-after.json")
artifact_set_count=$(jq -er '.artifact_sets | length' "$temporary/authority-after.json")
artifact_set_item_count=$(jq -er '.artifact_set_items | length' "$temporary/authority-after.json")
jq -n \
	--arg job_id "$job_id" \
	--arg started_at "$database_started_at" \
	--arg completed_at "$completed_at" \
	--arg harness_sha256 "$harness_sha256" \
	--arg previous_receipt_sha256 "$previous_scenario_receipt_sha256" \
	--arg probe_image "$probe_image" \
	--arg worker_agent_image "$expected_worker_agent_image" \
	--arg probe_binary_sha256 "$probe_binary_sha256" \
	--arg original_attempt_id "$original_attempt" \
	--arg replacement_attempt_id "$replacement_attempt" \
	--arg authority_file_sha256 "$authority_file_sha256" \
	--slurpfile retry_decision "$temporary/retry-decision.json" \
	--slurpfile worker_agent_rendered "$temporary/worker-agent-rendered-image.json" \
	--slurpfile worker_agent_before "$temporary/worker-agent-image-before.json" \
	--slurpfile worker_agent_after "$temporary/worker-agent-image-after.json" \
	--slurpfile control_reconnect "$temporary/control-reconnect.json" \
	--argjson original_fence "$original_fence" \
	--argjson replacement_fence "$replacement_fence" \
	--argjson visible_completions "$visible_completion_count" \
	--argjson posted_charges "$posted_charge_count" \
	--argjson artifact_rows "$artifact_row_count" \
	--argjson committed_artifacts "$committed_artifact_count" \
	--argjson artifact_sets "$artifact_set_count" \
	--argjson artifact_set_items "$artifact_set_item_count" '
	{
	  schema:"vela-lab-stale-fence-late-completion-v1",
	  status:"LAB_REHEARSAL_PASS",
	  evidence_boundary:"NON_PRODUCTION_MOCK_REHEARSAL",
	  production_gates:"0/9",fixed_scenarios_completed:10,fixed_scenarios_required:10,
	  job_id:$job_id,started_at:$started_at,completed_at:$completed_at,
	  harness_sha256:$harness_sha256,previous_receipt_sha256:$previous_receipt_sha256,
	  worker_agent:{image:$worker_agent_image,rendered:$worker_agent_rendered[0],
	    before:$worker_agent_before[0],after:$worker_agent_after[0]},
	  probe:{image:$probe_image,binary_sha256:$probe_binary_sha256,
	    transport:"WORKER_1_MTLS_GRPC",authority_source:"WORKER_1_READ_ONLY_RECOVERY_HOSTPATH",
	    authority_file_sha256:$authority_file_sha256,lease_token_recorded:false,
	    decision:"REJECTED_STALE_LEASE"},
	  control_reconnect:$control_reconnect[0],
	  attempts:{original:{attempt_id:$original_attempt_id,worker_id:"84000000-0000-0000-0000-000000000101",
	      fence:$original_fence,state:"FAILED"},
	    replacement:{attempt_id:$replacement_attempt_id,worker_id:"84000000-0000-0000-0000-000000000102",
	      fence:$replacement_fence,state:"SUCCEEDED"}},
	  retry_decision:$retry_decision[0],
	  visible_completions:$visible_completions,posted_charges:$posted_charges,
	  artifact_rows:$artifact_rows,committed_artifacts:$committed_artifacts,
	  artifact_sets:$artifact_sets,artifact_set_items:$artifact_set_items,
	  active_leases_after_recovery:0,
	  measurements:{"lost-accepted-job-count":0,"duplicate-visible-completion-count":0,
	    "duplicate-charge-count":0,"stale-authority-acceptance-count":0},
	  artifacts:["scenario-matrix.json","probe-receipt.json","probe.log","probe-pod.json",
	    "probe-warm-runtime-identity.json","probe-runtime-identity-before-replay.json",
	    "probe-runtime-identity-after-replay.json","visible-completion-fault-marker.json",
	    "original-finalization-authority.json","retry-decision.json",
	    "job-finalization-retry-snapshot.txt","finalization-retry-policy-before.txt",
	    "finalization-retry-policy-active.txt","finalization-retry-policy-during-job.txt",
	    "finalization-retry-policy-after.txt",
	    "signal.json","network-policy-partition.json","partition-canary-pod.json",
	    "partition-canary-server-dry-run.json","partition-canary.log","partition-canary-uid.txt",
	    "control-reconnect.json","control-fault-deployment-before.json",
	    "control-fault-deployment-enabled.json","control-fault-deployment-disabled.json",
	    "worker-agent-rendered-image.json",
	    "worker-agent-image-before.json","worker-agent-image-after.json",
	    "control-pod-before-reconnect.json","control-pod-after-reconnect.json","authority-after.json",
	    "authority-timeline.txt","raw-event-payloads.jsonl","measurements.txt","smoke-receipt.json"]
	}
	' >"$temporary/summary.json"
printf 'status=LAB_REHEARSAL_PASS production_gates=0/9 fixed_scenarios=10/10\n' >"$temporary/STATUS"

rm -f -- "$temporary/watchdog.pid" "$temporary/watchdog.log" "$temporary/probe-live.log"
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
printf 'schema=vela-lab-stale-fence-late-completion-wrapper-v1 output=%s job=%s result=LAB_REHEARSAL_PASS fixed_scenarios=10/10 production_gates=0/9\n' \
	"$output" "$job_id"

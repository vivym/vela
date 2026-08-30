#!/bin/sh

set -eu

namespace=vela-lab
control_node=vela-lab-control-1
worker1_id=84000000-0000-0000-0000-000000000101
worker2_id=84000000-0000-0000-0000-000000000102
expected_runner_image=10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3
expected_tool_image=10.1.200.17:5443/vela-lab/vela-lab-bootstrap@sha256:3f6a8bc440ee7bd7f9ba263d07435329c0863134217349db565cded2e9df9eac
previous_scenario_job_id=86a1e970-0847-4f8a-b9c1-4577ec2d6915
previous_scenario_receipt_sha256=a05cc044f7b2536cc58604aed95fadc5723806aeb814319d4058c3f4a210c3d9
previous_scenario_harness_sha256=6483f806b62c747110ff9a159d6e8bbba40a98efe46e599765a336874a21ed88
previous_scenario_receipt=/root/vela-lab-deploy-bc590e20/receipts/publisher-pre-puback-crash-v1
fault_phase=consumer-post-db-pre-ack-crash
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}
expected_control_image=
manifests=
temporary=
output=
job_id=
job_resource=
fault_pod_name=
fault_pod_uid=
committed=false
recovering=false
watchdog_marker=
watchdog_heartbeat=
watchdog_pid=
port_forward_pid=

fail() {
	printf 'consumer-post-db-pre-ack-crash: %s\n' "$*" >&2
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

heartbeat() {
	[ -z "$watchdog_heartbeat" ] || touch "$watchdog_heartbeat"
}

stop_nats_port_forward() {
	[ -z "$port_forward_pid" ] || kill "$port_forward_pid" >/dev/null 2>&1 || true
	[ -z "$port_forward_pid" ] || wait "$port_forward_pid" 2>/dev/null || true
	port_forward_pid=
}

fetch_nats_monitor() {
	fetch_pod_name=$1
	fetch_destination=$2
	fetch_log=$3
	"$kubectl_bin" port-forward --address=127.0.0.1 --namespace "$namespace" \
		"pod/$fetch_pod_name" :8222 >"$fetch_log" 2>&1 &
	port_forward_pid=$!
	fetch_local_port=
	fetch_iteration=0
	while [ "$fetch_iteration" -lt 10 ]; do
		if ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
			stop_nats_port_forward
			return 1
		fi
		fetch_local_port=$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9][0-9]*\) -> 8222$/\1/p' "$fetch_log" | head -n 1)
		[ -z "$fetch_local_port" ] || break
		fetch_iteration=$((fetch_iteration + 1))
		sleep 1
	done
	printf '%s\n' "$fetch_local_port" | grep -Eq '^[1-9][0-9]{3,4}$' || {
		stop_nats_port_forward
		return 1
	}
	if ! curl --connect-timeout 2 --max-time 5 --fail --silent --show-error \
		"http://127.0.0.1:$fetch_local_port/jsz?streams=true&consumers=true&config=true" >"$fetch_destination"; then
		stop_nats_port_forward
		return 1
	fi
	stop_nats_port_forward
	rm -f -- "$fetch_log"
}

capture_nats_stream_state() {
	state_destination=$1
	state_discovery=$state_destination.discovery.raw
	state_stream_raw=$state_destination.stream-leader.raw
	state_consumer_raw=$state_destination.consumer-leader.raw
	state_log=$state_destination.port-forward.log
	fetch_nats_monitor vela-lab-nats-0 "$state_discovery" "$state_log" || return 1
	state_stream_leader=$(jq -er '
	  [.account_details[].stream_detail[]? | select(.name == "VELA_EVENTS")]
	  | select(length == 1) | .[0].cluster.leader
	' "$state_discovery") || return 1
	printf '%s\n' "$state_stream_leader" | grep -Eq '^vela-lab-nats-[0-2]$' || return 1
	if [ "$state_stream_leader" = vela-lab-nats-0 ]; then
		mv "$state_discovery" "$state_stream_raw"
	else
		fetch_nats_monitor "$state_stream_leader" "$state_stream_raw" "$state_log" || return 1
		rm -f -- "$state_discovery"
	fi
	state_consumer_leader=$(jq -er '
	  [.account_details[].stream_detail[]? | select(.name == "VELA_EVENTS")
	    | .consumer_detail[]? | select(.name == "VELA_SCHEDULER")]
	  | select(length == 1) | .[0].cluster.leader
	' "$state_stream_raw") || return 1
	printf '%s\n' "$state_consumer_leader" | grep -Eq '^vela-lab-nats-[0-2]$' || return 1
	if [ "$state_consumer_leader" = "$state_stream_leader" ]; then
		state_consumer_raw=$state_stream_raw
	else
		fetch_nats_monitor "$state_consumer_leader" "$state_consumer_raw" "$state_log" || return 1
	fi
	jq -e --slurpfile consumer_source "$state_consumer_raw" '
	  [.account_details[].stream_detail[]? | select(.name == "VELA_EVENTS")] as $streams
	  | select(($streams | length) == 1)
	  | [$consumer_source[0].account_details[].stream_detail[]?
	      | select(.name == "VELA_EVENTS")
	      | .consumer_detail[]? | select(.name == "VELA_SCHEDULER")] as $consumers
	  | select(($consumers | length) == 1)
	  | {
	      schema:"vela-lab-nats-consumer-state-v1",
	      server_id:.server_id,
	      captured_at:.now,
	      stream:($streams[0] | {name, config, cluster, state}),
	      consumer:($consumers[0] | {
	        stream_name, name, config, delivered, ack_floor,
	        num_ack_pending, num_redelivered, num_waiting, num_pending, cluster
	      })
	    }
	' "$state_stream_raw" >"$state_destination" || return 1
	jq -e --arg stream_leader "$state_stream_leader" --arg consumer_leader "$state_consumer_leader" '
	  .schema == "vela-lab-nats-consumer-state-v1"
	  and .stream.name == "VELA_EVENTS"
	  and .stream.config.name == "VELA_EVENTS"
	  and .stream.config.subjects == ["vela.events.>"]
	  and .stream.config.num_replicas == 3
	  and .stream.config.duplicate_window == 600000000000
	  and .stream.config.deny_delete == true
	  and .stream.config.deny_purge == true
	  and .stream.cluster.leader == $stream_leader
	  and (.stream.cluster.replicas | length == 2)
	  and all(.stream.cluster.replicas[]; .current == true and ((.lag // 0) == 0))
	  and (.stream.state.messages | type == "number" and . >= 0 and floor == .)
	  and (.stream.state.last_seq | type == "number" and . >= 1 and floor == .)
	  and .consumer.stream_name == "VELA_EVENTS"
	  and .consumer.name == "VELA_SCHEDULER"
	  and .consumer.config.durable_name == "VELA_SCHEDULER"
	  and .consumer.config.name == "VELA_SCHEDULER"
	  and .consumer.config.ack_policy == "explicit"
	  and .consumer.config.ack_wait == 30000000000
	  and .consumer.config.max_deliver == -1
	  and .consumer.config.filter_subject == "vela.events.job.ready"
	  and .consumer.config.max_waiting == 32
	  and .consumer.config.max_ack_pending == 128
	  and .consumer.config.max_batch == 1
	  and .consumer.config.num_replicas == 3
	  and .consumer.config.metadata == {"vela.contract":"scheduler-wakeup","vela.revision":"1"}
	  and .consumer.cluster.leader == $consumer_leader
	  and (.consumer.cluster.replicas | length == 2)
	  and all(.consumer.cluster.replicas[]; .current == true and ((.lag // 0) == 0))
	  and (.consumer.delivered.consumer_seq | type == "number" and . >= 0 and floor == .)
	  and (.consumer.delivered.stream_seq | type == "number" and . >= 0 and floor == .)
	  and (.consumer.ack_floor.consumer_seq | type == "number" and . >= 0 and floor == .)
	  and (.consumer.ack_floor.stream_seq | type == "number" and . >= 0 and floor == .)
	  and (.consumer.num_ack_pending | type == "number" and . >= 0 and floor == .)
	  and (.consumer.num_redelivered | type == "number" and . >= 0 and floor == .)
	' "$state_destination" >/dev/null || return 1
	rm -f -- "$state_stream_raw"
	[ "$state_consumer_raw" = "$state_stream_raw" ] || rm -f -- "$state_consumer_raw"
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
	[ "$(sha256sum "$previous_scenario_receipt/SHA256SUMS" | awk '{print $1}')" = "$previous_scenario_receipt_sha256" ] ||
		fail "previous fixed-scenario receipt digest changed"
	[ "$(cat "$previous_scenario_receipt/STATUS")" = 'status=LAB_REHEARSAL_PASS production_gates=0/9 fixed_scenarios=6/10' ] ||
		fail "previous fixed-scenario STATUS is not a six-scenario lab pass"
	jq -e \
		--arg job_id "$previous_scenario_job_id" \
		--arg harness "$previous_scenario_harness_sha256" '
		  .schema == "vela-lab-publisher-pre-puback-crash-v1"
	  and .status == "LAB_REHEARSAL_PASS"
	  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
	  and .production_gates == "0/9"
		  and .fixed_scenarios_completed == 6
	  and .fixed_scenarios_required == 10
	  and .job_id == $job_id
	  and .harness_sha256 == $harness
	  and .visible_completions == 1
	  and .posted_charges == 1
	  and .artifact_rows == 2
	  and .committed_artifacts == 2
	' "$previous_scenario_receipt/summary.json" >/dev/null ||
		fail "previous fixed-scenario summary does not match the pinned evidence"
	jq -e \
		--arg publisher_job_id "$previous_scenario_job_id" '
	  .schema == "vela-lab-fault-scenario-matrix-v1"
	  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
	  and .production_gates == "0/9"
	  and (.scenarios | type == "array" and length == 10)
	  and ([.scenarios[].id] | sort) == ([
	    "process-kill", "worker-control-network-partition", "node-reboot",
	    "outbox-post-commit-crash", "publisher-pre-puback-crash",
	    "publisher-post-puback-pre-mark-crash", "consumer-post-db-pre-ack-crash",
	    "assignment-post-commit-pre-response-crash", "retry-budget-exhaustion",
	    "stale-fence-late-completion"
	  ] | sort)
		  and ([.scenarios[] | select(.status == "LAB_REHEARSAL_PASS")] | length) == 6
		  and any(.scenarios[];
		    .id == "publisher-pre-puback-crash"
		    and .status == "LAB_REHEARSAL_PASS"
		    and .job_id == $publisher_job_id)
		  and any(.scenarios[];
		    .id == "consumer-post-db-pre-ack-crash" and .status == "NOT_RUN")
	' "$previous_scenario_receipt/scenario-matrix.json" >/dev/null ||
		fail "previous fixed-scenario matrix does not match the pinned evidence"
}

load_tool_identity() {
	smoke_json=$($kubectl_bin create --dry-run=client -f "$manifests/60-smoke.yaml" -o json)
	tool_image=$(printf '%s\n' "$smoke_json" | jq -er '.spec.template.spec.containers | map(select(.name == "smoke")) | .[0].image')
	[ "$tool_image" = "$expected_tool_image" ] ||
		fail "lab tool image does not match the fixed private Registry digest"
}

delete_pod_by_uid() {
	name=$1
	uid=$2
	grace=$3
	printf '%s\n' "$uid" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' ||
		return 1
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
		heartbeat
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

wait_restarted_control_container() {
	destination=$1
	before_file=$2
	identity=$(control_identity "$before_file") || return 1
	name=$(printf '%s\n' "$identity" | cut -d '|' -f 1)
	uid=$(printf '%s\n' "$identity" | cut -d '|' -f 2)
	container_id=$(printf '%s\n' "$identity" | cut -d '|' -f 3)
	restart_count=$(printf '%s\n' "$identity" | cut -d '|' -f 4)
	printf '%s\n' "$restart_count" | grep -Eq '^[0-9]+$' || return 1
	expected_restart_count=$((restart_count + 1))
	error_file=$destination.error
	iteration=0
	while [ "$iteration" -lt 120 ]; do
		heartbeat
		pod=$($kubectl_bin get pod --namespace "$namespace" "$name" --ignore-not-found -o json) || return 1
		if [ -n "$pod" ] && printf '%s\n' "$pod" | jq -e \
			--arg uid "$uid" \
			--arg node "$control_node" \
			--arg image "$expected_control_image" \
			--arg old_container_id "containerd://$container_id" \
			--argjson restart_count "$expected_restart_count" '
		  select(
		    .metadata.uid == $uid
		    and .metadata.deletionTimestamp == null
		    and .spec.nodeName == $node
		    and .status.phase == "Running"
		    and (.spec.containers | map(select(.name == "control" and .image == $image)) | length) == 1
		    and ((.status.containerStatuses // []) | map(select(
		      .name == "control"
		      and .ready == true
			      and .restartCount == $restart_count
			      and .state.running.startedAt != null
			      and .containerID != $old_container_id
			      and (.containerID | test("^containerd://[0-9a-f]{64}$")))) | length) == 1
			  )
			' >"$destination" 2>"$error_file"; then
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

control_identity() {
	pod_file=$1
	jq -er '[
	  .metadata.name,
	  .metadata.uid,
	  (.status.containerStatuses[] | select(.name == "control") | .containerID | sub("^containerd://"; "")),
	  (.status.containerStatuses[] | select(.name == "control") | .restartCount | tostring),
	  (.status.containerStatuses[] | select(.name == "control") | .state.running.startedAt)
	] | join("|")' "$pod_file"
}

restore_fault_config() {
	[ -f "$temporary/control-runtime-before.json" ] || return 0
	baseline_uid=$(jq -er '.metadata.uid' "$temporary/control-runtime-before.json") || return 1
	current=$($kubectl_bin get configmap vela-lab-control-runtime --namespace "$namespace" -o json) || return 1
	[ "$(printf '%s\n' "$current" | jq -er '.metadata.uid')" = "$baseline_uid" ] || return 1
	value=$(printf '%s\n' "$current" | jq -r '.data.VELA_LAB_CONSUMER_FAULT_PHASE // ""')
	case "$value" in
		"") return 0 ;;
		"$fault_phase") ;;
		*) return 1 ;;
	esac
	resource_version=$(printf '%s\n' "$current" | jq -er '.metadata.resourceVersion') || return 1
	patch=$(jq -n \
		--arg uid "$baseline_uid" \
		--arg resource_version "$resource_version" \
		--arg value "$fault_phase" '[
		  {op:"test",path:"/metadata/uid",value:$uid},
		  {op:"test",path:"/metadata/resourceVersion",value:$resource_version},
		  {op:"test",path:"/data/VELA_LAB_CONSUMER_FAULT_PHASE",value:$value},
		  {op:"remove",path:"/data/VELA_LAB_CONSUMER_FAULT_PHASE"}
		]')
	$kubectl_bin patch configmap vela-lab-control-runtime --namespace "$namespace" \
		--type=json -p "$patch" >/dev/null
	current=$($kubectl_bin get configmap vela-lab-control-runtime --namespace "$namespace" -o json) || return 1
	[ "$(printf '%s\n' "$current" | jq -r '.data.VELA_LAB_CONSUMER_FAULT_PHASE // ""')" = "" ]
}

apply_fault_phase() {
	$kubectl_bin get configmap vela-lab-control-runtime --namespace "$namespace" \
		-o json >"$temporary/control-runtime-before.json"
	jq -e '
	  .metadata.name == "vela-lab-control-runtime"
	  and .metadata.namespace == "vela-lab"
	  and (.metadata.uid | test("^[0-9a-f-]{36}$"))
	  and (.data | type == "object")
	  and (.data | has("VELA_LAB_CONSUMER_FAULT_PHASE") | not)
	' "$temporary/control-runtime-before.json" >/dev/null ||
		fail "control runtime ConfigMap is not an approved baseline"
	uid=$(jq -er '.metadata.uid' "$temporary/control-runtime-before.json")
	resource_version=$(jq -er '.metadata.resourceVersion' "$temporary/control-runtime-before.json")
	patch=$(jq -n \
		--arg uid "$uid" \
		--arg resource_version "$resource_version" \
		--arg value "$fault_phase" '[
		  {op:"test",path:"/metadata/uid",value:$uid},
		  {op:"test",path:"/metadata/resourceVersion",value:$resource_version},
		  {op:"add",path:"/data/VELA_LAB_CONSUMER_FAULT_PHASE",value:$value}
		]')
	$kubectl_bin patch configmap vela-lab-control-runtime --namespace "$namespace" \
		--type=json -p "$patch" >/dev/null
	$kubectl_bin get configmap vela-lab-control-runtime --namespace "$namespace" \
		-o json >"$temporary/control-runtime-fault-enabled.json"
	jq -e --arg uid "$uid" --arg phase "$fault_phase" '
	  .metadata.uid == $uid and .data.VELA_LAB_CONSUMER_FAULT_PHASE == $phase
	' "$temporary/control-runtime-fault-enabled.json" >/dev/null ||
		fail "Consumer fault-phase ConfigMap mutation did not converge"
	printf 'applied_at=%s phase=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$fault_phase" \
		>"$temporary/FAULT_PHASE_APPLIED"
}

reload_control() {
	before_file=$1
	after_file=$2
	identity=$(control_identity "$before_file") || return 1
	name=$(printf '%s\n' "$identity" | cut -d '|' -f 1)
	uid=$(printf '%s\n' "$identity" | cut -d '|' -f 2)
	delete_pod_by_uid "$name" "$uid" 30 || return 1
	wait_ready_control_pod "$after_file" "$uid"
}

replace_control_pod_for_recovery() {
	before_file=$1
	after_file=$2
	error_file=$before_file.error
	iteration=0
	while [ "$iteration" -lt 120 ]; do
		heartbeat
		pods=$($kubectl_bin get pods --namespace "$namespace" \
			-l app.kubernetes.io/name=vela-lab-control -o json) || return 1
		if printf '%s\n' "$pods" | jq -e \
			--arg image "$expected_control_image" \
			--arg node "$control_node" '
		  [.items[] | select(.metadata.deletionTimestamp == null)] as $active
		  | select(($active | length) == 1)
		  | $active[0]
		  | select(
		      .spec.nodeName == $node
		      and (.spec.containers | map(select(
		        .name == "control" and .image == $image)) | length) == 1
		    )
		' >"$before_file" 2>"$error_file"; then
			rm -f -- "$error_file"
			break
		fi
		[ ! -s "$error_file" ] || return 1
		rm -f -- "$error_file"
		iteration=$((iteration + 1))
		sleep 1
	done
	[ -s "$before_file" ] || return 1
	name=$(jq -er '.metadata.name' "$before_file") || return 1
	uid=$(jq -er '.metadata.uid' "$before_file") || return 1
	delete_pod_by_uid "$name" "$uid" 30 || return 1
	wait_ready_control_pod "$after_file" "$uid"
}

verify_fault_runtime_cleared() {
	pod_file=$1
	output_file=$2
	current=$($kubectl_bin get configmap vela-lab-control-runtime --namespace "$namespace" -o json) || return 1
	[ "$(printf '%s\n' "$current" | jq -r '.data.VELA_LAB_CONSUMER_FAULT_PHASE // ""')" = "" ] || return 1
	jq -e '
	  .metadata.deletionTimestamp == null
	  and any(.spec.containers[];
	    .name == "control"
	    and ((.env // []) | all(.name != "VELA_LAB_CONSUMER_FAULT_PHASE"))
	    and any(.envFrom[]?; .configMapRef.name == "vela-lab-control-runtime"))
	' "$pod_file" >/dev/null || return 1
	name=$(jq -er '.metadata.name' "$pod_file") || return 1
	if $kubectl_bin exec --namespace "$namespace" "pod/$name" --container control -- \
		/usr/local/bin/vela-control --lab-read-consumer-fault-marker \
		>"$output_file" 2>&1; then
		return 1
	fi
	grep -F 'inspect lab consumer fault marker' "$output_file" >/dev/null &&
		grep -F 'no such file or directory' "$output_file" >/dev/null
}

warm_pod_json() {
	name=$1
	jq -n \
		--arg name "$name" \
		--arg namespace "$namespace" \
		--arg node "$control_node" \
		--arg image "$expected_runner_image" '
	  {
	    apiVersion:"v1", kind:"Pod",
	    metadata:{
	      name:$name, namespace:$namespace,
	      labels:{
	        "app.kubernetes.io/name":"vela-lab-consumer-fault-image-warm",
	        "app.kubernetes.io/component":"lab-rehearsal",
	        "vela.ai/environment":"non-production-lab"
	      }
	    },
	    spec:{
	      automountServiceAccountToken:false,
	      nodeName:$node,
	      restartPolicy:"Never",
	      containers:[{
	        name:"image-warm", image:$image, imagePullPolicy:"IfNotPresent",
	        command:["python3","-c"], args:["print(\"image_warm=PASS\")"],
	        resources:{requests:{cpu:"10m",memory:"16Mi"},limits:{cpu:"100m",memory:"64Mi"}},
	        securityContext:{
	          runAsNonRoot:true, runAsUser:10001, runAsGroup:10001,
	          allowPrivilegeEscalation:false, readOnlyRootFilesystem:true,
	          capabilities:{drop:["ALL"]}, seccompProfile:{type:"RuntimeDefault"}
	        },
	        volumeMounts:[{name:"tmp",mountPath:"/tmp"}]
	      }],
	      volumes:[{name:"tmp",emptyDir:{}}]
	    }
	  }
	'
}

warm_fault_image() {
	name=vela-lab-consumer-fault-image-warm-$(date +%s)-$$
	warm_pod_json "$name" >"$temporary/$name.json"
	$kubectl_bin create -f "$temporary/$name.json" >/dev/null
	pod=$($kubectl_bin get pod --namespace "$namespace" "$name" -o json)
	uid=$(printf '%s\n' "$pod" | jq -er '.metadata.uid')
	if ! $kubectl_bin wait pod/"$name" --namespace "$namespace" \
		--for=jsonpath='{.status.phase}'=Succeeded --timeout=120s >/dev/null; then
		$kubectl_bin describe pod/"$name" --namespace "$namespace" >"$temporary/image-warm-describe.txt" 2>&1 || true
		delete_pod_by_uid "$name" "$uid" 0 >/dev/null 2>&1 || true
		return 1
	fi
	$kubectl_bin logs pod/"$name" --namespace "$namespace" >"$temporary/image-warm.log"
	grep -Fx 'image_warm=PASS' "$temporary/image-warm.log" >/dev/null || return 1
	delete_pod_by_uid "$name" "$uid" 0
}

fault_script() {
	cat <<'EOF'
set -eu

python3 - "$EXPECTED_CONTAINER_ID" <<'PY'
import datetime
import os
import select
import signal
import sys

container_id = sys.argv[1]
if len(container_id) != 64 or any(character not in "0123456789abcdef" for character in container_id):
    raise RuntimeError("expected control container identity is invalid")
if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
    raise RuntimeError("fault image does not support pidfd signaling")


def process_start_ticks(pid):
    stat = open(f"/proc/{pid}/stat", encoding="utf-8").read()
    closing_paren = stat.rfind(")")
    if closing_paren < 0:
        raise RuntimeError("invalid process stat record")
    return stat[closing_paren + 2 :].split()[19]


def candidates():
    matches = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        try:
            cgroup = open(f"/proc/{pid}/cgroup", encoding="utf-8").read()
            if container_id not in cgroup:
                continue
            command = open(f"/proc/{pid}/cmdline", "rb").read().split(b"\0")[0]
            if command != b"/usr/local/bin/vela-control":
                continue
            status = open(f"/proc/{pid}/status", encoding="utf-8").read().splitlines()
            uid_line = next((line for line in status if line.startswith("Uid:")), "")
            if uid_line.split()[1:] != ["10001", "10001", "10001", "10001"]:
                raise RuntimeError("control process has an unexpected UID")
            matches.append((pid, process_start_ticks(pid), cgroup))
        except (FileNotFoundError, ProcessLookupError):
            continue
    return matches


matches = candidates()
if len(matches) != 1:
    raise RuntimeError(f"expected exactly one control process, found {len(matches)}")
pid, start_ticks, cgroup = matches[0]
pidfd = os.pidfd_open(pid)
try:
    confirmed = candidates()
    if len(confirmed) != 1 or confirmed[0][0] != pid or confirmed[0][1] != start_ticks:
        raise RuntimeError("control process identity changed before signal")
    signal_at = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    print(
        f"signal_armed=SIGKILL container_id={container_id} pid={pid} "
        f"start_ticks={start_ticks} signal_at={signal_at}",
        flush=True,
    )
    signal.pidfd_send_signal(pidfd, signal.SIGKILL)
    poller = select.poll()
    poller.register(pidfd, select.POLLIN)
    if not poller.poll(30_000):
        raise RuntimeError("control process did not exit after SIGKILL")
finally:
    os.close(pidfd)

print(
    "schema=vela-lab-control-consumer-crash-v1 signal=SIGKILL "
    f"signal_transport=pidfd_send_signal container_id={container_id} "
    f"old_pid={pid} old_start_ticks={start_ticks} signal_at={signal_at} "
    "production_gates=0/9"
)
PY
EOF
}

fault_pod_json() {
	name=$1
	control_pod_name=$2
	control_pod_uid=$3
	container_id=$4
	rehearsal_job_id=$5
	printf '%s\n' "$control_pod_uid" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' ||
		fail "fault Pod control Pod identity is invalid"
	printf '%s\n' "$container_id" | grep -Eq '^[0-9a-f]{64}$' || fail "fault Pod container identity is invalid"
	printf '%s\n' "$rehearsal_job_id" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' ||
		fail "fault Pod rehearsal Job identity is invalid"
	script=$(fault_script)
	jq -n \
		--arg name "$name" \
		--arg namespace "$namespace" \
		--arg node "$control_node" \
		--arg image "$expected_runner_image" \
		--arg script "$script" \
		--arg container_id "$container_id" \
		--arg control_pod_name "$control_pod_name" \
		--arg control_pod_uid "$control_pod_uid" \
		--arg rehearsal_job_id "$rehearsal_job_id" '
	  {
	    apiVersion:"v1", kind:"Pod",
	    metadata:{
	      name:$name, namespace:$namespace,
	      labels:{
	        "app.kubernetes.io/name":"vela-lab-consumer-control-fault",
	        "app.kubernetes.io/component":"lab-rehearsal",
	        "vela.ai/environment":"non-production-lab"
	      },
	      annotations:{
	        "vela.ai/rehearsal-job-id":$rehearsal_job_id,
	        "vela.ai/target-pod-name":$control_pod_name,
	        "vela.ai/target-pod-uid":$control_pod_uid
	      }
	    },
	    spec:{
	      automountServiceAccountToken:false,
	      hostPID:true,
	      nodeName:$node,
	      restartPolicy:"Never",
	      containers:[{
	        name:"fault-injector", image:$image, imagePullPolicy:"IfNotPresent",
	        command:["/bin/sh","-ec"], args:[$script],
	        env:[{name:"EXPECTED_CONTAINER_ID",value:$container_id}],
	        resources:{requests:{cpu:"10m",memory:"32Mi"},limits:{cpu:"100m",memory:"64Mi"}},
	        securityContext:{
	          runAsUser:0, runAsGroup:0,
	          allowPrivilegeEscalation:false, readOnlyRootFilesystem:true,
	          capabilities:{drop:["ALL"],add:["KILL"]},
	          appArmorProfile:{type:"Unconfined"},
	          seccompProfile:{type:"RuntimeDefault"}
	        },
	        volumeMounts:[{name:"tmp",mountPath:"/tmp"}]
	      }],
	      volumes:[{name:"tmp",emptyDir:{}}]
	    }
	  }
	'
}

persist_fault_pod_identity() {
	[ -n "$fault_pod_name" ] || return 1
	pod=$($kubectl_bin get pod --namespace "$namespace" "$fault_pod_name" -o json) || return 1
	printf '%s\n' "$pod" | jq -e \
		--arg name "$fault_pod_name" \
		--arg node "$control_node" \
		--arg image "$expected_runner_image" \
		--arg job_id "$job_id" '
	  .metadata.name == $name
	  and .metadata.namespace == "vela-lab"
	  and (.metadata.uid | test("^[0-9a-f-]{36}$"))
	  and .metadata.labels["app.kubernetes.io/name"] == "vela-lab-consumer-control-fault"
	  and .metadata.annotations["vela.ai/rehearsal-job-id"] == $job_id
	  and .spec.nodeName == $node
	  and .spec.hostPID == true
	  and .spec.automountServiceAccountToken == false
	  and .spec.restartPolicy == "Never"
	  and (.spec.containers | length) == 1
	  and .spec.containers[0].name == "fault-injector"
	  and .spec.containers[0].image == $image
	  and .spec.containers[0].imagePullPolicy == "IfNotPresent"
	  and .spec.containers[0].securityContext.runAsUser == 0
	  and .spec.containers[0].securityContext.runAsGroup == 0
	  and .spec.containers[0].securityContext.allowPrivilegeEscalation == false
	  and .spec.containers[0].securityContext.readOnlyRootFilesystem == true
	  and .spec.containers[0].securityContext.capabilities.drop == ["ALL"]
	  and .spec.containers[0].securityContext.capabilities.add == ["KILL"]
	  and .spec.containers[0].securityContext.appArmorProfile.type == "Unconfined"
	  and .spec.containers[0].securityContext.seccompProfile.type == "RuntimeDefault"
	' >/dev/null || return 1
	fault_pod_uid=$(printf '%s\n' "$pod" | jq -er '.metadata.uid') || return 1
	printf 'pod=%s\nuid=%s\njob_id=%s\n' "$fault_pod_name" "$fault_pod_uid" "$job_id" \
		>"$temporary/fault-pod-identity.txt"
}

neutralize_fault_pod() {
	if [ -z "$fault_pod_name" ] && [ -f "$temporary/fault-pod-name.txt" ]; then
		fault_pod_name=$(cat "$temporary/fault-pod-name.txt")
	fi
	[ -n "$fault_pod_name" ] || return 0
	if [ -z "$fault_pod_uid" ] && [ -f "$temporary/fault-pod-identity.txt" ]; then
		fault_pod_uid=$(sed -n 's/^uid=//p' "$temporary/fault-pod-identity.txt")
	fi
	current=$($kubectl_bin get pod --namespace "$namespace" "$fault_pod_name" --ignore-not-found -o json) || return 1
	[ -n "$current" ] || return 0
	[ -n "$fault_pod_uid" ] || persist_fault_pod_identity || return 1
	delete_pod_by_uid "$fault_pod_name" "$fault_pod_uid" 0
}

run_fault_pod() {
	control_file=$1
	identity=$(control_identity "$control_file") || return 1
	control_pod_name=$(printf '%s\n' "$identity" | cut -d '|' -f 1)
	control_pod_uid=$(printf '%s\n' "$identity" | cut -d '|' -f 2)
	container_id=$(printf '%s\n' "$identity" | cut -d '|' -f 3)
	fault_pod_name=vela-lab-consumer-control-fault-$(date +%s)-$$
	printf '%s\n' "$fault_pod_name" >"$temporary/fault-pod-name.txt"
	fault_pod_json "$fault_pod_name" "$control_pod_name" "$control_pod_uid" \
		"$container_id" "$job_id" >"$temporary/$fault_pod_name.json"
	$kubectl_bin create -f "$temporary/$fault_pod_name.json" >/dev/null || return 1
	persist_fault_pod_identity || return 1
	heartbeat
	if ! $kubectl_bin wait pod/"$fault_pod_name" --namespace "$namespace" \
		--for=jsonpath='{.status.phase}'=Succeeded --timeout=60s >/dev/null; then
		$kubectl_bin describe pod/"$fault_pod_name" --namespace "$namespace" \
			>"$temporary/fault-pod-describe.txt" 2>&1 || true
		$kubectl_bin logs pod/"$fault_pod_name" --namespace "$namespace" \
			>"$temporary/fault-injection.log" 2>&1 || true
		return 1
	fi
	$kubectl_bin logs pod/"$fault_pod_name" --namespace "$namespace" \
		>"$temporary/fault-injection.log"
	neutralize_fault_pod
}

recover_environment() {
	log_file=$1
	[ "$recovering" = false ] || return 0
	recovering=true
	recovery_result=0
	exec 9>"$temporary/recovery.lock"
	if ! flock -n 9; then
		printf 'another recovery owner holds the environment lock\n' >>"$log_file"
		recovering=false
		exec 9>&-
		return 1
	fi
	if [ -z "$job_id" ] && [ -f "$temporary/job-id.txt" ]; then
		job_id=$(cat "$temporary/job-id.txt")
	fi
	if [ -z "$job_resource" ] && [ -f "$temporary/kubernetes-job.txt" ]; then
		job_resource=$(cat "$temporary/kubernetes-job.txt")
	fi
	neutralize_fault_pod >>"$log_file" 2>&1 || recovery_result=1
	restore_fault_config >>"$log_file" 2>&1 || recovery_result=1
	if [ -f "$temporary/FAULT_PHASE_APPLIED" ] && [ ! -f "$temporary/FAULT_PHASE_CLEARED" ]; then
		if replace_control_pod_for_recovery \
			"$temporary/recovery-control-before.json" \
			"$temporary/recovery-control-after.json" >>"$log_file" 2>&1 &&
			verify_fault_runtime_cleared \
				"$temporary/recovery-control-after.json" \
				"$temporary/recovery-fault-runtime-cleared.txt" >>"$log_file" 2>&1; then
			printf 'reloaded_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
				>"$temporary/FAULT_PHASE_CLEARED"
		else
			recovery_result=1
		fi
	fi
	$kubectl_bin rollout status deployment/vela-lab-control --namespace "$namespace" \
		--timeout=120s >>"$log_file" 2>&1 || recovery_result=1
	if [ -n "$job_id" ]; then
		iteration=0
		while [ "$iteration" -lt 360 ]; do
			state=$(query_database "SELECT state FROM jobs WHERE id = '$job_id'::uuid;" 2>>"$log_file" || true)
			case "$state" in SUCCEEDED | FAILED | CANCELED) break ;; esac
			iteration=$((iteration + 1))
			sleep 1
		done
		case "$state" in SUCCEEDED | FAILED | CANCELED) ;; *) recovery_result=1 ;; esac
	fi
	iteration=0
	while [ "$iteration" -lt 120 ]; do
		pending_outbox=$(query_database \
			'SELECT count(*) FROM outbox_events WHERE published_at IS NULL OR claimed_by IS NOT NULL OR claim_token IS NOT NULL OR claim_expires_at IS NOT NULL;' \
			2>>"$log_file" || printf unknown)
		[ "$pending_outbox" = 0 ] && break
		iteration=$((iteration + 1))
		sleep 1
	done
	[ "$pending_outbox" = 0 ] || recovery_result=1
	[ "$(query_database 'SELECT count(*) FROM attempt_leases WHERE revoked_at IS NULL;' 2>>"$log_file" || printf unknown)" = 0 ] ||
		recovery_result=1
	[ "$(query_database "SELECT count(*) FROM workers WHERE id IN ('$worker1_id'::uuid, '$worker2_id'::uuid) AND lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY';" 2>>"$log_file" || printf unknown)" = 2 ] ||
		recovery_result=1
	flock -u 9
	exec 9>&-
	recovering=false
	return "$recovery_result"
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
	cleanup_result=$1
	trap - EXIT HUP INT TERM
	stop_nats_port_forward
	if [ "$committed" != true ] && [ -n "$temporary" ] && [ -d "$temporary" ]; then
		[ "$cleanup_result" -ne 0 ] || cleanup_result=1
		printf 'status=INCOMPLETE production_gates=0/9\n' >"$temporary/STATUS"
		if recover_environment "$temporary/cleanup.log"; then
			disarm_watchdog
		else
			printf 'consumer-post-db-pre-ack-crash: immediate recovery incomplete; armed watchdog retained\n' >&2
		fi
		printf 'consumer-post-db-pre-ack-crash: diagnostic receipt preserved at %s\n' "$temporary" >&2
	fi
	exit "$cleanup_result"
}
trap 'cleanup $?' EXIT
trap 'cleanup 129' HUP
trap 'cleanup 130' INT
trap 'cleanup 143' TERM

if [ "${1:-}" = --render-fault-pod ]; then
	trap - EXIT HUP INT TERM
	expected_control_image=${2:-}
	printf '%s\n' "$expected_control_image" | grep -Eq '^10\.1\.200\.17:5443/vela-lab/vela-control@sha256:[0-9a-f]{64}$' ||
		fail "render mode requires the immutable private control image"
	job_id=00000000-0000-0000-0000-000000000000
	fault_pod_json vela-lab-consumer-control-fault-render vela-lab-control-render \
		00000000-0000-0000-0000-000000000000 \
		0000000000000000000000000000000000000000000000000000000000000000 \
		"$job_id"
	exit 0
fi

if [ "${1:-}" = --render-warm-pod ]; then
	trap - EXIT HUP INT TERM
	warm_pod_json vela-lab-consumer-fault-image-warm-render
	exit 0
fi

if [ "${1:-}" = --watchdog ]; then
	trap - EXIT HUP INT TERM
	expected_control_image=${2:-}
	manifests=${3:-}
	temporary=${4:-}
	watchdog_marker=${5:-}
	[ -n "$expected_control_image" ] && [ -d "$temporary" ] && [ -f "$watchdog_marker" ] || exit 0
	watchdog_heartbeat=$temporary/WATCHDOG_HEARTBEAT
	export KUBECONFIG="$kubeconfig"
	sleep 300
	[ -f "$watchdog_marker" ] || exit 0
	while [ -f "$watchdog_marker" ]; do
		now=$(date +%s)
		updated=$(stat -c %Y "$watchdog_heartbeat" 2>/dev/null || printf 0)
		[ $((now - updated)) -lt 240 ] || break
		sleep 60
	done
	[ -f "$watchdog_marker" ] || exit 0
	printf 'started_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
		>"$temporary/WATCHDOG_RECOVERY_STARTED"
	recover_environment "$temporary/watchdog-recovery.log"
	printf 'status=RECOVERED_BY_WATCHDOG production_gates=0/9\n' \
		>"$temporary/WATCHDOG_STATUS"
	exit 0
fi

expected_control_image=${1:-}
manifests=${2:-}
output=${3:-}
apply=${4:-}
[ "$apply" = --apply ] ||
	fail "usage: $0 <control-image@sha256:digest> <rendered-manifest-directory> <new-output-directory> --apply"
[ "$(id -u)" -eq 0 ] || fail "run as root to read the RKE2 kubeconfig"
[ "$(hostname)" = marslab-server ] || fail "run only on the lab control host"
printf '%s\n' "$expected_control_image" | grep -Eq '^10\.1\.200\.17:5443/vela-lab/vela-control@sha256:[0-9a-f]{64}$' ||
	fail "control image must use the fixed private repository and an immutable digest"
case "$manifests" in /*) ;; *) fail "manifest directory must be absolute" ;; esac
case "$output" in /*) ;; *) fail "output directory must be absolute" ;; esac
[ ! -e "$output" ] && [ ! -L "$output" ] || fail "output path already exists"
[ -f "$manifests/60-smoke.yaml" ] || fail "60-smoke.yaml is absent"
[ -x "$kubectl_bin" ] || fail "$kubectl_bin is missing"
[ -r "$kubeconfig" ] || fail "$kubeconfig is unreadable"
for command in curl date flock jq sha256sum; do
	command -v "$command" >/dev/null 2>&1 || fail "$command is required"
done
export KUBECONFIG="$kubeconfig"
temporary=$(mktemp -d "$(dirname "$output")/.vela-lab-consumer-post-db-pre-ack-crash.XXXXXX")
chmod 0700 "$temporary"
load_tool_identity
validate_previous_scenario_receipt

$kubectl_bin rollout status deployment/vela-lab-control --namespace "$namespace" --timeout=60s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-worker-agent-1 --namespace "$namespace" --timeout=60s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-worker-agent-2 --namespace "$namespace" --timeout=60s >/dev/null
[ "$($kubectl_bin get pods --namespace "$namespace" -l app.kubernetes.io/name=vela-lab-consumer-control-fault -o json | jq '[.items[] | select(.status.phase != "Succeeded" and .status.phase != "Failed")] | length')" = 0 ] ||
	fail "an active consumer control fault Pod already exists"

global_before=$(query_database "
SELECT
  (SELECT count(*) FROM attempt_leases WHERE revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts),
  (SELECT count(*) FROM jobs WHERE state IN ('QUEUED','ASSIGNED','RUNNING','FINALIZING','RETRY_WAIT','CANCELING')),
  (SELECT count(*) FROM workers WHERE id IN ('$worker1_id'::uuid, '$worker2_id'::uuid) AND lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY'),
  (SELECT count(*) FROM outbox_events WHERE published_at IS NULL OR claimed_by IS NOT NULL OR claim_token IS NOT NULL OR claim_expires_at IS NOT NULL);")
printf '%s\n' "$global_before" >"$temporary/global-before.txt"
[ "$global_before" = '0|0|0|2|0' ] ||
	fail "global preflight does not preserve the idle published-Outbox boundary"

$kubectl_bin get nodes -o json >"$temporary/nodes-before.json"
jq -e '
  (.items | length) == 3
  and all(.items[]; any(.status.conditions[]; .type == "Ready" and .status == "True"))
  and any(.items[]; .metadata.name == "vela-lab-control-1" and (.status.allocatable["nvidia.com/gpu"] // "0") == "0")
' "$temporary/nodes-before.json" >/dev/null || fail "three-node Ready boundary is absent"
wait_ready_control_pod "$temporary/control-before.json" || fail "control Pod is not uniquely Ready"
warm_fault_image || fail "control-node fault image warm-up failed"

watchdog_marker=$temporary/WATCHDOG_ARMED
watchdog_heartbeat=$temporary/WATCHDOG_HEARTBEAT
printf 'armed_at=%s timeout_seconds=300\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$watchdog_marker"
heartbeat
nohup "$0" --watchdog "$expected_control_image" "$manifests" "$temporary" "$watchdog_marker" \
	>"$temporary/watchdog.log" 2>&1 &
watchdog_pid=$!
printf '%s\n' "$watchdog_pid" >"$temporary/watchdog.pid"

apply_fault_phase
reload_control "$temporary/control-before.json" "$temporary/control-fault-enabled.json" ||
	fail "control Pod did not reload the consumer fault phase"
fault_identity=$(control_identity "$temporary/control-fault-enabled.json")
fault_uid=$(printf '%s\n' "$fault_identity" | cut -d '|' -f 2)
fault_started_at=$(printf '%s\n' "$fault_identity" | cut -d '|' -f 5)
started_epoch=$(date -d "$fault_started_at" +%s)
age=$(( $(date +%s) - started_epoch ))
[ "$age" -ge 0 ] && [ "$age" -le 20 ] || fail "fault-enabled Consumer control Pod is outside the bounded startup window"
sleep 2
heartbeat
[ "$(query_database 'SELECT count(*) FROM outbox_events WHERE published_at IS NULL OR claim_token IS NOT NULL;')" = 0 ] ||
	fail "fault-enabled Consumer startup did not drain the baseline Outbox"
capture_nats_stream_state "$temporary/nats-stream-before-job.json" ||
	fail "could not capture the pre-Job VELA_EVENTS stream state"

database_started_at=$(query_database 'SELECT clock_timestamp();')
printf '%s\n' "$database_started_at" >"$temporary/database-started-at.txt"
job_resource=$($kubectl_bin create -f "$manifests/60-smoke.yaml" -o name)
case "$job_resource" in job.batch/vela-lab-smoke-*) ;; *) fail "unexpected smoke Job identity $job_resource" ;; esac
printf '%s\n' "$job_resource" >"$temporary/kubernetes-job.txt"

iteration=0
while [ "$iteration" -lt 30 ]; do
	heartbeat
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

control_pod_name=$(printf '%s\n' "$fault_identity" | cut -d '|' -f 1)
iteration=0
while [ "$iteration" -lt 30 ]; do
	heartbeat
	if $kubectl_bin exec --namespace "$namespace" "pod/$control_pod_name" --container control -- \
		/usr/local/bin/vela-control --lab-read-consumer-fault-marker \
		>"$temporary/consumer-marker.json" 2>"$temporary/consumer-marker-read-error.txt"; then
		break
	fi
	iteration=$((iteration + 1))
	sleep 1
done
[ -s "$temporary/consumer-marker.json" ] || fail "Consumer post-DB-pre-Ack marker was not observed through kubectl exec"
rm -f -- "$temporary/consumer-marker-read-error.txt"
jq -e --arg phase "$fault_phase" '
  (keys | sort) == ([
    "schema", "phase", "subject", "event_id", "stream", "consumer",
    "stream_sequence", "consumer_sequence", "num_delivered", "inbox_committed"
  ] | sort)
  and .schema == "vela-lab-consumer-fault-marker-v1"
  and .phase == $phase
  and .subject == "vela.events.job.ready"
  and (.event_id | test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"))
  and .stream == "VELA_EVENTS"
  and .consumer == "VELA_SCHEDULER"
  and (.stream_sequence | type == "number" and . >= 1 and floor == .)
  and (.consumer_sequence | type == "number" and . >= 1 and floor == .)
  and .num_delivered == 1
  and .inbox_committed == true
' "$temporary/consumer-marker.json" >/dev/null || fail "Consumer post-DB-pre-Ack marker is invalid"
marker_event_id=$(jq -er '.event_id' "$temporary/consumer-marker.json")
marker_stream_sequence=$(jq -er '.stream_sequence' "$temporary/consumer-marker.json")
marker_consumer_sequence=$(jq -er '.consumer_sequence' "$temporary/consumer-marker.json")

consumer_database_before=$(query_database "
SELECT job.state,
  (SELECT count(*) FROM attempts WHERE job_id = job.id),
  (SELECT count(*) FROM attempt_leases AS lease JOIN attempts AS attempt ON attempt.id = lease.attempt_id WHERE attempt.job_id = job.id AND lease.revoked_at IS NULL),
  (SELECT count(*) FROM inbox_receipts AS receipt
   WHERE receipt.consumer_name = 'scheduler' AND receipt.event_id = outbox.event_id
     AND receipt.aggregate_id = job.id AND receipt.event_type = 'job.ready'),
  outbox.event_id, outbox.event_type, outbox.publish_attempts,
  outbox.published_at IS NOT NULL,
  outbox.claimed_by IS NULL AND outbox.claim_token IS NULL AND outbox.claim_expires_at IS NULL,
  outbox.broker_stream, outbox.broker_sequence
FROM jobs AS job JOIN outbox_events AS outbox ON outbox.aggregate_id = job.id
WHERE job.id = '$job_id'::uuid AND outbox.event_type = 'job.ready';")
printf '%s\n' "$consumer_database_before" >"$temporary/consumer-database-before-crash.txt"
case "$consumer_database_before" in
	"ASSIGNED|1|1|1|$marker_event_id|job.ready|1|t|t|VELA_EVENTS|$marker_stream_sequence" | \
	"RUNNING|1|1|1|$marker_event_id|job.ready|1|t|t|VELA_EVENTS|$marker_stream_sequence" | \
	"FINALIZING|1|1|1|$marker_event_id|job.ready|1|t|t|VELA_EVENTS|$marker_stream_sequence" | \
	"SUCCEEDED|1|0|1|$marker_event_id|job.ready|1|t|t|VELA_EVENTS|$marker_stream_sequence") ;;
	*) fail "database did not remain at the exact post-commit pre-ack consumer boundary" ;;
esac
capture_nats_stream_state "$temporary/nats-stream-before-crash.json" ||
	fail "could not capture the pre-crash VELA_EVENTS stream state"
jq -e \
	--argjson stream_sequence "$marker_stream_sequence" \
	--argjson consumer_sequence "$marker_consumer_sequence" '
  .consumer.delivered.stream_seq == $stream_sequence
  and .consumer.delivered.consumer_seq == $consumer_sequence
  and .consumer.ack_floor.stream_seq < $stream_sequence
  and .consumer.ack_floor.consumer_seq < $consumer_sequence
  and .consumer.num_ack_pending == 1
' "$temporary/nats-stream-before-crash.json" >/dev/null ||
	fail "Scheduler consumer did not retain exactly one unacked first delivery"
printf 'schema=vela-lab-consumer-pre-ack-check-v1 event_id=%s stream_sequence=%s consumer_sequence=%s num_delivered=1 num_ack_pending=1 ack_floor_before_target=true inbox_committed=true\n' \
	"$marker_event_id" "$marker_stream_sequence" "$marker_consumer_sequence" \
	>"$temporary/consumer-pre-ack-check.txt"
query_database "
SELECT jsonb_build_object(
  'captured_at', clock_timestamp(),
  'job_id', job.id, 'job_state', job.state, 'job_version', job.version,
  'current_fence', job.current_fence,
  'attempt_count', (SELECT count(*) FROM attempts WHERE job_id = job.id),
  'active_lease_count', (SELECT count(*) FROM attempt_leases AS lease JOIN attempts AS attempt ON attempt.id = lease.attempt_id WHERE attempt.job_id = job.id AND lease.revoked_at IS NULL),
  'visible_completion_count', (SELECT count(*) FROM visible_completions WHERE job_id = job.id),
  'posted_charge_count', (SELECT count(*) FROM charges WHERE job_id = job.id AND reason = 'VISIBLE_COMPLETION' AND state = 'POSTED'),
  'artifact_count', (SELECT count(*) FROM artifacts WHERE job_id = job.id),
  'scheduler_inbox_receipts', (SELECT jsonb_agg(jsonb_build_object(
    'consumer_name', consumer_name, 'event_id', event_id,
    'aggregate_version', aggregate_version, 'event_type', event_type,
    'consumed_at', consumed_at
  )) FROM inbox_receipts WHERE consumer_name = 'scheduler' AND event_id = '$marker_event_id'::uuid),
  'outbox', (SELECT jsonb_agg(jsonb_build_object(
    'event_id', event_id, 'event_type', event_type, 'publish_attempts', publish_attempts,
    'claimed_by', claimed_by, 'claim_token', claim_token,
    'claim_expires_at', claim_expires_at, 'published_at', published_at,
    'broker_stream', broker_stream, 'broker_sequence', broker_sequence
  ) ORDER BY aggregate_version, event_type) FROM outbox_events WHERE aggregate_id = job.id)
)::text FROM jobs AS job WHERE job.id = '$job_id'::uuid;" >"$temporary/authority-before.json"

run_fault_pod "$temporary/control-fault-enabled.json" || fail "control SIGKILL injector did not complete"
fault_receipt=$(grep -E '^schema=vela-lab-control-consumer-crash-v1 signal=SIGKILL signal_transport=pidfd_send_signal container_id=[0-9a-f]{64} old_pid=[0-9]+ old_start_ticks=[0-9]+ signal_at=[^[:space:]]+ production_gates=0/9$' \
	"$temporary/fault-injection.log" || true)
[ "$(printf '%s\n' "$fault_receipt" | sed '/^$/d' | wc -l | tr -d ' ')" = 1 ] ||
	fail "control fault receipt is malformed or non-unique"
[ "$(grep -Ec '^signal_armed=SIGKILL container_id=[0-9a-f]{64} pid=[0-9]+ start_ticks=[0-9]+ signal_at=[^[:space:]]+$' "$temporary/fault-injection.log" || true)" = 1 ] ||
	fail "control signal claim is malformed or non-unique"
signal_at=$(printf '%s\n' "$fault_receipt" | sed -n 's/.* signal_at=\([^[:space:]]*\) .*/\1/p')
old_container_id=$(printf '%s\n' "$fault_receipt" | sed -n 's/.* container_id=\([0-9a-f]\{64\}\) .*/\1/p')
[ "$old_container_id" = "$(printf '%s\n' "$fault_identity" | cut -d '|' -f 3)" ] ||
	fail "fault receipt does not bind the fault-enabled consumer container"
fault_window=$(query_database "
SELECT
  '$signal_at'::timestamptz >= '$database_started_at'::timestamptz,
  '$signal_at'::timestamptz < '$database_started_at'::timestamptz + interval '90 seconds',
  floor(extract(epoch FROM ('$signal_at'::timestamptz - '$database_started_at'::timestamptz)))::bigint;")
printf '%s\n' "$fault_window" >"$temporary/fault-window.txt"
printf '%s\n' "$fault_window" | grep -Eq '^t\|t\|[0-9]+$' ||
	fail "SIGKILL did not occur inside the bounded post-DB-pre-Ack hook window"
fault_elapsed_seconds=$(printf '%s\n' "$fault_window" | cut -d '|' -f 3)
[ "$fault_elapsed_seconds" -lt 90 ] ||
	fail "SIGKILL exceeded the bounded post-DB-pre-Ack hook window"

wait_restarted_control_container "$temporary/control-after-crash.json" "$temporary/control-fault-enabled.json" ||
	fail "control did not complete the exact container restart after SIGKILL"
after_identity=$(control_identity "$temporary/control-after-crash.json")
after_uid=$(printf '%s\n' "$after_identity" | cut -d '|' -f 2)
after_container_id=$(printf '%s\n' "$after_identity" | cut -d '|' -f 3)
before_restarts=$(printf '%s\n' "$fault_identity" | cut -d '|' -f 4)
after_restarts=$(printf '%s\n' "$after_identity" | cut -d '|' -f 4)
[ "$after_uid" = "$fault_uid" ] || fail "control Pod identity changed instead of restarting its container"
[ "$after_container_id" != "$old_container_id" ] || fail "control container identity did not change after SIGKILL"
[ "$after_restarts" -eq $((before_restarts + 1)) ] || fail "control restart count did not advance exactly once"

minimum_redelivery_consumer_sequence=$((marker_consumer_sequence + 1))
iteration=0
while [ "$iteration" -lt 90 ]; do
	heartbeat
	if capture_nats_stream_state "$temporary/nats-consumer-recovery-sample.json"; then
		if jq -e \
			--argjson stream_sequence "$marker_stream_sequence" \
			--argjson minimum_consumer_sequence "$minimum_redelivery_consumer_sequence" '
          .consumer.delivered.stream_seq == $stream_sequence
          and .consumer.delivered.consumer_seq >= $minimum_consumer_sequence
          and .consumer.ack_floor.stream_seq >= $stream_sequence
          and .consumer.ack_floor.consumer_seq == .consumer.delivered.consumer_seq
          and .consumer.num_ack_pending == 0
        ' "$temporary/nats-consumer-recovery-sample.json" >/dev/null; then
			mv "$temporary/nats-consumer-recovery-sample.json" "$temporary/nats-stream-after-recovery.json"
			break
		fi
	fi
	iteration=$((iteration + 1))
	sleep 1
done
[ -s "$temporary/nats-stream-after-recovery.json" ] ||
	fail "Scheduler consumer did not confirm the redelivered target event"
redelivery_consumer_sequence=$(jq -er '.consumer.delivered.consumer_seq' "$temporary/nats-stream-after-recovery.json")
ack_floor_stream_sequence=$(jq -er '.consumer.ack_floor.stream_seq' "$temporary/nats-stream-after-recovery.json")
ack_floor_consumer_sequence=$(jq -er '.consumer.ack_floor.consumer_seq' "$temporary/nats-stream-after-recovery.json")
printf 'schema=vela-lab-consumer-redelivery-check-v1 event_id=%s stream_sequence=%s first_consumer_sequence=%s redelivery_consumer_sequence=%s delivery_count_lower_bound=2 num_ack_pending=0 ack_floor_stream_sequence=%s ack_floor_consumer_sequence=%s inbox_reapply_count=0\n' \
	"$marker_event_id" "$marker_stream_sequence" "$marker_consumer_sequence" \
	"$redelivery_consumer_sequence" "$ack_floor_stream_sequence" "$ack_floor_consumer_sequence" \
	>"$temporary/consumer-redelivery-check.txt"

consumer_database_after_ack=$(query_database "
SELECT job.state,
  (SELECT count(*) FROM attempts WHERE job_id = job.id),
  (SELECT count(*) FROM inbox_receipts AS receipt
   WHERE receipt.consumer_name = 'scheduler' AND receipt.event_id = outbox.event_id
     AND receipt.aggregate_id = job.id AND receipt.event_type = 'job.ready'),
  outbox.event_id, outbox.event_type, outbox.publish_attempts,
  outbox.published_at IS NOT NULL,
  outbox.claimed_by IS NULL AND outbox.claim_token IS NULL AND outbox.claim_expires_at IS NULL,
  outbox.broker_stream, outbox.broker_sequence
FROM jobs AS job JOIN outbox_events AS outbox ON outbox.aggregate_id = job.id
WHERE job.id = '$job_id'::uuid AND outbox.event_type = 'job.ready';")
printf '%s\n' "$consumer_database_after_ack" >"$temporary/consumer-database-after-ack.txt"
case "$consumer_database_after_ack" in
	"ASSIGNED|1|1|$marker_event_id|job.ready|1|t|t|VELA_EVENTS|$marker_stream_sequence" | \
	"RUNNING|1|1|$marker_event_id|job.ready|1|t|t|VELA_EVENTS|$marker_stream_sequence" | \
	"FINALIZING|1|1|$marker_event_id|job.ready|1|t|t|VELA_EVENTS|$marker_stream_sequence" | \
	"SUCCEEDED|1|1|$marker_event_id|job.ready|1|t|t|VELA_EVENTS|$marker_stream_sequence") ;;
	*) fail "redelivery changed the Scheduler Inbox or dispatch identity" ;;
esac

terminal=
iteration=0
while [ "$iteration" -lt 360 ]; do
	heartbeat
	terminal=$(query_database "
SELECT job.state,
  retry.attempts_started,
  (SELECT count(*) FROM attempts WHERE job_id = job.id),
  (SELECT count(*) FROM attempts WHERE job_id = job.id AND state = 'SUCCEEDED'),
  (SELECT count(*) FROM visible_completions WHERE job_id = job.id),
  (SELECT count(*) FROM charges WHERE job_id = job.id AND reason = 'VISIBLE_COMPLETION' AND state = 'POSTED'),
  (SELECT count(*) FROM artifacts WHERE job_id = job.id),
  (SELECT count(*) FROM artifacts WHERE job_id = job.id AND state = 'COMMITTED'),
  (SELECT count(*) FROM attempt_leases AS lease JOIN attempts AS attempt ON attempt.id = lease.attempt_id WHERE attempt.job_id = job.id AND lease.revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts)
FROM jobs AS job JOIN retry_runtime_states AS retry ON retry.job_id = job.id
WHERE job.id = '$job_id'::uuid;")
	printf '%s\n' "$terminal" >>"$temporary/authority-timeline.txt"
	case "$terminal" in
		'SUCCEEDED|1|1|1|1|1|2|2|0|0') break ;;
		FAILED'|'* | CANCELED'|'*) fail "application Job reached terminal failure after control crash" ;;
	esac
	iteration=$((iteration + 1))
	sleep 1
done
[ "$terminal" = 'SUCCEEDED|1|1|1|1|1|2|2|0|0' ] ||
	fail "application Job did not converge after consumer recovery"

if ! $kubectl_bin wait --namespace "$namespace" "$job_resource" --for=condition=complete --timeout=90s >/dev/null; then
	$kubectl_bin describe --namespace "$namespace" "$job_resource" >"$temporary/smoke-job-describe.txt" 2>&1 || true
	$kubectl_bin logs --namespace "$namespace" "$job_resource" >"$temporary/smoke-job.log" 2>&1 || true
	fail "smoke wrapper did not complete after application success"
fi
$kubectl_bin logs --namespace "$namespace" "$job_resource" >"$temporary/smoke-job.log"
jq -Rrc 'fromjson? | select(.status == "LAB VERIFIED")' "$temporary/smoke-job.log" >"$temporary/smoke-receipt-candidates.jsonl"
[ "$(wc -l <"$temporary/smoke-receipt-candidates.jsonl" | tr -d ' ')" = 1 ] ||
	fail "smoke wrapper emitted a non-unique LAB VERIFIED receipt"
jq -s '.[0]' "$temporary/smoke-receipt-candidates.jsonl" >"$temporary/smoke-receipt.json"
rm -f -- "$temporary/smoke-receipt-candidates.jsonl"
jq -e --arg job_id "$job_id" '
  .job_id == $job_id and .final_state == "SUCCEEDED" and .artifact_count == 2
  and (.artifact_kinds | sort) == ["THUMBNAIL","VIDEO"]
' "$temporary/smoke-receipt.json" >/dev/null || fail "smoke receipt does not match the rehearsed Job"

query_database "
SELECT jsonb_build_object(
  'job_id', job.id, 'job_state', job.state, 'current_fence', job.current_fence,
  'attempts_started', retry.attempts_started,
  'credit_reservation_state', reservation.state,
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
  'artifact_count', (SELECT count(*) FROM artifacts WHERE job_id = job.id),
  'committed_artifact_count', (SELECT count(*) FROM artifacts WHERE job_id = job.id AND state = 'COMMITTED'),
  'scheduler_inbox_receipts', (SELECT jsonb_agg(jsonb_build_object(
    'consumer_name', consumer_name, 'event_id', event_id,
    'aggregate_version', aggregate_version, 'event_type', event_type,
    'consumed_at', consumed_at
  )) FROM inbox_receipts WHERE consumer_name = 'scheduler' AND event_id = '$marker_event_id'::uuid),
  'outbox', (SELECT jsonb_agg(jsonb_build_object(
    'event_id', event_id, 'event_type', event_type, 'publish_attempts', publish_attempts,
    'published_at', published_at, 'broker_stream', broker_stream,
    'broker_sequence', broker_sequence, 'claimed_by', claimed_by
  ) ORDER BY aggregate_version, event_type) FROM outbox_events WHERE aggregate_id = job.id)
)::text
FROM jobs AS job
JOIN retry_runtime_states AS retry ON retry.job_id = job.id
JOIN credit_reservations AS reservation ON reservation.job_id = job.id
WHERE job.id = '$job_id'::uuid;" >"$temporary/authority-after.json"

query_database "
SELECT jsonb_build_object(
  'event_id', event_id, 'aggregate_version', aggregate_version,
  'event_type', event_type, 'schema_version', schema_version,
  'occurred_at', occurred_at, 'published_at', published_at,
  'broker_stream', broker_stream, 'broker_sequence', broker_sequence,
  'publish_attempts', publish_attempts,
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
  (SELECT count(*) FROM attempt_leases AS lease JOIN attempts AS attempt ON attempt.id = lease.attempt_id WHERE attempt.job_id = job.id AND lease.revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts),
  (SELECT count(*) FROM workers WHERE id IN ('$worker1_id'::uuid, '$worker2_id'::uuid) AND lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY')
FROM jobs AS job WHERE job.id = '$job_id'::uuid;")
printf '%s\n' "$measurements" >"$temporary/measurements.txt"
[ "$measurements" = '0|0|0|0|0|0|2' ] ||
	fail "final measurements do not satisfy the consumer crash lab contract"

restore_fault_config || fail "Consumer fault-phase ConfigMap could not be restored"
printf 'restored_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$temporary/FAULT_PHASE_REMOVED"
wait_ready_control_pod "$temporary/control-before-default-reload.json" ||
	fail "control was not Ready before clearing the consumer fault phase"
reload_control "$temporary/control-before-default-reload.json" "$temporary/control-default-restored.json" ||
	fail "control did not clear the consumer fault phase"
verify_fault_runtime_cleared \
	"$temporary/control-default-restored.json" \
	"$temporary/fault-runtime-cleared.txt" ||
	fail "control fault runtime or marker remained after the default reload"
printf 'reloaded_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$temporary/FAULT_PHASE_CLEARED"
$kubectl_bin get configmap vela-lab-control-runtime --namespace "$namespace" -o json \
	>"$temporary/control-runtime-restored.json"
jq -e --arg uid "$(jq -er '.metadata.uid' "$temporary/control-runtime-before.json")" '
  .metadata.uid == $uid and (.data | has("VELA_LAB_CONSUMER_FAULT_PHASE") | not)
' "$temporary/control-runtime-restored.json" >/dev/null || fail "Consumer fault-phase ConfigMap boundary was not restored"

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
  (.scenarios[] | select(.id == "consumer-post-db-pre-ack-crash")) = {
    id:"consumer-post-db-pre-ack-crash",
    status:"LAB_REHEARSAL_PASS",
    job_id:$job_id,
    started_at:$started_at,
    completed_at:$completed_at,
    fault:"CONTROL_PROCESS_SIGKILL_AFTER_SCHEDULER_DB_COMMIT_BEFORE_CONFIRMED_ACK",
    previous_receipt_sha256:$previous_receipt_sha256
  }
' "$previous_scenario_receipt/scenario-matrix.json" >"$temporary/scenario-matrix.json"
jq -e --arg job_id "$job_id" '
	  .schema == "vela-lab-fault-scenario-matrix-v1"
	  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
	  and .production_gates == "0/9"
	  and (.scenarios | length == 10)
	  and ([.scenarios[].id] | sort) == ([
	    "process-kill", "worker-control-network-partition", "node-reboot",
	    "outbox-post-commit-crash", "publisher-pre-puback-crash",
	    "publisher-post-puback-pre-mark-crash", "consumer-post-db-pre-ack-crash",
	    "assignment-post-commit-pre-response-crash", "retry-budget-exhaustion",
	    "stale-fence-late-completion"
	  ] | sort)
	  and ([.scenarios[] | select(.status == "LAB_REHEARSAL_PASS")] | length) == 7
  and any(.scenarios[]; .id == "consumer-post-db-pre-ack-crash" and .job_id == $job_id and .status == "LAB_REHEARSAL_PASS")
' "$temporary/scenario-matrix.json" >/dev/null || fail "seven-scenario matrix is invalid"

harness_sha256=$(sha256sum "$0" | awk '{print $1}')
old_pid=$(printf '%s\n' "$fault_receipt" | sed -n 's/.* old_pid=\([0-9][0-9]*\) .*/\1/p')
old_start_ticks=$(printf '%s\n' "$fault_receipt" | sed -n 's/.* old_start_ticks=\([0-9][0-9]*\) .*/\1/p')
nats_ack_floor_stream_before=$(jq -er '.consumer.ack_floor.stream_seq' "$temporary/nats-stream-before-crash.json")
nats_ack_floor_consumer_before=$(jq -er '.consumer.ack_floor.consumer_seq' "$temporary/nats-stream-before-crash.json")
nats_ack_pending_before=$(jq -er '.consumer.num_ack_pending' "$temporary/nats-stream-before-crash.json")
nats_ack_pending_after=$(jq -er '.consumer.num_ack_pending' "$temporary/nats-stream-after-recovery.json")
jq -n \
	--arg job_id "$job_id" \
	--arg started_at "$database_started_at" \
	--arg completed_at "$completed_at" \
	--arg harness_sha256 "$harness_sha256" \
	--arg previous_receipt_sha256 "$previous_scenario_receipt_sha256" \
	--arg control_image "$expected_control_image" \
	--arg container_id "$old_container_id" \
	--arg replacement_container_id "$after_container_id" \
	--arg signal_at "$signal_at" \
	--arg event_id "$marker_event_id" \
	--argjson old_pid "$old_pid" \
	--argjson old_start_ticks "$old_start_ticks" \
	--argjson fault_elapsed_seconds "$fault_elapsed_seconds" \
	--argjson stream_sequence "$marker_stream_sequence" \
	--argjson first_consumer_sequence "$marker_consumer_sequence" \
	--argjson redelivery_consumer_sequence "$redelivery_consumer_sequence" \
	--argjson ack_floor_stream_before "$nats_ack_floor_stream_before" \
	--argjson ack_floor_consumer_before "$nats_ack_floor_consumer_before" \
	--argjson ack_floor_stream_after "$ack_floor_stream_sequence" \
	--argjson ack_floor_consumer_after "$ack_floor_consumer_sequence" \
	--argjson ack_pending_before "$nats_ack_pending_before" \
	--argjson ack_pending_after "$nats_ack_pending_after" \
	--argjson restart_count "$after_restarts" '
  {
    schema:"vela-lab-consumer-post-db-pre-ack-crash-v1",
    status:"LAB_REHEARSAL_PASS",
    evidence_boundary:"NON_PRODUCTION_MOCK_REHEARSAL",
    production_gates:"0/9",
    fixed_scenarios_completed:7,
    fixed_scenarios_required:10,
    job_id:$job_id,
    started_at:$started_at,
    completed_at:$completed_at,
    harness_sha256:$harness_sha256,
    previous_receipt_sha256:$previous_receipt_sha256,
    control_image:$control_image,
    fault:{
      kind:"CONTROL_PROCESS_SIGKILL_AFTER_SCHEDULER_DB_COMMIT_BEFORE_CONFIRMED_ACK",
      signal_transport:"pidfd_send_signal",
      signal_at:$signal_at,
      container_id_before:$container_id,
      container_id_after:$replacement_container_id,
      old_pid:$old_pid,
      old_start_ticks:$old_start_ticks,
	  restart_count:$restart_count,
	  hook_timeout_seconds:120,
	  signal_bound_seconds:90,
	  signal_elapsed_seconds:$fault_elapsed_seconds
    },
    consumer:{
      event_id:$event_id,
      subject:"vela.events.job.ready",
      stream:"VELA_EVENTS",
      durable:"VELA_SCHEDULER",
      stream_sequence:$stream_sequence,
      first_consumer_sequence:$first_consumer_sequence,
      redelivery_consumer_sequence:$redelivery_consumer_sequence,
      first_num_delivered:1,
      delivery_count_lower_bound:2,
      ack_wait_nanoseconds:30000000000,
      ack_pending_before:$ack_pending_before,
      ack_pending_after:$ack_pending_after,
      ack_floor_stream_before:$ack_floor_stream_before,
      ack_floor_consumer_before:$ack_floor_consumer_before,
      ack_floor_stream_after:$ack_floor_stream_after,
      ack_floor_consumer_after:$ack_floor_consumer_after,
      inbox_receipts_before:1,
      inbox_receipts_after:1,
      attempts_before:1,
      attempts_after:1,
      handler_reapply_count:0,
      confirmed_ack_after_redelivery:true
    },
    visible_completions:1,
    posted_charges:1,
    artifact_rows:2,
    committed_artifacts:2,
    measurements:{
      "lost-accepted-job-count":0,
      "duplicate-visible-completion-count":0,
      "duplicate-charge-count":0,
      "stale-authority-acceptance-count":0
    },
    artifacts:[
      "scenario-matrix.json", "authority-before.json", "authority-after.json",
	  "consumer-marker.json", "consumer-database-before-crash.txt",
	  "consumer-database-after-ack.txt", "consumer-pre-ack-check.txt",
	  "consumer-redelivery-check.txt", "nats-stream-before-job.json",
	  "nats-stream-before-crash.json", "nats-stream-after-recovery.json",
	  "fault-window.txt",
      "fault-pod-identity.txt", "fault-injection.log", "raw-event-payloads.jsonl",
      "control-runtime-before.json", "control-runtime-fault-enabled.json",
      "control-runtime-restored.json", "control-fault-enabled.json",
      "control-after-crash.json", "control-default-restored.json",
      "fault-runtime-cleared.txt", "smoke-receipt.json"
    ]
  }
' >"$temporary/summary.json"
jq -e \
	--arg job_id "$job_id" \
	--arg event_id "$marker_event_id" \
	--argjson stream_sequence "$marker_stream_sequence" \
	--argjson first_consumer_sequence "$marker_consumer_sequence" '
  .schema == "vela-lab-consumer-post-db-pre-ack-crash-v1"
  and .status == "LAB_REHEARSAL_PASS"
  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
  and .production_gates == "0/9"
  and .fixed_scenarios_completed == 7
  and .fixed_scenarios_required == 10
  and .job_id == $job_id
  and .fault.kind == "CONTROL_PROCESS_SIGKILL_AFTER_SCHEDULER_DB_COMMIT_BEFORE_CONFIRMED_ACK"
  and .consumer.event_id == $event_id
  and .consumer.stream_sequence == $stream_sequence
  and .consumer.first_consumer_sequence == $first_consumer_sequence
  and .consumer.redelivery_consumer_sequence > $first_consumer_sequence
  and .consumer.first_num_delivered == 1
  and .consumer.delivery_count_lower_bound == 2
  and .consumer.ack_pending_before == 1
  and .consumer.ack_pending_after == 0
  and .consumer.ack_floor_stream_before < $stream_sequence
  and .consumer.ack_floor_stream_after >= $stream_sequence
  and .consumer.inbox_receipts_before == 1
  and .consumer.inbox_receipts_after == 1
  and .consumer.attempts_before == 1
  and .consumer.attempts_after == 1
  and .consumer.handler_reapply_count == 0
  and .visible_completions == 1
  and .posted_charges == 1
  and .artifact_rows == 2
  and .committed_artifacts == 2
  and all(.measurements[]; . == 0)
' "$temporary/summary.json" >/dev/null || fail "consumer crash summary is invalid"
printf 'status=LAB_REHEARSAL_PASS production_gates=0/9 fixed_scenarios=7/10\n' >"$temporary/STATUS"

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
printf 'schema=vela-lab-consumer-post-db-pre-ack-crash-wrapper-v1 output=%s job=%s result=LAB_REHEARSAL_PASS fixed_scenarios=7/10 production_gates=0/9\n' \
	"$output" "$job_id"

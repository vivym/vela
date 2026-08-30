#!/bin/sh

set -eu

namespace=vela-lab
control_node=vela-lab-control-1
worker1_id=84000000-0000-0000-0000-000000000101
worker2_id=84000000-0000-0000-0000-000000000102
expected_runner_image=10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3
expected_tool_image=10.1.200.17:5443/vela-lab/vela-lab-bootstrap@sha256:3f6a8bc440ee7bd7f9ba263d07435329c0863134217349db565cded2e9df9eac
previous_scenario_job_id=1c36decc-98a4-43c3-a92c-05c653650524
previous_scenario_receipt_sha256=674eaf8ac922f8fe4a9740435fbe3c08daa42a9ed88d5a519fee9c8703beb812
previous_scenario_harness_sha256=70dab9231d7f75f49abfc531dfd028a2316ae462ee6dcdb1af865980898034ef
previous_scenario_receipt=/root/vela-lab-deploy-bc590e20/receipts/outbox-post-commit-crash-v2
fault_phase=publisher-post-puback-pre-mark-crash
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

fail() {
	printf 'publisher-post-puback-pre-mark-crash: %s\n' "$*" >&2
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
	[ "$(cat "$previous_scenario_receipt/STATUS")" = 'status=LAB_REHEARSAL_PASS production_gates=0/9 fixed_scenarios=4/10' ] ||
		fail "previous fixed-scenario STATUS is not a four-scenario lab pass"
	jq -e \
		--arg job_id "$previous_scenario_job_id" \
		--arg harness "$previous_scenario_harness_sha256" '
		  .schema == "vela-lab-outbox-post-commit-crash-v1"
	  and .status == "LAB_REHEARSAL_PASS"
	  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
	  and .production_gates == "0/9"
		  and .fixed_scenarios_completed == 4
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
			--arg outbox_job_id "$previous_scenario_job_id" '
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
		  and ([.scenarios[] | select(.status == "LAB_REHEARSAL_PASS")] | length) == 4
		  and any(.scenarios[];
		    .id == "outbox-post-commit-crash"
		    and .status == "LAB_REHEARSAL_PASS"
		    and .job_id == $outbox_job_id)
		  and any(.scenarios[];
		    .id == "publisher-post-puback-pre-mark-crash" and .status == "NOT_RUN")
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
	value=$(printf '%s\n' "$current" | jq -r '.data.VELA_LAB_OUTBOX_FAULT_PHASE // ""')
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
		  {op:"test",path:"/data/VELA_LAB_OUTBOX_FAULT_PHASE",value:$value},
		  {op:"remove",path:"/data/VELA_LAB_OUTBOX_FAULT_PHASE"}
		]')
	$kubectl_bin patch configmap vela-lab-control-runtime --namespace "$namespace" \
		--type=json -p "$patch" >/dev/null
	current=$($kubectl_bin get configmap vela-lab-control-runtime --namespace "$namespace" -o json) || return 1
	[ "$(printf '%s\n' "$current" | jq -r '.data.VELA_LAB_OUTBOX_FAULT_PHASE // ""')" = "" ]
}

apply_fault_phase() {
	$kubectl_bin get configmap vela-lab-control-runtime --namespace "$namespace" \
		-o json >"$temporary/control-runtime-before.json"
	jq -e '
	  .metadata.name == "vela-lab-control-runtime"
	  and .metadata.namespace == "vela-lab"
	  and (.metadata.uid | test("^[0-9a-f-]{36}$"))
	  and (.data | type == "object")
	  and (.data | has("VELA_LAB_OUTBOX_FAULT_PHASE") | not)
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
		  {op:"add",path:"/data/VELA_LAB_OUTBOX_FAULT_PHASE",value:$value}
		]')
	$kubectl_bin patch configmap vela-lab-control-runtime --namespace "$namespace" \
		--type=json -p "$patch" >/dev/null
	$kubectl_bin get configmap vela-lab-control-runtime --namespace "$namespace" \
		-o json >"$temporary/control-runtime-fault-enabled.json"
	jq -e --arg uid "$uid" --arg phase "$fault_phase" '
	  .metadata.uid == $uid and .data.VELA_LAB_OUTBOX_FAULT_PHASE == $phase
	' "$temporary/control-runtime-fault-enabled.json" >/dev/null ||
		fail "Publisher fault-phase ConfigMap mutation did not converge"
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
	[ "$(printf '%s\n' "$current" | jq -r '.data.VELA_LAB_OUTBOX_FAULT_PHASE // ""')" = "" ] || return 1
	jq -e '
	  .metadata.deletionTimestamp == null
	  and any(.spec.containers[];
	    .name == "control"
	    and ((.env // []) | all(.name != "VELA_LAB_OUTBOX_FAULT_PHASE"))
	    and any(.envFrom[]?; .configMapRef.name == "vela-lab-control-runtime"))
	' "$pod_file" >/dev/null || return 1
	name=$(jq -er '.metadata.name' "$pod_file") || return 1
	if $kubectl_bin exec --namespace "$namespace" "pod/$name" --container control -- \
		/usr/local/bin/vela-control --lab-read-outbox-fault-marker \
		>"$output_file" 2>&1; then
		return 1
	fi
	grep -F 'inspect lab Outbox fault marker' "$output_file" >/dev/null &&
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
	        "app.kubernetes.io/name":"vela-lab-outbox-fault-image-warm",
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
	name=vela-lab-outbox-fault-image-warm-$(date +%s)-$$
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
    "schema=vela-lab-control-publisher-crash-v1 signal=SIGKILL "
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
	        "app.kubernetes.io/name":"vela-lab-outbox-control-fault",
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
	  and .metadata.labels["app.kubernetes.io/name"] == "vela-lab-outbox-control-fault"
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
	fault_pod_name=vela-lab-outbox-control-fault-$(date +%s)-$$
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
	if [ "$committed" != true ] && [ -n "$temporary" ] && [ -d "$temporary" ]; then
		[ "$cleanup_result" -ne 0 ] || cleanup_result=1
		printf 'status=INCOMPLETE production_gates=0/9\n' >"$temporary/STATUS"
		if recover_environment "$temporary/cleanup.log"; then
			disarm_watchdog
		else
			printf 'publisher-post-puback-pre-mark-crash: immediate recovery incomplete; armed watchdog retained\n' >&2
		fi
		printf 'publisher-post-puback-pre-mark-crash: diagnostic receipt preserved at %s\n' "$temporary" >&2
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
	fault_pod_json vela-lab-outbox-control-fault-render vela-lab-control-render \
		00000000-0000-0000-0000-000000000000 \
		0000000000000000000000000000000000000000000000000000000000000000 \
		"$job_id"
	exit 0
fi

if [ "${1:-}" = --render-warm-pod ]; then
	trap - EXIT HUP INT TERM
	warm_pod_json vela-lab-outbox-fault-image-warm-render
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
for command in date flock jq sha256sum; do
	command -v "$command" >/dev/null 2>&1 || fail "$command is required"
done
export KUBECONFIG="$kubeconfig"
temporary=$(mktemp -d "$(dirname "$output")/.vela-lab-publisher-post-puback-pre-mark-crash.XXXXXX")
chmod 0700 "$temporary"
load_tool_identity
validate_previous_scenario_receipt

$kubectl_bin rollout status deployment/vela-lab-control --namespace "$namespace" --timeout=60s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-worker-agent-1 --namespace "$namespace" --timeout=60s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-worker-agent-2 --namespace "$namespace" --timeout=60s >/dev/null
[ "$($kubectl_bin get pods --namespace "$namespace" -l app.kubernetes.io/name=vela-lab-outbox-control-fault -o json | jq '[.items[] | select(.status.phase != "Succeeded" and .status.phase != "Failed")] | length')" = 0 ] ||
	fail "an active outbox control fault Pod already exists"

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
	fail "control Pod did not reload the Publisher fault phase"
fault_identity=$(control_identity "$temporary/control-fault-enabled.json")
fault_uid=$(printf '%s\n' "$fault_identity" | cut -d '|' -f 2)
fault_started_at=$(printf '%s\n' "$fault_identity" | cut -d '|' -f 5)
started_epoch=$(date -d "$fault_started_at" +%s)
age=$(( $(date +%s) - started_epoch ))
[ "$age" -ge 0 ] && [ "$age" -le 20 ] || fail "fault-enabled Publisher control Pod is outside the bounded startup window"
sleep 2
heartbeat
[ "$(query_database 'SELECT count(*) FROM outbox_events WHERE published_at IS NULL OR claim_token IS NOT NULL;')" = 0 ] ||
	fail "fault-enabled Publisher startup did not drain the baseline Outbox"

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
		/usr/local/bin/vela-control --lab-read-outbox-fault-marker \
		>"$temporary/puback-marker.json" 2>"$temporary/puback-marker-read-error.txt"; then
		break
	fi
	iteration=$((iteration + 1))
	sleep 1
done
[ -s "$temporary/puback-marker.json" ] || fail "Publisher PubAck marker was not observed through kubectl exec"
rm -f -- "$temporary/puback-marker-read-error.txt"
jq -e --arg phase "$fault_phase" '
  (keys | sort) == (["broker_sequence", "broker_stream", "event_id", "phase", "schema", "subject"] | sort)
  and .schema == "vela-lab-outbox-fault-marker-v1"
  and .phase == $phase
  and .subject == "vela.events.job.ready"
  and (.event_id | test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"))
  and .broker_stream == "VELA_EVENTS"
  and (.broker_sequence | type == "number" and . >= 1 and floor == .)
' "$temporary/puback-marker.json" >/dev/null || fail "Publisher PubAck marker is invalid"
marker_event_id=$(jq -er '.event_id' "$temporary/puback-marker.json")
marker_stream=$(jq -er '.broker_stream' "$temporary/puback-marker.json")
marker_sequence=$(jq -er '.broker_sequence' "$temporary/puback-marker.json")

outbox_before=$(query_database "
SELECT job.state,
  (SELECT count(*) FROM attempts WHERE job_id = job.id),
  (SELECT count(*) FROM attempt_leases AS lease JOIN attempts AS attempt ON attempt.id = lease.attempt_id WHERE attempt.job_id = job.id AND lease.revoked_at IS NULL),
  outbox.event_id, outbox.event_type, outbox.publish_attempts,
  outbox.published_at IS NULL,
  outbox.claimed_by IS NOT NULL AND outbox.claim_token IS NOT NULL AND outbox.claim_expires_at IS NOT NULL,
  outbox.broker_stream IS NULL AND outbox.broker_sequence IS NULL
FROM jobs AS job JOIN outbox_events AS outbox ON outbox.aggregate_id = job.id
WHERE job.id = '$job_id'::uuid AND outbox.event_type = 'job.ready';")
printf '%s\n' "$outbox_before" >"$temporary/outbox-before-crash.txt"
case "$outbox_before" in
	"QUEUED|0|0|$marker_event_id|job.ready|1|t|t|t" | \
	"ASSIGNED|1|1|$marker_event_id|job.ready|1|t|t|t" | \
	"RUNNING|1|1|$marker_event_id|job.ready|1|t|t|t" | \
	"FINALIZING|1|1|$marker_event_id|job.ready|1|t|t|t" | \
	"SUCCEEDED|1|0|$marker_event_id|job.ready|1|t|t|t") ;;
	*) fail "Outbox event did not remain at the exact post-PubAck/pre-marker boundary" ;;
esac
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
  'outbox', (SELECT jsonb_agg(jsonb_build_object(
    'event_id', event_id, 'event_type', event_type, 'publish_attempts', publish_attempts,
    'claimed_by', claimed_by, 'claim_token', claim_token,
    'claim_expires_at', claim_expires_at, 'published_at', published_at,
    'broker_stream', broker_stream, 'broker_sequence', broker_sequence
  ) ORDER BY aggregate_version, event_type) FROM outbox_events WHERE aggregate_id = job.id)
)::text FROM jobs AS job WHERE job.id = '$job_id'::uuid;" >"$temporary/authority-before.json"

run_fault_pod "$temporary/control-fault-enabled.json" || fail "control SIGKILL injector did not complete"
fault_receipt=$(grep -E '^schema=vela-lab-control-publisher-crash-v1 signal=SIGKILL signal_transport=pidfd_send_signal container_id=[0-9a-f]{64} old_pid=[0-9]+ old_start_ticks=[0-9]+ signal_at=[^[:space:]]+ production_gates=0/9$' \
	"$temporary/fault-injection.log" || true)
[ "$(printf '%s\n' "$fault_receipt" | sed '/^$/d' | wc -l | tr -d ' ')" = 1 ] ||
	fail "control fault receipt is malformed or non-unique"
[ "$(grep -Ec '^signal_armed=SIGKILL container_id=[0-9a-f]{64} pid=[0-9]+ start_ticks=[0-9]+ signal_at=[^[:space:]]+$' "$temporary/fault-injection.log" || true)" = 1 ] ||
	fail "control signal claim is malformed or non-unique"
signal_at=$(printf '%s\n' "$fault_receipt" | sed -n 's/.* signal_at=\([^[:space:]]*\) .*/\1/p')
old_container_id=$(printf '%s\n' "$fault_receipt" | sed -n 's/.* container_id=\([0-9a-f]\{64\}\) .*/\1/p')
[ "$old_container_id" = "$(printf '%s\n' "$fault_identity" | cut -d '|' -f 3)" ] ||
	fail "fault receipt does not bind the fault-enabled Publisher container"

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
	fail "application Job did not converge after Outbox recovery"

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

outbox_after=
iteration=0
while [ "$iteration" -lt 90 ]; do
	heartbeat
	outbox_after=$(query_database "
SELECT event_type, publish_attempts,
  published_at IS NOT NULL AND published_at > '$signal_at'::timestamptz,
  broker_stream, broker_sequence,
  claimed_by IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL
FROM outbox_events
WHERE aggregate_id = '$job_id'::uuid
  AND event_id = '$marker_event_id'::uuid
  AND event_type = 'job.ready';")
	printf '%s\n' "$outbox_after" >>"$temporary/outbox-recovery-timeline.txt"
	[ "$outbox_after" = "job.ready|2|t|$marker_stream|$marker_sequence|t" ] && break
	case "$outbox_after" in job.ready'|'*) ;; *) fail "recovered Outbox event identity changed" ;; esac
	iteration=$((iteration + 1))
	sleep 1
done
printf '%s\n' "$outbox_after" >"$temporary/outbox-after-recovery.txt"
[ "$outbox_after" = "job.ready|2|t|$marker_stream|$marker_sequence|t" ] ||
	fail "recovered Outbox marker does not match the pre-crash PubAck"

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
	fail "final measurements do not satisfy the Outbox crash lab contract"

restore_fault_config || fail "Publisher fault-phase ConfigMap could not be restored"
printf 'restored_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$temporary/FAULT_PHASE_REMOVED"
wait_ready_control_pod "$temporary/control-before-default-reload.json" ||
	fail "control was not Ready before clearing the Publisher fault phase"
reload_control "$temporary/control-before-default-reload.json" "$temporary/control-default-restored.json" ||
	fail "control did not clear the Publisher fault phase"
verify_fault_runtime_cleared \
	"$temporary/control-default-restored.json" \
	"$temporary/fault-runtime-cleared.txt" ||
	fail "control fault runtime or marker remained after the default reload"
printf 'reloaded_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$temporary/FAULT_PHASE_CLEARED"
$kubectl_bin get configmap vela-lab-control-runtime --namespace "$namespace" -o json \
	>"$temporary/control-runtime-restored.json"
jq -e --arg uid "$(jq -er '.metadata.uid' "$temporary/control-runtime-before.json")" '
  .metadata.uid == $uid and (.data | has("VELA_LAB_OUTBOX_FAULT_PHASE") | not)
' "$temporary/control-runtime-restored.json" >/dev/null || fail "Publisher fault-phase ConfigMap boundary was not restored"

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
  (.scenarios[] | select(.id == "publisher-post-puback-pre-mark-crash")) = {
    id:"publisher-post-puback-pre-mark-crash",
    status:"LAB_REHEARSAL_PASS",
    job_id:$job_id,
    started_at:$started_at,
    completed_at:$completed_at,
    fault:"CONTROL_PROCESS_SIGKILL_AFTER_PUBLISHER_PUBACK_BEFORE_DATABASE_MARKER",
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
	  and ([.scenarios[] | select(.status == "LAB_REHEARSAL_PASS")] | length) == 5
  and any(.scenarios[]; .id == "publisher-post-puback-pre-mark-crash" and .job_id == $job_id and .status == "LAB_REHEARSAL_PASS")
' "$temporary/scenario-matrix.json" >/dev/null || fail "five-scenario matrix is invalid"

harness_sha256=$(sha256sum "$0" | awk '{print $1}')
old_pid=$(printf '%s\n' "$fault_receipt" | sed -n 's/.* old_pid=\([0-9][0-9]*\) .*/\1/p')
old_start_ticks=$(printf '%s\n' "$fault_receipt" | sed -n 's/.* old_start_ticks=\([0-9][0-9]*\) .*/\1/p')
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
	--arg broker_stream "$marker_stream" \
	--argjson broker_sequence "$marker_sequence" \
	--argjson old_pid "$old_pid" \
	--argjson old_start_ticks "$old_start_ticks" \
	--argjson restart_count "$after_restarts" '
  {
    schema:"vela-lab-publisher-post-puback-pre-mark-crash-v1",
    status:"LAB_REHEARSAL_PASS",
    evidence_boundary:"NON_PRODUCTION_MOCK_REHEARSAL",
    production_gates:"0/9",
    fixed_scenarios_completed:5,
    fixed_scenarios_required:10,
    job_id:$job_id,
    started_at:$started_at,
    completed_at:$completed_at,
    harness_sha256:$harness_sha256,
    previous_receipt_sha256:$previous_receipt_sha256,
    control_image:$control_image,
    fault:{
      kind:"CONTROL_PROCESS_SIGKILL_AFTER_PUBLISHER_PUBACK_BEFORE_DATABASE_MARKER",
      signal_transport:"pidfd_send_signal",
      signal_at:$signal_at,
      container_id_before:$container_id,
      container_id_after:$replacement_container_id,
      old_pid:$old_pid,
      old_start_ticks:$old_start_ticks,
      restart_count:$restart_count
    },
    outbox:{
      event_id:$event_id,
      event_type:"job.ready",
      publish_attempts_before_crash:1,
      publish_attempts_after_recovery:2,
      broker_stream:$broker_stream,
      broker_sequence:$broker_sequence,
      recovered_marker_matches_puback:true
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
	      "outbox-before-crash.txt", "outbox-after-recovery.txt", "outbox-recovery-timeline.txt",
	      "puback-marker.json",
      "fault-pod-identity.txt", "fault-injection.log", "raw-event-payloads.jsonl",
      "control-runtime-before.json", "control-runtime-fault-enabled.json",
      "control-runtime-restored.json", "control-fault-enabled.json",
      "control-after-crash.json", "control-default-restored.json",
      "fault-runtime-cleared.txt", "smoke-receipt.json"
    ]
  }
' >"$temporary/summary.json"
printf 'status=LAB_REHEARSAL_PASS production_gates=0/9 fixed_scenarios=5/10\n' >"$temporary/STATUS"

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
printf 'schema=vela-lab-publisher-post-puback-pre-mark-crash-wrapper-v1 output=%s job=%s result=LAB_REHEARSAL_PASS fixed_scenarios=5/10 production_gates=0/9\n' \
	"$output" "$job_id"

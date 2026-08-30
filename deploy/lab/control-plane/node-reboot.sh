#!/bin/sh

set -eu

namespace=vela-lab
worker1_id=84000000-0000-0000-0000-000000000101
worker2_id=84000000-0000-0000-0000-000000000102
worker1_node=vela-lab-worker-1
worker2_node=vela-lab-worker-2
worker1_internal_ip=10.1.200.19
replacement_preset_revision_id=84000000-0000-0000-0000-000000000201
replacement_certification_id=84000000-0000-0000-0000-000000000202
service_class_id=84000000-0000-0000-0000-000000000009
expected_runner_image=10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3
expected_tool_image=10.1.200.17:5443/vela-lab/vela-lab-bootstrap@sha256:3f6a8bc440ee7bd7f9ba263d07435329c0863134217349db565cded2e9df9eac
expected_old_preset_revision_id=84000000-0000-0000-0000-000000000005
previous_scenario_job_id=0831b136-2639-4139-ac1d-d6af9186b09c
previous_scenario_receipt_sha256=817edbf165a151d8a2552aadbfcef907a4651484d720cade36bae59a63f873fe
previous_scenario_harness_sha256=75331cb29a07a89c3d69c6a166e81772ea36ce8aef23afa84a93fa1687d9a0e8
previous_scenario_receipt=/root/vela-lab-deploy-bc590e20/receipts/consumer-post-db-pre-ack-crash-v1
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}
temporary=
output=
manifests=
job_id=
committed=false
recovering=false
reboot_observed=false
watchdog_marker=
watchdog_heartbeat=
watchdog_pid=
tool_image=
runner_container_id=
node_uid_before=
node_boot_id_before=
node_boot_id_after=

fail() {
	printf 'node-reboot: %s\n' "$*" >&2
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

load_tool_identity() {
	smoke_json=$($kubectl_bin create --dry-run=client -f "$manifests/60-smoke.yaml" -o json)
	tool_image=$(printf '%s\n' "$smoke_json" | jq -er '.spec.template.spec.containers | map(select(.name == "smoke")) | .[0].image')
	[ "$tool_image" = "$expected_tool_image" ] ||
		fail "lab tool image does not match the fixed private Registry digest"
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
	[ "$(cat "$previous_scenario_receipt/STATUS")" = 'status=LAB_REHEARSAL_PASS production_gates=0/9 fixed_scenarios=7/10' ] ||
		fail "previous fixed-scenario STATUS is not a seven-scenario lab pass"
	jq -e \
		--arg job_id "$previous_scenario_job_id" \
		--arg harness "$previous_scenario_harness_sha256" '
	  .schema == "vela-lab-consumer-post-db-pre-ack-crash-v1"
	  and .status == "LAB_REHEARSAL_PASS"
	  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
	  and .production_gates == "0/9"
	  and .fixed_scenarios_completed == 7
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
		--arg consumer_job_id "$previous_scenario_job_id" '
	  .schema == "vela-lab-fault-scenario-matrix-v1"
	  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
	  and .production_gates == "0/9"
	  and (.scenarios | type == "array" and length == 10)
	  and ([.scenarios[] | select(.status == "LAB_REHEARSAL_PASS")] | length) == 7
	  and any(.scenarios[];
	    .id == "worker-control-network-partition" and .status == "LAB_REHEARSAL_PASS")
	  and any(.scenarios[];
	    .id == "consumer-post-db-pre-ack-crash"
	    and .status == "LAB_REHEARSAL_PASS"
	    and .job_id == $consumer_job_id)
	  and any(.scenarios[];
	    .id == "node-reboot" and .status == "NOT_RUN")
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
		--arg runner_image "$expected_runner_image" \
		--arg script "$script" \
		--arg expected "$expected" \
		--arg requested "$requested" \
		--argjson read_only "$read_only" '
	  {
	    apiVersion: "v1",
	    kind: "Pod",
	    metadata: {
	      name: $name,
	      namespace: $namespace,
	      labels: {
	        "app.kubernetes.io/name": "vela-lab-node-reboot-runner-control",
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
	          {name: "REQUESTED_MODE", value: $requested},
	          {name: "EXPECTED_RUNNER_IMAGE", value: $runner_image}
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
	          {name: "runner-config", mountPath: "/runner-config", readOnly: $read_only},
	          {name: "tmp", mountPath: "/tmp"}
	        ]
	      }],
	      volumes: [
	        {name: "runner-config", hostPath: {path: "/var/lib/vela-lab/mock-runner/config", type: "Directory"}},
	        {name: "tmp", emptyDir: {}}
	      ]
	    }
	  }
	'
}

run_runner_pod() {
	name=$1
	node=$2
	read_only=$3
	script=$4
	expected=$5
	requested=$6
	log_file=$7
	runner_pod_json "$name" "$node" "$read_only" "$script" "$expected" "$requested" >"$temporary/$name.json"
	$kubectl_bin create -f "$temporary/$name.json" >/dev/null
	heartbeat
	if ! $kubectl_bin wait --namespace "$namespace" "pod/$name" \
		--for=jsonpath='{.status.phase}'=Succeeded --timeout=60s >/dev/null; then
		$kubectl_bin describe pod --namespace "$namespace" "$name" >>"$log_file" 2>&1 || true
		$kubectl_bin logs --namespace "$namespace" "$name" >>"$log_file" 2>&1 || true
		$kubectl_bin delete pod --namespace "$namespace" "$name" --ignore-not-found=true --wait=true >/dev/null 2>&1 || true
		return 1
	fi
	heartbeat
	$kubectl_bin logs --namespace "$namespace" "$name" >"$log_file"
	$kubectl_bin delete pod --namespace "$namespace" "$name" --wait=true >/dev/null
}

current_mock_mode() {
	node=$1
	name=vela-lab-process-mode-read-$(date +%s)-$$
	log_file=$temporary/$name.log
	# The variables are expanded inside the Runner-control Pod.
	# shellcheck disable=SC2016
	run_runner_pod "$name" "$node" true \
		'mode=/runner-config/mock-mode
test -f "$mode" && test ! -L "$mode"
test "$(stat -c %u:%g:%a "$mode")" = 0:0:444
sed -n 1p "$mode"' '' '' "$log_file" || return 1
	cat "$log_file"
}

current_runner_profiles() {
	node=$1
	name=vela-lab-process-profiles-read-$(date +%s)-$$
	log_file=$temporary/$name.log
	# The variables are expanded inside the Runner-control Pod.
	# shellcheck disable=SC2016
	run_runner_pod "$name" "$node" true \
		'profiles=/runner-config/profiles.json
test -f "$profiles" && test ! -L "$profiles"
test "$(stat -c %u:%g:%a "$profiles")" = 0:0:444
cat "$profiles"' '' '' "$log_file" || return 1
	cat "$log_file"
}

validate_runner_profiles() {
	profile_file=$1
	jq -e \
		--arg old "$expected_old_preset_revision_id" \
		--arg replacement "$replacement_preset_revision_id" '
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

current_runner_container_id() {
	node=$1
	name=vela-lab-process-identity-read-$(date +%s)-$$
	log_file=$temporary/$name.log
	# The variables are expanded inside the Runner-control Pod.
	# shellcheck disable=SC2016
	run_runner_pod "$name" "$node" true \
		'identity=/runner-config/container-identity
test -f "$identity" && test ! -L "$identity"
test "$(stat -c %u:%g:%a "$identity")" = 0:0:444
test "$(sed -n 1p "$identity")" = schema=vela-lab-runner-container-identity-v1
container_id=$(sed -n "s/^container_id=//p" "$identity")
image=$(sed -n "s/^image=//p" "$identity")
printf "%s\n" "$container_id" | grep -Eq "^[0-9a-f]{64}$"
test "$image" = "$EXPECTED_RUNNER_IMAGE"
test "$(wc -l <"$identity" | tr -d " ")" -eq 3
printf "%s\n" "$container_id"' '' '' "$log_file" || return 1
	cat "$log_file"
}

switch_mock_mode() {
	node=$1
	expected=$2
	requested=$3
	log_file=$4
	name=vela-lab-process-mode-write-$(date +%s)-$$
	# The variables are expanded inside the Runner-control Pod.
	# shellcheck disable=SC2016
	run_runner_pod "$name" "$node" false \
		'mode=/runner-config/mock-mode
wrapper=/runner-config/mock-backend-wrapper.sh
test -f "$mode" && test ! -L "$mode"
test -f "$wrapper" && test ! -L "$wrapper"
test "$(stat -c %u:%g:%a "$mode")" = 0:0:444
test "$(stat -c %u:%g:%a "$wrapper")" = 0:0:555
current=$(sed -n 1p "$mode")
test "$current" = "$EXPECTED_MODE"
temporary=$(mktemp /runner-config/.mock-mode.XXXXXX)
cleanup_mode() { rm -f -- "$temporary"; }
trap cleanup_mode EXIT HUP INT TERM
printf "%s\n" "$REQUESTED_MODE" >"$temporary"
chown 0:0 "$temporary"
chmod 0444 "$temporary"
mv -f -- "$temporary" "$mode"
temporary=
trap - EXIT HUP INT TERM
test "$(sed -n 1p "$mode")" = "$REQUESTED_MODE"
printf "schema=vela-lab-node-reboot-mode-v1 before=%s after=%s production_gates=0/9\n" "$current" "$REQUESTED_MODE"' \
		"$expected" "$requested" "$log_file"
}

fault_script() {
	cat <<'EOF'
set -eu

python3 - "$EXPECTED_RUNNER_IMAGE" "$EXPECTED_CONTAINER_ID" <<'PY'
import errno
import hashlib
import json
import os
import select
import signal
import sys
import time

expected_image, container_id = sys.argv[1:]
if len(container_id) != 64 or any(character not in "0123456789abcdef" for character in container_id):
    raise RuntimeError("expected container identity is invalid")
if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
    raise RuntimeError("fault image does not support pidfd signaling")

def read_json(path):
    with open(path, encoding="utf-8") as config_file:
        return json.load(config_file)


def validate_config(config, container_id):
    container_config = config.get("Config", {})
    labels = config.get("Config", {}).get("Labels", {})
    environment = container_config.get("Env") or []
    entrypoint = container_config.get("Entrypoint") or []
    if isinstance(entrypoint, str):
        entrypoint = [entrypoint]
    if (
        config.get("Name") != "/vela-h3-mock-runner"
        or labels.get("vela.ai.component") != "h3-mock-runner"
        or container_config.get("Image") != expected_image
        or container_config.get("User") != "10001:10001"
        or "VELA_RUNNER_SOCKET=/run/vela-runner/runner.sock" not in environment
        or "/opt/vela/venv/bin/vela-h3-runner" not in entrypoint
        or config.get("ID") not in (None, container_id)
    ):
        raise RuntimeError("managed Runner container metadata is invalid")


def start_ticks(pid):
    stat = open(f"/proc/{pid}/stat", encoding="utf-8").read()
    closing_paren = stat.rfind(")")
    if closing_paren < 0:
        raise RuntimeError("invalid process stat record")
    return stat[closing_paren + 2 :].split()[19]


config_path = "/docker-container/config.v2.json"
config = read_json(config_path)
validate_config(config, container_id)
state = config.get("State", {})
pid = state.get("Pid")
old_started_at = state.get("StartedAt")
old_restart_count = config.get("RestartCount")
if (
    not state.get("Running")
    or state.get("Restarting")
    or not isinstance(pid, int)
    or pid <= 1
    or not isinstance(old_started_at, str)
    or not old_started_at
    or not isinstance(old_restart_count, int)
):
    raise RuntimeError("managed Runner container is not stably running")
old_pid = pid
old_start_ticks = start_ticks(old_pid)
with open(f"/proc/{old_pid}/status", encoding="utf-8") as status_file:
    uid_line = next((line for line in status_file if line.startswith("Uid:")), "")
if uid_line.split()[1:2] != ["10001"]:
    raise RuntimeError("managed Runner PID has an unexpected UID")
with open(f"/proc/{old_pid}/cgroup", encoding="utf-8") as cgroup_file:
    old_cgroup = cgroup_file.read()
if container_id not in old_cgroup and container_id[:12] not in old_cgroup:
    raise RuntimeError("managed Runner PID is outside the expected cgroup")
old_cgroup_sha256 = hashlib.sha256(old_cgroup.encode()).hexdigest()

pidfd = os.pidfd_open(old_pid)
try:
    current = read_json(config_path)
    validate_config(current, container_id)
    current_state = current.get("State", {})
    if (
        current_state.get("Pid") != old_pid
        or not current_state.get("Running")
        or current_state.get("Restarting")
        or start_ticks(old_pid) != old_start_ticks
    ):
        raise RuntimeError("Runner process identity changed before signal")
    print(
        f"signal_armed=SIGKILL container_id={container_id} pid={old_pid} "
        f"start_ticks={old_start_ticks}",
        flush=True,
    )
    signal_transport = "pidfd_send_signal"
    try:
        signal.pidfd_send_signal(pidfd, signal.SIGKILL)
    except PermissionError as error:
        if error.errno != errno.EPERM:
            raise
        fallback = read_json(config_path)
        validate_config(fallback, container_id)
        fallback_state = fallback.get("State", {})
        if (
            fallback_state.get("Pid") != old_pid
            or not fallback_state.get("Running")
            or fallback_state.get("Restarting")
            or start_ticks(old_pid) != old_start_ticks
        ):
            raise RuntimeError("Runner process identity changed before kill fallback")
        os.kill(old_pid, signal.SIGKILL)
        signal_transport = "kill"
    print(f"signal_sent=SIGKILL transport={signal_transport}", flush=True)
    poller = select.poll()
    poller.register(pidfd, select.POLLIN)
    if not poller.poll(60_000):
        raise RuntimeError("Runner process did not exit after SIGKILL")
finally:
    os.close(pidfd)

deadline = time.monotonic() + 60
new_pid = None
new_start_ticks = None
new_started_at = None
new_restart_count = None
while time.monotonic() < deadline:
    try:
        replacement = read_json(config_path)
        validate_config(replacement, container_id)
        replacement_state = replacement.get("State", {})
        candidate_pid = replacement_state.get("Pid")
        if (
            replacement_state.get("Running")
            and not replacement_state.get("Restarting")
            and isinstance(candidate_pid, int)
            and candidate_pid > 1
            and candidate_pid != old_pid
            and replacement_state.get("StartedAt") != old_started_at
            and isinstance(replacement.get("RestartCount"), int)
            and replacement.get("RestartCount") > old_restart_count
        ):
            candidate_ticks = start_ticks(candidate_pid)
            with open(f"/proc/{candidate_pid}/cgroup", encoding="utf-8") as cgroup_file:
                candidate_cgroup = cgroup_file.read()
            if (
                candidate_ticks != old_start_ticks
                and (container_id in candidate_cgroup or container_id[:12] in candidate_cgroup)
            ):
                replacement_pidfd = os.pidfd_open(candidate_pid)
                try:
                    confirmed = read_json(config_path).get("State", {})
                    if confirmed.get("Running") and confirmed.get("Pid") == candidate_pid:
                        new_pid = candidate_pid
                        new_start_ticks = candidate_ticks
                        new_started_at = replacement_state.get("StartedAt")
                        new_restart_count = replacement.get("RestartCount")
                        break
                finally:
                    os.close(replacement_pidfd)
    except (FileNotFoundError, ProcessLookupError):
        pass
    time.sleep(1)
if new_pid is None:
    raise RuntimeError("replacement Runner process did not become stable")

print(
    "schema=vela-lab-runner-recovery-process-kill-v1 signal=SIGKILL "
    f"signal_transport={signal_transport} "
    f"container_id={container_id} old_pid={old_pid} new_pid={new_pid} "
    f"old_start_ticks={old_start_ticks} new_start_ticks={new_start_ticks} "
    f"old_started_at={old_started_at} new_started_at={new_started_at} "
    f"restart_count={new_restart_count} old_cgroup_sha256={old_cgroup_sha256} "
    "production_gates=0/9"
)
PY
EOF
}

fault_pod_json() {
	name=$1
	node=$2
	container_id=$3
	rehearsal_job_id=$4
	printf '%s\n' "$container_id" | grep -Eq '^[0-9a-f]{64}$' || fail "fault Pod container identity is invalid"
	printf '%s\n' "$rehearsal_job_id" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' ||
		fail "fault Pod rehearsal Job identity is invalid"
	script=$(fault_script)
	jq -n \
		--arg name "$name" \
		--arg namespace "$namespace" \
		--arg node "$node" \
		--arg image "$expected_runner_image" \
		--arg script "$script" \
		--arg runner_image "$expected_runner_image" \
		--arg container_id "$container_id" \
		--arg rehearsal_job_id "$rehearsal_job_id" '
	  {
	    apiVersion: "v1",
	    kind: "Pod",
	    metadata: {
	      name: $name,
	      namespace: $namespace,
		      labels: {
	        "app.kubernetes.io/name": "vela-lab-node-reboot-recovery-injector",
	        "app.kubernetes.io/component": "lab-rehearsal",
		        "vela.ai/environment": "non-production-lab"
		      },
		      annotations: {"vela.ai/rehearsal-job-id": $rehearsal_job_id}
	    },
	    spec: {
	      automountServiceAccountToken: false,
	      hostPID: true,
	      nodeName: $node,
	      restartPolicy: "Never",
	      containers: [{
	        name: "fault-injector",
	        image: $image,
	        imagePullPolicy: "IfNotPresent",
	        command: ["/bin/sh", "-ec"],
	        args: [$script],
	        env: [
	          {name: "EXPECTED_RUNNER_IMAGE", value: $runner_image},
	          {name: "EXPECTED_CONTAINER_ID", value: $container_id}
	        ],
	        resources: {
	          requests: {cpu: "10m", memory: "32Mi"},
	          limits: {cpu: "100m", memory: "64Mi"}
	        },
		        securityContext: {
		          runAsUser: 0,
		          runAsGroup: 0,
		          allowPrivilegeEscalation: false,
		          readOnlyRootFilesystem: true,
		          capabilities: {drop: ["ALL"], add: ["KILL"]},
		          appArmorProfile: {type: "Unconfined"},
		          seccompProfile: {type: "RuntimeDefault"}
		        },
	        volumeMounts: [
	          {name: "docker-container", mountPath: "/docker-container", readOnly: true},
	          {name: "tmp", mountPath: "/tmp"}
	        ]
	      }],
	      volumes: [
	        {name: "docker-container", hostPath: {path: ("/var/lib/docker/containers/" + $container_id), type: "Directory"}},
	        {name: "tmp", emptyDir: {}}
	      ]
	    }
	  }
	'
}

persist_fault_pod_identity() {
	log_file=$1
	[ -f "$temporary/fault-pod-name.txt" ] || return 1
	name=$(cat "$temporary/fault-pod-name.txt")
	printf '%s\n' "$name" | grep -Eq '^vela-lab-node-reboot-[0-9]+-[0-9]+$' || {
		printf 'saved fault Pod name is invalid\n' >>"$log_file"
		return 1
	}
	pod_json=$($kubectl_bin get pod --namespace "$namespace" "$name" -o json 2>>"$log_file") || return 1
	printf '%s\n' "$pod_json" | jq -e \
		--arg name "$name" \
		--arg node "$worker1_node" \
		--arg image "$expected_runner_image" \
		--arg container_id "$runner_container_id" \
		--arg job_id "$job_id" '
      .metadata.name == $name
      and .metadata.namespace == "vela-lab"
      and (.metadata.uid | test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"))
      and .metadata.labels["app.kubernetes.io/name"] == "vela-lab-node-reboot-recovery-injector"
      and .metadata.labels["app.kubernetes.io/component"] == "lab-rehearsal"
      and .metadata.labels["vela.ai/environment"] == "non-production-lab"
      and .metadata.annotations["vela.ai/rehearsal-job-id"] == $job_id
      and .spec.nodeName == $node
      and .spec.hostPID == true
      and (.spec.containers | length) == 1
      and .spec.containers[0].name == "fault-injector"
      and .spec.containers[0].image == $image
      and any(.spec.containers[0].env[]; .name == "EXPECTED_CONTAINER_ID" and .value == $container_id)
      and any(.spec.volumes[];
        .name == "docker-container"
        and .hostPath.path == ("/var/lib/docker/containers/" + $container_id)
        and .hostPath.type == "Directory")
    ' >/dev/null || {
		printf 'observed fault Pod does not match the owned manifest identity\n' >>"$log_file"
		return 1
	}
	uid=$(printf '%s\n' "$pod_json" | jq -er '.metadata.uid') || return 1
	printf 'pod=%s\nuid=%s\njob_id=%s\n' "$name" "$uid" "$job_id" >"$temporary/fault-pod-identity.txt"
}

neutralize_active_fault_pod() {
	log_file=$1
	[ -f "$temporary/fault-pod-name.txt" ] || return 0
	name=$(cat "$temporary/fault-pod-name.txt")
	if [ ! -f "$temporary/fault-pod-identity.txt" ]; then
		resource=$($kubectl_bin get pod --namespace "$namespace" "$name" --ignore-not-found -o name 2>>"$log_file") || return 1
		[ -n "$resource" ] || return 0
		persist_fault_pod_identity "$log_file" || return 1
	fi
	[ "$(wc -l <"$temporary/fault-pod-identity.txt" | tr -d ' ')" = 3 ] || return 1
	[ "$(sed -n 's/^pod=//p' "$temporary/fault-pod-identity.txt")" = "$name" ] || return 1
	uid=$(sed -n 's/^uid=//p' "$temporary/fault-pod-identity.txt")
	printf '%s\n' "$uid" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' || return 1
	[ "$(sed -n 's/^job_id=//p' "$temporary/fault-pod-identity.txt")" = "$job_id" ] || return 1
	resource=$($kubectl_bin get pod --namespace "$namespace" "$name" --ignore-not-found -o name 2>>"$log_file") || return 1
	[ -n "$resource" ] || return 0
	pod_json=$($kubectl_bin get pod --namespace "$namespace" "$name" -o json 2>>"$log_file") || return 1
	[ "$(printf '%s\n' "$pod_json" | jq -er '.metadata.uid')" = "$uid" ] || {
		printf 'fault Pod UID changed; refusing deletion\n' >>"$log_file"
		return 1
	}
	persist_fault_pod_identity "$log_file" || return 1
	jq -n --arg uid "$uid" '{apiVersion:"v1",kind:"DeleteOptions",preconditions:{uid:$uid},propagationPolicy:"Background"}' |
		$kubectl_bin delete --raw="/api/v1/namespaces/$namespace/pods/$name" -f - >>"$log_file" 2>&1 || return 1
	iteration=0
	while [ "$iteration" -lt 60 ]; do
		remaining=$($kubectl_bin get pod --namespace "$namespace" "$name" --ignore-not-found -o json 2>>"$log_file") || return 1
		[ -n "$remaining" ] || return 0
		[ "$(printf '%s\n' "$remaining" | jq -er '.metadata.uid')" = "$uid" ] || {
			printf 'a different Pod UID appeared while waiting for deletion\n' >>"$log_file"
			return 1
		}
		iteration=$((iteration + 1))
		sleep 1
	done
	printf 'UID-preconditioned fault Pod deletion did not converge\n' >>"$log_file"
	return 1
}

run_fault_pod() {
	log_file=$1
	name=vela-lab-node-reboot-$(date +%s)-$$
	printf '%s\n' "$runner_container_id" | grep -Eq '^[0-9a-f]{64}$' || fail "Runner container identity is unavailable"
	printf '%s\n' "$name" >"$temporary/fault-pod-name.txt"
	fault_pod_json "$name" "$worker1_node" "$runner_container_id" "$job_id" >"$temporary/$name.json"
	if ! $kubectl_bin create -f "$temporary/$name.json" >/dev/null; then
		persist_fault_pod_identity "$log_file" >/dev/null 2>&1 || true
		neutralize_active_fault_pod "$log_file" >/dev/null 2>&1 || true
		return 1
	fi
	persist_fault_pod_identity "$log_file" || return 1
	heartbeat
	if ! $kubectl_bin wait --namespace "$namespace" "pod/$name" \
		--for=jsonpath='{.status.phase}'=Succeeded --timeout=150s >/dev/null; then
		phase=$($kubectl_bin get pod --namespace "$namespace" "$name" -o jsonpath='{.status.phase}' 2>/dev/null || printf Unknown)
		$kubectl_bin describe pod --namespace "$namespace" "$name" >>"$log_file" 2>&1 || true
		$kubectl_bin logs --namespace "$namespace" "$name" >>"$log_file" 2>&1 || true
		if [ "$phase" != Succeeded ]; then
			neutralize_active_fault_pod "$log_file" || true
			return 1
		fi
	fi
	heartbeat
	$kubectl_bin logs --namespace "$namespace" "$name" >"$log_file"
	printf 'completed_at=%s pod=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$name" >"$temporary/FAULT_INJECTION_COMPLETED"
	neutralize_active_fault_pod "$log_file"
}

node_snapshot() {
	$kubectl_bin get node "$worker1_node" -o json
}

validate_node_snapshot() {
	file=$1
	expected_ready=$2
	jq -e \
		--arg node "$worker1_node" \
		--arg internal_ip "$worker1_internal_ip" \
		--arg ready "$expected_ready" '
	  .metadata.name == $node
	  and (.metadata.uid | test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"))
	  and ([.status.addresses[] | select(.type == "InternalIP" and .address == $internal_ip)] | length) == 1
	  and .status.allocatable["nvidia.com/gpu"] == "8"
	  and (.status.nodeInfo.bootID | test("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"))
	  and (.status.nodeInfo.machineID | type == "string" and length > 0)
	  and (.status.nodeInfo.systemUUID | type == "string" and length > 0)
	  and ([.status.conditions[] | select(.type == "Ready" and .status == $ready)] | length) == 1
	' "$file" >/dev/null
}

wait_for_node_ready() {
	log_file=$1
	iteration=0
	while [ "$iteration" -lt 360 ]; do
		if snapshot=$(node_snapshot 2>>"$log_file"); then
			ready=$(printf '%s\n' "$snapshot" | jq -er '[.status.conditions[] | select(.type == "Ready")][0].status' 2>>"$log_file" || printf Unknown)
			[ "$ready" != True ] || return 0
		fi
		iteration=$((iteration + 1))
		sleep 1
	done
	printf 'Worker 1 did not return to Kubernetes Ready\n' >>"$log_file"
	return 1
}

wait_for_node_reboot() {
	attempt_id=$1
	attempt_fence=$2
	node_snapshot >"$temporary/node-before-reboot.json"
	validate_node_snapshot "$temporary/node-before-reboot.json" True ||
		fail "Worker 1 node identity is not Ready or does not match the fixed lab target"
	node_uid_before=$(jq -er '.metadata.uid' "$temporary/node-before-reboot.json")
	node_boot_id_before=$(jq -er '.status.nodeInfo.bootID' "$temporary/node-before-reboot.json")
	action_id=$(cat /proc/sys/kernel/random/uuid)
	printf '%s\n' "$action_id" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' ||
		fail "could not create a canonical reboot action identity"
	action_armed_at=$(query_database 'SELECT clock_timestamp();')
	jq -n \
		--arg action_id "$action_id" \
		--arg armed_at "$action_armed_at" \
		--arg node "$worker1_node" \
		--arg node_uid "$node_uid_before" \
		--arg internal_ip "$worker1_internal_ip" \
		--arg boot_id "$node_boot_id_before" \
		--arg job_id "$job_id" \
		--arg attempt_id "$attempt_id" \
		--argjson fence "$attempt_fence" '
	  {
	    schema:"vela-lab-node-reboot-action-intent-v1",
	    environment:"NON_PRODUCTION_MOCK_REHEARSAL",
	    action:"NODE_REBOOT",
	    action_id:$action_id,
	    armed_at:$armed_at,
	    target:{node:$node,node_uid:$node_uid,internal_ip:$internal_ip,boot_id:$boot_id},
	    authority:{job_id:$job_id,attempt_id:$attempt_id,fence:$fence},
	    production_gates:"0/9"
	  }
	' >"$temporary/reboot-action-intent.json"
	printf 'status=ARMED action_id=%s node=%s node_uid=%s internal_ip=%s boot_id=%s job=%s attempt=%s fence=%s production_gates=0/9\n' \
		"$action_id" "$worker1_node" "$node_uid_before" "$worker1_internal_ip" "$node_boot_id_before" \
		"$job_id" "$attempt_id" "$attempt_fence" >"$temporary/ACTION_REQUIRED"
	printf 'action_required=NODE_REBOOT action_id=%s node=%s node_uid=%s internal_ip=%s boot_id=%s job=%s attempt=%s fence=%s production_gates=0/9\n' \
		"$action_id" "$worker1_node" "$node_uid_before" "$worker1_internal_ip" "$node_boot_id_before" \
		"$job_id" "$attempt_id" "$attempt_fence"

	unready_observed_at=
	unready_status=
	iteration=0
	while [ "$iteration" -lt 240 ]; do
		heartbeat
		if snapshot=$(node_snapshot 2>>"$temporary/node-reboot-observation.log"); then
			ready=$(printf '%s\n' "$snapshot" | jq -er '[.status.conditions[] | select(.type == "Ready")][0].status' 2>>"$temporary/node-reboot-observation.log" || printf Unknown)
			boot_id=$(printf '%s\n' "$snapshot" | jq -er '.status.nodeInfo.bootID' 2>>"$temporary/node-reboot-observation.log" || printf Unknown)
			if [ "$ready" != True ]; then
				unready_observed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
				unready_status=$ready
				printf '%s\n' "$snapshot" >"$temporary/node-unready.json"
				break
			fi
			[ "$boot_id" = "$node_boot_id_before" ] || fail "boot ID changed before an unavailable Node state was observed"
		else
			unready_observed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
			unready_status=API_UNAVAILABLE
			break
		fi
		iteration=$((iteration + 1))
		sleep 1
	done
	[ -n "$unready_observed_at" ] || fail "Worker 1 did not become unavailable inside the armed reboot window"
	printf 'observed_at=%s ready_status=%s\n' "$unready_observed_at" "$unready_status" >"$temporary/node-unready-observation.txt"

	stable_observations=0
	iteration=0
	while [ "$iteration" -lt 600 ]; do
		heartbeat
		if snapshot=$(node_snapshot 2>>"$temporary/node-reboot-observation.log"); then
			ready=$(printf '%s\n' "$snapshot" | jq -er '[.status.conditions[] | select(.type == "Ready")][0].status' 2>>"$temporary/node-reboot-observation.log" || printf Unknown)
			boot_id=$(printf '%s\n' "$snapshot" | jq -er '.status.nodeInfo.bootID' 2>>"$temporary/node-reboot-observation.log" || printf Unknown)
			if [ "$ready" = True ] && [ "$boot_id" != "$node_boot_id_before" ]; then
				printf '%s\n' "$snapshot" >"$temporary/node-after-reboot-candidate.json"
				if validate_node_snapshot "$temporary/node-after-reboot-candidate.json" True &&
					[ "$(jq -er '.metadata.uid' "$temporary/node-after-reboot-candidate.json")" = "$node_uid_before" ]; then
					stable_observations=$((stable_observations + 1))
					if [ "$stable_observations" -ge 5 ]; then
						mv "$temporary/node-after-reboot-candidate.json" "$temporary/node-after-reboot.json"
						break
					fi
				else
					stable_observations=0
				fi
			else
				stable_observations=0
			fi
		fi
		iteration=$((iteration + 1))
		sleep 1
	done
	[ -f "$temporary/node-after-reboot.json" ] || fail "Worker 1 did not return Ready with a new boot ID"
	validate_node_snapshot "$temporary/node-after-reboot.json" True ||
		fail "Worker 1 identity or GPU capacity changed across reboot"
	[ "$(jq -er '.metadata.uid' "$temporary/node-after-reboot.json")" = "$node_uid_before" ] ||
		fail "Worker 1 Kubernetes node UID changed across reboot"
	node_boot_id_after=$(jq -er '.status.nodeInfo.bootID' "$temporary/node-after-reboot.json")
	[ "$node_boot_id_after" != "$node_boot_id_before" ] || fail "Worker 1 boot ID did not change"
	reboot_observed=true
	printf 'observed_at=%s action_id=%s node=%s node_uid=%s boot_id_before=%s boot_id_after=%s production_gates=0/9\n' \
		"$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$action_id" "$worker1_node" "$node_uid_before" \
		"$node_boot_id_before" "$node_boot_id_after" >"$temporary/REBOOT_OBSERVED"
}

restore_rehearsal_worker() {
	worker_id=$1
	state=$(query_database "SELECT lifecycle_state || '|' || reachability_condition FROM workers WHERE id = '$worker_id'::uuid AND epoch = 1;")
	case "$state" in
		"READY|HEALTHY") return 0 ;;
		"DRAINING|HEALTHY" | "DRAINING|OFFLINE") ;;
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
	recovery_result=0
	exec 9>"$temporary/recovery.lock"
	if ! flock -n 9; then
		printf 'another recovery owner holds the environment lock\n' >>"$log_file"
		recovering=false
		exec 9>&-
		return 1
	fi
	if [ -z "$job_id" ] && [ -f "$temporary/job-id.txt" ]; then
		candidate=$(cat "$temporary/job-id.txt")
		if printf '%s\n' "$candidate" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'; then
			job_id=$candidate
		else
			printf 'invalid recovery Job identity file\n' >>"$log_file"
			recovery_result=1
		fi
	fi
	if [ -z "$runner_container_id" ] && [ -f "$temporary/runner-container-id.txt" ]; then
		runner_container_id=$(cat "$temporary/runner-container-id.txt")
	fi
	neutralize_active_fault_pod "$log_file" || recovery_result=1
	if [ -z "$job_id" ] && [ -f "$temporary/database-started-at.txt" ]; then
		database_started_at=$(cat "$temporary/database-started-at.txt")
		candidates=$(query_database "SELECT id FROM jobs WHERE created_at >= '$database_started_at'::timestamptz AND state IN ('QUEUED','ASSIGNED','RUNNING','FINALIZING','RETRY_WAIT','CANCELING') ORDER BY created_at;" 2>>"$log_file" || true)
		candidate_count=$(printf '%s\n' "$candidates" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')
		case "$candidate_count" in
			0) ;;
			1) job_id=$candidates ;;
			*) printf 'multiple active Jobs appeared inside the recovery window\n' >>"$log_file"; recovery_result=1 ;;
		esac
	fi
	wait_for_node_ready "$log_file" || recovery_result=1
	if [ -f "$temporary/REBOOT_OBSERVED" ]; then
		reboot_observed=true
	elif [ -f "$temporary/node-before-reboot.json" ]; then
		before_boot=$(jq -er '.status.nodeInfo.bootID' "$temporary/node-before-reboot.json" 2>>"$log_file" || printf Unknown)
		current_boot=$(node_snapshot 2>>"$log_file" | jq -er '.status.nodeInfo.bootID' 2>>"$log_file" || printf Unknown)
		if [ "$before_boot" != Unknown ] && [ "$current_boot" != Unknown ] && [ "$current_boot" != "$before_boot" ]; then
			reboot_observed=true
			printf 'recovery_observed_at=%s boot_id_before=%s boot_id_after=%s\n' \
				"$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$before_boot" "$current_boot" >"$temporary/REBOOT_OBSERVED_DURING_RECOVERY"
		fi
	fi
	mode=$(current_mock_mode "$worker1_node" 2>>"$log_file" || true)
	case "$mode" in
		hang) switch_mock_mode "$worker1_node" hang success "$temporary/mode-recovery.log" >>"$log_file" 2>&1 || recovery_result=1 ;;
		success) ;;
		*) printf 'unexpected Worker 1 mode during recovery: %s\n' "$mode" >>"$log_file"; recovery_result=1 ;;
	esac
	if [ -n "$job_id" ] && [ "$(query_database "SELECT count(*) FROM attempt_leases AS lease JOIN attempts AS attempt ON attempt.id = lease.attempt_id WHERE attempt.job_id = '$job_id'::uuid AND lease.revoked_at IS NULL;" 2>>"$log_file" || printf unknown)" != 0 ]; then
		if [ "$reboot_observed" != true ]; then
			if ! printf '%s\n' "$runner_container_id" | grep -Eq '^[0-9a-f]{64}$'; then
				printf 'Runner container identity is unavailable for pre-reboot recovery\n' >>"$log_file"
				recovery_result=1
			elif printf 'started_at=%s reason=HARNESS_RECOVERY_BEFORE_NODE_REBOOT\n' \
				"$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$temporary/RECOVERY_PROCESS_KILL_STARTED" &&
				run_fault_pod "$temporary/recovery-process-kill.log" >>"$log_file" 2>&1; then
				printf 'completed_at=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$temporary/RECOVERY_PROCESS_KILL_COMPLETED"
			else
				recovery_result=1
			fi
		fi
		iteration=0
		while [ "$iteration" -lt 300 ]; do
			[ "$(active_lease_count 2>>"$log_file" || printf unknown)" = 0 ] && break
			iteration=$((iteration + 1))
			sleep 1
		done
		[ "$(active_lease_count 2>>"$log_file" || printf unknown)" = 0 ] || recovery_result=1
	fi
	[ "$(active_lease_count 2>>"$log_file" || printf unknown)" = 0 ] || recovery_result=1
	[ "$($kubectl_bin get pods --namespace "$namespace" -l app.kubernetes.io/name=vela-lab-node-reboot-recovery-injector -o json 2>>"$log_file" | jq '[.items[] | select(.status.phase != "Succeeded" and .status.phase != "Failed")] | length' 2>>"$log_file" || printf unknown)" = 0 ] || recovery_result=1
	$kubectl_bin rollout status deployment/vela-lab-control --namespace "$namespace" --timeout=90s >>"$log_file" 2>&1 || recovery_result=1
	$kubectl_bin rollout status deployment/vela-lab-worker-agent-1 --namespace "$namespace" --timeout=90s >>"$log_file" 2>&1 || recovery_result=1
	$kubectl_bin rollout status deployment/vela-lab-worker-agent-2 --namespace "$namespace" --timeout=90s >>"$log_file" 2>&1 || recovery_result=1
	restore_rehearsal_worker "$worker1_id" >>"$log_file" 2>&1 || recovery_result=1
	restore_rehearsal_worker "$worker2_id" >>"$log_file" 2>&1 || recovery_result=1
	[ "$(query_database "SELECT count(*) FROM workers WHERE id IN ('$worker1_id'::uuid, '$worker2_id'::uuid) AND epoch = 1 AND lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY';" 2>>"$log_file" || printf unknown)" = 2 ] || recovery_result=1
	if [ -n "$job_id" ]; then
		iteration=0
		while [ "$iteration" -lt 300 ]; do
			state=$(query_database "SELECT state FROM jobs WHERE id = '$job_id'::uuid;" 2>>"$log_file" || printf UNKNOWN)
			case "$state" in SUCCEEDED | FAILED | CANCELED) break ;; esac
			iteration=$((iteration + 1))
			sleep 1
		done
		case "$state" in SUCCEEDED | FAILED | CANCELED) ;; *) recovery_result=1 ;; esac
		[ "$(active_lease_count 2>>"$log_file" || printf unknown)" = 0 ] || recovery_result=1
	fi
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
	cleanup_result=$?
	trap - EXIT HUP INT TERM
	if [ "$committed" != true ] && [ -n "$temporary" ] && [ -d "$temporary" ]; then
		printf 'status=INCOMPLETE production_gates=0/9\n' >"$temporary/STATUS"
		if recover_environment "$temporary/cleanup.log"; then
			disarm_watchdog
		else
			printf 'node-reboot: immediate recovery incomplete; armed watchdog retained\n' >&2
		fi
		printf 'node-reboot: diagnostic receipt preserved at %s\n' "$temporary" >&2
	fi
	exit "$cleanup_result"
}
trap cleanup EXIT HUP INT TERM

if [ "${1:-}" = --render-recovery-pod ]; then
	trap - EXIT HUP INT TERM
	tool_image=${2:-}
	[ "$tool_image" = "$expected_tool_image" ] ||
		fail "usage: $0 --render-recovery-pod $expected_tool_image"
	fault_pod_json vela-lab-node-reboot-render "$worker1_node" \
		0000000000000000000000000000000000000000000000000000000000000000 \
		00000000-0000-0000-0000-000000000000
	exit 0
fi

if [ "${1:-}" = --watchdog ]; then
	trap - EXIT HUP INT TERM
	manifests=${2:-}
	temporary=${3:-}
	watchdog_marker=${4:-}
	[ -n "$manifests" ] && [ -d "$temporary" ] && [ -f "$watchdog_marker" ] || exit 0
	watchdog_heartbeat=$temporary/WATCHDOG_HEARTBEAT
	export KUBECONFIG="$kubeconfig"
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
for command in flock jq sha256sum; do command -v "$command" >/dev/null 2>&1 || fail "$command is required"; done
export KUBECONFIG="$kubeconfig"
temporary=$(mktemp -d "$(dirname "$output")/.vela-lab-node-reboot.XXXXXX")
chmod 0700 "$temporary"
load_tool_identity
validate_previous_scenario_receipt

$kubectl_bin rollout status deployment/vela-lab-control --namespace "$namespace" --timeout=60s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-worker-agent-1 --namespace "$namespace" --timeout=60s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-worker-agent-2 --namespace "$namespace" --timeout=60s >/dev/null
[ "$($kubectl_bin get pods --namespace "$namespace" -l app.kubernetes.io/name=vela-lab-node-reboot-recovery-injector -o json | jq '[.items[] | select(.status.phase != "Succeeded" and .status.phase != "Failed")] | length')" -eq 0 ] ||
	fail "an active node-reboot recovery Pod already exists"
[ "$($kubectl_bin get pods --namespace "$namespace" -l app.kubernetes.io/name=vela-lab-node-reboot-runner-control -o json | jq '[.items[] | select(.status.phase != "Succeeded" and .status.phase != "Failed")] | length')" -eq 0 ] ||
	fail "an active node-reboot Runner-control Pod already exists"

global_before=$(query_database "
SELECT
  (SELECT count(*) FROM attempt_leases WHERE revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts),
  (SELECT count(*) FROM jobs WHERE state IN ('QUEUED','ASSIGNED','RUNNING','FINALIZING','RETRY_WAIT','CANCELING')),
  (SELECT count(*) FROM workers WHERE lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY'),
  (SELECT count(*) FROM service_class_revisions WHERE id = '$service_class_id'::uuid AND state = 'ACTIVE' AND max_attempts = 2),
  (SELECT count(*) FROM generation_preset_revisions WHERE id = '$replacement_preset_revision_id'::uuid AND state = 'ACTIVE' AND certified_p95_compute_seconds = 120),
  (SELECT count(*) FROM profile_certifications WHERE id = '$replacement_certification_id'::uuid AND state = 'ACTIVE' AND invalidated_at IS NULL);")
printf '%s\n' "$global_before" >"$temporary/global-before.txt"
[ "$global_before" = '0|0|0|2|1|1|1' ] ||
	fail "global preflight does not preserve the idle two-Worker lab authority boundary"

$kubectl_bin get nodes -o json >"$temporary/nodes-before.json"
node_snapshot >"$temporary/node-preflight.json"
validate_node_snapshot "$temporary/node-preflight.json" True ||
	fail "Worker 1 node preflight does not match the fixed Ready eight-GPU target"
$kubectl_bin get deployment --namespace "$namespace" vela-lab-control -o json >"$temporary/control-deployment-before.json"
[ "$(current_mock_mode "$worker1_node")" = success ] || fail "Worker 1 mock Runner is not in success mode"
[ "$(current_mock_mode "$worker2_node")" = success ] || fail "Worker 2 mock Runner is not in success mode"
current_runner_profiles "$worker1_node" >"$temporary/runner-profiles-worker-1-before.json"
validate_runner_profiles "$temporary/runner-profiles-worker-1-before.json" || fail "Worker 1 Runner profile allowlist is invalid"
current_runner_profiles "$worker2_node" >"$temporary/runner-profiles-worker-2-before.json"
validate_runner_profiles "$temporary/runner-profiles-worker-2-before.json" || fail "Worker 2 Runner profile allowlist is invalid"
runner_container_id=$(current_runner_container_id "$worker1_node") || fail "Worker 1 Runner container identity read failed"
printf '%s\n' "$runner_container_id" | grep -Eq '^[0-9a-f]{64}$' || fail "Worker 1 Runner container identity is invalid"
printf '%s\n' "$runner_container_id" >"$temporary/runner-container-id.txt"
printf 'path=%s\nsha256=%s\njob_id=%s\n' \
	"$previous_scenario_receipt" "$previous_scenario_receipt_sha256" "$previous_scenario_job_id" \
	>"$temporary/previous-scenario-receipt.txt"

watchdog_marker=$temporary/WATCHDOG_ARMED
watchdog_heartbeat=$temporary/WATCHDOG_HEARTBEAT
printf 'armed_at=%s timeout_seconds=300\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$watchdog_marker"
heartbeat
nohup "$0" --watchdog "$manifests" "$temporary" "$watchdog_marker" \
	>"$temporary/watchdog.log" 2>&1 &
watchdog_pid=$!
printf '%s\n' "$watchdog_pid" >"$temporary/watchdog.pid"

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
switch_mock_mode "$worker1_node" success hang "$temporary/mode-success-to-hang.log" ||
	fail "Worker 1 could not enter deterministic hang mode"

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

running=
iteration=0
while [ "$iteration" -lt 60 ]; do
	heartbeat
	running=$(query_database "
SELECT attempt.id, attempt.state, attempt.fence, lease.id, lease.expires_at
FROM attempts AS attempt
JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id AND lease.revoked_at IS NULL
WHERE attempt.job_id = '$job_id'::uuid AND attempt.worker_id = '$worker1_id'::uuid;")
	case "$running" in *'|RUNNING|'*) break ;; esac
	job_state=$(query_database "SELECT state FROM jobs WHERE id = '$job_id'::uuid;")
	case "$job_state" in FAILED | CANCELED | SUCCEEDED) fail "application Job reached terminal state $job_state before fault injection" ;; esac
	iteration=$((iteration + 1))
	sleep 1
done
case "$running" in *'|RUNNING|'*) ;; *) fail "Worker 1 did not enter RUNNING" ;; esac
printf '%s\n' "$running" >"$temporary/attempt-running-before-reboot.txt"
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

switch_mock_mode "$worker1_node" hang success "$temporary/mode-hang-to-success.log" ||
	fail "Worker 1 could not arm success-mode recovery"
restore_rehearsal_worker "$worker2_id" || fail "Worker 2 could not be restored before node reboot"
original_attempt=$(printf '%s\n' "$running" | cut -d '|' -f 1)
original_fence=$(printf '%s\n' "$running" | cut -d '|' -f 3)
printf '%s\n' "$original_attempt" | grep -Eq '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' ||
	fail "original Attempt identity is invalid"
printf '%s\n' "$original_fence" | grep -Eq '^[1-9][0-9]*$' || fail "original Attempt fence is invalid"
reboot_started_at=$(query_database 'SELECT clock_timestamp();')
printf '%s\n' "$reboot_started_at" >"$temporary/reboot-started-at.txt"
wait_for_node_reboot "$original_attempt" "$original_fence"
[ "$reboot_observed" = true ] || fail "node reboot observation did not complete"
$kubectl_bin rollout status deployment/vela-lab-worker-agent-1 --namespace "$namespace" --timeout=180s >/dev/null
runner_container_id_after=$(current_runner_container_id "$worker1_node") || fail "Worker 1 Runner identity was unavailable after reboot"
[ "$runner_container_id_after" = "$runner_container_id" ] || fail "managed Runner container identity changed across reboot"
printf '%s\n' "$runner_container_id_after" >"$temporary/runner-container-id-after.txt"

terminal=
iteration=0
while [ "$iteration" -lt 360 ]; do
	heartbeat
	terminal=$(query_database "
SELECT
  job.state,
  retry.attempts_started,
  (SELECT count(*) FROM attempts WHERE job_id = job.id),
  (SELECT count(*) FROM attempts WHERE job_id = job.id AND state = 'SUCCEEDED'),
  (SELECT count(*) FROM attempts WHERE job_id = job.id AND state = 'FAILED'),
  (SELECT count(*) FROM attempts WHERE job_id = job.id AND state = 'LOST'),
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
		'SUCCEEDED|2|2|1|0|1|1|1|2|2|0|0') break ;;
		SUCCEEDED'|'*) fail "application Job succeeded with an unexpected authority shape" ;;
		FAILED'|'* | CANCELED'|'*) fail "application Job reached terminal failure after node reboot" ;;
	esac
	iteration=$((iteration + 1))
	sleep 1
done
[ "$terminal" = 'SUCCEEDED|2|2|1|0|1|1|1|2|2|0|0' ] ||
	fail "application Job did not converge to the exact node-reboot authority shape"

restore_rehearsal_worker "$worker1_id" || fail "Worker 1 could not be restored after node-reboot recovery"
restore_rehearsal_worker "$worker2_id" || fail "Worker 2 could not be restored after node-reboot recovery"
[ "$(query_database "SELECT count(*) FROM workers WHERE id IN ('$worker1_id'::uuid, '$worker2_id'::uuid) AND epoch = 1 AND lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY';")" = 2 ] ||
	fail "both Workers did not return to READY/HEALTHY"
[ "$(current_mock_mode "$worker1_node")" = success ] || fail "Worker 1 did not remain in success mode"
[ "$(current_mock_mode "$worker2_node")" = success ] || fail "Worker 2 did not remain in success mode"

if ! $kubectl_bin wait --namespace "$namespace" "$job_resource" --for=condition=complete --timeout=90s >/dev/null; then
	$kubectl_bin describe --namespace "$namespace" "$job_resource" >"$temporary/smoke-job-describe.txt" 2>&1 || true
	$kubectl_bin logs --namespace "$namespace" "$job_resource" >"$temporary/smoke-job.log" 2>&1 || true
	fail "smoke wrapper did not complete after application success"
fi
heartbeat
$kubectl_bin logs --namespace "$namespace" "$job_resource" >"$temporary/smoke-job.log"
jq -Rrc 'fromjson? | select(.status == "LAB VERIFIED")' "$temporary/smoke-job.log" >"$temporary/smoke-receipt-candidates.jsonl"
[ "$(wc -l <"$temporary/smoke-receipt-candidates.jsonl" | tr -d ' ')" = 1 ] || fail "smoke wrapper emitted a non-unique LAB VERIFIED receipt"
jq -s '.[0]' "$temporary/smoke-receipt-candidates.jsonl" >"$temporary/smoke-receipt.json"
rm -f -- "$temporary/smoke-receipt-candidates.jsonl"
jq -e --arg job_id "$job_id" '
  .job_id == $job_id
  and .final_state == "SUCCEEDED"
  and .artifact_count == 2
  and (.artifact_kinds | sort) == ["THUMBNAIL","VIDEO"]
' "$temporary/smoke-receipt.json" >/dev/null || fail "smoke receipt does not match the rehearsed Job"

current_runner_profiles "$worker1_node" >"$temporary/runner-profiles-worker-1-after.json"
validate_runner_profiles "$temporary/runner-profiles-worker-1-after.json" || fail "Worker 1 Runner profile changed"
current_runner_profiles "$worker2_node" >"$temporary/runner-profiles-worker-2-after.json"
validate_runner_profiles "$temporary/runner-profiles-worker-2-after.json" || fail "Worker 2 Runner profile changed"
$kubectl_bin get deployment --namespace "$namespace" vela-lab-control -o json >"$temporary/control-deployment-after.json"

query_database "
SELECT jsonb_build_object(
  'job_id', job.id, 'job_state', job.state, 'current_fence', job.current_fence,
  'attempts_started', retry.attempts_started,
  'credit_reservation_state', reservation.state,
  'attempts', COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'attempt_id', attempt.id, 'attempt_number', attempt.attempt_number,
    'worker_id', attempt.worker_id, 'state', attempt.state, 'fence', attempt.fence,
    'failure_class', (SELECT decision.failure_class
      FROM execution_failure_decisions AS decision
      WHERE decision.attempt_id = attempt.id
      ORDER BY decision.decided_at DESC, decision.id
      LIMIT 1),
    'started_at', attempt.started_at, 'ended_at', attempt.ended_at,
    'lease_expires_at', lease.expires_at, 'lease_revoked_at', lease.revoked_at
  ) ORDER BY attempt.attempt_number)
  FROM attempts AS attempt LEFT JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id
  WHERE attempt.job_id = job.id), '[]'::jsonb),
  'visible_completion_count', (SELECT count(*) FROM visible_completions WHERE job_id = job.id),
  'posted_charge_count', (SELECT count(*) FROM charges WHERE job_id = job.id AND reason = 'VISIBLE_COMPLETION' AND state = 'POSTED'),
  'artifact_count', (SELECT count(*) FROM artifacts WHERE job_id = job.id),
  'committed_artifact_count', (SELECT count(*) FROM artifacts WHERE job_id = job.id AND state = 'COMMITTED')
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
	(SELECT count(*) FROM attempts AS attempt
	 WHERE attempt.id = '$original_attempt'::uuid
	   AND attempt.job_id = job.id
	   AND attempt.worker_id = '$worker1_id'::uuid
	   AND attempt.state = 'LOST'
	   AND attempt.fence = $original_fence
	   AND EXISTS (
	     SELECT 1 FROM execution_failure_decisions AS decision
	     WHERE decision.attempt_id = attempt.id
	       AND decision.failure_class = 'WORKER_LOST')),
	(SELECT count(*) FROM attempts AS attempt
	 WHERE attempt.job_id = job.id
	   AND attempt.worker_id = '$worker2_id'::uuid
	   AND attempt.state = 'SUCCEEDED'
	   AND attempt.fence > $original_fence),
  (SELECT count(*) FROM attempt_leases AS lease JOIN attempts AS attempt ON attempt.id = lease.attempt_id WHERE attempt.job_id = job.id AND lease.revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts),
  (SELECT count(*) FROM workers WHERE lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY')
FROM jobs AS job WHERE job.id = '$job_id'::uuid;")
printf '%s\n' "$measurements" >"$temporary/measurements.txt"
[ "$measurements" = '0|0|0|0|1|1|0|0|2' ] || fail "final measurements do not satisfy the node-reboot lab contract"

attempts_started=$(printf '%s\n' "$terminal" | cut -d '|' -f 2)
[ "$attempts_started" = 2 ] || fail "node reboot did not start exactly two Attempts"

exec 8>"$temporary/recovery.lock"
flock 8
disarm_watchdog
if [ -f "$temporary/WATCHDOG_RECOVERY_STARTED" ]; then
	flock -u 8
	exec 8>&-
	fail "watchdog recovery started; refusing to publish a PASS receipt"
fi
[ -f "$temporary/REBOOT_OBSERVED" ] || {
	flock -u 8
	exec 8>&-
	fail "reboot observation is absent; refusing to publish a PASS receipt"
}
[ ! -e "$temporary/RECOVERY_PROCESS_KILL_STARTED" ] || {
	flock -u 8
	exec 8>&-
	fail "recovery process kill was used; refusing to publish a node-reboot PASS"
}
flock -u 8
exec 8>&-
rm -f -- "$temporary/recovery.lock"

completed_at=$(query_database 'SELECT clock_timestamp();')
jq \
	--arg job_id "$job_id" \
	--arg started_at "$reboot_started_at" \
	--arg completed_at "$completed_at" \
	--arg previous_receipt_sha256 "$previous_scenario_receipt_sha256" '
  (.scenarios[] | select(.id == "node-reboot")) = {
    id: "node-reboot",
    status: "LAB_REHEARSAL_PASS",
    job_id: $job_id,
    started_at: $started_at,
    completed_at: $completed_at,
    fault: "NODE_REBOOT_WITH_KUBERNETES_BOOT_ID_CHANGE",
    previous_receipt_sha256: $previous_receipt_sha256
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
  and ([.scenarios[] | select(.status == "LAB_REHEARSAL_PASS")] | length) == 8
  and any(.scenarios[]; .id == "node-reboot" and .job_id == $job_id and .status == "LAB_REHEARSAL_PASS")
' "$temporary/scenario-matrix.json" >/dev/null || fail "eight-scenario matrix is invalid"

harness_sha256=$(sha256sum "$0" | awk '{print $1}')
jq -n \
	--arg job_id "$job_id" \
	--arg started_at "$reboot_started_at" \
	--arg completed_at "$completed_at" \
	--arg harness_sha256 "$harness_sha256" \
	--arg previous_receipt_sha256 "$previous_scenario_receipt_sha256" \
	--arg action_id "$action_id" \
	--arg node "$worker1_node" \
	--arg node_uid "$node_uid_before" \
	--arg internal_ip "$worker1_internal_ip" \
	--arg boot_id_before "$node_boot_id_before" \
	--arg boot_id_after "$node_boot_id_after" \
	--arg unready_observed_at "$unready_observed_at" \
	--arg unready_status "$unready_status" \
	--arg runner_container_id "$runner_container_id" \
	--arg original_attempt "$original_attempt" \
	--argjson attempts_started "$attempts_started" \
	--argjson original_fence "$original_fence" '
  {
    schema: "vela-lab-node-reboot-v1",
    status: "LAB_REHEARSAL_PASS",
    evidence_boundary: "NON_PRODUCTION_MOCK_REHEARSAL",
    production_gates: "0/9",
	fixed_scenarios_completed: 8,
    fixed_scenarios_required: 10,
    job_id: $job_id,
    started_at: $started_at,
    completed_at: $completed_at,
    harness_sha256: $harness_sha256,
    previous_receipt_sha256: $previous_receipt_sha256,
	reboot: {
	  kind: "NODE_REBOOT_WITH_KUBERNETES_BOOT_ID_CHANGE",
	  action_provenance: "OPERATOR_OUT_OF_BAND_REBOOT_OBSERVED_BY_KUBERNETES",
	  action_id: $action_id,
	  node: $node,
	  node_uid: $node_uid,
	  internal_ip: $internal_ip,
	  boot_id_before: $boot_id_before,
	  boot_id_after: $boot_id_after,
	  unavailable_observed: true,
	  unavailable_observed_at: $unready_observed_at,
	  unavailable_ready_status: $unready_status,
	  runner_container_id_before: $runner_container_id,
	  runner_container_id_after: $runner_container_id
	},
	original_authority: {attempt_id:$original_attempt,fence:$original_fence},
    attempts_started: $attempts_started,
    visible_completions: 1,
    posted_charges: 1,
    artifact_rows: 2,
    committed_artifacts: 2,
    measurements: {
      "lost-accepted-job-count": 0,
      "duplicate-visible-completion-count": 0,
      "duplicate-charge-count": 0,
      "stale-authority-acceptance-count": 0
    },
    artifacts: [
      "scenario-matrix.json",
	  "previous-scenario-receipt.txt",
	  "reboot-action-intent.json",
	  "ACTION_REQUIRED",
	  "REBOOT_OBSERVED",
	  "node-preflight.json",
	  "node-before-reboot.json",
	  "node-unready-observation.txt",
	  "node-after-reboot.json",
	  "node-reboot-observation.log",
	  "attempt-running-before-reboot.txt",
	  "runner-container-id.txt",
	  "runner-container-id-after.txt",
      "authority-before.json",
      "authority-after.json",
      "raw-event-payloads.jsonl",
      "smoke-receipt.json"
    ]
  }
' >"$temporary/summary.json"
jq -e \
	--arg job_id "$job_id" \
	--arg action_id "$action_id" \
	--arg node_uid "$node_uid_before" \
	--arg boot_id_before "$node_boot_id_before" \
	--arg boot_id_after "$node_boot_id_after" '
  .schema == "vela-lab-node-reboot-v1"
  and .status == "LAB_REHEARSAL_PASS"
  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
  and .production_gates == "0/9"
  and .fixed_scenarios_completed == 8
  and .fixed_scenarios_required == 10
  and .job_id == $job_id
  and .reboot.action_id == $action_id
  and .reboot.node_uid == $node_uid
  and .reboot.boot_id_before == $boot_id_before
  and .reboot.boot_id_after == $boot_id_after
  and .reboot.boot_id_after != .reboot.boot_id_before
  and .reboot.unavailable_observed == true
  and .original_authority.attempt_id != null
  and .attempts_started == 2
  and .visible_completions == 1
  and .posted_charges == 1
  and .artifact_rows == 2
  and .committed_artifacts == 2
  and all(.measurements[]; . == 0)
' "$temporary/summary.json" >/dev/null || fail "node reboot summary is invalid"
printf 'status=LAB_REHEARSAL_PASS production_gates=0/9 fixed_scenarios=8/10\n' >"$temporary/STATUS"

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
printf 'schema=vela-lab-node-reboot-wrapper-v1 output=%s job=%s result=LAB_REHEARSAL_PASS fixed_scenarios=8/10 production_gates=0/9\n' \
	"$output" "$job_id"

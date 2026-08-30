#!/bin/sh

set -eu

namespace=vela-lab
worker1_id=84000000-0000-0000-0000-000000000101
worker2_id=84000000-0000-0000-0000-000000000102
worker1_node=vela-lab-worker-1
worker2_node=vela-lab-worker-2
replacement_preset_revision_id=84000000-0000-0000-0000-000000000201
replacement_certification_id=84000000-0000-0000-0000-000000000202
service_class_id=84000000-0000-0000-0000-000000000009
expected_runner_image=10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3
expected_tool_image=10.1.200.17:5443/vela-lab/vela-lab-bootstrap@sha256:3f6a8bc440ee7bd7f9ba263d07435329c0863134217349db565cded2e9df9eac
expected_old_preset_revision_id=84000000-0000-0000-0000-000000000005
previous_scenario_job_id=edc4a45a-5587-4b58-b423-77538041c03b
previous_scenario_receipt_sha256=13d6cb09fcdc7522c54f668019d809b462b3700cc2cabd676b1891324110b8bc
previous_scenario_harness_sha256=b39652e15234f37cf9096f3a7268cfd1b2d830594b4ea4863d9eb9aefbdb132b
previous_scenario_receipt=/root/vela-lab-deploy-bc590e20/receipts/retry-budget-exhaustion-v2
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}
temporary=
output=
manifests=
job_id=
committed=false
recovering=false
fault_completed=false
watchdog_marker=
watchdog_heartbeat=
watchdog_pid=
tool_image=
runner_container_id=

fail() {
	printf 'process-kill: %s\n' "$*" >&2
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

claim_fault_injection() {
	owner=$1
	if ! mkdir "$temporary/FAULT_INJECTION_CLAIM" 2>/dev/null; then
		return 1
	fi
	printf 'owner=%s claimed_at=%s\n' "$owner" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$temporary/FAULT_INJECTION_OWNER"
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
	[ "$(cat "$previous_scenario_receipt/STATUS")" = 'status=LAB_REHEARSAL_PASS production_gates=0/9 fixed_scenarios=2/10' ] ||
		fail "previous fixed-scenario STATUS is not a two-scenario lab pass"
	jq -e \
		--arg job_id "$previous_scenario_job_id" \
		--arg harness "$previous_scenario_harness_sha256" '
	  .schema == "vela-lab-retry-budget-exhaustion-v1"
	  and .status == "LAB_REHEARSAL_PASS"
	  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
	  and .production_gates == "0/9"
	  and .fixed_scenarios_completed == 2
	  and .fixed_scenarios_required == 10
	  and .job_id == $job_id
	  and .harness_sha256 == $harness
	  and .visible_completions == 0
	  and .charges == 0
	  and .artifact_rows == 0
	  and .committed_artifacts == 0
	' "$previous_scenario_receipt/summary.json" >/dev/null ||
		fail "previous fixed-scenario summary does not match the pinned evidence"
	jq -e \
		--arg retry_job_id "$previous_scenario_job_id" '
	  .schema == "vela-lab-fault-scenario-matrix-v1"
	  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
	  and .production_gates == "0/9"
	  and (.scenarios | type == "array" and length == 10)
	  and ([.scenarios[] | select(.status == "LAB_REHEARSAL_PASS")] | length) == 2
	  and any(.scenarios[];
	    .id == "worker-control-network-partition" and .status == "LAB_REHEARSAL_PASS")
	  and any(.scenarios[];
	    .id == "retry-budget-exhaustion"
	    and .status == "LAB_REHEARSAL_PASS"
	    and .job_id == $retry_job_id)
	  and any(.scenarios[];
	    .id == "process-kill" and .status == "NOT_RUN")
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
	        "app.kubernetes.io/name": "vela-lab-process-kill-runner-control",
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
printf "schema=vela-lab-process-kill-mode-v1 before=%s after=%s production_gates=0/9\n" "$current" "$REQUESTED_MODE"' \
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
    "schema=vela-lab-runner-process-kill-v1 signal=SIGKILL "
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
	        "app.kubernetes.io/name": "vela-lab-process-kill-injector",
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
	printf '%s\n' "$name" | grep -Eq '^vela-lab-process-kill-[0-9]+-[0-9]+$' || {
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
      and .metadata.labels["app.kubernetes.io/name"] == "vela-lab-process-kill-injector"
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
	name=vela-lab-process-kill-$(date +%s)-$$
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
	printf '%s\n' "$runner_container_id" | grep -Eq '^[0-9a-f]{64}$' || recovery_result=1
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
	mode=$(current_mock_mode "$worker1_node" 2>>"$log_file" || true)
	case "$mode" in
		hang) switch_mock_mode "$worker1_node" hang success "$temporary/mode-recovery.log" >>"$log_file" 2>&1 || recovery_result=1 ;;
		success) ;;
		*) printf 'unexpected Worker 1 mode during recovery: %s\n' "$mode" >>"$log_file"; recovery_result=1 ;;
	esac
	if [ -n "$job_id" ] && [ "$(query_database "SELECT count(*) FROM attempt_leases AS lease JOIN attempts AS attempt ON attempt.id = lease.attempt_id WHERE attempt.job_id = '$job_id'::uuid AND lease.revoked_at IS NULL;" 2>>"$log_file" || printf unknown)" != 0 ]; then
		if [ "$fault_completed" != true ] && [ -f "$temporary/FAULT_INJECTION_COMPLETED" ]; then
			fault_completed=true
		fi
		if [ "$fault_completed" != true ] && claim_fault_injection recovery; then
			printf 'started_at=%s recovery=true\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$temporary/FAULT_INJECTION_STARTED"
			run_fault_pod "$temporary/recovery-fault-injection.log" >>"$log_file" 2>&1 || recovery_result=1
		elif [ "$fault_completed" != true ]; then
			printf 'fault injection ownership is already claimed; refusing duplicate SIGKILL\n' >>"$log_file"
			recovery_result=1
		fi
		iteration=0
		while [ "$iteration" -lt 180 ]; do
			[ "$(active_lease_count 2>>"$log_file" || printf unknown)" = 0 ] && break
			iteration=$((iteration + 1))
			sleep 1
		done
		[ "$(active_lease_count 2>>"$log_file" || printf unknown)" = 0 ] || recovery_result=1
	fi
	[ "$(active_lease_count 2>>"$log_file" || printf unknown)" = 0 ] || recovery_result=1
	[ "$($kubectl_bin get pods --namespace "$namespace" -l app.kubernetes.io/name=vela-lab-process-kill-injector -o json 2>>"$log_file" | jq '[.items[] | select(.status.phase != "Succeeded" and .status.phase != "Failed")] | length' 2>>"$log_file" || printf unknown)" = 0 ] || recovery_result=1
	$kubectl_bin rollout status deployment/vela-lab-control --namespace "$namespace" --timeout=90s >>"$log_file" 2>&1 || recovery_result=1
	$kubectl_bin rollout status deployment/vela-lab-worker-agent-1 --namespace "$namespace" --timeout=90s >>"$log_file" 2>&1 || recovery_result=1
	$kubectl_bin rollout status deployment/vela-lab-worker-agent-2 --namespace "$namespace" --timeout=90s >>"$log_file" 2>&1 || recovery_result=1
	restore_rehearsal_worker "$worker1_id" >>"$log_file" 2>&1 || recovery_result=1
	restore_rehearsal_worker "$worker2_id" >>"$log_file" 2>&1 || recovery_result=1
	[ "$(query_database "SELECT count(*) FROM workers WHERE id IN ('$worker1_id'::uuid, '$worker2_id'::uuid) AND epoch = 1 AND lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY';" 2>>"$log_file" || printf unknown)" = 2 ] || recovery_result=1
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
			printf 'process-kill: immediate recovery incomplete; armed watchdog retained\n' >&2
		fi
		printf 'process-kill: diagnostic receipt preserved at %s\n' "$temporary" >&2
	fi
	exit "$cleanup_result"
}
trap cleanup EXIT HUP INT TERM

if [ "${1:-}" = --render-fault-pod ]; then
	trap - EXIT HUP INT TERM
	tool_image=${2:-}
	[ "$tool_image" = "$expected_tool_image" ] ||
		fail "usage: $0 --render-fault-pod $expected_tool_image"
	fault_pod_json vela-lab-process-kill-render "$worker1_node" \
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
temporary=$(mktemp -d "$(dirname "$output")/.vela-lab-process-kill.XXXXXX")
chmod 0700 "$temporary"
load_tool_identity
validate_previous_scenario_receipt

$kubectl_bin rollout status deployment/vela-lab-control --namespace "$namespace" --timeout=60s >/dev/null
$kubectl_bin rollout status deployment/vela-lab-worker-agent-1 --namespace "$namespace" --timeout=60s >/dev/null
	$kubectl_bin rollout status deployment/vela-lab-worker-agent-2 --namespace "$namespace" --timeout=60s >/dev/null
	[ "$($kubectl_bin get pods --namespace "$namespace" -l app.kubernetes.io/name=vela-lab-process-kill-injector -o json | jq '[.items[] | select(.status.phase != "Succeeded" and .status.phase != "Failed")] | length')" -eq 0 ] ||
		fail "an active process-kill fault-injector Pod already exists"
	[ "$($kubectl_bin get pods --namespace "$namespace" -l app.kubernetes.io/name=vela-lab-process-kill-runner-control -o json | jq '[.items[] | select(.status.phase != "Succeeded" and .status.phase != "Failed")] | length')" -eq 0 ] ||
		fail "an active process-kill Runner-control Pod already exists"

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
printf '%s\n' "$running" >"$temporary/attempt-running-before-kill.txt"
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
fault_started_at=$(query_database 'SELECT clock_timestamp();')
printf '%s\n' "$fault_started_at" >"$temporary/fault-started-at.txt"
claim_fault_injection main || fail "fault injection ownership was claimed by recovery"
printf 'started_at=%s recovery=false\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >"$temporary/FAULT_INJECTION_STARTED"
run_fault_pod "$temporary/fault-injection.log" || fail "Runner SIGKILL injector did not prove restart identity"
fault_completed=true
fault_receipt=$(grep -E '^schema=vela-lab-runner-process-kill-v1 signal=SIGKILL signal_transport=(pidfd_send_signal|kill) container_id=[0-9a-f]{64} old_pid=[0-9]+ new_pid=[0-9]+ old_start_ticks=[0-9]+ new_start_ticks=[0-9]+ old_started_at=[^[:space:]]+ new_started_at=[^[:space:]]+ restart_count=[0-9]+ old_cgroup_sha256=[0-9a-f]{64} production_gates=0/9$' \
	"$temporary/fault-injection.log" || true)
[ "$(printf '%s\n' "$fault_receipt" | sed '/^$/d' | wc -l | tr -d ' ')" = 1 ] || fail "fault-injection receipt is malformed or non-unique"
[ "$(grep -Ec '^signal_armed=SIGKILL container_id=[0-9a-f]{64} pid=[0-9]+ start_ticks=[0-9]+$' "$temporary/fault-injection.log" || true)" = 1 ] ||
	fail "fault-injection signal claim is malformed or non-unique"
[ "$(grep -Ec '^signal_sent=SIGKILL transport=(pidfd_send_signal|kill)$' "$temporary/fault-injection.log" || true)" = 1 ] ||
	fail "fault-injection signal transport is malformed or non-unique"
restore_rehearsal_worker "$worker2_id" || fail "Worker 2 could not be restored for post-fault retry"

terminal=
iteration=0
while [ "$iteration" -lt 240 ]; do
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
		'SUCCEEDED|1|1|1|0|0|1|1|2|2|0|0' | \
			'SUCCEEDED|2|2|1|1|0|1|1|2|2|0|0' | \
			'SUCCEEDED|2|2|1|0|1|1|1|2|2|0|0') break ;;
		SUCCEEDED'|'*) fail "application Job succeeded with an unexpected authority shape" ;;
		FAILED'|'* | CANCELED'|'*) fail "application Job reached terminal failure after process kill" ;;
	esac
	iteration=$((iteration + 1))
	sleep 1
done
case "$terminal" in
	'SUCCEEDED|1|1|1|0|0|1|1|2|2|0|0' | \
		'SUCCEEDED|2|2|1|1|0|1|1|2|2|0|0' | \
		'SUCCEEDED|2|2|1|0|1|1|1|2|2|0|0') ;;
	*) fail "application Job did not converge to an accepted process-kill outcome" ;;
esac

restore_rehearsal_worker "$worker1_id" || fail "Worker 1 could not be restored after process-kill recovery"
restore_rehearsal_worker "$worker2_id" || fail "Worker 2 could not be restored after process-kill recovery"
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
  (SELECT count(*) FROM attempt_leases AS lease JOIN attempts AS attempt ON attempt.id = lease.attempt_id WHERE attempt.job_id = job.id AND lease.revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts),
  (SELECT count(*) FROM workers WHERE lifecycle_state = 'READY' AND reachability_condition = 'HEALTHY')
FROM jobs AS job WHERE job.id = '$job_id'::uuid;")
printf '%s\n' "$measurements" >"$temporary/measurements.txt"
[ "$measurements" = '0|0|0|0|0|0|2' ] || fail "final measurements do not satisfy the process-kill lab contract"

attempts_started=$(printf '%s\n' "$terminal" | cut -d '|' -f 2)
container_id=$(printf '%s\n' "$fault_receipt" | sed -n 's/.* container_id=\([0-9a-f]\{64\}\) .*/\1/p')
signal_transport=$(printf '%s\n' "$fault_receipt" | sed -n 's/.* signal_transport=\([^[:space:]]*\) .*/\1/p')
old_pid=$(printf '%s\n' "$fault_receipt" | sed -n 's/.* old_pid=\([0-9][0-9]*\) .*/\1/p')
new_pid=$(printf '%s\n' "$fault_receipt" | sed -n 's/.* new_pid=\([0-9][0-9]*\) .*/\1/p')
old_started_at=$(printf '%s\n' "$fault_receipt" | sed -n 's/.* old_started_at=\([^[:space:]]*\) .*/\1/p')
new_started_at=$(printf '%s\n' "$fault_receipt" | sed -n 's/.* new_started_at=\([^[:space:]]*\) .*/\1/p')
restart_count=$(printf '%s\n' "$fault_receipt" | sed -n 's/.* restart_count=\([0-9][0-9]*\) .*/\1/p')
case "$signal_transport" in pidfd_send_signal | kill) ;; *) fail "fault-injection signal transport is invalid" ;; esac
[ -n "$container_id" ] && [ -n "$old_pid" ] && [ -n "$new_pid" ] &&
	[ -n "$old_started_at" ] && [ -n "$new_started_at" ] && [ -n "$restart_count" ] ||
	fail "fault-injection identity could not be parsed"

exec 8>"$temporary/recovery.lock"
flock 8
disarm_watchdog
if [ -f "$temporary/WATCHDOG_RECOVERY_STARTED" ]; then
	flock -u 8
	exec 8>&-
	fail "watchdog recovery started; refusing to publish a PASS receipt"
fi
rmdir "$temporary/FAULT_INJECTION_CLAIM"
flock -u 8
exec 8>&-
rm -f -- "$temporary/recovery.lock"

completed_at=$(query_database 'SELECT clock_timestamp();')
jq \
	--arg job_id "$job_id" \
	--arg started_at "$fault_started_at" \
	--arg completed_at "$completed_at" \
	--arg previous_receipt_sha256 "$previous_scenario_receipt_sha256" '
  (.scenarios[] | select(.id == "process-kill")) = {
    id: "process-kill",
    status: "LAB_REHEARSAL_PASS",
    job_id: $job_id,
    started_at: $started_at,
    completed_at: $completed_at,
    fault: "RUNNER_MAIN_PROCESS_SIGKILL",
    previous_receipt_sha256: $previous_receipt_sha256
  }
' "$previous_scenario_receipt/scenario-matrix.json" >"$temporary/scenario-matrix.json"
jq -e --arg job_id "$job_id" '
  .schema == "vela-lab-fault-scenario-matrix-v1"
  and .evidence_boundary == "NON_PRODUCTION_MOCK_REHEARSAL"
  and .production_gates == "0/9"
  and (.scenarios | length == 10)
  and ([.scenarios[] | select(.status == "LAB_REHEARSAL_PASS")] | length) == 3
  and any(.scenarios[]; .id == "process-kill" and .job_id == $job_id and .status == "LAB_REHEARSAL_PASS")
' "$temporary/scenario-matrix.json" >/dev/null || fail "three-scenario matrix is invalid"

harness_sha256=$(sha256sum "$0" | awk '{print $1}')
jq -n \
	--arg job_id "$job_id" \
	--arg started_at "$fault_started_at" \
	--arg completed_at "$completed_at" \
	--arg harness_sha256 "$harness_sha256" \
	--arg previous_receipt_sha256 "$previous_scenario_receipt_sha256" \
	--arg container_id "$container_id" \
	--arg signal_transport "$signal_transport" \
	--arg old_started_at "$old_started_at" \
	--arg new_started_at "$new_started_at" \
	--argjson attempts_started "$attempts_started" \
	--argjson old_pid "$old_pid" \
	--argjson new_pid "$new_pid" \
	--argjson restart_count "$restart_count" '
  {
    schema: "vela-lab-process-kill-v1",
    status: "LAB_REHEARSAL_PASS",
    evidence_boundary: "NON_PRODUCTION_MOCK_REHEARSAL",
    production_gates: "0/9",
    fixed_scenarios_completed: 3,
    fixed_scenarios_required: 10,
    job_id: $job_id,
    started_at: $started_at,
    completed_at: $completed_at,
    harness_sha256: $harness_sha256,
    previous_receipt_sha256: $previous_receipt_sha256,
    fault: {
      kind: "RUNNER_MAIN_PROCESS_SIGKILL",
      signal_transport: $signal_transport,
      container_id: $container_id,
      old_pid: $old_pid,
      new_pid: $new_pid,
      started_at_before: $old_started_at,
      started_at_after: $new_started_at,
      restart_count: $restart_count
    },
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
	      "FAULT_INJECTION_OWNER",
	      "FAULT_INJECTION_STARTED",
	      "FAULT_INJECTION_COMPLETED",
	      "fault-pod-identity.txt",
	      "fault-injection.log",
      "authority-before.json",
      "authority-after.json",
      "raw-event-payloads.jsonl",
      "smoke-receipt.json"
    ]
  }
' >"$temporary/summary.json"
printf 'status=LAB_REHEARSAL_PASS production_gates=0/9 fixed_scenarios=3/10\n' >"$temporary/STATUS"

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
printf 'schema=vela-lab-process-kill-wrapper-v1 output=%s job=%s result=LAB_REHEARSAL_PASS fixed_scenarios=3/10 production_gates=0/9\n' \
	"$output" "$job_id"

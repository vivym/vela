#!/bin/sh
set -eu

repository_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
k3d_binary=${K3D_BIN:-k3d}
kubectl_binary=${KUBECTL_BIN:-kubectl}
docker_binary=${DOCKER_BIN:-docker}
timeout_binary=${TIMEOUT_BIN:-timeout}
namespace=vela-h3-disposable
image=${H3_DISPOSABLE_IMAGE:-vela-h3-member-campaign:disposable}
go_proxy=${H3_DISPOSABLE_GOPROXY:-https://proxy.golang.org,direct}
k3s_image=${H3_DISPOSABLE_K3S_IMAGE:-rancher/k3s:v1.31.5-k3s1}
retain_cluster=${H3_DISPOSABLE_RETAIN_CLUSTER:-0}
cluster_name=${H3_DISPOSABLE_CLUSTER_NAME:-vela-h3-disposable-$(date -u +%s)$(printf '%03d' "$(( $$ % 1000 ))")}

case "$cluster_name" in
vela-h3-disposable-*) ;;
*)
	echo "H3_DISPOSABLE_CLUSTER_NAME must begin with vela-h3-disposable-" >&2
	exit 2
	;;
esac
case "$cluster_name" in
*[!a-z0-9-]* | *--* | *-)
	echo "H3_DISPOSABLE_CLUSTER_NAME must be a canonical lowercase DNS label" >&2
	exit 2
	;;
esac
if [ "${#cluster_name}" -gt 32 ]; then
	echo "H3_DISPOSABLE_CLUSTER_NAME must be at most 32 characters for k3d" >&2
	exit 2
fi
if [ "$cluster_name" = "k3d-heimdall-staging" ]; then
	echo "refusing to target the unrelated k3d-heimdall-staging cluster" >&2
	exit 2
fi
case "$retain_cluster" in
0 | 1) ;;
*)
	echo "H3_DISPOSABLE_RETAIN_CLUSTER must be 0 or 1" >&2
	exit 2
	;;
esac

work_directory=$(mktemp -d "${TMPDIR:-/tmp}/vela-h3-disposable-work.XXXXXX")
kubeconfig="$work_directory/kubeconfig"
credential_directory="$work_directory/credentials"
created=false
cleaned=false

proxy_bypass=${NO_PROXY:-}
if [ -n "${no_proxy:-}" ]; then
	proxy_bypass=${proxy_bypass:+$proxy_bypass,}$no_proxy
fi
proxy_bypass=${proxy_bypass:+$proxy_bypass,}localhost,127.0.0.1,::1,0.0.0.0
NO_PROXY=$proxy_bypass
no_proxy=$proxy_bypass
export NO_PROXY no_proxy

if [ -n "${H3_DISPOSABLE_EVIDENCE_DIR:-}" ]; then
	evidence_directory=$H3_DISPOSABLE_EVIDENCE_DIR
	case "$evidence_directory" in
	/*) ;;
	*)
		echo "H3_DISPOSABLE_EVIDENCE_DIR must be an absolute new directory" >&2
		exit 2
		;;
	esac
	if [ -e "$evidence_directory" ]; then
		echo "refusing to replace existing evidence directory $evidence_directory" >&2
		exit 2
	fi
	mkdir -m 0700 "$evidence_directory"
else
	evidence_directory=$(mktemp -d "${TMPDIR:-/tmp}/vela-h3-disposable-evidence.XXXXXX")
fi
echo "evidence: $evidence_directory"

cleanup() {
	if [ "$cleaned" = true ]; then
		return 0
	fi
	cleaned=true
	cleanup_status=0
	if [ "$created" = true ]; then
		"$kubectl_binary" --kubeconfig "$kubeconfig" --request-timeout=10s -n "$namespace" logs \
			-l app.kubernetes.io/name=vela-h3-member-campaign --all-containers=true --prefix=true --tail=-1 \
			>"$evidence_directory/workload-final-or-failure.log" 2>&1 || true
		"$kubectl_binary" --kubeconfig "$kubeconfig" --request-timeout=10s -n "$namespace" logs \
			-l app.kubernetes.io/name=vela-h3-member-campaign --all-containers=true --prefix=true --tail=-1 --previous \
			>"$evidence_directory/workload-previous-final-or-failure.log" 2>&1 || true
		"$kubectl_binary" --kubeconfig "$kubeconfig" --request-timeout=10s -n "$namespace" get pods -o json \
			>"$evidence_directory/pod-inventory-final-or-failure.json" 2>/dev/null || true
		if [ "$retain_cluster" = 0 ]; then
			case "$cluster_name" in
			vela-h3-disposable-*)
				if ! "$timeout_binary" 45s "$k3d_binary" cluster delete "$cluster_name" \
					>"$evidence_directory/cluster-delete.log" 2>&1; then
					cleanup_status=1
				fi
				if ! "$timeout_binary" 10s "$k3d_binary" cluster list --no-headers \
					>"$evidence_directory/cluster-list-after-cleanup.txt" \
					2>>"$evidence_directory/cluster-delete.log"; then
					cleanup_status=1
				elif awk '{print $1}' "$evidence_directory/cluster-list-after-cleanup.txt" | \
					grep -Fx "$cluster_name" >/dev/null; then
					echo "cluster still exists after cleanup: $cluster_name" \
						>>"$evidence_directory/cluster-delete.log"
					cleanup_status=1
				fi
				;;
			esac
		fi
	fi
	if ! find "$work_directory" -depth -mindepth 1 -delete \
		2>"$evidence_directory/work-directory-cleanup-error.log"; then
		cleanup_status=1
	fi
	if ! rmdir "$work_directory" 2>>"$evidence_directory/work-directory-cleanup-error.log"; then
		cleanup_status=1
	fi
	if [ -e "$work_directory" ]; then
		echo "private campaign work directory still exists: $work_directory" \
			>>"$evidence_directory/work-directory-cleanup-error.log"
		cleanup_status=1
	fi
	return "$cleanup_status"
}

handle_exit() {
	exit_status=$?
	cleanup_result=0
	cleanup || cleanup_result=$?
	trap - EXIT
	if [ "$cleanup_result" -ne 0 ]; then
		echo "disposable H3 member campaign cleanup failed; see $evidence_directory" >&2
	fi
	if [ "$exit_status" -ne 0 ]; then
		exit "$exit_status"
	fi
	exit "$cleanup_result"
}
trap handle_exit EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

for command in "$k3d_binary" "$kubectl_binary" "$docker_binary" "$timeout_binary" openssl; do
	if ! command -v "$command" >/dev/null 2>&1; then
		preflight_error="required command is unavailable: $command"
		printf '%s\n' "$preflight_error" >"$evidence_directory/preflight-error.log"
		printf '%s\n' "$preflight_error" >&2
		exit 1
	fi
done
if ! "$k3d_binary" cluster list --no-headers \
	>"$evidence_directory/cluster-list-before-create.txt" \
	2>"$evidence_directory/cluster-list-before-create-error.log"; then
	cat "$evidence_directory/cluster-list-before-create-error.log" >&2
	exit 1
fi
if awk '{print $1}' "$evidence_directory/cluster-list-before-create.txt" | grep -Fx "$cluster_name" >/dev/null; then
	echo "refusing to reuse existing k3d cluster $cluster_name" >&2
	exit 1
fi

if ! "$docker_binary" info --format '{{.Architecture}}' \
	>"$evidence_directory/docker-architecture.txt" \
	2>"$evidence_directory/docker-info-error.log"; then
	cat "$evidence_directory/docker-info-error.log" >&2
	exit 1
fi
docker_architecture=$(cat "$evidence_directory/docker-architecture.txt")
case "$docker_architecture" in
amd64 | x86_64) image_platform=linux/amd64 ;;
arm64 | aarch64) image_platform=linux/arm64 ;;
*)
	echo "unsupported Docker server architecture: $docker_architecture" >&2
	exit 1
	;;
esac
source_revision=$(git -C "$repository_root" rev-parse HEAD)

if ! "$docker_binary" build \
	--platform "$image_platform" \
	--build-arg "RELEASE_REVISION=$source_revision" \
	--build-arg "GOPROXY=$go_proxy" \
	--file "$repository_root/deploy/h3-disposable-campaign/Dockerfile" \
	--tag "$image" \
	"$repository_root" >"$evidence_directory/docker-build.log" 2>&1; then
	cat "$evidence_directory/docker-build.log" >&2
	exit 1
fi

if ! "$k3d_binary" cluster create "$cluster_name" \
	--servers 1 \
	--agents 3 \
	--image "$k3s_image" \
	--no-lb \
	--k3s-arg "--disable=traefik@server:0" \
	--k3s-node-label "vela.ai/h3-campaign-role=leader@agent:0" \
	--k3s-node-label "vela.ai/h3-campaign-role=follower@agent:1" \
	--kubeconfig-update-default=false \
	--kubeconfig-switch-context=false \
	--wait \
	--timeout 3m >"$evidence_directory/cluster-create.log" 2>&1; then
	cat "$evidence_directory/cluster-create.log" >&2
	exit 1
fi
created=true
"$k3d_binary" kubeconfig get "$cluster_name" >"$kubeconfig"
chmod 0600 "$kubeconfig"
"$k3d_binary" image import --cluster "$cluster_name" "$image"

mkdir -m 0700 "$credential_directory"
openssl req -x509 -newkey ed25519 -nodes \
	-keyout "$credential_directory/ca.key" \
	-out "$credential_directory/ca.crt" \
	-days 1 -subj '/CN=Vela disposable member campaign CA' >/dev/null 2>&1
openssl req -new -newkey ed25519 -nodes \
	-keyout "$credential_directory/server.key" \
	-out "$credential_directory/server.csr" \
	-subj '/CN=follower.vela-h3-disposable.svc' >/dev/null 2>&1
cat >"$credential_directory/server.ext" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth
subjectAltName=DNS:follower.vela-h3-disposable.svc,URI:spiffe://vela.internal/stage-worker/49370000-0000-0000-0000-000000000020
EOF
openssl x509 -req -in "$credential_directory/server.csr" \
	-CA "$credential_directory/ca.crt" -CAkey "$credential_directory/ca.key" \
	-CAcreateserial -out "$credential_directory/server.crt" -days 1 \
	-extfile "$credential_directory/server.ext" >/dev/null 2>&1
openssl req -new -newkey ed25519 -nodes \
	-keyout "$credential_directory/client.key" \
	-out "$credential_directory/client.csr" \
	-subj '/CN=campaign-leader' >/dev/null 2>&1
cat >"$credential_directory/client.ext" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=clientAuth
subjectAltName=URI:spiffe://vela.internal/stage-worker/49370000-0000-0000-0000-000000000010
EOF
openssl x509 -req -in "$credential_directory/client.csr" \
	-CA "$credential_directory/ca.crt" -CAkey "$credential_directory/ca.key" \
	-CAcreateserial -out "$credential_directory/client.crt" -days 1 \
	-extfile "$credential_directory/client.ext" >/dev/null 2>&1
openssl rand -out "$credential_directory/authority.key" 32
chmod 0400 \
	"$credential_directory/ca.crt" \
	"$credential_directory/server.crt" \
	"$credential_directory/server.key" \
	"$credential_directory/client.crt" \
	"$credential_directory/client.key" \
	"$credential_directory/authority.key"

campaign_directory="$repository_root/deploy/h3-disposable-campaign"
"$kubectl_binary" --kubeconfig "$kubeconfig" apply -f "$campaign_directory/campaign.yaml"
"$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" create secret generic \
	vela-h3-member-campaign-credentials \
	--from-file=ca.crt="$credential_directory/ca.crt" \
	--from-file=server.crt="$credential_directory/server.crt" \
	--from-file=server.key="$credential_directory/server.key" \
	--from-file=client.crt="$credential_directory/client.crt" \
	--from-file=client.key="$credential_directory/client.key" \
	--from-file=authority.key="$credential_directory/authority.key"
"$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" rollout status \
	deployment/follower --timeout=90s

assert_receipt() {
	receipt_file=$1
	outcome=$2
	if [ "$(awk 'END { print NR }' "$receipt_file")" -ne 1 ]; then
		echo "receipt must contain exactly one JSON line: $receipt_file" >&2
		exit 1
	fi
	if ! grep -F "\"outcome\":\"$outcome\"" "$receipt_file" >/dev/null; then
		echo "receipt has unexpected outcome: $receipt_file" >&2
		exit 1
	fi
	for binding in '"exercise_id":' '"authority_digest":' '"leader_identity_digest":' '"follower_identity_digest":'; do
		if ! grep -F "$binding" "$receipt_file" >/dev/null; then
			echo "receipt omitted binding $binding: $receipt_file" >&2
			exit 1
		fi
	done
}

wait_for_job() {
	job_name=$1
	receipt_file=$2
	"$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" wait \
		--for=condition=complete "job/$job_name" --timeout=90s
	"$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" logs "job/$job_name" >"$receipt_file"
}

"$kubectl_binary" --kubeconfig "$kubeconfig" apply -f "$campaign_directory/normal-job.yaml"
wait_for_job member-campaign-normal "$evidence_directory/normal-receipt.json"
assert_receipt "$evidence_directory/normal-receipt.json" PASS
grep -F '"barrier_passed":true' "$evidence_directory/normal-receipt.json" >/dev/null
grep -F '"all_stopped":true' "$evidence_directory/normal-receipt.json" >/dev/null
"$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" get pods -o json \
	>"$evidence_directory/pod-inventory-normal.json"
leader_node=$("$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" get pod \
	-l job-name=member-campaign-normal -o jsonpath='{.items[0].spec.nodeName}')
follower_node=$("$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" get pod \
	-l app.kubernetes.io/component=follower -o jsonpath='{.items[0].spec.nodeName}')
if [ -z "$leader_node" ] || [ -z "$follower_node" ] || [ "$leader_node" = "$follower_node" ]; then
	echo "leader and follower were not pinned to distinct agent nodes" >&2
	exit 1
fi

prepared_before=$("$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" logs deployment/follower 2>/dev/null | \
	awk '/prepared member/ { count++ } END { print count + 0 }')
"$kubectl_binary" --kubeconfig "$kubeconfig" apply -f "$campaign_directory/fault-job.yaml"
prepared=false
attempt=0
while [ "$attempt" -lt 60 ]; do
	prepared_now=$("$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" logs deployment/follower 2>/dev/null | \
		awk '/prepared member/ { count++ } END { print count + 0 }')
	if [ "$prepared_now" -gt "$prepared_before" ]; then
		prepared=true
		break
	fi
	attempt=$((attempt + 1))
	sleep 1
done
if [ "$prepared" != true ]; then
	echo "follower did not report prepared member before the fault deadline" >&2
	exit 1
fi
"$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" get pods \
	-l app.kubernetes.io/component=follower -o json >"$evidence_directory/follower-before-fault.json"
"$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" logs deployment/follower \
	>"$evidence_directory/follower-before-fault.log"
"$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" scale deployment/follower --replicas=0
"$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" wait --for=delete pod \
	-l app.kubernetes.io/component=follower --timeout=60s
wait_for_job member-campaign-fault "$evidence_directory/fault-receipt.json"
assert_receipt "$evidence_directory/fault-receipt.json" FAULT_REJECTED
grep -F '"prepared_members":2' "$evidence_directory/fault-receipt.json" >/dev/null
grep -F '"started_members":1' "$evidence_directory/fault-receipt.json" >/dev/null
grep -F '"local_member_stopped":true' "$evidence_directory/fault-receipt.json" >/dev/null
grep -F '"remote_member_unavailable":true' "$evidence_directory/fault-receipt.json" >/dev/null
grep -F '"all_stopped":false' "$evidence_directory/fault-receipt.json" >/dev/null

"$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" scale deployment/follower --replicas=1
"$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" rollout status \
	deployment/follower --timeout=90s
"$kubectl_binary" --kubeconfig "$kubeconfig" apply -f "$campaign_directory/recovery-job.yaml"
wait_for_job member-campaign-recovery "$evidence_directory/recovery-receipt.json"
assert_receipt "$evidence_directory/recovery-receipt.json" PASS
grep -F '"barrier_passed":true' "$evidence_directory/recovery-receipt.json" >/dev/null
grep -F '"all_stopped":true' "$evidence_directory/recovery-receipt.json" >/dev/null

"$kubectl_binary" --kubeconfig "$kubeconfig" get nodes -o json >"$evidence_directory/cluster-nodes.json"
"$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" get pods -o json \
	>"$evidence_directory/pod-inventory.json"
"$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" get pods \
	-o custom-columns='NAME:.metadata.name,UID:.metadata.uid,NODE:.spec.nodeName,RESTARTS:.status.containerStatuses[0].restartCount,PHASE:.status.phase' \
	>"$evidence_directory/pod-inventory.txt"
"$kubectl_binary" --kubeconfig "$kubeconfig" -n "$namespace" logs deployment/follower \
	>"$evidence_directory/follower-recovery.log"
"$k3d_binary" version >"$evidence_directory/k3d-version.txt"
"$kubectl_binary" version --client=true -o yaml >"$evidence_directory/kubectl-version.yaml"
"$docker_binary" image inspect "$image" >"$evidence_directory/image-inspect.json"
if ! cleanup; then
	echo "disposable H3 member campaign cleanup failed; see $evidence_directory" >&2
	exit 1
fi
trap - EXIT HUP INT TERM
completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
cat >"$evidence_directory/summary.json" <<EOF
{"schema_version":1,"result":"PASS","scope":"disposable-fake-runtime-cross-node-member-transport","cluster_name":"$cluster_name","source_revision":"$source_revision","image":"$image","image_platform":"$image_platform","leader_node":"$leader_node","follower_node":"$follower_node","completed_at":"$completed_at","production_gate":false,"gpu":false,"dra":false}
EOF
echo "disposable H3 member campaign PASS"
if [ "$retain_cluster" = 1 ]; then
	echo "retained cluster: $cluster_name"
fi

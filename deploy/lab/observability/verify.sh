#!/bin/sh

set -eu
umask 077

output=${1:-}
namespace=vela-observability
kubectl_bin=${KUBECTL_BIN:-/var/lib/rancher/rke2/bin/kubectl}
kubeconfig=${KUBECONFIG:-/etc/rancher/rke2/rke2.yaml}
temporary=
prometheus_pid=
alertmanager_pid=
grafana_pid=
committed=false

fail() {
	printf 'verify-vela-lab-observability: %s\n' "$*" >&2
	exit 1
}

cleanup() {
	for process_id in "$prometheus_pid" "$alertmanager_pid" "$grafana_pid"; do
		if [ -n "$process_id" ]; then
			kill "$process_id" 2>/dev/null || true
			wait "$process_id" 2>/dev/null || true
		fi
	done
	if [ "$committed" != true ] && [ -n "$temporary" ] && [ -d "$temporary" ]; then
		find "$temporary" -xdev -mindepth 1 -delete
		rmdir "$temporary"
	fi
}

query_database() {
	sql=$1
	# PostgreSQL credentials are expanded only inside the database container.
	# shellcheck disable=SC2016
	printf '%s\n' "$sql" | "$kubectl_bin" exec --stdin --namespace vela-lab \
		statefulset/vela-lab-postgres -- sh -ec \
		'PGPASSWORD=$POSTGRES_PASSWORD exec psql --no-psqlrc --set=ON_ERROR_STOP=1 --tuples-only --no-align --field-separator="|" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"'
}

wait_http() {
	url=$1
	log=$2
	attempt=0
	while [ "$attempt" -lt 60 ]; do
		if curl --fail --silent --show-error --max-time 2 "$url" >"$log" 2>"$log.err"; then
			return
		fi
		attempt=$((attempt + 1))
		sleep 1
	done
	fail "$url did not become ready"
}

wait_for_heartbeat() {
	attempt=0
	while [ "$attempt" -lt 90 ]; do
		curl --fail --silent --show-error --max-time 3 http://127.0.0.1:19093/api/v2/alerts \
			>"$temporary/alertmanager-alerts.json" || true
		if jq -e 'any(.[]; .labels.alertname == "VelaLabObservabilityHeartbeat" and .status.state == "active")' \
			"$temporary/alertmanager-alerts.json" >/dev/null 2>&1; then
			return
		fi
		attempt=$((attempt + 1))
		sleep 1
	done
	fail "lab heartbeat did not reach Alertmanager within 90 seconds"
}

[ "$#" -eq 1 ] || fail "usage: $0 <new-output-directory>"
[ "$(id -u)" -eq 0 ] || fail "run as root"
[ "$(hostname)" = marslab-server ] || fail "run only on marslab-server"
case "$output" in /*) ;; *) fail "output directory must be absolute" ;; esac
[ ! -e "$output" ] && [ ! -L "$output" ] || fail "output path already exists"
[ -x "$kubectl_bin" ] || fail "$kubectl_bin is missing"
[ -r "$kubeconfig" ] || fail "$kubeconfig is unreadable"
for command_name in curl docker jq sha256sum; do
	command -v "$command_name" >/dev/null 2>&1 || fail "$command_name is required"
done
export KUBECONFIG="$kubeconfig"
temporary=$(mktemp -d "$(dirname "$output")/.vela-observability-verify.XXXXXX")
chmod 0700 "$temporary"
trap cleanup EXIT HUP INT TERM

[ "$(docker inspect --format '{{.Id}}' vela-registry 2>/dev/null)" = 2bd86fd8f7db91609a430dd8e12402bb5eb5def9454f297994f51ab9c1571d68 ] || fail "Registry container identity changed"
[ "$(docker inspect --format '{{.State.Status}}' vela-registry 2>/dev/null)" = running ] || fail "Registry is not running"
[ "$(docker inspect --format '{{.Id}}' fchip-4591d89ff18127a74b8a25a0 2>/dev/null)" = b0a653da3926e90d88a6d3329fab8a927456e23ddfd6acb7d7d40cf6f9db0c94 ] || fail "shared experiment container identity changed"
[ "$(docker inspect --format '{{.State.Status}}' fchip-4591d89ff18127a74b8a25a0 2>/dev/null)" = running ] || fail "shared experiment container is not running"

"$kubectl_bin" get nodes -o json >"$temporary/nodes.json"
jq -e '
  (.items | length) == 3
  and all(.items[]; any(.status.conditions[]; .type == "Ready" and .status == "True"))
  and any(.items[];
    .metadata.name == "vela-lab-control-1"
    and .metadata.labels["vela.ai/node-role"] == "control-storage"
    and (.status.allocatable["nvidia.com/gpu"] // "0") == "0")
  and ([.items[] | select(.metadata.name == "vela-lab-worker-1" or .metadata.name == "vela-lab-worker-2")
    | (.status.allocatable["nvidia.com/gpu"] // "0")] | sort) == ["8", "8"]
' "$temporary/nodes.json" >/dev/null || fail "three-node Ready GPU boundary drifted"

"$kubectl_bin" get namespace "$namespace" -o json >"$temporary/namespace.json"
jq -e '
  .metadata.labels["vela.ai/environment"] == "non-production-lab"
  and .metadata.labels["vela.ai/network-role"] == "observability"
' "$temporary/namespace.json" >/dev/null || fail "observability namespace labels drifted"

"$kubectl_bin" rollout status deployment/vela-lab-prometheus --namespace "$namespace" --timeout=180s >/dev/null
"$kubectl_bin" rollout status deployment/vela-lab-alertmanager --namespace "$namespace" --timeout=180s >/dev/null
"$kubectl_bin" rollout status deployment/vela-lab-grafana --namespace "$namespace" --timeout=180s >/dev/null
"$kubectl_bin" get deployment,pod,service,configmap,networkpolicy --namespace "$namespace" -o json >"$temporary/observability-resources.json"
"$kubectl_bin" get pods --namespace "$namespace" -o json >"$temporary/pods.json"
jq -e '
  (.items | length) == 3
  and all(.items[]; .spec.nodeName == "vela-lab-control-1" and .status.phase == "Running")
  and all(.items[]; all(.status.containerStatuses[]; .ready == true))
  and ([.items[].spec.containers[].image] | sort) == ([
    "10.1.200.17:5443/observability/alertmanager:v0.34.0@sha256:268d4bf0e4bc0fe6dbdef6a59ce81a2918c88458bf8edf7dd0572ad372a093e6",
    "10.1.200.17:5443/observability/grafana:13.2.0@sha256:95a8098fb092130e111b0264a9be4d3a2bd5405e5dba88d4b8f1f630b389614e",
    "10.1.200.17:5443/observability/prometheus:v3.14.0@sha256:e906cef998316bbe319f98711e1b4d8613ad37e14b08ff831d7036e77b7464f9"
  ] | sort)
  and ([.items[].spec.containers[].resources.requests["nvidia.com/gpu"]? // 0 | tonumber] | add // 0) == 0
  and all(.items[]; all(.spec.volumes[]?; has("hostPath") | not))
  and all(.items[]; all(.spec.volumes[]?; has("persistentVolumeClaim") | not))
' "$temporary/pods.json" >/dev/null || fail "observability Pod placement, image, storage, or GPU boundary drifted"

"$kubectl_bin" get networkpolicy control-ingress --namespace vela-lab -o json >"$temporary/control-ingress.json"
jq -e '
  ([.spec.ingress[] | select(any(.ports[]?; .port == 8081))] | length) == 1
  and ([.spec.ingress[] | select(any(.ports[]?; .port == 8081))][0].from | length) == 1
  and [.spec.ingress[] | select(any(.ports[]?; .port == 8081))][0].from[0].namespaceSelector.matchLabels["vela.ai/network-role"] == "observability"
  and [.spec.ingress[] | select(any(.ports[]?; .port == 8081))][0].from[0].podSelector.matchLabels["vela.ai/client-role"] == "otel-collector"
' "$temporary/control-ingress.json" >/dev/null || fail "control management ingress is not narrowly bound to the observability identity"

global_state=$(query_database '
SELECT
  (SELECT count(*) FROM attempt_leases WHERE revoked_at IS NULL),
  (SELECT count(*) FROM production_gate_receipts),
  (SELECT count(*) FROM jobs WHERE state IN ('\''QUEUED'\'','\''ASSIGNED'\'','\''RUNNING'\'','\''FINALIZING'\'','\''RETRY_WAIT'\'','\''CANCELING'\'')),
  (SELECT count(*) FROM workers WHERE lifecycle_state = '\''READY'\'' AND reachability_condition = '\''HEALTHY'\'');')
printf '%s\n' "$global_state" >"$temporary/database-boundary.txt"
[ "$global_state" = '0|0|0|2' ] || fail "database authority boundary is $global_state, expected 0|0|0|2"

"$kubectl_bin" port-forward --address=127.0.0.1 --namespace "$namespace" service/vela-lab-prometheus 19090:9090 >"$temporary/prometheus-port-forward.log" 2>&1 &
prometheus_pid=$!
"$kubectl_bin" port-forward --address=127.0.0.1 --namespace "$namespace" service/vela-lab-alertmanager 19093:9093 >"$temporary/alertmanager-port-forward.log" 2>&1 &
alertmanager_pid=$!
"$kubectl_bin" port-forward --address=127.0.0.1 --namespace "$namespace" service/vela-lab-grafana 13000:3000 >"$temporary/grafana-port-forward.log" 2>&1 &
grafana_pid=$!

wait_http http://127.0.0.1:19090/-/ready "$temporary/prometheus-ready.txt"
wait_http http://127.0.0.1:19093/-/ready "$temporary/alertmanager-ready.txt"
wait_http http://127.0.0.1:13000/api/health "$temporary/grafana-health.json"
jq -e '.database == "ok" and .version == "13.2.0"' "$temporary/grafana-health.json" >/dev/null || fail "Grafana health or version drifted"

curl --fail --silent --show-error http://127.0.0.1:19090/api/v1/targets >"$temporary/prometheus-targets.json"
jq -e '
  .status == "success"
  and ([.data.activeTargets[] | select(.labels.job == "vela-lab-control" and .health == "up")] | length) == 1
' "$temporary/prometheus-targets.json" >/dev/null || fail "Vela control scrape target is not uniquely up"

curl --fail --silent --show-error --get --data-urlencode 'query=vela_slo_report_exporter_last_scrape_success' \
	http://127.0.0.1:19090/api/v1/query >"$temporary/exporter-health-query.json"
jq -e '.status == "success" and (.data.result | length) == 1 and (.data.result[0].value[1] | tonumber) == 1' \
	"$temporary/exporter-health-query.json" >/dev/null || fail "SLO exporter success gauge is not exactly 1"

curl --fail --silent --show-error --get --data-urlencode 'match[]={__name__=~"vela_.+"}' \
	http://127.0.0.1:19090/api/v1/series >"$temporary/vela-series.json"
jq -e '
  .status == "success"
  and all(.data[]; ([
    .organization_id?, .project_id?, .principal_id?, .job_id?, .attempt_id?, .worker_id?
  ] | map(select(. != null)) | length) == 0)
' "$temporary/vela-series.json" >/dev/null || fail "forbidden high-cardinality identity label was scraped"

curl --fail --silent --show-error --get --data-urlencode 'query=vela_gateway_sli_requests_total' \
	http://127.0.0.1:19090/api/v1/query >"$temporary/gateway-sli-query.json"
curl --fail --silent --show-error http://127.0.0.1:19090/api/v1/rules >"$temporary/prometheus-rules.json"
jq -e '
  .status == "success"
  and any(.data.groups[]; .name == "vela-api-slo")
  and any(.data.groups[]; .name == "vela-preset-slo")
  and any(.data.groups[]; .name == "vela-lab-observability-smoke")
' "$temporary/prometheus-rules.json" >/dev/null || fail "Prometheus did not load all canonical and lab rule groups"

curl --fail --silent --show-error http://127.0.0.1:19093/api/v2/status >"$temporary/alertmanager-status.json"
wait_for_heartbeat
curl --fail --silent --show-error 'http://127.0.0.1:13000/api/search?query=Vela' >"$temporary/grafana-search.json"
jq -e 'any(.[]; .uid == "vela-statistical-slos" and .title == "Vela Statistical SLOs")' \
	"$temporary/grafana-search.json" >/dev/null || fail "provisioned Vela dashboard is absent"
curl --fail --silent --show-error http://127.0.0.1:13000/api/datasources/uid/prometheus/health \
	>"$temporary/grafana-datasource-health.json"
jq -e '.status == "OK"' "$temporary/grafana-datasource-health.json" >/dev/null || fail "Grafana Prometheus datasource is unhealthy"

"$kubectl_bin" rollout status deployment/vela-lab-control --namespace vela-lab --timeout=60s >/dev/null
"$kubectl_bin" rollout status deployment/vela-lab-worker-agent-1 --namespace vela-lab --timeout=60s >/dev/null
"$kubectl_bin" rollout status deployment/vela-lab-worker-agent-2 --namespace vela-lab --timeout=60s >/dev/null
gateway_series=$(jq '.data.result | length' "$temporary/gateway-sli-query.json")
printf 'schema=vela-lab-observability-postflight-v1\ncaptured_at=%s\nresult=LAB_REHEARSAL_PASS\nproduction_gates=0/9\nplacements=control-only\ngpu_requests=0\nprometheus_target=up\nslo_exporter_success=1\ngateway_sli_series=%s\nalert_delivery=VelaLabObservabilityHeartbeat\nalert_receiver=lab-null\npaging_integration=absent\n' \
	"$(date -u +%FT%TZ)" "$gateway_series" >"$temporary/STATUS"
(cd "$temporary" && find . -type f ! -name SHA256SUMS -print | LC_ALL=C sort | xargs sha256sum) >"$temporary/SHA256SUMS"
chmod 0600 "$temporary"/*
mv "$temporary" "$output"
committed=true
printf 'schema=vela-lab-observability-postflight-v1 output=%s result=LAB_REHEARSAL_PASS production_gates=0/9\n' "$output"

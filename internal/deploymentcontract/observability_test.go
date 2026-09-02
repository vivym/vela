package deploymentcontract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

type observabilityContract struct {
	SchemaVersion                  int      `json:"schema_version"`
	PublicAPIAvailabilityTargetPPM int      `json:"public_api_availability_target_ppm"`
	JobSLOAlgorithmRevision        string   `json:"job_slo_algorithm_revision"`
	ConfidenceMethod               string   `json:"confidence_method"`
	RequiredPresets                []string `json:"required_presets"`
	AllowedMetricLabels            []string `json:"allowed_metric_labels"`
	ForbiddenMetricLabels          []string `json:"forbidden_metric_labels"`
	RequiredRecordingRules         []string `json:"required_recording_rules"`
	RequiredAlerts                 []string `json:"required_alerts"`
	RequiredEvidenceArtifacts      []string `json:"required_evidence_artifacts"`
	ManagementEndpoint             struct {
		Port                 int    `json:"port"`
		Path                 string `json:"path"`
		PublicServiceExposed bool   `json:"public_service_exposed"`
		NamespaceLabel       string `json:"namespace_label"`
		PodLabel             string `json:"pod_label"`
	} `json:"management_endpoint"`
}

func TestObservabilityContractBindsMetricsRulesDashboardAndRunbook(t *testing.T) {
	directory := observabilityDirectory(t)
	encoded := readObservabilityFile(t, directory, "observability-contract.json")
	var contract observabilityContract
	if err := json.Unmarshal(encoded, &contract); err != nil {
		t.Fatalf("decode observability contract: %v", err)
	}
	if contract.SchemaVersion != 1 || contract.PublicAPIAvailabilityTargetPPM != 999000 ||
		contract.JobSLOAlgorithmRevision != "queued-visible-nearest-rank-wilson-v1" ||
		contract.ConfidenceMethod != "wilson-one-sided-95-v1" ||
		!reflect.DeepEqual(contract.RequiredPresets, []string{"quality", "balanced", "fast"}) ||
		contract.ManagementEndpoint.Port != 8081 || contract.ManagementEndpoint.Path != "/metrics" ||
		contract.ManagementEndpoint.PublicServiceExposed ||
		contract.ManagementEndpoint.NamespaceLabel != "vela.ai/network-role=observability" ||
		contract.ManagementEndpoint.PodLabel != "vela.ai/client-role=otel-collector" {
		t.Fatalf("observability contract = %#v", contract)
	}
	allowed := make(map[string]bool, len(contract.AllowedMetricLabels))
	for _, label := range contract.AllowedMetricLabels {
		allowed[label] = true
	}
	for _, label := range contract.ForbiddenMetricLabels {
		if allowed[label] {
			t.Fatalf("forbidden metric label %q is allowed", label)
		}
	}

	rules := string(readObservabilityFile(t, directory, "rules.yaml"))
	for _, rule := range append(
		append([]string(nil), contract.RequiredRecordingRules...),
		contract.RequiredAlerts...,
	) {
		if !strings.Contains(rules, rule) {
			t.Fatalf("observability rule %q is missing", rule)
		}
	}
	for _, label := range contract.ForbiddenMetricLabels {
		if strings.Contains(rules, label) {
			t.Fatalf("observability rules contain forbidden label %q", label)
		}
	}
	if !strings.Contains(rules, "14.4 * (1 - 0.999)") ||
		!strings.Contains(rules, "3 * (1 - 0.999)") ||
		!strings.Contains(rules, "absent(sum(vela_gateway_sli_requests_total") ||
		!strings.Contains(rules, "vela_slo_contract_report_coverage == 0") ||
		!strings.Contains(rules, "absent(vela_slo_contract_report_coverage)") {
		t.Fatalf("observability rules omit multi-window burn or missing-series fail-closed checks")
	}

	dashboard := readObservabilityFile(t, directory, "dashboard.json")
	var dashboardDocument map[string]any
	if err := json.Unmarshal(dashboard, &dashboardDocument); err != nil {
		t.Fatalf("decode observability dashboard: %v", err)
	}
	dashboardText := string(dashboard)
	for _, metric := range []string{
		"vela:api_availability:ratio_30d",
		"vela:api_error_budget_remaining:ratio_30d",
		"vela_slo_report_p95_milliseconds",
		"vela_slo_report_success_lower_bound_ratio",
		"vela_slo_report_eligible_jobs",
		"vela_slo_report_succeeded_jobs",
		"vela_slo_report_failed_jobs",
		"vela_slo_report_customer_canceled_jobs",
		"vela_slo_report_open_jobs",
		"vela_slo_contract_report_coverage",
		"vela_slo_report_sealed_timestamp_seconds",
	} {
		if !strings.Contains(dashboardText, metric) {
			t.Fatalf("dashboard metric %q is missing", metric)
		}
	}

	podMonitor := string(readObservabilityFile(t, directory, "pod-monitor.yaml"))
	for _, required := range []string{
		"kind: PodMonitor", "port: management", "path: /metrics",
		"interval: 30s", "scrapeTimeout: 5s", "vela-system",
	} {
		if !strings.Contains(podMonitor, required) {
			t.Fatalf("PodMonitor requirement %q is missing", required)
		}
	}
	runbook := string(readObservabilityFile(
		t,
		filepath.Join(observabilityRepositoryRoot(t), "docs", "runbooks"),
		"statistical-slo-breach.md",
	))
	for _, required := range []string{
		"Owner: Platform On-call (24x7)", "Management probes are excluded",
		"insufficient data, never PASS", "incident timeline records fired", "resolved times",
	} {
		if !strings.Contains(runbook, required) {
			t.Fatalf("SLO runbook requirement %q is missing", required)
		}
	}
}

func TestObservabilityEvidenceArtifactKindsStayExact(t *testing.T) {
	directory := observabilityDirectory(t)
	var contract observabilityContract
	if err := json.Unmarshal(
		readObservabilityFile(t, directory, "observability-contract.json"),
		&contract,
	); err != nil {
		t.Fatalf("decode observability contract: %v", err)
	}
	want := []string{
		"gateway-observations", "saleable-sku-snapshot", "dashboard",
		"alert-rules", "rule-tests", "runbook", "page-events",
	}
	if !reflect.DeepEqual(contract.RequiredEvidenceArtifacts, want) {
		t.Fatalf("observability evidence artifacts = %q, want %q", contract.RequiredEvidenceArtifacts, want)
	}
}

func TestStageCampaignObservabilityBindsMetricsAlertsDashboardAndRunbook(t *testing.T) {
	directory := observabilityDirectory(t)
	var contract observabilityContract
	if err := json.Unmarshal(
		readObservabilityFile(t, directory, "observability-contract.json"),
		&contract,
	); err != nil {
		t.Fatalf("decode observability contract: %v", err)
	}
	for _, label := range []string{"stage_kind", "state", "outcome", "reason", "algorithm_revision"} {
		if !contains(contract.AllowedMetricLabels, label) {
			t.Fatalf("Stage observability label %q is not allowlisted", label)
		}
	}
	for _, rule := range []string{
		"vela:stage_ready_oldest_age_seconds:max",
		"vela:stage_transfer_active_oldest_age_seconds:max",
		"vela:stage_scheduler_divergence:increase_5m",
	} {
		if !contains(contract.RequiredRecordingRules, rule) {
			t.Fatalf("Stage recording rule %q is not required", rule)
		}
	}
	for _, alert := range []string{
		"VelaStageAuthorityExporterFailed",
		"VelaStageReadyQueueStalled",
		"VelaStageSchedulerReplayDiverged",
		"VelaStageTransferTicketStuck",
		"VelaStageModelResidencyCoverageMissing",
	} {
		if !contains(contract.RequiredAlerts, alert) {
			t.Fatalf("Stage alert %q is not required", alert)
		}
	}

	rules := string(readObservabilityFile(t, directory, "rules.yaml"))
	for _, required := range []string{
		"vela_stage_authority_exporter_last_scrape_success",
		"vela_stage_ready_oldest_age_seconds",
		"vela_stage_scheduler_shadow_replay_total",
		"vela_stage_transfer_active_oldest_age_seconds",
		`stage_kind=~"ENCODER|DIT|VAE_DECODER"`,
		"docs/runbooks/h3-stage-campaign.md",
	} {
		if !strings.Contains(rules, required) {
			t.Fatalf("Stage observability rules omit %q", required)
		}
	}

	dashboard := string(readObservabilityFile(t, directory, "dashboard.json"))
	for _, metric := range []string{
		"vela_stage_run_state_count",
		"vela:stage_ready_oldest_age_seconds:max",
		"vela_stage_transfer_ticket_state_count",
		"vela:stage_transfer_active_oldest_age_seconds:max",
		"vela_stage_cache_entry_state_count",
		"vela_stage_model_residency_state_count",
		"vela_stage_scheduler_acquire_total",
		"vela_stage_scheduler_shadow_replay_total",
	} {
		if !strings.Contains(dashboard, metric) {
			t.Fatalf("Stage dashboard metric %q is missing", metric)
		}
	}

	runbook := string(readObservabilityFile(
		t,
		filepath.Join(observabilityRepositoryRoot(t), "docs", "runbooks"),
		"h3-stage-campaign.md",
	))
	for _, required := range []string{
		"Owner: Vela Runtime On-call (24x7)",
		"Do not unload a healthy resident model",
		"same-node", "cross-node", "exact cache", "N/N-1", "rollback",
		"Production Gate status remains unchanged",
	} {
		if !strings.Contains(runbook, required) {
			t.Fatalf("H3 Stage campaign runbook omits %q", required)
		}
	}
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func observabilityDirectory(t *testing.T) string {
	t.Helper()
	return filepath.Join(observabilityRepositoryRoot(t), "deploy", "observability")
}

func observabilityRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve observability test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(current), "..", ".."))
}

func readObservabilityFile(t *testing.T, directory, name string) []byte {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(directory, name))
	if err != nil {
		t.Fatalf("read observability file %q: %v", name, err)
	}
	return content
}

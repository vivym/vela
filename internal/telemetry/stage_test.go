package telemetry

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type staticStageReader struct {
	snapshot StageSnapshot
	err      error
}

func (reader staticStageReader) LatestStageSnapshot(context.Context) (StageSnapshot, error) {
	return reader.snapshot, reader.err
}

func TestStageCollectorExportsBoundedAuthoritativeDimensions(t *testing.T) {
	metrics := NewHTTPMetrics()
	collector := NewStageCollector(staticStageReader{snapshot: StageSnapshot{
		RunStates:                      []StageStateCount{{StageKind: "DIT", State: "RUNNING", Count: 7}},
		ReadyOldestAgeSeconds:          []StageValue{{StageKind: "VAE_DECODER", Value: 12}},
		TransferStates:                 []StateCount{{State: "CONSUMED", Count: 2}},
		TransferActiveOldestAgeSeconds: 31,
		HasActiveTransfers:             true,
		CacheStates:                    []StageStateCount{{StageKind: "ENCODER", State: "LIVE", Count: 3}},
		ResidencyStates:                []StageStateCount{{StageKind: "DIT", State: "READY", Count: 7}},
	}})
	if err := metrics.Register(collector); err != nil {
		t.Fatalf("register Stage collector: %v", err)
	}

	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := response.Body.String()
	for _, expected := range []string{
		`vela_stage_run_state_count{stage_kind="DIT",state="RUNNING"} 7`,
		`vela_stage_ready_oldest_age_seconds{stage_kind="VAE_DECODER"} 12`,
		`vela_stage_transfer_ticket_state_count{state="CONSUMED"} 2`,
		`vela_stage_transfer_active_oldest_age_seconds 31`,
		`vela_stage_cache_entry_state_count{stage_kind="ENCODER",state="LIVE"} 3`,
		`vela_stage_model_residency_state_count{stage_kind="DIT",state="READY"} 7`,
		`vela_stage_authority_exporter_last_scrape_success 1`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("Stage metrics omit %q:\n%s", expected, body)
		}
	}
	for _, forbidden := range []string{
		"organization_id", "project_id", "job_id", "attempt_id", "worker_id",
		"worker_instance_id", "model_revision_id", "stage_run_id",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("Stage metrics contain forbidden high-cardinality identity %q:\n%s", forbidden, body)
		}
	}
}

func TestStageCollectorFailsClosedOnDatabaseError(t *testing.T) {
	metrics := NewHTTPMetrics()
	if err := metrics.Register(NewStageCollector(staticStageReader{
		err: errors.New("database unavailable"),
	})); err != nil {
		t.Fatalf("register failing Stage collector: %v", err)
	}

	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := response.Body.String()
	if !strings.Contains(body, "vela_stage_authority_exporter_last_scrape_success 0") ||
		strings.Contains(body, "vela_stage_run_state_count") {
		t.Fatalf("Stage collector did not fail closed:\n%s", body)
	}
}

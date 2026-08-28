package telemetry

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type staticSLOReader struct {
	reports []SLOMetric
	err     error
}

func (reader staticSLOReader) LatestSLOReports(context.Context) ([]SLOMetric, error) {
	return reader.reports, reader.err
}

func TestSLOCollectorExportsOnlyControlledCatalogDimensions(t *testing.T) {
	metrics := NewHTTPMetrics()
	err := metrics.Register(NewSLOCollector(staticSLOReader{reports: []SLOMetric{{
		ModelRevisionID: "model-v1", GenerationPreset: "fast",
		GenerationPresetRevisionID: "fast-v1", ServiceClassRevisionID: "standard-v1",
		OutputSpecID: "1080p-v1", GenerationCount: 1, Result: "PASS",
		P95TargetMilliseconds: 30000, SuccessTargetPPM: 900000,
		P95Milliseconds: 19000, SuccessLowerBoundPPM: 950000,
		EligibleCount: 100, SucceededCount: 98, FailedCount: 2,
		CustomerCanceledCount: 3, OpenCount: 0, SealedAt: time.Unix(100, 0),
	}}}))
	if err != nil {
		t.Fatalf("register SLO collector: %v", err)
	}
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := response.Body.String()
	if !strings.Contains(body, `vela_slo_report_p95_milliseconds{generation_count="1",generation_preset="fast"`) ||
		!strings.Contains(body, `service_class_revision="standard-v1"`) ||
		!strings.Contains(body, `vela_slo_report_succeeded_jobs{generation_count="1",generation_preset="fast"`) ||
		!strings.Contains(body, `vela_slo_report_failed_jobs{generation_count="1",generation_preset="fast"`) ||
		!strings.Contains(body, `vela_slo_report_customer_canceled_jobs{generation_count="1",generation_preset="fast"`) ||
		strings.Contains(body, "organization_id") || strings.Contains(body, "project_id") ||
		strings.Contains(body, "job_id") || strings.Contains(body, "attempt_id") {
		t.Fatalf("unexpected statistical SLO metrics:\n%s", body)
	}
}

func TestSLOCollectorFailsClosedOnDatabaseError(t *testing.T) {
	metrics := NewHTTPMetrics()
	if err := metrics.Register(NewSLOCollector(staticSLOReader{err: errors.New("database unavailable")})); err != nil {
		t.Fatalf("register failing SLO collector: %v", err)
	}
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(response.Body.String(), "vela_slo_report_exporter_last_scrape_success 0") {
		t.Fatalf("missing fail-closed exporter metric:\n%s", response.Body.String())
	}
}

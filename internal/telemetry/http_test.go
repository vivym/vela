package telemetry

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

func TestHTTPMetricsUseRoutePatternsAndNeverRawIdentifiers(t *testing.T) {
	metrics := NewHTTPMetrics()
	router := chi.NewRouter()
	router.Use(metrics.Middleware)
	router.Get("/v1/jobs/{job_id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/jobs/customer-job-123", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("API response = %d", response.Code)
	}

	metricsResponse := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(metricsResponse, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, err := io.ReadAll(metricsResponse.Result().Body)
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, `vela_api_requests_total{method="GET",route="/v1/jobs/{job_id}",status="204"} 1`) {
		t.Fatalf("route-pattern metric missing:\n%s", text)
	}
	if strings.Contains(text, "customer-job-123") || strings.Contains(text, "organization_id") ||
		strings.Contains(text, "project_id") || strings.Contains(text, "attempt_id") {
		t.Fatalf("metrics contain a forbidden high-cardinality label or value:\n%s", text)
	}
}

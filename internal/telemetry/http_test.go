package telemetry

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	veladb "github.com/vivym/vela/internal/database"
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

func TestDatabaseRoleObservationCarriesServerRequestIDAndBoundedLabels(t *testing.T) {
	metrics := NewHTTPMetrics()
	var logs bytes.Buffer
	metrics.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	router := chi.NewRouter()
	router.Use(metrics.Middleware)
	router.Get("/v1/jobs/{job_id}", func(w http.ResponseWriter, r *http.Request) {
		metrics.ObserveRequestRole(r.Context(), veladb.RequestRoleObservation{
			Surface:       veladb.RequestRoleSurfaceJobRead,
			DatabaseLogin: "vela_request_login",
			DatabaseRole:  veladb.RoleRequest,
		})
		w.WriteHeader(http.StatusNoContent)
	})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/jobs/customer-job-123", nil)
	request.Header.Set("X-Request-ID", "client-controlled-request-id")
	router.ServeHTTP(response, request)
	requestID := response.Header().Get("X-Request-ID")
	if _, err := uuid.Parse(requestID); err != nil {
		t.Fatalf("X-Request-ID = %q, want server-generated UUID: %v", requestID, err)
	}
	if requestID == "client-controlled-request-id" {
		t.Fatal("server reused a client-controlled request ID")
	}

	metricsResponse := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(metricsResponse, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	metricText := metricsResponse.Body.String()
	if !strings.Contains(metricText,
		`vela_database_request_role_transactions_total{database_login="vela_request_login",database_role="vela_request",surface="job_read"} 1`) {
		t.Fatalf("request-role metric missing:\n%s", metricText)
	}
	if strings.Contains(metricText, requestID) || strings.Contains(metricText, "customer-job-123") {
		t.Fatalf("request-role metrics contain high-cardinality request data:\n%s", metricText)
	}
	logText := logs.String()
	if !strings.Contains(logText, `"msg":"database request role verified"`) ||
		!strings.Contains(logText, `"request_id":"`+requestID+`"`) ||
		!strings.Contains(logText, `"surface":"job_read"`) ||
		!strings.Contains(logText, `"database_login":"vela_request_login"`) ||
		!strings.Contains(logText, `"database_role":"vela_request"`) ||
		strings.Contains(logText, "customer-job-123") ||
		strings.Contains(logText, "client-controlled-request-id") {
		t.Fatalf("request-correlated database-role log is invalid:\n%s", logText)
	}
}

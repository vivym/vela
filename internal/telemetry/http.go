package telemetry

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/felixge/httpsnoop"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	veladb "github.com/vivym/vela/internal/database"
)

const requestIDHeader = "X-Request-ID"

type requestIDContextKey struct{}

// HTTPMetrics owns the control-plane API metric registry. It deliberately uses
// only bounded HTTP method, OpenAPI route pattern and status-code labels.
type HTTPMetrics struct {
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	dbRoles  *prometheus.CounterVec
	logger   *slog.Logger
}

func NewHTTPMetrics() *HTTPMetrics {
	registry := prometheus.NewRegistry()
	metrics := &HTTPMetrics{
		registry: registry,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "vela_api_requests_total",
			Help: "Public API requests classified by method, OpenAPI route and status code.",
		}, []string{"method", "route", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "vela_api_request_duration_seconds",
			Help:    "Public API request latency by method and OpenAPI route.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"method", "route"}),
		dbRoles: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "vela_database_request_role_transactions_total",
			Help: "Verified public-request database transactions by bounded surface, login and role.",
		}, []string{"surface", "database_login", "database_role"}),
		logger: slog.Default(),
	}
	registry.MustRegister(
		metrics.requests,
		metrics.duration,
		metrics.dbRoles,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return metrics
}

func (metrics *HTTPMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := uuid.NewString()
		w.Header().Set(requestIDHeader, requestID)
		r = r.WithContext(context.WithValue(r.Context(), requestIDContextKey{}, requestID))
		startedAt := time.Now()
		captured := httpsnoop.CaptureMetrics(next, w, r)
		route := "unmatched"
		if routeContext := chi.RouteContext(r.Context()); routeContext != nil {
			if pattern := routeContext.RoutePattern(); pattern != "" {
				route = pattern
			}
		}
		method := controlledMethod(r.Method)
		metrics.requests.WithLabelValues(method, route, strconv.Itoa(captured.Code)).Inc()
		metrics.duration.WithLabelValues(method, route).Observe(time.Since(startedAt).Seconds())
	})
}

func (metrics *HTTPMetrics) ObserveRequestRole(
	ctx context.Context,
	observation veladb.RequestRoleObservation,
) {
	metrics.dbRoles.WithLabelValues(
		string(observation.Surface),
		observation.DatabaseLogin,
		string(observation.DatabaseRole),
	).Inc()
	requestID, _ := ctx.Value(requestIDContextKey{}).(string)
	metrics.logger.InfoContext(
		ctx,
		"database request role verified",
		"request_id", requestID,
		"surface", observation.Surface,
		"database_login", observation.DatabaseLogin,
		"database_role", observation.DatabaseRole,
	)
}

func (metrics *HTTPMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

func (metrics *HTTPMetrics) Register(collector prometheus.Collector) error {
	return metrics.registry.Register(collector)
}

func controlledMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
		http.MethodPatch, http.MethodDelete, http.MethodOptions:
		return method
	default:
		return "OTHER"
	}
}

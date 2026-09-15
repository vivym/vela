package tracing_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func providerEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{"VELA_TRACING_ENABLED", "OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_TRACES_SAMPLER_ARG",
		"OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
		"OTEL_EXPORTER_OTLP_CERTIFICATE", "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE"} {
		t.Setenv(key, "")
	}
	old, oldHandler := otel.GetTracerProvider(), otel.GetErrorHandler()
	t.Cleanup(func() {
		if otel.GetTracerProvider() != old {
			otel.SetTracerProvider(old)
		}
		if otel.GetErrorHandler() != oldHandler {
			otel.SetErrorHandler(oldHandler)
		}
	})
}

func TestProviderExportsOTLPAndFlushesAfterProcessCancellation(t *testing.T) {
	for _, specific := range []bool{false, true} {
		t.Run(map[bool]string{false: "generic-endpoint", true: "trace-endpoint"}[specific], func(t *testing.T) {
			providerEnvironment(t)
			exports := make(chan *collector.ExportTraceServiceRequest, 8)
			paths := make(chan string, 8)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
				batch := &collector.ExportTraceServiceRequest{}
				if err != nil || proto.Unmarshal(body, batch) != nil || r.Header.Get("Content-Type") != "application/x-protobuf" {
					t.Error("invalid OTLP HTTP protobuf request")
					w.WriteHeader(400)
					return
				}
				exports <- batch
				paths <- r.URL.Path
				w.Header().Set("Content-Type", "application/x-protobuf")
			}))
			defer server.Close()
			t.Setenv("VELA_TRACING_ENABLED", "true")
			t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0")
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "private.resource=private-environment")
			t.Setenv("OTEL_SERVICE_NAME", "private-service-override")
			path := "/v1/traces"
			if specific {
				path = "/custom/traces"
				t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", server.URL+path)
			} else {
				t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
			}
			ctx, cancel := context.WithCancel(context.Background())
			stop, err := tracing.Start(ctx, "vela-test")
			if err != nil {
				t.Fatal(err)
			}
			_, root := otel.Tracer("test").Start(context.Background(), "unsampled-root")
			root.End()
			parent := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": parentHeader})
			_, child := otel.Tracer("test").Start(parent, "sampled-child")
			child.End()
			cancel()
			stop()
			select {
			case batch := <-exports:
				if received := <-paths; received != path {
					t.Fatalf("OTLP request path %q, want %q", received, path)
				}
				encoded := protojson.Format(batch)
				if !strings.Contains(encoded, "sampled-child") || !strings.Contains(encoded, "vela-test") ||
					strings.Contains(encoded, "unsampled-root") || strings.Contains(encoded, "private-") {
					t.Fatalf("sampling, resource bounds or shutdown flush failed: %s", encoded)
				}
			case <-time.After(time.Second):
				t.Fatal("shutdown did not export completed sampled span")
			}
		})
	}
}

func TestProviderRejectsInvalidConfigurationWithoutEchoingInput(t *testing.T) {
	for name, value := range map[string]string{
		"flag": "yes", "missing-endpoint": "", "scheme": "ftp://collector", "credentials": "https://user:private-password@collector",
		"query": "https://collector?token=private-token", "fragment": "http://collector/#private-fragment", "ratio-nan": "NaN",
		"ratio-inf": "+Inf", "ratio-negative": "-0.1", "ratio-large": "1.01",
	} {
		t.Run(name, func(t *testing.T) {
			providerEnvironment(t)
			t.Setenv("VELA_TRACING_ENABLED", "true")
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")
			switch {
			case name == "flag":
				t.Setenv("VELA_TRACING_ENABLED", value)
			case strings.HasPrefix(name, "ratio-"):
				t.Setenv("OTEL_TRACES_SAMPLER_ARG", value)
			default:
				t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", value)
			}
			stop, err := tracing.Start(context.Background(), "vela-test")
			if err == nil {
				stop()
				t.Fatal("invalid configuration accepted")
			}
			if strings.Contains(err.Error(), "private-") {
				t.Fatal("configuration error echoed sensitive input")
			}
		})
	}
	for _, flag := range []string{"", "false"} {
		t.Run("disabled-"+flag, func(t *testing.T) {
			providerEnvironment(t)
			t.Setenv("VELA_TRACING_ENABLED", flag)
			stop, err := tracing.Start(context.Background(), "vela-test")
			if err != nil {
				t.Fatal(err)
			}
			stop()
		})
	}
}

func TestCollectorFailureDoesNotBlockHTTPOrLogResponseData(t *testing.T) {
	providerEnvironment(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "private-collector-response", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	t.Setenv("VELA_TRACING_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT", "100")
	var output bytes.Buffer
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	defer slog.SetDefault(oldLogger)
	stop, err := tracing.Start(context.Background(), "vela-test")
	if err != nil {
		t.Fatal(err)
	}
	handler := tracing.HTTPServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	started := time.Now()
	// Exceed the configured queue capacity during an unavailable collector.
	for range 5000 {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
		if response.Code != http.StatusNoContent {
			t.Fatal("telemetry failure changed application response")
		}
	}
	elapsed := time.Since(started)
	stop()
	if elapsed > 3*time.Second {
		t.Fatalf("application requests waited for trace export: %s", elapsed)
	}
	if strings.Contains(output.String(), "private-") || strings.Contains(output.String(), server.URL) {
		t.Fatal("exporter error logged collector response or endpoint")
	}
}

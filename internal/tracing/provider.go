// Package tracing exports bounded, payload-free application traces. Trace context
// is correlation data and never grants application or runtime authority.
package tracing

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/url"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Start configures a process once, before its listeners or clients start. An
// absent flag leaves existing callers unchanged; enabled tracing requires an
// explicit OTLP endpoint. Collector outages do not block request execution.
func Start(ctx context.Context, service string) (func(), error) {
	if os.Getenv("VELA_TRACING_ENABLED") == "" || os.Getenv("VELA_TRACING_ENABLED") == "false" {
		return func() {}, nil
	}
	if os.Getenv("VELA_TRACING_ENABLED") != "true" {
		return nil, errors.New("VELA_TRACING_ENABLED must be true or false")
	}
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	if endpoint == "" {
		endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("tracing requires an explicit HTTP(S) OTLP endpoint without URL credentials or query")
	}
	ratio := 0.1
	if value := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); value != "" {
		ratio, err = strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 || ratio > 1 {
			return nil, errors.New("OTEL_TRACES_SAMPLER_ARG must be a finite ratio from zero to one")
		}
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		// Exporter errors can contain endpoint/response data; keep it out of logs.
		return nil, errors.New("configure application OTLP exporter")
	}
	identity := resource.NewSchemaless(attribute.String("service.name", service))
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(identity),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
		sdktrace.WithBatcher(resourceExporter{SpanExporter: exporter, identity: identity}, sdktrace.WithMaxQueueSize(2048),
			sdktrace.WithMaxExportBatchSize(256), sdktrace.WithBatchTimeout(time.Second),
			sdktrace.WithExportTimeout(5*time.Second)),
	)
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {
		slog.Error("application trace export failed")
	}))
	otel.SetTracerProvider(provider)
	return func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if provider.Shutdown(flushCtx) != nil {
			slog.Error("application trace shutdown did not complete")
		}
	}, nil
}

// The SDK merges OTEL_RESOURCE_ATTRIBUTES into WithResource. Override at the
// export boundary so arbitrary environment attributes never leave the process.
type resourceExporter struct {
	sdktrace.SpanExporter
	identity *resource.Resource
}

func (e resourceExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	bounded := make([]sdktrace.ReadOnlySpan, len(spans))
	for i, span := range spans {
		bounded[i] = resourceSpan{ReadOnlySpan: span, identity: e.identity}
	}
	return e.SpanExporter.ExportSpans(ctx, bounded)
}

type resourceSpan struct {
	sdktrace.ReadOnlySpan
	identity *resource.Resource
}

func (s resourceSpan) Resource() *resource.Resource { return s.identity }

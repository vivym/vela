package tracing

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// DurableParent returns only canonical W3C v00 trace context. Arbitrary vendor
// state and baggage are never persisted alongside business records.
func DurableParent(ctx context.Context) *string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return nil
	}
	sc = trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: sc.TraceID(), SpanID: sc.SpanID(),
		TraceFlags: sc.TraceFlags() & trace.FlagsSampled,
	})
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(trace.ContextWithSpanContext(ctx, sc), parentCarrier{carrier})
	parent := carrier.Get("traceparent")
	return &parent
}

// WithDurableParent starts from the saved message/job context, never from the
// polling loop's unrelated span. Missing or malformed context starts a new trace
// while preserving cancellation and other application context values.
func WithDurableParent(ctx context.Context, parent *string) context.Context {
	ctx = trace.ContextWithSpanContext(ctx, trace.SpanContext{})
	if parent == nil || CanonicalParent(*parent) == "" {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx, parentCarrier{propagation.MapCarrier{"traceparent": *parent}})
}

// CanonicalParent bounds optional diagnostic input before it can enter a
// journal. Invalid metadata is discarded rather than rejecting business work.
func CanonicalParent(parent string) string {
	if len(parent) != 55 || parent[:3] != "00-" || parent[35] != '-' || parent[52] != '-' ||
		(parent[53:] != "00" && parent[53:] != "01") {
		return ""
	}
	for _, part := range []string{parent[3:35], parent[36:52]} {
		nonzero := false
		for _, c := range part {
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return ""
			}
			nonzero = nonzero || c != '0'
		}
		if !nonzero {
			return ""
		}
	}
	return parent
}

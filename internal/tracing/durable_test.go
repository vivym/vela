package tracing_test

import (
	"context"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/tracing"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestDurableParentRoundTripAndIsolation(t *testing.T) {
	state, err := trace.ParseTraceState("vendor=private-state")
	if err != nil {
		t.Fatal(err)
	}
	input := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": parentHeader})
	sc := trace.SpanContextFromContext(input).WithTraceState(state).WithTraceFlags(0xff)
	input = trace.ContextWithSpanContext(input, sc)
	parent := tracing.DurableParent(input)
	if parent == nil || *parent != parentHeader {
		t.Fatalf("canonical parent = %v", parent)
	}
	loop, cancel := context.WithCancel(input)
	cancel()
	recovered := tracing.WithDurableParent(loop, parent)
	got := trace.SpanContextFromContext(recovered)
	if got.TraceID() != sc.TraceID() || got.SpanID() != sc.SpanID() || !got.IsRemote() || got.TraceState().Len() != 0 || got.TraceFlags() != trace.FlagsSampled || recovered.Err() != context.Canceled {
		t.Fatalf("durable propagation changed context or retained private state: %v", got)
	}
	if tracing.DurableParent(context.Background()) != nil {
		t.Fatal("absent span manufactured a durable parent")
	}
	unsampled := strings.TrimSuffix(parentHeader, "01") + "00"
	if got := trace.SpanContextFromContext(tracing.WithDurableParent(loop, &unsampled)); !got.IsValid() || got.IsSampled() {
		t.Fatal("unsampled parent lost")
	}
	for _, value := range []string{"", "private-invalid-input", strings.Repeat("x", 100000), strings.Replace(parentHeader, "00-", "ff-", 1), strings.Replace(parentHeader, "1234567890abcdef1234567890abcdef", strings.Repeat("0", 32), 1), strings.Replace(parentHeader, "-1234567890abcdef-", "-0000000000000000-", 1), parentHeader + "-extension"} {
		if trace.SpanContextFromContext(tracing.WithDurableParent(loop, &value)).IsValid() {
			t.Fatal("invalid durable parent retained polling context")
		}
	}
	if trace.SpanContextFromContext(tracing.WithDurableParent(loop, nil)).IsValid() {
		t.Fatal("nil parent retained polling trace")
	}
	for _, value := range []string{strings.ToUpper(parentHeader), parentHeader[:53] + "ff", parentHeader[:53] + "02", parentHeader[:53] + "03"} {
		if tracing.CanonicalParent(value) != "" || trace.SpanContextFromContext(tracing.WithDurableParent(loop, &value)).IsValid() {
			t.Fatal("noncanonical durable parent was not discarded")
		}
	}
}

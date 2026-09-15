package modelruntime_test

import (
	"context"
	"encoding/json"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/tracing"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestRuntimeTraceKeepsWatchdogParentAfterRequestCancellation(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()); otel.SetTracerProvider(old) })
	clock := newManualClock(time.Date(2026, 8, 30, 6, 30, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	authority := signRuntimeAuthority(t, signer, clock.Now())
	backend := modelruntime.NewFakeEncoderRuntime()
	service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
	client, _ := serveRuntime(t, service)
	parent := "00-abcdef1234567890abcdef1234567890-1234567890abcdef-01"
	ctx, cancel := context.WithCancel(tracing.WithDurableParent(t.Context(), &parent))
	prepared, err := client.PrepareStage(ctx, &velav1.ModelRuntimeServicePrepareStageRequest{Authority: authority, ExecutionSpec: runtimeExecutionSpec()})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("prepare: %v %v", prepared, err)
	}
	started, err := client.StartStage(ctx, &velav1.ModelRuntimeServiceStartStageRequest{Authority: authority})
	if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("start: %v %v", started, err)
	}
	cancel()
	clock.Advance(30 * time.Second)
	deadline := time.Now().Add(2 * time.Second)
	var watchdog sdktrace.ReadOnlySpan
	for watchdog == nil {
		for _, span := range recorder.Ended() {
			if span.Name() == "vela.runtime.deadline_cancel" {
				watchdog = span
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("watchdog did not finish its cancellation")
		}
		runtime.Gosched()
	}
	var prepare sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == "vela.runtime.prepare" {
			prepare = span
		}
	}
	if prepare == nil || watchdog.Parent().SpanID() != prepare.SpanContext().SpanID() || watchdog.SpanContext().TraceID() != prepare.SpanContext().TraceID() || watchdog.Status().Code == codes.Error {
		t.Fatal("watchdog lost the original Prepare parent or inherited caller cancellation")
	}
	inspection, err := client.InspectExecution(t.Context(), &velav1.ModelRuntimeServiceInspectExecutionRequest{SchemaVersion: 1, Authority: authority})
	if err != nil || inspection.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING {
		t.Fatal("trace completion did not correspond to actual cancellation")
	}
	// A nil gRPC error can still carry a rejected application operation.
	renewed := signRuntimeAuthority(t, signer, clock.Now())
	response, err := client.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: renewed})
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatal("renewal unexpectedly revived expired execution")
	}
	found := false
	for _, span := range recorder.Ended() {
		if span.Name() == "vela.runtime.status" && span.Status().Code == codes.Error {
			found = true
		}
	}
	if !found {
		t.Fatal("nil transport error hid a rejected runtime operation")
	}
	encoded, err := json.Marshal(tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"parameters_json", "lease_token", "execution_nonce", "output_manifest", "stage-key-9"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("trace exposed %s", forbidden)
		}
	}
	want := trace.SpanContextFromContext(tracing.WithDurableParent(t.Context(), &parent))
	if watchdog.SpanContext().TraceID() != want.TraceID() {
		t.Fatal("deadline trace is uncorrelated")
	}
}

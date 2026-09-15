package stageworkeragent_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/tracing"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const stageTraceParent = "00-abcdef1234567890abcdef1234567890-1234567890abcdef-01"

func recordStageTraces(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()); otel.SetTracerProvider(old) })
	return recorder
}

func assertStageTrace(t *testing.T, spans []sdktrace.ReadOnlySpan, operation string, count int) {
	t.Helper()
	parent := stageTraceParent
	want := trace.SpanContextFromContext(tracing.WithDurableParent(t.Context(), &parent))
	found := 0
	for _, span := range spans {
		if span.Name() != operation {
			continue
		}
		found++
		if span.SpanContext().TraceID() != want.TraceID() || span.SpanContext().TraceState().Len() != 0 {
			t.Fatalf("wrong trace for %s", operation)
		}
		if strings.HasPrefix(operation, "vela.stage.") && span.Parent().SpanID() != want.SpanID() {
			t.Fatalf("%s used polling or another stage's parent", operation)
		}
		if len(span.Attributes()) < 4 {
			t.Fatalf("%s omitted execution correlation", operation)
		}
	}
	if found != count {
		t.Fatalf("%s spans = %d, want %d", operation, found, count)
	}
}

func TestStageTraceSurvivesWorkerReopenRenewalAndStop(t *testing.T) {
	recorder := recordStageTraces(t)
	fixture := newSingleMemberMaterializationFixture(t)
	fixture.assignment.OriginTraceParent = stageTraceParent
	journal := durableFixtureAdmission(t, fixture)
	gate := journal.open(t)
	control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED}
	probe := &admissionRuntimeProbe{ModelRuntimeServiceClient: fixture.client}
	stream := durableFixtureStream(t, fixture, gate, control, nil, probe, nil)
	other := "00-99999999999999999999999999999999-7777777777777777-01"
	ctx := tracing.WithDurableParent(t.Context(), &other)
	if _, err := stream.ExecuteAcquiredAssignment(ctx, fixture.assignment, journal.acquireID); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Heartbeat(nil, 1); err == nil { //nolint:staticcheck // SA1012: deliberately verify that nil context is rejected before tracing.
		t.Fatal("nil heartbeat context was accepted")
	}
	renewal := proto.Clone(fixture.assignment).(*velav1.StageAssignment)
	renewal.Authority.StageVersion++
	renewal.Authority.IssuedAt = timestamppb.Now()
	renewal.Authority.ExpiresAt = timestamppb.New(renewal.Authority.ExpiresAt.AsTime().Add(time.Minute))
	journal.sign(t, renewal)
	control.renewedAuthority = renewal.Authority
	if _, err := stream.Heartbeat(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = journal.open(t)
	if got := admissionSnapshot(t, gate).Latest.OriginTraceParent; got != stageTraceParent {
		t.Fatal("reopened journal lost origin trace")
	}
	control.renewedAuthority = nil
	stream = durableFixtureStream(t, fixture, gate, control, nil, probe, nil)
	if result, err := stream.Reattach(ctx, renewal.Authority, "", nil); err != nil || !result.Accepted {
		t.Fatalf("reattach: %v", err)
	}
	if probe.prepares.Load() != 1 {
		t.Fatal("reattach repeated Prepare")
	}
	if _, err := stream.HandleStop(ctx, &velav1.StopStage{Authority: renewal.Authority, Reason: velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_PARENT_CANCELED}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Reattach(ctx, renewal.Authority, "", nil); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
		t.Fatalf("closed execution reopened: %v", err)
	}
	spans := recorder.Ended()
	for name, count := range map[string]int{"vela.stage.start": 1, "vela.stage.heartbeat": 1, "vela.stage.reattach": 2, "vela.stage.stop": 1, "vela.runtime.prepare": 1, "vela.runtime.start": 1, "vela.runtime.cancel": 1} {
		assertStageTrace(t, spans, name, count)
	}
	byID := map[trace.SpanID]sdktrace.ReadOnlySpan{}
	for _, span := range spans {
		byID[span.SpanContext().SpanID()] = span
	}
	for _, span := range spans {
		if span.Name() == "vela.runtime.prepare" {
			server := byID[span.Parent().SpanID()]
			if server == nil || server.SpanKind() != trace.SpanKindServer {
				t.Fatal("runtime missing actual gRPC server parent")
			}
			client := byID[server.Parent().SpanID()]
			if client == nil || client.SpanKind() != trace.SpanKindClient {
				t.Fatal("runtime missing actual gRPC client propagation")
			}
			worker := byID[client.Parent().SpanID()]
			if worker == nil || worker.Name() != "vela.stage.start" {
				t.Fatal("runtime RPC parent is not this assignment")
			}
		}
	}
}

func TestStageTraceMaterializationRecoveryUsesOriginalPendingExecution(t *testing.T) {
	recorder := recordStageTraces(t)
	fixture := newSingleMemberMaterializationFixture(t)
	fixture.assignment.OriginTraceParent = stageTraceParent
	journal := durableFixtureAdmission(t, fixture)
	gate := journal.open(t)
	control := newMaterializingStreamControl(t, fixture.authority)
	source, err := stageartifact.NewFilesystemLocalOutputSource(fixture.localRoot)
	if err != nil {
		t.Fatal(err)
	}
	outputRoot := t.TempDir()
	outputJournal, err := stageworkeragent.NewFileMaterializationJournal(outputRoot, 4)
	if err != nil {
		t.Fatal(err)
	}
	config := &stageworkeragent.MaterializationConfig{Validator: control.validator, Source: source, Publisher: &outageOncePublisher{failures: 1, objectVersion: "trace-l2-version"}, Journal: outputJournal,
		SourceLossEvidence: stageworkeragent.MaterializationSourceLossEvidenceFunc(func(context.Context, stageworkeragent.PendingMaterialization) (stageworkeragent.MaterializationSourceLossEvidence, error) {
			return stageworkeragent.MaterializationSourceLossEvidence{}, errors.New("unexpected source loss")
		})}
	stream := durableFixtureStream(t, fixture, gate, control, nil, nil, config)
	if _, err := stream.ExecuteAcquiredAssignment(t.Context(), fixture.assignment, journal.acquireID); err != nil {
		t.Fatal(err)
	}
	fixture.backend.MarkOutputReadyWithSize(fixture.manifest, int64(len(fixture.payload)))
	if result, err := stream.SealAndMaterialize(t.Context()); err == nil || !result.GPUReleased {
		t.Fatalf("expected recoverable outage: %+v %v", result, err)
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = journal.open(t)
	newer := journal.next(t, 2)
	newer.OriginTraceParent = "00-99999999999999999999999999999999-7777777777777777-01"
	beginAdmission(t, gate, newer, uuid.New()).Release()
	if err := outputJournal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := stageworkeragent.NewFileMaterializationJournal(outputRoot, 4)
	if err != nil {
		t.Fatal(err)
	}
	config.Journal = reopened
	stream = durableFixtureStream(t, fixture, gate, control, nil, nil, config)
	if result, err := stream.ResumeMaterializations(t.Context()); err != nil || !result.Committed {
		t.Fatalf("resume: %+v %v", result, err)
	}
	spans := recorder.Ended()
	assertStageTrace(t, spans, "vela.stage.materialize", 2)
	assertStageTrace(t, spans, "vela.stage.seal", 1)
	var states []codes.Code
	for _, span := range spans {
		if span.Name() == "vela.stage.materialize" {
			states = append(states, span.Status().Code)
		}
	}
	if len(states) != 2 || states[0] != codes.Error || states[1] != codes.Unset {
		t.Fatal("failed attempt and recovered publication lost their status")
	}
	if pending, err := reopened.List(t.Context()); err != nil || len(pending) != 0 {
		t.Fatal("successful recovery left materialization pending")
	}
}

func TestStageTraceControlErrorExcludesPrivateDetails(t *testing.T) {
	recorder := recordStageTraces(t)
	fixture := newSingleMemberMaterializationFixture(t)
	fixture.assignment.OriginTraceParent = stageTraceParent
	journal := durableFixtureAdmission(t, fixture)
	gate := journal.open(t)
	control := &recordingStreamControl{err: errors.New("private-stage-failure https://objects.invalid/model?credential=private-value")}
	stream := durableFixtureStream(t, fixture, gate, control, nil, nil, nil)
	if _, err := stream.ExecuteAcquiredAssignment(t.Context(), fixture.assignment, journal.acquireID); err == nil {
		t.Fatal("control failure was hidden")
	}
	spans := recorder.Ended()
	assertStageTrace(t, spans, "vela.stage.start", 1)
	for _, span := range spans {
		if span.Name() == "vela.stage.start" && span.Status().Code != codes.Error {
			t.Fatal("start span lost failure")
		}
	}
	encoded, err := json.Marshal(tracetest.SpanStubsFromReadOnlySpans(spans))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"private-", "objects.invalid", "credential", "parameters_json", "single-stage-key", "lease_token", "execution_nonce"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("trace exposed %s", forbidden)
		}
	}
}

func TestStageTraceInputIsOptionalBoundedAndDoesNotReplaceJournalOrigin(t *testing.T) {
	for _, parent := range []string{stageTraceParent, "", "private-" + strings.Repeat("x", 100000)} {
		t.Run(map[bool]string{true: "valid", false: "absent-or-invalid"}[parent == stageTraceParent], func(t *testing.T) {
			fixture := newAdmissionFixture(t)
			fixture.assignment.OriginTraceParent = parent
			gate := fixture.open(t)
			handle := beginAdmission(t, gate, fixture.assignment, fixture.acquireID)
			completeAdmissionInputs(t, handle)
			handle.Release()
			fixture.assignment.OriginTraceParent = "00-99999999999999999999999999999999-7777777777777777-01"
			handle = beginAdmission(t, gate, fixture.assignment, fixture.acquireID)
			completeAdmissionInputs(t, handle)
			handle.Release()
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			gate = fixture.open(t)
			if got := admissionSnapshot(t, gate).Latest.OriginTraceParent; got != tracing.CanonicalParent(parent) {
				t.Fatal("replay replaced journal origin")
			}
			wire, err := os.ReadFile(filepath.Join(fixture.config.Directory, "assignment-admission.json"))
			if err != nil {
				t.Fatal(err)
			}
			var state struct {
				SchemaVersion int `json:"schema_version"`
			}
			if err := json.Unmarshal(wire, &state); err != nil {
				t.Fatal(err)
			}
			want := 5
			if parent == stageTraceParent {
				want = 6
			}
			if state.SchemaVersion != want || strings.Contains(string(wire), "private-") {
				t.Fatal("journal version/bounds violated")
			}
		})
	}
}

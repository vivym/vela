//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/stageassignment"
	"github.com/vivym/vela/internal/tracing"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

func submitTracedStageJob(t *testing.T, handler http.Handler, key string, body []byte, parent string) httpResult {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/projects/"+testProjectID+"/jobs", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+testBearerCredential())
	r.Header.Set("Idempotency-Key", key)
	r.Header.Set("Traceparent", parent)
	r.Header.Set("Tracestate", "vendor=private-stage-state")
	w := httptest.NewRecorder()
	tracing.HTTPServer(handler).ServeHTTP(w, r)
	return httpResult{StatusCode: w.Code, Header: w.Header(), Body: w.Body.Bytes()}
}

func TestStageAssignmentTraceSurvivesDatabaseReplay(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()); otel.SetTracerProvider(old) })
	fixture := newStageSchedulerFixtureWithRequest(t, "durable-stage-trace", nil, asyncTraceParent)
	// Exercise the new wrapper's native Down/Up without changing existing grants.
	if err := goose.Down(fixture.database.Admin, filepath.Join("..", "..", "db", "migrations")); err != nil {
		t.Fatal(err)
	}
	if err := goose.Up(fixture.database.Admin, filepath.Join("..", "..", "db", "migrations")); err != nil {
		t.Fatal(err)
	}
	command, request := stageWorkerAcquireCommand(fixture), stageWorkerAcquireRequest(fixture)
	other := "00-99999999999999999999999999999999-7777777777777777-01"
	ctx := tracing.WithDurableParent(t.Context(), &other)
	first, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(ctx, command, request)
	if err != nil || first.Assignment == nil {
		t.Fatalf("acquire: %v", err)
	}
	var parent string
	if err := fixture.database.Admin.QueryRow(`SELECT origin_trace_parent FROM jobs WHERE id=$1`, first.Assignment.Authority.JobId).Scan(&parent); err != nil {
		t.Fatal(err)
	}
	if parent == "" || parent == asyncTraceParent || first.Assignment.GetOriginTraceParent() != parent {
		t.Fatal("assignment did not retain the admission span")
	}
	replayed, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(context.Background(), command, request)
	if err != nil || !proto.Equal(first.Assignment, replayed.Assignment) {
		t.Fatalf("new backend/pool changed durable assignment: %v", err)
	}
	a, _ := proto.MarshalOptions{Deterministic: true}.Marshal(first.Assignment)
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(replayed.Assignment)
	if !bytes.Equal(a, b) {
		t.Fatal("replay changed wire bytes")
	}
	// Trace metadata is deliberately not part of execution authority or its digest.
	first.Assignment.OriginTraceParent = "malformed-private-diagnostic"
	if _, err := stageassignment.Validate(first.Assignment); err != nil {
		t.Fatalf("diagnostic input rejected execution authority: %v", err)
	}
	var jobsRead, wrapperRead, oldWrapperRead bool
	if err := fixture.database.Admin.QueryRow(`SELECT
		has_table_privilege('vela_stage_worker_control','jobs','SELECT'),
		has_function_privilege('vela_stage_worker_control','vela_read_stage_assignment_execution(uuid,uuid)','EXECUTE'),
		has_function_privilege('vela_stage_worker_control','vela_read_stage_assignment_execution_v4(uuid,uuid)','EXECUTE')`).Scan(&jobsRead, &wrapperRead, &oldWrapperRead); err != nil {
		t.Fatal(err)
	}
	if jobsRead || !wrapperRead || oldWrapperRead {
		t.Fatal("trace lookup expanded worker SQL privileges")
	}
	want := trace.SpanContextFromContext(tracing.WithDurableParent(t.Context(), &parent))
	count := 0
	for _, span := range recorder.Ended() {
		if span.Name() == "vela.stage.assignment" {
			count++
			if span.SpanContext().TraceID() != want.TraceID() || span.Parent().SpanID() != want.SpanID() || span.SpanContext().TraceState().Len() != 0 {
				t.Fatal("assignment inherited a polling span or private state")
			}
		}
	}
	if count != 1 {
		t.Fatalf("assignment spans = %d; exact replay must not rebuild", count)
	}
}

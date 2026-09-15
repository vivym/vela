package tracing

import (
	"context"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// StartStage records only bounded execution IDs. Callers supply a static
// operation name, and validate execution authority independently of telemetry.
func StartStage(ctx context.Context, operation string, authority *velav1.StageAuthority) (context.Context, trace.Span) {
	attrs := make([]attribute.KeyValue, 0, 4)
	for _, id := range []struct{ key, value string }{
		{"vela.job.id", authority.GetJobId()},
		{"vela.stage_run.id", authority.GetStageRunId()},
		{"vela.stage_attempt.id", authority.GetStageAttemptId()},
		{"vela.worker_instance.id", authority.GetWorkerInstanceId()},
	} {
		if parsed, err := uuid.Parse(id.value); err == nil && parsed != uuid.Nil {
			attrs = append(attrs, attribute.String(id.key, parsed.String()))
		}
	}
	return otel.Tracer("vela/stage").Start(ctx, operation, trace.WithAttributes(attrs...))
}

// EndStage deliberately excludes raw errors, which may contain prompts, object
// URLs or credentials. Detailed business outcomes remain in their own records.
func EndStage(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, "stage operation failed")
	}
	span.End()
}

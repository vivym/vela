package modelruntime

import (
	"context"

	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/tracing"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

func startRuntimeTrace(ctx context.Context, operation string, verified stageauthority.Verified) (context.Context, func(velav1.ModelRuntimeCommandDecision)) {
	ctx, span := tracing.StartStage(ctx, operation, verified.Authority)
	return ctx, func(decision velav1.ModelRuntimeCommandDecision) {
		span.SetAttributes(attribute.Int("vela.runtime.decision", int(decision)))
		if decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED &&
			decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED {
			span.SetStatus(codes.Error, "runtime operation rejected")
		}
		span.End()
	}
}

package stagefinalization

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

type ReconcileResult struct {
	Claim      StageGraphFinalizationClaim
	Completion VisibleCompletionResult
}

func (s *Service) ReconcileNext(
	ctx context.Context,
	finalizer AuthenticatedFinalizer,
) (ReconcileResult, error) {
	claim, err := s.ClaimNextStageGraphFinalization(ctx, finalizer)
	if err != nil || claim.Decision == StageGraphFinalizationNoWork {
		return ReconcileResult{Claim: claim}, err
	}
	if claim.Decision != StageGraphFinalizationGranted || claim.JobID == uuid.Nil ||
		claim.JobVersion <= 0 || claim.Credentials.ClaimID == uuid.Nil ||
		claim.Credentials.AttemptID == uuid.Nil {
		return ReconcileResult{}, fmt.Errorf("stage graph finalization claim is incomplete")
	}
	completionID := uuid.NewSHA1(
		claim.JobID,
		[]byte(fmt.Sprintf(
			"vela-stage-graph-visible-completion-v1\n%s\n%s\n%d",
			claim.Credentials.ClaimID,
			claim.Credentials.AttemptID,
			claim.Credentials.Fence,
		)),
	)
	completion, err := s.CompleteStageGraphVisibleCompletion(
		ctx,
		finalizer,
		claim.Credentials,
		StageGraphVisibleCompletionCandidate{
			CompletionID: completionID, ExpectedJobVersion: claim.JobVersion,
		},
	)
	return ReconcileResult{Claim: claim, Completion: completion}, err
}

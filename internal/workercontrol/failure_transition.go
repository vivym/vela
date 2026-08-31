package workercontrol

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	store "github.com/vivym/vela/internal/store/sqlc"
)

type workerFailureTransition int

const (
	workerFailureReported workerFailureTransition = iota
	workerFailureDrain
	workerFailureLost
)

type attemptFailureTransition struct {
	Source                  store.ExecutionFailureSource
	AttemptState            store.AttemptState
	AllowRetry              bool
	WorkerTransition        workerFailureTransition
	ArtifactID              uuid.NullUUID
	ArtifactUploadID        uuid.NullUUID
	FinalizationFailureCode *string
}

func (s *Service) applyAttemptFailure(
	ctx context.Context,
	queries *store.Queries,
	workerRow store.LockWorkerAuthorityRow,
	authority store.LockFailureAuthorityRow,
	normalized normalizedFailureObservation,
	requestHash [sha256Size]byte,
	decidedAt time.Time,
	transition attemptFailureTransition,
) (RetryDecision, error) {
	binding, err := requireLegacyFailureBinding(authority)
	if err != nil {
		return RetryDecision{}, err
	}
	if !authority.WorkerPoolID.Valid {
		return RetryDecision{}, errors.New("legacy failure authority has no Worker pool")
	}
	workerPoolID := authority.WorkerPoolID.UUID
	if err := queries.LockExecutionFailureDecisionWrites(ctx); err != nil {
		return RetryDecision{}, fmt.Errorf("lock failure decision writes: %w", err)
	}
	protocol, err := queries.LockProfileCircuitProtocol(ctx)
	if err != nil {
		return RetryDecision{}, fmt.Errorf("lock ProfileCertification circuit protocol: %w", err)
	}
	project, err := queries.LockFailureProjectCounters(ctx, store.LockFailureProjectCountersParams{
		OrganizationID: authority.OrganizationID,
		ProjectID:      authority.ProjectID,
	})
	if err != nil {
		return RetryDecision{}, fmt.Errorf("lock Project failure counters: %w", err)
	}
	if project.RunningCount <= 0 {
		return RetryDecision{}, errors.New("project running counter is inconsistent with active Attempt")
	}
	if _, err := queries.LockFailurePoolCounters(ctx, workerPoolID); err != nil {
		return RetryDecision{}, fmt.Errorf("lock Worker pool failure counters: %w", err)
	}
	workerWasHealthy := protocol.RequireCircuitAggregation &&
		transition.Source == store.ExecutionFailureSourceWORKERREPORTED &&
		workerRow.ReachabilityCondition == store.WorkerReachabilityConditionHEALTHY
	circuitState := authority.CircuitBreakerState
	circuitOpen := false
	openedByDecision := false
	observedHealthyWorkers := int64(0)
	evidenceWindowStartedAt := decidedAt.Add(
		-time.Duration(authority.ExecutionCircuitFingerprintWindowSeconds) * time.Second,
	)
	if protocol.RequireCircuitAggregation {
		certification, lockErr := queries.LockProfileCertificationForFailure(
			ctx,
			store.LockProfileCertificationForFailureParams{
				ProfileCertificationID:     binding.profileCertificationID,
				ExecutionProfileRevisionID: binding.executionProfileRevisionID,
				ModelRevisionID:            authority.ModelRevisionID,
				GenerationPresetRevisionID: authority.GenerationPresetRevisionID,
				OutputSpecID:               authority.OutputSpecID,
			},
		)
		if lockErr != nil {
			return RetryDecision{}, fmt.Errorf("lock ProfileCertification for failure: %w", lockErr)
		}
		circuitOpen = certification.State == store.CatalogStateINVALID && certification.InvalidatedAt.Valid
		if certification.State == store.CatalogStateACTIVE && !certification.InvalidatedAt.Valid && workerWasHealthy {
			observedHealthyWorkers, err = queries.CountProfileCircuitHealthyWorkers(
				ctx,
				store.CountProfileCircuitHealthyWorkersParams{
					ProfileCertificationID: authority.ProfileCertificationID,
					FailureFingerprint:     normalized.FailureFingerprint,
					EvidenceWindowStartedAt: pgtype.Timestamptz{
						Time:  evidenceWindowStartedAt,
						Valid: true,
					},
					DecidedAt:               pgtype.Timestamptz{Time: decidedAt, Valid: true},
					CurrentWorkerID:         binding.workerID,
					CurrentWorkerWasHealthy: true,
				},
			)
			if err != nil {
				return RetryDecision{}, fmt.Errorf("count ProfileCertification circuit evidence: %w", err)
			}
			if observedHealthyWorkers >= int64(authority.ExecutionCircuitMinDistinctHealthyWorkers) {
				rows, invalidateErr := queries.InvalidateProfileCertificationForCircuit(
					ctx,
					store.InvalidateProfileCertificationForCircuitParams{
						OpenedAt:               pgtype.Timestamptz{Time: decidedAt, Valid: true},
						ProfileCertificationID: binding.profileCertificationID,
					},
				)
				if invalidateErr != nil || rows != 1 {
					return RetryDecision{}, changedRowsError(
						"invalidate ProfileCertification for circuit",
						rows,
						invalidateErr,
					)
				}
				circuitOpen = true
				openedByDecision = true
			}
		}
		if circuitOpen {
			circuitState = []byte(`{"state":"OPEN"}`)
		}
	}

	credit, err := queries.LockFailureCreditReservation(ctx, authority.JobID)
	if err != nil {
		return RetryDecision{}, fmt.Errorf("lock CreditReservation for failure: %w", err)
	}
	if credit.OrganizationID != authority.OrganizationID || credit.ProjectID != authority.ProjectID ||
		credit.State != store.CreditReservationStateRESERVED {
		return RetryDecision{}, errors.New("credit reservation is inconsistent with active Attempt")
	}

	attemptCompute, err := attemptComputeSeconds(authority, decidedAt)
	if err != nil {
		return RetryDecision{}, err
	}
	if attemptCompute > math.MaxInt64-authority.ComputeSecondsConsumed {
		return RetryDecision{}, errors.New("retry budget compute accounting overflows seconds")
	}
	totalCompute := authority.ComputeSecondsConsumed + attemptCompute
	attemptFinalization, err := attemptFinalizationSeconds(authority, decidedAt)
	if err != nil {
		return RetryDecision{}, err
	}
	if attemptFinalization > math.MaxInt64-authority.FinalizationSecondsConsumed {
		return RetryDecision{}, errors.New("retry budget finalization accounting overflows seconds")
	}
	totalFinalization := authority.FinalizationSecondsConsumed + attemptFinalization
	nextRetryAt, retry := retryTime(authority, normalized, totalCompute, decidedAt)
	retry = retry && transition.AllowRetry
	if retry && circuitOpen {
		retry, err = queries.HasAlternateActiveProfileCertification(
			ctx,
			store.HasAlternateActiveProfileCertificationParams{
				ModelRevisionID:            authority.ModelRevisionID,
				GenerationPresetRevisionID: authority.GenerationPresetRevisionID,
				OutputSpecID:               authority.OutputSpecID,
				ProfileCertificationID:     binding.profileCertificationID,
				ExecutionProfileRevisionID: binding.executionProfileRevisionID,
				WorkerPoolID:               authority.WorkerPoolID,
			},
		)
		if err != nil {
			return RetryDecision{}, fmt.Errorf("check alternate active ProfileCertification: %w", err)
		}
	}
	if !retry {
		nextRetryAt = nil
		organizationCredit, lockErr := queries.LockFailureOrganizationCredit(ctx, authority.OrganizationID)
		if lockErr != nil {
			return RetryDecision{}, fmt.Errorf("lock Organization credit for failure: %w", lockErr)
		}
		if organizationCredit.Currency != credit.Currency || organizationCredit.ReservedMinor < credit.AmountMinor {
			return RetryDecision{}, errors.New("organization credit is inconsistent with credit reservation")
		}
	}
	if authority.CurrentFence == math.MaxInt64 {
		return RetryDecision{}, errors.New("job fence overflows bigint")
	}
	jobFence := authority.CurrentFence + 1
	decidedAtValue := pgtype.Timestamptz{Time: decidedAt, Valid: true}

	var rows int64
	switch transition.AttemptState {
	case store.AttemptStateFAILED:
		rows, err = queries.MarkAttemptFailed(ctx, store.MarkAttemptFailedParams{
			DecidedAt:   decidedAtValue,
			AttemptID:   authority.AttemptID,
			WorkerID:    authority.WorkerID,
			WorkerEpoch: authority.WorkerEpoch,
			Fence:       authority.AttemptFence,
		})
	case store.AttemptStateLOST:
		rows, err = queries.MarkAttemptLost(ctx, store.MarkAttemptLostParams{
			DecidedAt:   decidedAtValue,
			AttemptID:   authority.AttemptID,
			WorkerID:    authority.WorkerID,
			WorkerEpoch: authority.WorkerEpoch,
			Fence:       authority.AttemptFence,
		})
	default:
		return RetryDecision{}, errors.New("unsupported terminal Attempt state")
	}
	if err != nil || rows != 1 {
		return RetryDecision{}, changedRowsError("transition Attempt to terminal failure", rows, err)
	}
	if rows, updateErr := queries.RevokeExecutionLeaseForFailure(ctx, store.RevokeExecutionLeaseForFailureParams{
		DecidedAt:   decidedAtValue,
		LeaseID:     authority.LeaseID,
		AttemptID:   authority.AttemptID,
		WorkerID:    binding.workerID,
		WorkerEpoch: binding.workerEpoch,
		Fence:       authority.AttemptFence,
	}); updateErr != nil || rows != 1 {
		return RetryDecision{}, changedRowsError("revoke active Lease for failure", rows, updateErr)
	}
	if err := updateWorkerForFailureTransition(
		ctx,
		queries,
		workerRow,
		authority,
		normalized.WorkerReusable,
		decidedAtValue,
		transition.WorkerTransition,
	); err != nil {
		return RetryDecision{}, err
	}

	excludedWorkers, err := appendExcludedWorker(
		authority.ExcludedWorkers,
		excludedWorkerRecord{
			WorkerID:    binding.workerID,
			WorkerEpoch: binding.workerEpoch,
			Reason:      normalized.FailureClass,
			ExpiresAt:   authority.JobExpiresAt.Time,
		},
	)
	if err != nil {
		return RetryDecision{}, err
	}
	fingerprints, err := appendFailureFingerprint(
		authority.FailureFingerprints,
		failureFingerprintRecord{
			Fingerprint:  normalized.FailureFingerprint,
			FailureClass: normalized.FailureClass,
			AttemptID:    authority.AttemptID,
			ObservedAt:   decidedAt,
		},
	)
	if err != nil {
		return RetryDecision{}, err
	}
	nextRetryValue := pgtype.Timestamptz{}
	if nextRetryAt != nil {
		nextRetryValue = pgtype.Timestamptz{Time: *nextRetryAt, Valid: true}
	}
	failureClass := normalized.FailureClass
	if rows, updateErr := queries.UpdateRetryRuntimeForFailure(ctx, store.UpdateRetryRuntimeForFailureParams{
		TotalComputeSeconds:         totalCompute,
		TotalFinalizationSeconds:    totalFinalization,
		FinalizationRetryIncrement:  finalizationRetryIncrement(authority, retry),
		NextRetryAt:                 nextRetryValue,
		CircuitBreakerState:         circuitState,
		FailureClass:                &failureClass,
		DecidedAt:                   decidedAtValue,
		JobID:                       authority.JobID,
		ExpectedVersion:             authority.RetryRuntimeVersion,
		PreviousComputeSeconds:      authority.ComputeSecondsConsumed,
		PreviousFinalizationSeconds: authority.FinalizationSecondsConsumed,
	}); updateErr != nil || rows != 1 {
		return RetryDecision{}, changedRowsError("update RetryRuntimeState for failure", rows, updateErr)
	}
	if rows, updateErr := queries.UpdateExecutionRetryEvidence(ctx, store.UpdateExecutionRetryEvidenceParams{
		ExcludedWorkers:     excludedWorkers,
		FailureFingerprints: fingerprints,
		DecidedAt:           decidedAtValue,
		JobID:               authority.JobID,
	}); updateErr != nil || rows != 1 {
		return RetryDecision{}, changedRowsError("update protected retry evidence", rows, updateErr)
	}

	var jobVersion int64
	if retry {
		if rows, updateErr := queries.MoveProjectRunningToRetryWait(ctx, store.MoveProjectRunningToRetryWaitParams{
			OrganizationID: authority.OrganizationID,
			ProjectID:      authority.ProjectID,
		}); updateErr != nil || rows != 1 {
			return RetryDecision{}, changedRowsError("move Project running counter to retry wait", rows, updateErr)
		}
		if rows, updateErr := queries.IncrementPoolRetryWait(ctx, workerPoolID); updateErr != nil || rows != 1 {
			return RetryDecision{}, changedRowsError("increment Worker pool retry-wait counter", rows, updateErr)
		}
		jobVersion, err = queries.MarkJobRetryWait(ctx, store.MarkJobRetryWaitParams{
			JobFence:        jobFence,
			DecidedAt:       decidedAtValue,
			JobID:           authority.JobID,
			ExpectedVersion: authority.JobVersion,
			PreviousFence:   authority.CurrentFence,
		})
		if err != nil {
			return RetryDecision{}, fmt.Errorf("transition Job to RETRY_WAIT: %w", err)
		}
	} else {
		if rows, updateErr := queries.DecrementProjectRunningForFailure(ctx, store.DecrementProjectRunningForFailureParams{
			OrganizationID: authority.OrganizationID,
			ProjectID:      authority.ProjectID,
		}); updateErr != nil || rows != 1 {
			return RetryDecision{}, changedRowsError("decrement Project running counter for failure", rows, updateErr)
		}
		jobVersion, err = queries.MarkJobFailedFromActive(ctx, store.MarkJobFailedFromActiveParams{
			JobFence:        jobFence,
			DecidedAt:       decidedAtValue,
			JobID:           authority.JobID,
			ExpectedVersion: authority.JobVersion,
			PreviousFence:   authority.CurrentFence,
		})
		if err != nil {
			return RetryDecision{}, fmt.Errorf("transition Job to FAILED: %w", err)
		}
		if err := releaseFailureCredit(ctx, queries, authority.OrganizationID, authority.JobID, credit, decidedAtValue); err != nil {
			return RetryDecision{}, err
		}
	}

	disposition := store.RetryDispositionFAILED
	if retry {
		disposition = store.RetryDispositionRETRYWAIT
	}
	decisionID := uuid.New()
	if err := queries.InsertExecutionFailureDecision(ctx, store.InsertExecutionFailureDecisionParams{
		ID:                         decisionID,
		OrganizationID:             authority.OrganizationID,
		ProjectID:                  authority.ProjectID,
		JobID:                      authority.JobID,
		AttemptID:                  uuid.NullUUID{UUID: authority.AttemptID, Valid: true},
		WorkerID:                   authority.WorkerID,
		WorkerEpoch:                authority.WorkerEpoch,
		AttemptFence:               &authority.AttemptFence,
		Source:                     transition.Source,
		Disposition:                disposition,
		AttemptState:               &transition.AttemptState,
		FailureClass:               normalized.FailureClass,
		FailureFingerprint:         normalized.FailureFingerprint,
		RequestHash:                requestHash[:],
		ErrorSummary:               normalized.ErrorSummary,
		BackendStage:               normalized.BackendStage,
		GpuUuids:                   mustMarshalJSON(normalized.GPUUUIDs),
		InferenceBackendRevision:   normalized.InferenceBackendRevision,
		RetryRecommended:           normalized.RetryRecommended,
		WorkerReusable:             normalized.WorkerReusable,
		CircuitProtocolVersion:     protocol.CircuitProtocolVersion,
		WorkerWasHealthy:           workerWasHealthy,
		AttemptComputeSeconds:      attemptCompute,
		TotalComputeSeconds:        totalCompute,
		AttemptFinalizationSeconds: attemptFinalization,
		TotalFinalizationSeconds:   totalFinalization,
		ArtifactID:                 transition.ArtifactID,
		ArtifactUploadID:           transition.ArtifactUploadID,
		FinalizationFailureCode:    transition.FinalizationFailureCode,
		NextRetryAt:                nextRetryValue,
		JobFence:                   jobFence,
		JobVersion:                 jobVersion,
		DecidedAt:                  decidedAtValue,
	}); err != nil {
		return RetryDecision{}, fmt.Errorf("insert durable RetryDecision: %w", err)
	}
	if openedByDecision {
		if err := queries.InsertProfileCertificationCircuitOpening(
			ctx,
			store.InsertProfileCertificationCircuitOpeningParams{
				ID:                                   uuid.New(),
				OrganizationID:                       authority.OrganizationID,
				ProjectID:                            authority.ProjectID,
				ProfileCertificationID:               binding.profileCertificationID,
				ExecutionProfileRevisionID:           binding.executionProfileRevisionID,
				TriggeringExecutionFailureDecisionID: decisionID,
				TriggeringJobID:                      authority.JobID,
				TriggeringAttemptID:                  authority.AttemptID,
				TriggeringWorkerID:                   binding.workerID,
				TriggeringWorkerEpoch:                binding.workerEpoch,
				TriggeringAttemptFence:               authority.AttemptFence,
				FailureClass:                         normalized.FailureClass,
				FailureFingerprint:                   normalized.FailureFingerprint,
				InferenceBackendRevision:             normalized.InferenceBackendRevision,
				PolicyFingerprintWindowSeconds:       authority.ExecutionCircuitFingerprintWindowSeconds,
				PolicyMinDistinctHealthyWorkers:      authority.ExecutionCircuitMinDistinctHealthyWorkers,
				ObservedDistinctHealthyWorkers:       int32(observedHealthyWorkers),
				EvidenceWindowStartedAt: pgtype.Timestamptz{
					Time:  evidenceWindowStartedAt,
					Valid: true,
				},
				OpenedAt: decidedAtValue,
			},
		); err != nil {
			return RetryDecision{}, fmt.Errorf("insert ProfileCertification circuit opening: %w", err)
		}
	}
	if retry {
		if err := insertRetryWaitEvent(
			ctx,
			queries,
			authority,
			normalized,
			attemptCompute,
			totalCompute,
			attemptFinalization,
			totalFinalization,
			jobFence,
			jobVersion,
			*nextRetryAt,
			decidedAt,
		); err != nil {
			return RetryDecision{}, err
		}
	} else if err := insertJobFailedEvent(
		ctx,
		queries,
		authority,
		normalized.FailureClass,
		transition.AttemptState,
		attemptCompute,
		totalCompute,
		attemptFinalization,
		totalFinalization,
		jobFence,
		jobVersion,
		decidedAt,
	); err != nil {
		return RetryDecision{}, err
	}

	decisionDisposition := RetryDispositionFailed
	if retry {
		decisionDisposition = RetryDispositionRetryWait
	}
	return RetryDecision{
		Disposition:                decisionDisposition,
		FailureClass:               normalized.FailureClass,
		AttemptID:                  authority.AttemptID,
		JobID:                      authority.JobID,
		AttemptState:               AttemptTerminalState(transition.AttemptState),
		AttemptComputeSeconds:      attemptCompute,
		TotalComputeSeconds:        totalCompute,
		AttemptFinalizationSeconds: attemptFinalization,
		TotalFinalizationSeconds:   totalFinalization,
		NextRetryAt:                nextRetryAt,
		JobFence:                   jobFence,
		JobVersion:                 jobVersion,
		DecidedAt:                  decidedAt,
	}, nil
}

func finalizationRetryIncrement(authority store.LockFailureAuthorityRow, retry bool) int32 {
	if authority.AttemptState == store.AttemptStateFINALIZING && retry {
		return 1
	}
	return 0
}

const sha256Size = 32

func updateWorkerForFailureTransition(
	ctx context.Context,
	queries *store.Queries,
	workerRow store.LockWorkerAuthorityRow,
	authority store.LockFailureAuthorityRow,
	reusable bool,
	decidedAt pgtype.Timestamptz,
	transition workerFailureTransition,
) error {
	if authority.WorkerEpoch == nil || workerRow.Epoch != *authority.WorkerEpoch {
		return nil
	}
	if transition == workerFailureReported {
		return updateWorkerAfterReportedFailure(ctx, queries, workerRow, reusable, decidedAt)
	}
	var rows int64
	var err error
	if transition == workerFailureLost {
		rows, err = queries.MarkWorkerLostAfterFailure(ctx, store.MarkWorkerLostAfterFailureParams{
			DecidedAt:   decidedAt,
			WorkerID:    workerRow.ID,
			WorkerEpoch: workerRow.Epoch,
		})
	} else {
		rows, err = queries.MarkWorkerDrainingAfterFailure(ctx, store.MarkWorkerDrainingAfterFailureParams{
			DecidedAt:   decidedAt,
			WorkerID:    workerRow.ID,
			WorkerEpoch: workerRow.Epoch,
		})
	}
	if err != nil || rows != 1 {
		return changedRowsError("transition Worker after reconciled failure", rows, err)
	}
	return nil
}

func releaseFailureCredit(
	ctx context.Context,
	queries *store.Queries,
	organizationID uuid.UUID,
	jobID uuid.UUID,
	credit store.LockFailureCreditReservationRow,
	decidedAt pgtype.Timestamptz,
) error {
	if rows, err := queries.ReleaseFailureCreditReservation(ctx, store.ReleaseFailureCreditReservationParams{
		DecidedAt:           decidedAt,
		CreditReservationID: credit.ID,
		JobID:               jobID,
	}); err != nil || rows != 1 {
		return changedRowsError("release CreditReservation for failure", rows, err)
	}
	if rows, err := queries.ReleaseOrganizationCreditForFailure(ctx, store.ReleaseOrganizationCreditForFailureParams{
		AmountMinor:    credit.AmountMinor,
		DecidedAt:      decidedAt,
		OrganizationID: organizationID,
		Currency:       credit.Currency,
	}); err != nil || rows != 1 {
		return changedRowsError("release Organization credit for failure", rows, err)
	}
	return nil
}

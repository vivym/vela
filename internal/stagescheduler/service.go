package stagescheduler

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"maps"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/attemptcoordinator"
)

const AbandonAssignmentRejected = "ASSIGNMENT_REJECTED"

var ErrShadowReplayDiverged = errors.New("StageScheduler shadow replay diverged")

type WorkerAuthority struct {
	CapacityPoolID         uuid.UUID
	StageProfileRevisionID uuid.UUID
	WorkerInstanceID       uuid.UUID
	WorkerInstanceEpoch    int64
	DeviceSetDigest        []byte
	MembershipDigest       []byte
	ModelResidencyID       uuid.UUID
	ModelRuntimeEpoch      int64
	CapacityVector         map[string]int64
}

type CapacityObservation struct {
	Sequence int64
}

type Assignment struct {
	ClaimID           uuid.UUID
	DecisionID        uuid.UUID
	StageRunID        uuid.UUID
	StageAttemptID    uuid.UUID
	StageAllocationID uuid.UUID
	StageLeaseID      uuid.UUID
	LeaseToken        [32]byte
	LeaseExpiresAt    time.Time
	LocalDeadlineAt   time.Time
}

type CapturedSnapshot struct {
	ID       uuid.UUID
	Snapshot Snapshot
}

type ClaimRequest struct {
	ClaimID            uuid.UUID
	DecisionID         uuid.UUID
	CapturedSnapshotID uuid.UUID
	SchedulerID        string
	ClaimExpiresAt     time.Time
	Evidence           DecisionEvidence
	Command            attemptcoordinator.AssignStageCommand
}

type ClaimResult struct {
	ClaimID  uuid.UUID
	Replayed bool
}

type ShadowSnapshot struct {
	ID                     uuid.UUID
	Snapshot               Snapshot
	ExpectedEvidenceDigest [32]byte
}

type ShadowReplayReceipt struct {
	ID                     uuid.UUID
	SnapshotID             uuid.UUID
	AlgorithmRevision      string
	ExpectedEvidenceDigest [32]byte
	ReplayedEvidenceDigest [32]byte
	Matched                bool
	ReplayedAt             time.Time
	ReplayedBy             string
}

type ShadowReplaySummary struct {
	Processed int
	Matched   int
	Diverged  int
}

type stageRepository interface {
	Capture(context.Context, WorkerAuthority, CapacityObservation) (CapturedSnapshot, error)
	Claim(context.Context, ClaimRequest) (ClaimResult, error)
	Commit(context.Context, uuid.UUID, uuid.UUID) error
	Abandon(context.Context, uuid.UUID, string) error
	ReconcileExpired(context.Context, int) (int64, error)
	ListShadowSnapshots(context.Context, int) ([]ShadowSnapshot, error)
	RecordShadowReplay(context.Context, ShadowReplayReceipt) error
}

type stageAttemptCoordinator interface {
	Apply(context.Context, attemptcoordinator.StageCommand) (attemptcoordinator.StageDecision, error)
}

type Config struct {
	SchedulerID      string
	ClaimTTL         time.Duration
	LeaseTTL         time.Duration
	LocalDeadlineTTL time.Duration
	SigningKeyID     string
	Now              func() time.Time
	Random           io.Reader
	Metrics          *Metrics
}

type Service struct {
	repository  stageRepository
	coordinator stageAttemptCoordinator
	config      Config
	metrics     *Metrics
}

func NewService(
	repository stageRepository,
	coordinator stageAttemptCoordinator,
	config Config,
) (*Service, error) {
	if repository == nil {
		return nil, errors.New("StageScheduler repository is required")
	}
	if coordinator == nil {
		return nil, errors.New("StageScheduler AttemptCoordinator is required")
	}
	if config.SchedulerID == "" || len(config.SchedulerID) > 200 {
		return nil, errors.New("StageScheduler identity is invalid")
	}
	if config.ClaimTTL <= 0 || config.LeaseTTL <= 0 || config.LocalDeadlineTTL <= 0 ||
		config.LocalDeadlineTTL > config.LeaseTTL {
		return nil, errors.New("StageScheduler claim or lease deadline is invalid")
	}
	if config.SigningKeyID == "" || len(config.SigningKeyID) > 200 {
		return nil, errors.New("StageScheduler signing key identity is invalid")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	return &Service{
		repository:  repository,
		coordinator: coordinator,
		config:      config,
		metrics:     config.Metrics,
	}, nil
}

func (service *Service) Acquire(
	ctx context.Context,
	authority WorkerAuthority,
	observation CapacityObservation,
) (Assignment, bool, error) {
	if service == nil || service.repository == nil || service.coordinator == nil {
		return Assignment{}, false, errors.New("StageScheduler is not configured")
	}
	metricResult := struct {
		outcome metricOutcome
		reason  metricReason
	}{outcome: metricOutcomeError, reason: metricReasonInternal}
	defer func() {
		service.metrics.observeAcquire(metricResult.outcome, metricResult.reason)
	}()
	if err := validateWorkerAuthority(authority, observation); err != nil {
		metricResult.outcome = metricOutcomeRejected
		metricResult.reason = metricReasonInvalidAuthority
		return Assignment{}, false, err
	}
	captured, err := service.repository.Capture(ctx, authority, observation)
	if err != nil {
		metricResult.outcome = metricOutcomeRejected
		metricResult.reason = metricReasonForError(err)
		return Assignment{}, false, fmt.Errorf("capture StageScheduler snapshot: %w", err)
	}
	if captured.ID == uuid.Nil {
		return Assignment{}, false, errors.New("StageScheduler captured snapshot identity is missing")
	}
	evidence, selected, err := Decide(captured.Snapshot)
	if err != nil {
		metricResult.outcome = metricOutcomeRejected
		metricResult.reason = metricReasonForError(err)
		return Assignment{}, false, err
	}
	if !selected {
		metricResult.outcome = metricOutcomeNoWork
		metricResult.reason = metricReasonNone
		return Assignment{}, false, nil
	}

	assignment, command, err := service.newAssignment(authority, evidence)
	if err != nil {
		return Assignment{}, false, err
	}
	claim := ClaimRequest{
		ClaimID:            assignment.ClaimID,
		DecisionID:         assignment.DecisionID,
		CapturedSnapshotID: captured.ID,
		SchedulerID:        service.config.SchedulerID,
		ClaimExpiresAt:     service.config.Now().UTC().Add(service.config.ClaimTTL),
		Evidence:           evidence,
		Command:            command,
	}
	claimed, err := service.repository.Claim(ctx, claim)
	if err != nil {
		metricResult.outcome = metricOutcomeRejected
		metricResult.reason = metricReasonForError(err)
		return Assignment{}, false, fmt.Errorf("claim StageScheduler decision: %w", err)
	}
	if claimed.ClaimID != claim.ClaimID {
		return Assignment{}, false, errors.New("StageScheduler claim identity changed")
	}
	decision, err := service.coordinator.Apply(ctx, command)
	if err != nil {
		metricResult.outcome = metricOutcomeRejected
		metricResult.reason = metricReasonAssignmentRejected
		abandonErr := service.repository.Abandon(
			ctx, claim.ClaimID, AbandonAssignmentRejected,
		)
		if abandonErr != nil {
			return Assignment{}, false, errors.Join(
				err,
				fmt.Errorf("abandon rejected StageScheduler claim: %w", abandonErr),
			)
		}
		return Assignment{}, false, err
	}
	if decision.StageRunID != command.StageRunID ||
		decision.StageAttemptID != command.StageAttemptID || decision.State != "ASSIGNED" {
		return Assignment{}, false, errors.New("AttemptCoordinator returned mismatched Stage assignment")
	}
	if err := service.repository.Commit(ctx, claim.ClaimID, command.StageAttemptID); err != nil {
		return Assignment{}, false, fmt.Errorf("commit StageScheduler claim: %w", err)
	}
	metricResult.outcome = metricOutcomeAssigned
	metricResult.reason = metricReasonNone
	return assignment, true, nil
}

func (service *Service) ReplayShadow(
	ctx context.Context,
	limit int,
) (ShadowReplaySummary, error) {
	if service == nil || service.repository == nil {
		return ShadowReplaySummary{}, errors.New("StageScheduler is not configured")
	}
	metricResult := struct {
		outcome metricOutcome
		reason  metricReason
	}{outcome: metricOutcomeError, reason: metricReasonInternal}
	defer func() {
		service.metrics.observeShadowReplay(metricResult.outcome, metricResult.reason)
	}()
	if limit < 1 || limit > 1000 {
		return ShadowReplaySummary{}, errors.New("StageScheduler shadow replay limit is invalid")
	}
	snapshots, err := service.repository.ListShadowSnapshots(ctx, limit)
	if err != nil {
		return ShadowReplaySummary{}, fmt.Errorf("list StageScheduler shadow snapshots: %w", err)
	}
	summary := ShadowReplaySummary{}
	if len(snapshots) == 0 {
		metricResult.outcome = metricOutcomeNoWork
		metricResult.reason = metricReasonNone
		return summary, nil
	}
	for _, snapshot := range snapshots {
		if snapshot.ID == uuid.Nil {
			return summary, errors.New("StageScheduler shadow snapshot identity is missing")
		}
		evidence, selected, err := Decide(snapshot.Snapshot)
		if err != nil {
			return summary, fmt.Errorf("replay StageScheduler shadow snapshot %s: %w", snapshot.ID, err)
		}
		if !selected {
			return summary, fmt.Errorf(
				"replay StageScheduler shadow snapshot %s: persisted decision has no winner",
				snapshot.ID,
			)
		}
		receipt := ShadowReplayReceipt{
			ID: deriveShadowReplayReceiptID(
				snapshot.ID,
				evidence.AlgorithmRevision,
				service.config.SchedulerID,
			),
			SnapshotID:             snapshot.ID,
			AlgorithmRevision:      evidence.AlgorithmRevision,
			ExpectedEvidenceDigest: snapshot.ExpectedEvidenceDigest,
			ReplayedEvidenceDigest: evidence.EvidenceDigest,
			Matched:                snapshot.ExpectedEvidenceDigest == evidence.EvidenceDigest,
			ReplayedAt:             service.config.Now().UTC(),
			ReplayedBy:             service.config.SchedulerID,
		}
		if err := service.repository.RecordShadowReplay(ctx, receipt); err != nil {
			return summary, fmt.Errorf(
				"record StageScheduler shadow replay for snapshot %s: %w",
				snapshot.ID,
				err,
			)
		}
		summary.Processed++
		if receipt.Matched {
			summary.Matched++
		} else {
			summary.Diverged++
		}
	}
	if summary.Diverged > 0 {
		metricResult.outcome = metricOutcomeDiverged
		metricResult.reason = metricReasonReplayDiverged
		return summary, ErrShadowReplayDiverged
	}
	metricResult.outcome = metricOutcomeMatched
	metricResult.reason = metricReasonNone
	return summary, nil
}

func (service *Service) ReconcileExpired(ctx context.Context, limit int) (int64, error) {
	if service == nil || service.repository == nil {
		return 0, errors.New("StageScheduler is not configured")
	}
	if limit < 1 || limit > 1000 {
		service.metrics.observeClaimReconcile(
			metricOutcomeRejected,
			metricReasonInvalidAuthority,
		)
		return 0, errors.New("StageScheduler reconcile limit is invalid")
	}
	processed, err := service.repository.ReconcileExpired(ctx, limit)
	if err != nil {
		service.metrics.observeClaimReconcile(
			metricOutcomeError,
			metricReasonForError(err),
		)
		return 0, fmt.Errorf("reconcile StageScheduler expired claims: %w", err)
	}
	service.metrics.observeClaimReconcile(metricOutcomeSuccess, metricReasonNone)
	return processed, nil
}

func deriveShadowReplayReceiptID(
	snapshotID uuid.UUID,
	algorithmRevision string,
	replayedBy string,
) uuid.UUID {
	identity := snapshotID.String() + "\x00" + algorithmRevision + "\x00" + replayedBy
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(identity))
}

func (service *Service) newAssignment(
	authority WorkerAuthority,
	evidence DecisionEvidence,
) (Assignment, attemptcoordinator.AssignStageCommand, error) {
	claimID, err := uuid.NewRandomFromReader(service.config.Random)
	if err != nil {
		return Assignment{}, attemptcoordinator.AssignStageCommand{}, fmt.Errorf("generate claim identity: %w", err)
	}
	decisionID, err := uuid.NewRandomFromReader(service.config.Random)
	if err != nil {
		return Assignment{}, attemptcoordinator.AssignStageCommand{}, fmt.Errorf("generate decision identity: %w", err)
	}
	commandID, err := uuid.NewRandomFromReader(service.config.Random)
	if err != nil {
		return Assignment{}, attemptcoordinator.AssignStageCommand{}, fmt.Errorf("generate command identity: %w", err)
	}
	stageAttemptID, err := uuid.NewRandomFromReader(service.config.Random)
	if err != nil {
		return Assignment{}, attemptcoordinator.AssignStageCommand{}, fmt.Errorf("generate StageAttempt identity: %w", err)
	}
	allocationID, err := uuid.NewRandomFromReader(service.config.Random)
	if err != nil {
		return Assignment{}, attemptcoordinator.AssignStageCommand{}, fmt.Errorf("generate StageAllocation identity: %w", err)
	}
	leaseID, err := uuid.NewRandomFromReader(service.config.Random)
	if err != nil {
		return Assignment{}, attemptcoordinator.AssignStageCommand{}, fmt.Errorf("generate StageLease identity: %w", err)
	}
	var token, nonce [32]byte
	if _, err := io.ReadFull(service.config.Random, token[:]); err != nil {
		return Assignment{}, attemptcoordinator.AssignStageCommand{}, fmt.Errorf("generate StageLease token: %w", err)
	}
	if _, err := io.ReadFull(service.config.Random, nonce[:]); err != nil {
		return Assignment{}, attemptcoordinator.AssignStageCommand{}, fmt.Errorf("generate execution nonce: %w", err)
	}
	now := service.config.Now().UTC()
	expiresAt := now.Add(service.config.LeaseTTL)
	localDeadlineAt := now.Add(service.config.LocalDeadlineTTL)
	assignment := Assignment{
		ClaimID:           claimID,
		DecisionID:        decisionID,
		StageRunID:        evidence.SelectedStageRunID,
		StageAttemptID:    stageAttemptID,
		StageAllocationID: allocationID,
		StageLeaseID:      leaseID,
		LeaseToken:        token,
		LeaseExpiresAt:    expiresAt,
		LocalDeadlineAt:   localDeadlineAt,
	}
	command := attemptcoordinator.AssignStageCommand{
		CommandID:              commandID,
		AttemptID:              evidence.SelectedAttemptID,
		StageRunID:             evidence.SelectedStageRunID,
		ExpectedAttemptFence:   evidence.AttemptFence,
		ExpectedStageFence:     evidence.StageFence,
		ExpectedStageVersion:   evidence.StageVersion,
		StageAttemptID:         stageAttemptID,
		StageAllocationID:      allocationID,
		StageLeaseID:           leaseID,
		StageProfileRevisionID: evidence.SelectedStageProfileRevisionID,
		CapacityPoolID:         authority.CapacityPoolID,
		WorkerInstanceID:       authority.WorkerInstanceID,
		WorkerInstanceEpoch:    authority.WorkerInstanceEpoch,
		DeviceSetDigest:        append([]byte(nil), authority.DeviceSetDigest...),
		MembershipDigest:       append([]byte(nil), authority.MembershipDigest...),
		ModelResidencyID:       authority.ModelResidencyID,
		ModelRuntimeEpoch:      authority.ModelRuntimeEpoch,
		CapacityVector:         maps.Clone(authority.CapacityVector),
		TokenDigest:            digestBytes(token),
		SigningKeyID:           service.config.SigningKeyID,
		ExecutionNonce:         append([]byte(nil), nonce[:]...),
		IssuedAt:               now,
		ExpiresAt:              expiresAt,
		LocalDeadlineAt:        localDeadlineAt,
	}
	return assignment, command, nil
}

func validateWorkerAuthority(authority WorkerAuthority, observation CapacityObservation) error {
	if authority.CapacityPoolID == uuid.Nil || authority.StageProfileRevisionID == uuid.Nil ||
		authority.WorkerInstanceID == uuid.Nil || authority.ModelResidencyID == uuid.Nil {
		return errors.New("StageScheduler Worker authority identities are required")
	}
	if authority.WorkerInstanceEpoch <= 0 || authority.ModelRuntimeEpoch <= 0 ||
		observation.Sequence <= 0 {
		return errors.New("StageScheduler Worker authority epochs are invalid")
	}
	if len(authority.DeviceSetDigest) != 32 || len(authority.MembershipDigest) != 32 ||
		len(authority.CapacityVector) == 0 {
		return errors.New("StageScheduler Worker authority evidence is incomplete")
	}
	for key, value := range authority.CapacityVector {
		if key == "" || value <= 0 {
			return errors.New("StageScheduler capacity vector is invalid")
		}
	}
	return nil
}

func digestBytes(value [32]byte) []byte {
	digest := sha256.Sum256(value[:])
	return append([]byte(nil), digest[:]...)
}

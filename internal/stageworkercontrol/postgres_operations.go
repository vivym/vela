package stageworkercontrol

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageartifact"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

type WorkerEvidenceOperations interface {
	RegisterWorkerEvidence(
		context.Context,
		CommandContext,
		*velav1.RegisterWorkerEvidenceRequest,
	) (ReadinessResult, error)
	ReportCapacityObservation(
		context.Context,
		CommandContext,
		*velav1.ReportStageCapacityObservationRequest,
	) (ReadinessResult, error)
}

type AssignmentOperations interface {
	AcquireStage(
		context.Context,
		CommandContext,
		*velav1.AcquireStageRequest,
	) (AcquireResult, error)
}

type ExecutionOperations interface {
	StartStage(
		context.Context,
		CommandContext,
		*velav1.StartStageRequest,
		VerifiedAuthorities,
	) (CommandResult, error)
	HeartbeatStage(
		context.Context,
		CommandContext,
		*velav1.HeartbeatStageRequest,
		VerifiedAuthorities,
	) (CommandResult, error)
}

type MaterializationIssuer interface {
	Seal(
		context.Context,
		stageartifact.IssueMaterializationRequest,
	) (stageartifact.IssuedMaterialization, error)
}

type StageArtifactOperations interface {
	Commit(context.Context, stageartifact.CommitCommand) (stageartifact.Artifact, error)
	FailSourceLost(
		context.Context,
		stageartifact.SourceLostCommand,
	) (stageartifact.SourceLostDecision, error)
}

type TransferOperations interface {
	Resolve(context.Context, stageartifact.ResolveTransferCommand) (stageartifact.TransferDescriptor, error)
	ConsumeWithResult(
		context.Context,
		stageartifact.ConsumeTransferCommand,
	) (stageartifact.ConsumedTransferTicket, error)
}

type StageAttemptOperations interface {
	Apply(
		context.Context,
		attemptcoordinator.StageCommand,
	) (attemptcoordinator.StageDecision, error)
}

type ReattachmentOperations interface {
	ReattachStage(
		context.Context,
		CommandContext,
		*velav1.ReattachStageRequest,
		VerifiedAuthorities,
	) (CommandResult, error)
}

type PostgresOperationConfig struct {
	WorkerEvidence        WorkerEvidenceOperations
	Assignments           AssignmentOperations
	Execution             ExecutionOperations
	MaterializationIssuer MaterializationIssuer
	StageArtifacts        StageArtifactOperations
	StageAttempts         StageAttemptOperations
	Reattachments         ReattachmentOperations
	Transfers             TransferOperations
}

// PostgresOperationBackend composes the database-authoritative implementations
// behind the StageWorkerControl operation surface. Each dependency must retain
// durable replay and fencing; the backend never substitutes Worker-local state.
type PostgresOperationBackend struct {
	workerEvidence        WorkerEvidenceOperations
	assignments           AssignmentOperations
	execution             ExecutionOperations
	materializationIssuer MaterializationIssuer
	stageArtifacts        StageArtifactOperations
	stageAttempts         StageAttemptOperations
	reattachments         ReattachmentOperations
	transfers             TransferOperations
}

func NewPostgresOperationBackend(config PostgresOperationConfig) (*PostgresOperationBackend, error) {
	if config.WorkerEvidence == nil || config.Assignments == nil || config.Execution == nil ||
		config.MaterializationIssuer == nil || config.StageArtifacts == nil ||
		config.StageAttempts == nil || config.Reattachments == nil || config.Transfers == nil {
		return nil, errors.New("PostgreSQL Stage Worker operation dependencies are incomplete")
	}
	return &PostgresOperationBackend{
		workerEvidence: config.WorkerEvidence, assignments: config.Assignments,
		execution: config.Execution, materializationIssuer: config.MaterializationIssuer,
		stageArtifacts: config.StageArtifacts, stageAttempts: config.StageAttempts,
		reattachments: config.Reattachments, transfers: config.Transfers,
	}, nil
}

func (backend *PostgresOperationBackend) RegisterWorkerEvidence(
	ctx context.Context,
	command CommandContext,
	request *velav1.RegisterWorkerEvidenceRequest,
) (ReadinessResult, error) {
	if backend == nil || backend.workerEvidence == nil {
		return ReadinessResult{}, errors.New("PostgreSQL Stage Worker operation backend is not configured")
	}
	return backend.workerEvidence.RegisterWorkerEvidence(ctx, command, request)
}

func (backend *PostgresOperationBackend) ReportCapacityObservation(
	ctx context.Context,
	command CommandContext,
	request *velav1.ReportStageCapacityObservationRequest,
) (ReadinessResult, error) {
	if backend == nil || backend.workerEvidence == nil {
		return ReadinessResult{}, errors.New("PostgreSQL Stage Worker operation backend is not configured")
	}
	return backend.workerEvidence.ReportCapacityObservation(ctx, command, request)
}

func (backend *PostgresOperationBackend) AcquireStage(
	ctx context.Context,
	command CommandContext,
	request *velav1.AcquireStageRequest,
) (AcquireResult, error) {
	if backend == nil || backend.assignments == nil {
		return AcquireResult{}, errors.New("PostgreSQL Stage Worker operation backend is not configured")
	}
	return backend.assignments.AcquireStage(ctx, command, request)
}

func (backend *PostgresOperationBackend) StartStage(
	ctx context.Context,
	command CommandContext,
	request *velav1.StartStageRequest,
	authorities VerifiedAuthorities,
) (CommandResult, error) {
	if backend == nil || backend.execution == nil {
		return CommandResult{}, errors.New("PostgreSQL Stage Worker operation backend is not configured")
	}
	return backend.execution.StartStage(ctx, command, request, authorities)
}

func (backend *PostgresOperationBackend) HeartbeatStage(
	ctx context.Context,
	command CommandContext,
	request *velav1.HeartbeatStageRequest,
	authorities VerifiedAuthorities,
) (CommandResult, error) {
	if backend == nil || backend.execution == nil {
		return CommandResult{}, errors.New("PostgreSQL Stage Worker operation backend is not configured")
	}
	return backend.execution.HeartbeatStage(ctx, command, request, authorities)
}

func (backend *PostgresOperationBackend) SealStageOutput(
	ctx context.Context,
	command CommandContext,
	request *velav1.SealStageOutputRequest,
	authorities VerifiedAuthorities,
) (SealResult, error) {
	if backend == nil || backend.materializationIssuer == nil {
		return SealResult{}, errors.New("PostgreSQL Stage Worker operation backend is not configured")
	}
	if err := exactStageCommand(command, request.GetAuthority(), authorities); err != nil {
		return SealResult{}, err
	}
	issued, err := backend.materializationIssuer.Seal(ctx, stageartifact.IssueMaterializationRequest{
		Stage:          *authorities.Stage,
		SourceSPIFFEID: command.Identity.SPIFFEID,
		LocalReceipt:   request.GetLocalReceipt(),
	})
	if err != nil {
		if negative, mapped := mapOperationCommandError(err); mapped {
			return SealResult{Command: &negative}, nil
		}
		return SealResult{}, fmt.Errorf("seal Stage Worker output: %w", err)
	}
	return SealResult{Authority: issued.Authority}, nil
}

func (backend *PostgresOperationBackend) CommitStageMaterialization(
	ctx context.Context,
	command CommandContext,
	request *velav1.CommitStageMaterializationRequest,
	authorities VerifiedAuthorities,
) (CommandResult, error) {
	if backend == nil || backend.stageArtifacts == nil {
		return CommandResult{}, errors.New("PostgreSQL Stage Worker operation backend is not configured")
	}
	materialization, err := exactMaterializationCommand(
		command, request.GetMaterializationAuthority(), authorities,
	)
	if err != nil {
		return CommandResult{}, err
	}
	leaseID, _ := uuid.Parse(materialization.GetStageMaterializationLeaseId())
	artifactID, _ := uuid.Parse(materialization.GetStageArtifactId())
	var digest [sha256.Size]byte
	copy(digest[:], materialization.GetSha256())
	artifact, err := backend.stageArtifacts.Commit(ctx, stageartifact.CommitCommand{
		CommandID:              command.CommandID,
		ProgressReceiptID:      derivedOperationID("materialization-progress", command.CommandID),
		MaterializationLeaseID: leaseID,
		ArtifactID:             artifactID,
		ObjectKey:              materialization.GetObjectKey(),
		ObjectVersion:          strings.TrimSpace(request.GetObjectVersion()),
		SHA256:                 digest,
		SizeBytes:              materialization.GetSizeBytes(),
		TokenDigest:            authorities.Materialization.Digest,
		CommittedAt:            request.GetCommittedAt().AsTime().UTC(),
	})
	if err != nil {
		if negative, mapped := mapOperationCommandError(err); mapped {
			return negative, nil
		}
		return CommandResult{}, fmt.Errorf("commit Stage Worker materialization: %w", err)
	}
	if artifact.ID != artifactID || artifact.ObjectKey != materialization.GetObjectKey() ||
		artifact.ObjectVersion != strings.TrimSpace(request.GetObjectVersion()) ||
		artifact.SHA256 != digest || artifact.SizeBytes != materialization.GetSizeBytes() {
		return CommandResult{}, errors.New("committed StageArtifact changed immutable identity")
	}
	return acceptedCommandResult(artifact.Replayed), nil
}

func (backend *PostgresOperationBackend) FailStage(
	ctx context.Context,
	command CommandContext,
	request *velav1.FailStageRequest,
	authorities VerifiedAuthorities,
) (CommandResult, error) {
	if backend == nil || backend.stageAttempts == nil {
		return CommandResult{}, errors.New("PostgreSQL Stage Worker operation backend is not configured")
	}
	stage, err := exactStageAuthority(command, request.GetAuthority(), authorities)
	if err != nil {
		return CommandResult{}, err
	}
	decision, err := backend.stageAttempts.Apply(ctx, attemptcoordinator.FailStageCommand{
		CommandID: command.CommandID, AttemptID: uuid.MustParse(stage.GetAttemptId()),
		StageRunID:           uuid.MustParse(stage.GetStageRunId()),
		StageAttemptID:       uuid.MustParse(stage.GetStageAttemptId()),
		StageLeaseID:         uuid.MustParse(stage.GetStageLeaseId()),
		ExpectedAttemptFence: stage.GetAttemptFence(), ExpectedStageFence: stage.GetStageFence(),
		ExpectedStageVersion: stage.GetStageVersion(), FailureClass: strings.TrimSpace(request.GetFailureClass()),
		FailureFingerprint: request.GetFailureFingerprint(), ConsumedResourceUnits: request.GetConsumedResourceUnits(),
		FailedAt: request.GetFailedAt().AsTime().UTC(), RetryAt: request.GetRetryAt().AsTime().UTC(),
	})
	if err != nil {
		if negative, mapped := mapOperationCommandError(err); mapped {
			return negative, nil
		}
		return CommandResult{}, fmt.Errorf("fail Stage Worker execution: %w", err)
	}
	if decision.StageRunID.String() != stage.GetStageRunId() ||
		decision.StageAttemptID.String() != stage.GetStageAttemptId() || decision.State != "READY" ||
		decision.StageVersion <= stage.GetStageVersion() {
		return CommandResult{}, errors.New("AttemptCoordinator returned mismatched Stage failure")
	}
	return acceptedCommandResult(decision.Replayed), nil
}

func (backend *PostgresOperationBackend) ReattachStage(
	ctx context.Context,
	command CommandContext,
	request *velav1.ReattachStageRequest,
	authorities VerifiedAuthorities,
) (CommandResult, error) {
	if backend == nil || backend.reattachments == nil {
		return CommandResult{}, errors.New("PostgreSQL Stage Worker operation backend is not configured")
	}
	return backend.reattachments.ReattachStage(ctx, command, request, authorities)
}

func (backend *PostgresOperationBackend) ReportMaterializationSourceLost(
	ctx context.Context,
	command CommandContext,
	request *velav1.ReportMaterializationSourceLostRequest,
	authorities VerifiedAuthorities,
) (CommandResult, error) {
	if backend == nil || backend.stageArtifacts == nil {
		return CommandResult{}, errors.New("PostgreSQL Stage Worker operation backend is not configured")
	}
	materialization, err := exactMaterializationCommand(
		command, request.GetMaterializationAuthority(), authorities,
	)
	if err != nil {
		return CommandResult{}, err
	}
	leaseID, _ := uuid.Parse(materialization.GetStageMaterializationLeaseId())
	var fingerprint [sha256.Size]byte
	copy(fingerprint[:], request.GetFailureFingerprint())
	decision, err := backend.stageArtifacts.FailSourceLost(ctx, stageartifact.SourceLostCommand{
		CommandID: command.CommandID, MaterializationLeaseID: leaseID,
		TokenDigest: authorities.Materialization.Digest, FailureFingerprint: fingerprint,
		ConsumedResourceUnits: request.GetConsumedResourceUnits(),
		LostAt:                request.GetLostAt().AsTime().UTC(), RetryAt: request.GetRetryAt().AsTime().UTC(),
	})
	if err != nil {
		if negative, mapped := mapOperationCommandError(err); mapped {
			return negative, nil
		}
		return CommandResult{}, fmt.Errorf("report lost StageArtifact source: %w", err)
	}
	if decision.StageRunID == uuid.Nil || decision.State != "READY" || decision.StageFence <= 0 ||
		decision.StageVersion <= 0 {
		return CommandResult{}, errors.New("StageArtifact repository returned mismatched source-loss decision")
	}
	return acceptedCommandResult(decision.Replayed), nil
}

func (backend *PostgresOperationBackend) ResolveInputTransfer(
	ctx context.Context,
	command CommandContext,
	request *velav1.ResolveInputTransferRequest,
	authorities VerifiedAuthorities,
) (ResolveInputTransferResult, error) {
	if backend == nil || backend.transfers == nil {
		return ResolveInputTransferResult{}, errors.New(
			"PostgreSQL Stage Worker transfer backend is not configured",
		)
	}
	if _, err := exactStageAuthority(command, request.GetAuthority(), authorities); err != nil {
		return ResolveInputTransferResult{}, err
	}
	var tokenDigest [sha256.Size]byte
	copy(tokenDigest[:], request.GetTokenDigest())
	descriptor, err := backend.transfers.Resolve(ctx, stageartifact.ResolveTransferCommand{
		TicketID: uuid.MustParse(request.GetTicketId()), TokenDigest: tokenDigest,
		Destination: stageartifact.TransferDestination{
			WorkerInstanceID:    uuid.MustParse(request.GetWorkerInstanceId()),
			WorkerInstanceEpoch: request.GetWorkerInstanceEpoch(),
			ModelResidencyID:    uuid.MustParse(request.GetModelResidencyId()),
			ModelRuntimeEpoch:   request.GetModelRuntimeEpoch(),
			ConnectorRevisionID: uuid.MustParse(request.GetConnectorRevisionId()),
		},
		ResolvedAt: request.GetResolvedAt().AsTime().UTC(),
	})
	if err != nil {
		if decision, mapped := mapOperationCommandError(err); mapped {
			return ResolveInputTransferResult{Command: &decision}, nil
		}
		return ResolveInputTransferResult{}, fmt.Errorf("resolve Stage input transfer: %w", err)
	}
	if descriptor.TicketID.String() != request.GetTicketId() {
		return ResolveInputTransferResult{}, errors.New("resolved input transfer changed ticket identity")
	}
	return ResolveInputTransferResult{Descriptor: &descriptor}, nil
}

func (backend *PostgresOperationBackend) ConsumeInputTransfer(
	ctx context.Context,
	command CommandContext,
	request *velav1.ConsumeInputTransferRequest,
	authorities VerifiedAuthorities,
) (CommandResult, error) {
	if backend == nil || backend.transfers == nil {
		return CommandResult{}, errors.New("PostgreSQL Stage Worker transfer backend is not configured")
	}
	if _, err := exactStageAuthority(command, request.GetAuthority(), authorities); err != nil {
		return CommandResult{}, err
	}
	var tokenDigest, outcomeDigest [sha256.Size]byte
	copy(tokenDigest[:], request.GetTokenDigest())
	copy(outcomeDigest[:], request.GetOutcomeDigest())
	consumed, err := backend.transfers.ConsumeWithResult(ctx, stageartifact.ConsumeTransferCommand{
		CommandID: command.CommandID, TicketID: uuid.MustParse(request.GetTicketId()),
		TokenDigest: tokenDigest, OutcomeDigest: outcomeDigest,
		Destination: stageartifact.TransferDestination{
			WorkerInstanceID:    uuid.MustParse(request.GetWorkerInstanceId()),
			WorkerInstanceEpoch: request.GetWorkerInstanceEpoch(),
			ModelResidencyID:    uuid.MustParse(request.GetModelResidencyId()),
			ModelRuntimeEpoch:   request.GetModelRuntimeEpoch(),
			ConnectorRevisionID: uuid.MustParse(request.GetConnectorRevisionId()),
		},
		ConsumedAt: request.GetConsumedAt().AsTime().UTC(),
	})
	if err != nil {
		if decision, mapped := mapOperationCommandError(err); mapped {
			return decision, nil
		}
		return CommandResult{}, fmt.Errorf("consume Stage input transfer: %w", err)
	}
	return acceptedCommandResult(consumed.Replayed), nil
}

func exactStageCommand(
	command CommandContext,
	request *velav1.StageAuthority,
	authorities VerifiedAuthorities,
) error {
	_, err := exactStageAuthority(command, request, authorities)
	return err
}

func exactStageAuthority(
	command CommandContext,
	request *velav1.StageAuthority,
	authorities VerifiedAuthorities,
) (*velav1.StageAuthority, error) {
	verified, err := executionStageAuthority(context.Background(), command, request, authorities)
	if err != nil {
		return nil, err
	}
	return verified.Authority, nil
}

func exactMaterializationCommand(
	command CommandContext,
	request *velav1.MaterializationAuthority,
	authorities VerifiedAuthorities,
) (*velav1.MaterializationAuthority, error) {
	if command.CommandID == uuid.Nil || command.ControlSessionEpoch <= 0 ||
		strings.TrimSpace(command.Identity.SPIFFEID) == "" || authorities.Stage != nil ||
		authorities.Materialization == nil || authorities.Materialization.Authority == nil ||
		request == nil || !proto.Equal(request, authorities.Materialization.Authority) {
		return nil, errors.New("Stage Worker materialization authority is incomplete")
	}
	digest, err := materializationauthority.Digest(authorities.Materialization.Authority)
	if err != nil || digest != authorities.Materialization.Digest {
		return nil, errors.New("Stage Worker materialization authority digest is inconsistent")
	}
	return authorities.Materialization.Authority, nil
}
func acceptedCommandResult(replayed bool) CommandResult {
	decision := velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED
	if replayed {
		decision = velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED
	}
	return CommandResult{Decision: decision}
}

func derivedOperationID(kind string, commandID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("vela/stage-worker/"+kind+"/"+commandID.String()))
}

func mapOperationCommandError(err error) (CommandResult, bool) {
	if result, mapped := mapExecutionCommandError(err); mapped {
		return result, true
	}
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return CommandResult{}, false
	}
	if strings.Contains(postgresError.ConstraintName, "replay_mismatch") ||
		strings.HasSuffix(postgresError.ConstraintName, "_invalid") {
		return CommandResult{
			Decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED,
			Detail:   "Stage Worker operation was rejected",
		}, true
	}
	return CommandResult{}, false
}

var _ OperationBackend = (*PostgresOperationBackend)(nil)

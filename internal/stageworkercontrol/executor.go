package stageworkercontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageassignment"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	maxControlDetailBytes     = 1000
	maxReadinessEvidenceBytes = 64 << 10
	maxBoundedStatusBytes     = 64 << 10
	maxNoWorkRetry            = time.Hour
)

type CommandContext struct {
	CommandID           uuid.UUID
	Identity            stageworkertransport.Identity
	ControlSessionEpoch int64
}

type ReadinessResult struct {
	WorkerInstanceID              uuid.UUID
	WorkerInstanceEpoch           int64
	Ready                         bool
	Reason                        string
	ModelRuntimeBarrierGeneration int64
	LeaderWorkerMemberID          uuid.UUID
	ControlSessionEpoch           int64
	CapacityObservationSequence   int64
}

type AcquireResult struct {
	Assignment *velav1.StageAssignment
	RetryAfter time.Duration
	Command    *CommandResult
}

type ResolveInputTransferResult struct {
	Descriptor *stageartifact.TransferDescriptor
	Command    *CommandResult
}

type CommandResult struct {
	Decision         velav1.StageWorkerCommandDecision
	RenewedAuthority *velav1.StageAuthority
	Detail           string
}

type SealResult struct {
	Authority *velav1.MaterializationAuthority
	Command   *CommandResult
}

type OperationBackend interface {
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
	AcquireStage(
		context.Context,
		CommandContext,
		*velav1.AcquireStageRequest,
	) (AcquireResult, error)
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
	SealStageOutput(
		context.Context,
		CommandContext,
		*velav1.SealStageOutputRequest,
		VerifiedAuthorities,
	) (SealResult, error)
	CommitStageMaterialization(
		context.Context,
		CommandContext,
		*velav1.CommitStageMaterializationRequest,
		VerifiedAuthorities,
	) (CommandResult, error)
	FailStage(
		context.Context,
		CommandContext,
		*velav1.FailStageRequest,
		VerifiedAuthorities,
	) (CommandResult, error)
	ReattachStage(
		context.Context,
		CommandContext,
		*velav1.ReattachStageRequest,
		VerifiedAuthorities,
	) (CommandResult, error)
	ReportMaterializationSourceLost(
		context.Context,
		CommandContext,
		*velav1.ReportMaterializationSourceLostRequest,
		VerifiedAuthorities,
	) (CommandResult, error)
	ResolveInputTransfer(
		context.Context,
		CommandContext,
		*velav1.ResolveInputTransferRequest,
		VerifiedAuthorities,
	) (ResolveInputTransferResult, error)
	ConsumeInputTransfer(
		context.Context,
		CommandContext,
		*velav1.ConsumeInputTransferRequest,
		VerifiedAuthorities,
	) (CommandResult, error)
}

type ProductionExecutor struct {
	backend OperationBackend
}

func NewProductionExecutor(backend OperationBackend) (*ProductionExecutor, error) {
	if backend == nil {
		return nil, errors.New("stage worker operation backend is required")
	}
	return &ProductionExecutor{backend: backend}, nil
}

func (executor *ProductionExecutor) Execute(
	ctx context.Context,
	identity stageworkertransport.Identity,
	sessionEpoch int64,
	operation Operation,
	request *velav1.StageWorkerControlServiceConnectRequest,
	authorities VerifiedAuthorities,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	if executor == nil || executor.backend == nil {
		return nil, errors.New("stage worker production executor is not configured")
	}
	commandID, err := uuid.Parse(request.GetRequestId())
	if ctx == nil || err != nil || strings.TrimSpace(identity.SPIFFEID) == "" || sessionEpoch <= 0 {
		return nil, errors.New("stage worker command context is invalid")
	}
	command := CommandContext{
		CommandID: commandID, Identity: identity, ControlSessionEpoch: sessionEpoch,
	}
	if err := validateOperationRequest(operation, request, authorities); err != nil {
		return rejectedResponse(request.GetRequestId(), operation, err.Error()), nil
	}

	switch operation {
	case OperationRegisterWorkerEvidence:
		result, err := executor.backend.RegisterWorkerEvidence(
			ctx, command, request.GetRegisterWorkerEvidence(),
		)
		return readinessResponse(request.GetRequestId(), result, err)
	case OperationReportCapacityObservation:
		result, err := executor.backend.ReportCapacityObservation(
			ctx, command, request.GetReportCapacityObservation(),
		)
		return readinessResponse(request.GetRequestId(), result, err)
	case OperationAcquireStage:
		result, err := executor.backend.AcquireStage(ctx, command, request.GetAcquireStage())
		return acquireResponse(request.GetRequestId(), result, err)
	case OperationStartStage:
		result, err := executor.backend.StartStage(
			ctx, command, request.GetStartStage(), authorities,
		)
		return commandResultResponse(request.GetRequestId(), operation, result, authorities, err)
	case OperationHeartbeatStage:
		result, err := executor.backend.HeartbeatStage(
			ctx, command, request.GetHeartbeatStage(), authorities,
		)
		return commandResultResponse(request.GetRequestId(), operation, result, authorities, err)
	case OperationSealStageOutput:
		result, err := executor.backend.SealStageOutput(
			ctx, command, request.GetSealStageOutput(), authorities,
		)
		return sealResponse(request.GetRequestId(), result, authorities, err)
	case OperationCommitStageMaterialization:
		result, err := executor.backend.CommitStageMaterialization(
			ctx, command, request.GetCommitStageMaterialization(), authorities,
		)
		return commandResultResponse(request.GetRequestId(), operation, result, authorities, err)
	case OperationFailStage:
		result, err := executor.backend.FailStage(
			ctx, command, request.GetFailStage(), authorities,
		)
		return commandResultResponse(request.GetRequestId(), operation, result, authorities, err)
	case OperationReattachStage:
		result, err := executor.backend.ReattachStage(
			ctx, command, request.GetReattachStage(), authorities,
		)
		return commandResultResponse(request.GetRequestId(), operation, result, authorities, err)
	case OperationReportMaterializationSourceLost:
		result, err := executor.backend.ReportMaterializationSourceLost(
			ctx, command, request.GetReportMaterializationSourceLost(), authorities,
		)
		return commandResultResponse(request.GetRequestId(), operation, result, authorities, err)
	case OperationResolveInputTransfer:
		result, err := executor.backend.ResolveInputTransfer(
			ctx, command, request.GetResolveInputTransfer(), authorities,
		)
		return resolvedInputTransferResponse(
			request.GetRequestId(), request.GetResolveInputTransfer().GetTicketId(),
			result, authorities, err,
		)
	case OperationConsumeInputTransfer:
		result, err := executor.backend.ConsumeInputTransfer(
			ctx, command, request.GetConsumeInputTransfer(), authorities,
		)
		return commandResultResponse(request.GetRequestId(), operation, result, authorities, err)
	default:
		return rejectedResponse(
			request.GetRequestId(), operation, "Stage Worker operation is unsupported",
		), nil
	}
}

func resolvedInputTransferResponse(
	requestID string,
	ticketID string,
	result ResolveInputTransferResult,
	authorities VerifiedAuthorities,
	err error,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	if err != nil {
		return nil, fmt.Errorf("execute %s: %w", OperationResolveInputTransfer.String(), err)
	}
	if (result.Descriptor == nil) == (result.Command == nil) {
		return nil, errors.New("stage worker transfer backend must return exactly one result")
	}
	if result.Command != nil {
		return commandResultResponse(
			requestID, OperationResolveInputTransfer, *result.Command, authorities, nil,
		)
	}
	descriptor := *result.Descriptor
	if descriptor.TicketID == uuid.Nil || descriptor.TicketID.String() != ticketID ||
		descriptor.ArtifactID == uuid.Nil || strings.TrimSpace(descriptor.ObjectKey) == "" ||
		strings.TrimSpace(descriptor.ObjectVersion) == "" ||
		descriptor.SHA256 == ([sha256.Size]byte{}) || descriptor.SizeBytes <= 0 ||
		strings.TrimSpace(descriptor.ContentType) == "" || len(descriptor.ContentType) > 200 {
		return nil, errors.New("stage worker transfer backend returned a malformed descriptor")
	}
	return &velav1.StageWorkerControlServiceConnectResponse{
		RequestId: requestID,
		Result: &velav1.StageWorkerControlServiceConnectResponse_ResolvedInputTransfer{
			ResolvedInputTransfer: &velav1.ResolvedInputTransfer{
				TicketId: descriptor.TicketID.String(), StageArtifactId: descriptor.ArtifactID.String(),
				ObjectKey: descriptor.ObjectKey, ObjectVersion: descriptor.ObjectVersion,
				Sha256: append([]byte(nil), descriptor.SHA256[:]...), SizeBytes: descriptor.SizeBytes,
				ContentType: descriptor.ContentType,
			},
		},
	}, nil
}

func readinessResponse(
	requestID string,
	result ReadinessResult,
	err error,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	if err != nil {
		return nil, fmt.Errorf("execute Stage Worker readiness operation: %w", err)
	}
	if result.WorkerInstanceID == uuid.Nil || result.WorkerInstanceEpoch <= 0 ||
		len(result.Reason) > maxControlDetailBytes {
		return nil, errors.New("stage worker readiness backend returned a malformed result")
	}
	leaderMemberID := ""
	if result.LeaderWorkerMemberID != uuid.Nil {
		leaderMemberID = result.LeaderWorkerMemberID.String()
	}
	return &velav1.StageWorkerControlServiceConnectResponse{
		RequestId: requestID,
		Result: &velav1.StageWorkerControlServiceConnectResponse_WorkerReadinessDecision{
			WorkerReadinessDecision: &velav1.WorkerReadinessDecision{
				WorkerInstanceId:    result.WorkerInstanceID.String(),
				WorkerInstanceEpoch: result.WorkerInstanceEpoch,
				Ready:               result.Ready, Reason: result.Reason,
				ModelRuntimeBarrierGeneration: result.ModelRuntimeBarrierGeneration,
				LeaderWorkerMemberId:          leaderMemberID,
				ControlSessionEpoch:           result.ControlSessionEpoch,
				CapacityObservationSequence:   result.CapacityObservationSequence,
			},
		},
	}, nil
}

func acquireResponse(
	requestID string,
	result AcquireResult,
	err error,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	if err != nil {
		return nil, fmt.Errorf("execute Stage Worker acquire: %w", err)
	}
	if result.Command != nil {
		if result.Assignment != nil || result.RetryAfter != 0 || result.Command.RenewedAuthority != nil ||
			(result.Command.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE &&
				result.Command.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED) ||
			strings.TrimSpace(result.Command.Detail) == "" ||
			len(result.Command.Detail) > maxControlDetailBytes {
			return nil, errors.New("stage worker acquire backend returned a malformed negative decision")
		}
		return commandResponse(
			requestID, OperationAcquireStage, result.Command.Decision, result.Command.Detail,
		), nil
	}
	response := &velav1.StageWorkerControlServiceConnectResponse{RequestId: requestID}
	if result.Assignment != nil {
		if result.RetryAfter != 0 {
			return nil, errors.New("stage worker acquire backend returned Assignment with no-work retry")
		}
		if _, validateErr := stageassignment.Validate(result.Assignment); validateErr != nil {
			return nil, errors.New("stage worker acquire backend returned a malformed Assignment")
		}
		response.Result = &velav1.StageWorkerControlServiceConnectResponse_StageAssignment{
			StageAssignment: result.Assignment,
		}
		return response, nil
	}
	if result.RetryAfter <= 0 || result.RetryAfter > maxNoWorkRetry {
		return nil, errors.New("stage worker acquire backend returned an invalid no-work retry")
	}
	response.Result = &velav1.StageWorkerControlServiceConnectResponse_NoWork{
		NoWork: &velav1.NoStageWork{RetryAfter: durationpb.New(result.RetryAfter)},
	}
	return response, nil
}

func sealResponse(
	requestID string,
	result SealResult,
	authorities VerifiedAuthorities,
	err error,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	if err != nil {
		return nil, fmt.Errorf("execute %s: %w", OperationSealStageOutput.String(), err)
	}
	if (result.Authority == nil) == (result.Command == nil) {
		return nil, errors.New("stage worker seal backend returned a malformed result")
	}
	if result.Command != nil {
		if result.Command.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE &&
			result.Command.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED {
			return nil, errors.New("stage worker seal backend returned an invalid negative decision")
		}
		return commandResultResponse(
			requestID, OperationSealStageOutput, *result.Command, authorities, nil,
		)
	}
	stageDigest, digestErr := commandAuthorityDigest(OperationSealStageOutput, authorities)
	if digestErr != nil {
		return nil, digestErr
	}
	if _, digestErr = materializationauthority.Digest(result.Authority); digestErr != nil ||
		!bytes.Equal(result.Authority.GetStageAuthorityDigest(), stageDigest[:]) {
		return nil, errors.New("stage worker seal backend returned a malformed MaterializationAuthority")
	}
	return &velav1.StageWorkerControlServiceConnectResponse{
		RequestId: requestID,
		Result: &velav1.StageWorkerControlServiceConnectResponse_MaterializationAuthority{
			MaterializationAuthority: result.Authority,
		},
	}, nil
}

func commandResultResponse(
	requestID string,
	operation Operation,
	result CommandResult,
	authorities VerifiedAuthorities,
	err error,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	if err != nil {
		return nil, fmt.Errorf("execute %s: %w", operation.String(), err)
	}
	if !terminalCommandDecision(result.Decision) {
		return nil, errors.New("stage worker backend returned an invalid command decision")
	}
	if len(result.Detail) > maxControlDetailBytes {
		return nil, errors.New("stage worker backend returned an oversized command detail")
	}
	if (result.Decision == velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE ||
		result.Decision == velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED) &&
		strings.TrimSpace(result.Detail) == "" {
		return nil, errors.New("stage worker negative command result lacks detail")
	}
	digest, digestErr := commandAuthorityDigest(operation, authorities)
	if digestErr != nil {
		return nil, digestErr
	}
	if result.RenewedAuthority != nil {
		if result.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED &&
			result.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED {
			return nil, errors.New("stage worker negative command result contains renewed authority")
		}
		if operation != OperationStartStage && operation != OperationHeartbeatStage {
			return nil, errors.New("stage worker operation cannot renew StageAuthority")
		}
		if authorities.Stage == nil || stageauthority.ValidateRenewal(
			authorities.Stage.Authority, result.RenewedAuthority,
		) != nil {
			return nil, errors.New("stage worker backend returned a malformed renewed StageAuthority")
		}
	}
	return &velav1.StageWorkerControlServiceConnectResponse{
		RequestId: requestID,
		Result: &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{
			StageCommandResult: &velav1.StageCommandResult{
				AuthorityDigest: digest[:], Decision: result.Decision,
				Operation: operation, Detail: result.Detail,
				RenewedAuthority: result.RenewedAuthority,
			},
		},
	}, nil
}

func terminalCommandDecision(decision velav1.StageWorkerCommandDecision) bool {
	switch decision {
	case velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED,
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE,
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED:
		return true
	default:
		return false
	}
}

func commandAuthorityDigest(
	operation Operation,
	authorities VerifiedAuthorities,
) ([sha256.Size]byte, error) {
	descriptor, ok := descriptorForOperation(operation)
	if !ok {
		return [sha256.Size]byte{}, errors.New("stage worker operation has no command authority evidence")
	}
	switch descriptor.authority {
	case operationAuthorityStage:
		if authorities.Stage == nil || authorities.Materialization != nil ||
			authorities.Stage.Digest == ([sha256.Size]byte{}) || authorities.Stage.Authority == nil {
			return [sha256.Size]byte{}, errors.New("stage worker command lacks exact StageAuthority evidence")
		}
		digest, err := stageauthority.Digest(authorities.Stage.Authority)
		if err != nil || digest != authorities.Stage.Digest {
			return [sha256.Size]byte{}, errors.New("stage worker command has mismatched StageAuthority evidence")
		}
		return authorities.Stage.Digest, nil
	case operationAuthorityMaterialization:
		if authorities.Materialization == nil || authorities.Stage != nil ||
			authorities.Materialization.Digest == ([sha256.Size]byte{}) ||
			authorities.Materialization.Authority == nil {
			return [sha256.Size]byte{}, errors.New("stage worker command lacks exact MaterializationAuthority evidence")
		}
		digest, err := materializationauthority.Digest(authorities.Materialization.Authority)
		if err != nil || digest != authorities.Materialization.Digest {
			return [sha256.Size]byte{}, errors.New("stage worker command has mismatched MaterializationAuthority evidence")
		}
		return authorities.Materialization.Digest, nil
	default:
		return [sha256.Size]byte{}, errors.New("stage worker operation has no command authority evidence")
	}
}

func validateOperationRequest(
	operation Operation,
	request *velav1.StageWorkerControlServiceConnectRequest,
	authorities VerifiedAuthorities,
) error {
	if request == nil || request.GetOperation() == nil {
		return errors.New("stage worker operation request is missing")
	}
	descriptor, ok := descriptorForOperation(operation)
	if !ok || descriptor.validate == nil {
		return errors.New("stage worker operation is unsupported")
	}
	return descriptor.validate(request, authorities)
}

func stageAuthorityHasBarrierGeneration(authority *velav1.StageAuthority, generation int64) bool {
	return authority != nil && generation > 0 &&
		authority.GetModelRuntimeBarrierGeneration() == generation
}

func validCapacityVector(vector map[string]int64) bool {
	if len(vector) == 0 || len(vector) > 100 {
		return false
	}
	for key, value := range vector {
		if strings.TrimSpace(key) == "" || len(key) > 100 || value < 0 {
			return false
		}
	}
	return true
}

func validTimestamp(value interface {
	CheckValid() error
}) bool {
	return value != nil && value.CheckValid() == nil
}

func parseUUID(value string) uuid.UUID {
	parsed, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil
	}
	return parsed
}

var _ Executor = (*ProductionExecutor)(nil)

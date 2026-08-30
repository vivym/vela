package stageworkercontrol

import (
	"context"
	"errors"
	"fmt"

	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

type Operation = velav1.StageWorkerOperation

const (
	OperationRegisterWorkerEvidence     = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REGISTER_WORKER_EVIDENCE
	OperationReportCapacityObservation  = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REPORT_CAPACITY_OBSERVATION
	OperationAcquireStage               = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_ACQUIRE_STAGE
	OperationStartStage                 = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE
	OperationHeartbeatStage             = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_HEARTBEAT_STAGE
	OperationSealStageOutput            = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_SEAL_STAGE_OUTPUT
	OperationCommitStageMaterialization = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_COMMIT_STAGE_MATERIALIZATION
	OperationFailStage                  = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_FAIL_STAGE
	OperationReattachStage              = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE
)

type Authorizer interface {
	IsActive(
		context.Context,
		stageworkertransport.Identity,
		int64,
		Operation,
		stageauthority.Verified,
	) (bool, error)
}

type Executor interface {
	Execute(
		context.Context,
		stageworkertransport.Identity,
		int64,
		Operation,
		*velav1.StageWorkerControlServiceConnectRequest,
		*stageauthority.Verified,
	) (*velav1.StageWorkerControlServiceConnectResponse, error)
}

type Config struct {
	Validator  *stageauthority.Validator
	Authorizer Authorizer
	Executor   Executor
}

type Handler struct {
	validator  *stageauthority.Validator
	authorizer Authorizer
	executor   Executor
}

func NewHandler(config Config) (*Handler, error) {
	if config.Validator == nil {
		return nil, errors.New("missing StageAuthority validator for Stage Worker control")
	}
	if config.Authorizer == nil {
		return nil, errors.New("missing durable authority checker for Stage Worker control")
	}
	if config.Executor == nil {
		return nil, errors.New("missing operation executor for Stage Worker control")
	}
	return &Handler{
		validator: config.Validator, authorizer: config.Authorizer, executor: config.Executor,
	}, nil
}

func (handler *Handler) Handle(
	ctx context.Context,
	identity stageworkertransport.Identity,
	sessionEpoch int64,
	request *velav1.StageWorkerControlServiceConnectRequest,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	if handler == nil || handler.validator == nil || handler.authorizer == nil ||
		handler.executor == nil {
		return nil, errors.New("missing configured Stage Worker control handler")
	}
	if ctx == nil || identity.SPIFFEID == "" || sessionEpoch <= 0 || request == nil ||
		request.GetRequestId() == "" {
		return nil, errors.New("incomplete Stage Worker control request context")
	}
	operation, authority, requiresAuthority := operationAuthority(request)
	if operation == velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_UNSPECIFIED {
		return rejectedResponse(request.GetRequestId(), operation, "operation is required"), nil
	}
	if !requiresAuthority {
		return handler.execute(ctx, identity, sessionEpoch, operation, request, nil)
	}
	verified, err := handler.validator.ValidateEnvelope(authority)
	if err != nil {
		return staleResponse(request.GetRequestId(), operation, err.Error()), nil
	}
	active, err := handler.authorizer.IsActive(
		ctx, identity, sessionEpoch, operation, verified,
	)
	if err != nil {
		return nil, fmt.Errorf("authorize %s: %w", operation.String(), err)
	}
	if !active {
		return staleResponse(
			request.GetRequestId(), operation, "StageAuthority no longer matches durable active authority",
		), nil
	}
	return handler.execute(ctx, identity, sessionEpoch, operation, request, &verified)
}

func (handler *Handler) execute(
	ctx context.Context,
	identity stageworkertransport.Identity,
	sessionEpoch int64,
	operation Operation,
	request *velav1.StageWorkerControlServiceConnectRequest,
	verified *stageauthority.Verified,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	response, err := handler.executor.Execute(
		ctx, identity, sessionEpoch, operation, request, verified,
	)
	if err != nil {
		return nil, err
	}
	if response == nil || response.GetRequestId() != request.GetRequestId() ||
		response.GetResult() == nil {
		return nil, errors.New("malformed response from Stage Worker control executor")
	}
	return response, nil
}

func operationAuthority(
	request *velav1.StageWorkerControlServiceConnectRequest,
) (Operation, *velav1.StageAuthority, bool) {
	switch operation := request.GetOperation().(type) {
	case *velav1.StageWorkerControlServiceConnectRequest_RegisterWorkerEvidence:
		return OperationRegisterWorkerEvidence, nil, false
	case *velav1.StageWorkerControlServiceConnectRequest_ReportCapacityObservation:
		return OperationReportCapacityObservation, nil, false
	case *velav1.StageWorkerControlServiceConnectRequest_AcquireStage:
		return OperationAcquireStage, nil, false
	case *velav1.StageWorkerControlServiceConnectRequest_StartStage:
		return OperationStartStage, operation.StartStage.GetAuthority(), true
	case *velav1.StageWorkerControlServiceConnectRequest_HeartbeatStage:
		return OperationHeartbeatStage, operation.HeartbeatStage.GetAuthority(), true
	case *velav1.StageWorkerControlServiceConnectRequest_SealStageOutput:
		return OperationSealStageOutput, operation.SealStageOutput.GetAuthority(), true
	case *velav1.StageWorkerControlServiceConnectRequest_CommitStageMaterialization:
		return OperationCommitStageMaterialization,
			operation.CommitStageMaterialization.GetAuthority(), true
	case *velav1.StageWorkerControlServiceConnectRequest_FailStage:
		return OperationFailStage, operation.FailStage.GetAuthority(), true
	case *velav1.StageWorkerControlServiceConnectRequest_ReattachStage:
		return OperationReattachStage, operation.ReattachStage.GetAuthority(), true
	default:
		return velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_UNSPECIFIED, nil, false
	}
}

func staleResponse(
	requestID string,
	operation Operation,
	detail string,
) *velav1.StageWorkerControlServiceConnectResponse {
	return commandResponse(
		requestID,
		operation,
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE,
		detail,
	)
}

func rejectedResponse(
	requestID string,
	operation Operation,
	detail string,
) *velav1.StageWorkerControlServiceConnectResponse {
	return commandResponse(
		requestID,
		operation,
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED,
		detail,
	)
}

func commandResponse(
	requestID string,
	operation Operation,
	decision velav1.StageWorkerCommandDecision,
	detail string,
) *velav1.StageWorkerControlServiceConnectResponse {
	if len(detail) > 1000 {
		detail = detail[:1000]
	}
	return &velav1.StageWorkerControlServiceConnectResponse{
		RequestId: requestID,
		Result: &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{
			StageCommandResult: &velav1.StageCommandResult{
				Decision: decision, Operation: operation, Detail: detail,
			},
		},
	}
}

package stageworkercontrol

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

type Operation = velav1.StageWorkerOperation

const (
	OperationRegisterWorkerEvidence          = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REGISTER_WORKER_EVIDENCE
	OperationReportCapacityObservation       = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REPORT_CAPACITY_OBSERVATION
	OperationAcquireStage                    = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_ACQUIRE_STAGE
	OperationStartStage                      = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE
	OperationHeartbeatStage                  = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_HEARTBEAT_STAGE
	OperationSealStageOutput                 = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_SEAL_STAGE_OUTPUT
	OperationCommitStageMaterialization      = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_COMMIT_STAGE_MATERIALIZATION
	OperationFailStage                       = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_FAIL_STAGE
	OperationReattachStage                   = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE
	OperationReportMaterializationSourceLost = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REPORT_MATERIALIZATION_SOURCE_LOST
	OperationResolveInputTransfer            = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_RESOLVE_INPUT_TRANSFER
	OperationConsumeInputTransfer            = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_CONSUME_INPUT_TRANSFER
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

type MaterializationAuthorizer interface {
	IsActive(
		context.Context,
		stageworkertransport.Identity,
		int64,
		materializationauthority.Verified,
	) (bool, error)
}

type VerifiedAuthorities struct {
	Stage           *stageauthority.Verified
	Materialization *materializationauthority.Verified
}

type Executor interface {
	Execute(
		context.Context,
		stageworkertransport.Identity,
		int64,
		Operation,
		*velav1.StageWorkerControlServiceConnectRequest,
		VerifiedAuthorities,
	) (*velav1.StageWorkerControlServiceConnectResponse, error)
}

type Config struct {
	Validator                 *stageauthority.Validator
	Authorizer                Authorizer
	MaterializationValidator  *materializationauthority.Validator
	MaterializationAuthorizer MaterializationAuthorizer
	Executor                  Executor
	MaxClockSkew              time.Duration
}

type Handler struct {
	validator                 *stageauthority.Validator
	authorizer                Authorizer
	materializationValidator  *materializationauthority.Validator
	materializationAuthorizer MaterializationAuthorizer
	executor                  Executor
	maxClockSkew              time.Duration
}

func NewHandler(config Config) (*Handler, error) {
	if config.Validator == nil {
		return nil, errors.New("missing StageAuthority validator for Stage Worker control")
	}
	if config.Authorizer == nil {
		return nil, errors.New("missing durable authority checker for Stage Worker control")
	}
	if config.MaterializationValidator == nil {
		return nil, errors.New("missing MaterializationAuthority validator for Stage Worker control")
	}
	if config.MaterializationAuthorizer == nil {
		return nil, errors.New("missing durable materialization authority checker for Stage Worker control")
	}
	if config.Executor == nil {
		return nil, errors.New("missing operation executor for Stage Worker control")
	}
	if config.MaxClockSkew < 0 || config.MaxClockSkew > time.Minute {
		return nil, errors.New("stage worker control clock skew is invalid")
	}
	return &Handler{
		validator: config.Validator, authorizer: config.Authorizer,
		materializationValidator:  config.MaterializationValidator,
		materializationAuthorizer: config.MaterializationAuthorizer,
		executor:                  config.Executor,
		maxClockSkew:              config.MaxClockSkew,
	}, nil
}

func (handler *Handler) Handle(
	ctx context.Context,
	identity stageworkertransport.Identity,
	sessionEpoch int64,
	request *velav1.StageWorkerControlServiceConnectRequest,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	if handler == nil || handler.validator == nil || handler.authorizer == nil ||
		handler.materializationValidator == nil || handler.materializationAuthorizer == nil ||
		handler.executor == nil {
		return nil, errors.New("missing configured Stage Worker control handler")
	}
	if ctx == nil || identity.SPIFFEID == "" || sessionEpoch <= 0 || request == nil ||
		request.GetRequestId() == "" {
		return nil, errors.New("incomplete Stage Worker control request context")
	}
	operation, authority, materializationAuthority, requiresAuthority := operationAuthority(request)
	if operation == velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_UNSPECIFIED {
		return rejectedResponse(request.GetRequestId(), operation, "operation is required"), nil
	}
	if !requiresAuthority {
		return handler.execute(
			ctx, identity, sessionEpoch, operation, request, VerifiedAuthorities{},
		)
	}
	if materializationAuthority != nil || operation == OperationCommitStageMaterialization ||
		operation == OperationReportMaterializationSourceLost {
		verified, err := handler.materializationValidator.ValidateWithClockSkew(
			materializationAuthority,
			handler.maxClockSkew,
		)
		if err != nil {
			return staleResponse(request.GetRequestId(), operation, err.Error()), nil
		}
		active, err := handler.materializationAuthorizer.IsActive(
			ctx, identity, sessionEpoch, verified,
		)
		if err != nil {
			return nil, fmt.Errorf("authorize %s: %w", operation.String(), err)
		}
		if !active {
			return staleResponse(
				request.GetRequestId(), operation,
				"MaterializationAuthority no longer matches durable active authority",
			), nil
		}
		return handler.execute(
			ctx, identity, sessionEpoch, operation, request,
			VerifiedAuthorities{Materialization: &verified},
		)
	}
	verified, err := handler.validator.ValidateEnvelopeWithClockSkew(
		authority,
		handler.maxClockSkew,
	)
	if operation == OperationSealStageOutput && errors.Is(err, stageauthority.ErrStale) {
		verified, err = handler.validator.ValidateEnvelopeSignature(authority)
	}
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
	return handler.execute(
		ctx, identity, sessionEpoch, operation, request, VerifiedAuthorities{Stage: &verified},
	)
}

func (handler *Handler) execute(
	ctx context.Context,
	identity stageworkertransport.Identity,
	sessionEpoch int64,
	operation Operation,
	request *velav1.StageWorkerControlServiceConnectRequest,
	authorities VerifiedAuthorities,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	response, err := handler.executor.Execute(
		ctx, identity, sessionEpoch, operation, request, authorities,
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
) (Operation, *velav1.StageAuthority, *velav1.MaterializationAuthority, bool) {
	operation := operationFromRequest(request)
	descriptor, ok := descriptorForOperation(operation)
	if !ok {
		return velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_UNSPECIFIED, nil, nil, false
	}
	var stage *velav1.StageAuthority
	if descriptor.stageAuthority != nil {
		stage = descriptor.stageAuthority(request)
	}
	var materialization *velav1.MaterializationAuthority
	if descriptor.materializationAuthority != nil {
		materialization = descriptor.materializationAuthority(request)
	}
	return operation, stage, materialization, descriptor.authority != operationAuthorityNone
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

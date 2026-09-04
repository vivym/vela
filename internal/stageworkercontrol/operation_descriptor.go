package stageworkercontrol

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

type operationAuthorityKind uint8

const (
	operationAuthorityNone operationAuthorityKind = iota
	operationAuthorityStage
	operationAuthorityMaterialization
)

type operationDescriptor struct {
	authority                operationAuthorityKind
	stageAuthority           func(*velav1.StageWorkerControlServiceConnectRequest) *velav1.StageAuthority
	materializationAuthority func(*velav1.StageWorkerControlServiceConnectRequest) *velav1.MaterializationAuthority
	validate                 func(*velav1.StageWorkerControlServiceConnectRequest, VerifiedAuthorities) error
	activeState              func(durableStageAuthority) bool
}

// operationDescriptors is the single metadata registry for every protocol
// operation. Protobuf oneof decoding and typed backend dispatch remain explicit.
var operationDescriptors = map[Operation]operationDescriptor{
	OperationRegisterWorkerEvidence: {
		validate: validateRegisterWorkerEvidence,
	},
	OperationReportCapacityObservation: {
		validate: validateCapacityObservation,
	},
	OperationAcquireStage: {
		validate: validateAcquireStage,
	},
	OperationStartStage: {
		authority: operationAuthorityStage,
		stageAuthority: func(request *velav1.StageWorkerControlServiceConnectRequest) *velav1.StageAuthority {
			return request.GetStartStage().GetAuthority()
		},
		validate:    validateStartStage,
		activeState: activeAssignedStage,
	},
	OperationHeartbeatStage: {
		authority: operationAuthorityStage,
		stageAuthority: func(request *velav1.StageWorkerControlServiceConnectRequest) *velav1.StageAuthority {
			return request.GetHeartbeatStage().GetAuthority()
		},
		validate:    validateHeartbeatStage,
		activeState: activeRunningStage,
	},
	OperationSealStageOutput: {
		authority: operationAuthorityStage,
		stageAuthority: func(request *velav1.StageWorkerControlServiceConnectRequest) *velav1.StageAuthority {
			return request.GetSealStageOutput().GetAuthority()
		},
		validate:    validateSealStageOutput,
		activeState: activeRunningStage,
	},
	OperationCommitStageMaterialization: {
		authority: operationAuthorityMaterialization,
		materializationAuthority: func(request *velav1.StageWorkerControlServiceConnectRequest) *velav1.MaterializationAuthority {
			return request.GetCommitStageMaterialization().GetMaterializationAuthority()
		},
		validate: validateCommitStageMaterialization,
	},
	OperationFailStage: {
		authority: operationAuthorityStage,
		stageAuthority: func(request *velav1.StageWorkerControlServiceConnectRequest) *velav1.StageAuthority {
			return request.GetFailStage().GetAuthority()
		},
		validate:    validateFailStage,
		activeState: activeAssignedOrRunningStage,
	},
	OperationReattachStage: {
		authority: operationAuthorityStage,
		stageAuthority: func(request *velav1.StageWorkerControlServiceConnectRequest) *velav1.StageAuthority {
			return request.GetReattachStage().GetAuthority()
		},
		validate:    validateReattachStage,
		activeState: activeAssignedOrRunningStage,
	},
	OperationReportMaterializationSourceLost: {
		authority: operationAuthorityMaterialization,
		materializationAuthority: func(request *velav1.StageWorkerControlServiceConnectRequest) *velav1.MaterializationAuthority {
			return request.GetReportMaterializationSourceLost().GetMaterializationAuthority()
		},
		validate: validateMaterializationSourceLost,
	},
	OperationResolveInputTransfer: {
		authority: operationAuthorityStage,
		stageAuthority: func(request *velav1.StageWorkerControlServiceConnectRequest) *velav1.StageAuthority {
			return request.GetResolveInputTransfer().GetAuthority()
		},
		validate:    validateResolveInputTransfer,
		activeState: activeAssignedStage,
	},
	OperationConsumeInputTransfer: {
		authority: operationAuthorityStage,
		stageAuthority: func(request *velav1.StageWorkerControlServiceConnectRequest) *velav1.StageAuthority {
			return request.GetConsumeInputTransfer().GetAuthority()
		},
		validate:    validateConsumeInputTransfer,
		activeState: activeAssignedStage,
	},
}

func descriptorForOperation(operation Operation) (operationDescriptor, bool) {
	descriptor, ok := operationDescriptors[operation]
	return descriptor, ok
}

func operationFromRequest(request *velav1.StageWorkerControlServiceConnectRequest) Operation {
	if request == nil {
		return velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_UNSPECIFIED
	}
	switch request.GetOperation().(type) {
	case *velav1.StageWorkerControlServiceConnectRequest_RegisterWorkerEvidence:
		return OperationRegisterWorkerEvidence
	case *velav1.StageWorkerControlServiceConnectRequest_ReportCapacityObservation:
		return OperationReportCapacityObservation
	case *velav1.StageWorkerControlServiceConnectRequest_AcquireStage:
		return OperationAcquireStage
	case *velav1.StageWorkerControlServiceConnectRequest_StartStage:
		return OperationStartStage
	case *velav1.StageWorkerControlServiceConnectRequest_HeartbeatStage:
		return OperationHeartbeatStage
	case *velav1.StageWorkerControlServiceConnectRequest_SealStageOutput:
		return OperationSealStageOutput
	case *velav1.StageWorkerControlServiceConnectRequest_CommitStageMaterialization:
		return OperationCommitStageMaterialization
	case *velav1.StageWorkerControlServiceConnectRequest_FailStage:
		return OperationFailStage
	case *velav1.StageWorkerControlServiceConnectRequest_ReattachStage:
		return OperationReattachStage
	case *velav1.StageWorkerControlServiceConnectRequest_ReportMaterializationSourceLost:
		return OperationReportMaterializationSourceLost
	case *velav1.StageWorkerControlServiceConnectRequest_ResolveInputTransfer:
		return OperationResolveInputTransfer
	case *velav1.StageWorkerControlServiceConnectRequest_ConsumeInputTransfer:
		return OperationConsumeInputTransfer
	default:
		return velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_UNSPECIFIED
	}
}

func validateRegisterWorkerEvidence(
	request *velav1.StageWorkerControlServiceConnectRequest,
	_ VerifiedAuthorities,
) error {
	value := request.GetRegisterWorkerEvidence()
	if value == nil || value.GetRuntimeIdentity() == nil ||
		value.GetCapacityObservationSequence() < 0 || len(value.GetDevices()) == 0 ||
		len(value.GetMembers()) == 0 || len(value.GetReadinessEvidence()) == 0 ||
		len(value.GetReadinessEvidence()) > maxReadinessEvidenceBytes {
		return errors.New("worker registration evidence is incomplete")
	}
	return nil
}

func validateCapacityObservation(
	request *velav1.StageWorkerControlServiceConnectRequest,
	_ VerifiedAuthorities,
) error {
	value := request.GetReportCapacityObservation()
	if value == nil || parseUUID(value.GetWorkerInstanceId()) == uuid.Nil ||
		value.GetWorkerInstanceEpoch() <= 0 || value.GetObservationSequence() <= 0 ||
		!validCapacityVector(value.GetCapacityVector()) ||
		!validTimestamp(value.GetObservedAt()) || !validTimestamp(value.GetExpiresAt()) ||
		!value.GetExpiresAt().AsTime().After(value.GetObservedAt().AsTime()) {
		return errors.New("stage capacity observation is invalid")
	}
	return nil
}

func validateAcquireStage(
	request *velav1.StageWorkerControlServiceConnectRequest,
	_ VerifiedAuthorities,
) error {
	value := request.GetAcquireStage()
	if value == nil || parseUUID(value.GetWorkerInstanceId()) == uuid.Nil ||
		parseUUID(value.GetModelResidencyId()) == uuid.Nil ||
		parseUUID(value.GetStageProfileRevisionId()) == uuid.Nil ||
		value.GetWorkerInstanceEpoch() <= 0 || value.GetCapacityObservationSequence() <= 0 ||
		value.GetModelRuntimeEpoch() <= 0 {
		return errors.New("stage acquire authority is incomplete")
	}
	return nil
}

func validateStartStage(
	request *velav1.StageWorkerControlServiceConnectRequest,
	authorities VerifiedAuthorities,
) error {
	if authorities.Stage == nil || !validTimestamp(request.GetStartStage().GetStartedAt()) {
		return errors.New("stage start evidence is invalid")
	}
	return nil
}

func validateHeartbeatStage(
	request *velav1.StageWorkerControlServiceConnectRequest,
	authorities VerifiedAuthorities,
) error {
	value := request.GetHeartbeatStage()
	if authorities.Stage == nil || value.GetSequence() <= 0 ||
		value.GetRuntimeState() == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED ||
		len(value.GetBoundedStatusJson()) == 0 || len(value.GetBoundedStatusJson()) > maxBoundedStatusBytes ||
		!json.Valid(value.GetBoundedStatusJson()) || !validTimestamp(value.GetObservedAt()) ||
		(value.GetLocalReceiptId() == "") != (len(value.GetLocalReceiptDigest()) == 0) ||
		(len(value.GetLocalReceiptDigest()) != 0 && len(value.GetLocalReceiptDigest()) != sha256.Size) {
		return errors.New("stage heartbeat evidence is invalid")
	}
	return nil
}

func validateSealStageOutput(
	request *velav1.StageWorkerControlServiceConnectRequest,
	authorities VerifiedAuthorities,
) error {
	if authorities.Stage == nil || request.GetSealStageOutput().GetLocalReceipt() == nil {
		return errors.New("stage seal evidence is invalid")
	}
	return nil
}

func validateCommitStageMaterialization(
	request *velav1.StageWorkerControlServiceConnectRequest,
	authorities VerifiedAuthorities,
) error {
	value := request.GetCommitStageMaterialization()
	if authorities.Materialization == nil || strings.TrimSpace(value.GetObjectVersion()) == "" ||
		!validTimestamp(value.GetCommittedAt()) {
		return errors.New("stage materialization commit evidence is invalid")
	}
	return nil
}

func validateFailStage(
	request *velav1.StageWorkerControlServiceConnectRequest,
	authorities VerifiedAuthorities,
) error {
	value := request.GetFailStage()
	if authorities.Stage == nil || strings.TrimSpace(value.GetFailureClass()) == "" ||
		len(value.GetFailureClass()) > 100 || len(value.GetFailureFingerprint()) != sha256.Size ||
		value.GetConsumedResourceUnits() <= 0 || !validTimestamp(value.GetFailedAt()) ||
		!validTimestamp(value.GetRetryAt()) ||
		!value.GetRetryAt().AsTime().After(value.GetFailedAt().AsTime()) ||
		len(value.GetDetail()) > maxControlDetailBytes {
		return errors.New("stage failure evidence is invalid")
	}
	return nil
}

func validateReattachStage(
	request *velav1.StageWorkerControlServiceConnectRequest,
	authorities VerifiedAuthorities,
) error {
	value := request.GetReattachStage()
	if authorities.Stage == nil ||
		value.GetObservedRuntimeState() == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED ||
		(value.GetLocalReceiptId() == "") != (len(value.GetLocalReceiptDigest()) == 0) ||
		(len(value.GetLocalReceiptDigest()) != 0 && len(value.GetLocalReceiptDigest()) != sha256.Size) {
		return errors.New("stage reattach evidence is invalid")
	}
	return nil
}

func validateMaterializationSourceLost(
	request *velav1.StageWorkerControlServiceConnectRequest,
	authorities VerifiedAuthorities,
) error {
	value := request.GetReportMaterializationSourceLost()
	if authorities.Materialization == nil || len(value.GetFailureFingerprint()) != sha256.Size ||
		value.GetConsumedResourceUnits() <= 0 || !validTimestamp(value.GetLostAt()) ||
		!validTimestamp(value.GetRetryAt()) ||
		!value.GetRetryAt().AsTime().After(value.GetLostAt().AsTime()) {
		return errors.New("stage materialization source-loss evidence is invalid")
	}
	return nil
}

func validateResolveInputTransfer(
	request *velav1.StageWorkerControlServiceConnectRequest,
	authorities VerifiedAuthorities,
) error {
	value := request.GetResolveInputTransfer()
	stage := authorities.Stage
	if stage == nil || value == nil || parseUUID(value.GetTicketId()) == uuid.Nil ||
		len(value.GetTokenDigest()) != sha256.Size ||
		parseUUID(value.GetConnectorRevisionId()) == uuid.Nil ||
		parseUUID(value.GetWorkerInstanceId()) == uuid.Nil ||
		parseUUID(value.GetModelResidencyId()) == uuid.Nil || value.GetWorkerInstanceEpoch() <= 0 ||
		value.GetModelRuntimeEpoch() <= 0 || !validTimestamp(value.GetResolvedAt()) ||
		value.GetWorkerInstanceId() != stage.Authority.GetWorkerInstanceId() ||
		value.GetWorkerInstanceEpoch() != stage.Authority.GetWorkerInstanceEpoch() ||
		value.GetModelResidencyId() != stage.Authority.GetModelResidencyId() ||
		!stageAuthorityHasBarrierGeneration(stage.Authority, value.GetModelRuntimeEpoch()) {
		return errors.New("stage input transfer resolve authority is invalid")
	}
	return nil
}

func validateConsumeInputTransfer(
	request *velav1.StageWorkerControlServiceConnectRequest,
	authorities VerifiedAuthorities,
) error {
	value := request.GetConsumeInputTransfer()
	stage := authorities.Stage
	if stage == nil || value == nil || parseUUID(value.GetTicketId()) == uuid.Nil ||
		len(value.GetTokenDigest()) != sha256.Size || len(value.GetOutcomeDigest()) != sha256.Size ||
		parseUUID(value.GetConnectorRevisionId()) == uuid.Nil ||
		parseUUID(value.GetWorkerInstanceId()) == uuid.Nil ||
		parseUUID(value.GetModelResidencyId()) == uuid.Nil || value.GetWorkerInstanceEpoch() <= 0 ||
		value.GetModelRuntimeEpoch() <= 0 || !validTimestamp(value.GetConsumedAt()) ||
		value.GetWorkerInstanceId() != stage.Authority.GetWorkerInstanceId() ||
		value.GetWorkerInstanceEpoch() != stage.Authority.GetWorkerInstanceEpoch() ||
		value.GetModelResidencyId() != stage.Authority.GetModelResidencyId() ||
		!stageAuthorityHasBarrierGeneration(stage.Authority, value.GetModelRuntimeEpoch()) {
		return errors.New("stage input transfer consume evidence is invalid")
	}
	return nil
}

func activeAssignedStage(snapshot durableStageAuthority) bool {
	return snapshot.stageAttemptState == "ASSIGNED" && snapshot.stageRunState == "ASSIGNED" &&
		(snapshot.attemptState == "ASSIGNED" || snapshot.attemptState == "RUNNING")
}

func activeRunningStage(snapshot durableStageAuthority) bool {
	return snapshot.stageAttemptState == "RUNNING" && snapshot.stageRunState == "RUNNING" &&
		snapshot.attemptState == "RUNNING"
}

func activeAssignedOrRunningStage(snapshot durableStageAuthority) bool {
	return (snapshot.stageAttemptState == "ASSIGNED" || snapshot.stageAttemptState == "RUNNING") &&
		(snapshot.stageRunState == "ASSIGNED" || snapshot.stageRunState == "RUNNING") &&
		(snapshot.attemptState == "ASSIGNED" || snapshot.attemptState == "RUNNING")
}

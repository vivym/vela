package modelruntimetransport

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const TerminalNonAdmissionContract = "vela-terminal-allocation-never-admitted-v1"

// ValidateTerminalAllocationScope verifies signed history without inventing an
// execution envelope. The receiver must independently trust the current reader.
func ValidateTerminalAllocationScope(validator *stageauthority.Validator, scope *velav1.ModelRuntimeTerminalAllocationScope, historical bool) (stageauthority.VerifiedTerminalDisposition, error) {
	invalid := errors.New("terminal allocation scope is invalid")
	if validator == nil || scope == nil || scope.GetSchemaVersion() != 1 || len(scope.ProtoReflect().GetUnknown()) != 0 ||
		scope.GetDisposition() == nil || proto.Size(scope.GetDisposition()) > 64<<10 {
		return stageauthority.VerifiedTerminalDisposition{}, invalid
	}
	var verified stageauthority.VerifiedTerminalDisposition
	var err error
	if historical {
		verified, err = validator.ValidateTerminalDispositionForReplay(scope.GetDisposition())
	} else {
		verified, err = validator.ValidateTerminalDispositionEnvelope(scope.GetDisposition())
	}
	if err != nil {
		return stageauthority.VerifiedTerminalDisposition{}, err
	}
	allocation := stageauthority.FindTerminalAllocation(verified.Disposition, scope.GetStageAllocationId())
	if allocation == nil || !proto.Equal(verified.Disposition, scope.GetDisposition()) || !validTerminalTarget(scope.GetIdentity(), verified.Disposition, allocation, historical) {
		return stageauthority.VerifiedTerminalDisposition{}, invalid
	}
	return verified, nil
}

func validTerminalTarget(identity *velav1.ModelRuntimeIdentity, disposition *velav1.StageTerminalDisposition, allocation *velav1.StageTerminalAllocation, historical bool) bool {
	if identity == nil || len(identity.ProtoReflect().GetUnknown()) != 0 || uuid.Validate(identity.GetModelResidencyId()) != nil ||
		uuid.Validate(identity.GetStageProfileRevisionId()) != nil || identity.GetRuntimeIdentity() == "" || len(identity.GetRuntimeIdentity()) > 200 ||
		!utf8.ValidString(identity.GetRuntimeIdentity()) || strings.TrimSpace(identity.GetRuntimeIdentity()) != identity.GetRuntimeIdentity() ||
		identity.GetModelRuntimeEpoch() <= 0 || identity.GetWorkerInstanceId() != disposition.GetWorkerInstanceId() ||
		identity.GetWorkerInstanceEpoch() != disposition.GetWorkerInstanceEpoch() || !bytes.Equal(identity.GetDeviceSetDigest(), disposition.GetDeviceSetDigest()) ||
		!bytes.Equal(identity.GetMembershipDigest(), disposition.GetMembershipDigest()) {
		return false
	}
	for _, member := range allocation.GetMembers() {
		if member.GetWorkerMemberId() == identity.GetWorkerMemberId() && member.GetMemberEpoch() == identity.GetWorkerMemberEpoch() {
			return historical || member.GetModelRuntimeEpoch() == identity.GetModelRuntimeEpoch() &&
				allocation.GetModelResidencyId() == identity.GetModelResidencyId() && allocation.GetModelRuntimeIdentity() == identity.GetRuntimeIdentity() &&
				allocation.GetStageProfileRevisionId() == identity.GetStageProfileRevisionId()
		}
	}
	return false
}

// ValidateTerminalNonAdmissionResult preserves the original signed checkpoint
// while binding its member and allocation to the independently verified query.
func ValidateTerminalNonAdmissionResult(validator *stageauthority.Validator, scope *velav1.ModelRuntimeTerminalAllocationScope, digest [sha256.Size]byte, result *velav1.ModelRuntimeTerminalNonAdmissionResult) error {
	invalid := errors.New("terminal non-admission result is invalid")
	if validator == nil || scope == nil || scope.GetIdentity() == nil || scope.GetDisposition() == nil || result == nil ||
		result.GetSchemaVersion() != 1 || len(result.ProtoReflect().GetUnknown()) != 0 || !proto.Equal(scope.GetIdentity(), result.GetIdentity()) ||
		!bytes.Equal(result.GetDispositionDigest(), digest[:]) || result.GetStageAllocationId() != scope.GetStageAllocationId() {
		return invalid
	}
	allocation := stageauthority.FindTerminalAllocation(scope.GetDisposition(), scope.GetStageAllocationId())
	if allocation == nil {
		return invalid
	}
	switch result.GetDecision() {
	case velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED:
	case velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE, velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED:
		if result.GetCheckpoint() != nil {
			return invalid
		}
		return nil
	default:
		return invalid
	}
	checkpoint := result.GetCheckpoint()
	if checkpoint == nil {
		return nil
	}
	if checkpoint.GetSchemaVersion() != 1 || len(checkpoint.ProtoReflect().GetUnknown()) != 0 || checkpoint.GetDisposition() == nil ||
		proto.Size(checkpoint.GetDisposition()) > 64<<10 || checkpoint.GetWorkerMemberId() != scope.GetIdentity().GetWorkerMemberId() ||
		checkpoint.GetStageAllocationId() != allocation.GetStageAllocationId() || checkpoint.GetExecutionSequence() != allocation.GetExecutionSequence() ||
		checkpoint.GetInstalledCutoff() < checkpoint.GetExecutionSequence() || checkpoint.GetContract() != TerminalNonAdmissionContract ||
		checkpoint.GetObservedAt() == nil || checkpoint.GetObservedAt().CheckValid() != nil || checkpoint.GetObservedAt().AsTime().IsZero() ||
		len(checkpoint.GetObservedAt().ProtoReflect().GetUnknown()) != 0 {
		return invalid
	}
	verified, err := validator.ValidateTerminalDispositionForReplay(checkpoint.GetDisposition())
	if err != nil {
		return err
	}
	if !proto.Equal(verified.Disposition, checkpoint.GetDisposition()) || !bytes.Equal(verified.Digest[:], checkpoint.GetDispositionDigest()) ||
		checkpoint.GetObservedAt().AsTime().Add(stageauthority.MaxTerminalObservationSkew).Before(verified.Disposition.GetObservedAt().AsTime()) ||
		stageauthority.ValidateSameTerminalAllocation(verified.Disposition, scope.GetDisposition(), scope.GetStageAllocationId()) != nil {
		return invalid
	}
	return nil
}

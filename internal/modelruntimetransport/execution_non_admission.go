package modelruntimetransport

import (
	"bytes"
	"crypto/sha256"
	"errors"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const ExecutionNonAdmissionContract = "vela-execution-never-admitted-v1"

func ValidateExecutionNonAdmissionResult(validator *stageauthority.Validator, scope *velav1.ModelRuntimeExecutionDrainScope, digest [sha256.Size]byte, result *velav1.ModelRuntimeExecutionNonAdmissionResult) error {
	invalid := errors.New("execution non-admission proof is invalid")
	if scope == nil || scope.GetAuthority() == nil || scope.GetIdentity() == nil || result == nil ||
		result.GetSchemaVersion() != 1 || len(result.ProtoReflect().GetUnknown()) != 0 ||
		!proto.Equal(result.GetIdentity(), scope.GetIdentity()) || !bytes.Equal(result.GetAuthorityDigest(), digest[:]) {
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
	if checkpoint.GetSchemaVersion() != 1 || len(checkpoint.ProtoReflect().GetUnknown()) != 0 ||
		checkpoint.GetAuthority().GetSchemaVersion() != stageauthority.SchemaVersionV2 || proto.Size(checkpoint.GetAuthority()) > 64<<10 ||
		checkpoint.GetWorkerMemberId() != scope.GetIdentity().GetWorkerMemberId() ||
		checkpoint.GetExecutionSequence() <= 0 || checkpoint.GetExecutionSequence() != scope.GetAuthority().GetExecutionSequence() ||
		checkpoint.GetInstalledCutoff() < checkpoint.GetExecutionSequence() || checkpoint.GetContract() != ExecutionNonAdmissionContract ||
		checkpoint.GetObservedAt() == nil || checkpoint.GetObservedAt().CheckValid() != nil || checkpoint.GetObservedAt().AsTime().IsZero() ||
		len(checkpoint.GetObservedAt().ProtoReflect().GetUnknown()) != 0 {
		return invalid
	}
	verified, err := validator.ValidateEnvelopeForReplay(checkpoint.GetAuthority(), 0)
	if err != nil {
		return err
	}
	if !proto.Equal(verified.Authority, checkpoint.GetAuthority()) || !bytes.Equal(verified.Digest[:], checkpoint.GetAuthorityDigest()) ||
		stageauthority.ValidateSameExecution(scope.GetAuthority(), verified.Authority) != nil {
		return invalid
	}
	return nil
}

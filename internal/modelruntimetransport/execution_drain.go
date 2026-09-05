package modelruntimetransport

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const ExecutionDrainContract = "vela-execution-writer-drain-v1"

// ValidateExecutionDrainScope verifies an exact historical signature without
// granting execution time. The receiver must separately trust the current target.
func ValidateExecutionDrainScope(validator *stageauthority.Validator, scope *velav1.ModelRuntimeExecutionDrainScope, skew time.Duration, historical bool) (stageauthority.Verified, error) {
	invalid := errors.New("execution drain scope is invalid")
	if scope == nil || scope.GetSchemaVersion() != 1 || len(scope.ProtoReflect().GetUnknown()) != 0 ||
		scope.GetAuthority().GetSchemaVersion() != stageauthority.SchemaVersionV2 || proto.Size(scope.GetAuthority()) > 64<<10 {
		return stageauthority.Verified{}, invalid
	}
	verified, err := validator.ValidateEnvelopeForReplay(scope.GetAuthority(), skew)
	if err != nil {
		return stageauthority.Verified{}, err
	}
	if !proto.Equal(scope.GetAuthority(), verified.Authority) || !validDrainTarget(scope.GetIdentity(), verified.Authority, historical) {
		return stageauthority.Verified{}, invalid
	}
	return verified, nil
}

func validDrainTarget(identity *velav1.ModelRuntimeIdentity, authority *velav1.StageAuthority, historical bool) bool {
	if identity == nil || authority == nil || len(identity.ProtoReflect().GetUnknown()) != 0 ||
		uuid.Validate(identity.GetModelResidencyId()) != nil || uuid.Validate(identity.GetStageProfileRevisionId()) != nil ||
		identity.GetRuntimeIdentity() == "" || len(identity.GetRuntimeIdentity()) > 200 ||
		!utf8.ValidString(identity.GetRuntimeIdentity()) || strings.TrimSpace(identity.GetRuntimeIdentity()) != identity.GetRuntimeIdentity() ||
		identity.GetModelRuntimeEpoch() <= 0 || identity.GetWorkerInstanceId() != authority.GetWorkerInstanceId() ||
		identity.GetWorkerInstanceEpoch() != authority.GetWorkerInstanceEpoch() ||
		!bytes.Equal(identity.GetDeviceSetDigest(), authority.GetDeviceSetDigest()) ||
		!bytes.Equal(identity.GetMembershipDigest(), authority.GetMembershipDigest()) {
		return false
	}
	for _, member := range authority.GetMembers() {
		if member.GetWorkerMemberId() == identity.GetWorkerMemberId() && member.GetMemberEpoch() == identity.GetWorkerMemberEpoch() {
			return historical || (member.GetModelRuntimeEpoch() == identity.GetModelRuntimeEpoch() &&
				identity.GetModelResidencyId() == authority.GetModelResidencyId() && identity.GetRuntimeIdentity() == authority.GetModelRuntimeIdentity() &&
				identity.GetStageProfileRevisionId() == authority.GetStageProfileRevisionId())
		}
	}
	return false
}

// ValidateExecutionDrainResult binds both the current journal owner and original
// execution. An accepted result without a checkpoint is still unproven.
func ValidateExecutionDrainResult(scope *velav1.ModelRuntimeExecutionDrainScope, digest [sha256.Size]byte, result *velav1.ModelRuntimeExecutionDrainResult) error {
	invalid := errors.New("execution drain result is invalid")
	if scope == nil || scope.GetAuthority() == nil || scope.GetIdentity() == nil || result == nil ||
		result.GetSchemaVersion() != 1 || len(result.ProtoReflect().GetUnknown()) != 0 ||
		!proto.Equal(result.GetIdentity(), scope.GetIdentity()) ||
		!bytes.Equal(result.GetAuthorityDigest(), digest[:]) || !utf8.ValidString(result.GetDetail()) || utf8.RuneCountInString(result.GetDetail()) > 1000 {
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
		!proto.Equal(checkpoint.GetAuthority(), scope.GetAuthority()) || !bytes.Equal(checkpoint.GetAuthorityDigest(), digest[:]) ||
		checkpoint.GetWorkerMemberId() != scope.GetIdentity().GetWorkerMemberId() ||
		checkpoint.GetExecutionSequence() <= 0 || checkpoint.GetExecutionSequence() != scope.GetAuthority().GetExecutionSequence() ||
		checkpoint.GetContract() != ExecutionDrainContract || checkpoint.GetDrainedAt() == nil ||
		checkpoint.GetDrainedAt().CheckValid() != nil || checkpoint.GetDrainedAt().AsTime().IsZero() ||
		len(checkpoint.GetDrainedAt().ProtoReflect().GetUnknown()) != 0 {
		return invalid
	}
	return nil
}

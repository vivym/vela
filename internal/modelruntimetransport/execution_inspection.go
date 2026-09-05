package modelruntimetransport

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"unicode/utf8"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// ValidateExecutionInspectionResponse binds a bounded observation to the exact
// queried envelope and resident member. It grants no drain or deletion authority.
func ValidateExecutionInspectionResponse(
	digest [sha256.Size]byte, identity *velav1.ModelRuntimeIdentity,
	response *velav1.ModelRuntimeServiceInspectExecutionResponse,
) error {
	invalid := errors.New("execution inspection response is invalid")
	if response == nil || response.GetSchemaVersion() != 1 || len(response.ProtoReflect().GetUnknown()) != 0 ||
		identity == nil || len(identity.ProtoReflect().GetUnknown()) != 0 ||
		!bytes.Equal(response.GetAuthorityDigest(), digest[:]) || !proto.Equal(response.GetRuntimeIdentity(), identity) ||
		!utf8.ValidString(response.GetDetail()) || utf8.RuneCountInString(response.GetDetail()) > 1000 || response.GetSequence() < 0 {
		return invalid
	}
	if response.GetKnown() {
		if response.GetState() < velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARING ||
			response.GetState() > velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED {
			return invalid
		}
	} else if response.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED || response.GetSequence() != 0 {
		return invalid
	}
	switch response.GetDecision() {
	case velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED:
		if response.GetObservedAt() == nil || response.GetObservedAt().CheckValid() != nil ||
			len(response.GetObservedAt().ProtoReflect().GetUnknown()) != 0 {
			return invalid
		}
	case velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE,
		velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED:
		if response.GetKnown() || response.GetObservedAt() != nil {
			return invalid
		}
	default:
		return invalid
	}
	return nil
}

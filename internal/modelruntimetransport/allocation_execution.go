package modelruntimetransport

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"unicode/utf8"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// ValidateAllocationExecutionResponse verifies the observed envelope separately
// from the query. It never converts a live observation into writer-drain proof.
func ValidateAllocationExecutionResponse(validator *stageauthority.Validator, authority *velav1.StageAuthority, digest [sha256.Size]byte, identity *velav1.ModelRuntimeIdentity, response *velav1.ModelRuntimeServiceInspectAllocationExecutionResponse) error {
	invalid := errors.New("allocation execution observation is invalid")
	if validator == nil || authority == nil || identity == nil || response == nil ||
		response.GetSchemaVersion() != 1 || len(response.ProtoReflect().GetUnknown()) != 0 ||
		!validDrainTarget(identity, authority, false) || !proto.Equal(response.GetRuntimeIdentity(), identity) ||
		!bytes.Equal(response.GetAuthorityDigest(), digest[:]) || !utf8.ValidString(response.GetDetail()) || utf8.RuneCountInString(response.GetDetail()) > 1000 {
		return invalid
	}
	switch response.GetDecision() {
	case velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED:
	case velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED, velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE:
		if response.GetObservedAuthority() != nil || response.GetInspection() != nil {
			return invalid
		}
		return nil
	default:
		return invalid
	}
	observed, inspection := response.GetObservedAuthority(), response.GetInspection()
	if observed == nil && inspection == nil {
		return nil
	}
	if observed == nil || inspection == nil || proto.Size(observed) > 64<<10 || !inspection.GetKnown() ||
		inspection.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		return invalid
	}
	verified, err := validator.ValidateEnvelopeForReplay(observed, 0)
	if err != nil {
		return err
	}
	if !proto.Equal(verified.Authority, observed) || stageauthority.ValidateSameExecution(authority, observed) != nil {
		return invalid
	}
	return ValidateExecutionInspectionResponse(verified.Digest, identity, inspection)
}

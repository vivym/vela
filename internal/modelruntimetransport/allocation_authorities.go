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

// ValidateAllocationAuthoritiesResponse binds the current reader and query
// independently of signed journal history. It establishes no live writer state.
func ValidateAllocationAuthoritiesResponse(validator *stageauthority.Validator, scope *velav1.ModelRuntimeExecutionDrainScope, digest [sha256.Size]byte, response *velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse) error {
	invalid := errors.New("retained allocation authorities are invalid")
	if validator == nil || scope == nil || !validDrainTarget(scope.GetIdentity(), scope.GetAuthority(), true) || response == nil ||
		response.GetSchemaVersion() != 1 || len(response.ProtoReflect().GetUnknown()) != 0 ||
		!proto.Equal(response.GetIdentity(), scope.GetIdentity()) || !bytes.Equal(response.GetAuthorityDigest(), digest[:]) ||
		!utf8.ValidString(response.GetDetail()) || utf8.RuneCountInString(response.GetDetail()) > 1000 {
		return invalid
	}
	switch response.GetDecision() {
	case velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED:
	case velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED, velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE:
		if response.GetAuthorities() != nil {
			return invalid
		}
		return nil
	default:
		return invalid
	}
	history := response.GetAuthorities()
	if history == nil {
		return nil
	}
	if len(history.ProtoReflect().GetUnknown()) != 0 || history.GetOriginal() == nil ||
		(history.GetAccepted() == nil && history.GetConfirmed() != nil) {
		return invalid
	}
	var verified [3]stageauthority.Verified
	for index, authority := range []*velav1.StageAuthority{history.GetOriginal(), history.GetAccepted(), history.GetConfirmed()} {
		if authority == nil {
			continue
		}
		if authority.GetSchemaVersion() != stageauthority.SchemaVersionV2 || proto.Size(authority) > 64<<10 {
			return invalid
		}
		candidate, err := validator.ValidateEnvelopeForReplay(authority, 0)
		if err != nil || !proto.Equal(candidate.Authority, authority) || stageauthority.ValidateSameExecution(scope.GetAuthority(), authority) != nil {
			return invalid
		}
		verified[index] = candidate
	}
	original, accepted, confirmed := verified[0], verified[1], verified[2]
	if accepted.Authority == nil {
		return nil // Migrated journals cannot reconstruct renewal history.
	}
	if original.Digest != accepted.Digest && stageauthority.ValidateRenewal(original.Authority, accepted.Authority) != nil {
		return invalid
	}
	if confirmed.Authority == nil {
		if accepted.Digest != original.Digest {
			return invalid
		}
		return nil
	}
	if (original.Digest != confirmed.Digest && stageauthority.ValidateRenewal(original.Authority, confirmed.Authority) != nil) ||
		(accepted.Digest != confirmed.Digest && stageauthority.ValidateRenewal(confirmed.Authority, accepted.Authority) != nil) {
		return invalid
	}
	return nil
}

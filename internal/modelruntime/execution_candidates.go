package modelruntime

import (
	"bytes"
	"context"
	"errors"
	"slices"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// RetainedAllocationAuthorities is journal history, not a live observation or
// permission to enter a replacement backend. Legacy records have only Original.
type RetainedAllocationAuthorities struct {
	Original  *velav1.StageAuthority
	Accepted  *velav1.StageAuthority
	Confirmed *velav1.StageAuthority
}

func (supervisor *Supervisor) InspectRetainedAllocationAuthorities(ctx context.Context, authority *velav1.StageAuthority) (*RetainedAllocationAuthorities, error) {
	if supervisor == nil || supervisor.floor == nil || ctx == nil {
		return nil, ErrExecutionStateRecovery
	}
	verified, err := supervisor.floor.validator.ValidateEnvelopeForReplay(authority, supervisor.services[0].maxClockSkew)
	if err != nil {
		return nil, err
	}
	if err := supervisor.matchRetainedExecutionScope(verified.Authority); err != nil {
		return nil, err
	}
	admission := supervisor.admission
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := admission.checkStateLocked(); err != nil {
		return nil, err
	}
	if admission.store == nil {
		return nil, ErrExecutionStateRecovery
	}
	for _, record := range admission.store.state.Executions {
		original, err := admission.store.retainedAuthority(record.Authority)
		if err != nil {
			return nil, err
		}
		if stageauthority.ValidateSameExecution(original.Authority, verified.Authority) != nil {
			continue
		}
		result := &RetainedAllocationAuthorities{Original: original.Authority}
		if record.Candidates != nil {
			accepted, err := admission.store.retainedAuthority(record.Candidates.Accepted)
			if err != nil {
				return nil, err
			}
			result.Accepted = accepted.Authority
			if len(record.Candidates.Confirmed) != 0 {
				confirmed, err := admission.store.retainedAuthority(record.Candidates.Confirmed)
				if err != nil {
					return nil, err
				}
				result.Confirmed = confirmed.Authority
			}
		}
		return result, nil
	}
	return nil, nil
}

func (store *executionStateFile) validateCandidates(original stageauthority.Verified, candidates *executionDiskCandidates) error {
	accepted, err := store.retainedAuthority(candidates.Accepted)
	if err != nil {
		return err
	}
	if original.Digest != accepted.Digest && stageauthority.ValidateRenewal(original.Authority, accepted.Authority) != nil {
		return errors.New("accepted renewal does not belong to the retained execution")
	}
	if len(candidates.Confirmed) == 0 {
		if accepted.Digest != original.Digest {
			return errors.New("renewal has no prior confirmed backend authority")
		}
		return nil
	}
	confirmed, err := store.retainedAuthority(candidates.Confirmed)
	if err != nil {
		return err
	}
	if original.Digest != confirmed.Digest && stageauthority.ValidateRenewal(original.Authority, confirmed.Authority) != nil ||
		accepted.Digest != confirmed.Digest && stageauthority.ValidateRenewal(confirmed.Authority, accepted.Authority) != nil {
		return errors.New("confirmed backend authority is outside the retained renewal interval")
	}
	return nil
}

// The admission mutex owns the journal. A new candidate pair is persisted before
// dispatch; backend acknowledgement is persisted before returning success.
func (store *executionStateFile) saveCandidates(accepted stageauthority.Verified, confirmed *stageauthority.Verified) error {
	index, err := store.retainedExecutionIndex(accepted)
	if err != nil {
		return err
	}
	previous := store.state.Executions[index]
	if previous.Candidates == nil {
		return errors.New("legacy execution has unknown renewal history")
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(accepted.Authority)
	if err != nil {
		return err
	}
	candidates := &executionDiskCandidates{Accepted: wire}
	if confirmed != nil {
		candidates.Confirmed, err = proto.MarshalOptions{Deterministic: true}.Marshal(confirmed.Authority)
		if err != nil {
			return err
		}
	}
	if bytes.Equal(candidates.Accepted, previous.Candidates.Accepted) && bytes.Equal(candidates.Confirmed, previous.Candidates.Confirmed) {
		return nil
	}
	if previous.Drain != nil {
		return errors.New("drained execution candidate history is immutable")
	}
	if len(previous.Candidates.Confirmed) != 0 && !bytes.Equal(previous.Candidates.Confirmed, candidates.Confirmed) &&
		!bytes.Equal(candidates.Accepted, candidates.Confirmed) {
		return errors.New("confirmed backend authority cannot regress")
	}
	original, err := store.retainedAuthority(previous.Authority)
	if err != nil {
		return err
	}
	if err := store.validateCandidates(original, candidates); err != nil {
		return err
	}
	before, err := store.retainedAuthority(previous.Candidates.Accepted)
	if err != nil {
		return err
	}
	if before.Digest != accepted.Digest {
		if !bytes.Equal(previous.Candidates.Accepted, previous.Candidates.Confirmed) ||
			!bytes.Equal(previous.Candidates.Confirmed, candidates.Confirmed) || stageauthority.ValidateRenewal(before.Authority, accepted.Authority) != nil {
			return errBackendAuthorityUncertain
		}
	}
	state := store.state
	state.Executions = slices.Clone(state.Executions)
	state.Executions[index].Candidates = candidates
	return store.persist(state)
}

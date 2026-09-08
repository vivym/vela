package modelruntime

import (
	"bytes"
	"errors"
	"slices"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// A draft has no filesystem, backend or admission handles. Domain mutations
// construct a candidate state; the owner validates the complete candidate before
// publishing it. This is not process authentication or permission to run work.
type executionJournalDraft struct {
	executionJournal
	changed bool
}

func (draft *executionJournalDraft) replace(state executionDiskState) error {
	draft.state, draft.changed = state, true
	return nil
}

// A caller-supplied Verified value is not a trust boundary. Recompute its signed
// identity using the journal owner's verifier before applying a typed mutation.
func (draft *executionJournalDraft) verifyMutationAuthority(value stageauthority.Verified) (stageauthority.Verified, error) {
	if value.Authority == nil || proto.Size(value.Authority) > maxExecutionWireBytes {
		return stageauthority.Verified{}, errors.New("journal mutation has no bounded execution authority")
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(value.Authority)
	if err != nil {
		return stageauthority.Verified{}, err
	}
	return draft.retainedAuthority(wire)
}

func (store *executionStateFile) transition(mutate func(*executionJournalDraft) error) error {
	if err := store.check(); err != nil {
		return err
	}
	draft := &executionJournalDraft{executionJournal: store.executionJournal}
	if err := mutate(draft); err != nil {
		return err
	}
	if !draft.changed {
		return nil
	}
	// persist validates the complete candidate once, before any filesystem write.
	return store.persist(draft.state)
}

func (store *executionStateFile) saveHighest(authority *velav1.StageAuthority, maxClockSkew time.Duration) error {
	return store.transition(func(draft *executionJournalDraft) error { return draft.admit(authority, maxClockSkew) })
}

func (store *executionStateFile) saveFloor(disposition *velav1.StageTerminalDisposition) error {
	return store.transition(func(draft *executionJournalDraft) error { return draft.installFloor(disposition) })
}

// maxClockSkew comes from the owner's trusted Service configuration, not a
// request field. An IPC adapter must resolve it from its approved runtime route.
func (store *executionJournalDraft) admit(authority *velav1.StageAuthority, maxClockSkew time.Duration) error {
	verified, err := store.scope.floor.validator.ValidateEnvelopeWithClockSkew(authority, maxClockSkew)
	if err != nil {
		return err
	}
	if err := store.scope.matchRetainedExecutionScope(verified.Authority); err != nil {
		return err
	}
	if authority.GetExecutionSequence() <= store.state.Floor {
		return errExecutionFloor
	}
	if err := store.workerHealthError(); err != nil {
		return err
	}
	state := store.state
	if len(state.Executions) >= maxRetainedExecutions {
		return ErrExecutionHistoryFull
	}
	if authority.GetExecutionSequence() <= state.Highest {
		return errors.New("ModelRuntime execution watermark cannot regress")
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(authority)
	if err != nil || len(wire) > maxExecutionWireBytes {
		return errors.New("ModelRuntime execution authority exceeds its persistence bound")
	}
	state.Highest, state.Authority = authority.GetExecutionSequence(), wire
	state.Executions = append(slices.Clone(state.Executions), retainedExecution{Authority: bytes.Clone(wire),
		Candidates: &executionDiskCandidates{Accepted: bytes.Clone(wire)}})
	return store.replace(state)
}

func (store *executionJournalDraft) installFloor(disposition *velav1.StageTerminalDisposition) error {
	verified, err := store.scope.floor.validator.ValidateTerminalDispositionEnvelope(disposition)
	if err != nil {
		return err
	}
	if err := store.scope.matchExecutionFloorScope(verified.Disposition); err != nil {
		return err
	}
	state := store.state
	if disposition.GetCutoff() <= state.Floor {
		return errors.New("ModelRuntime execution floor cannot regress")
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(disposition)
	if err != nil {
		return err
	}
	state.Floor, state.Disposition = disposition.GetCutoff(), wire
	return store.replace(state)
}

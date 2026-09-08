package modelruntime

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

// These test-only controls bypass Service validation to exercise the durable
// owner's boundary using the signed fixtures in the external test package.
type ExecutionMutationForTest struct {
	Kind         string
	Authority    stageauthority.Verified
	Confirmed    *stageauthority.Verified
	Disposition  *velav1.StageTerminalDisposition
	Receipt      *velav1.LocalMaterializationReceipt
	Health       *FailureEvidence
	Drain        BackendDrain
	Observed     time.Time
	RouteIndex   int
	AllocationID string
	Manifest     LaunchManifest
	Incarnation  uuid.UUID
}

func ApplyExecutionMutationForTest(supervisor *Supervisor, mutation ExecutionMutationForTest) error {
	admission := supervisor.admission
	admission.mu.Lock()
	defer admission.mu.Unlock()
	store := admission.store
	switch mutation.Kind {
	case "admit":
		return store.saveHighest(mutation.Authority.Authority, supervisor.services[0].maxClockSkew)
	case "floor":
		return store.saveFloor(mutation.Disposition)
	case "candidates":
		return store.saveCandidates(mutation.Authority, mutation.Confirmed)
	case "seal":
		return store.saveSeal(mutation.Authority, mutation.Receipt)
	case "health":
		return store.saveHealth(mutation.Authority, mutation.Health)
	case "drain":
		return store.saveDrain(mutation.Authority, mutation.Drain, mutation.Observed)
	case "non-admission":
		return store.saveNonAdmission(mutation.Authority, supervisor.services[mutation.RouteIndex].journalRoute(), mutation.Observed)
	case "terminal-non-admission":
		return store.saveTerminalNonAdmission(mutation.Disposition, mutation.AllocationID, supervisor.services[mutation.RouteIndex].journalRoute(), mutation.Observed)
	case "startup", "abort-startup", "abort-non-admission", "abort-terminal-non-admission":
		return store.transition(func(draft *executionJournalDraft) error {
			var err error
			switch mutation.Kind {
			case "abort-non-admission":
				err = draft.recordNonAdmission(mutation.Authority, supervisor.services[mutation.RouteIndex].journalRoute(), mutation.Observed)
			case "abort-terminal-non-admission":
				err = draft.recordTerminalNonAdmission(mutation.Disposition, mutation.AllocationID, supervisor.services[mutation.RouteIndex].journalRoute(), mutation.Observed)
			default:
				err = draft.recordBackendStartup(mutation.Manifest, mutation.Incarnation, mutation.Observed)
			}
			if err != nil || mutation.Kind == "startup" {
				return err
			}
			return errors.New("injected abort after candidate construction")
		})
	case "revalidate":
		return store.persist(store.state)
	case "invalid-repeated-authority":
		return store.transition(func(draft *executionJournalDraft) error {
			if err := draft.recordCandidates(mutation.Authority, mutation.Confirmed); err != nil {
				return err
			}
			// Original and accepted bytes already validate. The corrupt confirmed
			// bytes must not inherit their proof merely by naming the same execution.
			candidates := draft.state.Executions[0].Candidates
			candidates.Confirmed = bytes.Clone(candidates.Confirmed)
			candidates.Confirmed[len(candidates.Confirmed)-1] ^= 1
			return nil
		})
	case "invalid-history", "abort-candidates":
		return store.transition(func(draft *executionJournalDraft) error {
			if err := draft.recordCandidates(mutation.Authority, mutation.Confirmed); err != nil {
				return err
			}
			if mutation.Kind == "abort-candidates" {
				return errors.New("injected abort after candidate construction")
			}
			draft.state.Highest++ // Invalid witness discovered only by full validation.
			return nil
		})
	case "invalid-schema", "invalid-id", "invalid-scope", "invalid-root", "invalid-lock", "invalid-proof":
		next := store.state
		switch mutation.Kind {
		case "invalid-schema":
			next.SchemaVersion--
		case "invalid-id":
			next.ID[0] ^= 1
		case "invalid-scope":
			next.Scope[0] ^= 1
		case "invalid-root":
			next.Root.Inode++
		case "invalid-lock":
			next.Lock.Inode++
		case "invalid-proof":
			next.Highest++
		}
		return store.persist(next)
	default:
		return errors.New("unknown test mutation")
	}
}

func ExecutionJournalDocumentForTest(supervisor *Supervisor) ([]byte, error) {
	admission := supervisor.admission
	admission.mu.Lock()
	defer admission.mu.Unlock()
	return json.Marshal(admission.store.state)
}

func SetExecutionJournalValidatorForTest(supervisor *Supervisor, validator *stageauthority.Validator) func() {
	admission := supervisor.admission
	admission.mu.Lock()
	previous := admission.store.scope.floor.validator
	admission.store.scope.floor.validator = validator
	admission.mu.Unlock()
	return func() {
		admission.mu.Lock()
		defer admission.mu.Unlock()
		admission.store.scope.floor.validator = previous
	}
}

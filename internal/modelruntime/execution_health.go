package modelruntime

import (
	"bytes"
	"errors"
	"slices"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrWorkerHealthUnproven = errors.New("ModelRuntime Worker health is denied or its legacy history is unknown")

// Each allocation retains its own latest explicit backend health assertion.
// A response from another resident profile cannot clear this allocation's denial.
// STOPPED, writer drain and process replacement carry no health assertion.
type executionDiskHealth struct {
	Authority []byte `json:"authority"`
	Evidence  []byte `json:"evidence"`
}

func failureEvidenceProto(evidence *FailureEvidence) *velav1.ModelRuntimeFailureEvidence {
	return &velav1.ModelRuntimeFailureEvidence{
		FailureClass: evidence.FailureClass, FailureFingerprint: bytes.Clone(evidence.FailureFingerprint),
		Detail: evidence.Detail, WorkerReusable: evidence.WorkerReusable, ConsumedResourceUnits: evidence.ConsumedResourceUnits,
		FailedAt: timestamppb.New(evidence.FailedAt.UTC()), RetryAt: timestamppb.New(evidence.RetryAt.UTC()),
	}
}

func decodeHealthEvidence(wire []byte) (*velav1.ModelRuntimeFailureEvidence, error) {
	if len(wire) == 0 || len(wire) > maxExecutionWireBytes {
		return nil, errors.New("worker health evidence exceeds its persistence bound")
	}
	evidence := &velav1.ModelRuntimeFailureEvidence{}
	if err := proto.Unmarshal(wire, evidence); err != nil {
		return nil, err
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(evidence)
	if err != nil || !bytes.Equal(wire, canonical) || len(evidence.ProtoReflect().GetUnknown()) != 0 ||
		evidence.GetFailedAt() == nil || evidence.GetRetryAt() == nil ||
		evidence.GetFailedAt().CheckValid() != nil || evidence.GetRetryAt().CheckValid() != nil ||
		len(evidence.GetFailedAt().ProtoReflect().GetUnknown()) != 0 || len(evidence.GetRetryAt().ProtoReflect().GetUnknown()) != 0 {
		return nil, errors.New("worker health evidence is not canonical bounded failure evidence")
	}
	if err := validateFailureEvidence(&FailureEvidence{FailureClass: evidence.GetFailureClass(), FailureFingerprint: evidence.GetFailureFingerprint(),
		Detail: evidence.GetDetail(), WorkerReusable: evidence.GetWorkerReusable(), ConsumedResourceUnits: evidence.GetConsumedResourceUnits(),
		FailedAt: evidence.GetFailedAt().AsTime(), RetryAt: evidence.GetRetryAt().AsTime()}); err != nil {
		return nil, err
	}
	return evidence, nil
}

func (journal *executionJournal) validateHealth(record retainedExecution) error {
	if record.Health == nil {
		return nil // No assertion was recorded; absence is not a positive health test.
	}
	if journal.state.SchemaVersion < 8 || record.Candidates == nil || len(record.Candidates.Confirmed) == 0 {
		return errors.New("worker health evidence has no confirmed execution history")
	}
	original, err := journal.retainedAuthority(record.Authority)
	if err != nil {
		return err
	}
	health, err := journal.retainedAuthority(record.Health.Authority)
	if err != nil {
		return err
	}
	confirmed, err := journal.retainedAuthority(record.Candidates.Confirmed)
	if err != nil {
		return err
	}
	if original.Digest != health.Digest && stageauthority.ValidateRenewal(original.Authority, health.Authority) != nil ||
		health.Digest != confirmed.Digest && stageauthority.ValidateRenewal(health.Authority, confirmed.Authority) != nil {
		return errors.New("worker health authority is outside confirmed execution history")
	}
	_, err = decodeHealthEvidence(record.Health.Evidence)
	return err
}

func (journal *executionJournal) workerHealthDenied() bool {
	for _, record := range journal.state.Executions {
		if record.Health != nil {
			evidence, err := decodeHealthEvidence(record.Health.Evidence)
			if err != nil || !evidence.GetWorkerReusable() {
				return true
			}
		}
	}
	return false
}

func (journal *executionJournal) workerHealthError() error {
	if journal.state.HealthHistoryUnknown || journal.workerHealthDenied() {
		return ErrWorkerHealthUnproven
	}
	return nil
}

// Called under the admission mutex after exact backend authority confirmation,
// before a successful Status response or live health clearance becomes visible.
func (store *executionStateFile) saveHealth(verified stageauthority.Verified, evidence *FailureEvidence) error {
	return store.transition(func(draft *executionJournalDraft) error { return draft.recordHealth(verified, evidence) })
}

func (store *executionJournalDraft) recordHealth(verified stageauthority.Verified, evidence *FailureEvidence) error {
	if evidence == nil {
		return errors.New("worker health mutation requires explicit failure evidence")
	}
	verified, err := store.verifyMutationAuthority(verified)
	if err != nil {
		return err
	}
	index, err := store.retainedExecutionIndex(verified)
	if err != nil {
		return err
	}
	authority, err := proto.MarshalOptions{Deterministic: true}.Marshal(verified.Authority)
	if err != nil {
		return err
	}
	record := store.state.Executions[index]
	if record.Candidates == nil || !bytes.Equal(authority, record.Candidates.Accepted) || !bytes.Equal(authority, record.Candidates.Confirmed) {
		return errors.New("worker health update requires exact confirmed backend authority")
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(failureEvidenceProto(evidence))
	if err != nil {
		return err
	}
	if record.Health != nil && bytes.Equal(record.Health.Authority, authority) && bytes.Equal(record.Health.Evidence, wire) {
		return nil
	}
	record.Health = &executionDiskHealth{Authority: authority, Evidence: wire}
	if err := store.validateHealth(record); err != nil {
		return err
	}
	next := store.state
	next.Executions = slices.Clone(next.Executions)
	next.Executions[index] = record
	return store.replace(next)
}

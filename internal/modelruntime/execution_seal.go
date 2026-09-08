package modelruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// A seal is recorded before drain. Only a matching durable drain makes it
// replayable to the Worker; the record alone does not establish writer exclusion.
type executionDiskSeal struct {
	Authority []byte `json:"authority"`
	Receipt   []byte `json:"receipt"`
}

func decodeSealedReceipt(wire []byte, authority *velav1.StageAuthority) (*velav1.LocalMaterializationReceipt, error) {
	if len(wire) == 0 || len(wire) > maxSealedOutputBytes+256 {
		return nil, errors.New("sealed receipt exceeds its persistence bound")
	}
	receipt := &velav1.LocalMaterializationReceipt{}
	if err := proto.Unmarshal(wire, receipt); err != nil {
		return nil, err
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(receipt)
	if err != nil || !bytes.Equal(wire, canonical) || len(receipt.ProtoReflect().GetUnknown()) != 0 ||
		len(receipt.GetOutputManifestJson()) == 0 || len(receipt.GetOutputManifestJson()) > maxSealedOutputBytes ||
		receipt.GetTotalSizeBytes() < 0 || receipt.GetSealedAt() == nil || receipt.GetSealedAt().CheckValid() != nil ||
		len(receipt.GetSealedAt().ProtoReflect().GetUnknown()) != 0 {
		return nil, errors.New("sealed receipt is not canonical bounded output evidence")
	}
	digest := sha256.Sum256(receipt.GetOutputManifestJson())
	id := uuid.NewSHA1(uuid.NameSpaceOID, append([]byte(authority.GetStageLeaseId()+"\x00"), digest[:]...))
	if !bytes.Equal(receipt.GetManifestSha256(), digest[:]) || receipt.GetReceiptId() != id.String() {
		return nil, errors.New("sealed receipt identity or manifest digest changed")
	}
	return receipt, nil
}

func (journal *executionJournal) validateSeal(record retainedExecution) error {
	if record.Seal == nil {
		return nil // Absence is not evidence that a legacy execution never sealed.
	}
	if journal.state.SchemaVersion < 7 || record.Candidates == nil ||
		!bytes.Equal(record.Seal.Authority, record.Candidates.Accepted) ||
		!bytes.Equal(record.Seal.Authority, record.Candidates.Confirmed) {
		return errors.New("sealed receipt is outside confirmed execution history")
	}
	verified, err := journal.retainedAuthority(record.Seal.Authority)
	if err != nil {
		return err
	}
	if _, err := decodeSealedReceipt(record.Seal.Receipt, verified.Authority); err != nil {
		return err
	}
	if record.Drain != nil && !bytes.Equal(record.Drain.Authority, record.Seal.Authority) {
		return errors.New("sealed receipt and writer drain name different authorities")
	}
	return nil
}

func (store *executionStateFile) saveSeal(verified stageauthority.Verified, receipt *velav1.LocalMaterializationReceipt) error {
	return store.transition(func(draft *executionJournalDraft) error { return draft.recordSeal(verified, receipt) })
}

func (store *executionJournalDraft) recordSeal(verified stageauthority.Verified, receipt *velav1.LocalMaterializationReceipt) error {
	verified, err := store.verifyMutationAuthority(verified)
	if err != nil {
		return err
	}
	index, err := store.retainedExecutionIndex(verified)
	if err != nil {
		return err
	}
	previous := store.state.Executions[index]
	authority, err := proto.MarshalOptions{Deterministic: true}.Marshal(verified.Authority)
	if err != nil {
		return err
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(receipt)
	if err != nil {
		return err
	}
	if previous.Seal != nil {
		if bytes.Equal(previous.Seal.Authority, authority) && bytes.Equal(previous.Seal.Receipt, wire) {
			return nil
		}
		return errors.New("sealed receipt is immutable")
	}
	if previous.Drain != nil {
		return errors.New("drained execution cannot acquire a new sealed receipt")
	}
	next := store.state
	next.Executions = slices.Clone(next.Executions)
	next.Executions[index].Seal = &executionDiskSeal{Authority: authority, Receipt: wire}
	if err := store.validateSeal(next.Executions[index]); err != nil {
		return err
	}
	return store.replace(next)
}

func (service *Service) checkpointSealedReceipt(verified stageauthority.Verified, receipt *velav1.LocalMaterializationReceipt) error {
	admission := service.executionAdmission()
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if err := admission.checkStateLocked(); err != nil {
		return err
	}
	if admission.store == nil {
		return nil
	}
	if err := admission.store.saveSeal(verified, receipt); err != nil {
		return admission.failStateLocked(err)
	}
	return nil
}

// replaySealedOutput returns only a previously committed receipt with matching
// drain. It validates historical authority but never extends its lifetime,
// enters a backend, publishes an artifact or proves that output bytes still exist.
func (supervisor *Supervisor) replaySealedOutput(ctx context.Context, authority *velav1.StageAuthority) (*velav1.ModelRuntimeServiceSealOutputResponse, error) {
	if supervisor == nil || supervisor.admission == nil || supervisor.floor == nil {
		return nil, nil
	}
	if ctx == nil {
		return nil, errors.New("sealed receipt replay requires context")
	}
	admission := supervisor.admission
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := admission.checkStateLocked(); err != nil {
		return nil, errors.Join(ErrExecutionStateRecovery, err)
	}
	if admission.store == nil {
		return nil, nil
	}
	verified, err := supervisor.floor.validator.ValidateEnvelopeForReplay(authority, supervisor.services[0].maxClockSkew)
	if err != nil {
		return nil, err
	}
	store := admission.store
	if err := store.view().scope.matchRetainedExecutionScope(verified.Authority); err != nil {
		return nil, err
	}
	for _, record := range store.view().state.Executions {
		if record.Seal == nil || record.Drain == nil || record.Drain.Result.AuthorityDigest != verified.Digest {
			continue
		}
		receipt, err := decodeSealedReceipt(record.Seal.Receipt, verified.Authority)
		if err != nil {
			return nil, err
		}
		binding := cloneBinding(store.view().scope.binding)
		binding.ModelResidencyID, binding.ModelRuntimeIdentity = verified.Authority.GetModelResidencyId(), verified.Authority.GetModelRuntimeIdentity()
		binding.StageProfileRevisionID = verified.Authority.GetStageProfileRevisionId()
		for _, member := range verified.Authority.GetMembers() {
			if member.GetWorkerMemberId() == binding.WorkerMemberID {
				binding.ModelRuntimeEpoch = member.GetModelRuntimeEpoch()
			}
		}
		return &velav1.ModelRuntimeServiceSealOutputResponse{AuthorityDigest: bytes.Clone(verified.Digest[:]),
			Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED,
			State:    velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED, Receipt: receipt,
			RuntimeIdentity: runtimeIdentityProto(binding), Detail: "durable sealed output receipt replayed; local source must be revalidated"}, nil
	}
	return nil, nil
}

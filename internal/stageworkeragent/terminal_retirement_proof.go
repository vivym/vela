package stageworkeragent

import (
	"bytes"
	"crypto/sha256"

	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func (gate *FileAssignmentAdmission) validateRetirementProofs(value *velav1.StageTerminalDisposition, entry terminalRetirementEntry) error {
	verified, err := gate.validator.ValidateTerminalDispositionForReplay(value)
	if err != nil {
		return err
	}
	allocations := value.GetAllocations()
	members := allocations[0].GetMembers()
	if len(entry.Floors) != len(members) || len(entry.Proofs) != len(allocations)*len(members) {
		return ErrScratchRetirementUnproven
	}
	for index, member := range members {
		ack := &velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse{}
		if err := readRetirementWire(entry.Floors[index], ack); err != nil {
			return err
		}
		if err := gate.validateRetirementReader(value, allocations[0], member.GetWorkerMemberId(), ack.GetIdentity()); err != nil {
			return err
		}
		if err := modelruntimetransport.ValidateExecutionFloorAcknowledgementForVersion(ack.GetSchemaVersion(), ack.GetIdentity(), verified.Digest, value.GetCutoff(), ack); err != nil {
			return err
		}
	}
	for index, allocation := range allocations {
		for memberIndex, member := range allocation.GetMembers() {
			stored := entry.Proofs[index*len(members)+memberIndex]
			if err := gate.validateRetirementMemberProof(value, verified.Digest, allocation, member.GetWorkerMemberId(), stored); err != nil {
				return err
			}
		}
	}
	return nil
}

func (gate *FileAssignmentAdmission) validateRetirementReader(value *velav1.StageTerminalDisposition, allocation *velav1.StageTerminalAllocation, memberID string, identity *velav1.ModelRuntimeIdentity) error {
	if identity.GetWorkerMemberId() != memberID {
		return ErrScratchRetirementUnproven
	}
	_, err := modelruntimetransport.ValidateTerminalAllocationScope(gate.validator, &velav1.ModelRuntimeTerminalAllocationScope{
		SchemaVersion: 1, Identity: identity, Disposition: value, StageAllocationId: allocation.GetStageAllocationId(),
	}, true)
	return err
}

func (gate *FileAssignmentAdmission) validateRetirementMemberProof(value *velav1.StageTerminalDisposition, dispositionDigest [sha256.Size]byte, allocation *velav1.StageTerminalAllocation, memberID string, stored terminalRetirementProof) error {
	kinds := 0
	for _, wire := range [][]byte{stored.Drain, stored.NeverAdmitted, stored.TerminalAbsence} {
		if len(wire) != 0 {
			kinds++
		}
	}
	if kinds != 1 {
		return ErrScratchRetirementUnproven
	}
	var query *velav1.StageAuthority
	var queryDigest [sha256.Size]byte
	if len(stored.Authority) != 0 {
		var err error
		query, err = gate.decodeAuthority(stored.Authority)
		if err != nil {
			return err
		}
		if err := stageauthority.ValidateTerminalAllocation(value, allocation, query); err != nil {
			return err
		}
		queryDigest, err = stageauthority.Digest(query)
		if err != nil || allocation.GetStageAllocationId() == value.GetStageAllocationId() && !bytes.Equal(queryDigest[:], value.GetOriginalAuthorityDigest()) {
			return ErrScratchRetirementUnproven
		}
	}
	if len(stored.TerminalAbsence) > 0 {
		result := &velav1.ModelRuntimeTerminalNonAdmissionResult{}
		if err := readRetirementWire(stored.TerminalAbsence, result); err != nil {
			return err
		}
		if err := gate.validateRetirementReader(value, allocation, memberID, result.GetIdentity()); err != nil {
			return err
		}
		scope := &velav1.ModelRuntimeTerminalAllocationScope{SchemaVersion: 1, Identity: result.GetIdentity(), Disposition: value, StageAllocationId: allocation.GetStageAllocationId()}
		if err := modelruntimetransport.ValidateTerminalNonAdmissionResult(gate.validator, scope, dispositionDigest, result); err != nil {
			return err
		}
		if result.GetCheckpoint() == nil {
			return ErrScratchRetirementUnproven
		}
		return nil
	}
	if query == nil {
		return ErrScratchRetirementUnproven
	}
	scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Authority: query}
	if len(stored.Drain) > 0 {
		result := &velav1.ModelRuntimeExecutionDrainResult{}
		if err := readRetirementWire(stored.Drain, result); err != nil {
			return err
		}
		scope.Identity = result.GetIdentity()
		if err := gate.validateRetirementReader(value, allocation, memberID, scope.Identity); err != nil {
			return err
		}
		if err := modelruntimetransport.ValidateAllocationDrainResult(gate.validator, scope, queryDigest, result); err != nil {
			return err
		}
		if result.GetCheckpoint() == nil {
			return ErrScratchRetirementUnproven
		}
	} else {
		result := &velav1.ModelRuntimeExecutionNonAdmissionResult{}
		if err := readRetirementWire(stored.NeverAdmitted, result); err != nil {
			return err
		}
		scope.Identity = result.GetIdentity()
		if err := gate.validateRetirementReader(value, allocation, memberID, scope.Identity); err != nil {
			return err
		}
		if err := modelruntimetransport.ValidateExecutionNonAdmissionResult(gate.validator, scope, queryDigest, result); err != nil {
			return err
		}
		if result.GetCheckpoint() == nil {
			return ErrScratchRetirementUnproven
		}
	}
	// Result validators bind checkpoints; this also checks the query scope shape.
	_, err := modelruntimetransport.ValidateExecutionDrainScope(gate.validator, scope, 0, true)
	return err
}

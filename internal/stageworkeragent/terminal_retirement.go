package stageworkeragent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

type TerminalRetirementPhase string

const (
	TerminalRetirementIntent  TerminalRetirementPhase = "INTENT"
	TerminalRetirementReady   TerminalRetirementPhase = "READY"
	TerminalRetirementRetired TerminalRetirementPhase = "RETIRED"
)

type TerminalRetirementSnapshot struct {
	StageRunID uuid.UUID
	Phase      TerminalRetirementPhase
	Cutoff     int64
}

type terminalRetirementEntry struct {
	StageRunID  uuid.UUID                     `json:"stage_run_id"`
	Phase       TerminalRetirementPhase       `json:"phase"`
	Cutoff      int64                         `json:"cutoff"`
	Disposition []byte                        `json:"disposition"`
	Floors      [][]byte                      `json:"runtime_floors,omitempty"`
	Proofs      []terminalRetirementProof     `json:"execution_proofs,omitempty"`
	Directories []terminalRetirementDirectory `json:"directories,omitempty"`
}

type terminalRetirementProof struct {
	Authority       []byte `json:"query_authority,omitempty"`
	Drain           []byte `json:"drain,omitempty"`
	NeverAdmitted   []byte `json:"never_admitted,omitempty"`
	TerminalAbsence []byte `json:"terminal_absence,omitempty"`
}

// TerminalScratchRetirement combines the existing admission, Runtime and bound
// filesystem protocols. It is explicitly constructed; default assembly retains.
type TerminalScratchRetirement struct {
	gate    *FileAssignmentAdmission
	runtime *Agent
}

func NewTerminalScratchRetirement(gate *FileAssignmentAdmission, runtime *Agent, outputOwnershipContract string) (*TerminalScratchRetirement, error) {
	if gate == nil || runtime == nil || runtime.floor == nil || outputOwnershipContract != AttemptOwnedFilesystemScratchV1 {
		return nil, ErrScratchRetirementUnproven
	}
	return &TerminalScratchRetirement{gate: gate, runtime: runtime}, nil
}

// Retire persists local exclusion before Runtime calls. A failed call leaves an
// intent or a fully proven READY record, never a deletion permission inferred
// from a partial collection. Caller paths and unverified completion flags are
// not accepted. Every output directory is derived from signed allocation history.
func (retirer *TerminalScratchRetirement) Retire(ctx context.Context, disposition *velav1.StageTerminalDisposition, authorities map[string]*velav1.StageAuthority, targets map[string]*velav1.ModelRuntimeIdentity) (TerminalRetirementSnapshot, error) {
	if retirer == nil || ctx == nil {
		return TerminalRetirementSnapshot{}, ErrScratchRetirementUnproven
	}
	ctx, cancel := context.WithTimeout(ctx, retirer.runtime.floor.timeout)
	defer cancel()
	history, err := retirer.runtime.terminalExecutionQueries(ctx, disposition, authorities, targets, true)
	if err != nil {
		return TerminalRetirementSnapshot{}, err
	}
	if _, err := retirer.runtime.recoveryExecutionFloorTargets(history.verified.Disposition, history.readers); err != nil {
		return TerminalRetirementSnapshot{}, err
	}
	value := history.verified.Disposition
	snapshot, err := retirer.gate.beginTerminalRetirement(ctx, value)
	if err != nil {
		return snapshot, err
	}
	if snapshot.Phase != TerminalRetirementIntent {
		return retirer.Resume(ctx, snapshot.StageRunID)
	}
	if err := retirer.gate.waitInputWriters(ctx, value.GetCutoff()); err != nil {
		return snapshot, err
	}
	floors, err := retirer.runtime.InstallRecoveryExecutionFloor(ctx, value, history.readers)
	if err != nil {
		return snapshot, err
	}
	excluded, err := retirer.runtime.CheckpointTerminalExecutionExclusions(ctx, value, history.authorities, history.readers)
	if err != nil {
		return snapshot, err
	}
	if err := retirer.gate.checkpointTerminalRetirement(ctx, value, history.authorities, floors, excluded); err != nil {
		return snapshot, err
	}
	return retirer.Resume(ctx, snapshot.StageRunID)
}

// Resume uses only an already durable complete proof. Expiry and retired Runtime
// profiles cannot invalidate that past checkpoint. INTENT still needs fresh
// Control history and successful collection through Retire.
func (retirer *TerminalScratchRetirement) Resume(ctx context.Context, stageRunID uuid.UUID) (TerminalRetirementSnapshot, error) {
	if retirer == nil || ctx == nil || stageRunID == uuid.Nil {
		return TerminalRetirementSnapshot{}, ErrScratchRetirementUnproven
	}
	gate := retirer.gate
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return TerminalRetirementSnapshot{}, err
	}
	index := retirementIndex(gate.state, stageRunID)
	if index < 0 {
		return TerminalRetirementSnapshot{}, ErrScratchRetirementUnproven
	}
	entry := gate.state.Retirements[index]
	snapshot := retirementSnapshot(entry)
	if entry.Phase == TerminalRetirementRetired {
		return snapshot, nil
	}
	if entry.Phase != TerminalRetirementReady {
		return snapshot, ErrScratchRetirementUnproven
	}
	if err := gate.validateRetirements(gate.state); err != nil {
		return snapshot, err
	}
	if err := gate.retireTerminalDirectories(ctx, entry.Directories); err != nil {
		return snapshot, err
	}
	next := cloneAdmissionState(gate.state)
	next.Retirements[index].Phase = TerminalRetirementRetired
	if next.Latest != nil {
		authority, err := gate.decodeAuthority(next.Latest.OriginalWire)
		if err != nil {
			return snapshot, err
		}
		if authority.GetStageRunId() == stageRunID.String() {
			next.Latest.Phase = AssignmentClosed
		}
	}
	if err := gate.commit(ctx, next); err != nil {
		return snapshot, err
	}
	if err := ctx.Err(); err != nil {
		return snapshot, err
	}
	return retirementSnapshot(next.Retirements[index]), nil
}

func (gate *FileAssignmentAdmission) beginTerminalRetirement(ctx context.Context, disposition *velav1.StageTerminalDisposition) (TerminalRetirementSnapshot, error) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return TerminalRetirementSnapshot{}, err
	}
	verified, err := gate.validator.ValidateTerminalDispositionEnvelope(disposition)
	if err != nil {
		return TerminalRetirementSnapshot{}, err
	}
	if err := gate.matchFloor(verified.Disposition, gate.state, false); err != nil {
		return TerminalRetirementSnapshot{}, err
	}
	value := verified.Disposition
	id := uuid.MustParse(value.GetStageRunId())
	index := retirementIndex(gate.state, id)
	if index >= 0 {
		previous := gate.state.Retirements[index]
		old, err := gate.retirementDisposition(previous)
		if err != nil || !sameRetirementHistory(old, value) {
			return TerminalRetirementSnapshot{}, ErrScratchRetirementUnproven
		}
		if previous.Phase != TerminalRetirementIntent {
			return retirementSnapshot(previous), nil
		}
	} else if len(gate.state.Retirements) >= gate.state.MaxRecords {
		return TerminalRetirementSnapshot{}, ErrAdmissionCapacity
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(value)
	if err != nil || len(wire) > maxAdmissionFloorBytes {
		return TerminalRetirementSnapshot{}, ErrScratchRetirementUnproven
	}
	next := cloneAdmissionState(gate.state)
	if value.GetCutoff() > next.Floor {
		next.Floor, next.FloorWire = value.GetCutoff(), bytes.Clone(wire)
	}
	entry := terminalRetirementEntry{StageRunID: id, Phase: TerminalRetirementIntent, Cutoff: value.GetCutoff(), Disposition: wire}
	if index >= 0 {
		next.Retirements[index] = entry
	} else {
		next.Retirements = append(next.Retirements, entry)
		slices.SortFunc(next.Retirements, func(a, b terminalRetirementEntry) int { return bytes.Compare(a.StageRunID[:], b.StageRunID[:]) })
	}
	if err := gate.commit(ctx, next); err != nil {
		return TerminalRetirementSnapshot{}, err
	}
	return retirementSnapshot(entry), ctx.Err()
}

func (gate *FileAssignmentAdmission) checkpointTerminalRetirement(ctx context.Context, disposition *velav1.StageTerminalDisposition, authorities map[string]*velav1.StageAuthority, floors ExecutionFloorResult, exclusions TerminalExecutionExclusionResult) error {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return err
	}
	verified, err := gate.validator.ValidateTerminalDispositionEnvelope(disposition)
	if err != nil {
		return err
	}
	index := retirementIndex(gate.state, uuid.MustParse(disposition.GetStageRunId()))
	if index < 0 {
		return ErrScratchRetirementUnproven
	}
	entry := gate.state.Retirements[index]
	previous, err := gate.retirementDisposition(entry)
	if err != nil || !sameRetirementHistory(previous, disposition) {
		return ErrScratchRetirementUnproven
	}
	if entry.Phase != TerminalRetirementIntent {
		return nil
	}
	if pending, err := gate.pendingInputWriter(ctx, disposition.GetCutoff()); err != nil || pending != nil {
		return errors.Join(ErrInputWritersUnproven, err)
	}
	if !floors.AllInstalled || !exclusions.AllExcluded || floors.DispositionDigest != verified.Digest || exclusions.DispositionDigest != verified.Digest ||
		floors.Cutoff != entry.Cutoff || exclusions.Cutoff != entry.Cutoff || exclusions.RequiredAllocations != len(disposition.GetAllocations()) ||
		floors.RequiredMembers != len(disposition.GetAllocations()[0].GetMembers()) || exclusions.RequiredMembers != floors.RequiredMembers ||
		len(floors.Acknowledgements) != floors.RequiredMembers || len(exclusions.Allocations) != exclusions.RequiredAllocations {
		return ErrScratchRetirementUnproven
	}
	entry.Disposition, err = proto.MarshalOptions{Deterministic: true}.Marshal(disposition)
	if err != nil {
		return err
	}
	for _, member := range disposition.GetAllocations()[0].GetMembers() {
		wire, err := retirementWire(floors.Acknowledgements[member.GetWorkerMemberId()])
		if err != nil {
			return err
		}
		entry.Floors = append(entry.Floors, wire)
	}
	for _, allocation := range disposition.GetAllocations() {
		members := exclusions.Allocations[allocation.GetStageAllocationId()]
		if len(members) != exclusions.RequiredMembers {
			return ErrScratchRetirementUnproven
		}
		for _, member := range allocation.GetMembers() {
			proof, ok := members[member.GetWorkerMemberId()]
			if !ok {
				return ErrScratchRetirementUnproven
			}
			stored := terminalRetirementProof{}
			for _, field := range []struct {
				message proto.Message
				target  *[]byte
			}{
				{authorities[allocation.GetStageAllocationId()], &stored.Authority}, {proof.Drain, &stored.Drain},
				{proof.NeverAdmitted, &stored.NeverAdmitted}, {proof.TerminalNeverAdmitted, &stored.TerminalAbsence},
			} {
				if field.message.ProtoReflect().IsValid() {
					*field.target, err = retirementWire(field.message)
					if err != nil {
						return err
					}
				}
			}
			entry.Proofs = append(entry.Proofs, stored)
		}
	}
	entry.Directories, err = gate.bindTerminalDirectories(disposition)
	if err != nil {
		return err
	}
	entry.Phase = TerminalRetirementReady
	next := cloneAdmissionState(gate.state)
	next.Retirements[index] = entry
	if err := gate.commit(ctx, next); err != nil {
		return err
	}
	return ctx.Err()
}

func retirementSnapshot(entry terminalRetirementEntry) TerminalRetirementSnapshot {
	return TerminalRetirementSnapshot{StageRunID: entry.StageRunID, Phase: entry.Phase, Cutoff: entry.Cutoff}
}

func retirementIndex(state assignmentAdmissionState, stageRunID uuid.UUID) int {
	return slices.IndexFunc(state.Retirements, func(entry terminalRetirementEntry) bool { return entry.StageRunID == stageRunID })
}

func sameRetirementHistory(a, b *velav1.StageTerminalDisposition) bool {
	if a == nil || b == nil || a.GetCutoff() != b.GetCutoff() || len(a.GetAllocations()) != len(b.GetAllocations()) {
		return false
	}
	for _, allocation := range b.GetAllocations() {
		if stageauthority.ValidateSameTerminalAllocation(a, b, allocation.GetStageAllocationId()) != nil {
			return false
		}
	}
	return true
}

func retirementWire(message proto.Message) ([]byte, error) {
	if message == nil || !message.ProtoReflect().IsValid() || proto.Size(message) > maxAdmissionFloorBytes {
		return nil, ErrScratchRetirementUnproven
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(message)
}

func readRetirementWire(wire []byte, message proto.Message) error {
	if len(wire) == 0 || len(wire) > maxAdmissionFloorBytes || proto.Unmarshal(wire, message) != nil {
		return ErrScratchRetirementUnproven
	}
	canonical, err := retirementWire(message)
	if err != nil || !bytes.Equal(wire, canonical) {
		return ErrScratchRetirementUnproven
	}
	return nil
}

func (gate *FileAssignmentAdmission) retirementDisposition(entry terminalRetirementEntry) (*velav1.StageTerminalDisposition, error) {
	value := &velav1.StageTerminalDisposition{}
	if err := readRetirementWire(entry.Disposition, value); err != nil {
		return nil, err
	}
	verified, err := gate.validator.ValidateTerminalDispositionForReplay(value)
	if err != nil || !proto.Equal(verified.Disposition, value) || entry.StageRunID.String() != value.GetStageRunId() || entry.Cutoff != value.GetCutoff() {
		return nil, ErrScratchRetirementUnproven
	}
	return value, nil
}

func (gate *FileAssignmentAdmission) validateRetirements(state assignmentAdmissionState) error {
	if len(state.Retirements) > state.MaxRecords {
		return ErrAdmissionCapacity
	}
	var previous uuid.UUID
	for _, entry := range state.Retirements {
		if bytes.Compare(previous[:], entry.StageRunID[:]) >= 0 || entry.Cutoff > state.Floor {
			return ErrScratchRetirementUnproven
		}
		value, err := gate.retirementDisposition(entry)
		if err != nil {
			return err
		}
		if err := gate.matchFloor(value, state, false); err != nil {
			return err
		}
		switch entry.Phase {
		case TerminalRetirementIntent:
			if len(entry.Floors) != 0 || len(entry.Proofs) != 0 || len(entry.Directories) != 0 {
				return ErrScratchRetirementUnproven
			}
		case TerminalRetirementReady, TerminalRetirementRetired:
			if err := gate.validateRetirementProofs(value, entry); err != nil {
				return err
			}
			if err := validateRetirementDirectories(value, entry.Directories); err != nil {
				return err
			}
			for _, admitted := range admissionEntries(state) {
				authority, err := gate.decodeAuthority(admitted.OriginalWire)
				if err != nil {
					return err
				}
				if authority.GetExecutionSequence() <= entry.Cutoff && admitted.InputDrain == nil {
					return ErrInputWritersUnproven
				}
				if authority.GetStageRunId() == value.GetStageRunId() && stageauthority.ValidateTerminalAllocation(value, stageauthority.FindTerminalAllocation(value, authority.GetStageAllocationId()), authority) != nil {
					return fmt.Errorf("retirement omits or conflicts with admitted allocation: %w", ErrScratchRetirementUnproven)
				}
			}
		default:
			return ErrScratchRetirementUnproven
		}
		previous = entry.StageRunID
	}
	return nil
}

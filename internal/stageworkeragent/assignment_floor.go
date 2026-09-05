package stageworkeragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const maxAdmissionFloorBytes = 4 << 20

// WorkerExecutionFloorResult keeps the local input checkpoint separate from
// all-member Runtime installation. Neither result proves backend writer drain.
type WorkerExecutionFloorResult struct {
	Input    *AssignmentFloorInstallation
	Runtimes ExecutionFloorResult
}

// InstallExecutionFloor persists local exclusion before dispatching to Runtimes.
// Failure after that point leaves local admission closed and can be retried.
func (agent *StreamAgent) InstallExecutionFloor(ctx context.Context, disposition *velav1.StageTerminalDisposition) (WorkerExecutionFloorResult, error) {
	result := WorkerExecutionFloorResult{}
	if agent == nil || agent.admission == nil || agent.runtime == nil || agent.runtime.floor == nil || ctx == nil || disposition == nil {
		return result, errors.New("worker execution floor requires durable input and Runtime configuration")
	}
	disposition = proto.Clone(disposition).(*velav1.StageTerminalDisposition)
	var err error
	result.Input, err = agent.admission.InstallExecutionFloor(ctx, disposition)
	if err != nil {
		return result, err
	}
	result.Runtimes, err = agent.runtime.InstallExecutionFloor(ctx, disposition)
	return result, err
}

// AssignmentFloorInstallation is a durable input admission checkpoint only.
// WaitInputWriters separately requires persisted completion of every retained
// input invocation through its cutoff. Neither result proves Runtime drain.
type AssignmentFloorInstallation struct {
	Cutoff            int64
	DispositionDigest [sha256.Size]byte
	gate              *FileAssignmentAdmission
	cutoff            int64
}

func (installation *AssignmentFloorInstallation) WaitInputWriters(ctx context.Context) error {
	if installation == nil || installation.gate == nil || ctx == nil {
		return ErrAdmissionClosed
	}
	return installation.gate.waitInputWriters(ctx, installation.cutoff)
}

// InstallExecutionFloor closes Begin, EnterRuntime and renewal through the signed
// cutoff. It retains execution records and does not stop any backend or delete data.
func (gate *FileAssignmentAdmission) InstallExecutionFloor(ctx context.Context, disposition *velav1.StageTerminalDisposition) (*AssignmentFloorInstallation, error) {
	if gate == nil || ctx == nil {
		return nil, ErrAdmissionClosed
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return nil, err
	}
	verified, err := gate.validator.ValidateTerminalDispositionEnvelope(disposition)
	if err != nil {
		return nil, err
	}
	if err := gate.matchFloor(verified.Disposition, gate.state, true); err != nil {
		return nil, err
	}
	if verified.Disposition.GetCutoff() > gate.state.Floor {
		if proto.Size(verified.Disposition) > maxAdmissionFloorBytes {
			return nil, errors.New("assignment admission floor exceeds its bound")
		}
		wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(verified.Disposition)
		if err != nil {
			return nil, err
		}
		next := cloneAdmissionState(gate.state)
		next.Floor, next.FloorWire = verified.Disposition.GetCutoff(), wire
		if err := gate.commit(ctx, next); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	installation := &AssignmentFloorInstallation{Cutoff: gate.state.Floor, DispositionDigest: verified.Digest, gate: gate, cutoff: gate.state.Floor}
	return installation, nil
}

func (gate *FileAssignmentAdmission) decodeFloor(state assignmentAdmissionState) (*velav1.StageTerminalDisposition, error) {
	if len(state.FloorWire) == 0 || len(state.FloorWire) > maxAdmissionFloorBytes {
		return nil, ErrAdmissionClosed
	}
	value := &velav1.StageTerminalDisposition{}
	if err := proto.Unmarshal(state.FloorWire, value); err != nil {
		return nil, err
	}
	// Recovery restores a restriction; it grants no execution to historical routes.
	verified, err := gate.validator.ValidateTerminalDispositionSignature(value)
	if err != nil {
		return nil, err
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(verified.Disposition)
	if err != nil || !bytes.Equal(canonical, state.FloorWire) || verified.Disposition.GetCutoff() != state.Floor {
		return nil, errors.New("assignment admission floor witness does not match")
	}
	if err := gate.matchFloor(verified.Disposition, state, false); err != nil {
		return nil, err
	}
	return verified.Disposition, nil
}

func (gate *FileAssignmentAdmission) matchFloor(value *velav1.StageTerminalDisposition, state assignmentAdmissionState, currentRuntimes bool) error {
	if value.GetWorkerInstanceId() != state.WorkerInstanceID.String() || value.GetWorkerInstanceEpoch() != state.WorkerInstanceEpoch ||
		value.GetWorkerMemberId() != state.WorkerMemberID.String() {
		return stageauthority.ErrRuntimeMismatch
	}
	configured := make(map[string]bool)
	for _, binding := range gate.bindings {
		configured[binding.Runtime.WorkerMemberID] = true
	}
	allocations := value.GetAllocations()
	if !currentRuntimes {
		// Signature validation requires identical membership across the history.
		// Restoring a restriction needs this topology, not current Runtime routes.
		allocations = allocations[:1]
	}
	for _, allocation := range allocations {
		if len(allocation.GetMembers()) != len(configured) {
			return stageauthority.ErrRuntimeMismatch
		}
		for _, member := range allocation.GetMembers() {
			matches := 0
			for _, configured := range gate.bindings {
				binding := ExecutionFloorBinding(configured)
				if currentRuntimes && binding.matches(value, allocation, member) || !currentRuntimes && binding.matchesTopology(value, allocation, member) {
					matches++
				}
			}
			if matches == 0 || currentRuntimes && matches != 1 {
				return stageauthority.ErrRuntimeMismatch
			}
		}
	}
	return nil
}

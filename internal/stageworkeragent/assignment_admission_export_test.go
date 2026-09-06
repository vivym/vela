package stageworkeragent

import (
	"encoding/json"
	"errors"
	"os"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func LegacyAssignmentAdmissionStateForTest(document []byte, schema int) ([]byte, error) {
	var state assignmentAdmissionState
	if err := json.Unmarshal(document, &state); err != nil {
		return nil, err
	}
	state.SchemaVersion, state.Scope = schema, nil
	return json.Marshal(state)
}

func CopyAssignmentAdmissionScopeForTest(document, source []byte) ([]byte, error) {
	var state, other assignmentAdmissionState
	if err := json.Unmarshal(document, &state); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(source, &other); err != nil {
		return nil, err
	}
	state.Scope = other.Scope
	return json.Marshal(state)
}

// SetAssignmentAdmissionSyncHookForTest injects the post-Rename durability boundary.
func SetAssignmentAdmissionSyncHookForTest(gate *FileAssignmentAdmission, hook func(func() error) error) func() {
	gate.mu.Lock()
	original := gate.files.syncDirectory
	gate.files.syncDirectory = func(root *os.Root) error { return hook(func() error { return original(root) }) }
	gate.mu.Unlock()
	return func() {
		gate.mu.Lock()
		defer gate.mu.Unlock()
		gate.files.syncDirectory = original
	}
}

func SetTerminalRetirementDirectoryHookForTest(gate *FileAssignmentAdmission, hook func(int) error) func() {
	gate.mu.Lock()
	original := gate.retirementAfterDirectory
	gate.retirementAfterDirectory = hook
	gate.mu.Unlock()
	return func() {
		gate.mu.Lock()
		defer gate.mu.Unlock()
		gate.retirementAfterDirectory = original
	}
}

func SetTerminalRetirementAbsentParentSyncHookForTest(gate *FileAssignmentAdmission, hook func(string, func() error) error) func() {
	gate.mu.Lock()
	original := gate.retirementSyncAbsentParent
	gate.retirementSyncAbsentParent = func(root *os.Root) error {
		return hook(root.Name(), func() error { return syncScratchDirectory(root, ".") })
	}
	gate.mu.Unlock()
	return func() {
		gate.mu.Lock()
		defer gate.mu.Unlock()
		gate.retirementSyncAbsentParent = original
	}
}

func CorruptTerminalRetirementForTest(document []byte, fault string) ([]byte, error) {
	var state assignmentAdmissionState
	if err := json.Unmarshal(document, &state); err != nil {
		return nil, err
	}
	entry := &state.Retirements[0]
	switch fault {
	case "missing-floor":
		entry.Floors = nil
	case "swapped-floor":
		entry.Floors[0] = entry.Floors[1]
	case "missing-proof":
		entry.Proofs = entry.Proofs[1:]
	case "swapped-proof":
		entry.Proofs[0] = entry.Proofs[1]
	case "mixed-proof":
		entry.Proofs[0].Drain = entry.Proofs[0].TerminalAbsence
	case "signature", "member", "decision", "contract":
		result := &velav1.ModelRuntimeTerminalNonAdmissionResult{}
		if err := proto.Unmarshal(entry.Proofs[0].TerminalAbsence, result); err != nil {
			return nil, err
		}
		switch fault {
		case "signature":
			result.Checkpoint.Disposition.Signature[0] ^= 1
		case "member":
			result.Identity.WorkerMemberEpoch++
		case "decision":
			result.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		case "contract":
			result.Checkpoint.Contract = "STOPPED"
		}
		wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(result)
		if err != nil {
			return nil, err
		}
		entry.Proofs[0].TerminalAbsence = wire
	case "input-drain":
		state.Latest.InputDrain = nil
	case "directory-root":
		entry.Directories[0].Root = 0
	case "directory-path":
		entry.Directories[0].Path = "unrelated"
	case "directory-identity":
		entry.Directories[0].Components[0].Inode = 0
	case "phase":
		entry.Phase = "DRAINED"
	case "cutoff":
		entry.Cutoff++
	default:
		return nil, errors.New("unknown retirement corruption")
	}
	return json.Marshal(state)
}

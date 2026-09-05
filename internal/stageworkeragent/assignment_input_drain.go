package stageworkeragent

import (
	"context"
	"errors"
	"time"
)

const AssignmentInputDrainContract = "vela-assignment-input-writer-drain-v1"

var ErrInputWritersUnproven = errors.New("assignment input writer drain is unproven")

// AssignmentInputDrainCheckpoint belongs to its admission record's immutable
// execution identity. It covers input work only, not Runtime/output writers.
type AssignmentInputDrainCheckpoint struct {
	Contract   string    `json:"contract"`
	ObservedAt time.Time `json:"observed_at"`
}

// CompleteInputs persists the caller's assertion that all input tasks returned
// and their writable handles are closed. The caller must perform no more input
// work under this handle, even if execution has not yet entered Runtime.
// Release alone never establishes this checkpoint.
func (handle *AssignmentAdmission) CompleteInputs(ctx context.Context) error {
	if handle == nil || handle.gate == nil || ctx == nil {
		return ErrAdmissionClosed
	}
	gate := handle.gate
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return err
	}
	if gate.active != handle || gate.state.Latest == nil || gate.state.Latest.Identity != handle.identity {
		return ErrAdmissionClosed
	}
	if gate.state.Latest.InputDrain != nil {
		return nil
	}
	next := cloneAdmissionState(gate.state)
	next.Latest.InputDrain = &AssignmentInputDrainCheckpoint{Contract: AssignmentInputDrainContract, ObservedAt: time.Now().UTC()}
	if err := gate.commit(ctx, next); err != nil {
		return err
	}
	close(handle.inputsDone)
	return ctx.Err()
}

// A floor closes all future input entry through cutoff. Every retained record
// in that range must independently have completed its latest admitted input
// invocation. Missing process-local handles never replace durable evidence.
func (gate *FileAssignmentAdmission) waitInputWriters(ctx context.Context, cutoff int64) error {
	for {
		gate.mu.Lock()
		pending, err := gate.pendingInputWriter(ctx, cutoff)
		gate.mu.Unlock()
		if err != nil {
			return err
		}
		if pending == nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pending.inputsDone:
		case <-pending.done:
		}
	}
}

func (gate *FileAssignmentAdmission) pendingInputWriter(ctx context.Context, cutoff int64) (*AssignmentAdmission, error) {
	if err := gate.available(ctx); err != nil {
		return nil, err
	}
	if cutoff <= 0 || gate.state.Floor < cutoff {
		return nil, ErrAdmissionClosed
	}
	var pending *AssignmentAdmission
	for _, entry := range admissionEntries(gate.state) {
		authority, err := gate.decodeAuthority(entry.OriginalWire)
		if err != nil {
			return nil, err
		}
		if authority.GetExecutionSequence() > cutoff || entry.InputDrain != nil {
			continue
		}
		if gate.active == nil || gate.active.identity != entry.Identity {
			return nil, ErrInputWritersUnproven
		}
		pending = gate.active
	}
	return pending, nil
}

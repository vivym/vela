package modelruntime

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/strictjson"
)

// executionJournal contains only data and semantic validators. The live file
// store embeds it; snapshot verification has no filesystem or mutation methods.
type executionJournal struct {
	scope executionJournalScope
	state executionDiskState
}

// ExecutionJournalIdentity is an independent expectation, not data learned from
// the document being inspected. Its storage requires trusted Node provenance;
// UUID/scope alone cannot establish original journal ownership.
type ExecutionJournalIdentity struct {
	JournalID uuid.UUID
	Scope     [sha256.Size]byte
	Storage   journalbinding.StorageIdentity
}

// ExecutionJournalSnapshot holds a fully verified current-schema document.
// It proves semantics of supplied bytes, not their provenance, freshness,
// durability, held lock, executing process or current Fleet activation.
type ExecutionJournalSnapshot struct {
	status                ExecutionJournalStatus
	digest                [sha256.Size]byte
	launchDigest          [sha256.Size]byte
	nonAdmissions         int
	terminalNonAdmissions int
	verified              bool
}

func (snapshot ExecutionJournalSnapshot) Status() ExecutionJournalStatus { return snapshot.status }
func (snapshot ExecutionJournalSnapshot) Digest() [sha256.Size]byte      { return snapshot.digest }
func (snapshot ExecutionJournalSnapshot) NonAdmissions() (int, int) {
	return snapshot.nonAdmissions, snapshot.terminalNonAdmissions
}

// MatchStartup checks the persisted unresolved nonce and exact launch. A valid
// retained execution history is still ineligible for first backend startup.
// This comparison is not permission; callers must independently bind Registry,
// current storage, process ownership, effective launch and activation.
func (snapshot ExecutionJournalSnapshot) MatchStartup(request BackendStartupRequest) error {
	state := snapshot.status
	if !snapshot.verified || request.Validate() != nil || request.JournalID != state.JournalID || request.JournalScope != state.Scope ||
		state.BackendLifecycle.State != BackendLifecycleUnresolved || request.IncarnationID != state.BackendLifecycle.IncarnationID ||
		request.LaunchDigest != state.BackendLifecycle.LaunchDigest || request.LaunchDigest != snapshot.launchDigest ||
		state.Highest != 0 || state.Floor != 0 || state.RetainedExecutions != 0 || snapshot.nonAdmissions != 0 || snapshot.terminalNonAdmissions != 0 {
		return ErrBackendStartupDenied
	}
	return nil
}

// VerifyExecutionJournalSnapshot applies the same complete canonical decoding,
// signature, scope, watermark, retained history, drain, renewal, non-admission
// and lifecycle checks used by live recovery. It acquires no locks, writes no
// files, performs no recovery and never implicitly upgrades a legacy document.
// document and lockDocument must be independently read from original storage;
// a caller-provided copy does not become authentic merely by passing this API.
func VerifyExecutionJournalSnapshot(document, lockDocument []byte, manifest LaunchManifest, validator *stageauthority.Validator, expected ExecutionJournalIdentity) (ExecutionJournalSnapshot, error) {
	if expected.JournalID == uuid.Nil || expected.Scope == ([sha256.Size]byte{}) || !expected.Storage.Valid() ||
		string(lockDocument) != expected.JournalID.String() {
		return ExecutionJournalSnapshot{}, errors.New("execution journal snapshot has no matching independent identity")
	}
	manifest = cloneLaunchManifest(manifest)
	scope, err := executionScopeForManifest(manifest, validator)
	if err != nil {
		return ExecutionJournalSnapshot{}, err
	}
	digest, err := scope.digest()
	if err != nil || digest != expected.Scope {
		return ExecutionJournalSnapshot{}, errors.New("execution journal snapshot scope differs from trusted launch")
	}
	state, err := decodeExecutionJournal(document)
	if err != nil {
		return ExecutionJournalSnapshot{}, err
	}
	if state.SchemaVersion != 8 || state.ID != expected.JournalID || state.Scope != expected.Scope ||
		state.Root != executionFileIdentity(expected.Storage.Root) || state.Lock != executionFileIdentity(expected.Storage.Lock) {
		return ExecutionJournalSnapshot{}, errors.New("execution journal snapshot ownership or schema changed")
	}
	journal := executionJournal{scope: scope, state: state}
	if err := journal.validateProofs(); err != nil {
		return ExecutionJournalSnapshot{}, err
	}
	launch, err := EncodeLaunchManifest(manifest)
	if err != nil {
		return ExecutionJournalSnapshot{}, err
	}
	return ExecutionJournalSnapshot{status: journal.status(), digest: sha256.Sum256(document), launchDigest: sha256.Sum256(launch),
		nonAdmissions: len(state.NonAdmissions), terminalNonAdmissions: len(state.TerminalNonAdmissions), verified: true}, nil
}

func executionScopeForManifest(manifest LaunchManifest, validator *stageauthority.Validator) (executionJournalScope, error) {
	if validator == nil {
		return executionJournalScope{}, errors.New("execution journal requires a signature verifier")
	}
	bindings, err := manifest.RuntimeBindings()
	if err != nil {
		return executionJournalScope{}, err
	}
	floor, err := manifest.bindExecutionFloorConfig(ExecutionFloorConfig{}, validator)
	if err != nil {
		return executionJournalScope{}, err
	}
	verifier, err := newExecutionFloorVerifier(*floor, bindings[0])
	if err != nil {
		return executionJournalScope{}, err
	}
	return executionJournalScope{binding: cloneBinding(bindings[0]), floor: verifier}, nil
}

func decodeExecutionJournal(document []byte) (executionDiskState, error) {
	if len(document) == 0 || len(document) > maxExecutionStateBytes {
		return executionDiskState{}, errors.New("ModelRuntime execution state exceeds its bound")
	}
	if err := strictjson.RejectDuplicateKeys(document); err != nil {
		return executionDiskState{}, err
	}
	var state executionDiskState
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return executionDiskState{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return executionDiskState{}, errors.New("ModelRuntime execution state has trailing data")
	}
	canonical, err := json.Marshal(state)
	if err != nil || !bytes.Equal(canonical, document) {
		return executionDiskState{}, errors.New("ModelRuntime execution state is not canonical")
	}
	return state, nil
}

func (journal *executionJournal) status() ExecutionJournalStatus {
	state := journal.state
	result := ExecutionJournalStatus{Storage: journalbinding.StorageIdentity{Root: journalbinding.FileIdentity(state.Root), Lock: journalbinding.FileIdentity(state.Lock)},
		JournalID: state.ID, SchemaVersion: state.SchemaVersion, Scope: state.Scope, Highest: state.Highest, Floor: state.Floor,
		RetainedExecutions: len(state.Executions), BackendLifecycle: *state.BackendLifecycle}
	result.WorkerReuseDenied = journal.workerHealthDenied()
	result.HealthHistoryUnknown = state.HealthHistoryUnknown
	for _, record := range state.Executions {
		if record.Drain == nil {
			result.PendingExecutions++
		}
	}
	return result
}

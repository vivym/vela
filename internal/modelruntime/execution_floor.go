package modelruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

// ExecutionFloorMember is trusted topology supplied at construction, never
// learned from the disposition being verified. Runtime epochs come from Services.
type ExecutionFloorMember struct {
	WorkerMemberID     string
	MemberEpoch        int64
	IdentityDigest     []byte
	DeviceSubsetDigest []byte
}

type ExecutionFloorConfig struct {
	Validator *stageauthority.Validator
	Members   []ExecutionFloorMember
	State     *ExecutionFloorStateConfig
}

// ExecutionFloorStateConfig defaults to recovery of existing state. Initialize
// is an explicit, one-time bootstrap into an already trusted empty directory.
// Empty storage alone does not prove a new Worker; ordinary restarts must never
// infer Initialize from missing files.
type ExecutionFloorStateConfig struct {
	Directory  string
	Initialize bool
	// UpgradeV2 permits validated schema-2 to current-schema recovery. It preserves all
	// restrictions and pending/drained history, and creates no non-admission proof.
	UpgradeV2 bool
	// UpgradeV3 preserves schema-3 evidence without inventing terminal allocation
	// non-admission proof. Upgrade flags are mutually exclusive.
	UpgradeV3 bool
	// UpgradeV4 preserves schema-4 evidence without inventing renewal candidates.
	UpgradeV4 bool
	// UpgradeV5 preserves schema-5 evidence and marks unrecorded backend history
	// unknown. No upgrade infers that an empty execution journal never ran models.
	UpgradeV5 bool
	// UpgradeV6 preserves lifecycle and execution history. Missing old receipts
	// remain unknown; upgrading cannot recreate a lost sealed output receipt.
	UpgradeV6 bool
	// UpgradeV7 preserves receipts and lifecycle, marking legacy execution health
	// unknown. It grants neither health clearance nor backend replacement.
	UpgradeV7 bool
}

type executionFloorVerifier struct {
	validator *stageauthority.Validator
	members   map[string]ExecutionFloorMember
}

// ExecutionFloorInstallation reports an admission checkpoint. Durable means its
// floor is persisted; neither form proves backend drain or authorizes deletion.
type ExecutionFloorInstallation struct {
	Cutoff            int64
	Durable           bool
	DispositionDigest [sha256.Size]byte
	pending           []<-chan struct{}
}

// WaitAcceptedOperations waits only for calls admitted before this checkpoint.
// Backend tasks, child writers, later cancellation calls, and Worker input
// resolvers need separate drain evidence, even when this returns successfully.
func (installation *ExecutionFloorInstallation) WaitAcceptedOperations(ctx context.Context) error {
	if installation == nil {
		return errors.New("ModelRuntime execution floor installation is missing")
	}
	for _, done := range installation.pending {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return ctx.Err()
}

// NewSupervisorWithExecutionFloor enables signed cutoff installation. A State
// configuration additionally persists restrictions across process restarts.
func NewSupervisorWithExecutionFloor(config ExecutionFloorConfig, services ...*Service) (*Supervisor, error) {
	return newSupervisor(&config, services...)
}

func newExecutionFloorVerifier(config ExecutionFloorConfig, baseline stageauthority.RuntimeBinding) (*executionFloorVerifier, error) {
	if config.Validator == nil || len(config.Members) != len(baseline.Members) {
		return nil, errors.New("ModelRuntime execution floor requires a verifier and complete trusted membership")
	}
	verifier := &executionFloorVerifier{validator: config.Validator, members: make(map[string]ExecutionFloorMember)}
	for _, member := range config.Members {
		if _, duplicate := verifier.members[member.WorkerMemberID]; duplicate ||
			len(member.IdentityDigest) != sha256.Size || len(member.DeviceSubsetDigest) != sha256.Size {
			return nil, errors.New("ModelRuntime execution floor trusted member is invalid")
		}
		member.IdentityDigest = bytes.Clone(member.IdentityDigest)
		member.DeviceSubsetDigest = bytes.Clone(member.DeviceSubsetDigest)
		verifier.members[member.WorkerMemberID] = member
	}
	localFound := false
	for _, member := range baseline.Members {
		configured, found := verifier.members[member.ID]
		if !found || configured.MemberEpoch != member.Epoch {
			return nil, errors.New("ModelRuntime execution floor trusted membership does not match resident topology")
		}
		localFound = localFound || member.ID == baseline.WorkerMemberID && member.Epoch == baseline.WorkerMemberEpoch
	}
	if !localFound {
		return nil, errors.New("ModelRuntime execution floor topology omits the local member")
	}
	return verifier, nil
}

func (supervisor *Supervisor) InstallExecutionFloor(ctx context.Context, value *velav1.StageTerminalDisposition) (*ExecutionFloorInstallation, error) {
	return supervisor.installExecutionFloor(ctx, value, false)
}

// InstallRecoveryExecutionFloor restricts the current durable journal owner.
// Historical routes need not be resident; their writer proofs remain separate.
func (supervisor *Supervisor) InstallRecoveryExecutionFloor(ctx context.Context, value *velav1.StageTerminalDisposition) (*ExecutionFloorInstallation, error) {
	return supervisor.installExecutionFloor(ctx, value, true)
}

func (supervisor *Supervisor) installExecutionFloor(ctx context.Context, value *velav1.StageTerminalDisposition, recovery bool) (*ExecutionFloorInstallation, error) {
	if supervisor == nil || supervisor.floor == nil || supervisor.admission == nil || ctx == nil {
		return nil, errors.New("ModelRuntime execution floor verifier is not configured")
	}
	admission := supervisor.admission
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := admission.checkStateLocked(ctx); err != nil {
		return nil, err
	}
	if recovery && admission.store == nil {
		return nil, errors.New("execution floor recovery requires a durable journal owner")
	}
	// Validate freshness at the same admission boundary that installs the cutoff.
	verified, err := supervisor.floor.validator.ValidateTerminalDispositionEnvelope(value)
	if err != nil {
		return nil, err
	}
	if err := supervisor.matchExecutionFloorScope(verified.Disposition, !recovery); err != nil {
		return nil, err
	}
	if verified.Disposition.GetCutoff() > admission.floor && admission.store != nil {
		if err := admission.store.saveFloorContext(ctx, verified.Disposition); err != nil {
			if isExecutionJournalRejection(err) || errors.Is(err, stageauthority.ErrStale) {
				return nil, err
			}
			return nil, admission.failStateLocked(err)
		}
	}
	admission.floor = max(admission.floor, verified.Disposition.GetCutoff())
	installation := &ExecutionFloorInstallation{Cutoff: admission.floor, Durable: admission.store != nil, DispositionDigest: verified.Digest}
	for _, operation := range admission.active {
		if operation.sequence <= admission.floor {
			installation.pending = append(installation.pending, operation.done)
		}
	}
	return installation, nil
}

func (supervisor *Supervisor) matchExecutionFloorScope(value *velav1.StageTerminalDisposition, currentRuntimes bool) error {
	if err := supervisor.journalScope().matchExecutionFloorScope(value); err != nil {
		return err
	}
	if currentRuntimes {
		for _, allocation := range value.GetAllocations() {
			if supervisor.terminalAllocationService(allocation) == nil {
				return stageauthority.ErrRuntimeMismatch
			}
		}
	}
	return nil
}

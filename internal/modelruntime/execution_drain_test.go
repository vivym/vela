package modelruntime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

type executionDrainBackend struct {
	*modelruntime.FakeRuntime
	failure         string
	prepareFailure  bool
	failureEvidence *modelruntime.FailureEvidence
	entered         chan struct{}
	joined          <-chan struct{}
	drainCalls      atomic.Int64
	sealCalls       atomic.Int64
	closed          atomic.Bool
}

func (b *executionDrainBackend) Close() error { b.closed.Store(true); return nil }

func (b *executionDrainBackend) Prepare(ctx context.Context, verified stageauthority.Verified, spec *velav1.StageExecutionSpec) error {
	if err := b.FakeRuntime.Prepare(ctx, verified, spec); err != nil {
		return err
	}
	if b.prepareFailure {
		return errors.New("injected partial Prepare failure")
	}
	return nil
}

func (b *executionDrainBackend) Seal(ctx context.Context, verified stageauthority.Verified) (modelruntime.SealedOutput, error) {
	b.sealCalls.Add(1)
	return b.FakeRuntime.Seal(ctx, verified)
}

func (b *executionDrainBackend) Status(ctx context.Context, verified stageauthority.Verified) (modelruntime.BackendStatus, error) {
	if b.failureEvidence != nil {
		return modelruntime.BackendStatus{State: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED,
			FailureEvidence: b.failureEvidence}, nil
	}
	return b.FakeRuntime.Status(ctx, verified)
}

func (b *executionDrainBackend) DrainExecution(ctx context.Context, verified stageauthority.Verified) (modelruntime.BackendDrain, error) {
	if b.drainCalls.Add(1) == 1 && b.entered != nil {
		close(b.entered)
	}
	if b.joined != nil {
		select {
		case <-b.joined:
		case <-ctx.Done():
			return modelruntime.BackendDrain{}, ctx.Err()
		}
	}
	if b.failure == "error" {
		return modelruntime.BackendDrain{}, errors.New("injected incomplete writer drain")
	}
	if b.prepareFailure || b.failureEvidence != nil {
		if err := b.Cancel(ctx, verified, velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP); err != nil {
			return modelruntime.BackendDrain{}, err
		}
		b.FinishStop()
	}
	result, err := b.FakeRuntime.DrainExecution(ctx, verified)
	switch b.failure {
	case "contract":
		result.Contract = "status-is-stopped"
	case "digest":
		result.AuthorityDigest[0] ^= 1
	case "sequence":
		result.ExecutionSequence++
	}
	return result, err
}

func newExecutionDrainFixture(t *testing.T, directory string, backend modelruntime.Backend) *executionFloorFixture {
	t.Helper()
	f := newExecutionFloorFixture(t, "")
	f.supervisor.Close()
	f.otherBackend = modelruntime.NewFakeVAERuntime()
	f.services = []*modelruntime.Service{
		newRuntimeService(t, f.clock, f.validator, f.bindings[0], backend),
		newRuntimeService(t, f.clock, f.validator, f.bindings[1], f.otherBackend),
	}
	var err error
	f.supervisor, err = modelruntime.NewSupervisorWithExecutionFloor(modelruntime.ExecutionFloorConfig{
		Validator: f.validator, State: &modelruntime.ExecutionFloorStateConfig{Directory: directory, Initialize: true},
		Members: []modelruntime.ExecutionFloorMember{{WorkerMemberID: f.bindings[0].WorkerMemberID,
			MemberEpoch: f.bindings[0].WorkerMemberEpoch, IdentityDigest: bytes.Repeat([]byte{0x66}, 32), DeviceSubsetDigest: bytes.Repeat([]byte{0x67}, 32)}},
	}, f.services...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.supervisor.Close)
	return f
}

func readyDrainOutput(t *testing.T, f *executionFloorFixture, backend *modelruntime.FakeRuntime, authority *velav1.StageAuthority) {
	t.Helper()
	prepareFloorRuntime(t, f.supervisor, authority)
	response, err := f.supervisor.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: authority})
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("start drain fixture: %v %v", response, err)
	}
	backend.MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/dit.bin"}`))
}

func assertExecutionDrainCheckpoint(t *testing.T, supervisor *modelruntime.Supervisor, authority *velav1.StageAuthority, expected bool) {
	t.Helper()
	checkpoint, err := supervisor.InspectExecutionDrain(t.Context(), authority)
	if err != nil || (checkpoint != nil) != expected {
		t.Fatalf("persisted drain expected=%t: %+v %v", expected, checkpoint, err)
	}
	if checkpoint != nil {
		digest, err := stageauthority.Digest(authority)
		if err != nil || checkpoint.Result.AuthorityDigest != digest || !proto.Equal(checkpoint.Authority, authority) ||
			checkpoint.WorkerMemberID != authority.GetMembers()[0].GetWorkerMemberId() || checkpoint.DrainedAt.IsZero() {
			t.Fatalf("drain checkpoint lost exact identity: %+v %v", checkpoint, err)
		}
	}
}

func assertRecoveryDrainBlocks(t *testing.T, f *executionFloorFixture, authority *velav1.StageAuthority) {
	t.Helper()
	response, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: authority, ExecutionSpec: runtimeExecutionSpec()})
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED || !strings.Contains(response.GetDetail(), modelruntime.ErrExecutionDrainUnproven.Error()) {
		t.Fatalf("recovery admitted new work before historical writer drain: %v %v", response, err)
	}
	probe, err := f.supervisor.ProbeReadiness(t.Context(), &velav1.ModelRuntimeServiceProbeReadinessRequest{
		Identity: response.GetRuntimeIdentity(), Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_CANARY,
	})
	if err != nil || probe.GetReady() || !strings.Contains(probe.GetDetail(), modelruntime.ErrExecutionDrainUnproven.Error()) {
		t.Fatalf("pending historical drain advertised readiness: %v %v", probe, err)
	}
}

func TestExecutionDrainJoinsWriterBeforeSealReleaseAndRetainsAcrossReplacement(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	joined, releaseWriter := make(chan struct{}), make(chan struct{})
	backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), entered: make(chan struct{}), joined: joined}
	f := newExecutionDrainFixture(t, directory, backend)
	readyDrainOutput(t, f, backend.FakeRuntime, f.authorities[0])
	assertExecutionDrainCheckpoint(t, f.supervisor, f.authorities[0], false)
	file, err := os.Create(filepath.Join(t.TempDir(), "writer.bin"))
	if err != nil {
		t.Fatal(err)
	}
	writerError := make(chan error, 1)
	go func() {
		defer close(joined)
		<-releaseWriter
		_, writeErr := file.Write([]byte("last execution write"))
		writerError <- errors.Join(writeErr, file.Close())
	}()
	t.Cleanup(func() {
		select {
		case <-releaseWriter:
		default:
			close(releaseWriter)
		}
		<-joined
	})
	sealed := make(chan *velav1.ModelRuntimeServiceSealOutputResponse, 1)
	go func() {
		response, _ := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
		sealed <- response
	}()
	select {
	case <-backend.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("drain was not entered")
	}
	assertExecutionDrainCheckpoint(t, f.supervisor, f.authorities[0], false)
	other, err := f.services[1].PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authorities[1], ExecutionSpec: runtimeExecutionSpec()})
	if err != nil || other.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
		t.Fatalf("writer drain released the shared slot early: %v %v", other, err)
	}
	select {
	case response := <-sealed:
		t.Fatalf("Seal returned with an open writer: %v", response)
	default:
	}
	close(releaseWriter)
	if err := <-writerError; err != nil {
		t.Fatal(err)
	}
	response := <-sealed
	if response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || response.GetReceipt() == nil {
		t.Fatalf("drained Seal failed: %v", response)
	}
	assertExecutionDrainCheckpoint(t, f.supervisor, f.authorities[0], true)
	if _, err := file.Write([]byte("late write")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("writer handle was not closed: %v", err)
	}
	prepareFloorRuntime(t, f.supervisor, f.authorities[1])
	assertExecutionDrainCheckpoint(t, f.supervisor, f.authorities[0], true)
	if backend.closed.Load() {
		t.Fatal("per-execution drain unloaded residency")
	}
	f.supervisor.Close()
	recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now().Add(2*time.Minute))
	assertExecutionDrainCheckpoint(t, recovered.supervisor, f.authorities[0], true)
	assertExecutionDrainCheckpoint(t, recovered.supervisor, f.authorities[1], false)
	if checkpoint, err := recovered.supervisor.DrainExecution(t.Context(), f.authorities[1]); checkpoint != nil || !errors.Is(err, stageauthority.ErrRuntimeMismatch) {
		t.Fatalf("restarted Runtime inferred historical drain: %+v %v", checkpoint, err)
	}
}

func TestExecutionDrainRejectsIncompleteProofAndReplaysSealWithoutPhysicalWork(t *testing.T) {
	for _, fault := range []string{"unsupported", "error", "contract", "digest", "sequence", "timeout"} {
		t.Run(fault, func(t *testing.T) {
			backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), failure: fault}
			var configured modelruntime.Backend = backend
			if fault == "unsupported" {
				configured = struct{ modelruntime.Backend }{backend}
			}
			if fault == "timeout" {
				backend.joined = make(chan struct{})
			}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), configured)
			readyDrainOutput(t, f, backend.FakeRuntime, f.authorities[0])
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			response, err := f.supervisor.SealOutput(ctx, &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
			cancel()
			if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED || response.GetReceipt() != nil {
				t.Fatalf("incomplete drain accepted Seal: %v %v", response, err)
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, f.authorities[0], false)
			other, err := f.services[1].PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authorities[1], ExecutionSpec: runtimeExecutionSpec()})
			if err != nil || other.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED || backend.closed.Load() {
				t.Fatalf("unproven drain released/unloaded the resident slot: %v %v", other, err)
			}
			if fault == "unsupported" {
				return
			}
			backend.failure, backend.joined = "", nil
			response, err = f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
			if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED || response.GetReceipt() == nil || backend.sealCalls.Load() != 1 {
				t.Fatalf("drain retry repeated Seal or lost receipt: %v %v seals=%d", response, err, backend.sealCalls.Load())
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, f.authorities[0], true)
			prepareFloorRuntime(t, f.supervisor, f.authorities[1])
		})
	}
}

func TestExecutionDrainRetriesExactExpiredStoppedExecutionBelowFloor(t *testing.T) {
	backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), failure: "error"}
	f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
	if checkpoint, err := f.supervisor.DrainExecution(t.Context(), f.authorities[0]); err != nil || checkpoint != nil || backend.drainCalls.Load() != 0 {
		t.Fatalf("nonterminal execution entered drain: %+v %v", checkpoint, err)
	}
	f.clock.Advance(time.Second)
	unseen := renewWatchdogAuthority(t, f.signer, f.authorities[0], f.clock.Now())
	response, err := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
		Authority: f.authorities[0], Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	})
	if err != nil || !response.GetCancellationAcknowledged() {
		t.Fatalf("cancel: %v %v", response, err)
	}
	backend.FinishStop()
	status, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: f.authorities[0]})
	if err != nil || status.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
		t.Fatalf("STOPPED without drain released slot: %v %v", status, err)
	}
	if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(2 * time.Minute)
	backend.failure = ""
	before := backend.drainCalls.Load()
	if checkpoint, err := f.supervisor.DrainExecution(t.Context(), unseen); err != nil || checkpoint != nil || backend.drainCalls.Load() != before {
		t.Fatalf("historical drain installed an unseen renewal: %+v %v", checkpoint, err)
	}
	checkpoint, err := f.supervisor.DrainExecution(t.Context(), f.authorities[0])
	if err != nil || checkpoint == nil || backend.closed.Load() {
		t.Fatalf("exact expired drain failed: %+v %v", checkpoint, err)
	}
	assertExecutionDrainCheckpoint(t, f.supervisor, f.authorities[0], true)
	assertFloorCommandsRejected(t, f.supervisor, f.authorities[0])
	prepareFloorRuntime(t, f.supervisor, f.authority(t, 1, 12))
}

func TestExecutionDrainPersistenceFailureRequiresRecovery(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime()}
	f := newExecutionDrainFixture(t, directory, backend)
	readyDrainOutput(t, f, backend.FakeRuntime, f.authorities[0])
	restore := modelruntime.SetExecutionStateSyncHookForTest(f.supervisor, func(func() error) error {
		return errors.New("injected drain fsync failure")
	})
	response, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
	restore()
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED || !strings.Contains(response.GetDetail(), "state recovery") {
		t.Fatalf("uncommitted drain was acknowledged: %v %v", response, err)
	}
	if checkpoint, err := f.supervisor.InspectExecutionDrain(t.Context(), f.authorities[0]); checkpoint != nil || !errors.Is(err, modelruntime.ErrExecutionStateRecovery) {
		t.Fatalf("uncertain drain escaped failed state: %+v %v", checkpoint, err)
	}
	assertFloorCommandsRejected(t, f.supervisor, f.authority(t, 1, 100))
	if backend.drainCalls.Load() != 1 || backend.closed.Load() {
		t.Fatal("persistence failure restarted or unloaded backend")
	}
	f.supervisor.Close()
	recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now().Add(2*time.Minute))
	assertExecutionDrainCheckpoint(t, recovered.supervisor, f.authorities[0], true)
}

func TestExecutionDrainRetainsPartialPrepareFailure(t *testing.T) {
	for _, failure := range []string{"error", ""} {
		t.Run("drain="+failure, func(t *testing.T) {
			backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), prepareFailure: true, failure: failure}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			response, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authorities[0], ExecutionSpec: runtimeExecutionSpec()})
			if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("partial prepare: %v %v", response, err)
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, f.authorities[0], failure == "")
			if failure != "" {
				status, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: f.authorities[0]})
				if err != nil || status.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED ||
					status.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED {
					t.Fatalf("backend PREPARED reopened a failed Prepare: %v %v", status, err)
				}
				started, err := f.supervisor.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: f.authorities[0]})
				if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
					t.Fatalf("partially failed Prepare reached Start: %v %v", started, err)
				}
				sealed, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
				if err != nil || sealed.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED ||
					sealed.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED {
					t.Fatalf("partially failed Prepare reached output Seal: %v %v", sealed, err)
				}
			}
			other, err := f.services[1].PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authorities[1], ExecutionSpec: runtimeExecutionSpec()})
			expected := velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
			if failure != "" {
				expected = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
			}
			if err != nil || other.GetDecision() != expected {
				t.Fatalf("partial prepare released unproven writer: %v %v", other, err)
			}
		})
	}
}

func TestExecutionDrainDoesNotOverrideFailedWorkerHealth(t *testing.T) {
	for _, reusable := range []bool{false, true} {
		t.Run(map[bool]string{false: "unhealthy", true: "reusable"}[reusable], func(t *testing.T) {
			backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), failure: "error"}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			prepareFloorRuntime(t, f.supervisor, f.authorities[0])
			backend.failureEvidence = &modelruntime.FailureEvidence{FailureClass: "backend_oom", FailureFingerprint: bytes.Repeat([]byte{0x91}, 32),
				WorkerReusable: reusable, ConsumedResourceUnits: 1, FailedAt: f.clock.Now(), RetryAt: f.clock.Now().Add(time.Second)}
			response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: f.authorities[0]})
			if err != nil || response.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED {
				t.Fatalf("failed status: %v %v", response, err)
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, f.authorities[0], false)
			backend.failure = ""
			if checkpoint, err := f.supervisor.DrainExecution(t.Context(), f.authorities[0]); err != nil || checkpoint == nil {
				t.Fatalf("drain failed execution: %+v %v", checkpoint, err)
			}
			other, err := f.services[1].PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authorities[1], ExecutionSpec: runtimeExecutionSpec()})
			if err != nil || (other.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED) != reusable {
				t.Fatalf("drain overrode WorkerReusable=%t: %v %v", reusable, other, err)
			}
		})
	}
}

func TestExecutionDrainHistoryBackpressureNeverEvictsAndDoesNotPoisonReads(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	for sequence := int64(10); sequence < 42; sequence++ {
		authority := f.authority(t, 0, sequence)
		readyDrainOutput(t, f, f.backend.FakeRuntime, authority)
		response, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: authority})
		if err != nil || response.GetReceipt() == nil {
			t.Fatalf("seal sequence %d: %v %v", sequence, response, err)
		}
		if sequence == 10 {
			f.authorities[0] = authority
		}
	}
	response, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority(t, 0, 42), ExecutionSpec: runtimeExecutionSpec()})
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED || !strings.Contains(response.GetDetail(), modelruntime.ErrExecutionHistoryFull.Error()) {
		t.Fatalf("history overflow failed open or poisoned admission: %v %v", response, err)
	}
	assertExecutionDrainCheckpoint(t, f.supervisor, f.authorities[0], true)
	if state := readDurableExecutionState(t, directory); state.Highest != 41 || state.SchemaVersion != 3 {
		t.Fatalf("history overflow consumed new authority: %+v", state)
	}
}

func TestExecutionDrainRenewalCheckpointRemainsExactAfterRestart(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	readyDrainOutput(t, f, f.backend.FakeRuntime, f.authorities[0])
	f.clock.Advance(time.Second)
	renewal := renewWatchdogAuthority(t, f.signer, f.authorities[0], f.clock.Now())
	sealed, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: renewal})
	if err != nil || sealed.GetReceipt() == nil {
		t.Fatalf("seal renewed execution: %v %v", sealed, err)
	}
	assertExecutionDrainCheckpoint(t, f.supervisor, f.authorities[0], false)
	assertExecutionDrainCheckpoint(t, f.supervisor, renewal, true)
	f.supervisor.Close()
	recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now().Add(2*time.Minute))
	assertExecutionDrainCheckpoint(t, recovered.supervisor, f.authorities[0], false)
	assertExecutionDrainCheckpoint(t, recovered.supervisor, renewal, true)
	checkpoint, err := recovered.supervisor.InspectExecutionDrain(t.Context(), renewal)
	if err != nil || checkpoint == nil {
		t.Fatalf("read recovered drain: %+v %v", checkpoint, err)
	}
	checkpoint.Authority.Signature[0] ^= 1
	assertExecutionDrainCheckpoint(t, recovered.supervisor, renewal, true)
	future := renewWatchdogAuthority(t, recovered.signer, renewal, recovered.clock.Now().Add(time.Minute))
	if checkpoint, err := recovered.supervisor.InspectExecutionDrain(t.Context(), future); checkpoint != nil || err == nil {
		t.Fatalf("future envelope entered drain inspection: %+v %v", checkpoint, err)
	}
}

type retainedExecutionDocument struct {
	Authority []byte `json:"authority"`
	Drain     *struct {
		Authority []byte                    `json:"authority"`
		Result    modelruntime.BackendDrain `json:"result"`
		DrainedAt time.Time                 `json:"drained_at"`
	} `json:"drain"`
}

func TestExecutionDrainRecoveryRejectsDamagedProofs(t *testing.T) {
	for _, damage := range []string{"digest", "sequence", "contract", "authority", "original signature", "time", "missing history", "duplicate history"} {
		t.Run(damage, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			readyDrainOutput(t, f, f.backend.FakeRuntime, f.authorities[0])
			sealed, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
			if err != nil || sealed.GetReceipt() == nil {
				t.Fatalf("seal fixture: %v %v", sealed, err)
			}
			f.supervisor.Close()
			state := readDurableExecutionState(t, directory)
			var records []retainedExecutionDocument
			if err := json.Unmarshal(state.Executions, &records); err != nil || len(records) != 1 || records[0].Drain == nil {
				t.Fatalf("read drain fixture: %+v %v", records, err)
			}
			switch damage {
			case "digest":
				records[0].Drain.Result.AuthorityDigest[0] ^= 1
			case "sequence":
				records[0].Drain.Result.ExecutionSequence++
			case "contract":
				records[0].Drain.Result.Contract = "stopped"
			case "authority":
				other := f.authority(t, 1, 12)
				records[0].Drain.Authority, err = proto.MarshalOptions{Deterministic: true}.Marshal(other)
				if err != nil {
					t.Fatal(err)
				}
				records[0].Drain.Result.AuthorityDigest, err = stageauthority.Digest(other)
				if err != nil {
					t.Fatal(err)
				}
				records[0].Drain.Result.ExecutionSequence = other.GetExecutionSequence()
			case "original signature":
				records[0].Authority[len(records[0].Authority)-1] ^= 1
			case "time":
				records[0].Drain.DrainedAt = time.Time{}
			case "missing history":
				records = nil
			case "duplicate history":
				records = append(records, records[0])
			}
			state.Executions, err = json.Marshal(records)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, durableStateFileName), encodeDurableExecutionState(t, state), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := tryDurableExecutionFixture(t, directory, false); err == nil {
				t.Fatal("damaged retained drain proof was accepted")
			}
		})
	}
}

func TestExecutionDrainAbruptProcessRecovery(t *testing.T) {
	for _, mode := range []string{"pending", "drained"} {
		t.Run(mode, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			command := exec.Command(os.Args[0], "-test.run=^TestExecutionDrainProcessHelper$")
			command.Env = append(os.Environ(), "VELA_DRAIN_TEST_ROOT="+directory, "VELA_DRAIN_TEST_MODE="+mode)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("drain process helper: %v %s", err, output)
			}
			state := readDurableExecutionState(t, directory)
			authority := &velav1.StageAuthority{}
			if err := proto.Unmarshal(state.Authority, authority); err != nil {
				t.Fatal(err)
			}
			recovered := durableExecutionFixture(t, directory, false, "", 10, time.Date(2026, 9, 5, 9, 0, 0, 0, time.UTC))
			assertExecutionDrainCheckpoint(t, recovered.supervisor, authority, mode == "drained")
			if mode == "pending" {
				assertRecoveryDrainBlocks(t, recovered, recovered.authority(t, 1, 12))
			} else {
				prepareFloorRuntime(t, recovered.supervisor, recovered.authority(t, 1, 12))
			}
		})
	}
}

func TestExecutionDrainProcessHelper(t *testing.T) {
	directory, mode := os.Getenv("VELA_DRAIN_TEST_ROOT"), os.Getenv("VELA_DRAIN_TEST_MODE")
	if directory == "" || mode == "" {
		return
	}
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	readyDrainOutput(t, f, f.backend.FakeRuntime, f.authorities[0])
	if mode == "drained" {
		response, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
		if err != nil || response.GetReceipt() == nil {
			t.Fatalf("seal before abrupt exit: %v %v", response, err)
		}
	}
	os.Exit(0)
}

func TestExecutionDrainRejectsLegacyJournalWithoutReinitializing(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	f.supervisor.Close()
	state := readDurableExecutionState(t, directory)
	state.SchemaVersion = 1
	wire := encodeDurableExecutionState(t, state)
	if err := os.WriteFile(filepath.Join(directory, durableStateFileName), wire, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := tryDurableExecutionFixture(t, directory, false); err == nil {
		t.Fatal("schema-1 journal was silently upgraded to complete drain history")
	}
	remaining, err := os.ReadFile(filepath.Join(directory, durableStateFileName))
	if err != nil || !bytes.Equal(wire, remaining) {
		t.Fatal("failed recovery changed legacy history")
	}
}

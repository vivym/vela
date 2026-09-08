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
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func workerHealthEvidence(f *executionFloorFixture, reusable bool) *modelruntime.FailureEvidence {
	return &modelruntime.FailureEvidence{FailureClass: "backend_oom", FailureFingerprint: bytes.Repeat([]byte{0x91}, 32),
		WorkerReusable: reusable, ConsumedResourceUnits: 1, FailedAt: f.clock.Now(), RetryAt: f.clock.Now().Add(time.Second)}
}

func reportWorkerHealth(t *testing.T, f *executionFloorFixture, backend *executionDrainBackend, reusable bool) *velav1.ModelRuntimeServiceStatusResponse {
	t.Helper()
	backend.failureEvidence = workerHealthEvidence(f, reusable)
	client, _ := serveRuntimeServer(t, f.supervisor)
	response, err := client.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: f.authorities[0]})
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
		response.GetFailureEvidence() == nil || response.GetFailureEvidence().GetWorkerReusable() != reusable {
		t.Fatalf("explicit WorkerReusable=%t status: %v %v", reusable, response, err)
	}
	return response
}

func TestWorkerHealthDenialSurvivesDrainedRuntimeReplacement(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime()}
	f := newExecutionDrainFixture(t, directory, backend)
	authority := f.authorities[0]
	prepareFloorRuntime(t, f.supervisor, authority)
	backend.failureEvidence = &modelruntime.FailureEvidence{FailureClass: "backend_oom", FailureFingerprint: bytes.Repeat([]byte{0x91}, 32),
		WorkerReusable: false, ConsumedResourceUnits: 1, FailedAt: f.clock.Now(), RetryAt: f.clock.Now().Add(time.Second)}
	client, _ := serveRuntimeServer(t, f.supervisor)
	response, err := client.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: authority})
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || response.GetFailureEvidence() == nil {
		t.Fatalf("failed status: %v %v", response, err)
	}
	if checkpoint, err := f.supervisor.DrainExecution(t.Context(), authority); err != nil || checkpoint == nil {
		t.Fatalf("drain failed execution: %+v %v", checkpoint, err)
	}
	if err := f.supervisor.Shutdown(); err != nil {
		t.Fatal(err)
	}
	recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now().Add(2*time.Minute))
	for index := range recovered.authorities {
		next := recovered.authority(t, index, 12)
		probe, err := recovered.supervisor.ProbeReadiness(t.Context(), &velav1.ModelRuntimeServiceProbeReadinessRequest{
			Identity: inspectionIdentity(next), Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP})
		if err != nil || probe.GetReady() {
			t.Fatalf("replacement forgot persistent health denial: %v %v", probe, err)
		}
		response, err := recovered.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: next, ExecutionSpec: runtimeExecutionSpec()})
		if err != nil || response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
			t.Fatalf("replacement admitted work on unhealthy member: %v %v", response, err)
		}
	}
}

func TestWorkerHealthPersistenceUncertaintyWithholdsStatusAndReuse(t *testing.T) {
	for _, clearing := range []bool{false, true} {
		t.Run(map[bool]string{false: "denial", true: "clearance"}[clearing], func(t *testing.T) {
			backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime()}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			prepareFloorRuntime(t, f.supervisor, f.authorities[0])
			if clearing {
				reportWorkerHealth(t, f, backend, false)
				if _, err := f.supervisor.DrainExecution(t.Context(), f.authorities[0]); err != nil {
					t.Fatal(err)
				}
			}
			backend.failureEvidence = workerHealthEvidence(f, clearing)
			restore := modelruntime.SetExecutionStateSyncHookForTest(f.supervisor, func(func() error) error { return errors.New("injected health sync uncertainty") })
			defer restore()
			response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: f.authorities[0]})
			if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED || response.GetFailureEvidence() != nil {
				t.Fatalf("uncertain health was acknowledged: %v %v", response, err)
			}
			for index := range f.authorities {
				next := f.authority(t, index, 12)
				probe, err := f.supervisor.ProbeReadiness(t.Context(), &velav1.ModelRuntimeServiceProbeReadinessRequest{Identity: inspectionIdentity(next), Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP})
				if err != nil || probe.GetReady() || !strings.Contains(probe.GetDetail(), "state recovery") {
					t.Fatalf("uncertain health restored readiness: %v %v", probe, err)
				}
				prepared, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: next, ExecutionSpec: runtimeExecutionSpec()})
				if err != nil || prepared.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
					t.Fatalf("uncertain health admitted execution: %v %v", prepared, err)
				}
			}
		})
	}
}

func TestWorkerHealthEvidenceRejectsCorruptionOnRecoveryAndSnapshot(t *testing.T) {
	for _, fault := range []string{"fingerprint", "class", "units", "failed-at", "retry-at", "unknown", "unknown-time", "authority", "unconfirmed", "future-renewal", "legacy-schema", "oversized"} {
		t.Run(fault, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime()}
			f := newExecutionDrainFixture(t, directory, backend)
			prepareFloorRuntime(t, f.supervisor, f.authorities[0])
			response := reportWorkerHealth(t, f, backend, false)
			f.supervisor.Close()
			config := recoveredRuntimeServerConfig(t, f, directory)
			identity, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
			if err != nil || !identity.WorkerReuseDenied || identity.HealthHistoryUnknown {
				t.Fatalf("health observation missing from journal status: %+v %v", identity, err)
			}
			state := readDurableExecutionState(t, directory)
			records := readSealedHistory(t, directory)
			evidence := proto.Clone(response.GetFailureEvidence()).(*velav1.ModelRuntimeFailureEvidence)
			switch fault {
			case "fingerprint":
				evidence.FailureFingerprint = nil
			case "class":
				evidence.FailureClass = " invalid "
			case "units":
				evidence.ConsumedResourceUnits = 0
			case "failed-at":
				evidence.FailedAt = nil
			case "retry-at":
				evidence.RetryAt = evidence.FailedAt
			case "unknown":
				evidence.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "unknown-time":
				evidence.FailedAt.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "authority":
				records[0].Health.Authority[0] ^= 1
			case "unconfirmed":
				records[0].Candidates.Confirmed = nil
			case "future-renewal":
				records[0].Health.Authority, err = proto.MarshalOptions{Deterministic: true}.Marshal(renewWatchdogAuthority(t, f.signer, f.authorities[0], f.clock.Now().Add(time.Second)))
				if err != nil {
					t.Fatal(err)
				}
			case "legacy-schema":
				state.SchemaVersion = 7
			case "oversized":
				evidence.Detail = strings.Repeat("x", 65536)
			}
			records[0].Health.Evidence, err = proto.MarshalOptions{Deterministic: true}.Marshal(evidence)
			if err != nil {
				t.Fatal(err)
			}
			state.Executions, err = json.Marshal(records)
			if err != nil {
				t.Fatal(err)
			}
			wire := encodeDurableExecutionState(t, state)
			if err := os.WriteFile(filepath.Join(directory, durableStateFileName), wire, 0o600); err != nil {
				t.Fatal(err)
			}
			_, lock := snapshotFiles(t, directory)
			if _, err := modelruntime.VerifyExecutionJournalSnapshot(wire, lock, config.Manifest, config.Validator, snapshotIdentity(identity)); err == nil {
				t.Fatal("snapshot accepted corrupt Worker health evidence")
			}
			config.ExecutionFloor.State.UpgradeV7 = fault == "legacy-schema"
			if _, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State); err == nil {
				t.Fatal("recovery accepted corrupt Worker health evidence")
			}
		})
	}
}

func TestWorkerHealthSurvivesActualProcessCrash(t *testing.T) {
	for _, phase := range []string{"denial-renamed", "denial-synced", "denial-response", "clearance-renamed", "clearance-synced", "clearance-response"} {
		t.Run(phase, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestWorkerHealthCrashHelper$", "-test.timeout=15s")
			command.Env = append(os.Environ(), "VELA_HEALTH_CRASH_ROOT="+directory, "VELA_HEALTH_CRASH_PHASE="+phase)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 94 || bytes.Contains(output, []byte("WARNING: DATA RACE")) {
				t.Fatalf("health crash boundary not reached: %v %s", err, output)
			}
			cleared := strings.HasPrefix(phase, "clearance")
			recovered := durableExecutionFixture(t, directory, false, "", 10, time.Date(2026, 9, 5, 9, 0, 0, 0, time.UTC))
			next := recovered.authority(t, 1, 12)
			probe, err := recovered.supervisor.ProbeReadiness(t.Context(), &velav1.ModelRuntimeServiceProbeReadinessRequest{Identity: inspectionIdentity(next), Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP})
			if err != nil || probe.GetReady() != cleared {
				t.Fatalf("crash recovery lost explicit health state: %v %v", probe, err)
			}
			response, err := recovered.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: next, ExecutionSpec: runtimeExecutionSpec()})
			if err != nil || (response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED) != cleared {
				t.Fatalf("crash recovery admission disagreed with health: %v %v", response, err)
			}
		})
	}
}

func TestWorkerHealthCrashHelper(t *testing.T) {
	directory, phase := os.Getenv("VELA_HEALTH_CRASH_ROOT"), os.Getenv("VELA_HEALTH_CRASH_PHASE")
	if directory == "" {
		return
	}
	backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime()}
	f := newExecutionDrainFixture(t, directory, backend)
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
	clearing := strings.HasPrefix(phase, "clearance")
	reportWorkerHealth(t, f, backend, !clearing)
	if _, err := f.supervisor.DrainExecution(t.Context(), f.authorities[0]); err != nil {
		t.Fatal(err)
	}
	modelruntime.SetExecutionStateSyncHookForTest(f.supervisor, func(syncDirectory func() error) error {
		if strings.HasSuffix(phase, "renamed") {
			os.Exit(94)
		}
		if err := syncDirectory(); err != nil {
			return err
		}
		if strings.HasSuffix(phase, "synced") {
			os.Exit(94)
		}
		return nil
	})
	reportWorkerHealth(t, f, backend, clearing)
	if strings.HasSuffix(phase, "response") {
		os.Exit(94)
	}
	t.Fatal("unknown health crash phase")
}

func TestWorkerHealthMigrationDoesNotInferHealthyDrainedHistory(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	readyDrainOutput(t, f, f.backend.FakeRuntime, f.authorities[0])
	sealed, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
	if err != nil || sealed.GetReceipt() == nil {
		t.Fatal("fixture seal failed", err)
	}
	f.supervisor.Close()
	state := readDurableExecutionState(t, directory)
	state.SchemaVersion = 7
	if err := os.WriteFile(filepath.Join(directory, durableStateFileName), encodeDurableExecutionState(t, state), 0o600); err != nil {
		t.Fatal(err)
	}
	config := recoveredRuntimeServerConfig(t, f, directory)
	if _, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State); err == nil {
		t.Fatal("ordinary recovery implicitly upgraded schema 7")
	}
	config.ExecutionFloor.State.UpgradeV7 = true
	upgraded, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
	if err != nil || upgraded.SchemaVersion != 8 || !upgraded.HealthHistoryUnknown || upgraded.WorkerReuseDenied || upgraded.JournalID.String() != state.ID {
		t.Fatalf("migration invented healthy legacy history: %+v %v", upgraded, err)
	}
	recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now().Add(2*time.Minute))
	response, err := recovered.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: recovered.authority(t, 1, 12), ExecutionSpec: runtimeExecutionSpec()})
	if err != nil || response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || !strings.Contains(response.GetDetail(), modelruntime.ErrWorkerHealthUnproven.Error()) {
		t.Fatalf("legacy health uncertainty admitted fresh work: %v %v", response, err)
	}
	replay, err := recovered.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
	if err != nil || !proto.Equal(replay.GetReceipt(), sealed.GetReceipt()) {
		t.Fatalf("health gate destroyed historical sealed receipt: %v %v", replay, err)
	}
}

func TestWorkerHealthClearanceCannotClearAnotherProfileDenial(t *testing.T) {
	f := newExecutionFloorFixture(t, "")
	f.supervisor.Close()
	backends := []*executionDrainBackend{{FakeRuntime: modelruntime.NewFakeDiTRuntime()}, {FakeRuntime: modelruntime.NewFakeVAERuntime()}}
	f.services = []*modelruntime.Service{
		newRuntimeService(t, f.clock, f.validator, f.bindings[0], backends[0]),
		newRuntimeService(t, f.clock, f.validator, f.bindings[1], backends[1]),
	}
	var err error
	f.supervisor, err = modelruntime.NewSupervisorWithExecutionFloor(modelruntime.ExecutionFloorConfig{
		Validator: f.validator, State: &modelruntime.ExecutionFloorStateConfig{Directory: privateExecutionStateDirectory(t), Initialize: true},
		Members: []modelruntime.ExecutionFloorMember{{WorkerMemberID: f.bindings[0].WorkerMemberID, MemberEpoch: f.bindings[0].WorkerMemberEpoch,
			IdentityDigest: bytes.Repeat([]byte{0x66}, 32), DeviceSubsetDigest: bytes.Repeat([]byte{0x67}, 32)}},
	}, f.services...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.supervisor.Close)
	client, _ := serveRuntimeServer(t, f.supervisor)
	report := func(index int, reusable bool) {
		t.Helper()
		backends[index].failureEvidence = workerHealthEvidence(f, reusable)
		response, err := client.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: f.authorities[index]})
		if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
			t.Fatalf("profile %d health status: %v %v", index, response, err)
		}
	}
	for index := range backends {
		prepareFloorRuntime(t, f.supervisor, f.authorities[index])
		report(index, true)
	}
	// Both resident Services retain drained executions. Each may still report
	// explicit health for its own exact authority; these are separate assertions.
	report(0, false)
	report(1, false)
	report(1, true)
	for _, authority := range f.authorities {
		probe, err := client.ProbeReadiness(t.Context(), &velav1.ModelRuntimeServiceProbeReadinessRequest{Identity: inspectionIdentity(authority), Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP})
		if err != nil || probe.GetReady() {
			t.Fatalf("other profile cleared denial: %v %v", probe, err)
		}
	}
	report(0, true)
	prepareFloorRuntime(t, f.supervisor, f.authority(t, 1, 12))
}

func TestWorkerHealthDeniedHistoryPreventsProductionBackendFactory(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime()}
	f := newExecutionDrainFixture(t, directory, backend)
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
	reportWorkerHealth(t, f, backend, false)
	if _, err := f.supervisor.DrainExecution(t.Context(), f.authorities[0]); err != nil {
		t.Fatal(err)
	}
	f.supervisor.Close()
	config := recoveredRuntimeServerConfig(t, f, directory)
	journal, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
	if err != nil || !journal.WorkerReuseDenied {
		t.Fatalf("missing denied history: %+v %v", journal, err)
	}
	config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, journal, nil)
	calls := 0
	config.BackendFactory = func(context.Context, modelruntime.LaunchRuntime, stageauthority.RuntimeBinding, modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
		calls++
		return nil, errors.New("unhealthy history reached backend factory")
	}
	server, err := modelruntime.StartRuntimeServer(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	client, err := modelruntimetransport.Dial(t.Context(), modelruntimetransport.Config{SocketPath: config.SocketPath, ExpectedUID: uint32(os.Geteuid())})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	identities, err := client.DiscoverRuntimeIdentities(t.Context(), &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
		WorkerInstanceId: config.Manifest.WorkerInstanceID, WorkerInstanceEpoch: config.Manifest.WorkerInstanceEpoch,
		WorkerMemberId: config.Manifest.WorkerMemberID, WorkerMemberEpoch: config.Manifest.WorkerMemberEpoch,
	})
	if err != nil || len(identities.GetIdentities()) != 2 {
		t.Fatalf("health recovery lost discovery: %v %v", identities, err)
	}
	for _, identity := range identities.Identities {
		probe, err := client.ProbeReadiness(t.Context(), &velav1.ModelRuntimeServiceProbeReadinessRequest{Identity: identity, Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP})
		if err != nil || probe.GetReady() {
			t.Fatalf("health recovery reported ready: %v %v", probe, err)
		}
		assertRecoveryServerDeniesExecutionWithReason(t, f, client, identity, modelruntime.ErrWorkerHealthUnproven)
	}
	if calls != 0 {
		t.Fatalf("health denial allowed %d replacement factory calls", calls)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
	if err != nil || after != journal {
		t.Fatalf("recovery changed denied history: %+v %v", after, err)
	}
}

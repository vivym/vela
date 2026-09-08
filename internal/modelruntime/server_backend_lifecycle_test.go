package modelruntime_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestRuntimeServerRetainsBackendOwnershipAfterFailedOrIdleStartup(t *testing.T) {
	for _, boundary := range []string{"first-factory-fails", "second-factory-fails", "idle-close"} {
		t.Run(boundary, func(t *testing.T) {
			f := newExecutionFloorFixture(t, "")
			config := recoveredRuntimeServerConfig(t, f, privateExecutionStateDirectory(t))
			config.ExecutionFloor.State.Initialize = true
			factory, calls := config.BackendFactory, 0
			var incarnation modelruntime.BackendLifecycleStatus
			config.BackendFactory = func(ctx context.Context, runtime modelruntime.LaunchRuntime, binding stageauthority.RuntimeBinding, backend modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
				calls++
				observed := assertRecordedBackendLifecycle(t, config)
				if calls == 1 {
					incarnation = observed
				} else if incarnation != observed {
					t.Fatal("AUX factories did not share one durable startup incarnation")
				}
				if boundary == "first-factory-fails" && calls == 1 || boundary == "second-factory-fails" && calls == 2 {
					return nil, errors.New("test initialization failed after possibly spawning a writer")
				}
				return factory(ctx, runtime, binding, backend)
			}
			server, err := modelruntime.StartRuntimeServer(t.Context(), config)
			if boundary == "idle-close" {
				if err != nil {
					t.Fatal(err)
				}
				if err := server.Close(); err != nil {
					t.Fatal(err)
				}
			} else if err == nil || server != nil {
				t.Fatal("startup failure did not reach the selected boundary")
			}
			before, err := os.ReadFile(filepath.Join(config.ExecutionFloor.State.Directory, durableStateFileName))
			if err != nil {
				t.Fatal(err)
			}
			config.ExecutionFloor.State.Initialize = false
			journal, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
			if err != nil || journal.PendingExecutions != 0 || journal.BackendLifecycle != incarnation {
				t.Fatalf("idle lifecycle unexpectedly retained execution history: %+v %v", journal, err)
			}
			config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, journal, nil)
			prior := calls
			recovered, err := modelruntime.StartRuntimeServer(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = recovered.Close() })
			if calls != prior {
				t.Fatalf("unresolved backend incarnation allowed replacement factory calls: %d", calls-prior)
			}
			client, err := modelruntimetransport.Dial(t.Context(), modelruntimetransport.Config{SocketPath: config.SocketPath, ExpectedUID: uint32(os.Geteuid())})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			discovered, err := client.DiscoverRuntimeIdentities(t.Context(), &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
				WorkerInstanceId: config.Manifest.WorkerInstanceID, WorkerInstanceEpoch: config.Manifest.WorkerInstanceEpoch,
				WorkerMemberId: config.Manifest.WorkerMemberID, WorkerMemberEpoch: config.Manifest.WorkerMemberEpoch,
			})
			if err != nil || len(discovered.GetIdentities()) != 2 {
				t.Fatalf("recovery lost identity discovery: %v %v", discovered, err)
			}
			for _, identity := range discovered.Identities {
				ready, err := client.ProbeReadiness(t.Context(), &velav1.ModelRuntimeServiceProbeReadinessRequest{
					Identity: identity, Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_BACKEND,
				})
				if err != nil || ready.GetReady() {
					t.Fatalf("unresolved backend incarnation became ready: %v %v", ready, err)
				}
				assertRecoveryServerDeniesExecutionWithReason(t, f, client, identity, modelruntime.ErrBackendIncarnationUnproven)
			}
			if err := recovered.Close(); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(filepath.Join(config.ExecutionFloor.State.Directory, durableStateFileName))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("recovery changed unresolved lifecycle history: %v", err)
			}
		})
	}
}

func TestRuntimeServerFreezesLaunchBeforeBackendCallbacks(t *testing.T) {
	for _, mutation := range []string{"caller-manifest", "shared-runtime-slices"} {
		t.Run(mutation, func(t *testing.T) {
			f := newExecutionFloorFixture(t, "")
			config := recoveredRuntimeServerConfig(t, f, privateExecutionStateDirectory(t))
			config.ExecutionFloor.State.Initialize = true
			command, environment := []string{"/approved-backend"}, []string{"APPROVED=value"}
			for index := range config.Manifest.Runtimes {
				config.Manifest.Runtimes[index].Command = command
				config.Manifest.Runtimes[index].Environment = environment
			}
			wire, err := modelruntime.EncodeLaunchManifest(config.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			var frozen modelruntime.LaunchManifest
			if err := json.Unmarshal(wire, &frozen); err != nil {
				t.Fatal(err)
			}
			factory, calls := config.BackendFactory, 0
			config.BackendFactory = func(ctx context.Context, runtime modelruntime.LaunchRuntime, binding stageauthority.RuntimeBinding, backend modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
				index := calls
				calls++
				lifecycle := readDurableExecutionState(t, config.ExecutionFloor.State.Directory).BackendLifecycle
				if lifecycle == nil || lifecycle.LaunchDigest != sha256.Sum256(wire) {
					t.Fatal("factory did not retain the original durable launch digest")
				}
				expected, err := frozen.Runtimes[index].ProcessBackendConfig(frozen.LocalDevices)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(runtime, frozen.Runtimes[index]) || !reflect.DeepEqual(backend, expected) {
					t.Errorf("factory %d configuration differs from the durable launch preimage", calls)
				}
				if calls == 1 {
					// Mutations are synchronous, after the journal write and before
					// the second factory. No concurrent caller writes are assumed.
					if mutation == "caller-manifest" {
						config.Manifest.Runtimes[1].Command[0] = "/changed-backend"
						config.Manifest.Runtimes[1].Environment[0] = "APPROVED=changed"
						config.Manifest.Runtimes[1].ScratchRoot += "-changed"
						config.Manifest.LocalDevices[0].DeviceEpoch++
					} else {
						runtime.Command[0] = "/changed-through-factory"
						runtime.Environment[0] = "APPROVED=changed-through-factory"
					}
				}
				return factory(ctx, runtime, binding, backend)
			}
			server, err := modelruntime.StartRuntimeServer(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = server.Close() })
			if calls != len(frozen.Runtimes) {
				t.Fatalf("factory calls = %d, expected %d", calls, len(frozen.Runtimes))
			}
			if err := server.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func assertRecordedBackendLifecycle(t *testing.T, config modelruntime.RuntimeServerConfig) modelruntime.BackendLifecycleStatus {
	t.Helper()
	state := readDurableExecutionState(t, config.ExecutionFloor.State.Directory)
	lifecycle := state.BackendLifecycle
	wire, err := modelruntime.EncodeLaunchManifest(config.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if state.SchemaVersion != 7 || lifecycle == nil || lifecycle.State != modelruntime.BackendLifecycleUnresolved || lifecycle.IncarnationID == uuid.Nil ||
		lifecycle.LaunchDigest != sha256.Sum256(wire) || lifecycle.RecordedAt.IsZero() || lifecycle.RecordedAt.After(time.Now()) {
		t.Fatalf("factory entered without its durable startup intent: %+v", lifecycle)
	}
	return *lifecycle
}

func TestRuntimeServerDrainDoesNotRetireBackendIncarnation(t *testing.T) {
	f := newExecutionFloorFixture(t, "")
	config := recoveredRuntimeServerConfig(t, f, privateExecutionStateDirectory(t))
	config.ExecutionFloor.State.Initialize = true
	backend, calls := modelruntime.NewFakeEncoderRuntime(), 0
	config.BackendFactory = func(context.Context, modelruntime.LaunchRuntime, stageauthority.RuntimeBinding, modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
		calls++
		if calls == 1 {
			return backend, nil
		}
		return modelruntime.NewFakeVAERuntime(), nil
	}
	server, err := modelruntime.StartRuntimeServer(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	incarnation := assertRecordedBackendLifecycle(t, config)
	client, err := modelruntimetransport.Dial(t.Context(), modelruntimetransport.Config{SocketPath: config.SocketPath, ExpectedUID: uint32(os.Geteuid())})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	discovered, err := client.DiscoverRuntimeIdentities(t.Context(), &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
		WorkerInstanceId: config.Manifest.WorkerInstanceID, WorkerInstanceEpoch: config.Manifest.WorkerInstanceEpoch,
		WorkerMemberId: config.Manifest.WorkerMemberID, WorkerMemberEpoch: config.Manifest.WorkerMemberEpoch,
	})
	if err != nil || len(discovered.GetIdentities()) != 2 {
		t.Fatalf("discover active runtime: %v %v", discovered, err)
	}
	identity := discovered.Identities[0]
	authority := f.authority(t, 0, 12)
	authority.Members[0].ModelRuntimeEpoch = identity.ModelRuntimeEpoch
	authority, err = f.signer.Sign(authority)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: authority, ExecutionSpec: runtimeExecutionSpec()})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("prepare before drain: %v %v", prepared, err)
	}
	started, err := client.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: authority})
	if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("start before drain: %v %v", started, err)
	}
	backend.MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/dit.bin"}`))
	sealed, err := client.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: authority})
	if err != nil || sealed.GetReceipt() == nil {
		t.Fatalf("seal and drain: %v %v", sealed, err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	config.ExecutionFloor.State.Initialize = false
	journal, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
	if err != nil || journal.PendingExecutions != 0 || journal.RetainedExecutions != 1 || journal.BackendLifecycle != incarnation {
		t.Fatalf("execution drain changed backend ownership: %+v %v", journal, err)
	}
	config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, journal, nil)
	recovered, err := modelruntime.StartRuntimeServer(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Close(); err != nil || calls != 2 {
		t.Fatalf("drained Stage execution authorized replacement models: %v calls=%d", err, calls)
	}
}

func TestRuntimeServerBackendStartupPersistenceFailureDoesNotDispatch(t *testing.T) {
	for _, boundary := range []string{"renamed-not-synced", "synced-error", "synced-canceled"} {
		t.Run(boundary, func(t *testing.T) {
			config := journalRuntimeServerConfig(t)
			factory, calls := config.BackendFactory, 0
			config.BackendFactory = func(ctx context.Context, runtime modelruntime.LaunchRuntime, binding stageauthority.RuntimeBinding, backend modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
				calls++
				return factory(ctx, runtime, binding, backend)
			}
			injected := errors.New("test startup persistence interrupted")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			hooks := 0
			server, err := modelruntime.StartRuntimeServerWithStateSyncHookForTest(ctx, config, func(sync func() error) error {
				hooks++
				if boundary == "renamed-not-synced" {
					return injected
				}
				if err := sync(); err != nil {
					return err
				}
				if boundary == "synced-canceled" {
					cancel()
					return nil
				}
				return injected
			})
			wantErr := injected
			if boundary == "synced-canceled" {
				wantErr = context.Canceled
			}
			if server != nil || !errors.Is(err, wantErr) || hooks != 1 || calls != 0 {
				t.Fatalf("incomplete persistence dispatched a backend: server=%v err=%v hooks=%d calls=%d", server, err, hooks, calls)
			}
			config.ExecutionFloor.State.Initialize = false
			journal, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
			if err != nil || journal.BackendLifecycle.State != modelruntime.BackendLifecycleUnresolved || journal.PendingExecutions != 0 {
				t.Fatalf("interrupted intent was lost or became execution history: %+v %v", journal, err)
			}
			recovered, err := modelruntime.StartRuntimeServer(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			if err := recovered.Close(); err != nil || calls != 0 {
				t.Fatalf("recovery reused an interrupted startup grant: %v calls=%d", err, calls)
			}
		})
	}
}

func TestRuntimeServerRejectsMalformedBackendLifecycleBeforeFactories(t *testing.T) {
	for _, fault := range []string{"missing", "unknown-state", "zero-id", "wrong-id-version", "zero-digest", "zero-time", "unstarted-with-history", "legacy-with-history"} {
		t.Run(fault, func(t *testing.T) {
			config := journalRuntimeServerConfig(t)
			server, err := modelruntime.StartRuntimeServer(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			if err := server.Close(); err != nil {
				t.Fatal(err)
			}
			config.ExecutionFloor.State.Initialize = false
			state := readDurableExecutionState(t, config.ExecutionFloor.State.Directory)
			switch fault {
			case "missing":
				state.BackendLifecycle = nil
			case "unknown-state":
				state.BackendLifecycle.State = "RETIRED"
			case "zero-id":
				state.BackendLifecycle.IncarnationID = uuid.Nil
			case "wrong-id-version":
				state.BackendLifecycle.IncarnationID = uuid.NewSHA1(uuid.NameSpaceDNS, []byte("invalid-incarnation"))
			case "zero-digest":
				state.BackendLifecycle.LaunchDigest = [sha256.Size]byte{}
			case "zero-time":
				state.BackendLifecycle.RecordedAt = time.Time{}
			case "unstarted-with-history":
				state.BackendLifecycle.State = modelruntime.BackendLifecycleUnstarted
			case "legacy-with-history":
				state.SchemaVersion = 5
				config.ExecutionFloor.State.UpgradeV5 = true
			}
			// Marshal directly: legacy-with-history intentionally violates the old
			// schema instead of using the legitimate legacy fixture encoder.
			wire, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(config.ExecutionFloor.State.Directory, durableStateFileName)
			if err := os.WriteFile(path, wire, 0o600); err != nil {
				t.Fatal(err)
			}
			calls := 0
			config.BackendFactory = func(context.Context, modelruntime.LaunchRuntime, stageauthority.RuntimeBinding, modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
				calls++
				return modelruntime.NewFakeEncoderRuntime(), nil
			}
			if recovered, err := modelruntime.StartRuntimeServer(t.Context(), config); err == nil || recovered != nil || calls != 0 {
				t.Fatalf("malformed lifecycle permitted startup: err=%v calls=%d", err, calls)
			}
			if after, err := os.ReadFile(path); err != nil || !bytes.Equal(wire, after) {
				t.Fatalf("failed recovery rewrote malformed lifecycle evidence: %v", err)
			}
		})
	}
}

package modelruntime_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestRuntimeServerRejectsUnavailableJournalBeforeEpochOrBackendStartup(t *testing.T) {
	for _, fault := range []string{"missing", "corrupt", "locked", "wrong-scope", "bootstrap-again", "conflicting-upgrade"} {
		t.Run(fault, func(t *testing.T) {
			root := t.TempDir()
			manifest := runtimeServerManifest(root)
			stateRoot := filepath.Join(root, "admission")
			if err := os.Mkdir(stateRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			validator, err := stageauthority.NewValidator(map[string][]byte{"authority-v1": make([]byte, 32)}, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			epochCalls, backendCalls := 0, 0
			config := modelruntime.RuntimeServerConfig{
				Manifest: manifest, Validator: validator, CancelTimeout: time.Second,
				SocketPath:     filepath.Join(privateSocketRoot(t), "runtime.sock"),
				ExecutionFloor: &modelruntime.ExecutionFloorConfig{State: &modelruntime.ExecutionFloorStateConfig{Directory: stateRoot, Initialize: true}},
				EpochStore: modelruntime.EpochStoreFunc(func(stageauthority.RuntimeBinding) (int64, error) {
					epochCalls++
					return int64(epochCalls), nil
				}),
				BackendFactory: func(context.Context, modelruntime.LaunchRuntime, stageauthority.RuntimeBinding, modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
					backendCalls++
					return modelruntime.NewFakeEncoderRuntime(), nil
				},
			}
			server, err := modelruntime.StartRuntimeServer(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = server.Close() })
			if fault != "locked" {
				if err := server.Close(); err != nil {
					t.Fatal(err)
				}
			}
			config.SocketPath = filepath.Join(privateSocketRoot(t), "second.sock")
			config.ExecutionFloor.State.Initialize = false
			switch fault {
			case "missing":
				if err := os.Remove(filepath.Join(stateRoot, "execution-admission.json")); err != nil {
					t.Fatal(err)
				}
			case "corrupt":
				if err := os.WriteFile(filepath.Join(stateRoot, "execution-admission.json"), []byte("{}"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "wrong-scope":
				config.Manifest.WorkerInstanceEpoch++
			case "bootstrap-again":
				config.ExecutionFloor.State.Initialize = true
			case "conflicting-upgrade":
				config.ExecutionFloor.State.UpgradeV2, config.ExecutionFloor.State.UpgradeV3 = true, true
			}
			epochCalls, backendCalls = 0, 0
			second, err := modelruntime.StartRuntimeServer(t.Context(), config)
			if second != nil {
				_ = second.Close()
			}
			if err == nil || second != nil || epochCalls != 0 || backendCalls != 0 {
				t.Fatalf("journal rejection occurred after startup: error=%v epoch calls=%d backend calls=%d", err, epochCalls, backendCalls)
			}
			if _, err := os.Lstat(config.SocketPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("rejected startup published socket: %v", err)
			}
		})
	}
}

func TestRuntimeServerHoldsJournalLockAcrossEpochAllocationAndWarmup(t *testing.T) {
	config := journalRuntimeServerConfig(t)
	competitor := config
	competitor.SocketPath = filepath.Join(privateSocketRoot(t), "competitor.sock")
	competitor.ExecutionFloor = &modelruntime.ExecutionFloorConfig{State: &modelruntime.ExecutionFloorStateConfig{
		Directory: config.ExecutionFloor.State.Directory,
	}}
	competitorCalls := 0
	competitor.EpochStore = modelruntime.EpochStoreFunc(func(stageauthority.RuntimeBinding) (int64, error) {
		competitorCalls++
		return 1, nil
	})
	competitor.BackendFactory = func(context.Context, modelruntime.LaunchRuntime, stageauthority.RuntimeBinding, modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
		competitorCalls++
		return modelruntime.NewFakeEncoderRuntime(), nil
	}
	assertLocked := func() {
		t.Helper()
		second, err := modelruntime.StartRuntimeServer(t.Context(), competitor)
		if second != nil {
			_ = second.Close()
		}
		if err == nil || second != nil || competitorCalls != 0 {
			t.Fatalf("competitor entered startup: %v calls=%d", err, competitorCalls)
		}
	}
	epochStore, factory := config.EpochStore, config.BackendFactory
	epochCalls, backendCalls := 0, 0
	config.EpochStore = modelruntime.EpochStoreFunc(func(binding stageauthority.RuntimeBinding) (int64, error) {
		epochCalls++
		assertLocked()
		return epochStore.Next(binding)
	})
	config.BackendFactory = func(ctx context.Context, runtime modelruntime.LaunchRuntime, binding stageauthority.RuntimeBinding, backendConfig modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
		backendCalls++
		assertLocked()
		if _, err := os.Lstat(config.SocketPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("socket published during warmup: %v", err)
		}
		return factory(ctx, runtime, binding, backendConfig)
	}
	server, err := modelruntime.StartRuntimeServer(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if epochCalls != 2 || backendCalls != 2 {
		t.Fatalf("AUX startup incomplete: epochs=%d backends=%d", epochCalls, backendCalls)
	}
	assertLocked()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	config.ExecutionFloor.State.Initialize = false
	recovered, err := modelruntime.StartRuntimeServer(t.Context(), config)
	if err != nil {
		t.Fatalf("ordinary restart after shutdown: %v", err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	assertLocked()
}

func TestRuntimeServerRejectsJournalReplacementDuringWarmup(t *testing.T) {
	for _, replacement := range []string{"directory", "state", "lock", "content"} {
		for _, warmup := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/runtime-%d", replacement, warmup), func(t *testing.T) {
				config := journalRuntimeServerConfig(t)
				root := config.ExecutionFloor.State.Directory
				factory, epochStore := config.BackendFactory, config.EpochStore
				calls, epochs := 0, 0
				config.EpochStore = modelruntime.EpochStoreFunc(func(binding stageauthority.RuntimeBinding) (int64, error) {
					epochs++
					return epochStore.Next(binding)
				})
				config.BackendFactory = func(ctx context.Context, runtime modelruntime.LaunchRuntime, binding stageauthority.RuntimeBinding, backendConfig modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
					calls++
					if calls == warmup {
						switch replacement {
						case "directory":
							if err := os.Rename(root, root+".retained"); err != nil {
								t.Fatal(err)
							}
							if err := os.Mkdir(root, 0o700); err != nil {
								t.Fatal(err)
							}
						case "state", "lock":
							name := durableStateFileName
							if replacement == "lock" {
								name = durableStateLockName
							}
							path := filepath.Join(root, name)
							data, err := os.ReadFile(path)
							if err != nil {
								t.Fatal(err)
							}
							if err := os.Rename(path, path+".retained"); err != nil {
								t.Fatal(err)
							}
							if err := os.WriteFile(path, data, 0o600); err != nil {
								t.Fatal(err)
							}
						case "content":
							if err := os.WriteFile(filepath.Join(root, durableStateFileName), []byte("{}"), 0o600); err != nil {
								t.Fatal(err)
							}
						}
					}
					return factory(ctx, runtime, binding, backendConfig)
				}
				server, err := modelruntime.StartRuntimeServer(t.Context(), config)
				if server != nil {
					_ = server.Close()
				}
				if server != nil || err == nil || epochs != warmup || calls != warmup {
					t.Fatalf("journal replacement did not stop startup: %v epochs=%d backends=%d", err, epochs, calls)
				}
				if _, err := os.Lstat(config.SocketPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("replaced journal published socket: %v", err)
				}
			})
		}
	}
}

func TestRuntimeServerReleasesJournalAfterCleanStartupRollback(t *testing.T) {
	for _, failure := range []string{"epoch", "backend", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			config := journalRuntimeServerConfig(t)
			epochStore, factory := config.EpochStore, config.BackendFactory
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			first := newLifecycleBackend(modelruntime.NewFakeEncoderRuntime(), nil)
			epochCalls, backendCalls := 0, 0
			config.EpochStore = modelruntime.EpochStoreFunc(func(binding stageauthority.RuntimeBinding) (int64, error) {
				epochCalls++
				if failure == "epoch" && epochCalls == 2 {
					return 0, errTestRuntimeStartup
				}
				return epochStore.Next(binding)
			})
			config.BackendFactory = func(context.Context, modelruntime.LaunchRuntime, stageauthority.RuntimeBinding, modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
				backendCalls++
				if backendCalls == 1 {
					return first, nil
				}
				if failure == "cancel" {
					cancel(errTestRuntimeStartup)
					return modelruntime.NewFakeVAERuntime(), nil
				}
				return nil, errTestRuntimeStartup
			}
			server, err := modelruntime.StartRuntimeServer(ctx, config)
			if server != nil {
				_ = server.Close()
			}
			if server != nil || !errors.Is(err, errTestRuntimeStartup) {
				t.Fatalf("startup failure: %v", err)
			}
			select {
			case <-first.Done():
			default:
				t.Fatal("rollback did not close the started backend")
			}
			if _, err := os.Lstat(config.SocketPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed startup published socket: %v", err)
			}
			config.ExecutionFloor.State.Initialize = false
			config.EpochStore, config.BackendFactory = epochStore, factory
			recovered, err := modelruntime.StartRuntimeServer(t.Context(), config)
			if err != nil {
				t.Fatalf("rollback did not release journal for recovery: %v", err)
			}
			t.Cleanup(func() { _ = recovered.Close() })
		})
	}
}

func TestRuntimeServerCancellationBeforeStartupDoesNotInitializeJournal(t *testing.T) {
	config := journalRuntimeServerConfig(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	server, err := modelruntime.StartRuntimeServer(ctx, config)
	if server != nil {
		_ = server.Close()
	}
	if server != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup: %v", err)
	}
	entries, err := os.ReadDir(config.ExecutionFloor.State.Directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("canceled startup changed journal directory: %v %v", entries, err)
	}
}

func TestRuntimeServerRecoversExistingSupervisorJournalBeforeNewEpochs(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Now())
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
	if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
		t.Fatal(err)
	}
	if err := f.supervisor.Shutdown(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(directory, durableStateFileName))
	if err != nil {
		t.Fatal(err)
	}
	config := recoveredRuntimeServerConfig(t, f, directory)
	binding := f.bindings[0]
	history, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
	if err != nil || history.Highest != 10 || history.Floor != 11 || history.RetainedExecutions != 1 || history.PendingExecutions != 1 {
		t.Fatalf("offline preparation lost retained history: %+v %v", history, err)
	}
	config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, history, nil)
	factory := config.BackendFactory
	backendCalls := 0
	config.BackendFactory = func(ctx context.Context, runtime modelruntime.LaunchRuntime, binding stageauthority.RuntimeBinding, backendConfig modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
		backendCalls++
		return factory(ctx, runtime, binding, backendConfig)
	}
	server, err := modelruntime.StartRuntimeServer(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	if backendCalls != 0 {
		t.Fatalf("pending historical writers allowed replacement backend startup: calls=%d", backendCalls)
	}
	client, err := modelruntimetransport.Dial(t.Context(), modelruntimetransport.Config{SocketPath: config.SocketPath, ExpectedUID: uint32(os.Geteuid())})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	binding.ModelRuntimeEpoch = 10
	identity := discoverExecutionFloorIdentity(t, client, binding)
	identities, err := stageworkeragent.DiscoverRuntimeIdentities(t.Context(), client, stageworkeragent.RuntimeIdentityExpectation{
		WorkerInstanceID: binding.WorkerInstanceID, WorkerInstanceEpoch: binding.WorkerInstanceEpoch,
		WorkerMemberID: binding.WorkerMemberID, WorkerMemberEpoch: binding.WorkerMemberEpoch,
		RegistryVerifier: config.RegistryVerifier, RegistryBinding: config.RegistryBinding,
	})
	if err != nil || len(identities) != len(config.Manifest.Runtimes) {
		t.Fatalf("pending recovery lost verified journal discovery: %v %v", identities, err)
	}
	if identity.GetModelRuntimeEpoch() != 10 {
		t.Fatalf("replacement runtime epoch: %v", identity)
	}
	response, err := client.ProbeReadiness(t.Context(), &velav1.ModelRuntimeServiceProbeReadinessRequest{
		Identity: identity, Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP,
	})
	if err != nil || response.GetReady() {
		t.Fatalf("pending historical writer advertised readiness: %v %v", response, err)
	}
	query := f.authority(t, 0, 11)
	query.Members[0].ModelRuntimeEpoch = 10
	query, err = f.signer.Sign(query)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: query, ExecutionSpec: runtimeExecutionSpec()})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("new epoch bypassed retained floor: %v %v", prepared, err)
	}
	after, err := os.ReadFile(filepath.Join(directory, durableStateFileName))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("startup rewrote existing journal evidence: %v", err)
	}
}

func journalRuntimeServerConfig(t *testing.T) modelruntime.RuntimeServerConfig {
	t.Helper()
	root := t.TempDir()
	stateRoot := filepath.Join(root, "admission")
	if err := os.Mkdir(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	validator, err := stageauthority.NewValidator(map[string][]byte{"authority-v1": make([]byte, 32)}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	epochStore, err := modelruntime.NewFileEpochStore(filepath.Join(root, "epochs"))
	if err != nil {
		t.Fatal(err)
	}
	return modelruntime.RuntimeServerConfig{
		Manifest: runtimeServerManifest(root), Validator: validator, CancelTimeout: time.Second,
		SocketPath: filepath.Join(privateSocketRoot(t), "runtime.sock"), EpochStore: epochStore,
		ExecutionFloor: &modelruntime.ExecutionFloorConfig{State: &modelruntime.ExecutionFloorStateConfig{Directory: stateRoot, Initialize: true}},
		BackendFactory: func(_ context.Context, runtime modelruntime.LaunchRuntime, _ stageauthority.RuntimeBinding, _ modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
			if runtime.Component == "ENCODER" {
				return modelruntime.NewFakeEncoderRuntime(), nil
			}
			return modelruntime.NewFakeVAERuntime(), nil
		},
	}
}

func recoveredRuntimeServerConfig(t *testing.T, f *executionFloorFixture, directory string) modelruntime.RuntimeServerConfig {
	t.Helper()
	config := journalRuntimeServerConfig(t)
	config.ExecutionFloor.State = &modelruntime.ExecutionFloorStateConfig{Directory: directory}
	config.Validator = f.validator
	manifest, binding := &config.Manifest, f.bindings[0]
	manifest.WorkerInstanceEpoch, manifest.WorkerMemberEpoch = binding.WorkerInstanceEpoch, binding.WorkerMemberEpoch
	manifest.DeviceSetDigest, manifest.MembershipDigest = hex.EncodeToString(binding.DeviceSetDigest), hex.EncodeToString(binding.MembershipDigest)
	manifest.Devices[0].Epoch, manifest.LocalDevices[0].DeviceEpoch = binding.Devices[0].Epoch, binding.Devices[0].Epoch
	manifest.Members[0].Epoch = binding.WorkerMemberEpoch
	manifest.Members[0].IdentityDigest, manifest.Members[0].DeviceSubsetDigest = repeatHex("6"), hex.EncodeToString(bytes.Repeat([]byte{0x67}, 32))
	for i, binding := range f.bindings {
		manifest.Runtimes[i].RuntimeIdentity, manifest.Runtimes[i].ModelResidencyID = binding.ModelRuntimeIdentity, binding.ModelResidencyID
		manifest.Runtimes[i].StageProfileRevisionID, manifest.Runtimes[i].ModelRuntimeEpochFloor = binding.StageProfileRevisionID, binding.ModelRuntimeEpoch
	}
	return config
}

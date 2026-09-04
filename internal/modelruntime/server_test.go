package modelruntime_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errTestRuntimeCrashed = errors.New("test resident runtime crashed")
var errTestRuntimeShutdown = errors.New("test resident runtime shutdown failed")
var errTestRuntimeStartup = errors.New("test resident runtime startup failed")

func TestStartRuntimeServerPublishesPrivateSocketAfterEveryRuntimeIsReady(t *testing.T) {
	root := t.TempDir()
	temporaryRoot, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		t.Fatalf("resolve temporary root: %v", err)
	}
	socketRoot, err := os.MkdirTemp(temporaryRoot, "vela-rt-")
	if err != nil {
		t.Fatalf("create socket root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	manifest := runtimeServerManifest(root)
	epochStore, err := modelruntime.NewFileEpochStore(filepath.Join(root, "epochs"))
	if err != nil {
		t.Fatalf("NewFileEpochStore: %v", err)
	}
	validator, err := stageauthority.NewValidator(
		map[string][]byte{"authority-v1": make([]byte, 32)}, time.Now,
	)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}

	releaseSecondRuntime := make(chan struct{})
	var factoryMu sync.Mutex
	factoryCalls := 0
	result := make(chan struct {
		server *modelruntime.RuntimeServer
		err    error
	}, 1)
	go func() {
		server, startErr := modelruntime.StartRuntimeServer(
			context.Background(),
			modelruntime.RuntimeServerConfig{
				Manifest: manifest, EpochStore: epochStore, Validator: validator,
				SocketPath: filepath.Join(socketRoot, "runtime.sock"), CancelTimeout: 5 * time.Second,
				BackendFactory: func(
					_ context.Context,
					runtime modelruntime.LaunchRuntime,
					_ stageauthority.RuntimeBinding,
					_ modelruntime.ProcessBackendConfig,
				) (modelruntime.Backend, error) {
					factoryMu.Lock()
					factoryCalls++
					call := factoryCalls
					factoryMu.Unlock()
					if call == 2 {
						<-releaseSecondRuntime
					}
					if runtime.Component == "ENCODER" {
						return modelruntime.NewFakeEncoderRuntime(), nil
					}
					return modelruntime.NewFakeVAERuntime(), nil
				},
			},
		)
		result <- struct {
			server *modelruntime.RuntimeServer
			err    error
		}{server: server, err: startErr}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case started := <-result:
			t.Fatalf("StartRuntimeServer returned before all runtime factories were ready: %v", started.err)
		default:
		}
		factoryMu.Lock()
		calls := factoryCalls
		factoryMu.Unlock()
		if calls == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("resident runtime factories did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	socketPath := filepath.Join(socketRoot, "runtime.sock")
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runtime socket was published before warmup completed: %v", err)
	}
	close(releaseSecondRuntime)
	started := <-result
	if started.err != nil {
		t.Fatalf("StartRuntimeServer: %v", started.err)
	}
	t.Cleanup(func() { _ = started.server.Close() })

	info, err := os.Lstat(socketPath)
	if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 {
		t.Fatalf("runtime socket = %#v error=%v", info, err)
	}
	dialContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := modelruntimetransport.Dial(dialContext, modelruntimetransport.Config{
		SocketPath: socketPath, ExpectedUID: uint32(os.Geteuid()),
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()
	discovered, err := client.DiscoverRuntimeIdentities(
		context.Background(),
		&velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
			WorkerInstanceId: manifest.WorkerInstanceID, WorkerInstanceEpoch: manifest.WorkerInstanceEpoch,
			WorkerMemberId: manifest.WorkerMemberID, WorkerMemberEpoch: manifest.WorkerMemberEpoch,
		},
	)
	if err != nil || len(discovered.GetIdentities()) != 2 ||
		discovered.GetIdentities()[0].GetModelRuntimeEpoch() != 1 ||
		discovered.GetIdentities()[1].GetModelRuntimeEpoch() != 1 {
		t.Fatalf("DiscoverRuntimeIdentities = %#v error=%v", discovered, err)
	}
}

func TestStartRuntimeServerRejectsInvalidClockSkew(t *testing.T) {
	root := t.TempDir()
	epochStore, err := modelruntime.NewFileEpochStore(filepath.Join(root, "epochs"))
	if err != nil {
		t.Fatalf("NewFileEpochStore: %v", err)
	}
	validator, err := stageauthority.NewValidator(
		map[string][]byte{"authority-v1": make([]byte, 32)}, time.Now,
	)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	for _, maxClockSkew := range []time.Duration{-time.Nanosecond, time.Minute + time.Nanosecond} {
		t.Run(maxClockSkew.String(), func(t *testing.T) {
			server, startErr := modelruntime.StartRuntimeServer(
				context.Background(),
				modelruntime.RuntimeServerConfig{
					Manifest: runtimeServerManifest(root), EpochStore: epochStore, Validator: validator,
					SocketPath: filepath.Join(root, "runtime.sock"), CancelTimeout: 5 * time.Second,
					MaxClockSkew: maxClockSkew,
				},
			)
			if server != nil || startErr == nil || startErr.Error() != "ModelRuntime server clock skew is invalid" {
				t.Fatalf("StartRuntimeServer skew=%s server=%v error=%v", maxClockSkew, server, startErr)
			}
		})
	}
}

func TestRuntimeServerPropagatesClockSkewToServices(t *testing.T) {
	root := t.TempDir()
	socketRoot := privateSocketRoot(t)
	manifest := runtimeServerManifest(root)
	manifest.WorkerRole = "encoder"
	manifest.SharedSlotException = ""
	manifest.Runtimes = manifest.Runtimes[:1]
	now := time.Date(2026, 9, 4, 10, 30, 0, 0, time.UTC)
	keys := map[string][]byte{"authority-v1": bytes.Repeat([]byte{0x31}, 32)}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	validator, err := stageauthority.NewValidator(keys, func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	epochStore, err := modelruntime.NewFileEpochStore(filepath.Join(root, "epochs"))
	if err != nil {
		t.Fatalf("NewFileEpochStore: %v", err)
	}
	socketPath := filepath.Join(socketRoot, "runtime.sock")
	server, err := modelruntime.StartRuntimeServer(context.Background(), modelruntime.RuntimeServerConfig{
		Manifest: manifest, EpochStore: epochStore, Validator: validator,
		SocketPath: socketPath, CancelTimeout: 5 * time.Second, MaxClockSkew: 30 * time.Second,
		BackendFactory: func(
			context.Context,
			modelruntime.LaunchRuntime,
			stageauthority.RuntimeBinding,
			modelruntime.ProcessBackendConfig,
		) (modelruntime.Backend, error) {
			return modelruntime.NewFakeEncoderRuntime(), nil
		},
	})
	if err != nil {
		t.Fatalf("StartRuntimeServer: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	dialContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := modelruntimetransport.Dial(dialContext, modelruntimetransport.Config{
		SocketPath: socketPath, ExpectedUID: uint32(os.Geteuid()),
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()
	discovered, err := client.DiscoverRuntimeIdentities(
		context.Background(),
		&velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
			WorkerInstanceId: manifest.WorkerInstanceID, WorkerInstanceEpoch: manifest.WorkerInstanceEpoch,
			WorkerMemberId: manifest.WorkerMemberID, WorkerMemberEpoch: manifest.WorkerMemberEpoch,
		},
	)
	if err != nil || len(discovered.GetIdentities()) != 1 {
		t.Fatalf("DiscoverRuntimeIdentities = %#v error=%v", discovered, err)
	}
	spec := &velav1.StageExecutionSpec{ParametersJson: []byte(`{"frames":1}`)}
	withinSkew := runtimeServerAuthority(
		t,
		signer,
		manifest,
		discovered.GetIdentities()[0].GetModelRuntimeEpoch(),
		now.Add(3200*time.Microsecond),
		spec,
	)
	prepared, err := client.PrepareStage(
		context.Background(),
		&velav1.ModelRuntimeServicePrepareStageRequest{Authority: withinSkew, ExecutionSpec: spec},
	)
	if err != nil || prepared.GetDecision() !=
		velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("within-skew PrepareStage = %#v error=%v", prepared, err)
	}
	overSkew := runtimeServerAuthority(
		t,
		signer,
		manifest,
		discovered.GetIdentities()[0].GetModelRuntimeEpoch(),
		now.Add(30*time.Second+time.Millisecond),
		spec,
	)
	rejected, err := client.PrepareStage(
		context.Background(),
		&velav1.ModelRuntimeServicePrepareStageRequest{Authority: overSkew, ExecutionSpec: spec},
	)
	if err != nil || rejected.GetDecision() !=
		velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("over-skew PrepareStage = %#v error=%v", rejected, err)
	}
}

func runtimeServerAuthority(
	t *testing.T,
	signer *stageauthority.Signer,
	manifest modelruntime.LaunchManifest,
	modelRuntimeEpoch int64,
	issuedAt time.Time,
	spec *velav1.StageExecutionSpec,
) *velav1.StageAuthority {
	t.Helper()
	digest, err := stageauthority.ExecutionSpecDigest(spec)
	if err != nil {
		t.Fatalf("ExecutionSpecDigest: %v", err)
	}
	runtime := manifest.Runtimes[0]
	authority, err := signer.Sign(&velav1.StageAuthority{
		SchemaVersion:       1,
		JobId:               "11000000-0000-0000-0000-000000000001",
		AttemptId:           "11000000-0000-0000-0000-000000000002",
		StageRunId:          "11000000-0000-0000-0000-000000000003",
		StageAttemptId:      "11000000-0000-0000-0000-000000000004",
		StageAllocationId:   "11000000-0000-0000-0000-000000000005",
		StageLeaseId:        "11000000-0000-0000-0000-000000000006",
		AttemptFence:        1,
		StageFence:          2,
		StageVersion:        3,
		WorkerInstanceId:    manifest.WorkerInstanceID,
		WorkerInstanceEpoch: manifest.WorkerInstanceEpoch,
		DeviceSetDigest:     bytes.Repeat([]byte{0xaa}, 32),
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: manifest.Devices[0].ID, DeviceEpoch: manifest.Devices[0].Epoch,
		}},
		MembershipDigest: bytes.Repeat([]byte{0xbb}, 32),
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: manifest.WorkerMemberID, MemberEpoch: manifest.WorkerMemberEpoch,
			ModelRuntimeEpoch: modelRuntimeEpoch, IdentityDigest: bytes.Repeat([]byte{0x61}, 32),
		}},
		ModelResidencyId: runtime.ModelResidencyID, ModelRuntimeIdentity: runtime.RuntimeIdentity,
		ModelRuntimeBarrierGeneration: modelRuntimeEpoch,
		StageProfileRevisionId:        runtime.StageProfileRevisionID,
		CapacityObservationSequence:   1,
		CapacityVector:                map[string]int64{"active_stage_slots": 1},
		LeaseToken:                    bytes.Repeat([]byte{0x62}, 32),
		ExecutionNonce:                bytes.Repeat([]byte{0x63}, 32),
		ExecutionSpecDigest:           digest[:],
		SigningKeyId:                  "authority-v1",
		IssuedAt:                      timestamppb.New(issuedAt),
		ExpiresAt:                     timestamppb.New(issuedAt.Add(5 * time.Minute)),
		MonotonicValidFor:             durationpb.New(5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("Sign StageAuthority: %v", err)
	}
	return authority
}

func TestRuntimeServerStopsPublishingWhenResidentBackendExits(t *testing.T) {
	root := t.TempDir()
	socketRoot := privateSocketRoot(t)
	manifest := runtimeServerManifest(root)
	backend := newLifecycleBackend(modelruntime.NewFakeEncoderRuntime(), nil)
	server := startTestRuntimeServer(t, manifest, socketRoot, func(
		_ context.Context,
		runtime modelruntime.LaunchRuntime,
		_ stageauthority.RuntimeBinding,
		_ modelruntime.ProcessBackendConfig,
	) (modelruntime.Backend, error) {
		if runtime.Component == "ENCODER" {
			return backend, nil
		}
		return modelruntime.NewFakeVAERuntime(), nil
	})
	backend.exit(errTestRuntimeCrashed)
	if err := server.Wait(); !errors.Is(err, errTestRuntimeCrashed) {
		t.Fatalf("Wait error = %v, want resident runtime crash", err)
	}
	if err := server.Close(); !errors.Is(err, errTestRuntimeCrashed) {
		t.Fatalf("Close error = %v, want resident runtime crash", err)
	}
	if _, err := os.Lstat(filepath.Join(socketRoot, "runtime.sock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale runtime socket remains after backend exit: %v", err)
	}
}

func TestRuntimeServerReportsResidentBackendShutdownFailure(t *testing.T) {
	root := t.TempDir()
	socketRoot := privateSocketRoot(t)
	manifest := runtimeServerManifest(root)
	manifest.WorkerRole = "encoder"
	manifest.SharedSlotException = ""
	manifest.Runtimes = manifest.Runtimes[:1]
	backend := newLifecycleBackend(modelruntime.NewFakeEncoderRuntime(), errTestRuntimeShutdown)
	server := startTestRuntimeServer(t, manifest, socketRoot, func(
		context.Context,
		modelruntime.LaunchRuntime,
		stageauthority.RuntimeBinding,
		modelruntime.ProcessBackendConfig,
	) (modelruntime.Backend, error) {
		return backend, nil
	})
	if err := server.Close(); !errors.Is(err, errTestRuntimeShutdown) {
		t.Fatalf("Close error = %v, want backend shutdown failure", err)
	}
}

func TestStartRuntimeServerAbortsWarmupWhenEarlierBackendExits(t *testing.T) {
	root := t.TempDir()
	socketRoot := privateSocketRoot(t)
	manifest := runtimeServerManifest(root)
	epochStore, err := modelruntime.NewFileEpochStore(filepath.Join(root, "epochs"))
	if err != nil {
		t.Fatalf("NewFileEpochStore: %v", err)
	}
	validator, err := stageauthority.NewValidator(
		map[string][]byte{"authority-v1": make([]byte, 32)}, time.Now,
	)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	encoder := newLifecycleBackend(modelruntime.NewFakeEncoderRuntime(), nil)
	vaeWarmupStarted := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, startErr := modelruntime.StartRuntimeServer(context.Background(), modelruntime.RuntimeServerConfig{
			Manifest: manifest, EpochStore: epochStore, Validator: validator,
			SocketPath: filepath.Join(socketRoot, "runtime.sock"), CancelTimeout: 5 * time.Second,
			BackendFactory: func(
				ctx context.Context,
				runtime modelruntime.LaunchRuntime,
				_ stageauthority.RuntimeBinding,
				_ modelruntime.ProcessBackendConfig,
			) (modelruntime.Backend, error) {
				if runtime.Component == "ENCODER" {
					return encoder, nil
				}
				close(vaeWarmupStarted)
				<-ctx.Done()
				return nil, context.Cause(ctx)
			},
		})
		result <- startErr
	}()
	<-vaeWarmupStarted
	encoder.exit(errTestRuntimeCrashed)
	if startErr := <-result; !errors.Is(startErr, errTestRuntimeCrashed) {
		t.Fatalf("StartRuntimeServer error = %v, want earlier backend crash", startErr)
	}
	if _, err := os.Lstat(filepath.Join(socketRoot, "runtime.sock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runtime socket was published after readiness loss: %v", err)
	}
}

func TestStartRuntimeServerReportsStartupAndRollbackFailures(t *testing.T) {
	root := t.TempDir()
	socketRoot := privateSocketRoot(t)
	manifest := runtimeServerManifest(root)
	epochStore, err := modelruntime.NewFileEpochStore(filepath.Join(root, "epochs"))
	if err != nil {
		t.Fatalf("NewFileEpochStore: %v", err)
	}
	validator, err := stageauthority.NewValidator(
		map[string][]byte{"authority-v1": make([]byte, 32)}, time.Now,
	)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	encoder := newLifecycleBackend(modelruntime.NewFakeEncoderRuntime(), errTestRuntimeShutdown)
	_, startErr := modelruntime.StartRuntimeServer(context.Background(), modelruntime.RuntimeServerConfig{
		Manifest: manifest, EpochStore: epochStore, Validator: validator,
		SocketPath: filepath.Join(socketRoot, "runtime.sock"), CancelTimeout: 5 * time.Second,
		BackendFactory: func(
			_ context.Context,
			runtime modelruntime.LaunchRuntime,
			_ stageauthority.RuntimeBinding,
			_ modelruntime.ProcessBackendConfig,
		) (modelruntime.Backend, error) {
			if runtime.Component == "ENCODER" {
				return encoder, nil
			}
			return nil, errTestRuntimeStartup
		},
	})
	if !errors.Is(startErr, errTestRuntimeStartup) || !errors.Is(startErr, errTestRuntimeShutdown) {
		t.Fatalf("StartRuntimeServer error = %v, want startup and rollback failures", startErr)
	}
}

type lifecycleBackend struct {
	*modelruntime.FakeRuntime
	done     chan struct{}
	closeErr error
	err      error
	once     sync.Once
}

func newLifecycleBackend(runtime *modelruntime.FakeRuntime, closeErr error) *lifecycleBackend {
	return &lifecycleBackend{FakeRuntime: runtime, done: make(chan struct{}), closeErr: closeErr}
}

func (backend *lifecycleBackend) Done() <-chan struct{} { return backend.done }

func (backend *lifecycleBackend) Err() error { return backend.err }

func (backend *lifecycleBackend) Close() error {
	backend.exit(nil)
	return backend.closeErr
}

func (backend *lifecycleBackend) exit(err error) {
	backend.once.Do(func() {
		backend.err = err
		close(backend.done)
	})
}

func privateSocketRoot(t *testing.T) string {
	t.Helper()
	temporaryRoot, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		t.Fatalf("resolve temporary root: %v", err)
	}
	root, err := os.MkdirTemp(temporaryRoot, "vela-rt-")
	if err != nil {
		t.Fatalf("create socket root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func startTestRuntimeServer(
	t *testing.T,
	manifest modelruntime.LaunchManifest,
	socketRoot string,
	factory modelruntime.RuntimeBackendFactory,
) *modelruntime.RuntimeServer {
	t.Helper()
	epochStore, err := modelruntime.NewFileEpochStore(filepath.Join(t.TempDir(), "epochs"))
	if err != nil {
		t.Fatalf("NewFileEpochStore: %v", err)
	}
	validator, err := stageauthority.NewValidator(
		map[string][]byte{"authority-v1": make([]byte, 32)}, time.Now,
	)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	server, err := modelruntime.StartRuntimeServer(context.Background(), modelruntime.RuntimeServerConfig{
		Manifest: manifest, EpochStore: epochStore, Validator: validator,
		SocketPath: filepath.Join(socketRoot, "runtime.sock"), CancelTimeout: 5 * time.Second,
		BackendFactory: factory,
	})
	if err != nil {
		t.Fatalf("StartRuntimeServer: %v", err)
	}
	return server
}

func runtimeServerManifest(root string) modelruntime.LaunchManifest {
	return modelruntime.LaunchManifest{
		SchemaVersion:           1,
		WorkerProfileRevisionID: "71000000-0000-0000-0000-000000000001",
		WorkerRole:              "aux", CapacitySlots: 1, SharedSlotException: "H3_AUX_ENCODER_VAE",
		WorkerInstanceID: "21000000-0000-0000-0000-000000000001", WorkerInstanceEpoch: 7,
		WorkerMemberID: "41000000-0000-0000-0000-000000000001", WorkerMemberEpoch: 11,
		DeviceSetDigest: repeatHex("a"), MembershipDigest: repeatHex("b"),
		Devices: []modelruntime.LaunchDeviceEpoch{{ID: "31000000-0000-0000-0000-000000000001", Epoch: 13}},
		Members: []modelruntime.LaunchMemberEpoch{{ID: "41000000-0000-0000-0000-000000000001", Epoch: 11}},
		LocalDevices: []modelruntime.DriverDevice{{
			DeviceID: "31000000-0000-0000-0000-000000000001", DeviceEpoch: 13,
			GPUUUID: "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0",
		}},
		Runtimes: []modelruntime.LaunchRuntime{
			runtimeServerLaunchRuntime(root, "51000000-0000-0000-0000-000000000001", "encoder-runtime", "61000000-0000-0000-0000-000000000001", "ENCODER"),
			runtimeServerLaunchRuntime(root, "51000000-0000-0000-0000-000000000002", "vae-runtime", "61000000-0000-0000-0000-000000000002", "VAE_DECODER"),
		},
	}
}

func runtimeServerLaunchRuntime(root, residency, identity, profile, component string) modelruntime.LaunchRuntime {
	return modelruntime.LaunchRuntime{
		ModelResidencyID: residency, RuntimeIdentity: identity, StageProfileRevisionID: profile,
		Component: component, ModelComponentRevision: "minimax-h3-" + component + "-r1",
		RuntimeImageDigest: repeatHex("c"), Command: []string{"/usr/local/bin/minimax-h3-driver"},
		ScratchRoot: root, InputRoot: filepath.Join(root, "inputs"),
		OutputRoot:            filepath.Join(root, identity+"-outputs"),
		InitializationTimeout: "2h", ShutdownTimeout: "2m",
	}
}

func repeatHex(value string) string {
	result := ""
	for len(result) < 64 {
		result += value
	}
	return result
}

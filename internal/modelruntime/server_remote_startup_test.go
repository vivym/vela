package modelruntime_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"
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
	"google.golang.org/protobuf/proto"
)

func remoteRuntimeServerFixture(t *testing.T) (modelruntime.RuntimeServerConfig, *modelruntime.ExecutionJournalOwner, string) {
	t.Helper()
	config := journalRuntimeServerConfig(t)
	routes, err := modelruntime.RemoteStartupBindings(config.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	directory := config.ExecutionFloor.State.Directory
	owner, err := modelruntime.OpenExecutionJournalOwner(modelruntime.ExecutionJournalOwnerConfig{Manifest: config.Manifest, Validator: config.Validator,
		State: *config.ExecutionFloor.State, Routes: routes, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	startup, err := owner.RecordBackendStartupIntent(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	status, err := owner.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, status, nil)
	identity := modelruntime.ExecutionJournalIdentity{JournalID: status.JournalID, Scope: status.Scope, Storage: status.Storage}
	config.RemoteStartup = &modelruntime.RemoteRuntimeStartup{Journal: modelruntime.RemoteExecutionJournalConfig{
		Manifest: config.Manifest, Validator: config.Validator, Identity: identity, Startup: startup,
		Transport: &directJournalTransport{owner: owner, identity: identity, role: modelruntime.JournalRuntimeRole}, Timeout: time.Second},
		Authorize: func(context.Context, modelruntime.RemoteBackendStartupRequest) error { return nil }}
	config.EpochStore, config.ExecutionFloor = nil, nil
	return config, owner, directory
}

func TestRuntimeServerRemoteStartupBeforeFactories(t *testing.T) {
	for _, scenario := range []string{"permit", "deny", "cancel-after-permit", "changed-after-permit", "changed-after-first-factory", "gate-alias-mutation"} {
		t.Run(scenario, func(t *testing.T) {
			config, owner, directory := remoteRuntimeServerFixture(t)
			manifestWire, err := modelruntime.EncodeLaunchManifest(config.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			bindingWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(config.RegistryBinding)
			if err != nil {
				t.Fatal(err)
			}
			routes, err := modelruntime.RemoteStartupBindings(config.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			gateCalls := 0
			var backends []*floorBlockingBackend
			changeJournal := func() {
				path := filepath.Join(directory, durableStateFileName)
				wire, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, append(wire, ' '), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			config.RemoteStartup.Authorize = func(ctx context.Context, request modelruntime.RemoteBackendStartupRequest) error {
				gateCalls++
				if gateCalls != 1 {
					return errors.New("fixture denies repeated permission for original incarnation")
				}
				status, err := owner.Status(ctx)
				if err != nil || len(backends) != 0 || request.Intent.Validate() != nil || request.Intent.JournalID != status.JournalID ||
					request.Intent.JournalScope != status.Scope || request.JournalIdentity.Storage != status.Storage || request.Intent.IncarnationID != status.BackendLifecycle.IncarnationID ||
					request.Intent.LaunchDigest != sha256.Sum256(manifestWire) || request.Intent.RegistryBindingDigest != sha256.Sum256(bindingWire) ||
					!reflect.DeepEqual(request.Bindings, routes) {
					t.Fatalf("remote authorization did not precede factories with exact owner/epochs: %+v %v", request, err)
				}
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 45*time.Second {
					t.Fatal("unbounded authorizer context")
				}
				switch scenario {
				case "deny":
					return errors.New("fixture denies Node permission")
				case "cancel-after-permit":
					cancel()
				case "changed-after-permit":
					changeJournal()
				case "gate-alias-mutation":
					request.Bindings[0].ModelRuntimeEpoch++
					request.Bindings[0].DeviceSetDigest[0] ^= 1
					config.RemoteStartup.Journal.Manifest.Runtimes[0].ModelRuntimeEpochFloor++
				}
				return nil
			}
			config.BackendFactory = func(_ context.Context, runtime modelruntime.LaunchRuntime, binding stageauthority.RuntimeBinding, _ modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
				if gateCalls != 1 || !reflect.DeepEqual(binding, routes[len(backends)]) {
					t.Fatalf("factory used unapproved epoch/config: %+v", binding)
				}
				backend := &floorBlockingBackend{FakeRuntime: modelruntime.NewFakeEncoderRuntime(), resume: make(chan struct{})}
				if runtime.Component == "VAE" {
					backend.FakeRuntime = modelruntime.NewFakeVAERuntime()
				}
				backends = append(backends, backend)
				if scenario == "changed-after-first-factory" {
					changeJournal()
				}
				return backend, nil
			}
			server, err := modelruntime.StartRuntimeServer(ctx, config)
			if scenario == "permit" || scenario == "gate-alias-mutation" {
				if err != nil || server == nil || len(backends) != len(routes) || gateCalls != 1 {
					t.Fatalf("authorized remote startup: factories=%d gate=%d err=%v", len(backends), gateCalls, err)
				}
				client, err := modelruntimetransport.Dial(ctx, modelruntimetransport.Config{SocketPath: config.SocketPath, ExpectedUID: uint32(os.Getuid())})
				if err != nil {
					t.Fatal(err)
				}
				discovery, err := client.DiscoverRuntimeIdentities(ctx, &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
					WorkerInstanceId: routes[0].WorkerInstanceID, WorkerInstanceEpoch: routes[0].WorkerInstanceEpoch,
					WorkerMemberId: routes[0].WorkerMemberID, WorkerMemberEpoch: routes[0].WorkerMemberEpoch})
				if err != nil || len(discovery.GetIdentities()) != len(routes) || !proto.Equal(discovery.GetJournalBinding(), config.RegistryBinding) {
					t.Fatalf("remote server failed Registry-bound discovery: %v %v", discovery, err)
				}
				_ = client.Close()
				if err := server.Close(); err != nil {
					t.Fatal(err)
				}
				if scenario == "permit" {
					if retry, err := modelruntime.StartRuntimeServer(ctx, config); err == nil || retry != nil || gateCalls != 2 || len(backends) != len(routes) {
						t.Fatalf("server bypassed repeated Node permission refusal: %v %v", retry, err)
					}
				}
			} else if err == nil || server != nil || (scenario == "changed-after-first-factory" && len(backends) != 1) ||
				(scenario != "changed-after-first-factory" && len(backends) != 0) {
				t.Fatalf("failed remote startup entered more factories: factories=%d err=%v", len(backends), err)
			}
			for _, backend := range backends {
				if !backend.closed.Load() {
					t.Fatal("startup rollback/server close leaked backend")
				}
			}
			if _, err := os.Lstat(config.SocketPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed/closed remote startup left socket: %v", err)
			}
		})
	}
}

func TestRuntimeServerRemoteStartupRejectsMixedCustody(t *testing.T) {
	for _, fault := range []string{"local-epoch", "local-state", "local-gate", "missing-gate", "missing-registry", "signature", "manifest", "verifier", "incarnation", "journal", "overflow"} {
		t.Run(fault, func(t *testing.T) {
			config, _, _ := remoteRuntimeServerFixture(t)
			calls := 0
			config.BackendFactory = func(context.Context, modelruntime.LaunchRuntime, stageauthority.RuntimeBinding, modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
				calls++
				return modelruntime.NewFakeEncoderRuntime(), nil
			}
			switch fault {
			case "local-epoch":
				config.EpochStore = modelruntime.EpochStoreFunc(func(stageauthority.RuntimeBinding) (int64, error) {
					t.Fatal("remote startup allocated local epoch")
					return 0, nil
				})
			case "local-state":
				config.ExecutionFloor = &modelruntime.ExecutionFloorConfig{}
			case "local-gate":
				config.BackendStartupGate = func(context.Context, modelruntime.BackendStartupRequest) error {
					t.Fatal("remote startup borrowed local gate")
					return nil
				}
			case "missing-gate":
				config.RemoteStartup.Authorize = nil
			case "missing-registry":
				config.RegistryBinding, config.RegistryVerifier = nil, nil
			case "signature":
				config.RegistryBinding.Signature[0] ^= 1
			case "manifest":
				config.RemoteStartup.Journal.Manifest.DeviceSetDigest = repeatHex("8")
			case "verifier":
				config.RemoteStartup.Journal.Validator = nil
			case "incarnation":
				config.RemoteStartup.Journal.Startup.IncarnationID = uuid.New()
			case "journal":
				config.RemoteStartup.Journal.Identity.JournalID = uuid.New()
			case "overflow":
				config.Manifest.Runtimes[0].ModelRuntimeEpochFloor = math.MaxInt64
			}
			if server, err := modelruntime.StartRuntimeServer(t.Context(), config); err == nil || server != nil || calls != 0 {
				t.Fatalf("invalid remote configuration entered factory: server=%v err=%v calls=%d", server, err, calls)
			}
		})
	}
}

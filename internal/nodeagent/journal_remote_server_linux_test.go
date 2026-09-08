package nodeagent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func journalEndpointRunRemoteServer(t *testing.T, socket string, request journalEndpointControl, decoder *json.Decoder, encoder *json.Encoder) {
	t.Helper()
	validator, err := stageauthority.NewValidator(map[string][]byte{"journal-test": bytes.Repeat([]byte{73}, 32)}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	seed := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{17}, ed25519.SeedSize))
	verifier, err := journalbinding.NewVerifier(map[string][]byte{"registry": seed.Public().(ed25519.PublicKey)})
	if err != nil {
		t.Fatal(err)
	}
	var binding velav1.WorkerBootstrapBinding
	if err := proto.Unmarshal(request.RegistryBinding, &binding); err != nil {
		t.Fatal(err)
	}
	var backend *journalEndpointBackend
	factories := 0
	server, err := modelruntime.StartRuntimeServer(t.Context(), modelruntime.RuntimeServerConfig{
		Manifest: *request.Manifest, Validator: validator, SocketPath: request.RuntimeSocket, CancelTimeout: time.Second,
		RegistryBinding: &binding, RegistryVerifier: verifier,
		RemoteStartup: &modelruntime.RemoteRuntimeStartup{Journal: modelruntime.RemoteExecutionJournalConfig{
			Manifest: *request.Manifest, Validator: validator, Identity: request.Identity, Startup: request.Startup,
			Transport: modelruntime.UnixRuntimeJournalTransport{Socket: socket, Identity: request.Identity}, Timeout: 5 * time.Second},
			Authorize: func(_ context.Context, intent modelruntime.RemoteBackendStartupRequest) error {
				// This is an explicit fixture authorizer over inherited control
				// pipes, not a production Fleet/Registry permission issuer.
				journalBarrierReport(t, encoder, journalEndpointReport{RemoteStartup: &intent, FactoryCalls: factories})
				var decision journalEndpointControl
				if err := decoder.Decode(&decision); err != nil || !decision.AllowRemoteStartup {
					return errors.New("parent Node fixture denied remote startup")
				}
				return nil
			}},
		BackendFactory: func(context.Context, modelruntime.LaunchRuntime, stageauthority.RuntimeBinding, modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
			factories++
			backend = &journalEndpointBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime()}
			return backend, nil
		},
	})
	if err != nil {
		journalBarrierReport(t, encoder, journalEndpointReport{Error: err.Error(), FactoryCalls: factories})
		return
	}
	defer func() { _ = server.Close() }()
	journalBarrierReport(t, encoder, journalEndpointReport{SupervisorReady: true, FactoryCalls: factories})
	for {
		var control journalEndpointControl
		if err := decoder.Decode(&control); err != nil {
			t.Fatal(err)
		}
		switch control.SupervisorAction {
		case "output":
			backend.MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/native.bin"}`))
		case "close":
			if err := server.Close(); err != nil {
				t.Fatal(err)
			}
			journalBarrierReport(t, encoder, journalEndpointReport{SupervisorCompleted: true, FactoryCalls: factories})
			return
		default:
			t.Fatalf("unknown remote server action: %q", control.SupervisorAction)
		}
		journalBarrierReport(t, encoder, journalEndpointReport{FactoryCalls: factories, PrepareCalls: backend.prepareCalls.Load(), StartCalls: backend.startCalls.Load()})
	}
}

func TestJournalServerStartsRemoteRuntimeBeforeActualWorkerExecution(t *testing.T) {
	for _, allow := range []bool{true, false} {
		t.Run(map[bool]string{true: "permit", false: "deny"}[allow], func(t *testing.T) {
			f := newJournalEndpointFixtureWithEpochOffset(t, 1)
			server, done := startJournalTestServer(t, f, 30*time.Second)
			directory := filepath.Join(filepath.Dir(f.listener.Addr().String()), "workload")
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chown(directory, 65532, 65532); err != nil {
				t.Fatal(err)
			}
			binding := journalRemoteServerBinding(t, f)
			socket := filepath.Join(directory, "runtime.sock")
			request := journalServerRequest(t, f.runtime, journalEndpointControl{RemoteServer: true, Identity: f.identity, Manifest: &f.manifest,
				Startup: f.startup, RuntimeSocket: socket, RegistryBinding: binding})
			routes, err := modelruntime.RemoteStartupBindings(f.manifest)
			if err != nil {
				t.Fatal(err)
			}
			intent := request.RemoteStartup
			if intent == nil || request.FactoryCalls != 0 || intent.Intent.Validate() != nil || intent.JournalIdentity != f.identity ||
				intent.Intent.IncarnationID != f.startup.IncarnationID || intent.Intent.LaunchDigest != f.startup.LaunchDigest ||
				intent.Intent.RegistryBindingDigest != sha256.Sum256(binding) || !reflect.DeepEqual(intent.Bindings, routes) {
				t.Fatalf("remote server called factory before exact Node declaration: %+v", request)
			}
			if _, err := os.Lstat(socket); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("Runtime published RPC socket before permission: %v", err)
			}
			result := journalServerRequest(t, f.runtime, journalEndpointControl{AllowRemoteStartup: allow})
			if !allow {
				if result.Error == "" || result.FactoryCalls != 0 || result.SupervisorReady {
					t.Fatalf("denied server started backend: %+v", result)
				}
			} else {
				if !result.SupervisorReady || result.FactoryCalls != 1 || result.Error != "" {
					t.Fatalf("permitted server did not start exactly one backend: %+v", result)
				}
				started := journalServerRequest(t, f.worker, journalEndpointControl{WorkerSocket: socket, WorkerAction: "execute", Authority: f.authority, Identity: f.identity})
				if started.Error != "" || started.Barrier == nil || !started.Barrier.BarrierPassed {
					t.Fatalf("actual Worker could not execute on remotely started server: %+v", started)
				}
				calls := journalServerRequest(t, f.runtime, journalEndpointControl{SupervisorAction: "output"})
				if calls.PrepareCalls != 1 || calls.StartCalls != 1 || calls.FactoryCalls != 1 {
					t.Fatalf("remote server repeated backend calls: %+v", calls)
				}
				journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: "seal", Authority: f.authority})
				if report := journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: "drain", Authority: f.authority}); !report.Checkpoint {
					t.Fatal("actual remote server did not persist exact drain")
				}
				journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: "close", Authority: f.authority})
				if report := journalServerRequest(t, f.runtime, journalEndpointControl{SupervisorAction: "close"}); !report.SupervisorCompleted {
					t.Fatalf("remote server did not close: %+v", report)
				}
			}
			status, err := f.owner.Status(t.Context())
			expectedHighest := int64(0)
			if allow {
				expectedHighest = 1
			}
			if err != nil || status.Highest != expectedHighest || status.PendingExecutions != 0 || status.BackendLifecycle != f.startup {
				t.Fatalf("remote server changed Node lifecycle or lost drain: %+v %v", status, err)
			}
			entries, err := os.ReadDir(directory)
			if err != nil || len(entries) != 0 {
				t.Fatalf("Runtime created local journal/epoch files or retained socket: %v %v", entries, err)
			}
			if err := server.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			assertJournalServerJoined(t, server, done)
			t.Logf("actual remote Runtime server: permit=%v; factory permission precedes RPC publication; Node owns journal; fixture matches exact proposed epochs; Worker uses real RPC", allow)
		})
	}
}

func journalRemoteServerBinding(t *testing.T, f journalEndpointFixture) []byte {
	t.Helper()
	signer, err := journalbinding.NewSigner("registry", bytes.Repeat([]byte{17}, ed25519.SeedSize))
	if err != nil {
		t.Fatal(err)
	}
	request := uuid.NewString()
	binding, err := signer.Sign(&velav1.WorkerBootstrapBinding{SchemaVersion: journalbinding.SchemaVersion,
		Claim: &velav1.WorkerBootstrapClaim{RequestId: request, WorkerInstanceId: f.manifest.WorkerInstanceID, WorkerInstanceEpoch: f.manifest.WorkerInstanceEpoch,
			WorkerMemberId: f.manifest.WorkerMemberID, WorkerMemberEpoch: f.manifest.WorkerMemberEpoch, NodeIdentity: "cpu-node", ActorIdentity: "node-agent/cpu-node",
			BundleDigest: bytes.Repeat([]byte{1}, 32), ClaimedAt: timestamppb.Now()},
		Pair: &velav1.WorkerBootstrapJournalPair{RequestId: request, ActorIdentity: "node-agent/cpu-node", WorkerJournalId: uuid.NewString(), WorkerScope: bytes.Repeat([]byte{2}, 32),
			RuntimeJournalId: f.identity.JournalID.String(), RuntimeScope: f.identity.Scope[:], RecordedAt: timestamppb.Now()}})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

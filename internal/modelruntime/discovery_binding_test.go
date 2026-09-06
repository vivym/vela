package modelruntime_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestRuntimeDiscoveryRequiresHeldRegistryBoundState(t *testing.T) {
	for _, mode := range []string{"bound", "unbound-journal", "nondurable"} {
		t.Run(mode, func(t *testing.T) {
			config := journalRuntimeServerConfig(t)
			journal, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
			if err != nil {
				t.Fatal(err)
			}
			config.ExecutionFloor.State.Initialize = false
			binding, verifier := runtimeRegistryBinding(t, config, journal, nil)
			switch mode {
			case "bound":
				config.RegistryBinding, config.RegistryVerifier = binding, verifier
			case "nondurable":
				config.ExecutionFloor = nil
			}
			server, err := modelruntime.StartRuntimeServer(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = server.Close() })
			client := dialDiscoveryRuntime(t, config.SocketPath)
			expected := stageworkeragent.RuntimeIdentityExpectation{
				WorkerInstanceID: config.Manifest.WorkerInstanceID, WorkerInstanceEpoch: config.Manifest.WorkerInstanceEpoch,
				WorkerMemberID: config.Manifest.WorkerMemberID, WorkerMemberEpoch: config.Manifest.WorkerMemberEpoch,
			}
			if identities, err := stageworkeragent.DiscoverRuntimeIdentities(t.Context(), client, expected); err != nil || len(identities) != 2 {
				t.Fatalf("ordinary discovery compatibility: %v %v", identities, err)
			}
			expected.RegistryVerifier, expected.RegistryBinding = verifier, binding
			identities, err := stageworkeragent.DiscoverRuntimeIdentities(t.Context(), client, expected)
			if mode == "bound" {
				if err != nil || len(identities) != 2 {
					t.Fatalf("held signed journal was rejected: %v %v", identities, err)
				}
			} else if err == nil || identities != nil {
				t.Fatalf("unbound Runtime passed durable discovery: %v %v", identities, err)
			}
		})
	}
}

func TestRuntimeDiscoveryRechecksJournalOwnershipAndRetainsFailure(t *testing.T) {
	for _, fault := range []string{"state", "replacement", "lock", "directory"} {
		t.Run(fault, func(t *testing.T) {
			config := journalRuntimeServerConfig(t)
			journal, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
			if err != nil {
				t.Fatal(err)
			}
			config.ExecutionFloor.State.Initialize = false
			config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, journal, nil)
			want := proto.Clone(config.RegistryBinding).(*velav1.WorkerBootstrapBinding)
			server, err := modelruntime.StartRuntimeServer(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = server.Close() })
			client := dialDiscoveryRuntime(t, config.SocketPath)
			request := &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
				WorkerInstanceId: config.Manifest.WorkerInstanceID, WorkerInstanceEpoch: config.Manifest.WorkerInstanceEpoch,
				WorkerMemberId: config.Manifest.WorkerMemberID, WorkerMemberEpoch: config.Manifest.WorkerMemberEpoch,
			}
			// Neither the caller's startup configuration nor a previous response can
			// change the Registry evidence retained by the serving Runtime.
			config.RegistryBinding.Signature[0] ^= 1
			for range 2 {
				response, err := client.DiscoverRuntimeIdentities(t.Context(), request)
				if err != nil || !proto.Equal(response.GetJournalBinding(), want) {
					t.Fatalf("bound discovery: %v %v", response, err)
				}
				response.JournalBinding.Signature[0] ^= 1
			}
			path := config.ExecutionFloor.State.Directory
			switch fault {
			case "state", "replacement":
				path = filepath.Join(path, durableStateFileName)
			case "lock":
				path = filepath.Join(path, durableStateLockName)
			}
			if err := os.Rename(path, path+".retained"); err != nil {
				t.Fatal(err)
			}
			if fault == "replacement" {
				wire, err := os.ReadFile(path + ".retained")
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, wire, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			for attempt := range 2 {
				response, err := client.DiscoverRuntimeIdentities(t.Context(), request)
				if response != nil || status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("lost journal advertised ownership: %v %v", response, err)
				}
				if attempt == 0 {
					if err := os.Rename(path+".retained", path); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestRuntimeDiscoveryRejectsClosedSupervisorAndCanceledContext(t *testing.T) {
	f := durableExecutionFixture(t, privateExecutionStateDirectory(t), true, "", 9, time.Time{})
	request := &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
		WorkerInstanceId: f.bindings[0].WorkerInstanceID, WorkerInstanceEpoch: f.bindings[0].WorkerInstanceEpoch,
		WorkerMemberId: f.bindings[0].WorkerMemberID, WorkerMemberEpoch: f.bindings[0].WorkerMemberEpoch,
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if response, err := f.supervisor.DiscoverRuntimeIdentities(ctx, request); response != nil || status.Code(err) != codes.Canceled {
		t.Fatalf("canceled discovery returned identities: %v %v", response, err)
	}
	if err := f.supervisor.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if response, err := f.supervisor.DiscoverRuntimeIdentities(t.Context(), request); response != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("closed supervisor returned identities: %v %v", response, err)
	}
}

func dialDiscoveryRuntime(t *testing.T, socket string) *modelruntimetransport.Client {
	t.Helper()
	client, err := modelruntimetransport.Dial(t.Context(), modelruntimetransport.Config{SocketPath: socket, ExpectedUID: uint32(os.Geteuid())})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

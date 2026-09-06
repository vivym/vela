//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/workerjournal"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWorkerBootstrapBindingCommandUsesCommittedRegistryIdentity(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "vela-node-agent")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/vela-node-agent")
	build.Dir = repositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Node Agent: %s %v", output, err)
	}
	database, service, request := newWorkerBootstrapFixture(t)
	seed := bytes.Repeat([]byte{19}, ed25519.SeedSize)
	signer, err := journalbinding.NewSigner("registry", seed)
	if err != nil {
		t.Fatal(err)
	}
	publicKeys := map[string][]byte{"registry": ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)}
	verifier, err := journalbinding.NewVerifier(publicKeys)
	if err != nil {
		t.Fatal(err)
	}
	var rejectReceipt, dropBinding, dropped atomic.Bool
	var claimCalls, receiptCalls atomic.Int32
	rejectReceipt.Store(true)
	clients := bootstrapMutualTLSClients(t, service, func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == velav1.FleetMaintenanceService_ClaimWorkerBootstrap_FullMethodName {
			claimCalls.Add(1)
		}
		if info.FullMethod == velav1.FleetMaintenanceService_RecordWorkerBootstrapReceipt_FullMethodName {
			receiptCalls.Add(1)
			if rejectReceipt.Load() {
				return nil, status.Error(codes.Unavailable, "receipt not committed")
			}
		}
		response, err := handler(ctx, req)
		if err == nil && info.FullMethod == velav1.FleetMaintenanceService_LookupWorkerBootstrapBinding_FullMethodName && dropBinding.Load() && dropped.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "binding response lost")
		}
		return response, err
	}, signer)
	preparation, scratch := workerBootstrapCommandFiles(t, request)
	invoke := func(principal int, args ...string) ([]byte, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		arguments := append([]string{"bootstrap"}, clients[principal].arguments...)
		command := exec.CommandContext(ctx, binary, append(arguments, args...)...)
		command.Env = append(os.Environ(), "VELA_NODE_AGENT_ID=", "VELA_NODE_AGENT_NVIDIA_SMI_PATH=/nonexistent")
		var stdout, stderr bytes.Buffer
		command.Stdout, command.Stderr = &stdout, &stderr
		err := command.Run()
		if err != nil {
			if stdout.Len() != 0 {
				t.Fatalf("rejected binding command printed success: %s", stdout.Bytes())
			}
			t.Logf("rejected command: %s", stderr.Bytes())
		}
		return stdout.Bytes(), err
	}
	if _, err := invoke(0, preparation...); err == nil {
		t.Fatal("preparation reported success without committed receipt")
	}
	var operation struct {
		RequestID uuid.UUID `json:"request_id"`
	}
	operationWire, err := os.ReadFile(filepath.Join(scratch, "bootstrap", "operation.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(operationWire, &operation); err != nil || operation.RequestID == uuid.Nil {
		t.Fatalf("read retained operation: %v", err)
	}
	lookup := &velav1.LookupWorkerBootstrapBindingRequest{RequestId: operation.RequestID.String()}
	if _, err := clients[0].rpc.LookupWorkerBootstrapBinding(t.Context(), lookup); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("signed pending bootstrap: %v", err)
	}
	publicPath := filepath.Join(t.TempDir(), "binding-verifiers.json")
	keyWire, err := json.Marshal(publicKeys)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(publicPath, keyWire, 0o600); err != nil {
		t.Fatal(err)
	}
	arguments := []string{"--action", "binding", "--request-id", operation.RequestID.String(), "--binding-verifier-keyring-file", publicPath}
	if _, err := invoke(0, arguments...); err == nil {
		t.Fatal("command accepted pending receipt")
	}
	rejectReceipt.Store(false)
	if _, err := invoke(0, preparation...); err != nil {
		t.Fatal(err)
	}
	history, err := clients[0].bootstrap.LookupWorkerBootstrap(t.Context(), operation.RequestID)
	if err != nil || history.Receipt == nil {
		t.Fatalf("receipt was not committed: %v", err)
	}
	registrySnapshot := func() string {
		t.Helper()
		var value string
		if err := database.Admin.QueryRow("SELECT json_agg(t ORDER BY request_id)::text FROM worker_bootstrap_claims t").Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	localSnapshot := func() map[string]string {
		t.Helper()
		result := make(map[string]string)
		if err := filepath.WalkDir(scratch, func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			wire, err := os.ReadFile(path)
			result[path] = string(wire)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return result
	}
	registryBefore, localBefore := registrySnapshot(), localSnapshot()
	for index, code := range map[int]codes.Code{1: codes.NotFound, 2: codes.NotFound, 3: codes.Unauthenticated} {
		if _, err := clients[index].rpc.LookupWorkerBootstrapBinding(t.Context(), lookup); status.Code(err) != code {
			t.Fatalf("binding exposed to another principal: %v", err)
		}
		if _, err := invoke(index, arguments...); err == nil {
			t.Fatal("unauthorized command obtained binding")
		}
	}
	dropBinding.Store(true)
	if _, err := invoke(0, arguments...); err == nil || !dropped.Load() {
		t.Fatal("binding command ignored response loss")
	}
	first, err := invoke(0, arguments...)
	if err != nil {
		t.Fatal(err)
	}
	bindingPath := filepath.Join(t.TempDir(), "binding.json")
	if err := os.WriteFile(bindingPath, first, 0o600); err != nil {
		t.Fatal(err)
	}
	binding, err := journalbinding.LoadFile(bindingPath, verifier)
	if err != nil || binding.GetClaim().GetRequestId() != operation.RequestID.String() ||
		binding.GetPair().GetWorkerJournalId() != history.Receipt.WorkerJournalID.String() ||
		binding.GetPair().GetRuntimeJournalId() != history.Receipt.RuntimeJournalID.String() ||
		!binding.GetPair().GetRecordedAt().AsTime().Equal(history.RecordedAt) {
		t.Fatalf("command did not preserve signed Registry identity: %v", err)
	}
	if replay, err := invoke(0, arguments...); err != nil || !bytes.Equal(replay, first) {
		t.Fatalf("replayed binding changed identity: %v", err)
	}
	publicKeys["registry"][0] ^= 1
	wrongKeys, err := json.Marshal(publicKeys)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(publicPath, wrongKeys, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := invoke(0, arguments...); err == nil {
		t.Fatal("command trusted server response without its configured public key")
	}
	if registrySnapshot() != registryBefore || !reflect.DeepEqual(localBefore, localSnapshot()) || claimCalls.Load() != 1 || receiptCalls.Load() != 2 {
		t.Fatal("binding lookup mutated Registry or local state, or repeated bootstrap mutations")
	}
	startRegistryBoundCPURuntime(t, preparation, scratch, binding, verifier)
	if registrySnapshot() != registryBefore || !reflect.DeepEqual(localBefore, localSnapshot()) {
		t.Fatal("idle bound CPU startup changed Registry or journal history")
	}
}

func startRegistryBoundCPURuntime(t *testing.T, preparation []string, scratch string, binding *velav1.WorkerBootstrapBinding, verifier *journalbinding.Verifier) {
	t.Helper()
	var launchPath string
	for index, argument := range preparation {
		if argument == "--launch-manifest-file" {
			launchPath = preparation[index+1]
		}
	}
	manifest, err := modelruntime.LoadLaunchManifest(launchPath)
	if err != nil {
		t.Fatal(err)
	}
	// Match the local mount mapping used by explicit Node bootstrap.
	for index := range manifest.Runtimes {
		manifest.Runtimes[index].ScratchRoot = scratch
		manifest.Runtimes[index].InputRoot = filepath.Join(scratch, "inputs")
		manifest.Runtimes[index].OutputRoot = filepath.Join(scratch, "outputs")
	}
	keys, err := stageauthority.DeriveVerifierKeyring(map[string][]byte{"bootstrap-key": bytes.Repeat([]byte{3}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	defer stageauthority.ClearKeyring(keys)
	validator, err := stageauthority.NewVerifier(keys, nil)
	if err != nil {
		t.Fatal(err)
	}
	epochStore, err := modelruntime.NewFileEpochStore(filepath.Join(t.TempDir(), "epochs"))
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	socketRoot, err := os.MkdirTemp(root, "vela-bind-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	state := modelruntime.ExecutionFloorStateConfig{Directory: filepath.Join(scratch, "runtime-admission")}
	workerConfig, err := workerjournal.AssignmentConfig(manifest, stageworkeragent.AssignmentAdmissionConfig{
		Directory: filepath.Join(scratch, "worker-admission"), MaxRecords: 4, Validator: validator,
		RegistryBinding: binding, RegistryVerifier: verifier, DeferRuntimeRoutes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := stageworkeragent.NewFileAssignmentAdmission(workerConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = worker.Close() })
	server, err := modelruntime.StartRuntimeServer(t.Context(), modelruntime.RuntimeServerConfig{
		Manifest: manifest, EpochStore: epochStore, Validator: validator, SocketPath: filepath.Join(socketRoot, "runtime.sock"), CancelTimeout: time.Second,
		ExecutionFloor: &modelruntime.ExecutionFloorConfig{State: &state}, RegistryBinding: binding, RegistryVerifier: verifier,
		BackendFactory: func(context.Context, modelruntime.LaunchRuntime, stageauthority.RuntimeBinding, modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
			if _, err := modelruntime.PrepareExecutionJournal(t.Context(), manifest, validator, state); err == nil {
				t.Fatal("bound journal lock was released before CPU backend startup")
			}
			if _, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), workerConfig); err == nil {
				t.Fatal("bound Worker journal was released during Runtime startup")
			}
			return modelruntime.NewFakeDiTRuntime(), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	client, err := modelruntimetransport.Dial(t.Context(), modelruntimetransport.Config{SocketPath: filepath.Join(socketRoot, "runtime.sock"), ExpectedUID: uint32(os.Geteuid())})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	identities, err := stageworkeragent.DiscoverRuntimeIdentities(t.Context(), client, stageworkeragent.RuntimeIdentityExpectation{
		WorkerInstanceID: manifest.WorkerInstanceID, WorkerInstanceEpoch: manifest.WorkerInstanceEpoch,
		WorkerMemberID: manifest.WorkerMemberID, WorkerMemberEpoch: manifest.WorkerMemberEpoch,
		RegistryBinding: binding, RegistryVerifier: verifier,
	})
	if err != nil || len(identities) != 1 || identities[0].GetModelRuntimeEpoch() != 2 {
		t.Fatalf("Registry-bound CPU runtime identity: %v %v", identities, err)
	}
	routes, err := manifest.RuntimeBindings()
	if err != nil {
		t.Fatal(err)
	}
	routes[0].ModelRuntimeEpoch = identities[0].GetModelRuntimeEpoch()
	current := workerConfig.Bindings[0]
	current.Runtime = routes[0]
	if err := worker.BindRuntimeRoutes(t.Context(), []stageworkeragent.AdmissionRuntimeBinding{current}); err != nil {
		t.Fatalf("bind actual Runtime discovery to recorded Worker journal: %v", err)
	}
	if _, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), workerConfig); err == nil {
		t.Fatal("Runtime discovery reopened the bound Worker journal")
	}
	ready, err := client.ProbeReadiness(t.Context(), &velav1.ModelRuntimeServiceProbeReadinessRequest{
		Identity: identities[0], Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP,
	})
	if err != nil || !ready.GetReady() {
		t.Fatalf("fresh bound CPU runtime was not warm: %v %v", ready, err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := worker.Close(); err != nil {
		t.Fatal(err)
	}
	workerJournal, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), workerConfig)
	if err != nil || workerJournal.JournalID.String() != binding.GetPair().GetWorkerJournalId() || !bytes.Equal(workerJournal.Scope[:], binding.GetPair().GetWorkerScope()) {
		t.Fatalf("CPU startup replaced bound Worker journal: %+v %v", workerJournal, err)
	}
	recovered, err := modelruntime.PrepareExecutionJournal(t.Context(), manifest, validator, state)
	if err != nil || recovered.JournalID.String() != binding.GetPair().GetRuntimeJournalId() || !bytes.Equal(recovered.Scope[:], binding.GetPair().GetRuntimeScope()) {
		t.Fatalf("CPU startup replaced bound journal: %+v %v", recovered, err)
	}
}

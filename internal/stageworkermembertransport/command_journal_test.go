package stageworkermembertransport

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMemberCommandsRetainWorkerJournalAcrossRuntimeCall(t *testing.T) {
	for _, operation := range []string{"prepare", "start", "status"} {
		for _, outcome := range []string{"success", "runtime-error", "canceled", "replaced"} {
			t.Run(operation+"/"+outcome, func(t *testing.T) {
				f := newServerFixture(t, time.Time{})
				gate, config := commandWorkerJournal(t, f, true)
				f.server.workerJournal = gate
				entered, resume := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(resume) }) }
				defer unblock()
				f.server.runtime = &commandRuntimeHook{ModelRuntimeServiceClient: f.runtime, call: func() error {
					close(entered)
					<-resume
					if outcome == "runtime-error" {
						return status.Error(codes.Unavailable, "injected Runtime failure")
					}
					return nil
				}}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- runMemberCommand(ctx, f, operation) }()
				select {
				case <-entered:
				case err := <-done:
					t.Fatalf("forwarding rejected healthy journal: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatal("Runtime call did not start")
				}
				if err := gate.Close(); !errors.Is(err, stageworkeragent.ErrStageWorkerBusy) {
					t.Fatalf("forwarding released Worker ownership: %v", err)
				}
				if _, err := gate.InspectJournalBinding(t.Context()); err != nil {
					t.Fatalf("forwarding blocked observation: %v", err)
				}
				want := codes.OK
				switch outcome {
				case "runtime-error":
					want = codes.Unavailable
				case "canceled":
					cancel()
					want = codes.Canceled
					if err := gate.Close(); !errors.Is(err, stageworkeragent.ErrStageWorkerBusy) {
						t.Fatalf("cancellation released an unfinished call: %v", err)
					}
				case "replaced":
					replaceCommandJournalState(t, config.Directory)
					want = codes.FailedPrecondition
				}
				unblock()
				select {
				case err := <-done:
					if status.Code(err) != want {
						t.Fatalf("forwarding result: %v, want %s", err, want)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("Runtime return did not release forwarding call")
				}
				if err := gate.Close(); err != nil {
					t.Fatalf("completed forwarding leaked a reference: %v", err)
				}
			})
		}
	}
}

func TestMemberCommandsCheckAuthorizationAndJournalCapabilityBeforeRuntime(t *testing.T) {
	for _, operation := range []string{"prepare", "start", "status"} {
		for _, fault := range []string{"unauthorized", "wrong-scope", "observer-only"} {
			t.Run(operation+"/"+fault, func(t *testing.T) {
				f := newServerFixture(t, time.Time{})
				binding, _ := memberDiscoveryBinding(t, f.runtime.identity)
				guard := &commandJournalGuard{discoveryJournalObserver: discoveryJournalObserver{binding: binding}}
				f.server.workerJournal = guard
				want, retains, releases := codes.FailedPrecondition, 1, 1
				switch fault {
				case "unauthorized":
					f.auth.identity.SPIFFEID = f.localSPIFFE
					want, retains, releases = codes.PermissionDenied, 0, 0
				case "wrong-scope":
					guard.binding.Claim.WorkerMemberId = uuid.NewString()
				case "observer-only":
					f.server.workerJournal = &guard.discoveryJournalObserver
					retains, releases = 0, 0
				}
				err := runMemberCommand(t.Context(), f, operation)
				if status.Code(err) != want || guard.retains != retains || guard.releases != releases ||
					f.runtime.prepareCalls+f.runtime.startCalls+f.runtime.statusCalls != 0 {
					t.Fatalf("invalid command reached journal/Runtime: %v retains=%d releases=%d", err, guard.retains, guard.releases)
				}
			})
		}
	}
}

type commandJournalGuard struct {
	discoveryJournalObserver
	retains, releases int
}

func (guard *commandJournalGuard) RetainJournalBinding(context.Context) (*velav1.WorkerBootstrapBinding, func() error, error) {
	guard.retains++
	return guard.binding, func() error { guard.releases++; return nil }, nil
}

type commandRuntimeHook struct {
	velav1.ModelRuntimeServiceClient
	call func() error
}

func (runtime *commandRuntimeHook) PrepareStage(ctx context.Context, request *velav1.ModelRuntimeServicePrepareStageRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServicePrepareStageResponse, error) {
	if err := runtime.call(); err != nil {
		return nil, err
	}
	return runtime.ModelRuntimeServiceClient.PrepareStage(ctx, request, options...)
}

func (runtime *commandRuntimeHook) StartStage(ctx context.Context, request *velav1.ModelRuntimeServiceStartStageRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceStartStageResponse, error) {
	if err := runtime.call(); err != nil {
		return nil, err
	}
	return runtime.ModelRuntimeServiceClient.StartStage(ctx, request, options...)
}

func (runtime *commandRuntimeHook) Status(ctx context.Context, request *velav1.ModelRuntimeServiceStatusRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceStatusResponse, error) {
	if err := runtime.call(); err != nil {
		return nil, err
	}
	return runtime.ModelRuntimeServiceClient.Status(ctx, request, options...)
}

func replaceCommandJournalState(t *testing.T, directory string) {
	t.Helper()
	path := filepath.Join(directory, "assignment-admission.json")
	wire, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".retained"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, wire, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestMemberCommandsRejectUnavailableWorkerJournalBeforeRuntime(t *testing.T) {
	for _, operation := range []string{"prepare", "start", "status"} {
		for _, fault := range []string{"closed", "replaced", "unbound"} {
			t.Run(operation+"/"+fault, func(t *testing.T) {
				f := newServerFixture(t, time.Time{})
				gate, config := commandWorkerJournal(t, f, fault != "unbound")
				f.server.workerJournal = gate
				switch fault {
				case "closed":
					if err := gate.Close(); err != nil {
						t.Fatal(err)
					}
				case "replaced":
					replaceCommandJournalState(t, config.Directory)
				}
				err := runMemberCommand(t.Context(), f, operation)
				calls := f.runtime.prepareCalls + f.runtime.startCalls + f.runtime.statusCalls
				if status.Code(err) != codes.FailedPrecondition || calls != 0 {
					t.Fatalf("unavailable Worker journal reached Runtime: calls=%d error=%v", calls, err)
				}
			})
		}
	}
}

func runMemberCommand(ctx context.Context, f *serverFixture, operation string) error {
	switch operation {
	case "prepare":
		_, err := f.server.PrepareStage(ctx, &velav1.StageWorkerMemberServicePrepareStageRequest{
			TargetWorkerMemberId: f.local.ID, Command: &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority, ExecutionSpec: f.spec},
		})
		return err
	case "start":
		_, err := f.server.StartStage(ctx, &velav1.StageWorkerMemberServiceStartStageRequest{
			TargetWorkerMemberId: f.local.ID, Command: &velav1.ModelRuntimeServiceStartStageRequest{Authority: f.authority},
		})
		return err
	default:
		_, err := f.server.Status(ctx, &velav1.StageWorkerMemberServiceStatusRequest{
			TargetWorkerMemberId: f.local.ID, Command: &velav1.ModelRuntimeServiceStatusRequest{Authority: f.authority},
		})
		return err
	}
}

func commandWorkerJournal(t *testing.T, f *serverFixture, bound bool) (*stageworkeragent.FileAssignmentAdmission, stageworkeragent.AssignmentAdmissionConfig) {
	t.Helper()
	root := t.TempDir()
	for _, name := range []string{"state", "inputs", "outputs"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	a := f.authority
	config := stageworkeragent.AssignmentAdmissionConfig{
		Directory: filepath.Join(root, "state"), InputRoot: filepath.Join(root, "inputs"), OutputRoot: filepath.Join(root, "outputs"),
		WorkerInstanceID: uuid.MustParse(a.GetWorkerInstanceId()), WorkerInstanceEpoch: a.GetWorkerInstanceEpoch(),
		WorkerMemberID: uuid.MustParse(f.local.ID), MaxRecords: 4, Validator: f.server.validator, Initialize: true,
	}
	for _, member := range a.GetMembers() {
		binding := stageauthority.RuntimeBinding{
			WorkerInstanceID: a.GetWorkerInstanceId(), WorkerInstanceEpoch: a.GetWorkerInstanceEpoch(),
			WorkerMemberID: member.GetWorkerMemberId(), WorkerMemberEpoch: member.GetMemberEpoch(), ModelRuntimeEpoch: member.GetModelRuntimeEpoch(),
			DeviceSetDigest: a.GetDeviceSetDigest(), MembershipDigest: a.GetMembershipDigest(),
			ModelResidencyID: a.GetModelResidencyId(), ModelRuntimeIdentity: a.GetModelRuntimeIdentity(), StageProfileRevisionID: a.GetStageProfileRevisionId(),
		}
		for _, device := range a.GetDevices() {
			binding.Devices = append(binding.Devices, stageauthority.DeviceEpoch{ID: device.GetDeviceId(), Epoch: device.GetDeviceEpoch()})
		}
		for _, peer := range a.GetMembers() {
			binding.Members = append(binding.Members, stageauthority.MemberEpoch{ID: peer.GetWorkerMemberId(), Epoch: peer.GetMemberEpoch()})
		}
		config.Bindings = append(config.Bindings, stageworkeragent.AdmissionRuntimeBinding{
			Runtime: binding, IdentityDigest: [32]byte(member.GetIdentityDigest()), DeviceSubsetDigest: [32]byte(bytesOf('s')),
		})
	}
	journal, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	if bound {
		binding, verifier := memberDiscoveryBinding(t, f.runtime.identity)
		binding.Pair.WorkerJournalId, binding.Pair.WorkerScope = journal.JournalID.String(), journal.Scope[:]
		config.RegistryBinding, config.RegistryVerifier = signMemberDiscoveryBinding(t, binding), verifier
	}
	gate, err := stageworkeragent.NewFileAssignmentAdmission(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gate.Close() })
	return gate, config
}

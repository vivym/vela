package modelruntime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// Match the Node endpoint: domain rejection is distinct from an uncertain
// transport result, but does not disclose the journal owner's internal error.
type endpointRejectionWriter struct{ *directJournalTransport }

func (w endpointRejectionWriter) Apply(ctx context.Context, command modelruntime.JournalCommand) (modelruntime.JournalMutationReceipt, error) {
	receipt, err := w.directJournalTransport.Apply(ctx, command)
	if errors.Is(err, modelruntime.ErrExecutionNonAdmissionUnproven) {
		return receipt, modelruntime.ErrJournalRejected
	}
	return receipt, err
}

type failedJournalWriter struct{ err error }

func (w failedJournalWriter) Apply(context.Context, modelruntime.JournalCommand) (modelruntime.JournalMutationReceipt, error) {
	return modelruntime.JournalMutationReceipt{}, w.err
}

func TestJournalWorkerNonAdmissionUncertainWriteStopsBeforeRuntime(t *testing.T) {
	f, _, _, _ := remoteSupervisorFixture(t)
	// Any RPC call on this embedded nil client panics: uncertain writes must
	// not be inspected away, retried, or converted into proof.
	rpc := &struct {
		velav1.ModelRuntimeServiceClient
	}{}
	for _, failure := range []error{errors.New("lost durable reply"), context.Canceled, modelruntime.ErrExecutionStateRecovery, errors.Join(modelruntime.ErrJournalRejected, modelruntime.ErrExecutionStateRecovery)} {
		client, err := modelruntime.NewJournalWorkerClient(rpc, failedJournalWriter{failure})
		if err != nil {
			t.Fatal(err)
		}
		r, err := client.CheckpointStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: &velav1.ModelRuntimeExecutionDrainScope{Authority: f.authorities[0]}})
		if r != nil || !errors.Is(err, failure) {
			t.Fatalf("uncertain mutation bypassed: %v %v", r, err)
		}
		tr, err := client.CheckpointStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{Scope: &velav1.ModelRuntimeTerminalAllocationScope{Disposition: f.disposition(t)}})
		if tr != nil || !errors.Is(err, failure) {
			t.Fatalf("uncertain terminal mutation bypassed: %v %v", tr, err)
		}
	}
}

func TestJournalWorkerRejectedNonAdmissionStillValidatesRuntimeScope(t *testing.T) {
	f, _, _, _ := remoteSupervisorFixture(t)
	rpc, _ := serveRuntimeServer(t, f.supervisor)
	client, err := modelruntime.NewJournalWorkerClient(rpc, failedJournalWriter{modelruntime.ErrJournalRejected})
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"signature", "identity", "unknown"} {
		scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: inspectionIdentity(f.authorities[0]), Authority: proto.Clone(f.authorities[0]).(*velav1.StageAuthority)}
		r := &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: scope}
		switch fault {
		case "signature":
			scope.Authority.Signature[0] ^= 1
		case "identity":
			scope.Identity.ModelRuntimeEpoch++
		case "unknown":
			r.ProtoReflect().SetUnknown([]byte{0x78, 1})
		}
		if response, err := client.CheckpointStageNonAdmission(t.Context(), r); response != nil || err == nil {
			t.Fatalf("accepted invalid %s: %v %v", fault, response, err)
		}
	}
}

func TestJournalWorkerAdmittedExecutionNonAdmissionDoesNotBlockDrainRecovery(t *testing.T) {
	f, owner, transport, _ := remoteSupervisorFixture(t)
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
	rpc, _ := serveRuntimeServer(t, f.supervisor)
	writer := endpointRejectionWriter{&directJournalTransport{owner: owner, identity: transport.identity, role: modelruntime.JournalWorkerRole}}
	client, err := modelruntime.NewJournalWorkerClient(rpc, writer)
	if err != nil {
		t.Fatal(err)
	}
	disposition := f.disposition(t)
	floor, err := client.InstallStageExecutionFloor(t.Context(), &velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest{
		SchemaVersion: 1, Identity: inspectionIdentity(f.authorities[0]), Disposition: disposition,
	})
	if err != nil || !floor.GetDurable() {
		t.Fatalf("floor: %v %v", floor, err)
	}
	scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: inspectionIdentity(f.authorities[0]), Authority: f.authorities[0]}
	response, err := client.CheckpointStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: scope})
	if err != nil || response.GetResult() == nil || response.GetResult().GetCheckpoint() != nil {
		t.Fatalf("admitted execution must return unproven so Worker can recover real drain: %v %v", response, err)
	}
	terminal, err := client.CheckpointStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{
		Scope: &velav1.ModelRuntimeTerminalAllocationScope{SchemaVersion: 1, Identity: scope.Identity, Disposition: disposition, StageAllocationId: scope.Authority.StageAllocationId},
	})
	if err != nil || terminal.GetResult() == nil || terminal.GetResult().GetCheckpoint() != nil {
		t.Fatalf("admitted terminal allocation must not forge proof or block recovery: %v %v", terminal, err)
	}
	if proof, err := f.supervisor.InspectNonAdmission(t.Context(), scope.Authority); err != nil || proof != nil {
		t.Fatalf("admitted execution gained false non-admission proof: %v %v", proof, err)
	}
}

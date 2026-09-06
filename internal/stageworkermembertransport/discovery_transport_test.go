package stageworkermembertransport

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestMemberDiscoveryTLSAndUnixWorksBeforeAssignmentAndDuringRecovery(t *testing.T) {
	f, floor, _ := newMemberFloorFixture(t)
	directory := privateMemberFloorDirectory(t)
	chain := startMemberFloorChain(t, f, floor.Command.Disposition, directory, true, false)
	request := discoveryRequestFor(f.runtime.identity)
	response, err := chain.client.DiscoverRuntimeIdentities(t.Context(), request)
	if err != nil || len(response.GetIdentities()) != 1 || !proto.Equal(response.Identities[0], f.runtime.identity) {
		t.Fatalf("pre-assignment mTLS discovery: %v %v", response, err)
	}
	prepared, err := chain.client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority, ExecutionSpec: f.spec})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("prepare durable recovery fixture: %v %v", prepared, err)
	}
	chain.close()
	f.runtime.identity.ModelRuntimeEpoch++
	chain = startMemberFloorChain(t, f, floor.Command.Disposition, directory, false, false)
	statePath := filepath.Join(directory, "execution-admission.json")
	before, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		response, err := chain.client.DiscoverRuntimeIdentities(t.Context(), request)
		if err != nil || len(response.GetIdentities()) != 1 || !proto.Equal(response.Identities[0], f.runtime.identity) {
			t.Fatalf("recovery mTLS discovery: %v %v", response, err)
		}
		ready, err := chain.supervisor.ProbeReadiness(t.Context(), &velav1.ModelRuntimeServiceProbeReadinessRequest{
			Identity: response.Identities[0], Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_DEVICE,
		})
		if err != nil || ready.GetReady() || !strings.Contains(ready.GetDetail(), modelruntime.ErrExecutionDrainUnproven.Error()) {
			t.Fatalf("discovery changed pending writer readiness: %v %v", ready, err)
		}
	}
	if response, err := chain.dial(t, chain.followerCredentials).DiscoverRuntimeIdentities(t.Context(), request); response != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("nonleader certificate discovered runtimes: %v %v", response, err)
	}
	after, err := os.ReadFile(statePath)
	if err != nil || !bytes.Equal(before, after) || chain.backend.closed.Load() || chain.backend.cancelCalls.Load() != 0 {
		t.Fatalf("discovery changed durable history or backend lifecycle: %v", err)
	}
}

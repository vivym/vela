package fleettransport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

const fleetSPIFFE = "spiffe://vela.internal/fleet-controller/primary"

type recordingFleetService struct {
	plan       fleet.ApprovedResidencyPlan
	evidence   fleet.WorkerInstanceEvidence
	mutation   fleet.MutationAuthorizationRequest
	workerSize int
}

func (service *recordingFleetService) Apply(
	_ context.Context,
	plan fleet.ApprovedResidencyPlan,
) (fleet.ActuationPlan, error) {
	service.plan = plan
	return fleet.ActuationPlan{PlanRevisionID: plan.ID, WorkerInstanceCount: service.workerSize}, nil
}

func (service *recordingFleetService) Observe(
	_ context.Context,
	evidence fleet.WorkerInstanceEvidence,
) (fleet.WorkerInstanceDecision, error) {
	service.evidence = evidence
	modelRuntimeEpoch := int64(1)
	if len(evidence.Residencies) != 0 {
		modelRuntimeEpoch = evidence.Residencies[0].ModelRuntimeEpoch
	}
	return fleet.WorkerInstanceDecision{
		WorkerInstanceID: evidence.WorkerInstanceID, InstanceEpoch: evidence.InstanceEpoch,
		ControlSessionEpoch: evidence.ControlSessionEpoch, ModelRuntimeEpoch: modelRuntimeEpoch,
		Readiness: fleet.WorkerInstanceReady,
	}, nil
}

func (service *recordingFleetService) AuthorizeMutation(
	_ context.Context,
	request fleet.MutationAuthorizationRequest,
) (fleet.MutationAuthorizationResult, error) {
	service.mutation = request
	return fleet.MutationAuthorizationResult{RequestUID: request.RequestUID, Authorized: true}, nil
}

func TestServerMapsResidencyPlanAndWorkerInstanceAuthority(t *testing.T) {
	workerID := uuid.MustParse("10000000-0000-0000-0000-000000000001")
	planID := uuid.MustParse("20000000-0000-0000-0000-000000000001")
	bundleID := uuid.MustParse("30000000-0000-0000-0000-000000000001")
	memberID := uuid.MustParse("40000000-0000-0000-0000-000000000001")
	service := &recordingFleetService{workerSize: 1}
	server, err := NewServer(service, Config{
		SPIFFEIdentity: fleetSPIFFE, ActorIdentity: "fleet-controller/primary",
	})
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	ctx := verifiedPeerContext(t, fleetSPIFFE)
	apply, err := server.ApplyResidencyPlan(ctx, &velav1.ApplyResidencyPlanRequest{
		ApprovedPlanJson: []byte(`{"schema_version":1,"id":"` + planID.String() + `","worker_instances":[{"id":"` + workerID.String() + `"}]}`),
	})
	if err != nil || apply.GetPlanRevisionId() != planID.String() || service.plan.ID != planID {
		t.Fatalf("apply response=%#v plan=%#v error=%v", apply, service.plan, err)
	}
	evidenceJSON := []byte(`{"worker_instance_id":"` + workerID.String() + `","instance_epoch":7,"control_session_epoch":8,"residencies":[{"model_runtime_epoch":9}]}`)
	observed, err := server.ObserveWorkerInstance(ctx, &velav1.ObserveWorkerInstanceRequest{EvidenceJson: evidenceJSON})
	if err != nil || observed.GetWorkerInstanceId() != workerID.String() ||
		service.evidence.ObservedBy != "fleet-controller/primary" {
		t.Fatalf("observe response=%#v evidence=%#v error=%v", observed, service.evidence, err)
	}
	mutation, err := server.AuthorizeMutation(ctx, &velav1.AuthorizeMutationRequest{
		RequestUid: "request-1", Operation: velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_DELETE,
		KubernetesUid: "pod-uid", Namespace: "vela-system", Name: "worker-instance-1",
		WorkerInstanceId: workerID.String(), WorkerInstanceEpoch: 7,
		ResidencyPlanRevisionId: planID.String(), WorkerBundleId: bundleID.String(),
		WorkerMemberId: memberID.String(), RequestDigest: make([]byte, 32),
	})
	if err != nil || !mutation.GetAuthorized() || service.mutation.ActorIdentity != "fleet-controller/primary" ||
		service.mutation.WorkerInstanceID != workerID {
		t.Fatalf("mutation response=%#v request=%#v error=%v", mutation, service.mutation, err)
	}
}

func TestServerRestrictsNodeAgentObservationToItsNode(t *testing.T) {
	agentID := uuid.MustParse("50000000-0000-0000-0000-000000000001")
	workerID := uuid.MustParse("10000000-0000-0000-0000-000000000001")
	identity := "spiffe://vela.internal/node-agent/bm9kZS1h/" + agentID.String()
	service := &recordingFleetService{}
	server, err := NewServer(service, Config{
		SPIFFEIdentity: fleetSPIFFE, ActorIdentity: "fleet-controller/primary",
		NodeAgentRegistrations: []NodeAgentRegistration{{
			NodeIdentity: "node-a", AgentID: agentID, SPIFFEIdentity: identity,
		}},
	})
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	evidence := []byte(`{"worker_instance_id":"` + workerID.String() + `","instance_epoch":1,"control_session_epoch":2,"residencies":[{"model_runtime_epoch":3}],"members":[{"node_identity":"node-b"}]}`)
	if _, err := server.ObserveWorkerInstance(
		verifiedPeerContext(t, identity),
		&velav1.ObserveWorkerInstanceRequest{EvidenceJson: evidence},
	); err == nil {
		t.Fatal("Node Agent observed a WorkerInstance owned by another node")
	}
}

func verifiedPeerContext(t *testing.T, identity string) context.Context {
	t.Helper()
	uri, err := url.Parse(identity)
	if err != nil {
		t.Fatalf("parse identity: %v", err)
	}
	leaf := &x509.Certificate{Raw: []byte(identity), URIs: []*url.URL{uri}}
	state := tls.ConnectionState{
		HandshakeComplete: true,
		PeerCertificates:  []*x509.Certificate{leaf},
		VerifiedChains:    [][]*x509.Certificate{{leaf}},
	}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: state}})
}

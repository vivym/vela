package fleetadmission

import (
	"context"
	"net"
	"testing"

	"github.com/vivym/vela/internal/fleettransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type mutationRPCRecorder struct {
	velav1.UnimplementedFleetMaintenanceServiceServer
	requests chan *velav1.AuthorizeMutationRequest
}

func (r *mutationRPCRecorder) AuthorizeMutation(_ context.Context, request *velav1.AuthorizeMutationRequest) (*velav1.AuthorizeMutationResponse, error) {
	r.requests <- request
	return &velav1.AuthorizeMutationResponse{RequestUid: request.RequestUid, Authorized: true}, nil
}

// Exercise the production handler/client seam. A fake domain authorizer alone
// misses client-side rejection of a caller-supplied audit identity.
func TestRetirementAdmissionReachesFleetRPC(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	rpc := &mutationRPCRecorder{requests: make(chan *velav1.AuthorizeMutationRequest, 2)}
	server := grpc.NewServer()
	velav1.RegisterFleetMaintenanceServiceServer(server, rpc)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///fleet", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client, err := fleettransport.NewClient(connection)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(client, Config{FleetUsername: fleetUsername, CreateValidator: exactPodValidator{}})
	if err != nil {
		t.Fatal(err)
	}
	pod := workerInstancePod(true)
	deleted := pod.DeepCopy()
	deleted.Finalizers = nil
	deleted.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "kubectl-patch", Operation: metav1.ManagedFieldsOperationUpdate}}
	for _, test := range []struct {
		name, operation string
		object          any
		want            velav1.FleetMutationOperation
	}{
		{"delete", "DELETE", nil, velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_DELETE},
		{"finalizer", "UPDATE", deleted, velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_REMOVE_FINALIZER},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := serveAdmission(t, handler, review(test.name, test.operation, fleetUsername, pod, test.object)); !got.Response.Allowed {
				t.Fatalf("retirement denied before RPC: %+v", got.Response.Status)
			}
			select {
			case request := <-rpc.requests:
				if request.RequestUid != test.name || request.Operation != test.want || request.KubernetesUid != string(pod.UID) {
					t.Fatalf("changed mutation: %+v", request)
				}
			default:
				t.Fatal("authorized without reaching Fleet RPC")
			}
		})
	}
	if got := serveAdmission(t, handler, review("non-fleet", "DELETE", "other-user", pod, nil)); got.Response.Allowed {
		t.Fatal("non-Fleet actor accepted")
	}
	select {
	case <-rpc.requests:
		t.Fatal("non-Fleet actor reached RPC")
	default:
	}
}

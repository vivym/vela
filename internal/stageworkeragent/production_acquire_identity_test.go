package stageworkeragent_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestProductionDiscoveryPreservesOriginalAcquireIdentity(t *testing.T) {
	fixture := newAdmissionFixture(t)
	control := &acquireIdentityControl{assignment: fixture.assignment}
	agent := newAcquireIdentityAgent(t, fixture, control)
	discovery, err := agent.Discover(t.Context(), 17)
	if err != nil {
		t.Fatal(err)
	}
	if discovery.AcquireCommandID == uuid.Nil || discovery.AcquireCommandID.String() != control.requestID ||
		!proto.Equal(discovery.Assignment, fixture.assignment) {
		t.Fatalf("discovery lost original Acquire identity: %#v, sent %q", discovery, control.requestID)
	}
	gate := fixture.open(t)
	beginAdmission(t, gate, discovery.Assignment, discovery.AcquireCommandID).Release()
	if snapshot := admissionSnapshot(t, gate); snapshot.Latest.AcquireCommandID.String() != control.requestID {
		t.Fatal("journal lookup ID differs from the actual Acquire request")
	}
	discovery.Assignment.Authority.StageAttemptId = uuid.NewString()
	if proto.Equal(discovery.Assignment, fixture.assignment) {
		t.Fatal("discovery assignment aliases the transport response")
	}
	second, err := agent.Discover(t.Context(), 0)
	if err != nil || second.AcquireCommandID == discovery.AcquireCommandID || second.AcquireCommandID.String() != control.requestID {
		t.Fatalf("distinct Acquire poll reused a command ID: %#v, %v", second, err)
	}
}

func TestProductionDiscoveryRejectsUnboundAcquireResponse(t *testing.T) {
	for _, mismatch := range []string{"missing", "invalid", "nil-uuid", "different", "noncanonical"} {
		t.Run(mismatch, func(t *testing.T) {
			fixture := newAdmissionFixture(t)
			control := &acquireIdentityControl{assignment: fixture.assignment, mismatch: mismatch}
			agent := newAcquireIdentityAgent(t, fixture, control)
			discovery, err := agent.Discover(t.Context(), 17)
			if err == nil || discovery.Assignment != nil || discovery.AcquireCommandID != uuid.Nil {
				t.Fatalf("unbound assignment was returned: %#v, %v", discovery, err)
			}
		})
	}
}

func newAcquireIdentityAgent(t *testing.T, fixture admissionFixture, control *acquireIdentityControl) *stageworkeragent.ProductionAgent {
	t.Helper()
	identity := runtimeIdentityForMember(fixture.assignment.Authority, fixture.config.WorkerMemberID.String())
	control.productionControl = &productionControl{identity: identity}
	agent, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: control, Runtime: &readinessRuntime{identity: identity}, RuntimeIdentity: identity,
		Devices: fixture.assignment.Authority.Devices, Members: fixture.assignment.Authority.Members,
		CapacityVector: fixture.assignment.Authority.CapacityVector, CapacityTTL: 2 * time.Minute,
		ObservationSequenceSource: &capacitySequenceSource{values: []int64{18}},
		Now:                       func() time.Time { return time.Unix(0, fixture.clock.Load()) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return agent
}

type acquireIdentityControl struct {
	*productionControl
	assignment *velav1.StageAssignment
	requestID  string
	mismatch   string
}

func (control *acquireIdentityControl) Exchange(ctx context.Context, request *velav1.StageWorkerControlServiceConnectRequest) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	if request.GetAcquireStage() == nil {
		return control.productionControl.Exchange(ctx, request)
	}
	control.requestID = request.GetRequestId()
	responseID := control.requestID
	switch control.mismatch {
	case "missing":
		responseID = ""
	case "invalid":
		responseID = "invalid"
	case "nil-uuid":
		responseID = uuid.Nil.String()
	case "different":
		responseID = uuid.NewString()
	case "noncanonical":
		responseID = "urn:uuid:" + responseID
	}
	return &velav1.StageWorkerControlServiceConnectResponse{
		RequestId: responseID,
		Result:    &velav1.StageWorkerControlServiceConnectResponse_StageAssignment{StageAssignment: control.assignment},
	}, nil
}

package fleettransport

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func startupTransportFixture(t *testing.T) (fleet.RuntimeStartupRequest, string, *bootstrapServiceStub) {
	t.Helper()
	bootstrap, identity := bootstrapTransportFixture(t)
	principal, _ := parseNodeAgentSPIFFEIdentity(identity)
	service := &bootstrapServiceStub{}
	_, err := service.ClaimWorkerBootstrap(t.Context(), bootstrap)
	mustBootstrap(t, err)
	r := fleet.RuntimeStartupRequest{RequestID: uuid.New(), BootstrapRequestID: bootstrap.RequestID,
		NodeIdentity: principal.NodeIdentity, ActorIdentity: principal.actorIdentity(), RuntimeJournalID: uuid.New(),
		RuntimeScope: bytes.Repeat([]byte{1}, 32), LaunchDigest: bytes.Repeat([]byte{2}, 32),
		OwnerObservationDigest: bytes.Repeat([]byte{3}, 32), IncarnationID: uuid.New(),
		Epochs: []fleet.RuntimeStartupEpoch{{ModelResidencyID: uuid.New(), ModelRuntimeIdentity: "cpu-fixture",
			StageProfileRevisionID: uuid.New(), ModelRuntimeEpoch: 2}}}
	_, err = service.RecordWorkerBootstrapReceipt(t.Context(), fleet.WorkerBootstrapReceipt{
		RequestID: bootstrap.RequestID, ActorIdentity: r.ActorIdentity, WorkerJournalID: uuid.New(),
		WorkerScope: bytes.Repeat([]byte{4}, 32), RuntimeJournalID: r.RuntimeJournalID, RuntimeScope: r.RuntimeScope})
	mustBootstrap(t, err)
	return r, identity, service
}

type startupServiceStub struct {
	result fleet.RuntimeStartupReservation
	calls  int
}

func (s *startupServiceStub) ReserveRuntimeStartup(_ context.Context, _ fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
	s.calls++
	return s.result, nil
}

func (s *startupServiceStub) LookupRuntimeStartup(_ context.Context, _ fleet.WorkerBootstrapLookup) (fleet.RuntimeStartupReservation, error) {
	return s.result, nil
}

func TestRuntimeStartupTransportRejectsBeforeReservation(t *testing.T) {
	r, identity, bootstrap := startupTransportFixture(t)
	principal, _ := parseNodeAgentSPIFFEIdentity(identity)
	service := &startupServiceStub{result: fleet.RuntimeStartupReservation{RuntimeStartupRequest: r, Fresh: true, ReservedAt: time.Now().UTC()}}
	config := Config{SPIFFEIdentity: fleetSPIFFE, ActorIdentity: "fleet/primary", BootstrapService: bootstrap,
		RuntimeStartupService: service, NodeAgentRegistrations: []NodeAgentRegistration{{NodeIdentity: principal.NodeIdentity,
			AgentID: principal.AgentID, SPIFFEIdentity: identity}}}
	server, err := NewServer(bootstrap, config)
	mustBootstrap(t, err)
	wire := encodeRuntimeStartupRequest(r)
	for _, ctx := range []context.Context{context.Background(), verifiedPeerContext(t, fleetSPIFFE)} {
		if _, err := server.ReserveRuntimeStartup(ctx, wire); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("untrusted caller: %v", err)
		}
	}
	ctx := verifiedPeerContext(t, identity)
	for name, mutate := range map[string]func(*velav1.ReserveRuntimeStartupRequest){
		"unknown":        func(w *velav1.ReserveRuntimeStartupRequest) { w.ProtoReflect().SetUnknown([]byte{0x48, 1}) },
		"nested unknown": func(w *velav1.ReserveRuntimeStartupRequest) { w.Epochs[0].ProtoReflect().SetUnknown([]byte{0x28, 1}) },
		"zero request":   func(w *velav1.ReserveRuntimeStartupRequest) { w.RequestId = uuid.Nil.String() },
		"missing epochs": func(w *velav1.ReserveRuntimeStartupRequest) { w.Epochs = nil },
		"nil epoch":      func(w *velav1.ReserveRuntimeStartupRequest) { w.Epochs[0] = nil },
		"too many epochs": func(w *velav1.ReserveRuntimeStartupRequest) {
			for len(w.Epochs) <= 64 {
				w.Epochs = append(w.Epochs, proto.CloneOf(w.Epochs[0]))
			}
		},
		"duplicate":  func(w *velav1.ReserveRuntimeStartupRequest) { w.Epochs = append(w.Epochs, proto.CloneOf(w.Epochs[0])) },
		"zero scope": func(w *velav1.ReserveRuntimeStartupRequest) { clear(w.RuntimeScope) },
		"bad incarnation": func(w *velav1.ReserveRuntimeStartupRequest) {
			w.IncarnationId = uuid.NewSHA1(uuid.Nil, []byte("bad")).String()
		},
		"epoch": func(w *velav1.ReserveRuntimeStartupRequest) { w.Epochs[0].ModelRuntimeEpoch = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := proto.CloneOf(wire)
			mutate(bad)
			if _, err := server.ReserveRuntimeStartup(ctx, bad); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("invalid request: %v", err)
			}
		})
	}
	wrongPair := proto.CloneOf(wire)
	wrongPair.RuntimeJournalId = uuid.NewString()
	if _, err := server.ReserveRuntimeStartup(ctx, wrongPair); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("wrong journal pair: %v", err)
	}
	if service.calls != 0 {
		t.Fatal("rejected requests consumed first use")
	}
	response, err := server.ReserveRuntimeStartup(ctx, wire)
	if err != nil || !response.GetFresh() || response.GetReservation().GetActorIdentity() != r.ActorIdentity || service.calls != 1 {
		t.Fatalf("valid authenticated reservation: %v %v", response, err)
	}
	lookup := &velav1.LookupRuntimeStartupRequest{RequestId: r.RequestID.String()}
	if _, err := server.LookupRuntimeStartup(ctx, lookup); status.Code(err) != codes.Internal {
		t.Fatalf("authoritative Fresh escaped history endpoint: %v", err)
	}
	service.result.Fresh = false
	if _, err := server.LookupRuntimeStartup(ctx, lookup); err != nil {
		t.Fatal(err)
	}
	service.result.NodeIdentity = "other-node"
	if _, err := server.ReserveRuntimeStartup(ctx, wire); status.Code(err) != codes.Internal {
		t.Fatalf("changed authoritative principal accepted: %v", err)
	}
	config.RuntimeStartupService = nil
	server, err = NewServer(bootstrap, config)
	mustBootstrap(t, err)
	if _, err := server.ReserveRuntimeStartup(ctx, wire); status.Code(err) != codes.Unimplemented {
		t.Fatalf("missing reservation service: %v", err)
	}
}

type startupRPCStub struct {
	velav1.UnimplementedFleetMaintenanceServiceServer
	response *velav1.ReserveRuntimeStartupResponse
	fault    string
}

func (s *startupRPCStub) ReserveRuntimeStartup(context.Context, *velav1.ReserveRuntimeStartupRequest) (*velav1.ReserveRuntimeStartupResponse, error) {
	if s.fault == "unavailable" {
		return nil, status.Error(codes.Unavailable, "committed response lost")
	}
	return s.response, nil
}

func (s *startupRPCStub) LookupRuntimeStartup(context.Context, *velav1.LookupRuntimeStartupRequest) (*velav1.LookupRuntimeStartupResponse, error) {
	response := &velav1.LookupRuntimeStartupResponse{Reservation: s.response.Reservation}
	if s.fault == "lookup-unknown" {
		response.ProtoReflect().SetUnknown([]byte{0x10, 1}) // fabricated Fresh field
	}
	return response, nil
}

func TestRuntimeStartupClientHistoryCannotMintFresh(t *testing.T) {
	r, identity, _ := startupTransportFixture(t)
	for _, fault := range []string{"", "request", "node", "actor", "time", "lookup-unknown", "unavailable"} {
		t.Run(fault, func(t *testing.T) {
			response := &velav1.ReserveRuntimeStartupResponse{Fresh: true, Reservation: encodeRuntimeStartupReservation(
				fleet.RuntimeStartupReservation{RuntimeStartupRequest: r, ReservedAt: time.Now().UTC()})}
			switch fault {
			case "request":
				response.Reservation.Request.RequestId = uuid.NewString()
			case "node":
				response.Reservation.NodeIdentity = "other-node"
			case "actor":
				response.Reservation.ActorIdentity = "other-actor"
			case "time":
				response.Reservation.ReservedAt = nil
			}
			base := newTestClient(t, &startupRPCStub{response: response, fault: fault})
			client, err := base.WorkerBootstrap(identity)
			mustBootstrap(t, err)
			var result fleet.RuntimeStartupReservation
			if fault == "unavailable" {
				result, err = client.ReserveRuntimeStartup(t.Context(), r)
			} else {
				result, err = client.LookupRuntimeStartup(t.Context(), r.RequestID)
			}
			if fault == "" {
				if err != nil || result.Fresh || !sameRuntimeStartupRequest(result.RuntimeStartupRequest, r) {
					t.Fatalf("history minted permission or lost identity: %+v %v", result, err)
				}
			} else if err == nil || !reflect.DeepEqual(result, fleet.RuntimeStartupReservation{}) {
				t.Fatalf("bad outcome exposed reservation: %+v %v", result, err)
			}
		})
	}
}

func TestRuntimeStartupClientRejectsChangedResponses(t *testing.T) {
	r, identity, _ := startupTransportFixture(t)
	for name, mutate := range map[string]func(*velav1.ReserveRuntimeStartupResponse){
		"request": func(w *velav1.ReserveRuntimeStartupResponse) { w.Reservation.Request.RequestId = uuid.NewString() },
		"bootstrap": func(w *velav1.ReserveRuntimeStartupResponse) {
			w.Reservation.Request.BootstrapRequestId = uuid.NewString()
		},
		"journal": func(w *velav1.ReserveRuntimeStartupResponse) {
			w.Reservation.Request.RuntimeJournalId = uuid.NewString()
		},
		"scope":       func(w *velav1.ReserveRuntimeStartupResponse) { w.Reservation.Request.RuntimeScope[0]++ },
		"incarnation": func(w *velav1.ReserveRuntimeStartupResponse) { w.Reservation.Request.IncarnationId = uuid.NewString() },
		"launch":      func(w *velav1.ReserveRuntimeStartupResponse) { w.Reservation.Request.LaunchDigest[0]++ },
		"owner":       func(w *velav1.ReserveRuntimeStartupResponse) { w.Reservation.Request.OwnerObservationDigest[0]++ },
		"node":        func(w *velav1.ReserveRuntimeStartupResponse) { w.Reservation.NodeIdentity = "other-node" },
		"actor":       func(w *velav1.ReserveRuntimeStartupResponse) { w.Reservation.ActorIdentity = "other-actor" },
		"epoch":       func(w *velav1.ReserveRuntimeStartupResponse) { w.Reservation.Request.Epochs[0].ModelRuntimeEpoch++ },
		"time":        func(w *velav1.ReserveRuntimeStartupResponse) { w.Reservation.ReservedAt = nil },
		"unknown":     func(w *velav1.ReserveRuntimeStartupResponse) { w.ProtoReflect().SetUnknown([]byte{0x18, 1}) },
		"unknown reservation": func(w *velav1.ReserveRuntimeStartupResponse) {
			w.Reservation.ProtoReflect().SetUnknown([]byte{0x28, 1})
		},
		"unknown request": func(w *velav1.ReserveRuntimeStartupResponse) {
			w.Reservation.Request.ProtoReflect().SetUnknown([]byte{0x48, 1})
		},
		"unknown epoch": func(w *velav1.ReserveRuntimeStartupResponse) {
			w.Reservation.Request.Epochs[0].ProtoReflect().SetUnknown([]byte{0x28, 1})
		},
		"unknown timestamp": func(w *velav1.ReserveRuntimeStartupResponse) {
			w.Reservation.ReservedAt.ProtoReflect().SetUnknown([]byte{0x18, 1})
		},
	} {
		t.Run(name, func(t *testing.T) {
			response := &velav1.ReserveRuntimeStartupResponse{Fresh: true, Reservation: encodeRuntimeStartupReservation(
				fleet.RuntimeStartupReservation{RuntimeStartupRequest: r, ReservedAt: time.Now().UTC()})}
			mutate(response)
			base := newTestClient(t, &startupRPCStub{response: response})
			client, err := base.WorkerBootstrap(identity)
			mustBootstrap(t, err)
			result, err := client.ReserveRuntimeStartup(t.Context(), r)
			if err == nil || !reflect.DeepEqual(result, fleet.RuntimeStartupReservation{}) {
				t.Fatalf("malformed result exposed permission: %+v %v", result, err)
			}
		})
	}
}

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
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBootstrapAbandonmentServerPreservesPrincipalAndExclusiveOutcome(t *testing.T) {
	request, identity := bootstrapTransportFixture(t)
	principal, _ := parseNodeAgentSPIFFEIdentity(identity)
	signer, _ := bindingTransportKeys(t)
	service := &bootstrapServiceStub{}
	server, err := NewServer(service, Config{SPIFFEIdentity: fleetSPIFFE, ActorIdentity: "fleet/primary", BootstrapService: service, BootstrapSigner: signer,
		NodeAgentRegistrations: []NodeAgentRegistration{{NodeIdentity: principal.NodeIdentity, AgentID: principal.AgentID, SPIFFEIdentity: identity}}})
	mustBootstrap(t, err)
	abandon := &velav1.AbandonWorkerBootstrapRequest{RequestId: request.RequestID.String()}
	for _, ctx := range []context.Context{context.Background(), verifiedPeerContext(t, fleetSPIFFE)} {
		if _, err := server.AbandonWorkerBootstrap(ctx, abandon); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("untrusted abandonment reached authority: %v", err)
		}
	}
	ctx := verifiedPeerContext(t, identity)
	unknown := proto.CloneOf(abandon)
	unknown.ProtoReflect().SetUnknown([]byte{0x12, 0x01, 'x'})
	for _, bad := range []*velav1.AbandonWorkerBootstrapRequest{nil, {}, {RequestId: uuid.Nil.String()}, unknown} {
		if _, err := server.AbandonWorkerBootstrap(ctx, bad); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid abandonment reached authority: %v", err)
		}
	}
	_, err = server.ClaimWorkerBootstrap(ctx, bootstrapRequestProto(request))
	mustBootstrap(t, err)
	first, err := server.AbandonWorkerBootstrap(ctx, abandon)
	mustBootstrap(t, err)
	if first.GetClaim().GetActorIdentity() != principal.actorIdentity() || first.GetAbandonment().GetFencedInstanceEpoch() != 2 {
		t.Fatal("abandonment changed principal or fence identity")
	}
	again, err := server.AbandonWorkerBootstrap(ctx, abandon)
	if err != nil || !proto.Equal(first, again) {
		t.Fatalf("replay changed abandonment: %v", err)
	}
	history, err := server.LookupWorkerBootstrap(ctx, &velav1.LookupWorkerBootstrapRequest{RequestId: request.RequestID.String()})
	if err != nil || history.GetPair() != nil || !proto.Equal(history.GetAbandonment(), first.GetAbandonment()) {
		t.Fatalf("history lost terminal outcome: %v", err)
	}
	if _, err := server.LookupWorkerBootstrapBinding(ctx, &velav1.LookupWorkerBootstrapBindingRequest{RequestId: request.RequestID.String()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("abandoned claim was signed for serving: %v", err)
	}
	receipt := fleet.WorkerBootstrapReceipt{RequestID: request.RequestID, WorkerJournalID: uuid.New(), RuntimeJournalID: uuid.New(),
		WorkerScope: bytes.Repeat([]byte{1}, 32), RuntimeScope: bytes.Repeat([]byte{2}, 32), ActorIdentity: principal.actorIdentity()}
	if _, err := server.RecordWorkerBootstrapReceipt(ctx, &velav1.RecordWorkerBootstrapReceiptRequest{RequestId: request.RequestID.String(),
		WorkerJournalId: receipt.WorkerJournalID.String(), RuntimeJournalId: receipt.RuntimeJournalID.String(), WorkerScope: receipt.WorkerScope, RuntimeScope: receipt.RuntimeScope}); status.Code(err) != codes.Aborted {
		t.Fatalf("abandoned claim completed: %v", err)
	}
	service.receipt, service.recordedAt = &receipt, time.Now()
	if _, err := server.LookupWorkerBootstrap(ctx, &velav1.LookupWorkerBootstrapRequest{RequestId: request.RequestID.String()}); status.Code(err) != codes.Internal {
		t.Fatalf("mutually exclusive terminal outcomes were accepted: %v", err)
	}
}

type abandonmentRPCStub struct {
	velav1.UnimplementedFleetMaintenanceServiceServer
	response *velav1.AbandonWorkerBootstrapResponse
	pair     *velav1.WorkerBootstrapJournalPair
}

func (rpc *abandonmentRPCStub) AbandonWorkerBootstrap(context.Context, *velav1.AbandonWorkerBootstrapRequest) (*velav1.AbandonWorkerBootstrapResponse, error) {
	return rpc.response, nil
}

func (rpc *abandonmentRPCStub) LookupWorkerBootstrap(context.Context, *velav1.LookupWorkerBootstrapRequest) (*velav1.LookupWorkerBootstrapResponse, error) {
	return &velav1.LookupWorkerBootstrapResponse{Claim: rpc.response.Claim, Abandonment: rpc.response.Abandonment, Pair: rpc.pair}, nil
}

func TestBootstrapAbandonmentClientRejectsMalformedOutcomes(t *testing.T) {
	for _, fault := range []string{"", "request", "node", "actor", "missing", "epoch", "time", "unknown", "unknown-outcome", "unknown-time"} {
		t.Run(fault, func(t *testing.T) {
			request, identity := bootstrapTransportFixture(t)
			claim, err := bootstrapTarget(request)
			mustBootstrap(t, err)
			claim.ClaimedAt = time.Now().UTC()
			response := &velav1.AbandonWorkerBootstrapResponse{Claim: encodeBootstrapClaim(claim, request.ActorIdentity),
				Abandonment: &velav1.WorkerBootstrapAbandonment{FencedInstanceEpoch: claim.WorkerInstanceEpoch + 1, AbandonedAt: timestamppb.Now()}}
			switch fault {
			case "request":
				response.Claim.RequestId = uuid.NewString()
			case "node":
				response.Claim.NodeIdentity = "other-node"
			case "actor":
				response.Claim.ActorIdentity = "other-actor"
			case "missing":
				response.Abandonment = nil
			case "epoch":
				response.Abandonment.FencedInstanceEpoch++
			case "time":
				response.Abandonment.AbandonedAt = nil
			case "unknown":
				response.ProtoReflect().SetUnknown([]byte{0x1a, 0x01, 'x'})
			case "unknown-outcome":
				response.Abandonment.ProtoReflect().SetUnknown([]byte{0x1a, 0x01, 'x'})
			case "unknown-time":
				response.Abandonment.AbandonedAt.ProtoReflect().SetUnknown([]byte{0x1a, 0x01, 'x'})
			}
			rpc := &abandonmentRPCStub{response: response}
			base := newTestClient(t, rpc)
			client, err := base.WorkerBootstrap(identity)
			mustBootstrap(t, err)
			result, err := client.AbandonWorkerBootstrap(t.Context(), request.RequestID)
			if fault != "" {
				if err == nil || !reflect.DeepEqual(result, fleet.WorkerBootstrapHistory{}) {
					t.Fatalf("invalid response exposed terminal history: %+v %v", result, err)
				}
				return
			}
			if err != nil || result.Abandonment == nil || result.Receipt != nil || result.Claim.Fresh {
				t.Fatalf("valid abandonment did not preserve its historical scope: %+v %v", result, err)
			}
			history, err := client.LookupWorkerBootstrap(t.Context(), request.RequestID)
			if err != nil || !reflect.DeepEqual(history, result) {
				t.Fatalf("history differs from original abandonment: %+v %v", history, err)
			}
			rpc.pair = encodeBootstrapPair(fleet.WorkerBootstrapReceipt{RequestID: request.RequestID, ActorIdentity: request.ActorIdentity,
				WorkerJournalID: uuid.New(), RuntimeJournalID: uuid.New(), WorkerScope: bytes.Repeat([]byte{1}, 32), RuntimeScope: bytes.Repeat([]byte{2}, 32)}, time.Now())
			if result, err := client.LookupWorkerBootstrap(t.Context(), request.RequestID); err == nil || !reflect.DeepEqual(result, fleet.WorkerBootstrapHistory{}) {
				t.Fatalf("contradictory history was accepted: %+v %v", result, err)
			}
		})
	}
}

package fleettransport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/journalbinding"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func bindingTransportKeys(t *testing.T) (*journalbinding.Signer, *journalbinding.Verifier) {
	t.Helper()
	seed := bytes.Repeat([]byte{8}, ed25519.SeedSize)
	signer, err := journalbinding.NewSigner("registry", seed)
	mustBootstrap(t, err)
	verifier, err := journalbinding.NewVerifier(map[string][]byte{"registry": ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)})
	mustBootstrap(t, err)
	return signer, verifier
}

func TestBootstrapBindingServerRequiresCommittedScopedReceipt(t *testing.T) {
	request, identity := bootstrapTransportFixture(t)
	principal, _ := parseNodeAgentSPIFFEIdentity(identity)
	signer, verifier := bindingTransportKeys(t)
	service := &bootstrapServiceStub{}
	configuration := Config{SPIFFEIdentity: fleetSPIFFE, ActorIdentity: "fleet/primary", BootstrapService: service, BootstrapSigner: signer,
		NodeAgentRegistrations: []NodeAgentRegistration{{NodeIdentity: principal.NodeIdentity, AgentID: principal.AgentID, SPIFFEIdentity: identity}}}
	server, err := NewServer(service, configuration)
	mustBootstrap(t, err)
	ctx := verifiedPeerContext(t, identity)
	lookup := &velav1.LookupWorkerBootstrapBindingRequest{RequestId: request.RequestID.String()}
	for _, unauthorized := range []context.Context{context.Background(), verifiedPeerContext(t, fleetSPIFFE)} {
		if _, err := server.LookupWorkerBootstrapBinding(unauthorized, lookup); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("unauthenticated signing: %v", err)
		}
	}
	for _, malformed := range []*velav1.LookupWorkerBootstrapBindingRequest{nil, {}, {RequestId: uuid.Nil.String()}, {RequestId: "URN:UUID:" + request.RequestID.String()}} {
		if _, err := server.LookupWorkerBootstrapBinding(ctx, malformed); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("malformed signing request: %v", err)
		}
	}
	if _, err := server.LookupWorkerBootstrapBinding(ctx, lookup); status.Code(err) != codes.NotFound {
		t.Fatalf("signed unclaimed operation: %v", err)
	}
	_, err = server.ClaimWorkerBootstrap(ctx, bootstrapRequestProto(request))
	mustBootstrap(t, err)
	if _, err := server.LookupWorkerBootstrapBinding(ctx, lookup); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("signed unrecorded journal pair: %v", err)
	}
	_, err = server.RecordWorkerBootstrapReceipt(ctx, &velav1.RecordWorkerBootstrapReceiptRequest{RequestId: request.RequestID.String(),
		WorkerJournalId: uuid.NewString(), RuntimeJournalId: uuid.NewString(), WorkerScope: bytes.Repeat([]byte{1}, 32), RuntimeScope: bytes.Repeat([]byte{2}, 32)})
	mustBootstrap(t, err)
	response, err := server.LookupWorkerBootstrapBinding(ctx, lookup)
	mustBootstrap(t, err)
	verified, err := verifier.Verify(response.GetBinding())
	mustBootstrap(t, err)
	if verified.Claim.RequestId != request.RequestID.String() || verified.Pair.WorkerJournalId != service.receipt.WorkerJournalID.String() {
		t.Fatal("signed different Registry history")
	}
	replay, err := server.LookupWorkerBootstrapBinding(ctx, lookup)
	mustBootstrap(t, err)
	if !proto.Equal(response, replay) || service.claimCalls != 1 || service.receiptCalls != 1 {
		t.Fatal("binding lookup changed immutable evidence or repeated bootstrap mutation")
	}
	service.receipt.WorkerScope = make([]byte, 32)
	if _, err := server.LookupWorkerBootstrapBinding(ctx, lookup); status.Code(err) != codes.Internal {
		t.Fatalf("signed malformed authoritative receipt: %v", err)
	}
	configuration.BootstrapSigner = nil
	server, err = NewServer(service, configuration)
	mustBootstrap(t, err)
	if _, err := server.LookupWorkerBootstrapBinding(ctx, lookup); status.Code(err) != codes.Unimplemented {
		t.Fatalf("binding configured without dedicated signer: %v", err)
	}
	configuration.BootstrapSigner, configuration.BootstrapService = signer, nil
	if _, err := NewServer(service, configuration); err == nil {
		t.Fatal("signer configured without Registry")
	}
}

type bindingRPCStub struct {
	velav1.UnimplementedFleetMaintenanceServiceServer
	response *velav1.LookupWorkerBootstrapBindingResponse
	err      error
}

func (rpc *bindingRPCStub) LookupWorkerBootstrapBinding(context.Context, *velav1.LookupWorkerBootstrapBindingRequest) (*velav1.LookupWorkerBootstrapBindingResponse, error) {
	return rpc.response, rpc.err
}

func TestBootstrapBindingClientChecksSignatureAndRequestedPrincipal(t *testing.T) {
	request, identity := bootstrapTransportFixture(t)
	signer, verifier := bindingTransportKeys(t)
	claim, err := bootstrapTarget(request)
	mustBootstrap(t, err)
	claim.ClaimedAt = time.Now().UTC()
	receipt := fleet.WorkerBootstrapReceipt{RequestID: request.RequestID, ActorIdentity: request.ActorIdentity,
		WorkerJournalID: uuid.New(), RuntimeJournalID: uuid.New(), WorkerScope: bytes.Repeat([]byte{1}, 32), RuntimeScope: bytes.Repeat([]byte{2}, 32)}
	unsigned := &velav1.WorkerBootstrapBinding{SchemaVersion: journalbinding.SchemaVersion,
		Claim: encodeBootstrapClaim(claim, request.ActorIdentity), Pair: encodeBootstrapPair(receipt, time.Now().UTC())}
	for _, fault := range []string{"", "request", "node", "actor", "tamper", "unsigned", "unknown", "missing", "unavailable"} {
		t.Run(fault, func(t *testing.T) {
			value := proto.Clone(unsigned).(*velav1.WorkerBootstrapBinding)
			switch fault {
			case "request":
				value.Claim.RequestId = uuid.NewString()
				value.Pair.RequestId = value.Claim.RequestId
			case "node":
				value.Claim.NodeIdentity = "other-node"
			case "actor":
				value.Claim.ActorIdentity, value.Pair.ActorIdentity = "other-actor", "other-actor"
			}
			signed, err := signer.Sign(value)
			mustBootstrap(t, err)
			rpc := &bindingRPCStub{response: &velav1.LookupWorkerBootstrapBindingResponse{Binding: signed}}
			switch fault {
			case "tamper":
				signed.Pair.RuntimeScope[0] ^= 1
			case "unsigned":
				signed.Signature = nil
			case "unknown":
				rpc.response.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "missing":
				rpc.response.Binding = nil
			case "unavailable":
				rpc.err = status.Error(codes.Unavailable, "response lost")
			}
			client, err := newTestClient(t, rpc).WorkerBootstrap(identity)
			mustBootstrap(t, err)
			binding, err := client.LookupWorkerBootstrapBinding(t.Context(), request.RequestID, verifier)
			if fault == "" {
				mustBootstrap(t, err)
				if !proto.Equal(binding, signed) {
					t.Fatal("changed verified binding")
				}
			} else if err == nil || binding != nil {
				t.Fatalf("exposed invalid binding: %v", err)
			}
			if binding, err := client.LookupWorkerBootstrapBinding(t.Context(), request.RequestID, nil); err == nil || binding != nil {
				t.Fatal("accepted absent trusted verifier")
			}
		})
	}
}

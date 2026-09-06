package fleettransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestBootstrapServerAuthenticatesNodeBeforeConsumingFirstUse(t *testing.T) {
	request, identity := bootstrapTransportFixture(t)
	principal, _ := parseNodeAgentSPIFFEIdentity(identity)
	otherIdentity := nodeAgentSPIFFEPrefix + base64.RawURLEncoding.EncodeToString([]byte("other-node")) + "/" + uuid.NewString()
	other, _ := parseNodeAgentSPIFFEIdentity(otherIdentity)
	for _, caller := range []struct {
		name string
		ctx  context.Context
		code codes.Code
	}{
		{"missing", context.Background(), codes.Unauthenticated},
		{"fleet-controller", verifiedPeerContext(t, fleetSPIFFE), codes.Unauthenticated},
		{"unregistered", verifiedPeerContext(t, nodeAgentSPIFFEPrefix+"Y3B1LW5vZGUtMQ/"+uuid.NewString()), codes.Unauthenticated},
		{"other-node", verifiedPeerContext(t, otherIdentity), codes.PermissionDenied},
	} {
		t.Run(caller.name, func(t *testing.T) {
			service := &bootstrapServiceStub{}
			server, err := NewServer(service, Config{SPIFFEIdentity: fleetSPIFFE, ActorIdentity: "fleet/primary", BootstrapService: service,
				NodeAgentRegistrations: []NodeAgentRegistration{
					{NodeIdentity: principal.NodeIdentity, AgentID: principal.AgentID, SPIFFEIdentity: identity},
					{NodeIdentity: other.NodeIdentity, AgentID: other.AgentID, SPIFFEIdentity: otherIdentity}}})
			mustBootstrap(t, err)
			_, err = server.ClaimWorkerBootstrap(caller.ctx, bootstrapRequestProto(request))
			if status.Code(err) != caller.code || service.claimCalls != 0 {
				t.Fatalf("unauthorized claim consumed first use: %v, calls=%d", err, service.claimCalls)
			}
		})
	}
	service := &bootstrapServiceStub{}
	server, err := NewServer(service, Config{SPIFFEIdentity: fleetSPIFFE, ActorIdentity: "fleet/primary", BootstrapService: service,
		NodeAgentRegistrations: []NodeAgentRegistration{{NodeIdentity: principal.NodeIdentity, AgentID: principal.AgentID, SPIFFEIdentity: identity}}})
	mustBootstrap(t, err)
	ctx := verifiedPeerContext(t, identity)
	for _, fault := range []string{"unknown-actor", "duplicate-json", "not-canonical", "oversize", "wrong-target", "wrong-epoch"} {
		bad := bootstrapRequestProto(request)
		switch fault {
		case "unknown-actor":
			bad.ProtoReflect().SetUnknown([]byte{0x32, 0x01, 'x'})
		case "duplicate-json":
			bad.BundleManifest = append([]byte(`{"schema":"wrong",`), request.BundleManifest[1:]...)
		case "not-canonical":
			bad.BundleManifest = append(bytes.Clone(request.BundleManifest), '\n')
		case "oversize":
			bad.BundleManifest = make([]byte, fleet.MaximumWorkerBootstrapManifestBytes+1)
		case "wrong-target":
			bad.WorkerMemberId = uuid.NewString()
		case "wrong-epoch":
			bad.WorkerInstanceEpoch++
		}
		_, err := server.ClaimWorkerBootstrap(ctx, bad)
		if status.Code(err) != codes.InvalidArgument || service.claimCalls != 0 {
			t.Fatalf("%s reached first-use authority: %v", fault, err)
		}
	}
	response, err := server.ClaimWorkerBootstrap(ctx, bootstrapRequestProto(request))
	mustBootstrap(t, err)
	if !response.GetFresh() || response.GetClaim().GetActorIdentity() != principal.actorIdentity() || service.actor != principal.actorIdentity() {
		t.Fatal("claim did not use authenticated actor")
	}
	replay, err := server.ClaimWorkerBootstrap(ctx, bootstrapRequestProto(request))
	mustBootstrap(t, err)
	if replay.GetFresh() || !proto.Equal(replay.GetClaim(), response.GetClaim()) {
		t.Fatal("replay changed claim identity or reissued permission")
	}
	history, err := server.LookupWorkerBootstrap(ctx, &velav1.LookupWorkerBootstrapRequest{RequestId: request.RequestID.String()})
	mustBootstrap(t, err)
	if history.GetPair() != nil || !proto.Equal(history.GetClaim(), response.GetClaim()) || service.claimCalls != 2 {
		t.Fatal("history lookup acquired permission or changed history")
	}
}

func TestBootstrapServerFailsClosedWithoutConfiguredAuthority(t *testing.T) {
	server, err := NewServer(&recordingFleetService{}, Config{SPIFFEIdentity: fleetSPIFFE, ActorIdentity: "fleet/primary"})
	mustBootstrap(t, err)
	_, err = server.ClaimWorkerBootstrap(context.Background(), nil)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("missing bootstrap service: %v", err)
	}
}

func TestBootstrapReceiptRejectsZeroScopeBeforeRegistry(t *testing.T) {
	request, identity := bootstrapTransportFixture(t)
	principal, _ := parseNodeAgentSPIFFEIdentity(identity)
	service := &bootstrapServiceStub{}
	server, err := NewServer(service, Config{SPIFFEIdentity: fleetSPIFFE, ActorIdentity: "fleet/primary", BootstrapService: service,
		NodeAgentRegistrations: []NodeAgentRegistration{{NodeIdentity: principal.NodeIdentity, AgentID: principal.AgentID, SPIFFEIdentity: identity}}})
	mustBootstrap(t, err)
	for _, field := range []string{"worker", "runtime"} {
		receipt := &velav1.RecordWorkerBootstrapReceiptRequest{RequestId: request.RequestID.String(), WorkerJournalId: uuid.NewString(), RuntimeJournalId: uuid.NewString(),
			WorkerScope: bytes.Repeat([]byte{1}, 32), RuntimeScope: bytes.Repeat([]byte{2}, 32)}
		if field == "worker" {
			clear(receipt.WorkerScope)
		} else {
			clear(receipt.RuntimeScope)
		}
		if _, err := server.RecordWorkerBootstrapReceipt(verifiedPeerContext(t, identity), receipt); status.Code(err) != codes.InvalidArgument || service.receiptCalls != 0 {
			t.Fatalf("zero scope reached Registry: %v writes=%d", err, service.receiptCalls)
		}
	}
}

func TestBootstrapClientRejectsChangedClaimResponses(t *testing.T) {
	request, identity := bootstrapTransportFixture(t)
	for _, fault := range []string{"request", "worker", "epoch", "member", "member-epoch", "node", "actor", "digest", "time", "unknown", "unknown-claim", "unavailable"} {
		t.Run(fault, func(t *testing.T) {
			rpc := &bootstrapRPCStub{request: request, fault: fault}
			base := newTestClient(t, rpc)
			client, err := base.WorkerBootstrap(identity)
			mustBootstrap(t, err)
			claim, err := client.ClaimWorkerBootstrap(t.Context(), request)
			if err == nil || !reflect.DeepEqual(claim, fleet.WorkerBootstrapClaim{}) {
				t.Fatalf("invalid response exposed initialization authority: %+v %v", claim, err)
			}
		})
	}
}

func TestBootstrapClientRequiresExactReceiptAndReadOnlyHistory(t *testing.T) {
	request, identity := bootstrapTransportFixture(t)
	for _, fault := range []string{"", "request", "worker", "runtime", "scope", "actor", "time", "unknown"} {
		t.Run(fault, func(t *testing.T) {
			rpc := &bootstrapRPCStub{request: request, pairFault: fault}
			base := newTestClient(t, rpc)
			client, err := base.WorkerBootstrap(identity)
			mustBootstrap(t, err)
			receipt := fleet.WorkerBootstrapReceipt{RequestID: request.RequestID, WorkerJournalID: uuid.New(), RuntimeJournalID: uuid.New(),
				WorkerScope: bytes.Repeat([]byte{1}, 32), RuntimeScope: bytes.Repeat([]byte{2}, 32), ActorIdentity: client.ActorIdentity()}
			when, err := client.RecordWorkerBootstrapReceipt(t.Context(), receipt)
			if fault == "" {
				mustBootstrap(t, err)
				if when.IsZero() {
					t.Fatal("receipt returned empty committed timestamp")
				}
				history, err := client.LookupWorkerBootstrap(t.Context(), request.RequestID)
				mustBootstrap(t, err)
				if history.Claim.Fresh || history.Receipt == nil || !reflect.DeepEqual(*history.Receipt, receipt) || !history.RecordedAt.Equal(when) {
					t.Fatal("lookup changed receipt or returned a fresh grant")
				}
			} else if err == nil || !when.IsZero() {
				t.Fatalf("receipt accepted mismatched response: %v %v", when, err)
			}
		})
	}
}

type bootstrapServiceStub struct {
	recordingFleetService
	mu           sync.Mutex
	claim        fleet.WorkerBootstrapClaim
	actor        string
	receipt      *fleet.WorkerBootstrapReceipt
	recordedAt   time.Time
	claimCalls   int
	receiptCalls int
}

func (service *bootstrapServiceStub) ClaimWorkerBootstrap(_ context.Context, request fleet.WorkerBootstrapRequest) (fleet.WorkerBootstrapClaim, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.claimCalls++
	if service.claim.RequestID != uuid.Nil {
		claim := service.claim
		claim.Fresh = false
		return claim, nil
	}
	claim, err := bootstrapTarget(request)
	if err != nil {
		return fleet.WorkerBootstrapClaim{}, err
	}
	claim.ClaimedAt, claim.Fresh = time.Now().UTC(), true
	service.claim, service.actor = claim, request.ActorIdentity
	return claim, nil
}

func (service *bootstrapServiceStub) RecordWorkerBootstrapReceipt(_ context.Context, request fleet.WorkerBootstrapReceipt) (time.Time, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.receiptCalls++
	if request.RequestID != service.claim.RequestID || request.ActorIdentity != service.actor {
		return time.Time{}, &fleet.Failure{Code: fleet.FailureNotFound}
	}
	if service.receipt == nil {
		service.receipt, service.recordedAt = &request, time.Now().UTC()
	} else if !reflect.DeepEqual(*service.receipt, request) {
		return time.Time{}, &fleet.Failure{Code: fleet.FailureConflict}
	}
	return service.recordedAt, nil
}

func (service *bootstrapServiceStub) LookupWorkerBootstrap(_ context.Context, request fleet.WorkerBootstrapLookup) (fleet.WorkerBootstrapHistory, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if request.RequestID != service.claim.RequestID || request.NodeIdentity != service.claim.NodeIdentity || request.ActorIdentity != service.actor {
		return fleet.WorkerBootstrapHistory{}, &fleet.Failure{Code: fleet.FailureNotFound}
	}
	claim := service.claim
	claim.Fresh = false
	return fleet.WorkerBootstrapHistory{Claim: claim, ActorIdentity: service.actor, Receipt: service.receipt, RecordedAt: service.recordedAt}, nil
}

type bootstrapRPCStub struct {
	velav1.UnimplementedFleetMaintenanceServiceServer
	request   fleet.WorkerBootstrapRequest
	fault     string
	pairFault string
	pair      *velav1.WorkerBootstrapJournalPair
}

func (rpc *bootstrapRPCStub) ClaimWorkerBootstrap(_ context.Context, _ *velav1.ClaimWorkerBootstrapRequest) (*velav1.ClaimWorkerBootstrapResponse, error) {
	if rpc.fault == "unavailable" {
		return nil, status.Error(codes.Unavailable, "committed response lost")
	}
	claim, err := bootstrapTarget(rpc.request)
	if err != nil {
		return nil, err
	}
	claim.ClaimedAt = time.Now().UTC()
	response := &velav1.ClaimWorkerBootstrapResponse{Fresh: true, Claim: encodeBootstrapClaim(claim, rpc.request.ActorIdentity)}
	switch rpc.fault {
	case "request":
		response.Claim.RequestId = uuid.NewString()
	case "worker":
		response.Claim.WorkerInstanceId = uuid.NewString()
	case "epoch":
		response.Claim.WorkerInstanceEpoch++
	case "member":
		response.Claim.WorkerMemberId = uuid.NewString()
	case "member-epoch":
		response.Claim.WorkerMemberEpoch++
	case "node":
		response.Claim.NodeIdentity = "other-node"
	case "actor":
		response.Claim.ActorIdentity = "other-actor"
	case "digest":
		response.Claim.BundleDigest = make([]byte, 32)
	case "time":
		response.Claim.ClaimedAt = nil
	case "unknown":
		response.ProtoReflect().SetUnknown([]byte{0x78, 1})
	case "unknown-claim":
		response.Claim.ProtoReflect().SetUnknown([]byte{0x78, 1})
	}
	return response, nil
}

func (rpc *bootstrapRPCStub) RecordWorkerBootstrapReceipt(_ context.Context, request *velav1.RecordWorkerBootstrapReceiptRequest) (*velav1.RecordWorkerBootstrapReceiptResponse, error) {
	if request == nil {
		return nil, errors.New("missing receipt")
	}
	rpc.pair = encodeBootstrapPair(fleet.WorkerBootstrapReceipt{RequestID: uuid.MustParse(request.GetRequestId()),
		WorkerJournalID: uuid.MustParse(request.GetWorkerJournalId()), WorkerScope: request.GetWorkerScope(),
		RuntimeJournalID: uuid.MustParse(request.GetRuntimeJournalId()), RuntimeScope: request.GetRuntimeScope(), ActorIdentity: rpc.request.ActorIdentity}, time.Now().UTC())
	response := &velav1.RecordWorkerBootstrapReceiptResponse{Pair: rpc.pair}
	switch rpc.pairFault {
	case "request":
		response.Pair.RequestId = uuid.NewString()
	case "worker":
		response.Pair.WorkerJournalId = uuid.NewString()
	case "runtime":
		response.Pair.RuntimeJournalId = uuid.NewString()
	case "scope":
		response.Pair.WorkerScope = make([]byte, 32)
	case "actor":
		response.Pair.ActorIdentity = "other"
	case "time":
		response.Pair.RecordedAt = nil
	case "unknown":
		response.Pair.ProtoReflect().SetUnknown([]byte{0x78, 1})
	}
	return response, nil
}

func (rpc *bootstrapRPCStub) LookupWorkerBootstrap(ctx context.Context, _ *velav1.LookupWorkerBootstrapRequest) (*velav1.LookupWorkerBootstrapResponse, error) {
	claim, err := rpc.ClaimWorkerBootstrap(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &velav1.LookupWorkerBootstrapResponse{Claim: claim.GetClaim(), Pair: rpc.pair}, nil
}

func bootstrapRequestProto(request fleet.WorkerBootstrapRequest) *velav1.ClaimWorkerBootstrapRequest {
	return &velav1.ClaimWorkerBootstrapRequest{RequestId: request.RequestID.String(), WorkerInstanceId: request.WorkerInstanceID.String(),
		WorkerInstanceEpoch: request.WorkerInstanceEpoch, WorkerMemberId: request.WorkerMemberID.String(), BundleManifest: bytes.Clone(request.BundleManifest)}
}

func bootstrapTransportFixture(t *testing.T) (fleet.WorkerBootstrapRequest, string) {
	t.Helper()
	workerID, memberID, poolID := uuid.New(), uuid.New(), uuid.New()
	memberDigest := sha256.Sum256([]byte("spiffe://vela.internal/stage-worker/" + memberID.String()))
	image := "registry.example/vela@sha256:" + strings.Repeat("a", 64)
	bundle := fleetcontroller.WorkerBundleActuation{SchemaVersion: 2, PlanRevisionID: uuid.New(), WorkerBundleID: uuid.New(), Namespace: "vela",
		InitImage: image, StageWorkerAgentImage: image, RuntimeImage: image, StageWorkerConfigMap: "worker-config",
		ModelRuntimeVerifierConfigMap: "verifier", StageWorkerControlTLSSecret: "control-tls", StageWorkerAuthoritySecret: "authority",
		ArtifactStoreCredentialsSecret: "artifact", ArtifactStoreCASecret: "artifact-ca",
		WorkerInstances: []fleetcontroller.WorkerInstanceActuation{{ID: workerID, InstanceEpoch: 1, WorkerProfileRevisionID: uuid.New(),
			CapacityPoolID: poolID, Role: "cpu-thumbnail", CapacitySlots: 1, DeviceSetDigest: strings.Repeat("b", 64), MembershipDigest: strings.Repeat("c", 64),
			ModelRuntimes: []fleetcontroller.ModelRuntimeProcess{{ModelResidencyID: uuid.New(), CapacityPoolID: poolID, StageProfileRevisionID: uuid.New(),
				ModelRuntimeEpochFloor: 1, Component: "CPU_MEDIA", ModelComponentRevision: "cpu-mock-v1", RuntimeIdentity: "cpu-runtime",
				Command: []string{"/nonexistent-mock-backend"}, InitializationTimeout: "1s", ShutdownTimeout: "1s"}},
			Members: []fleetcontroller.WorkerMemberActuation{{ID: memberID, MemberEpoch: 1, Key: "member-0", NodeIdentity: "cpu-node-1",
				ResourceClass: "CPU", DeviceCount: 1, IdentityDigest: hex.EncodeToString(memberDigest[:]), DeviceSubsetDigest: strings.Repeat("d", 64),
				DeviceConstraints: []fleetcontroller.DeviceConstraint{{DeviceID: uuid.New(), DeviceEpoch: 1, ResourceClass: "CPU"}}}},
		}}}
	var err error
	bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
	mustBootstrap(t, err)
	manifest, err := fleetcontroller.WorkerBundleActuationManifest(bundle)
	mustBootstrap(t, err)
	identity := nodeAgentSPIFFEPrefix + base64.RawURLEncoding.EncodeToString([]byte("cpu-node-1")) + "/" + uuid.NewString()
	principal, _ := parseNodeAgentSPIFFEIdentity(identity)
	return fleet.WorkerBootstrapRequest{RequestID: uuid.New(), WorkerInstanceID: workerID, WorkerInstanceEpoch: 1,
		WorkerMemberID: memberID, BundleManifest: manifest, ActorIdentity: principal.actorIdentity()}, identity
}

func mustBootstrap(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

package fleettransport

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

// BootstrapClient binds every response to the expected registered Node Agent.
// The server independently derives the actual principal from the TLS peer.
type BootstrapClient struct {
	service   velav1.FleetMaintenanceServiceClient
	principal nodeAgentPrincipal
}

func (client *Client) WorkerBootstrap(spiffeIdentity string) (*BootstrapClient, error) {
	principal, valid := parseNodeAgentSPIFFEIdentity(spiffeIdentity)
	if client == nil || client.service == nil || !valid {
		return nil, errors.New("worker bootstrap client requires a Fleet connection and canonical Node Agent identity")
	}
	return &BootstrapClient{service: client.service, principal: principal}, nil
}

func (client *BootstrapClient) ActorIdentity() string { return client.principal.actorIdentity() }
func (client *BootstrapClient) NodeIdentity() string  { return client.principal.NodeIdentity }

func (client *BootstrapClient) ClaimWorkerBootstrap(ctx context.Context, request fleet.WorkerBootstrapRequest) (fleet.WorkerBootstrapClaim, error) {
	if !client.configured(ctx) || request.ActorIdentity != client.ActorIdentity() {
		return fleet.WorkerBootstrapClaim{}, errors.New("worker bootstrap client actor is invalid")
	}
	expected, err := bootstrapTarget(request)
	if err != nil || expected.NodeIdentity != client.NodeIdentity() {
		return fleet.WorkerBootstrapClaim{}, errors.New("worker bootstrap request does not belong to this node")
	}
	response, err := client.service.ClaimWorkerBootstrap(ctx, &velav1.ClaimWorkerBootstrapRequest{
		RequestId: request.RequestID.String(), WorkerInstanceId: request.WorkerInstanceID.String(), WorkerInstanceEpoch: request.WorkerInstanceEpoch,
		WorkerMemberId: request.WorkerMemberID.String(), BundleManifest: bytes.Clone(request.BundleManifest),
	}, grpc.MaxCallSendMsgSize(MaximumMessageBytes))
	if err != nil {
		return fleet.WorkerBootstrapClaim{}, err
	}
	if !knownBootstrapMessage(response) {
		return fleet.WorkerBootstrapClaim{}, errors.New("worker bootstrap claim response is invalid")
	}
	claim, err := client.decodeClaim(response.GetClaim())
	if err != nil || !sameBootstrapClaimScope(claim, expected) {
		return fleet.WorkerBootstrapClaim{}, errors.New("worker bootstrap claim response changed the requested authority")
	}
	claim.Fresh = response.GetFresh()
	return claim, nil
}

func (client *BootstrapClient) RecordWorkerBootstrapReceipt(ctx context.Context, receipt fleet.WorkerBootstrapReceipt) (time.Time, error) {
	if !client.configured(ctx) || receipt.ActorIdentity != client.ActorIdentity() || !validBootstrapReceipt(receipt) {
		return time.Time{}, errors.New("worker bootstrap receipt request is invalid")
	}
	response, err := client.service.RecordWorkerBootstrapReceipt(ctx, &velav1.RecordWorkerBootstrapReceiptRequest{
		RequestId: receipt.RequestID.String(), WorkerJournalId: receipt.WorkerJournalID.String(), WorkerScope: bytes.Clone(receipt.WorkerScope),
		RuntimeJournalId: receipt.RuntimeJournalID.String(), RuntimeScope: bytes.Clone(receipt.RuntimeScope)})
	if err != nil {
		return time.Time{}, err
	}
	if !knownBootstrapMessage(response) {
		return time.Time{}, errors.New("worker bootstrap receipt response is invalid")
	}
	actual, recordedAt, err := client.decodePair(response.GetPair())
	if err != nil || actual.RequestID != receipt.RequestID || actual.WorkerJournalID != receipt.WorkerJournalID ||
		actual.RuntimeJournalID != receipt.RuntimeJournalID || !bytes.Equal(actual.WorkerScope, receipt.WorkerScope) || !bytes.Equal(actual.RuntimeScope, receipt.RuntimeScope) {
		return time.Time{}, errors.New("worker bootstrap receipt response changed the journal pair")
	}
	return recordedAt, nil
}

func (client *BootstrapClient) LookupWorkerBootstrap(ctx context.Context, requestID uuid.UUID) (fleet.WorkerBootstrapHistory, error) {
	if !client.configured(ctx) || requestID == uuid.Nil {
		return fleet.WorkerBootstrapHistory{}, errors.New("worker bootstrap lookup request is invalid")
	}
	response, err := client.service.LookupWorkerBootstrap(ctx, &velav1.LookupWorkerBootstrapRequest{RequestId: requestID.String()})
	if err != nil {
		return fleet.WorkerBootstrapHistory{}, err
	}
	if !knownBootstrapMessage(response) {
		return fleet.WorkerBootstrapHistory{}, errors.New("worker bootstrap history response is invalid")
	}
	claim, err := client.decodeClaim(response.GetClaim())
	if err != nil || claim.RequestID != requestID {
		return fleet.WorkerBootstrapHistory{}, errors.New("worker bootstrap history response changed the requested identity")
	}
	result := fleet.WorkerBootstrapHistory{Claim: claim, ActorIdentity: client.ActorIdentity()}
	if response.GetPair() != nil {
		receipt, recordedAt, err := client.decodePair(response.GetPair())
		if err != nil || receipt.RequestID != requestID {
			return fleet.WorkerBootstrapHistory{}, errors.New("worker bootstrap history contains an invalid journal pair")
		}
		result.Receipt, result.RecordedAt = &receipt, recordedAt
	}
	return result, nil
}

func (client *BootstrapClient) configured(ctx context.Context) bool {
	return ctx != nil && client != nil && client.service != nil && client.principal.SPIFFEIdentity != ""
}

func (client *BootstrapClient) decodeClaim(value *velav1.WorkerBootstrapClaim) (fleet.WorkerBootstrapClaim, error) {
	if !knownBootstrapMessage(value) || value.GetActorIdentity() != client.ActorIdentity() || value.GetNodeIdentity() != client.NodeIdentity() ||
		!validBootstrapTime(value.GetClaimedAt()) {
		return fleet.WorkerBootstrapClaim{}, errors.New("worker bootstrap claim principal or timestamp is invalid")
	}
	requestID, requestErr := bootstrapUUID(value.GetRequestId())
	workerID, workerErr := bootstrapUUID(value.GetWorkerInstanceId())
	memberID, memberErr := bootstrapUUID(value.GetWorkerMemberId())
	claim := fleet.WorkerBootstrapClaim{RequestID: requestID, WorkerInstanceID: workerID, WorkerInstanceEpoch: value.GetWorkerInstanceEpoch(),
		WorkerMemberID: memberID, WorkerMemberEpoch: value.GetWorkerMemberEpoch(), NodeIdentity: value.GetNodeIdentity(),
		BundleDigest: bytes.Clone(value.GetBundleDigest()), ClaimedAt: value.GetClaimedAt().AsTime()}
	if requestErr != nil || workerErr != nil || memberErr != nil || !validBootstrapClaim(claim) {
		return fleet.WorkerBootstrapClaim{}, errors.New("worker bootstrap claim metadata is invalid")
	}
	return claim, nil
}

func (client *BootstrapClient) decodePair(value *velav1.WorkerBootstrapJournalPair) (fleet.WorkerBootstrapReceipt, time.Time, error) {
	if !knownBootstrapMessage(value) || value.GetActorIdentity() != client.ActorIdentity() || !validBootstrapTime(value.GetRecordedAt()) {
		return fleet.WorkerBootstrapReceipt{}, time.Time{}, errors.New("worker bootstrap receipt principal or timestamp is invalid")
	}
	requestID, requestErr := bootstrapUUID(value.GetRequestId())
	workerID, workerErr := bootstrapUUID(value.GetWorkerJournalId())
	runtimeID, runtimeErr := bootstrapUUID(value.GetRuntimeJournalId())
	receipt := fleet.WorkerBootstrapReceipt{RequestID: requestID, WorkerJournalID: workerID, WorkerScope: bytes.Clone(value.GetWorkerScope()),
		RuntimeJournalID: runtimeID, RuntimeScope: bytes.Clone(value.GetRuntimeScope()), ActorIdentity: value.GetActorIdentity()}
	if requestErr != nil || workerErr != nil || runtimeErr != nil || !validBootstrapReceipt(receipt) {
		return fleet.WorkerBootstrapReceipt{}, time.Time{}, errors.New("worker bootstrap journal pair is invalid")
	}
	return receipt, value.GetRecordedAt().AsTime(), nil
}

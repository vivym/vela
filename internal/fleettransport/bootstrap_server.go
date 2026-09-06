package fleettransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/journalbinding"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MaximumMessageBytes includes the bounded bundle plus protobuf envelope.
// Existing plan and observation handlers retain their smaller payload bounds.
const MaximumMessageBytes = fleet.MaximumWorkerBootstrapManifestBytes + (64 << 10)

type WorkerBootstrapService interface {
	ClaimWorkerBootstrap(context.Context, fleet.WorkerBootstrapRequest) (fleet.WorkerBootstrapClaim, error)
	RecordWorkerBootstrapReceipt(context.Context, fleet.WorkerBootstrapReceipt) (time.Time, error)
	LookupWorkerBootstrap(context.Context, fleet.WorkerBootstrapLookup) (fleet.WorkerBootstrapHistory, error)
	AbandonWorkerBootstrap(context.Context, fleet.WorkerBootstrapLookup) (fleet.WorkerBootstrapHistory, error)
}

func (server *Server) authenticateBootstrap(ctx context.Context) (nodeAgentPrincipal, error) {
	if server == nil || server.bootstrap == nil {
		return nodeAgentPrincipal{}, status.Error(codes.Unimplemented, "Worker bootstrap service is not configured")
	}
	identity, verified := verifiedPeerSPIFFEIdentity(ctx)
	principal, exists := server.nodeAgentPrincipals[identity]
	if !verified || !exists {
		return nodeAgentPrincipal{}, status.Error(codes.Unauthenticated, "registered Node Agent mTLS identity is required")
	}
	return principal, nil
}

func (server *Server) ClaimWorkerBootstrap(ctx context.Context, request *velav1.ClaimWorkerBootstrapRequest) (*velav1.ClaimWorkerBootstrapResponse, error) {
	principal, err := server.authenticateBootstrap(ctx)
	if err != nil {
		return nil, err
	}
	if !knownBootstrapMessage(request) {
		return nil, invalidRequest("Worker bootstrap claim")
	}
	requestID, requestErr := bootstrapUUID(request.GetRequestId())
	workerID, workerErr := bootstrapUUID(request.GetWorkerInstanceId())
	memberID, memberErr := bootstrapUUID(request.GetWorkerMemberId())
	parsed := fleet.WorkerBootstrapRequest{RequestID: requestID, WorkerInstanceID: workerID,
		WorkerInstanceEpoch: request.GetWorkerInstanceEpoch(), WorkerMemberID: memberID,
		BundleManifest: bytes.Clone(request.GetBundleManifest()), ActorIdentity: principal.actorIdentity()}
	target, err := bootstrapTarget(parsed)
	if requestErr != nil || workerErr != nil || memberErr != nil || err != nil {
		return nil, invalidRequest("Worker bootstrap claim")
	}
	// Reject before invoking the first-use mutation. Checking the returned node
	// alone would already have consumed another node's unique permission.
	if target.NodeIdentity != principal.NodeIdentity {
		return nil, status.Error(codes.PermissionDenied, "Worker bootstrap target belongs to another node")
	}
	claim, err := server.bootstrap.ClaimWorkerBootstrap(ctx, parsed)
	if err != nil {
		return nil, mapServiceError("claim Worker bootstrap", err)
	}
	if !sameBootstrapClaimScope(claim, target) || !validBootstrapTime(timestamppb.New(claim.ClaimedAt)) {
		return nil, invalidAuthoritativeResult("Worker bootstrap claim")
	}
	return &velav1.ClaimWorkerBootstrapResponse{Claim: encodeBootstrapClaim(claim, principal.actorIdentity()), Fresh: claim.Fresh}, nil
}

func (server *Server) RecordWorkerBootstrapReceipt(ctx context.Context, request *velav1.RecordWorkerBootstrapReceiptRequest) (*velav1.RecordWorkerBootstrapReceiptResponse, error) {
	principal, err := server.authenticateBootstrap(ctx)
	if err != nil {
		return nil, err
	}
	if !knownBootstrapMessage(request) {
		return nil, invalidRequest("Worker bootstrap receipt")
	}
	requestID, requestErr := bootstrapUUID(request.GetRequestId())
	workerID, workerErr := bootstrapUUID(request.GetWorkerJournalId())
	runtimeID, runtimeErr := bootstrapUUID(request.GetRuntimeJournalId())
	receipt := fleet.WorkerBootstrapReceipt{RequestID: requestID, WorkerJournalID: workerID, WorkerScope: bytes.Clone(request.GetWorkerScope()),
		RuntimeJournalID: runtimeID, RuntimeScope: bytes.Clone(request.GetRuntimeScope()), ActorIdentity: principal.actorIdentity()}
	if requestErr != nil || workerErr != nil || runtimeErr != nil || !validBootstrapReceipt(receipt) {
		return nil, invalidRequest("Worker bootstrap receipt")
	}
	if _, err := server.lookupBootstrap(ctx, requestID, principal); err != nil {
		return nil, err
	}
	recordedAt, err := server.bootstrap.RecordWorkerBootstrapReceipt(ctx, receipt)
	if err != nil {
		return nil, mapServiceError("record Worker bootstrap receipt", err)
	}
	if !validBootstrapTime(timestamppb.New(recordedAt)) {
		return nil, invalidAuthoritativeResult("Worker bootstrap receipt")
	}
	return &velav1.RecordWorkerBootstrapReceiptResponse{Pair: encodeBootstrapPair(receipt, recordedAt)}, nil
}

func (server *Server) LookupWorkerBootstrap(ctx context.Context, request *velav1.LookupWorkerBootstrapRequest) (*velav1.LookupWorkerBootstrapResponse, error) {
	principal, err := server.authenticateBootstrap(ctx)
	if err != nil {
		return nil, err
	}
	if !knownBootstrapMessage(request) {
		return nil, invalidRequest("Worker bootstrap lookup")
	}
	requestID, err := bootstrapUUID(request.GetRequestId())
	if err != nil {
		return nil, invalidRequest("Worker bootstrap lookup")
	}
	history, err := server.lookupBootstrap(ctx, requestID, principal)
	if err != nil {
		return nil, err
	}
	result := &velav1.LookupWorkerBootstrapResponse{Claim: encodeBootstrapClaim(history.Claim, history.ActorIdentity)}
	if history.Receipt != nil {
		result.Pair = encodeBootstrapPair(*history.Receipt, history.RecordedAt)
	}
	if history.Abandonment != nil {
		result.Abandonment = encodeBootstrapAbandonment(*history.Abandonment)
	}
	return result, nil
}

func (server *Server) AbandonWorkerBootstrap(ctx context.Context, request *velav1.AbandonWorkerBootstrapRequest) (*velav1.AbandonWorkerBootstrapResponse, error) {
	principal, err := server.authenticateBootstrap(ctx)
	if err != nil {
		return nil, err
	}
	if !knownBootstrapMessage(request) {
		return nil, invalidRequest("Worker bootstrap abandonment")
	}
	requestID, err := bootstrapUUID(request.GetRequestId())
	if err != nil {
		return nil, invalidRequest("Worker bootstrap abandonment")
	}
	history, err := server.bootstrap.AbandonWorkerBootstrap(ctx, fleet.WorkerBootstrapLookup{
		RequestID: requestID, NodeIdentity: principal.NodeIdentity, ActorIdentity: principal.actorIdentity()})
	if err != nil {
		return nil, mapServiceError("abandon Worker bootstrap", err)
	}
	if !validBootstrapHistory(history, requestID, principal) || history.Abandonment == nil {
		return nil, invalidAuthoritativeResult("Worker bootstrap abandonment")
	}
	return &velav1.AbandonWorkerBootstrapResponse{Claim: encodeBootstrapClaim(history.Claim, history.ActorIdentity),
		Abandonment: encodeBootstrapAbandonment(*history.Abandonment)}, nil
}

func (server *Server) LookupWorkerBootstrapBinding(ctx context.Context, request *velav1.LookupWorkerBootstrapBindingRequest) (*velav1.LookupWorkerBootstrapBindingResponse, error) {
	principal, err := server.authenticateBootstrap(ctx)
	if err != nil {
		return nil, err
	}
	if server.bootstrapSigner == nil {
		return nil, status.Error(codes.Unimplemented, "Registry journal binding signer is not configured")
	}
	if !knownBootstrapMessage(request) {
		return nil, invalidRequest("Worker bootstrap binding")
	}
	requestID, err := bootstrapUUID(request.GetRequestId())
	if err != nil {
		return nil, invalidRequest("Worker bootstrap binding")
	}
	history, err := server.lookupBootstrap(ctx, requestID, principal)
	if err != nil {
		return nil, err
	}
	if history.Receipt == nil {
		return nil, status.Error(codes.FailedPrecondition, "Worker bootstrap has no committed journal pair")
	}
	binding, err := server.bootstrapSigner.Sign(&velav1.WorkerBootstrapBinding{
		SchemaVersion: journalbinding.SchemaVersion, Claim: encodeBootstrapClaim(history.Claim, history.ActorIdentity),
		Pair: encodeBootstrapPair(*history.Receipt, history.RecordedAt),
	})
	if err != nil {
		return nil, invalidAuthoritativeResult("Worker bootstrap binding")
	}
	return &velav1.LookupWorkerBootstrapBindingResponse{Binding: binding}, nil
}

func (server *Server) lookupBootstrap(ctx context.Context, requestID uuid.UUID, principal nodeAgentPrincipal) (fleet.WorkerBootstrapHistory, error) {
	history, err := server.bootstrap.LookupWorkerBootstrap(ctx, fleet.WorkerBootstrapLookup{
		RequestID: requestID, NodeIdentity: principal.NodeIdentity, ActorIdentity: principal.actorIdentity()})
	if err != nil {
		return fleet.WorkerBootstrapHistory{}, mapServiceError("lookup Worker bootstrap", err)
	}
	if !validBootstrapHistory(history, requestID, principal) {
		return fleet.WorkerBootstrapHistory{}, invalidAuthoritativeResult("Worker bootstrap history")
	}
	return history, nil
}

func validBootstrapHistory(history fleet.WorkerBootstrapHistory, requestID uuid.UUID, principal nodeAgentPrincipal) bool {
	if history.Claim.Fresh || history.Claim.RequestID != requestID || history.Claim.NodeIdentity != principal.NodeIdentity ||
		history.ActorIdentity != principal.actorIdentity() || !validBootstrapClaim(history.Claim) ||
		history.Receipt == nil && !history.RecordedAt.IsZero() || history.Receipt != nil &&
		(!validBootstrapReceipt(*history.Receipt) || history.Receipt.RequestID != requestID || history.Receipt.ActorIdentity != history.ActorIdentity ||
			!validBootstrapTime(timestamppb.New(history.RecordedAt))) {
		return false
	}
	return history.Abandonment == nil || history.Receipt == nil && validBootstrapAbandonment(*history.Abandonment, history.Claim)
}

func validBootstrapAbandonment(value fleet.WorkerBootstrapAbandonment, claim fleet.WorkerBootstrapClaim) bool {
	return value.FencedInstanceEpoch > claim.WorkerInstanceEpoch && value.FencedInstanceEpoch-claim.WorkerInstanceEpoch == 1 &&
		validBootstrapTime(timestamppb.New(value.AbandonedAt))
}

func encodeBootstrapAbandonment(value fleet.WorkerBootstrapAbandonment) *velav1.WorkerBootstrapAbandonment {
	return &velav1.WorkerBootstrapAbandonment{FencedInstanceEpoch: value.FencedInstanceEpoch, AbandonedAt: timestamppb.New(value.AbandonedAt)}
}

func bootstrapTarget(request fleet.WorkerBootstrapRequest) (fleet.WorkerBootstrapClaim, error) {
	if request.RequestID == uuid.Nil || request.WorkerInstanceID == uuid.Nil || request.WorkerMemberID == uuid.Nil || request.WorkerInstanceEpoch <= 0 {
		return fleet.WorkerBootstrapClaim{}, errors.New("worker bootstrap identity is invalid")
	}
	bundle, err := fleetcontroller.ParseWorkerBundleActuationManifest(request.BundleManifest)
	if err != nil {
		return fleet.WorkerBootstrapClaim{}, err
	}
	for _, worker := range bundle.WorkerInstances {
		if worker.ID != request.WorkerInstanceID || worker.InstanceEpoch != request.WorkerInstanceEpoch {
			continue
		}
		for _, member := range worker.Members {
			if member.ID == request.WorkerMemberID {
				digest := sha256.Sum256(request.BundleManifest)
				return fleet.WorkerBootstrapClaim{RequestID: request.RequestID, WorkerInstanceID: worker.ID, WorkerInstanceEpoch: worker.InstanceEpoch,
					WorkerMemberID: member.ID, WorkerMemberEpoch: member.MemberEpoch, NodeIdentity: member.NodeIdentity, BundleDigest: digest[:]}, nil
			}
		}
	}
	return fleet.WorkerBootstrapClaim{}, errors.New("worker bootstrap target is absent from the bundle")
}

func sameBootstrapClaimScope(a, b fleet.WorkerBootstrapClaim) bool {
	return a.RequestID == b.RequestID && a.WorkerInstanceID == b.WorkerInstanceID && a.WorkerInstanceEpoch == b.WorkerInstanceEpoch &&
		a.WorkerMemberID == b.WorkerMemberID && a.WorkerMemberEpoch == b.WorkerMemberEpoch && a.NodeIdentity == b.NodeIdentity && bytes.Equal(a.BundleDigest, b.BundleDigest)
}

func validBootstrapClaim(claim fleet.WorkerBootstrapClaim) bool {
	return claim.RequestID != uuid.Nil && claim.WorkerInstanceID != uuid.Nil && claim.WorkerInstanceEpoch > 0 &&
		claim.WorkerMemberID != uuid.Nil && claim.WorkerMemberEpoch > 0 && validText(claim.NodeIdentity, 253) &&
		validBootstrapDigest(claim.BundleDigest) && validBootstrapTime(timestamppb.New(claim.ClaimedAt))
}

func validBootstrapReceipt(receipt fleet.WorkerBootstrapReceipt) bool {
	return receipt.RequestID != uuid.Nil && receipt.WorkerJournalID != uuid.Nil && receipt.RuntimeJournalID != uuid.Nil &&
		receipt.WorkerJournalID != receipt.RuntimeJournalID && validBootstrapDigest(receipt.WorkerScope) && validBootstrapDigest(receipt.RuntimeScope)
}

func validBootstrapDigest(value []byte) bool {
	return len(value) == sha256.Size && [sha256.Size]byte(value) != ([sha256.Size]byte{})
}

func encodeBootstrapClaim(claim fleet.WorkerBootstrapClaim, actor string) *velav1.WorkerBootstrapClaim {
	return &velav1.WorkerBootstrapClaim{RequestId: claim.RequestID.String(), WorkerInstanceId: claim.WorkerInstanceID.String(),
		WorkerInstanceEpoch: claim.WorkerInstanceEpoch, WorkerMemberId: claim.WorkerMemberID.String(), WorkerMemberEpoch: claim.WorkerMemberEpoch,
		NodeIdentity: claim.NodeIdentity, BundleDigest: bytes.Clone(claim.BundleDigest), ClaimedAt: timestamppb.New(claim.ClaimedAt), ActorIdentity: actor}
}

func encodeBootstrapPair(receipt fleet.WorkerBootstrapReceipt, recordedAt time.Time) *velav1.WorkerBootstrapJournalPair {
	return &velav1.WorkerBootstrapJournalPair{RequestId: receipt.RequestID.String(), WorkerJournalId: receipt.WorkerJournalID.String(),
		WorkerScope: bytes.Clone(receipt.WorkerScope), RuntimeJournalId: receipt.RuntimeJournalID.String(), RuntimeScope: bytes.Clone(receipt.RuntimeScope),
		RecordedAt: timestamppb.New(recordedAt), ActorIdentity: receipt.ActorIdentity}
}

func bootstrapUUID(value string) (uuid.UUID, error) {
	id, err := parseUUID(value)
	if err != nil || id.String() != value {
		return uuid.Nil, errors.New("bootstrap UUID is not canonical")
	}
	return id, nil
}

func knownBootstrapMessage(message proto.Message) bool {
	return message != nil && message.ProtoReflect().IsValid() && len(message.ProtoReflect().GetUnknown()) == 0
}

func validBootstrapTime(value *timestamppb.Timestamp) bool {
	return knownBootstrapMessage(value) && value.IsValid() && !value.AsTime().IsZero()
}

package fleet

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

const MaximumWorkerBootstrapManifestBytes = 4 << 20

type WorkerBootstrapRequest struct {
	RequestID           uuid.UUID
	WorkerInstanceID    uuid.UUID
	WorkerInstanceEpoch int64
	WorkerMemberID      uuid.UUID
	BundleManifest      []byte
	ActorIdentity       string
}

type WorkerBootstrapClaim struct {
	RequestID           uuid.UUID
	Fresh               bool
	WorkerInstanceID    uuid.UUID
	WorkerInstanceEpoch int64
	WorkerMemberID      uuid.UUID
	WorkerMemberEpoch   int64
	NodeIdentity        string
	BundleDigest        []byte
	ClaimedAt           time.Time
}

type WorkerBootstrapReceipt struct {
	RequestID        uuid.UUID
	WorkerJournalID  uuid.UUID
	WorkerScope      []byte
	RuntimeJournalID uuid.UUID
	RuntimeScope     []byte
	ActorIdentity    string
}

// ClaimWorkerBootstrap consumes first-use authority before any local journal
// initialization. Fresh is true only for the insert that committed this claim.
// A replay, lost response or ambiguous error never permits initialization.
func (service *Service) ClaimWorkerBootstrap(ctx context.Context, request WorkerBootstrapRequest) (WorkerBootstrapClaim, error) {
	if service == nil || service.registryPool == nil {
		return WorkerBootstrapClaim{}, errors.New("fleet service is not configured")
	}
	if request.RequestID == uuid.Nil || request.WorkerInstanceID == uuid.Nil || request.WorkerInstanceEpoch <= 0 ||
		request.WorkerMemberID == uuid.Nil || !validText(request.ActorIdentity, 500) || len(request.BundleManifest) == 0 ||
		len(request.BundleManifest) > MaximumWorkerBootstrapManifestBytes {
		return WorkerBootstrapClaim{}, &Failure{Code: FailureInvalid, Message: "Worker bootstrap request is invalid"}
	}
	var result WorkerBootstrapClaim
	err := service.registryPool.QueryRow(ctx, `
		SELECT request_id, fresh, worker_instance_id, worker_instance_epoch,
		       worker_member_id, worker_member_epoch, node_identity, bundle_digest, claimed_at
		FROM vela_claim_worker_bootstrap($1, $2, $3, $4, $5, $6)
	`, request.RequestID, request.WorkerInstanceID, request.WorkerInstanceEpoch, request.WorkerMemberID,
		request.BundleManifest, request.ActorIdentity).Scan(&result.RequestID, &result.Fresh, &result.WorkerInstanceID,
		&result.WorkerInstanceEpoch, &result.WorkerMemberID, &result.WorkerMemberEpoch, &result.NodeIdentity,
		&result.BundleDigest, &result.ClaimedAt)
	if err != nil {
		return WorkerBootstrapClaim{}, mapDatabaseError("claim Worker bootstrap", err)
	}
	return result, nil
}

// RecordWorkerBootstrapReceipt binds the prepared local journal identities to
// the consumed claim. It records the trusted provisioner's report, not readiness
// or independent filesystem observation. Exact replay preserves the first time.
func (service *Service) RecordWorkerBootstrapReceipt(ctx context.Context, receipt WorkerBootstrapReceipt) (time.Time, error) {
	if service == nil || service.registryPool == nil {
		return time.Time{}, errors.New("fleet service is not configured")
	}
	if receipt.RequestID == uuid.Nil || receipt.WorkerJournalID == uuid.Nil || receipt.RuntimeJournalID == uuid.Nil ||
		receipt.WorkerJournalID == receipt.RuntimeJournalID || len(receipt.WorkerScope) != 32 || len(receipt.RuntimeScope) != 32 ||
		!validText(receipt.ActorIdentity, 500) {
		return time.Time{}, &Failure{Code: FailureInvalid, Message: "Worker bootstrap receipt is invalid"}
	}
	var recordedAt time.Time
	err := service.registryPool.QueryRow(ctx, `
		SELECT vela_record_worker_bootstrap_receipt($1, $2, $3, $4, $5, $6)
	`, receipt.RequestID, receipt.WorkerJournalID, receipt.WorkerScope, receipt.RuntimeJournalID,
		receipt.RuntimeScope, receipt.ActorIdentity).Scan(&recordedAt)
	if err != nil {
		return time.Time{}, mapDatabaseError("record Worker bootstrap receipt", err)
	}
	return recordedAt, nil
}

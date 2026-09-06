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

type WorkerBootstrapLookup struct {
	RequestID     uuid.UUID
	NodeIdentity  string
	ActorIdentity string
}

type WorkerBootstrapHistory struct {
	Claim         WorkerBootstrapClaim
	ActorIdentity string
	Receipt       *WorkerBootstrapReceipt
	RecordedAt    time.Time
	Abandonment   *WorkerBootstrapAbandonment
}

// WorkerBootstrapAbandonment permanently rejects receipt completion. It does
// not prove local process termination or authorize scratch/device reclamation.
type WorkerBootstrapAbandonment struct {
	FencedInstanceEpoch int64
	AbandonedAt         time.Time
}

// LookupWorkerBootstrap observes immutable history without acquiring permission.
// Missing requests and requests belonging to another principal both return NotFound.
func (service *Service) LookupWorkerBootstrap(ctx context.Context, request WorkerBootstrapLookup) (WorkerBootstrapHistory, error) {
	if service == nil || service.registryPool == nil {
		return WorkerBootstrapHistory{}, errors.New("fleet service is not configured")
	}
	if request.RequestID == uuid.Nil || !validText(request.NodeIdentity, 253) || !validText(request.ActorIdentity, 500) {
		return WorkerBootstrapHistory{}, &Failure{Code: FailureInvalid, Message: "Worker bootstrap lookup is invalid"}
	}
	var result WorkerBootstrapHistory
	var workerID, runtimeID *uuid.UUID
	var workerScope, runtimeScope []byte
	var recordedAt *time.Time
	var abandonedAt *time.Time
	var fencedEpoch *int64
	err := service.registryPool.QueryRow(ctx, `
		SELECT request_id, worker_instance_id, worker_instance_epoch, worker_member_id, worker_member_epoch,
		       node_identity, bundle_digest, claimed_at, actor_identity,
		       worker_journal_id, worker_scope, runtime_journal_id, runtime_scope, recorded_at,
		       fenced_instance_epoch, abandoned_at
		FROM vela_lookup_worker_bootstrap_v2($1, $2, $3)
	`, request.RequestID, request.NodeIdentity, request.ActorIdentity).Scan(
		&result.Claim.RequestID, &result.Claim.WorkerInstanceID, &result.Claim.WorkerInstanceEpoch,
		&result.Claim.WorkerMemberID, &result.Claim.WorkerMemberEpoch, &result.Claim.NodeIdentity,
		&result.Claim.BundleDigest, &result.Claim.ClaimedAt, &result.ActorIdentity,
		&workerID, &workerScope, &runtimeID, &runtimeScope, &recordedAt, &fencedEpoch, &abandonedAt)
	if err != nil {
		return WorkerBootstrapHistory{}, mapDatabaseError("lookup Worker bootstrap", err)
	}
	if workerID != nil && runtimeID != nil && recordedAt != nil {
		result.Receipt = &WorkerBootstrapReceipt{RequestID: request.RequestID, ActorIdentity: result.ActorIdentity,
			WorkerJournalID: *workerID, WorkerScope: workerScope, RuntimeJournalID: *runtimeID, RuntimeScope: runtimeScope}
		result.RecordedAt = *recordedAt
	} else if workerID != nil || runtimeID != nil || recordedAt != nil || workerScope != nil || runtimeScope != nil {
		return WorkerBootstrapHistory{}, errors.New("worker bootstrap history contains an incomplete receipt")
	}
	if fencedEpoch != nil && abandonedAt != nil && result.Receipt == nil &&
		*fencedEpoch > result.Claim.WorkerInstanceEpoch && *fencedEpoch-result.Claim.WorkerInstanceEpoch == 1 && !abandonedAt.IsZero() {
		result.Abandonment = &WorkerBootstrapAbandonment{FencedInstanceEpoch: *fencedEpoch, AbandonedAt: *abandonedAt}
	} else if fencedEpoch != nil || abandonedAt != nil {
		return WorkerBootstrapHistory{}, errors.New("worker bootstrap history contains invalid abandonment")
	}
	return result, nil
}

// AbandonWorkerBootstrap fences only unobserved first-use authority and records
// an immutable terminal outcome. It never grants another initialization.
func (service *Service) AbandonWorkerBootstrap(ctx context.Context, request WorkerBootstrapLookup) (WorkerBootstrapHistory, error) {
	if service == nil || service.registryPool == nil {
		return WorkerBootstrapHistory{}, errors.New("fleet service is not configured")
	}
	if request.RequestID == uuid.Nil || !validText(request.NodeIdentity, 253) || !validText(request.ActorIdentity, 500) {
		return WorkerBootstrapHistory{}, &Failure{Code: FailureInvalid, Message: "Worker bootstrap abandonment is invalid"}
	}
	var abandonedAt time.Time
	if err := service.registryPool.QueryRow(ctx, `SELECT vela_abandon_worker_bootstrap($1, $2, $3)`,
		request.RequestID, request.NodeIdentity, request.ActorIdentity).Scan(&abandonedAt); err != nil {
		return WorkerBootstrapHistory{}, mapDatabaseError("abandon Worker bootstrap", err)
	}
	history, err := service.LookupWorkerBootstrap(ctx, request)
	if err != nil {
		return WorkerBootstrapHistory{}, err
	}
	if history.Abandonment == nil || !history.Abandonment.AbandonedAt.Equal(abandonedAt) {
		return WorkerBootstrapHistory{}, errors.New("worker bootstrap abandonment has no matching committed history")
	}
	return history, nil
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
		[32]byte(receipt.WorkerScope) == ([32]byte{}) || [32]byte(receipt.RuntimeScope) == ([32]byte{}) ||
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

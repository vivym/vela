package retention

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/artifactstore"
)

const retentionReceiptTimeout = 5 * time.Second

var errStorageIdentityInvalid = errors.New("artifact storage identity was invalid")

type deletionTargetAction string

const (
	targetActionObjectVersion   deletionTargetAction = "OBJECT_VERSION"
	targetActionObjectDiscovery deletionTargetAction = "OBJECT_DISCOVERY"
	targetActionMultipartPrefix deletionTargetAction = "MULTIPART_PREFIX"
)

type deletionStorageOutcome string

const (
	storageOutcomeDeleted             deletionStorageOutcome = "DELETED"
	storageOutcomeAlreadyAbsent       deletionStorageOutcome = "ALREADY_ABSENT"
	storageOutcomeMultipartAborted    deletionStorageOutcome = "MULTIPART_ABORTED"
	storageOutcomeNoIncompleteUploads deletionStorageOutcome = "NO_INCOMPLETE_UPLOADS"
)

type deletionExecutionResult struct {
	discoveredVersionID string
	outcome             deletionStorageOutcome
}

type DeletionStore interface {
	DeleteExactVersion(context.Context, string, string) error
	ResolveCurrentVersion(context.Context, string) (artifactstore.ObjectVersion, bool, error)
	ListIncompleteMultipartUploads(
		context.Context,
		string,
	) ([]artifactstore.IncompleteMultipartUpload, error)
	AbortMultipartUpload(context.Context, artifactstore.MultipartUpload) error
}

type ReconcilerConfig struct {
	InstanceID string
	BatchSize  int
	ClaimTTL   time.Duration
	RetryDelay time.Duration
}

type ReconcileResult struct {
	RequestContentExpired   int
	ArtifactRequestsCreated int
	Claimed                 int
	Completed               int
	Failed                  int
}

type Reconciler struct {
	pool         *pgxpool.Pool
	store        DeletionStore
	instanceID   string
	batchSize    int
	claimSeconds int32
	retrySeconds int32
}

func NewReconciler(
	pool *pgxpool.Pool,
	store DeletionStore,
	config ReconcilerConfig,
) (*Reconciler, error) {
	if pool == nil {
		return nil, errors.New("content deletion reconciler database pool is required")
	}
	if store == nil {
		return nil, errors.New("content deletion reconciler store is required")
	}
	if len(config.InstanceID) < 1 || len(config.InstanceID) > 500 ||
		strings.TrimSpace(config.InstanceID) != config.InstanceID {
		return nil, errors.New("content deletion reconciler instance id is invalid")
	}
	if config.BatchSize < 1 || config.BatchSize > 1000 {
		return nil, errors.New("content deletion reconciler batch size must be between 1 and 1000")
	}
	claimSeconds, ok := exactDurationSeconds(config.ClaimTTL, time.Hour)
	if !ok {
		return nil, errors.New("content deletion reconciler claim TTL must be between one second and one hour")
	}
	retrySeconds, ok := exactDurationSeconds(config.RetryDelay, 24*time.Hour)
	if !ok {
		return nil, errors.New("content deletion reconciler retry delay must be between one second and 24 hours")
	}
	return &Reconciler{
		pool:         pool,
		store:        store,
		instanceID:   config.InstanceID,
		batchSize:    config.BatchSize,
		claimSeconds: claimSeconds,
		retrySeconds: retrySeconds,
	}, nil
}

func (r *Reconciler) ReconcileBatch(ctx context.Context) (ReconcileResult, error) {
	if r == nil || r.pool == nil || r.store == nil {
		return ReconcileResult{}, errors.New("content deletion reconciler is not configured")
	}
	if ctx == nil {
		return ReconcileResult{}, errors.New("content deletion reconciliation context is required")
	}
	var result ReconcileResult
	if err := r.pool.QueryRow(ctx, `
		SELECT request_content_completed, artifact_requests_created
		FROM vela_enqueue_expired_content_deletions($1)
	`, r.batchSize).Scan(
		&result.RequestContentExpired,
		&result.ArtifactRequestsCreated,
	); err != nil {
		return ReconcileResult{}, fmt.Errorf("enqueue expired Customer Content: %w", err)
	}
	var reconcileErrors []error
	for range r.batchSize {
		current, err := r.reconcileOne(ctx)
		result.Claimed += current.Claimed
		result.Completed += current.Completed
		result.Failed += current.Failed
		if err != nil {
			reconcileErrors = append(reconcileErrors, err)
		}
		if current.Claimed == 0 {
			break
		}
	}
	return result, errors.Join(reconcileErrors...)
}

func (r *Reconciler) reconcileOne(ctx context.Context) (ReconcileResult, error) {
	claim, found, err := r.claim(ctx)
	if err != nil {
		return ReconcileResult{}, err
	}
	if !found {
		return ReconcileResult{}, nil
	}
	result := ReconcileResult{Claimed: 1}
	execution, operationErr := r.execute(ctx, claim)
	if operationErr != nil {
		result.Failed = 1
		errorCode := "STORAGE_OPERATION_FAILED"
		if errors.Is(operationErr, errStorageIdentityInvalid) {
			errorCode = "STORAGE_IDENTITY_INVALID"
		}
		if markErr := r.markRetry(ctx, claim, errorCode); markErr != nil {
			return result, errors.Join(
				errors.New("content deletion storage operation failed"),
				markErr,
			)
		}
		return result, errors.New("content deletion storage operation failed")
	}
	marked, err := r.markCompleted(ctx, claim, execution)
	if err != nil {
		result.Failed = 1
		return result, err
	}
	if !marked {
		result.Failed = 1
		return result, errors.New("content deletion claim became stale after storage success")
	}
	result.Completed = 1
	return result, nil
}

func (r *Reconciler) claim(ctx context.Context) (deletionTargetClaim, bool, error) {
	claimID := uuid.New()
	var claim deletionTargetClaim
	var action string
	var objectVersionID pgtype.Text
	err := r.pool.QueryRow(ctx, `
		SELECT target_id, target_action::text, object_key, object_version_id
		FROM vela_claim_content_deletion_target($1, $2, $3)
	`, r.instanceID, claimID, r.claimSeconds).Scan(
		&claim.targetID,
		&action,
		&claim.objectKey,
		&objectVersionID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return deletionTargetClaim{}, false, nil
	}
	if err != nil {
		return deletionTargetClaim{}, false, fmt.Errorf("claim Content Deletion target: %w", err)
	}
	claim.claimID = claimID
	claim.action = deletionTargetAction(action)
	if objectVersionID.Valid {
		claim.objectVersionID = objectVersionID.String
	}
	return claim, true, nil
}

func (r *Reconciler) execute(
	ctx context.Context,
	claim deletionTargetClaim,
) (deletionExecutionResult, error) {
	switch claim.action {
	case targetActionObjectVersion:
		if claim.objectKey == "" || claim.objectVersionID == "" {
			return deletionExecutionResult{}, errStorageIdentityInvalid
		}
		if err := r.store.DeleteExactVersion(
			ctx,
			claim.objectKey,
			claim.objectVersionID,
		); err != nil {
			return deletionExecutionResult{}, err
		}
		return deletionExecutionResult{outcome: storageOutcomeDeleted}, nil
	case targetActionObjectDiscovery:
		if claim.objectKey == "" || claim.objectVersionID != "" {
			return deletionExecutionResult{}, errStorageIdentityInvalid
		}
		version, exists, err := r.store.ResolveCurrentVersion(ctx, claim.objectKey)
		if err != nil {
			return deletionExecutionResult{}, err
		}
		if !exists {
			return deletionExecutionResult{outcome: storageOutcomeAlreadyAbsent}, nil
		}
		if version.ObjectKey != claim.objectKey || version.VersionID == "" {
			return deletionExecutionResult{}, errStorageIdentityInvalid
		}
		if err := r.store.DeleteExactVersion(
			ctx,
			version.ObjectKey,
			version.VersionID,
		); err != nil {
			return deletionExecutionResult{}, err
		}
		return deletionExecutionResult{
			discoveredVersionID: version.VersionID,
			outcome:             storageOutcomeDeleted,
		}, nil
	case targetActionMultipartPrefix:
		if claim.objectKey == "" || !strings.HasSuffix(claim.objectKey, "/") ||
			claim.objectVersionID != "" {
			return deletionExecutionResult{}, errStorageIdentityInvalid
		}
		uploads, err := r.store.ListIncompleteMultipartUploads(ctx, claim.objectKey)
		if err != nil {
			return deletionExecutionResult{}, err
		}
		for _, upload := range uploads {
			if !strings.HasPrefix(upload.ObjectKey, claim.objectKey) || upload.UploadID == "" {
				return deletionExecutionResult{}, errStorageIdentityInvalid
			}
			if err := r.store.AbortMultipartUpload(ctx, artifactstore.MultipartUpload{
				ObjectKey: upload.ObjectKey,
				UploadID:  upload.UploadID,
			}); err != nil {
				return deletionExecutionResult{}, err
			}
		}
		if len(uploads) == 0 {
			return deletionExecutionResult{outcome: storageOutcomeNoIncompleteUploads}, nil
		}
		return deletionExecutionResult{outcome: storageOutcomeMultipartAborted}, nil
	default:
		return deletionExecutionResult{}, errStorageIdentityInvalid
	}
}

func (r *Reconciler) markRetry(
	ctx context.Context,
	claim deletionTargetClaim,
	errorCode string,
) error {
	receiptContext, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		retentionReceiptTimeout,
	)
	defer cancel()
	var marked bool
	if err := r.pool.QueryRow(receiptContext, `
		SELECT vela_retry_content_deletion_target($1, $2, $3, $4)
	`, claim.targetID, claim.claimID, r.retrySeconds, errorCode).Scan(&marked); err != nil {
		return fmt.Errorf("record Content Deletion retry: %w", err)
	}
	if !marked {
		return errors.New("content deletion claim became stale after storage failure")
	}
	return nil
}

func (r *Reconciler) markCompleted(
	ctx context.Context,
	claim deletionTargetClaim,
	execution deletionExecutionResult,
) (bool, error) {
	receiptContext, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		retentionReceiptTimeout,
	)
	defer cancel()
	var discovered any
	if execution.discoveredVersionID != "" {
		discovered = execution.discoveredVersionID
	}
	var marked bool
	if err := r.pool.QueryRow(receiptContext, `
		SELECT marked
		FROM vela_complete_content_deletion_target($1, $2, $3, $4, $5)
	`, claim.targetID, claim.claimID, uuid.New(), discovered, string(execution.outcome)).Scan(&marked); err != nil {
		return false, fmt.Errorf("record completed Content Deletion target: %w", err)
	}
	return marked, nil
}

type deletionTargetClaim struct {
	targetID        uuid.UUID
	claimID         uuid.UUID
	action          deletionTargetAction
	objectKey       string
	objectVersionID string
}

func exactDurationSeconds(duration, maximum time.Duration) (int32, bool) {
	if duration < time.Second || duration > maximum || duration%time.Second != 0 {
		return 0, false
	}
	return int32(duration / time.Second), true
}

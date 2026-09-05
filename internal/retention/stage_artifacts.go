package retention

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/vivym/vela/internal/artifactstore"
)

func (r *Reconciler) reconcileStageArtifacts(ctx context.Context) (ReconcileResult, error) {
	var result ReconcileResult
	if _, err := r.pool.Exec(ctx, `SELECT vela_prepare_stage_artifact_lifecycle($1)`, r.batchSize); err != nil {
		return result, fmt.Errorf("prepare StageArtifact lifecycle: %w", err)
	}
	var failures []error
	for range r.batchSize {
		claimID := uuid.New()
		var artifactID uuid.UUID
		var objectKey, objectVersion string
		var digest []byte
		var size int64
		err := r.pool.QueryRow(ctx, `
			SELECT artifact_id, object_key, object_version, sha256, size_bytes
			FROM vela_claim_stage_artifact_deletion($1, $2)
		`, claimID, r.claimSeconds).Scan(&artifactID, &objectKey, &objectVersion, &digest, &size)
		if errors.Is(err, pgx.ErrNoRows) {
			break
		}
		if err != nil {
			return result, errors.Join(append(failures, fmt.Errorf("claim StageArtifact deletion: %w", err))...)
		}
		result.Claimed++
		operationErr := r.store.DeleteExactVersion(ctx, objectKey, objectVersion)
		if operationErr == nil {
			operationErr = r.fencePublication(ctx, objectKey, size, digest)
		}
		receiptCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), retentionReceiptTimeout)
		var marked bool
		receiptErr := r.pool.QueryRow(receiptCtx, `
			SELECT vela_finish_stage_artifact_deletion($1, $2, $3, $4)
		`, artifactID, claimID, operationErr == nil, r.retrySeconds).Scan(&marked)
		cancel()
		if operationErr != nil || receiptErr != nil || !marked {
			result.Failed++
			// Storage errors may include customer object names or credentials.
			failures = append(failures, errors.New("StageArtifact exact-version deletion did not complete"))
			continue
		}
		result.Completed++
	}
	return result, errors.Join(failures...)
}

func (r *Reconciler) reconcileStageMaterializations(ctx context.Context) (ReconcileResult, error) {
	var result ReconcileResult
	var failures []error
	for range r.batchSize {
		claimID := uuid.New()
		var leaseID uuid.UUID
		var objectKey string
		var objectVersion pgtype.Text
		var resolved bool
		var digest []byte
		var size int64
		err := r.pool.QueryRow(ctx, `
			SELECT lease_id, object_key, object_version, resolved, sha256, size_bytes
			FROM vela_claim_stage_materialization_deletion($1, $2)
		`, claimID, r.claimSeconds).Scan(&leaseID, &objectKey, &objectVersion, &resolved, &digest, &size)
		if errors.Is(err, pgx.ErrNoRows) {
			break
		}
		if err != nil {
			return result, errors.Join(append(failures, fmt.Errorf("claim orphan Stage materialization: %w", err))...)
		}
		result.Claimed++
		if !resolved {
			object, exists, resolveErr := r.store.ResolveCurrentVersion(ctx, objectKey)
			err = resolveErr
			if err == nil && exists {
				if object.ObjectKey != objectKey || object.VersionID == "" {
					err = errStorageIdentityInvalid
				} else if !artifactstore.IsPublicationFence(object) {
					objectVersion = pgtype.Text{String: object.VersionID, Valid: true}
				}
			}
			if err == nil {
				var marked bool
				err = r.pool.QueryRow(ctx, `SELECT vela_resolve_stage_materialization_deletion($1, $2, $3)`,
					leaseID, claimID, objectVersion).Scan(&marked)
				if err == nil && !marked {
					err = errors.New("orphan materialization version claim became stale")
				}
			}
		}
		if err == nil && objectVersion.Valid {
			err = r.store.DeleteExactVersion(ctx, objectKey, objectVersion.String)
		}
		if err == nil {
			err = r.fencePublication(ctx, objectKey, size, digest)
		}
		receiptCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), retentionReceiptTimeout)
		var marked bool
		receiptErr := r.pool.QueryRow(receiptCtx, `
			SELECT vela_finish_stage_materialization_deletion($1, $2, $3, $4)
		`, leaseID, claimID, err == nil, r.retrySeconds).Scan(&marked)
		cancel()
		if err != nil || receiptErr != nil || !marked {
			result.Failed++
			failures = append(failures, errors.New("orphan Stage materialization deletion did not complete"))
			continue
		}
		result.Completed++
	}
	return result, errors.Join(failures...)
}

func (r *Reconciler) fencePublication(ctx context.Context, key string, size int64, digest []byte) error {
	store, ok := r.store.(artifactstore.VersionedStore)
	if !ok || len(digest) != sha256.Size || size <= 0 {
		return errors.New("conditional publication fencing is not configured")
	}
	return artifactstore.FenceConditionalPublication(ctx, store, key, size, [sha256.Size]byte(digest))
}

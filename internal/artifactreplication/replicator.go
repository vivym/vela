package artifactreplication

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/artifactstore"
)

const maxBatchSize = 1000

type SourceStore interface {
	ReadExactVersion(context.Context, string, string) (*artifactstore.ExactVersionReader, error)
}

type BackupStore interface {
	PutIfAbsent(
		context.Context,
		string,
		string,
		io.Reader,
		int64,
		[sha256.Size]byte,
	) (artifactstore.ObjectVersion, error)
	HeadCurrentVersion(context.Context, string) (artifactstore.ObjectVersion, error)
}

type Config struct {
	InstanceID string
	BatchSize  int
	ClaimTTL   time.Duration
	RetryDelay time.Duration
}

type Result struct {
	Claimed   int
	Completed int
	Failed    int
}

type Replicator struct {
	pool         *pgxpool.Pool
	source       SourceStore
	backup       BackupStore
	instanceID   string
	batchSize    int
	claimSeconds int32
	retrySeconds int32
}

type claim struct {
	id              uuid.UUID
	claimID         uuid.UUID
	objectKey       string
	objectVersionID string
	sizeBytes       int64
	sha256          [sha256.Size]byte
	contentType     string
}

func New(
	pool *pgxpool.Pool,
	source SourceStore,
	backup BackupStore,
	config Config,
) (*Replicator, error) {
	if pool == nil || source == nil || backup == nil {
		return nil, errors.New("artifact backup replication dependencies are required")
	}
	if config.InstanceID == "" || len(config.InstanceID) > 500 ||
		strings.TrimSpace(config.InstanceID) != config.InstanceID {
		return nil, errors.New("artifact backup replication instance id is invalid")
	}
	if config.BatchSize < 1 || config.BatchSize > maxBatchSize {
		return nil, errors.New("artifact backup replication batch size must be between 1 and 1000")
	}
	claimSeconds, ok := exactDurationSeconds(config.ClaimTTL, time.Hour)
	if !ok {
		return nil, errors.New("artifact backup replication claim TTL must be between one second and one hour")
	}
	retrySeconds, ok := exactDurationSeconds(config.RetryDelay, 24*time.Hour)
	if !ok {
		return nil, errors.New("artifact backup replication retry delay must be between one second and 24 hours")
	}
	return &Replicator{
		pool: pool, source: source, backup: backup,
		instanceID: config.InstanceID, batchSize: config.BatchSize,
		claimSeconds: claimSeconds, retrySeconds: retrySeconds,
	}, nil
}

func (replicator *Replicator) ReplicateBatch(ctx context.Context) (Result, error) {
	if replicator == nil || replicator.pool == nil || replicator.source == nil ||
		replicator.backup == nil {
		return Result{}, errors.New("artifact backup Replicator is not configured")
	}
	if ctx == nil {
		return Result{}, errors.New("artifact backup replication context is required")
	}
	var result Result
	var replicationErrors []error
	for range replicator.batchSize {
		current, err := replicator.replicateOne(ctx)
		result.Claimed += current.Claimed
		result.Completed += current.Completed
		result.Failed += current.Failed
		if err != nil {
			replicationErrors = append(replicationErrors, err)
		}
		if current.Claimed == 0 {
			break
		}
	}
	return result, errors.Join(replicationErrors...)
}

func (replicator *Replicator) replicateOne(ctx context.Context) (Result, error) {
	tx, err := replicator.pool.Begin(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("begin Artifact backup replication: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	claimed, found, err := replicator.claim(ctx, tx)
	if err != nil {
		return Result{}, err
	}
	if !found {
		return Result{}, nil
	}
	result := Result{Claimed: 1}
	backupObject, errorCode, copyErr := replicator.copy(ctx, claimed)
	if copyErr != nil {
		result.Failed = 1
		if err := replicator.markRetry(ctx, tx, claimed, errorCode); err != nil {
			return result, errors.Join(copyErr, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return result, errors.Join(copyErr, fmt.Errorf("commit Artifact backup retry: %w", err))
		}
		return result, copyErr
	}
	marked, err := replicator.markCompleted(ctx, tx, claimed, backupObject)
	if err != nil {
		result.Failed = 1
		return result, err
	}
	if !marked {
		result.Failed = 1
		return result, errors.New("artifact backup replication claim became stale after storage success")
	}
	if err := tx.Commit(ctx); err != nil {
		result.Failed = 1
		return result, fmt.Errorf("commit Artifact backup replication: %w", err)
	}
	result.Completed = 1
	return result, nil
}

func (replicator *Replicator) claim(
	ctx context.Context,
	tx pgx.Tx,
) (claim, bool, error) {
	claimed := claim{claimID: uuid.New()}
	var digest []byte
	err := tx.QueryRow(ctx, `
		SELECT replication_id, source_object_key, source_object_version_id,
		       source_size_bytes, source_sha256, source_content_type
		FROM vela_claim_artifact_backup_replication($1, $2, $3)
	`, replicator.instanceID, claimed.claimID, replicator.claimSeconds).Scan(
		&claimed.id,
		&claimed.objectKey,
		&claimed.objectVersionID,
		&claimed.sizeBytes,
		&digest,
		&claimed.contentType,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return claim{}, false, nil
	}
	if err != nil {
		return claim{}, false, fmt.Errorf("claim Artifact backup replication: %w", err)
	}
	if len(digest) != sha256.Size {
		return claim{}, false, errors.New("claimed Artifact backup replication has invalid checksum")
	}
	copy(claimed.sha256[:], digest)
	return claimed, true, nil
}

func (replicator *Replicator) copy(
	ctx context.Context,
	claimed claim,
) (artifactstore.ObjectVersion, string, error) {
	source, err := replicator.source.ReadExactVersion(
		ctx,
		claimed.objectKey,
		claimed.objectVersionID,
	)
	if err != nil {
		if errors.Is(err, artifactstore.ErrObjectVersionNotFound) {
			return artifactstore.ObjectVersion{}, "SOURCE_MISSING",
				errors.New("artifact backup source version is absent")
		}
		return artifactstore.ObjectVersion{}, "STORAGE_OPERATION_FAILED",
			fmt.Errorf("read Artifact backup source version: %w", err)
	}
	defer func() { _ = source.Close() }()
	if source.ObjectKey != claimed.objectKey || source.VersionID != claimed.objectVersionID ||
		source.SizeBytes != claimed.sizeBytes || source.ContentType != claimed.contentType {
		return artifactstore.ObjectVersion{}, "SOURCE_IDENTITY_MISMATCH",
			errors.New("artifact backup source identity does not match committed metadata")
	}

	backup, err := replicator.backup.PutIfAbsent(
		ctx,
		claimed.objectKey,
		claimed.contentType,
		source,
		claimed.sizeBytes,
		claimed.sha256,
	)
	if errors.Is(err, artifactstore.ErrObjectAlreadyExists) {
		backup, err = replicator.backup.HeadCurrentVersion(ctx, claimed.objectKey)
	}
	if err != nil {
		return artifactstore.ObjectVersion{}, "STORAGE_OPERATION_FAILED",
			fmt.Errorf("write Artifact backup version: %w", err)
	}
	expectedChecksum := base64.StdEncoding.EncodeToString(claimed.sha256[:])
	if backup.ObjectKey != claimed.objectKey || backup.VersionID == "" ||
		backup.SizeBytes != claimed.sizeBytes || backup.ContentType != claimed.contentType ||
		backup.ChecksumSHA256 != expectedChecksum {
		return artifactstore.ObjectVersion{}, "BACKUP_CONFLICT",
			errors.New("artifact backup object conflicts with committed metadata")
	}
	return backup, "", nil
}

func (replicator *Replicator) markCompleted(
	ctx context.Context,
	tx pgx.Tx,
	claimed claim,
	backup artifactstore.ObjectVersion,
) (bool, error) {
	var marked bool
	err := tx.QueryRow(ctx, `
		SELECT vela_complete_artifact_backup_replication($1, $2, $3, $4, $5, $6)
	`, claimed.id, claimed.claimID, backup.VersionID, claimed.sizeBytes,
		claimed.sha256[:], claimed.contentType).Scan(&marked)
	if err != nil {
		return false, fmt.Errorf("complete Artifact backup replication: %w", err)
	}
	return marked, nil
}

func (replicator *Replicator) markRetry(
	ctx context.Context,
	tx pgx.Tx,
	claimed claim,
	errorCode string,
) error {
	var marked bool
	err := tx.QueryRow(ctx, `
		SELECT vela_retry_artifact_backup_replication($1, $2, $3, $4)
	`, claimed.id, claimed.claimID, replicator.retrySeconds, errorCode).Scan(&marked)
	if err != nil {
		return fmt.Errorf("retry Artifact backup replication: %w", err)
	}
	if !marked {
		return errors.New("artifact backup replication claim became stale after storage failure")
	}
	return nil
}

func exactDurationSeconds(value time.Duration, maximum time.Duration) (int32, bool) {
	if value < time.Second || value > maximum || value%time.Second != 0 {
		return 0, false
	}
	return int32(value / time.Second), true
}

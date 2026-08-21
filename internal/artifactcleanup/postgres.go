package artifactcleanup

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	store "github.com/vivym/vela/internal/store/sqlc"
)

type PostgresRegistry struct {
	pool *pgxpool.Pool
}

func NewPostgresRegistry(pool *pgxpool.Pool) (*PostgresRegistry, error) {
	if pool == nil {
		return nil, errors.New("artifact multipart registry database pool is required")
	}
	return &PostgresRegistry{pool: pool}, nil
}

func (registry *PostgresRegistry) IsMultipartUploadRecorded(
	ctx context.Context,
	objectKey string,
	uploadID string,
) (bool, error) {
	if registry == nil || registry.pool == nil {
		return false, errors.New("artifact multipart registry is not configured")
	}
	if ctx == nil || !strings.HasPrefix(objectKey, "artifacts/") ||
		len(objectKey) > 1024 || strings.ContainsRune(objectKey, '\x00') ||
		uploadID == "" || len(uploadID) > 2000 || strings.ContainsRune(uploadID, '\x00') {
		return false, errors.New("invalid Artifact multipart registry lookup")
	}
	return store.New(registry.pool).IsArtifactMultipartUploadRecorded(
		ctx,
		store.IsArtifactMultipartUploadRecordedParams{
			ObjectKey:         objectKey,
			MultipartUploadID: &uploadID,
		},
	)
}

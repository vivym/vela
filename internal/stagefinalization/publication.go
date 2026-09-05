package stagefinalization

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/vivym/vela/internal/artifactstore"
	store "github.com/vivym/vela/internal/store/sqlc"
)

func reservePublicStageArtifact(ctx context.Context, tx pgx.Tx, authority store.LockStageGraphFinalizationCompletionAuthorityRow, source StageGraphFinalizationSource) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO artifacts (id, organization_id, project_id, job_id, attempt_id, attempt_fence,
		    kind, ordinal, object_key, expected_content_type, expires_at, source_stage_artifact_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (attempt_id, kind, ordinal) DO NOTHING
	`, source.publicArtifactID, authority.OrganizationID, authority.ProjectID, authority.JobID,
		authority.AttemptID, authority.AttemptFence, source.ArtifactKind, source.Ordinal,
		source.publicObjectKey, source.ContentType, source.publicExpiresAt, source.StageArtifactID)
	if err != nil {
		return fmt.Errorf("reserve Job-owned public Artifact: %w", err)
	}
	var id, sourceID uuid.UUID
	var key, state string
	var expiresAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT id, source_stage_artifact_id, object_key, state::text, expires_at FROM artifacts
		WHERE attempt_id = $1 AND kind = $2 AND ordinal = $3 FOR UPDATE
	`, authority.AttemptID, source.ArtifactKind, source.Ordinal).Scan(&id, &sourceID, &key, &state, &expiresAt)
	if err != nil || id != source.publicArtifactID || sourceID != source.StageArtifactID ||
		key != source.publicObjectKey || state != "STAGING" || !expiresAt.Equal(source.publicExpiresAt) {
		return errors.New("job-owned public Artifact reservation is inconsistent")
	}
	return nil
}

func (s *Service) publishStageArtifactCopy(ctx context.Context, source StageGraphFinalizationSource) (artifactstore.ObjectVersion, error) {
	deadline := source.publicExpiresAt
	if source.ExpiresAt.Before(deadline) {
		deadline = source.ExpiresAt
	}
	if !deadline.After(time.Now()) {
		return artifactstore.ObjectVersion{}, errors.New("public Artifact copy authority expired")
	}
	copyCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	reader, err := s.artifactStore.ReadExactVersion(copyCtx, source.ObjectKey, source.ObjectVersion)
	if err != nil {
		return artifactstore.ObjectVersion{}, fmt.Errorf("read public Artifact copy source: %w", err)
	}
	defer func() { _ = reader.Close() }()
	object, err := s.artifactStore.PutIfAbsent(copyCtx, source.publicObjectKey, source.ContentType,
		reader, source.SizeBytes, source.SHA256)
	if errors.Is(err, artifactstore.ErrObjectAlreadyExists) {
		var exists bool
		object, exists, err = s.artifactStore.ResolveCurrentVersion(copyCtx, source.publicObjectKey)
		if err == nil && !exists {
			err = errors.New("public Artifact copy disappeared during replay")
		}
	}
	if err != nil {
		return artifactstore.ObjectVersion{}, fmt.Errorf("publish Job-owned Artifact copy: %w", err)
	}
	checksum, err := base64.StdEncoding.DecodeString(object.ChecksumSHA256)
	if err != nil || object.ObjectKey != source.publicObjectKey || object.VersionID == "" ||
		object.SizeBytes != source.SizeBytes || object.ContentType != source.ContentType || !bytes.Equal(checksum, source.SHA256[:]) {
		return artifactstore.ObjectVersion{}, errors.New("public Artifact copy exact identity mismatch")
	}
	verified, err := s.artifactStore.ReadExactVersion(copyCtx, object.ObjectKey, object.VersionID)
	if err != nil {
		return artifactstore.ObjectVersion{}, err
	}
	defer func() { _ = verified.Close() }()
	digest := sha256.New()
	written, err := io.Copy(digest, io.LimitReader(verified, source.SizeBytes+1))
	if err != nil || written != source.SizeBytes || !bytes.Equal(digest.Sum(nil), source.SHA256[:]) {
		return artifactstore.ObjectVersion{}, errors.New("public Artifact copy failed integrity verification")
	}
	return object, nil
}

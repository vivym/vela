//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/artifactaccess"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/stagefinalization"
)

func roleDatabaseURL(t *testing.T, adminDSN, username, password string) string {
	t.Helper()
	dsn, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	dsn.User = url.UserPassword(username, password)
	return dsn.String()
}

func waitForRoleDatabaseLock(t *testing.T, database *sql.DB, role string) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		if err := database.QueryRow(`
			SELECT count(*)
			FROM pg_stat_activity
			WHERE usename = $1 AND wait_event_type = 'Lock'
		`, role).Scan(&waiting); err != nil {
			t.Fatalf("inspect %s database lock wait: %v", role, err)
		}
		if waiting >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s database lock waiter was not observed", role)
}

type recordingArtifactSigner struct{}

func (*recordingArtifactSigner) PresignExactVersion(
	_ context.Context,
	_ string,
	versionID string,
) (artifactstore.SignedRead, error) {
	issuedAt := time.Now().UTC()
	return artifactstore.SignedRead{
		URL:       "https://download.invalid/exact/" + versionID,
		IssuedAt:  issuedAt,
		ExpiresAt: issuedAt.Add(15 * time.Minute),
	}, nil
}

func testArtifactAccessService(pool *pgxpool.Pool) *artifactaccess.Service {
	return artifactaccess.NewService(pool, &recordingArtifactSigner{})
}

type artifactInspectorFunc func(
	context.Context,
	stagefinalization.ArtifactInspectionRequest,
) (stagefinalization.ArtifactInspection, error)

func (inspect artifactInspectorFunc) Inspect(
	ctx context.Context,
	request stagefinalization.ArtifactInspectionRequest,
) (stagefinalization.ArtifactInspection, error) {
	return inspect(ctx, request)
}

func validInspectionForRequest(
	request stagefinalization.ArtifactInspectionRequest,
) stagefinalization.ArtifactInspection {
	return stagefinalization.ArtifactInspection{
		ObjectVersionID:   request.ObjectVersionID,
		SizeBytes:         request.ExpectedSizeBytes,
		SHA256:            request.ExpectedSHA256,
		ContentType:       request.ExpectedContentType,
		Width:             request.ExpectedWidth,
		Height:            request.ExpectedHeight,
		DurationMillis:    request.ExpectedDurationMillis,
		FrameRateMilli:    request.ExpectedFrameRateMilli,
		FrameCount:        request.ExpectedFrameCount,
		Codec:             request.ExpectedCodec,
		Container:         request.ExpectedContainer,
		ValidatorRevision: "ffprobe-stage-visible-completion-v1",
	}
}

func visibleCompletionService(t *testing.T, dsn string) *stagefinalization.Service {
	return stageGraphVisibleCompletionService(
		t,
		dsn,
		artifactInspectorFunc(func(
			_ context.Context,
			request stagefinalization.ArtifactInspectionRequest,
		) (stagefinalization.ArtifactInspection, error) {
			return validInspectionForRequest(request), nil
		}),
	)
}

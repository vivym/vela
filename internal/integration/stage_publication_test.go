//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/stagefinalization"
)

func TestStageGraphPublicCopyReplaysAfterUploadResponseLossAndClaimReplacement(t *testing.T) {
	outcome := runCPUMediaH3Graph(t)
	ctx := context.Background()
	objectStore := &publicCopyResponseLossStore{Local: outcome.objectStore, failNext: true}
	service := visibleCompletionService(t, outcome.database.DSN, objectStore)
	firstOwner := stagefinalization.AuthenticatedFinalizer{ID: "spiffe://vela.internal/finalizer/copy-first"}
	first, err := service.ClaimNextStageGraphFinalization(ctx, firstOwner)
	if err != nil || first.Decision != stagefinalization.StageGraphFinalizationGranted {
		t.Fatalf("first claim=%+v err=%v", first, err)
	}
	if _, err := service.CompleteStageGraphVisibleCompletion(ctx, firstOwner, first.Credentials,
		stagefinalization.StageGraphVisibleCompletionCandidate{CompletionID: uuid.New(), ExpectedJobVersion: first.JobVersion}); err == nil {
		t.Fatal("copy response loss was not observed")
	}
	var staged int
	if err := outcome.database.Admin.QueryRow(`SELECT count(*) FROM artifacts WHERE job_id=$1 AND state='STAGING'`,
		outcome.jobID).Scan(&staged); err != nil || staged != 2 || len(objectStore.created) != 1 {
		t.Fatalf("durable reservations=%d created=%d err=%v", staged, len(objectStore.created), err)
	}
	if _, err := outcome.database.Admin.Exec(`UPDATE stage_graph_finalization_claims SET expires_at=clock_timestamp() WHERE id=$1`, first.ClaimID); err != nil {
		t.Fatal(err)
	}
	secondOwner := stagefinalization.AuthenticatedFinalizer{ID: "spiffe://vela.internal/finalizer/copy-second"}
	second, err := service.ClaimNextStageGraphFinalization(ctx, secondOwner)
	if err != nil || second.Decision != stagefinalization.StageGraphFinalizationGranted || second.ClaimID == first.ClaimID {
		t.Fatalf("replacement claim=%+v err=%v", second, err)
	}
	candidate := stagefinalization.StageGraphVisibleCompletionCandidate{CompletionID: uuid.New(), ExpectedJobVersion: second.JobVersion}
	completed, err := service.CompleteStageGraphVisibleCompletion(ctx, secondOwner, second.Credentials, candidate)
	if err != nil || completed.Decision != stagefinalization.VisibleCompletionCommitted {
		t.Fatalf("complete replacement=%+v err=%v", completed, err)
	}
	replay, err := service.CompleteStageGraphVisibleCompletion(ctx, secondOwner, second.Credentials, candidate)
	if err != nil || replay.CompletionID != completed.CompletionID || replay.ChargeID != completed.ChargeID {
		t.Fatalf("completion replay=%+v err=%v", replay, err)
	}
	var committed, charges, completions int
	if err := outcome.database.Admin.QueryRow(`SELECT
		(SELECT count(*) FROM artifacts WHERE job_id=$1 AND state='COMMITTED'),
		(SELECT count(*) FROM charges WHERE job_id=$1),
		(SELECT count(*) FROM visible_completions WHERE job_id=$1)`, outcome.jobID).Scan(&committed, &charges, &completions); err != nil {
		t.Fatal(err)
	}
	if committed != 2 || charges != 1 || completions != 1 || len(objectStore.created) != 2 {
		t.Fatalf("committed=%d charges=%d completions=%d physical copies=%d", committed, charges, completions, len(objectStore.created))
	}
	acceptStageContentDeletion(t, outcome.database, outcome.jobID)
	if _, err := publicCopyDeletionReconciler(t, outcome.database,
		&stageDeletionResponseLossStore{Local: outcome.objectStore}).ReconcileBatch(ctx); err != nil {
		t.Fatal(err)
	}
	replay, err = service.CompleteStageGraphVisibleCompletion(ctx, secondOwner, second.Credentials, candidate)
	if err != nil || replay.Decision != stagefinalization.VisibleCompletionCommitted ||
		replay.CompletionID != completed.CompletionID || replay.ChargeID != completed.ChargeID {
		t.Fatalf("completion replay after content deletion=%+v err=%v", replay, err)
	}
}

func TestStageGraphPublicCopySurvivesCacheSourceContentDeletion(t *testing.T) {
	fixture := runH3CampaignEvidenceFixture(t)
	completeLabCacheSource(t, fixture)
	ctx := context.Background()
	type exactObject struct {
		key, version string
		digest       []byte
	}
	rows, err := fixture.database.Admin.Query(`SELECT target.object_key,target.object_version_id,target.sha256
		FROM artifacts target JOIN artifacts source ON source.source_stage_artifact_id=target.source_stage_artifact_id
		WHERE target.job_id=$1 AND source.job_id=$2 AND target.state='COMMITTED'`, fixture.cacheJobID, fixture.sourceJobID)
	if err != nil {
		t.Fatal(err)
	}
	var copies []exactObject
	for rows.Next() {
		var object exactObject
		if err := rows.Scan(&object.key, &object.version, &object.digest); err != nil {
			t.Fatal(err)
		}
		copies = append(copies, object)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(copies) == 0 {
		t.Fatal("fixture has no shared cache output provenance")
	}
	deletion := acceptStageContentDeletion(t, fixture.database, fixture.sourceJobID)
	objectStore := &stageDeletionResponseLossStore{Local: fixture.objectStore}
	reconciler := publicCopyDeletionReconciler(t, fixture.database, objectStore)
	if _, err := reconciler.ReconcileBatch(ctx); err != nil {
		t.Fatal(err)
	}
	var requestState string
	var sourceRemaining, targetCommitted int
	if err := fixture.database.Admin.QueryRow(`SELECT request.state::text,
		(SELECT count(*) FROM artifacts WHERE job_id=$2 AND state<>'DELETED')+
		(SELECT count(*) FROM stage_artifacts WHERE job_id=$2 AND state<>'DELETED'),
		(SELECT count(*) FROM artifacts WHERE job_id=$3 AND state='COMMITTED')
		FROM content_deletion_requests request WHERE request.id=$1`, deletion.RequestID, fixture.sourceJobID, fixture.cacheJobID).
		Scan(&requestState, &sourceRemaining, &targetCommitted); err != nil {
		t.Fatal(err)
	}
	if requestState != "COMPLETED" || sourceRemaining != 0 || targetCommitted != 2 {
		t.Fatalf("deletion=%s source remaining=%d consumer committed=%d", requestState, sourceRemaining, targetCommitted)
	}
	for _, object := range copies {
		reader, err := objectStore.ReadExactVersion(ctx, object.key, object.version)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.New()
		_, readErr := io.Copy(digest, reader)
		_ = reader.Close()
		if readErr != nil || string(digest.Sum(nil)) != string(object.digest) {
			t.Fatalf("consumer copy integrity error=%v", readErr)
		}
	}
}

func TestStageGraphAbandonedPublicCopyWaitsForPublicationDeadline(t *testing.T) {
	outcome := runCPUMediaH3Graph(t)
	ctx := context.Background()
	var deadline time.Time
	tx, err := outcome.database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(`UPDATE attempts SET finalization_deadline_at=clock_timestamp()+interval '2 seconds'
		WHERE id=$1 RETURNING finalization_deadline_at`, outcome.attemptID).Scan(&deadline); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	objectStore := &publicCopyResponseLossStore{Local: outcome.objectStore, failNext: true}
	service := visibleCompletionService(t, outcome.database.DSN, objectStore)
	owner := stagefinalization.AuthenticatedFinalizer{ID: "spiffe://vela.internal/finalizer/copy-abandoned"}
	claim, err := service.ClaimNextStageGraphFinalization(ctx, owner)
	if err != nil || claim.Decision != stagefinalization.StageGraphFinalizationGranted {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	if _, err := service.CompleteStageGraphVisibleCompletion(ctx, owner, claim.Credentials,
		stagefinalization.StageGraphVisibleCompletionCandidate{CompletionID: uuid.New(), ExpectedJobVersion: claim.JobVersion}); err == nil {
		t.Fatal("copy response loss was not observed")
	}
	deletion := acceptStageContentDeletion(t, outcome.database, outcome.jobID)
	reconciler := publicCopyDeletionReconciler(t, outcome.database, &stageDeletionResponseLossStore{Local: outcome.objectStore})
	if _, err := reconciler.ReconcileBatch(ctx); err != nil {
		t.Fatal(err)
	}
	var requestState string
	if err := outcome.database.Admin.QueryRow(`SELECT state::text FROM content_deletion_requests WHERE id=$1`, deletion.RequestID).Scan(&requestState); err != nil || requestState == "COMPLETED" {
		t.Fatalf("premature completion=%s err=%v", requestState, err)
	}
	for _, object := range objectStore.created {
		reader, err := objectStore.ReadExactVersion(ctx, object.ObjectKey, object.VersionID)
		if err != nil {
			t.Fatalf("publication reclaimed before deadline: %v", err)
		}
		_ = reader.Close()
	}
	time.Sleep(time.Until(deadline.Add(time.Millisecond)))
	if _, err := reconciler.ReconcileBatch(ctx); err != nil {
		t.Fatal(err)
	}
	if err := outcome.database.Admin.QueryRow(`SELECT state::text FROM content_deletion_requests WHERE id=$1`, deletion.RequestID).Scan(&requestState); err != nil || requestState != "COMPLETED" {
		t.Fatalf("abandoned completion=%s err=%v", requestState, err)
	}
	for _, object := range objectStore.created {
		if _, err := objectStore.ReadExactVersion(ctx, object.ObjectKey, object.VersionID); !errors.Is(err, artifactstore.ErrObjectVersionNotFound) {
			t.Fatalf("abandoned copy remains: %v", err)
		}
	}
}

func TestStageGraphPublicCopyMigrationEmptyDownUp(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 74)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 73); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(database.Admin, migrations, 74); err != nil {
		t.Fatal(err)
	}
}

func TestStageGraphPublicCopyCleanupFencesPutThatIgnoresCancellation(t *testing.T) {
	for _, lateFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "marker wins", true: "late content wins"}[lateFirst], func(t *testing.T) {
			outcome := runCPUMediaH3Graph(t)
			ctx := context.Background()
			tx, err := outcome.database.Admin.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
				t.Fatal(err)
			}
			var deadline time.Time
			if err := tx.QueryRow(`UPDATE attempts SET finalization_deadline_at=clock_timestamp()+interval '2 seconds'
				WHERE id=$1 RETURNING finalization_deadline_at`, outcome.attemptID).Scan(&deadline); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			objectStore := &delayedPublicCopyStore{Local: outcome.objectStore, started: make(chan struct{}), release: make(chan struct{})}
			service := visibleCompletionService(t, outcome.database.DSN, objectStore)
			owner := stagefinalization.AuthenticatedFinalizer{ID: "spiffe://vela.internal/finalizer/delayed-copy"}
			claim, err := service.ClaimNextStageGraphFinalization(ctx, owner)
			if err != nil || claim.Decision != stagefinalization.StageGraphFinalizationGranted {
				t.Fatalf("claim=%+v err=%v", claim, err)
			}
			finished := make(chan error, 1)
			go func() {
				_, err := service.CompleteStageGraphVisibleCompletion(ctx, owner, claim.Credentials,
					stagefinalization.StageGraphVisibleCompletionCandidate{CompletionID: uuid.New(), ExpectedJobVersion: claim.JobVersion})
				finished <- err
			}()
			<-objectStore.started
			deletion := acceptStageContentDeletion(t, outcome.database, outcome.jobID)
			reconciler := publicCopyDeletionReconciler(t, outcome.database, objectStore)
			time.Sleep(time.Until(deadline.Add(time.Millisecond)))
			if lateFirst {
				close(objectStore.release)
				if err := <-finished; err == nil {
					t.Fatal("expired publication unexpectedly completed")
				}
			}
			if _, err := reconciler.ReconcileBatch(ctx); err != nil {
				t.Fatal(err)
			}
			var state string
			if err := outcome.database.Admin.QueryRow(`SELECT state::text FROM content_deletion_requests WHERE id=$1`, deletion.RequestID).Scan(&state); err != nil || state != "COMPLETED" {
				t.Fatalf("fenced deletion=%s err=%v", state, err)
			}
			if !lateFirst {
				close(objectStore.release)
				if err := <-finished; err == nil {
					t.Fatal("late publication bypassed permanent fence")
				}
			}
			marker, exists, err := objectStore.ResolveCurrentVersion(ctx, objectStore.key)
			if err != nil || !exists || !artifactstore.IsPublicationFence(marker) {
				t.Fatalf("marker=%+v exists=%t err=%v", marker, exists, err)
			}
			if objectStore.lateVersion != "" {
				if _, err := objectStore.ReadExactVersion(ctx, objectStore.key, objectStore.lateVersion); !errors.Is(err, artifactstore.ErrObjectVersionNotFound) {
					t.Fatalf("late exact content remains: %v", err)
				}
			}
			if _, err := reconciler.ReconcileBatch(ctx); err != nil {
				t.Fatal(err)
			}
			after, _, _ := objectStore.ResolveCurrentVersion(ctx, objectStore.key)
			if after.VersionID != marker.VersionID {
				t.Fatal("cleanup replay removed publication fence")
			}
		})
	}
}

type delayedPublicCopyStore struct {
	*artifactstore.Local
	mu               sync.Mutex
	delayed          bool
	started, release chan struct{}
	key, lateVersion string
}

func (store *delayedPublicCopyStore) PutIfAbsent(ctx context.Context, key, contentType string, reader io.Reader, size int64, digest [sha256.Size]byte) (artifactstore.ObjectVersion, error) {
	store.mu.Lock()
	delay := !store.delayed
	store.delayed = true
	store.mu.Unlock()
	if !delay {
		return store.Local.PutIfAbsent(ctx, key, contentType, reader, size, digest)
	}
	payload, err := io.ReadAll(reader)
	if err != nil {
		return artifactstore.ObjectVersion{}, err
	}
	store.key = key
	close(store.started)
	<-store.release
	object, err := store.Local.PutIfAbsent(context.Background(), key, contentType, bytes.NewReader(payload), size, digest)
	store.lateVersion = object.VersionID
	return object, err
}

func (*delayedPublicCopyStore) ListIncompleteMultipartUploads(context.Context, string) ([]artifactstore.IncompleteMultipartUpload, error) {
	return nil, nil
}
func (*delayedPublicCopyStore) AbortMultipartUpload(context.Context, artifactstore.MultipartUpload) error {
	return errors.New("unexpected multipart upload")
}

type publicCopyResponseLossStore struct {
	*artifactstore.Local
	failNext bool
	created  []artifactstore.ObjectVersion
}

func (store *publicCopyResponseLossStore) PutIfAbsent(ctx context.Context, key, contentType string, reader io.Reader, size int64, digest [sha256.Size]byte) (artifactstore.ObjectVersion, error) {
	object, err := store.Local.PutIfAbsent(ctx, key, contentType, reader, size, digest)
	if err == nil {
		store.created = append(store.created, object)
		if store.failNext {
			store.failNext = false
			return artifactstore.ObjectVersion{}, errors.New("simulated public copy response loss")
		}
	}
	return object, err
}

func acceptStageContentDeletion(t *testing.T, database testDatabase, jobID uuid.UUID) retention.DeletionRequest {
	t.Helper()
	if _, err := database.Admin.Exec(`UPDATE credentials SET scopes=ARRAY['jobs:submit','jobs:read','jobs:cancel','content_deletion:manage'] WHERE id=$1`, testCredentialID); err != nil {
		t.Fatal(err)
	}
	principal, err := identity.NewAuthenticator(newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"), testCredentialPepper).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatal(err)
	}
	service, err := retention.NewService(newRolePool(t, database.DSN, "vela_retention_request_login", "vela-retention-request-password"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.AcceptContentDeletion(context.Background(), principal, uuid.MustParse(testProjectID), jobID, "delete-"+jobID.String())
	if err != nil {
		t.Fatal(err)
	}
	return result
}

type emptyPublicCopyBackupStore struct{}

func (*emptyPublicCopyBackupStore) PurgeObjectVersions(context.Context, string) (artifactstore.ObjectVersionsPurgeResult, error) {
	return artifactstore.ObjectVersionsPurgeResult{}, nil
}

func publicCopyDeletionReconciler(t *testing.T, database testDatabase, objectStore retention.DeletionStore) *retention.Reconciler {
	t.Helper()
	reconciler, err := retention.NewReconciler(newRolePool(t, database.DSN, "vela_retention_login", "vela-retention-password"), objectStore, retention.ReconcilerConfig{
		InstanceID: "public-copy-deletion", BatchSize: 100, ClaimTTL: time.Minute, RetryDelay: time.Second,
		BackupPool: newRolePool(t, database.DSN, "vela_backup_retention_login", "vela-backup-retention-password"), BackupStore: &emptyPublicCopyBackupStore{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return reconciler
}

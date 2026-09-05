//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stagecache"
)

func TestStageArtifactContentDeletionWaitsForConsumerAndRetriesExactVersion(t *testing.T) {
	fixture := newPinnedStageCacheFixture(t, "stage-lifecycle-deletion")
	ctx := context.Background()
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel', 'content_deletion:manage']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatal(err)
	}
	principal, err := identity.NewAuthenticator(
		newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(ctx, testBearerCredential())
	if err != nil {
		t.Fatal(err)
	}
	service, err := retention.NewService(newRolePool(
		t, fixture.database.DSN, "vela_retention_request_login", "vela-retention-request-password",
	))
	if err != nil {
		t.Fatal(err)
	}
	store := &stageDeletionResponseLossStore{Local: fixture.objectStore, failNext: true}
	reconciler := newStageLifecycleReconciler(t, fixture.database, store)
	canceled := cancelJob(t, fixture.serverURL, testProjectID, fixture.sourceJobID.String(), testBearerCredential())
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel source: %d %s", canceled.StatusCode, canceled.Body)
	}
	if _, err := reconciler.ReconcileBatch(ctx); err != nil {
		t.Fatal(err)
	}
	var sourcePins, targetPins int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*) FILTER (WHERE owner_job_id = $1 AND state = 'ACTIVE'),
		       count(*) FILTER (WHERE owner_job_id = $2 AND state = 'ACTIVE')
		FROM stage_artifact_pins
	`, fixture.sourceJobID, fixture.targetJobID).Scan(&sourcePins, &targetPins); err != nil {
		t.Fatal(err)
	}
	if sourcePins != 0 || targetPins != 2 || store.deletes != 0 {
		t.Fatalf("terminal release source=%d target=%d deletes=%d", sourcePins, targetPins, store.deletes)
	}
	deletion, err := service.AcceptContentDeletion(ctx, principal, uuid.MustParse(testProjectID),
		fixture.sourceJobID, "stage-source-delete")
	if err != nil {
		t.Fatal(err)
	}
	if result, err := reconciler.ReconcileBatch(ctx); err != nil || result.Completed != 0 || store.deletes != 0 {
		t.Fatalf("pinned deletion result=%+v deletes=%d err=%v", result, store.deletes, err)
	}
	var entryState, artifactState, requestState, objectKey string
	if err := fixture.database.Admin.QueryRow(`
		SELECT entry.state::text, artifact.state::text, request.state::text, artifact.object_key
		FROM stage_cache_entries AS entry
		JOIN stage_artifacts AS artifact ON artifact.id = entry.stage_artifact_id
		JOIN content_deletion_requests AS request ON request.id = $2
		WHERE entry.id = $1
	`, fixture.entryID, deletion.RequestID).Scan(&entryState, &artifactState, &requestState, &objectKey); err != nil {
		t.Fatal(err)
	}
	if entryState != "EVICTING" || artifactState != "COMMITTED" || requestState == "COMPLETED" {
		t.Fatalf("pinned state cache=%s artifact=%s request=%s", entryState, artifactState, requestState)
	}
	reader, err := store.ReadExactVersion(ctx, objectKey, fixture.hit.ObjectVersion)
	if err != nil {
		t.Fatalf("pinned exact version lost: %v", err)
	}
	_ = reader.Close()
	canceled = cancelJob(t, fixture.serverURL, testProjectID, fixture.targetJobID.String(), testBearerCredential())
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel consumer: %d %s", canceled.StatusCode, canceled.Body)
	}
	result, err := reconciler.ReconcileBatch(ctx)
	if err == nil || result.Failed != 1 || store.deletes != 1 {
		t.Fatalf("response loss result=%+v deletes=%d err=%v", result, store.deletes, err)
	}
	if err := fixture.database.Admin.QueryRow(`SELECT state::text FROM content_deletion_requests WHERE id = $1`,
		deletion.RequestID).Scan(&requestState); err != nil || requestState == "COMPLETED" {
		t.Fatalf("unverified request completion=%s err=%v", requestState, err)
	}
	if _, err := fixture.database.Admin.Exec(`UPDATE stage_artifact_deletions SET retry_at = clock_timestamp()`); err != nil {
		t.Fatal(err)
	}
	result, err = reconciler.ReconcileBatch(ctx)
	if err != nil || result.Completed < 2 || store.deletes != 2 {
		t.Fatalf("exact-version replay result=%+v deletes=%d err=%v", result, store.deletes, err)
	}
	var pendingPins, chargeCount, completedDeletions int
	if err := fixture.database.Admin.QueryRow(`
		SELECT artifact.state::text, request.state::text,
		       (SELECT count(*) FROM stage_artifact_pins WHERE state = 'ACTIVE'),
		       (SELECT count(*) FROM charges),
		       (SELECT count(*) FROM stage_artifact_deletions WHERE completed_at IS NOT NULL)
		FROM stage_artifacts AS artifact JOIN content_deletion_requests AS request ON request.id = $2
		WHERE artifact.id = $1
	`, fixture.hit.ArtifactID, deletion.RequestID).Scan(&artifactState, &requestState, &pendingPins, &chargeCount, &completedDeletions); err != nil {
		t.Fatal(err)
	}
	if artifactState != "DELETED" || requestState != "COMPLETED" || pendingPins != 0 || chargeCount != 2 || completedDeletions != 1 {
		t.Fatalf("completed artifact=%s request=%s pins=%d charges=%d deletes=%d", artifactState, requestState, pendingPins, chargeCount, completedDeletions)
	}
	if _, err := store.ReadExactVersion(ctx, objectKey, fixture.hit.ObjectVersion); !errors.Is(err, artifactstore.ErrObjectVersionNotFound) {
		t.Fatalf("deleted StageArtifact remains readable: %v", err)
	}
	if _, err := reconciler.ReconcileBatch(ctx); err != nil || store.deletes != 2 {
		t.Fatalf("terminal replay repeated object deletion: deletes=%d err=%v", store.deletes, err)
	}
}

func TestStageMaterializationOrphanDeletionAfterUploadBeforeCommit(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry allowed", true: "retry exhausted"}[exhausted], func(t *testing.T) {
			database, _, coordinator, _, attemptID, runID, _ := newStageGraphCancellationFixture(t, "orphan-materialization")
			assignment := assignAndStartEncoder(t, database, coordinator, attemptID, runID, time.Now().Add(time.Hour))
			if exhausted {
				tx, err := database.Admin.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Exec(`UPDATE stage_retry_budgets SET max_attempts=attempts_consumed WHERE stage_run_id=$1`, runID); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			repository, err := stageartifact.NewPostgresRepository(newRolePool(t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password"))
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Millisecond)
			payload := []byte("uploaded object whose database commit was lost")
			digest := sha256.Sum256(payload)
			lease, err := repository.Seal(ctx, stageartifact.SealCommand{
				CommandID: uuid.New(), AttemptID: attemptID, StageRunID: runID,
				StageAttemptID: assignment.StageAttemptID, StageAllocationID: assignment.StageAllocationID,
				StageLeaseID: assignment.StageLeaseID, ExpectedAttemptFence: 1, ExpectedStageFence: 1,
				ExpectedStageVersion: 3, OutputPort: "conditioning", LocalReceiptID: "orphan-output",
				LocalReceiptDigest: digest, ManifestSHA256: digest, SHA256: digest, LineageDigest: digest,
				TokenDigest: digest, SizeBytes: int64(len(payload)), ArtifactID: uuid.New(), MaterializationLeaseID: uuid.New(),
				ObjectKey:   "artifacts/stage/org/project/" + attemptID.String() + "/encoder/orphan.bin",
				ContentType: "application/octet-stream", SealedAt: now, LeaseExpiresAt: now.Add(time.Second),
			})
			if err != nil {
				t.Fatal(err)
			}
			store := &stageDeletionResponseLossStore{Local: artifactstore.NewLocal(), failNext: true}
			publisher, err := stageartifact.NewObjectStorePublisher(store, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			object, err := publisher.Publish(ctx, lease, bytes.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			reconciler := newStageLifecycleReconciler(t, database, store)
			if result, err := reconciler.ReconcileBatch(ctx); err != nil || result.Claimed != 0 || store.deletes != 0 {
				t.Fatalf("live publication was reclaimed: %+v deletes=%d err=%v", result, store.deletes, err)
			}
			time.Sleep(time.Until(lease.ExpiresAt.Add(time.Millisecond)))
			if result, err := reconciler.ReconcileBatch(ctx); err == nil || result.Failed != 1 || store.deletes != 1 {
				t.Fatalf("orphan response loss=%+v deletes=%d err=%v", result, store.deletes, err)
			}
			var savedVersion string
			if err := database.Admin.QueryRow(`
		SELECT object_version FROM stage_materialization_deletions WHERE materialization_lease_id = $1
	`, lease.ID).Scan(&savedVersion); err != nil || savedVersion != object.ObjectVersion {
				t.Fatalf("orphan exact version was not frozen: %s err=%v", savedVersion, err)
			}
			if _, err := database.Admin.Exec(`UPDATE stage_materialization_deletions SET retry_at = clock_timestamp()`); err != nil {
				t.Fatal(err)
			}
			if result, err := reconciler.ReconcileBatch(ctx); err != nil || result.Completed != 1 || store.deletes != 2 {
				t.Fatalf("orphan exact replay=%+v deletes=%d err=%v", result, store.deletes, err)
			}
			_, err = repository.Commit(ctx, stageartifact.CommitCommand{
				CommandID: uuid.New(), ProgressReceiptID: uuid.New(), MaterializationLeaseID: lease.ID,
				ArtifactID: lease.ArtifactID, ObjectKey: lease.ObjectKey, ObjectVersion: object.ObjectVersion,
				SHA256: lease.SHA256, SizeBytes: lease.SizeBytes, TokenDigest: lease.TokenDigest, CommittedAt: now,
			})
			assertPostgresConstraint(t, err, "stage_artifact_commit_authority_stale")
			if _, err := store.ReadExactVersion(ctx, lease.ObjectKey, object.ObjectVersion); !errors.Is(err, artifactstore.ErrObjectVersionNotFound) {
				t.Fatalf("orphan remains readable: %v", err)
			}
			if _, err := store.PutIfAbsent(ctx, lease.ObjectKey, lease.ContentType, bytes.NewReader(payload), lease.SizeBytes, lease.SHA256); !errors.Is(err, artifactstore.ErrObjectAlreadyExists) {
				t.Fatalf("retired materialization key accepted a delayed conditional PUT: %v", err)
			}
			expectedState := "RETRY_WAIT"
			if exhausted {
				expectedState = "FAILED"
			}
			if decisions, err := coordinator.Reconcile(ctx, 100); err != nil || len(decisions) != 1 ||
				decisions[0].StageRunID != runID || decisions[0].State != expectedState ||
				decisions[0].Reason != "MATERIALIZATION_AUTHORITY_EXPIRED" {
				t.Fatalf("execution recovery after retention cleanup=%+v err=%v", decisions, err)
			}
		})
	}
}

func TestStageCacheExpiryRecyclesQuotaWithoutDeletingPinnedArtifact(t *testing.T) {
	fixture := newPinnedStageCacheFixture(t, "stage-lifecycle-expiry")
	ctx := context.Background()
	now := time.Now().UTC()
	if _, err := fixture.cache.SetProjectControl(ctx, stagecache.ProjectControlCommand{
		OrganizationID: uuid.MustParse(testOrganizationID), ProjectID: uuid.MustParse(testProjectID),
		CachePolicyRevisionID: uuid.MustParse(h3CachePolicyID), Enabled: true,
		MaxEntries: 2, MaxBytes: 1 << 30, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	key := fixture.cacheKey
	key[0] ^= 1
	command := stagecache.AdmitCommand{
		CommandID: uuid.New(), EntryID: uuid.New(), ArtifactID: fixture.hit.ArtifactID,
		CachePolicyRevisionID: uuid.MustParse(h3CachePolicyID), StageProfileRevisionID: uuid.MustParse(encoderStageProfileID),
		ResultEquivalenceRevisionID: uuid.MustParse(h3EncoderEquivalence), Scope: stagecache.ScopeProject,
		StageKey: "encoder", CacheKeyDigest: key, AdmittedAt: now, ExpiresAt: now.Add(20 * time.Millisecond),
	}
	if _, err := fixture.cache.Admit(ctx, command); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(command.ExpiresAt.Add(time.Millisecond)))
	store := &stageDeletionResponseLossStore{Local: fixture.objectStore}
	if _, err := newStageLifecycleReconciler(t, fixture.database, store).ReconcileBatch(ctx); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := fixture.database.Admin.QueryRow(`SELECT state::text FROM stage_cache_entries WHERE id = $1`,
		command.EntryID).Scan(&state); err != nil || state != "EXPIRED" || store.deletes != 0 {
		t.Fatalf("expired cache state=%s object deletes=%d err=%v", state, store.deletes, err)
	}
	command.CommandID, command.EntryID = uuid.New(), uuid.New()
	command.AdmittedAt = time.Now().UTC()
	command.ExpiresAt = command.AdmittedAt.Add(time.Hour)
	if result, err := fixture.cache.Admit(ctx, command); err != nil || result.Deduplicated {
		t.Fatalf("expired exact key cannot be re-admitted: %+v err=%v", result, err)
	}
}

func TestStageArtifactLifecycleMigrationEmptyDownUp(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 71)
	if err := goose.DownTo(database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), 70); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), 71); err != nil {
		t.Fatal(err)
	}
}

func TestStageArtifactDeletionReceiptRejectsExpiryWhileWaitingForLock(t *testing.T) {
	fixture := newPinnedStageCacheFixture(t, "stage-deletion-receipt-clock")
	ctx := context.Background()
	acceptStageContentDeletion(t, fixture.database, fixture.sourceJobID)
	canceled := cancelJob(t, fixture.serverURL, testProjectID, fixture.targetJobID.String(), testBearerCredential())
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel consumer: %d %s", canceled.StatusCode, canceled.Body)
	}
	pool := newRolePool(t, fixture.database.DSN, "vela_retention_login", "vela-retention-password")
	if _, err := pool.Exec(ctx, `SELECT vela_prepare_stage_artifact_lifecycle(100)`); err != nil {
		t.Fatal(err)
	}
	claimID := uuid.New()
	var artifactID uuid.UUID
	var key, version string
	if err := pool.QueryRow(ctx, `SELECT artifact_id,object_key,object_version
		FROM vela_claim_stage_artifact_deletion($1,1)`, claimID).Scan(&artifactID, &key, &version); err != nil {
		t.Fatal(err)
	}
	if err := fixture.objectStore.DeleteExactVersion(ctx, key, version); err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var expiresAt time.Time
	if err := tx.QueryRow(`SELECT claim_expires_at FROM stage_artifact_deletions
		WHERE stage_artifact_id=$1 FOR UPDATE`, artifactID).Scan(&expiresAt); err != nil {
		t.Fatal(err)
	}
	type finishOutcome struct {
		marked bool
		err    error
	}
	finished := make(chan finishOutcome, 1)
	go func() {
		var outcome finishOutcome
		outcome.err = pool.QueryRow(ctx, `SELECT vela_finish_stage_artifact_deletion($1,$2,true,1)`,
			artifactID, claimID).Scan(&outcome.marked)
		finished <- outcome
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_retention_login")
	time.Sleep(time.Until(expiresAt.Add(time.Millisecond)))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	result := <-finished
	if result.err != nil || result.marked {
		t.Fatalf("expired deletion receipt marked=%t err=%v", result.marked, result.err)
	}
	var state string
	if err := fixture.database.Admin.QueryRow(`SELECT state::text FROM stage_artifacts WHERE id=$1`, artifactID).
		Scan(&state); err != nil || state != "DELETION_BLOCKED" {
		t.Fatalf("expired receipt state=%s err=%v", state, err)
	}
}

type stageDeletionResponseLossStore struct {
	*artifactstore.Local
	failNext bool
	deletes  int
}

func (store *stageDeletionResponseLossStore) DeleteExactVersion(ctx context.Context, key, version string) error {
	store.deletes++
	if err := store.Local.DeleteExactVersion(ctx, key, version); err != nil {
		return err
	}
	if store.failNext {
		store.failNext = false
		return errors.New("simulated object deletion response loss")
	}
	return nil
}

func (store *stageDeletionResponseLossStore) ListIncompleteMultipartUploads(context.Context, string) ([]artifactstore.IncompleteMultipartUpload, error) {
	return nil, nil
}

func (store *stageDeletionResponseLossStore) AbortMultipartUpload(context.Context, artifactstore.MultipartUpload) error {
	return errors.New("unexpected multipart upload")
}

func newStageLifecycleReconciler(t *testing.T, database testDatabase, store retention.DeletionStore) *retention.Reconciler {
	t.Helper()
	reconciler, err := retention.NewReconciler(newRolePool(t, database.DSN, "vela_retention_login", "vela-retention-password"),
		store, retention.ReconcilerConfig{InstanceID: "stage-lifecycle", BatchSize: 100, ClaimTTL: time.Minute, RetryDelay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return reconciler
}

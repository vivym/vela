//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/artifactreplication"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/workercontrol"
)

type committedReplicationArtifact struct {
	id          uuid.UUID
	object      artifactstore.ObjectVersion
	payload     []byte
	digest      [sha256.Size]byte
	contentType string
}

type committedReplicationFixture struct {
	start     startFixture
	minio     *minIOFixture
	primary   *artifactstore.S3
	backup    *artifactstore.S3
	artifacts []committedReplicationArtifact
}

type artifactBackupDatabaseClaim struct {
	id          uuid.UUID
	objectKey   string
	versionID   string
	sizeBytes   int64
	digest      []byte
	contentType string
}

func TestCommittedArtifactBackupReplicationCopiesExactVersionOnce(t *testing.T) {
	ctx := context.Background()
	fixture := newCommittedReplicationFixture(t, "artifact-backup-exact-version", 71)

	first := fixture.artifacts[0]
	replacement := []byte("newer PRIMARY version must not replace the committed source")
	replacementDigest := sha256.Sum256(replacement)
	replaced, err := fixture.minio.admin.PutObject(ctx, &s3.PutObjectInput{
		Bucket:         aws.String(fixture.minio.bucket),
		Key:            aws.String(first.object.ObjectKey),
		Body:           bytes.NewReader(replacement),
		ContentLength:  aws.Int64(int64(len(replacement))),
		ContentType:    aws.String(first.contentType),
		ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(replacementDigest[:])),
	})
	if err != nil || aws.ToString(replaced.VersionId) == "" ||
		aws.ToString(replaced.VersionId) == first.object.VersionID {
		t.Fatalf("write newer PRIMARY version = %#v error=%v", replaced, err)
	}

	pool := newRolePool(
		t, fixture.start.database.DSN,
		"vela_artifact_replication_login", "vela-artifact-replication-password",
	)
	backup := &recordingArtifactBackupStore{delegate: fixture.backup}
	firstReplicator := newArtifactReplicator(t, pool, fixture.primary, backup, "replicator-a", 10)
	secondReplicator := newArtifactReplicator(t, pool, fixture.primary, backup, "replicator-b", 10)
	type outcome struct {
		result artifactreplication.Result
		err    error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for _, replicator := range []*artifactreplication.Replicator{firstReplicator, secondReplicator} {
		go func(replicator *artifactreplication.Replicator) {
			<-start
			result, callErr := replicator.ReplicateBatch(ctx)
			results <- outcome{result: result, err: callErr}
		}(replicator)
	}
	close(start)
	var total artifactreplication.Result
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent Artifact backup replication: %v; storage errors: %v", result.err, backup.Errors())
		}
		total.Claimed += result.result.Claimed
		total.Completed += result.result.Completed
		total.Failed += result.result.Failed
	}
	if total.Claimed != len(fixture.artifacts) || total.Completed != len(fixture.artifacts) ||
		total.Failed != 0 {
		t.Fatalf("concurrent Artifact backup result = %#v, want one completion per intent", total)
	}

	for _, artifact := range fixture.artifacts {
		assertCompletedArtifactReplication(t, fixture, artifact, 1)
	}
	replicated, err := firstReplicator.ReplicateBatch(ctx)
	if err != nil || replicated != (artifactreplication.Result{}) {
		t.Fatalf("terminal Artifact backup replay = %#v error=%v", replicated, err)
	}
}

func TestArtifactBackupReplicationRecoversAfterSuccessfulWriteResponseLoss(t *testing.T) {
	fixture := newCommittedReplicationFixture(t, "artifact-backup-response-loss", 73)
	pool := newRolePool(
		t, fixture.start.database.DSN,
		"vela_artifact_replication_login", "vela-artifact-replication-password",
	)
	backup := &successfulWriteResponseLossStore{delegate: fixture.backup}
	replicator := newArtifactReplicator(t, pool, fixture.primary, backup, "response-loss", 10)
	first, err := replicator.ReplicateBatch(context.Background())
	if !errors.Is(err, errSimulatedWriteResponseLoss) ||
		first.Claimed != len(fixture.artifacts) ||
		first.Completed != len(fixture.artifacts)-1 || first.Failed != 1 || backup.losses != 1 {
		t.Fatalf("response-loss first pass = %#v losses=%d error=%v", first, backup.losses, err)
	}
	if _, err := fixture.start.database.Admin.Exec(`
		UPDATE artifact_backup_replications
		SET next_retry_at = clock_timestamp() - interval '1 second'
		WHERE state = 'PENDING'
	`); err != nil {
		t.Fatalf("make response-loss retry due: %v", err)
	}
	second, err := replicator.ReplicateBatch(context.Background())
	if err != nil || second != (artifactreplication.Result{Claimed: 1, Completed: 1}) {
		t.Fatalf("response-loss recovery pass = %#v error=%v", second, err)
	}
	for _, artifact := range fixture.artifacts {
		expectedAttempts := 1
		if artifact.object.ObjectKey == backup.lostObjectKey {
			expectedAttempts = 2
		}
		assertCompletedArtifactReplication(t, fixture, artifact, expectedAttempts)
	}
}

func TestContentDeletionBeforeArtifactBackupClaimCancelsReplication(t *testing.T) {
	fixture := newCommittedReplicationFixture(t, "artifact-backup-deletion-first", 75)
	requestService, principal := contentDeletionAuthority(t, fixture.start.database)
	deletion, err := requestService.AcceptContentDeletion(
		context.Background(), principal, uuid.MustParse(testProjectID),
		fixture.start.candidate.JobID, "artifact-backup-deletion-first",
	)
	if err != nil || deletion.State != "PENDING" {
		t.Fatalf("accept deletion before Artifact backup claim = %#v error=%v", deletion, err)
	}
	assertReplicationStateCounts(t, fixture.start.database.Admin, 0, len(fixture.artifacts))

	pool := newRolePool(
		t, fixture.start.database.DSN,
		"vela_artifact_replication_login", "vela-artifact-replication-password",
	)
	replicator := newArtifactReplicator(t, pool, fixture.primary, fixture.backup, "deletion-first", 10)
	result, err := replicator.ReplicateBatch(context.Background())
	if err != nil || result != (artifactreplication.Result{}) {
		t.Fatalf("canceled Artifact backup replication = %#v error=%v", result, err)
	}
	for _, artifact := range fixture.artifacts {
		if _, headErr := fixture.backup.HeadCurrentVersion(
			context.Background(), artifact.object.ObjectKey,
		); headErr == nil {
			t.Fatalf("canceled Artifact %s unexpectedly exists in backup", artifact.id)
		}
	}
}

func TestContentDeletionWaitsForActiveArtifactBackupCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	fixture := newCommittedReplicationFixture(t, "artifact-backup-copy-delete-race", 77)
	pool := newRolePool(
		t, fixture.start.database.DSN,
		"vela_artifact_replication_login", "vela-artifact-replication-password",
	)
	blockingBackup := &blockingArtifactBackupStore{
		delegate: fixture.backup,
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	replicator := newArtifactReplicator(t, pool, fixture.primary, blockingBackup, "blocking-copy", 1)
	type replicationOutcome struct {
		result artifactreplication.Result
		err    error
	}
	replicationResults := make(chan replicationOutcome, 1)
	go func() {
		result, callErr := replicator.ReplicateBatch(ctx)
		replicationResults <- replicationOutcome{result: result, err: callErr}
	}()
	select {
	case <-blockingBackup.entered:
	case <-ctx.Done():
		t.Fatalf("Artifact backup copy did not reach blocked storage write: %v", ctx.Err())
	}

	requestService, principal := contentDeletionAuthority(t, fixture.start.database)
	type deletionOutcome struct {
		request retention.DeletionRequest
		err     error
	}
	deletionResults := make(chan deletionOutcome, 1)
	go func() {
		request, callErr := requestService.AcceptContentDeletion(
			ctx, principal, uuid.MustParse(testProjectID),
			fixture.start.candidate.JobID, "artifact-backup-copy-delete-race",
		)
		deletionResults <- deletionOutcome{request: request, err: callErr}
	}()
	waitForDatabaseRoleLock(t, ctx, fixture.start.database.Admin, "vela_retention_request_login")
	select {
	case result := <-deletionResults:
		t.Fatalf("Content Deletion passed active copy transaction: %#v error=%v", result.request, result.err)
	default:
	}
	var receipts int
	if err := fixture.start.database.Admin.QueryRow(`
		SELECT count(*)
		FROM content_deletion_receipts AS receipt
		JOIN content_deletion_requests AS request ON request.id = receipt.request_id
		WHERE request.job_id = $1
	`, fixture.start.candidate.JobID).Scan(&receipts); err != nil || receipts != 0 {
		t.Fatalf("deletion receipts during active copy = %d error=%v, want 0", receipts, err)
	}

	close(blockingBackup.release)
	var replicated replicationOutcome
	select {
	case replicated = <-replicationResults:
	case <-ctx.Done():
		t.Fatalf("blocked Artifact backup did not finish: %v", ctx.Err())
	}
	if replicated.err != nil || replicated.result != (artifactreplication.Result{Claimed: 1, Completed: 1}) {
		t.Fatalf("copy-first Artifact backup result = %#v error=%v", replicated.result, replicated.err)
	}
	var deleted deletionOutcome
	select {
	case deleted = <-deletionResults:
	case <-ctx.Done():
		t.Fatalf("Content Deletion did not resume after Artifact backup commit: %v", ctx.Err())
	}
	if deleted.err != nil || deleted.request.State != "PENDING" {
		t.Fatalf("resumed Content Deletion = %#v error=%v", deleted.request, deleted.err)
	}
	assertReplicationStateCounts(t, fixture.start.database.Admin, 1, len(fixture.artifacts)-1)

	retentionPool := newRolePool(
		t, fixture.start.database.DSN, "vela_retention_login", "vela-retention-password",
	)
	backupRetentionPool := newRolePool(
		t, fixture.start.database.DSN,
		"vela_backup_retention_login", "vela-backup-retention-password",
	)
	reconciler, err := retention.NewReconciler(
		retentionPool,
		fixture.primary,
		retention.ReconcilerConfig{
			InstanceID: "copy-first-retention", BatchSize: 100,
			ClaimTTL: time.Minute, RetryDelay: time.Minute,
			BackupPool: backupRetentionPool, BackupStore: fixture.backup,
		},
	)
	if err != nil {
		t.Fatalf("create copy-first Content Deletion reconciler: %v", err)
	}
	for range 4 {
		result, reconcileErr := reconciler.ReconcileBatch(ctx)
		if reconcileErr != nil {
			t.Fatalf("reconcile copy-first Content Deletion = %#v error=%v", result, reconcileErr)
		}
		if result.Claimed == 0 {
			break
		}
	}
	var requestState string
	if err := fixture.start.database.Admin.QueryRow(`
		SELECT state::text FROM content_deletion_requests WHERE id = $1
	`, deleted.request.RequestID).Scan(&requestState); err != nil || requestState != "COMPLETED" {
		t.Fatalf("copy-first Content Deletion state = %q error=%v", requestState, err)
	}
	for _, artifact := range fixture.artifacts {
		versions, listErr := fixture.minio.admin.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket: aws.String("vela-artifact-replication-backup"),
			Prefix: aws.String(artifact.object.ObjectKey),
		})
		if listErr != nil || len(versions.Versions) != 0 || len(versions.DeleteMarkers) != 0 {
			t.Fatalf(
				"backup versions after deletion for %s = versions %d markers %d error=%v",
				artifact.id, len(versions.Versions), len(versions.DeleteMarkers), listErr,
			)
		}
	}
}

func TestArtifactBackupReplicationMigrationFencesClaimsAndPreservesEvidence(t *testing.T) {
	fixture := newCommittedReplicationFixture(t, "artifact-backup-migration-fencing", 79)
	pool := newRolePool(
		t, fixture.start.database.DSN,
		"vela_artifact_replication_login", "vela-artifact-replication-password",
	)
	claims := make(map[uuid.UUID]artifactBackupDatabaseClaim, len(fixture.artifacts))
	claimIDs := make(map[uuid.UUID]uuid.UUID, len(fixture.artifacts))
	for range len(fixture.artifacts) {
		claimID := uuid.New()
		claim, err := claimArtifactBackupReplication(
			context.Background(), pool, "migration-claim-owner", claimID, 1,
		)
		if err != nil {
			t.Fatalf("claim Artifact backup migration fixture: %v", err)
		}
		claims[claim.id] = claim
		claimIDs[claim.id] = claimID
	}
	if _, err := claimArtifactBackupReplication(
		context.Background(), pool, "migration-claim-owner", uuid.New(), 1,
	); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("claim before lease expiry error = %v, want pgx.ErrNoRows", err)
	}

	time.Sleep(1100 * time.Millisecond)
	var expiredClaim artifactBackupDatabaseClaim
	for _, candidate := range claims {
		expiredClaim = candidate
		break
	}
	var expiredCompleted bool
	if err := pool.QueryRow(context.Background(), `
		SELECT vela_complete_artifact_backup_replication($1, $2, $3, $4, $5, $6)
	`, expiredClaim.id, claimIDs[expiredClaim.id], "expired-backup-version",
		expiredClaim.sizeBytes, expiredClaim.digest, expiredClaim.contentType,
	).Scan(&expiredCompleted); err != nil || expiredCompleted {
		t.Fatalf("expired Artifact backup completion = %t error=%v, want false", expiredCompleted, err)
	}
	var expiredRetried bool
	if err := pool.QueryRow(context.Background(), `
		SELECT vela_retry_artifact_backup_replication($1, $2, $3, $4)
	`, expiredClaim.id, claimIDs[expiredClaim.id], 60, "STORAGE_OPERATION_FAILED").Scan(
		&expiredRetried,
	); err != nil || expiredRetried {
		t.Fatalf("expired Artifact backup retry = %t error=%v, want false", expiredRetried, err)
	}
	replacementClaimID := uuid.New()
	reclaimed, err := claimArtifactBackupReplication(
		context.Background(), pool, "migration-recovery-owner", replacementClaimID, 60,
	)
	if err != nil {
		t.Fatalf("recover expired Artifact backup claim: %v", err)
	}
	oldClaimID := claimIDs[reclaimed.id]
	var staleCompleted bool
	if err := pool.QueryRow(context.Background(), `
		SELECT vela_complete_artifact_backup_replication($1, $2, $3, $4, $5, $6)
	`, reclaimed.id, oldClaimID, "stale-backup-version", reclaimed.sizeBytes,
		reclaimed.digest, reclaimed.contentType).Scan(&staleCompleted); err != nil || staleCompleted {
		t.Fatalf("stale Artifact backup completion = %t error=%v, want false", staleCompleted, err)
	}
	var staleRetried bool
	if err := pool.QueryRow(context.Background(), `
		SELECT vela_retry_artifact_backup_replication($1, $2, $3, $4)
	`, reclaimed.id, oldClaimID, 60, "STORAGE_OPERATION_FAILED").Scan(&staleRetried); err != nil || staleRetried {
		t.Fatalf("stale Artifact backup retry = %t error=%v, want false", staleRetried, err)
	}
	var completed bool
	if err := pool.QueryRow(context.Background(), `
		SELECT vela_complete_artifact_backup_replication($1, $2, $3, $4, $5, $6)
	`, reclaimed.id, replacementClaimID, "durable-backup-version", reclaimed.sizeBytes,
		reclaimed.digest, reclaimed.contentType).Scan(&completed); err != nil || !completed {
		t.Fatalf("current Artifact backup completion = %t error=%v, want true", completed, err)
	}
	deletionService, principal := contentDeletionAuthority(t, fixture.start.database)
	deletion, err := deletionService.AcceptContentDeletion(
		context.Background(), principal, uuid.MustParse(testProjectID),
		fixture.start.candidate.JobID, "artifact-backup-terminal-immutability",
	)
	if err != nil || deletion.State != "PENDING" {
		t.Fatalf("cancel remaining Artifact backup intents = %#v error=%v", deletion, err)
	}
	var canceledID uuid.UUID
	if err := fixture.start.database.Admin.QueryRow(`
		SELECT id FROM artifact_backup_replications WHERE state = 'CANCELED' LIMIT 1
	`).Scan(&canceledID); err != nil {
		t.Fatalf("read canceled Artifact backup evidence: %v", err)
	}
	for terminalState, replicationID := range map[string]uuid.UUID{
		"COMPLETED": reclaimed.id,
		"CANCELED":  canceledID,
	} {
		for operation, statement := range map[string]string{
			"update": `UPDATE artifact_backup_replications
				SET attempt_count = attempt_count WHERE id = $1`,
			"delete": `DELETE FROM artifact_backup_replications WHERE id = $1`,
		} {
			_, err := fixture.start.database.Admin.Exec(statement, replicationID)
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "P0001" {
				t.Fatalf(
					"%s Artifact backup evidence %s error = %v, want P0001",
					terminalState, operation, err,
				)
			}
		}
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err = goose.DownTo(fixture.start.database.Admin, migrations, 28)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
		postgresError.ConstraintName != "artifact_backup_replication_requires_empty_evidence" {
		t.Fatalf("Artifact backup migration Down error = %v, want named SQLSTATE 55000", err)
	}
	version, versionErr := goose.GetDBVersion(fixture.start.database.Admin)
	if versionErr != nil || version != 29 {
		t.Fatalf("Artifact backup migration version after refused Down = %d error=%v", version, versionErr)
	}
}

func TestArtifactBackupReplicationMigrationBackfillsSchema28Artifacts(t *testing.T) {
	fixture := newCommittedReplicationFixture(t, "artifact-backup-schema-28-backfill", 81)
	tx, err := fixture.start.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin schema-28 Artifact backup fixture cleanup: %v", err)
	}
	if _, err := tx.Exec("SET LOCAL session_replication_role = replica"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("disable replication evidence guard for migration fixture: %v", err)
	}
	if _, err := tx.Exec("DELETE FROM artifact_backup_replications"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("remove migration-29 Artifact backup rows: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit migration-29 Artifact backup cleanup: %v", err)
	}
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(fixture.start.database.Admin, migrations, 28); err != nil {
		t.Fatalf("contract empty Artifact backup migration: %v", err)
	}

	requestService, principal := contentDeletionAuthority(t, fixture.start.database)
	deletion, err := requestService.AcceptContentDeletion(
		context.Background(), principal, uuid.MustParse(testProjectID),
		fixture.start.candidate.JobID, "artifact-backup-schema-28-deletion",
	)
	if err != nil || deletion.State != "PENDING" {
		t.Fatalf("create schema-28 Content Deletion = %#v error=%v", deletion, err)
	}
	lateArtifactID := uuid.New()
	if _, err := fixture.start.database.Admin.Exec(`
		INSERT INTO artifacts (
			id, organization_id, project_id, job_id, attempt_id, attempt_fence,
			kind, ordinal, object_key, expected_content_type,
			object_version_id, size_bytes, sha256, content_type, uploaded_at,
			verification_id, verification_request_hash, validation_receipt,
			verified_at, retention_expires_at, state, expires_at, created_at, updated_at
		)
		SELECT
			$2, organization_id, project_id, job_id, attempt_id, attempt_fence,
			kind, ordinal + 100, object_key || '.schema-28-late', expected_content_type,
			object_version_id, size_bytes, sha256, content_type, uploaded_at,
			verification_id, verification_request_hash, validation_receipt,
			verified_at, retention_expires_at, state, expires_at,
			clock_timestamp(), clock_timestamp()
		FROM artifacts
		WHERE id = $1
	`, fixture.artifacts[0].id, lateArtifactID); err != nil {
		t.Fatalf("create schema-28 committed Artifact without deletion target: %v", err)
	}
	if err := goose.UpTo(fixture.start.database.Admin, migrations, 29); err != nil {
		t.Fatalf("expand Artifact backup migration over schema-28 data: %v", err)
	}
	var pending, canceled int
	if err := fixture.start.database.Admin.QueryRow(`
		SELECT count(*) FILTER (WHERE state = 'PENDING'),
		       count(*) FILTER (WHERE state = 'CANCELED')
		FROM artifact_backup_replications
		WHERE job_id = $1
	`, fixture.start.candidate.JobID).Scan(&pending, &canceled); err != nil {
		t.Fatalf("read schema-28 Artifact backup backfill: %v", err)
	}
	if pending != 1 || canceled != len(fixture.artifacts) {
		t.Fatalf(
			"schema-28 Artifact backup backfill = pending %d canceled %d, want 1/%d",
			pending, canceled, len(fixture.artifacts),
		)
	}
	var lateSourceVersion string
	if err := fixture.start.database.Admin.QueryRow(`
		SELECT source_object_version_id
		FROM artifact_backup_replications
		WHERE artifact_id = $1 AND state = 'PENDING'
	`, lateArtifactID).Scan(&lateSourceVersion); err != nil ||
		lateSourceVersion != fixture.artifacts[0].object.VersionID {
		t.Fatalf("schema-28 pending source version = %q error=%v", lateSourceVersion, err)
	}
}

func newCommittedReplicationFixture(
	t *testing.T,
	key string,
	epoch int64,
) committedReplicationFixture {
	t.Helper()
	ctx := context.Background()
	minio := newMinIOFixture(t, "vela-artifact-replication-primary")
	minio.enableVersioning(t)
	if err := minio.store.ValidateBucket(ctx); err != nil {
		t.Fatalf("validate PRIMARY Artifact bucket: %v", err)
	}
	backup := newAdditionalMinIOStore(t, minio, "vela-artifact-replication-backup")
	fixture := newStartFixture(t, key, epoch)
	if started, err := fixture.service.Start(
		ctx, fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start Artifact backup fixture = %#v error=%v", started, err)
	}
	completionService := visibleCompletionService(t, fixture.database.DSN)
	plan, err := completionService.BeginFinalization(ctx, fixture.worker, fixture.credentials)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("begin Artifact backup finalization = %#v error=%v", plan, err)
	}
	var beforeCommit int
	if err := fixture.database.Admin.QueryRow(
		"SELECT count(*) FROM artifact_backup_replications",
	).Scan(&beforeCommit); err != nil || beforeCommit != 0 {
		t.Fatalf("replication intents before commit = %d error=%v, want 0", beforeCommit, err)
	}

	artifacts := make([]committedReplicationArtifact, 0, len(plan.Artifacts))
	artifactIDs := make([]uuid.UUID, 0, len(plan.Artifacts))
	for index, planned := range plan.Artifacts {
		claimID := uuid.New()
		claim, claimErr := completionService.ClaimArtifactUpload(
			ctx, fixture.worker, fixture.credentials, planned.UploadID, claimID,
		)
		if claimErr != nil || claim.Decision != workercontrol.ArtifactUploadClaimGranted {
			t.Fatalf("claim Artifact %d = %#v error=%v", index, claim, claimErr)
		}
		session, sessionErr := minio.store.CreateMultipartUpload(
			ctx, planned.ObjectKey, claim.ExpectedContentType,
		)
		if sessionErr != nil {
			t.Fatalf("create PRIMARY multipart upload %d: %v", index, sessionErr)
		}
		recorded, recordErr := completionService.RecordArtifactMultipartSession(
			ctx, fixture.worker, fixture.credentials, planned.UploadID, claimID, session.UploadID,
		)
		if recordErr != nil || recorded.Decision != workercontrol.ArtifactMultipartSessionRecorded {
			t.Fatalf("record PRIMARY multipart upload %d = %#v error=%v", index, recorded, recordErr)
		}
		payload := []byte(fmt.Sprintf("committed Artifact backup payload %d for %s", index, key))
		digest := sha256.Sum256(payload)
		part, partErr := minio.store.UploadPart(
			ctx, session, 1, bytes.NewReader(payload), int64(len(payload)), digest,
		)
		if partErr != nil {
			t.Fatalf("upload PRIMARY Artifact part %d: %v", index, partErr)
		}
		report := workercontrol.ArtifactUploadReport{
			SizeBytes: int64(len(payload)), SHA256: digest,
			ContentType: claim.ExpectedContentType,
			CompletedParts: []workercontrol.ArtifactUploadPart{{
				Number: part.Number, ETag: part.ETag,
				SizeBytes: part.SizeBytes, ChecksumSHA256: part.ChecksumSHA256,
			}},
		}
		intent, intentErr := completionService.RecordArtifactCompletionIntent(
			ctx, fixture.worker, fixture.credentials, planned.UploadID, report,
		)
		if intentErr != nil || intent.Decision != workercontrol.ArtifactCompletionIntentRecorded {
			t.Fatalf("record Artifact completion intent %d = %#v error=%v", index, intent, intentErr)
		}
		object, completeErr := minio.store.CompleteMultipartUpload(
			ctx, session, []artifactstore.CompletedPart{part},
		)
		if completeErr != nil || object.VersionID == "" {
			t.Fatalf("complete PRIMARY Artifact %d = %#v error=%v", index, object, completeErr)
		}
		report.ObjectVersionID = object.VersionID
		uploaded, uploadErr := completionService.RecordArtifactUploaded(
			ctx, fixture.worker, fixture.credentials, planned.UploadID, report,
		)
		if uploadErr != nil || uploaded.Decision != workercontrol.ArtifactUploadRecorded {
			t.Fatalf("record PRIMARY Artifact %d = %#v error=%v", index, uploaded, uploadErr)
		}
		verified, verifyErr := completionService.VerifyArtifact(
			ctx, fixture.worker, fixture.credentials, planned.UploadID, uuid.New(),
		)
		if verifyErr != nil || verified.Decision != workercontrol.ArtifactVerified {
			t.Fatalf("verify PRIMARY Artifact %d = %#v error=%v", index, verified, verifyErr)
		}
		artifacts = append(artifacts, committedReplicationArtifact{
			id: planned.ArtifactID, object: object, payload: payload,
			digest: digest, contentType: claim.ExpectedContentType,
		})
		artifactIDs = append(artifactIDs, planned.ArtifactID)
	}
	if err := fixture.database.Admin.QueryRow(
		"SELECT count(*) FROM artifact_backup_replications",
	).Scan(&beforeCommit); err != nil || beforeCommit != 0 {
		t.Fatalf("replication intents for VERIFIED artifacts = %d error=%v, want 0", beforeCommit, err)
	}
	completed, err := completionService.CompleteVisibleCompletion(
		ctx, fixture.worker, fixture.credentials,
		workercontrol.VisibleCompletionCandidate{
			CompletionID: uuid.New(), ExpectedJobVersion: plan.JobVersion,
			ArtifactIDs: artifactIDs,
		},
	)
	if err != nil || completed.Decision != workercontrol.VisibleCompletionCommitted {
		t.Fatalf("commit visible Artifact set = %#v error=%v", completed, err)
	}
	var intentCount int
	if err := fixture.database.Admin.QueryRow(
		"SELECT count(*) FROM artifact_backup_replications WHERE job_id = $1",
		fixture.candidate.JobID,
	).Scan(&intentCount); err != nil || intentCount != len(artifacts) {
		t.Fatalf("committed Artifact replication intents = %d error=%v", intentCount, err)
	}
	return committedReplicationFixture{
		start: fixture, minio: minio, primary: minio.store, backup: backup, artifacts: artifacts,
	}
}

func claimArtifactBackupReplication(
	ctx context.Context,
	pool *pgxpool.Pool,
	owner string,
	claimID uuid.UUID,
	claimSeconds int,
) (artifactBackupDatabaseClaim, error) {
	var claim artifactBackupDatabaseClaim
	err := pool.QueryRow(ctx, `
		SELECT replication_id, source_object_key, source_object_version_id,
		       source_size_bytes, source_sha256, source_content_type
		FROM vela_claim_artifact_backup_replication($1, $2, $3)
	`, owner, claimID, claimSeconds).Scan(
		&claim.id, &claim.objectKey, &claim.versionID,
		&claim.sizeBytes, &claim.digest, &claim.contentType,
	)
	return claim, err
}

func newArtifactReplicator(
	t *testing.T,
	pool *pgxpool.Pool,
	source artifactreplication.SourceStore,
	backup artifactreplication.BackupStore,
	instanceID string,
	batchSize int,
) *artifactreplication.Replicator {
	t.Helper()
	replicator, err := artifactreplication.New(
		pool, source, backup,
		artifactreplication.Config{
			InstanceID: instanceID, BatchSize: batchSize,
			ClaimTTL: time.Minute, RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create Artifact backup Replicator: %v", err)
	}
	return replicator
}

func assertCompletedArtifactReplication(
	t *testing.T,
	fixture committedReplicationFixture,
	artifact committedReplicationArtifact,
	expectedAttempts int,
) {
	t.Helper()
	var (
		state, sourceVersion, backupVersion, contentType string
		sizeBytes                                        int64
		digest                                           []byte
		attempts                                         int
	)
	if err := fixture.start.database.Admin.QueryRow(`
		SELECT state::text, source_object_version_id, backup_object_version_id,
		       backup_size_bytes, backup_sha256, backup_content_type, attempt_count
		FROM artifact_backup_replications
		WHERE artifact_id = $1
	`, artifact.id).Scan(
		&state, &sourceVersion, &backupVersion, &sizeBytes, &digest, &contentType, &attempts,
	); err != nil {
		t.Fatalf("read completed Artifact replication %s: %v", artifact.id, err)
	}
	if state != "COMPLETED" || sourceVersion != artifact.object.VersionID || backupVersion == "" ||
		sizeBytes != int64(len(artifact.payload)) || !bytes.Equal(digest, artifact.digest[:]) ||
		contentType != artifact.contentType || attempts != expectedAttempts {
		t.Fatalf(
			"completed Artifact replication %s = state %s source %s backup %s size %d type %s attempts %d",
			artifact.id, state, sourceVersion, backupVersion, sizeBytes, contentType, attempts,
		)
	}
	reader, err := fixture.backup.ReadExactVersion(
		context.Background(), artifact.object.ObjectKey, backupVersion,
	)
	if err != nil {
		t.Fatalf("read completed backup Artifact %s: %v", artifact.id, err)
	}
	body, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(body, artifact.payload) ||
		reader.SizeBytes != int64(len(artifact.payload)) ||
		reader.ContentType != artifact.contentType ||
		reader.ChecksumSHA256 != base64.StdEncoding.EncodeToString(artifact.digest[:]) {
		t.Fatalf(
			"backup Artifact %s metadata/body mismatch read=%v close=%v body=%q object=%#v",
			artifact.id, readErr, closeErr, body, reader.ObjectVersion,
		)
	}
}

func assertReplicationStateCounts(t *testing.T, admin *sql.DB, completed, canceled int) {
	t.Helper()
	var gotCompleted, gotCanceled int
	if err := admin.QueryRow(`
		SELECT count(*) FILTER (WHERE state = 'COMPLETED'),
		       count(*) FILTER (WHERE state = 'CANCELED')
		FROM artifact_backup_replications
	`).Scan(&gotCompleted, &gotCanceled); err != nil {
		t.Fatalf("read Artifact replication state counts: %v", err)
	}
	if gotCompleted != completed || gotCanceled != canceled {
		t.Fatalf(
			"Artifact replication state counts = completed %d canceled %d, want %d/%d",
			gotCompleted, gotCanceled, completed, canceled,
		)
	}
}

type successfulWriteResponseLossStore struct {
	delegate      artifactreplication.BackupStore
	mu            sync.Mutex
	losses        int
	lostObjectKey string
}

var errSimulatedWriteResponseLoss = errors.New("simulated successful write response loss")

type recordingArtifactBackupStore struct {
	delegate artifactreplication.BackupStore
	mu       sync.Mutex
	errors   []error
}

func (store *recordingArtifactBackupStore) PutIfAbsent(
	ctx context.Context,
	objectKey, contentType string,
	body io.Reader,
	sizeBytes int64,
	digest [sha256.Size]byte,
) (artifactstore.ObjectVersion, error) {
	object, err := store.delegate.PutIfAbsent(
		ctx, objectKey, contentType, body, sizeBytes, digest,
	)
	if err != nil {
		store.mu.Lock()
		store.errors = append(store.errors, err)
		store.mu.Unlock()
	}
	return object, err
}

func (store *recordingArtifactBackupStore) HeadCurrentVersion(
	ctx context.Context,
	objectKey string,
) (artifactstore.ObjectVersion, error) {
	object, err := store.delegate.HeadCurrentVersion(ctx, objectKey)
	if err != nil {
		store.mu.Lock()
		store.errors = append(store.errors, err)
		store.mu.Unlock()
	}
	return object, err
}

func (store *recordingArtifactBackupStore) Errors() []error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]error(nil), store.errors...)
}

func (store *successfulWriteResponseLossStore) PutIfAbsent(
	ctx context.Context,
	objectKey, contentType string,
	body io.Reader,
	sizeBytes int64,
	digest [sha256.Size]byte,
) (artifactstore.ObjectVersion, error) {
	object, err := store.delegate.PutIfAbsent(
		ctx, objectKey, contentType, body, sizeBytes, digest,
	)
	if err != nil {
		return object, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.losses == 0 {
		store.losses++
		store.lostObjectKey = objectKey
		return artifactstore.ObjectVersion{}, errSimulatedWriteResponseLoss
	}
	return object, nil
}

func (store *successfulWriteResponseLossStore) HeadCurrentVersion(
	ctx context.Context,
	objectKey string,
) (artifactstore.ObjectVersion, error) {
	return store.delegate.HeadCurrentVersion(ctx, objectKey)
}

type blockingArtifactBackupStore struct {
	delegate artifactreplication.BackupStore
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (store *blockingArtifactBackupStore) PutIfAbsent(
	ctx context.Context,
	objectKey, contentType string,
	body io.Reader,
	sizeBytes int64,
	digest [sha256.Size]byte,
) (artifactstore.ObjectVersion, error) {
	blocked := false
	store.once.Do(func() {
		blocked = true
		close(store.entered)
	})
	if blocked {
		select {
		case <-store.release:
		case <-ctx.Done():
			return artifactstore.ObjectVersion{}, ctx.Err()
		}
	}
	return store.delegate.PutIfAbsent(ctx, objectKey, contentType, body, sizeBytes, digest)
}

func (store *blockingArtifactBackupStore) HeadCurrentVersion(
	ctx context.Context,
	objectKey string,
) (artifactstore.ObjectVersion, error) {
	return store.delegate.HeadCurrentVersion(ctx, objectKey)
}

func waitForDatabaseRoleLock(t *testing.T, ctx context.Context, admin *sql.DB, role string) {
	t.Helper()
	for {
		var waiting int
		err := admin.QueryRow(`
			SELECT count(*) FROM pg_stat_activity
			WHERE usename = $1 AND wait_event_type = 'Lock'
		`, role).Scan(&waiting)
		if err != nil {
			t.Fatalf("inspect %s database lock wait: %v", role, err)
		}
		if waiting > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s did not wait on Artifact replication lock: %v", role, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

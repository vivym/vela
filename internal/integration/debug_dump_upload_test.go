//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivym/vela/internal/workercontrol"
)

func TestCurrentWorkerPersistsAuthorizedDebugDumpOutsideArtifacts(t *testing.T) {
	fixture := newAssignmentFixture(t, "authorized-debug-dump-upload", 7)
	authorizationID := seedActiveDebugDumpAuthorization(
		t, fixture.database, fixture.candidate.JobID,
	)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("Acquire authorized Assignment: %v", err)
	}
	if assignment.DebugDumpAuthorization == nil ||
		assignment.DebugDumpAuthorization.AuthorizationID != authorizationID {
		t.Fatalf("Assignment debug dump authorization = %#v", assignment.DebugDumpAuthorization)
	}
	credentials := leaseCredentials(assignment)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start authorized Assignment = %#v error=%v", started, err)
	}

	content := []byte(`{"attempt_id":"bounded","schema_version":1}`)
	digest := sha256.Sum256(content)
	dumpID := uuid.New()
	claimID := uuid.New()
	claim, err := fixture.service.ClaimDebugDumpUpload(
		context.Background(), fixture.worker, credentials,
		workercontrol.DebugDumpUploadIntent{
			DebugDumpID: dumpID, AuthorizationID: authorizationID,
			SizeBytes: int64(len(content)), SHA256: digest,
			ContentType: "application/vnd.vela.debug-dump+json",
		},
		claimID,
	)
	if err != nil || claim.Decision != workercontrol.DebugDumpUploadClaimGranted {
		t.Fatalf("ClaimDebugDumpUpload = %#v error=%v", claim, err)
	}
	wantPrefix := "debug-dumps/00000000-0000-0000-0000-000000000001/" +
		testProjectID + "/" + assignment.JobID.String() + "/" + authorizationID.String() +
		"/" + assignment.AttemptID.String() + "/"
	if !strings.HasPrefix(claim.ObjectKey, wantPrefix) ||
		!strings.HasSuffix(claim.ObjectKey, dumpID.String()) {
		t.Fatalf("debug dump object key = %q, want prefix %q", claim.ObjectKey, wantPrefix)
	}

	session, err := fixture.service.RecordDebugDumpMultipartSession(
		context.Background(), fixture.worker, credentials, dumpID, claimID, "debug-upload-1",
	)
	if err != nil || session.Decision != workercontrol.DebugDumpMultipartSessionRecorded {
		t.Fatalf("RecordDebugDumpMultipartSession = %#v error=%v", session, err)
	}
	report := workercontrol.DebugDumpUploadReport{
		ObjectVersionID: "version-1", SizeBytes: int64(len(content)), SHA256: digest,
		ContentType: "application/vnd.vela.debug-dump+json",
		CompletedParts: []workercontrol.DebugDumpUploadPart{{
			Number: 1, ETag: "etag-1", SizeBytes: int64(len(content)),
			ChecksumSHA256: base64.StdEncoding.EncodeToString(digest[:]),
		}},
	}
	intent, err := fixture.service.RecordDebugDumpCompletionIntent(
		context.Background(), fixture.worker, credentials, dumpID,
		workercontrol.DebugDumpUploadReport{
			SizeBytes: report.SizeBytes, SHA256: report.SHA256,
			ContentType: report.ContentType, CompletedParts: report.CompletedParts,
		},
	)
	if err != nil || intent.Decision != workercontrol.DebugDumpCompletionIntentRecorded {
		t.Fatalf("RecordDebugDumpCompletionIntent = %#v error=%v", intent, err)
	}
	internalPool := newRolePool(
		t, fixture.database.DSN, "vela_internal_login", "vela-internal-password",
	)
	if _, err := internalPool.Exec(context.Background(), `
		UPDATE debug_dumps
		SET state = 'AVAILABLE', object_version_id = $2, size_bytes = $3,
			sha256 = $4, content_type = $5, uploaded_at = clock_timestamp()
		WHERE id = $1
	`, dumpID, report.ObjectVersionID, report.SizeBytes,
		report.SHA256[:], report.ContentType); postgresCode(err) != "42501" {
		t.Fatalf("internal forged first debug dump receipt error = %v, want SQLSTATE 42501", err)
	}
	completed, err := fixture.service.RecordDebugDumpUploaded(
		context.Background(), fixture.worker, credentials, dumpID, report,
	)
	if err != nil || completed.Decision != workercontrol.DebugDumpUploadRecorded ||
		completed.ObjectVersionID != report.ObjectVersionID {
		t.Fatalf("RecordDebugDumpUploaded = %#v error=%v", completed, err)
	}

	var state, objectKey, versionID string
	var artifactCount, eventCount int
	if err := fixture.database.Admin.QueryRow(`
		SELECT dump.state::text, dump.object_key, dump.object_version_id,
			(SELECT count(*) FROM artifacts WHERE attempt_id = dump.attempt_id),
			(SELECT count(*) FROM debug_dump_events
			 WHERE debug_dump_id = dump.id AND action = 'UPLOADED')
		FROM debug_dumps AS dump
		WHERE dump.id = $1
	`, dumpID).Scan(&state, &objectKey, &versionID, &artifactCount, &eventCount); err != nil {
		t.Fatalf("read persisted debug dump: %v", err)
	}
	if state != "AVAILABLE" || objectKey != claim.ObjectKey || versionID != "version-1" ||
		artifactCount != 0 || eventCount != 1 {
		t.Fatalf(
			"persisted debug dump state/key/version/artifacts/events = %s/%s/%s/%d/%d",
			state, objectKey, versionID, artifactCount, eventCount,
		)
	}

	if _, err := internalPool.Exec(context.Background(), `
		UPDATE debug_dumps SET object_version_id = 'forged-version' WHERE id = $1
	`, dumpID); postgresCode(err) != "42501" {
		t.Fatalf("internal debug dump receipt mutation error = %v, want SQLSTATE 42501", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE debug_dumps SET object_version_id = 'forged-version' WHERE id = $1
	`, dumpID); postgresCode(err) != "55000" {
		t.Fatalf("owner debug dump receipt mutation error = %v, want SQLSTATE 55000", err)
	}
	var canSetDeletedAt, canSetState, canSetObjectVersion, canRecordUploaded bool
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			has_column_privilege('vela_internal', 'debug_dumps', 'deleted_at', 'UPDATE'),
			has_column_privilege('vela_internal', 'debug_dumps', 'state', 'UPDATE'),
			has_column_privilege('vela_internal', 'debug_dumps', 'object_version_id', 'UPDATE'),
			has_function_privilege(
				'vela_internal',
				'vela_record_debug_dump_uploaded(uuid,uuid,bigint,text,bigint,bytea,text,jsonb,bytea,timestamptz)',
				'EXECUTE'
			)
	`).Scan(&canSetDeletedAt, &canSetState, &canSetObjectVersion, &canRecordUploaded); err != nil {
		t.Fatalf("inspect internal debug dump deletion privilege: %v", err)
	}
	if canSetDeletedAt || canSetState || canSetObjectVersion || !canRecordUploaded {
		t.Fatalf(
			"internal debug dump deleted/state/version/function privileges = %t/%t/%t/%t",
			canSetDeletedAt, canSetState, canSetObjectVersion, canRecordUploaded,
		)
	}
	if _, err := internalPool.Exec(context.Background(), `
		INSERT INTO debug_dump_events (
			id, organization_id, project_id, job_id, authorization_id, debug_dump_id,
			action, outcome_code, actor_kind, worker_id, worker_epoch, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, 'DELETED', 'DELETED', 'WORKER', $7, $8,
			clock_timestamp()
		)
	`, uuid.New(), uuid.MustParse(testOrganizationID), uuid.MustParse(testProjectID),
		assignment.JobID, authorizationID, dumpID, assignment.WorkerID,
		assignment.WorkerEpoch); postgresCode(err) != "23514" {
		t.Fatalf("internal forged deletion event error = %v, want SQLSTATE 23514", err)
	}
}

func postgresCode(err error) string {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		return postgresError.Code
	}
	return ""
}

func TestDebugDumpUploadRejectsRevokedAuthorizationAndStaleLease(t *testing.T) {
	fixture := newAssignmentFixture(t, "rejected-debug-dump-upload", 7)
	authorizationID := seedActiveDebugDumpAuthorization(
		t, fixture.database, fixture.candidate.JobID,
	)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("Acquire authorized Assignment: %v", err)
	}
	credentials := leaseCredentials(assignment)
	if _, err := fixture.service.Start(context.Background(), fixture.worker, credentials); err != nil {
		t.Fatalf("Start authorized Assignment: %v", err)
	}
	intent := workercontrol.DebugDumpUploadIntent{
		DebugDumpID: uuid.New(), AuthorizationID: authorizationID, SizeBytes: 2,
		SHA256:      sha256.Sum256([]byte(`{}`)),
		ContentType: "application/vnd.vela.debug-dump+json",
	}

	stale := credentials
	stale.Fence++
	claim, err := fixture.service.ClaimDebugDumpUpload(
		context.Background(), fixture.worker, stale, intent, uuid.New(),
	)
	if err != nil || claim.Decision != workercontrol.DebugDumpUploadClaimRejected {
		t.Fatalf("stale DebugDump claim = %#v error=%v", claim, err)
	}
	revocationHash := sha256.Sum256([]byte("fixture-revoke"))
	if _, err := fixture.database.Admin.Exec(`
		UPDATE debug_dump_authorizations
		SET revoked_at = clock_timestamp(),
			revoked_by_principal_id = actor_principal_id,
			revoked_by_session_id = actor_session_id,
			revocation_idempotency_key = 'fixture-revoke',
			revocation_request_hash = $2
		WHERE id = $1
	`, authorizationID, revocationHash[:]); err != nil {
		t.Fatalf("revoke debug dump authorization: %v", err)
	}
	claim, err = fixture.service.ClaimDebugDumpUpload(
		context.Background(), fixture.worker, credentials, intent, uuid.New(),
	)
	if err != nil || claim.Decision != workercontrol.DebugDumpUploadClaimRejected {
		t.Fatalf("revoked DebugDump claim = %#v error=%v", claim, err)
	}
}

func seedActiveDebugDumpAuthorization(
	t *testing.T,
	database testDatabase,
	jobID uuid.UUID,
) uuid.UUID {
	t.Helper()
	authorizationID := uuid.New()
	sessionID := uuid.New()
	principalID := uuid.New()
	seedHumanRoleFixture(
		t, database.Admin, principalID, "debug-dump-upload-fixture-"+authorizationID.String(),
		nil, map[string][]string{testProjectID: {"ProjectAdmin"}},
	)
	var organizationID, projectID uuid.UUID
	var retentionHours int
	if err := database.Admin.QueryRow(`
		SELECT organization_id, project_id, retention_debug_hours
		FROM jobs
		WHERE id = $1
	`, jobID).Scan(&organizationID, &projectID, &retentionHours); err != nil {
		t.Fatalf("read debug dump fixture Job: %v", err)
	}
	if retentionHours != 72 {
		t.Fatalf("debug dump retention hours = %d, want 72", retentionHours)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	requestHash := sha256.Sum256([]byte("debug dump fixture authorization"))
	if _, err := database.Admin.Exec(`
		INSERT INTO project_actor_session_attributions (
			organization_id, project_id, actor_session_id, principal_id,
			actor_kind, first_attributed_at
		) VALUES ($1, $2, $3, $4, 'HUMAN', $5)
	`, organizationID, projectID, sessionID, principalID, now); err != nil {
		t.Fatalf("seed debug dump actor attribution: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO debug_dump_authorizations (
			id, organization_id, project_id, job_id, purpose, idempotency_key,
			request_hash, actor_principal_id, actor_session_id, authorized_at, expires_at
		) VALUES ($1, $2, $3, $4, 'CUSTOMER_SUPPORT', $5, $6, $7, $8,
			$9::timestamptz, $9::timestamptz + interval '72 hours')
	`, authorizationID, organizationID, projectID, jobID,
		"fixture-"+authorizationID.String(), requestHash[:], principalID, sessionID, now); err != nil {
		t.Fatalf("seed active debug dump authorization: %v", err)
	}
	return authorizationID
}

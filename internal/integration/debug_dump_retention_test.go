//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/workercontrol"
)

func TestExpiredDebugDumpUsesExistingDeletionRetryAndReceiptLifecycle(t *testing.T) {
	fixture := newAssignmentFixture(t, "debug-dump-retention", 7)
	authorizationID := seedActiveDebugDumpAuthorization(
		t, fixture.database, fixture.candidate.JobID,
	)
	dumpID, objectKey, versionID, _, _ := persistAvailableDebugDump(
		t, fixture, authorizationID,
	)
	expireDebugDumpAuthorization(t, fixture, authorizationID, dumpID)

	prefix := "debug-dumps/" + testOrganizationID + "/" + testProjectID + "/" +
		fixture.candidate.JobID.String() + "/" + authorizationID.String() + "/"
	store := &recordingRetentionStore{
		listErr: errors.New("injected multipart listing failure"),
		multipart: []artifactstore.IncompleteMultipartUpload{{
			ObjectKey: prefix + "orphan/read-upload", UploadID: "debug-retention-upload",
			InitiatedAt: time.Now().UTC().Add(-time.Hour),
		}},
	}
	reconciler, err := retention.NewReconciler(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_retention_login",
			"vela-retention-password",
		),
		store,
		retention.ReconcilerConfig{
			InstanceID: "debug-dump-retention-reconciler",
			BatchSize:  10,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create debug dump retention Reconciler: %v", err)
	}

	first, firstErr := reconciler.ReconcileBatch(context.Background())
	if firstErr == nil || first.ContentDeletionRequestsCreated != 1 || first.Claimed != 2 ||
		first.Completed != 1 || first.Failed != 1 {
		t.Fatalf("first debug dump retention result = %#v error=%v", first, firstErr)
	}
	if len(store.deleted) != 1 || store.deleted[0].ObjectKey != objectKey ||
		store.deleted[0].VersionID != versionID {
		t.Fatalf("debug dump exact deletion = %#v", store.deleted)
	}
	var retryingTargets, availableDumps int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM content_deletion_targets
			 WHERE debug_dump_authorization_id = $1 AND state = 'RETRY_WAIT'),
			(SELECT count(*) FROM debug_dumps WHERE id = $2 AND state = 'DELETED')
	`, authorizationID, dumpID).Scan(&retryingTargets, &availableDumps); err != nil {
		t.Fatalf("read retrying debug dump deletion: %v", err)
	}
	if retryingTargets != 1 || availableDumps != 1 {
		t.Fatalf(
			"retrying targets/deleted dumps after exact delete = %d/%d, want 1/1",
			retryingTargets,
			availableDumps,
		)
	}

	store.listErr = nil
	if _, err := fixture.database.Admin.Exec(`
		UPDATE content_deletion_targets
		SET next_retry_at = clock_timestamp()
		WHERE debug_dump_authorization_id = $1 AND state = 'RETRY_WAIT'
	`, authorizationID); err != nil {
		t.Fatalf("make debug dump target retry eligible: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE content_deletion_requests
		SET next_retry_at = clock_timestamp()
		WHERE debug_dump_authorization_id = $1 AND state = 'RETRY_WAIT'
	`, authorizationID); err != nil {
		t.Fatalf("make debug dump request retry eligible: %v", err)
	}
	second, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || second.ContentDeletionRequestsCreated != 0 || second.Claimed != 1 ||
		second.Completed != 1 || second.Failed != 0 {
		t.Fatalf("second debug dump retention result = %#v error=%v", second, err)
	}
	if len(store.aborted) != 1 || store.aborted[0].ObjectKey != prefix+"orphan/read-upload" ||
		store.aborted[0].UploadID != "debug-retention-upload" {
		t.Fatalf("debug dump multipart aborts = %#v", store.aborted)
	}

	var (
		requestState, dumpState                                 string
		requestCount, targetCount, receiptCount, receiptTargets int
		deletedEvents, artifactCount, artifactSetItems, charges int
		visibleCompletions                                      int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			request.state::text,
			dump.state::text,
			(SELECT count(*) FROM content_deletion_requests
			 WHERE debug_dump_authorization_id = $1 AND source = 'RETENTION_DEBUG_DUMP'),
			(SELECT count(*) FROM content_deletion_targets
			 WHERE debug_dump_authorization_id = $1),
			(SELECT count(*) FROM content_deletion_receipts
			 WHERE request_id = request.id),
			(SELECT count(*) FROM content_deletion_receipt_targets
			 WHERE request_id = request.id),
			(SELECT count(*) FROM debug_dump_events
			 WHERE debug_dump_id = $2 AND action = 'DELETED'),
			(SELECT count(*) FROM artifacts WHERE job_id = request.job_id),
			(SELECT count(*) FROM artifact_set_items AS item
			 JOIN artifact_sets AS artifact_set ON artifact_set.id = item.artifact_set_id
			 WHERE artifact_set.job_id = request.job_id),
			(SELECT count(*) FROM charges WHERE job_id = request.job_id),
			(SELECT count(*) FROM visible_completions WHERE job_id = request.job_id)
		FROM content_deletion_requests AS request
		JOIN debug_dumps AS dump ON dump.id = $2
		WHERE request.debug_dump_authorization_id = $1
		  AND request.source = 'RETENTION_DEBUG_DUMP'
	`, authorizationID, dumpID).Scan(
		&requestState,
		&dumpState,
		&requestCount,
		&targetCount,
		&receiptCount,
		&receiptTargets,
		&deletedEvents,
		&artifactCount,
		&artifactSetItems,
		&charges,
		&visibleCompletions,
	); err != nil {
		t.Fatalf("read completed debug dump retention evidence: %v", err)
	}
	if requestState != "COMPLETED" || dumpState != "DELETED" || requestCount != 1 ||
		targetCount != 2 || receiptCount != 1 || receiptTargets != 2 || deletedEvents != 1 ||
		artifactCount != 0 || artifactSetItems != 0 || charges != 0 || visibleCompletions != 0 {
		t.Fatalf(
			"debug retention request/dump/counts = %s/%s/%d/%d/%d/%d/%d/%d/%d/%d/%d",
			requestState,
			dumpState,
			requestCount,
			targetCount,
			receiptCount,
			receiptTargets,
			deletedEvents,
			artifactCount,
			artifactSetItems,
			charges,
			visibleCompletions,
		)
	}
}

func TestExpiredIncompleteDebugDumpDiscoversObjectAndAbortsMultipart(t *testing.T) {
	fixture := newAssignmentFixture(t, "incomplete-debug-dump-retention", 7)
	authorizationID := seedActiveDebugDumpAuthorization(
		t, fixture.database, fixture.candidate.JobID,
	)
	dumpID, claim := persistIncompleteDebugDump(t, fixture, authorizationID)
	expireDebugDumpAuthorization(t, fixture, authorizationID, dumpID)
	store := &recordingRetentionStore{
		resolved: map[string]artifactstore.ObjectVersion{
			claim.ObjectKey: {
				ObjectKey: claim.ObjectKey, VersionID: "discovered-debug-version",
			},
		},
		multipart: []artifactstore.IncompleteMultipartUpload{{
			ObjectKey:   claim.ObjectKey,
			UploadID:    "incomplete-debug-upload",
			InitiatedAt: time.Now().UTC().Add(-time.Hour),
		}},
	}
	reconciler, err := retention.NewReconciler(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_retention_login",
			"vela-retention-password",
		),
		store,
		retention.ReconcilerConfig{
			InstanceID: "incomplete-debug-dump-retention-reconciler",
			BatchSize:  10,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create incomplete debug dump Reconciler: %v", err)
	}
	result, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || result.ContentDeletionRequestsCreated != 1 || result.Claimed != 2 ||
		result.Completed != 2 || result.Failed != 0 {
		t.Fatalf("incomplete debug dump retention result = %#v error=%v", result, err)
	}
	if len(store.deleted) != 1 || store.deleted[0].ObjectKey != claim.ObjectKey ||
		store.deleted[0].VersionID != "discovered-debug-version" ||
		len(store.aborted) != 1 || store.aborted[0].ObjectKey != claim.ObjectKey ||
		store.aborted[0].UploadID != "incomplete-debug-upload" {
		t.Fatalf("incomplete debug dump store calls = %#v / %#v", store.deleted, store.aborted)
	}
	var state, discoveredVersion string
	var deletedEvents int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			dump.state::text,
			target.discovered_object_version_id,
			(SELECT count(*) FROM debug_dump_events
			 WHERE debug_dump_id = dump.id AND action = 'DELETED')
		FROM debug_dumps AS dump
		JOIN content_deletion_targets AS target ON target.debug_dump_id = dump.id
		WHERE dump.id = $1 AND target.action = 'OBJECT_DISCOVERY'
	`, dumpID).Scan(&state, &discoveredVersion, &deletedEvents); err != nil {
		t.Fatalf("read incomplete debug dump deletion evidence: %v", err)
	}
	if state != "DELETED" || discoveredVersion != "discovered-debug-version" ||
		deletedEvents != 1 {
		t.Fatalf(
			"incomplete debug dump state/discovery/events = %s/%s/%d",
			state,
			discoveredVersion,
			deletedEvents,
		)
	}
}

func TestExpiredIncompleteDebugDumpAlreadyAbsentCompletesDeletion(t *testing.T) {
	fixture := newAssignmentFixture(t, "absent-incomplete-debug-dump-retention", 7)
	authorizationID := seedActiveDebugDumpAuthorization(
		t, fixture.database, fixture.candidate.JobID,
	)
	dumpID, _ := persistIncompleteDebugDump(t, fixture, authorizationID)
	expireDebugDumpAuthorization(t, fixture, authorizationID, dumpID)

	store := &recordingRetentionStore{}
	reconciler, err := retention.NewReconciler(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_retention_login",
			"vela-retention-password",
		),
		store,
		retention.ReconcilerConfig{
			InstanceID: "absent-incomplete-debug-dump-retention-reconciler",
			BatchSize:  10,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create absent incomplete debug dump Reconciler: %v", err)
	}
	result, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || result.ContentDeletionRequestsCreated != 1 || result.Claimed != 2 ||
		result.Completed != 2 || result.Failed != 0 {
		t.Fatalf("absent incomplete debug dump retention result = %#v error=%v", result, err)
	}
	if len(store.deleted) != 0 || len(store.aborted) != 0 {
		t.Fatalf("absent incomplete debug dump store calls = %#v / %#v", store.deleted, store.aborted)
	}

	var dumpState, requestState, targetOutcome, eventOutcome string
	if err := fixture.database.Admin.QueryRow(`
		SELECT dump.state::text, request.state::text,
			target.storage_outcome, event.outcome_code
		FROM debug_dumps AS dump
		JOIN content_deletion_requests AS request
		  ON request.debug_dump_authorization_id = dump.authorization_id
		 AND request.source = 'RETENTION_DEBUG_DUMP'
		JOIN content_deletion_targets AS target
		  ON target.request_id = request.id
		 AND target.debug_dump_id = dump.id
		 AND target.action = 'OBJECT_DISCOVERY'
		JOIN debug_dump_events AS event
		  ON event.debug_dump_id = dump.id
		 AND event.action = 'DELETED'
		WHERE dump.id = $1
	`, dumpID).Scan(&dumpState, &requestState, &targetOutcome, &eventOutcome); err != nil {
		t.Fatalf("read absent incomplete debug dump deletion evidence: %v", err)
	}
	if dumpState != "DELETED" || requestState != "COMPLETED" ||
		targetOutcome != "ALREADY_ABSENT" || eventOutcome != "ALREADY_ABSENT" {
		t.Fatalf(
			"absent incomplete debug dump evidence = %s/%s/%s/%s",
			dumpState, requestState, targetOutcome, eventOutcome,
		)
	}
}

func TestCustomerContentDeletionRevokesAndDeletesDebugDumpInSameRequest(t *testing.T) {
	fixture := newAssignmentFixture(t, "debug-dump-customer-deletion", 7)
	authorizationID := seedActiveDebugDumpAuthorization(
		t, fixture.database, fixture.candidate.JobID,
	)
	dumpID, objectKey, versionID, _, _ := persistAvailableDebugDump(
		t, fixture, authorizationID,
	)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'content_deletion:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant Content Deletion scope: %v", err)
	}
	actor, err := identity.NewAuthenticator(
		newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate Content Deletion Service Principal: %v", err)
	}
	requestService, err := retention.NewService(newRolePool(
		t,
		fixture.database.DSN,
		"vela_retention_request_login",
		"vela-retention-request-password",
	))
	if err != nil {
		t.Fatalf("create Content Deletion request service: %v", err)
	}
	deletion, err := requestService.AcceptContentDeletion(
		context.Background(),
		actor,
		uuid.MustParse(testProjectID),
		fixture.candidate.JobID,
		"debug-dump-customer-deletion",
	)
	if err != nil {
		t.Fatalf("accept debug dump Customer Content Deletion: %v", err)
	}

	var revoked bool
	var targetCount, debugTargetCount, revokeEvents int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			candidate.revoked_at IS NOT NULL,
			(SELECT count(*) FROM content_deletion_targets
			 WHERE request_id = $2),
			(SELECT count(*) FROM content_deletion_targets
			 WHERE request_id = $2 AND debug_dump_authorization_id = $1),
			(SELECT count(*) FROM debug_dump_events
			 WHERE authorization_id = $1 AND action = 'REVOKED'
			   AND actor_kind = 'SERVICE')
		FROM debug_dump_authorizations AS candidate
		WHERE candidate.id = $1
	`, authorizationID, deletion.RequestID).Scan(
		&revoked,
		&targetCount,
		&debugTargetCount,
		&revokeEvents,
	); err != nil {
		t.Fatalf("read Customer Content Deletion debug targets: %v", err)
	}
	if !revoked || targetCount != 3 || debugTargetCount != 2 || revokeEvents != 1 {
		t.Fatalf(
			"customer deletion revoked/targets/debug/events = %t/%d/%d/%d",
			revoked,
			targetCount,
			debugTargetCount,
			revokeEvents,
		)
	}

	debugPrefix := "debug-dumps/" + testOrganizationID + "/" + testProjectID + "/" +
		fixture.candidate.JobID.String() + "/" + authorizationID.String() + "/"
	artifactPrefix := "artifacts/" + testOrganizationID + "/" + testProjectID + "/" +
		fixture.candidate.JobID.String() + "/"
	store := &recordingRetentionStore{multipart: []artifactstore.IncompleteMultipartUpload{
		{
			ObjectKey: debugPrefix + "orphan/customer-delete", UploadID: "debug-customer-upload",
			InitiatedAt: time.Now().UTC().Add(-time.Hour),
		},
		{
			ObjectKey: artifactPrefix + "orphan/customer-delete", UploadID: "artifact-customer-upload",
			InitiatedAt: time.Now().UTC().Add(-time.Hour),
		},
	}}
	reconciler, err := retention.NewReconciler(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_retention_login",
			"vela-retention-password",
		),
		store,
		retention.ReconcilerConfig{
			InstanceID: "debug-dump-customer-deletion-reconciler",
			BatchSize:  10,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create Customer Content Deletion Reconciler: %v", err)
	}
	result, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || result.Claimed != 3 || result.Completed != 3 || result.Failed != 0 {
		t.Fatalf("debug Customer Content Deletion result = %#v error=%v", result, err)
	}
	if len(store.deleted) != 1 || store.deleted[0].ObjectKey != objectKey ||
		store.deleted[0].VersionID != versionID || len(store.aborted) != 2 {
		t.Fatalf("debug Customer Content Deletion store calls = %#v / %#v", store.deleted, store.aborted)
	}

	var requestState, dumpState string
	var receiptTargets, deletedEvents int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			request.state::text,
			dump.state::text,
			(SELECT count(*) FROM content_deletion_receipt_targets
			 WHERE request_id = request.id),
			(SELECT count(*) FROM debug_dump_events
			 WHERE debug_dump_id = dump.id AND action = 'DELETED')
		FROM content_deletion_requests AS request
		JOIN debug_dumps AS dump ON dump.id = $2
		WHERE request.id = $1 AND request.source = 'CUSTOMER'
	`, deletion.RequestID, dumpID).Scan(
		&requestState,
		&dumpState,
		&receiptTargets,
		&deletedEvents,
	); err != nil {
		t.Fatalf("read completed Customer Content Deletion evidence: %v", err)
	}
	if requestState != "COMPLETED" || dumpState != "DELETED" ||
		receiptTargets != 3 || deletedEvents != 1 {
		t.Fatalf(
			"customer deletion request/dump/receipt/events = %s/%s/%d/%d",
			requestState,
			dumpState,
			receiptTargets,
			deletedEvents,
		)
	}
}

func TestDebugDumpMigrationEmptyDownUpAndDurableEvidenceRefusal(t *testing.T) {
	t.Run("empty down and up", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
		if err := goose.DownTo(database.Admin, migrations, 26); err != nil {
			t.Fatalf("contract empty debug dump migration: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 26 {
			t.Fatalf("debug dump migration version after Down = %d error=%v", version, err)
		}
		var debugTable *string
		var sourceValues string
		if err := database.Admin.QueryRow(`
			SELECT
				to_regclass('public.debug_dump_authorizations')::text,
				(
					SELECT string_agg(value.enumlabel, ',' ORDER BY value.enumsortorder)
					FROM pg_catalog.pg_enum AS value
					JOIN pg_catalog.pg_type AS enum_type ON enum_type.oid = value.enumtypid
					WHERE enum_type.typname = 'content_deletion_source'
				)
		`).Scan(&debugTable, &sourceValues); err != nil {
			t.Fatalf("inspect debug dump migration Down: %v", err)
		}
		if debugTable != nil || strings.Contains(sourceValues, "RETENTION_DEBUG_DUMP") ||
			!strings.Contains(sourceValues, "RETENTION_INCOMPLETE_ARTIFACT") {
			t.Fatalf("debug dump migration Down table/sources = %v/%q", debugTable, sourceValues)
		}
		if err := goose.UpTo(database.Admin, migrations, 27); err != nil {
			t.Fatalf("re-expand debug dump migration: %v", err)
		}
		version, err = goose.GetDBVersion(database.Admin)
		if err != nil || version != 27 {
			t.Fatalf("debug dump migration version after Down/Up = %d error=%v", version, err)
		}
	})

	t.Run("durable evidence refuses down", func(t *testing.T) {
		fixture := newAssignmentFixture(t, "debug-dump-down-refusal", 7)
		seedActiveDebugDumpAuthorization(t, fixture.database, fixture.candidate.JobID)
		migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
		err := goose.DownTo(fixture.database.Admin, migrations, 26)
		if err == nil || !strings.Contains(
			err.Error(), "debug_dump_contract_requires_empty_evidence",
		) {
			t.Fatalf("debug dump durable evidence Down error = %v", err)
		}
		version, versionErr := goose.GetDBVersion(fixture.database.Admin)
		if versionErr != nil || version != 27 {
			t.Fatalf(
				"debug dump migration version after refused Down = %d error=%v",
				version,
				versionErr,
			)
		}
	})
}

func expireDebugDumpAuthorization(
	t *testing.T,
	fixture assignmentFixture,
	authorizationID uuid.UUID,
	dumpID uuid.UUID,
) {
	t.Helper()
	expiresAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	authorizedAt := expiresAt.Add(-72 * time.Hour)
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin debug dump expiry setup: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`SET LOCAL session_replication_role = 'replica'`); err != nil {
		t.Fatalf("disable debug authorization immutable trigger: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE debug_dump_authorizations
		SET authorized_at = $2, expires_at = $3
		WHERE id = $1
	`, authorizationID, authorizedAt, expiresAt); err != nil {
		t.Fatalf("move debug dump authorization into past: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE debug_dumps
		SET expires_at = $2, updated_at = clock_timestamp()
		WHERE id = $1
	`, dumpID, expiresAt); err != nil {
		t.Fatalf("move debug dump object into past: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit debug dump expiry setup: %v", err)
	}
}

func persistIncompleteDebugDump(
	t *testing.T,
	fixture assignmentFixture,
	authorizationID uuid.UUID,
) (uuid.UUID, workercontrol.DebugDumpUploadClaim) {
	t.Helper()
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("Acquire incomplete debug dump Assignment: %v", err)
	}
	credentials := leaseCredentials(assignment)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start incomplete debug dump Assignment = %#v error=%v", started, err)
	}
	content := []byte(`{"attempt_id":"incomplete","schema_version":1}`)
	digest := sha256.Sum256(content)
	dumpID := uuid.New()
	claimID := uuid.New()
	claim, err := fixture.service.ClaimDebugDumpUpload(
		context.Background(),
		fixture.worker,
		credentials,
		workercontrol.DebugDumpUploadIntent{
			DebugDumpID: dumpID, AuthorizationID: authorizationID,
			SizeBytes: int64(len(content)), SHA256: digest,
			ContentType: "application/vnd.vela.debug-dump+json",
		},
		claimID,
	)
	if err != nil || claim.Decision != workercontrol.DebugDumpUploadClaimGranted {
		t.Fatalf("Claim incomplete debug dump = %#v error=%v", claim, err)
	}
	if session, err := fixture.service.RecordDebugDumpMultipartSession(
		context.Background(),
		fixture.worker,
		credentials,
		dumpID,
		claimID,
		"incomplete-debug-upload",
	); err != nil || session.Decision != workercontrol.DebugDumpMultipartSessionRecorded {
		t.Fatalf("Record incomplete debug dump session = %#v error=%v", session, err)
	}
	return dumpID, claim
}

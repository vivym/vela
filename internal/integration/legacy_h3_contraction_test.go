//go:build integration

package integration_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/workercontrol"
)

func TestLegacyH3ContractionPreparationArchivesAndFreezesMachineAuthority(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	var functionOwner string
	var securityDefiner, publicExecute bool
	if err := database.Admin.QueryRow(`
		SELECT owner.rolname, procedure.prosecdef,
			EXISTS (
				SELECT 1
				FROM pg_catalog.aclexplode(
					COALESCE(
						procedure.proacl,
						pg_catalog.acldefault('f', procedure.proowner)
					)
				) AS privilege
				WHERE privilege.grantee = 0
				  AND privilege.privilege_type = 'EXECUTE'
			)
		FROM pg_catalog.pg_proc AS procedure
		JOIN pg_catalog.pg_roles AS owner ON owner.oid = procedure.proowner
		WHERE procedure.oid =
			'vela_prepare_legacy_h3_contraction(uuid,text)'::regprocedure
	`).Scan(&functionOwner, &securityDefiner, &publicExecute); err != nil {
		t.Fatalf("inspect Legacy H3 preparation function boundary: %v", err)
	}
	if functionOwner != "vela_catalog_promotion_owner" ||
		!securityDefiner || publicExecute {
		t.Fatalf(
			"Legacy H3 preparation boundary = owner %s security definer %t public %t",
			functionOwner,
			securityDefiner,
			publicExecute,
		)
	}
	setLeaseRenewalProtocolGate(t, database.Admin, true, "legacy archive fixture")
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	legacyJob := submitCutoverJob(t, server.URL, "legacy-h3-archive-fixture")
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch,
			lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/h3-contraction-archive', 7,
			'READY', 'HEALTHY'
		)
	`, testWorkerID); err != nil {
		t.Fatalf("seed Legacy H3 archive Worker: %v", err)
	}
	internalPool := newRolePool(
		t, database.DSN, "vela_internal_login", "vela-internal-password",
	)
	workerService, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create Legacy H3 archive Worker coordinator: %v", err)
	}
	assignment, err := workerService.Acquire(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: uuid.MustParse(testWorkerID)},
		7,
		&workercontrol.AssignmentCandidate{
			JobID:              uuid.MustParse(legacyJob.JobID),
			ExpectedJobVersion: 1,
			ExecutionProfileRevisionID: uuid.MustParse(
				"00000000-0000-0000-0000-000000000014",
			),
		},
	)
	if err != nil {
		t.Fatalf("create Legacy H3 archive Assignment: %v", err)
	}
	var leaseTokenDigestHex, leaseSigningKeyID string
	var leaseTokenClaimExpiresAt time.Time
	var leaseRenewalProtocolVersion int16
	if err := database.Admin.QueryRow(`
		SELECT encode(token_digest, 'hex'), signing_key_id,
		       token_claim_expires_at, renewal_protocol_version
		FROM attempt_leases
		WHERE attempt_id = $1
	`, assignment.AttemptID).Scan(
		&leaseTokenDigestHex,
		&leaseSigningKeyID,
		&leaseTokenClaimExpiresAt,
		&leaseRenewalProtocolVersion,
	); err != nil {
		t.Fatalf("read Legacy H3 archive Lease authority: %v", err)
	}
	terminalTx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Legacy H3 terminal fixture: %v", err)
	}
	defer func() { _ = terminalTx.Rollback() }()
	for _, operation := range []struct {
		name      string
		statement string
		arguments []any
	}{
		{
			name: "revoke Lease",
			statement: `UPDATE attempt_leases
				SET revoked_at = clock_timestamp(), updated_at = clock_timestamp()
				WHERE attempt_id = $1`,
			arguments: []any{assignment.AttemptID},
		},
		{
			name: "finish LOST Attempt",
			statement: `UPDATE attempts
				SET state = 'LOST', ended_at = clock_timestamp(),
					updated_at = clock_timestamp()
				WHERE id = $1`,
			arguments: []any{assignment.AttemptID},
		},
		{
			name: "write terminal event",
			statement: `INSERT INTO outbox_events (
					event_id, organization_id, project_id, aggregate_type,
					aggregate_id, aggregate_version, event_type, schema_version,
					payload, occurred_at
				)
				SELECT $1, organization_id, project_id, 'Job', id, version + 1,
					'job.failed', 1, convert_to('{}', 'UTF8'), clock_timestamp()
				FROM jobs WHERE id = $2`,
			arguments: []any{uuid.New(), assignment.JobID},
		},
		{
			name: "finish Job",
			statement: `UPDATE jobs
				SET state = 'FAILED', version = version + 1,
					updated_at = clock_timestamp()
				WHERE id = $1`,
			arguments: []any{assignment.JobID},
		},
		{
			name:      "release Project capacity",
			statement: `UPDATE projects SET running_count = running_count - 1 WHERE id = $1 AND running_count > 0`,
			arguments: []any{uuid.MustParse(testProjectID)},
		},
		{
			name:      "release Worker",
			statement: `UPDATE workers SET lifecycle_state = 'READY', updated_at = clock_timestamp() WHERE id = $1`,
			arguments: []any{assignment.WorkerID},
		},
	} {
		if _, err := terminalTx.Exec(operation.statement, operation.arguments...); err != nil {
			t.Fatalf("terminalize Legacy H3 archive authority (%s): %v", operation.name, err)
		}
	}
	if err := terminalTx.Commit(); err != nil {
		t.Fatalf("commit Legacy H3 terminal fixture: %v", err)
	}
	if _, err := database.Admin.Exec(
		`DELETE FROM outbox_events WHERE aggregate_id = $1`,
		assignment.JobID,
	); err != nil {
		t.Fatalf("drain Legacy H3 event backlog: %v", err)
	}
	promotion := stageCutoverPromotionPool(t, database)
	activateProductionStageOnlyCutover(t, database, promotion)

	startInventoryID := captureLegacyAuthorityInventory(
		t, promotion, "legacy-h3-contraction-window-start",
	)
	startEvidenceID := recordStageCutoverExternalDrainEvidence(
		t, promotion, [5]int64{}, "legacy-h3-contraction-window-start",
	)
	time.Sleep(1100 * time.Millisecond)
	endInventoryID := captureLegacyAuthorityInventory(
		t, promotion, "legacy-h3-contraction-window-end",
	)
	endEvidenceID := recordStageCutoverExternalDrainEvidence(
		t, promotion, [5]int64{}, "legacy-h3-contraction-window-end",
	)
	zeroBacklogReceiptID := uuid.New()
	if _, err := promotion.Exec(context.Background(), `
		SELECT * FROM vela_seal_stage_cutover_zero_backlog(
			$1, $2, $3, $4, $5, $6
		)
	`, zeroBacklogReceiptID, startInventoryID, endInventoryID,
		startEvidenceID, endEvidenceID, "integration-legacy-h3-contraction"); err != nil {
		t.Fatalf("seal zero backlog before Legacy H3 contraction: %v", err)
	}

	type result struct {
		zeroBacklogReceiptID uuid.UUID
		cutoverRevisionID    uuid.UUID
		preparedAt           time.Time
		archiveDigest        []byte
		contentDigest        []byte
		replayed             bool
	}
	prepare := func() result {
		t.Helper()
		var got result
		if err := promotion.QueryRow(context.Background(), `
			SELECT zero_backlog_receipt_id, cutover_revision_id, prepared_at,
			       archive_digest, content_digest, replayed
			FROM vela_prepare_legacy_h3_contraction($1, $2)
		`, zeroBacklogReceiptID, "integration-legacy-h3-contraction").Scan(
			&got.zeroBacklogReceiptID,
			&got.cutoverRevisionID,
			&got.preparedAt,
			&got.archiveDigest,
			&got.contentDigest,
			&got.replayed,
		); err != nil {
			t.Fatalf("prepare Legacy H3 contraction: %v", err)
		}
		return got
	}
	created := prepare()
	if created.zeroBacklogReceiptID != zeroBacklogReceiptID ||
		created.cutoverRevisionID == uuid.Nil || created.preparedAt.IsZero() ||
		len(created.archiveDigest) != 32 || len(created.contentDigest) != 32 ||
		created.replayed {
		t.Fatalf("Legacy H3 contraction result = %#v", created)
	}
	replayed := prepare()
	if !replayed.replayed ||
		replayed.zeroBacklogReceiptID != created.zeroBacklogReceiptID ||
		replayed.cutoverRevisionID != created.cutoverRevisionID ||
		!replayed.preparedAt.Equal(created.preparedAt) ||
		string(replayed.archiveDigest) != string(created.archiveDigest) ||
		string(replayed.contentDigest) != string(created.contentDigest) {
		t.Fatalf("replayed Legacy H3 contraction result = %#v", replayed)
	}
	_, err = promotion.Exec(context.Background(), `
		SELECT * FROM vela_prepare_legacy_h3_contraction($1, $2)
	`, zeroBacklogReceiptID, "changed-contraction-actor")
	assertPostgresConstraint(t, err, "legacy_h3_contraction_preparation_replay_mismatch")
	_, err = promotion.Exec(context.Background(), `
		SELECT * FROM vela_prepare_legacy_h3_contraction($1, $2)
	`, uuid.New(), "integration-legacy-h3-contraction")
	assertPostgresConstraint(t, err, "legacy_h3_contraction_preparation_already_completed")

	_, err = database.Admin.Exec(`
		DELETE FROM legacy_h3_execution_archive
		WHERE zero_backlog_receipt_id = $1
	`, zeroBacklogReceiptID)
	assertPostgresConstraint(t, err, "stage_cutover_history_immutable")

	var archiveCount int
	var archivedJobPool, archivedAttemptWorker string
	var archivedLeaseTokenDigest, archivedLeaseSigningKeyID string
	var archivedLeaseTokenClaimExpiresAt time.Time
	var archivedLeaseRenewalProtocolVersion int16
	var archiveRowsValid bool
	if err := database.Admin.QueryRow(`
		SELECT count(*),
			bool_and(octet_length(content_digest) = 32),
			max(machine_authority ->> 'worker_pool_id')
				FILTER (WHERE record_kind = 'JOB'),
			max(machine_authority ->> 'worker_id')
				FILTER (WHERE record_kind = 'ATTEMPT'),
			max(machine_authority ->> 'token_digest')
				FILTER (WHERE record_kind = 'LEASE'),
			max(machine_authority ->> 'signing_key_id')
				FILTER (WHERE record_kind = 'LEASE'),
			max((machine_authority ->> 'token_claim_expires_at')::timestamptz)
				FILTER (WHERE record_kind = 'LEASE'),
			max((machine_authority ->> 'renewal_protocol_version')::smallint)
				FILTER (WHERE record_kind = 'LEASE')
		FROM legacy_h3_execution_archive
		WHERE zero_backlog_receipt_id = $1
	`, zeroBacklogReceiptID).Scan(
		&archiveCount,
		&archiveRowsValid,
		&archivedJobPool,
		&archivedAttemptWorker,
		&archivedLeaseTokenDigest,
		&archivedLeaseSigningKeyID,
		&archivedLeaseTokenClaimExpiresAt,
		&archivedLeaseRenewalProtocolVersion,
	); err != nil {
		t.Fatalf("inspect Legacy H3 archive rows: %v", err)
	}
	if archiveCount != 3 || !archiveRowsValid ||
		archivedJobPool != "00000000-0000-0000-0000-000000000005" ||
		archivedAttemptWorker != testWorkerID ||
		archivedLeaseTokenDigest != `\x`+leaseTokenDigestHex ||
		archivedLeaseSigningKeyID != leaseSigningKeyID ||
		!archivedLeaseTokenClaimExpiresAt.Equal(leaseTokenClaimExpiresAt) ||
		archivedLeaseRenewalProtocolVersion != leaseRenewalProtocolVersion {
		t.Fatalf(
			"Legacy H3 archive = rows %d valid %t pool %s worker %s "+
				"lease digest %s key %s claim expiry %s protocol %d",
			archiveCount,
			archiveRowsValid,
			archivedJobPool,
			archivedAttemptWorker,
			archivedLeaseTokenDigest,
			archivedLeaseSigningKeyID,
			archivedLeaseTokenClaimExpiresAt,
			archivedLeaseRenewalProtocolVersion,
		)
	}
	for _, mutation := range []struct {
		name      string
		statement string
		argument  any
	}{
		{
			name:      "Job",
			statement: `UPDATE jobs SET updated_at = clock_timestamp() WHERE id = $1`,
			argument:  assignment.JobID,
		},
		{
			name:      "Attempt",
			statement: `UPDATE attempts SET state = 'FAILED' WHERE id = $1`,
			argument:  assignment.AttemptID,
		},
		{
			name: "Lease",
			statement: `UPDATE attempt_leases
				SET updated_at = clock_timestamp() WHERE attempt_id = $1`,
			argument: assignment.AttemptID,
		},
	} {
		_, err = database.Admin.Exec(mutation.statement, mutation.argument)
		if err == nil {
			t.Fatalf("prepared Legacy H3 %s mutation succeeded", mutation.name)
		}
		assertPostgresConstraint(
			t, err, "legacy_h3_contraction_preparation_frozen",
		)
	}

	var prepared bool
	if err := database.Admin.QueryRow(`
		SELECT
			to_regclass('public.legacy_h3_execution_archive') IS NOT NULL
			AND to_regclass('public.legacy_h3_contraction_readiness_receipts') IS NOT NULL
			AND to_regclass('public.attempt_leases') IS NOT NULL
			AND to_regtype('public.execution_authority_kind') IS NOT NULL
			AND to_regprocedure('public.vela_apply_stage_command(jsonb)') IS NOT NULL
	`).Scan(&prepared); err != nil {
		t.Fatalf("inspect prepared Legacy H3 contraction: %v", err)
	}
	if !prepared {
		t.Fatal("Legacy H3 contraction preparation did not preserve the release boundary")
	}

	stageJob := submitCutoverJob(t, server.URL, "stage-after-contraction-preparation")
	var stageAuthority string
	if err := database.Admin.QueryRow(`
		SELECT execution_authority_kind::text FROM jobs WHERE id = $1
	`, stageJob.JobID).Scan(&stageAuthority); err != nil {
		t.Fatalf("read Stage Job after contraction preparation: %v", err)
	}
	if stageAuthority != "STAGE_GRAPH" {
		t.Fatalf("post-preparation Job authority = %s, want STAGE_GRAPH", stageAuthority)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err = goose.DownTo(database.Admin, migrations, 52)
	assertPostgresConstraint(t, err, "legacy_h3_contraction_rollback_is_unsafe")
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil || version != 53 {
		t.Fatalf(
			"Legacy H3 schema version after refused Down = %d error=%v",
			version,
			versionErr,
		)
	}
}

func TestLegacyH3ContractionPreparationRequiresZeroBacklogSeal(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	promotion := stageCutoverPromotionPool(t, database)

	_, err := promotion.Exec(context.Background(), `
		SELECT * FROM vela_prepare_legacy_h3_contraction($1, $2)
	`, uuid.New(), "integration-legacy-h3-contraction")
	assertPostgresConstraint(t, err, "legacy_h3_contraction_receipt_required")
}

func TestLegacyH3ContractionPreparationMigrationRoundTripBeforeReceipt(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	assertLostTerminal := func(want bool) {
		t.Helper()
		var definitionsWithLost int
		if err := database.Admin.QueryRow(`
			SELECT count(*) FILTER (
				WHERE pg_get_functiondef(signature::regprocedure) LIKE '%''LOST''%'
			)
			FROM (VALUES
				('vela_capture_legacy_authority_inventory(uuid,text)'),
				('vela_current_legacy_authority_inventory_total()')
			) AS function(signature)
		`).Scan(&definitionsWithLost); err != nil {
			t.Fatalf("inspect Legacy H3 inventory function semantics: %v", err)
		}
		wantDefinitionsWithLost := 0
		if want {
			wantDefinitionsWithLost = 2
		}
		if definitionsWithLost != wantDefinitionsWithLost {
			t.Fatalf(
				"Legacy H3 inventory functions containing LOST = %d, want %d",
				definitionsWithLost,
				wantDefinitionsWithLost,
			)
		}
	}

	assertLostTerminal(true)

	if err := goose.DownTo(database.Admin, migrations, 52); err != nil {
		t.Fatalf("contract empty Legacy H3 guard migration: %v", err)
	}
	assertLostTerminal(false)
	if err := goose.UpTo(database.Admin, migrations, 53); err != nil {
		t.Fatalf("re-expand empty Legacy H3 guard migration: %v", err)
	}
	assertLostTerminal(true)
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 53 {
		t.Fatalf("Legacy H3 round-trip schema version = %d error=%v", version, err)
	}
}

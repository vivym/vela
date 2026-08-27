//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/catalogpromotion"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/productiongates"
)

func TestCatalogPromotionPlanAppliesAndReplaysAtomically(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedThreePresetCatalog(t, database)
	promotionPool := newRolePool(
		t,
		database.DSN,
		"vela_catalog_promotion_login",
		"vela-catalog-promotion-password",
	)
	service, err := catalogpromotion.New(context.Background(), promotionPool)
	if err != nil {
		t.Fatalf("configure Catalog Promotion service: %v", err)
	}
	planPath := writeCatalogPromotionFiles(t, false)
	first, err := service.Apply(context.Background(), planPath)
	if err != nil {
		t.Fatalf("apply Catalog Promotion plan: %v", err)
	}
	if first.ProtocolVersion != 2 || len(first.ReceiptIDs) != len(productiongates.AllGates()) {
		t.Fatalf("Catalog Promotion result = %#v", first)
	}
	second, err := service.Apply(context.Background(), planPath)
	if err != nil {
		t.Fatalf("replay Catalog Promotion plan: %v", err)
	}
	if second.ManifestDigest != first.ManifestDigest || second.ReleaseDigest != first.ReleaseDigest ||
		second.ProtocolVersion != first.ProtocolVersion {
		t.Fatalf("replayed Catalog Promotion = %#v, want %#v", second, first)
	}

	var receipts, evidence, bindings, transitions int64
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM production_gate_receipts),
			(SELECT count(*) FROM profile_certification_evidence),
			(SELECT count(*) FROM rate_card_release_bindings),
			(SELECT count(*) FROM catalog_evidence_protocol_transitions)
	`).Scan(&receipts, &evidence, &bindings, &transitions); err != nil {
		t.Fatalf("read applied Catalog Promotion evidence: %v", err)
	}
	if receipts != 9 || evidence != 3 || bindings != 1 || transitions != 1 {
		t.Fatalf(
			"Catalog Promotion rows = receipts %d evidence %d bindings %d transitions %d",
			receipts, evidence, bindings, transitions,
		)
	}
}

func TestCatalogPromotionPlanFailureRollsBackManifest(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedThreePresetCatalog(t, database)
	promotionPool := newRolePool(
		t,
		database.DSN,
		"vela_catalog_promotion_login",
		"vela-catalog-promotion-password",
	)
	service, err := catalogpromotion.New(context.Background(), promotionPool)
	if err != nil {
		t.Fatalf("configure Catalog Promotion service: %v", err)
	}
	_, err = service.Apply(context.Background(), writeCatalogPromotionFiles(t, true))
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.ConstraintName != "profile_certification_not_promotable" {
		t.Fatalf("invalid Catalog Promotion error = %v", err)
	}
	var manifests, receipts, evidence int64
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM production_gate_manifests),
			(SELECT count(*) FROM production_gate_receipts),
			(SELECT count(*) FROM profile_certification_evidence)
	`).Scan(&manifests, &receipts, &evidence); err != nil {
		t.Fatalf("read rolled-back Catalog Promotion rows: %v", err)
	}
	if manifests != 0 || receipts != 0 || evidence != 0 {
		t.Fatalf("failed Catalog Promotion left rows %d/%d/%d", manifests, receipts, evidence)
	}
}

func TestCatalogPromotionRequiresSealedThreePresetEvidence(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedThreePresetCatalog(t, database)
	promotion := newRolePool(
		t,
		database.DSN,
		"vela_catalog_promotion_login",
		"vela-catalog-promotion-password",
	)
	if err := veladb.VerifyRole(context.Background(), promotion, veladb.RoleCatalogPromotion); err != nil {
		t.Fatalf("verify Catalog Promotion role: %v", err)
	}
	receipts := recordAndSealLaunchManifest(t, promotion)

	_, err := promotion.Exec(context.Background(), `
		SELECT * FROM vela_enable_evidenced_catalog($1)
	`, receipts[productiongates.GatePresetCertification])
	assertCatalogDatabaseError(
		t, err, "55000", "catalog_evidence_is_incomplete", "enable without evidence",
	)

	presetReceiptID := receipts[productiongates.GatePresetCertification]
	promoteSeededCatalog(t, promotion, presetReceiptID)

	var protocolVersion int
	var mode string
	var replayed bool
	err = promotion.QueryRow(context.Background(), `
		SELECT protocol_version, mode::text, replayed
		FROM vela_enable_evidenced_catalog($1)
	`, presetReceiptID).Scan(&protocolVersion, &mode, &replayed)
	if err != nil || protocolVersion != 2 || mode != "EVIDENCED" || replayed {
		t.Fatalf("enable evidenced Catalog = version %d mode %q replayed %t error %v", protocolVersion, mode, replayed, err)
	}

	err = promotion.QueryRow(context.Background(), `
		SELECT protocol_version, mode::text, replayed
		FROM vela_enable_evidenced_catalog($1)
	`, presetReceiptID).Scan(&protocolVersion, &mode, &replayed)
	if err != nil || !replayed {
		t.Fatalf("replay evidenced Catalog transition = replayed %t error %v", replayed, err)
	}

	_, err = database.Admin.Exec(`
		INSERT INTO model_revisions (id, stable_id, revision, state, content_hash)
		VALUES ($1, 'unreceipted-model', 1, 'ACTIVE', repeat('a', 64))
	`, uuid.New())
	assertCatalogDatabaseError(
		t, err, "55000", "catalog_active_revision_requires_evidence", "unreceipted ACTIVE revision",
	)

	_, err = database.Admin.Exec(`
		INSERT INTO rate_card_lines (
			id, rate_card_revision_id, model_revision_id,
			generation_preset_revision_id, service_class_revision_id,
			output_spec_id, unit_amount_minor, currency
		) SELECT $1, id, $2, $3, $4, $5, 1, 'CNY'
		FROM rate_card_revisions WHERE id = $6
	`,
		uuid.New(),
		uuid.MustParse("00000000-0000-0000-0000-000000000010"),
		uuid.MustParse("00000000-0000-0000-0000-000000000011"),
		uuid.MustParse("00000000-0000-0000-0000-000000000012"),
		uuid.MustParse("00000000-0000-0000-0000-000000000013"),
		uuid.MustParse("00000000-0000-0000-0000-000000000016"),
	)
	assertCatalogDatabaseError(
		t, err, "55000", "active_rate_card_lines_are_immutable", "mutate ACTIVE RateCard lines",
	)

	inactiveRateCardID := uuid.MustParse("35000000-0000-0000-0000-000000000299")
	if _, err := database.Admin.Exec(`
		INSERT INTO rate_card_revisions (id, revision, state, effective_at)
		VALUES ($1, 2, 'CANARY', clock_timestamp())
	`, inactiveRateCardID); err != nil {
		t.Fatalf("seed inactive RateCard for active-line move: %v", err)
	}
	_, err = database.Admin.Exec(`
		UPDATE rate_card_lines SET rate_card_revision_id = $1 WHERE id = $2
	`,
		inactiveRateCardID,
		uuid.MustParse("00000000-0000-0000-0000-000000000017"),
	)
	assertCatalogDatabaseError(
		t, err, "55000", "active_rate_card_lines_are_immutable", "move line out of ACTIVE RateCard",
	)

	_, err = database.Admin.Exec(`TRUNCATE rate_card_lines CASCADE`)
	assertCatalogDatabaseError(
		t, err, "55000", "active_rate_card_lines_are_immutable", "truncate ACTIVE RateCard lines",
	)

	_, err = promotion.Exec(context.Background(), `
		INSERT INTO production_gate_receipts (
			id, manifest_digest, schema_version, gate, release_digest,
			configuration_revision, validation_environment, result, owner_identity,
			acceptance_threshold, observed_result, evidence_ref, evidence_digest,
			started_at, completed_at, recorded_at
		) VALUES (
			$1, decode(repeat('01', 32), 'hex'), 1, 'preset-certification',
			decode(repeat('02', 32), 'hex'), 'config', 'environment', 'PASS', 'owner',
			'threshold', 'observed', 'evidence.json', decode(repeat('03', 32), 'hex'),
			clock_timestamp(), clock_timestamp(), clock_timestamp()
		)
	`, uuid.New())
	assertCatalogDatabaseError(t, err, "42501", "", "direct receipt insert")

	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "evidenced-catalog-admission", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"admit only after evidenced catalog promotion"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("Admission after evidenced promotion = %d body=%s", accepted.StatusCode, accepted.Body)
	}
}

func TestCatalogPromotionSupportsSubsequentRelease(t *testing.T) {
	database, promotion, firstReceiptID := prepareCatalogTransition(t)

	var protocolVersion int
	var mode string
	var replayed bool
	if err := promotion.QueryRow(context.Background(), `
		SELECT protocol_version, mode::text, replayed
		FROM vela_enable_evidenced_catalog($1)
	`, firstReceiptID).Scan(&protocolVersion, &mode, &replayed); err != nil ||
		protocolVersion != 2 || mode != "EVIDENCED" || replayed {
		t.Fatalf(
			"initial Catalog transition = version %d mode %q replayed %t error %v",
			protocolVersion, mode, replayed, err,
		)
	}

	seedSubsequentCatalogRelease(t, database)
	secondReceipts := recordAndSealLaunchManifestForRelease(t, promotion, 2)
	secondReceiptID := secondReceipts[productiongates.GatePresetCertification]
	for index, certificationID := range []uuid.UUID{
		uuid.MustParse("36000000-0000-0000-0000-000000000014"),
		uuid.MustParse("36000000-0000-0000-0000-000000000024"),
		uuid.MustParse("36000000-0000-0000-0000-000000000034"),
	} {
		promoteProfileCertification(
			t,
			promotion,
			uuid.MustParse(fmt.Sprintf("36000000-0000-0000-0000-%012d", 401+index)),
			certificationID,
			secondReceiptID,
		)
	}
	promoteRateCardRevision(
		t,
		promotion,
		uuid.MustParse("36000000-0000-0000-0000-000000000501"),
		uuid.MustParse("36000000-0000-0000-0000-000000000016"),
		secondReceiptID,
	)
	if err := promotion.QueryRow(context.Background(), `
		SELECT protocol_version, mode::text, replayed
		FROM vela_enable_evidenced_catalog($1)
	`, secondReceiptID).Scan(&protocolVersion, &mode, &replayed); err != nil ||
		protocolVersion != 2 || mode != "EVIDENCED" || !replayed {
		t.Fatalf(
			"subsequent Catalog release = version %d mode %q replayed %t error %v",
			protocolVersion, mode, replayed, err,
		)
	}

	var transitionReceiptID uuid.UUID
	var transitions, secondBindings int64
	if err := database.Admin.QueryRow(`
		SELECT
			state.launch_receipt_id,
			(SELECT count(*) FROM catalog_evidence_protocol_transitions),
			(SELECT count(*) FROM rate_card_release_bindings
			 WHERE launch_receipt_id = $1)
		FROM catalog_evidence_protocol_state AS state
		WHERE state.singleton
	`, secondReceiptID).Scan(&transitionReceiptID, &transitions, &secondBindings); err != nil {
		t.Fatalf("read subsequent Catalog release authority: %v", err)
	}
	if transitionReceiptID != firstReceiptID || transitions != 1 || secondBindings != 1 {
		t.Fatalf(
			"subsequent release authority = transition receipt %s transitions %d bindings %d",
			transitionReceiptID, transitions, secondBindings,
		)
	}
}

func TestCatalogEvidenceTransitionSerializesWithActiveWrites(t *testing.T) {
	t.Run("writer commits before transition scan", func(t *testing.T) {
		database, promotion, receiptID := prepareCatalogTransition(t)
		writer, err := database.Admin.Begin()
		if err != nil {
			t.Fatalf("begin unreceipted Catalog writer: %v", err)
		}
		defer func() { _ = writer.Rollback() }()
		if _, err := writer.Exec(`
			INSERT INTO model_revisions (id, stable_id, revision, state, content_hash)
			VALUES ($1, 'concurrent-unreceipted-model', 1, 'ACTIVE', repeat('a', 64))
		`, uuid.New()); err != nil {
			t.Fatalf("stage concurrent unreceipted ACTIVE revision: %v", err)
		}

		enableErrors := make(chan error, 1)
		go func() {
			_, enableErr := promotion.Exec(context.Background(), `
				SELECT * FROM vela_enable_evidenced_catalog($1)
			`, receiptID)
			enableErrors <- enableErr
		}()
		waitForRoleDatabaseLock(t, database.Admin, "vela_catalog_promotion_login")
		if err := writer.Commit(); err != nil {
			t.Fatalf("commit concurrent unreceipted ACTIVE revision: %v", err)
		}
		select {
		case enableErr := <-enableErrors:
			assertCatalogDatabaseError(
				t, enableErr, "55000", "catalog_evidence_is_incomplete",
				"transition after concurrent unreceipted ACTIVE revision",
			)
		case <-time.After(10 * time.Second):
			t.Fatal("Catalog evidence transition did not finish after writer commit")
		}
	})

	t.Run("transition commits before writer validation", func(t *testing.T) {
		database, promotion, receiptID := prepareCatalogTransition(t)
		transition, err := promotion.Begin(context.Background())
		if err != nil {
			t.Fatalf("begin Catalog evidence transition: %v", err)
		}
		defer func() { _ = transition.Rollback(context.Background()) }()
		var protocolVersion int
		var mode string
		var replayed bool
		if err := transition.QueryRow(context.Background(), `
			SELECT protocol_version, mode::text, replayed
			FROM vela_enable_evidenced_catalog($1)
		`, receiptID).Scan(&protocolVersion, &mode, &replayed); err != nil {
			t.Fatalf("stage Catalog evidence transition: %v", err)
		}

		writerErrors := make(chan error, 1)
		go func() {
			_, writerErr := database.Admin.Exec(`
				INSERT INTO model_revisions (id, stable_id, revision, state, content_hash)
				VALUES ($1, 'post-transition-unreceipted-model', 1, 'ACTIVE', repeat('b', 64))
			`, uuid.New())
			writerErrors <- writerErr
		}()
		waitForRoleDatabaseLock(t, database.Admin, "postgres")
		if err := transition.Commit(context.Background()); err != nil {
			t.Fatalf("commit Catalog evidence transition: %v", err)
		}
		select {
		case writerErr := <-writerErrors:
			assertCatalogDatabaseError(
				t, writerErr, "55000", "catalog_active_revision_requires_evidence",
				"writer after concurrent Catalog evidence transition",
			)
		case <-time.After(10 * time.Second):
			t.Fatal("unreceipted Catalog writer did not finish after transition commit")
		}
	})
}

func TestCatalogPromotionRejectsNonExactPresetCoverage(t *testing.T) {
	t.Run("duplicate stable Preset revision", func(t *testing.T) {
		database, promotion, receiptID := prepareCatalogTransition(t)
		seedDuplicateQualityPreset(t, database)
		if _, err := database.Admin.Exec(`
			UPDATE generation_preset_revisions SET state = 'CANARY'
			WHERE id = '35000000-0000-0000-0000-000000000011'
		`); err != nil {
			t.Fatalf("supersede evidenced quality Preset revision: %v", err)
		}
		promoteProfileCertification(
			t,
			promotion,
			uuid.MustParse("37000000-0000-0000-0000-000000000401"),
			uuid.MustParse("37000000-0000-0000-0000-000000000014"),
			receiptID,
		)
		_, err := promotion.Exec(context.Background(), `
			SELECT * FROM vela_promote_rate_card($1, $2, $3)
		`,
			uuid.MustParse("35000000-0000-0000-0000-000000000201"),
			uuid.MustParse("00000000-0000-0000-0000-000000000016"),
			receiptID,
		)
		assertCatalogDatabaseError(
			t, err, "55000", "rate_card_requires_three_preset_evidence",
			"RateCard with duplicate stable Preset revision",
		)
	})

	t.Run("inactive Preset revision", func(t *testing.T) {
		database, promotion, receiptID := prepareCatalogTransition(t)
		if _, err := database.Admin.Exec(`
			UPDATE generation_preset_revisions SET state = 'CANARY'
			WHERE id = '35000000-0000-0000-0000-000000000011'
		`); err != nil {
			t.Fatalf("deactivate evidenced quality Preset: %v", err)
		}
		_, err := promotion.Exec(context.Background(), `
			SELECT * FROM vela_promote_rate_card($1, $2, $3)
		`,
			uuid.MustParse("35000000-0000-0000-0000-000000000201"),
			uuid.MustParse("00000000-0000-0000-0000-000000000016"),
			receiptID,
		)
		assertCatalogDatabaseError(
			t, err, "55000", "rate_card_requires_three_preset_evidence",
			"RateCard with inactive Preset revision",
		)
	})
}

func TestCatalogPromotionRejectsMetricsBelowThreshold(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedThreePresetCatalog(t, database)
	promotion := newRolePool(
		t,
		database.DSN,
		"vela_catalog_promotion_login",
		"vela-catalog-promotion-password",
	)
	receipts := recordAndSealLaunchManifest(t, promotion)

	_, err := promotion.Exec(context.Background(), `
		SELECT * FROM vela_promote_profile_certification(
			$1, $2, $3, 'h3-8gpu-driver-r1', 'h3-video-quality-v2',
			820000, 819999, 990000, 999000,
			900000, 1800000, 1700000,
			500000, 450000, 'CNY', 950000, 990000, $4
		)
	`,
		uuid.New(),
		uuid.MustParse("00000000-0000-0000-0000-000000000015"),
		uuid.MustParse(testInferenceBackendRevisionID),
		receipts[productiongates.GatePresetCertification],
	)
	assertCatalogDatabaseError(
		t, err, "22023", "", "quality result below threshold",
	)

	var evidence int64
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM profile_certification_evidence
	`).Scan(&evidence); err != nil {
		t.Fatalf("count rejected certification evidence: %v", err)
	}
	if evidence != 0 {
		t.Fatalf("rejected certification left %d evidence rows", evidence)
	}
}

func TestCatalogPromotionMigrationEmptyDownUpAndDurableEvidenceRefusal(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		if err := goose.DownTo(database.Admin, migrations, 31); err != nil {
			t.Fatalf("migrate empty Catalog Promotion schema down: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 31 {
			t.Fatalf("Catalog Promotion version after Down = %d error=%v", version, err)
		}
		for _, table := range []string{
			"production_gate_manifests",
			"production_gate_receipts",
			"inference_backend_revisions",
			"profile_certification_evidence",
			"rate_card_release_bindings",
			"catalog_evidence_protocol_state",
			"catalog_evidence_protocol_transitions",
		} {
			assertTableDoesNotExist(t, database.Admin, table)
		}
		if err := goose.UpTo(database.Admin, migrations, 32); err != nil {
			t.Fatalf("migrate Catalog Promotion schema up: %v", err)
		}
		version, err = goose.GetDBVersion(database.Admin)
		if err != nil || version != 32 {
			t.Fatalf("Catalog Promotion version after Down Up = %d error=%v", version, err)
		}
		promotion := newRolePool(
			t,
			database.DSN,
			"vela_catalog_promotion_login",
			"vela-catalog-promotion-password",
		)
		if err := veladb.VerifyRole(
			context.Background(), promotion, veladb.RoleCatalogPromotion,
		); err != nil {
			t.Fatalf("verify Catalog Promotion role after Down Up: %v", err)
		}
	})

	t.Run("durable evidence refuses Down", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		promotion := newRolePool(
			t,
			database.DSN,
			"vela_catalog_promotion_login",
			"vela-catalog-promotion-password",
		)
		recordAndSealLaunchManifest(t, promotion)
		err := goose.DownTo(database.Admin, migrations, 31)
		assertPostgresConstraint(t, err, "catalog_promotion_rollback_is_unsafe")
		version, versionErr := goose.GetDBVersion(database.Admin)
		if versionErr != nil || version != 32 {
			t.Fatalf(
				"Catalog Promotion version after evidence refusal = %d error=%v",
				version,
				versionErr,
			)
		}
		var receipts int64
		if err := database.Admin.QueryRow(`
			SELECT count(*) FROM production_gate_receipts
		`).Scan(&receipts); err != nil || receipts != int64(len(productiongates.AllGates())) {
			t.Fatalf("durable Launch Receipts after rollback refusal = %d error=%v", receipts, err)
		}
	})
}

func TestCurrentCatalogPromoterFailsClosedAgainstSchema31(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 31); err != nil {
		t.Fatalf("contract Catalog Promotion schema before current promoter probe: %v", err)
	}
	promotion := newRolePool(
		t,
		database.DSN,
		"vela_catalog_promotion_login",
		"vela-catalog-promotion-password",
	)
	_, err := catalogpromotion.New(context.Background(), promotion)
	if err == nil || !strings.Contains(err.Error(), "Catalog Promotion transaction privilege boundary") {
		t.Fatalf("current Catalog Promoter schema 31 error = %v, want privilege-boundary refusal", err)
	}
}

const (
	testInferenceBackendRevisionID = "35000000-0000-0000-0000-000000000001"
)

func prepareCatalogTransition(
	t *testing.T,
) (testDatabase, *pgxpool.Pool, uuid.UUID) {
	t.Helper()
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedThreePresetCatalog(t, database)
	promotion := newRolePool(
		t,
		database.DSN,
		"vela_catalog_promotion_login",
		"vela-catalog-promotion-password",
	)
	receipts := recordAndSealLaunchManifest(t, promotion)
	receiptID := receipts[productiongates.GatePresetCertification]
	promoteSeededCatalog(t, promotion, receiptID)
	return database, promotion, receiptID
}

func seedThreePresetCatalog(t *testing.T, database testDatabase) {
	t.Helper()
	transaction, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin three-Preset Catalog fixture: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	if _, err := transaction.Exec(`
		INSERT INTO inference_backend_revisions (
			id, stable_id, revision, state, content_digest
		) VALUES ($1, 'sglang-vela', 1, 'CANARY', decode(repeat('35', 32), 'hex'))
	`, uuid.MustParse(testInferenceBackendRevisionID)); err != nil {
		t.Fatalf("seed InferenceBackendRevision: %v", err)
	}
	if _, err := transaction.Exec(`
		INSERT INTO generation_preset_revisions (
			id, model_revision_id, stable_id, revision, state,
			certified_p95_compute_seconds
		) VALUES
			('35000000-0000-0000-0000-000000000011', $1, 'quality', 1, 'CANARY', 2400),
			('35000000-0000-0000-0000-000000000021', $1, 'fast', 1, 'CANARY', 900)
	`, uuid.MustParse("00000000-0000-0000-0000-000000000010")); err != nil {
		t.Fatalf("seed GenerationPresetRevisions: %v", err)
	}
	if _, err := transaction.Exec(`
		INSERT INTO execution_profile_revisions (
			id, model_revision_id, worker_pool_id, stable_id, revision, state
		) VALUES
			('35000000-0000-0000-0000-000000000012', $1, $2, 'h3-quality', 1, 'CANARY'),
			('35000000-0000-0000-0000-000000000022', $1, $2, 'h3-fast', 1, 'CANARY')
	`,
		uuid.MustParse("00000000-0000-0000-0000-000000000010"),
		uuid.MustParse("00000000-0000-0000-0000-000000000005"),
	); err != nil {
		t.Fatalf("seed ExecutionProfileRevisions: %v", err)
	}
	if _, err := transaction.Exec(`
		INSERT INTO profile_certifications (
			id, model_revision_id, generation_preset_revision_id, output_spec_id,
			execution_profile_revision_id, state, evidence_digest, certified_at
		) VALUES
			('35000000-0000-0000-0000-000000000014', $1,
			 '35000000-0000-0000-0000-000000000011', $2,
			 '35000000-0000-0000-0000-000000000012', 'CANARY', repeat('a', 32),
			 clock_timestamp()),
			('35000000-0000-0000-0000-000000000024', $1,
			 '35000000-0000-0000-0000-000000000021', $2,
			 '35000000-0000-0000-0000-000000000022', 'CANARY', repeat('b', 32),
			 clock_timestamp())
	`,
		uuid.MustParse("00000000-0000-0000-0000-000000000010"),
		uuid.MustParse("00000000-0000-0000-0000-000000000013"),
	); err != nil {
		t.Fatalf("seed ProfileCertifications: %v", err)
	}
	if _, err := transaction.Exec(`
		INSERT INTO rate_card_lines (
			id, rate_card_revision_id, model_revision_id,
			generation_preset_revision_id, service_class_revision_id,
			output_spec_id, unit_amount_minor, currency
		) VALUES
			('35000000-0000-0000-0000-000000000016', $1, $2,
			 '35000000-0000-0000-0000-000000000011', $3, $4, 2000, 'CNY'),
			('35000000-0000-0000-0000-000000000026', $1, $2,
			 '35000000-0000-0000-0000-000000000021', $3, $4, 800, 'CNY')
	`,
		uuid.MustParse("00000000-0000-0000-0000-000000000016"),
		uuid.MustParse("00000000-0000-0000-0000-000000000010"),
		uuid.MustParse("00000000-0000-0000-0000-000000000012"),
		uuid.MustParse("00000000-0000-0000-0000-000000000013"),
	); err != nil {
		t.Fatalf("seed RateCard lines: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit three-Preset Catalog fixture: %v", err)
	}
}

func seedSubsequentCatalogRelease(t *testing.T, database testDatabase) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		INSERT INTO execution_profile_revisions (
			id, model_revision_id, worker_pool_id, stable_id, revision, state
		) VALUES
			('36000000-0000-0000-0000-000000000012',
			 '00000000-0000-0000-0000-000000000010',
			 '00000000-0000-0000-0000-000000000005', 'h3-balanced-release-2', 1, 'CANARY'),
			('36000000-0000-0000-0000-000000000022',
			 '00000000-0000-0000-0000-000000000010',
			 '00000000-0000-0000-0000-000000000005', 'h3-quality-release-2', 1, 'CANARY'),
			('36000000-0000-0000-0000-000000000032',
			 '00000000-0000-0000-0000-000000000010',
			 '00000000-0000-0000-0000-000000000005', 'h3-fast-release-2', 1, 'CANARY');

		INSERT INTO profile_certifications (
			id, model_revision_id, generation_preset_revision_id, output_spec_id,
			execution_profile_revision_id, state, evidence_digest, certified_at
		) VALUES
			('36000000-0000-0000-0000-000000000014',
			 '00000000-0000-0000-0000-000000000010',
			 '00000000-0000-0000-0000-000000000011',
			 '00000000-0000-0000-0000-000000000013',
			 '36000000-0000-0000-0000-000000000012', 'CANARY', repeat('c', 32),
			 clock_timestamp()),
			('36000000-0000-0000-0000-000000000024',
			 '00000000-0000-0000-0000-000000000010',
			 '35000000-0000-0000-0000-000000000011',
			 '00000000-0000-0000-0000-000000000013',
			 '36000000-0000-0000-0000-000000000022', 'CANARY', repeat('d', 32),
			 clock_timestamp()),
			('36000000-0000-0000-0000-000000000034',
			 '00000000-0000-0000-0000-000000000010',
			 '35000000-0000-0000-0000-000000000021',
			 '00000000-0000-0000-0000-000000000013',
			 '36000000-0000-0000-0000-000000000032', 'CANARY', repeat('e', 32),
			 clock_timestamp());

		INSERT INTO rate_card_revisions (id, revision, state, effective_at)
		VALUES (
			'36000000-0000-0000-0000-000000000016', 2, 'CANARY',
			clock_timestamp()
		);
		INSERT INTO rate_card_lines (
			id, rate_card_revision_id, model_revision_id,
			generation_preset_revision_id, service_class_revision_id,
			output_spec_id, unit_amount_minor, currency
		) VALUES
			('36000000-0000-0000-0000-000000000017',
			 '36000000-0000-0000-0000-000000000016',
			 '00000000-0000-0000-0000-000000000010',
			 '00000000-0000-0000-0000-000000000011',
			 '00000000-0000-0000-0000-000000000012',
			 '00000000-0000-0000-0000-000000000013', 1300, 'CNY'),
			('36000000-0000-0000-0000-000000000027',
			 '36000000-0000-0000-0000-000000000016',
			 '00000000-0000-0000-0000-000000000010',
			 '35000000-0000-0000-0000-000000000011',
			 '00000000-0000-0000-0000-000000000012',
			 '00000000-0000-0000-0000-000000000013', 2050, 'CNY'),
			('36000000-0000-0000-0000-000000000037',
			 '36000000-0000-0000-0000-000000000016',
			 '00000000-0000-0000-0000-000000000010',
			 '35000000-0000-0000-0000-000000000021',
			 '00000000-0000-0000-0000-000000000012',
			 '00000000-0000-0000-0000-000000000013', 850, 'CNY');
	`); err != nil {
		t.Fatalf("seed subsequent Catalog release: %v", err)
	}
}

func seedDuplicateQualityPreset(t *testing.T, database testDatabase) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		INSERT INTO generation_preset_revisions (
			id, model_revision_id, stable_id, revision, state,
			certified_p95_compute_seconds
		) VALUES (
			'37000000-0000-0000-0000-000000000011',
			'00000000-0000-0000-0000-000000000010',
			'quality', 2, 'CANARY', 2300
		);
		INSERT INTO execution_profile_revisions (
			id, model_revision_id, worker_pool_id, stable_id, revision, state
		) VALUES (
			'37000000-0000-0000-0000-000000000012',
			'00000000-0000-0000-0000-000000000010',
			'00000000-0000-0000-0000-000000000005',
			'h3-quality-duplicate', 1, 'CANARY'
		);
		INSERT INTO profile_certifications (
			id, model_revision_id, generation_preset_revision_id, output_spec_id,
			execution_profile_revision_id, state, evidence_digest, certified_at
		) VALUES (
			'37000000-0000-0000-0000-000000000014',
			'00000000-0000-0000-0000-000000000010',
			'37000000-0000-0000-0000-000000000011',
			'00000000-0000-0000-0000-000000000013',
			'37000000-0000-0000-0000-000000000012',
			'CANARY', repeat('f', 32), clock_timestamp()
		);
		INSERT INTO rate_card_lines (
			id, rate_card_revision_id, model_revision_id,
			generation_preset_revision_id, service_class_revision_id,
			output_spec_id, unit_amount_minor, currency
		) VALUES (
			'37000000-0000-0000-0000-000000000016',
			'00000000-0000-0000-0000-000000000016',
			'00000000-0000-0000-0000-000000000010',
			'37000000-0000-0000-0000-000000000011',
			'00000000-0000-0000-0000-000000000012',
			'00000000-0000-0000-0000-000000000013', 2100, 'CNY'
		);
	`); err != nil {
		t.Fatalf("seed duplicate quality Preset revision: %v", err)
	}
}

func recordAndSealLaunchManifest(
	t *testing.T,
	pool *pgxpool.Pool,
) map[productiongates.Gate]uuid.UUID {
	t.Helper()
	return recordAndSealLaunchManifestForRelease(t, pool, 1)
}

func recordAndSealLaunchManifestForRelease(
	t *testing.T,
	pool *pgxpool.Pool,
	release int,
) map[productiongates.Gate]uuid.UUID {
	t.Helper()
	manifestDigest := sha256.Sum256([]byte(fmt.Sprintf("release-manifest-%d", release)))
	releaseDigest := sha256.Sum256([]byte(fmt.Sprintf("release-image-%d", release)))
	evidenceDigest := sha256.Sum256([]byte(fmt.Sprintf("production-evidence-%d", release)))
	receipts := make(map[productiongates.Gate]uuid.UUID, len(productiongates.AllGates()))
	transaction, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin Launch Receipt transaction: %v", err)
	}
	defer func() { _ = transaction.Rollback(context.Background()) }()
	for index, gate := range productiongates.AllGates() {
		receiptID := uuid.MustParse(fmt.Sprintf(
			"35000000-0000-0000-0000-%012d",
			release*1000+301+index,
		))
		receipts[gate] = receiptID
		startedAt := time.Date(2026, 8, 28, 1+release, index, 0, 0, time.UTC)
		var returnedID uuid.UUID
		var replayed bool
		if err := transaction.QueryRow(context.Background(), `
			SELECT receipt_id, replayed
			FROM vela_record_production_gate_receipt(
				$1, 1, $2, $3, $4, 'h3-validation-rack-1',
				'PASS', 'platform-oncall@example.invalid',
				'all gate assertions pass', 'all gate assertions passed',
				$5, $6, $7, $8, $9, $10
			)
		`,
			receiptID,
			string(gate),
			releaseDigest[:],
			fmt.Sprintf("release-config-%d", release),
			"evidence/"+string(gate)+".json",
			evidenceDigest[:],
			manifestDigest[:],
			startedAt,
			startedAt.Add(time.Minute),
			startedAt.Add(2*time.Minute),
		).Scan(&returnedID, &replayed); err != nil {
			t.Fatalf("record %s Launch Receipt: %v", gate, err)
		}
		if returnedID != receiptID || replayed {
			t.Fatalf("record %s Launch Receipt = id %s replayed %t", gate, returnedID, replayed)
		}
	}
	var sealed bool
	var receiptCount int
	if err := transaction.QueryRow(context.Background(), `
		SELECT sealed, receipt_count FROM vela_seal_production_gate_manifest($1)
	`, manifestDigest[:]).Scan(&sealed, &receiptCount); err != nil {
		t.Fatalf("seal Launch Receipt manifest: %v", err)
	}
	if !sealed || receiptCount != len(productiongates.AllGates()) {
		t.Fatalf("sealed manifest = sealed %t receipt count %d", sealed, receiptCount)
	}
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatalf("commit Launch Receipt manifest: %v", err)
	}
	return receipts
}

func promoteSeededCatalog(t *testing.T, pool *pgxpool.Pool, receiptID uuid.UUID) {
	t.Helper()
	for index, certificationID := range []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000015"),
		uuid.MustParse("35000000-0000-0000-0000-000000000014"),
		uuid.MustParse("35000000-0000-0000-0000-000000000024"),
	} {
		promoteProfileCertification(
			t,
			pool,
			uuid.MustParse(fmt.Sprintf("35000000-0000-0000-0000-%012d", 100+index)),
			certificationID,
			receiptID,
		)
	}
	promoteRateCardRevision(
		t,
		pool,
		uuid.MustParse("35000000-0000-0000-0000-000000000201"),
		uuid.MustParse("00000000-0000-0000-0000-000000000016"),
		receiptID,
	)
}

func promoteRateCardRevision(
	t *testing.T,
	pool *pgxpool.Pool,
	bindingID uuid.UUID,
	rateCardID uuid.UUID,
	receiptID uuid.UUID,
) {
	t.Helper()
	var returnedID uuid.UUID
	var replayed bool
	var state string
	if err := pool.QueryRow(context.Background(), `
		SELECT binding_id, replayed, rate_card_state::text
		FROM vela_promote_rate_card($1, $2, $3)
	`, bindingID, rateCardID, receiptID).Scan(&returnedID, &replayed, &state); err != nil {
		t.Fatalf("promote RateCardRevision %s: %v", rateCardID, err)
	}
	if returnedID != bindingID || replayed || state != "ACTIVE" {
		t.Fatalf(
			"promote RateCardRevision %s = binding %s replayed %t state %q",
			rateCardID, returnedID, replayed, state,
		)
	}
}

func promoteProfileCertification(
	t *testing.T,
	pool *pgxpool.Pool,
	evidenceID uuid.UUID,
	certificationID uuid.UUID,
	receiptID uuid.UUID,
) {
	t.Helper()
	var returnedID uuid.UUID
	var replayed bool
	var state string
	if err := pool.QueryRow(context.Background(), `
		SELECT evidence_id, replayed, certification_state::text
		FROM vela_promote_profile_certification(
			$1, $2, $3, 'h3-8gpu-driver-r1', 'h3-video-quality-v2',
			820000, 850000, 990000, 999000,
			900000, 1800000, 1700000,
			500000, 450000, 'CNY', 950000, 990000, $4
		)
	`,
		evidenceID,
		certificationID,
		uuid.MustParse(testInferenceBackendRevisionID),
		receiptID,
	).Scan(&returnedID, &replayed, &state); err != nil {
		t.Fatalf("promote ProfileCertification %s: %v", certificationID, err)
	}
	if returnedID != evidenceID || replayed || state != "ACTIVE" {
		t.Fatalf(
			"promote ProfileCertification %s = evidence %s replayed %t state %q",
			certificationID, returnedID, replayed, state,
		)
	}
}

func assertCatalogDatabaseError(
	t *testing.T,
	err error,
	code string,
	constraint string,
	label string,
) {
	t.Helper()
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != code ||
		(constraint != "" && postgresError.ConstraintName != constraint) {
		t.Fatalf("%s error = %v, want SQLSTATE %s constraint %s", label, err, code, constraint)
	}
}

func writeCatalogPromotionFiles(t *testing.T, invalidCertification bool) string {
	t.Helper()
	directory := t.TempDir()
	evidence := []byte(`{"result":"real-environment evidence fixture"}`)
	evidenceDigest := sha256.Sum256(evidence)
	manifest := productiongates.Manifest{
		SchemaVersion: 1,
		Receipts:      make([]productiongates.Receipt, 0, len(productiongates.AllGates())),
	}
	releaseDigest := sha256.Sum256([]byte("catalog-promotion-release"))
	for index, gate := range productiongates.AllGates() {
		evidenceRef := filepath.ToSlash(filepath.Join("evidence", string(gate)+".json"))
		evidencePath := filepath.Join(directory, filepath.FromSlash(evidenceRef))
		if err := os.MkdirAll(filepath.Dir(evidencePath), 0o700); err != nil {
			t.Fatalf("create Catalog evidence directory: %v", err)
		}
		if err := os.WriteFile(evidencePath, evidence, 0o600); err != nil {
			t.Fatalf("write Catalog evidence: %v", err)
		}
		startedAt := time.Date(2026, 8, 28, 3, index, 0, 0, time.UTC)
		manifest.Receipts = append(manifest.Receipts, productiongates.Receipt{
			SchemaVersion:         1,
			Gate:                  gate,
			ReleaseDigest:         "sha256:" + hex.EncodeToString(releaseDigest[:]),
			ConfigurationRevision: "catalog-config-1",
			ValidationEnvironment: "h3-validation-rack-1",
			Result:                productiongates.ResultPass,
			Owner:                 "platform-oncall@example.invalid",
			AcceptanceThreshold:   "all gate assertions pass",
			ObservedResult:        "all gate assertions passed",
			EvidenceRef:           evidenceRef,
			EvidenceDigest:        "sha256:" + hex.EncodeToString(evidenceDigest[:]),
			StartedAt:             startedAt,
			CompletedAt:           startedAt.Add(time.Minute),
			RecordedAt:            startedAt.Add(2 * time.Minute),
		})
	}
	writeJSONFixture(t, filepath.Join(directory, "launch-receipts.json"), manifest)

	certificationIDs := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000015"),
		uuid.MustParse("35000000-0000-0000-0000-000000000014"),
		uuid.MustParse("35000000-0000-0000-0000-000000000024"),
	}
	if invalidCertification {
		certificationIDs[1] = uuid.MustParse("35000000-0000-0000-0000-000000000999")
	}
	certifications := make([]catalogpromotion.CertificationPromotion, 0, len(certificationIDs))
	for index, certificationID := range certificationIDs {
		certifications = append(certifications, catalogpromotion.CertificationPromotion{
			EvidenceID:                 uuid.MustParse(fmt.Sprintf("35000000-0000-0000-0000-%012d", 401+index)),
			ProfileCertificationID:     certificationID,
			InferenceBackendRevisionID: uuid.MustParse(testInferenceBackendRevisionID),
			HardwareDriverBaseline:     "h3-8gpu-driver-r1",
			BenchmarkCorpusRevision:    "h3-video-quality-v2",
			QualityThresholdPPM:        820000,
			QualityObservedPPM:         850000,
			SuccessRateThresholdPPM:    990000,
			SuccessRateObservedPPM:     999000,
			P50Milliseconds:            900000,
			P95ThresholdMilliseconds:   1800000,
			P95ObservedMilliseconds:    1700000,
			CostThresholdMinor:         500000,
			CostObservedMinor:          450000,
			CostCurrency:               "CNY",
			ConfidenceThresholdPPM:     950000,
			ConfidenceObservedPPM:      990000,
		})
	}
	plan := catalogpromotion.Plan{
		SchemaVersion:  1,
		ManifestRef:    "launch-receipts.json",
		Certifications: certifications,
		RateCards: []catalogpromotion.RateCardPromotion{{
			BindingID:          uuid.MustParse("35000000-0000-0000-0000-000000000501"),
			RateCardRevisionID: uuid.MustParse("00000000-0000-0000-0000-000000000016"),
		}},
		EnableEvidenced: true,
	}
	planPath := filepath.Join(directory, "catalog-promotion.json")
	writeJSONFixture(t, planPath, plan)
	return planPath
}

func writeJSONFixture(t *testing.T, path string, value any) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode %s fixture: %v", filepath.Base(path), err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write %s fixture: %v", filepath.Base(path), err)
	}
}

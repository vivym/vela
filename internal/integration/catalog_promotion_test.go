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
	"github.com/vivym/vela/internal/slo"
	"github.com/vivym/vela/internal/sloevidence"
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
	service := newCatalogPromotionService(t, promotionPool)
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

func TestCatalogPromotionLoadsNestedReleaseBundle(t *testing.T) {
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
	service := newCatalogPromotionService(t, promotionPool)
	planPath := writeCatalogPromotionFiles(t, false)
	plan, err := catalogpromotion.LoadPlan(planPath)
	if err != nil {
		t.Fatalf("load Catalog Promotion plan: %v", err)
	}
	plan.ReleaseBundleRef = relocateCatalogReleaseBundleFixture(
		t,
		filepath.Dir(planPath),
		plan.ReleaseBundleRef,
		"nested/release",
	)
	writeJSONFixture(t, planPath, plan)
	result, err := service.Apply(context.Background(), planPath)
	if err != nil || !strings.HasPrefix(result.ReleaseDigest, "sha256:") {
		t.Fatalf("apply nested Catalog Promotion plan = %#v error=%v", result, err)
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
	service := newCatalogPromotionService(t, promotionPool)
	_, err := service.Apply(context.Background(), writeCatalogPromotionFiles(t, true))
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

func TestCatalogPromotionRejectsPlanEvidenceMismatchBeforeDatabaseMutation(t *testing.T) {
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
	service := newCatalogPromotionService(t, promotionPool)
	for _, test := range []struct {
		name   string
		mutate func(*catalogpromotion.Plan)
	}{
		{
			name: "certification metric",
			mutate: func(plan *catalogpromotion.Plan) {
				plan.Certifications[0].QualityObservedPPM++
			},
		},
		{
			name: "RateCard binding ID",
			mutate: func(plan *catalogpromotion.Plan) {
				plan.RateCards[0].BindingID = uuid.MustParse("35000000-0000-0000-0000-000000000599")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			planPath := writeCatalogPromotionFiles(t, false)
			plan, err := catalogpromotion.LoadPlan(planPath)
			if err != nil {
				t.Fatalf("load Catalog promotion fixture: %v", err)
			}
			test.mutate(&plan)
			writeJSONFixture(t, planPath, plan)
			if _, err := service.Apply(context.Background(), planPath); err == nil ||
				!strings.Contains(err.Error(), "does not match verified preset evidence") {
				t.Fatalf("mismatched Catalog promotion error = %v", err)
			}
			var manifests, receipts, evidence, bindings int64
			if err := database.Admin.QueryRow(`
				SELECT
					(SELECT count(*) FROM production_gate_manifests),
					(SELECT count(*) FROM production_gate_receipts),
					(SELECT count(*) FROM profile_certification_evidence),
					(SELECT count(*) FROM rate_card_release_bindings)
			`).Scan(&manifests, &receipts, &evidence, &bindings); err != nil {
				t.Fatalf("read pre-transaction Catalog rows: %v", err)
			}
			if manifests != 0 || receipts != 0 || evidence != 0 || bindings != 0 {
				t.Fatalf(
					"mismatched Catalog plan left rows %d/%d/%d/%d",
					manifests,
					receipts,
					evidence,
					bindings,
				)
			}
		})
	}
}

func TestCatalogPromotionRejectsInvalidOrMismatchedReleaseBundleBeforeDatabaseMutation(t *testing.T) {
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
	service := newCatalogPromotionService(t, promotionPool)
	for _, test := range []struct {
		name       string
		mutate     func(*testing.T, string)
		errorMatch string
	}{
		{
			name: "missing bundle",
			mutate: func(t *testing.T, directory string) {
				t.Helper()
				if err := os.Remove(filepath.Join(directory, "release-bundle.json")); err != nil {
					t.Fatalf("remove release bundle fixture: %v", err)
				}
			},
			errorMatch: "load release bundle",
		},
		{
			name: "invalid bundle",
			mutate: func(t *testing.T, directory string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(directory, "release-bundle.json"), []byte("{}"), 0o600); err != nil {
					t.Fatalf("write invalid release bundle fixture: %v", err)
				}
			},
			errorMatch: "load release bundle",
		},
		{
			name: "mismatched valid bundle",
			mutate: func(t *testing.T, directory string) {
				t.Helper()
				writeCatalogReleaseBundleFixture(t, directory, "-different-release")
			},
			errorMatch: "supply-chain manifest does not bind the release bundle",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			planPath := writeCatalogPromotionFiles(t, false)
			test.mutate(t, filepath.Dir(planPath))
			if _, err := service.Apply(context.Background(), planPath); err == nil ||
				!strings.Contains(err.Error(), test.errorMatch) {
				t.Fatalf("Catalog promotion error = %v, want %q", err, test.errorMatch)
			}
			var manifests, receipts, evidence, bindings int64
			if err := database.Admin.QueryRow(`
				SELECT
					(SELECT count(*) FROM production_gate_manifests),
					(SELECT count(*) FROM production_gate_receipts),
					(SELECT count(*) FROM profile_certification_evidence),
					(SELECT count(*) FROM rate_card_release_bindings)
			`).Scan(&manifests, &receipts, &evidence, &bindings); err != nil {
				t.Fatalf("read pre-transaction Catalog rows: %v", err)
			}
			if manifests != 0 || receipts != 0 || evidence != 0 || bindings != 0 {
				t.Fatalf(
					"failed Catalog release binding left rows %d/%d/%d/%d",
					manifests,
					receipts,
					evidence,
					bindings,
				)
			}
		})
	}
}

func TestCatalogPromotionRejectsInvalidSupplyChainBeforeDatabaseMutation(t *testing.T) {
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
	service := newCatalogPromotionService(t, promotionPool)
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{
			name: "missing manifest",
			mutate: func(t *testing.T, directory string) {
				t.Helper()
				if err := os.Remove(filepath.Join(directory, "supply-chain", "manifest.json")); err != nil {
					t.Fatalf("remove supply-chain manifest fixture: %v", err)
				}
			},
		},
		{
			name: "tampered SBOM",
			mutate: func(t *testing.T, directory string) {
				t.Helper()
				path := filepath.Join(directory, "supply-chain", "image-0.spdx.json")
				encoded, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("read supply-chain SBOM fixture: %v", err)
				}
				if err := os.WriteFile(path, append(encoded, '\n'), 0o600); err != nil {
					t.Fatalf("tamper supply-chain SBOM fixture: %v", err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			planPath := writeCatalogPromotionFiles(t, false)
			test.mutate(t, filepath.Dir(planPath))
			if _, err := service.Apply(context.Background(), planPath); err == nil ||
				!strings.Contains(err.Error(), "load release supply-chain evidence") {
				t.Fatalf("Catalog promotion supply-chain error = %v", err)
			}
			var manifests, receipts, evidence, bindings int64
			if err := database.Admin.QueryRow(`
				SELECT
					(SELECT count(*) FROM production_gate_manifests),
					(SELECT count(*) FROM production_gate_receipts),
					(SELECT count(*) FROM profile_certification_evidence),
					(SELECT count(*) FROM rate_card_release_bindings)
			`).Scan(&manifests, &receipts, &evidence, &bindings); err != nil {
				t.Fatalf("read pre-transaction Catalog rows: %v", err)
			}
			if manifests != 0 || receipts != 0 || evidence != 0 || bindings != 0 {
				t.Fatalf(
					"failed Catalog supply chain left rows %d/%d/%d/%d",
					manifests,
					receipts,
					evidence,
					bindings,
				)
			}
		})
	}
}

func TestCatalogPromotionRejectsUnpinnedTrustPolicyBeforeDatabaseMutation(t *testing.T) {
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
	policy := writeCatalogSupplyChainPolicySource(t)
	policy.Digest = catalogReleaseDigest("different-policy")
	service, err := catalogpromotion.New(context.Background(), promotionPool, policy)
	if err != nil {
		t.Fatalf("configure Catalog Promotion service: %v", err)
	}
	if _, err := service.Apply(context.Background(), writeCatalogPromotionFiles(t, false)); err == nil ||
		!strings.Contains(err.Error(), "trust policy digest mismatch") {
		t.Fatalf("Catalog promotion unpinned policy error = %v", err)
	}
	var manifests, receipts, evidence, bindings int64
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM production_gate_manifests),
			(SELECT count(*) FROM production_gate_receipts),
			(SELECT count(*) FROM profile_certification_evidence),
			(SELECT count(*) FROM rate_card_release_bindings)
	`).Scan(&manifests, &receipts, &evidence, &bindings); err != nil {
		t.Fatalf("read pre-transaction Catalog rows: %v", err)
	}
	if manifests != 0 || receipts != 0 || evidence != 0 || bindings != 0 {
		t.Fatalf(
			"unpinned Catalog trust policy left rows %d/%d/%d/%d",
			manifests,
			receipts,
			evidence,
			bindings,
		)
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
	_, err := catalogpromotion.New(
		context.Background(),
		promotion,
		writeCatalogSupplyChainPolicySource(t),
	)
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
	bundle := writeCatalogReleaseBundleFixture(t, directory, "")
	writeCatalogSupplyChainFixture(t, directory, bundle)
	manifest := productiongates.Manifest{
		SchemaVersion: 1,
		Receipts:      make([]productiongates.Receipt, 0, len(productiongates.AllGates())),
	}
	certificationIDs := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000015"),
		uuid.MustParse("35000000-0000-0000-0000-000000000014"),
		uuid.MustParse("35000000-0000-0000-0000-000000000024"),
	}
	if invalidCertification {
		certificationIDs[1] = uuid.MustParse("35000000-0000-0000-0000-000000000999")
	}
	for index, gate := range productiongates.AllGates() {
		startedAt := time.Date(2026, 8, 28, 3, index, 0, 0, time.UTC)
		completedAt := startedAt.Add(time.Minute)
		if gate == productiongates.GateRealH3Soak {
			completedAt = startedAt.Add(72 * time.Hour)
		}
		acceptanceThreshold := "all gate assertions pass"
		observedResult := "all gate assertions passed"
		var gateEvidence []byte
		if gate == productiongates.GateObservabilityOnCall {
			gateEvidence = catalogObservabilityEvidenceFixture(
				t,
				directory,
				bundle.ReleaseDigest,
				bundle.ConfigurationRevision,
			)
		} else {
			typed := catalogTypedEvidenceFixture(
				t,
				directory,
				gate,
				bundle.ReleaseDigest,
				bundle.ConfigurationRevision,
				startedAt,
				completedAt,
				certificationIDs,
			)
			acceptanceThreshold = typed.AcceptanceThreshold()
			observedResult = typed.ObservedResult()
			var err error
			gateEvidence, err = json.Marshal(typed)
			if err != nil {
				t.Fatalf("encode %s Catalog evidence: %v", gate, err)
			}
		}
		evidenceDigest := sha256.Sum256(gateEvidence)
		evidenceRef := filepath.ToSlash(filepath.Join("evidence", string(gate)+".json"))
		evidencePath := filepath.Join(directory, filepath.FromSlash(evidenceRef))
		if err := os.MkdirAll(filepath.Dir(evidencePath), 0o700); err != nil {
			t.Fatalf("create Catalog evidence directory: %v", err)
		}
		if err := os.WriteFile(evidencePath, gateEvidence, 0o600); err != nil {
			t.Fatalf("write Catalog evidence: %v", err)
		}
		manifest.Receipts = append(manifest.Receipts, productiongates.Receipt{
			SchemaVersion:         1,
			Gate:                  gate,
			ReleaseDigest:         bundle.ReleaseDigest,
			ConfigurationRevision: bundle.ConfigurationRevision,
			ValidationEnvironment: "h3-validation-rack-1",
			Result:                productiongates.ResultPass,
			Owner:                 "platform-oncall@example.invalid",
			AcceptanceThreshold:   acceptanceThreshold,
			ObservedResult:        observedResult,
			EvidenceRef:           evidenceRef,
			EvidenceDigest:        "sha256:" + hex.EncodeToString(evidenceDigest[:]),
			StartedAt:             startedAt,
			CompletedAt:           completedAt,
			RecordedAt:            completedAt.Add(time.Minute),
		})
	}
	writeJSONFixture(t, filepath.Join(directory, "launch-receipts.json"), manifest)

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
		SchemaVersion:          2,
		ManifestRef:            "launch-receipts.json",
		ReleaseBundleRef:       "release-bundle.json",
		SupplyChainManifestRef: "supply-chain/manifest.json",
		Certifications:         certifications,
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

func catalogTypedEvidenceFixture(
	t *testing.T,
	directory string,
	gate productiongates.Gate,
	releaseDigest string,
	configurationRevision string,
	startedAt,
	completedAt time.Time,
	certificationIDs []uuid.UUID,
) productiongates.TypedEvidence {
	t.Helper()
	contract, ok := productiongates.TypedEvidenceContractForGate(gate)
	if !ok {
		t.Fatalf("typed evidence contract missing for %s", gate)
	}
	evidence := productiongates.TypedEvidence{
		SchemaVersion: 1, Gate: gate, CriteriaRevision: contract.CriteriaRevision,
		ReleaseDigest: releaseDigest, ConfigurationRevision: configurationRevision,
		ValidationEnvironment: "h3-validation-rack-1",
		Owner:                 "platform-oncall@example.invalid",
		StartedAt:             startedAt, CompletedAt: completedAt,
		Checks:       make([]productiongates.EvidenceCheck, 0, len(contract.CheckIDs)),
		Measurements: make([]productiongates.EvidenceMeasurement, 0, len(contract.Measurements)),
		Artifacts:    make([]productiongates.EvidenceArtifact, 0, len(contract.ArtifactKinds)),
	}
	for _, id := range contract.CheckIDs {
		evidence.Checks = append(evidence.Checks, productiongates.EvidenceCheck{ID: id, Passed: true})
	}
	for _, requirement := range contract.Measurements {
		evidence.Measurements = append(evidence.Measurements, productiongates.EvidenceMeasurement{
			ID: requirement.ID, Unit: requirement.Unit,
			Comparator: requirement.Comparator, Threshold: requirement.Threshold,
			Observed: requirement.Threshold,
		})
	}
	if gate == productiongates.GatePresetCertification {
		claims := make([]productiongates.PresetCertificationClaim, 0, len(certificationIDs))
		for index, preset := range []string{"quality", "balanced", "fast"} {
			claims = append(claims, productiongates.PresetCertificationClaim{
				EvidenceID:                 uuid.MustParse(fmt.Sprintf("35000000-0000-0000-0000-%012d", 401+index)).String(),
				ProfileCertificationID:     certificationIDs[index].String(),
				InferenceBackendRevisionID: uuid.MustParse(testInferenceBackendRevisionID).String(),
				SaleableGroupID:            "model-v1/standard-v1/1080p-v1/CNY",
				StablePreset:               preset,
				HardwareDriverBaseline:     "h3-8gpu-driver-r1",
				BenchmarkCorpusRevision:    "h3-video-quality-v2",
				SampleCount:                100,
				QualityThresholdPPM:        820000, QualityObservedPPM: 850000,
				SuccessRateThresholdPPM: 990000, SuccessRateObservedPPM: 999000,
				P50Milliseconds: 900000, P95ThresholdMilliseconds: 1800000,
				P95ObservedMilliseconds: 1700000,
				CostThresholdMinor:      500000, CostObservedMinor: 450000, CostCurrency: "CNY",
				ConfidenceThresholdPPM: 950000, ConfidenceObservedPPM: 990000,
			})
		}
		evidence.PresetCertification = &productiongates.PresetCertificationClaims{
			SaleableGroupIDs: []string{"model-v1/standard-v1/1080p-v1/CNY"},
			Certifications:   claims,
			RateCards: []productiongates.RateCardPromotionClaim{{
				BindingID: uuid.MustParse("35000000-0000-0000-0000-000000000501").String(),
				RateCardRevisionID: uuid.MustParse(
					"00000000-0000-0000-0000-000000000016",
				).String(),
			}},
		}
	}
	catalogWriteTypedEvidenceArtifacts(t, directory, &evidence, contract.ArtifactKinds)
	return evidence
}

func catalogWriteTypedEvidenceArtifacts(
	t *testing.T,
	directory string,
	evidence *productiongates.TypedEvidence,
	kinds []string,
) {
	t.Helper()
	payloads := make([]productiongates.TypedEvidenceArtifact, len(kinds))
	for index, kind := range kinds {
		payloads[index] = productiongates.TypedEvidenceArtifact{
			SchemaVersion: 1, Gate: evidence.Gate, Kind: kind,
			ReleaseDigest: evidence.ReleaseDigest, ConfigurationRevision: evidence.ConfigurationRevision,
			ValidationEnvironment: evidence.ValidationEnvironment, Owner: evidence.Owner,
			StartedAt: evidence.StartedAt, CompletedAt: evidence.CompletedAt,
			Checks:       make([]productiongates.EvidenceCheck, 0),
			Measurements: make([]productiongates.EvidenceMeasurement, 0),
		}
	}
	for index, check := range evidence.Checks {
		payloads[index%len(payloads)].Checks = append(payloads[index%len(payloads)].Checks, check)
	}
	for index, measurement := range evidence.Measurements {
		payloads[index%len(payloads)].Measurements = append(
			payloads[index%len(payloads)].Measurements,
			measurement,
		)
	}
	for index := range payloads {
		if evidence.Gate == productiongates.GatePresetCertification {
			payloads[index].PresetCertification = evidence.PresetCertification
		}
		content, err := json.Marshal(payloads[index])
		if err != nil {
			t.Fatalf("encode Catalog %s/%s artifact: %v", evidence.Gate, payloads[index].Kind, err)
		}
		ref := filepath.ToSlash(filepath.Join("typed", string(evidence.Gate), payloads[index].Kind+".json"))
		path := filepath.Join(directory, filepath.FromSlash(ref))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("create Catalog typed artifact directory: %v", err)
		}
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("write Catalog typed artifact: %v", err)
		}
		evidence.Artifacts = append(evidence.Artifacts, productiongates.EvidenceArtifact{
			Kind: payloads[index].Kind, Ref: ref, Digest: sloevidence.Digest(content),
		})
	}
}

func catalogObservabilityEvidenceFixture(
	t *testing.T,
	directory,
	releaseDigest,
	configurationRevision string,
) []byte {
	t.Helper()
	window := slo.Window{
		Start: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
	}
	evaluatedAt := window.End.Add(time.Hour)
	cohorts := make([]sloevidence.CohortEvidence, 0, 3)
	contractIDs := make([]string, 0, 3)
	for _, preset := range []string{"quality", "balanced", "fast"} {
		contract := slo.Contract{
			RevisionID: "catalog-slo-" + preset + "-v1", ModelRevisionID: "model-v1",
			GenerationPreset: preset, GenerationPresetRevisionID: preset + "-v1",
			ServiceClassRevisionID: "standard-v1", OutputSpecID: "1080p-v1",
			GenerationCount: 1, P95TargetMilliseconds: 60_000,
			SuccessTargetPPM: 800_000, MinimumSample: 20,
			ConfidenceMethod:      slo.ConfidenceMethod,
			OneSidedConfidencePPM: slo.OneSidedConfidencePPM,
			CancellationPolicy:    slo.CancellationPolicy,
		}
		observations := make([]slo.Observation, 0, 20)
		for index := 0; index < 20; index++ {
			acceptedAt := window.Start.Add(time.Duration(index+1) * time.Minute)
			completedAt := acceptedAt.Add(time.Duration(index+1) * time.Second)
			observations = append(observations, slo.Observation{
				JobID:              fmt.Sprintf("catalog-%s-%02d", preset, index),
				ContractRevisionID: contract.RevisionID,
				AcceptedAt:         acceptedAt, ExpiresAt: acceptedAt.Add(time.Hour),
				Outcome:    slo.OutcomeSucceeded,
				TerminalAt: &completedAt, VisibleCompletedAt: &completedAt,
			})
		}
		report, err := slo.Evaluate(contract, window, evaluatedAt, observations)
		if err != nil || report.Result != slo.ResultPass {
			t.Fatalf("build Catalog observability cohort: report=%#v error=%v", report, err)
		}
		cohorts = append(cohorts, sloevidence.CohortEvidence{
			Contract: contract, Observations: observations, Report: report,
		})
		contractIDs = append(contractIDs, contract.RevisionID)
	}
	apiReport, err := slo.EvaluateAvailability(1_000_000, 1_000_000, 10_000)
	if err != nil || apiReport.Result != slo.ResultPass {
		t.Fatalf("build Catalog API availability evidence: report=%#v error=%v", apiReport, err)
	}
	artifacts := make([]sloevidence.Artifact, 0, 7)
	for _, kind := range []sloevidence.ArtifactKind{
		sloevidence.ArtifactGatewayObservations, sloevidence.ArtifactSaleableSKUSnapshot,
		sloevidence.ArtifactDashboard, sloevidence.ArtifactAlertRules,
		sloevidence.ArtifactRuleTests, sloevidence.ArtifactRunbook,
		sloevidence.ArtifactPageEvents,
	} {
		content := []byte("Catalog fixture for " + kind)
		switch kind {
		case sloevidence.ArtifactGatewayObservations:
			content = catalogGatewayObservationsArtifact(t, window, 1_000_000, 1_000_000)
		case sloevidence.ArtifactSaleableSKUSnapshot:
			content = catalogSaleableSnapshotArtifact(t, evaluatedAt, cohorts)
		}
		ref := filepath.ToSlash(filepath.Join("observability", string(kind)+".json"))
		path := filepath.Join(directory, filepath.FromSlash(ref))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("create Catalog observability artifact directory: %v", err)
		}
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("write Catalog observability artifact: %v", err)
		}
		artifacts = append(artifacts, sloevidence.Artifact{
			Kind: kind, Ref: ref, Digest: sloevidence.Digest(content),
		})
	}
	firedAt := evaluatedAt.Add(time.Hour)
	evidence := sloevidence.Evidence{
		SchemaVersion:         1,
		ReleaseDigest:         releaseDigest,
		ConfigurationRevision: configurationRevision,
		ValidationEnvironment: "h3-validation-rack-1",
		Owner:                 "platform-oncall@example.invalid", Coverage: "24x7",
		Window: window, EvaluatedAt: evaluatedAt,
		SaleableContractRevisionIDs: contractIDs,
		API: sloevidence.APIEvidence{
			EligibleCount: 1_000_000, GoodCount: 1_000_000,
			MinimumSample: 10_000, Report: apiReport,
		},
		Cohorts: cohorts, Artifacts: artifacts,
		Exercise: sloevidence.Exercise{
			AlertFiredAt: firedAt, AlertDeliveredAt: firedAt.Add(time.Minute),
			AlertAckedAt: firedAt.Add(2 * time.Minute),
			ResolvedAt:   firedAt.Add(3 * time.Minute), Result: slo.ResultPass,
		},
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatalf("encode Catalog observability evidence: %v", err)
	}
	return encoded
}

func catalogGatewayObservationsArtifact(
	t *testing.T,
	window slo.Window,
	eligible,
	good int,
) []byte {
	t.Helper()
	streams := make([]sloevidence.GatewayObservationStream, 0, 2)
	for _, source := range []sloevidence.GatewayObservationSource{
		sloevidence.GatewaySourceExternalGateway,
		sloevidence.GatewaySourceSyntheticProbe,
	} {
		buckets := make([]sloevidence.GatewayObservationBucket, 0, 31)
		start := window.Start
		for start.Before(window.End) {
			end := start.Add(24 * time.Hour)
			if end.After(window.End) {
				end = window.End
			}
			bucket := sloevidence.GatewayObservationBucket{Start: start, End: end}
			if len(buckets) == 0 && source == sloevidence.GatewaySourceExternalGateway {
				bucket.EligibleCount = eligible
				bucket.GoodCount = good
			}
			buckets = append(buckets, bucket)
			start = end
		}
		streams = append(streams, sloevidence.GatewayObservationStream{
			Source: source, Buckets: buckets,
		})
	}
	encoded, err := json.Marshal(sloevidence.GatewayObservations{
		SchemaVersion: 1, Window: window, Streams: streams,
	})
	if err != nil {
		t.Fatalf("encode Catalog gateway observations: %v", err)
	}
	return encoded
}

func catalogSaleableSnapshotArtifact(
	t *testing.T,
	capturedAt time.Time,
	cohorts []sloevidence.CohortEvidence,
) []byte {
	t.Helper()
	contracts := make([]slo.Contract, 0, len(cohorts))
	for _, cohort := range cohorts {
		contracts = append(contracts, cohort.Contract)
	}
	encoded, err := json.Marshal(sloevidence.SaleableSKUSnapshot{
		SchemaVersion: 1, CapturedAt: capturedAt, Contracts: contracts,
	})
	if err != nil {
		t.Fatalf("encode Catalog saleable SKU snapshot: %v", err)
	}
	return encoded
}

func sha256Sum(value []byte) []byte {
	digest := sha256.Sum256(value)
	return digest[:]
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

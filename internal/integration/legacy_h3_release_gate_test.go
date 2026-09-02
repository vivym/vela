//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/legacyh3reachability"
	"github.com/vivym/vela/internal/productiongates"
	"github.com/vivym/vela/internal/releasebundle"
)

type legacyH3ReleaseGateFixture struct {
	database              testDatabase
	promotion             *pgxpool.Pool
	zeroBacklogReceiptID  uuid.UUID
	cutoverRevisionID     uuid.UUID
	launchManifestDigest  []byte
	releaseDigest         []byte
	configurationRevision string
	configurationManifest []byte
	reachabilityEvidence  []byte
	sourceRevision        string
}

type legacyH3ReleaseAuthorizationResult struct {
	zeroBacklogReceiptID uuid.UUID
	cutoverRevisionID    uuid.UUID
	launchManifestDigest []byte
	releaseDigest        []byte
	configuration        string
	sourceRevision       string
	reachabilityDigest   []byte
	authorizedAt         time.Time
	contentDigest        []byte
	replayed             bool
}

func TestLegacyH3ContractionAuthorizationBindsCurrentReleaseAndReachabilityEvidence(t *testing.T) {
	fixture := newLegacyH3ReleaseGateFixture(t)
	evidenceDigest := sha256.Sum256(fixture.reachabilityEvidence)

	created, err := authorizeLegacyH3Contraction(
		fixture,
		fixture.launchManifestDigest,
		fixture.releaseDigest,
		fixture.configurationRevision,
		fixture.configurationManifest,
		fixture.reachabilityEvidence,
		"integration-release-authorizer",
	)
	if err != nil {
		t.Fatalf("authorize Legacy H3 release contraction: %v", err)
	}
	if created.zeroBacklogReceiptID != fixture.zeroBacklogReceiptID ||
		created.cutoverRevisionID != fixture.cutoverRevisionID ||
		string(created.launchManifestDigest) != string(fixture.launchManifestDigest) ||
		string(created.releaseDigest) != string(fixture.releaseDigest) ||
		created.configuration != fixture.configurationRevision ||
		created.sourceRevision != fixture.sourceRevision ||
		string(created.reachabilityDigest) != string(evidenceDigest[:]) ||
		created.authorizedAt.IsZero() || len(created.contentDigest) != 32 ||
		created.replayed {
		t.Fatalf("Legacy H3 release authorization = %#v", created)
	}

	replayed, err := authorizeLegacyH3Contraction(
		fixture,
		fixture.launchManifestDigest,
		fixture.releaseDigest,
		fixture.configurationRevision,
		fixture.configurationManifest,
		fixture.reachabilityEvidence,
		"integration-release-authorizer",
	)
	if err != nil {
		t.Fatalf("replay Legacy H3 release contraction authorization: %v", err)
	}
	if !replayed.replayed || replayed.zeroBacklogReceiptID != created.zeroBacklogReceiptID ||
		replayed.cutoverRevisionID != created.cutoverRevisionID ||
		!replayed.authorizedAt.Equal(created.authorizedAt) ||
		string(replayed.contentDigest) != string(created.contentDigest) {
		t.Fatalf("replayed Legacy H3 release authorization = %#v", replayed)
	}

	changedEvidence := append(append([]byte(nil), fixture.reachabilityEvidence...), '\n')
	_, err = authorizeLegacyH3Contraction(
		fixture,
		fixture.launchManifestDigest,
		fixture.releaseDigest,
		fixture.configurationRevision,
		fixture.configurationManifest,
		changedEvidence,
		"integration-release-authorizer",
	)
	assertPostgresConstraint(t, err, "legacy_h3_contraction_authorization_replay_mismatch")
	_, err = authorizeLegacyH3Contraction(
		fixture,
		fixture.launchManifestDigest,
		fixture.releaseDigest,
		fixture.configurationRevision,
		fixture.configurationManifest,
		fixture.reachabilityEvidence,
		"changed-release-authorizer",
	)
	assertPostgresConstraint(t, err, "legacy_h3_contraction_authorization_replay_mismatch")

	_, err = fixture.database.Admin.Exec(`
		UPDATE legacy_h3_contraction_authorizations
		SET authorized_by = 'forged'
	`)
	assertPostgresConstraint(t, err, "stage_cutover_history_immutable")

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err = goose.DownTo(fixture.database.Admin, migrations, 56)
	assertPostgresConstraint(t, err, "legacy_h3_contraction_authorization_rollback_is_unsafe")
	version, versionErr := goose.GetDBVersion(fixture.database.Admin)
	if versionErr != nil || version != 57 {
		t.Fatalf(
			"Legacy H3 release gate version after refused Down = %d error=%v",
			version,
			versionErr,
		)
	}
}

func TestLegacyH3ContractionAuthorizationRejectsUnverifiedOrMismatchedEvidence(t *testing.T) {
	fixture := newLegacyH3ReleaseGateFixture(t)
	authorize := func(
		launchManifestDigest,
		releaseDigest []byte,
		configurationRevision string,
		configurationManifest,
		reachabilityEvidence []byte,
	) error {
		_, err := authorizeLegacyH3Contraction(
			fixture,
			launchManifestDigest,
			releaseDigest,
			configurationRevision,
			configurationManifest,
			reachabilityEvidence,
			"integration-release-authorizer",
		)
		return err
	}

	t.Run("arbitrary digest", func(t *testing.T) {
		err := authorize(
			fixture.launchManifestDigest,
			fixture.releaseDigest,
			fixture.configurationRevision,
			fixture.configurationManifest,
			make([]byte, sha256.Size),
		)
		assertPostgresConstraint(t, err, "legacy_h3_contraction_reachability_evidence_invalid")
	})
	t.Run("malformed JSON", func(t *testing.T) {
		err := authorize(
			fixture.launchManifestDigest,
			fixture.releaseDigest,
			fixture.configurationRevision,
			fixture.configurationManifest,
			[]byte("{"),
		)
		assertPostgresConstraint(t, err, "legacy_h3_contraction_reachability_evidence_invalid")
	})
	t.Run("FAIL result", func(t *testing.T) {
		evidence := decodeLegacyH3ReachabilityEvidence(t, fixture.reachabilityEvidence)
		evidence.Result = legacyh3reachability.ResultFail
		err := authorize(
			fixture.launchManifestDigest,
			fixture.releaseDigest,
			fixture.configurationRevision,
			fixture.configurationManifest,
			encodeLegacyH3ReachabilityEvidence(t, evidence),
		)
		assertPostgresConstraint(t, err, "legacy_h3_contraction_reachability_evidence_invalid")
	})
	t.Run("changed check", func(t *testing.T) {
		evidence := decodeLegacyH3ReachabilityEvidence(t, fixture.reachabilityEvidence)
		evidence.Checks[0].Passed = false
		err := authorize(
			fixture.launchManifestDigest,
			fixture.releaseDigest,
			fixture.configurationRevision,
			fixture.configurationManifest,
			encodeLegacyH3ReachabilityEvidence(t, evidence),
		)
		assertPostgresConstraint(t, err, "legacy_h3_contraction_reachability_evidence_invalid")
	})
	t.Run("configuration bytes", func(t *testing.T) {
		changedConfiguration := append(append([]byte(nil), fixture.configurationManifest...), '\n')
		err := authorize(
			fixture.launchManifestDigest,
			fixture.releaseDigest,
			fixture.configurationRevision,
			changedConfiguration,
			fixture.reachabilityEvidence,
		)
		assertPostgresConstraint(t, err, "legacy_h3_contraction_configuration_manifest_invalid")
	})
	t.Run("source revision", func(t *testing.T) {
		evidence := decodeLegacyH3ReachabilityEvidence(t, fixture.reachabilityEvidence)
		evidence.SourceRevision = strings.Repeat("e", 40)
		err := authorize(
			fixture.launchManifestDigest,
			fixture.releaseDigest,
			fixture.configurationRevision,
			fixture.configurationManifest,
			encodeLegacyH3ReachabilityEvidence(t, evidence),
		)
		assertPostgresConstraint(t, err, "legacy_h3_contraction_reachability_evidence_invalid")
	})
	t.Run("launch manifest", func(t *testing.T) {
		changedManifest := append([]byte(nil), fixture.launchManifestDigest...)
		changedManifest[0]++
		err := authorize(
			changedManifest,
			fixture.releaseDigest,
			fixture.configurationRevision,
			fixture.configurationManifest,
			fixture.reachabilityEvidence,
		)
		assertPostgresConstraint(t, err, "legacy_h3_contraction_launch_manifest_mismatch")
	})
	t.Run("release digest", func(t *testing.T) {
		changedRelease := append([]byte(nil), fixture.releaseDigest...)
		changedRelease[0]++
		evidence := decodeLegacyH3ReachabilityEvidence(t, fixture.reachabilityEvidence)
		evidence.ReleaseDigest = "sha256:" + hex.EncodeToString(changedRelease)
		err := authorize(
			fixture.launchManifestDigest,
			changedRelease,
			fixture.configurationRevision,
			fixture.configurationManifest,
			encodeLegacyH3ReachabilityEvidence(t, evidence),
		)
		assertPostgresConstraint(t, err, "legacy_h3_contraction_release_binding_mismatch")
	})
	t.Run("configuration revision", func(t *testing.T) {
		var configuration releasebundle.ConfigurationManifest
		if err := json.Unmarshal(fixture.configurationManifest, &configuration); err != nil {
			t.Fatalf("decode configuration manifest: %v", err)
		}
		configuration.SourceRevision = strings.Repeat("e", 40)
		changedConfiguration, err := json.Marshal(configuration)
		if err != nil {
			t.Fatalf("encode changed configuration manifest: %v", err)
		}
		changedDigest := sha256.Sum256(changedConfiguration)
		changedRevision := "sha256:" + hex.EncodeToString(changedDigest[:])
		evidence := decodeLegacyH3ReachabilityEvidence(t, fixture.reachabilityEvidence)
		evidence.ConfigurationRevision = changedRevision
		evidence.SourceRevision = configuration.SourceRevision
		err = authorize(
			fixture.launchManifestDigest,
			fixture.releaseDigest,
			changedRevision,
			changedConfiguration,
			encodeLegacyH3ReachabilityEvidence(t, evidence),
		)
		assertPostgresConstraint(t, err, "legacy_h3_contraction_release_binding_mismatch")
	})
}

func TestLegacyH3ContractionAuthorizationMigrationRoundTripBeforeAuthorization(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 57)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")

	if err := goose.DownTo(database.Admin, migrations, 56); err != nil {
		t.Fatalf("contract empty Legacy H3 release gate: %v", err)
	}
	assertTableDoesNotExist(t, database.Admin, "legacy_h3_contraction_authorizations")
	if err := goose.UpTo(database.Admin, migrations, 57); err != nil {
		t.Fatalf("re-expand empty Legacy H3 release gate: %v", err)
	}
	assertTableExists(t, database.Admin, "legacy_h3_contraction_authorizations")
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 57 {
		t.Fatalf("Legacy H3 release gate round-trip version = %d error=%v", version, err)
	}
}

func newLegacyH3ReleaseGateFixture(t *testing.T) legacyH3ReleaseGateFixture {
	return newLegacyH3ReleaseGateFixtureWithPreCutoverSetup(t, nil)
}

func newLegacyH3ReleaseGateFixtureWithPreCutoverSetup(
	t *testing.T,
	setup func(testDatabase),
) legacyH3ReleaseGateFixture {
	t.Helper()
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 57)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	if setup != nil {
		setup(database)
	}
	promotion := stageCutoverPromotionPool(t, database)
	bundle, configurationManifest, reachabilityEvidence :=
		buildLegacyH3ReleaseEvidence(t)
	releaseDigest, err := hex.DecodeString(strings.TrimPrefix(bundle.ReleaseDigest, "sha256:"))
	if err != nil || len(releaseDigest) != sha256.Size {
		t.Fatalf("decode contracted release digest %q: %v", bundle.ReleaseDigest, err)
	}
	launchManifestDigest := recordAndSealLegacyH3LaunchManifest(
		t,
		promotion,
		releaseDigest,
		bundle.ConfigurationRevision,
	)
	cutoverRevisionID := activateLegacyH3ProductionStageOnlyCutover(
		t,
		database,
		promotion,
		releaseDigest,
		bundle.ConfigurationRevision,
		launchManifestDigest,
	)

	startInventoryID := captureLegacyAuthorityInventory(t, promotion, "release-gate-window-start")
	startEvidenceID := recordStageCutoverExternalDrainEvidence(
		t, promotion, [5]int64{}, "release-gate-window-start",
	)
	time.Sleep(1100 * time.Millisecond)
	endInventoryID := captureLegacyAuthorityInventory(t, promotion, "release-gate-window-end")
	endEvidenceID := recordStageCutoverExternalDrainEvidence(
		t, promotion, [5]int64{}, "release-gate-window-end",
	)
	zeroBacklogReceiptID := uuid.New()
	if _, err := promotion.Exec(context.Background(), `
		SELECT * FROM vela_seal_stage_cutover_zero_backlog($1, $2, $3, $4, $5, $6)
	`, zeroBacklogReceiptID, startInventoryID, endInventoryID,
		startEvidenceID, endEvidenceID, "integration-release-gate"); err != nil {
		t.Fatalf("seal zero backlog before Legacy H3 release gate: %v", err)
	}
	if _, err := promotion.Exec(context.Background(), `
		SELECT * FROM vela_prepare_legacy_h3_contraction($1, $2)
	`, zeroBacklogReceiptID, "integration-release-gate"); err != nil {
		t.Fatalf("prepare Legacy H3 release gate: %v", err)
	}

	return legacyH3ReleaseGateFixture{
		database:              database,
		promotion:             promotion,
		zeroBacklogReceiptID:  zeroBacklogReceiptID,
		cutoverRevisionID:     cutoverRevisionID,
		launchManifestDigest:  launchManifestDigest,
		releaseDigest:         releaseDigest,
		configurationRevision: bundle.ConfigurationRevision,
		configurationManifest: configurationManifest,
		reachabilityEvidence:  reachabilityEvidence,
		sourceRevision:        bundle.ConfigurationManifest.SourceRevision,
	}
}

func buildLegacyH3ReleaseEvidence(
	t *testing.T,
) (releasebundle.Bundle, []byte, []byte) {
	t.Helper()
	root := t.TempDir()
	for _, path := range []string{
		"proto/vela/v1/stage_worker_control.proto",
		"cmd/vela-stage-worker-agent/main.go",
		"internal/stageworkeragent/agent.go",
		"internal/stagescheduler/service.go",
		"deploy/stage-worker/kustomization.yaml",
	} {
		writeLegacyH3ReachabilityFixture(t, root, path, "stage\n")
	}
	runLegacyH3FixtureGit(t, root, "init", "--quiet")
	runLegacyH3FixtureGit(t, root, "add", ".")
	runLegacyH3FixtureGit(
		t,
		root,
		"-c", "user.name=Legacy H3 Release Gate",
		"-c", "user.email=legacy-h3-release-gate@example.invalid",
		"commit", "--quiet", "-m", "contracted source",
	)
	sourceRevision := strings.TrimSpace(runLegacyH3FixtureGit(t, root, "rev-parse", "HEAD"))
	releaseDigest := sha256.Sum256([]byte("contracted-stage-release"))
	bundle := releasebundle.Bundle{
		SchemaVersion: legacyh3reachability.ContractedReleaseSchemaVersion,
		ReleaseDigest: "sha256:" + hex.EncodeToString(releaseDigest[:]),
		ConfigurationManifest: releasebundle.ConfigurationManifest{
			SchemaVersion:  legacyh3reachability.ContractedReleaseSchemaVersion,
			SourceRevision: sourceRevision,
			FinalRenders:   []releasebundle.NamedArtifact{{Name: "stage-worker"}},
		},
		OCIImages: []releasebundle.OCIImage{{
			Image: "ghcr.io/vivym/vela-stage-worker-agent@sha256:" + strings.Repeat("c", 64),
		}},
	}
	configurationManifest, err := json.Marshal(bundle.ConfigurationManifest)
	if err != nil {
		t.Fatalf("encode contracted configuration manifest: %v", err)
	}
	configurationDigest := sha256.Sum256(configurationManifest)
	bundle.ConfigurationRevision = "sha256:" + hex.EncodeToString(configurationDigest[:])
	evidence, reachabilityEvidence, _, err := legacyh3reachability.Scan(
		root,
		bundle,
		sourceRevision,
		"integration/release-gate",
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	)
	if err != nil || evidence.Result != legacyh3reachability.ResultPass {
		t.Fatalf("scan contracted release source: evidence=%#v error=%v", evidence, err)
	}
	return bundle, configurationManifest, reachabilityEvidence
}

func recordAndSealLegacyH3LaunchManifest(
	t *testing.T,
	pool *pgxpool.Pool,
	releaseDigest []byte,
	configurationRevision string,
) []byte {
	t.Helper()
	manifestDigest := sha256.Sum256([]byte("legacy-h3-contracted-launch-manifest"))
	transaction, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin Legacy H3 Launch Receipt transaction: %v", err)
	}
	defer func() { _ = transaction.Rollback(context.Background()) }()
	for index, gate := range productiongates.AllGates() {
		evidenceDigest := sha256.Sum256([]byte("legacy-h3-release-gate/" + string(gate)))
		startedAt := time.Date(2026, 9, 1, 1, index, 0, 0, time.UTC)
		if _, err := transaction.Exec(context.Background(), `
			SELECT * FROM vela_record_production_gate_receipt(
				$1, 1, $2, $3, $4, 'h3-validation-rack-1',
				'PASS', 'platform-oncall@example.invalid',
				'all gate assertions pass', 'all gate assertions passed',
				$5, $6, $7, $8, $9, $10
			)
		`, uuid.New(), string(gate), releaseDigest, configurationRevision,
			"evidence/"+string(gate)+".json", evidenceDigest[:], manifestDigest[:],
			startedAt, startedAt.Add(time.Minute), startedAt.Add(2*time.Minute)); err != nil {
			t.Fatalf("record %s Legacy H3 Launch Receipt: %v", gate, err)
		}
	}
	var sealed bool
	var receiptCount int
	if err := transaction.QueryRow(context.Background(), `
		SELECT sealed, receipt_count FROM vela_seal_production_gate_manifest($1)
	`, manifestDigest[:]).Scan(&sealed, &receiptCount); err != nil {
		t.Fatalf("seal Legacy H3 Launch Receipt manifest: %v", err)
	}
	if !sealed || receiptCount != len(productiongates.AllGates()) {
		t.Fatalf("sealed manifest = sealed %t receipt count %d", sealed, receiptCount)
	}
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatalf("commit Legacy H3 Launch Receipt manifest: %v", err)
	}
	return append([]byte(nil), manifestDigest[:]...)
}

func activateLegacyH3ProductionStageOnlyCutover(
	t *testing.T,
	database testDatabase,
	promotion *pgxpool.Pool,
	releaseDigest []byte,
	configurationRevision string,
	launchManifestDigest []byte,
) uuid.UUID {
	t.Helper()
	var currentRevision int64
	var previousID uuid.UUID
	var connectorSetDigest []byte
	if err := database.Admin.QueryRow(`
		SELECT revision.revision, revision.id,
		       vela_execution_profile_connector_set_digest($1, $2)
		FROM stage_cutover_control AS control
		JOIN stage_cutover_revisions AS revision
		  ON revision.id = control.current_revision_id
		WHERE control.singleton
	`, graphExecutionProfileID, stageGraphID).Scan(
		&currentRevision,
		&previousID,
		&connectorSetDigest,
	); err != nil {
		t.Fatalf("read Legacy H3 production cutover binding: %v", err)
	}
	cutoverID := uuid.New()
	var activatedID uuid.UUID
	if err := promotion.QueryRow(context.Background(), `
		SELECT (vela_activate_stage_cutover(
			$1, $2, $3, 'PRODUCTION', 'STAGE_ONLY', 10000,
			$4, $5, 2147483648, 1, $6, $7,
			sha256(convert_to($7, 'UTF8')), $8, $9,
			'integration-catalog-promotion',
			'activate release-bound production Stage-only contraction gate'
		)).id
	`, cutoverID, currentRevision+1, previousID, stageGraphID,
		graphExecutionProfileID, releaseDigest, configurationRevision,
		connectorSetDigest, launchManifestDigest).Scan(&activatedID); err != nil {
		t.Fatalf("activate release-bound production Stage-only cutover: %v", err)
	}
	if activatedID != cutoverID {
		t.Fatalf("production Stage-only cutover id = %s, want %s", activatedID, cutoverID)
	}
	return cutoverID
}

func authorizeLegacyH3Contraction(
	fixture legacyH3ReleaseGateFixture,
	launchManifestDigest,
	releaseDigest []byte,
	configurationRevision string,
	configurationManifest,
	reachabilityEvidence []byte,
	actor string,
) (legacyH3ReleaseAuthorizationResult, error) {
	var result legacyH3ReleaseAuthorizationResult
	err := fixture.promotion.QueryRow(context.Background(), `
		SELECT zero_backlog_receipt_id, cutover_revision_id,
		       launch_manifest_digest, release_digest,
		       configuration_revision, source_revision,
		       reachability_evidence_digest,
		       authorized_at, content_digest, replayed
		FROM vela_authorize_legacy_h3_contraction($1, $2, $3, $4, $5, $6, $7)
	`, fixture.zeroBacklogReceiptID, launchManifestDigest, releaseDigest,
		configurationRevision, configurationManifest, reachabilityEvidence, actor).Scan(
		&result.zeroBacklogReceiptID,
		&result.cutoverRevisionID,
		&result.launchManifestDigest,
		&result.releaseDigest,
		&result.configuration,
		&result.sourceRevision,
		&result.reachabilityDigest,
		&result.authorizedAt,
		&result.contentDigest,
		&result.replayed,
	)
	return result, err
}

func decodeLegacyH3ReachabilityEvidence(
	t *testing.T,
	encoded []byte,
) legacyh3reachability.Evidence {
	t.Helper()
	var evidence legacyh3reachability.Evidence
	if err := json.Unmarshal(encoded, &evidence); err != nil {
		t.Fatalf("decode Legacy H3 reachability evidence: %v", err)
	}
	return evidence
}

func encodeLegacyH3ReachabilityEvidence(
	t *testing.T,
	evidence legacyh3reachability.Evidence,
) []byte {
	t.Helper()
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatalf("encode Legacy H3 reachability evidence: %v", err)
	}
	return encoded
}

func writeLegacyH3ReachabilityFixture(t *testing.T, root, relative, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create Legacy H3 reachability fixture directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write Legacy H3 reachability fixture: %v", err)
	}
}

func runLegacyH3FixtureGit(t *testing.T, root string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root}, arguments...)...)
	command.Env = append(
		os.Environ(),
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_CONFIG_NOSYSTEM=1",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", arguments, err, output)
	}
	return string(output)
}

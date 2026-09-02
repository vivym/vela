//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/h3campaignevidence"
	"github.com/vivym/vela/internal/h3stage"
	"github.com/vivym/vela/internal/releasebundle"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stagecache"
	"github.com/vivym/vela/internal/stagefinalization"
	"github.com/vivym/vela/internal/telemetry"
)

const campaignResidencyPlanID = "49200000-0000-0000-0000-000000000201"

const h3ExactCacheCanonicalizationRevisionID = "49200000-0000-0000-0000-000000000251"

func TestH3CampaignEvidenceCapturesAuthoritativeLineageTransferAndCache(t *testing.T) {
	fixture := runH3CampaignEvidenceFixture(t)
	bundlePath := writeH3CampaignReleaseBundle(t)
	binding, err := h3campaignevidence.LoadEvidenceBinding(
		bundlePath, uuid.MustParse(campaignResidencyPlanID), "h3-integration",
		"spiffe://vela/test/campaign-reader",
	)
	if err != nil {
		t.Fatalf("load H3 Campaign release binding: %v", err)
	}
	pool, err := pgxpool.New(context.Background(), roleDatabaseURL(
		t, fixture.database.DSN,
		"vela_h3_campaign_evidence_login", "vela-h3-campaign-evidence-password",
	))
	if err != nil {
		t.Fatalf("open H3 Campaign evidence database: %v", err)
	}
	t.Cleanup(pool.Close)
	reader, err := h3campaignevidence.NewPostgresReader(pool)
	if err != nil {
		t.Fatalf("construct H3 Campaign reader: %v", err)
	}
	evidence, err := h3campaignevidence.Capture(
		context.Background(), reader, h3campaignevidence.CaptureRequest{
			EvidenceBinding: binding,
			Selection: h3campaignevidence.Selection{
				SameNodeJobID:  fixture.sameNodeJobID,
				CrossNodeJobID: fixture.crossNodeJobID,
				CacheJobID:     fixture.cacheJobID,
			},
		},
	)
	if err != nil {
		t.Fatalf("verify captured H3 Campaign: %v", err)
	}
	if len(evidence.Runs) != 2 || len(evidence.CacheRun.Hits) != 2 {
		t.Fatalf("captured H3 Campaign evidence = %#v", evidence)
	}
}

func TestStageTelemetryReadsAuthoritativeCampaignState(t *testing.T) {
	fixture := runH3CampaignEvidenceFixture(t)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE worker_instances AS worker
		SET capacity_pool_id = encoder_route.capacity_pool_id
		FROM model_residencies AS vae_residency
		JOIN model_runtime_capacity_routes AS vae_route
		  ON vae_route.model_residency_id = vae_residency.id
		JOIN stage_profile_revisions AS vae_profile
		  ON vae_profile.id = vae_route.stage_profile_revision_id
		JOIN stage_definition_revisions AS vae_definition
		  ON vae_definition.id = vae_profile.stage_definition_revision_id
		CROSS JOIN LATERAL (
			SELECT route.capacity_pool_id
			FROM model_runtime_capacity_routes AS route
			JOIN stage_profile_revisions AS profile
			  ON profile.id = route.stage_profile_revision_id
			JOIN stage_definition_revisions AS definition
			  ON definition.id = profile.stage_definition_revision_id
			WHERE definition.stage_kind = 'ENCODER'
			LIMIT 1
		) AS encoder_route
		WHERE worker.id = vae_residency.worker_instance_id
		  AND vae_definition.stage_kind = 'VAE_DECODER'
	`); err != nil {
		t.Fatalf("drift legacy WorkerInstance CapacityPool away from VAE route: %v", err)
	}
	pool := newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password")
	reader := telemetry.NewPostgresStageSnapshotReader(pool)

	snapshot, err := reader.LatestStageSnapshot(context.Background())
	if err != nil {
		t.Fatalf("read Stage telemetry snapshot: %v", err)
	}
	if stageStateCount(snapshot.RunStates, "ENCODER", "SUCCEEDED") < 4 ||
		stageStateCount(snapshot.RunStates, "DIT", "SUCCEEDED") < 4 ||
		stageStateCount(snapshot.RunStates, "VAE_DECODER", "SUCCEEDED") < 3 ||
		stageStateCount(snapshot.CacheStates, "ENCODER", "LIVE") != 1 ||
		stageStateCount(snapshot.CacheStates, "DIT", "LIVE") != 1 ||
		stateCount(snapshot.TransferStates, "CONSUMED") != 4 ||
		stageStateCount(snapshot.ResidencyStates, "ENCODER", "READY") == 0 ||
		stageStateCount(snapshot.ResidencyStates, "DIT", "READY") == 0 ||
		stageStateCount(snapshot.ResidencyStates, "VAE_DECODER", "READY") == 0 {
		t.Fatalf("Stage telemetry snapshot does not close campaign authority: %#v", snapshot)
	}
}

func TestStageTelemetryMigrationPreservesReadOnlyPrivilegeBoundary(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 56)
	pool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")

	assertStageTelemetryPrivileges(t, pool, map[string]bool{
		"stage_runs":          true,
		"transfer_tickets":    true,
		"stage_cache_entries": true,
	})
	if _, err := pool.Exec(context.Background(), "SET ROLE vela_attempt_coordinator_owner"); err == nil {
		t.Fatal("Stage telemetry login can SET ROLE to the Stage authority owner")
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 55); err != nil {
		t.Fatalf("contract Stage telemetry reader migration: %v", err)
	}
	assertStageTelemetryPrivileges(t, pool, map[string]bool{
		"stage_runs":          true,
		"transfer_tickets":    false,
		"stage_cache_entries": false,
	})

	if err := goose.UpTo(database.Admin, migrations, 56); err != nil {
		t.Fatalf("re-expand Stage telemetry reader migration: %v", err)
	}
	assertStageTelemetryPrivileges(t, pool, map[string]bool{
		"stage_runs":          true,
		"transfer_tickets":    true,
		"stage_cache_entries": true,
	})
}

func TestH3ExactCacheReconciliationMigrationEmptyDownUpRestoresRoleSurface(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 65)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	assertH3ExactCacheReconciliationRoleSurface(t, database)

	cache, err := stagecache.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	))
	if err != nil {
		t.Fatalf("construct H3 exact cache migration repository: %v", err)
	}
	for _, action := range []string{"ADMIT", "HIT"} {
		candidates, readErr := cache.ReadH3ExactCandidates(context.Background(), action, 10)
		if readErr != nil || len(candidates) != 0 {
			t.Fatalf("read empty H3 exact cache %s candidates = %#v error=%v", action, candidates, readErr)
		}
	}

	if err := goose.DownTo(database.Admin, migrations, 64); err != nil {
		t.Fatalf("contract H3 exact cache reconciliation migration: %v", err)
	}
	var functionsRemoved bool
	if err := database.Admin.QueryRow(`
		SELECT
			to_regprocedure('vela_read_h3_exact_cache_candidates(text,integer)') IS NULL
			AND to_regprocedure(
				'vela_find_h3_exact_cache_entry(uuid,uuid,uuid,uuid,uuid,text,bytea,timestamp with time zone)'
			) IS NULL
	`).Scan(&functionsRemoved); err != nil {
		t.Fatalf("inspect contracted H3 exact cache reconciliation surface: %v", err)
	}
	if !functionsRemoved {
		t.Fatal("H3 exact cache reconciliation functions survived migration Down")
	}

	if err := goose.UpTo(database.Admin, migrations, 65); err != nil {
		t.Fatalf("re-expand H3 exact cache reconciliation migration: %v", err)
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 65 {
		t.Fatalf("H3 exact cache reconciliation version after Down Up = %d error=%v", version, err)
	}
	assertH3ExactCacheReconciliationRoleSurface(t, database)
}

func assertH3ExactCacheReconciliationRoleSurface(t *testing.T, database testDatabase) {
	t.Helper()
	var readerOwner, finderOwner string
	var coordinatorCanRead, coordinatorCanFind, internalCanRead, internalCanFind bool
	if err := database.Admin.QueryRow(`
		SELECT
			COALESCE(pg_get_userbyid(reader.proowner), ''),
			COALESCE(pg_get_userbyid(finder.proowner), ''),
			COALESCE(has_function_privilege(
				'vela_attempt_coordinator_login', reader.oid, 'EXECUTE'
			), false),
			COALESCE(has_function_privilege(
				'vela_attempt_coordinator_login', finder.oid, 'EXECUTE'
			), false),
			COALESCE(has_function_privilege(
				'vela_internal_login', reader.oid, 'EXECUTE'
			), false),
			COALESCE(has_function_privilege(
				'vela_internal_login', finder.oid, 'EXECUTE'
			), false)
		FROM (VALUES (
			to_regprocedure('vela_read_h3_exact_cache_candidates(text,integer)')
		)) AS reader_function(oid)
		LEFT JOIN pg_proc AS reader ON reader.oid = reader_function.oid
		CROSS JOIN (VALUES (
			to_regprocedure(
				'vela_find_h3_exact_cache_entry(uuid,uuid,uuid,uuid,uuid,text,bytea,timestamp with time zone)'
			)
		)) AS finder_function(oid)
		LEFT JOIN pg_proc AS finder ON finder.oid = finder_function.oid
	`).Scan(
		&readerOwner, &finderOwner,
		&coordinatorCanRead, &coordinatorCanFind,
		&internalCanRead, &internalCanFind,
	); err != nil {
		t.Fatalf("inspect H3 exact cache reconciliation role surface: %v", err)
	}
	if readerOwner != "vela_attempt_coordinator_owner" ||
		finderOwner != "vela_attempt_coordinator_owner" ||
		!coordinatorCanRead || !coordinatorCanFind || internalCanRead || internalCanFind {
		t.Fatalf(
			"H3 exact cache role surface owners=%s/%s coordinator=%t/%t internal=%t/%t",
			readerOwner, finderOwner, coordinatorCanRead, coordinatorCanFind,
			internalCanRead, internalCanFind,
		)
	}
}

func assertStageTelemetryPrivileges(
	t *testing.T,
	pool *pgxpool.Pool,
	selectPrivileges map[string]bool,
) {
	t.Helper()
	for table, wantSelect := range selectPrivileges {
		for _, privilege := range []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"} {
			var allowed bool
			if err := pool.QueryRow(
				context.Background(),
				"SELECT has_table_privilege(current_user, $1, $2)",
				table,
				privilege,
			).Scan(&allowed); err != nil {
				t.Fatalf("inspect Stage telemetry %s privilege on %s: %v", privilege, table, err)
			}
			want := privilege == "SELECT" && wantSelect
			if allowed != want {
				t.Fatalf(
					"Stage telemetry %s privilege on %s = %t, want %t",
					privilege,
					table,
					allowed,
					want,
				)
			}
		}
	}
}

func stageStateCount(metrics []telemetry.StageStateCount, stageKind, state string) float64 {
	for _, metric := range metrics {
		if metric.StageKind == stageKind && metric.State == state {
			return metric.Count
		}
	}
	return 0
}

func stateCount(metrics []telemetry.StateCount, state string) float64 {
	for _, metric := range metrics {
		if metric.State == state {
			return metric.Count
		}
	}
	return 0
}

func writeH3CampaignReleaseBundle(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	base := writeCatalogReleaseBundleFixture(t, directory, "h3-campaign")
	if len(base.OCIImages) != 3 {
		t.Fatalf("campaign base OCI images = %d, want 3", len(base.OCIImages))
	}
	planBytes, err := os.ReadFile(filepath.Join(directory, "release-bundle-plan.json"))
	if err != nil {
		t.Fatalf("read campaign release build plan: %v", err)
	}
	var plan releasebundle.BuildPlan
	if err := json.Unmarshal(planBytes, &plan); err != nil {
		t.Fatalf("decode campaign release build plan: %v", err)
	}

	controlImage := bundleImageWithName(t, base, "/vela-control@")
	stageImage := bundleImageWithName(t, base, "/vela-stage-worker-agent@")
	runtimeImage := bundleImageWithName(t, base, "/vela-h3-stage-runtime@")
	images := []string{controlImage, stageImage, runtimeImage}

	planID := uuid.MustParse(campaignResidencyPlanID)
	bundleID := uuid.MustParse("49200000-0000-0000-0000-000000000202")
	poolID := uuid.MustParse("49200000-0000-0000-0000-000000000203")
	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000204")
	profileID := uuid.MustParse(ditWorkerProfileID)
	for index := range plan.ExternalResources {
		resource := &plan.ExternalResources[index]
		if resource.Kind == "Secret" && resource.Namespace == "vela-system" &&
			resource.Name == "stage-worker-control-r1" {
			resource.RequiredKeys = []string{
				"49200000-0000-0000-0000-000000000205.tls.crt",
				"49200000-0000-0000-0000-000000000205.tls.key",
				"ca.crt",
			}
		}
	}
	actuation := fleetcontroller.WorkerBundleActuation{
		SchemaVersion: 1, PlanRevisionID: planID, WorkerBundleID: bundleID,
		Namespace: "vela-system", InitImage: images[0],
		StageWorkerAgentImage:          stageImage,
		RuntimeImage:                   runtimeImage,
		StageWorkerConfigMap:           "vela-stage-worker-runtime-r1",
		ModelRuntimeVerifierConfigMap:  "model-runtime-verifier-r1",
		StageWorkerControlTLSSecret:    "stage-worker-control-r1",
		StageWorkerAuthoritySecret:     "stage-worker-authority-r1",
		ArtifactStoreCredentialsSecret: "stage-worker-artifact-credentials-r1",
		ArtifactStoreCASecret:          "stage-worker-artifact-ca-r1",
		WorkerInstances: []fleetcontroller.WorkerInstanceActuation{{
			ID: workerID, InstanceEpoch: 1, WorkerProfileRevisionID: profileID,
			CapacityPoolID: poolID, Role: "dit", CapacitySlots: 1,
			DeviceSetDigest: strings.Repeat("1", 64), MembershipDigest: strings.Repeat("2", 64),
			ModelRuntimes: []fleetcontroller.ModelRuntimeProcess{{
				ModelResidencyID:       uuid.MustParse("49200000-0000-0000-0000-000000000207"),
				CapacityPoolID:         poolID,
				StageProfileRevisionID: uuid.MustParse(ditStageProfileID),
				ModelRuntimeEpochFloor: 1,
				Component:              "DIT", ModelComponentRevision: "h3-dit-v1",
				RuntimeIdentity: "h3-dit-runtime-r1", Command: []string{"/opt/vela/bin/h3-dit"},
				InitializationTimeout: "2h", ShutdownTimeout: "2m",
			}},
			Members: []fleetcontroller.WorkerMemberActuation{{
				ID: uuid.MustParse("49200000-0000-0000-0000-000000000205"), MemberEpoch: 1,
				Key: "member-0", NodeIdentity: "campaign-node-a", ResourceClass: "GPU", DeviceCount: 1,
				IdentityDigest: "c4e4970f516c8e4cb61c23d077f071afec0dcd82110cfcb70c9031b410e23089",
				DeviceConstraints: []fleetcontroller.DeviceConstraint{{
					DeviceID: uuid.MustParse("49200000-0000-0000-0000-000000000206"), DeviceEpoch: 1,
					GPUUUID: "GPU-00000000-0000-0000-0000-000000000201", PCIBDF: "0000:41:00.0",
				}},
			}},
		}},
	}
	actuation.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(actuation)
	if err != nil {
		t.Fatalf("digest campaign WorkerBundle actuation: %v", err)
	}
	rollout := fleetcontroller.ResidencyPlanRollout{
		ApprovedPlan: fleet.ApprovedResidencyPlan{
			SchemaVersion: 1, ID: planID, StableID: "h3-campaign-r1", Revision: 1,
			ContentDigest: actuation.RevisionDigest, ApprovalEvidenceDigest: strings.Repeat("e", 64),
			ApprovedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), ApprovedBy: "fleet/campaign-test",
			CapacityPools: []fleet.PlannedCapacityPool{{
				ID: poolID, StableID: "h3-campaign-dit",
				StageProfileRevisionID: uuid.MustParse(ditStageProfileID),
				ResourceClass:          "GPU", SecurityClass: "INTERNAL", Region: "cn-shanghai",
				MaxReadyQueueDepth: 128,
			}},
			WorkerBundles: []fleet.PlannedWorkerBundle{{
				ID: bundleID, StableID: "campaign-node-a", DesiredGeneration: 1,
				LayoutDigest: actuation.RevisionDigest,
			}},
			WorkerInstances: []fleet.PlannedWorkerInstance{{
				ID: workerID, WorkerProfileRevisionID: profileID, CapacityPoolID: poolID,
				WorkerBundleID: bundleID, DesiredMemberCount: 1, DesiredDeviceCount: 1,
				ModelRuntimeRoutes: []fleet.PlannedModelRuntimeRoute{{
					ModelResidencyID:       uuid.MustParse("49200000-0000-0000-0000-000000000207"),
					CapacityPoolID:         poolID,
					StageProfileRevisionID: uuid.MustParse(ditStageProfileID),
				}},
			}},
		},
		WorkerBundles: []fleetcontroller.WorkerBundleActuation{actuation},
	}
	encoded, err := json.Marshal(map[string]any{
		"schema_version": 1, "rollouts": []fleetcontroller.ResidencyPlanRollout{rollout},
	})
	if err != nil {
		t.Fatalf("encode campaign ResidencyPlan rollout: %v", err)
	}
	render := catalogReleaseRender("fleet-controller", controlImage, images)
	identity := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: vela-fleet-residency-plan-rollouts-r1\n  namespace: vela-system\n"
	target := identity +
		"immutable: true\ndata:\n  rollouts.json: |-\n    " +
		strings.ReplaceAll(string(encoded), "\n", "\n    ") + "\n"
	if !strings.Contains(render, identity) {
		t.Fatal("campaign Fleet render lacks target replacement point")
	}
	writeCatalogReleaseFile(
		t, filepath.Join(directory, "render-fleet-controller.yaml"),
		[]byte(strings.Replace(render, identity, target, 1)),
	)

	planPath := filepath.Join(directory, "campaign-release-bundle-plan.json")
	writeJSONFixture(t, planPath, plan)
	_, bundleBytes, err := releasebundle.BuildFromSource(newCatalogReleaseSource(t), planPath)
	if err != nil {
		t.Fatalf("build campaign release bundle: %v", err)
	}
	bundlePath := filepath.Join(directory, "campaign-release-bundle.json")
	writeCatalogReleaseFile(t, bundlePath, bundleBytes)
	return bundlePath
}

func bundleImageWithName(t *testing.T, bundle releasebundle.Bundle, marker string) string {
	t.Helper()
	for _, image := range bundle.OCIImages {
		if strings.Contains(image.Image, marker) {
			return image.Image
		}
	}
	t.Fatalf("release bundle omitted image matching %q", marker)
	return ""
}

type h3CampaignEvidenceFixture struct {
	database       testDatabase
	sameNodeJobID  uuid.UUID
	crossNodeJobID uuid.UUID
	cacheJobID     uuid.UUID
}

func runH3CampaignEvidenceFixture(t *testing.T) h3CampaignEvidenceFixture {
	t.Helper()
	database, coordinator, serverURL := newH3IntegrationEnvironmentWithCatalogSetup(
		t, seedH3CampaignThumbnailOutput,
	)
	seedWorkerRegistryPlan(t, database.Admin)
	seedH3CampaignResidencyPlan(t, database)
	finalizer := visibleCompletionService(t, database.DSN)

	registry, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("construct campaign cache Worker Registry: %v", err)
	}
	artifacts, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct campaign cache StageArtifact repository: %v", err)
	}
	cache, err := stagecache.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	))
	if err != nil {
		t.Fatalf("construct campaign Stage Cache repository: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	if _, err := cache.SetProjectControl(context.Background(), stagecache.ProjectControlCommand{
		OrganizationID:        uuid.MustParse(testOrganizationID),
		ProjectID:             uuid.MustParse(testProjectID),
		CachePolicyRevisionID: uuid.MustParse(h3CachePolicyID),
		Enabled:               true, MaxEntries: 100, MaxBytes: 1 << 30, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("enable campaign Project Stage Cache: %v", err)
	}

	sourceJob, sourceAttemptID := instantiateH3IntegrationGraph(
		t, database, serverURL, "h3-campaign-cache-source",
	)
	sourceStages := h3IntegrationStages(
		[]string{"campaign-cache-source-encoder", "campaign-cache-source-dit", "unused"},
		map[string]string{"encoder": "image/webp"},
	)[:2]
	for index := range sourceStages {
		sourceStages[index].residencyPlanID = uuid.MustParse(campaignResidencyPlanID)
	}
	for index := range sourceStages {
		var stageRunID uuid.UUID
		var stageVersion int64
		if err := database.Admin.QueryRow(`
			SELECT id, version
			FROM stage_runs
			WHERE attempt_id = $1 AND stage_key = $2
		`, sourceAttemptID, sourceStages[index].key).Scan(&stageRunID, &stageVersion); err != nil {
			t.Fatalf("read campaign %s cache source: %v", sourceStages[index].key, err)
		}
		assignment := assignH3IntegrationStage(
			t, database, coordinator, registry, sourceAttemptID, stageRunID,
			stageVersion, sourceStages[index], 0xe0+byte(index),
		)
		authority := signedAssignedStageAuthority(
			t, database, sourceJob, assignment, stageVersion+1,
		)
		_ = startH3IntegrationStage(t, database, assignment, authority)
		_ = materializeH3IntegrationStage(
			t, artifacts, artifactstore.NewLocal(), sourceAttemptID, stageRunID,
			assignment, sourceStages[index],
			[]byte("campaign reusable "+sourceStages[index].key+" output"),
			[]byte(`{"kind":"`+sourceStages[index].key+`","campaign":"cache-source"}`),
		)
	}
	reconciler, err := stagecache.NewH3ExactReconciler(cache, stagecache.H3ExactReconcilerConfig{
		ProjectScopeKeys: map[uuid.UUID][]byte{
			uuid.MustParse(testProjectID): []byte("0123456789abcdef0123456789abcdef"),
		},
		InputCanonicalizationRevisionID: uuid.MustParse(h3ExactCacheCanonicalizationRevisionID),
		SeedAndRNGRevision:              "sglang-minimax-h3-philox-v1",
		BatchSize:                       100,
		ExpectedSavedComputeMinor:       10_000,
		CarryCostMinor:                  10,
	})
	if err != nil {
		t.Fatalf("construct campaign H3 exact cache reconciler: %v", err)
	}
	admitted, err := reconciler.Reconcile(context.Background())
	if err != nil || admitted.AdmissionCandidates != 2 || admitted.Admitted != 2 || admitted.Hits != 0 {
		t.Fatalf("reconcile campaign H3 exact cache admissions = %#v error=%v", admitted, err)
	}

	cacheJob, cacheAttemptID := instantiateH3IntegrationGraph(
		t, database, serverURL, "h3-campaign-cache-target",
	)
	encoderHit, err := reconciler.Reconcile(context.Background())
	if err != nil || encoderHit.Hits != 1 {
		t.Fatalf("reconcile campaign Encoder exact cache hit = %#v error=%v", encoderHit, err)
	}
	ditHit, err := reconciler.Reconcile(context.Background())
	if err != nil || ditHit.Hits != 1 {
		t.Fatalf("reconcile campaign DiT exact cache hit = %#v error=%v", ditHit, err)
	}
	var exactCacheHits int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM stage_cache_references AS reference
		JOIN stage_runs AS run ON run.id = reference.owner_stage_run_id
		WHERE run.attempt_id = $1
		  AND run.stage_key IN ('encoder', 'dit')
		  AND run.state = 'SUCCEEDED'
		  AND reference.state = 'ACTIVE'
	`, cacheAttemptID).Scan(&exactCacheHits); err != nil {
		t.Fatalf("count reconciled campaign exact cache hits: %v", err)
	}
	if exactCacheHits != 2 {
		t.Fatalf("reconciled campaign exact cache hits = %d, want 2", exactCacheHits)
	}

	var vaeRunID uuid.UUID
	var vaeVersion int64
	if err := database.Admin.QueryRow(`
		SELECT id, version FROM stage_runs
		WHERE attempt_id = $1 AND stage_key = 'vae'
	`, cacheAttemptID).Scan(&vaeRunID, &vaeVersion); err != nil {
		t.Fatalf("read campaign cache target VAE StageRun: %v", err)
	}
	vae := h3IntegrationStage{
		key: "vae", stage: h3stage.StageVAEDecoder,
		profileStableID: h3stage.VAESingleGPUProfile,
		profileID:       h3VAEStageProfileID, workerProfileID: h3VAEWorkerProfileID,
		component: "h3-vae-v1", outputPort: "video",
		outputInterface: "49000000-0000-0000-0000-000000000013",
		nodeIdentity:    "campaign-cache-vae", contentType: "video/mp4",
		residencyPlanID: uuid.MustParse(campaignResidencyPlanID),
	}
	assignment := assignH3IntegrationStage(
		t, database, coordinator, registry, cacheAttemptID, vaeRunID, vaeVersion, vae, 0xf3,
	)
	authority := signedAssignedStageAuthority(t, database, cacheJob, assignment, vaeVersion+1)
	_ = startH3IntegrationStage(t, database, assignment, authority)
	_ = materializeH3IntegrationStage(
		t, artifacts, artifactstore.NewLocal(), cacheAttemptID, vaeRunID,
		assignment, vae, []byte("campaign cache VAE output"),
		[]byte(`{"kind":"video","campaign":"cache"}`),
	)
	cacheJobID := uuid.MustParse(cacheJob.JobID)
	completeH3CampaignGraph(t, finalizer, cacheJobID)

	same := runSplitH3StageGraphInEnvironmentWithContentTypes(
		t, database, coordinator, serverURL,
		[]string{"campaign-node-a", "campaign-node-a", "campaign-node-a"},
		"h3-campaign-same-node", map[string]string{"encoder": "image/webp"}, 0xc0,
		uuid.MustParse(campaignResidencyPlanID),
	)
	completeH3CampaignGraph(t, finalizer, same.jobID)
	cross := runSplitH3StageGraphInEnvironmentWithContentTypes(
		t, database, coordinator, serverURL,
		[]string{"campaign-node-b", "campaign-node-c", "campaign-node-d"},
		"h3-campaign-cross-node", map[string]string{"encoder": "image/webp"}, 0xd0,
		uuid.MustParse(campaignResidencyPlanID),
	)
	completeH3CampaignGraph(t, finalizer, cross.jobID)

	return h3CampaignEvidenceFixture{
		database: database, sameNodeJobID: same.jobID,
		crossNodeJobID: cross.jobID, cacheJobID: cacheJobID,
	}
}

func seedH3CampaignResidencyPlan(t *testing.T, database testDatabase) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		INSERT INTO residency_plan_revisions (
			id, stable_id, revision, content_digest, approval_evidence_digest,
			approved_at, approved_by
		) VALUES (
			$1, 'h3-campaign-r1', 1, decode(repeat('91', 32), 'hex'),
			decode(repeat('92', 32), 'hex'), clock_timestamp(), 'fleet/campaign-test'
		)
	`, uuid.MustParse(campaignResidencyPlanID)); err != nil {
		t.Fatalf("seed campaign ResidencyPlan: %v", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE worker_bundles
		SET residency_plan_revision_id = $1
		WHERE id = $2
	`, uuid.MustParse(campaignResidencyPlanID), uuid.MustParse(workerRegistryBundleID)); err != nil {
		t.Fatalf("bind campaign WorkerBundle to ResidencyPlan: %v", err)
	}
}

func seedH3CampaignThumbnailOutput(t *testing.T, database testDatabase) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		INSERT INTO execution_graph_outputs (
			execution_graph_revision_id, output_key, interface_revision_id,
			source_stage_key, source_port, required
		) VALUES (
			$1, 'thumbnail', '49000000-0000-0000-0000-000000000011',
			'encoder', 'conditioning', true
		)
	`, stageGraphID); err != nil {
		t.Fatalf("seed campaign thumbnail graph output: %v", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE execution_graph_revisions
		SET content_digest = vela_execution_graph_content_digest(id)
		WHERE id = $1
	`, stageGraphID); err != nil {
		t.Fatalf("refresh campaign graph content digest: %v", err)
	}
}

func completeH3CampaignGraph(
	t *testing.T,
	service *stagefinalization.Service,
	jobID uuid.UUID,
) {
	t.Helper()
	finalizer := stagefinalization.AuthenticatedFinalizer{
		ID: "spiffe://vela.internal/finalizer/h3-campaign-" + jobID.String(),
	}
	claim, err := service.ClaimNextStageGraphFinalization(context.Background(), finalizer)
	if err != nil {
		t.Fatalf("claim campaign Stage graph finalization for %s: %v", jobID, err)
	}
	if claim.Decision != stagefinalization.StageGraphFinalizationGranted || claim.JobID != jobID {
		t.Fatalf("campaign finalization claim for %s = %#v", jobID, claim)
	}
	completed, err := service.CompleteStageGraphVisibleCompletion(
		context.Background(), finalizer, claim.Credentials,
		stagefinalization.StageGraphVisibleCompletionCandidate{
			CompletionID: uuid.New(), ExpectedJobVersion: claim.JobVersion,
		},
	)
	if err != nil || completed.Decision != stagefinalization.VisibleCompletionCommitted {
		t.Fatalf("complete campaign Stage graph %s = %#v error=%v", jobID, completed, err)
	}
}

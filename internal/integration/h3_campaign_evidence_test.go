//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
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
	actuation := fleetcontroller.WorkerBundleActuation{
		SchemaVersion: 1, PlanRevisionID: planID, WorkerBundleID: bundleID,
		Namespace: "vela-system", InitImage: images[0],
		StageWorkerAgentImage:          stageImage,
		RuntimeImage:                   runtimeImage,
		StageWorkerConfigMap:           "vela-stage-worker-runtime-r1",
		StageWorkerControlTLSSecret:    "stage-worker-control-r1",
		StageWorkerAuthoritySecret:     "stage-worker-authority-r1",
		ArtifactStoreCredentialsSecret: "stage-worker-artifact-credentials-r1",
		ArtifactStoreCASecret:          "stage-worker-artifact-ca-r1",
		WorkerInstances: []fleetcontroller.WorkerInstanceActuation{{
			ID: workerID, InstanceEpoch: 1, WorkerProfileRevisionID: profileID,
			CapacityPoolID: poolID, Role: "dit", CapacitySlots: 1,
			ModelRuntimes: []fleetcontroller.ModelRuntimeProcess{{
				Component: "DIT", ModelComponentRevision: "h3-dit-v1",
				RuntimeIdentity: "h3-dit-runtime-r1", Command: []string{"/opt/vela/bin/h3-dit"},
			}},
			Members: []fleetcontroller.WorkerMemberActuation{{
				ID: uuid.MustParse("49200000-0000-0000-0000-000000000205"), MemberEpoch: 1,
				Key: "member-0", NodeIdentity: "campaign-node-a", ResourceClass: "GPU", DeviceCount: 1,
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

	type cacheSource struct {
		stageKey      string
		artifactID    uuid.UUID
		entryID       uuid.UUID
		stageProfile  uuid.UUID
		equivalenceID uuid.UUID
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
	sources := []cacheSource{
		{stageKey: "encoder", stageProfile: uuid.MustParse(encoderStageProfileID),
			equivalenceID: uuid.MustParse(h3EncoderEquivalence)},
		{stageKey: "dit", stageProfile: uuid.MustParse(ditStageProfileID),
			equivalenceID: uuid.MustParse("49000000-0000-0000-0000-000000000024")},
	}
	for index := range sources {
		var stageRunID uuid.UUID
		var stageVersion int64
		if err := database.Admin.QueryRow(`
			SELECT id, version
			FROM stage_runs
			WHERE attempt_id = $1 AND stage_key = $2
		`, sourceAttemptID, sources[index].stageKey).Scan(&stageRunID, &stageVersion); err != nil {
			t.Fatalf("read campaign %s cache source: %v", sources[index].stageKey, err)
		}
		assignment := assignH3IntegrationStage(
			t, database, coordinator, registry, sourceAttemptID, stageRunID,
			stageVersion, sourceStages[index], 0xe0+byte(index),
		)
		authority := signedAssignedStageAuthority(
			t, database, sourceJob, assignment, stageVersion+1,
		)
		_ = startH3IntegrationStage(t, database, assignment, authority)
		artifact := materializeH3IntegrationStage(
			t, artifacts, artifactstore.NewLocal(), sourceAttemptID, stageRunID,
			assignment, sourceStages[index],
			[]byte("campaign reusable "+sources[index].stageKey+" output"),
			[]byte(`{"kind":"`+sources[index].stageKey+`","campaign":"cache-source"}`),
		)
		sources[index].artifactID = artifact.ID
		key := sha256.Sum256([]byte("h3-campaign/exact/" + sources[index].stageKey))
		entryID := uuid.New()
		if _, err := cache.Admit(context.Background(), stagecache.AdmitCommand{
			CommandID: uuid.New(), EntryID: entryID, ArtifactID: sources[index].artifactID,
			CachePolicyRevisionID:       uuid.MustParse(h3CachePolicyID),
			StageProfileRevisionID:      sources[index].stageProfile,
			ResultEquivalenceRevisionID: sources[index].equivalenceID,
			Scope:                       stagecache.ScopeProject, StageKey: sources[index].stageKey,
			CacheKeyDigest: key, ExpectedSavedComputeMinor: 10_000,
			CarryCostMinor: 10, AdmittedAt: now, ExpiresAt: now.Add(time.Hour),
		}); err != nil {
			t.Fatalf("admit campaign %s exact cache entry: %v", sources[index].stageKey, err)
		}
		sources[index].entryID = entryID
	}

	cacheJob, cacheAttemptID := instantiateH3IntegrationGraph(
		t, database, serverURL, "h3-campaign-cache-target",
	)
	hitAt := time.Now().UTC().Truncate(time.Millisecond)
	for index, source := range sources {
		var targetRunID uuid.UUID
		var targetVersion int64
		if err := database.Admin.QueryRow(`
			SELECT id, version
			FROM stage_runs
			WHERE attempt_id = $1 AND stage_key = $2
		`, cacheAttemptID, source.stageKey).Scan(&targetRunID, &targetVersion); err != nil {
			t.Fatalf("read campaign cache target %s StageRun: %v", source.stageKey, err)
		}
		key := sha256.Sum256([]byte("h3-campaign/exact/" + source.stageKey))
		hit, err := cache.Hit(context.Background(), stagecache.HitCommand{
			CommandID: uuid.New(), EntryID: source.entryID, PinID: uuid.New(),
			AttemptID: cacheAttemptID, StageRunID: targetRunID,
			StageProfileRevisionID: source.stageProfile,
			ExpectedOrganizationID: uuid.MustParse(testOrganizationID),
			ExpectedProjectID:      uuid.MustParse(testProjectID),
			ExpectedAttemptFence:   1, ExpectedStageFence: 1,
			ExpectedStageVersion: targetVersion, ProgressReceiptID: uuid.New(),
			CacheKeyDigest: key, HitAt: hitAt.Add(time.Duration(index+1) * time.Millisecond),
		})
		if err != nil || hit.StageState != "SUCCEEDED" {
			t.Fatalf("hit campaign %s exact cache entry = %#v error=%v", source.stageKey, hit, err)
		}
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

//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stagecache"
	"github.com/vivym/vela/internal/workercontrol"
)

const (
	h3CachePolicyID      = "49000000-0000-0000-0000-000000000020"
	h3OrgCachePolicyID   = "49800000-0000-0000-0000-000000000020"
	h3EncoderEquivalence = "49000000-0000-0000-0000-000000000023"
	h3VAEEquivalence     = "49000000-0000-0000-0000-000000000025"
)

func TestStageCacheAdmitAndHitPinsExactArtifactAtBillableStart(t *testing.T) {
	database, coordinator, serverURL := newH3IntegrationEnvironment(t)
	seedWorkerRegistryPlan(t, database.Admin)
	registry, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_internal_login", "vela-internal-password",
	))
	if err != nil {
		t.Fatalf("construct cache fixture Worker Registry: %v", err)
	}
	artifacts, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct cache fixture StageArtifact repository: %v", err)
	}
	cache, err := stagecache.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	))
	if err != nil {
		t.Fatalf("construct Stage Cache repository: %v", err)
	}

	sourceJob, sourceAttemptID := instantiateH3IntegrationGraph(
		t, database, coordinator, serverURL, "stage-cache-source",
	)
	var sourceRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'
	`, sourceAttemptID).Scan(&sourceRunID); err != nil {
		t.Fatalf("read source Encoder StageRun: %v", err)
	}
	encoder := h3IntegrationStage{
		key: "encoder", stage: "ENCODER",
		profileStableID: "h3-encoder-single-gpu", profileID: encoderStageProfileID,
		workerProfileID: encoderWorkerProfileID, component: "h3-encoder-v1",
		outputPort:      "conditioning",
		outputInterface: "49000000-0000-0000-0000-000000000011",
		nodeIdentity:    "cache-source-node",
	}
	assignment := assignH3IntegrationStage(
		t, database, coordinator, registry, sourceAttemptID, sourceRunID, 1, encoder, 0xd1,
	)
	authority := signedAssignedStageAuthority(t, database, sourceJob, assignment, 2)
	_ = startH3IntegrationStage(t, database, assignment, authority)
	objectStore := artifactstore.NewLocal()
	sourceArtifact := materializeH3IntegrationStage(
		t, artifacts, objectStore, sourceAttemptID, sourceRunID, assignment, encoder,
		[]byte("exact reusable encoder conditioning"),
		[]byte(`{"kind":"conditioning","cache":"exact"}`),
	)

	now := time.Now().UTC().Truncate(time.Millisecond)
	control, err := cache.SetProjectControl(context.Background(), stagecache.ProjectControlCommand{
		OrganizationID:        uuid.MustParse(testOrganizationID),
		ProjectID:             uuid.MustParse(testProjectID),
		CachePolicyRevisionID: uuid.MustParse(h3CachePolicyID),
		Enabled:               true, MaxEntries: 100, MaxBytes: 1 << 30, UpdatedAt: now,
	})
	if err != nil || control.Version != 1 || !control.Enabled {
		t.Fatalf("enable Project Stage Cache = %#v error=%v", control, err)
	}
	cacheKey := sha256.Sum256([]byte("project/exact/encoder/cache-key-v1"))
	admitCommand := stagecache.AdmitCommand{
		CommandID: uuid.New(), EntryID: uuid.New(), ArtifactID: sourceArtifact.ID,
		CachePolicyRevisionID:       uuid.MustParse(h3CachePolicyID),
		StageProfileRevisionID:      uuid.MustParse(encoderStageProfileID),
		ResultEquivalenceRevisionID: uuid.MustParse(h3EncoderEquivalence),
		Scope:                       stagecache.ScopeProject, StageKey: "encoder", CacheKeyDigest: cacheKey,
		ExpectedSavedComputeMinor: 10_000, CarryCostMinor: 10,
		AdmittedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	admitted, err := cache.Admit(context.Background(), admitCommand)
	if err != nil {
		t.Fatalf("admit exact Encoder Stage Cache entry: %v", err)
	}
	replayedAdmission, err := cache.Admit(context.Background(), admitCommand)
	if err != nil {
		t.Fatalf("replay exact Encoder Stage Cache admission: %v", err)
	}
	if admitted.EntryID != admitCommand.EntryID || admitted.ArtifactID != sourceArtifact.ID ||
		admitted.ObjectVersion != sourceArtifact.ObjectVersion || admitted.Replayed ||
		admitted.Deduplicated || !replayedAdmission.Replayed ||
		replayedAdmission.EntryID != admitted.EntryID {
		t.Fatalf("Stage Cache admissions first=%#v replay=%#v", admitted, replayedAdmission)
	}

	targetJob, targetAttemptID := instantiateH3IntegrationGraph(
		t, database, coordinator, serverURL, "stage-cache-target",
	)
	var targetRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'
	`, targetAttemptID).Scan(&targetRunID); err != nil {
		t.Fatalf("read target Encoder StageRun: %v", err)
	}
	hitAt := time.Now().UTC()
	hitCommand := stagecache.HitCommand{
		CommandID: uuid.New(), EntryID: admitted.EntryID, PinID: uuid.New(),
		AttemptID: targetAttemptID, StageRunID: targetRunID,
		StageProfileRevisionID: uuid.MustParse(encoderStageProfileID),
		ExpectedOrganizationID: uuid.MustParse(testOrganizationID),
		ExpectedProjectID:      uuid.MustParse(testProjectID),
		ExpectedAttemptFence:   1, ExpectedStageFence: 1, ExpectedStageVersion: 1,
		ProgressReceiptID: uuid.New(), CacheKeyDigest: cacheKey,
		HitAt: hitAt,
	}
	hit, err := cache.Hit(context.Background(), hitCommand)
	if err != nil {
		t.Fatalf("hit exact Encoder Stage Cache entry: %v", err)
	}
	replayedHit, err := cache.Hit(context.Background(), hitCommand)
	if err != nil {
		t.Fatalf("replay exact Encoder Stage Cache hit: %v", err)
	}
	if hit.EntryID != admitted.EntryID || hit.ArtifactID != sourceArtifact.ID ||
		hit.PinID != hitCommand.PinID || hit.ObjectVersion != sourceArtifact.ObjectVersion ||
		hit.SHA256 != sourceArtifact.SHA256 || hit.StageRunID != targetRunID ||
		hit.StageState != "SUCCEEDED" || hit.StageFence != 1 || hit.StageVersion != 2 ||
		hit.Replayed || !replayedHit.Replayed || replayedHit.PinID != hit.PinID {
		t.Fatalf("Stage Cache hits first=%#v replay=%#v", hit, replayedHit)
	}

	var jobState, pinState, pinVersion string
	var billableStartedAt time.Time
	var activePins, progressReceipts int
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, job.billable_started_at,
		       pin.state::text, pin.exact_object_version,
		       (SELECT count(*) FROM stage_artifact_pins
		        WHERE id = $3 AND state = 'ACTIVE'),
		       (SELECT count(*) FROM stage_progress_receipts
		        WHERE stage_run_id = $2 AND progress_kind = 'EXACT_CACHE')
		FROM jobs AS job
		JOIN stage_artifact_pins AS pin ON pin.id = $3
		WHERE job.id = $1
	`, targetJob.JobID, targetRunID, hit.PinID).Scan(
		&jobState, &billableStartedAt, &pinState, &pinVersion,
		&activePins, &progressReceipts,
	); err != nil {
		t.Fatalf("read exact cache transaction effects: %v", err)
	}
	if jobState != "RUNNING" || !billableStartedAt.Equal(hitCommand.HitAt) ||
		pinState != "ACTIVE" || pinVersion != sourceArtifact.ObjectVersion ||
		activePins != 1 || progressReceipts != 1 {
		t.Fatalf(
			"cache effects job=%s billable=%s pin=%s/%s active=%d receipts=%d",
			jobState, billableStartedAt, pinState, pinVersion, activePins, progressReceipts,
		)
	}
}

func TestStageCacheLeafBindsExactArtifactForFinalization(t *testing.T) {
	database, coordinator, serverURL := newH3IntegrationEnvironment(t)
	seedWorkerRegistryPlan(t, database.Admin)
	registry, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_internal_login", "vela-internal-password",
	))
	if err != nil {
		t.Fatalf("construct cache-leaf Worker Registry: %v", err)
	}
	artifacts, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct cache-leaf StageArtifact repository: %v", err)
	}
	cache, err := stagecache.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	))
	if err != nil {
		t.Fatalf("construct cache-leaf repository: %v", err)
	}

	sourceJob, sourceAttemptID := instantiateH3IntegrationGraph(
		t, database, coordinator, serverURL, "stage-cache-leaf-source",
	)
	advanceUnboundIntegrationCacheStage(t, database, coordinator, sourceAttemptID, "encoder", 1, 0xe1)
	advanceUnboundIntegrationCacheStage(t, database, coordinator, sourceAttemptID, "dit", 2, 0xe2)
	var sourceVAERunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'vae'
	`, sourceAttemptID).Scan(&sourceVAERunID); err != nil {
		t.Fatalf("read source VAE StageRun: %v", err)
	}
	vae := h3IntegrationStage{
		key: "vae", stage: "VAE", profileStableID: "h3-vae-single-gpu",
		profileID: h3VAEStageProfileID, workerProfileID: h3VAEWorkerProfileID,
		component: "h3-vae-v1", outputPort: "video",
		outputInterface: "49000000-0000-0000-0000-000000000013",
		nodeIdentity:    "cache-leaf-source-node", contentType: "video/mp4",
	}
	assignment := assignH3IntegrationStage(
		t, database, coordinator, registry, sourceAttemptID, sourceVAERunID, 2, vae, 0xe3,
	)
	authority := signedAssignedStageAuthority(t, database, sourceJob, assignment, 3)
	_ = startH3IntegrationStage(t, database, assignment, authority)
	sourceArtifact := materializeH3IntegrationStage(
		t, artifacts, artifactstore.NewLocal(), sourceAttemptID, sourceVAERunID,
		assignment, vae, []byte("exact reusable VAE video"),
		[]byte(`{"kind":"video","cache":"exact"}`),
	)

	now := time.Now().UTC().Truncate(time.Millisecond)
	if _, err := cache.SetProjectControl(context.Background(), stagecache.ProjectControlCommand{
		OrganizationID: uuid.MustParse(testOrganizationID), ProjectID: uuid.MustParse(testProjectID),
		CachePolicyRevisionID: uuid.MustParse(h3CachePolicyID), Enabled: true,
		MaxEntries: 100, MaxBytes: 1 << 30, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("enable VAE Stage Cache: %v", err)
	}
	cacheKey := sha256.Sum256([]byte("project/exact/vae/cache-key-v1"))
	entryID := uuid.New()
	if _, err := cache.Admit(context.Background(), stagecache.AdmitCommand{
		CommandID: uuid.New(), EntryID: entryID, ArtifactID: sourceArtifact.ID,
		CachePolicyRevisionID:       uuid.MustParse(h3CachePolicyID),
		StageProfileRevisionID:      uuid.MustParse(h3VAEStageProfileID),
		ResultEquivalenceRevisionID: uuid.MustParse(h3VAEEquivalence),
		Scope:                       stagecache.ScopeProject, StageKey: "vae", CacheKeyDigest: cacheKey,
		ExpectedSavedComputeMinor: 20_000, CarryCostMinor: 20,
		AdmittedAt: now, ExpiresAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("admit exact VAE Stage Cache entry: %v", err)
	}

	_, targetAttemptID := instantiateH3IntegrationGraph(
		t, database, coordinator, serverURL, "stage-cache-leaf-target",
	)
	advanceUnboundIntegrationCacheStage(t, database, coordinator, targetAttemptID, "encoder", 1, 0xe4)
	advanceUnboundIntegrationCacheStage(t, database, coordinator, targetAttemptID, "dit", 2, 0xe5)
	var targetVAERunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'vae'
	`, targetAttemptID).Scan(&targetVAERunID); err != nil {
		t.Fatalf("read target VAE StageRun: %v", err)
	}
	hit, err := cache.Hit(context.Background(), stagecache.HitCommand{
		CommandID: uuid.New(), EntryID: entryID, PinID: uuid.New(),
		AttemptID: targetAttemptID, StageRunID: targetVAERunID,
		StageProfileRevisionID: uuid.MustParse(h3VAEStageProfileID),
		ExpectedOrganizationID: uuid.MustParse(testOrganizationID),
		ExpectedProjectID:      uuid.MustParse(testProjectID),
		ExpectedAttemptFence:   1, ExpectedStageFence: 1, ExpectedStageVersion: 2,
		ProgressReceiptID: uuid.New(), CacheKeyDigest: cacheKey,
		HitAt: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("hit exact VAE Stage Cache entry: %v", err)
	}
	if hit.ArtifactID != sourceArtifact.ID || hit.StageRunID != targetVAERunID ||
		hit.StageState != "SUCCEEDED" || hit.StageVersion != 3 {
		t.Fatalf("VAE Stage Cache hit = %#v", hit)
	}

	var jobState, attemptState, graphState, sourceKind, referenceState string
	var boundArtifactID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, attempt.state::text, attempt.graph_state::text,
		       binding.source_kind::text, binding.stage_artifact_id,
		       reference.state::text
		FROM attempts AS attempt
		JOIN jobs AS job ON job.id = attempt.job_id
		JOIN stage_run_output_bindings AS binding
		  ON binding.attempt_id = attempt.id AND binding.stage_run_id = $2
		JOIN stage_cache_references AS reference
		  ON reference.id = binding.stage_cache_reference_id
		WHERE attempt.id = $1
	`, targetAttemptID, targetVAERunID).Scan(
		&jobState, &attemptState, &graphState, &sourceKind, &boundArtifactID, &referenceState,
	); err != nil {
		t.Fatalf("read exact-cache StageRun output binding: %v", err)
	}
	if jobState != "FINALIZING" || attemptState != "FINALIZING" ||
		graphState != "FINALIZING" || sourceKind != "EXACT_CACHE" ||
		boundArtifactID != sourceArtifact.ID || referenceState != "ACTIVE" {
		t.Fatalf(
			"cache-leaf authority=%s/%s/%s source=%s artifact=%s reference=%s",
			jobState, attemptState, graphState, sourceKind, boundArtifactID, referenceState,
		)
	}

	finalizerService := visibleCompletionService(t, database.DSN)
	var targetClaim workercontrol.StageGraphFinalizationClaim
	for index := 0; index < 2; index++ {
		claim, err := finalizerService.ClaimNextStageGraphFinalization(
			context.Background(), workercontrol.AuthenticatedFinalizer{
				ID: "spiffe://vela.internal/finalizer/cache-leaf-" + string(rune('a'+index)),
			},
		)
		if err != nil || claim.Decision != workercontrol.StageGraphFinalizationGranted {
			t.Fatalf("claim cache-leaf finalization %d = %#v error=%v", index, claim, err)
		}
		if claim.Credentials.AttemptID == targetAttemptID {
			targetClaim = claim
		}
	}
	if targetClaim.Credentials.AttemptID != targetAttemptID ||
		targetClaim.Source.StageRunID != targetVAERunID ||
		targetClaim.Source.StageArtifactID != sourceArtifact.ID ||
		targetClaim.Source.ObjectVersion != sourceArtifact.ObjectVersion {
		t.Fatalf("target cache-leaf finalization claim = %#v", targetClaim)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err = goose.DownTo(database.Admin, migrations, 47)
	assertPostgresConstraint(t, err, "stage_cutover_control_rollback_is_unsafe")
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil || version != 49 {
		t.Fatalf("StageRun output binding version after refused Down = %d error=%v", version, versionErr)
	}
}

func TestStageRunOutputBindingMigrationRoundTrip(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")

	if err := goose.DownTo(database.Admin, migrations, 47); err != nil {
		t.Fatalf("migrate empty StageRun output binding down: %v", err)
	}
	assertTableDoesNotExist(t, database.Admin, "stage_run_output_bindings")
	if err := goose.UpTo(database.Admin, migrations, 48); err != nil {
		t.Fatalf("migrate StageRun output binding up again: %v", err)
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 48 {
		t.Fatalf("StageRun output binding version after Down Up = %d error=%v", version, err)
	}
}

func TestStageCacheDeletionWaitsForExactExecutionPin(t *testing.T) {
	fixture := newPinnedStageCacheFixture(t, "stage-cache-deletion")
	requestedAt := time.Now().UTC()
	deleteCommand := stagecache.DeletionCommand{
		CommandID: uuid.New(), OrganizationID: uuid.MustParse(testOrganizationID),
		ProjectID: uuid.MustParse(testProjectID), SourceJobID: fixture.sourceJobID,
		RequestedAt: requestedAt,
	}
	deleted, err := fixture.cache.RequestDeletion(context.Background(), deleteCommand)
	if err != nil {
		t.Fatalf("request pinned Stage Cache deletion: %v", err)
	}
	replayedDeletion, err := fixture.cache.RequestDeletion(context.Background(), deleteCommand)
	if err != nil {
		t.Fatalf("replay pinned Stage Cache deletion: %v", err)
	}
	if deleted.DeletedCount != 0 || deleted.BlockedCount != 1 || deleted.Replayed ||
		!replayedDeletion.Replayed || replayedDeletion.BlockedCount != 1 {
		t.Fatalf("Stage Cache deletion first=%#v replay=%#v", deleted, replayedDeletion)
	}

	lateJob, lateAttemptID := instantiateH3IntegrationGraph(
		t, fixture.database, fixture.coordinator, fixture.serverURL,
		"stage-cache-late-hit-after-deletion",
	)
	var lateRunID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'
	`, lateAttemptID).Scan(&lateRunID); err != nil {
		t.Fatalf("read late cache-hit Encoder StageRun: %v", err)
	}
	_, err = fixture.cache.Hit(context.Background(), stagecache.HitCommand{
		CommandID: uuid.New(), EntryID: fixture.entryID, PinID: uuid.New(),
		AttemptID: lateAttemptID, StageRunID: lateRunID,
		StageProfileRevisionID: uuid.MustParse(encoderStageProfileID),
		ExpectedOrganizationID: uuid.MustParse(testOrganizationID),
		ExpectedProjectID:      uuid.MustParse(testProjectID),
		ExpectedAttemptFence:   1, ExpectedStageFence: 1, ExpectedStageVersion: 1,
		ProgressReceiptID: uuid.New(), CacheKeyDigest: fixture.cacheKey,
		HitAt: time.Now().UTC(),
	})
	assertPostgresConstraint(t, err, "stage_cache_entry_unavailable")
	var latePins int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*) FROM stage_artifact_pins WHERE owner_job_id = $1
	`, lateJob.JobID).Scan(&latePins); err != nil || latePins != 0 {
		t.Fatalf("late cache hit pins = %d error=%v, want 0", latePins, err)
	}

	releasedAt := requestedAt.Add(time.Millisecond)
	releaseCommand := stagecache.ReleasePinCommand{
		CommandID: uuid.New(), PinID: fixture.hit.PinID,
		OwnerJobID: fixture.targetJobID, OwnerStageRunID: fixture.targetRunID,
		ReleaseReason: "STAGE_INPUT_CONSUMED", ReleasedAt: releasedAt,
	}
	released, err := fixture.cache.ReleaseExecutionPin(context.Background(), releaseCommand)
	if err != nil {
		t.Fatalf("release Stage Cache ExecutionPin: %v", err)
	}
	replayedRelease, err := fixture.cache.ReleaseExecutionPin(context.Background(), releaseCommand)
	if err != nil {
		t.Fatalf("replay Stage Cache ExecutionPin release: %v", err)
	}
	if released.PinID != fixture.hit.PinID || !released.ReleasedAt.Equal(releasedAt) ||
		released.Replayed || !replayedRelease.Replayed {
		t.Fatalf("Stage Cache pin release first=%#v replay=%#v", released, replayedRelease)
	}
	reconciled, err := fixture.cache.ReconcileDeletions(
		context.Background(), releasedAt.Add(time.Millisecond), 100,
	)
	if err != nil || reconciled != 0 {
		t.Fatalf("reconcile while source graph pin is active = %d error=%v, want 0", reconciled, err)
	}
	var sourcePinID, sourceOwnerRunID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT id, owner_stage_run_id
		FROM stage_artifact_pins
		WHERE stage_artifact_id = $1 AND owner_job_id = $2 AND state = 'ACTIVE'
	`, fixture.hit.ArtifactID, fixture.sourceJobID).Scan(
		&sourcePinID, &sourceOwnerRunID,
	); err != nil {
		t.Fatalf("read source graph ExecutionPin: %v", err)
	}
	_, err = fixture.cache.ReleaseExecutionPin(context.Background(), stagecache.ReleasePinCommand{
		CommandID: uuid.New(), PinID: sourcePinID, OwnerJobID: fixture.sourceJobID,
		OwnerStageRunID: sourceOwnerRunID, ReleaseReason: "FORGED_CACHE_RELEASE",
		ReleasedAt: releasedAt.Add(2 * time.Millisecond),
	})
	assertPostgresConstraint(t, err, "stage_cache_pin_release_stale")
	if _, err := fixture.database.Admin.Exec(`
		UPDATE stage_artifact_pins
		SET state = 'RELEASED', released_at = $2,
		    release_reason = 'SOURCE_GRAPH_CLEANUP'
		WHERE id = $1 AND state = 'ACTIVE'
	`, sourcePinID, releasedAt.Add(3*time.Millisecond)); err != nil {
		t.Fatalf("simulate source graph pin cleanup: %v", err)
	}
	reconciled, err = fixture.cache.ReconcileDeletions(
		context.Background(), releasedAt.Add(4*time.Millisecond), 100,
	)
	if err != nil || reconciled != 1 {
		t.Fatalf("reconcile unpinned Stage Cache deletion = %d error=%v, want 1", reconciled, err)
	}
	var entryState, pinState, exactVersion string
	if err := fixture.database.Admin.QueryRow(`
		SELECT entry.state::text, pin.state::text, pin.exact_object_version
		FROM stage_cache_entries AS entry
		JOIN stage_artifact_pins AS pin ON pin.id = $2
		WHERE entry.id = $1
	`, fixture.entryID, fixture.hit.PinID).Scan(
		&entryState, &pinState, &exactVersion,
	); err != nil {
		t.Fatalf("read reconciled Stage Cache deletion: %v", err)
	}
	if entryState != "DELETED" || pinState != "RELEASED" ||
		exactVersion != fixture.hit.ObjectVersion {
		t.Fatalf(
			"reconciled Stage Cache entry/pin/version = %s/%s/%s",
			entryState, pinState, exactVersion,
		)
	}
}

func TestStageCacheScopeAndPolicyFailClosed(t *testing.T) {
	fixture := newPinnedStageCacheFixture(t, "stage-cache-scope")
	now := time.Now().UTC()
	if _, err := fixture.cache.SetProjectControl(
		context.Background(),
		stagecache.ProjectControlCommand{
			OrganizationID:        uuid.MustParse(testOrganizationID),
			ProjectID:             uuid.MustParse(testProjectID),
			CachePolicyRevisionID: uuid.MustParse(h3CachePolicyID),
			Enabled:               false, MaxEntries: 100, MaxBytes: 1 << 30, UpdatedAt: now,
		},
	); err != nil {
		t.Fatalf("disable Project Stage Cache lookup: %v", err)
	}
	disabledJob, disabledAttemptID := instantiateH3IntegrationGraph(
		t, fixture.database, fixture.coordinator, fixture.serverURL,
		"stage-cache-disabled-target",
	)
	var disabledRunID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'
	`, disabledAttemptID).Scan(&disabledRunID); err != nil {
		t.Fatalf("read disabled-cache target Encoder StageRun: %v", err)
	}
	_, err := fixture.cache.Hit(context.Background(), stagecache.HitCommand{
		CommandID: uuid.New(), EntryID: fixture.entryID, PinID: uuid.New(),
		AttemptID: disabledAttemptID, StageRunID: disabledRunID,
		StageProfileRevisionID: uuid.MustParse(encoderStageProfileID),
		ExpectedOrganizationID: uuid.MustParse(testOrganizationID),
		ExpectedProjectID:      uuid.MustParse(testProjectID),
		ExpectedAttemptFence:   1, ExpectedStageFence: 1, ExpectedStageVersion: 1,
		ProgressReceiptID: uuid.New(), CacheKeyDigest: fixture.cacheKey,
		HitAt: time.Now().UTC(),
	})
	assertPostgresConstraint(t, err, "stage_cache_policy_disabled")
	var disabledPins int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*) FROM stage_artifact_pins WHERE owner_job_id = $1
	`, disabledJob.JobID).Scan(&disabledPins); err != nil || disabledPins != 0 {
		t.Fatalf("disabled cache hit pins = %d error=%v, want 0", disabledPins, err)
	}

	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO stage_cache_policy_revisions (
			id, stable_id, revision, state, allowed_stage_keys, scope_ceiling,
			ttl_seconds, quota_policy, encryption_policy, deletion_policy, content_digest
		) VALUES (
			$1, 'h3-exact-cache-organization', 1, 'CERTIFIED', ARRAY['encoder'],
			'ORGANIZATION', 86400, '{}', '{}', '{}', decode(repeat('98', 32), 'hex')
		)
	`, h3OrgCachePolicyID); err != nil {
		t.Fatalf("seed Organization Stage Cache policy: %v", err)
	}
	organizationKey := sha256.Sum256([]byte("organization/exact/encoder/cache-key-v1"))
	organizationAdmit := stagecache.AdmitCommand{
		CommandID: uuid.New(), EntryID: uuid.New(), ArtifactID: fixture.hit.ArtifactID,
		CachePolicyRevisionID:       uuid.MustParse(h3OrgCachePolicyID),
		StageProfileRevisionID:      uuid.MustParse(encoderStageProfileID),
		ResultEquivalenceRevisionID: uuid.MustParse(h3EncoderEquivalence),
		Scope:                       stagecache.ScopeOrganization, StageKey: "encoder",
		CacheKeyDigest: organizationKey, ExpectedSavedComputeMinor: 10_000,
		CarryCostMinor: 10, AdmittedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	_, err = fixture.cache.Admit(context.Background(), organizationAdmit)
	assertPostgresConstraint(t, err, "stage_cache_policy_disabled")
	authorization, err := fixture.cache.AuthorizeOrganization(
		context.Background(),
		stagecache.OrganizationAuthorizationCommand{
			OrganizationID:        uuid.MustParse(testOrganizationID),
			CachePolicyRevisionID: uuid.MustParse(h3OrgCachePolicyID),
			Enabled:               true, MaxEntries: 100, MaxBytes: 1 << 30,
			UpdatedAt: now.Add(time.Millisecond),
		},
	)
	if err != nil || authorization.Version != 1 || !authorization.Enabled {
		t.Fatalf("authorize Organization Stage Cache = %#v error=%v", authorization, err)
	}
	organizationAdmit.CommandID = uuid.New()
	admitted, err := fixture.cache.Admit(context.Background(), organizationAdmit)
	if err != nil || admitted.EntryID != organizationAdmit.EntryID {
		t.Fatalf("admit Organization Stage Cache = %#v error=%v", admitted, err)
	}

	_, isolatedAttemptID := instantiateH3IntegrationGraph(
		t, fixture.database, fixture.coordinator, fixture.serverURL,
		"stage-cache-forged-organization",
	)
	var isolatedRunID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'
	`, isolatedAttemptID).Scan(&isolatedRunID); err != nil {
		t.Fatalf("read Organization-cache target Encoder StageRun: %v", err)
	}
	hitCommand := stagecache.HitCommand{
		CommandID: uuid.New(), EntryID: admitted.EntryID, PinID: uuid.New(),
		AttemptID: isolatedAttemptID, StageRunID: isolatedRunID,
		StageProfileRevisionID: uuid.MustParse(encoderStageProfileID),
		ExpectedOrganizationID: uuid.New(), ExpectedProjectID: uuid.MustParse(testProjectID),
		ExpectedAttemptFence: 1, ExpectedStageFence: 1, ExpectedStageVersion: 1,
		ProgressReceiptID: uuid.New(), CacheKeyDigest: organizationKey,
		HitAt: time.Now().UTC(),
	}
	_, err = fixture.cache.Hit(context.Background(), hitCommand)
	assertPostgresConstraint(t, err, "stage_cache_entry_unavailable")
	hitCommand.CommandID = uuid.New()
	hitCommand.PinID = uuid.New()
	hitCommand.ProgressReceiptID = uuid.New()
	hitCommand.ExpectedOrganizationID = uuid.MustParse(testOrganizationID)
	hitCommand.ExpectedProjectID = uuid.New()
	_, err = fixture.cache.Hit(context.Background(), hitCommand)
	assertPostgresConstraint(t, err, "stage_cache_entry_unavailable")
	var forgedPins int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*) FROM stage_artifact_pins WHERE owner_stage_run_id = $1
	`, isolatedRunID).Scan(&forgedPins); err != nil || forgedPins != 0 {
		t.Fatalf("forged Organization cache hit pins = %d error=%v, want 0", forgedPins, err)
	}
}

func TestStageCacheCancellationBlocksLateAdmission(t *testing.T) {
	fixture := newPinnedStageCacheFixture(t, "stage-cache-cancellation")
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cache cancellation scope: %v", err)
	}
	canceled := cancelJob(
		t, fixture.serverURL, testProjectID, fixture.sourceJobID.String(),
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel cache source Job status = %d body=%s", canceled.StatusCode, canceled.Body)
	}
	lateKey := sha256.Sum256([]byte("late/cache/admission/after/cancellation"))
	_, err := fixture.cache.Admit(context.Background(), stagecache.AdmitCommand{
		CommandID: uuid.New(), EntryID: uuid.New(), ArtifactID: fixture.hit.ArtifactID,
		CachePolicyRevisionID:       uuid.MustParse(h3CachePolicyID),
		StageProfileRevisionID:      uuid.MustParse(encoderStageProfileID),
		ResultEquivalenceRevisionID: uuid.MustParse(h3EncoderEquivalence),
		Scope:                       stagecache.ScopeProject, StageKey: "encoder", CacheKeyDigest: lateKey,
		ExpectedSavedComputeMinor: 10_000, CarryCostMinor: 10,
		AdmittedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	assertPostgresConstraint(t, err, "stage_cache_admit_authority_stale")
	var lateEntries int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*) FROM stage_cache_entries WHERE cache_key_digest = $1
	`, lateKey[:]).Scan(&lateEntries); err != nil || lateEntries != 0 {
		t.Fatalf("late canceled cache entries = %d error=%v, want 0", lateEntries, err)
	}
}

func TestStageCacheConcurrentAdmissionCannotExceedQuota(t *testing.T) {
	fixture := newPinnedStageCacheFixture(t, "stage-cache-quota")
	now := time.Now().UTC()
	if _, err := fixture.cache.SetProjectControl(
		context.Background(),
		stagecache.ProjectControlCommand{
			OrganizationID:        uuid.MustParse(testOrganizationID),
			ProjectID:             uuid.MustParse(testProjectID),
			CachePolicyRevisionID: uuid.MustParse(h3CachePolicyID),
			Enabled:               true, MaxEntries: 2, MaxBytes: 1 << 30, UpdatedAt: now,
		},
	); err != nil {
		t.Fatalf("set Stage Cache concurrency quota: %v", err)
	}
	commands := []stagecache.AdmitCommand{
		{
			CommandID: uuid.New(), EntryID: uuid.New(), ArtifactID: fixture.hit.ArtifactID,
			CachePolicyRevisionID:       uuid.MustParse(h3CachePolicyID),
			StageProfileRevisionID:      uuid.MustParse(encoderStageProfileID),
			ResultEquivalenceRevisionID: uuid.MustParse(h3EncoderEquivalence),
			Scope:                       stagecache.ScopeProject, StageKey: "encoder",
			CacheKeyDigest:            sha256.Sum256([]byte("quota/concurrent/a")),
			ExpectedSavedComputeMinor: 10_000, CarryCostMinor: 10,
			AdmittedAt: now, ExpiresAt: now.Add(time.Hour),
		},
		{
			CommandID: uuid.New(), EntryID: uuid.New(), ArtifactID: fixture.hit.ArtifactID,
			CachePolicyRevisionID:       uuid.MustParse(h3CachePolicyID),
			StageProfileRevisionID:      uuid.MustParse(encoderStageProfileID),
			ResultEquivalenceRevisionID: uuid.MustParse(h3EncoderEquivalence),
			Scope:                       stagecache.ScopeProject, StageKey: "encoder",
			CacheKeyDigest:            sha256.Sum256([]byte("quota/concurrent/b")),
			ExpectedSavedComputeMinor: 10_000, CarryCostMinor: 10,
			AdmittedAt: now, ExpiresAt: now.Add(time.Hour),
		},
	}
	start := make(chan struct{})
	errorsByCommand := make([]error, len(commands))
	var wait sync.WaitGroup
	for index := range commands {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			_, errorsByCommand[index] = fixture.cache.Admit(
				context.Background(), commands[index],
			)
		}(index)
	}
	close(start)
	wait.Wait()
	successes := 0
	quotaErrors := 0
	for _, err := range errorsByCommand {
		if err == nil {
			successes++
			continue
		}
		assertPostgresConstraint(t, err, "stage_cache_quota_exceeded")
		quotaErrors++
	}
	var entries int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*) FROM stage_cache_entries
		WHERE scope = 'PROJECT' AND scope_project_id = $1 AND state = 'LIVE'
	`, testProjectID).Scan(&entries); err != nil {
		t.Fatalf("count quota-controlled Stage Cache entries: %v", err)
	}
	if successes != 1 || quotaErrors != 1 || entries != 2 {
		t.Fatalf(
			"concurrent Stage Cache quota successes/errors/entries = %d/%d/%d, want 1/1/2",
			successes, quotaErrors, entries,
		)
	}
}

func TestStageCacheRejectsProfileEquivalenceMismatch(t *testing.T) {
	fixture := newPinnedStageCacheFixture(t, "stage-cache-equivalence")
	now := time.Now().UTC()
	mismatchedKey := sha256.Sum256([]byte("mismatched/profile/equivalence"))
	_, err := fixture.cache.Admit(context.Background(), stagecache.AdmitCommand{
		CommandID: uuid.New(), EntryID: uuid.New(), ArtifactID: fixture.hit.ArtifactID,
		CachePolicyRevisionID:       uuid.MustParse(h3CachePolicyID),
		StageProfileRevisionID:      uuid.MustParse(encoderStageProfileID),
		ResultEquivalenceRevisionID: uuid.MustParse("49000000-0000-0000-0000-000000000024"),
		Scope:                       stagecache.ScopeProject, StageKey: "encoder", CacheKeyDigest: mismatchedKey,
		ExpectedSavedComputeMinor: 10_000, CarryCostMinor: 10,
		AdmittedAt: now, ExpiresAt: now.Add(time.Hour),
	})
	assertPostgresConstraint(t, err, "stage_cache_admit_authority_stale")
}

func TestStageCacheMigrationRoundTripAndDurableEvidenceRefusal(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		if err := goose.DownTo(database.Admin, migrations, 43); err != nil {
			t.Fatalf("migrate empty Stage Cache down: %v", err)
		}
		assertTableDoesNotExist(t, database.Admin, "stage_cache_entries")
		assertTableDoesNotExist(t, database.Admin, "stage_cache_references")
		if err := goose.UpTo(database.Admin, migrations, 44); err != nil {
			t.Fatalf("migrate Stage Cache up again: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 44 {
			t.Fatalf("Stage Cache version after Down Up = %d error=%v", version, err)
		}
	})

	t.Run("durable evidence refuses Down", func(t *testing.T) {
		fixture := newPinnedStageCacheFixture(t, "stage-cache-migration-refusal")
		err := goose.DownTo(fixture.database.Admin, migrations, 43)
		assertPostgresConstraint(t, err, "stage_cutover_control_rollback_is_unsafe")
		version, versionErr := goose.GetDBVersion(fixture.database.Admin)
		if versionErr != nil || version != 49 {
			t.Fatalf(
				"Stage Cache version after refused Down = %d error=%v",
				version, versionErr,
			)
		}
		var entries, commands, pins, bindings int
		if err := fixture.database.Admin.QueryRow(`
			SELECT
				(SELECT count(*) FROM stage_cache_entries),
				(SELECT count(*) FROM stage_cache_commands),
				(SELECT count(*) FROM stage_artifact_pins WHERE id = $1),
				(SELECT count(*) FROM stage_run_output_bindings
				 WHERE stage_run_id = $2 AND stage_artifact_id = $3)
		`, fixture.hit.PinID, fixture.targetRunID, fixture.hit.ArtifactID).Scan(
			&entries, &commands, &pins, &bindings,
		); err != nil {
			t.Fatalf("read Stage Cache evidence after refused Down: %v", err)
		}
		if entries != 1 || commands != 2 || pins != 1 || bindings != 1 {
			t.Fatalf(
				"Stage Cache evidence after refused Down entries/commands/pins/bindings = %d/%d/%d/%d",
				entries, commands, pins, bindings,
			)
		}
	})
}

func TestStageCacheExactIdentityAndCommandEvidenceAreImmutable(t *testing.T) {
	fixture := newPinnedStageCacheFixture(t, "stage-cache-immutability")
	for _, test := range []struct {
		name       string
		statement  string
		argument   any
		constraint string
	}{
		{
			name: "entry exact object version",
			statement: `UPDATE stage_cache_entries
				SET exact_object_version = 'forged-version' WHERE id = $1`,
			argument: fixture.entryID, constraint: "stage_cache_entry_identity_immutable",
		},
		{
			name: "reference pin identity",
			statement: `UPDATE stage_cache_references
				SET execution_pin_id = NULL WHERE execution_pin_id = $1`,
			argument: fixture.hit.PinID, constraint: "stage_cache_reference_identity_immutable",
		},
		{
			name: "command result",
			statement: `UPDATE stage_cache_commands
				SET result = result || '{"forged":true}'::jsonb
				WHERE result ->> 'entry_id' = $1::text`,
			argument: fixture.entryID, constraint: "stage_cache_command_immutable",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := fixture.database.Admin.Exec(test.statement, test.argument)
			assertPostgresConstraint(t, err, test.constraint)
		})
	}
}

type pinnedStageCacheFixture struct {
	database    testDatabase
	coordinator *attemptcoordinator.Service
	serverURL   string
	cache       *stagecache.PostgresRepository
	sourceJobID uuid.UUID
	entryID     uuid.UUID
	cacheKey    stagecache.Digest
	targetJobID uuid.UUID
	targetRunID uuid.UUID
	hit         stagecache.HitDecision
}

func newPinnedStageCacheFixture(t *testing.T, idempotencyPrefix string) pinnedStageCacheFixture {
	t.Helper()
	database, coordinator, serverURL := newH3IntegrationEnvironment(t)
	seedWorkerRegistryPlan(t, database.Admin)
	registry, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_internal_login", "vela-internal-password",
	))
	if err != nil {
		t.Fatalf("construct pinned-cache Worker Registry: %v", err)
	}
	artifacts, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct pinned-cache StageArtifact repository: %v", err)
	}
	cache, err := stagecache.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	))
	if err != nil {
		t.Fatalf("construct pinned Stage Cache repository: %v", err)
	}
	sourceJob, sourceAttemptID := instantiateH3IntegrationGraph(
		t, database, coordinator, serverURL, idempotencyPrefix+"-source",
	)
	var sourceRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'
	`, sourceAttemptID).Scan(&sourceRunID); err != nil {
		t.Fatalf("read pinned-cache source Encoder StageRun: %v", err)
	}
	encoder := h3IntegrationStage{
		key: "encoder", stage: "ENCODER",
		profileStableID: "h3-encoder-single-gpu", profileID: encoderStageProfileID,
		workerProfileID: encoderWorkerProfileID, component: "h3-encoder-v1",
		outputPort:      "conditioning",
		outputInterface: "49000000-0000-0000-0000-000000000011",
		nodeIdentity:    "pinned-cache-source-node",
	}
	assignment := assignH3IntegrationStage(
		t, database, coordinator, registry, sourceAttemptID, sourceRunID, 1, encoder, 0xd2,
	)
	authority := signedAssignedStageAuthority(t, database, sourceJob, assignment, 2)
	_ = startH3IntegrationStage(t, database, assignment, authority)
	sourceArtifact := materializeH3IntegrationStage(
		t, artifacts, artifactstore.NewLocal(), sourceAttemptID, sourceRunID,
		assignment, encoder, []byte("pinned exact encoder conditioning"),
		[]byte(`{"kind":"conditioning","cache":"pinned"}`),
	)
	now := time.Now().UTC()
	if _, err := cache.SetProjectControl(context.Background(), stagecache.ProjectControlCommand{
		OrganizationID:        uuid.MustParse(testOrganizationID),
		ProjectID:             uuid.MustParse(testProjectID),
		CachePolicyRevisionID: uuid.MustParse(h3CachePolicyID),
		Enabled:               true, MaxEntries: 100, MaxBytes: 1 << 30, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("enable pinned Project Stage Cache: %v", err)
	}
	cacheKey := sha256.Sum256([]byte(idempotencyPrefix + "/exact/encoder"))
	entryID := uuid.New()
	if _, err := cache.Admit(context.Background(), stagecache.AdmitCommand{
		CommandID: uuid.New(), EntryID: entryID, ArtifactID: sourceArtifact.ID,
		CachePolicyRevisionID:       uuid.MustParse(h3CachePolicyID),
		StageProfileRevisionID:      uuid.MustParse(encoderStageProfileID),
		ResultEquivalenceRevisionID: uuid.MustParse(h3EncoderEquivalence),
		Scope:                       stagecache.ScopeProject, StageKey: "encoder", CacheKeyDigest: cacheKey,
		ExpectedSavedComputeMinor: 10_000, CarryCostMinor: 10,
		AdmittedAt: now, ExpiresAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("admit pinned Stage Cache entry: %v", err)
	}
	targetJob, targetAttemptID := instantiateH3IntegrationGraph(
		t, database, coordinator, serverURL, idempotencyPrefix+"-target",
	)
	var targetRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'
	`, targetAttemptID).Scan(&targetRunID); err != nil {
		t.Fatalf("read pinned-cache target Encoder StageRun: %v", err)
	}
	hit, err := cache.Hit(context.Background(), stagecache.HitCommand{
		CommandID: uuid.New(), EntryID: entryID, PinID: uuid.New(),
		AttemptID: targetAttemptID, StageRunID: targetRunID,
		StageProfileRevisionID: uuid.MustParse(encoderStageProfileID),
		ExpectedOrganizationID: uuid.MustParse(testOrganizationID),
		ExpectedProjectID:      uuid.MustParse(testProjectID),
		ExpectedAttemptFence:   1, ExpectedStageFence: 1, ExpectedStageVersion: 1,
		ProgressReceiptID: uuid.New(), CacheKeyDigest: cacheKey, HitAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("pin Stage Cache entry: %v", err)
	}
	return pinnedStageCacheFixture{
		database: database, coordinator: coordinator, serverURL: serverURL, cache: cache,
		sourceJobID: uuid.MustParse(sourceJob.JobID), entryID: entryID, cacheKey: cacheKey,
		targetJobID: uuid.MustParse(targetJob.JobID), targetRunID: targetRunID, hit: hit,
	}
}

func advanceUnboundIntegrationCacheStage(
	t *testing.T,
	database testDatabase,
	coordinator *attemptcoordinator.Service,
	attemptID uuid.UUID,
	stageKey string,
	expectedVersion int64,
	digestByte byte,
) uuid.UUID {
	t.Helper()
	var stageRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = $2
	`, attemptID, stageKey).Scan(&stageRunID); err != nil {
		t.Fatalf("read unbound-cache %s StageRun: %v", stageKey, err)
	}
	decision, err := coordinator.Apply(context.Background(), attemptcoordinator.ExactCacheAdvanceCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: stageRunID,
		ExpectedAttemptFence: 1, ExpectedStageFence: 1,
		ExpectedStageVersion: expectedVersion, ProgressReceiptID: uuid.New(),
		CacheSourceIdentity: "unbound-integration-cache/" + stageKey,
		OutputDigest:        bytesOf(digestByte, sha256.Size),
		AdvancedAt:          time.Now().UTC().Truncate(time.Millisecond),
	})
	if err != nil {
		t.Fatalf("advance unbound-cache %s StageRun: %v", stageKey, err)
	}
	if decision.State != "SUCCEEDED" || decision.StageVersion != expectedVersion+1 {
		t.Fatalf("unbound-cache %s decision = %#v", stageKey, decision)
	}
	return stageRunID
}

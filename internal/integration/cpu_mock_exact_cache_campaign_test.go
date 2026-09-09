//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/stagecache"
	"github.com/vivym/vela/internal/stagefinalization"
)

func TestCPUMockExactCacheSourceTargetCampaign(t *testing.T) {
	runCPUMockRuntimeCampaign(t, cpuCampaignMode{exactCache: true})
}

func TestCPUMockExactCacheProductionLoopCampaign(t *testing.T) {
	runCPUMockRuntimeCampaign(t, cpuCampaignMode{exactCache: true, durableStream: true, productionLoop: true})
}

type cpuExactCacheBinding struct {
	Stage            string    `json:"stage"`
	SourceArtifactID uuid.UUID `json:"source_artifact_id"`
	ObjectVersion    string    `json:"object_version"`
	PinID            uuid.UUID `json:"pin_id"`
}

type cpuExactPublicCopy struct {
	JobID         uuid.UUID `json:"job_id"`
	Kind          string    `json:"kind"`
	ObjectKey     string    `json:"object_key"`
	ObjectVersion string    `json:"object_version"`
	SHA256        string    `json:"sha256"`
	SizeBytes     int64     `json:"size_bytes"`
}

func runCPUExactCacheSourceTarget(t *testing.T, ctx context.Context, database testDatabase,
	serverURL, root string, store *artifactstore.Local, workers []*cpuLoadWorker,
	finalizer *stagefinalization.Service, maintenance *retention.Reconciler,
	budget cpuLoadBudget, sourceDigest string, binaryDigests map[string]string, ffprobeVersion string) {
	t.Helper()
	started := time.Now().UTC()
	before := observeCPULoad(t, database, root)
	before.Processes = observeCPULoadProcesses(t)
	if len(workers) != 4 || len(before.Processes) != 5 {
		t.Fatalf("expected four native workers: %+v", before.Processes)
	}
	cache, err := stagecache.NewPostgresRepository(newRolePool(t, database.DSN,
		"vela_attempt_coordinator_login", "vela-attempt-coordinator-password"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.SetProjectControl(ctx, stagecache.ProjectControlCommand{
		OrganizationID: uuid.MustParse(testOrganizationID), ProjectID: uuid.MustParse(testProjectID),
		CachePolicyRevisionID: uuid.MustParse(h3CachePolicyID), Enabled: true,
		MaxEntries: 100, MaxBytes: 1 << 30, UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	productionLoop := len(workers) > 0 && workers[0].durable != nil && workers[0].durable.production != nil
	if productionLoop {
		for _, worker := range workers {
			worker.durable.control.armed.Store(true)
		}
	}
	loopCtx, cancelLoops := context.WithCancel(ctx)
	defer cancelLoops()
	var loops sync.WaitGroup
	loopErrors := make(chan error, len(workers))
	if productionLoop {
		for _, worker := range workers {
			worker := worker
			loops.Add(1)
			go func() {
				defer loops.Done()
				if err := worker.durable.production.Run(loopCtx); err != nil && loopCtx.Err() == nil {
					loopErrors <- fmt.Errorf("%s ProductionAgent.Run: %w", worker.stage.key, err)
				}
			}()
		}
		waitCPUProductionReady(t, ctx, workers)
	}
	source, sourceAttempt := instantiateH3IntegrationGraph(t, database, serverURL, "cpu-exact-source")
	if !productionLoop {
		for _, worker := range workers {
			executeCPUExactCacheStage(t, ctx, worker, source.JobID)
		}
	} else {
		waitCPUExactCacheStages(t, ctx, database, sourceAttempt.String(), 4, loopErrors)
	}
	completeCPUExactCacheJob(t, ctx, finalizer, uuid.MustParse(source.JobID))
	afterSource := waitForCPUExactCacheObservation(t, ctx, database, root, func(observation cpuLoadObservation) bool {
		return observation.ScratchBytes == 0 && observation.RuntimeWatchdogs == 0
	})
	afterSource.Processes = observeCPULoadProcesses(t)
	assertCPULoadResidentProcesses(t, before.Processes, afterSource.Processes)
	if afterSource.ScratchBytes != 0 || afterSource.RuntimeWatchdogs != 0 {
		t.Fatalf("source runtime did not drain: %+v", afterSource)
	}
	reconciler, err := stagecache.NewH3ExactReconciler(cache, stagecache.H3ExactReconcilerConfig{
		ProjectScopeKeys:                map[uuid.UUID][]byte{uuid.MustParse(testProjectID): []byte("0123456789abcdef0123456789abcdef")},
		InputCanonicalizationRevisionID: uuid.MustParse(h3ExactCacheCanonicalizationRevisionID),
		SeedAndRNGRevision:              "sglang-minimax-h3-philox-v1", BatchSize: 100,
		ExpectedSavedComputeMinor: 10000, CarryCostMinor: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := reconciler.Reconcile(ctx)
	if err != nil || admitted.Admitted != 2 || admitted.Hits != 0 {
		t.Fatalf("admit actual native source: %+v err=%v", admitted, err)
	}
	target, targetAttempt := instantiateH3IntegrationGraph(t, database, serverURL, "cpu-exact-target")
	if source.JobID == target.JobID || sourceAttempt == targetAttempt {
		t.Fatal("source/target identities are not distinct")
	}
	var requestMatches bool
	if err := database.Admin.QueryRow(`SELECT a.request_hash=b.request_hash FROM jobs a,jobs b WHERE a.id=$1 AND b.id=$2`, source.JobID, target.JobID).Scan(&requestMatches); err != nil || !requestMatches {
		t.Fatalf("source/target frozen inputs differ: %v", err)
	}
	var hits []stagecache.H3ExactReconcileResult
	for range 2 {
		hit, err := reconciler.Reconcile(ctx)
		if err != nil || hit.Hits != 1 {
			t.Fatalf("actual native-source cache hit: %+v err=%v", hit, err)
		}
		hits = append(hits, hit)
	}
	rows, err := database.Admin.Query(`SELECT run.stage_key,artifact.id,artifact.object_version,pin.id
		FROM stage_run_output_bindings binding JOIN stage_runs run ON run.id=binding.stage_run_id
		JOIN stage_artifacts artifact ON artifact.id=binding.stage_artifact_id
		JOIN stage_cache_references reference ON reference.id=binding.stage_cache_reference_id
		JOIN stage_artifact_pins pin ON pin.id=reference.execution_pin_id
		WHERE binding.attempt_id=$1 AND binding.source_kind='EXACT_CACHE' AND run.state='SUCCEEDED'
		AND artifact.job_id=$2 AND pin.state='ACTIVE' AND reference.state='ACTIVE'
		AND pin.exact_object_version=artifact.object_version ORDER BY run.stage_key`, targetAttempt, source.JobID)
	if err != nil {
		t.Fatal(err)
	}
	var bindings []cpuExactCacheBinding
	for rows.Next() {
		var binding cpuExactCacheBinding
		if err := rows.Scan(&binding.Stage, &binding.SourceArtifactID, &binding.ObjectVersion, &binding.PinID); err != nil {
			t.Fatal(err)
		}
		bindings = append(bindings, binding)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 2 || bindings[0].Stage != "dit" || bindings[1].Stage != "encoder" {
		t.Fatalf("exact source bindings/pins: %+v", bindings)
	}
	if !productionLoop {
		for _, worker := range workers[2:] {
			executeCPUExactCacheStage(t, ctx, worker, target.JobID)
		}
	} else {
		waitCPUExactCacheStages(t, ctx, database, targetAttempt.String(), 2, loopErrors)
	}
	completeCPUExactCacheJob(t, ctx, finalizer, uuid.MustParse(target.JobID))
	if productionLoop {
		cancelLoops()
		loops.Wait()
		select {
		case err := <-loopErrors:
			t.Fatal(err)
		default:
		}
	}
	var sourcePhysical, targetPhysical, transfers, usage, cacheProgress, jobFailures, creditMismatch int
	if err := database.Admin.QueryRow(`SELECT
		(SELECT count(*) FROM stage_attempts attempt JOIN stage_runs run ON run.id=attempt.stage_run_id WHERE run.attempt_id=$1),
		(SELECT count(*) FROM stage_attempts attempt JOIN stage_runs run ON run.id=attempt.stage_run_id WHERE run.attempt_id=$2),
		(SELECT count(*) FROM transfer_tickets WHERE state='CONSUMED'),
		(SELECT count(*) FROM resource_usage_records WHERE resource_kind='ALLOCATION_NANOSECOND' AND attribution='DIRECT'),
		(SELECT count(*) FROM stage_progress_receipts WHERE progress_kind='EXACT_CACHE'),
		(SELECT count(*) FROM jobs job WHERE state<>'SUCCEEDED' OR
		 (SELECT count(*) FROM charges WHERE job_id=job.id)<>1 OR (SELECT count(*) FROM visible_completions WHERE job_id=job.id)<>1),
		(SELECT count(*) FROM organization_credit_accounts account WHERE reserved_minor<>0 OR unsettled_posted_minor<>
		 (SELECT coalesce(sum(amount_minor),0) FROM charges WHERE organization_id=account.organization_id))`, sourceAttempt, targetAttempt).
		Scan(&sourcePhysical, &targetPhysical, &transfers, &usage, &cacheProgress, &jobFailures, &creditMismatch); err != nil {
		t.Fatal(err)
	}
	if sourcePhysical != 4 || targetPhysical != 2 || transfers != 5 || usage != 6 || cacheProgress != 2 || jobFailures != 0 || creditMismatch != 0 {
		t.Fatalf("cache conservation physical=%d/%d transfer=%d usage=%d cache_progress=%d jobs=%d credit=%d", sourcePhysical, targetPhysical, transfers, usage, cacheProgress, jobFailures, creditMismatch)
	}
	copies := verifyCPUExactCachePublicCopies(t, ctx, database, store)
	runtime.GC()
	afterTarget := waitForCPUExactCacheObservation(t, ctx, database, root, func(observation cpuLoadObservation) bool {
		return observation.Completed == 2 && observation.ScratchBytes == 0 &&
			observation.RuntimeWatchdogs == 0 && observation.ActiveAllocations == 0 &&
			observation.ActiveLeases == 0 && observation.ReservedStorage == 0 &&
			observation.Running == 0 && observation.Queued == 0 &&
			observation.PoolCounters == 0 && observation.ReservedCredit == 0
	})
	afterTarget.Processes = observeCPULoadProcesses(t)
	assertCPULoadResidentProcesses(t, before.Processes, afterTarget.Processes)
	if afterTarget.Completed != 2 || afterTarget.Charges != 2 || afterTarget.ScratchBytes != 0 || afterTarget.RuntimeWatchdogs != 0 ||
		afterTarget.ActiveAllocations != 0 || afterTarget.ActiveLeases != 0 || afterTarget.ReservedStorage != 0 ||
		afterTarget.Running != 0 || afterTarget.Queued != 0 || afterTarget.PoolCounters != 0 || afterTarget.ReservedCredit != 0 {
		t.Fatalf("target resources did not drain: %+v", afterTarget)
	}
	if afterTarget.CacheEntries != 2 || afterTarget.CacheCarryingRefs != 2 {
		t.Fatalf("expected two retained cache entries/carrying references: %+v", afterTarget)
	}
	if afterTarget.PostedCredit != budget.CreditLimitMinor {
		t.Fatalf("final Charges do not match the initial fixed-price budget: posted=%d budget=%+v", afterTarget.PostedCredit, budget)
	}
	maintenanceResult, afterMaintenance := maintainCPULoad(t, ctx, database, root, maintenance, afterTarget)
	verifyCPUExactCachePublicCopies(t, ctx, database, store)
	if digest := cpuLoadSourceDigest(t); digest != sourceDigest {
		t.Fatalf("source changed during exact-cache campaign: %s -> %s", sourceDigest, digest)
	}
	receipt := map[string]any{"schema_version": 1, "evidence_class": "LOCAL_CPU_MOCK_EXACT_CACHE", "production_gate": false,
		"initial_budget":     budget,
		"source_tree_sha256": sourceDigest, "runtime_binary_sha256": binaryDigests, "go_version": runtime.Version(), "ffprobe_version": ffprobeVersion,
		"started_at": started, "elapsed_seconds": time.Since(started).Seconds(), "source_job_id": source.JobID, "target_job_id": target.JobID,
		"source_physical_stages": sourcePhysical, "target_physical_stages": targetPhysical, "consumed_transfers": transfers, "direct_allocation_receipts": usage,
		"admission": admitted, "hits": hits, "exact_bindings_and_pins": bindings, "verified_public_copies": copies,
		"before": before, "after_source": afterSource, "after_target": afterTarget,
		"after_maintenance": afterMaintenance, "maintenance_result": maintenanceResult,
		"capacity_reports_by_worker": cpuLoadCapacityReports(t, workers),
		"limitations": []string{"CPU mock, local exact-version store and loopback Runtime gRPC; no real H3/GPU or Production Gate evidence",
			"public Go control services and low-level agent; production stream handler/journal replay is covered by separate tests",
			"BITWISE equivalence/policy are static fixture catalog declarations; native mock payloads and ADMIT/HIT outcomes are measured",
			"Project cache remains enabled with two admitted entries; cache carrying history is retained",
			"Reconciler drives real ADMIT/HIT before target physical Acquire; this does not benchmark a cache/scheduler race",
			"real ffprobe verifies media; host execution does not prove Linux sandboxing; native subprocesses are not race-instrumented"}}
	if workers[0].durable != nil {
		receipt["durable_records_by_worker"] = assertCPUDurableJournals(t, workers, !productionLoop)
		replayed := int64(0)
		for _, worker := range workers {
			replayed += worker.replayedCommits.Load()
		}
		receipt["durable_stream"] = true
		receipt["replayed_commits"] = replayed
		receipt["limitations"] = append(cpuDurableStreamLimitations(),
			"BITWISE policy is a fixture declaration; native payloads, exact versions, ADMIT/HIT and billing are measured",
			"Reconciler drives ADMIT/HIT before target Acquire; cache/scheduler races and organization isolation are separate tests")
		if productionLoop {
			receipt["limitations"] = append(receipt["limitations"].([]string),
				"ProductionAgent control-session reattach is exercised with the CPU transport fixture; Node/Fleet network replacement remains separate")
		}
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CPU_MOCK_EXACT_CACHE_RECEIPT %s", encoded)
}

func waitForCPUExactCacheObservation(
	t *testing.T,
	ctx context.Context,
	database testDatabase,
	root string,
	ready func(cpuLoadObservation) bool,
) cpuLoadObservation {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		observation := observeCPULoad(t, database, root)
		if ready(observation) {
			return observation
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for CPU exact-cache resources to drain: %v; last=%+v", ctx.Err(), observation)
		case <-deadline.C:
			t.Fatalf("CPU exact-cache resources did not drain within 30s: %+v", observation)
		case <-ticker.C:
		}
	}
}

func waitCPUProductionReady(t *testing.T, ctx context.Context, workers []*cpuLoadWorker) {
	t.Helper()
	for {
		ready := true
		for _, worker := range workers {
			if worker.durable == nil || worker.durable.control.registrations.Load() == 0 || worker.capacityReports.Load() == 0 {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		if err := waitCPULoad(ctx, 25*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
}

func waitCPUExactCacheStages(t *testing.T, ctx context.Context, database testDatabase, jobID string, want int, loopErrors <-chan error) {
	t.Helper()
	for {
		select {
		case err := <-loopErrors:
			t.Fatal(err)
		default:
		}
		var succeeded int
		if err := database.Admin.QueryRowContext(ctx, `SELECT count(*) FROM stage_attempts attempt
			JOIN stage_runs run ON run.id=attempt.stage_run_id
			WHERE run.attempt_id=$1 AND attempt.state='SUCCEEDED'`, jobID).Scan(&succeeded); err != nil {
			t.Fatal(err)
		}
		if succeeded >= want {
			return
		}
		if err := waitCPULoad(ctx, 25*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
}

func executeCPUExactCacheStage(t *testing.T, ctx context.Context, worker *cpuLoadWorker, jobID string) {
	t.Helper()
	for {
		result, err := worker.acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if result.Assignment == nil {
			if err := waitCPULoad(ctx, 25*time.Millisecond); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if result.Assignment.GetAuthority().GetJobId() != jobID {
			t.Fatalf("%s acquired another Job", worker.stage.key)
		}
		if err := worker.execute(ctx, result.Assignment); err != nil {
			t.Fatalf("%s native execution: %v", worker.stage.key, err)
		}
		return
	}
}

func completeCPUExactCacheJob(t *testing.T, ctx context.Context, service *stagefinalization.Service, jobID uuid.UUID) {
	t.Helper()
	identity := stagefinalization.AuthenticatedFinalizer{ID: "spiffe://vela.internal/finalizer/cpu-exact-cache"}
	claim, err := service.ClaimNextStageGraphFinalization(ctx, identity)
	if err != nil || claim.Decision != stagefinalization.StageGraphFinalizationGranted || claim.JobID != jobID {
		t.Fatalf("finalization claim=%+v err=%v", claim, err)
	}
	candidate := stagefinalization.StageGraphVisibleCompletionCandidate{CompletionID: uuid.New(), ExpectedJobVersion: claim.JobVersion}
	completed, err := service.CompleteStageGraphVisibleCompletion(ctx, identity, claim.Credentials, candidate)
	if err != nil || completed.Decision != stagefinalization.VisibleCompletionCommitted {
		t.Fatalf("real inspected completion=%+v err=%v", completed, err)
	}
	replay, err := service.CompleteStageGraphVisibleCompletion(ctx, identity, claim.Credentials, candidate)
	if err != nil || replay.CompletionID != completed.CompletionID || replay.ChargeID != completed.ChargeID {
		t.Fatalf("completion replay=%+v err=%v", replay, err)
	}
}

func verifyCPUExactCachePublicCopies(t *testing.T, ctx context.Context, database testDatabase, store *artifactstore.Local) []cpuExactPublicCopy {
	t.Helper()
	rows, err := database.Admin.Query(`SELECT artifact.job_id,artifact.kind::text,artifact.object_key,artifact.object_version_id,artifact.sha256,artifact.size_bytes,
		source.object_key,source.object_version FROM artifacts artifact JOIN stage_artifacts source ON source.id=artifact.source_stage_artifact_id
		WHERE artifact.state='COMMITTED' ORDER BY artifact.kind,artifact.job_id`)
	if err != nil {
		t.Fatal(err)
	}
	var copies []cpuExactPublicCopy
	keys := map[string]bool{}
	byKind := map[string]string{}
	for rows.Next() {
		var copy cpuExactPublicCopy
		var digest []byte
		var sourceKey, sourceVersion string
		if err := rows.Scan(&copy.JobID, &copy.Kind, &copy.ObjectKey, &copy.ObjectVersion, &digest, &copy.SizeBytes, &sourceKey, &sourceVersion); err != nil {
			t.Fatal(err)
		}
		if copy.ObjectKey == sourceKey || keys[copy.ObjectKey] {
			t.Fatalf("public copy ownership collision: %+v", copy)
		}
		keys[copy.ObjectKey] = true
		for _, object := range [][2]string{{copy.ObjectKey, copy.ObjectVersion}, {sourceKey, sourceVersion}} {
			reader, err := store.ReadExactVersion(ctx, object[0], object[1])
			if err != nil {
				t.Fatal(err)
			}
			hash := sha256.New()
			size, readErr := io.Copy(hash, reader)
			closeErr := reader.Close()
			if readErr != nil || closeErr != nil || size != copy.SizeBytes || !bytes.Equal(hash.Sum(nil), digest) {
				t.Fatalf("exact public/source bytes differ: %s %v %v", object[0], readErr, closeErr)
			}
		}
		copy.SHA256 = hex.EncodeToString(digest)
		if prior, ok := byKind[copy.Kind]; ok && prior != copy.SHA256 {
			t.Fatalf("same-input source/target %s content differs", copy.Kind)
		}
		byKind[copy.Kind] = copy.SHA256
		copies = append(copies, copy)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if len(copies) != 4 || len(byKind) != 2 {
		t.Fatalf("expected VIDEO/THUMBNAIL for two Jobs: %+v", copies)
	}
	return copies
}

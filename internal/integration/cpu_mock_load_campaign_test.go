//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/artifactvalidator"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stagefinalization"
	"github.com/vivym/vela/internal/stagescheduler"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/stageworkercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type cpuLoadObservation struct {
	ObservedAt        time.Time        `json:"observed_at,omitzero"`
	Queued            int              `json:"queued_jobs"`
	Running           int              `json:"running_jobs"`
	ActiveAllocations int              `json:"active_allocations"`
	ActiveLeases      int              `json:"active_leases"`
	ReservedStorage   int              `json:"reserved_storage"`
	ReservedBytes     int64            `json:"reserved_storage_bytes"`
	MaterializedBytes int64            `json:"materialized_stage_bytes"`
	HeldBuffers       int              `json:"held_buffers"`
	Completed         int              `json:"visible_completions"`
	AcquireIntents    int              `json:"acquire_intents"`
	DatabaseBytes     int64            `json:"database_bytes"`
	HeapBytes         uint64           `json:"test_process_heap_bytes"`
	Goroutines        int              `json:"test_process_goroutines"`
	ScratchBytes      int64            `json:"runtime_scratch_bytes"`
	JournalBytes      int64            `json:"local_journal_bytes,omitempty"`
	InputBytes        int64            `json:"runtime_input_bytes"`
	OutputBytes       int64            `json:"runtime_output_bytes"`
	RuntimeWatchdogs  int              `json:"runtime_watchdog_goroutines"`
	ReservedCredit    int64            `json:"reserved_credit_minor"`
	PostedCredit      int64            `json:"posted_credit_minor"`
	Charges           int              `json:"charges"`
	PoolCounters      int              `json:"pool_active_counters"`
	ExecutionPins     int              `json:"active_execution_pins"`
	FinalizationPins  int              `json:"active_finalization_pins"`
	CachePins         int              `json:"active_cache_pins"`
	CacheEntries      int              `json:"live_cache_entries"`
	CacheCarryingRefs int              `json:"active_cache_carrying_references"`
	CommittedObjects  int              `json:"committed_stage_artifacts"`
	Processes         []cpuLoadProcess `json:"processes,omitempty"`
}

type cpuLoadProcess struct {
	PID      int    `json:"pid"`
	Name     string `json:"name"`
	RSSBytes int64  `json:"rss_bytes"`
	OpenFDs  int    `json:"open_fds"`
}

type cpuLoadBudget struct {
	PlannedJobs      int    `json:"planned_jobs"`
	UnitAmountMinor  int64  `json:"fixed_price_per_job_minor"`
	CreditLimitMinor int64  `json:"initial_credit_limit_minor"`
	Currency         string `json:"currency"`
}

type cpuLoadReceipt struct {
	SchemaVersion       int                         `json:"schema_version"`
	EvidenceClass       string                      `json:"evidence_class"`
	ProductionGate      bool                        `json:"production_gate"`
	StartedAt           time.Time                   `json:"started_at"`
	Jobs                int                         `json:"jobs"`
	Waves               int                         `json:"waves"`
	ConcurrentArrivals  int                         `json:"concurrent_arrivals_per_wave"`
	PersistentWorkers   int                         `json:"persistent_workers"`
	ProjectRunningLimit int                         `json:"project_running_limit"`
	InitialBudget       cpuLoadBudget               `json:"initial_budget"`
	ElapsedSeconds      float64                     `json:"elapsed_seconds"`
	JobsPerSecond       float64                     `json:"jobs_per_second"`
	MeanQueueSeconds    float64                     `json:"mean_queue_seconds"`
	MaxQueueSeconds     float64                     `json:"max_queue_seconds"`
	MeanLatencySeconds  float64                     `json:"mean_latency_seconds"`
	Peak                cpuLoadObservation          `json:"sampled_peak"`
	Before              cpuLoadObservation          `json:"before_arrivals"`
	AfterWave           []cpuLoadObservation        `json:"after_wave"`
	AfterMaintenance    []cpuLoadObservation        `json:"after_maintenance"`
	MaintenanceResults  []retention.ReconcileResult `json:"maintenance_results"`
	AfterIdle           cpuLoadObservation          `json:"after_idle"`
	AcquireRetries      int64                       `json:"acquire_transaction_retries"`
	AcquireDeadlocks    int64                       `json:"acquire_deadlock_retries"`
	AcquireConflicts    int64                       `json:"acquire_serialization_retries"`
	AcquireStalePolls   int64                       `json:"acquire_confirmed_stale_polls,omitempty"`
	CapacityReports     map[string]int64            `json:"capacity_reports_by_worker"`
	SourceTreeSHA256    string                      `json:"source_tree_sha256"`
	RuntimeBinaries     map[string]string           `json:"runtime_binary_sha256"`
	GoVersion           string                      `json:"go_version"`
	FFprobeVersion      string                      `json:"ffprobe_version"`
	Limitations         []string                    `json:"limitations"`
	DurableStream       bool                        `json:"durable_stream,omitempty"`
	ReplayedCommits     int64                       `json:"replayed_materialization_commits,omitempty"`
	DurableRecords      map[string]int              `json:"durable_records_by_worker,omitempty"`
}

// This opt-in campaign uses actual subprocesses and ffprobe; ordinary integration
// shards continue to run without requiring the host media toolchain.
func TestCPUMockConcurrentAdmissionRuntimeCampaign(t *testing.T) {
	runCPUMockRuntimeCampaign(t, false, false)
}

func runCPUMockRuntimeCampaign(t *testing.T, exactCache, durableStream bool) {
	t.Helper()
	if os.Getenv("VELA_RUN_CPU_MOCK_CAMPAIGN") != "1" {
		t.Skip("set VELA_RUN_CPU_MOCK_CAMPAIGN=1 for the bounded subprocess campaign")
	}
	waves := cpuLoadIntegerSetting(t, "VELA_CPU_MOCK_WAVES", 2, 2, 256)
	width := cpuLoadIntegerSetting(t, "VELA_CPU_MOCK_WIDTH", 8, 3, 10)
	if durableStream && waves*width > 32 {
		t.Fatal("durable campaign exceeds retained Runtime history; reclamation must be implemented before sustained operation")
	}
	timeout := 90 * time.Second
	if value := os.Getenv("VELA_CPU_MOCK_TIMEOUT"); value != "" {
		var err error
		timeout, err = time.ParseDuration(value)
		if err != nil || timeout < 10*time.Second || timeout > 30*time.Minute {
			t.Fatalf("VELA_CPU_MOCK_TIMEOUT must be between 10s and 30m: %q", value)
		}
	}
	sourceDigest := cpuLoadSourceDigest(t)
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Fatal(err)
	}
	version, err := exec.Command(ffprobe, "-version").Output()
	if err != nil || !strings.HasPrefix(string(version), "ffprobe version 8.0.1 ") {
		t.Fatalf("campaign requires ffprobe 8.0.1: %v %s", err, version)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "vela-h3-stage-mock")
	build := exec.Command("go", "build", "-mod=readonly", "-trimpath", "-buildvcs=false", "-o", binary, "./cmd/vela-h3-stage-mock")
	build.Dir = repositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build mock: %v %s", err, output)
	}
	thumbnailBinary := filepath.Join(root, "vela-lab-cpu-thumbnail-mock")
	build = exec.Command("go", "build", "-mod=readonly", "-trimpath", "-buildvcs=false", "-o", thumbnailBinary, "./cmd/vela-lab-cpu-thumbnail-mock")
	build.Dir = repositoryRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build thumbnail mock: %v %s", err, output)
	}
	binaryDigests := map[string]string{}
	for _, path := range []string{binary, thumbnailBinary} {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		binaryDigests[filepath.Base(path)] = fmt.Sprintf("%x", sha256.Sum256(content))
	}
	database, coordinator, serverURL := newH3IntegrationEnvironmentWithCatalogSetup(t, seedCPULoadThumbnailCatalog)
	plannedJobs := waves * width
	if exactCache {
		plannedJobs = 2
	}
	budget := seedCPULoadCreditBudget(t, database, plannedJobs)
	startRecord, err := json.Marshal(map[string]any{"source_tree_sha256": sourceDigest,
		"runtime_binary_sha256": binaryDigests, "go_version": runtime.Version(),
		"initial_budget": budget, "exact_cache": exactCache, "durable_stream": durableStream})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CPU_MOCK_CAMPAIGN_START %s", startRecord)
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		logs, err := database.Container.Logs(context.Background())
		if err != nil {
			t.Logf("PostgreSQL logs: %v", err)
			return
		}
		defer func() { _ = logs.Close() }()
		content, err := io.ReadAll(io.LimitReader(logs, 1<<20))
		if err == nil {
			t.Logf("CPU_CAMPAIGN_POSTGRES_LOG\n%s", content)
		}
	})
	seedWorkerRegistryPlan(t, database.Admin)
	registry, err := fleet.NewService(newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password"))
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := stageartifact.NewPostgresRepository(newRolePool(t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password"))
	if err != nil {
		t.Fatal(err)
	}
	store := artifactstore.NewLocal()
	keys := map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)}
	validator, err := stageauthority.NewValidator(keys, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := stageworkercontrol.NewPostgresExecutionBackend(
		newRolePool(t, database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password"), signer,
		stageworkercontrol.PostgresExecutionConfig{ActiveSigningKeyID: "stage-authority-key-v1", AuthorityTTL: time.Minute,
			LocalDeadlineTTL: 50 * time.Second, MaxClockSkew: time.Second, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := stageworkercontrol.NewPostgresWorkerEvidenceBackend(newRolePool(t, database.DSN,
		"vela_stage_worker_control_login", "vela-stage-worker-control-password"))
	if err != nil {
		t.Fatal(err)
	}
	tickets, err := stageartifact.NewTransferTicketKeyringSigner("stage-authority-key-v1", keys)
	if err != nil {
		t.Fatal(err)
	}
	connector, err := stageartifact.NewObjectStorePullConnector(store, artifacts, tickets, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	materializer, err := stageartifact.NewMaterializer(store, artifacts, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	inspector, err := artifactvalidator.NewInspector(store, cpuLoadFFprobe{binary: ffprobe}, artifactvalidator.Config{
		MaxInputBytes: 64 << 20, MaxProbeOutputBytes: 1 << 20, Timeout: 10 * time.Second,
		ExpectedFFprobeVersion: "8.0.1", ValidatorRevision: "cpu-load-ffprobe-8.0.1", SpoolDirectory: root})
	if err != nil {
		t.Fatal(err)
	}
	finalizer := stageGraphVisibleCompletionService(t, database.DSN, inspector, store)
	maintenance := newStageLifecycleReconciler(t, database, &stageDeletionResponseLossStore{Local: store})
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	workers := make([]*cpuLoadWorker, 0, 4)
	stages := h3IntegrationStages([]string{"cpu-load-encoder", "cpu-load-dit", "cpu-load-vae"}, nil)
	stages = append(stages, h3IntegrationStage{key: "thumbnail", profileID: cpuThumbnailStageProfileID, workerProfileID: cpuThumbnailWorkerProfileID,
		component: "ffmpeg-thumbnail-v1", outputPort: "thumbnail", outputInterface: "49700000-0000-0000-0000-000000000016",
		nodeIdentity: "cpu-load-thumbnail", resourceClass: "CPU", capacityVector: map[string]int64{
			"cpu_milli": 2000, "memory_bytes": 4294967296, "scratch_bytes": 34359738368, "concurrency": 1}})
	for i, stage := range stages {
		worker := seedH3IntegrationWorker(t, database, registry, stage, byte(0xb1+i))
		repository, err := stagescheduler.NewPostgresRepository(newRolePool(t, database.DSN, "vela_stage_scheduler_login", "vela-stage-scheduler-password"))
		if err != nil {
			t.Fatal(err)
		}
		fixture := stageSchedulerFixture{database: database, repository: repository, coordinator: coordinator,
			authority: stagescheduler.WorkerAuthority{CapacityPoolID: worker.poolID, StageProfileRevisionID: uuid.MustParse(stage.profileID),
				WorkerInstanceID: worker.workerID, WorkerInstanceEpoch: worker.authority.InstanceEpoch,
				DeviceSetDigest: worker.authority.DeviceSetDigest, MembershipDigest: worker.authority.MembershipDigest,
				ModelResidencyID: worker.authority.ModelResidencyID, ModelRuntimeEpoch: worker.authority.ModelRuntimeEpoch,
				CapacityVector: worker.capacity}, observation: stagescheduler.CapacityObservation{Sequence: worker.evidence.Capacity.Sequence}}
		resident := newCPULoadWorker(t, ctx, root, binary, stage, worker, fixture,
			validator, execution, evidence, artifacts, tickets, connector, materializer, durableStream)
		if err := resident.reportCapacity(ctx); err != nil {
			t.Fatal(err)
		}
		worker.evidence.ControlSessionEpoch = resident.controlSessionEpoch
		worker.evidence.Capacity.Sequence = resident.fixture.observation.Sequence
		registerStageSchedulerRuntime(t, database, worker.evidence, worker.authority, stage.profileID)
		if durableStream {
			configureCPUDurableWorker(t, resident, store, keys)
		}
		workers = append(workers, resident)
	}
	if exactCache {
		runCPUExactCacheSourceTarget(t, ctx, database, serverURL, root, store, workers, finalizer, maintenance,
			budget, sourceDigest, binaryDigests, strings.SplitN(string(version), "\n", 2)[0])
		return
	}
	var wg sync.WaitGroup
	failures := make(chan error, len(workers)+1)
	for _, worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := worker.run(ctx); err != nil && ctx.Err() == nil {
				failures <- err
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := finalizeCPULoad(ctx, finalizer); err != nil && ctx.Err() == nil {
			failures <- err
		}
	}()
	defer func() { cancel(); wg.Wait() }()
	receipt := cpuLoadReceipt{SchemaVersion: 1, EvidenceClass: "LOCAL_CPU_MOCK_RUNTIME_LOAD", Jobs: waves * width,
		Waves: waves, ConcurrentArrivals: width, PersistentWorkers: len(workers), ProjectRunningLimit: 2,
		InitialBudget:    budget,
		SourceTreeSHA256: sourceDigest, RuntimeBinaries: binaryDigests, GoVersion: runtime.Version(),
		FFprobeVersion: strings.SplitN(string(version), "\n", 2)[0],
		Limitations: []string{"bounded CPU load campaign, not a production soak", "logical GPU profiles executed only by CPU mock processes",
			"loopback gRPC runtime; control services called through their public Go boundaries", "local exact-version object store, not remote S3",
			"production StageWorkerControl stream handler and StreamAgent durable journal are not exercised by this campaign",
			"ffprobe parses real mock media; host invocation does not prove Linux sandbox isolation",
			"heap and goroutines describe the test process; resource sampling adds load",
			"go test -race instruments the test process; native mock subprocesses are built without race instrumentation",
			"ps/lsof cover the test process and native workers; PostgreSQL container and Docker VM RSS/FD are not sampled",
			"raw after_wave observations precede one production retention batch; after_maintenance records the separate result",
			"database history and exact objects are retained; successful local scratch uses production retirement API; crash/restart journal recovery has separate coverage"}}
	if durableStream {
		receipt.DurableStream = true
		receipt.Limitations = cpuDurableStreamLimitations()
	}
	receipt.Before = observeCPULoad(t, database, root)
	receipt.Before.Processes = observeCPULoadProcesses(t)
	if len(receipt.Before.Processes) != len(workers)+1 {
		t.Fatalf("expected test process and %d resident runtime processes: %+v", len(workers), receipt.Before.Processes)
	}
	receipt.StartedAt = time.Now().UTC()
	for wave := 0; wave < waves; wave++ {
		results := make(chan httpResult, width)
		arrivalErrors := make(chan error, width)
		gate := make(chan struct{})
		for index := 0; index < width; index++ {
			go func() {
				<-gate
				body := []byte(fmt.Sprintf(`{"model":"minimax-h3","generation_preset":"balanced","service_class":"standard","output_spec":"video-1080p-5s-24fps","generation_count":1,"prompt":"cpu load wave %d job %d","h3":{"seed":%d}}`, wave, index, wave*width+index))
				result, err := doSubmitJob(serverURL, testProjectID, testBearerCredential(), fmt.Sprintf("cpu-load-%d-%d", wave, index), body)
				if err != nil {
					arrivalErrors <- err
					return
				}
				results <- result
			}()
		}
		close(gate)
		for range width {
			select {
			case err := <-arrivalErrors:
				t.Fatal(err)
			case result := <-results:
				if result.StatusCode != http.StatusAccepted {
					t.Fatalf("HTTP Admission: %d %s", result.StatusCode, result.Body)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		for {
			select {
			case err := <-failures:
				t.Fatal(err)
			default:
			}
			observation := observeCPULoad(t, database, root)
			mergeCPULoadPeak(&receipt.Peak, observation)
			if observation.Running > 2 || observation.Queued > 10 || observation.ActiveAllocations > len(workers) {
				t.Fatalf("observed control-plane bound violation: %+v", observation)
			}
			if observation.Completed == (wave+1)*width && observation.ScratchBytes == 0 {
				observation.Processes = observeCPULoadProcesses(t)
				assertCPULoadResidentProcesses(t, receipt.Before.Processes, observation.Processes)
				t.Logf("CPU_MOCK_LOAD_WAVE %d %+v", wave+1, observation)
				receipt.AfterWave = append(receipt.AfterWave, observation)
				result, afterMaintenance := maintainCPULoad(t, ctx, database, root, maintenance, observation)
				receipt.MaintenanceResults = append(receipt.MaintenanceResults, result)
				receipt.AfterMaintenance = append(receipt.AfterMaintenance, afterMaintenance)
				break
			}
			if err := waitCPULoad(ctx, 10*time.Millisecond); err != nil {
				t.Fatal(err)
			}
		}
	}
	receipt.ElapsedSeconds = time.Since(receipt.StartedAt).Seconds()
	receipt.JobsPerSecond = float64(receipt.Jobs) / receipt.ElapsedSeconds
	assertCPULoadConservation(t, database, receipt.Jobs)
	if err := database.Admin.QueryRow(`SELECT avg(extract(epoch FROM (job.billable_started_at-job.created_at))),
		max(extract(epoch FROM (job.billable_started_at-job.created_at))),
		avg(extract(epoch FROM (attempt.ended_at-job.created_at))) FROM jobs AS job JOIN attempts AS attempt ON attempt.job_id=job.id`).
		Scan(&receipt.MeanQueueSeconds, &receipt.MaxQueueSeconds, &receipt.MeanLatencySeconds); err != nil {
		t.Fatal(err)
	}
	if receipt.Peak.Running < 2 || receipt.Peak.Queued == 0 {
		t.Fatalf("campaign did not exercise overlap and queueing: %+v", receipt.Peak)
	}
	if err := waitCPULoad(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	receipt.AfterIdle = observeCPULoad(t, database, root)
	receipt.AfterIdle.Processes = observeCPULoadProcesses(t)
	assertCPULoadResidentProcesses(t, receipt.Before.Processes, receipt.AfterIdle.Processes)
	for _, worker := range workers {
		receipt.AcquireRetries += worker.retries.Load()
		receipt.AcquireDeadlocks += worker.deadlocks.Load()
		receipt.AcquireConflicts += worker.conflicts.Load()
		receipt.AcquireStalePolls += worker.stalePolls.Load()
		receipt.ReplayedCommits += worker.replayedCommits.Load()
	}
	if durableStream {
		receipt.DurableRecords = assertCPUDurableJournals(t, workers)
	}
	receipt.CapacityReports = cpuLoadCapacityReports(t, workers)
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	if receipt.AfterIdle.AcquireIntents != receipt.AfterWave[len(receipt.AfterWave)-1].AcquireIntents {
		t.Fatalf("idle Acquire persisted new intent rows: %d -> %d", receipt.AfterWave[len(receipt.AfterWave)-1].AcquireIntents, receipt.AfterIdle.AcquireIntents)
	}
	if receipt.AfterIdle.RuntimeWatchdogs != 0 {
		t.Fatalf("idle Runtime retained %d watchdog goroutines after every Stage completed", receipt.AfterIdle.RuntimeWatchdogs)
	}
	if receipt.AfterIdle.ScratchBytes != 0 {
		t.Fatalf("idle runtime retained %d scratch bytes after committed retirement", receipt.AfterIdle.ScratchBytes)
	}
	if receipt.AfterIdle.ExecutionPins != 0 || receipt.AfterIdle.FinalizationPins != 0 || receipt.AfterIdle.CachePins != 0 {
		t.Fatalf("idle runtime retained live pins after production maintenance: %+v", receipt.AfterIdle)
	}
	if receipt.AfterIdle.PostedCredit != budget.CreditLimitMinor {
		t.Fatalf("final Charges do not match the initial fixed-price budget: posted=%d budget=%+v", receipt.AfterIdle.PostedCredit, budget)
	}
	if digest := cpuLoadSourceDigest(t); digest != receipt.SourceTreeSHA256 {
		t.Fatalf("Go/SQL source tree changed during campaign: %s -> %s", receipt.SourceTreeSHA256, digest)
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CPU_MOCK_LOAD_RECEIPT %s", encoded)
}

type cpuLoadFFprobe struct{ binary string }

func (probe cpuLoadFFprobe) Probe(ctx context.Context, file *os.File) ([]byte, error) {
	return exec.CommandContext(ctx, probe.binary, "-v", "error", "-hide_banner", "-protocol_whitelist", "file",
		"-probesize", "67108864", "-analyzeduration", "10000000", "-show_entries",
		"program_version=version:stream=codec_name,codec_type,width,height,avg_frame_rate,nb_frames,duration:format=format_name,duration,size",
		"-of", "json", file.Name()).Output()
}

type cpuLoadWorker struct {
	stage                 h3IntegrationStage
	fixture               stageSchedulerFixture
	assignments           *stageworkercontrol.PostgresAssignmentBackend
	execution             *stageworkercontrol.PostgresExecutionBackend
	evidence              *stageworkercontrol.PostgresWorkerEvidenceBackend
	controlSessionEpoch   int64
	nextCapacityReport    time.Time
	capacityReports       atomic.Int64
	validator             *stageauthority.Validator
	artifacts             *stageartifact.PostgresRepository
	tickets               *stageartifact.TransferTicketSigner
	connector             *stageartifact.ObjectStorePullConnector
	materializer          *stageartifact.Materializer
	agent                 *stageworkeragent.Agent
	scratchRetirer        *stageworkeragent.FilesystemScratchRetirer
	inputRoot, outputRoot string
	retries               atomic.Int64
	deadlocks             atomic.Int64
	conflicts             atomic.Int64
	durable               *cpuDurableWorker
	replayedCommits       atomic.Int64
	stalePolls            atomic.Int64
}

func newCPULoadWorker(t *testing.T, ctx context.Context, root, binary string, stage h3IntegrationStage,
	worker h3IntegrationWorker, fixture stageSchedulerFixture, validator *stageauthority.Validator,
	execution *stageworkercontrol.PostgresExecutionBackend, evidence *stageworkercontrol.PostgresWorkerEvidenceBackend,
	artifacts *stageartifact.PostgresRepository,
	tickets *stageartifact.TransferTicketSigner, connector *stageartifact.ObjectStorePullConnector,
	materializer *stageartifact.Materializer, durableStream bool) *cpuLoadWorker {
	t.Helper()
	scratch := filepath.Join(root, stage.key)
	inputRoot, outputRoot := filepath.Join(scratch, "inputs"), filepath.Join(scratch, "outputs")
	for _, path := range []string{inputRoot, outputRoot} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	scratchRetirer, err := stageworkeragent.NewFilesystemScratchRetirer(inputRoot, outputRoot)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := scratchRetirer.Close(); err != nil {
			t.Error(err)
		}
	})
	member, device := worker.evidence.Members[0], worker.evidence.DeviceSet.Devices[0]
	binding := stageauthority.RuntimeBinding{WorkerInstanceID: worker.workerID.String(), WorkerInstanceEpoch: worker.authority.InstanceEpoch,
		WorkerMemberID: member.ID.String(), WorkerMemberEpoch: member.MemberEpoch,
		DeviceSetDigest: worker.authority.DeviceSetDigest[:], MembershipDigest: worker.authority.MembershipDigest[:],
		Devices:          []stageauthority.DeviceEpoch{{ID: device.ID.String(), Epoch: device.DeviceEpoch}},
		Members:          []stageauthority.MemberEpoch{{ID: member.ID.String(), Epoch: member.MemberEpoch}},
		ModelResidencyID: worker.authority.ModelResidencyID.String(), ModelRuntimeIdentity: worker.evidence.Residencies[0].RuntimeIdentity,
		StageProfileRevisionID: stage.profileID}
	component := map[string]string{"encoder": "ENCODER", "dit": "DIT", "vae": "VAE_DECODER", "thumbnail": "CPU_MEDIA"}[stage.key]
	executable := filepath.Join(root, "vela-lab-cpu-thumbnail-mock")
	if stage.key != "thumbnail" {
		executable = filepath.Join(root, map[string]string{"encoder": "h3-encoder", "dit": "h3-dit", "vae": "h3-vae-decoder"}[stage.key])
		if err := os.Link(binary, executable); err != nil {
			t.Fatal(err)
		}
	}
	var durable *cpuDurableWorker
	var identityDigest, subsetDigest []byte
	if durableStream {
		identityDigest, err = hex.DecodeString(member.IdentityDigest)
		if err != nil {
			t.Fatal(err)
		}
		subsetDigest, err = hex.DecodeString(member.DeviceSubsetDigest)
		if err != nil {
			t.Fatal(err)
		}
		admissionBinding := binding
		admissionBinding.ModelRuntimeEpoch = worker.authority.ModelRuntimeEpoch
		durable = newCPUDurableJournals(t, scratch, admissionBinding, validator, identityDigest, subsetDigest)
	}
	service, err := modelruntime.NewService(modelruntime.Config{Binding: binding, Validator: validator,
		EpochStore:    modelruntime.EpochStoreFunc(func(stageauthority.RuntimeBinding) (int64, error) { return worker.authority.ModelRuntimeEpoch, nil }),
		CancelTimeout: time.Second,
		BackendFactory: func(allocated stageauthority.RuntimeBinding) (modelruntime.Backend, error) {
			return modelruntime.NewProcessBackend(ctx, allocated, modelruntime.ProcessBackendConfig{
				Component: component, ModelComponentRevision: stage.component, Command: []string{executable},
				LocalDevices: []modelruntime.DriverDevice{{DeviceID: device.ID.String(), DeviceEpoch: device.DeviceEpoch, ResourceClass: stage.resourceClass, GPUUUID: device.GPUUUID, PCIBDF: device.PCIBDF}},
				ScratchRoot:  scratch, InputRoot: inputRoot, OutputRoot: outputRoot,
				InitializationTimeout: 10 * time.Second, ShutdownTimeout: 5 * time.Second})
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.Close)
	var runtimeServer velav1.ModelRuntimeServiceServer = service
	if durableStream {
		durable.supervisor, err = modelruntime.NewSupervisorWithExecutionFloor(modelruntime.ExecutionFloorConfig{
			Validator: validator, State: &modelruntime.ExecutionFloorStateConfig{Directory: filepath.Join(scratch, "runtime-admission"), Initialize: true},
			Members: []modelruntime.ExecutionFloorMember{{WorkerMemberID: binding.WorkerMemberID, MemberEpoch: binding.WorkerMemberEpoch, IdentityDigest: identityDigest, DeviceSubsetDigest: subsetDigest}},
		}, service)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(durable.supervisor.Close)
		runtimeServer = durable.supervisor
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	velav1.RegisterModelRuntimeServiceServer(server, runtimeServer)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	agent, err := stageworkeragent.New(stageworkeragent.Config{Members: []stageworkeragent.RuntimeMember{{ID: member.ID.String(), Client: velav1.NewModelRuntimeServiceClient(connection)}}})
	if err != nil {
		t.Fatal(err)
	}
	return &cpuLoadWorker{stage: stage, fixture: fixture, assignments: newPostgresAssignmentTestBackend(t, fixture),
		execution: execution, evidence: evidence, controlSessionEpoch: worker.evidence.ControlSessionEpoch,
		validator: validator, artifacts: artifacts, tickets: tickets, connector: connector,
		materializer: materializer, agent: agent, scratchRetirer: scratchRetirer, inputRoot: inputRoot, outputRoot: outputRoot, durable: durable}
}

func (worker *cpuLoadWorker) run(ctx context.Context) error {
	for ctx.Err() == nil {
		result, err := worker.acquire(ctx)
		if err != nil {
			var postgres *pgconn.PgError
			if errors.As(err, &postgres) {
				return fmt.Errorf("%s Acquire: %w; detail=%s where=%s", worker.stage.key, err, postgres.Detail, postgres.Where)
			}
			return fmt.Errorf("%s Acquire: %w", worker.stage.key, err)
		}
		if result.Assignment == nil {
			if err := waitCPULoad(ctx, 25*time.Millisecond); err != nil {
				return err
			}
			continue
		}
		if err := worker.execute(ctx, result.Assignment); err != nil {
			return fmt.Errorf("%s execution: %w", worker.stage.key, err)
		}
	}
	return ctx.Err()
}

func (worker *cpuLoadWorker) acquire(ctx context.Context) (stageworkercontrol.AcquireResult, error) {
	if err := worker.reportCapacity(ctx); err != nil {
		return stageworkercontrol.AcquireResult{}, err
	}
	if worker.durable != nil {
		return worker.acquireDurable(ctx)
	}
	command := worker.controlCommand()
	result, err := worker.assignments.AcquireStage(ctx, command, stageWorkerAcquireRequest(worker.fixture))
	for retry := 0; err != nil && retry < 32; retry++ {
		var postgres *pgconn.PgError
		if !errors.As(err, &postgres) || (postgres.Code != "40001" && postgres.Code != "40P01") {
			break
		}
		worker.retries.Add(1)
		if postgres.Code == "40P01" {
			worker.deadlocks.Add(1)
		} else {
			worker.conflicts.Add(1)
		}
		if waitErr := waitCPULoad(ctx, 50*time.Millisecond); waitErr != nil {
			return stageworkercontrol.AcquireResult{}, waitErr
		}
		result, err = worker.assignments.AcquireStage(ctx, command, stageWorkerAcquireRequest(worker.fixture))
	}
	return result, err
}

func (worker *cpuLoadWorker) reportCapacity(ctx context.Context) error {
	now := time.Now().UTC()
	if now.Before(worker.nextCapacityReport) {
		return nil
	}
	sequence := (worker.fixture.authority.WorkerInstanceEpoch << 32) + 1
	request := &velav1.ReportStageCapacityObservationRequest{
		WorkerInstanceId:    worker.fixture.authority.WorkerInstanceID.String(),
		WorkerInstanceEpoch: worker.fixture.authority.WorkerInstanceEpoch,
		ObservationSequence: sequence, CapacityVector: worker.fixture.authority.CapacityVector,
		ObservedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(2 * time.Minute)),
	}
	result, err := worker.evidence.ReportCapacityObservation(ctx, worker.controlCommand(), request)
	if err == nil && !result.Ready && result.ControlSessionEpoch > worker.controlSessionEpoch {
		worker.controlSessionEpoch = result.ControlSessionEpoch
		result, err = worker.evidence.ReportCapacityObservation(ctx, worker.controlCommand(), request)
	}
	if err != nil {
		return fmt.Errorf("renew capacity observation: %w", err)
	}
	if !result.Ready || result.CapacityObservationSequence != sequence || result.ControlSessionEpoch != worker.controlSessionEpoch {
		return fmt.Errorf("renew capacity observation: %+v", result)
	}
	worker.fixture.observation.Sequence = sequence
	worker.nextCapacityReport = now.Add(30 * time.Second)
	worker.capacityReports.Add(1)
	return nil
}

func (worker *cpuLoadWorker) controlCommand() stageworkercontrol.CommandContext {
	command := stageWorkerAcquireCommand(worker.fixture)
	command.ControlSessionEpoch = worker.controlSessionEpoch
	return command
}

func cpuLoadCapacityReports(t *testing.T, workers []*cpuLoadWorker) map[string]int64 {
	t.Helper()
	reports := make(map[string]int64, len(workers))
	for _, worker := range workers {
		reports[worker.stage.key] = worker.capacityReports.Load()
		if reports[worker.stage.key] < 1 {
			t.Fatalf("%s did not report actual capacity", worker.stage.key)
		}
	}
	return reports
}

func (worker *cpuLoadWorker) execute(ctx context.Context, assignment *velav1.StageAssignment) error {
	if worker.durable != nil {
		return worker.executeDurable(ctx, assignment)
	}
	authority := assignment.GetAuthority()
	for i, input := range assignment.GetExecutionSpec().GetInputs() {
		ticket := stageartifact.SignedTransferTicket{Token: assignment.GetInputTransferTickets()[i].GetTransferTicket()}
		claims, err := worker.tickets.Verify(ticket, time.Now())
		if err != nil {
			return err
		}
		target, err := stageworkeragent.NewFilesystemInputTransferTarget(worker.inputRoot, uuid.MustParse(authority.GetStageRunId()), input)
		if err != nil {
			return err
		}
		_, pullErr := worker.connector.Pull(ctx, ticket, claims.Destination, target)
		closeErr := target.Close()
		if err := errors.Join(pullErr, closeErr); err != nil {
			return err
		}
	}
	barrier, err := worker.agent.PrepareAndStart(ctx, assignment)
	if err != nil || !barrier.BarrierPassed {
		return fmt.Errorf("runtime barrier %+v: %w", barrier, err)
	}
	verified, err := worker.validator.ValidateEnvelope(authority)
	if err != nil {
		return err
	}
	started, err := worker.execution.StartStage(ctx, worker.controlCommand(),
		&velav1.StartStageRequest{Authority: authority, StartedAt: timestamppb.New(barrier.StartedAt)},
		stageworkercontrol.VerifiedAuthorities{Stage: &verified})
	if err != nil {
		return err
	}
	if started.RenewedAuthority == nil {
		return fmt.Errorf("start decision %s", started.Decision)
	}
	authority = started.RenewedAuthority
	receipt, err := worker.agent.SealOutput(ctx, authority)
	if err != nil {
		return err
	}
	manifest, err := stageartifact.ParseLocalOutputManifestV1(receipt.GetOutputManifestJson())
	if err != nil {
		return err
	}
	lineage, err := manifest.LineageDigest()
	if err != nil {
		return err
	}
	manifestDigest := sha256.Sum256(receipt.GetOutputManifestJson())
	artifactID, leaseID := uuid.New(), uuid.New()
	now := time.Now().UTC()
	expires := now.Add(30 * time.Second)
	if authority.GetExpiresAt().AsTime().Before(expires) {
		expires = authority.GetExpiresAt().AsTime()
	}
	lease, err := worker.artifacts.Seal(ctx, stageartifact.SealCommand{
		CommandID: uuid.New(), AttemptID: uuid.MustParse(authority.GetAttemptId()), StageRunID: uuid.MustParse(authority.GetStageRunId()),
		StageAttemptID: uuid.MustParse(authority.GetStageAttemptId()), StageAllocationID: uuid.MustParse(authority.GetStageAllocationId()),
		StageLeaseID: uuid.MustParse(authority.GetStageLeaseId()), ExpectedAttemptFence: authority.GetAttemptFence(),
		ExpectedStageFence: authority.GetStageFence(), ExpectedStageVersion: authority.GetStageVersion(),
		OutputPort: manifest.OutputPort, LocalReceiptID: receipt.GetReceiptId(), LocalReceiptDigest: manifestDigest,
		ManifestSHA256: manifestDigest, SHA256: manifest.PayloadSHA256, LineageDigest: lineage,
		TokenDigest: sha256.Sum256([]byte(leaseID.String())), SizeBytes: manifest.SizeBytes, ArtifactID: artifactID,
		MaterializationLeaseID: leaseID, ObjectKey: "artifacts/stage/cpu-load/" + artifactID.String(), ContentType: manifest.ContentType,
		SealedAt: now, LeaseExpiresAt: expires})
	if err != nil {
		return err
	}
	file, err := os.Open(filepath.Join(worker.outputRoot, filepath.FromSlash(manifest.LocalLocator)))
	if err != nil {
		return err
	}
	committed, materializeErr := worker.materializer.Materialize(ctx, lease, file)
	if err := errors.Join(materializeErr, file.Close()); err != nil {
		return err
	}
	return worker.scratchRetirer.RetireCommitted(ctx, manifest, committed)
}

func finalizeCPULoad(ctx context.Context, service *stagefinalization.Service) error {
	identity := stagefinalization.AuthenticatedFinalizer{ID: "spiffe://vela.internal/finalizer/cpu-load"}
	for ctx.Err() == nil {
		claim, err := service.ClaimNextStageGraphFinalization(ctx, identity)
		if err != nil {
			return err
		}
		if claim.Decision == stagefinalization.StageGraphFinalizationNoWork {
			if err := waitCPULoad(ctx, 10*time.Millisecond); err != nil {
				return err
			}
			continue
		}
		candidate := stagefinalization.StageGraphVisibleCompletionCandidate{CompletionID: uuid.New(), ExpectedJobVersion: claim.JobVersion}
		completed, err := service.CompleteStageGraphVisibleCompletion(ctx, identity, claim.Credentials, candidate)
		if err != nil {
			return err
		}
		if completed.Decision != stagefinalization.VisibleCompletionCommitted {
			return fmt.Errorf("visible completion: %s", completed.Decision)
		}
		replay, err := service.CompleteStageGraphVisibleCompletion(ctx, identity, claim.Credentials, candidate)
		if err != nil || replay.ChargeID != completed.ChargeID || replay.CompletionID != completed.CompletionID {
			return fmt.Errorf("visible completion replay differs: %w", err)
		}
	}
	return ctx.Err()
}

func observeCPULoad(t *testing.T, database testDatabase, root string) cpuLoadObservation {
	t.Helper()
	observation := cpuLoadObservation{ObservedAt: time.Now().UTC()}
	if err := database.Admin.QueryRow(`SELECT project.queued_count, project.running_count,
		(SELECT count(*) FROM stage_allocations WHERE state='ALLOCATED'),
		(SELECT count(*) FROM stage_leases WHERE state='ACTIVE'),
		(SELECT count(*) FROM stage_storage_reservations WHERE state='RESERVED'),
		(SELECT coalesce(sum(reserved_bytes),0) FROM stage_storage_reservations WHERE state='RESERVED'),
		(SELECT coalesce(sum(consumed_bytes),0) FROM stage_storage_reservations),
		(SELECT count(*) FROM edge_buffer_credits WHERE state='HELD'),
		(SELECT count(*) FROM visible_completions), (SELECT count(*) FROM stage_worker_acquire_intents),
		pg_database_size(current_database()),
		(SELECT coalesce(sum(reserved_minor),0) FROM organization_credit_accounts),
		(SELECT coalesce(sum(unsettled_posted_minor),0) FROM organization_credit_accounts),
		(SELECT count(*) FROM charges),
		(SELECT coalesce(sum(ready_count+claimed_count+active_allocation_count),0) FROM stage_capacity_pool_counters),
		(SELECT count(*) FROM stage_artifact_pins WHERE state='ACTIVE' AND pin_kind='EXECUTION'),
		(SELECT count(*) FROM stage_artifact_pins WHERE state='ACTIVE' AND pin_kind='FINALIZATION'),
		(SELECT count(*) FROM stage_artifact_pins WHERE state='ACTIVE' AND pin_kind='CACHE'),
		(SELECT count(*) FROM stage_cache_entries WHERE state='LIVE'),
		(SELECT count(*) FROM stage_cache_references WHERE state='ACTIVE' AND execution_pin_id IS NULL),
		(SELECT count(*) FROM stage_artifacts WHERE state='COMMITTED')
		FROM projects AS project WHERE id=$1`, testProjectID).
		Scan(&observation.Queued, &observation.Running, &observation.ActiveAllocations, &observation.ActiveLeases,
			&observation.ReservedStorage, &observation.ReservedBytes, &observation.MaterializedBytes,
			&observation.HeldBuffers, &observation.Completed, &observation.AcquireIntents, &observation.DatabaseBytes,
			&observation.ReservedCredit, &observation.PostedCredit, &observation.Charges, &observation.PoolCounters,
			&observation.ExecutionPins, &observation.FinalizationPins, &observation.CachePins,
			&observation.CacheEntries, &observation.CacheCarryingRefs, &observation.CommittedObjects); err != nil {
		t.Fatal(err)
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	observation.HeapBytes, observation.Goroutines = memory.HeapAlloc, runtime.NumGoroutine()
	observation.RuntimeWatchdogs = countCPULoadWatchdogs(t)
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.IsDir() || path == filepath.Join(root, "vela-h3-stage-mock") || path == filepath.Join(root, "vela-lab-cpu-thumbnail-mock") || strings.HasPrefix(entry.Name(), "h3-") {
			return nil
		}
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err == nil {
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			parts := strings.Split(filepath.ToSlash(relative), "/")
			if len(parts) > 2 && (parts[1] == "worker-admission" || parts[1] == "runtime-admission" || parts[1] == "materialization-journal" || parts[1] == "input-transfer-journal" || entry.Name() == ".vela-assignment-admission") {
				observation.JournalBytes += info.Size()
				return nil
			}
			observation.ScratchBytes += info.Size()
			if len(parts) > 2 && parts[1] == "inputs" {
				observation.InputBytes += info.Size()
			} else if len(parts) > 2 && parts[1] == "outputs" {
				observation.OutputBytes += info.Size()
			}
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return observation
}

func seedCPULoadCreditBudget(t *testing.T, database testDatabase, jobs int) cpuLoadBudget {
	t.Helper()
	budget := cpuLoadBudget{PlannedJobs: jobs}
	if err := database.Admin.QueryRow(`SELECT unit_amount_minor,currency FROM rate_card_lines
		WHERE id='00000000-0000-0000-0000-000000000017'`).Scan(&budget.UnitAmountMinor, &budget.Currency); err != nil {
		t.Fatal(err)
	}
	if budget.UnitAmountMinor <= 0 || jobs <= 0 {
		t.Fatalf("invalid campaign budget: %+v", budget)
	}
	budget.CreditLimitMinor = int64(jobs) * budget.UnitAmountMinor
	if _, err := database.Admin.Exec(`UPDATE organization_credit_accounts SET contract_credit_limit_minor=$2
		WHERE organization_id=$1`, testOrganizationID, budget.CreditLimitMinor); err != nil {
		t.Fatal(err)
	}
	return budget
}

func maintainCPULoad(t *testing.T, ctx context.Context, database testDatabase, root string,
	maintenance *retention.Reconciler, before cpuLoadObservation) (retention.ReconcileResult, cpuLoadObservation) {
	t.Helper()
	result, err := maintenance.ReconcileBatch(ctx)
	if err != nil || result.Failed != 0 || result.Claimed != 0 || result.Completed != 0 {
		t.Fatalf("production maintenance unexpectedly deleted unexpired artifacts: %+v err=%v", result, err)
	}
	after := observeCPULoad(t, database, root)
	if after.ExecutionPins != 0 || after.FinalizationPins != 0 || after.HeldBuffers != 0 || after.CachePins != before.CachePins ||
		after.CacheEntries != before.CacheEntries || after.CacheCarryingRefs != before.CacheCarryingRefs ||
		after.CommittedObjects != before.CommittedObjects {
		t.Fatalf("production maintenance did not retire terminal ownership and preserve unexpired objects/cache: before=%+v after=%+v", before, after)
	}
	return result, after
}

func mergeCPULoadPeak(peak *cpuLoadObservation, value cpuLoadObservation) {
	peak.Queued = max(peak.Queued, value.Queued)
	peak.Running = max(peak.Running, value.Running)
	peak.ActiveAllocations = max(peak.ActiveAllocations, value.ActiveAllocations)
	peak.ActiveLeases = max(peak.ActiveLeases, value.ActiveLeases)
	peak.ReservedStorage = max(peak.ReservedStorage, value.ReservedStorage)
	peak.ReservedBytes = max(peak.ReservedBytes, value.ReservedBytes)
	peak.MaterializedBytes = max(peak.MaterializedBytes, value.MaterializedBytes)
	peak.HeldBuffers = max(peak.HeldBuffers, value.HeldBuffers)
	peak.Completed = max(peak.Completed, value.Completed)
	peak.AcquireIntents = max(peak.AcquireIntents, value.AcquireIntents)
	peak.DatabaseBytes = max(peak.DatabaseBytes, value.DatabaseBytes)
	peak.HeapBytes = max(peak.HeapBytes, value.HeapBytes)
	peak.Goroutines = max(peak.Goroutines, value.Goroutines)
	peak.ScratchBytes = max(peak.ScratchBytes, value.ScratchBytes)
	peak.JournalBytes = max(peak.JournalBytes, value.JournalBytes)
	peak.InputBytes = max(peak.InputBytes, value.InputBytes)
	peak.OutputBytes = max(peak.OutputBytes, value.OutputBytes)
	peak.RuntimeWatchdogs = max(peak.RuntimeWatchdogs, value.RuntimeWatchdogs)
	peak.ReservedCredit = max(peak.ReservedCredit, value.ReservedCredit)
	peak.PostedCredit = max(peak.PostedCredit, value.PostedCredit)
	peak.Charges = max(peak.Charges, value.Charges)
	peak.PoolCounters = max(peak.PoolCounters, value.PoolCounters)
	peak.ExecutionPins = max(peak.ExecutionPins, value.ExecutionPins)
	peak.FinalizationPins = max(peak.FinalizationPins, value.FinalizationPins)
	peak.CachePins = max(peak.CachePins, value.CachePins)
	peak.CacheEntries = max(peak.CacheEntries, value.CacheEntries)
	peak.CacheCarryingRefs = max(peak.CacheCarryingRefs, value.CacheCarryingRefs)
	peak.CommittedObjects = max(peak.CommittedObjects, value.CommittedObjects)
}

func cpuLoadIntegerSetting(t *testing.T, name string, fallback, minimum, maximum int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < minimum || parsed > maximum {
		t.Fatalf("%s must be between %d and %d: %q", name, minimum, maximum, value)
	}
	return parsed
}

func cpuLoadSourceDigest(t *testing.T) string {
	t.Helper()
	root := repositoryRoot(t)
	sources := map[string]string{}
	for _, directory := range []string{"cmd", "internal", "proto/gen", "db"} {
		if err := filepath.WalkDir(filepath.Join(root, directory), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || (filepath.Ext(path) != ".go" && filepath.Ext(path) != ".sql") {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			sources[filepath.ToSlash(relative)] = fmt.Sprintf("%x", sha256.Sum256(content))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"go.mod", "go.sum",
		"internal/h3mockbackend/testdata/video-1080p-5s-24fps.mp4",
		"internal/h3mockbackend/testdata/thumbnail-320x180.webp",
		"internal/artifactvalidator/testdata/h264_16x16_1fps.mp4.b64"} {
		content, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		sources[name] = fmt.Sprintf("%x", sha256.Sum256(content))
	}
	encoded, err := json.Marshal(sources)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(encoded))
}

func observeCPULoadProcesses(t *testing.T) []cpuLoadProcess {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	listing, err := exec.CommandContext(ctx, "ps", "-axo", "pid=,ppid=,rss=,comm=").Output()
	if err != nil {
		t.Fatalf("sample resident processes: %v", err)
	}
	var processes []cpuLoadProcess
	for _, line := range strings.Split(string(listing), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		pid, pidErr := strconv.Atoi(fields[0])
		parent, parentErr := strconv.Atoi(fields[1])
		rss, rssErr := strconv.ParseInt(fields[2], 10, 64)
		name := filepath.Base(strings.Join(fields[3:], " "))
		resident := name == "h3-encoder" || name == "h3-dit" || name == "h3-vae-decoder" || name == "vela-lab-cpu-thumbnail-mock"
		if pidErr != nil || parentErr != nil || rssErr != nil || (pid != os.Getpid() && (parent != os.Getpid() || !resident)) {
			continue
		}
		processes = append(processes, cpuLoadProcess{PID: pid, Name: name, RSSBytes: rss * 1024})
	}
	sort.Slice(processes, func(i, j int) bool { return processes[i].PID < processes[j].PID })
	pids := make([]string, len(processes))
	byPID := map[int]int{}
	for index, process := range processes {
		pids[index] = strconv.Itoa(process.PID)
		byPID[process.PID] = index
	}
	if len(pids) == 0 {
		t.Fatal("process sampler found no current process")
	}
	listing, err = exec.CommandContext(ctx, "lsof", "-nP", "-a", "-p", strings.Join(pids, ","), "-F", "pf").Output()
	if err != nil {
		t.Fatalf("sample process descriptors: %v", err)
	}
	index := -1
	for _, line := range strings.Split(string(listing), "\n") {
		if len(line) < 2 {
			continue
		}
		if line[0] == 'p' {
			pid, err := strconv.Atoi(line[1:])
			var found bool
			index, found = byPID[pid]
			if err != nil || !found {
				index = -1
			}
		} else if line[0] == 'f' && index >= 0 {
			if _, err := strconv.Atoi(line[1:]); err == nil {
				processes[index].OpenFDs++
			}
		}
	}
	return processes
}

func assertCPULoadResidentProcesses(t *testing.T, before, after []cpuLoadProcess) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("resident process count changed: %+v -> %+v", before, after)
	}
	for index := range before {
		if before[index].PID != after[index].PID || before[index].Name != after[index].Name {
			t.Fatalf("resident process identity changed: %+v -> %+v", before, after)
		}
	}
}

func assertCPULoadConservation(t *testing.T, database testDatabase, jobs int) {
	t.Helper()
	var successes, attempts, stages, charges, completions, transfers, usage int
	var active, unsettledStorage, projectCounters, reservationMismatch, poolOverlap int
	var accountMismatch, poolCounters int
	if err := database.Admin.QueryRow(`SELECT
		(SELECT count(*) FROM jobs WHERE state='SUCCEEDED'), (SELECT count(*) FROM attempts WHERE state='SUCCEEDED' AND graph_state='SUCCEEDED'),
		(SELECT count(*) FROM stage_attempts WHERE state='SUCCEEDED'), (SELECT count(*) FROM charges), (SELECT count(*) FROM visible_completions),
		(SELECT count(*) FROM transfer_tickets WHERE state='CONSUMED'),
		(SELECT count(*) FROM resource_usage_records WHERE resource_kind='ALLOCATION_NANOSECOND' AND attribution='DIRECT'),
		(SELECT count(*) FROM stage_allocations WHERE state='ALLOCATED')+(SELECT count(*) FROM stage_leases WHERE state='ACTIVE')+
		(SELECT count(*) FROM stage_materialization_leases WHERE state='ACTIVE')+
		(SELECT count(*) FROM edge_buffer_credits WHERE state='HELD'),
		(SELECT count(*) FROM stage_storage_reservations WHERE state='RESERVED'),
		(SELECT queued_count+running_count+retry_wait_count FROM projects WHERE id=$1),
		(SELECT count(*) FROM credit_reservations WHERE state<>'CONSUMED'),
		(SELECT count(*) FROM stage_allocations AS a JOIN stage_allocations AS b ON a.id<b.id
		 AND a.worker_instance_id=b.worker_instance_id AND a.allocated_at<b.released_at AND b.allocated_at<a.released_at),
		(SELECT count(*) FROM organization_credit_accounts AS account WHERE reserved_minor<>0 OR unsettled_posted_minor<>
		 (SELECT coalesce(sum(amount_minor),0) FROM charges WHERE organization_id=account.organization_id)),
		(SELECT coalesce(sum(ready_count+claimed_count+active_allocation_count),0) FROM stage_capacity_pool_counters)`, testProjectID).
		Scan(&successes, &attempts, &stages, &charges, &completions, &transfers, &usage, &active, &unsettledStorage, &projectCounters, &reservationMismatch, &poolOverlap, &accountMismatch, &poolCounters); err != nil {
		t.Fatal(err)
	}
	if successes != jobs || attempts != jobs || stages != jobs*4 || charges != jobs || completions != jobs || transfers != jobs*3 || usage != jobs*4 ||
		active != 0 || unsettledStorage != 0 || projectCounters != 0 || reservationMismatch != 0 || poolOverlap != 0 || accountMismatch != 0 || poolCounters != 0 {
		t.Fatalf("campaign conservation jobs/attempts/stages/charges/completions/transfers/usage=%d/%d/%d/%d/%d/%d/%d active=%d unsettled_storage=%d counters=%d reservations=%d worker overlaps=%d accounts=%d pool counters=%d",
			successes, attempts, stages, charges, completions, transfers, usage, active, unsettledStorage, projectCounters, reservationMismatch, poolOverlap, accountMismatch, poolCounters)
	}
}

func waitCPULoad(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func countCPULoadWatchdogs(t *testing.T) int {
	t.Helper()
	var stacks bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&stacks, 2); err != nil {
		t.Fatal(err)
	}
	return strings.Count(stacks.String(), "modelruntime.(*Service).resetWatchdogLocked.func1")
}

func seedCPULoadThumbnailCatalog(t *testing.T, database testDatabase) {
	t.Helper()
	seedCPUMediaExecutionGraph(t, database)
	if _, err := database.Admin.Exec(fmt.Sprintf(`
		INSERT INTO execution_graph_stages(execution_graph_revision_id,stage_key,stage_definition_revision_id,required,max_fan_out)
		SELECT '%[1]s',stage_key,stage_definition_revision_id,required,max_fan_out FROM execution_graph_stages
		WHERE execution_graph_revision_id='%[2]s' AND stage_key='thumbnail';
		INSERT INTO execution_graph_edges(id,execution_graph_revision_id,source_stage_key,source_port,destination_stage_key,destination_port,buffer_class)
		VALUES ('49800000-0000-0000-0000-000000000064','%[1]s','vae','video','thumbnail','frames','L2_DURABLE');
		INSERT INTO execution_graph_outputs(execution_graph_revision_id,output_key,interface_revision_id,source_stage_key,source_port,required)
		SELECT '%[1]s',output_key,interface_revision_id,source_stage_key,source_port,required FROM execution_graph_outputs
		WHERE execution_graph_revision_id='%[2]s' AND output_key='thumbnail';
		INSERT INTO execution_profile_stage_options(execution_profile_revision_id,execution_graph_revision_id,stage_key,stage_definition_revision_id,stage_profile_revision_id,preference,eligibility_metadata)
		SELECT '%[3]s','%[1]s',stage_key,stage_definition_revision_id,stage_profile_revision_id,preference,eligibility_metadata FROM execution_profile_stage_options
		WHERE execution_profile_revision_id='%[4]s' AND stage_key='thumbnail';
		INSERT INTO execution_profile_connector_options(execution_profile_revision_id,execution_graph_revision_id,execution_graph_edge_id,connector_revision_id,required_topology_policy,preference)
		VALUES ('%[3]s','%[1]s','49800000-0000-0000-0000-000000000064','49700000-0000-0000-0000-000000000054','{}',0);
		UPDATE execution_graph_revisions SET content_digest=vela_execution_graph_content_digest(id) WHERE id='%[1]s';
	`, stageGraphID, cpuMediaGraphID, graphExecutionProfileID, cpuMediaExecutionProfileID)); err != nil {
		t.Fatal(err)
	}
}

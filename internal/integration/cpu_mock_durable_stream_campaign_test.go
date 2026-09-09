//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/authoritypolicy"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestCPUMockDurableStreamJobCampaign(t *testing.T) {
	runCPUMockRuntimeCampaign(t, cpuCampaignMode{durableStream: true})
}

func TestCPUMockDurableStreamExactCacheCampaign(t *testing.T) {
	runCPUMockRuntimeCampaign(t, cpuCampaignMode{exactCache: true, durableStream: true})
}

type cpuDurableWorker struct {
	supervisor      *modelruntime.Supervisor
	admission       *stageworkeragent.FileAssignmentAdmission
	stream          *stageworkeragent.StreamAgent
	config          stageworkeragent.DurableStreamConfig
	control         *cpuCommitLossControl
	acquireID       uuid.UUID
	root            string
	redial          func() *stageworkertransport.Client
	productionLoop  bool
	production      *stageworkeragent.ProductionAgent
	productionState *stageworkeragent.FileProductionState
	runtimeClient   velav1.ModelRuntimeServiceClient
}

var errCPUCommitResponseLost = errors.New("CPU campaign dropped committed materialization response")

type cpuCommitLossControl struct {
	*stageworkertransport.Client
	armed             atomic.Bool
	dropped           atomic.Int64
	worker            *cpuLoadWorker
	lostID            atomic.Pointer[string]
	registrations     atomic.Int64
	heartbeats        atomic.Int64
	staleAcquires     atomic.Int64
	futureAssignments atomic.Int64
}

func (control *cpuCommitLossControl) Exchange(ctx context.Context, request *velav1.StageWorkerControlServiceConnectRequest) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	response, err := control.Client.Exchange(ctx, request)
	if assignment := response.GetStageAssignment(); err == nil && assignment != nil &&
		assignment.GetAuthority().GetIssuedAt().AsTime().After(control.worker.consumerNow()) {
		control.futureAssignments.Add(1)
	}
	if err == nil && request.GetCommitStageMaterialization() != nil &&
		response.GetStageCommandResult().GetDecision() == velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED && control.armed.Swap(false) {
		control.dropped.Add(1)
		id := request.GetRequestId()
		control.lostID.Store(&id)
		return nil, errCPUCommitResponseLost
	}
	if err == nil && request.GetCommitStageMaterialization() != nil && response.GetRequestId() == request.GetRequestId() &&
		response.GetStageCommandResult().GetDecision() == velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED {
		if id := control.lostID.Load(); id != nil && *id == request.GetRequestId() && control.lostID.CompareAndSwap(id, nil) {
			control.worker.replayedCommits.Add(1)
		}
	}
	if err == nil && control.worker.durable.productionLoop && request.GetReportCapacityObservation() != nil && response.GetWorkerReadinessDecision().GetReady() {
		control.worker.capacityReports.Add(1)
	}
	if err == nil && request.GetRegisterWorkerEvidence() != nil && response.GetWorkerReadinessDecision().GetReady() {
		control.registrations.Add(1)
	}
	if err == nil && request.GetHeartbeatStage() != nil && response.GetStageCommandResult().GetDecision() == velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED {
		control.heartbeats.Add(1)
	}
	if err == nil && request.GetAcquireStage() != nil && response.GetStageCommandResult().GetDecision() == velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE {
		control.staleAcquires.Add(1)
	}
	return response, err
}

func newCPUDurableJournals(t *testing.T, scratch string, binding stageauthority.RuntimeBinding, validator *stageauthority.Validator, identity, subset []byte) *cpuDurableWorker {
	t.Helper()
	for _, name := range []string{"runtime-admission", "worker-admission"} {
		if err := os.Mkdir(filepath.Join(scratch, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	gate, err := stageworkeragent.NewFileAssignmentAdmission(stageworkeragent.AssignmentAdmissionConfig{
		Initialize: true, Directory: filepath.Join(scratch, "worker-admission"), InputRoot: filepath.Join(scratch, "inputs"), OutputRoot: filepath.Join(scratch, "outputs"),
		WorkerInstanceID: uuid.MustParse(binding.WorkerInstanceID), WorkerInstanceEpoch: binding.WorkerInstanceEpoch, WorkerMemberID: uuid.MustParse(binding.WorkerMemberID),
		Validator: validator, MaxClockSkew: authoritypolicy.ProductionMaxClockSkew, MaxRecords: 32, Bindings: []stageworkeragent.AdmissionRuntimeBinding{{Runtime: binding, IdentityDigest: [32]byte(identity), DeviceSubsetDigest: [32]byte(subset)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := gate.Close(); err != nil {
			t.Error(err)
		}
	})
	return &cpuDurableWorker{admission: gate, root: scratch}
}

func configureCPUDurableWorker(t *testing.T, worker *cpuLoadWorker, store *artifactstore.Local, keys map[string][]byte) {
	t.Helper()
	durable := worker.durable
	pool := newRolePool(t, worker.fixture.database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password")
	materializationSigner, err := materializationauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := stageartifact.NewMaterializationAuthorityIssuer(worker.artifacts, materializationSigner, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	history, err := stageworkercontrol.NewPostgresTerminalHistoryReader(pool, worker.validator)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := stageworkercontrol.NewTerminalDispositionBackend(history, signer, worker.validator, "stage-authority-key-v1")
	if err != nil {
		t.Fatal(err)
	}
	reattach, err := stageworkercontrol.NewPostgresReattachmentBackend(pool)
	if err != nil {
		t.Fatal(err)
	}
	operations, err := stageworkercontrol.NewPostgresOperationBackend(stageworkercontrol.PostgresOperationConfig{
		TerminalDispositions: terminal, WorkerEvidence: worker.evidence, Assignments: worker.assignments, Execution: worker.execution,
		MaterializationIssuer: issuer, StageArtifacts: worker.artifacts, StageAttempts: worker.fixture.coordinator, Reattachments: reattach, Transfers: worker.artifacts,
	})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := stageworkercontrol.NewProductionExecutor(operations)
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := stageworkercontrol.NewPostgresAuthorizer(pool)
	if err != nil {
		t.Fatal(err)
	}
	materializationValidator, err := materializationauthority.NewValidator(keys, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := stageworkercontrol.NewHandler(stageworkercontrol.Config{Validator: worker.validator, Authorizer: authorizer,
		MaterializationValidator: materializationValidator, MaterializationAuthorizer: worker.artifacts, Executor: executor, MaxClockSkew: authoritypolicy.ProductionMaxClockSkew})
	if err != nil {
		t.Fatal(err)
	}
	var sources []stageworkertransport.ControlSessionEpochSource
	if durable.productionLoop {
		sources = append(sources, durable.productionState)
	}
	dial := terminalDispositionDialer(t, handler, sources...)
	durable.redial = func() *stageworkertransport.Client {
		return dial(worker.controlCommand().Identity.SPIFFEID, worker.controlSessionEpoch)
	}
	durable.control = &cpuCommitLossControl{Client: durable.redial(), worker: worker}
	durable.control.armed.Store(true)
	inputJournal, err := stageworkeragent.NewFileInputTransferJournal(filepath.Join(durable.root, "input-transfer-journal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := inputJournal.Close(); err != nil {
			t.Error(err)
		}
	})
	connectorID := "49000000-0000-0000-0000-000000000050"
	switch worker.stage.key {
	case "vae":
		connectorID = "49000000-0000-0000-0000-000000000051"
	case "thumbnail":
		connectorID = "49700000-0000-0000-0000-000000000054"
	}
	resolver, err := stageworkeragent.NewAssignmentInputResolver(stageworkeragent.AssignmentInputResolverConfig{
		Store: store, TicketSigner: worker.tickets, Control: durable.control, InputRoot: worker.inputRoot,
		ConnectorRevisionID: uuid.MustParse(connectorID), Now: worker.consumerNow, Journal: inputJournal, MaxClockSkew: authoritypolicy.ProductionMaxClockSkew,
	})
	if err != nil {
		t.Fatal(err)
	}
	journal, err := stageworkeragent.NewFileMaterializationJournal(filepath.Join(durable.root, "materialization-journal"), 32)
	if err != nil {
		t.Fatal(err)
	}
	source, err := stageartifact.NewFilesystemLocalOutputSource(worker.outputRoot)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := stageartifact.NewObjectStorePublisher(store, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	consumerMaterializationValidator, err := materializationauthority.NewValidator(keys, worker.consumerNow)
	if err != nil {
		t.Fatal(err)
	}
	durable.config = stageworkeragent.DurableStreamConfig{Runtime: worker.agent, Control: durable.control, Admission: durable.admission, InputResolver: resolver,
		Materialization: &stageworkeragent.MaterializationConfig{Validator: consumerMaterializationValidator, Source: source, Publisher: publisher, Journal: journal, MaxClockSkew: authoritypolicy.ProductionMaxClockSkew,
			ScratchRetirer: worker.scratchRetirer, OutputOwnershipContract: stageworkeragent.AttemptOwnedFilesystemScratchV1,
			SourceLossEvidence: stageworkeragent.MaterializationSourceLossEvidenceFunc(func(context.Context, stageworkeragent.PendingMaterialization) (stageworkeragent.MaterializationSourceLossEvidence, error) {
				return stageworkeragent.MaterializationSourceLossEvidence{}, errors.New("CPU successful Job campaign unexpectedly lost its source")
			}),
		},
	}
	durable.stream, err = stageworkeragent.NewDurableStreamAgent(durable.config)
	if err != nil {
		t.Fatal(err)
	}
}

func (worker *cpuLoadWorker) acquireDurable(ctx context.Context) (stageworkercontrol.AcquireResult, error) {
	for retry := 0; retry < 32; retry++ {
		result, stale, err := worker.acquireDurableOnce(ctx)
		if err != nil || !stale {
			return result, err
		}
		// STALE is a confirmed negative result, so the next poll gets a new
		// command ID. An uncertain transport failure is never retried here.
		worker.stalePolls.Add(1)
		if err := waitCPULoad(ctx, 50*time.Millisecond); err != nil {
			return stageworkercontrol.AcquireResult{}, err
		}
	}
	return stageworkercontrol.AcquireResult{}, errors.New("durable Acquire remained stale for 32 polls")
}

func (worker *cpuLoadWorker) acquireDurableOnce(ctx context.Context) (stageworkercontrol.AcquireResult, bool, error) {
	id := uuid.New()
	response, err := worker.durable.control.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{RequestId: id.String(),
		Operation: &velav1.StageWorkerControlServiceConnectRequest_AcquireStage{AcquireStage: stageWorkerAcquireRequest(worker.fixture)}})
	if err != nil {
		return stageworkercontrol.AcquireResult{}, false, err
	}
	if response.GetRequestId() != id.String() {
		return stageworkercontrol.AcquireResult{}, false, errors.New("Acquire response changed command identity")
	}
	if assignment := response.GetStageAssignment(); assignment != nil {
		worker.durable.acquireID = id
		return stageworkercontrol.AcquireResult{Assignment: assignment}, false, nil
	}
	if response.GetNoWork() != nil {
		return stageworkercontrol.AcquireResult{}, false, nil
	}
	if response.GetStageCommandResult().GetDecision() == velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE {
		return stageworkercontrol.AcquireResult{}, true, nil
	}
	return stageworkercontrol.AcquireResult{}, false, fmt.Errorf("durable Acquire: %v", response.GetStageCommandResult())
}

func (worker *cpuLoadWorker) executeDurable(ctx context.Context, assignment *velav1.StageAssignment) error {
	durable := worker.durable
	result, err := durable.stream.ExecuteAcquiredAssignment(ctx, assignment, durable.acquireID)
	if err != nil || !result.ControlStartAccepted {
		return fmt.Errorf("durable execution: %+v: %w", result, err)
	}
	materialized, err := durable.stream.SealAndMaterialize(ctx)
	if errors.Is(err, errCPUCommitResponseLost) {
		// Replace the transport within the same fixture-authenticated logical
		// session. Production session reattachment is a separate campaign.
		if err := durable.control.Close(); err != nil {
			return err
		}
		durable.control.Client = durable.redial()
		// Reconstruct the stream from its actual on-disk materialization record.
		// The resident Runtime stays live; this is not a process-restart claim.
		journal, openErr := stageworkeragent.NewFileMaterializationJournal(filepath.Join(durable.root, "materialization-journal"), 32)
		if openErr != nil {
			return openErr
		}
		pending, readErr := journal.List(ctx)
		if readErr != nil || len(pending) != 1 || pending[0].ConfirmedDisposition != "" {
			return fmt.Errorf("lost commit has no exact pending record: %+v %w", pending, readErr)
		}
		durable.config.Materialization.Journal = journal
		durable.stream, err = stageworkeragent.NewDurableStreamAgent(durable.config)
		if err != nil {
			return err
		}
		materialized, err = durable.stream.ResumeMaterializations(ctx)
	}
	if err != nil || !materialized.Committed {
		return fmt.Errorf("durable materialization: %+v: %w", materialized, err)
	}
	return nil
}

func assertCPUDurableJournals(t *testing.T, workers []*cpuLoadWorker, requireReplay bool) map[string]int {
	t.Helper()
	counts := make(map[string]int, len(workers))
	for _, worker := range workers {
		durable := worker.durable
		if durable == nil || requireReplay && (durable.control.dropped.Load() != 1 || worker.replayedCommits.Load() != 1) {
			t.Fatalf("%s did not recover its actual lost commit", worker.stage.key)
		}
		state, err := durable.admission.Snapshot(t.Context())
		if err != nil || state.Latest == nil || state.Latest.Phase != stageworkeragent.AssignmentClosed || state.Latest.InputDrain == nil {
			t.Fatalf("%s final Worker journal: %+v %v", worker.stage.key, state, err)
		}
		for _, entry := range state.Pending {
			if entry.Phase != stageworkeragent.AssignmentClosed || entry.InputDrain == nil {
				t.Fatalf("%s retained unfinished Worker entry", worker.stage.key)
			}
		}
		var expected int
		if err := worker.fixture.database.Admin.QueryRow(`SELECT count(*) FROM stage_allocations
			WHERE worker_instance_id=$1`, worker.fixture.authority.WorkerInstanceID).Scan(&expected); err != nil {
			t.Fatal(err)
		}
		if expected != len(state.Pending)+1 {
			t.Fatalf("%s Worker history differs from PostgreSQL: %d != %d", worker.stage.key, len(state.Pending)+1, expected)
		}
		originals := make(map[string]*velav1.StageAuthority, expected)
		for _, entry := range append(state.Pending, *state.Latest) {
			originals[entry.Original.GetStageAttemptId()] = entry.Original
		}
		wire, err := os.ReadFile(filepath.Join(durable.root, "runtime-admission", "execution-admission.json"))
		if err != nil {
			t.Fatal(err)
		}
		var journal struct {
			SchemaVersion int `json:"schema_version"`
			Executions    []struct {
				Authority []byte          `json:"authority"`
				Seal      json.RawMessage `json:"seal"`
				Drain     json.RawMessage `json:"drain"`
			} `json:"executions"`
		}
		if err := json.Unmarshal(wire, &journal); err != nil || journal.SchemaVersion != 8 || len(journal.Executions) != len(state.Pending)+1 {
			t.Fatalf("%s Runtime and Worker history disagree: %+v %v", worker.stage.key, journal, err)
		}
		for _, entry := range journal.Executions {
			var authority velav1.StageAuthority
			if err := proto.Unmarshal(entry.Authority, &authority); err != nil || !proto.Equal(&authority, originals[authority.GetStageAttemptId()]) {
				t.Fatalf("%s Runtime history differs from original Worker authority: %v", worker.stage.key, err)
			}
			delete(originals, authority.GetStageAttemptId())
			if len(entry.Seal) == 0 || string(entry.Seal) == "null" || len(entry.Drain) == 0 || string(entry.Drain) == "null" {
				t.Fatalf("%s completed without durable seal/drain", worker.stage.key)
			}
		}
		if len(originals) != 0 {
			t.Fatalf("%s Runtime history omitted Worker allocations", worker.stage.key)
		}
		pending, err := durable.config.Materialization.Journal.List(t.Context())
		if err != nil || len(pending) != 0 {
			t.Fatalf("%s retained unresolved materialization: %+v %v", worker.stage.key, pending, err)
		}
		counts[worker.stage.key] = expected
	}
	return counts
}

func cpuDurableStreamLimitations() []string {
	return []string{"bounded CPU mock Jobs, not sustained open-loop throughput or GPU certification",
		"real mTLS Control stream and durable StreamAgent; Runtime gRPC and model processes are local CPU fixtures",
		"Worker readiness/Registry setup uses fixture services; production Fleet activation, Node custody/startup and ProductionAgent orchestration are separate",
		"lost committed responses reconstruct StreamAgent/file journal and replace mTLS transport in the same fixture session; processes remain live; production session reattachment is separate",
		"bounded harness retries confirmed STALE Acquire results with new command IDs; production discovery orchestration is not exercised",
		"input/output payload retirement is measured separately from retained journal metadata; this campaign does not exercise the separate history reclamation protocol",
		"local exact-version object store, not remote S3; physical isolation and power-cut durability are separate",
		"host race instrumentation does not instrument native mock subprocesses; sampled resources are not full VM peaks"}
}

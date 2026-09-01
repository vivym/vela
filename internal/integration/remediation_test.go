//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/artifactaccess"
	"github.com/vivym/vela/internal/breakglass"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/organizationreporting"
	"github.com/vivym/vela/internal/remediation"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/webhook"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

const remediationGPUUUID = "GPU-00000000-0000-0000-0000-000000000004"

type remediationDeviceEpochSource map[string]int64

func (source remediationDeviceEpochSource) CurrentDeviceEpoch(gpuUUID string) (int64, bool) {
	epoch, ok := source[gpuUUID]
	return epoch, ok
}

func TestRemediationPlatformAPIRequiresDistinctL6ApprovalBeforeExecution(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	workerID := seedRemediationWorker(t, database)
	if _, err := database.Admin.Exec(`
		INSERT INTO platform_operator_oidc_bindings (
			id, issuer, subject, display_name
		) VALUES
			($1, 'https://platform-identity.example.com', 'remediation-requester', 'Requester'),
			($2, 'https://platform-identity.example.com', 'remediation-approver', 'Approver'),
			($3, 'https://platform-identity.example.com', 'remediation-third', 'Third')
	`, platformOperatorRequesterID, platformOperatorApproverID, platformOperatorThirdID); err != nil {
		t.Fatalf("seed remediation Platform Operators: %v", err)
	}
	verifier := tokenOIDCVerifier{
		"remediation-requester-token": {
			Issuer: "https://platform-identity.example.com", Subject: "remediation-requester",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		},
		"remediation-approver-token": {
			Issuer: "https://platform-identity.example.com", Subject: "remediation-approver",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		},
		"remediation-third-token": {
			Issuer: "https://platform-identity.example.com", Subject: "remediation-third",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		},
	}
	service, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create remediation HTTP service: %v", err)
	}
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          identity.NewAuthenticator(nil, []byte("remediation-http-pepper")),
		PlatformAuthenticator:  breakglass.NewAuthenticator(newRolePool(t, database.DSN, "vela_platform_operator_auth_login", "vela-platform-operator-auth-password"), verifier),
		Remediation:            service,
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              &retention.Service{},
		Admission:              &admission.Service{},
		Cancellation:           &cancellation.Service{},
		Artifacts:              &artifactaccess.Service{},
		Webhooks:               &webhook.Service{},
	})
	if err != nil {
		t.Fatalf("create remediation HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	operationID := uuid.New()
	evidence := sha256.Sum256([]byte("L6 hardware fault"))
	body, err := json.Marshal(map[string]any{
		"operation_id": operationID, "worker_instance_id": workerID, "worker_instance_epoch": 1,
		"node_identity":   "node-remediation-1",
		"device_identity": remediationGPUUUID,
		"failure_class":   "GPU_UNRECOVERABLE", "evidence_sha256": hex.EncodeToString(evidence[:]),
		"certification_revision": "matrix-l6-v1", "action_level": "L6_BMC_POWER_CYCLE",
	})
	if err != nil {
		t.Fatalf("encode remediation HTTP request: %v", err)
	}
	endpoint := server.URL + "/v1/platform/remediation/operations"
	unauthorized := doBreakGlassHTTP(t, http.MethodPost, endpoint, "customer-token", "remediation-http-1", body)
	if unauthorized.StatusCode != http.StatusUnauthorized {
		t.Fatalf("customer remediation request status = %d body=%s", unauthorized.StatusCode, unauthorized.Body)
	}
	created := doBreakGlassHTTP(t, http.MethodPost, endpoint, "remediation-requester-token", "remediation-http-1", body)
	if created.StatusCode != http.StatusCreated {
		t.Fatalf("create remediation status = %d body=%s", created.StatusCode, created.Body)
	}
	replayed := doBreakGlassHTTP(t, http.MethodPost, endpoint, "remediation-requester-token", "remediation-http-1", body)
	if replayed.StatusCode != http.StatusOK {
		t.Fatalf("replay remediation status = %d body=%s", replayed.StatusCode, replayed.Body)
	}
	executionEndpoint := endpoint + "/" + operationID.String() + "/execution"
	blocked := doBreakGlassHTTP(t, http.MethodPost, executionEndpoint, "remediation-requester-token", "", nil)
	if blocked.StatusCode != http.StatusConflict {
		t.Fatalf("unapproved L6 execution status = %d body=%s", blocked.StatusCode, blocked.Body)
	}
	approvalEndpoint := endpoint + "/" + operationID.String() + "/approvals"
	first := doBreakGlassHTTP(t, http.MethodPost, approvalEndpoint, "remediation-approver-token", "", nil)
	if first.StatusCode != http.StatusOK {
		t.Fatalf("first L6 approval status = %d body=%s", first.StatusCode, first.Body)
	}
	second := doBreakGlassHTTP(t, http.MethodPost, approvalEndpoint, "remediation-third-token", "", nil)
	if second.StatusCode != http.StatusOK {
		t.Fatalf("second L6 approval status = %d body=%s", second.StatusCode, second.Body)
	}
	started := doBreakGlassHTTP(t, http.MethodPost, executionEndpoint, "remediation-requester-token", "", nil)
	if started.StatusCode != http.StatusOK {
		t.Fatalf("approved L6 execution status = %d body=%s", started.StatusCode, started.Body)
	}
	var projection struct {
		State          string `json:"state"`
		RequestedBy    string `json:"requested_by"`
		FirstApprover  string `json:"first_approver"`
		SecondApprover string `json:"second_approver"`
	}
	if err := json.Unmarshal(started.Body, &projection); err != nil {
		t.Fatalf("decode started remediation: %v", err)
	}
	if projection.State != "EXECUTING" ||
		projection.RequestedBy != "platform-operator/"+platformOperatorRequesterID ||
		projection.FirstApprover != "platform-operator/"+platformOperatorApproverID ||
		projection.SecondApprover != "platform-operator/"+platformOperatorThirdID {
		t.Fatalf("started remediation projection = %#v", projection)
	}
}

func TestRemediationDispatcherQuarantinesFailedCertifiedPostcheck(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	workerID := seedRemediationWorker(t, database)
	service, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create remediation runtime service: %v", err)
	}
	const (
		gpuUUID = remediationGPUUUID
		pciBDF  = "0000:41:00.0"
		actor   = "controller/remediation-dispatcher"
		spiffe  = "spiffe://vela.internal/controller/remediation-dispatcher"
	)
	evidence := sha256.Sum256([]byte("GPU process failure"))
	request := remediation.Request{
		OperationID: uuid.New(), WorkerInstanceID: workerID, WorkerInstanceEpoch: 1,
		NodeIdentity: "node-remediation-1", DeviceIdentity: gpuUUID,
		FailureClass: "PROCESS_FAILURE", EvidenceDigest: evidence[:],
		CertificationRevision: "matrix-v1", ActionLevel: remediation.ActionL0ProcessRestart,
		IdempotencyKey: "runtime-postcheck-failure", RequestedBy: "platform-operator/test",
	}
	if _, err := service.Request(context.Background(), request); err != nil {
		t.Fatalf("request runtime remediation: %v", err)
	}
	if _, err := service.Start(context.Background(), request.OperationID, workerID, 1, "platform-operator/test"); err != nil {
		t.Fatalf("start runtime remediation: %v", err)
	}
	runner := remediationRuntimeCommandRunner{postcheckHealthy: false}
	allowlisted, err := remediation.NewAllowlistedExecutor(runner, map[remediation.ActionLevel]struct {
		Path string
		Args []string
	}{remediation.ActionL0ProcessRestart: {Path: "/usr/local/libexec/vela-process-restart"}})
	if err != nil {
		t.Fatalf("configure runtime remediation allowlist: %v", err)
	}
	policy, err := nodeagent.NewStaticCapabilityPolicy(map[string]nodeagent.DeviceCapability{
		gpuUUID: {
			DeviceID: uuid.MustParse(workerRegistryDeviceID), DeviceEpoch: 1,
			GPUUUID: gpuUUID, PCIBDF: pciBDF, CertificationRevision: "matrix-v1",
			FailureClasses: map[string]bool{"PROCESS_FAILURE": true},
			Actions:        map[remediation.ActionLevel]bool{remediation.ActionL0ProcessRestart: true},
		},
	}, remediationDeviceEpochSource{gpuUUID: 1})
	if err != nil {
		t.Fatalf("configure runtime remediation capability matrix: %v", err)
	}
	fence, err := nodeagent.NewCommandFence(runner, "/usr/local/libexec/vela-fence", nil)
	if err != nil {
		t.Fatalf("configure runtime remediation fence: %v", err)
	}
	postcheck, err := nodeagent.NewCommandPostcheck(runner, "/usr/local/libexec/vela-postcheck", nil)
	if err != nil {
		t.Fatalf("configure runtime remediation post-check: %v", err)
	}
	limiter, err := nodeagent.NewRateLimiter(nodeagent.RateLimit{
		MinimumInterval: time.Second, Window: time.Minute, MaxExecutions: 1,
	})
	if err != nil {
		t.Fatalf("configure runtime remediation rate limit: %v", err)
	}
	executor, err := nodeagent.NewCertifiedExecutor(allowlisted, policy, fence, postcheck, limiter)
	if err != nil {
		t.Fatalf("configure runtime certified executor: %v", err)
	}
	resolver, err := nodeagent.NewStaticControllerIdentityResolver(map[string]string{spiffe: actor})
	if err != nil {
		t.Fatalf("configure runtime controller identity: %v", err)
	}
	hostLedger, err := nodeagent.NewFileLedger(t.TempDir())
	if err != nil {
		t.Fatalf("configure runtime host ledger: %v", err)
	}
	host, err := nodeagent.NewServer(nodeagent.NodeAgentIdentity{
		NodeIdentity: "node-remediation-1", AgentID: uuid.New(), AgentEpoch: 1,
	}, resolver, executor, hostLedger)
	if err != nil {
		t.Fatalf("configure runtime Node Agent: %v", err)
	}
	client, err := nodeagent.NewClient(&directNodeAgentClient{server: host, spiffe: spiffe}, actor)
	if err != nil {
		t.Fatalf("configure runtime Node Agent client: %v", err)
	}
	dispatcher, err := nodeagent.NewExecutionDispatcher(service, integrationAgentResolver{client: client}, actor, 10)
	if err != nil {
		t.Fatalf("configure runtime remediation dispatcher: %v", err)
	}
	dispatched, err := dispatcher.RunOnce(context.Background())
	if err != nil || dispatched.Dispatched != 1 || dispatched.Deferred != 0 {
		t.Fatalf("runtime remediation dispatch = %#v error=%v", dispatched, err)
	}
	operation, err := service.Get(context.Background(), request.OperationID)
	if err != nil || operation.State != remediation.StateQuarantined || operation.ResultCode != "POSTCHECK_FAILED" {
		t.Fatalf("failed runtime remediation = %#v error=%v", operation, err)
	}
}

func TestRemediationOperationIsBoundedIdempotentAndAudited(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	workerID := seedRemediationWorker(t, database)
	service, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create Remediation service: %v", err)
	}

	evidence := sha256.Sum256([]byte("worker fault evidence"))
	request := remediation.Request{
		OperationID:           uuid.MustParse("10000000-0000-0000-0000-000000001920"),
		WorkerInstanceID:      workerID,
		WorkerInstanceEpoch:   1,
		NodeIdentity:          "node-remediation-1",
		DeviceIdentity:        remediationGPUUUID,
		FailureClass:          "CUDA_CONTEXT_STALE",
		EvidenceDigest:        evidence[:],
		CertificationRevision: "matrix-v1",
		ActionLevel:           remediation.ActionL0ProcessRestart,
		IdempotencyKey:        "remediation-idempotency-1",
		RequestedBy:           "node-agent-1",
	}
	created, err := service.Request(context.Background(), request)
	if err != nil || created.Replayed || created.State != remediation.StateRequested ||
		string(created.WorkerLifecycleState) != "DRAINING" {
		t.Fatalf("request Remediation = %#v error=%v", created, err)
	}
	replayed, err := service.Request(context.Background(), request)
	if err != nil || !replayed.Replayed || replayed.OperationID != request.OperationID {
		t.Fatalf("replay Remediation = %#v error=%v", replayed, err)
	}

	_, err = service.Start(context.Background(), request.OperationID, workerID, 2, "node-agent-1")
	assertRemediationFailure(t, err, remediation.FailureConflict)
	started, err := service.Start(context.Background(), request.OperationID, workerID, 1, "node-agent-1")
	if err != nil || started.State != remediation.StateExecuting {
		t.Fatalf("start Remediation = %#v error=%v", started, err)
	}
	claimID := uuid.MustParse("10000000-0000-0000-0000-000001920001")
	claim, err := service.ClaimExecution(
		context.Background(), request.OperationID, workerID, 1, claimID, "node-agent-1",
	)
	if err != nil || claim.Replayed || claim.ClaimID != claimID {
		t.Fatalf("claim Remediation execution = %#v error=%v", claim, err)
	}
	replayedClaim, err := service.ClaimExecution(
		context.Background(), request.OperationID, workerID, 1, claimID, "node-agent-1",
	)
	if err != nil || !replayedClaim.Replayed || replayedClaim.ClaimID != claimID {
		t.Fatalf("replay Remediation execution = %#v error=%v", replayedClaim, err)
	}
	_, err = service.ClaimExecution(
		context.Background(), request.OperationID, workerID, 1, uuid.New(), "node-agent-2",
	)
	assertRemediationFailure(t, err, remediation.FailureConflict)
	postcheck := sha256.Sum256([]byte("post-check"))
	completed, err := service.Complete(context.Background(), remediation.Completion{
		OperationID: request.OperationID, WorkerInstanceID: workerID, WorkerInstanceEpoch: 1,
		Success: true, ResultCode: "POSTCHECK_OK", ResultDetail: "process restarted and checks passed",
		PostcheckHash: postcheck[:], ActorIdentity: "node-agent-1",
	})
	if err != nil || completed.State != remediation.StateSucceeded ||
		string(completed.WorkerLifecycleState) != "FENCED" || string(completed.WorkerReachability) != "UNREACHABLE" {
		t.Fatalf("complete Remediation = %#v error=%v", completed, err)
	}
	operation, err := service.Get(context.Background(), request.OperationID)
	if err != nil || operation.State != remediation.StateSucceeded || operation.ResultCode != "POSTCHECK_OK" {
		t.Fatalf("get Remediation operation = %#v error=%v", operation, err)
	}
	if operation.DeviceID != uuid.MustParse(workerRegistryDeviceID) || operation.DeviceEpoch != 1 {
		t.Fatalf("Remediation exact Device authority = %s/%d", operation.DeviceID, operation.DeviceEpoch)
	}
	var workerEpoch int64
	var lifecycle, reachability string
	var activeBindings int
	if err := database.Admin.QueryRow(`
		SELECT worker.instance_epoch, worker.lifecycle_state, worker.reachability_state,
			(SELECT count(*) FROM active_device_bindings AS binding
			 WHERE binding.worker_instance_id = worker.id)
		FROM worker_instances AS worker
		WHERE worker.id = $1
	`, workerID).Scan(&workerEpoch, &lifecycle, &reachability, &activeBindings); err != nil {
		t.Fatalf("read post-Remediation WorkerInstance authority: %v", err)
	}
	if workerEpoch != 2 || lifecycle != "FENCED" || reachability != "UNREACHABLE" || activeBindings != 0 {
		t.Fatalf("post-Remediation WorkerInstance authority = epoch %d %s/%s bindings=%d", workerEpoch, lifecycle, reachability, activeBindings)
	}
	var events int
	if err := database.Admin.QueryRow(
		"SELECT count(*) FROM remediation_operation_events WHERE operation_id = $1",
		request.OperationID,
	).Scan(&events); err != nil {
		t.Fatalf("count Remediation audit events: %v", err)
	}
	if events != 3 {
		t.Fatalf("Remediation audit event count = %d, want 3", events)
	}
	assertRemediationSQLState(t, database.Admin, `
		UPDATE remediation_operations
		SET device_identity = 'GPU-REM-MUTATED'
		WHERE id = $1
	`, request.OperationID, "55000")
	assertRemediationSQLState(t, database.Admin, `
		DELETE FROM remediation_operations WHERE id = $1
	`, request.OperationID, "55000")
	assertRemediationSQLState(t, database.Admin, `
		UPDATE remediation_operation_events
		SET result_code = 'MUTATED'
		WHERE operation_id = $1 AND sequence = 1
	`, request.OperationID, "55000")
	assertRemediationSQLState(t, database.Admin, `
		DELETE FROM remediation_operation_events
		WHERE operation_id = $1 AND sequence = 1
	`, request.OperationID, "55000")

	conflicting := request
	conflicting.OperationID = uuid.MustParse("10000000-0000-0000-0000-000000001921")
	conflicting.DeviceIdentity = "GPU-REM-1"
	_, err = service.Request(context.Background(), conflicting)
	assertRemediationFailure(t, err, remediation.FailureConflict)
}

func TestRemediationRequestRequiresExactWorkerInstanceDeviceBinding(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	workerID := seedRemediationWorker(t, database)
	service, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create Device-bound Remediation service: %v", err)
	}
	evidence := sha256.Sum256([]byte("wrong Device binding evidence"))
	request := remediation.Request{
		OperationID: uuid.New(), WorkerInstanceID: workerID, WorkerInstanceEpoch: 1,
		NodeIdentity:   "node-remediation-1",
		DeviceIdentity: "GPU-11111111-1111-1111-1111-111111111111",
		FailureClass:   "GPU_FAULT", EvidenceDigest: evidence[:],
		CertificationRevision: "matrix-v1", ActionLevel: remediation.ActionL2GPUReset,
		IdempotencyKey: "wrong-device-binding", RequestedBy: "node-agent-1",
	}
	_, err = service.Request(context.Background(), request)
	assertRemediationFailure(t, err, remediation.FailureConflict)
	var operations int
	if err := database.Admin.QueryRow(
		"SELECT count(*) FROM remediation_operations WHERE worker_instance_id = $1",
		workerID,
	).Scan(&operations); err != nil {
		t.Fatalf("count rejected Device-bound Remediation operations: %v", err)
	}
	if operations != 0 {
		t.Fatalf("rejected Device-bound Remediation persisted %d operations", operations)
	}
}

func TestRemediationExecutionClaimSerializesConcurrentProcesses(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	workerID := seedRemediationWorker(t, database)
	firstService, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create first Remediation service: %v", err)
	}
	secondService, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create second Remediation service: %v", err)
	}
	evidence := sha256.Sum256([]byte("concurrent execution claim"))
	request := remediation.Request{
		OperationID: uuid.New(), WorkerInstanceID: workerID, WorkerInstanceEpoch: 1,
		NodeIdentity: "node-remediation-1", DeviceIdentity: remediationGPUUUID,
		FailureClass: "GPU_FAULT", EvidenceDigest: evidence[:], CertificationRevision: "matrix-v1",
		ActionLevel: remediation.ActionL2GPUReset, IdempotencyKey: "concurrent-execution-claim",
		RequestedBy: "controller/setup",
	}
	if _, err := firstService.Request(context.Background(), request); err != nil {
		t.Fatalf("request concurrent-claim Remediation: %v", err)
	}
	if _, err := firstService.Start(context.Background(), request.OperationID, workerID, 1, "controller/setup"); err != nil {
		t.Fatalf("start concurrent-claim Remediation: %v", err)
	}
	type claimCall struct {
		service *remediation.Service
		claimID uuid.UUID
		actor   string
	}
	type claimOutcome struct {
		call   claimCall
		result remediation.ClaimResult
		err    error
	}
	calls := []claimCall{
		{service: firstService, claimID: uuid.New(), actor: "controller/first"},
		{service: secondService, claimID: uuid.New(), actor: "controller/second"},
	}
	start := make(chan struct{})
	outcomes := make(chan claimOutcome, len(calls))
	var callers sync.WaitGroup
	for _, call := range calls {
		call := call
		callers.Add(1)
		go func() {
			defer callers.Done()
			<-start
			result, callErr := call.service.ClaimExecution(
				context.Background(), request.OperationID, workerID, 1, call.claimID, call.actor,
			)
			outcomes <- claimOutcome{call: call, result: result, err: callErr}
		}()
	}
	close(start)
	callers.Wait()
	close(outcomes)
	var winner *claimOutcome
	conflicts := 0
	for outcome := range outcomes {
		if outcome.err == nil {
			copyOfOutcome := outcome
			winner = &copyOfOutcome
			continue
		}
		var failure *remediation.Failure
		if !errors.As(outcome.err, &failure) || failure.Code != remediation.FailureConflict {
			t.Fatalf("concurrent claim error = %v, want FailureConflict", outcome.err)
		}
		conflicts++
	}
	if winner == nil || conflicts != 1 || winner.result.Replayed || winner.result.ClaimID != winner.call.claimID {
		t.Fatalf("concurrent claim winner=%#v conflicts=%d", winner, conflicts)
	}
	replayed, err := winner.call.service.ClaimExecution(
		context.Background(), request.OperationID, workerID, 1, winner.call.claimID, winner.call.actor,
	)
	if err != nil || !replayed.Replayed || replayed.ClaimID != winner.call.claimID {
		t.Fatalf("replay winning concurrent claim = %#v error=%v", replayed, err)
	}
}

func TestRemediationL6RequiresTwoApproversAndQuarantinesOnFailure(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	workerID := seedRemediationWorker(t, database)
	service, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create Remediation service: %v", err)
	}
	evidence := sha256.Sum256([]byte("BMC evidence"))
	request := remediation.Request{
		OperationID:           uuid.MustParse("10000000-0000-0000-0000-000000001922"),
		WorkerInstanceID:      workerID,
		WorkerInstanceEpoch:   1,
		NodeIdentity:          "node-remediation-1",
		DeviceIdentity:        remediationGPUUUID,
		FailureClass:          "NODE_UNRESPONSIVE",
		EvidenceDigest:        evidence[:],
		CertificationRevision: "matrix-v2",
		ActionLevel:           remediation.ActionL6BMCPowerCycle,
		IdempotencyKey:        "remediation-l6-1",
		RequestedBy:           "node-agent-1",
	}
	created, err := service.Request(context.Background(), request)
	if err != nil || created.State != remediation.StateApprovalRequired || !created.RequiresApproval {
		t.Fatalf("request L6 Remediation = %#v error=%v", created, err)
	}
	_, err = service.Start(context.Background(), request.OperationID, workerID, 1, "node-agent-1")
	assertRemediationFailure(t, err, remediation.FailureConflict)
	first, err := service.Approve(context.Background(), request.OperationID, "operator-a")
	if err != nil || first.ApprovalCount != 1 || !first.RequiresApproval {
		t.Fatalf("first L6 approval = %#v error=%v", first, err)
	}
	_, err = service.Start(context.Background(), request.OperationID, workerID, 1, "node-agent-1")
	assertRemediationFailure(t, err, remediation.FailureConflict)
	replayed, err := service.Approve(context.Background(), request.OperationID, "operator-a")
	if err != nil || !replayed.Replayed || replayed.ApprovalCount != 1 {
		t.Fatalf("replayed first L6 approval = %#v error=%v", replayed, err)
	}
	second, err := service.Approve(context.Background(), request.OperationID, "operator-b")
	if err != nil || second.ApprovalCount != 2 || second.State != remediation.StateRequested {
		t.Fatalf("second L6 approval = %#v error=%v", second, err)
	}
	if _, err := service.Start(context.Background(), request.OperationID, workerID, 1, "node-agent-1"); err != nil {
		t.Fatalf("start approved L6 Remediation: %v", err)
	}
	completed, err := service.Complete(context.Background(), remediation.Completion{
		OperationID: request.OperationID, WorkerInstanceID: workerID, WorkerInstanceEpoch: 1,
		Success: false, ResultCode: "BMC_POSTCHECK_FAILED", ResultDetail: "node identity did not return",
		ActorIdentity: "node-agent-1",
	})
	if err != nil || completed.State != remediation.StateQuarantined ||
		string(completed.WorkerLifecycleState) != "FENCED" || string(completed.WorkerReachability) != "UNREACHABLE" {
		t.Fatalf("failed L6 Remediation = %#v error=%v", completed, err)
	}
	var lifecycle, reachability string
	if err := database.Admin.QueryRow(
		"SELECT lifecycle_state, reachability_state FROM worker_instances WHERE id = $1", workerID,
	).Scan(&lifecycle, &reachability); err != nil {
		t.Fatalf("read fenced WorkerInstance: %v", err)
	}
	if lifecycle != "FENCED" || reachability != "UNREACHABLE" {
		t.Fatalf("fenced WorkerInstance = %s/%s", lifecycle, reachability)
	}
}

func TestRemediationL7QuarantineIsImmediateAndEpochBound(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	workerID := seedRemediationWorker(t, database)
	service, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create Remediation service: %v", err)
	}
	evidence := sha256.Sum256([]byte("quarantine evidence"))
	request := remediation.Request{
		OperationID:           uuid.MustParse("10000000-0000-0000-0000-000000001923"),
		WorkerInstanceID:      workerID,
		WorkerInstanceEpoch:   9,
		NodeIdentity:          "node-remediation-1",
		DeviceIdentity:        remediationGPUUUID,
		FailureClass:          "IDENTITY_AMBIGUOUS",
		EvidenceDigest:        evidence[:],
		CertificationRevision: "fail-closed-v1",
		ActionLevel:           remediation.ActionL7Quarantine,
		IdempotencyKey:        "remediation-l7-wrong-epoch",
		RequestedBy:           "node-agent-1",
	}
	_, err = service.Request(context.Background(), request)
	assertRemediationFailure(t, err, remediation.FailureConflict)
	request.WorkerInstanceEpoch = 1
	request.IdempotencyKey = "remediation-l7-1"
	request.CertificationRevision = ""
	result, err := service.Request(context.Background(), request)
	if err != nil || result.State != remediation.StateQuarantined || result.Replayed {
		t.Fatalf("immediate L7 Remediation = %#v error=%v", result, err)
	}
	var startedAt, finishedAt sql.NullTime
	var resultCode string
	if err := database.Admin.QueryRow(`
		SELECT started_at, finished_at, result_code
		FROM remediation_operations WHERE id = $1
	`, request.OperationID).Scan(&startedAt, &finishedAt, &resultCode); err != nil {
		t.Fatalf("read immediate L7 operation: %v", err)
	}
	if !startedAt.Valid || !finishedAt.Valid || resultCode != "QUARANTINED_BY_POLICY" {
		t.Fatalf(
			"L7 terminal fields = started %t finished %t result %q",
			startedAt.Valid, finishedAt.Valid, resultCode,
		)
	}
	if _, err := service.Request(context.Background(), remediation.Request{
		OperationID:      uuid.MustParse("10000000-0000-0000-0000-000000001929"),
		WorkerInstanceID: workerID, WorkerInstanceEpoch: 1, NodeIdentity: request.NodeIdentity,
		DeviceIdentity: request.DeviceIdentity, FailureClass: "NEW_FAULT",
		EvidenceDigest: evidence[:], CertificationRevision: "matrix-v1",
		ActionLevel:    remediation.ActionL0ProcessRestart,
		IdempotencyKey: "remediation-after-quarantine", RequestedBy: "node-agent-1",
	}); err == nil {
		t.Fatal("new remediation was accepted for a quarantined Worker")
	} else {
		assertRemediationFailure(t, err, remediation.FailureConflict)
	}
	assertRemediationSQLState(t, database.Admin, `
		UPDATE worker_instances SET lifecycle_state = 'READY' WHERE id = $1
	`, workerID, "23514")
}

func TestRemediationRecoveryQuarantinesExpiredExecutingOperation(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	workerID := seedRemediationWorker(t, database)
	service, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create Remediation service: %v", err)
	}
	evidence := sha256.Sum256([]byte("orphaned execution evidence"))
	request := remediation.Request{
		OperationID:      uuid.MustParse("10000000-0000-0000-0000-000000001931"),
		WorkerInstanceID: workerID, WorkerInstanceEpoch: 1, NodeIdentity: "node-remediation-1",
		DeviceIdentity: remediationGPUUUID, FailureClass: "NODE_AGENT_LOST",
		EvidenceDigest: evidence[:], CertificationRevision: "matrix-orphan-v1",
		ActionLevel:    remediation.ActionL0ProcessRestart,
		IdempotencyKey: "remediation-orphaned-execution-1", RequestedBy: "node-agent-orphan",
	}
	if _, err := service.Request(context.Background(), request); err != nil {
		t.Fatalf("request orphaned Remediation: %v", err)
	}
	if _, err := service.Start(context.Background(), request.OperationID, workerID, 1, request.RequestedBy); err != nil {
		t.Fatalf("start orphaned Remediation: %v", err)
	}
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin deadline fixture transaction: %v", err)
	}
	if _, err := tx.Exec("SET LOCAL session_replication_role = 'replica'"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("disable triggers for deadline fixture: %v", err)
	}
	if _, err := tx.Exec(
		"UPDATE remediation_operations SET deadline_at = requested_at + interval '1 millisecond' WHERE id = $1",
		request.OperationID,
	); err != nil {
		_ = tx.Rollback()
		t.Fatalf("expire Remediation deadline fixture: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit deadline fixture: %v", err)
	}
	recovered, err := service.Recover(context.Background(), remediation.Recovery{
		OperationID: request.OperationID, ActorIdentity: "remediation-reconciler",
	})
	if err != nil || recovered.State != remediation.StateQuarantined ||
		recovered.ResultCode != "REMEDIATION_DEADLINE_EXPIRED" {
		t.Fatalf("recovered orphaned Remediation = %#v error=%v", recovered, err)
	}
	replayed, err := service.Recover(context.Background(), remediation.Recovery{
		OperationID: request.OperationID, ActorIdentity: "remediation-reconciler",
	})
	if err != nil || !replayed.Replayed || replayed.State != remediation.StateQuarantined {
		t.Fatalf("replayed recovery = %#v error=%v", replayed, err)
	}
}

func TestRemediationMutationsShareWorkerLockOrder(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	workerID := seedRemediationWorker(t, database)
	service, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create Remediation service: %v", err)
	}
	evidence := sha256.Sum256([]byte("lock order evidence"))
	request := remediation.Request{
		OperationID:      uuid.MustParse("10000000-0000-0000-0000-000000001932"),
		WorkerInstanceID: workerID, WorkerInstanceEpoch: 1, NodeIdentity: "node-remediation-1",
		DeviceIdentity: remediationGPUUUID, FailureClass: "LOCK_ORDER_TEST",
		EvidenceDigest: evidence[:], CertificationRevision: "matrix-lock-v1",
		ActionLevel:    remediation.ActionL0ProcessRestart,
		IdempotencyKey: "remediation-lock-order-1", RequestedBy: "node-agent-lock",
	}
	if _, err := service.Request(context.Background(), request); err != nil {
		t.Fatalf("request lock-order Remediation: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		_, callErr := service.Start(ctx, request.OperationID, workerID, 1, request.RequestedBy)
		errs <- callErr
	}()
	go func() {
		defer wait.Done()
		<-start
		_, callErr := service.Request(ctx, request)
		errs <- callErr
	}()
	close(start)
	wait.Wait()
	close(errs)
	for callErr := range errs {
		if callErr != nil {
			t.Fatalf("concurrent request/start lock-order call: %v", callErr)
		}
	}
	postcheck := sha256.Sum256([]byte("lock order post-check"))
	completed, err := service.Complete(ctx, remediation.Completion{
		OperationID: request.OperationID, WorkerInstanceID: workerID, WorkerInstanceEpoch: 1,
		Success: true, ResultCode: "POSTCHECK_OK", ResultDetail: "lock order complete",
		PostcheckHash: postcheck[:], ActorIdentity: request.RequestedBy,
	})
	if err != nil || completed.State != remediation.StateSucceeded {
		t.Fatalf("lock-order completion = %#v error=%v", completed, err)
	}
}

func TestRemediationCompletionQuarantinesWorkerInstanceWithActiveStageLease(t *testing.T) {
	database, _, coordinator, _, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "remediation-active-stage-lease")
	assignment := assignEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().UTC().Add(time.Minute),
	)
	workerID := assignment.WorkerInstanceID
	service, err := remediation.NewService(newRolePool(
		t, database.DSN, "vela_remediation_login", "vela-remediation-password",
	))
	if err != nil {
		t.Fatalf("create active StageLease Remediation service: %v", err)
	}
	evidence := sha256.Sum256([]byte("active StageLease evidence"))
	request := remediation.Request{
		OperationID:           uuid.MustParse("10000000-0000-0000-0000-000000001928"),
		WorkerInstanceID:      workerID,
		WorkerInstanceEpoch:   1,
		NodeIdentity:          "h3-node-01",
		DeviceIdentity:        remediationGPUUUID,
		FailureClass:          "STAGE_LEASE_STUCK",
		EvidenceDigest:        evidence[:],
		CertificationRevision: "matrix-active-stage-lease-v1",
		ActionLevel:           remediation.ActionL0ProcessRestart,
		IdempotencyKey:        "remediation-active-stage-lease-1",
		RequestedBy:           "node-agent-active-1",
	}
	if _, err := service.Request(context.Background(), request); err != nil {
		t.Fatalf("request active StageLease Remediation: %v", err)
	}
	if _, err := service.Start(context.Background(), request.OperationID, workerID, 1, request.RequestedBy); err != nil {
		t.Fatalf("start active StageLease Remediation: %v", err)
	}
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin active StageLease invariant fixture: %v", err)
	}
	if _, err := tx.Exec("SET LOCAL session_replication_role = 'replica'"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("disable StageLease authority triggers for invariant fixture: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE stage_leases
		SET state = 'ACTIVE', revoked_at = NULL, revoke_reason = NULL
		WHERE id = $1
	`, assignment.StageLeaseID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("restore active StageLease invariant fixture: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit active StageLease invariant fixture: %v", err)
	}
	postcheck := sha256.Sum256([]byte("post-check-with-active-stage-lease"))
	completed, err := service.Complete(context.Background(), remediation.Completion{
		OperationID: request.OperationID, WorkerInstanceID: workerID, WorkerInstanceEpoch: 1,
		Success: true, ResultCode: "POSTCHECK_OK", ResultDetail: "post-check passed but StageLease remains active",
		PostcheckHash: postcheck[:], ActorIdentity: request.RequestedBy,
	})
	if err != nil || completed.State != remediation.StateQuarantined || completed.ResultCode != "ACTIVE_STAGE_LEASE_REMAINS" {
		t.Fatalf("active StageLease completion = %#v error=%v", completed, err)
	}
	var lifecycle, reachability string
	if err := database.Admin.QueryRow(
		"SELECT lifecycle_state, reachability_state FROM worker_instances WHERE id = $1", workerID,
	).Scan(&lifecycle, &reachability); err != nil {
		t.Fatalf("read active StageLease fenced WorkerInstance: %v", err)
	}
	if lifecycle != "FENCED" || reachability != "UNREACHABLE" {
		t.Fatalf("active StageLease WorkerInstance = %s/%s", lifecycle, reachability)
	}
}

func TestRemediationMigrationDownAndUpPreservesRoles(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 19)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 18); err != nil {
		t.Fatalf("Remediation migration down: %v", err)
	}
	assertTableDoesNotExist(t, database.Admin, "remediation_operations")
	assertTableDoesNotExist(t, database.Admin, "remediation_operation_events")
	assertTableDoesNotExist(t, database.Admin, "remediation_execution_claims")
	var nodeIdentityExists bool
	if err := database.Admin.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'workers' AND column_name = 'node_identity'
		)
	`).Scan(&nodeIdentityExists); err != nil {
		t.Fatalf("inspect node_identity after down: %v", err)
	}
	if nodeIdentityExists {
		t.Fatal("node_identity survived Remediation migration down")
	}
	assertRoleExists(t, database.Admin, "vela_remediation")
	assertRoleExists(t, database.Admin, "vela_remediation_owner")
	if err := goose.UpTo(database.Admin, migrations, 22); err != nil {
		t.Fatalf("Remediation migration up: %v", err)
	}
	assertTableExists(t, database.Admin, "remediation_operations")
	assertTableExists(t, database.Admin, "remediation_operation_events")
	assertTableExists(t, database.Admin, "remediation_execution_claims")
	var dispatchFunction string
	if err := database.Admin.QueryRow(
		"SELECT to_regprocedure('vela_list_executing_remediation(integer)')",
	).Scan(&dispatchFunction); err != nil {
		t.Fatalf("inspect Remediation dispatch function: %v", err)
	}
	if dispatchFunction == "" {
		t.Fatal("Remediation dispatch function missing after migration up")
	}
	if err := database.Admin.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'workers' AND column_name = 'node_identity'
		)
	`).Scan(&nodeIdentityExists); err != nil {
		t.Fatalf("inspect node_identity after up: %v", err)
	}
	if !nodeIdentityExists {
		t.Fatal("node_identity missing after Remediation migration up")
	}
}

type remediationRuntimeCommandRunner struct {
	postcheckHealthy bool
}

func (runner remediationRuntimeCommandRunner) Run(
	_ context.Context,
	plan remediation.Plan,
	path string,
	_ []string,
) ([]byte, error) {
	if path == "/usr/local/libexec/vela-process-restart" {
		return []byte("process restarted"), nil
	}
	evidence := map[string]any{
		"operation_id": plan.OperationID.String(), "execution_claim_id": plan.ExecutionClaimID.String(),
		"worker_instance_id": plan.WorkerInstanceID.String(), "worker_instance_epoch": plan.WorkerInstanceEpoch,
		"device_id": plan.DeviceID.String(), "device_epoch": plan.DeviceEpoch,
		"node_identity": plan.NodeIdentity, "device_identity": plan.DeviceIdentity,
		"gpu_uuid": plan.GPUUUID, "pci_bdf": plan.PCIBDF,
		"failure_class": plan.FailureClass, "action_level": string(plan.ActionLevel),
		"certification_revision":  plan.CertificationRevision,
		"failure_evidence_sha256": hex.EncodeToString(plan.FailureEvidenceDigest),
	}
	switch path {
	case "/usr/local/libexec/vela-fence":
		evidence["new_assignments_stopped"] = true
		evidence["target_processes_stopped"] = true
	case "/usr/local/libexec/vela-postcheck":
		evidence["device_healthy"] = runner.postcheckHealthy
		evidence["inference_backend_healthy"] = runner.postcheckHealthy
		evidence["detail"] = "runtime post-check evidence"
	default:
		return nil, errors.New("unexpected remediation runtime command")
	}
	return json.Marshal(evidence)
}

type directNodeAgentClient struct {
	velav1.NodeAgentServiceClient
	server *nodeagent.Server
	spiffe string
}

func (client *directNodeAgentClient) ExecuteRemediation(
	ctx context.Context,
	request *velav1.ExecuteRemediationRequest,
	_ ...grpc.CallOption,
) (*velav1.ExecuteRemediationResponse, error) {
	identity, err := url.Parse(client.spiffe)
	if err != nil {
		return nil, err
	}
	certificate := &x509.Certificate{Raw: []byte(client.spiffe), URIs: []*url.URL{identity}}
	authenticated := peer.NewContext(ctx, &peer.Peer{AuthInfo: credentials.TLSInfo{
		State: tls.ConnectionState{
			HandshakeComplete: true,
			PeerCertificates:  []*x509.Certificate{certificate},
			VerifiedChains:    [][]*x509.Certificate{{certificate}},
		},
	}})
	return client.server.ExecuteRemediation(authenticated, request)
}

type integrationAgentResolver struct {
	client *nodeagent.Client
}

func (resolver integrationAgentResolver) Resolve(
	context.Context,
	string,
) (*nodeagent.Client, error) {
	return resolver.client, nil
}

func seedRemediationWorker(t *testing.T, database testDatabase) uuid.UUID {
	t.Helper()
	workerID := uuid.MustParse("10000000-0000-0000-0000-000000001925")
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedWorkerRegistryPlan(t, database.Admin)
	seedWorkerInstance(t, database.Admin, workerID, workerRegistryProfileID, 1, 1)
	evidence := workerRegistryEvidenceValue(t, workerID, 0xe8)
	evidence.DeviceSet.Devices[0].NodeIdentity = "node-remediation-1"
	registry, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("construct Remediation Worker Registry: %v", err)
	}
	if _, err := registry.Observe(context.Background(), evidence); err != nil {
		t.Fatalf("observe Remediation WorkerInstance: %v", err)
	}
	return workerID
}

func assertRemediationFailure(t *testing.T, err error, code remediation.FailureCode) {
	t.Helper()
	var failure *remediation.Failure
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("Remediation error = %v, want %s", err, code)
	}
}

func assertRemediationSQLState(t *testing.T, database *sql.DB, statement string, argument uuid.UUID, code string) {
	t.Helper()
	_, err := database.Exec(statement, argument)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != code {
		t.Fatalf("Remediation mutation error = %v, want SQLSTATE %s", err, code)
	}
}

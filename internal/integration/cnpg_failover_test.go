//go:build integration && cnpg

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/scheduler"
	"github.com/vivym/vela/internal/workercontrol"
	corev1 "k8s.io/api/core/v1"
)

const (
	cnpgNamespace   = "vela-system"
	cnpgClusterName = "vela-postgres"
	cnpgServiceName = "vela-postgres-rw"
)

func TestCloudNativePGSingleNodeFailoverPreservesAuthorityAndNoQuorumFailsClosed(
	t *testing.T,
) {
	harness := newCNPGHarness(t)
	harness.waitForThreeReadyInstances(t, 5*time.Minute)
	harness.startPortForward(t)

	database := harness.openDatabase(t)
	harness.assertReplicationHealth(t, database.Admin, 2)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "CNPG failover conformance")
	seedAdmissionFixture(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, adjacentRolloutNMinusOneCommit)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}

	const (
		chargedWorkerID = "00000000-0000-0000-0000-000000000020"
		queuedWorkerID  = "00000000-0000-0000-0000-000000000021"
	)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	seedSchedulerCapacityShares(t, database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   2,
		ProjectWeight:  1,
	})
	seedSchedulerWorkers(t, database.Admin, poolID, profileID,
		schedulerWorker{
			ID:       uuid.MustParse(chargedWorkerID),
			SPIFFEID: "spiffe://vela.internal/worker/cnpg-charged",
			Epoch:    7,
		},
		schedulerWorker{
			ID:       uuid.MustParse(queuedWorkerID),
			SPIFFEID: "spiffe://vela.internal/worker/cnpg-queued",
			Epoch:    11,
		},
	)

	internalPool := newRolePool(
		t, database.DSN, "vela_internal_login", "vela-internal-password",
	)
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create CNPG Assignment coordinator: %v", err)
	}
	schedulerPool := newRolePool(
		t, database.DSN, "vela_scheduler_login", "vela-scheduler-password",
	)
	scheduling, err := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "cnpg-failover-conformance",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 1,
	})
	if err != nil {
		t.Fatalf("create CNPG Scheduler: %v", err)
	}
	capacityPredictor, err := scheduler.NewCapacityPredictor(schedulerPool)
	if err != nil {
		t.Fatalf("create CNPG Admission capacity predictor: %v", err)
	}
	requestPool := newRolePool(
		t, database.DSN, "vela_request_login", "vela-request-password",
	)
	currentAdmission := admission.NewService(requestPool, capacityPredictor)
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	currentAdmissionPrincipal, err := identity.NewAuthenticator(
		authPool,
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate current CNPG Admission principal: %v", err)
	}

	harness.assertReplicationHealth(t, database.Admin, 2)
	t.Logf("CNPG initial placement=%s", strings.Join(harness.postgresPlacement(t), ","))

	server := admissionServerForDatabase(t, database)
	chargedJob := submitCNPGJob(t, server.URL, "cnpg-charged", "retain charged authority")
	reservedJob := submitCNPGJob(t, server.URL, "cnpg-reserved", "retain reserved authority")
	chargedDispatch, ok, err := scheduling.RunOnce(context.Background(), poolID)
	if err != nil || !ok {
		t.Fatalf("create charged Scheduler Assignment = ok %t error=%v", ok, err)
	}
	if chargedDispatch.Assignment.JobID != uuid.MustParse(chargedJob.JobID) {
		t.Fatalf(
			"charged Scheduler Assignment Job = %s, want %s",
			chargedDispatch.Assignment.JobID,
			chargedJob.JobID,
		)
	}
	chargedAssignment := chargedDispatch.Assignment
	started, err := coordinator.Start(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: chargedAssignment.WorkerID},
		leaseCredentials(chargedAssignment),
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("reach Billable Start = %#v error=%v", started, err)
	}
	canceled := cancelJob(
		t,
		server.URL,
		testProjectID,
		chargedJob.JobID,
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("charged cancellation status = %d body=%s", canceled.StatusCode, canceled.Body)
	}
	var cancellation cancelResponse
	if err := json.Unmarshal(canceled.Body, &cancellation); err != nil {
		t.Fatalf("decode charged cancellation: %v", err)
	}
	if cancellation.Decision != "CANCELING" || cancellation.Charge == nil ||
		cancellation.Charge.ChargeID == "" {
		t.Fatalf("charged cancellation = %#v", cancellation)
	}

	committed := readCNPGAuthoritySnapshot(t, database.Admin)
	oldPrimary := harness.currentPrimary(t)
	oldPrimaryNode := harness.nodeForPod(t, oldPrimary)
	harness.assertReplicationHealth(t, database.Admin, 2)

	failoverStarted := time.Now()
	harness.stopKindNode(t, oldPrimaryNode)
	t.Cleanup(func() { harness.startKindNodeBestEffort(oldPrimaryNode) })
	newPrimary := harness.waitForDifferentPrimary(t, oldPrimary, 5*time.Minute)
	harness.restartPortForward(t)
	harness.waitForDatabase(t, database.Admin, 2*time.Minute)
	failoverDuration := time.Since(failoverStarted)
	if failoverDuration > 5*time.Minute {
		t.Fatalf("single-node failover RTO = %s, want <= 5m", failoverDuration)
	}
	if recovered := readCNPGAuthoritySnapshot(t, database.Admin); recovered != committed {
		t.Fatalf("authority changed across failover:\nbefore=%s\nafter=%s", committed, recovered)
	}
	t.Logf(
		"CNPG single-node failover old_primary=%s old_node=%s new_primary=%s rto=%s authority_sha256_input_bytes=%d",
		oldPrimary,
		oldPrimaryNode,
		newPrimary,
		failoverDuration,
		len(committed),
	)

	harness.startKindNode(t, oldPrimaryNode)
	harness.waitForThreeReadyInstances(t, 5*time.Minute)
	harness.assertReplicationHealth(t, database.Admin, 2)
	stable := readCNPGAuthoritySnapshot(t, database.Admin)
	if stable != committed {
		t.Fatalf("authority changed after old primary rejoined:\nbefore=%s\nafter=%s", committed, stable)
	}

	primary := harness.currentPrimary(t)
	primaryNode := harness.nodeForPod(t, primary)
	standbyNodes := harness.standbyNodes(t, primaryNode)
	for _, node := range standbyNodes {
		harness.stopKindNode(t, node)
		node := node
		t.Cleanup(func() { harness.startKindNodeBestEffort(node) })
	}
	harness.waitForNoStreamingStandby(t, database.Admin, 2*time.Minute)

	admissionResult, admissionErr := submitCNPGJobWithTimeout(
		server.URL,
		"cnpg-no-quorum-admission",
		"no quorum must not accept this Job",
		4*time.Second,
	)
	if admissionErr == nil && admissionResult.StatusCode == http.StatusAccepted {
		t.Fatalf("no-quorum Admission returned 202: %s", admissionResult.Body)
	}
	currentAdmissionContext, cancelCurrentAdmission := context.WithTimeout(
		context.Background(),
		4*time.Second,
	)
	defer cancelCurrentAdmission()
	currentAdmissionJob, currentAdmissionErr := currentAdmission.Submit(
		currentAdmissionContext,
		currentAdmissionPrincipal,
		uuid.MustParse(testProjectID),
		"cnpg-no-quorum-current-admission-structured",
		admission.Request{
			Model:            "minimax-h3",
			GenerationPreset: "balanced",
			ServiceClass:     "standard",
			OutputSpec:       "video-1080p-5s-24fps",
			GenerationCount:  1,
			Prompt:           "prove current Admission returns structured no-quorum failure",
		},
	)
	var currentAdmissionPostgresError *pgconn.PgError
	if currentAdmissionErr == nil || currentAdmissionJob.ID != uuid.Nil ||
		!errors.As(currentAdmissionErr, &currentAdmissionPostgresError) ||
		currentAdmissionPostgresError.Code != "55000" {
		t.Fatalf(
			"no-quorum current Admission Submit = job %#v error=%v, want SQLSTATE 55000 and no Job",
			currentAdmissionJob,
			currentAdmissionErr,
		)
	}

	acquireContext, cancelAcquire := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancelAcquire()
	dispatch, ok, schedulerErr := scheduling.RunOnce(acquireContext, poolID)
	var schedulerPostgresError *pgconn.PgError
	if schedulerErr == nil || ok || dispatch.Assignment.AttemptID != uuid.Nil ||
		!errors.As(schedulerErr, &schedulerPostgresError) || schedulerPostgresError.Code != "55000" {
		t.Fatalf(
			"no-quorum Scheduler RunOnce = %#v ok=%t error=%v, want SQLSTATE 55000 and no Assignment",
			dispatch,
			ok,
			schedulerErr,
		)
	}
	nMinusOneAdmission := runNMinusOneAdmissionFailureProbe(
		t,
		nMinusOne.AdmissionProbe,
		database.DSN,
		"cnpg-no-quorum-n-minus-one-admission",
	)
	if nMinusOneAdmission.SQLState != "55000" || nMinusOneAdmission.JobID != "" {
		t.Fatalf(
			"no-quorum N-1 Admission = %#v, want SQLSTATE 55000 and no Job",
			nMinusOneAdmission,
		)
	}
	nMinusOneScheduler := runNMinusOneSchedulerProbe(
		t,
		nMinusOne.SchedulerProbe,
		database.DSN,
		poolID,
		"cnpg-no-quorum-n-minus-one-scheduler",
	)
	if nMinusOneScheduler.SQLState != "55000" || nMinusOneScheduler.Dispatched ||
		nMinusOneScheduler.IntentID != uuid.Nil || nMinusOneScheduler.AttemptID != uuid.Nil ||
		nMinusOneScheduler.JobID != uuid.Nil || nMinusOneScheduler.WorkerID != uuid.Nil ||
		nMinusOneScheduler.LeaseFence != 0 || nMinusOneScheduler.LeaseToken != "" {
		t.Fatalf(
			"no-quorum N-1 Scheduler = %#v, want SQLSTATE 55000 and no authority",
			nMinusOneScheduler,
		)
	}

	harness.startKindNode(t, standbyNodes[0])
	harness.waitForThreeOrTwoReadyInstances(t, 5*time.Minute)
	harness.assertReplicationHealth(t, database.Admin, 1)
	harness.waitForDatabase(t, database.Admin, time.Minute)
	if recovered := readCNPGAuthoritySnapshot(t, database.Admin); recovered != stable {
		t.Fatalf(
			"no-quorum operations changed authority after recovery:\nbefore=%s\nafter=%s",
			stable,
			recovered,
		)
	}
	harness.startKindNode(t, standbyNodes[1])
	harness.waitForThreeReadyInstances(t, 5*time.Minute)
	harness.assertReplicationHealth(t, database.Admin, 2)
	finalPlacement := harness.postgresPlacement(t)
	finalCounts := readCNPGAuthorityCounts(t, database.Admin)

	t.Logf(
		"CNPG no-quorum primary=%s primary_node=%s stopped_standbys=%s admission_status=%d admission_error=%v current_admission_sqlstate=%s current_admission_error=%v scheduler_sqlstate=%s scheduler_error=%v n_minus_one=%s n_minus_one_admission_sqlstate=%s n_minus_one_scheduler_sqlstate=%s final_placement=%s authority_counts=%s reserved_job=%s",
		primary,
		primaryNode,
		strings.Join(standbyNodes, ","),
		admissionResult.StatusCode,
		admissionErr,
		currentAdmissionPostgresError.Code,
		currentAdmissionErr,
		schedulerPostgresError.Code,
		schedulerErr,
		adjacentRolloutNMinusOneCommit,
		nMinusOneAdmission.SQLState,
		nMinusOneScheduler.SQLState,
		strings.Join(finalPlacement, ","),
		finalCounts,
		reservedJob.JobID,
	)
}

func submitCNPGJob(t *testing.T, serverURL, key, prompt string) jobResponse {
	t.Helper()
	result := submitJob(t, serverURL, key, cnpgAdmissionBody(prompt))
	if result.StatusCode != http.StatusAccepted {
		t.Fatalf("CNPG Admission status = %d body=%s", result.StatusCode, result.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(result.Body, &job); err != nil {
		t.Fatalf("decode CNPG Accepted Job: %v", err)
	}
	return job
}

func submitCNPGJobWithTimeout(
	serverURL, key, prompt string,
	timeout time.Duration,
) (httpResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		serverURL+"/v1/projects/"+testProjectID+"/jobs",
		bytes.NewReader(cnpgAdmissionBody(prompt)),
	)
	if err != nil {
		return httpResult{}, fmt.Errorf("create bounded Admission request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+testBearerCredential())
	request.Header.Set("Idempotency-Key", key)
	response, err := (&http.Client{Timeout: timeout}).Do(request)
	if err != nil {
		return httpResult{}, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return httpResult{}, err
	}
	return httpResult{StatusCode: response.StatusCode, Header: response.Header.Clone(), Body: body}, nil
}

func cnpgAdmissionBody(prompt string) []byte {
	body, err := json.Marshal(map[string]any{
		"model":             "minimax-h3",
		"generation_preset": "balanced",
		"service_class":     "standard",
		"output_spec":       "video-1080p-5s-24fps",
		"generation_count":  1,
		"prompt":            prompt,
	})
	if err != nil {
		panic(err)
	}
	return body
}

func readCNPGAuthoritySnapshot(t *testing.T, database *sql.DB) string {
	t.Helper()
	var snapshot string
	if err := database.QueryRow(`
		SELECT jsonb_build_object(
			'jobs', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row.id), '[]') FROM jobs AS row),
			'retry_runtime_states', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row.job_id), '[]') FROM retry_runtime_states AS row),
			'credit_reservations', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row.job_id), '[]') FROM credit_reservations AS row),
			'outbox_events', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row.event_id), '[]') FROM outbox_events AS row),
			'scheduler_dispatch_intents', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row.id), '[]') FROM scheduler_dispatch_intents AS row),
			'attempts', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row.id), '[]') FROM attempts AS row),
			'attempt_leases', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row.attempt_id), '[]') FROM attempt_leases AS row),
			'cancellation_decisions', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row.id), '[]') FROM job_cancellation_decisions AS row),
			'charges', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row.id), '[]') FROM charges AS row),
			'idempotency_results', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row.idempotency_key), '[]') FROM idempotency_results AS row),
			'organization_credit_accounts', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row.organization_id), '[]') FROM organization_credit_accounts AS row),
			'projects', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row.id), '[]') FROM projects AS row),
			'worker_pools', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row.id), '[]') FROM worker_pools AS row),
			'workers', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY row.id), '[]') FROM workers AS row)
		)::text
	`).Scan(&snapshot); err != nil {
		t.Fatalf("read CNPG authority snapshot: %v", err)
	}
	return snapshot
}

func readCNPGAuthorityCounts(t *testing.T, database *sql.DB) string {
	t.Helper()
	var counts string
	if err := database.QueryRow(`
		SELECT jsonb_build_object(
			'jobs', (SELECT count(*) FROM jobs),
			'retry_runtime_states', (SELECT count(*) FROM retry_runtime_states),
			'credit_reservations', (SELECT count(*) FROM credit_reservations),
			'outbox_events', (SELECT count(*) FROM outbox_events),
			'scheduler_dispatch_intents', (SELECT count(*) FROM scheduler_dispatch_intents),
			'attempts', (SELECT count(*) FROM attempts),
			'attempt_leases', (SELECT count(*) FROM attempt_leases),
			'cancellation_decisions', (SELECT count(*) FROM job_cancellation_decisions),
			'charges', (SELECT count(*) FROM charges)
		)::text
	`).Scan(&counts); err != nil {
		t.Fatalf("read CNPG authority counts: %v", err)
	}
	return counts
}

type cnpgHarness struct {
	kubeconfig  string
	kindCluster string
	localPort   int
	portForward *exec.Cmd
	forwardLog  bytes.Buffer
}

func newCNPGHarness(t *testing.T) *cnpgHarness {
	t.Helper()
	kubeconfig := os.Getenv("VELA_CNPG_KUBECONFIG")
	kindCluster := os.Getenv("VELA_CNPG_KIND_CLUSTER")
	if kubeconfig == "" || kindCluster == "" {
		t.Fatal("VELA_CNPG_KUBECONFIG and VELA_CNPG_KIND_CLUSTER are required; run make test-cnpg-failover")
	}
	if kindCluster != "vela-cnpg-failover" {
		t.Fatalf("unexpected CNPG conformance kind cluster %q", kindCluster)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve CNPG port-forward port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release CNPG port-forward port: %v", err)
	}
	harness := &cnpgHarness{
		kubeconfig:  kubeconfig,
		kindCluster: kindCluster,
		localPort:   port,
	}
	t.Cleanup(func() { harness.stopPortForward() })
	return harness
}

func (harness *cnpgHarness) openDatabase(t *testing.T) testDatabase {
	t.Helper()
	encodedPassword := harness.kubectl(
		t,
		"-n", cnpgNamespace,
		"get", "secret", cnpgClusterName+"-superuser",
		"-o", "jsonpath={.data.password}",
	)
	password, err := base64.StdEncoding.DecodeString(encodedPassword)
	if err != nil {
		t.Fatalf("decode CNPG superuser password: %v", err)
	}
	dsn := (&url.URL{
		Scheme: "postgres",
		User:   url.UserPassword("postgres", string(password)),
		Host:   net.JoinHostPort("127.0.0.1", strconv.Itoa(harness.localPort)),
		Path:   "/postgres",
		RawQuery: url.Values{
			"sslmode": []string{"disable"},
		}.Encode(),
	}).String()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open CNPG database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	harness.waitForDatabase(t, database, time.Minute)
	return testDatabase{Admin: database, DSN: dsn}
}

func (harness *cnpgHarness) startPortForward(t *testing.T) {
	t.Helper()
	harness.forwardLog.Reset()
	harness.portForward = exec.Command(
		"kubectl",
		"--kubeconfig", harness.kubeconfig,
		"-n", cnpgNamespace,
		"port-forward", "service/"+cnpgServiceName,
		fmt.Sprintf("%d:5432", harness.localPort),
		"--address=127.0.0.1",
	)
	harness.portForward.Stdout = &harness.forwardLog
	harness.portForward.Stderr = &harness.forwardLog
	if err := harness.portForward.Start(); err != nil {
		t.Fatalf("start CNPG port forward: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		connection, err := net.DialTimeout(
			"tcp",
			net.JoinHostPort("127.0.0.1", strconv.Itoa(harness.localPort)),
			200*time.Millisecond,
		)
		if err == nil {
			_ = connection.Close()
			return
		}
		if harness.portForward.ProcessState != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	harness.stopPortForward()
	t.Fatalf("CNPG port forward did not become ready: %s", harness.forwardLog.String())
}

func (harness *cnpgHarness) restartPortForward(t *testing.T) {
	t.Helper()
	harness.stopPortForward()
	harness.startPortForward(t)
}

func (harness *cnpgHarness) stopPortForward() {
	if harness.portForward == nil || harness.portForward.Process == nil {
		return
	}
	_ = harness.portForward.Process.Kill()
	_ = harness.portForward.Wait()
	harness.portForward = nil
}

func (harness *cnpgHarness) waitForDatabase(
	t *testing.T,
	database *sql.DB,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		lastErr = database.PingContext(ctx)
		cancel()
		if lastErr == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("CNPG database did not become reachable: %v", lastErr)
}

func (harness *cnpgHarness) currentPrimary(t *testing.T) string {
	t.Helper()
	primary := harness.kubectl(
		t,
		"-n", cnpgNamespace,
		"get", "cluster", cnpgClusterName,
		"-o", "jsonpath={.status.currentPrimary}",
	)
	if primary == "" {
		t.Fatal("CloudNativePG status has no current primary")
	}
	return primary
}

func (harness *cnpgHarness) waitForDifferentPrimary(
	t *testing.T,
	oldPrimary string,
	timeout time.Duration,
) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastOutput string
	for time.Now().Before(deadline) {
		output, err := harness.kubectlResult(
			"-n", cnpgNamespace,
			"get", "cluster", cnpgClusterName,
			"-o", "jsonpath={.status.currentPrimary}",
		)
		lastOutput = output
		if err == nil && output != "" && output != oldPrimary {
			return output
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("CloudNativePG did not replace primary %s; last status=%q", oldPrimary, lastOutput)
	return ""
}

func (harness *cnpgHarness) nodeForPod(t *testing.T, pod string) string {
	t.Helper()
	node := harness.kubectl(
		t,
		"-n", cnpgNamespace,
		"get", "pod", pod,
		"-o", "jsonpath={.spec.nodeName}",
	)
	harness.requireOwnedKindNode(t, node)
	return node
}

func (harness *cnpgHarness) standbyNodes(t *testing.T, primaryNode string) []string {
	t.Helper()
	pods := harness.postgresPods(t)
	nodes := make([]string, 0, 2)
	for _, pod := range pods.Items {
		if pod.Spec.NodeName != primaryNode {
			harness.requireOwnedKindNode(t, pod.Spec.NodeName)
			nodes = append(nodes, pod.Spec.NodeName)
		}
	}
	if len(nodes) != 2 || nodes[0] == nodes[1] {
		t.Fatalf("CloudNativePG standby nodes = %v, want two distinct nodes", nodes)
	}
	return nodes
}

func (harness *cnpgHarness) postgresPods(t *testing.T) corev1.PodList {
	t.Helper()
	output := harness.kubectl(
		t,
		"-n", cnpgNamespace,
		"get", "pods",
		"-l", "cnpg.io/cluster="+cnpgClusterName,
		"-o", "json",
	)
	var pods corev1.PodList
	if err := json.Unmarshal([]byte(output), &pods); err != nil {
		t.Fatalf("decode CloudNativePG Pods: %v", err)
	}
	return pods
}

func (harness *cnpgHarness) postgresPlacement(t *testing.T) []string {
	t.Helper()
	pods := harness.postgresPods(t)
	placement := make([]string, 0, len(pods.Items))
	for _, pod := range pods.Items {
		placement = append(placement, pod.Name+"@"+pod.Spec.NodeName)
	}
	sort.Strings(placement)
	return placement
}

func (harness *cnpgHarness) waitForThreeReadyInstances(t *testing.T, timeout time.Duration) {
	t.Helper()
	harness.waitForReadyInstances(t, 3, timeout)
	pods := harness.postgresPods(t)
	nodes := make(map[string]bool, len(pods.Items))
	for _, pod := range pods.Items {
		if podReady(pod) {
			nodes[pod.Spec.NodeName] = true
		}
	}
	if len(nodes) != 3 {
		t.Fatalf("ready CloudNativePG instances occupy %d nodes, want 3: %#v", len(nodes), nodes)
	}
}

func (harness *cnpgHarness) waitForThreeOrTwoReadyInstances(
	t *testing.T,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pods := harness.postgresPods(t)
		ready := 0
		for _, pod := range pods.Items {
			if podReady(pod) {
				ready++
			}
		}
		if ready >= 2 {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatal("CloudNativePG did not restore two ready instances")
}

func (harness *cnpgHarness) waitForReadyInstances(
	t *testing.T,
	want int,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pods := harness.postgresPods(t)
		ready := 0
		for _, pod := range pods.Items {
			if podReady(pod) {
				ready++
			}
		}
		if ready == want {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("CloudNativePG did not reach %d ready instances", want)
}

func podReady(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (harness *cnpgHarness) assertReplicationHealth(
	t *testing.T,
	database *sql.DB,
	wantStreaming int,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	var (
		primary     bool
		streaming   int
		synchronous int
		current     int
		lastErr     error
	)
	for time.Now().Before(deadline) {
		lastErr = database.QueryRow(`
			SELECT
				NOT pg_is_in_recovery(),
				count(*) FILTER (WHERE state = 'streaming'),
				count(*) FILTER (
					WHERE state = 'streaming' AND sync_state IN ('sync', 'quorum')
				),
				count(*) FILTER (
					WHERE state = 'streaming' AND replay_lsn >= pg_current_wal_lsn()
				)
			FROM pg_stat_replication
		`).Scan(&primary, &streaming, &synchronous, &current)
		if lastErr == nil && primary && streaming == wantStreaming && synchronous >= 1 &&
			current == wantStreaming {
			t.Logf(
				"CNPG replication health streaming=%d synchronous=%d current=%d",
				streaming,
				synchronous,
				current,
			)
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf(
		"CloudNativePG replication health = primary %t streaming %d synchronous %d current %d error=%v; want streaming/current %d and at least one synchronous standby",
		primary,
		streaming,
		synchronous,
		current,
		lastErr,
		wantStreaming,
	)
}

func (harness *cnpgHarness) waitForNoStreamingStandby(
	t *testing.T,
	database *sql.DB,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var count int
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := database.QueryRowContext(ctx, `
			SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'
		`).Scan(&count)
		cancel()
		if err == nil && count == 0 {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("CloudNativePG still reports %d streaming standbys", count)
}

func (harness *cnpgHarness) stopKindNode(t *testing.T, node string) {
	t.Helper()
	harness.requireOwnedKindNode(t, node)
	harness.docker(t, "stop", "--time", "1", node)
}

func (harness *cnpgHarness) startKindNode(t *testing.T, node string) {
	t.Helper()
	harness.requireOwnedKindNode(t, node)
	harness.docker(t, "start", node)
}

func (harness *cnpgHarness) startKindNodeBestEffort(node string) {
	if !strings.HasPrefix(node, harness.kindCluster+"-") {
		return
	}
	command := exec.Command("docker", "start", node)
	_ = command.Run()
}

func (harness *cnpgHarness) requireOwnedKindNode(t *testing.T, node string) {
	t.Helper()
	if !strings.HasPrefix(node, harness.kindCluster+"-") {
		t.Fatalf("refusing Docker action on non-test node %q", node)
	}
	label, err := commandOutput(
		"docker", "inspect", "--format",
		`{{index .Config.Labels "io.x-k8s.kind.cluster"}}`,
		node,
	)
	if err != nil || label != harness.kindCluster {
		t.Fatalf("Docker node %q ownership = %q error=%v", node, label, err)
	}
}

func (harness *cnpgHarness) docker(t *testing.T, arguments ...string) string {
	t.Helper()
	output, err := commandOutput("docker", arguments...)
	if err != nil {
		t.Fatalf("docker %s: %v output=%s", strings.Join(arguments, " "), err, output)
	}
	return output
}

func (harness *cnpgHarness) kubectl(t *testing.T, arguments ...string) string {
	t.Helper()
	output, err := harness.kubectlResult(arguments...)
	if err != nil {
		t.Fatalf("kubectl %s: %v output=%s", strings.Join(arguments, " "), err, output)
	}
	return output
}

func (harness *cnpgHarness) kubectlResult(arguments ...string) (string, error) {
	arguments = append([]string{"--kubeconfig", harness.kubeconfig}, arguments...)
	return commandOutput("kubectl", arguments...)
}

func commandOutput(name string, arguments ...string) (string, error) {
	command := exec.Command(name, arguments...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		return strings.TrimSpace(string(output) + "\n" + stderr.String()), err
	}
	return strings.TrimSpace(string(output)), nil
}

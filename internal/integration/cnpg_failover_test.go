//go:build integration && cnpg

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/stageworkercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
)

const (
	cnpgNamespace   = "vela-system"
	cnpgClusterName = "vela-postgres"
	cnpgServiceName = "vela-postgres-rw"
)

func TestCloudNativePGSingleNodeFailoverPreservesAuthorityAndNoQuorumFailsClosed(t *testing.T) {
	harness := newCNPGHarness(t)
	harness.waitForThreeReadyInstances(t, 5*time.Minute)
	harness.startPortForward(t)
	database := harness.openDatabase(t)
	harness.assertReplicationHealth(t, database.Admin, 2)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedEncoderAssignmentProfile(t, database)
	seedDiTAssignmentProfile(t, database)
	seedVAEIntegrationProfile(t, database)
	activateH3StageGraph(t, database)
	seedWorkerRegistryPlan(t, database.Admin)
	if _, err := database.Admin.Exec(`UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel'] WHERE id = $1`, testCredentialID); err != nil {
		t.Fatal(err)
	}
	coordinator, err := attemptcoordinator.NewService(newRolePool(t, database.DSN,
		"vela_attempt_coordinator_login", "vela-attempt-coordinator-password"))
	if err != nil {
		t.Fatal(err)
	}
	server := admissionServerForDatabase(t, database)
	chargedJob, chargedAttempt := instantiateH3IntegrationGraph(t, database, server.URL, "cnpg-charged")
	reservedJob, reservedAttempt := instantiateH3IntegrationGraph(t, database, server.URL, "cnpg-reserved")
	encoderRun := func(attempt uuid.UUID) uuid.UUID {
		t.Helper()
		var run uuid.UUID
		if err := database.Admin.QueryRow(`SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'`,
			attempt).Scan(&run); err != nil {
			t.Fatal(err)
		}
		return run
	}
	stage := h3IntegrationStages([]string{"cnpg-charged-worker", "unused-dit", "unused-vae"}, nil)[0]
	chargedFixture := newH3AssignmentWorkerFixture(t, database, coordinator, encoderRun(chargedAttempt), stage, 0xc1)
	chargedBackend := newPostgresAssignmentTestBackend(t, chargedFixture)
	chargedCommand := stageWorkerAcquireCommand(chargedFixture)
	chargedRequest := stageWorkerAcquireRequest(chargedFixture)
	charged, err := chargedBackend.AcquireStage(context.Background(), chargedCommand, chargedRequest)
	if err != nil || charged.Assignment == nil || charged.Assignment.GetAuthority().GetJobId() != chargedJob.JobID {
		t.Fatalf("CNPG current Stage assignment=%#v error=%v", charged, err)
	}
	startCNPGStage(t, database, chargedCommand, charged.Assignment)
	canceled := cancelJob(t, server.URL, testProjectID, chargedJob.JobID, testBearerCredential())
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("CNPG billable Stage cancellation status=%d body=%s", canceled.StatusCode, canceled.Body)
	}
	var chargeCount, reservedCount int
	if err := database.Admin.QueryRow(`SELECT
		(SELECT count(*) FROM charges WHERE job_id = $1),
		(SELECT count(*) FROM credit_reservations WHERE job_id = $2 AND state = 'RESERVED')`,
		chargedJob.JobID, reservedJob.JobID).Scan(&chargeCount, &reservedCount); err != nil || chargeCount != 1 || reservedCount != 1 {
		t.Fatalf("CNPG charge/reserved=%d/%d error=%v", chargeCount, reservedCount, err)
	}
	harness.assertReplicationHealth(t, database.Admin, 2)
	t.Logf("CNPG initial placement=%s", strings.Join(harness.postgresPlacement(t), ","))
	committed := readCNPGAuthoritySnapshot(t, database.Admin)
	oldPrimary := harness.currentPrimary(t)
	oldPrimaryNode := harness.nodeForPod(t, oldPrimary)
	started := time.Now()
	harness.stopKindNode(t, oldPrimaryNode)
	t.Cleanup(func() { harness.startKindNodeBestEffort(oldPrimaryNode) })
	newPrimary := harness.waitForDifferentPrimary(t, oldPrimary, 5*time.Minute)
	harness.restartPortForward(t)
	harness.waitForDatabase(t, database.Admin, 2*time.Minute)
	elapsed := time.Since(started)
	if elapsed > 5*time.Minute {
		t.Fatalf("CNPG automatic failover took %s, want <=5m", elapsed)
	}
	assertCNPGAuthoritySnapshot(t, database.Admin, committed, "automatic failover")
	replay, err := chargedBackend.AcquireStage(context.Background(), chargedCommand, chargedRequest)
	if err != nil || !proto.Equal(replay.Assignment, charged.Assignment) {
		t.Fatalf("CNPG durable StageAssignment replay changed: result=%#v error=%v", replay, err)
	}
	t.Logf("CNPG failover old_primary=%s old_node=%s new_primary=%s elapsed=%s authority_sha256=%x",
		oldPrimary, oldPrimaryNode, newPrimary, elapsed, sha256.Sum256([]byte(committed)))
	harness.startKindNode(t, oldPrimaryNode)
	harness.waitForThreeReadyInstances(t, 5*time.Minute)
	harness.assertReplicationHealth(t, database.Admin, 2)
	assertCNPGAuthoritySnapshot(t, database.Admin, committed, "old primary rejoin")

	// Establish fresh current Worker authority before removing the replicas, so
	// a stale Fleet observation cannot mask the missing-quorum assignment path.
	stage.nodeIdentity = "cnpg-reserved-worker"
	reservedFixture := newH3AssignmentWorkerFixture(t, database, coordinator, encoderRun(reservedAttempt), stage, 0xc2)
	reservedBackend := newPostgresAssignmentTestBackend(t, reservedFixture)
	stable := readCNPGAuthoritySnapshot(t, database.Admin)
	primary := harness.currentPrimary(t)
	primaryNode := harness.nodeForPod(t, primary)
	standbys := harness.standbyNodes(t, primaryNode)
	for _, node := range standbys {
		harness.stopKindNode(t, node)
		t.Cleanup(func() { harness.startKindNodeBestEffort(node) })
	}
	harness.waitForNoStreamingStandby(t, database.Admin, 2*time.Minute)
	admissionStarted := time.Now()
	admissionResult, admissionErr := submitCNPGJobWithTimeout(server.URL, "cnpg-no-quorum", 4*time.Second)
	admissionElapsed := time.Since(admissionStarted)
	if admissionErr == nil && admissionResult.StatusCode == http.StatusAccepted {
		t.Fatalf("no-quorum Admission returned Accepted: %s", admissionResult.Body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	acquireStarted := time.Now()
	blocked, assignmentErr := reservedBackend.AcquireStage(ctx,
		stageWorkerAcquireCommand(reservedFixture), stageWorkerAcquireRequest(reservedFixture))
	acquireElapsed := time.Since(acquireStarted)
	var postgresError *pgconn.PgError
	if assignmentErr == nil || blocked.Assignment != nil ||
		!errors.As(assignmentErr, &postgresError) || postgresError.Code != "55000" {
		t.Fatalf("no-quorum Stage acquire=%#v error=%v, want SQLSTATE 55000 and no assignment", blocked, assignmentErr)
	}
	harness.startKindNode(t, standbys[0])
	harness.waitForThreeOrTwoReadyInstances(t, 5*time.Minute)
	harness.assertReplicationHealth(t, database.Admin, 1)
	assertCNPGAuthoritySnapshot(t, database.Admin, stable, "no-quorum rejected operations")
	harness.startKindNode(t, standbys[1])
	harness.waitForThreeReadyInstances(t, 5*time.Minute)
	harness.assertReplicationHealth(t, database.Admin, 2)
	t.Logf("CNPG no-quorum admission_status=%d admission_elapsed=%s admission_error=%v stage_sqlstate=%s stage_elapsed=%s stage_error=%v final_placement=%s authority_sha256=%x",
		admissionResult.StatusCode, admissionElapsed, admissionErr, postgresError.Code, acquireElapsed, assignmentErr,
		strings.Join(harness.postgresPlacement(t), ","), sha256.Sum256([]byte(stable)))
}

func startCNPGStage(t *testing.T, database testDatabase, command stageworkercontrol.CommandContext,
	assignment *velav1.StageAssignment) {
	t.Helper()
	started, err := startIntegrationAssignedStage(t, database, command, assignment)
	if err != nil || started.RenewedAuthority == nil {
		t.Fatalf("CNPG current Stage Billable Start=%#v error=%v", started, err)
	}
}

func submitCNPGJobWithTimeout(serverURL, key string, timeout time.Duration) (httpResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		serverURL+"/v1/projects/"+testProjectID+"/jobs", strings.NewReader(`{
		"model":"minimax-h3","generation_preset":"balanced","service_class":"standard",
		"output_spec":"video-1080p-5s-24fps","generation_count":1,"prompt":"no quorum"}`))
	if err != nil {
		return httpResult{}, err
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
	return httpResult{StatusCode: response.StatusCode, Header: response.Header.Clone(), Body: body}, err
}

func readCNPGAuthoritySnapshot(t *testing.T, database *sql.DB) string {
	t.Helper()
	tables := []string{"jobs", "credit_reservations", "outbox_events", "attempts", "stage_runs",
		"stage_attempts", "stage_leases", "stage_allocations", "stage_worker_commands", "stage_authority_renewals",
		"stage_worker_acquire_intents", "stage_worker_acquire_results", "stage_scheduler_claims",
		"stage_scheduler_snapshot_traces", "stage_decision_evidence", "job_cancellation_decisions", "charges", "idempotency_results",
		"organization_credit_accounts", "projects", "stage_retry_budgets", "attempt_retry_budgets"}
	var pieces []string
	for _, table := range tables {
		pieces = append(pieces, fmt.Sprintf("'%s', (SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY to_jsonb(row)::text), '[]') FROM %s AS row)", table, table))
	}
	var snapshot string
	if err := database.QueryRow("SELECT jsonb_build_object(" + strings.Join(pieces, ",") + ")::text").Scan(&snapshot); err != nil {
		t.Fatalf("read current Stage CNPG authority: %v", err)
	}
	return snapshot
}

func assertCNPGAuthoritySnapshot(t *testing.T, database *sql.DB, before, phase string) {
	t.Helper()
	if after := readCNPGAuthoritySnapshot(t, database); before != after {
		t.Fatalf("CNPG %s changed durable authority: before_sha256=%x after_sha256=%x", phase,
			sha256.Sum256([]byte(before)), sha256.Sum256([]byte(after)))
	}
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

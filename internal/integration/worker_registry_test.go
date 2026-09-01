//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleettransport"
	"github.com/vivym/vela/internal/h3launchevidence"
	"github.com/vivym/vela/internal/nodeagent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

const (
	workerRegistryProfileID = "49000000-0000-0000-0000-000000000022"
	workerRegistryPoolID    = "49200000-0000-0000-0000-000000000001"
	workerRegistryBundleID  = "49200000-0000-0000-0000-000000000002"
	workerRegistryNodeID    = "49200000-0000-0000-0000-000000000003"
	workerRegistryDeviceID  = "49200000-0000-0000-0000-000000000004"
	multiWorkerProfileID    = "49200000-0000-0000-0000-000000000100"
	multiStageProfileID     = "49200000-0000-0000-0000-000000000101"
	multiCapacityPoolID     = "49200000-0000-0000-0000-000000000102"
)

func TestWorkerRegistryRejectsSharedGPUAcrossWorkerInstances(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedWorkerRegistryPlan(t, database.Admin)
	fleetPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")

	firstWorkerID := uuid.MustParse("49200000-0000-0000-0000-000000000010")
	secondWorkerID := uuid.MustParse("49200000-0000-0000-0000-000000000011")
	for _, workerID := range []uuid.UUID{firstWorkerID, secondWorkerID} {
		if _, err := database.Admin.Exec(`
			INSERT INTO worker_instances (
				id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
				lifecycle_state, reachability_state, instance_epoch,
				control_session_epoch, desired_member_count, desired_device_count
			) VALUES ($1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 1, 1)
		`, workerID, workerRegistryProfileID, workerRegistryPoolID, workerRegistryBundleID); err != nil {
			t.Fatalf("seed WorkerInstance %s: %v", workerID, err)
		}
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	for _, candidate := range []struct {
		workerID     uuid.UUID
		identityByte byte
	}{
		{workerID: firstWorkerID, identityByte: 0x10},
		{workerID: secondWorkerID, identityByte: 0x11},
	} {
		candidate := candidate
		go func() {
			<-start
			_, err := fleetPool.Exec(
				context.Background(),
				"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
				workerRegistryEvidence(candidate.workerID, candidate.identityByte),
			)
			results <- err
		}()
	}
	close(start)
	var succeeded, rejected int
	for range 2 {
		err := <-results
		if err == nil {
			succeeded++
			continue
		}
		if postgresConstraint(err, "device_already_bound_to_worker_instance") {
			rejected++
			continue
		}
		t.Fatalf("concurrent WorkerInstance binding error = %v", err)
	}
	if succeeded != 1 || rejected != 1 {
		t.Fatalf("concurrent WorkerInstance binding succeeded=%d rejected=%d, want 1/1", succeeded, rejected)
	}
}

func TestWorkerRegistryRefreshesExistingWorkerInstanceEvidence(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedWorkerRegistryPlan(t, database.Admin)
	fleetPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	service, err := fleet.NewService(fleetPool)
	if err != nil {
		t.Fatalf("construct Worker Registry service: %v", err)
	}
	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000012")
	seedWorkerInstance(t, database.Admin, workerID, workerRegistryProfileID, 1, 1)
	evidence := workerRegistryEvidenceValue(t, workerID, 0x12)
	if _, err := service.Observe(context.Background(), evidence); err != nil {
		t.Fatalf("observe initial WorkerInstance evidence: %v", err)
	}
	refreshedAt := evidence.ObservedAt.Add(time.Second)
	evidence.ObservedAt = refreshedAt
	evidence.Capacity.Sequence++
	evidence.Capacity.ObservedAt = refreshedAt
	evidence.Capacity.ExpiresAt = refreshedAt.Add(time.Minute)
	if _, err := service.Observe(context.Background(), evidence); err != nil {
		t.Fatalf("refresh existing WorkerInstance evidence: %v", err)
	}

	var epochs, members, memberDevices, residencies, observations int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM worker_instance_epochs WHERE worker_instance_id = $1),
			(SELECT count(*) FROM worker_members WHERE worker_instance_id = $1),
			(SELECT count(*) FROM worker_member_devices WHERE worker_instance_id = $1),
			(SELECT count(*) FROM model_residencies WHERE worker_instance_id = $1),
			(SELECT count(*) FROM capacity_observations WHERE worker_instance_id = $1)
	`, workerID).Scan(&epochs, &members, &memberDevices, &residencies, &observations); err != nil {
		t.Fatalf("read refreshed WorkerInstance evidence: %v", err)
	}
	if epochs != 1 || members != 1 || memberDevices != 1 || residencies != 1 || observations != 2 {
		t.Fatalf("refreshed evidence rows=%d epochs/%d members/%d member devices/%d residencies/%d observations", epochs, members, memberDevices, residencies, observations)
	}

	conflicting := evidence
	conflicting.Residencies = append([]fleet.ModelResidencyEvidence(nil), evidence.Residencies...)
	conflicting.Capacity.Sequence++
	conflicting.Residencies[0].RuntimeIdentity += "-changed"
	_, err = fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		mustJSON(t, conflicting),
	)
	assertPostgresConstraint(t, err, "model_residency_identity_conflict")

	_, err = fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		mustJSON(t, evidence),
	)
	assertPostgresConstraint(t, err, "capacity_observation_sequence_stale")

	notReady := evidence
	notReady.Residencies = append([]fleet.ModelResidencyEvidence(nil), evidence.Residencies...)
	notReady.Capacity.Sequence++
	notReady.Residencies[0].State = "DRAINING"
	_, err = fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		mustJSON(t, notReady),
	)
	assertPostgresConstraint(t, err, "model_residency_observation_not_ready")

	var runtimeIdentity string
	if err := database.Admin.QueryRow(`
		SELECT residency.runtime_identity,
		       (SELECT count(*) FROM capacity_observations WHERE worker_instance_id = $1)
		FROM model_residencies AS residency
		WHERE residency.worker_instance_id = $1
	`, workerID).Scan(&runtimeIdentity, &observations); err != nil {
		t.Fatalf("read fail-closed refreshed WorkerInstance evidence: %v", err)
	}
	if runtimeIdentity != evidence.Residencies[0].RuntimeIdentity || observations != 2 {
		t.Fatalf("failed refresh changed runtime=%q observations=%d", runtimeIdentity, observations)
	}
}

func TestH3LaunchEvidenceCapturesAuthoritativeRegistrySnapshot(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	fleetPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	service, err := fleet.NewService(fleetPool)
	if err != nil {
		t.Fatalf("construct Worker Registry service: %v", err)
	}
	proposalID := uuid.MustParse("49200000-0000-0000-0000-000000000260")
	if _, err := service.Propose(context.Background(), fleet.ResidencyPlanInputs{
		ProposalID: proposalID, InputDigest: mustDigestBytes(t, digestHex(0xa0)),
		ConfidencePPM: 900000, ExpiresAt: time.Now().UTC().Add(time.Hour),
		MinCapacity: map[string]int64{"dit": 1}, DesiredCapacity: map[string]int64{"dit": 1},
		MaxCapacity: map[string]int64{"dit": 1}, Cooldown: time.Hour,
		BudgetMicroUnits: 1000000, ReasonCodes: []string{"LAUNCH_EVIDENCE"},
		ProposedBy: "capacity-simulator/launch-evidence",
	}); err != nil {
		t.Fatalf("record launch evidence ResidencyProposal: %v", err)
	}
	encodedPlan, err := json.Marshal(approvedResidencyPlanFixture(proposalID))
	if err != nil {
		t.Fatalf("encode launch evidence ResidencyPlan: %v", err)
	}
	var plan fleet.ApprovedResidencyPlan
	if err := json.Unmarshal(encodedPlan, &plan); err != nil {
		t.Fatalf("decode launch evidence ResidencyPlan: %v", err)
	}
	if _, err := service.Apply(context.Background(), plan); err != nil {
		t.Fatalf("apply launch evidence ResidencyPlan: %v", err)
	}
	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000204")
	evidence := workerRegistryEvidenceValue(t, workerID, 0xa1)
	if _, err := service.Observe(context.Background(), evidence); err != nil {
		t.Fatalf("observe launch evidence WorkerInstance: %v", err)
	}
	reader, err := h3launchevidence.NewPostgresRegistryReader(fleetPool)
	if err != nil {
		t.Fatalf("construct launch evidence Registry reader: %v", err)
	}
	snapshot, err := reader.Capture(context.Background(), plan.ID)
	if err != nil {
		t.Fatalf("capture launch evidence Registry snapshot: %v", err)
	}
	if snapshot.DatabaseTime.IsZero() || snapshot.TransactionID == "" || snapshot.SnapshotID == "" ||
		len(snapshot.Workers) != 1 || snapshot.Workers[0].ID != workerID ||
		len(snapshot.Workers[0].Members) != 1 || len(snapshot.Workers[0].Members[0].Devices) != 1 ||
		len(snapshot.Workers[0].Residencies) != 1 || snapshot.Workers[0].Residencies[0].State != "READY" {
		t.Fatalf("authoritative Registry snapshot = %#v", snapshot)
	}
}

func TestNodeAgentWorkerInstanceReporterPersistsThroughMutualTLS(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedWorkerRegistryPlan(t, database.Admin)
	fleetPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	service, err := fleet.NewService(fleetPool)
	if err != nil {
		t.Fatalf("construct Worker Registry service: %v", err)
	}
	workerInstanceID := uuid.MustParse("49200000-0000-0000-0000-000000000013")
	seedWorkerInstance(t, database.Admin, workerInstanceID, workerRegistryProfileID, 1, 1)

	agentID := uuid.MustParse("49200000-0000-0000-0000-000000000014")
	localIdentity := nodeagent.NodeAgentIdentity{
		NodeIdentity: "h3-node-01", AgentID: agentID, AgentEpoch: 7,
	}
	nodeAgentSPIFFE := nodeagent.NodeAgentSPIFFEIdentity(localIdentity)
	transportServer, err := fleettransport.NewServer(service, fleettransport.Config{
		SPIFFEIdentity: "spiffe://vela.internal/fleet-controller/primary",
		ActorIdentity:  "fleet-controller/primary",
		NodeAgentRegistrations: []fleettransport.NodeAgentRegistration{{
			NodeIdentity: localIdentity.NodeIdentity, AgentID: localIdentity.AgentID,
			SPIFFEIdentity: nodeAgentSPIFFE,
		}},
	})
	if err != nil {
		t.Fatalf("construct Fleet transport server: %v", err)
	}

	caCertificate, caKey, caPEM := issueWorkerTransportTestCA(t)
	serverName := "fleet-maintenance.internal"
	serverCertificate, serverKey := issueWorkerTransportTestCertificate(
		t, caCertificate, caKey, pkix.Name{CommonName: serverName},
		[]string{serverName}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	)
	clientCertificate, clientKey := issueWorkerTransportTestCertificate(
		t, caCertificate, caKey, pkix.Name{CommonName: "vela-node-agent"}, nil,
		[]*url.URL{mustParseWorkerSPIFFEID(t, nodeAgentSPIFFE)},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	directory := t.TempDir()
	paths := map[string]string{
		"serverCertificate": filepath.Join(directory, "server.crt"),
		"serverKey":         filepath.Join(directory, "server.key"),
		"clientCA":          filepath.Join(directory, "client-ca.crt"),
		"clientCertificate": filepath.Join(directory, "client.crt"),
		"clientKey":         filepath.Join(directory, "client.key"),
		"serverCA":          filepath.Join(directory, "server-ca.crt"),
	}
	for path, content := range map[string][]byte{
		paths["serverCertificate"]: serverCertificate,
		paths["serverKey"]:         serverKey,
		paths["clientCA"]:          caPEM,
		paths["clientCertificate"]: clientCertificate,
		paths["clientKey"]:         clientKey,
		paths["serverCA"]:          caPEM,
	} {
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("write Fleet transport TLS fixture %s: %v", path, err)
		}
	}
	serverCredentials, err := fleettransport.NewServerTLSCredentials(
		paths["serverCertificate"], paths["serverKey"], paths["clientCA"],
	)
	if err != nil {
		t.Fatalf("construct Fleet server TLS credentials: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Fleet mTLS integration: %v", err)
	}
	grpcServer := grpc.NewServer(grpc.Creds(serverCredentials))
	velav1.RegisterFleetMaintenanceServiceServer(grpcServer, transportServer)
	serveError := make(chan error, 1)
	go func() { serveError <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		<-serveError
	})
	clientCredentials, err := fleettransport.NewClientTLSCredentials(
		paths["clientCertificate"], paths["clientKey"], paths["serverCA"], serverName,
	)
	if err != nil {
		t.Fatalf("construct Node Agent Fleet client TLS credentials: %v", err)
	}
	dialContext, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
	client, err := fleettransport.DialClient(dialContext, listener.Addr().String(), clientCredentials)
	cancelDial()
	if err != nil {
		t.Fatalf("dial Fleet as Node Agent: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	templateEvidence := workerRegistryEvidenceValue(t, workerInstanceID, 0x13)
	attested := templateEvidence.DeviceSet.Devices[0]
	templateEvidence.ObservedAt = time.Time{}
	templateEvidence.ObservedBy = ""
	templateEvidence.DeviceSet.MembershipDigest = ""
	templateEvidence.DeviceSet.TopologyDigest = ""
	templateEvidence.DeviceSet.Devices[0].NodeEpoch = 0
	templateEvidence.DeviceSet.Devices[0].AgentSessionEpoch = 0
	templateEvidence.DeviceSet.Devices[0].NodeAttestationDigest = ""
	templateEvidence.DeviceSet.Devices[0].DeviceEpoch = 0
	templateEvidence.DeviceSet.Devices[0].Health = ""
	templateEvidence.DeviceSet.Devices[0].AttestationDigest = ""
	templateEvidence.Members[0].DeviceSubsetDigest = ""
	templateEvidence.Members[0].IdentityDigest = ""
	templateEvidence.Capacity.Sequence = 0
	templateEvidence.Capacity.ObservedAt = time.Time{}
	templateEvidence.Capacity.ExpiresAt = time.Time{}
	reporter, err := nodeagent.NewWorkerInstanceEvidenceReporter(
		staticIntegrationWorkerInstanceProbe{device: nodeagent.AttestedWorkerDevice{
			DeviceID: attested.ID, ComputeNodeID: attested.ComputeNodeID,
			NodeIdentity: attested.NodeIdentity, GPUUUID: attested.GPUUUID, PCIBDF: attested.PCIBDF,
			NodeEpoch: 1, AgentSessionEpoch: 1, DeviceEpoch: 1,
			NodeAttestationDigest: digestHex(0x43), DeviceAttestationDigest: digestHex(0x44),
			Health: "HEALTHY",
		}},
		client,
		staticIntegrationObservationSequencer{sequence: 1},
		2*time.Minute,
		time.Now,
	)
	if err != nil {
		t.Fatalf("construct Node Agent WorkerInstance reporter: %v", err)
	}
	decision, err := reporter.Report(context.Background(), nodeagent.WorkerInstanceEvidenceTemplate{
		Evidence: templateEvidence, ObservedBy: nodeAgentSPIFFE,
	})
	if err != nil || decision.Readiness != fleet.WorkerInstanceReady {
		t.Fatalf("report WorkerInstance through Node Agent mTLS decision=%#v error=%v", decision, err)
	}

	var lifecycle, residencyState, observedBy string
	var observations int
	if err := database.Admin.QueryRow(`
		SELECT worker.lifecycle_state::text, worker.observed_by,
		       residency.state::text,
		       (SELECT count(*) FROM capacity_observations WHERE worker_instance_id = worker.id)
		FROM worker_instances AS worker
		JOIN model_residencies AS residency ON residency.worker_instance_id = worker.id
		WHERE worker.id = $1
	`, workerInstanceID).Scan(&lifecycle, &observedBy, &residencyState, &observations); err != nil {
		t.Fatalf("read Node Agent mTLS WorkerInstance observation: %v", err)
	}
	expectedActor := strings.Replace(
		nodeAgentSPIFFE,
		"spiffe://vela.internal/node-agent/",
		"node-agent/",
		1,
	)
	if lifecycle != "READY" || residencyState != "READY" || observations != 1 ||
		observedBy != expectedActor {
		t.Fatalf(
			"Node Agent mTLS observation lifecycle=%s residency=%s observations=%d actor=%q",
			lifecycle,
			residencyState,
			observations,
			observedBy,
		)
	}
}

func TestWorkerRegistryReconnectPreservesRuntimeAndFenceInvalidatesAuthority(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedWorkerRegistryPlan(t, database.Admin)
	fleetPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000020")
	seedWorkerInstance(t, database.Admin, workerID, workerRegistryProfileID, 1, 1)
	if _, err := fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		workerRegistryEvidence(workerID, 0x20),
	); err != nil {
		t.Fatalf("observe WorkerInstance before reconnect: %v", err)
	}

	var (
		deviceSetDigest  []byte
		membershipDigest []byte
		residencyID      uuid.UUID
		modelEpoch       int64
	)
	if err := database.Admin.QueryRow(`
		SELECT worker.device_set_digest, worker.membership_digest,
			residency.id, residency.model_runtime_epoch
		FROM worker_instances AS worker
		JOIN model_residencies AS residency ON residency.worker_instance_id = worker.id
		WHERE worker.id = $1 AND residency.state = 'READY'
	`, workerID).Scan(
		&deviceSetDigest, &membershipDigest, &residencyID, &modelEpoch,
	); err != nil {
		t.Fatalf("read WorkerInstance authority: %v", err)
	}
	assertWorkerInstanceAuthority(
		t, fleetPool, workerID, 1, deviceSetDigest, membershipDigest,
		residencyID, modelEpoch, true,
	)

	var instanceEpoch, controlSessionEpoch, reconnectModelEpoch int64
	if err := fleetPool.QueryRow(context.Background(), `
		SELECT instance_epoch, control_session_epoch, model_runtime_epoch
		FROM vela_reconnect_worker_instance($1, 1, 1, 'control-session-2', $2, $3)
	`, workerID, time.Now().UTC(),
		"worker-agent/h3-node-01").Scan(
		&instanceEpoch, &controlSessionEpoch, &reconnectModelEpoch,
	); err != nil {
		t.Fatalf("reconnect WorkerInstance Agent: %v", err)
	}
	if instanceEpoch != 1 || controlSessionEpoch != 2 || reconnectModelEpoch != modelEpoch {
		t.Fatalf(
			"reconnect epochs = instance %d control %d model %d, want 1/2/%d",
			instanceEpoch, controlSessionEpoch, reconnectModelEpoch, modelEpoch,
		)
	}
	assertWorkerInstanceAuthority(
		t, fleetPool, workerID, 1, deviceSetDigest, membershipDigest,
		residencyID, modelEpoch, true,
	)

	var fencedEpoch int64
	var lifecycle string
	if err := fleetPool.QueryRow(context.Background(), `
		SELECT instance_epoch, lifecycle_state::text
		FROM vela_fence_worker_instance($1, 1, 'device epoch changed', $2)
	`, workerID, "node-agent/h3-node-01").Scan(&fencedEpoch, &lifecycle); err != nil {
		t.Fatalf("fence WorkerInstance: %v", err)
	}
	if fencedEpoch != 2 || lifecycle != "FENCED" {
		t.Fatalf("fenced WorkerInstance = epoch %d lifecycle %q, want 2/FENCED", fencedEpoch, lifecycle)
	}
	assertWorkerInstanceAuthority(
		t, fleetPool, workerID, 1, deviceSetDigest, membershipDigest,
		residencyID, modelEpoch, false,
	)
	var bindings int
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM active_device_bindings WHERE worker_instance_id = $1
	`, workerID).Scan(&bindings); err != nil || bindings != 0 {
		t.Fatalf("active bindings after fence = %d error=%v, want zero", bindings, err)
	}
	var residencyState string
	if err := database.Admin.QueryRow(`
		SELECT state::text FROM model_residencies WHERE id = $1
	`, residencyID).Scan(&residencyState); err != nil || residencyState != "DRAINING" {
		t.Fatalf(
			"ModelResidency after fence = %q error=%v, want DRAINING not RELEASED",
			residencyState, err,
		)
	}
}

func TestWorkerRegistryReleaseRequiresApprovalAndBreakEvenEvidence(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedWorkerRegistryPlan(t, database.Admin)
	fleetPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	schedulerPool := newRolePool(
		t, database.DSN, "vela_scheduler_login", "vela-scheduler-password",
	)
	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000030")
	seedWorkerInstance(t, database.Admin, workerID, workerRegistryProfileID, 1, 1)
	if _, err := fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		workerRegistryEvidence(workerID, 0x30),
	); err != nil {
		t.Fatalf("observe WorkerInstance before release: %v", err)
	}
	var residencyID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id FROM model_residencies
		WHERE worker_instance_id = $1 AND state = 'READY'
	`, workerID).Scan(&residencyID); err != nil {
		t.Fatalf("read ModelResidency before release: %v", err)
	}

	if _, err := schedulerPool.Exec(context.Background(), `
		UPDATE model_residencies SET state = 'RELEASED' WHERE id = $1
	`, residencyID); !isPermissionDenied(err) {
		t.Fatalf("Scheduler direct ModelResidency mutation error = %v, want permission denied", err)
	}
	if _, err := fleetPool.Exec(context.Background(), `
		SELECT * FROM vela_begin_worker_instance_drain($1, 1, 'approved capacity change', $2)
	`, workerID, "fleet/operator-1"); err != nil {
		t.Fatalf("begin WorkerInstance drain: %v", err)
	}

	operationID := uuid.MustParse("49200000-0000-0000-0000-000000000031")
	_, err := fleetPool.Exec(context.Background(), `
		SELECT * FROM vela_approve_model_residency_release(
			$1, $2, 1, 'CAPACITY_CHANGE', 'residency-plan-v2', 'fleet/operator-1',
			60, 3600, decode(repeat('61', 32), 'hex')
		)
	`, operationID, residencyID)
	assertPostgresConstraint(t, err, "model_residency_release_below_break_even")

	if _, err := schedulerPool.Exec(context.Background(), `
		SELECT * FROM vela_approve_model_residency_release(
			$1, $2, 1, 'CAPACITY_CHANGE', 'residency-plan-v2', 'scheduler',
			7200, 3600, decode(repeat('62', 32), 'hex')
		)
	`, operationID, residencyID); !isPermissionDenied(err) {
		t.Fatalf("Scheduler release approval error = %v, want permission denied", err)
	}

	var state string
	if err := fleetPool.QueryRow(context.Background(), `
		SELECT state::text FROM vela_approve_model_residency_release(
			$1, $2, 1, 'CAPACITY_CHANGE', 'residency-plan-v2', 'fleet/operator-1',
			7200, 3600, decode(repeat('63', 32), 'hex')
		)
	`, operationID, residencyID).Scan(&state); err != nil {
		t.Fatalf("approve ModelResidency release: %v", err)
	}
	if state != "APPROVED" {
		t.Fatalf("release approval state = %q, want APPROVED", state)
	}
	if err := database.Admin.QueryRow(`
		SELECT state::text FROM model_residencies WHERE id = $1
	`, residencyID).Scan(&state); err != nil || state != "READY" {
		t.Fatalf("residency after approval = %q error=%v, want READY", state, err)
	}

	if err := fleetPool.QueryRow(context.Background(), `
		SELECT state::text
		FROM vela_complete_model_residency_release(
			$1, 1, decode(repeat('64', 32), 'hex'), 'node-agent/h3-node-01'
		)
	`, operationID).Scan(&state); err != nil {
		t.Fatalf("complete ModelResidency release: %v", err)
	}
	if state != "COMPLETED" {
		t.Fatalf("release completion state = %q, want COMPLETED", state)
	}
	var releasedAt sql.NullTime
	if err := database.Admin.QueryRow(`
		SELECT state::text, released_at FROM model_residencies WHERE id = $1
	`, residencyID).Scan(&state, &releasedAt); err != nil || state != "RELEASED" || !releasedAt.Valid {
		t.Fatalf(
			"completed residency = state %q released_at %v error=%v",
			state, releasedAt, err,
		)
	}
}

func TestWorkerRegistryAuthorizesPodMutationOnlyAfterExactResidencyRelease(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	pool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	service, err := fleet.NewService(pool)
	if err != nil {
		t.Fatalf("construct WorkerRegistryAndFleet service: %v", err)
	}
	var registry fleet.WorkerRegistryAndFleet = service

	proposalID := uuid.MustParse("49200000-0000-0000-0000-000000000230")
	if _, err := registry.Propose(context.Background(), fleet.ResidencyPlanInputs{
		ProposalID:       proposalID,
		InputDigest:      mustDigestBytes(t, digestHex(0xf0)),
		ConfidencePPM:    900000,
		ExpiresAt:        time.Now().UTC().Add(time.Hour),
		MinCapacity:      map[string]int64{"dit": 7},
		DesiredCapacity:  map[string]int64{"dit": 7},
		MaxCapacity:      map[string]int64{"dit": 14},
		Cooldown:         24 * time.Hour,
		BudgetMicroUnits: 1000000,
		ReasonCodes:      []string{"WARM_RESIDENCY_FLOOR"},
		ProposedBy:       "capacity-simulator/shadow",
	}); err != nil {
		t.Fatalf("record Pod mutation ResidencyProposal: %v", err)
	}
	encodedPlan, err := json.Marshal(approvedResidencyPlanFixture(proposalID))
	if err != nil {
		t.Fatalf("encode Pod mutation ResidencyPlan: %v", err)
	}
	var approvedPlan fleet.ApprovedResidencyPlan
	if err := json.Unmarshal(encodedPlan, &approvedPlan); err != nil {
		t.Fatalf("decode Pod mutation ResidencyPlan: %v", err)
	}
	if _, err := registry.Apply(context.Background(), approvedPlan); err != nil {
		t.Fatalf("apply Pod mutation ResidencyPlan: %v", err)
	}

	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000204")
	evidence := workerRegistryEvidenceValue(t, workerID, 0xf1)
	if _, err := registry.Observe(context.Background(), evidence); err != nil {
		t.Fatalf("observe Pod mutation WorkerInstance: %v", err)
	}
	request := fleet.MutationAuthorizationRequest{
		RequestUID: "worker-instance-pod-delete-1", ActorIdentity: "fleet/controller",
		Operation:     fleet.MutationDelete,
		KubernetesUID: "worker-instance-pod-uid-1", Namespace: "vela-system",
		Name:                    "wi-49200000000000000000000000000204-ba3790e0",
		WorkerInstanceID:        workerID,
		WorkerInstanceEpoch:     evidence.InstanceEpoch,
		ResidencyPlanRevisionID: approvedPlan.ID,
		WorkerBundleID:          approvedPlan.WorkerBundles[0].ID,
		WorkerMemberID:          evidence.Members[0].ID,
		RequestDigest:           mustDigestBytes(t, digestHex(0xf2)),
	}
	_, err = service.AuthorizeMutation(context.Background(), request)
	assertFleetFailure(t, err, fleet.FailureConflict)

	if _, err := registry.Drain(context.Background(), fleet.WorkerInstanceDrainRequest{
		WorkerInstanceID:      workerID,
		ExpectedInstanceEpoch: evidence.InstanceEpoch,
		Reason:                "approved WorkerInstance retirement",
		RequestedBy:           "fleet/operator-4",
	}); err != nil {
		t.Fatalf("drain Pod mutation WorkerInstance: %v", err)
	}
	_, err = service.AuthorizeMutation(context.Background(), request)
	assertFleetFailure(t, err, fleet.FailureConflict)

	releaseID := uuid.MustParse("49200000-0000-0000-0000-000000000231")
	if _, err := pool.Exec(context.Background(), `
		SELECT * FROM vela_approve_model_residency_release(
			$1, $2, 1, 'REVISION_ROLLOUT', 'residency-plan-v2', 'fleet/operator-4',
			7200, 3600, decode(repeat('f3', 32), 'hex')
		)
	`, releaseID, evidence.Residencies[0].ID); err != nil {
		t.Fatalf("approve Pod mutation ModelResidency release: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
		SELECT * FROM vela_complete_model_residency_release(
			$1, 1, decode(repeat('f4', 32), 'hex'), 'node-agent/h3-node-01'
		)
	`, releaseID); err != nil {
		t.Fatalf("complete Pod mutation ModelResidency release: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*fleet.MutationAuthorizationRequest)
	}{
		{
			name: "ResidencyPlan revision",
			mutate: func(candidate *fleet.MutationAuthorizationRequest) {
				candidate.ResidencyPlanRevisionID = uuid.MustParse(
					"49200000-0000-0000-0000-000000000299",
				)
			},
		},
		{
			name: "WorkerBundle",
			mutate: func(candidate *fleet.MutationAuthorizationRequest) {
				candidate.WorkerBundleID = uuid.MustParse(
					"49200000-0000-0000-0000-000000000298",
				)
			},
		},
		{
			name: "WorkerMember",
			mutate: func(candidate *fleet.MutationAuthorizationRequest) {
				candidate.WorkerMemberID = uuid.MustParse(
					"49200000-0000-0000-0000-000000000297",
				)
			},
		},
		{
			name: "WorkerInstance epoch",
			mutate: func(candidate *fleet.MutationAuthorizationRequest) {
				candidate.WorkerInstanceEpoch++
			},
		},
	} {
		t.Run("rejects mismatched "+test.name, func(t *testing.T) {
			candidate := request
			candidate.RequestUID = "worker-instance-pod-mismatch-" + test.name
			candidate.RequestDigest = mustDigestBytes(t, digestHex(0xf5))
			test.mutate(&candidate)
			_, err := service.AuthorizeMutation(context.Background(), candidate)
			assertFleetFailure(t, err, fleet.FailureConflict)
		})
	}

	authorized, err := service.AuthorizeMutation(context.Background(), request)
	if err != nil || !authorized.Authorized || authorized.Replayed ||
		authorized.RequestUID != request.RequestUID {
		t.Fatalf("authorize released WorkerInstance Pod = %#v error=%v", authorized, err)
	}
	replayed, err := service.AuthorizeMutation(context.Background(), request)
	if err != nil || !replayed.Authorized || !replayed.Replayed ||
		replayed.RequestUID != request.RequestUID {
		t.Fatalf("replay released WorkerInstance Pod authorization = %#v error=%v", replayed, err)
	}
	conflicting := request
	conflicting.RequestDigest = mustDigestBytes(t, digestHex(0xf5))
	_, err = service.AuthorizeMutation(context.Background(), conflicting)
	assertFleetFailure(t, err, fleet.FailureConflict)

	finalizerRequest := request
	finalizerRequest.RequestUID = "worker-instance-pod-finalizer-1"
	finalizerRequest.Operation = fleet.MutationRemoveFinalizer
	finalizerRequest.RequestDigest = mustDigestBytes(t, digestHex(0xf6))
	finalized, err := service.AuthorizeMutation(context.Background(), finalizerRequest)
	if err != nil || !finalized.Authorized || finalized.Replayed {
		t.Fatalf("authorize released WorkerInstance Pod finalizer = %#v error=%v", finalized, err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE worker_instance_pod_mutation_authorizations
		SET actor_identity = 'fleet/rewritten'
		WHERE request_uid = $1
	`, request.RequestUID); err == nil || !strings.Contains(err.Error(), "55000") {
		t.Fatalf("rewrite WorkerInstance Pod authorization error=%v, want SQLSTATE 55000", err)
	}
}

func TestWorkerRegistryAuthorizesFencedOldEpochPodCleanup(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	pool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	service, err := fleet.NewService(pool)
	if err != nil {
		t.Fatalf("construct WorkerRegistryAndFleet service: %v", err)
	}
	var registry fleet.WorkerRegistryAndFleet = service

	proposalID := uuid.MustParse("49200000-0000-0000-0000-000000000232")
	if _, err := registry.Propose(context.Background(), fleet.ResidencyPlanInputs{
		ProposalID:       proposalID,
		InputDigest:      mustDigestBytes(t, digestHex(0xf7)),
		ConfidencePPM:    900000,
		ExpiresAt:        time.Now().UTC().Add(time.Hour),
		MinCapacity:      map[string]int64{"dit": 7},
		DesiredCapacity:  map[string]int64{"dit": 7},
		MaxCapacity:      map[string]int64{"dit": 14},
		Cooldown:         24 * time.Hour,
		BudgetMicroUnits: 1000000,
		ReasonCodes:      []string{"WARM_RESIDENCY_FLOOR"},
		ProposedBy:       "capacity-simulator/shadow",
	}); err != nil {
		t.Fatalf("record fenced cleanup ResidencyProposal: %v", err)
	}
	encodedPlan, err := json.Marshal(approvedResidencyPlanFixture(proposalID))
	if err != nil {
		t.Fatalf("encode fenced cleanup ResidencyPlan: %v", err)
	}
	var approvedPlan fleet.ApprovedResidencyPlan
	if err := json.Unmarshal(encodedPlan, &approvedPlan); err != nil {
		t.Fatalf("decode fenced cleanup ResidencyPlan: %v", err)
	}
	if _, err := registry.Apply(context.Background(), approvedPlan); err != nil {
		t.Fatalf("apply fenced cleanup ResidencyPlan: %v", err)
	}
	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000204")
	evidence := workerRegistryEvidenceValue(t, workerID, 0xf8)
	if _, err := registry.Observe(context.Background(), evidence); err != nil {
		t.Fatalf("observe fenced cleanup WorkerInstance: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `
		SELECT * FROM vela_fence_worker_instance($1, $2, $3, $4)
	`, workerID, evidence.InstanceEpoch, "device authority changed", "node-agent/h3-node-01"); err != nil {
		t.Fatalf("fence old WorkerInstance epoch: %v", err)
	}
	request := fleet.MutationAuthorizationRequest{
		RequestUID: "worker-instance-fenced-pod-delete-1", ActorIdentity: "fleet/controller",
		Operation:     fleet.MutationDelete,
		KubernetesUID: "worker-instance-fenced-pod-uid-1", Namespace: "vela-system",
		Name:                    "wi-49200000000000000000000000000204-ba3790e0",
		WorkerInstanceID:        workerID,
		WorkerInstanceEpoch:     evidence.InstanceEpoch,
		ResidencyPlanRevisionID: approvedPlan.ID,
		WorkerBundleID:          approvedPlan.WorkerBundles[0].ID,
		WorkerMemberID:          evidence.Members[0].ID,
		RequestDigest:           mustDigestBytes(t, digestHex(0xf9)),
	}
	authorized, err := service.AuthorizeMutation(context.Background(), request)
	if err != nil || !authorized.Authorized || authorized.Replayed {
		t.Fatalf("authorize fenced old WorkerInstance Pod = %#v error=%v", authorized, err)
	}
	currentEpoch := request
	currentEpoch.RequestUID = "worker-instance-fenced-pod-delete-current"
	currentEpoch.WorkerInstanceEpoch++
	currentEpoch.RequestDigest = mustDigestBytes(t, digestHex(0xfa))
	_, err = service.AuthorizeMutation(context.Background(), currentEpoch)
	assertFleetFailure(t, err, fleet.FailureConflict)
}

func assertFleetFailure(t *testing.T, err error, code fleet.FailureCode) {
	t.Helper()
	var failure *fleet.Failure
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("Fleet error = %v, want %s", err, code)
	}
}

func TestWorkerRegistryRequiresCompleteMultiMemberIdentity(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedWorkerRegistryPlan(t, database.Admin)
	seedMultiMemberProfile(t, database.Admin)
	fleetPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000103")
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_instances (
			id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
			lifecycle_state, reachability_state, instance_epoch,
			control_session_epoch, desired_member_count, desired_device_count
		) VALUES (
			$1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 2, 4
		)
	`, workerID, multiWorkerProfileID, multiCapacityPoolID,
		workerRegistryBundleID); err != nil {
		t.Fatalf("seed multi-member WorkerInstance: %v", err)
	}

	_, err := fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		multiMemberWorkerEvidence(workerID, false),
	)
	assertPostgresConstraint(t, err, "worker_instance_membership_incomplete")

	if _, err := fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		multiMemberWorkerEvidence(workerID, true),
	); err != nil {
		t.Fatalf("observe complete multi-member WorkerInstance: %v", err)
	}
	var memberCount, bindingCount int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM worker_members
			 WHERE worker_instance_id = $1 AND readiness = 'READY'),
			(SELECT count(*) FROM active_device_bindings
			 WHERE worker_instance_id = $1)
	`, workerID).Scan(&memberCount, &bindingCount); err != nil {
		t.Fatalf("read multi-member registry coverage: %v", err)
	}
	if memberCount != 2 || bindingCount != 4 {
		t.Fatalf(
			"multi-member coverage = %d members/%d bindings, want 2/4",
			memberCount, bindingCount,
		)
	}

	if _, err := fleetPool.Exec(context.Background(), `
		SELECT * FROM vela_fence_worker_instance($1, 1, 'member-1 epoch changed', $2)
	`, workerID, "node-agent/llm-node-a"); err != nil {
		t.Fatalf("fence multi-member WorkerInstance: %v", err)
	}
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM active_device_bindings WHERE worker_instance_id = $1
	`, workerID).Scan(&bindingCount); err != nil || bindingCount != 0 {
		t.Fatalf("multi-member bindings after fence = %d error=%v, want zero", bindingCount, err)
	}
}

func TestWorkerRegistryProposalIsAdvisoryUntilApprovedPlan(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	fleetPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	schedulerPool := newRolePool(
		t, database.DSN, "vela_scheduler_login", "vela-scheduler-password",
	)
	proposalID := uuid.MustParse("49200000-0000-0000-0000-000000000200")
	proposal := map[string]any{
		"schema_version":     1,
		"id":                 proposalID.String(),
		"input_digest":       digestHex(0xc0),
		"confidence_ppm":     900000,
		"expires_at":         time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
		"min_capacity":       map[string]any{"dit": 7},
		"desired_capacity":   map[string]any{"dit": 14},
		"max_capacity":       map[string]any{"dit": 21},
		"cooldown_seconds":   86400,
		"budget_micro_units": 1000000,
		"reason_codes":       []string{"QUEUE_PRESSURE", "WARM_RESIDENCY_FLOOR"},
		"proposed_by":        "capacity-simulator/shadow",
	}
	encodedProposal, err := json.Marshal(proposal)
	if err != nil {
		t.Fatalf("encode ResidencyProposal: %v", err)
	}
	var recordedProposalID uuid.UUID
	if err := fleetPool.QueryRow(context.Background(), `
		SELECT proposal_id FROM vela_record_residency_proposal($1::jsonb)
	`, encodedProposal).Scan(&recordedProposalID); err != nil {
		t.Fatalf("record ResidencyProposal: %v", err)
	}
	if recordedProposalID != proposalID {
		t.Fatalf("recorded proposal id = %s, want %s", recordedProposalID, proposalID)
	}
	var workerCount int
	if err := database.Admin.QueryRow(`SELECT count(*) FROM worker_instances`).Scan(&workerCount); err != nil {
		t.Fatalf("count Workers after proposal: %v", err)
	}
	if workerCount != 0 {
		t.Fatalf("advisory proposal created %d WorkerInstances", workerCount)
	}

	plan := approvedResidencyPlanFixture(proposalID)
	delete(plan, "approval_evidence_digest")
	encodedPlan, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("encode unapproved ResidencyPlan: %v", err)
	}
	_, err = fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_apply_residency_plan($1::jsonb)",
		encodedPlan,
	)
	assertPostgresConstraint(t, err, "residency_plan_approval_required")

	plan = approvedResidencyPlanFixture(proposalID)
	encodedPlan, err = json.Marshal(plan)
	if err != nil {
		t.Fatalf("encode approved ResidencyPlan: %v", err)
	}
	if _, err := schedulerPool.Exec(
		context.Background(),
		"SELECT * FROM vela_apply_residency_plan($1::jsonb)",
		encodedPlan,
	); !isPermissionDenied(err) {
		t.Fatalf("Scheduler ResidencyPlan apply error = %v, want permission denied", err)
	}
	var appliedWorkers int
	if err := fleetPool.QueryRow(context.Background(), `
		SELECT worker_instance_count FROM vela_apply_residency_plan($1::jsonb)
	`, encodedPlan).Scan(&appliedWorkers); err != nil {
		t.Fatalf("apply approved ResidencyPlan: %v", err)
	}
	if appliedWorkers != 1 {
		t.Fatalf("applied WorkerInstance count = %d, want 1", appliedWorkers)
	}
	var lifecycle string
	if err := database.Admin.QueryRow(`
		SELECT lifecycle_state::text FROM worker_instances
		WHERE id = '49200000-0000-0000-0000-000000000204'
	`).Scan(&lifecycle); err != nil || lifecycle != "PROVISIONING" {
		t.Fatalf("planned WorkerInstance lifecycle = %q error=%v", lifecycle, err)
	}
}

func TestWorkerRegistryAndFleetGoInterfaceOwnsCommandSequence(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	pool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	service, err := fleet.NewService(pool)
	if err != nil {
		t.Fatalf("construct WorkerRegistryAndFleet service: %v", err)
	}
	var registry fleet.WorkerRegistryAndFleet = service

	proposalID := uuid.MustParse("49200000-0000-0000-0000-000000000210")
	proposal, err := registry.Propose(context.Background(), fleet.ResidencyPlanInputs{
		ProposalID:       proposalID,
		InputDigest:      mustDigestBytes(t, digestHex(0xd0)),
		ConfidencePPM:    900000,
		ExpiresAt:        time.Now().UTC().Add(time.Hour),
		MinCapacity:      map[string]int64{"dit": 7},
		DesiredCapacity:  map[string]int64{"dit": 14},
		MaxCapacity:      map[string]int64{"dit": 21},
		Cooldown:         24 * time.Hour,
		BudgetMicroUnits: 1000000,
		ReasonCodes:      []string{"QUEUE_PRESSURE", "WARM_RESIDENCY_FLOOR"},
		ProposedBy:       "capacity-simulator/shadow",
	})
	if err != nil || proposal.ID != proposalID {
		t.Fatalf("propose residency = %#v error=%v", proposal, err)
	}

	encodedPlan, err := json.Marshal(approvedResidencyPlanFixture(proposalID))
	if err != nil {
		t.Fatalf("encode Go Interface ResidencyPlan: %v", err)
	}
	var approvedPlan fleet.ApprovedResidencyPlan
	if err := json.Unmarshal(encodedPlan, &approvedPlan); err != nil {
		t.Fatalf("decode Go Interface ResidencyPlan: %v", err)
	}
	actuation, err := registry.Apply(context.Background(), approvedPlan)
	if err != nil || actuation.WorkerInstanceCount != 1 {
		t.Fatalf("apply residency plan = %#v error=%v", actuation, err)
	}

	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000204")
	var evidence fleet.WorkerInstanceEvidence
	if err := json.Unmarshal(workerRegistryEvidence(workerID, 0xd1), &evidence); err != nil {
		t.Fatalf("decode WorkerInstance evidence: %v", err)
	}
	decision, err := registry.Observe(context.Background(), evidence)
	if err != nil || decision.Readiness != fleet.WorkerInstanceReady {
		t.Fatalf("observe WorkerInstance = %#v error=%v", decision, err)
	}
	transition, err := registry.Drain(context.Background(), fleet.WorkerInstanceDrainRequest{
		WorkerInstanceID:      workerID,
		ExpectedInstanceEpoch: 1,
		Reason:                "approved rollout",
		RequestedBy:           "fleet/operator-3",
	})
	if err != nil || transition.Lifecycle != fleet.WorkerInstanceDraining {
		t.Fatalf("drain WorkerInstance = %#v error=%v", transition, err)
	}
}

func TestWorkerRegistryAuthorityFailsClosedWithoutFreshCapacityObservation(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedWorkerRegistryPlan(t, database.Admin)
	pool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	service, err := fleet.NewService(pool)
	if err != nil {
		t.Fatalf("construct Worker Registry authority reader: %v", err)
	}
	var registry fleet.WorkerRegistryAndFleet = service
	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000220")
	seedWorkerInstance(t, database.Admin, workerID, workerRegistryProfileID, 1, 1)
	evidence := workerRegistryEvidenceValue(t, workerID, 0xe0)
	fleetPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	if _, err := fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		mustJSON(t, evidence),
	); err != nil {
		t.Fatalf("observe WorkerInstance: %v", err)
	}
	authority := workerAuthority(t, evidence)
	matches, err := registry.AuthorityMatches(context.Background(), authority)
	if err != nil || !matches {
		t.Fatalf("fresh WorkerInstance authority matches=%t error=%v, want true", matches, err)
	}

	if _, err := database.Admin.Exec(
		"DELETE FROM capacity_observations WHERE worker_instance_id = $1",
		workerID,
	); err != nil {
		t.Fatalf("remove CapacityObservation fixture: %v", err)
	}
	matches, err = registry.AuthorityMatches(context.Background(), authority)
	if err != nil || matches {
		t.Fatalf("missing-capacity WorkerInstance authority matches=%t error=%v, want false", matches, err)
	}
	if _, err := fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_fence_worker_instance($1, 1, 'freshness test turnover', $2)",
		workerID,
		"fleet/freshness-test",
	); err != nil {
		t.Fatalf("fence first WorkerInstance before expired-capacity fixture: %v", err)
	}

	expiredWorkerID := uuid.MustParse("49200000-0000-0000-0000-000000000221")
	seedWorkerInstance(t, database.Admin, expiredWorkerID, workerRegistryProfileID, 1, 1)
	expired := workerRegistryEvidenceValue(t, expiredWorkerID, 0xe1)
	expired.Capacity.ObservedAt = time.Now().UTC().Add(-2 * time.Minute)
	expired.Capacity.ExpiresAt = time.Now().UTC().Add(-time.Minute)
	if _, err := fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		mustJSON(t, expired),
	); err != nil {
		t.Fatalf("observe WorkerInstance with expired capacity evidence: %v", err)
	}
	matches, err = registry.AuthorityMatches(context.Background(), workerAuthority(t, expired))
	if err != nil || matches {
		t.Fatalf("expired-capacity WorkerInstance authority matches=%t error=%v, want false", matches, err)
	}
}

func TestWorkerRegistryMigrationRollbackRequiresEmptyAuthority(t *testing.T) {
	t.Run("empty registry rolls back", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 35)
		migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
		if err := goose.DownTo(database.Admin, migrations, 34); err != nil {
			t.Fatalf("roll back empty Worker Registry: %v", err)
		}
	})

	t.Run("proposal authority refuses rollback", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 35)
		migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
		fleetPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
		proposal := map[string]any{
			"schema_version":     1,
			"id":                 "49200000-0000-0000-0000-000000000222",
			"input_digest":       digestHex(0xe2),
			"confidence_ppm":     800000,
			"expires_at":         time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
			"min_capacity":       map[string]int64{"dit": 7},
			"desired_capacity":   map[string]int64{"dit": 14},
			"max_capacity":       map[string]int64{"dit": 21},
			"cooldown_seconds":   3600,
			"budget_micro_units": 1000000,
			"reason_codes":       []string{"QUEUE_PRESSURE"},
			"proposed_by":        "capacity-simulator/shadow",
		}
		if _, err := fleetPool.Exec(
			context.Background(),
			"SELECT * FROM vela_record_residency_proposal($1::jsonb)",
			mustJSON(t, proposal),
		); err != nil {
			t.Fatalf("record rollback guard proposal: %v", err)
		}
		err := goose.DownTo(database.Admin, migrations, 34)
		assertPostgresConstraint(t, err, "worker_registry_rollback_is_unsafe")
	})
}

func workerAuthority(t *testing.T, evidence fleet.WorkerInstanceEvidence) fleet.WorkerInstanceAuthority {
	t.Helper()
	membership := mustDigestBytes(t, evidence.DeviceSet.MembershipDigest)
	topology := mustDigestBytes(t, evidence.DeviceSet.TopologyDigest)
	deviceSetDigest := sha256.Sum256(append(append([]byte(nil), membership...), topology...))
	return fleet.WorkerInstanceAuthority{
		WorkerInstanceID:  evidence.WorkerInstanceID,
		InstanceEpoch:     evidence.InstanceEpoch,
		DeviceSetDigest:   deviceSetDigest[:],
		MembershipDigest:  membership,
		ModelResidencyID:  evidence.Residencies[0].ID,
		ModelRuntimeEpoch: evidence.Residencies[0].ModelRuntimeEpoch,
	}
}

func workerRegistryEvidenceValue(
	t *testing.T,
	workerID uuid.UUID,
	identityByte byte,
) fleet.WorkerInstanceEvidence {
	t.Helper()
	var evidence fleet.WorkerInstanceEvidence
	if err := json.Unmarshal(workerRegistryEvidence(workerID, identityByte), &evidence); err != nil {
		t.Fatalf("decode WorkerInstance evidence fixture: %v", err)
	}
	return evidence
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode JSON fixture: %v", err)
	}
	return encoded
}

func mustDigestBytes(t *testing.T, encoded string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode fixture digest: %v", err)
	}
	return decoded
}

func approvedResidencyPlanFixture(proposalID uuid.UUID) map[string]any {
	now := time.Now().UTC().Truncate(time.Millisecond)
	return map[string]any{
		"schema_version":           1,
		"id":                       "49200000-0000-0000-0000-000000000201",
		"stable_id":                "h3-dit-residency",
		"revision":                 1,
		"source_proposal_id":       proposalID.String(),
		"content_digest":           digestHex(0xc1),
		"approval_evidence_digest": digestHex(0xc2),
		"approved_at":              now.Format(time.RFC3339Nano),
		"approved_by":              "fleet/operator-2",
		"capacity_pools": []map[string]any{{
			"id":                        "49200000-0000-0000-0000-000000000202",
			"stable_id":                 "h3-dit-plan-pool",
			"stage_profile_revision_id": "49000000-0000-0000-0000-000000000041",
			"resource_class":            "GPU",
			"security_class":            "INTERNAL",
			"region":                    "cn-shanghai",
			"max_ready_queue_depth":     1024,
		}},
		"worker_bundles": []map[string]any{{
			"id":                 "49200000-0000-0000-0000-000000000203",
			"stable_id":          "h3-node-plan-bundle",
			"desired_generation": 1,
			"layout_digest":      digestHex(0xc3),
		}},
		"worker_instances": []map[string]any{{
			"id":                         "49200000-0000-0000-0000-000000000204",
			"worker_profile_revision_id": workerRegistryProfileID,
			"capacity_pool_id":           "49200000-0000-0000-0000-000000000202",
			"worker_bundle_id":           "49200000-0000-0000-0000-000000000203",
			"desired_member_count":       1,
			"desired_device_count":       1,
		}},
	}
}

func assertWorkerInstanceAuthority(
	t *testing.T,
	database *pgxpool.Pool,
	workerID uuid.UUID,
	instanceEpoch int64,
	deviceSetDigest []byte,
	membershipDigest []byte,
	residencyID uuid.UUID,
	modelRuntimeEpoch int64,
	want bool,
) {
	t.Helper()
	var matches bool
	if err := database.QueryRow(context.Background(), `
		SELECT vela_worker_instance_authority_matches($1, $2, $3, $4, $5, $6)
	`, workerID, instanceEpoch, deviceSetDigest, membershipDigest,
		residencyID, modelRuntimeEpoch).Scan(&matches); err != nil {
		t.Fatalf("match WorkerInstance authority: %v", err)
	}
	if matches != want {
		t.Fatalf("WorkerInstance authority match = %t, want %t", matches, want)
	}
}

func seedWorkerRegistryPlan(t *testing.T, database *sql.DB) {
	t.Helper()
	if _, err := database.Exec(`
		INSERT INTO capacity_pools (
			id, stable_id, stage_profile_revision_id, resource_class,
			security_class, region, max_ready_queue_depth, state
		) VALUES (
			$1, 'h3-dit-shanghai', '49000000-0000-0000-0000-000000000041',
			'GPU', 'INTERNAL', 'cn-shanghai', 1024, 'ACTIVE'
		)
	`, workerRegistryPoolID); err != nil {
		t.Fatalf("seed Worker Registry CapacityPool: %v", err)
	}
	if _, err := database.Exec(`
		INSERT INTO worker_bundles (
			id, stable_id, plan_revision, desired_generation, observed_generation,
			lifecycle_state, layout_digest, approved_by
		) VALUES (
			$1, 'h3-node-01-layout', 'plan-h3-v1', 1, 0, 'APPLYING',
			decode(repeat('42', 32), 'hex'), 'fleet-test'
		)
	`, workerRegistryBundleID); err != nil {
		t.Fatalf("seed Worker Registry WorkerBundle: %v", err)
	}
}

func seedWorkerInstance(
	t *testing.T,
	database *sql.DB,
	workerID uuid.UUID,
	profileID string,
	memberCount int,
	deviceCount int,
) {
	t.Helper()
	if _, err := database.Exec(`
		INSERT INTO worker_instances (
			id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
			lifecycle_state, reachability_state, instance_epoch,
			control_session_epoch, desired_member_count, desired_device_count
		) VALUES ($1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, $5, $6)
	`, workerID, profileID, workerRegistryPoolID, workerRegistryBundleID,
		memberCount, deviceCount); err != nil {
		t.Fatalf("seed WorkerInstance %s: %v", workerID, err)
	}
}

func seedMultiMemberProfile(t *testing.T, database *sql.DB) {
	t.Helper()
	if _, err := database.Exec(`
		INSERT INTO worker_profile_revisions (
			id, stable_id, revision, state, device_count, member_count,
			device_set_shape, resident_model_revisions, capacity_limits,
			readiness_checks, content_digest
		) VALUES (
			$1, 'future-llm-4gpu-2node', 1, 'CERTIFIED', 4, 2,
			'{"kind":"certified-multi-node","devices":4,"members":2}',
			'["future-llm-component-v1"]', '{"concurrency":1}',
			'{"membership_barrier":true,"warmup":true}',
			decode(repeat('70', 32), 'hex')
		)
	`, multiWorkerProfileID); err != nil {
		t.Fatalf("seed multi-member WorkerProfile: %v", err)
	}
	if _, err := database.Exec(`
		INSERT INTO stage_profile_revisions (
			id, stable_id, revision, state, stage_definition_revision_id,
			model_component_revision, runtime_image_digest,
			worker_profile_revision_id, result_equivalence_revision_id,
			certified_capacity_vector, content_digest
		) VALUES (
			$1, 'future-llm-stage', 1, 'CERTIFIED',
			'49000000-0000-0000-0000-000000000031', 'future-llm-component-v1',
			'sha256:2222222222222222222222222222222222222222222222222222222222222222',
			$2, '49000000-0000-0000-0000-000000000024', '{"concurrency":1}',
			decode(repeat('71', 32), 'hex')
		)
	`, multiStageProfileID, multiWorkerProfileID); err != nil {
		t.Fatalf("seed multi-member StageProfile: %v", err)
	}
	if _, err := database.Exec(`
		INSERT INTO capacity_pools (
			id, stable_id, stage_profile_revision_id, resource_class,
			security_class, region, max_ready_queue_depth, state
		) VALUES (
			$1, 'future-llm-multinode', $2, 'GPU', 'INTERNAL',
			'cn-shanghai', 128, 'ACTIVE'
		)
	`, multiCapacityPoolID, multiStageProfileID); err != nil {
		t.Fatalf("seed multi-member CapacityPool: %v", err)
	}
}

func multiMemberWorkerEvidence(workerID uuid.UUID, complete bool) []byte {
	observedAt := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	nodeIDs := []string{
		"49200000-0000-0000-0000-000000000110",
		"49200000-0000-0000-0000-000000000111",
	}
	devices := make([]map[string]any, 0, 4)
	deviceIDs := make([]string, 0, 4)
	for index := 0; index < 4; index++ {
		deviceID := uuid.MustParse(
			"49200000-0000-0000-0000-00000000011" + string(rune('2'+index)),
		).String()
		deviceIDs = append(deviceIDs, deviceID)
		nodeIndex := index / 2
		devices = append(devices, map[string]any{
			"id":                      deviceID,
			"compute_node_id":         nodeIDs[nodeIndex],
			"node_identity":           "llm-node-" + string(rune('a'+nodeIndex)),
			"region":                  "cn-shanghai",
			"network_domain":          "ib-fabric-a",
			"fault_domain":            "power-" + string(rune('a'+nodeIndex)),
			"node_epoch":              1,
			"agent_session_epoch":     1,
			"node_attestation_digest": digestHex(byte(0x80 + nodeIndex)),
			"kind":                    "GPU",
			"gpu_uuid": "GPU-00000000-0000-0000-0000-00000000010" +
				string([]byte{'0' + byte(index)}),
			"pci_bdf":            "0000:4" + string([]byte{'1' + byte(index)}) + ":00.0",
			"device_epoch":       1,
			"ordinal":            index,
			"health":             "HEALTHY",
			"attestation_digest": digestHex(byte(0x90 + index)),
		})
	}
	members := []map[string]any{
		{
			"id":                   "49200000-0000-0000-0000-000000000120",
			"member_key":           "member-0",
			"compute_node_id":      nodeIDs[0],
			"member_epoch":         1,
			"device_ids":           deviceIDs[:2],
			"device_subset_digest": digestHex(0xa0),
			"identity_digest":      digestHex(0xa1),
			"readiness":            "READY",
		},
		{
			"id":                   "49200000-0000-0000-0000-000000000121",
			"member_key":           "member-1",
			"compute_node_id":      nodeIDs[1],
			"member_epoch":         1,
			"device_ids":           deviceIDs[2:],
			"device_subset_digest": digestHex(0xa2),
			"identity_digest":      digestHex(0xa3),
			"readiness":            "READY",
		},
	}
	if !complete {
		members = members[:1]
	}
	evidence := map[string]any{
		"schema_version":        1,
		"worker_instance_id":    workerID.String(),
		"instance_epoch":        1,
		"control_session_epoch": 1,
		"device_set": map[string]any{
			"id":                "49200000-0000-0000-0000-000000000122",
			"membership_digest": digestHex(0xb0),
			"topology_digest":   digestHex(0xb1),
			"devices":           devices,
		},
		"members": members,
		"residencies": []map[string]any{{
			"id":                       "49200000-0000-0000-0000-000000000123",
			"model_component_revision": "future-llm-component-v1",
			"runtime_identity":         "future-llm-runtime@sha256:runtime-v1",
			"runtime_image_digest":     "sha256:2222222222222222222222222222222222222222222222222222222222222222",
			"model_runtime_epoch":      1,
			"state":                    "READY",
			"warmup_evidence_digest":   digestHex(0xb2),
			"canary_evidence_digest":   digestHex(0xb3),
		}},
		"capacity": map[string]any{
			"sequence":    1,
			"vector":      map[string]any{"concurrency": 1},
			"observed_at": observedAt.Format(time.RFC3339Nano),
			"expires_at":  observedAt.Add(2 * time.Minute).Format(time.RFC3339Nano),
		},
		"observed_at": observedAt.Format(time.RFC3339Nano),
		"observed_by": "node-agent/llm-members",
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		panic(err)
	}
	return encoded
}

func workerRegistryEvidence(workerID uuid.UUID, identityByte byte) []byte {
	observedAt := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	evidence := map[string]any{
		"schema_version":        1,
		"worker_instance_id":    workerID.String(),
		"instance_epoch":        1,
		"control_session_epoch": 1,
		"device_set": map[string]any{
			"id":                uuid.NewSHA1(workerID, []byte("device-set")).String(),
			"membership_digest": digestHex(identityByte),
			"topology_digest":   digestHex(identityByte + 1),
			"devices": []map[string]any{{
				"id":                      workerRegistryDeviceID,
				"compute_node_id":         workerRegistryNodeID,
				"node_identity":           "h3-node-01",
				"region":                  "cn-shanghai",
				"network_domain":          "rack-a",
				"fault_domain":            "power-a",
				"node_epoch":              1,
				"agent_session_epoch":     1,
				"node_attestation_digest": digestHex(0x43),
				"kind":                    "GPU",
				"gpu_uuid":                "GPU-00000000-0000-0000-0000-000000000004",
				"pci_bdf":                 "0000:41:00.0",
				"device_epoch":            1,
				"ordinal":                 0,
				"health":                  "HEALTHY",
				"attestation_digest":      digestHex(0x44),
			}},
		},
		"members": []map[string]any{{
			"id":                   uuid.NewSHA1(workerID, []byte("member-0")).String(),
			"member_key":           "member-0",
			"compute_node_id":      workerRegistryNodeID,
			"member_epoch":         1,
			"device_ids":           []string{workerRegistryDeviceID},
			"device_subset_digest": digestHex(identityByte + 3),
			"identity_digest":      digestHex(identityByte + 4),
			"readiness":            "READY",
		}},
		"residencies": []map[string]any{{
			"id":                       uuid.NewSHA1(workerID, []byte("residency")).String(),
			"model_component_revision": "h3-component-v1",
			"runtime_identity":         "h3-dit-runtime@sha256:runtime-v1",
			"runtime_image_digest":     "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			"model_runtime_epoch":      1,
			"state":                    "READY",
			"warmup_evidence_digest":   digestHex(identityByte + 5),
			"canary_evidence_digest":   digestHex(identityByte + 6),
		}},
		"capacity": map[string]any{
			"sequence":    1,
			"vector":      map[string]any{"concurrency": 1},
			"observed_at": observedAt.Format(time.RFC3339Nano),
			"expires_at":  observedAt.Add(2 * time.Minute).Format(time.RFC3339Nano),
		},
		"observed_at": observedAt.Format(time.RFC3339Nano),
		"observed_by": "node-agent/h3-node-01",
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		panic(err)
	}
	return encoded
}

type staticIntegrationWorkerInstanceProbe struct {
	device nodeagent.AttestedWorkerDevice
}

func (probe staticIntegrationWorkerInstanceProbe) AttestWorkerInstanceDevices(
	ctx context.Context,
	expected []nodeagent.ExpectedWorkerDevice,
) ([]nodeagent.AttestedWorkerDevice, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(expected) != 1 || expected[0].DeviceID != probe.device.DeviceID ||
		expected[0].ComputeNodeID != probe.device.ComputeNodeID ||
		expected[0].NodeIdentity != probe.device.NodeIdentity ||
		expected[0].GPUUUID != probe.device.GPUUUID || expected[0].PCIBDF != probe.device.PCIBDF {
		return nil, errors.New("unexpected WorkerInstance Device identity")
	}
	return []nodeagent.AttestedWorkerDevice{probe.device}, nil
}

type staticIntegrationObservationSequencer struct {
	sequence int64
}

func (sequencer staticIntegrationObservationSequencer) NextWorkerInstanceObservationSequence(
	ctx context.Context,
	workerInstanceID uuid.UUID,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if workerInstanceID == uuid.Nil || sequencer.sequence <= 0 {
		return 0, errors.New("invalid WorkerInstance observation sequence")
	}
	return sequencer.sequence, nil
}

func digestHex(value byte) string {
	encoded := make([]byte, 64)
	const digits = "0123456789abcdef"
	for index := 0; index < len(encoded); index += 2 {
		encoded[index] = digits[value>>4]
		encoded[index+1] = digits[value&0x0f]
	}
	return string(encoded)
}

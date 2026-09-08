//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestCPUMockProductionLoopJobCampaign(t *testing.T) {
	runCPUMockRuntimeCampaign(t, cpuCampaignMode{durableStream: true, productionLoop: true})
}

func configureCPUProductionState(t *testing.T, worker *cpuLoadWorker, fixture h3IntegrationWorker) {
	t.Helper()
	state, err := stageworkeragent.NewFileProductionState(stageworkeragent.FileProductionStateConfig{
		Directory: filepath.Join(worker.durable.root, "production-state"), WorkerInstanceID: fixture.workerID,
		WorkerInstanceEpoch: fixture.authority.InstanceEpoch, WorkerMemberID: fixture.evidence.Members[0].ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Error(err)
		}
	})
	worker.durable.productionState = state
}

func configureCPUProductionLoop(t *testing.T, worker *cpuLoadWorker, fixture h3IntegrationWorker) {
	t.Helper()
	durable := worker.durable
	member, device := fixture.evidence.Members[0], fixture.evidence.DeviceSet.Devices[0]
	identities, err := stageworkeragent.DiscoverRuntimeIdentities(t.Context(), durable.runtimeClient, stageworkeragent.RuntimeIdentityExpectation{
		WorkerInstanceID: fixture.workerID.String(), WorkerInstanceEpoch: fixture.authority.InstanceEpoch,
		WorkerMemberID: member.ID.String(), WorkerMemberEpoch: member.MemberEpoch,
	})
	if err != nil || len(identities) != 1 {
		t.Fatalf("discover native CPU Runtime: %+v %v", identities, err)
	}
	// The fixture catalog approves the measured static mock canary before the
	// production loop starts. This is not independent real-model certification.
	var checks []stageworkeragent.ReadinessEvidenceCheck
	for _, check := range []velav1.ModelRuntimeReadinessCheck{
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_DEVICE,
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_BACKEND,
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP,
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_CANARY,
	} {
		response, err := durable.runtimeClient.ProbeReadiness(t.Context(), &velav1.ModelRuntimeServiceProbeReadinessRequest{Identity: identities[0], Check: check})
		if err != nil || !response.GetReady() {
			t.Fatalf("native CPU canary: %v %v", response, err)
		}
		checks = append(checks, stageworkeragent.ReadinessEvidenceCheck{Check: check.String(), Evidence: response.Evidence, Detail: response.Detail})
	}
	evidence, err := stageworkeragent.EncodeReadinessEvidence(checks)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(evidence)
	if _, err := worker.fixture.database.Admin.Exec(`UPDATE model_residencies SET canary_evidence_digest=$2 WHERE id=$1`, fixture.authority.ModelResidencyID, digest[:]); err != nil {
		t.Fatal(err)
	}
	identityDigest, err := hex.DecodeString(member.IdentityDigest)
	if err != nil {
		t.Fatal(err)
	}
	durable.production, err = stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: durable.control, Runtime: durable.runtimeClient, Stream: durable.stream, RuntimeIdentities: identities,
		Devices: []*velav1.StageAuthorityDeviceEpoch{{DeviceId: device.ID.String(), DeviceEpoch: device.DeviceEpoch}},
		Members: []*velav1.StageAuthorityMemberEpoch{{WorkerMemberId: member.ID.String(), MemberEpoch: member.MemberEpoch,
			ModelRuntimeEpoch: fixture.authority.ModelRuntimeEpoch, IdentityDigest: identityDigest}},
		CapacityVector: fixture.capacity, CapacityTTL: time.Minute, HeartbeatInterval: 100 * time.Millisecond,
		RetryMinimum: 25 * time.Millisecond, RetryMaximum: time.Second, ObservationSequenceSource: durable.productionState,
		Now: time.Now, Wait: waitCPULoad,
		RetryObserver: func(operation string, err error) {
			t.Logf("%s production retry %s: %v", worker.stage.key, operation, err)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func cpuProductionLoopLimitations() []string {
	return []string{
		"bounded native CPU mock Job campaign; no GPU certification or Production Gate evidence",
		"production Run, readiness, capacity, heartbeat, mTLS transport and durable stream execute in the test host process",
		"Fleet membership and static native mock canary approval use fixture catalog setup; Node custody, protected mounts and production startup issuer remain separate",
		"application acknowledgement loss occurs after the real mTLS client received ACCEPTED; resident processes and StreamAgent remain live during automatic retry",
		"retained journal metadata is measured separately from payload scratch; history reclamation and sustained arrivals remain open",
		"Runtime gRPC is loopback and artifact store is local exact-version storage; remote storage and process isolation are separate",
		"race detector covers Go test-host components, not native model subprocesses; sampled resources omit Docker VM peaks",
	}
}

type cpuProductionObservation struct {
	Registrations               int64 `json:"accepted_registrations"`
	CapacityReports             int64 `json:"accepted_capacity_reports"`
	Heartbeats                  int64 `json:"accepted_heartbeats"`
	StaleAcquires               int64 `json:"confirmed_stale_acquires"`
	ControlSessionEpoch         int64 `json:"control_session_epoch"`
	CapacityObservationSequence int64 `json:"capacity_observation_sequence"`
}

func assertCPUProductionState(t *testing.T, workers []*cpuLoadWorker) map[string]cpuProductionObservation {
	t.Helper()
	result := make(map[string]cpuProductionObservation, len(workers))
	for _, worker := range workers {
		control := worker.durable.control
		var state struct {
			ControlSessionEpoch         int64 `json:"control_session_epoch"`
			CapacityObservationSequence int64 `json:"capacity_observation_sequence"`
		}
		wire, err := os.ReadFile(filepath.Join(worker.durable.root, "production-state", "stage-worker-production-state.json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(wire, &state); err != nil {
			t.Fatal(err)
		}
		if state.ControlSessionEpoch <= 2 || state.ControlSessionEpoch != control.CurrentControlSessionEpoch() {
			t.Fatalf("%s production session did not reconnect durably: %+v", worker.stage.key, state)
		}
		var acceptedSequence, capacitySession int64
		if err := worker.fixture.database.Admin.QueryRow(`SELECT observation_sequence, stage_worker_control_session_epoch
			FROM capacity_observations WHERE worker_instance_id=$1 AND stage_worker_control_session_epoch IS NOT NULL
			ORDER BY observation_sequence DESC LIMIT 1`, worker.fixture.authority.WorkerInstanceID).Scan(&acceptedSequence, &capacitySession); err != nil {
			t.Fatal(err)
		}
		if acceptedSequence <= 0 || acceptedSequence > state.CapacityObservationSequence || capacitySession != state.ControlSessionEpoch {
			t.Fatalf("%s PostgreSQL session/sequence differs from durable client: accepted=%d session=%d local=%+v", worker.stage.key, acceptedSequence, capacitySession, state)
		}
		if control.registrations.Load() == 0 || control.heartbeats.Load() == 0 || worker.capacityReports.Load() < 2 {
			t.Fatalf("%s did not exercise production evidence/heartbeat/capacity", worker.stage.key)
		}
		result[worker.stage.key] = cpuProductionObservation{Registrations: control.registrations.Load(), CapacityReports: worker.capacityReports.Load(),
			Heartbeats: control.heartbeats.Load(), StaleAcquires: control.staleAcquires.Load(), ControlSessionEpoch: state.ControlSessionEpoch,
			CapacityObservationSequence: state.CapacityObservationSequence}
	}
	return result
}

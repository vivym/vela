package nodeagent_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/nodeagent"
)

func TestWorkerInstanceEvidenceReporterAttestsExactDeviceSetBeforeObserve(t *testing.T) {
	now := time.Date(2026, 8, 30, 14, 0, 0, 0, time.UTC)
	deviceID := uuid.MustParse("49400000-0000-0000-0000-000000000001")
	nodeID := uuid.MustParse("49400000-0000-0000-0000-000000000002")
	memberID := uuid.MustParse("49400000-0000-0000-0000-000000000003")
	workerID := uuid.MustParse("49400000-0000-0000-0000-000000000004")
	probe := &recordingWorkerInstanceDeviceProbe{observations: []nodeagent.AttestedWorkerDevice{{
		DeviceID: deviceID, ComputeNodeID: nodeID, NodeIdentity: "h3-node-01",
		GPUUUID: "GPU-00000000-0000-0000-0000-000000000001",
		PCIBDF:  "0000:41:00.0", NodeEpoch: 7, AgentSessionEpoch: 9, DeviceEpoch: 11,
		NodeAttestationDigest:   strings.Repeat("a", 64),
		DeviceAttestationDigest: strings.Repeat("b", 64), Health: "HEALTHY",
	}}}
	observer := &recordingWorkerInstanceObserver{decision: fleet.WorkerInstanceDecision{
		WorkerInstanceID: workerID, InstanceEpoch: 1, ControlSessionEpoch: 1,
		ModelRuntimeEpoch: 3, Readiness: fleet.WorkerInstanceReady,
	}}
	reporter, err := nodeagent.NewWorkerInstanceEvidenceReporter(
		probe,
		observer,
		2*time.Minute,
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatalf("create WorkerInstance evidence reporter: %v", err)
	}
	template := workerInstanceEvidenceTemplate(workerID, deviceID, nodeID, memberID)
	decision, err := reporter.Report(context.Background(), template)
	if err != nil || decision.Readiness != fleet.WorkerInstanceReady {
		t.Fatalf("report WorkerInstance evidence decision=%#v error=%v", decision, err)
	}
	if probe.calls != 1 || len(observer.evidence) != 1 {
		t.Fatalf("WorkerInstance evidence calls probe=%d observe=%d", probe.calls, len(observer.evidence))
	}
	evidence := observer.evidence[0]
	device := evidence.DeviceSet.Devices[0]
	if device.GPUUUID != probe.observations[0].GPUUUID || device.PCIBDF != probe.observations[0].PCIBDF ||
		device.DeviceEpoch != 11 || device.AgentSessionEpoch != 9 ||
		len(evidence.DeviceSet.MembershipDigest) != 64 || len(evidence.DeviceSet.TopologyDigest) != 64 ||
		len(evidence.Members[0].DeviceSubsetDigest) != 64 || len(evidence.Members[0].IdentityDigest) != 64 ||
		!evidence.ObservedAt.Equal(now) || !evidence.Capacity.ObservedAt.Equal(now) ||
		!evidence.Capacity.ExpiresAt.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("attested WorkerInstance evidence=%#v", evidence)
	}
}

func TestWorkerInstanceEvidenceReporterRejectsUnexpectedPhysicalDevice(t *testing.T) {
	workerID := uuid.MustParse("49400000-0000-0000-0000-000000000004")
	deviceID := uuid.MustParse("49400000-0000-0000-0000-000000000001")
	nodeID := uuid.MustParse("49400000-0000-0000-0000-000000000002")
	memberID := uuid.MustParse("49400000-0000-0000-0000-000000000003")
	probe := &recordingWorkerInstanceDeviceProbe{observations: []nodeagent.AttestedWorkerDevice{{
		DeviceID: deviceID, ComputeNodeID: nodeID, NodeIdentity: "h3-node-01",
		GPUUUID: "GPU-00000000-0000-0000-0000-000000000099",
		PCIBDF:  "0000:41:00.0", NodeEpoch: 7, AgentSessionEpoch: 9, DeviceEpoch: 11,
		NodeAttestationDigest:   strings.Repeat("a", 64),
		DeviceAttestationDigest: strings.Repeat("b", 64), Health: "HEALTHY",
	}}}
	observer := &recordingWorkerInstanceObserver{}
	reporter, err := nodeagent.NewWorkerInstanceEvidenceReporter(
		probe, observer, time.Minute, time.Now,
	)
	if err != nil {
		t.Fatalf("create WorkerInstance evidence reporter: %v", err)
	}
	if _, err := reporter.Report(
		context.Background(),
		workerInstanceEvidenceTemplate(workerID, deviceID, nodeID, memberID),
	); err == nil {
		t.Fatal("WorkerInstance evidence accepted an unexpected GPU UUID")
	}
	if len(observer.evidence) != 0 {
		t.Fatalf("unexpected physical device reached Observe: %#v", observer.evidence)
	}
}

func workerInstanceEvidenceTemplate(
	workerID uuid.UUID,
	deviceID uuid.UUID,
	nodeID uuid.UUID,
	memberID uuid.UUID,
) nodeagent.WorkerInstanceEvidenceTemplate {
	return nodeagent.WorkerInstanceEvidenceTemplate{
		Evidence: fleet.WorkerInstanceEvidence{
			SchemaVersion: 1, WorkerInstanceID: workerID,
			InstanceEpoch: 1, ControlSessionEpoch: 1,
			DeviceSet: fleet.WorkerDeviceSetEvidence{
				ID: uuid.MustParse("49400000-0000-0000-0000-000000000005"),
				Devices: []fleet.WorkerDeviceEvidence{{
					ID: deviceID, ComputeNodeID: nodeID, NodeIdentity: "h3-node-01",
					Region: "cn-shanghai", NetworkDomain: "serving-a", FaultDomain: "rack-a",
					Kind: "GPU", GPUUUID: "GPU-00000000-0000-0000-0000-000000000001",
					PCIBDF: "0000:41:00.0", Ordinal: 0,
				}},
			},
			Members: []fleet.WorkerMemberEvidence{{
				ID: memberID, MemberKey: "member-0", ComputeNodeID: nodeID,
				MemberEpoch: 1, DeviceIDs: []uuid.UUID{deviceID}, Readiness: "READY",
			}},
			Residencies: []fleet.ModelResidencyEvidence{{
				ID:                     uuid.MustParse("49400000-0000-0000-0000-000000000006"),
				ModelComponentRevision: "h3-dit-v1", RuntimeIdentity: "h3-dit-runtime-v1",
				RuntimeImageDigest: strings.Repeat("c", 64), ModelRuntimeEpoch: 3,
				State: "READY", WarmupEvidenceDigest: strings.Repeat("d", 64),
				CanaryEvidenceDigest: strings.Repeat("e", 64),
			}},
			Capacity: fleet.WorkerCapacityEvidence{
				Sequence: 1, Vector: map[string]int64{"concurrency": 1},
			},
		},
		ObservedBy: "node-agent/h3-node-01",
	}
}

type recordingWorkerInstanceDeviceProbe struct {
	calls        int
	observations []nodeagent.AttestedWorkerDevice
}

func (probe *recordingWorkerInstanceDeviceProbe) AttestWorkerInstanceDevices(
	_ context.Context,
	_ []nodeagent.ExpectedWorkerDevice,
) ([]nodeagent.AttestedWorkerDevice, error) {
	probe.calls++
	return append([]nodeagent.AttestedWorkerDevice(nil), probe.observations...), nil
}

type recordingWorkerInstanceObserver struct {
	evidence []fleet.WorkerInstanceEvidence
	decision fleet.WorkerInstanceDecision
}

func (observer *recordingWorkerInstanceObserver) Observe(
	_ context.Context,
	evidence fleet.WorkerInstanceEvidence,
) (fleet.WorkerInstanceDecision, error) {
	observer.evidence = append(observer.evidence, evidence)
	return observer.decision, nil
}

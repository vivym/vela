package fleetcontroller

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestDecodeDesiredConfiguration(t *testing.T) {
	revisions, retirements, err := DecodeDesiredConfiguration(
		[]byte(validDesiredConfiguration),
		"vela-system",
	)
	if err != nil {
		t.Fatalf("DecodeDesiredConfiguration: %v", err)
	}
	if len(revisions) != 1 || len(retirements) != 0 ||
		revisions[0].WorkerPoolID != uuid.MustParse("20000000-0000-0000-0000-000000000001") ||
		revisions[0].Namespace != "vela-system" ||
		revisions[0].ReadinessTimeout != 30*time.Minute ||
		len(revisions[0].Placements) != 2 ||
		revisions[0].Placements[0].NodeIdentity != "h3-node-01" ||
		revisions[0].Placements[0].DaemonSetName != "h3-worker-pool-primary-node-01" ||
		revisions[0].Placements[1].NodeIdentity != "h3-node-02" ||
		revisions[0].Placements[1].WorkerControlTLSSecret != "worker-control-tls-node-2" {
		t.Fatalf("decoded Fleet configuration = %#v / %#v", revisions, retirements)
	}
}

func TestDecodeDesiredConfigurationRejectsUnknownField(t *testing.T) {
	encoded := strings.Replace(
		validDesiredConfiguration,
		"kind: FleetDesiredRevisions",
		"kind: FleetDesiredRevisions\nunknown: value",
		1,
	)
	if _, _, err := DecodeDesiredConfiguration([]byte(encoded), "vela-system"); err == nil {
		t.Fatal("DecodeDesiredConfiguration accepted an unknown field")
	}
}

func TestDecodeDesiredConfigurationDecodesMultiPlacementRetirement(t *testing.T) {
	encoded := strings.Replace(validDesiredConfiguration, "retirements: []", `retirements:
  - revision: eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
    workerPoolID: 40000000-0000-0000-0000-000000000001
    workerPoolName: h3-worker-pool-old
    workerPoolKubernetesUID: kubernetes-old-worker-pool-uid
    reason: retire immutable H3 revision
    deadline: "2026-08-27T12:00:00Z"
    placements:
      - daemonSetName: h3-worker-pool-old-node-1
        daemonSetKubernetesUID: kubernetes-old-daemonset-uid-1
        workers:
          - operationID: 40000000-0000-0000-0000-000000000011
            workerID: 40000000-0000-0000-0000-000000000012
            workerEpoch: 8
            podName: h3-worker-old-node-1
            podKubernetesUID: kubernetes-old-worker-pod-uid-1
      - daemonSetName: h3-worker-pool-old-node-2
        daemonSetKubernetesUID: kubernetes-old-daemonset-uid-2
        workers:
          - operationID: 40000000-0000-0000-0000-000000000021
            workerID: 40000000-0000-0000-0000-000000000022
            workerEpoch: 9
            podName: h3-worker-old-node-2
            podKubernetesUID: kubernetes-old-worker-pod-uid-2
`, 1)
	_, retirements, err := DecodeDesiredConfiguration([]byte(encoded), "vela-system")
	if err != nil {
		t.Fatalf("DecodeDesiredConfiguration: %v", err)
	}
	if len(retirements) != 1 || len(retirements[0].Placements) != 2 ||
		retirements[0].Placements[0].DaemonSetName != "h3-worker-pool-old-node-1" ||
		retirements[0].Placements[1].DaemonSetKubernetesUID != "kubernetes-old-daemonset-uid-2" ||
		len(retirements[0].Placements[1].Workers) != 1 ||
		retirements[0].Placements[1].Workers[0].WorkerEpoch != 9 {
		t.Fatalf("decoded multi-placement retirement = %#v", retirements)
	}
}

const validDesiredConfiguration = `apiVersion: fleet.vela.ai/v1alpha1
kind: FleetDesiredRevisions
revisions:
  - workerPoolID: 20000000-0000-0000-0000-000000000001
    name: h3-worker-pool-primary
    revision: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    workerProfile: h3
    nodeSelector:
      vela.ai/worker-profile: h3
      vela.ai/worker-pool: launch
    initImage: docker.io/library/busybox@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
    workerAgentImage: ghcr.io/vivym/vela-worker-agent@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
    runnerImage: ghcr.io/vivym/vela-h3-runner@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
    artifactStoreTLSSecret: artifact-store-ca-r1
    executionProfileRevisionID: 30000000-0000-0000-0000-000000000001
    inferenceBackendRevision: h3-backend-r1
    readinessTimeout: 30m
    capacityPolicy:
      workerHighWatermarkBytes: 800
      workerLowWatermarkBytes: 400
      workerCriticalFreeBytes: 100
      poolHighWatermarkBytes: 5600
      poolLowWatermarkBytes: 2800
      observationMaxAge: 2m
    placements:
      - nodeIdentity: h3-node-01
        daemonSetName: h3-worker-pool-primary-node-01
        workerRuntimeConfigMap: worker-runtime-node-1
        runnerProfilesConfigMap: runner-profiles-node-1
        runnerGPURolesConfigMap: runner-gpu-roles-node-1
        workerControlTLSSecret: worker-control-tls-node-1
      - nodeIdentity: h3-node-02
        daemonSetName: h3-worker-pool-primary-node-02
        workerRuntimeConfigMap: worker-runtime-node-2
        runnerProfilesConfigMap: runner-profiles-node-2
        runnerGPURolesConfigMap: runner-gpu-roles-node-2
        workerControlTLSSecret: worker-control-tls-node-2
retirements: []
`

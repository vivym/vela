//go:build linux

package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontroller"
	"k8s.io/apimachinery/pkg/types"
)

// Drive the production launcher with the real Fleet renderer. DRA cannot be
// reproduced by the direct CRI path, so reject it before dialing or creating
// any workload. A missing CRI setting makes that ordering observable.
func TestProductionLauncherRejectsFleetDRABeforeCRI(t *testing.T) {
	bundle, err := fleetcontroller.BuildH3WorkerBundleActuation(launcherH3BundleSpec())
	if err != nil {
		t.Fatal(err)
	}
	pods, claims, err := fleetcontroller.MaterializeWorkerInstanceLaunchResources(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if len(pods) != 8 || len(claims) != 8 {
		t.Fatalf("unexpected Fleet resources: %d pods, %d claims", len(pods), len(claims))
	}
	t.Setenv("VELA_RUNTIME_LAUNCHER_CRI_SOCKET", "")
	for _, pod := range pods {
		pod.UID = types.UID(uuid.NewString())
		_, err := launchProductionWorkload(context.Background(), "/run/vela/startup.sock", &pod)
		if err == nil || !strings.Contains(err.Error(), "resource claims") {
			t.Errorf("Fleet Pod %s must reject DRA before CRI access; got %v", pod.Name, err)
		}
	}
}

func launcherPinnedImage(name string, digit byte) string {
	return "registry.example/" + name + "@sha256:" + strings.Repeat(string(digit), 64)
}

func launcherH3BundleSpec() fleetcontroller.H3WorkerBundleSpec {
	devices := [8]fleetcontroller.DeviceConstraint{}
	memberEpochs := [8]int64{}
	deviceSetDigests := [8]string{}
	deviceSubsetDigests := [8]string{}
	membershipDigests := [8]string{}
	ditRuntimes := [7]fleetcontroller.ModelRuntimeProcess{}
	for index := range devices {
		devices[index] = fleetcontroller.DeviceConstraint{
			DeviceID: uuid.MustParse(
				"49300000-0000-0000-0000-0000000001" + fmt.Sprintf("%02d", index),
			),
			DeviceEpoch: int64(index + 11),
			GPUUUID:     "GPU-00000000-0000-0000-0000-00000000000" + string(rune('0'+index)),
			PCIBDF:      "0000:4" + string(rune('1'+index)) + ":00.0",
		}
		memberEpochs[index] = int64(index + 21)
		deviceSetDigests[index] = strings.Repeat(string(rune('1'+index)), 64)
		deviceSubsetDigests[index] = strings.Repeat(string("abcdef12"[index]), 64)
		membershipDigests[index] = strings.Repeat(string("89abcdef"[index]), 64)
		if index > 0 {
			ditRuntimes[index-1] = fleetcontroller.ModelRuntimeProcess{
				ModelResidencyID: uuid.MustParse(
					"49300000-0000-0000-0000-0000000004" + fmt.Sprintf("%02d", index),
				),
				StageProfileRevisionID: uuid.MustParse("49300000-0000-0000-0000-000000000302"),
				ModelRuntimeEpochFloor: int64(index + 30),
				Component:              "DIT", ModelComponentRevision: "h3-dit-v1",
				RuntimeIdentity: "h3-dit-runtime-v1", Command: []string{"/opt/vela/bin/h3-dit"},
				InitializationTimeout: "2h", ShutdownTimeout: "2m",
			}
		}
	}
	return fleetcontroller.H3WorkerBundleSpec{
		SchemaVersion:  2,
		PlanRevisionID: uuid.MustParse("49300000-0000-0000-0000-000000000001"),
		WorkerBundleID: uuid.MustParse("49300000-0000-0000-0000-000000000002"),
		Namespace:      "vela-system", NodeIdentity: "h3-node-01",
		AuxCapacityPoolID:              uuid.MustParse("49300000-0000-0000-0000-000000000003"),
		VAEDecoderCapacityPoolID:       uuid.MustParse("49300000-0000-0000-0000-000000000007"),
		DiTCapacityPoolID:              uuid.MustParse("49300000-0000-0000-0000-000000000004"),
		AuxWorkerProfileRevisionID:     uuid.MustParse("49300000-0000-0000-0000-000000000005"),
		DiTWorkerProfileRevisionID:     uuid.MustParse("49300000-0000-0000-0000-000000000006"),
		InitImage:                      launcherPinnedImage("busybox", 'b'),
		StageWorkerAgentImage:          launcherPinnedImage("vela-stage-worker-agent", 'c'),
		RuntimeImage:                   launcherPinnedImage("vela-h3-stage-runtime", 'd'),
		StageWorkerConfigMap:           "stage-worker-runtime-r1",
		ModelRuntimeVerifierConfigMap:  "model-runtime-verifier-r1",
		StageWorkerControlTLSSecret:    "stage-worker-control-tls-r1",
		StageWorkerAuthoritySecret:     "stage-worker-authority-r1",
		ArtifactStoreCredentialsSecret: "artifact-store-credentials-r1",
		ArtifactStoreCASecret:          "artifact-store-ca-r1",
		Devices:                        devices,
		MemberEpochs:                   memberEpochs,
		DeviceSetDigests:               deviceSetDigests,
		DeviceSubsetDigests:            deviceSubsetDigests,
		MembershipDigests:              membershipDigests,
		Encoder: fleetcontroller.ModelRuntimeProcess{
			ModelResidencyID:       uuid.MustParse("49300000-0000-0000-0000-000000000201"),
			StageProfileRevisionID: uuid.MustParse("49300000-0000-0000-0000-000000000301"),
			ModelRuntimeEpochFloor: 31,
			Component:              "ENCODER", ModelComponentRevision: "h3-encoder-v1",
			RuntimeIdentity: "h3-encoder-runtime-v1", Command: []string{"/opt/vela/bin/h3-encoder"},
			InitializationTimeout: "2h", ShutdownTimeout: "2m",
		},
		DiT: ditRuntimes,
		VAEDecoder: fleetcontroller.ModelRuntimeProcess{
			ModelResidencyID:       uuid.MustParse("49300000-0000-0000-0000-000000000209"),
			StageProfileRevisionID: uuid.MustParse("49300000-0000-0000-0000-000000000309"),
			ModelRuntimeEpochFloor: 39,
			Component:              "VAE_DECODER", ModelComponentRevision: "h3-vae-decoder-v1",
			RuntimeIdentity: "h3-vae-decoder-runtime-v1", Command: []string{"/opt/vela/bin/h3-vae-decoder"},
			InitializationTimeout: "2h", ShutdownTimeout: "2m",
		},
	}
}

package nodeagent

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
)

func TestRuntimeLaunchPlanAuthenticatesCompleteConfiguration(t *testing.T) {
	fixture := runtimeLaunchFixture(t)
	plan, err := VerifyRuntimeLaunchPlan("cpu-node", fixture.verifier, fixture.binding, fixture.wire)
	if err != nil || plan.MatchManifest(fixture.launch) != nil || !proto.Equal(plan.RegistryBinding(), fixture.binding) {
		t.Fatalf("authenticate canonical member plan: %v", err)
	}
	for _, scenario := range []string{"command", "environment", "image", "scratch", "timeout", "runtime-epoch", "profile", "device-epoch", "member-epoch"} {
		t.Run(scenario, func(t *testing.T) {
			other := runtimeLaunchFixture(t)
			plan, err := VerifyRuntimeLaunchPlan("cpu-node", other.verifier, other.binding, other.wire)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "command":
				other.launch.Runtimes[0].Command = []string{"/different-backend"}
			case "environment":
				other.launch.Runtimes[0].Environment = []string{"MODEL_MODE=other"}
			case "image":
				other.launch.Runtimes[0].RuntimeImageDigest = strings.Repeat("e", 64)
			case "scratch":
				other.launch.Runtimes[0].ScratchRoot = "/other"
				other.launch.Runtimes[0].InputRoot, other.launch.Runtimes[0].OutputRoot = "/other/inputs", "/other/outputs"
			case "timeout":
				other.launch.Runtimes[0].InitializationTimeout = "2s"
			case "runtime-epoch":
				other.launch.Runtimes[0].ModelRuntimeEpochFloor++
			case "profile":
				other.launch.Runtimes[0].StageProfileRevisionID = uuid.NewString()
			case "device-epoch":
				other.launch.Devices[0].Epoch++
				other.launch.LocalDevices[0].DeviceEpoch++
			case "member-epoch":
				other.launch.WorkerMemberEpoch++
				other.launch.Members[0].Epoch++
			}
			if _, err := modelruntime.EncodeLaunchManifest(other.launch); err != nil {
				t.Fatalf("mutation must remain structurally valid: %v", err)
			}
			if !errors.Is(plan.MatchManifest(other.launch), ErrRuntimeLaunchPlan) {
				t.Fatal("structurally valid configuration escaped the signed bundle")
			}
		})
	}
	fixture.wire[0] = '!'
	fixture.binding.Signature[0] ^= 1
	copy := plan.RegistryBinding()
	copy.Claim.BundleDigest[0] ^= 1
	pod := plan.ExpectedPod()
	pod.Spec.Containers[0].Image = "changed"
	pod.Spec.InitContainers[1].Env[0].Value = "changed"
	if plan.MatchManifest(fixture.launch) != nil || plan.ExpectedPod().Spec.Containers[0].Image == "changed" ||
		plan.ExpectedPod().Spec.InitContainers[1].Env[0].Value == "changed" {
		t.Fatal("input/output mutation changed the retained plan")
	}
	if _, err := fixture.verifier.Verify(plan.RegistryBinding()); err != nil {
		t.Fatal("returned binding aliases the retained signature")
	}
	for _, empty := range []*RuntimeLaunchPlan{nil, {}} {
		if empty.RegistryBinding() != nil || empty.ExpectedPod() != nil || empty.MatchManifest(fixture.launch) == nil {
			t.Fatal("unverified plan yielded trusted configuration")
		}
	}
}

func TestRuntimeLaunchPlanRejectsUnboundHistory(t *testing.T) {
	for _, scenario := range []string{"no-verifier", "no-binding", "unsigned", "tampered", "different-node", "claim-node", "worker", "member",
		"worker-epoch", "member-epoch", "bundle-digest", "noncanonical", "duplicate-key", "unknown-field", "invalid-bundle", "oversized"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := runtimeLaunchFixture(t)
			binding, verifier, wire, node := fixture.binding, fixture.verifier, fixture.wire, "cpu-node"
			signedMutation := false
			switch scenario {
			case "no-verifier":
				verifier = nil
			case "no-binding":
				binding = nil
			case "unsigned":
				binding.Signature = nil
			case "tampered":
				binding.Pair.RuntimeScope[0] ^= 1
			case "different-node":
				node = "other-node"
			case "claim-node":
				binding.Claim.NodeIdentity, node, signedMutation = "other-node", "other-node", true
			case "worker":
				binding.Claim.WorkerInstanceId, signedMutation = uuid.NewString(), true
			case "member":
				binding.Claim.WorkerMemberId, signedMutation = uuid.NewString(), true
			case "worker-epoch":
				binding.Claim.WorkerInstanceEpoch++
				signedMutation = true
			case "member-epoch":
				binding.Claim.WorkerMemberEpoch++
				signedMutation = true
			case "bundle-digest":
				binding.Claim.BundleDigest[0] ^= 1
				signedMutation = true
			case "noncanonical", "duplicate-key", "unknown-field", "invalid-bundle":
				switch scenario {
				case "noncanonical":
					wire = append(wire, '\n')
				case "duplicate-key":
					wire = bytes.Replace(wire, []byte(`"schema":`), []byte(`"schema":"duplicate","schema":`), 1)
				case "unknown-field":
					wire = bytes.Replace(wire, []byte(`"schema":`), []byte(`"unknown":true,"schema":`), 1)
				case "invalid-bundle":
					wire = []byte(`{"schema":"vela.worker-bundle-actuation/v2","bundle":{}}`)
				}
				digest := sha256.Sum256(wire)
				binding.Claim.BundleDigest, signedMutation = digest[:], true
			case "oversized":
				wire = make([]byte, fleet.MaximumWorkerBootstrapManifestBytes+1)
			}
			if signedMutation {
				binding.SigningKeyId, binding.Signature = "", nil
				var err error
				binding, err = fixture.signer.Sign(binding)
				if err != nil {
					t.Fatal(err)
				}
			}
			if plan, err := VerifyRuntimeLaunchPlan(node, verifier, binding, wire); !errors.Is(err, ErrRuntimeLaunchPlan) || plan != nil {
				t.Fatalf("unbound plan accepted: %v", err)
			}
		})
	}
}

func TestRuntimeLaunchPlanPreservesMemberAndAUXTopology(t *testing.T) {
	for _, shape := range []string{"aux", "llm"} {
		t.Run(shape, func(t *testing.T) {
			fixture := runtimeLaunchFixture(t)
			worker := &fixture.bundle.WorkerInstances[0]
			worker.Role = shape
			worker.Members[0].ResourceClass = "GPU"
			worker.Members[0].DeviceConstraints[0].ResourceClass = "GPU"
			worker.Members[0].DeviceConstraints[0].GPUUUID = "GPU-" + uuid.NewString()
			worker.Members[0].DeviceConstraints[0].PCIBDF = "0000:41:00.0"
			index := 0
			if shape == "aux" {
				worker.SharedSlotException = "H3_AUX_ENCODER_VAE"
				worker.ModelRuntimes[0].Component = "ENCODER"
				vae := worker.ModelRuntimes[0]
				vae.Component, vae.RuntimeIdentity = "VAE_DECODER", "vae-test"
				vae.ModelResidencyID, vae.StageProfileRevisionID, vae.CapacityPoolID = uuid.New(), uuid.New(), uuid.New()
				vae.Command = []string{"/nonexistent-vae"}
				worker.ModelRuntimes = append(worker.ModelRuntimes, vae)
			} else {
				worker.ModelRuntimes[0].Component = "LLM"
				fixture.bundle.StageWorkerMemberPKISecret = "member-pki"
				other := worker.Members[0]
				other.ID, other.Key, other.NodeIdentity, other.MemberEpoch = uuid.New(), "member-1", "cpu-node-b", 2
				identity := sha256.Sum256([]byte("spiffe://vela.internal/stage-worker/" + other.ID.String()))
				other.IdentityDigest, other.DeviceSubsetDigest = hex.EncodeToString(identity[:]), strings.Repeat("e", 64)
				other.DeviceConstraints = append([]fleetcontroller.DeviceConstraint(nil), other.DeviceConstraints...)
				other.DeviceConstraints[0].DeviceID, other.DeviceConstraints[0].GPUUUID = uuid.New(), "GPU-"+uuid.NewString()
				worker.Members = append(worker.Members, other)
				index = 1
			}
			fixture.bind(t, 0, index)
			node := worker.Members[index].NodeIdentity
			plan, err := VerifyRuntimeLaunchPlan(node, fixture.verifier, fixture.binding, fixture.wire)
			if err != nil || plan.MatchManifest(fixture.launch) != nil || plan.ExpectedPod().Spec.NodeSelector[corev1.LabelHostname] != node ||
				len(fixture.launch.Members) != len(worker.Members) || len(fixture.launch.Runtimes) != len(worker.ModelRuntimes) {
				t.Fatalf("approved topology was collapsed or changed: %v", err)
			}
			if shape == "aux" {
				fixture.launch.Runtimes[1].Command = []string{"/changed-vae"}
			} else {
				other, err := fleetcontroller.WorkerMemberLaunchManifest(fixture.bundle, worker.ID, worker.Members[0].ID)
				if err != nil {
					t.Fatal(err)
				}
				fixture.launch = other
			}
			if plan.MatchManifest(fixture.launch) == nil {
				t.Fatal("AUX second runtime or sibling member bypassed the plan")
			}
		})
	}
}

type runtimeLaunchTestFixture struct {
	bundle   fleetcontroller.WorkerBundleActuation
	wire     []byte
	launch   modelruntime.LaunchManifest
	binding  *velav1.WorkerBootstrapBinding
	signer   *journalbinding.Signer
	verifier *journalbinding.Verifier
}

func runtimeLaunchFixture(t *testing.T) runtimeLaunchTestFixture {
	t.Helper()
	workerID, memberID, poolID := uuid.New(), uuid.New(), uuid.New()
	memberDigest := sha256.Sum256([]byte("spiffe://vela.internal/stage-worker/" + memberID.String()))
	image := "registry.example/vela@sha256:" + strings.Repeat("a", 64)
	bundle := fleetcontroller.WorkerBundleActuation{SchemaVersion: 2, PlanRevisionID: uuid.New(), WorkerBundleID: uuid.New(), Namespace: "vela-system",
		InitImage: image, StageWorkerAgentImage: image, RuntimeImage: image, StageWorkerConfigMap: "worker-config",
		ModelRuntimeVerifierConfigMap: "verifier", StageWorkerControlTLSSecret: "control-tls", StageWorkerAuthoritySecret: "authority",
		ArtifactStoreCredentialsSecret: "artifact", ArtifactStoreCASecret: "artifact-ca",
		WorkerInstances: []fleetcontroller.WorkerInstanceActuation{{ID: workerID, InstanceEpoch: 1, WorkerProfileRevisionID: uuid.New(),
			CapacityPoolID: poolID, Role: "cpu-thumbnail", CapacitySlots: 1, DeviceSetDigest: strings.Repeat("b", 64), MembershipDigest: strings.Repeat("c", 64),
			ModelRuntimes: []fleetcontroller.ModelRuntimeProcess{{ModelResidencyID: uuid.New(), CapacityPoolID: poolID, StageProfileRevisionID: uuid.New(),
				ModelRuntimeEpochFloor: 1, Component: "CPU_MEDIA", ModelComponentRevision: "cpu-mock-v1", RuntimeIdentity: "cpu-runtime",
				Command: []string{"/nonexistent-mock-backend"}, InitializationTimeout: "1s", ShutdownTimeout: "1s"}},
			Members: []fleetcontroller.WorkerMemberActuation{{ID: memberID, MemberEpoch: 1, Key: "member-0", NodeIdentity: "cpu-node",
				ResourceClass: "CPU", DeviceCount: 1, IdentityDigest: hex.EncodeToString(memberDigest[:]), DeviceSubsetDigest: strings.Repeat("d", 64),
				DeviceConstraints: []fleetcontroller.DeviceConstraint{{DeviceID: uuid.New(), DeviceEpoch: 1, ResourceClass: "CPU"}}}},
		}}}
	seed := bytes.Repeat([]byte{71}, ed25519.SeedSize)
	signer, err := journalbinding.NewSigner("registry-launch-test", seed)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := journalbinding.NewVerifier(map[string][]byte{"registry-launch-test": ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)})
	if err != nil {
		t.Fatal(err)
	}
	fixture := runtimeLaunchTestFixture{bundle: bundle, signer: signer, verifier: verifier}
	fixture.bind(t, 0, 0)
	return fixture
}

func (fixture *runtimeLaunchTestFixture) bind(t *testing.T, workerIndex, memberIndex int) {
	t.Helper()
	var err error
	fixture.bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(fixture.bundle)
	if err != nil {
		t.Fatal(err)
	}
	fixture.wire, err = fleetcontroller.WorkerBundleActuationManifest(fixture.bundle)
	if err != nil {
		t.Fatal(err)
	}
	worker := fixture.bundle.WorkerInstances[workerIndex]
	member := worker.Members[memberIndex]
	fixture.launch, err = fleetcontroller.WorkerMemberLaunchManifest(fixture.bundle, worker.ID, member.ID)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(fixture.wire)
	request := uuid.NewString()
	now := time.Now().UTC()
	fixture.binding, err = fixture.signer.Sign(&velav1.WorkerBootstrapBinding{SchemaVersion: journalbinding.SchemaVersion,
		Claim: &velav1.WorkerBootstrapClaim{RequestId: request, WorkerInstanceId: worker.ID.String(), WorkerInstanceEpoch: worker.InstanceEpoch,
			WorkerMemberId: member.ID.String(), WorkerMemberEpoch: member.MemberEpoch, NodeIdentity: member.NodeIdentity,
			BundleDigest: digest[:], ClaimedAt: timestamppb.New(now), ActorIdentity: "node/launch-test"},
		Pair: &velav1.WorkerBootstrapJournalPair{RequestId: request, WorkerJournalId: uuid.NewString(), RuntimeJournalId: uuid.NewString(),
			WorkerScope: bytes.Repeat([]byte{1}, 32), RuntimeScope: bytes.Repeat([]byte{2}, 32), RecordedAt: timestamppb.New(now), ActorIdentity: "node/launch-test"}})
	if err != nil {
		t.Fatal(err)
	}
}

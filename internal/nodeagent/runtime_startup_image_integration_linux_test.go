//go:build integration && linux

package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	contentapi "github.com/containerd/containerd/api/services/content/v1"
	"github.com/google/uuid"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func verifyRuntimeStartupImageReservation(t *testing.T, fixture *containerdProcessFixture, observer *RuntimeContainerObserver, image RuntimeImageTarget) {
	t.Helper()
	for _, fault := range []string{"none", "node", "binding", "journal", "scope", "launch", "incarnation", "manifest-only", "missing-images", "policy", "substituted-executable", "before-fleet-task-change", "after-fleet-task-change", "fleet-loss", "image-observer-closed", "before-fleet-journal-close", "after-fleet-journal-close"} {
		t.Run(fault, func(t *testing.T) {
			plan, owner, request := startupImagePlanFixture(t, image)
			switch fault {
			case "node":
				request.NodeIdentity = "other-node"
			case "binding":
				request.RegistryBindingDigest[0] ^= 1
			case "journal":
				request.JournalID = uuid.New()
			case "scope":
				request.JournalScope[0] ^= 1
			case "launch":
				request.LaunchDigest[0] ^= 1
			case "incarnation":
				request.IncarnationID = uuid.New()
			}
			payload, err := modelruntime.EncodeBackendStartupRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			if fault == "manifest-only" {
				payload = plan.manifest
			}
			client := runtimev1.NewRuntimeServiceClient(fixture.connection)
			target, listener := fixture.createCRICallerPayload(t, client, fault, plan, payload)
			if _, err := client.StartContainer(t.Context(), &runtimev1.StartContainerRequest{ContainerId: target.ContainerID}); err != nil {
				t.Fatal(err)
			}
			if err := listener.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			connection, err := listener.AcceptUnix()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = connection.Close() }()
			caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: plan.uid, GID: plan.gid})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = caller.Close() }()
			pod := plan.ExpectedPod()
			pod.UID, pod.ResourceVersion, pod.Spec.NodeName = types.UID(target.PodUID.String()), "1", "cpu-node"
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: target.ContainerName, RestartCount: int32(target.ContainerAttempt), ContainerID: "containerd://" + target.ContainerID, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
			pods := &runtimeLaunchPodFixture{pod: *pod, key: fleetcontroller.ResourceKey{Namespace: pod.Namespace, Name: pod.Name}}
			images, err := DialRuntimeImageObserver(t.Context(), RuntimeImageObserverConfig{RuntimeContainerObserverConfig: RuntimeContainerObserverConfig{SocketPath: fixture.socket, NodeIdentity: "cpu-node"}, Namespace: "k8s.io", Snapshotter: "native"})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = images.Close() }()
			if fault == "before-fleet-journal-close" || fault == "after-fleet-journal-close" {
				boundary := 2
				if fault == "after-fleet-journal-close" {
					boundary = 3
				}
				images.content = &startupImageContentBoundary{ContentClient: images.content, manifest: image.ManifestDigest, at: boundary, run: func() {
					if err := owner.Close(); err != nil {
						t.Fatal(err)
					}
				}}
			}
			imageConfig := RuntimeStartupImageConfig{Images: images, StateDirectory: filepath.Join(fixture.root, "state"), RuntimePolicy: runtimeTaskPolicyFixture()}
			if fault == "missing-images" {
				imageConfig.Images = nil
			}
			if fault == "policy" {
				imageConfig.RuntimePolicy.RuntimeBinaryPath = "/unapproved/runc"
			}
			ledger, directory := startupTestLedger(t)
			registry := &startupReservationRegistryFixture{node: "cpu-node", actor: plan.binding.Claim.ActorIdentity}
			config := RuntimeStartupReservationConfig{Plan: plan, Pods: pods, Observer: observer, Caller: caller, Journal: owner, Registry: registry}
			changeTask := func() {
				t.Helper()
				path := filepath.Join(imageConfig.StateDirectory, "io.containerd.runtime.v2.task", "k8s.io", target.ContainerID, "config.json")
				wire, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var task specs.Spec
				if err := json.Unmarshal(wire, &task); err != nil {
					t.Fatal(err)
				}
				// This does not change argv/executable. Detect mutation of other
				// retained task bytes, without claiming approval of their values.
				task.Process.Env = append(task.Process.Env, "CHANGED_DURING_RESERVATION=1")
				changed, err := json.Marshal(task)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, changed, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "before-fleet-task-change" {
				ledger.boundary = func(phase string) error {
					if phase == "after-sync" {
						changeTask()
					}
					return nil
				}
			}
			registry.reserve = func(_ context.Context, sent fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
				if sent.RuntimeJournalID != request.JournalID || sent.IncarnationID != request.IncarnationID || len(sent.Epochs) != 1 || sent.Epochs[0].ModelRuntimeEpoch != 2 {
					t.Fatal("reservation did not bind original request to Node-held epoch routes")
				}
				switch fault {
				case "fleet-loss":
					return fleet.RuntimeStartupReservation{}, errors.New("fixture committed response lost")
				case "after-fleet-task-change":
					changeTask()
				case "image-observer-closed":
					if err := images.Close(); err != nil {
						t.Fatal(err)
					}
				}
				return fleet.RuntimeStartupReservation{RuntimeStartupRequest: sent, Fresh: true, ReservedAt: time.Now().UTC()}, nil
			}
			result, err := ledger.ReserveImageRemote(t.Context(), config, imageConfig)
			wantCalls, wantIntent := 0, 0
			switch fault {
			case "none", "after-fleet-task-change", "fleet-loss", "image-observer-closed", "after-fleet-journal-close":
				wantCalls, wantIntent = 1, 1
			case "before-fleet-task-change", "before-fleet-journal-close":
				wantIntent = 1
			}
			if registry.calls != wantCalls || len(ledger.starts) != wantIntent {
				t.Fatalf("wrong authority side effects: calls=%d intent=%d err=%v", registry.calls, len(ledger.starts), err)
			}
			if fault == "none" {
				if err != nil || result.JournalID != request.JournalID {
					t.Fatalf("valid image-bound startup did not reserve: %+v %v", result, err)
				}
			} else if err == nil || result != (RuntimeStartupReservationRecord{}) || len(ledger.reservations) != 0 {
				t.Fatalf("failed observation returned a receipt: %+v %v", result, err)
			}
			if wantIntent != 0 {
				if _, err := ledger.ReserveImageRemote(t.Context(), config, imageConfig); err == nil || registry.calls != wantCalls {
					t.Fatalf("live retry consumed another reservation: %v", err)
				}
				if err := ledger.Close(); err != nil {
					t.Fatal(err)
				}
				recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = recovered.Close() }()
				history, err := recovered.InspectReservation(t.Context(), request.JournalID)
				if fault == "none" {
					if err != nil || history != result {
						t.Fatalf("recovery lost receipt: %v", err)
					}
				} else if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("recovery manufactured receipt: %v", err)
				}
				if _, err := recovered.ReserveImageRemote(t.Context(), config, imageConfig); err == nil || registry.calls != wantCalls {
					t.Fatalf("restart consumed another reservation: %v", err)
				}
				if _, err := recovered.RecordExit(t.Context(), request.JournalID); !errors.Is(err, ErrRuntimeNamespaceOwnerLost) {
					t.Fatalf("restart recreated original owner handle: %v", err)
				}
			}
			criFixture := *fixture
			criFixture.ctx = metadata.NewOutgoingContext(t.Context(), metadata.Pairs("containerd-namespace", "k8s.io"))
			assertRuntimeImageResourcesReleased(t, &criFixture)
			t.Logf("actual CRI startup envelope + root journal + image observation: fault=%s Fleet fixture calls=%d durable intents=%d", fault, registry.calls, wantIntent)
		})
	}
}

// Keep real daemon content and measurement; interrupt journal custody precisely
// inside the second or third image observation, after the preceding checks.
type startupImageContentBoundary struct {
	contentapi.ContentClient
	manifest  string
	at, calls int
	run       func()
}

func (client *startupImageContentBoundary) Info(ctx context.Context, request *contentapi.InfoRequest, options ...grpc.CallOption) (*contentapi.InfoResponse, error) {
	if request.Digest == client.manifest {
		client.calls++
		if client.calls == client.at {
			client.run()
		}
	}
	return client.ContentClient.Info(ctx, request, options...)
}

func startupImagePlanFixture(t *testing.T, image RuntimeImageTarget) (*RuntimeLaunchPlan, *modelruntime.ExecutionJournalOwner, modelruntime.BackendStartupRequest) {
	t.Helper()
	launch := runtimeLaunchFixture(t)
	launch.bundle.RuntimeImage = "docker.io/vela/caller-runtime@" + image.ManifestDigest
	launch.bind(t, 0, 0)
	validator, err := stageauthority.NewValidator(map[string][]byte{"authority": make([]byte, 32)}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := modelruntime.RemoteStartupBindings(launch.launch)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	owner, err := modelruntime.OpenExecutionJournalOwner(modelruntime.ExecutionJournalOwnerConfig{Manifest: launch.launch, Validator: validator,
		State: modelruntime.ExecutionFloorStateConfig{Directory: directory, Initialize: true}, Routes: routes, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	startup, err := owner.RecordBackendStartupIntent(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	journal, err := owner.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	launch.binding.Pair.RuntimeJournalId, launch.binding.Pair.RuntimeScope = journal.JournalID.String(), journal.Scope[:]
	launch.binding.Signature, launch.binding.SigningKeyId = nil, ""
	launch.binding, err = launch.signer.Sign(launch.binding)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := VerifyRuntimeLaunchPlan("cpu-node", launch.verifier, launch.binding, launch.wire)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := proto.MarshalOptions{Deterministic: true}.Marshal(plan.binding)
	if err != nil {
		t.Fatal(err)
	}
	return plan, owner, modelruntime.BackendStartupRequest{SchemaVersion: 1, NodeIdentity: "cpu-node", RegistryBindingDigest: sha256.Sum256(binding),
		JournalID: journal.JournalID, JournalScope: journal.Scope, IncarnationID: startup.IncarnationID, LaunchDigest: startup.LaunchDigest}
}

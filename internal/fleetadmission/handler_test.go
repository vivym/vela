package fleetadmission

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestProtectedPodDeleteRejectsNonFleetActorBeforeAuthorization(t *testing.T) {
	authorizer := &recordingAuthorizer{}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	review := protectedPodDeleteReview(
		"admission-delete-non-fleet",
		"system:serviceaccount:argocd:argocd-application-controller",
	)
	response := serveAdmission(t, handler, review)
	if response.Allowed {
		t.Fatalf("non-Fleet protected Pod delete was allowed: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("non-Fleet delete reached authorizer: %#v", authorizer.requests)
	}
}

func TestProtectedPodDeleteRequiresPersistedFleetAuthorization(t *testing.T) {
	const requestUID = "admission-delete-fleet"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	review := protectedPodDeleteReview(
		requestUID,
		"system:serviceaccount:vela-system:vela-fleet-controller",
	)
	response := serveAdmission(t, handler, review)
	if !response.Allowed || response.UID != requestUID {
		t.Fatalf("authorized Fleet delete response = %#v", response)
	}
	if len(authorizer.requests) != 1 {
		t.Fatalf("Fleet delete authorization calls = %d, want 1", len(authorizer.requests))
	}
	request := authorizer.requests[0]
	if request.RequestUID != requestUID ||
		request.ActorIdentity != "system:serviceaccount:vela-system:vela-fleet-controller" ||
		request.ResourceKind != fleet.ProtectedPod || request.Operation != fleet.MutationDelete ||
		request.KubernetesUID != "kubernetes-worker-pod-uid-1" ||
		request.Namespace != "vela-workers" || request.Name != "h3-worker-node-1" ||
		request.WorkerPoolID.String() != "00000000-0000-0000-0000-000000000005" ||
		request.WorkerID.String() != "23000000-0000-0000-0000-000000000041" ||
		request.WorkerEpoch != 20 || len(request.DrainOperationIDs) != 1 ||
		request.DrainOperationIDs[0].String() != "23000000-0000-0000-0000-000000000042" ||
		len(request.RequestDigest) != 32 {
		t.Fatalf("Fleet delete authorization request = %#v", request)
	}
}

func TestProtectedPodDeleteRejectsNilWorkerIdentityBeforeAuthorization(t *testing.T) {
	const requestUID = "admission-delete-nil-worker"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	review := bytes.Replace(
		protectedPodDeleteReview(
			requestUID,
			"system:serviceaccount:vela-system:vela-fleet-controller",
		),
		[]byte("23000000-0000-0000-0000-000000000041"),
		[]byte("00000000-0000-0000-0000-000000000000"),
		1,
	)
	response := serveAdmission(t, handler, review)
	if response.Allowed {
		t.Fatalf("nil protected Pod Worker identity was allowed: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("nil Worker identity reached authorizer: %#v", authorizer.requests)
	}
}

func TestProtectedResourceUpdateCannotRemoveFleetOwnershipLabels(t *testing.T) {
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: "admission-remove-owner-label", Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	response := serveAdmission(t, handler, protectedPodOwnershipRemovalReview())
	if response.Allowed {
		t.Fatalf("protected ownership-label removal was allowed: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("ownership-label removal reached authorizer: %#v", authorizer.requests)
	}
}

func TestProtectedPodFinalizerRemovalRequiresPersistedAuthorization(t *testing.T) {
	const requestUID = "admission-remove-finalizer"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	response := serveAdmission(t, handler, protectedPodFinalizerRemovalReview())
	if !response.Allowed {
		t.Fatalf("authorized finalizer removal was denied: %#v", response)
	}
	if len(authorizer.requests) != 1 ||
		authorizer.requests[0].Operation != fleet.MutationRemoveFinalizer ||
		authorizer.requests[0].ResourceKind != fleet.ProtectedPod {
		t.Fatalf("finalizer-removal authorization requests = %#v", authorizer.requests)
	}
}

func TestProtectedPodFinalizerRemovalRejectsAnyAdditionalMutation(t *testing.T) {
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: "admission-remove-finalizer", Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	review := bytes.Replace(
		protectedPodFinalizerRemovalReview(),
		[]byte(`"finalizers":[]`),
		[]byte(`"finalizers":[],"spec":{"nodeName":"other-node"}`),
		1,
	)
	response := serveAdmission(t, handler, review)
	if response.Allowed {
		t.Fatalf("finalizer removal with PodSpec mutation was allowed: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("combined finalizer and PodSpec mutation reached authorizer: %#v", authorizer.requests)
	}
}

func TestFleetMayAttachDrainIdentityWithoutDestructiveMutation(t *testing.T) {
	authorizer := &recordingAuthorizer{}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	response := serveAdmission(t, handler, protectedPodDrainIntentReview(false))
	if !response.Allowed {
		t.Fatalf("drain-identity-only update was denied: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("non-destructive drain identity update reached authorizer: %#v", authorizer.requests)
	}
}

func TestFleetCannotAttachDrainIdentityWithAnotherMutation(t *testing.T) {
	authorizer := &recordingAuthorizer{}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	response := serveAdmission(t, handler, protectedPodDrainIntentReview(true))
	if response.Allowed {
		t.Fatalf("drain identity update with another mutation was allowed: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("combined drain identity update reached authorizer: %#v", authorizer.requests)
	}
}

func TestProtectedDaemonSetImagePatchRequiresCompletePoolAuthorization(t *testing.T) {
	const requestUID = "admission-daemonset-image"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	response := serveAdmission(t, handler, protectedDaemonSetImageReview())
	if !response.Allowed {
		t.Fatalf("authorized DaemonSet image patch was denied: %#v", response)
	}
	if len(authorizer.requests) != 1 {
		t.Fatalf("DaemonSet image authorization calls = %d, want 1", len(authorizer.requests))
	}
	request := authorizer.requests[0]
	if request.ResourceKind != fleet.ProtectedDaemonSet ||
		request.Operation != fleet.MutationPatchImage || request.WorkerID != [16]byte{} ||
		request.WorkerEpoch != 0 || len(request.DrainOperationIDs) != 2 ||
		request.DrainOperationIDs[0].String() != "23000000-0000-0000-0000-000000000045" ||
		request.DrainOperationIDs[1].String() != "23000000-0000-0000-0000-000000000046" {
		t.Fatalf("DaemonSet image authorization request = %#v", request)
	}
}

func TestProtectedDaemonSetImagePatchRejectsUnpinnedImageBeforeAuthorization(t *testing.T) {
	const requestUID = "admission-daemonset-unpinned-image"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	response := serveAdmission(t, handler, protectedDaemonSetImageReviewWith(
		requestUID,
		"ghcr.io/vivym/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"ghcr.io/vivym/worker:latest",
	))
	if response.Allowed {
		t.Fatalf("unpinned protected DaemonSet image was allowed: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("unpinned image patch reached authorizer: %#v", authorizer.requests)
	}
}

func TestProtectedDaemonSetImagePatchRejectsAnyAdditionalPodSpecMutation(t *testing.T) {
	const (
		requestUID = "admission-daemonset-image-with-privilege"
		oldImage   = "ghcr.io/vivym/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		newImage   = "ghcr.io/vivym/worker@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	oldObject := protectedDaemonSetObject(oldImage)
	newObject := bytes.Replace(
		protectedDaemonSetObject(newImage),
		[]byte(`"name":"h3-runner","image":"`+newImage+`"`),
		[]byte(`"name":"h3-runner","image":"`+newImage+`","securityContext":{"privileged":true}`),
		1,
	)
	response := serveAdmission(t, handler, protectedDaemonSetUpdateReview(
		requestUID,
		"system:serviceaccount:vela-system:vela-fleet-controller",
		oldObject,
		newObject,
	))
	if response.Allowed {
		t.Fatalf("image patch with privileged container mutation was allowed: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("combined image and privilege mutation reached authorizer: %#v", authorizer.requests)
	}
}

func TestProtectedDaemonSetSelectorPatchRequiresCompletePoolAuthorization(t *testing.T) {
	const requestUID = "admission-daemonset-selector"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	oldObject := protectedDaemonSetObject(
		"ghcr.io/vivym/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	newObject := bytes.ReplaceAll(
		oldObject,
		[]byte("vela-h3-worker"),
		[]byte("vela-h3-worker-revision-2"),
	)
	response := serveAdmission(t, handler, protectedDaemonSetUpdateReview(
		requestUID,
		"system:serviceaccount:vela-system:vela-fleet-controller",
		oldObject,
		newObject,
	))
	if !response.Allowed {
		t.Fatalf("authorized DaemonSet selector patch was denied: %#v", response)
	}
	if len(authorizer.requests) != 1 ||
		authorizer.requests[0].Operation != fleet.MutationPatchSelector ||
		len(authorizer.requests[0].DrainOperationIDs) != 2 {
		t.Fatalf("DaemonSet selector authorization requests = %#v", authorizer.requests)
	}
}

func TestProtectedDaemonSetSelectorPatchRejectsAnyAdditionalPodSpecMutation(t *testing.T) {
	const requestUID = "admission-daemonset-selector-with-host-network"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	oldObject := protectedDaemonSetObject(
		"ghcr.io/vivym/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	newObject := bytes.ReplaceAll(oldObject, []byte("vela-h3-worker"), []byte("vela-h3-worker-revision-2"))
	newObject = bytes.Replace(
		newObject,
		[]byte(`"schedulingGates":[`),
		[]byte(`"hostNetwork":true,"schedulingGates":[`),
		1,
	)
	if !bytes.Contains(newObject, []byte(`"hostNetwork":true`)) {
		t.Fatal("test setup did not add the host network mutation")
	}
	response := serveAdmission(t, handler, protectedDaemonSetUpdateReview(
		requestUID,
		"system:serviceaccount:vela-system:vela-fleet-controller",
		oldObject,
		newObject,
	))
	if response.Allowed {
		t.Fatalf("selector patch with host network mutation was allowed: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("combined selector and host network mutation reached authorizer: %#v", authorizer.requests)
	}
}

func TestProtectedDaemonSetDeleteRequiresCompletePoolAuthorization(t *testing.T) {
	const requestUID = "admission-daemonset-delete"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	response := serveAdmission(t, handler, protectedDaemonSetDeleteReview(requestUID))
	if !response.Allowed {
		t.Fatalf("authorized DaemonSet delete was denied: %#v", response)
	}
	if len(authorizer.requests) != 1 ||
		authorizer.requests[0].ResourceKind != fleet.ProtectedDaemonSet ||
		authorizer.requests[0].Operation != fleet.MutationDelete ||
		len(authorizer.requests[0].DrainOperationIDs) != 2 {
		t.Fatalf("DaemonSet delete authorization requests = %#v", authorizer.requests)
	}
}

func TestProtectedDaemonSetFinalizerRemovalRequiresCompletePoolAuthorization(t *testing.T) {
	const requestUID = "admission-daemonset-remove-finalizer"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	oldObject := protectedDaemonSetObject(
		"ghcr.io/vivym/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	newObject := bytes.Replace(
		oldObject,
		[]byte(`"finalizers":["fleet.vela.ai/drain-protection"]`),
		[]byte(`"finalizers":[]`),
		1,
	)
	response := serveAdmission(t, handler, protectedDaemonSetUpdateReview(
		requestUID,
		"system:serviceaccount:vela-system:vela-fleet-controller",
		oldObject,
		newObject,
	))
	if !response.Allowed {
		t.Fatalf("authorized DaemonSet finalizer removal was denied: %#v", response)
	}
	if len(authorizer.requests) != 1 ||
		authorizer.requests[0].Operation != fleet.MutationRemoveFinalizer {
		t.Fatalf("DaemonSet finalizer authorization requests = %#v", authorizer.requests)
	}
}

func TestProtectedDaemonSetFinalizerRemovalRejectsAnyAdditionalMutation(t *testing.T) {
	const requestUID = "admission-daemonset-remove-finalizer-with-host-network"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	oldObject := protectedDaemonSetObject(
		"ghcr.io/vivym/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	newObject := bytes.Replace(
		oldObject,
		[]byte(`"finalizers":["fleet.vela.ai/drain-protection"]`),
		[]byte(`"finalizers":[]`),
		1,
	)
	newObject = bytes.Replace(
		newObject,
		[]byte(`"schedulingGates":[`),
		[]byte(`"hostNetwork":true,"schedulingGates":[`),
		1,
	)
	response := serveAdmission(t, handler, protectedDaemonSetUpdateReview(
		requestUID,
		"system:serviceaccount:vela-system:vela-fleet-controller",
		oldObject,
		newObject,
	))
	if response.Allowed {
		t.Fatalf("finalizer removal with host network mutation was allowed: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("combined finalizer and host network mutation reached authorizer: %#v", authorizer.requests)
	}
}

func TestProtectedDaemonSetRejectsCombinedSelectorAndImagePatch(t *testing.T) {
	const requestUID = "admission-daemonset-combined-patch"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	oldObject := protectedDaemonSetObject(
		"ghcr.io/vivym/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	newObject := protectedDaemonSetObject(
		"ghcr.io/vivym/worker@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	)
	newObject = bytes.ReplaceAll(
		newObject,
		[]byte("vela-h3-worker"),
		[]byte("vela-h3-worker-revision-2"),
	)
	response := serveAdmission(t, handler, protectedDaemonSetUpdateReview(
		requestUID,
		"system:serviceaccount:vela-system:vela-fleet-controller",
		oldObject,
		newObject,
	))
	if response.Allowed {
		t.Fatalf("combined DaemonSet selector and image patch was allowed: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("combined DaemonSet patch reached authorizer: %#v", authorizer.requests)
	}
}

func TestProtectedDaemonSetRejectsCombinedNodeSelectorAndImagePatch(t *testing.T) {
	const requestUID = "admission-daemonset-combined-node-selector-patch"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	oldObject := protectedDaemonSetObject(
		"ghcr.io/vivym/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	newObject := protectedDaemonSetObject(
		"ghcr.io/vivym/worker@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	)
	newObject = bytes.Replace(
		newObject,
		[]byte(`"vela.ai/worker-pool":"launch"`),
		[]byte(`"vela.ai/worker-pool":"canary"`),
		1,
	)
	response := serveAdmission(t, handler, protectedDaemonSetUpdateReview(
		requestUID,
		"system:serviceaccount:vela-system:vela-fleet-controller",
		oldObject,
		newObject,
	))
	if response.Allowed {
		t.Fatalf("combined DaemonSet node selector and image patch was allowed: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("combined DaemonSet node selector patch reached authorizer: %#v", authorizer.requests)
	}
}

func TestFleetMayCreateExactProtectedDaemonSetWithoutDrainAuthorization(t *testing.T) {
	authorizer := &recordingAuthorizer{}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	response := serveAdmission(t, handler, protectedDaemonSetCreateReview())
	if !response.Allowed {
		t.Fatalf("exact protected DaemonSet create was denied: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("protected DaemonSet create reached drain authorizer: %#v", authorizer.requests)
	}
	withoutGate := bytes.Replace(
		protectedDaemonSetCreateReview(),
		[]byte(`"schedulingGates":[{"name":"fleet.vela.ai/identity-binding"}],`),
		nil,
		1,
	)
	if response := serveAdmission(t, handler, withoutGate); response.Allowed {
		t.Fatalf("protected DaemonSet without identity scheduling gate was allowed: %#v", response)
	}
}

func TestFleetMayCreateProtectedDaemonSetWhenSelectorIsTemplateLabelSubset(t *testing.T) {
	authorizer := &recordingAuthorizer{}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	review := bytes.Replace(
		protectedDaemonSetCreateReview(),
		[]byte(`"metadata":{"labels":{"app.kubernetes.io/name":"vela-h3-worker"}}`),
		[]byte(`"metadata":{"labels":{"app.kubernetes.io/name":"vela-h3-worker","vela.ai/fleet-protected":"true","vela.ai/fleet-revision":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}}`),
		1,
	)
	if !bytes.Contains(review, []byte(`"vela.ai/fleet-revision":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"`)) {
		t.Fatal("test setup did not add materialized template labels")
	}
	response := serveAdmission(t, handler, review)
	if !response.Allowed {
		t.Fatalf("protected DaemonSet with selector subset was denied: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("protected DaemonSet create reached drain authorizer: %#v", authorizer.requests)
	}
}

func TestProtectedWorkerPoolDeleteRequiresCompletePoolAuthorization(t *testing.T) {
	const requestUID = "admission-worker-pool-delete"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	response := serveAdmission(t, handler, protectedWorkerPoolDeleteReview())
	if !response.Allowed {
		t.Fatalf("authorized WorkerPool delete was denied: %#v", response)
	}
	if len(authorizer.requests) != 1 {
		t.Fatalf("WorkerPool delete authorization calls = %d, want 1", len(authorizer.requests))
	}
	request := authorizer.requests[0]
	if request.ResourceKind != fleet.ProtectedWorkerPool || request.Operation != fleet.MutationDelete ||
		request.WorkerID != [16]byte{} || request.WorkerEpoch != 0 ||
		len(request.DrainOperationIDs) != 2 {
		t.Fatalf("WorkerPool delete authorization request = %#v", request)
	}
}

func TestProtectedWorkerPoolFinalizerRemovalRequiresCompletePoolAuthorization(t *testing.T) {
	const requestUID = "admission-worker-pool-remove-finalizer"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	oldObject := protectedWorkerPoolObject(true)
	newObject := bytes.Replace(
		oldObject,
		[]byte(`"finalizers":["fleet.vela.ai/drain-protection"]`),
		[]byte(`"finalizers":[]`),
		1,
	)
	response := serveAdmission(t, handler, protectedWorkerPoolUpdateReview(
		requestUID,
		"system:serviceaccount:vela-system:vela-fleet-controller",
		oldObject,
		newObject,
	))
	if !response.Allowed {
		t.Fatalf("authorized WorkerPool finalizer removal was denied: %#v", response)
	}
	if len(authorizer.requests) != 1 ||
		authorizer.requests[0].Operation != fleet.MutationRemoveFinalizer {
		t.Fatalf("WorkerPool finalizer authorization requests = %#v", authorizer.requests)
	}
}

func TestArgoCannotPruneProtectedWorkerPool(t *testing.T) {
	authorizer := &recordingAuthorizer{}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	review := bytes.Replace(
		protectedWorkerPoolDeleteReview(),
		[]byte("system:serviceaccount:vela-system:vela-fleet-controller"),
		[]byte("system:serviceaccount:argocd:argocd-application-controller"),
		1,
	)
	response := serveAdmission(t, handler, review)
	if response.Allowed {
		t.Fatalf("Argo prune of protected WorkerPool was allowed: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("Argo prune reached authorizer: %#v", authorizer.requests)
	}
}

func TestFleetMayCreateExactProtectedWorkerPoolWithoutDrainAuthorization(t *testing.T) {
	authorizer := &recordingAuthorizer{}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	response := serveAdmission(t, handler, protectedWorkerPoolCreateReview())
	if !response.Allowed {
		t.Fatalf("exact protected WorkerPool create was denied: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("protected WorkerPool create reached drain authorizer: %#v", authorizer.requests)
	}
}

func TestFleetCannotCreateProtectedWorkerPoolWithInvalidCapacityPolicy(t *testing.T) {
	handler, err := NewHandler(&recordingAuthorizer{}, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	review := bytes.Replace(
		protectedWorkerPoolCreateReview(),
		[]byte(`"workerLowWatermarkBytes":400`),
		[]byte(`"workerLowWatermarkBytes":800`),
		1,
	)
	response := serveAdmission(t, handler, review)
	if response.Allowed {
		t.Fatalf("protected WorkerPool with inverted capacity policy was allowed: %#v", response)
	}
}

func TestFleetCannotCreateProtectedWorkerPoolOutsidePlacementAndSelectorBounds(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "placement missing required field", mutate: func(object map[string]any) {
			placement := object["spec"].(map[string]any)["placements"].([]any)[0].(map[string]any)
			delete(placement, "workerControlTLSSecret")
		}},
		{name: "duplicate placement", mutate: func(object map[string]any) {
			spec := object["spec"].(map[string]any)
			placement := spec["placements"].([]any)[0]
			spec["placements"] = []any{placement, placement}
		}},
		{name: "more than 1024 placements", mutate: func(object map[string]any) {
			spec := object["spec"].(map[string]any)
			placement := spec["placements"].([]any)[0]
			placements := make([]any, 1025)
			for index := range placements {
				clone := maps.Clone(placement.(map[string]any))
				clone["nodeIdentity"] = fmt.Sprintf("h3-node-%d", index)
				clone["daemonSetName"] = fmt.Sprintf("h3-worker-%d", index)
				clone["workerRuntimeConfigMap"] = fmt.Sprintf("runtime-%d", index)
				clone["runnerProfilesConfigMap"] = fmt.Sprintf("profiles-%d", index)
				clone["runnerGPURolesConfigMap"] = fmt.Sprintf("gpu-roles-%d", index)
				clone["workerControlTLSSecret"] = fmt.Sprintf("worker-tls-%d", index)
				placements[index] = clone
			}
			spec["placements"] = placements
		}},
		{name: "placement label value exceeds 63 characters", mutate: func(object map[string]any) {
			placement := object["spec"].(map[string]any)["placements"].([]any)[0].(map[string]any)
			placement["nodeIdentity"] = strings.Repeat("a", 63) + ".b"
		}},
		{name: "placement contains template marker", mutate: func(object map[string]any) {
			placement := object["spec"].(map[string]any)["placements"].([]any)[0].(map[string]any)
			placement["workerRuntimeConfigMap"] = "worker-runtime-placeholder"
		}},
		{name: "invalid selector key", mutate: func(object map[string]any) {
			selector := object["spec"].(map[string]any)["nodeSelector"].(map[string]any)
			selector["bad key"] = "value"
		}},
		{name: "invalid selector value", mutate: func(object map[string]any) {
			selector := object["spec"].(map[string]any)["nodeSelector"].(map[string]any)
			selector["vela.ai/rack"] = "bad/value"
		}},
		{name: "explicit hostname selector", mutate: func(object map[string]any) {
			selector := object["spec"].(map[string]any)["nodeSelector"].(map[string]any)
			selector[corev1.LabelHostname] = ""
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, err := NewHandler(&recordingAuthorizer{}, Config{
				FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
			})
			if err != nil {
				t.Fatalf("create Fleet admission handler: %v", err)
			}
			var review map[string]any
			if err := json.Unmarshal(protectedWorkerPoolCreateReview(), &review); err != nil {
				t.Fatalf("decode WorkerPool admission fixture: %v", err)
			}
			object := review["request"].(map[string]any)["object"].(map[string]any)
			test.mutate(object)
			encoded, err := json.Marshal(review)
			if err != nil {
				t.Fatalf("encode WorkerPool admission fixture: %v", err)
			}
			response := serveAdmission(t, handler, encoded)
			if response.Allowed {
				t.Fatal("invalid protected WorkerPool was allowed")
			}
		})
	}
}

func TestDaemonSetControllerMayCreateExactProtectedWorkerPod(t *testing.T) {
	authorizer := &recordingAuthorizer{}
	validator := &recordingPodCreateValidator{}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername:         "system:serviceaccount:vela-system:vela-fleet-controller",
		PodControllerUsername: "system:kube-controller-manager",
		PodCreateValidator:    validator,
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	response := serveAdmission(t, handler, protectedPodCreateReview())
	if !response.Allowed {
		t.Fatalf("exact DaemonSet-owned Worker Pod create was denied: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("protected Worker Pod create reached drain authorizer: %#v", authorizer.requests)
	}
	if len(validator.pods) != 1 {
		t.Fatalf("authoritative Pod validation calls = %d, want 1", len(validator.pods))
	}
}

func TestDaemonSetControllerPodCreateUsesNativeAuthoritativeValidation(t *testing.T) {
	validator := &recordingPodCreateValidator{validate: func(pod corev1.Pod) error {
		if len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].SecurityContext == nil ||
			pod.Spec.Containers[0].SecurityContext.Privileged == nil ||
			!*pod.Spec.Containers[0].SecurityContext.Privileged {
			return errors.New("privileged PodSpec was not decoded")
		}
		return errors.New("protected Pod differs from authoritative DaemonSet template")
	}}
	handler, err := NewHandler(&recordingAuthorizer{}, Config{
		FleetUsername:         "system:serviceaccount:vela-system:vela-fleet-controller",
		PodControllerUsername: "system:kube-controller-manager",
		PodCreateValidator:    validator,
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	review := bytes.Replace(
		protectedPodCreateReview(),
		[]byte(`"name":"h3-runner",`),
		[]byte(`"name":"h3-runner","securityContext":{"privileged":true},`),
		1,
	)
	response := serveAdmission(t, handler, review)
	if response.Allowed {
		t.Fatalf("privileged counterfeit protected Pod was allowed: %#v", response)
	}
	if len(validator.pods) != 1 {
		t.Fatalf("authoritative Pod validation calls = %d, want 1", len(validator.pods))
	}
}

func TestActualMaterializedDaemonSetPodPassesThroughAdmission(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	resources, err := fleetcontroller.NewKubernetesResources(client, "vela-system")
	if err != nil {
		t.Fatalf("create Kubernetes Fleet resources: %v", err)
	}
	reconciler, err := fleetcontroller.NewReconciler(
		resources,
		&admissionDrainReader{},
		&admissionIdentityResolver{},
		&admissionReadinessStarter{},
		&admissionCapacityPolicyConfigurator{},
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}
	desired := admissionDesiredRevision()
	if _, err := reconciler.Reconcile(context.Background(), desired); err != nil {
		t.Fatalf("materialize protected DaemonSet: %v", err)
	}
	daemonSetResource := schema.GroupVersionResource{
		Group: "apps", Version: "v1", Resource: "daemonsets",
	}
	live, err := client.Resource(daemonSetResource).Namespace(desired.Namespace).Get(
		context.Background(), desired.Placements[0].DaemonSetName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get materialized DaemonSet: %v", err)
	}
	live.SetUID(types.UID("kubernetes-daemonset-uid-1"))
	live, err = client.Resource(daemonSetResource).Namespace(desired.Namespace).Update(
		context.Background(), live, metav1.UpdateOptions{},
	)
	if err != nil {
		t.Fatalf("assign materialized DaemonSet UID: %v", err)
	}
	var daemonSet appsv1.DaemonSet
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(live.Object, &daemonSet); err != nil {
		t.Fatalf("decode materialized DaemonSet: %v", err)
	}
	controller := true
	pod := corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: *daemonSet.Spec.Template.ObjectMeta.DeepCopy(),
		Spec:       *daemonSet.Spec.Template.Spec.DeepCopy(),
	}
	pod.Namespace = desired.Namespace
	pod.Name = "h3-worker-node-1"
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "DaemonSet", Name: daemonSet.Name,
		UID: daemonSet.UID, Controller: &controller,
	}}
	pod.Spec.Affinity = admissionTargetNodeAffinity("h3-node-01")
	rawPod, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("encode materialized Pod: %v", err)
	}
	rawReview, err := json.Marshal(admissionReview{
		APIVersion: "admission.k8s.io/v1",
		Kind:       "AdmissionReview",
		Request: &admissionRequest{
			UID: "admission-materialized-worker-pod", Operation: "CREATE",
			Kind:      groupVersionKind{Version: "v1", Kind: "Pod"},
			Namespace: pod.Namespace, Name: pod.Name,
			UserInfo: userInfo{Username: "system:kube-controller-manager"},
			Object:   rawPod,
		},
	})
	if err != nil {
		t.Fatalf("encode materialized Pod AdmissionReview: %v", err)
	}
	handler, err := NewHandler(&recordingAuthorizer{}, Config{
		FleetUsername:         "system:serviceaccount:vela-system:vela-fleet-controller",
		PodControllerUsername: "system:kube-controller-manager",
		PodCreateValidator:    resources,
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	response := serveAdmission(t, handler, rawReview)
	if !response.Allowed {
		t.Fatalf("actual materialized protected Pod was denied: %#v", response)
	}
}

func TestFleetMayBindExactWorkerIdentityAndRemoveSchedulingGate(t *testing.T) {
	authorizer := &recordingAuthorizer{}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername:         "system:serviceaccount:vela-system:vela-fleet-controller",
		PodControllerUsername: "system:kube-controller-manager",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	response := serveAdmission(t, handler, protectedPodIdentityBindingReview(
		"system:serviceaccount:vela-system:vela-fleet-controller",
		"h3-node-01",
		true,
		true,
		false,
	))
	if !response.Allowed {
		t.Fatalf("exact Worker identity binding was denied: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("non-destructive Worker identity binding reached authorizer: %#v", authorizer.requests)
	}
}

func TestWorkerIdentityBindingRejectsAnyOtherMutationOrActor(t *testing.T) {
	authorizer := &recordingAuthorizer{}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername:         "system:serviceaccount:vela-system:vela-fleet-controller",
		PodControllerUsername: "system:kube-controller-manager",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	tests := []struct {
		name            string
		username        string
		nodeIdentity    string
		includeIdentity bool
		removeGate      bool
		mutateSpec      bool
	}{
		{
			name: "non-Fleet actor", username: "system:kube-controller-manager",
			nodeIdentity: "h3-node-01", includeIdentity: true, removeGate: true,
		},
		{
			name: "target Node", username: "system:serviceaccount:vela-system:vela-fleet-controller",
			nodeIdentity: "h3-node-02", includeIdentity: true, removeGate: true,
		},
		{
			name: "gate retained", username: "system:serviceaccount:vela-system:vela-fleet-controller",
			nodeIdentity: "h3-node-01", includeIdentity: true,
		},
		{
			name: "identity absent", username: "system:serviceaccount:vela-system:vela-fleet-controller",
			nodeIdentity: "h3-node-01", removeGate: true,
		},
		{
			name: "combined Pod spec mutation", username: "system:serviceaccount:vela-system:vela-fleet-controller",
			nodeIdentity: "h3-node-01", includeIdentity: true, removeGate: true, mutateSpec: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := serveAdmission(t, handler, protectedPodIdentityBindingReview(
				test.username,
				test.nodeIdentity,
				test.includeIdentity,
				test.removeGate,
				test.mutateSpec,
			))
			if response.Allowed {
				t.Fatalf("invalid Worker identity binding was allowed: %#v", response)
			}
		})
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("invalid Worker identity binding reached authorizer: %#v", authorizer.requests)
	}
}

func TestProtectedWorkerPodCreateRejectsMalformedShape(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{
			name: "GPU request",
			mutate: func(review []byte) []byte {
				return bytes.Replace(review, []byte(`"nvidia.com/gpu":"8"`), []byte(`"nvidia.com/gpu":"7"`), 1)
			},
		},
		{
			name: "DaemonSet owner",
			mutate: func(review []byte) []byte {
				return bytes.Replace(review, []byte(`"controller":true`), []byte(`"controller":false`), 1)
			},
		},
		{
			name: "protection finalizer",
			mutate: func(review []byte) []byte {
				return bytes.Replace(
					review,
					[]byte(`"finalizers":["fleet.vela.ai/drain-protection"]`),
					[]byte(`"finalizers":[]`),
					1,
				)
			},
		},
		{
			name: "identity scheduling gate",
			mutate: func(review []byte) []byte {
				return bytes.Replace(
					review,
					[]byte(`"schedulingGates":[{"name":"fleet.vela.ai/identity-binding"}],`),
					nil,
					1,
				)
			},
		},
		{
			name: "unique target Node affinity",
			mutate: func(review []byte) []byte {
				return bytes.Replace(review, []byte(`"h3-node-01"`), []byte(`"h3-node-01","h3-node-02"`), 1)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authorizer := &recordingAuthorizer{}
			handler, err := NewHandler(authorizer, Config{
				FleetUsername:         "system:serviceaccount:vela-system:vela-fleet-controller",
				PodControllerUsername: "system:kube-controller-manager",
			})
			if err != nil {
				t.Fatalf("create Fleet admission handler: %v", err)
			}
			response := serveAdmission(t, handler, test.mutate(protectedPodCreateReview()))
			if response.Allowed {
				t.Fatalf("malformed protected Worker Pod create was allowed: %#v", response)
			}
			if len(authorizer.requests) != 0 {
				t.Fatalf("malformed Worker Pod create reached authorizer: %#v", authorizer.requests)
			}
		})
	}
}

func TestProtectedPodDeleteRejectsMalformedDrainIdentityBeforeAuthorization(t *testing.T) {
	const requestUID = "admission-delete-malformed-drain"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	review := bytes.Replace(
		protectedPodDeleteReview(
			requestUID,
			"system:serviceaccount:vela-system:vela-fleet-controller",
		),
		[]byte("23000000-0000-0000-0000-000000000042"),
		[]byte("not-a-drain-operation"),
		1,
	)
	response := serveAdmission(t, handler, review)
	if response.Allowed {
		t.Fatalf("malformed DrainOperation identity was allowed: %#v", response)
	}
	if len(authorizer.requests) != 0 {
		t.Fatalf("malformed DrainOperation identity reached authorizer: %#v", authorizer.requests)
	}
}

func TestProtectedMutationDeniesDatabaseOutage(t *testing.T) {
	authorizer := &recordingAuthorizer{err: errors.New("database unavailable")}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	response := serveAdmission(t, handler, protectedPodDeleteReview(
		"admission-delete-database-outage",
		"system:serviceaccount:vela-system:vela-fleet-controller",
	))
	if response.Allowed {
		t.Fatalf("protected mutation was allowed during database outage: %#v", response)
	}
	if len(authorizer.requests) != 1 {
		t.Fatalf("database-outage authorization calls = %d, want 1", len(authorizer.requests))
	}
}

func TestProtectedMutationReplayDigestIgnoresJSONFormatting(t *testing.T) {
	const requestUID = "admission-delete-canonical-replay"
	authorizer := &recordingAuthorizer{result: fleet.MutationAuthorizationResult{
		RequestUID: requestUID, Authorized: true,
	}}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	review := protectedPodDeleteReview(
		requestUID,
		"system:serviceaccount:vela-system:vela-fleet-controller",
	)
	var compact bytes.Buffer
	if err := json.Compact(&compact, review); err != nil {
		t.Fatalf("compact AdmissionReview: %v", err)
	}
	if response := serveAdmission(t, handler, review); !response.Allowed {
		t.Fatalf("original canonical replay was denied: %#v", response)
	}
	if response := serveAdmission(t, handler, compact.Bytes()); !response.Allowed {
		t.Fatalf("reformatted canonical replay was denied: %#v", response)
	}
	if len(authorizer.requests) != 2 ||
		!bytes.Equal(authorizer.requests[0].RequestDigest, authorizer.requests[1].RequestDigest) {
		t.Fatalf("canonical replay authorization requests = %#v", authorizer.requests)
	}
}

func TestDaemonSetControllerCannotMutateProtectedWorkerPod(t *testing.T) {
	handler, err := NewHandler(&recordingAuthorizer{}, Config{
		FleetUsername:         "system:serviceaccount:vela-system:vela-fleet-controller",
		PodControllerUsername: "system:kube-controller-manager",
	})
	if err != nil {
		t.Fatalf("create Fleet admission handler: %v", err)
	}
	deleteReview := protectedPodDeleteReview(
		"admission-controller-delete-pod",
		"system:kube-controller-manager",
	)
	updateReview := bytes.Replace(
		protectedPodFinalizerRemovalReview(),
		[]byte("system:serviceaccount:vela-system:vela-fleet-controller"),
		[]byte("system:kube-controller-manager"),
		1,
	)
	for name, review := range map[string][]byte{
		"delete": deleteReview,
		"update": updateReview,
	} {
		t.Run(name, func(t *testing.T) {
			response := serveAdmission(t, handler, review)
			if response.Allowed {
				t.Fatalf("DaemonSet controller protected Pod %s was allowed: %#v", name, response)
			}
		})
	}
}

type recordingAuthorizer struct {
	requests []fleet.MutationAuthorizationRequest
	result   fleet.MutationAuthorizationResult
	err      error
}

type recordingPodCreateValidator struct {
	pods     []corev1.Pod
	validate func(corev1.Pod) error
}

func (validator *recordingPodCreateValidator) ValidateProtectedPodCreate(
	_ context.Context,
	pod corev1.Pod,
) error {
	validator.pods = append(validator.pods, pod)
	if validator.validate == nil {
		return nil
	}
	return validator.validate(pod)
}

type admissionDrainReader struct{}

func (*admissionDrainReader) GetDrain(context.Context, uuid.UUID) (fleet.DrainResult, error) {
	return fleet.DrainResult{}, nil
}

func (*admissionDrainReader) RequestDrain(
	context.Context,
	fleet.DrainRequest,
) (fleet.DrainResult, error) {
	return fleet.DrainResult{}, nil
}

func (*admissionDrainReader) ReconcileDrain(
	context.Context,
	uuid.UUID,
) (fleet.DrainResult, error) {
	return fleet.DrainResult{}, nil
}

func (*admissionDrainReader) HasRetirementAuthorization(
	context.Context,
	fleet.RetirementAuthorizationRequest,
) (bool, error) {
	return false, nil
}

func (*admissionDrainReader) HasRetirementCompletion(
	context.Context,
	fleet.RetirementAuthorizationRequest,
) (bool, error) {
	return false, nil
}

func (*admissionDrainReader) RecordRetirementCompletion(
	context.Context,
	fleet.RetirementAuthorizationRequest,
) (fleet.RetirementCompletionResult, error) {
	return fleet.RetirementCompletionResult{}, nil
}

type admissionIdentityResolver struct{}

func (*admissionIdentityResolver) ResolveWorkerIdentity(
	context.Context,
	fleet.WorkerIdentityRequest,
) (fleet.WorkerIdentity, error) {
	return fleet.WorkerIdentity{}, nil
}

type admissionReadinessStarter struct{}

func (*admissionReadinessStarter) BeginReadiness(
	context.Context,
	fleet.ReadinessRequest,
) (fleet.ReadinessResult, error) {
	return fleet.ReadinessResult{}, nil
}

type admissionCapacityPolicyConfigurator struct{}

func (*admissionCapacityPolicyConfigurator) ConfigureCapacityPolicy(
	_ context.Context,
	policy fleet.CapacityPolicy,
) (fleet.CapacityPolicyResult, error) {
	return fleet.CapacityPolicyResult{
		WorkerPoolID: policy.WorkerPoolID,
		Revision:     policy.Revision,
	}, nil
}

func admissionDesiredRevision() fleetcontroller.DesiredRevision {
	return fleetcontroller.DesiredRevision{
		WorkerPoolID:  uuid.MustParse("00000000-0000-0000-0000-000000000005"),
		Namespace:     "vela-system",
		Name:          "h3-worker-pool-primary",
		Revision:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		WorkerProfile: "h3",
		NodeSelector: map[string]string{
			"vela.ai/worker-profile": "h3",
			"vela.ai/worker-pool":    "launch",
		},
		InitImage:                  "docker.io/library/busybox@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		WorkerAgentImage:           "ghcr.io/vivym/vela-worker-agent@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		RunnerImage:                "ghcr.io/vivym/vela-h3-runner@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ArtifactStoreTLSSecret:     "vela-artifact-store-ca-a1",
		ExecutionProfileRevisionID: uuid.MustParse("23000000-0000-0000-0000-000000000043"),
		InferenceBackendRevision:   "sglang-h3-v3",
		ReadinessTimeout:           30 * time.Minute,
		CapacityPolicy: fleetcontroller.CapacityPolicySpec{
			WorkerHighWatermarkBytes: 800,
			WorkerLowWatermarkBytes:  400,
			WorkerCriticalFreeBytes:  100,
			PoolHighWatermarkBytes:   5600,
			PoolLowWatermarkBytes:    2800,
			ObservationMaxAge:        2 * time.Minute,
		},
		Placements: []fleetcontroller.WorkerPlacement{{
			NodeIdentity: "h3-node-01", DaemonSetName: "h3-worker-pool-primary-node-01",
			WorkerRuntimeConfigMap:  "vela-worker-runtime-a1",
			RunnerProfilesConfigMap: "vela-runner-profiles-a1",
			RunnerGPURolesConfigMap: "vela-runner-gpu-roles-a1",
			WorkerControlTLSSecret:  "vela-worker-control-mtls-a1",
		}},
	}
}

func admissionTargetNodeAffinity(node string) *corev1.Affinity {
	return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchFields: []corev1.NodeSelectorRequirement{{
					Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{node},
				}},
			}},
		},
	}}
}

func (authorizer *recordingAuthorizer) AuthorizeMutation(
	_ context.Context,
	request fleet.MutationAuthorizationRequest,
) (fleet.MutationAuthorizationResult, error) {
	authorizer.requests = append(authorizer.requests, request)
	return authorizer.result, authorizer.err
}

type decodedAdmissionResponse struct {
	UID     string `json:"uid"`
	Allowed bool   `json:"allowed"`
	Status  *struct {
		Message string `json:"message"`
	} `json:"status,omitempty"`
}

func serveAdmission(t *testing.T, handler http.Handler, body []byte) decodedAdmissionResponse {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/validate", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("admission status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var review struct {
		Response decodedAdmissionResponse `json:"response"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &review); err != nil {
		t.Fatalf("decode AdmissionReview response: %v body=%s", err, recorder.Body.String())
	}
	return review.Response
}

func protectedPodDeleteReview(uid, username string) []byte {
	return []byte(`{
		"apiVersion":"admission.k8s.io/v1",
		"kind":"AdmissionReview",
		"request":{
			"uid":"` + uid + `",
			"operation":"DELETE",
			"kind":{"group":"","version":"v1","kind":"Pod"},
			"resource":{"group":"","version":"v1","resource":"pods"},
			"namespace":"vela-workers",
			"name":"h3-worker-node-1",
			"userInfo":{"username":"` + username + `"},
			"oldObject":{
				"apiVersion":"v1",
				"kind":"Pod",
				"metadata":{
					"uid":"kubernetes-worker-pod-uid-1",
					"namespace":"vela-workers",
					"name":"h3-worker-node-1",
					"labels":{
						"vela.ai/fleet-protected":"true",
						"vela.ai/worker-pool-id":"00000000-0000-0000-0000-000000000005",
						"vela.ai/fleet-revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
						"vela.ai/worker-id":"23000000-0000-0000-0000-000000000041",
						"vela.ai/worker-epoch":"20"
					},
					"annotations":{
						"vela.ai/drain-operation-ids":"23000000-0000-0000-0000-000000000042"
					},
					"finalizers":["fleet.vela.ai/drain-protection"]
				}
			}
		}
	}`)
}

func protectedPodOwnershipRemovalReview() []byte {
	return []byte(`{
		"apiVersion":"admission.k8s.io/v1",
		"kind":"AdmissionReview",
		"request":{
			"uid":"admission-remove-owner-label",
			"operation":"UPDATE",
			"kind":{"group":"","version":"v1","kind":"Pod"},
			"namespace":"vela-workers",
			"name":"h3-worker-node-1",
			"userInfo":{"username":"system:serviceaccount:vela-system:vela-fleet-controller"},
			"oldObject":` + string(protectedPodObject(true)) + `,
			"object":` + string(protectedPodObject(false)) + `
		}
	}`)
}

func protectedPodFinalizerRemovalReview() []byte {
	oldObject := protectedPodObject(true)
	newObject := bytes.Replace(
		oldObject,
		[]byte(`"finalizers":["fleet.vela.ai/drain-protection"]`),
		[]byte(`"finalizers":[]`),
		1,
	)
	return []byte(`{
		"apiVersion":"admission.k8s.io/v1",
		"kind":"AdmissionReview",
		"request":{
			"uid":"admission-remove-finalizer",
			"operation":"UPDATE",
			"kind":{"group":"","version":"v1","kind":"Pod"},
			"namespace":"vela-workers",
			"name":"h3-worker-node-1",
			"userInfo":{"username":"system:serviceaccount:vela-system:vela-fleet-controller"},
			"oldObject":` + string(oldObject) + `,
			"object":` + string(newObject) + `
		}
	}`)
}

func protectedPodDrainIntentReview(withSpecMutation bool) []byte {
	newObject := protectedPodObject(true)
	oldObject := bytes.Replace(
		newObject,
		[]byte(`"vela.ai/drain-operation-ids":"23000000-0000-0000-0000-000000000042"`),
		nil,
		1,
	)
	if withSpecMutation {
		newObject = bytes.Replace(
			newObject,
			[]byte(`"finalizers":["fleet.vela.ai/drain-protection"]`),
			[]byte(`"finalizers":["fleet.vela.ai/drain-protection"],"spec":{"nodeName":"other-node"}`),
			1,
		)
	}
	return []byte(`{
		"apiVersion":"admission.k8s.io/v1",
		"kind":"AdmissionReview",
		"request":{
			"uid":"admission-attach-drain-identity",
			"operation":"UPDATE",
			"kind":{"group":"","version":"v1","kind":"Pod"},
			"namespace":"vela-workers",
			"name":"h3-worker-node-1",
			"userInfo":{"username":"system:serviceaccount:vela-system:vela-fleet-controller"},
			"oldObject":` + string(oldObject) + `,
			"object":` + string(newObject) + `
		}
	}`)
}

func protectedDaemonSetImageReview() []byte {
	return protectedDaemonSetImageReviewWith(
		"admission-daemonset-image",
		"ghcr.io/vivym/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"ghcr.io/vivym/worker@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	)
}

func protectedDaemonSetImageReviewWith(uid, oldImage, newImage string) []byte {
	return protectedDaemonSetUpdateReview(
		uid,
		"system:serviceaccount:vela-system:vela-fleet-controller",
		protectedDaemonSetObject(oldImage),
		protectedDaemonSetObject(newImage),
	)
}

func protectedDaemonSetUpdateReview(uid, username string, oldObject, object []byte) []byte {
	return []byte(`{
		"apiVersion":"admission.k8s.io/v1",
		"kind":"AdmissionReview",
		"request":{
			"uid":"` + uid + `",
			"operation":"UPDATE",
			"kind":{"group":"apps","version":"v1","kind":"DaemonSet"},
			"namespace":"vela-workers",
			"name":"h3-worker-pool-primary",
			"userInfo":{"username":"` + username + `"},
			"oldObject":` + string(oldObject) + `,
			"object":` + string(object) + `
		}
	}`)
}

func protectedDaemonSetCreateReview() []byte {
	object := bytes.Replace(
		protectedDaemonSetObject("ghcr.io/vivym/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		[]byte(`"uid":"kubernetes-daemonset-uid-1",`),
		nil,
		1,
	)
	object = bytes.Replace(
		object,
		[]byte(`"vela.ai/drain-operation-ids":"23000000-0000-0000-0000-000000000046,23000000-0000-0000-0000-000000000045"`),
		nil,
		1,
	)
	return []byte(`{
		"apiVersion":"admission.k8s.io/v1",
		"kind":"AdmissionReview",
		"request":{
			"uid":"admission-daemonset-create",
			"operation":"CREATE",
			"kind":{"group":"apps","version":"v1","kind":"DaemonSet"},
			"namespace":"vela-workers",
			"name":"h3-worker-pool-primary",
			"userInfo":{"username":"system:serviceaccount:vela-system:vela-fleet-controller"},
			"object":` + string(object) + `
		}
	}`)
}

func protectedDaemonSetDeleteReview(uid string) []byte {
	return []byte(`{
		"apiVersion":"admission.k8s.io/v1",
		"kind":"AdmissionReview",
		"request":{
			"uid":"` + uid + `",
			"operation":"DELETE",
			"kind":{"group":"apps","version":"v1","kind":"DaemonSet"},
			"namespace":"vela-workers",
			"name":"h3-worker-pool-primary",
			"userInfo":{"username":"system:serviceaccount:vela-system:vela-fleet-controller"},
			"oldObject":` + string(protectedDaemonSetObject("ghcr.io/vivym/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")) + `
		}
	}`)
}

func protectedWorkerPoolDeleteReview() []byte {
	return []byte(`{
		"apiVersion":"admission.k8s.io/v1",
		"kind":"AdmissionReview",
		"request":{
			"uid":"admission-worker-pool-delete",
			"operation":"DELETE",
			"kind":{"group":"fleet.vela.ai","version":"v1alpha1","kind":"WorkerPool"},
			"namespace":"vela-workers",
			"name":"h3-worker-pool-primary",
			"userInfo":{"username":"system:serviceaccount:vela-system:vela-fleet-controller"},
			"oldObject":` + string(protectedWorkerPoolObject(true)) + `
		}
	}`)
}

func protectedWorkerPoolCreateReview() []byte {
	object := bytes.Replace(
		protectedWorkerPoolObject(false),
		[]byte(`"vela.ai/drain-operation-ids":"23000000-0000-0000-0000-000000000045,23000000-0000-0000-0000-000000000046"`),
		nil,
		1,
	)
	return []byte(`{
		"apiVersion":"admission.k8s.io/v1",
		"kind":"AdmissionReview",
		"request":{
			"uid":"admission-worker-pool-create",
			"operation":"CREATE",
			"kind":{"group":"fleet.vela.ai","version":"v1alpha1","kind":"WorkerPool"},
			"namespace":"vela-workers",
			"name":"h3-worker-pool-primary",
			"userInfo":{"username":"system:serviceaccount:vela-system:vela-fleet-controller"},
			"object":` + string(object) + `
		}
	}`)
}

func protectedWorkerPoolUpdateReview(uid, username string, oldObject, object []byte) []byte {
	return []byte(`{
		"apiVersion":"admission.k8s.io/v1",
		"kind":"AdmissionReview",
		"request":{
			"uid":"` + uid + `",
			"operation":"UPDATE",
			"kind":{"group":"fleet.vela.ai","version":"v1alpha1","kind":"WorkerPool"},
			"namespace":"vela-workers",
			"name":"h3-worker-pool-primary",
			"userInfo":{"username":"` + username + `"},
			"oldObject":` + string(oldObject) + `,
			"object":` + string(object) + `
		}
	}`)
}

func protectedPodCreateReview() []byte {
	return []byte(`{
		"apiVersion":"admission.k8s.io/v1",
		"kind":"AdmissionReview",
		"request":{
			"uid":"admission-worker-pod-create",
			"operation":"CREATE",
			"kind":{"group":"","version":"v1","kind":"Pod"},
			"namespace":"vela-workers",
			"name":"h3-worker-node-1",
			"userInfo":{"username":"system:kube-controller-manager"},
			"object":{
				"apiVersion":"v1",
				"kind":"Pod",
				"metadata":{
					"namespace":"vela-workers",
					"name":"h3-worker-node-1",
					"labels":{
						"vela.ai/fleet-protected":"true",
						"vela.ai/worker-pool-id":"00000000-0000-0000-0000-000000000005",
						"vela.ai/fleet-revision":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
					},
					"finalizers":["fleet.vela.ai/drain-protection"],
					"ownerReferences":[{
						"apiVersion":"apps/v1","kind":"DaemonSet",
						"name":"h3-worker-pool-primary","uid":"kubernetes-daemonset-uid-1",
						"controller":true
					}]
				},
				"spec":{
					"serviceAccountName":"vela-worker",
					"automountServiceAccountToken":false,
					"schedulingGates":[{"name":"fleet.vela.ai/identity-binding"}],
					"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{
						"nodeSelectorTerms":[{"matchFields":[{
							"key":"metadata.name","operator":"In","values":["h3-node-01"]
						}]}]
					}}},
					"nodeSelector":{"vela.ai/worker-profile":"h3","vela.ai/worker-pool":"launch"},
					"tolerations":[{"key":"vela.ai/h3","operator":"Equal","value":"true","effect":"NoSchedule"}],
					"containers":[{
						"name":"h3-runner",
						"image":"ghcr.io/vivym/worker@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
						"resources":{"requests":{"nvidia.com/gpu":"8"},"limits":{"nvidia.com/gpu":"8"}}
					}]
				}
			}
		}
	}`)
}

func protectedPodIdentityBindingReview(
	username string,
	newNodeIdentity string,
	includeIdentity bool,
	removeGate bool,
	mutateSpec bool,
) []byte {
	return []byte(`{
		"apiVersion":"admission.k8s.io/v1",
		"kind":"AdmissionReview",
		"request":{
			"uid":"admission-worker-pod-identity-binding",
			"operation":"UPDATE",
			"kind":{"group":"","version":"v1","kind":"Pod"},
			"namespace":"vela-workers",
			"name":"h3-worker-node-1",
			"userInfo":{"username":"` + username + `"},
			"oldObject":` + string(protectedIdentityGatedPodObject("h3-node-01", false, false, false)) + `,
			"object":` + string(protectedIdentityGatedPodObject(
		newNodeIdentity,
		includeIdentity,
		removeGate,
		mutateSpec,
	)) + `
		}
	}`)
}

func protectedIdentityGatedPodObject(
	nodeIdentity string,
	includeIdentity bool,
	removeGate bool,
	mutateSpec bool,
) []byte {
	identity := ""
	if includeIdentity {
		identity = `,
				"vela.ai/worker-id":"23000000-0000-0000-0000-000000000041",
				"vela.ai/worker-epoch":"20"`
	}
	gate := `"schedulingGates":[{"name":"fleet.vela.ai/identity-binding"}],`
	if removeGate {
		gate = ""
	}
	serviceAccount := "vela-worker"
	if mutateSpec {
		serviceAccount = "vela-worker-mutated"
	}
	return []byte(`{
		"apiVersion":"v1",
		"kind":"Pod",
		"metadata":{
			"uid":"kubernetes-worker-pod-uid-1",
			"namespace":"vela-workers",
			"name":"h3-worker-node-1",
			"labels":{
				"vela.ai/fleet-protected":"true",
				"vela.ai/worker-pool-id":"00000000-0000-0000-0000-000000000005",
				"vela.ai/fleet-revision":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"` + identity + `
			},
			"finalizers":["fleet.vela.ai/drain-protection"],
			"ownerReferences":[{
				"apiVersion":"apps/v1","kind":"DaemonSet",
				"name":"h3-worker-pool-primary","uid":"kubernetes-daemonset-uid-1",
				"controller":true
			}]
		},
		"spec":{
			"serviceAccountName":"` + serviceAccount + `",
			"automountServiceAccountToken":false,
			` + gate + `
			"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{
				"nodeSelectorTerms":[{"matchFields":[{
					"key":"metadata.name","operator":"In","values":["` + nodeIdentity + `"]
				}]}]
			}}},
			"nodeSelector":{"vela.ai/worker-profile":"h3","vela.ai/worker-pool":"launch"},
			"tolerations":[{"key":"vela.ai/h3","operator":"Equal","value":"true","effect":"NoSchedule"}],
			"containers":[{
				"name":"h3-runner",
				"image":"ghcr.io/vivym/worker@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
				"resources":{"requests":{"nvidia.com/gpu":"8"},"limits":{"nvidia.com/gpu":"8"}}
			}]
		}
	}`)
}

func protectedWorkerPoolObject(withUID bool) []byte {
	uid := ""
	if withUID {
		uid = `"uid":"kubernetes-worker-pool-uid-1",`
	}
	return []byte(`{
		"apiVersion":"fleet.vela.ai/v1alpha1",
		"kind":"WorkerPool",
		"metadata":{` + uid + `
			"namespace":"vela-workers",
			"name":"h3-worker-pool-primary",
			"labels":{
				"vela.ai/fleet-protected":"true",
				"vela.ai/worker-pool-id":"00000000-0000-0000-0000-000000000005",
				"vela.ai/fleet-revision":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
			},
			"annotations":{
				"vela.ai/drain-operation-ids":"23000000-0000-0000-0000-000000000045,23000000-0000-0000-0000-000000000046"
			},
			"finalizers":["fleet.vela.ai/drain-protection"]
		},
			"spec":{
				"revision":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
				"workerProfile":"h3",
				"nodeSelector":{"vela.ai/worker-profile":"h3","vela.ai/worker-pool":"launch"},
				"placements":[{
					"nodeIdentity":"h3-node-01",
					"daemonSetName":"h3-worker-pool-primary-node-01",
					"workerRuntimeConfigMap":"vela-worker-runtime-a1",
					"runnerProfilesConfigMap":"vela-runner-profiles-a1",
					"runnerGPURolesConfigMap":"vela-runner-gpu-roles-a1",
					"workerControlTLSSecret":"vela-worker-control-mtls-a1"
				}],
				"capacityPolicy":{
					"workerHighWatermarkBytes":800,
					"workerLowWatermarkBytes":400,
					"workerCriticalFreeBytes":100,
					"poolHighWatermarkBytes":5600,
					"poolLowWatermarkBytes":2800,
					"observationMaxAgeSeconds":120
				}
			}
	}`)
}

func protectedDaemonSetObject(image string) []byte {
	return []byte(`{
		"apiVersion":"apps/v1",
		"kind":"DaemonSet",
		"metadata":{
			"uid":"kubernetes-daemonset-uid-1",
			"namespace":"vela-workers",
			"name":"h3-worker-pool-primary",
			"labels":{
				"vela.ai/fleet-protected":"true",
				"vela.ai/worker-pool-id":"00000000-0000-0000-0000-000000000005",
				"vela.ai/fleet-revision":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
			},
			"annotations":{
				"vela.ai/drain-operation-ids":"23000000-0000-0000-0000-000000000046,23000000-0000-0000-0000-000000000045"
			},
			"finalizers":["fleet.vela.ai/drain-protection"]
		},
		"spec":{
			"updateStrategy":{"type":"OnDelete"},
			"selector":{"matchLabels":{"app.kubernetes.io/name":"vela-h3-worker"}},
			"template":{
				"metadata":{"labels":{"app.kubernetes.io/name":"vela-h3-worker"}},
				"spec":{
					"schedulingGates":[{"name":"fleet.vela.ai/identity-binding"}],
					"nodeSelector":{"vela.ai/worker-profile":"h3","vela.ai/worker-pool":"launch","kubernetes.io/hostname":"h3-node-01"},
					"tolerations":[{"key":"vela.ai/h3","operator":"Equal","value":"true","effect":"NoSchedule"}],
					"containers":[{
						"name":"h3-runner","image":"` + image + `",
						"resources":{"requests":{"nvidia.com/gpu":"8"},"limits":{"nvidia.com/gpu":"8"}}
					}]
				}
			}
		}
	}`)
}

func protectedPodObject(protected bool) []byte {
	protectedLabel := ""
	if protected {
		protectedLabel = `"vela.ai/fleet-protected":"true",`
	}
	return []byte(`{
		"apiVersion":"v1",
		"kind":"Pod",
		"metadata":{
			"uid":"kubernetes-worker-pod-uid-1",
			"namespace":"vela-workers",
			"name":"h3-worker-node-1",
			"labels":{` + protectedLabel + `
				"vela.ai/worker-pool-id":"00000000-0000-0000-0000-000000000005",
				"vela.ai/fleet-revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"vela.ai/worker-id":"23000000-0000-0000-0000-000000000041",
				"vela.ai/worker-epoch":"20"
			},
			"annotations":{
				"vela.ai/drain-operation-ids":"23000000-0000-0000-0000-000000000042"
			},
			"finalizers":["fleet.vela.ai/drain-protection"]
		}
	}`)
}

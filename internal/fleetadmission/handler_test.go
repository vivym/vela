package fleetadmission

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontract"
	corev1 "k8s.io/api/core/v1"
)

const fleetUsername = "system:serviceaccount:vela-system:vela-fleet-controller"

type recordingAuthorizer struct {
	request fleet.MutationAuthorizationRequest
	err     error
}

func (authorizer *recordingAuthorizer) AuthorizeMutation(
	_ context.Context,
	request fleet.MutationAuthorizationRequest,
) (fleet.MutationAuthorizationResult, error) {
	authorizer.request = request
	if authorizer.err != nil {
		return fleet.MutationAuthorizationResult{}, authorizer.err
	}
	return fleet.MutationAuthorizationResult{RequestUID: request.RequestUID, Authorized: true}, nil
}

type exactPodValidator struct {
	want corev1.Pod
}

func (validator exactPodValidator) ValidateProtectedPodCreate(_ context.Context, pod corev1.Pod) error {
	if validator.want.Name != "" && pod.Name != validator.want.Name {
		return errors.New("unexpected Pod")
	}
	return nil
}

func TestWorkerInstancePodCreateRequiresFleetActorAndAuthoritativeShape(t *testing.T) {
	pod := workerInstancePod(false)
	handler, err := NewHandler(&recordingAuthorizer{}, Config{
		FleetUsername: fleetUsername, PodCreateValidator: exactPodValidator{want: pod},
	})
	if err != nil {
		t.Fatalf("create handler: %v", err)
	}
	response := serveAdmission(t, handler, review("create-1", "CREATE", fleetUsername, nil, pod))
	if !response.Response.Allowed {
		t.Fatalf("exact WorkerInstance Pod create denied: %#v", response.Response.Status)
	}
	response = serveAdmission(t, handler, review("create-2", "CREATE", "system:serviceaccount:kube-system:daemon-set-controller", nil, pod))
	if response.Response.Allowed {
		t.Fatal("non-Fleet actor created a protected WorkerInstance Pod")
	}
}

func TestWorkerInstancePodDeleteRequiresExactRegistryAuthorization(t *testing.T) {
	authorizer := &recordingAuthorizer{}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: fleetUsername, PodCreateValidator: exactPodValidator{},
	})
	if err != nil {
		t.Fatalf("create handler: %v", err)
	}
	pod := workerInstancePod(true)
	response := serveAdmission(t, handler, review("delete-1", "DELETE", fleetUsername, pod, nil))
	if !response.Response.Allowed {
		t.Fatalf("authorized WorkerInstance Pod delete denied: %#v", response.Response.Status)
	}
	request := authorizer.request
	if request.RequestUID != "delete-1" || request.Operation != fleet.MutationDelete ||
		request.KubernetesUID != "pod-kubernetes-uid" || request.WorkerInstanceEpoch != 7 ||
		request.WorkerInstanceID.String() != "10000000-0000-0000-0000-000000000001" ||
		request.ResidencyPlanRevisionID.String() != "20000000-0000-0000-0000-000000000001" ||
		request.WorkerBundleID.String() != "30000000-0000-0000-0000-000000000001" ||
		request.WorkerMemberID.String() != "40000000-0000-0000-0000-000000000001" ||
		len(request.RequestDigest) != 32 {
		t.Fatalf("WorkerInstance Pod authorization request=%#v", request)
	}
}

func TestWorkerInstancePodFinalizerRemovalRejectsAdditionalMutation(t *testing.T) {
	authorizer := &recordingAuthorizer{}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: fleetUsername, PodCreateValidator: exactPodValidator{},
	})
	if err != nil {
		t.Fatalf("create handler: %v", err)
	}
	oldPod := workerInstancePod(true)
	newPod := oldPod.DeepCopy()
	newPod.Finalizers = nil
	response := serveAdmission(t, handler, review("update-1", "UPDATE", fleetUsername, oldPod, newPod))
	if !response.Response.Allowed || authorizer.request.Operation != fleet.MutationRemoveFinalizer {
		t.Fatalf("exact finalizer removal denied: response=%#v request=%#v", response, authorizer.request)
	}
	newPod.Spec.NodeName = "another-node"
	response = serveAdmission(t, handler, review("update-2", "UPDATE", fleetUsername, oldPod, newPod))
	if response.Response.Allowed {
		t.Fatal("finalizer removal with an additional mutation was allowed")
	}
}

func TestWorkerInstanceMutationDigestIgnoresJSONFormatting(t *testing.T) {
	authorizer := &recordingAuthorizer{}
	handler, err := NewHandler(authorizer, Config{
		FleetUsername: fleetUsername, PodCreateValidator: exactPodValidator{},
	})
	if err != nil {
		t.Fatalf("create handler: %v", err)
	}
	pod := workerInstancePod(true)
	first := review("same-uid", "DELETE", fleetUsername, pod, nil)
	serveAdmission(t, handler, first)
	firstDigest := append([]byte(nil), authorizer.request.RequestDigest...)
	var decoded any
	if err := json.Unmarshal(first, &decoded); err != nil {
		t.Fatalf("decode review: %v", err)
	}
	reformatted, err := json.MarshalIndent(decoded, "", "  ")
	if err != nil {
		t.Fatalf("reformat review: %v", err)
	}
	serveAdmission(t, handler, reformatted)
	if !bytes.Equal(firstDigest, authorizer.request.RequestDigest) {
		t.Fatal("semantic replay changed the mutation digest")
	}
}

func workerInstancePod(withUID bool) corev1.Pod {
	pod := corev1.Pod{}
	pod.APIVersion = "v1"
	pod.Kind = "Pod"
	pod.Namespace = "vela-system"
	pod.Name = "worker-instance-1-member-0"
	if withUID {
		pod.UID = "pod-kubernetes-uid"
	}
	pod.Labels = map[string]string{
		fleetcontract.ProtectedLabel:               "true",
		fleetcontract.WorkerInstanceIDLabel:        "10000000-0000-0000-0000-000000000001",
		fleetcontract.WorkerInstanceEpochLabel:     "7",
		fleetcontract.ResidencyPlanRevisionIDLabel: "20000000-0000-0000-0000-000000000001",
		fleetcontract.WorkerBundleIDLabel:          "30000000-0000-0000-0000-000000000001",
		fleetcontract.WorkerMemberIDLabel:          "40000000-0000-0000-0000-000000000001",
	}
	pod.Finalizers = []string{fleetcontract.ProtectionFinalizer}
	return pod
}

func review(uid, operation, username string, oldPod, pod any) []byte {
	request := admissionRequest{
		UID: uid, Operation: operation,
		Kind:      groupVersionKind{Version: "v1", Kind: "Pod"},
		Namespace: "vela-system", Name: "worker-instance-1-member-0",
		UserInfo: userInfo{Username: username},
	}
	if oldPod != nil {
		request.OldObject, _ = json.Marshal(oldPod)
	}
	if pod != nil {
		request.Object, _ = json.Marshal(pod)
	}
	encoded, _ := json.Marshal(admissionReview{
		APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview", Request: &request,
	})
	return encoded
}

func serveAdmission(t *testing.T, handler http.Handler, body []byte) admissionReview {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/validate", bytes.NewReader(body))
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("admission status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response admissionReview
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || response.Response == nil {
		t.Fatalf("decode admission response: %v body=%s", err, recorder.Body.String())
	}
	return response
}

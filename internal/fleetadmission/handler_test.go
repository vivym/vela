package fleetadmission

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
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
	wantPod     corev1.Pod
	wantService corev1.Service
	wantSecret  corev1.Secret
}

func (validator exactPodValidator) ValidateProtectedPodCreate(_ context.Context, pod corev1.Pod) error {
	if validator.wantPod.Name != "" && !reflect.DeepEqual(pod, validator.wantPod) {
		return errors.New("unexpected Pod")
	}
	return nil
}

func (validator exactPodValidator) ValidateProtectedServiceCreate(_ context.Context, service corev1.Service) error {
	if validator.wantService.Name != "" && !reflect.DeepEqual(service, validator.wantService) {
		return errors.New("unexpected Service")
	}
	return nil
}

func (validator exactPodValidator) ValidateProtectedSecretCreate(_ context.Context, secret corev1.Secret) error {
	if validator.wantSecret.Name != "" && !reflect.DeepEqual(secret, validator.wantSecret) {
		return errors.New("unexpected Secret")
	}
	return nil
}

func TestWorkerInstancePodCreateRequiresFleetActorAndAuthoritativeShape(t *testing.T) {
	pod := workerInstancePod(false)
	handler, err := NewHandler(&recordingAuthorizer{}, Config{
		FleetUsername: fleetUsername, CreateValidator: exactPodValidator{wantPod: pod},
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
		FleetUsername: fleetUsername, CreateValidator: exactPodValidator{},
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
		FleetUsername: fleetUsername, CreateValidator: exactPodValidator{},
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
		FleetUsername: fleetUsername, CreateValidator: exactPodValidator{},
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

func TestWorkerMemberServiceAndSecretRequireAuthoritativeCreateAndRemainImmutable(t *testing.T) {
	service := workerMemberService()
	secret := workerMemberSecret()
	handler, err := NewHandler(&recordingAuthorizer{}, Config{
		FleetUsername:   fleetUsername,
		CreateValidator: exactPodValidator{wantService: service, wantSecret: secret},
	})
	if err != nil {
		t.Fatalf("create handler: %v", err)
	}
	for _, resource := range []struct {
		kind  string
		name  string
		value any
	}{
		{kind: "Service", name: service.Name, value: service},
		{kind: "Secret", name: secret.Name, value: secret},
	} {
		response := serveAdmission(t, handler, resourceReview(
			"create-"+resource.kind, "CREATE", fleetUsername,
			resource.kind, resource.name, nil, resource.value,
		))
		if !response.Response.Allowed {
			t.Fatalf("exact %s create denied: %#v", resource.kind, response.Response.Status)
		}
		response = serveAdmission(t, handler, resourceReview(
			"delete-"+resource.kind, "DELETE", fleetUsername,
			resource.kind, resource.name, resource.value, nil,
		))
		if response.Response.Allowed {
			t.Fatalf("protected %s delete was allowed without a resource cleanup protocol", resource.kind)
		}
	}
	drifted := service.DeepCopy()
	drifted.Spec.Ports = nil
	response := serveAdmission(t, handler, resourceReview(
		"create-drifted-service", "CREATE", fleetUsername,
		"Service", drifted.Name, nil, drifted,
	))
	if response.Response.Allowed {
		t.Fatal("drifted protected member Service create was allowed")
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

func workerMemberService() corev1.Service {
	service := corev1.Service{}
	service.APIVersion = "v1"
	service.Kind = "Service"
	service.Namespace = "vela-system"
	service.Name = "worker-instance-1-member-0-member"
	service.Labels = workerMemberLabels()
	service.Finalizers = []string{fleetcontract.ProtectionFinalizer}
	service.Spec.Ports = []corev1.ServicePort{{Name: "grpc-member", Port: 7444}}
	return service
}

func workerMemberSecret() corev1.Secret {
	immutable := true
	secret := corev1.Secret{}
	secret.APIVersion = "v1"
	secret.Kind = "Secret"
	secret.Namespace = "vela-system"
	secret.Name = "worker-instance-1-member-0-member-tls"
	secret.Labels = workerMemberLabels()
	secret.Finalizers = []string{fleetcontract.ProtectionFinalizer}
	secret.Immutable = &immutable
	secret.Type = corev1.SecretTypeOpaque
	secret.Data = map[string][]byte{"client.crt": []byte("certificate")}
	return secret
}

func workerMemberLabels() map[string]string {
	return map[string]string{
		fleetcontract.ProtectedLabel:               "true",
		fleetcontract.WorkerInstanceIDLabel:        "10000000-0000-0000-0000-000000000001",
		fleetcontract.WorkerInstanceEpochLabel:     "7",
		fleetcontract.ResidencyPlanRevisionIDLabel: "20000000-0000-0000-0000-000000000001",
		fleetcontract.WorkerBundleIDLabel:          "30000000-0000-0000-0000-000000000001",
		fleetcontract.WorkerMemberIDLabel:          "40000000-0000-0000-0000-000000000001",
	}
}

func review(uid, operation, username string, oldPod, pod any) []byte {
	return resourceReview(uid, operation, username, "Pod", "worker-instance-1-member-0", oldPod, pod)
}

func resourceReview(uid, operation, username, kind, name string, oldObject, object any) []byte {
	request := admissionRequest{
		UID: uid, Operation: operation,
		Kind:      groupVersionKind{Version: "v1", Kind: kind},
		Namespace: "vela-system", Name: name,
		UserInfo: userInfo{Username: username},
	}
	if oldObject != nil {
		request.OldObject, _ = json.Marshal(oldObject)
	}
	if object != nil {
		request.Object, _ = json.Marshal(object)
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

package fleetadmission

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontract"
	"github.com/vivym/vela/internal/runtimelaunch"
	corev1 "k8s.io/api/core/v1"
)

const maximumReviewBytes = 2 << 20

type MutationAuthorizer interface {
	AuthorizeMutation(
		context.Context,
		fleet.MutationAuthorizationRequest,
	) (fleet.MutationAuthorizationResult, error)
}

type ProtectedResourceCreateValidator interface {
	ValidateProtectedPodCreate(context.Context, corev1.Pod) error
	ValidateProtectedServiceCreate(context.Context, corev1.Service) error
	ValidateProtectedSecretCreate(context.Context, corev1.Secret) error
}

type Config struct {
	FleetUsername      string
	NodeUsernamePrefix string
	NetworkUsername    string
	CreateValidator    ProtectedResourceCreateValidator
}

type Handler struct {
	authorizer         MutationAuthorizer
	fleetUsername      string
	nodeUsernamePrefix string
	networkUsername    string
	createValidator    ProtectedResourceCreateValidator
}

type admissionReview struct {
	APIVersion string             `json:"apiVersion"`
	Kind       string             `json:"kind"`
	Request    *admissionRequest  `json:"request,omitempty"`
	Response   *admissionResponse `json:"response,omitempty"`
}

type admissionRequest struct {
	UID       string           `json:"uid"`
	Operation string           `json:"operation"`
	Kind      groupVersionKind `json:"kind"`
	Namespace string           `json:"namespace"`
	Name      string           `json:"name"`
	UserInfo  userInfo         `json:"userInfo"`
	Object    json.RawMessage  `json:"object"`
	OldObject json.RawMessage  `json:"oldObject"`
}

type groupVersionKind struct {
	Group   string `json:"group"`
	Version string `json:"version"`
	Kind    string `json:"kind"`
}

type userInfo struct {
	Username string `json:"username"`
}

type objectMetadata struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		UID        string            `json:"uid"`
		Namespace  string            `json:"namespace"`
		Name       string            `json:"name"`
		Labels     map[string]string `json:"labels"`
		Finalizers []string          `json:"finalizers"`
	} `json:"metadata"`
}

type normalizedMutation struct {
	Operation               fleet.MutationOperation `json:"operation"`
	KubernetesUID           string                  `json:"kubernetes_uid"`
	Namespace               string                  `json:"namespace"`
	Name                    string                  `json:"name"`
	WorkerInstanceID        uuid.UUID               `json:"worker_instance_id"`
	WorkerInstanceEpoch     int64                   `json:"worker_instance_epoch"`
	ResidencyPlanRevisionID uuid.UUID               `json:"residency_plan_revision_id"`
	WorkerBundleID          uuid.UUID               `json:"worker_bundle_id"`
	WorkerMemberID          uuid.UUID               `json:"worker_member_id"`
	Target                  any                     `json:"target"`
}

type admissionResponse struct {
	UID     string          `json:"uid"`
	Allowed bool            `json:"allowed"`
	Status  *responseStatus `json:"status,omitempty"`
}

type responseStatus struct {
	Message string `json:"message"`
}

func NewHandler(authorizer MutationAuthorizer, config Config) (*Handler, error) {
	if authorizer == nil {
		return nil, errors.New("fleet mutation authorizer is required")
	}
	if config.FleetUsername == "" || strings.TrimSpace(config.FleetUsername) != config.FleetUsername {
		return nil, errors.New("fleet Kubernetes username is required")
	}
	if config.CreateValidator == nil {
		return nil, errors.New("fleet protected resource create validator is required")
	}
	return &Handler{
		authorizer: authorizer, fleetUsername: config.FleetUsername,
		nodeUsernamePrefix: config.NodeUsernamePrefix,
		networkUsername:    config.NetworkUsername,
		createValidator:    config.CreateValidator,
	}, nil
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "AdmissionReview requires POST", http.StatusMethodNotAllowed)
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maximumReviewBytes)
	var review admissionReview
	decoder := json.NewDecoder(request.Body)
	if err := decoder.Decode(&review); err != nil || review.Request == nil ||
		review.APIVersion != "admission.k8s.io/v1" || review.Kind != "AdmissionReview" ||
		review.Request.UID == "" {
		http.Error(writer, "invalid AdmissionReview", http.StatusBadRequest)
		return
	}
	object, err := admissionObject(review.Request)
	if err != nil {
		writeAdmissionReview(writer, review.Request.UID, false, "protected resource metadata is invalid")
		return
	}
	protected := object.Metadata.Labels[fleetcontract.ProtectedLabel] == "true"
	if review.Request.Operation == "UPDATE" {
		oldObject, oldErr := decodeObject(review.Request.OldObject)
		if oldErr != nil {
			writeAdmissionReview(writer, review.Request.UID, false, "protected resource metadata is invalid")
			return
		}
		protected = protected || oldObject.Metadata.Labels[fleetcontract.ProtectedLabel] == "true"
	}
	if !protected {
		writeAdmissionReview(writer, review.Request.UID, true, "")
		return
	}
	if handler.networkUsername != "" && review.Request.UserInfo.Username == handler.networkUsername {
		allowed := authorizePodNetworkObservation(review.Request)
		writeAdmissionReview(writer, review.Request.UID, allowed, "network service may only update observed sandbox and IP metadata")
		return
	}
	if handler.nodeUsernamePrefix != "" && strings.HasPrefix(review.Request.UserInfo.Username, handler.nodeUsernamePrefix) {
		allowed := handler.authorizeNodeStartupGate(request.Context(), review.Request)
		writeAdmissionReview(writer, review.Request.UID, allowed, "Node may only release its exact approved startup gate")
		return
	}
	if review.Request.UserInfo.Username != handler.fleetUsername {
		writeAdmissionReview(writer, review.Request.UID, false, "protected resources are owned by the Fleet Controller")
		return
	}
	resourceKind := protectedWorkerResourceKind(review.Request, object)
	if resourceKind == "" {
		writeAdmissionReview(writer, review.Request.UID, false, "Fleet protected resource kind or authority is invalid")
		return
	}
	if review.Request.Operation == "CREATE" {
		var validationErr error
		switch resourceKind {
		case "Pod":
			var pod corev1.Pod
			if err := json.Unmarshal(review.Request.Object, &pod); err != nil {
				validationErr = err
			} else {
				validationErr = handler.createValidator.ValidateProtectedPodCreate(request.Context(), pod)
			}
		case "Service":
			var service corev1.Service
			if err := json.Unmarshal(review.Request.Object, &service); err != nil {
				validationErr = err
			} else {
				validationErr = handler.createValidator.ValidateProtectedServiceCreate(request.Context(), service)
			}
		case "Secret":
			var secret corev1.Secret
			if err := json.Unmarshal(review.Request.Object, &secret); err != nil {
				validationErr = err
			} else {
				validationErr = handler.createValidator.ValidateProtectedSecretCreate(request.Context(), secret)
			}
		}
		if validationErr != nil {
			slog.Warn("protected resource create rejected", "kind", resourceKind, "namespace", review.Request.Namespace, "name", review.Request.Name, "error", validationErr)
			writeAdmissionReview(writer, review.Request.UID, false, "protected resource create shape is invalid")
			return
		}
		writeAdmissionReview(writer, review.Request.UID, true, "")
		return
	}
	if resourceKind != "Pod" {
		writeAdmissionReview(writer, review.Request.UID, false, "protected member Services and Secrets are immutable")
		return
	}
	mutation, err := protectedWorkerInstancePodMutationRequest(review.Request, object)
	if err != nil {
		writeAdmissionReview(writer, review.Request.UID, false, "protected WorkerInstance Pod mutation is invalid")
		return
	}
	result, err := handler.authorizer.AuthorizeMutation(request.Context(), mutation)
	if err != nil || !result.Authorized || result.RequestUID != review.Request.UID {
		writeAdmissionReview(writer, review.Request.UID, false, "protected mutation authorization was denied")
		return
	}
	writeAdmissionReview(writer, review.Request.UID, true, "")
}

func authorizePodNetworkObservation(request *admissionRequest) bool {
	if request.Operation != "UPDATE" || request.Kind.Kind != "Pod" || request.Kind.Group != "" || request.Kind.Version != "v1" {
		return false
	}
	var old, current corev1.Pod
	if json.Unmarshal(request.OldObject, &old) != nil || json.Unmarshal(request.Object, &current) != nil ||
		old.UID == "" || old.UID != current.UID || old.Spec.NodeName == "" ||
		old.Annotations[runtimelaunch.ProtocolAnnotation] != runtimelaunch.Protocol {
		return false
	}
	fleetcontract.RemovePodNetworkObservations(&old)
	fleetcontract.RemovePodNetworkObservations(&current)
	old.ManagedFields, current.ManagedFields = nil, nil
	return reflect.DeepEqual(old, current)
}

// Node's narrow bootstrap permission is separate from Fleet's retirement
// authority. It cannot create/delete Pods, change images, or remove finalizers.
func (handler *Handler) authorizeNodeStartupGate(ctx context.Context, request *admissionRequest) bool {
	if request.Operation != "UPDATE" || request.Kind.Kind != "Pod" || request.Kind.Group != "" || request.Kind.Version != "v1" {
		return false
	}
	var old, current corev1.Pod
	if json.Unmarshal(request.OldObject, &old) != nil || json.Unmarshal(request.Object, &current) != nil {
		return false
	}
	node := old.Spec.NodeSelector[corev1.LabelHostname]
	if node == "" || request.UserInfo.Username != handler.nodeUsernamePrefix+node || old.UID == "" || current.UID != old.UID ||
		old.DeletionTimestamp != nil || old.Spec.NodeName != "" ||
		old.Annotations[runtimelaunch.ProtocolAnnotation] != runtimelaunch.Protocol || old.Spec.RestartPolicy != corev1.RestartPolicyNever ||
		len(old.Spec.SchedulingGates) != 1 || old.Spec.SchedulingGates[0].Name != runtimelaunch.Gate || len(current.Spec.SchedulingGates) != 0 {
		return false
	}
	// Validate the signed template shape. The existing Pod UID was bound above;
	// create validation correctly rejects a server-assigned UID in its input.
	template := *old.DeepCopy()
	template.UID = ""
	if err := handler.createValidator.ValidateProtectedPodCreate(ctx, template); err != nil {
		return false
	}
	old.Spec.SchedulingGates = nil
	old.ManagedFields, current.ManagedFields = nil, nil
	old.Generation, current.Generation = 0, 0
	return reflect.DeepEqual(old, current)
}

func admissionObject(request *admissionRequest) (objectMetadata, error) {
	raw := request.Object
	if request.Operation == "DELETE" {
		raw = request.OldObject
	}
	return decodeObject(raw)
}

func decodeObject(raw json.RawMessage) (objectMetadata, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return objectMetadata{}, errors.New("admission review object is required")
	}
	var object objectMetadata
	if err := json.Unmarshal(raw, &object); err != nil {
		return objectMetadata{}, err
	}
	return object, nil
}

func protectedWorkerResourceKind(request *admissionRequest, object objectMetadata) string {
	if request.Kind.Group != "" || request.Kind.Version != "v1" ||
		(request.Kind.Kind != "Pod" && request.Kind.Kind != "Service" && request.Kind.Kind != "Secret") ||
		object.APIVersion != "v1" || object.Kind != request.Kind.Kind ||
		object.Metadata.Namespace != request.Namespace || object.Metadata.Name != request.Name ||
		object.Metadata.Namespace == "" || object.Metadata.Name == "" ||
		object.Metadata.Labels[fleetcontract.WorkerInstanceIDLabel] == "" ||
		object.Metadata.Labels[fleetcontract.WorkerMemberIDLabel] == "" ||
		object.Metadata.Labels[fleetcontract.WorkerIDLabel] != "" ||
		object.Metadata.Labels[fleetcontract.WorkerEpochLabel] != "" ||
		object.Metadata.Labels[fleetcontract.WorkerPoolIDLabel] != "" {
		return ""
	}
	return request.Kind.Kind
}

func protectedWorkerInstancePodMutationRequest(
	request *admissionRequest,
	object objectMetadata,
) (fleet.MutationAuthorizationRequest, error) {
	if object.Metadata.UID == "" {
		return fleet.MutationAuthorizationRequest{}, errors.New("WorkerInstance Pod UID is required")
	}
	operation := fleet.MutationDelete
	switch request.Operation {
	case "DELETE":
		if !hasFinalizer(object.Metadata.Finalizers, fleetcontract.ProtectionFinalizer) {
			return fleet.MutationAuthorizationRequest{}, errors.New("WorkerInstance Pod protection finalizer is absent")
		}
	case "UPDATE":
		oldObject, err := decodeObject(request.OldObject)
		if err != nil || !sameWorkerInstanceAuthority(oldObject, object) ||
			!hasFinalizer(oldObject.Metadata.Finalizers, fleetcontract.ProtectionFinalizer) ||
			hasFinalizer(object.Metadata.Finalizers, fleetcontract.ProtectionFinalizer) ||
			!changesOnlyProtectionFinalizer(request.OldObject, request.Object) {
			return fleet.MutationAuthorizationRequest{}, errors.New("WorkerInstance Pod update is not an exact finalizer removal")
		}
		operation = fleet.MutationRemoveFinalizer
	default:
		return fleet.MutationAuthorizationRequest{}, errors.New("WorkerInstance Pod operation is invalid")
	}
	workerInstanceID, workerErr := uuid.Parse(object.Metadata.Labels[fleetcontract.WorkerInstanceIDLabel])
	workerInstanceEpoch, epochErr := strconv.ParseInt(
		object.Metadata.Labels[fleetcontract.WorkerInstanceEpochLabel], 10, 64,
	)
	planID, planErr := uuid.Parse(object.Metadata.Labels[fleetcontract.ResidencyPlanRevisionIDLabel])
	bundleID, bundleErr := uuid.Parse(object.Metadata.Labels[fleetcontract.WorkerBundleIDLabel])
	memberID, memberErr := uuid.Parse(object.Metadata.Labels[fleetcontract.WorkerMemberIDLabel])
	if workerErr != nil || workerInstanceID == uuid.Nil || epochErr != nil || workerInstanceEpoch <= 0 ||
		planErr != nil || planID == uuid.Nil || bundleErr != nil || bundleID == uuid.Nil ||
		memberErr != nil || memberID == uuid.Nil {
		return fleet.MutationAuthorizationRequest{}, errors.New("WorkerInstance Pod authority labels are invalid")
	}
	target, err := canonicalMutationTarget(request)
	if err != nil {
		return fleet.MutationAuthorizationRequest{}, err
	}
	normalized := normalizedMutation{
		Operation: operation, KubernetesUID: object.Metadata.UID,
		Namespace: object.Metadata.Namespace, Name: object.Metadata.Name,
		WorkerInstanceID: workerInstanceID, WorkerInstanceEpoch: workerInstanceEpoch,
		ResidencyPlanRevisionID: planID, WorkerBundleID: bundleID, WorkerMemberID: memberID,
		Target: target,
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return fleet.MutationAuthorizationRequest{}, err
	}
	digest := sha256.Sum256(encoded)
	return fleet.MutationAuthorizationRequest{
		// FleetUsername is checked before this request is constructed. The
		// authenticated Fleet RPC server supplies the durable audit identity.
		RequestUID: request.UID,
		Operation:  operation, KubernetesUID: object.Metadata.UID,
		Namespace: object.Metadata.Namespace, Name: object.Metadata.Name,
		WorkerInstanceID: workerInstanceID, WorkerInstanceEpoch: workerInstanceEpoch,
		ResidencyPlanRevisionID: planID, WorkerBundleID: bundleID, WorkerMemberID: memberID,
		RequestDigest: digest[:],
	}, nil
}

func sameWorkerInstanceAuthority(oldObject, newObject objectMetadata) bool {
	if oldObject.APIVersion != newObject.APIVersion || oldObject.Kind != newObject.Kind ||
		oldObject.Metadata.UID != newObject.Metadata.UID ||
		oldObject.Metadata.Namespace != newObject.Metadata.Namespace ||
		oldObject.Metadata.Name != newObject.Metadata.Name {
		return false
	}
	for _, label := range []string{
		fleetcontract.ProtectedLabel,
		fleetcontract.WorkerInstanceIDLabel,
		fleetcontract.WorkerInstanceEpochLabel,
		fleetcontract.ResidencyPlanRevisionIDLabel,
		fleetcontract.WorkerBundleIDLabel,
		fleetcontract.WorkerMemberIDLabel,
	} {
		if oldObject.Metadata.Labels[label] != newObject.Metadata.Labels[label] {
			return false
		}
	}
	return true
}

func canonicalMutationTarget(request *admissionRequest) (any, error) {
	var oldObject any
	if len(request.OldObject) > 0 && string(request.OldObject) != "null" {
		if err := json.Unmarshal(request.OldObject, &oldObject); err != nil {
			return nil, err
		}
	}
	var object any
	if request.Operation != "DELETE" && len(request.Object) > 0 && string(request.Object) != "null" {
		if err := json.Unmarshal(request.Object, &object); err != nil {
			return nil, err
		}
	}
	return struct {
		Operation string `json:"operation"`
		OldObject any    `json:"old_object,omitempty"`
		Object    any    `json:"object,omitempty"`
	}{Operation: request.Operation, OldObject: oldObject, Object: object}, nil
}

func hasFinalizer(finalizers []string, expected string) bool {
	for _, finalizer := range finalizers {
		if finalizer == expected {
			return true
		}
	}
	return false
}

func changesOnlyProtectionFinalizer(oldRaw, newRaw json.RawMessage) bool {
	var oldState, newState map[string]any
	if json.Unmarshal(oldRaw, &oldState) != nil || json.Unmarshal(newRaw, &newState) != nil {
		return false
	}
	oldMetadata, oldOK := oldState["metadata"].(map[string]any)
	newMetadata, newOK := newState["metadata"].(map[string]any)
	if !oldOK || !newOK {
		return false
	}
	oldFinalizers, oldOK := stringSlice(oldMetadata["finalizers"])
	newFinalizers, newOK := stringSlice(newMetadata["finalizers"])
	if !oldOK || !newOK {
		return false
	}
	expected := make([]string, 0, len(oldFinalizers)-1)
	removed := 0
	for _, finalizer := range oldFinalizers {
		if finalizer == fleetcontract.ProtectionFinalizer {
			removed++
			continue
		}
		expected = append(expected, finalizer)
	}
	if removed != 1 || !reflect.DeepEqual(expected, newFinalizers) {
		return false
	}
	delete(oldMetadata, "finalizers")
	delete(newMetadata, "finalizers")
	// The API server updates field ownership while applying a finalizer patch.
	// It is server bookkeeping, not a caller-controlled workload mutation.
	delete(oldMetadata, "managedFields")
	delete(newMetadata, "managedFields")
	return reflect.DeepEqual(oldState, newState)
}

func stringSlice(value any) ([]string, bool) {
	if value == nil {
		return []string{}, true
	}
	values, ok := value.([]any)
	if !ok {
		return nil, false
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		text, ok := value.(string)
		if !ok {
			return nil, false
		}
		result = append(result, text)
	}
	return result, true
}

func writeAdmissionReview(writer http.ResponseWriter, uid string, allowed bool, message string) {
	response := &admissionResponse{UID: uid, Allowed: allowed}
	if message != "" {
		response.Status = &responseStatus{Message: message}
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(admissionReview{
		APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview", Response: response,
	})
}

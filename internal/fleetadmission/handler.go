package fleetadmission

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontract"
	corev1 "k8s.io/api/core/v1"
)

const (
	protectedLabel      = fleetcontract.ProtectedLabel
	workerPoolLabel     = fleetcontract.WorkerPoolIDLabel
	fleetRevisionLabel  = fleetcontract.FleetRevisionLabel
	workerIDLabel       = fleetcontract.WorkerIDLabel
	workerEpochLabel    = fleetcontract.WorkerEpochLabel
	drainIDsAnnotation  = fleetcontract.DrainOperationIDsAnnotation
	protectionFinalizer = fleetcontract.ProtectionFinalizer
	identityBindingGate = fleetcontract.IdentityBindingSchedulingGate
	maximumReviewBytes  = 2 << 20
)

type MutationAuthorizer interface {
	AuthorizeMutation(
		context.Context,
		fleet.MutationAuthorizationRequest,
	) (fleet.MutationAuthorizationResult, error)
}

type ProtectedPodCreateValidator interface {
	ValidateProtectedPodCreate(context.Context, corev1.Pod) error
}

type Config struct {
	FleetUsername         string
	PodControllerUsername string
	PodCreateValidator    ProtectedPodCreateValidator
}

type Handler struct {
	authorizer            MutationAuthorizer
	fleetUsername         string
	podControllerUsername string
	podCreateValidator    ProtectedPodCreateValidator
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

type schedulingGate struct {
	Name string `json:"name"`
}

type nodeSelectorRequirement struct {
	Key      string   `json:"key"`
	Operator string   `json:"operator"`
	Values   []string `json:"values"`
}

type nodeSelectorTerm struct {
	MatchExpressions []nodeSelectorRequirement `json:"matchExpressions"`
	MatchFields      []nodeSelectorRequirement `json:"matchFields"`
}

type nodeSelector struct {
	NodeSelectorTerms []nodeSelectorTerm `json:"nodeSelectorTerms"`
}

type nodeAffinity struct {
	RequiredDuringSchedulingIgnoredDuringExecution nodeSelector `json:"requiredDuringSchedulingIgnoredDuringExecution"`
}

type podAffinity struct {
	NodeAffinity nodeAffinity `json:"nodeAffinity"`
}

type protectedObject struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		UID             string            `json:"uid"`
		Namespace       string            `json:"namespace"`
		Name            string            `json:"name"`
		Labels          map[string]string `json:"labels"`
		Annotations     map[string]string `json:"annotations"`
		Finalizers      []string          `json:"finalizers"`
		OwnerReferences []struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Name       string `json:"name"`
			UID        string `json:"uid"`
			Controller bool   `json:"controller"`
		} `json:"ownerReferences"`
	} `json:"metadata"`
	Spec struct {
		Revision       string            `json:"revision"`
		WorkerProfile  string            `json:"workerProfile"`
		DaemonSetName  string            `json:"daemonSetName"`
		NodeSelector   map[string]string `json:"nodeSelector"`
		CapacityPolicy struct {
			WorkerHighWatermarkBytes int64 `json:"workerHighWatermarkBytes"`
			WorkerLowWatermarkBytes  int64 `json:"workerLowWatermarkBytes"`
			WorkerCriticalFreeBytes  int64 `json:"workerCriticalFreeBytes"`
			PoolHighWatermarkBytes   int64 `json:"poolHighWatermarkBytes"`
			PoolLowWatermarkBytes    int64 `json:"poolLowWatermarkBytes"`
			ObservationMaxAgeSeconds int64 `json:"observationMaxAgeSeconds"`
		} `json:"capacityPolicy"`
		NodeName                     string           `json:"nodeName"`
		SchedulingGates              []schedulingGate `json:"schedulingGates"`
		Affinity                     podAffinity      `json:"affinity"`
		ServiceAccountName           string           `json:"serviceAccountName"`
		AutomountServiceAccountToken *bool            `json:"automountServiceAccountToken"`
		Tolerations                  []struct {
			Key      string `json:"key"`
			Operator string `json:"operator"`
			Value    string `json:"value"`
			Effect   string `json:"effect"`
		} `json:"tolerations"`
		Containers []struct {
			Name      string `json:"name"`
			Image     string `json:"image"`
			Resources struct {
				Requests map[string]string `json:"requests"`
				Limits   map[string]string `json:"limits"`
			} `json:"resources"`
		} `json:"containers"`
		UpdateStrategy struct {
			Type string `json:"type"`
		} `json:"updateStrategy"`
		Selector struct {
			MatchLabels map[string]string `json:"matchLabels"`
		} `json:"selector"`
		Template struct {
			Metadata struct {
				Labels map[string]string `json:"labels"`
			} `json:"metadata"`
			Spec struct {
				NodeSelector    map[string]string `json:"nodeSelector"`
				SchedulingGates []schedulingGate  `json:"schedulingGates"`
				Tolerations     []struct {
					Key      string `json:"key"`
					Operator string `json:"operator"`
					Value    string `json:"value"`
					Effect   string `json:"effect"`
				} `json:"tolerations"`
				Containers []struct {
					Name      string `json:"name"`
					Image     string `json:"image"`
					Resources struct {
						Requests map[string]string `json:"requests"`
						Limits   map[string]string `json:"limits"`
					} `json:"resources"`
				} `json:"containers"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
}

type normalizedMutation struct {
	ResourceKind      fleet.ProtectedResourceKind `json:"resource_kind"`
	Operation         fleet.MutationOperation     `json:"operation"`
	KubernetesUID     string                      `json:"kubernetes_uid"`
	Namespace         string                      `json:"namespace"`
	Name              string                      `json:"name"`
	WorkerPoolID      uuid.UUID                   `json:"worker_pool_id"`
	WorkerID          uuid.UUID                   `json:"worker_id"`
	WorkerEpoch       int64                       `json:"worker_epoch"`
	DrainOperationIDs []uuid.UUID                 `json:"drain_operation_ids"`
	FleetRevision     string                      `json:"fleet_revision"`
	TargetState       string                      `json:"target_state"`
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
	if config.PodControllerUsername != "" &&
		strings.TrimSpace(config.PodControllerUsername) != config.PodControllerUsername {
		return nil, errors.New("pod controller Kubernetes username is invalid")
	}
	return &Handler{
		authorizer: authorizer, fleetUsername: config.FleetUsername,
		podControllerUsername: config.PodControllerUsername,
		podCreateValidator:    config.PodCreateValidator,
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
	protected := object.Metadata.Labels[protectedLabel] == "true"
	identityBinding := false
	if review.Request.Operation == "UPDATE" {
		oldObject, oldErr := decodeAdmissionObject(review.Request.OldObject)
		if oldErr != nil {
			writeAdmissionReview(writer, review.Request.UID, false, "protected resource metadata is invalid")
			return
		}
		oldProtected := oldObject.Metadata.Labels[protectedLabel] == "true"
		identityBinding = oldProtected && workerIdentityBindingOnlyUpdate(
			review.Request,
			oldObject,
			object,
		)
		if oldProtected && !identityBinding && !preservesOwnershipIdentity(oldObject, object) {
			writeAdmissionReview(writer, review.Request.UID, false, "protected resource ownership cannot be changed")
			return
		}
		protected = protected || oldProtected
	}
	authorizedActor := review.Request.UserInfo.Username == handler.fleetUsername
	if protected && review.Request.Operation == "CREATE" &&
		review.Request.Kind.Group == "" && review.Request.Kind.Version == "v1" &&
		review.Request.Kind.Kind == "Pod" && handler.podControllerUsername != "" &&
		review.Request.UserInfo.Username == handler.podControllerUsername {
		authorizedActor = true
	}
	if protected && !authorizedActor {
		writeAdmissionReview(writer, review.Request.UID, false, "protected resources are owned by the Fleet Controller")
		return
	}
	if protected {
		if review.Request.Operation == "CREATE" {
			if !validProtectedCreate(request.Context(), review.Request, object, handler.podCreateValidator) {
				writeAdmissionReview(writer, review.Request.UID, false, "protected resource create shape is invalid")
				return
			}
			writeAdmissionReview(writer, review.Request.UID, true, "")
			return
		}
		if identityBinding {
			writeAdmissionReview(writer, review.Request.UID, true, "")
			return
		}
		if drainIdentityOnlyUpdate(review.Request, object) {
			writeAdmissionReview(writer, review.Request.UID, true, "")
			return
		}
		mutation, mutationErr := protectedMutationRequest(review.Request, object)
		if mutationErr != nil {
			writeAdmissionReview(writer, review.Request.UID, false, "protected resource mutation is invalid")
			return
		}
		result, authorizationErr := handler.authorizer.AuthorizeMutation(request.Context(), mutation)
		if authorizationErr != nil || !result.Authorized || result.RequestUID != review.Request.UID {
			writeAdmissionReview(writer, review.Request.UID, false, "protected mutation authorization was denied")
			return
		}
		writeAdmissionReview(writer, review.Request.UID, true, "")
		return
	}
	writeAdmissionReview(writer, review.Request.UID, true, "")
}

func workerIdentityBindingOnlyUpdate(
	request *admissionRequest,
	oldObject protectedObject,
	newObject protectedObject,
) bool {
	if request.Operation != "UPDATE" || request.Kind.Group != "" ||
		request.Kind.Version != "v1" || request.Kind.Kind != "Pod" ||
		oldObject.APIVersion != "v1" || oldObject.Kind != "Pod" ||
		newObject.APIVersion != "v1" || newObject.Kind != "Pod" ||
		oldObject.Metadata.UID == "" || oldObject.Metadata.Namespace != request.Namespace ||
		oldObject.Metadata.Name != request.Name ||
		oldObject.Metadata.Labels[workerIDLabel] != "" ||
		oldObject.Metadata.Labels[workerEpochLabel] != "" ||
		len(newObject.Spec.SchedulingGates) != 0 || !validH3WorkerPodShape(oldObject) {
		return false
	}
	workerID, workerErr := uuid.Parse(newObject.Metadata.Labels[workerIDLabel])
	workerEpoch, epochErr := strconv.ParseInt(newObject.Metadata.Labels[workerEpochLabel], 10, 64)
	if workerErr != nil || workerID == uuid.Nil || epochErr != nil || workerEpoch <= 0 {
		return false
	}
	oldNode, oldNodeErr := targetNodeIdentity(oldObject)
	newNode, newNodeErr := targetNodeIdentity(newObject)
	if oldNodeErr != nil || newNodeErr != nil || oldNode != newNode {
		return false
	}

	var oldState map[string]any
	if err := json.Unmarshal(request.OldObject, &oldState); err != nil {
		return false
	}
	var newState map[string]any
	if err := json.Unmarshal(request.Object, &newState); err != nil {
		return false
	}
	if !stripWorkerIdentityBindingState(oldState, false) ||
		!stripWorkerIdentityBindingState(newState, true) {
		return false
	}
	return reflect.DeepEqual(oldState, newState)
}

func stripWorkerIdentityBindingState(state map[string]any, bound bool) bool {
	metadata, ok := state["metadata"].(map[string]any)
	if !ok {
		return false
	}
	labels, ok := metadata["labels"].(map[string]any)
	if !ok {
		return false
	}
	_, workerPresent := labels[workerIDLabel]
	_, epochPresent := labels[workerEpochLabel]
	if bound != workerPresent || bound != epochPresent {
		return false
	}
	delete(labels, workerIDLabel)
	delete(labels, workerEpochLabel)

	spec, ok := state["spec"].(map[string]any)
	if !ok {
		return false
	}
	gatesValue, gatesPresent := spec["schedulingGates"]
	if bound {
		if gatesPresent {
			gates, ok := gatesValue.([]any)
			if !ok || len(gates) != 0 {
				return false
			}
			delete(spec, "schedulingGates")
		}
		return true
	}
	if !gatesPresent {
		return false
	}
	gates, ok := gatesValue.([]any)
	if !ok || len(gates) != 1 {
		return false
	}
	gate, ok := gates[0].(map[string]any)
	if !ok || len(gate) != 1 || gate["name"] != identityBindingGate {
		return false
	}
	delete(spec, "schedulingGates")
	return true
}

func drainIdentityOnlyUpdate(request *admissionRequest, object protectedObject) bool {
	if request.Operation != "UPDATE" || !validDrainIntentObject(request, object) {
		return false
	}
	var oldState map[string]any
	if err := json.Unmarshal(request.OldObject, &oldState); err != nil {
		return false
	}
	var newState map[string]any
	if err := json.Unmarshal(request.Object, &newState); err != nil {
		return false
	}
	oldDrain, oldPresent, ok := stripDrainIdentity(oldState)
	if !ok {
		return false
	}
	newDrain, newPresent, ok := stripDrainIdentity(newState)
	if !ok || !newPresent || oldPresent && oldDrain == newDrain {
		return false
	}
	if drainIDs, err := parseDrainOperationIDs(newDrain); err != nil || len(drainIDs) == 0 {
		return false
	}
	return reflect.DeepEqual(oldState, newState)
}

func validDrainIntentObject(request *admissionRequest, object protectedObject) bool {
	if object.Metadata.UID == "" || object.Metadata.Namespace == "" || object.Metadata.Name == "" ||
		object.Metadata.Namespace != request.Namespace || object.Metadata.Name != request.Name ||
		object.Metadata.Labels[protectedLabel] != "true" ||
		!hasFinalizer(object, protectionFinalizer) ||
		!validSHA256Label(object.Metadata.Labels[fleetRevisionLabel]) {
		return false
	}
	workerPoolID, err := uuid.Parse(object.Metadata.Labels[workerPoolLabel])
	if err != nil || workerPoolID == uuid.Nil {
		return false
	}
	switch {
	case request.Kind.Group == "" && request.Kind.Version == "v1" && request.Kind.Kind == "Pod" &&
		object.APIVersion == "v1" && object.Kind == "Pod":
		workerID, workerErr := uuid.Parse(object.Metadata.Labels[workerIDLabel])
		workerEpoch, epochErr := strconv.ParseInt(object.Metadata.Labels[workerEpochLabel], 10, 64)
		return workerErr == nil && workerID != uuid.Nil && epochErr == nil && workerEpoch > 0
	case request.Kind.Group == "apps" && request.Kind.Version == "v1" &&
		request.Kind.Kind == "DaemonSet" && object.APIVersion == "apps/v1" &&
		object.Kind == "DaemonSet":
		images, imageErr := daemonSetImages(object)
		return object.Metadata.Labels[workerIDLabel] == "" &&
			object.Metadata.Labels[workerEpochLabel] == "" && validDaemonSetShape(object) &&
			imageErr == nil && allImagesPinned(images)
	case request.Kind.Group == "fleet.vela.ai" && request.Kind.Version == "v1alpha1" &&
		request.Kind.Kind == "WorkerPool" && object.APIVersion == "fleet.vela.ai/v1alpha1" &&
		object.Kind == "WorkerPool":
		return object.Metadata.Labels[workerIDLabel] == "" &&
			object.Metadata.Labels[workerEpochLabel] == "" && validWorkerPoolShape(object) &&
			object.Spec.Revision == object.Metadata.Labels[fleetRevisionLabel]
	default:
		return false
	}
}

func stripDrainIdentity(state map[string]any) (string, bool, bool) {
	metadata, ok := state["metadata"].(map[string]any)
	if !ok {
		return "", false, false
	}
	annotationsValue, annotationsPresent := metadata["annotations"]
	if !annotationsPresent {
		return "", false, true
	}
	annotations, ok := annotationsValue.(map[string]any)
	if !ok {
		return "", false, false
	}
	drainValue, drainPresent := annotations[drainIDsAnnotation]
	if !drainPresent {
		if len(annotations) == 0 {
			delete(metadata, "annotations")
		}
		return "", false, true
	}
	drain, ok := drainValue.(string)
	if !ok {
		return "", false, false
	}
	delete(annotations, drainIDsAnnotation)
	if len(annotations) == 0 {
		delete(metadata, "annotations")
	}
	return drain, true, true
}

func validProtectedCreate(
	ctx context.Context,
	request *admissionRequest,
	object protectedObject,
	podValidator ProtectedPodCreateValidator,
) bool {
	if object.Metadata.UID != "" || object.Metadata.Namespace == "" || object.Metadata.Name == "" ||
		object.Metadata.Namespace != request.Namespace || object.Metadata.Name != request.Name ||
		object.Metadata.Labels[protectedLabel] != "true" ||
		object.Metadata.Annotations[drainIDsAnnotation] != "" ||
		!hasFinalizer(object, protectionFinalizer) ||
		!validSHA256Label(object.Metadata.Labels[fleetRevisionLabel]) {
		return false
	}
	workerPoolID, err := uuid.Parse(object.Metadata.Labels[workerPoolLabel])
	if err != nil || workerPoolID == uuid.Nil {
		return false
	}
	switch {
	case request.Kind.Group == "apps" && request.Kind.Version == "v1" &&
		request.Kind.Kind == "DaemonSet" && object.APIVersion == "apps/v1" &&
		object.Kind == "DaemonSet":
		if object.Metadata.Labels[workerIDLabel] != "" ||
			object.Metadata.Labels[workerEpochLabel] != "" || !validDaemonSetShape(object) {
			return false
		}
		images, err := daemonSetImages(object)
		if err != nil {
			return false
		}
		for _, image := range images {
			if !validPinnedImage(image) {
				return false
			}
		}
		return true
	case request.Kind.Group == "fleet.vela.ai" && request.Kind.Version == "v1alpha1" &&
		request.Kind.Kind == "WorkerPool" && object.APIVersion == "fleet.vela.ai/v1alpha1" &&
		object.Kind == "WorkerPool":
		return object.Metadata.Labels[workerIDLabel] == "" &&
			object.Metadata.Labels[workerEpochLabel] == "" && validWorkerPoolShape(object) &&
			object.Spec.Revision == object.Metadata.Labels[fleetRevisionLabel]
	case request.Kind.Group == "" && request.Kind.Version == "v1" &&
		request.Kind.Kind == "Pod" && object.APIVersion == "v1" && object.Kind == "Pod":
		if object.Metadata.Labels[workerIDLabel] != "" ||
			object.Metadata.Labels[workerEpochLabel] != "" ||
			!validH3WorkerPodShape(object) || podValidator == nil {
			return false
		}
		var pod corev1.Pod
		if err := json.Unmarshal(request.Object, &pod); err != nil {
			return false
		}
		return podValidator.ValidateProtectedPodCreate(ctx, pod) == nil
	default:
		return false
	}
}

func validH3WorkerPodShape(object protectedObject) bool {
	if object.Spec.ServiceAccountName != "vela-worker" ||
		object.Spec.AutomountServiceAccountToken == nil ||
		*object.Spec.AutomountServiceAccountToken ||
		object.Spec.NodeName != "" ||
		len(object.Spec.SchedulingGates) != 1 ||
		object.Spec.SchedulingGates[0].Name != identityBindingGate ||
		object.Spec.NodeSelector["vela.ai/worker-profile"] != "h3" ||
		object.Spec.NodeSelector["vela.ai/worker-pool"] == "" {
		return false
	}
	if _, err := targetNodeIdentity(object); err != nil {
		return false
	}
	if len(object.Metadata.OwnerReferences) != 1 {
		return false
	}
	owner := object.Metadata.OwnerReferences[0]
	if owner.APIVersion != "apps/v1" || owner.Kind != "DaemonSet" || owner.Name == "" ||
		owner.UID == "" || !owner.Controller {
		return false
	}
	hasH3Toleration := false
	for _, toleration := range object.Spec.Tolerations {
		if toleration.Key == "vela.ai/h3" && toleration.Operator == "Equal" &&
			toleration.Value == "true" && toleration.Effect == "NoSchedule" {
			hasH3Toleration = true
		}
	}
	if !hasH3Toleration {
		return false
	}
	runnerCount := 0
	for _, container := range object.Spec.Containers {
		if !validPinnedImage(container.Image) {
			return false
		}
		if container.Name == "h3-runner" {
			runnerCount++
			if container.Resources.Requests["nvidia.com/gpu"] != "8" ||
				container.Resources.Limits["nvidia.com/gpu"] != "8" {
				return false
			}
		}
	}
	return runnerCount == 1
}

func targetNodeIdentity(object protectedObject) (string, error) {
	terms := object.Spec.Affinity.NodeAffinity.
		RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 1 || len(terms[0].MatchExpressions) != 0 ||
		len(terms[0].MatchFields) != 1 {
		return "", errors.New("protected Worker Pod target Node affinity is invalid")
	}
	requirement := terms[0].MatchFields[0]
	if requirement.Key != "metadata.name" || requirement.Operator != "In" ||
		len(requirement.Values) != 1 || requirement.Values[0] == "" ||
		len(requirement.Values[0]) > 253 ||
		strings.TrimSpace(requirement.Values[0]) != requirement.Values[0] {
		return "", errors.New("protected Worker Pod target Node identity is invalid")
	}
	return requirement.Values[0], nil
}

func admissionObject(request *admissionRequest) (protectedObject, error) {
	raw := request.Object
	if request.Operation == "DELETE" {
		raw = request.OldObject
	}
	if len(raw) == 0 || string(raw) == "null" {
		return protectedObject{}, errors.New("admission review object is required")
	}
	return decodeAdmissionObject(raw)
}

func decodeAdmissionObject(raw json.RawMessage) (protectedObject, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return protectedObject{}, errors.New("admission review object is required")
	}
	var object protectedObject
	if err := json.Unmarshal(raw, &object); err != nil {
		return protectedObject{}, err
	}
	return object, nil
}

func preservesOwnershipIdentity(oldObject, newObject protectedObject) bool {
	if oldObject.APIVersion != newObject.APIVersion || oldObject.Kind != newObject.Kind ||
		oldObject.Metadata.UID != newObject.Metadata.UID ||
		oldObject.Metadata.Namespace != newObject.Metadata.Namespace ||
		oldObject.Metadata.Name != newObject.Metadata.Name {
		return false
	}
	for _, label := range []string{
		protectedLabel, workerPoolLabel, fleetRevisionLabel, workerIDLabel, workerEpochLabel,
	} {
		if oldObject.Metadata.Labels[label] != newObject.Metadata.Labels[label] {
			return false
		}
	}
	return true
}

func protectedMutationRequest(
	request *admissionRequest,
	object protectedObject,
) (fleet.MutationAuthorizationRequest, error) {
	if request.Kind.Group == "apps" && request.Kind.Version == "v1" &&
		request.Kind.Kind == "DaemonSet" {
		return protectedDaemonSetMutationRequest(request, object)
	}
	if request.Kind.Group == "fleet.vela.ai" && request.Kind.Version == "v1alpha1" &&
		request.Kind.Kind == "WorkerPool" {
		return protectedWorkerPoolMutationRequest(request, object)
	}
	if request.Kind.Group != "" || request.Kind.Version != "v1" || request.Kind.Kind != "Pod" ||
		object.APIVersion != "v1" || object.Kind != "Pod" ||
		object.Metadata.UID == "" || object.Metadata.Namespace == "" ||
		object.Metadata.Name == "" || object.Metadata.Namespace != request.Namespace ||
		object.Metadata.Name != request.Name {
		return fleet.MutationAuthorizationRequest{}, errors.New("protected Pod identity is invalid")
	}
	operation := fleet.MutationDelete
	switch request.Operation {
	case "DELETE":
		if !hasFinalizer(object, protectionFinalizer) {
			return fleet.MutationAuthorizationRequest{}, errors.New("protected Pod finalizer is absent")
		}
	case "UPDATE":
		oldObject, err := decodeAdmissionObject(request.OldObject)
		if err != nil || !hasFinalizer(oldObject, protectionFinalizer) ||
			hasFinalizer(object, protectionFinalizer) ||
			!changesOnlyProtectionFinalizer(request.OldObject, request.Object) {
			return fleet.MutationAuthorizationRequest{}, errors.New("protected Pod update is not authorized")
		}
		operation = fleet.MutationRemoveFinalizer
	default:
		return fleet.MutationAuthorizationRequest{}, errors.New("protected Pod operation is invalid")
	}
	workerPoolID, err := uuid.Parse(object.Metadata.Labels[workerPoolLabel])
	if err != nil || workerPoolID == uuid.Nil {
		return fleet.MutationAuthorizationRequest{}, errors.New("worker pool label is invalid")
	}
	workerID, err := uuid.Parse(object.Metadata.Labels[workerIDLabel])
	if err != nil || workerID == uuid.Nil {
		return fleet.MutationAuthorizationRequest{}, errors.New("worker id label is invalid")
	}
	workerEpoch, err := strconv.ParseInt(object.Metadata.Labels[workerEpochLabel], 10, 64)
	if err != nil || workerEpoch <= 0 {
		return fleet.MutationAuthorizationRequest{}, errors.New("worker epoch label is invalid")
	}
	fleetRevision := object.Metadata.Labels[fleetRevisionLabel]
	if !validSHA256Label(fleetRevision) {
		return fleet.MutationAuthorizationRequest{}, errors.New("fleet revision label is invalid")
	}
	drainIDs, err := parseDrainOperationIDs(object.Metadata.Annotations[drainIDsAnnotation])
	if err != nil || len(drainIDs) != 1 {
		return fleet.MutationAuthorizationRequest{}, errors.New("drain operation annotation is invalid")
	}
	normalized := normalizedMutation{
		ResourceKind: fleet.ProtectedPod, Operation: operation,
		KubernetesUID: object.Metadata.UID, Namespace: object.Metadata.Namespace,
		Name: object.Metadata.Name, WorkerPoolID: workerPoolID, WorkerID: workerID,
		WorkerEpoch: workerEpoch, DrainOperationIDs: drainIDs, FleetRevision: fleetRevision,
	}
	return buildMutationAuthorizationRequest(request, normalized)
}

func protectedDaemonSetMutationRequest(
	request *admissionRequest,
	object protectedObject,
) (fleet.MutationAuthorizationRequest, error) {
	if object.APIVersion != "apps/v1" || object.Kind != "DaemonSet" ||
		object.Metadata.UID == "" || object.Metadata.Namespace == "" ||
		object.Metadata.Name == "" || object.Metadata.Namespace != request.Namespace ||
		object.Metadata.Name != request.Name || !validDaemonSetShape(object) {
		return fleet.MutationAuthorizationRequest{}, errors.New("protected DaemonSet shape is invalid")
	}
	workerPoolID, err := uuid.Parse(object.Metadata.Labels[workerPoolLabel])
	if err != nil || workerPoolID == uuid.Nil || object.Metadata.Labels[workerIDLabel] != "" ||
		object.Metadata.Labels[workerEpochLabel] != "" {
		return fleet.MutationAuthorizationRequest{}, errors.New("daemonSet Worker pool identity is invalid")
	}
	fleetRevision := object.Metadata.Labels[fleetRevisionLabel]
	if !validSHA256Label(fleetRevision) {
		return fleet.MutationAuthorizationRequest{}, errors.New("fleet revision label is invalid")
	}
	drainIDs, err := parseDrainOperationIDs(object.Metadata.Annotations[drainIDsAnnotation])
	if err != nil {
		return fleet.MutationAuthorizationRequest{}, errors.New("drain operation annotation is invalid")
	}
	operation := fleet.MutationDelete
	switch request.Operation {
	case "DELETE":
		if !hasFinalizer(object, protectionFinalizer) {
			return fleet.MutationAuthorizationRequest{}, errors.New("protected DaemonSet finalizer is absent")
		}
		images, imageErr := daemonSetImages(object)
		if imageErr != nil || !allImagesPinned(images) {
			return fleet.MutationAuthorizationRequest{}, errors.New("protected DaemonSet image is invalid")
		}
	case "UPDATE":
		oldObject, oldErr := decodeAdmissionObject(request.OldObject)
		if oldErr != nil || !validDaemonSetShape(oldObject) {
			return fleet.MutationAuthorizationRequest{}, errors.New("old protected DaemonSet shape is invalid")
		}
		_, imageErr := daemonSetImages(oldObject)
		if imageErr != nil {
			return fleet.MutationAuthorizationRequest{}, imageErr
		}
		newImages, imageErr := daemonSetImages(object)
		if imageErr != nil {
			return fleet.MutationAuthorizationRequest{}, imageErr
		}
		if !allImagesPinned(newImages) {
			return fleet.MutationAuthorizationRequest{}, errors.New("daemonSet image is not pinned")
		}
		selectorChanged, exactSelectorMutation := classifyDaemonSetSelectorMutation(
			request.OldObject,
			request.Object,
		)
		if selectorChanged && !exactSelectorMutation {
			return fleet.MutationAuthorizationRequest{}, errors.New("daemonSet selector mutation changes additional fields")
		}
		imagesChanged, exactImageMutation := classifyDaemonSetImageMutation(
			request.OldObject,
			request.Object,
		)
		if imagesChanged && !exactImageMutation {
			return fleet.MutationAuthorizationRequest{}, errors.New("daemonSet image mutation changes additional fields")
		}
		finalizerRemoved := hasFinalizer(oldObject, protectionFinalizer) &&
			!hasFinalizer(object, protectionFinalizer) &&
			changesOnlyProtectionFinalizer(request.OldObject, request.Object)
		changeCount := 0
		for _, changed := range []bool{exactSelectorMutation, exactImageMutation, finalizerRemoved} {
			if changed {
				changeCount++
			}
		}
		if changeCount != 1 {
			return fleet.MutationAuthorizationRequest{}, errors.New("daemonSet mutation is ambiguous or unsupported")
		}
		operation = fleet.MutationPatchImage
		if exactSelectorMutation {
			operation = fleet.MutationPatchSelector
		} else if finalizerRemoved {
			operation = fleet.MutationRemoveFinalizer
		}
	default:
		return fleet.MutationAuthorizationRequest{}, errors.New("daemonSet mutation is unsupported")
	}
	normalized := normalizedMutation{
		ResourceKind: fleet.ProtectedDaemonSet, Operation: operation,
		KubernetesUID: object.Metadata.UID, Namespace: object.Metadata.Namespace,
		Name: object.Metadata.Name, WorkerPoolID: workerPoolID,
		DrainOperationIDs: drainIDs, FleetRevision: fleetRevision,
	}
	return buildMutationAuthorizationRequest(request, normalized)
}

func protectedWorkerPoolMutationRequest(
	request *admissionRequest,
	object protectedObject,
) (fleet.MutationAuthorizationRequest, error) {
	if object.APIVersion != "fleet.vela.ai/v1alpha1" || object.Kind != "WorkerPool" ||
		object.Metadata.UID == "" || object.Metadata.Namespace == "" ||
		object.Metadata.Name == "" || object.Metadata.Namespace != request.Namespace ||
		object.Metadata.Name != request.Name || !validWorkerPoolShape(object) {
		return fleet.MutationAuthorizationRequest{}, errors.New("protected WorkerPool shape is invalid")
	}
	workerPoolID, err := uuid.Parse(object.Metadata.Labels[workerPoolLabel])
	if err != nil || workerPoolID == uuid.Nil || object.Metadata.Labels[workerIDLabel] != "" ||
		object.Metadata.Labels[workerEpochLabel] != "" {
		return fleet.MutationAuthorizationRequest{}, errors.New("workerPool identity is invalid")
	}
	fleetRevision := object.Metadata.Labels[fleetRevisionLabel]
	if !validSHA256Label(fleetRevision) || object.Spec.Revision != fleetRevision {
		return fleet.MutationAuthorizationRequest{}, errors.New("workerPool revision is invalid")
	}
	drainIDs, err := parseDrainOperationIDs(object.Metadata.Annotations[drainIDsAnnotation])
	if err != nil {
		return fleet.MutationAuthorizationRequest{}, errors.New("drain operation annotation is invalid")
	}
	operation := fleet.MutationDelete
	switch request.Operation {
	case "DELETE":
		if !hasFinalizer(object, protectionFinalizer) {
			return fleet.MutationAuthorizationRequest{}, errors.New("workerPool finalizer is absent")
		}
	case "UPDATE":
		oldObject, oldErr := decodeAdmissionObject(request.OldObject)
		if oldErr != nil || !validWorkerPoolShape(oldObject) ||
			!hasFinalizer(oldObject, protectionFinalizer) ||
			hasFinalizer(object, protectionFinalizer) ||
			!changesOnlyProtectionFinalizer(request.OldObject, request.Object) {
			return fleet.MutationAuthorizationRequest{}, errors.New("workerPool update is not authorized")
		}
		operation = fleet.MutationRemoveFinalizer
	default:
		return fleet.MutationAuthorizationRequest{}, errors.New("workerPool operation is invalid")
	}
	normalized := normalizedMutation{
		ResourceKind: fleet.ProtectedWorkerPool, Operation: operation,
		KubernetesUID: object.Metadata.UID, Namespace: object.Metadata.Namespace,
		Name: object.Metadata.Name, WorkerPoolID: workerPoolID,
		DrainOperationIDs: drainIDs, FleetRevision: fleetRevision,
	}
	return buildMutationAuthorizationRequest(request, normalized)
}

func buildMutationAuthorizationRequest(
	request *admissionRequest,
	normalized normalizedMutation,
) (fleet.MutationAuthorizationRequest, error) {
	targetState, err := canonicalMutationTarget(request)
	if err != nil {
		return fleet.MutationAuthorizationRequest{}, err
	}
	normalized.TargetState = targetState
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return fleet.MutationAuthorizationRequest{}, err
	}
	digest := sha256.Sum256(encoded)
	return fleet.MutationAuthorizationRequest{
		RequestUID: request.UID, ActorIdentity: request.UserInfo.Username,
		ResourceKind: normalized.ResourceKind, Operation: normalized.Operation,
		KubernetesUID: normalized.KubernetesUID, Namespace: normalized.Namespace,
		Name: normalized.Name, WorkerPoolID: normalized.WorkerPoolID,
		WorkerID: normalized.WorkerID, WorkerEpoch: normalized.WorkerEpoch,
		DrainOperationIDs: normalized.DrainOperationIDs, RequestDigest: digest[:],
	}, nil
}

func validWorkerPoolShape(object protectedObject) bool {
	policy := object.Spec.CapacityPolicy
	return object.Spec.WorkerProfile == "h3" && object.Spec.DaemonSetName != "" &&
		object.Spec.DaemonSetName == object.Metadata.Name &&
		object.Spec.NodeSelector["vela.ai/worker-profile"] == "h3" &&
		object.Spec.NodeSelector["vela.ai/worker-pool"] != "" &&
		policy.WorkerHighWatermarkBytes > 0 && policy.WorkerLowWatermarkBytes >= 0 &&
		policy.WorkerLowWatermarkBytes < policy.WorkerHighWatermarkBytes &&
		policy.WorkerCriticalFreeBytes >= 0 && policy.PoolHighWatermarkBytes > 0 &&
		policy.PoolLowWatermarkBytes >= 0 &&
		policy.PoolLowWatermarkBytes < policy.PoolHighWatermarkBytes &&
		policy.ObservationMaxAgeSeconds >= 10 && policy.ObservationMaxAgeSeconds <= 600
}

func validDaemonSetShape(object protectedObject) bool {
	if object.Spec.UpdateStrategy.Type != "OnDelete" ||
		len(object.Spec.Template.Spec.SchedulingGates) != 1 ||
		object.Spec.Template.Spec.SchedulingGates[0].Name != identityBindingGate ||
		object.Spec.Template.Spec.NodeSelector["vela.ai/worker-profile"] != "h3" ||
		object.Spec.Template.Spec.NodeSelector["vela.ai/worker-pool"] == "" ||
		!selectorMatchesTemplateLabels(
			object.Spec.Selector.MatchLabels,
			object.Spec.Template.Metadata.Labels,
		) {
		return false
	}
	hasH3Toleration := false
	for _, toleration := range object.Spec.Template.Spec.Tolerations {
		if toleration.Key == "vela.ai/h3" && toleration.Operator == "Equal" &&
			toleration.Value == "true" && toleration.Effect == "NoSchedule" {
			hasH3Toleration = true
		}
	}
	if !hasH3Toleration {
		return false
	}
	runnerCount := 0
	for _, container := range object.Spec.Template.Spec.Containers {
		if container.Name == "h3-runner" {
			runnerCount++
			if container.Resources.Requests["nvidia.com/gpu"] != "8" ||
				container.Resources.Limits["nvidia.com/gpu"] != "8" {
				return false
			}
		}
	}
	return runnerCount == 1
}

func selectorMatchesTemplateLabels(selector, labels map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func daemonSetImages(object protectedObject) (map[string]string, error) {
	images := make(map[string]string, len(object.Spec.Template.Spec.Containers))
	for _, container := range object.Spec.Template.Spec.Containers {
		if container.Name == "" || container.Image == "" {
			return nil, errors.New("daemonSet container image identity is invalid")
		}
		if _, exists := images[container.Name]; exists {
			return nil, errors.New("daemonSet container name is duplicated")
		}
		images[container.Name] = container.Image
	}
	return images, nil
}

func validPinnedImage(image string) bool {
	marker := strings.LastIndex(image, "@sha256:")
	return marker > 0 && validSHA256Label(image[marker+len("@sha256:"):])
}

func allImagesPinned(images map[string]string) bool {
	for _, image := range images {
		if !validPinnedImage(image) {
			return false
		}
	}
	return len(images) > 0
}

func classifyDaemonSetImageMutation(oldRaw, newRaw json.RawMessage) (bool, bool) {
	var oldState, newState map[string]any
	if json.Unmarshal(oldRaw, &oldState) != nil || json.Unmarshal(newRaw, &newState) != nil {
		return false, false
	}
	oldImages, oldOK := stripDaemonSetImages(oldState)
	newImages, newOK := stripDaemonSetImages(newState)
	if !oldOK || !newOK || reflect.DeepEqual(oldImages, newImages) {
		return false, false
	}
	return true, allImagesPinned(newImages) && reflect.DeepEqual(oldState, newState)
}

func classifyDaemonSetSelectorMutation(oldRaw, newRaw json.RawMessage) (bool, bool) {
	var oldState, newState map[string]any
	if json.Unmarshal(oldRaw, &oldState) != nil || json.Unmarshal(newRaw, &newState) != nil {
		return false, false
	}
	oldSelector, oldOK := stripDaemonSetSelectorState(oldState)
	newSelector, newOK := stripDaemonSetSelectorState(newState)
	if !oldOK || !newOK || reflect.DeepEqual(oldSelector, newSelector) {
		return false, false
	}
	return true, reflect.DeepEqual(oldState, newState)
}

func stripDaemonSetSelectorState(state map[string]any) (map[string]any, bool) {
	spec, ok := state["spec"].(map[string]any)
	if !ok {
		return nil, false
	}
	selector, selectorOK := spec["selector"].(map[string]any)
	template, templateOK := spec["template"].(map[string]any)
	if !selectorOK || !templateOK {
		return nil, false
	}
	metadata, metadataOK := template["metadata"].(map[string]any)
	podSpec, podSpecOK := template["spec"].(map[string]any)
	labels, labelsOK := metadata["labels"].(map[string]any)
	nodeSelector, nodeSelectorOK := podSpec["nodeSelector"].(map[string]any)
	tolerations, tolerationsOK := podSpec["tolerations"].([]any)
	if !metadataOK || !podSpecOK || !labelsOK || !nodeSelectorOK || !tolerationsOK {
		return nil, false
	}
	result := map[string]any{
		"selector":       selector,
		"templateLabels": labels,
		"nodeSelector":   nodeSelector,
		"tolerations":    tolerations,
	}
	delete(spec, "selector")
	delete(metadata, "labels")
	delete(podSpec, "nodeSelector")
	delete(podSpec, "tolerations")
	return result, true
}

func stripDaemonSetImages(state map[string]any) (map[string]string, bool) {
	spec, ok := state["spec"].(map[string]any)
	if !ok {
		return nil, false
	}
	template, ok := spec["template"].(map[string]any)
	if !ok {
		return nil, false
	}
	podSpec, ok := template["spec"].(map[string]any)
	if !ok {
		return nil, false
	}
	images := make(map[string]string)
	for _, field := range []string{"initContainers", "containers"} {
		value, present := podSpec[field]
		if !present {
			if field == "containers" {
				return nil, false
			}
			continue
		}
		containers, ok := value.([]any)
		if !ok || field == "containers" && len(containers) == 0 {
			return nil, false
		}
		for _, value := range containers {
			container, ok := value.(map[string]any)
			if !ok {
				return nil, false
			}
			name, nameOK := container["name"].(string)
			image, imageOK := container["image"].(string)
			key := field + "/" + name
			if !nameOK || name == "" || !imageOK || image == "" {
				return nil, false
			}
			if _, duplicate := images[key]; duplicate {
				return nil, false
			}
			images[key] = image
			delete(container, "image")
		}
	}
	return images, true
}

func canonicalMutationTarget(request *admissionRequest) (string, error) {
	var oldObject any
	if len(request.OldObject) > 0 && string(request.OldObject) != "null" {
		if err := json.Unmarshal(request.OldObject, &oldObject); err != nil {
			return "", err
		}
	}
	var object any
	if request.Operation != "DELETE" && len(request.Object) > 0 && string(request.Object) != "null" {
		if err := json.Unmarshal(request.Object, &object); err != nil {
			return "", err
		}
	}
	target := struct {
		Operation string `json:"operation"`
		OldObject any    `json:"old_object,omitempty"`
		Object    any    `json:"object,omitempty"`
	}{Operation: request.Operation, OldObject: oldObject, Object: object}
	encoded, err := json.Marshal(target)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func parseDrainOperationIDs(value string) ([]uuid.UUID, error) {
	parts := strings.Split(value, ",")
	operationIDs := make([]uuid.UUID, 0, len(parts))
	seen := make(map[uuid.UUID]struct{}, len(parts))
	for _, part := range parts {
		if part == "" || strings.TrimSpace(part) != part {
			return nil, errors.New("drain operation id is invalid")
		}
		operationID, err := uuid.Parse(part)
		if err != nil || operationID == uuid.Nil {
			return nil, errors.New("drain operation id is invalid")
		}
		if _, exists := seen[operationID]; exists {
			return nil, errors.New("drain operation id is duplicated")
		}
		seen[operationID] = struct{}{}
		operationIDs = append(operationIDs, operationID)
	}
	sort.Slice(operationIDs, func(left, right int) bool {
		return operationIDs[left].String() < operationIDs[right].String()
	})
	return operationIDs, nil
}

func validSHA256Label(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func hasFinalizer(object protectedObject, expected string) bool {
	for _, finalizer := range object.Metadata.Finalizers {
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
		if finalizer == protectionFinalizer {
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
	return reflect.DeepEqual(oldState, newState)
}

func stringSlice(value any) ([]string, bool) {
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
		APIVersion: "admission.k8s.io/v1",
		Kind:       "AdmissionReview",
		Response:   response,
	})
}

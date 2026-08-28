package fleetcontroller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontract"
	"github.com/vivym/vela/internal/imageref"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	protectedLabel      = fleetcontract.ProtectedLabel
	workerPoolLabel     = fleetcontract.WorkerPoolIDLabel
	fleetRevisionLabel  = fleetcontract.FleetRevisionLabel
	workerIDLabel       = fleetcontract.WorkerIDLabel
	workerEpochLabel    = fleetcontract.WorkerEpochLabel
	protectionFinalizer = fleetcontract.ProtectionFinalizer

	IdentityBindingSchedulingGate = fleetcontract.IdentityBindingSchedulingGate
)

var (
	ErrResourceNotFound         = errors.New("fleet resource not found")
	ErrRetirementUnproven       = errors.New("fleet retirement is not proven by a persisted completion receipt")
	ErrRetirementPending        = errors.New("fleet retirement is waiting for exact Kubernetes resource absence")
	ErrImmutableDesiredRevision = errors.New("immutable Fleet desired revision differs from live resource")
	ErrProtectedResourceDrift   = errors.New("protected Fleet resource differs from desired revision")
	ErrDrainIncomplete          = errors.New("worker drain is not complete for the protected resource")
	ErrWorkerPodIdentityInvalid = errors.New("pending Worker Pod identity is invalid")
	ErrWorkerIdentityConflict   = errors.New("resolved Worker identity conflicts with pending Pod")
	ErrWorkerReadinessConflict  = errors.New("worker readiness result conflicts with pending Pod")
	readinessCycleNamespace     = uuid.MustParse("f40d4e9e-f80d-5640-96d3-daceee2812ed")
)

type ResourceKind string

const (
	ResourcePod        ResourceKind = "POD"
	ResourceDaemonSet  ResourceKind = "DAEMONSET"
	ResourceWorkerPool ResourceKind = "WORKER_POOL"
)

type ResourceKey struct {
	Namespace string
	Name      string
}

type Metadata struct {
	Namespace  string
	Name       string
	Labels     map[string]string
	Finalizers []string
}

type WorkerPoolSpec struct {
	Revision       string
	WorkerProfile  string
	DaemonSetName  string
	NodeSelector   map[string]string
	CapacityPolicy CapacityPolicySpec
}

type WorkerPool struct {
	Metadata Metadata
	Spec     WorkerPoolSpec
}

type DaemonSet struct {
	Metadata       Metadata
	UpdateStrategy string
	Selector       map[string]string
	Template       corev1.PodTemplateSpec
}

type CapacityPolicySpec struct {
	WorkerHighWatermarkBytes int64
	WorkerLowWatermarkBytes  int64
	WorkerCriticalFreeBytes  int64
	PoolHighWatermarkBytes   int64
	PoolLowWatermarkBytes    int64
	ObservationMaxAge        time.Duration
}

type DesiredRevision struct {
	WorkerPoolID               uuid.UUID
	Namespace                  string
	Name                       string
	Revision                   string
	WorkerProfile              string
	DaemonSetName              string
	NodeSelector               map[string]string
	InitImage                  string
	WorkerAgentImage           string
	RunnerImage                string
	WorkerRuntimeConfigMap     string
	RunnerProfilesConfigMap    string
	RunnerGPURolesConfigMap    string
	WorkerControlTLSSecret     string
	ArtifactStoreTLSSecret     string
	ExecutionProfileRevisionID uuid.UUID
	InferenceBackendRevision   string
	ReadinessTimeout           time.Duration
	CapacityPolicy             CapacityPolicySpec
}

type ProtectedResource struct {
	Kind          ResourceKind
	KubernetesUID string
	Namespace     string
	Name          string
	WorkerPoolID  uuid.UUID
	WorkerID      uuid.UUID
	WorkerEpoch   int64
}

type NodeSelectorRequirement struct {
	Key      string
	Operator string
	Values   []string
}

type NodeSelectorTerm struct {
	MatchExpressions []NodeSelectorRequirement
	MatchFields      []NodeSelectorRequirement
}

type WorkerPod struct {
	KubernetesUID        string
	CreatedAt            time.Time
	Metadata             Metadata
	SchedulingGates      []string
	RequiredNodeAffinity []NodeSelectorTerm
}

type WorkerPodIdentityBinding struct {
	KubernetesUID string
	Namespace     string
	Name          string
	WorkerID      uuid.UUID
	WorkerPoolID  uuid.UUID
	WorkerEpoch   int64
	NodeIdentity  string
}

type Resources interface {
	GetWorkerPool(context.Context, ResourceKey) (WorkerPool, error)
	CreateWorkerPool(context.Context, WorkerPool) error
	GetDaemonSet(context.Context, ResourceKey) (DaemonSet, error)
	CreateDaemonSet(context.Context, DaemonSet) error
	BindWorkerIdentity(context.Context, WorkerPodIdentityBinding) error
	AttachDrainOperations(context.Context, ProtectedResource, []uuid.UUID) error
	Delete(context.Context, ProtectedResource) error
	RemoveFinalizer(context.Context, ProtectedResource) error
	IsAbsent(context.Context, ProtectedResource) (bool, error)
}

type DrainCoordinator interface {
	GetDrain(context.Context, uuid.UUID) (fleet.DrainResult, error)
	RequestDrain(context.Context, fleet.DrainRequest) (fleet.DrainResult, error)
	ReconcileDrain(context.Context, uuid.UUID) (fleet.DrainResult, error)
	HasRetirementAuthorization(
		context.Context,
		fleet.RetirementAuthorizationRequest,
	) (bool, error)
	HasRetirementCompletion(
		context.Context,
		fleet.RetirementAuthorizationRequest,
	) (bool, error)
	RecordRetirementCompletion(
		context.Context,
		fleet.RetirementAuthorizationRequest,
	) (fleet.RetirementCompletionResult, error)
}

type IdentityResolver interface {
	ResolveWorkerIdentity(
		context.Context,
		fleet.WorkerIdentityRequest,
	) (fleet.WorkerIdentity, error)
}

type ReadinessStarter interface {
	BeginReadiness(context.Context, fleet.ReadinessRequest) (fleet.ReadinessResult, error)
}

type CapacityPolicyConfigurator interface {
	ConfigureCapacityPolicy(
		context.Context,
		fleet.CapacityPolicy,
	) (fleet.CapacityPolicyResult, error)
}

type ReconcileResult struct {
	WorkerPoolCreated        bool
	DaemonSetCreated         bool
	CapacityPolicyConfigured bool
	Converged                bool
}

type Reconciler struct {
	resources  Resources
	drains     DrainCoordinator
	identities IdentityResolver
	readiness  ReadinessStarter
	capacity   CapacityPolicyConfigurator
}

func NewReconciler(
	resources Resources,
	drains DrainCoordinator,
	identities IdentityResolver,
	readiness ReadinessStarter,
	capacity CapacityPolicyConfigurator,
) (*Reconciler, error) {
	if resources == nil {
		return nil, errors.New("fleet Kubernetes resources are required")
	}
	if drains == nil {
		return nil, errors.New("fleet drain reader is required")
	}
	if identities == nil {
		return nil, errors.New("fleet Worker identity resolver is required")
	}
	if readiness == nil {
		return nil, errors.New("fleet Worker readiness starter is required")
	}
	if capacity == nil {
		return nil, errors.New("fleet capacity policy configurator is required")
	}
	return &Reconciler{
		resources: resources, drains: drains, identities: identities, readiness: readiness,
		capacity: capacity,
	}, nil
}

func (reconciler *Reconciler) BindWorkerPodIdentity(
	ctx context.Context,
	pod WorkerPod,
	desired DesiredRevision,
) (WorkerPodIdentityBinding, error) {
	if reconciler == nil || reconciler.resources == nil || reconciler.identities == nil ||
		reconciler.readiness == nil {
		return WorkerPodIdentityBinding{}, errors.New("fleet reconciler is not configured")
	}
	if err := ValidateDesiredRevision(desired); err != nil {
		return WorkerPodIdentityBinding{}, err
	}
	workerPoolID, nodeIdentity, err := pendingWorkerPodIdentity(pod)
	if err != nil {
		return WorkerPodIdentityBinding{}, err
	}
	if pod.CreatedAt.IsZero() {
		return WorkerPodIdentityBinding{}, ErrWorkerPodIdentityInvalid
	}
	if desired.WorkerPoolID != workerPoolID || desired.Namespace != pod.Metadata.Namespace ||
		desired.Revision != pod.Metadata.Labels[fleetRevisionLabel] {
		return WorkerPodIdentityBinding{}, ErrWorkerIdentityConflict
	}
	identity, err := reconciler.identities.ResolveWorkerIdentity(ctx, fleet.WorkerIdentityRequest{
		NodeIdentity: nodeIdentity, WorkerPoolID: workerPoolID,
		KubernetesUID: pod.KubernetesUID, Namespace: pod.Metadata.Namespace,
		Name: pod.Metadata.Name,
	})
	if err != nil {
		return WorkerPodIdentityBinding{}, fmt.Errorf("resolve pending Worker Pod identity: %w", err)
	}
	if identity.WorkerID == uuid.Nil || identity.WorkerPoolID != workerPoolID ||
		identity.WorkerEpoch <= 0 || identity.NodeIdentity != nodeIdentity {
		return WorkerPodIdentityBinding{}, ErrWorkerIdentityConflict
	}
	binding := WorkerPodIdentityBinding{
		KubernetesUID: pod.KubernetesUID,
		Namespace:     pod.Metadata.Namespace,
		Name:          pod.Metadata.Name,
		WorkerID:      identity.WorkerID,
		WorkerPoolID:  identity.WorkerPoolID,
		WorkerEpoch:   identity.WorkerEpoch,
		NodeIdentity:  identity.NodeIdentity,
	}
	cycleID := uuid.NewSHA1(readinessCycleNamespace, []byte(pod.KubernetesUID))
	readiness, err := reconciler.readiness.BeginReadiness(ctx, fleet.ReadinessRequest{
		CycleID: cycleID, WorkerID: identity.WorkerID, WorkerPoolID: identity.WorkerPoolID,
		WorkerEpoch: identity.WorkerEpoch, NodeIdentity: identity.NodeIdentity,
		ExecutionProfileRevisionID: desired.ExecutionProfileRevisionID,
		InferenceBackendRevision:   desired.InferenceBackendRevision,
		Deadline:                   pod.CreatedAt.UTC().Add(desired.ReadinessTimeout),
	})
	if err != nil {
		return WorkerPodIdentityBinding{}, fmt.Errorf("begin pending Worker readiness: %w", err)
	}
	if readiness.CycleID != cycleID || readiness.State != fleet.ReadinessChecking ||
		readiness.NextCheck != fleet.ReadinessIdentity || readiness.WorkerLifecycle != "WARMING" ||
		readiness.WorkerReachability != "SUSPECT" {
		return WorkerPodIdentityBinding{}, ErrWorkerReadinessConflict
	}
	if err := reconciler.resources.BindWorkerIdentity(ctx, binding); err != nil {
		return WorkerPodIdentityBinding{}, fmt.Errorf("bind pending Worker Pod identity: %w", err)
	}
	return binding, nil
}

func (reconciler *Reconciler) Reconcile(
	ctx context.Context,
	desired DesiredRevision,
) (ReconcileResult, error) {
	if reconciler == nil || reconciler.resources == nil || reconciler.capacity == nil {
		return ReconcileResult{}, errors.New("fleet reconciler is not configured")
	}
	if err := ValidateDesiredRevision(desired); err != nil {
		return ReconcileResult{}, err
	}
	desiredWorkerPool := materializeWorkerPool(desired)
	desiredDaemonSet := materializeDaemonSet(desired)
	result := ReconcileResult{}

	workerPool, err := reconciler.resources.GetWorkerPool(ctx, ResourceKey{
		Namespace: desired.Namespace,
		Name:      desired.Name,
	})
	if errors.Is(err, ErrResourceNotFound) {
		if err := reconciler.resources.CreateWorkerPool(ctx, desiredWorkerPool); err != nil {
			return ReconcileResult{}, fmt.Errorf("create protected WorkerPool: %w", err)
		}
		result.WorkerPoolCreated = true
	} else if err != nil {
		return ReconcileResult{}, fmt.Errorf("get protected WorkerPool: %w", err)
	} else if !reflect.DeepEqual(workerPool, desiredWorkerPool) {
		return ReconcileResult{}, ErrImmutableDesiredRevision
	}

	daemonSet, err := reconciler.resources.GetDaemonSet(ctx, ResourceKey{
		Namespace: desired.Namespace,
		Name:      desired.DaemonSetName,
	})
	if errors.Is(err, ErrResourceNotFound) {
		if err := reconciler.resources.CreateDaemonSet(ctx, desiredDaemonSet); err != nil {
			return ReconcileResult{}, fmt.Errorf("create protected OnDelete DaemonSet: %w", err)
		}
		result.DaemonSetCreated = true
	} else if err != nil {
		return ReconcileResult{}, fmt.Errorf("get protected DaemonSet: %w", err)
	} else if !reflect.DeepEqual(daemonSet, desiredDaemonSet) {
		return ReconcileResult{}, ErrProtectedResourceDrift
	}

	policy := fleet.CapacityPolicy{
		WorkerPoolID: desired.WorkerPoolID, Revision: desired.Revision,
		WorkerHighWatermarkBytes: desired.CapacityPolicy.WorkerHighWatermarkBytes,
		WorkerLowWatermarkBytes:  desired.CapacityPolicy.WorkerLowWatermarkBytes,
		WorkerCriticalFreeBytes:  desired.CapacityPolicy.WorkerCriticalFreeBytes,
		PoolHighWatermarkBytes:   desired.CapacityPolicy.PoolHighWatermarkBytes,
		PoolLowWatermarkBytes:    desired.CapacityPolicy.PoolLowWatermarkBytes,
		ObservationMaxAge:        desired.CapacityPolicy.ObservationMaxAge,
	}
	configured, err := reconciler.capacity.ConfigureCapacityPolicy(ctx, policy)
	if err != nil {
		return result, fmt.Errorf("configure authoritative Fleet capacity policy: %w", err)
	}
	if configured.WorkerPoolID != desired.WorkerPoolID || configured.Revision != desired.Revision {
		return result, ErrImmutableDesiredRevision
	}
	result.CapacityPolicyConfigured = !configured.Replayed
	result.Converged = !result.WorkerPoolCreated && !result.DaemonSetCreated && configured.Replayed
	return result, nil
}

func (reconciler *Reconciler) RetireWorkerPod(
	ctx context.Context,
	pod ProtectedResource,
	drainID uuid.UUID,
) error {
	if reconciler == nil || reconciler.resources == nil || reconciler.drains == nil {
		return errors.New("fleet reconciler is not configured")
	}
	if err := validateProtectedWorkerPod(pod); err != nil {
		return err
	}
	if drainID == uuid.Nil {
		return errors.New("drain operation id is required")
	}
	drain, err := reconciler.drains.GetDrain(ctx, drainID)
	if err != nil {
		return fmt.Errorf("get Worker drain before Pod retirement: %w", err)
	}
	if drain.OperationID != drainID || drain.State != fleet.DrainComplete ||
		drain.WorkerID != pod.WorkerID || drain.WorkerEpoch != pod.WorkerEpoch {
		return ErrDrainIncomplete
	}
	completed, err := reconciler.retireProtectedResource(ctx, pod, []uuid.UUID{drainID})
	if err != nil {
		return err
	}
	if !completed {
		return ErrRetirementPending
	}
	return nil
}

func materializeWorkerPool(desired DesiredRevision) WorkerPool {
	return WorkerPool{
		Metadata: protectedMetadata(desired.Namespace, desired.Name, desired),
		Spec: WorkerPoolSpec{
			Revision:       desired.Revision,
			WorkerProfile:  desired.WorkerProfile,
			DaemonSetName:  desired.DaemonSetName,
			NodeSelector:   cloneMap(desired.NodeSelector),
			CapacityPolicy: desired.CapacityPolicy,
		},
	}
}

func materializeDaemonSet(desired DesiredRevision) DaemonSet {
	selector := map[string]string{"app.kubernetes.io/name": desired.DaemonSetName}
	return DaemonSet{
		Metadata:       protectedMetadata(desired.Namespace, desired.DaemonSetName, desired),
		UpdateStrategy: "OnDelete",
		Selector:       cloneMap(selector),
		Template:       h3WorkerPodTemplate(desired, selector),
	}
}

func protectedMetadata(namespace, name string, desired DesiredRevision) Metadata {
	return Metadata{
		Namespace: namespace,
		Name:      name,
		Labels: map[string]string{
			protectedLabel:     "true",
			workerPoolLabel:    desired.WorkerPoolID.String(),
			fleetRevisionLabel: desired.Revision,
		},
		Finalizers: []string{protectionFinalizer},
	}
}

func ValidateDesiredRevision(desired DesiredRevision) error {
	if desired.WorkerPoolID == uuid.Nil || !validResourceName(desired.Namespace) ||
		!validResourceName(desired.Name) || !validResourceName(desired.DaemonSetName) ||
		desired.WorkerProfile != "h3" || !validSHA256(desired.Revision) ||
		desired.NodeSelector["vela.ai/worker-profile"] != "h3" ||
		!validResourceName(desired.NodeSelector["vela.ai/worker-pool"]) ||
		!validPinnedImage(desired.InitImage) ||
		!validPinnedImage(desired.WorkerAgentImage) || !validPinnedImage(desired.RunnerImage) ||
		!validResourceName(desired.WorkerRuntimeConfigMap) ||
		!validResourceName(desired.RunnerProfilesConfigMap) ||
		!validResourceName(desired.RunnerGPURolesConfigMap) ||
		!validResourceName(desired.WorkerControlTLSSecret) ||
		!validResourceName(desired.ArtifactStoreTLSSecret) ||
		desired.ExecutionProfileRevisionID == uuid.Nil ||
		!validProductionRevision(desired.InferenceBackendRevision, 200) ||
		desired.ReadinessTimeout <= 0 || desired.ReadinessTimeout > 2*time.Hour ||
		!validCapacityPolicySpec(desired.CapacityPolicy) {
		return errors.New("fleet desired revision is invalid")
	}
	return nil
}

func validCapacityPolicySpec(policy CapacityPolicySpec) bool {
	return policy.WorkerHighWatermarkBytes > 0 && policy.WorkerLowWatermarkBytes >= 0 &&
		policy.WorkerLowWatermarkBytes < policy.WorkerHighWatermarkBytes &&
		policy.WorkerCriticalFreeBytes >= 0 && policy.PoolHighWatermarkBytes > 0 &&
		policy.PoolLowWatermarkBytes >= 0 &&
		policy.PoolLowWatermarkBytes < policy.PoolHighWatermarkBytes &&
		policy.ObservationMaxAge >= 10*time.Second &&
		policy.ObservationMaxAge <= 10*time.Minute && policy.ObservationMaxAge%time.Second == 0
}

func validateProtectedWorkerPod(pod ProtectedResource) error {
	if pod.Kind != ResourcePod || pod.KubernetesUID == "" ||
		!validResourceName(pod.Namespace) || !validResourceName(pod.Name) ||
		pod.WorkerPoolID == uuid.Nil || pod.WorkerID == uuid.Nil || pod.WorkerEpoch <= 0 {
		return errors.New("protected Worker Pod identity is invalid")
	}
	return nil
}

func pendingWorkerPodIdentity(pod WorkerPod) (uuid.UUID, string, error) {
	if pod.KubernetesUID == "" || !validResourceName(pod.Metadata.Namespace) ||
		!validResourceName(pod.Metadata.Name) ||
		pod.Metadata.Labels[protectedLabel] != "true" ||
		!validSHA256(pod.Metadata.Labels[fleetRevisionLabel]) ||
		pod.Metadata.Labels[workerIDLabel] != "" ||
		pod.Metadata.Labels[workerEpochLabel] != "" ||
		!containsString(pod.Metadata.Finalizers, protectionFinalizer) ||
		len(pod.SchedulingGates) != 1 ||
		pod.SchedulingGates[0] != IdentityBindingSchedulingGate ||
		len(pod.RequiredNodeAffinity) != 1 {
		return uuid.Nil, "", ErrWorkerPodIdentityInvalid
	}
	workerPoolID, err := uuid.Parse(pod.Metadata.Labels[workerPoolLabel])
	if err != nil || workerPoolID == uuid.Nil {
		return uuid.Nil, "", ErrWorkerPodIdentityInvalid
	}
	term := pod.RequiredNodeAffinity[0]
	if len(term.MatchExpressions) != 0 || len(term.MatchFields) != 1 {
		return uuid.Nil, "", ErrWorkerPodIdentityInvalid
	}
	requirement := term.MatchFields[0]
	if requirement.Key != "metadata.name" || requirement.Operator != "In" ||
		len(requirement.Values) != 1 || !validResourceName(requirement.Values[0]) {
		return uuid.Nil, "", ErrWorkerPodIdentityInvalid
	}
	return workerPoolID, requirement.Values[0], nil
}

func validBoundedText(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, '\x00')
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func validResourceName(value string) bool {
	return len(validation.IsDNS1123Subdomain(value)) == 0 && !containsTemplateMarker(value)
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value ||
		value == strings.Repeat("0", sha256.Size*2) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validPinnedImage(value string) bool {
	return !containsTemplateMarker(value) && imageref.ValidPinned(value)
}

func validProductionRevision(value string, maximum int) bool {
	return validBoundedText(value, maximum) && !containsTemplateMarker(value) &&
		value != strings.Repeat("0", 64) && value != "sha256:"+strings.Repeat("0", 64)
}

func containsTemplateMarker(value string) bool {
	lower := strings.ToLower(value)
	return strings.Contains(lower, "placeholder") || strings.Contains(lower, "replace-with") ||
		strings.Contains(lower, "changeme") || strings.Contains(lower, "todo") ||
		strings.Contains(lower, ".invalid")
}

func cloneMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

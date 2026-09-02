package h3launchevidence_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/h3launchevidence"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCaptureDoubleReadsLiveAuthoritiesBeforeProducingEvidence(t *testing.T) {
	input := exactLaunchInput(t)
	now := time.Now().UTC()
	input.Rollout.ApprovedPlan.ApprovedAt = now.Add(-time.Hour)
	input.Kubernetes.Claims[0].CreationTimestamp = metav1.NewTime(now.Add(-2 * time.Minute))
	input.Kubernetes.Claims[0].Status.Allocation.AllocationTimestamp =
		timePointer(now.Add(-time.Minute))
	kubernetes := kubernetesReaderForInput(input)
	registry := &sequenceRegistryReader{snapshots: []h3launchevidence.RegistrySnapshot{
		input.Registry,
		input.Registry,
	}}

	evidence, err := h3launchevidence.Capture(context.Background(), kubernetes, registry, h3launchevidence.CaptureRequest{
		ReleaseDigest: input.ReleaseDigest, ConfigurationRevision: input.ConfigurationRevision,
		ValidationEnvironment: input.ValidationEnvironment, CollectorIdentity: input.CollectorIdentity,
		Rollout: input.Rollout, ExternalResources: input.ExternalResources,
	})
	if err != nil {
		t.Fatalf("Capture stable live launch: %v", err)
	}
	if registry.calls != 2 || evidence.ResidencyPlanRevisionID != input.Rollout.ApprovedPlan.ID ||
		evidence.RegistryDatabaseTime.IsZero() || evidence.CapturedAt.Before(evidence.RegistryDatabaseTime) {
		t.Fatalf("stable capture evidence=%#v Registry calls=%d", evidence, registry.calls)
	}
}

func TestCaptureRejectsAuthorityDriftAcrossCollectionWindow(t *testing.T) {
	input := exactLaunchInput(t)
	now := time.Now().UTC()
	input.Rollout.ApprovedPlan.ApprovedAt = now.Add(-time.Hour)
	input.Kubernetes.Claims[0].CreationTimestamp = metav1.NewTime(now.Add(-2 * time.Minute))
	input.Kubernetes.Claims[0].Status.Allocation.AllocationTimestamp =
		timePointer(now.Add(-time.Minute))
	drifted := input.Registry
	drifted.Workers = append([]h3launchevidence.RegistryWorker(nil), input.Registry.Workers...)
	drifted.Workers[0].ControlSessionEpoch++
	registry := &sequenceRegistryReader{snapshots: []h3launchevidence.RegistrySnapshot{
		input.Registry,
		drifted,
	}}
	_, err := h3launchevidence.Capture(
		context.Background(),
		kubernetesReaderForInput(input),
		registry,
		h3launchevidence.CaptureRequest{
			ReleaseDigest: input.ReleaseDigest, ConfigurationRevision: input.ConfigurationRevision,
			ValidationEnvironment: input.ValidationEnvironment, CollectorIdentity: input.CollectorIdentity,
			Rollout: input.Rollout, ExternalResources: input.ExternalResources,
		},
	)
	if !errors.Is(err, h3launchevidence.ErrUnstableLaunchAuthority) {
		t.Fatalf("Capture drift error = %v, want ErrUnstableLaunchAuthority", err)
	}
}

func TestCaptureRejectsSameNameExternalResourceRecreation(t *testing.T) {
	input := exactLaunchInput(t)
	now := time.Now().UTC()
	input.Rollout.ApprovedPlan.ApprovedAt = now.Add(-time.Hour)
	input.Kubernetes.Claims[0].CreationTimestamp = metav1.NewTime(now.Add(-2 * time.Minute))
	input.Kubernetes.Claims[0].Status.Allocation.AllocationTimestamp = timePointer(now.Add(-time.Minute))
	reader := &recreatedSecretReader{fakeKubernetesReader: kubernetesReaderForInput(input)}
	registry := &sequenceRegistryReader{snapshots: []h3launchevidence.RegistrySnapshot{input.Registry, input.Registry}}

	_, err := h3launchevidence.Capture(
		context.Background(), reader, registry,
		h3launchevidence.CaptureRequest{
			ReleaseDigest: input.ReleaseDigest, ConfigurationRevision: input.ConfigurationRevision,
			ValidationEnvironment: input.ValidationEnvironment, CollectorIdentity: input.CollectorIdentity,
			Rollout: input.Rollout, ExternalResources: input.ExternalResources,
		},
	)
	if !errors.Is(err, h3launchevidence.ErrUnstableLaunchAuthority) {
		t.Fatalf("Capture recreated Secret error = %v, want ErrUnstableLaunchAuthority", err)
	}
}

type recreatedSecretReader struct {
	*fakeKubernetesReader
	secretReads int
}

func (reader *recreatedSecretReader) Secret(
	ctx context.Context,
	namespace,
	name string,
) (corev1.Secret, error) {
	secret, err := reader.fakeKubernetesReader.Secret(ctx, namespace, name)
	if err != nil {
		return corev1.Secret{}, err
	}
	reader.secretReads++
	if reader.secretReads > 1 {
		secret.UID = "replacement-secret-uid"
	}
	return secret, nil
}

type sequenceRegistryReader struct {
	snapshots []h3launchevidence.RegistrySnapshot
	calls     int
}

func (reader *sequenceRegistryReader) Capture(
	_ context.Context,
	_ uuid.UUID,
) (h3launchevidence.RegistrySnapshot, error) {
	if reader.calls >= len(reader.snapshots) {
		return h3launchevidence.RegistrySnapshot{}, errors.New("unexpected Registry capture")
	}
	snapshot := reader.snapshots[reader.calls]
	reader.calls++
	snapshot.DatabaseTime = time.Now().UTC()
	snapshot.TransactionID = "backend-42"
	snapshot.SnapshotID = "00000003-0000001B-1"
	return snapshot, nil
}

func kubernetesReaderForInput(input h3launchevidence.Input) *fakeKubernetesReader {
	reader := &fakeKubernetesReader{
		namespaceUIDs: map[string]string{
			"kube-system": input.Kubernetes.ClusterUID,
			"vela-system": input.Kubernetes.NamespaceUID,
		},
		configMaps: make(map[string]corev1.ConfigMap), secrets: make(map[string]corev1.Secret),
		pods: make(map[string]corev1.Pod), templates: make(map[string]resourcev1.ResourceClaimTemplate),
		claims: make(map[string]resourcev1.ResourceClaim), nodes: make(map[string]corev1.Node),
		resourceSlices: input.Kubernetes.ResourceSlices,
	}
	for _, configMap := range input.Kubernetes.ConfigMaps {
		reader.configMaps[namespacedKey(configMap.Namespace, configMap.Name)] = configMap
	}
	for _, secret := range input.Kubernetes.Secrets {
		reader.secrets[namespacedKey(secret.Namespace, secret.Name)] = secret
	}
	for _, pod := range input.Kubernetes.Pods {
		reader.pods[namespacedKey(pod.Namespace, pod.Name)] = pod
	}
	for _, template := range input.Kubernetes.ClaimTemplates {
		reader.templates[namespacedKey(template.Namespace, template.Name)] = template
	}
	for _, claim := range input.Kubernetes.Claims {
		reader.claims[namespacedKey(claim.Namespace, claim.Name)] = claim
	}
	for _, node := range input.Kubernetes.Nodes {
		reader.nodes[node.Name] = node
	}
	return reader
}

var _ h3launchevidence.RegistryReader = (*sequenceRegistryReader)(nil)

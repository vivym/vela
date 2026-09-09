package nodeagent

import (
	"context"
	"errors"
	"strings"

	"github.com/vivym/vela/internal/fleetcontroller"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

var ErrRuntimeLaunchPodReader = errors.New("authenticated Kubernetes Pod reader is unavailable or returned an invalid identity")

// KubernetesRuntimeLaunchPodReader is the production adapter for the
// RuntimeLaunchPodReader seam. The client must already be authenticated by
// trusted Node assembly. It never accepts a Pod or namespace from Runtime;
// callers pass the namespace/name derived from a verified RuntimeLaunchPlan.
type KubernetesRuntimeLaunchPodReader struct {
	core coreclient.CoreV1Interface
}

func NewKubernetesRuntimeLaunchPodReader(core coreclient.CoreV1Interface) (*KubernetesRuntimeLaunchPodReader, error) {
	if core == nil {
		return nil, ErrRuntimeLaunchPodReader
	}
	return &KubernetesRuntimeLaunchPodReader{core: core}, nil
}

func (reader *KubernetesRuntimeLaunchPodReader) GetWorkerInstancePod(ctx context.Context, key fleetcontroller.ResourceKey) (corev1.Pod, error) {
	if err := contextError(ctx); err != nil {
		return corev1.Pod{}, err
	}
	if reader == nil || reader.core == nil || !validPodResourcePart(key.Namespace) || !validPodResourcePart(key.Name) {
		return corev1.Pod{}, ErrRuntimeLaunchPodReader
	}
	pod, err := reader.core.Pods(key.Namespace).Get(ctx, key.Name, metav1.GetOptions{})
	if err != nil {
		return corev1.Pod{}, err
	}
	if pod == nil || pod.Namespace != key.Namespace || pod.Name != key.Name || pod.UID == "" || pod.ResourceVersion == "" {
		return corev1.Pod{}, ErrRuntimeLaunchPodReader
	}
	return *pod.DeepCopy(), nil
}

func validPodResourcePart(value string) bool {
	return value != "" && len(value) <= 253 && value == strings.TrimSpace(value) && !strings.ContainsAny(value, "\\/\x00")
}

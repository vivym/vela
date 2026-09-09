package nodeagent

import (
	"context"
	"errors"
	"testing"

	"k8s.io/client-go/kubernetes/fake"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontroller"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestKubernetesRuntimeLaunchPodReaderReadsExactVerifiedKey(t *testing.T) {
	key := fleetcontroller.ResourceKey{Namespace: "vela-system", Name: "worker-a"}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, UID: types.UID(uuid.NewString()), ResourceVersion: "17"}}
	reader, err := NewKubernetesRuntimeLaunchPodReader(fake.NewSimpleClientset(pod).CoreV1())
	if err != nil {
		t.Fatal(err)
	}
	got, err := reader.GetWorkerInstancePod(t.Context(), key)
	if err != nil || got.Namespace != key.Namespace || got.Name != key.Name || got.ResourceVersion != pod.ResourceVersion {
		t.Fatalf("pod=%+v err=%v", got, err)
	}
	got.Labels = map[string]string{"mutated": "caller"}
	again, err := reader.GetWorkerInstancePod(t.Context(), key)
	if err != nil || len(again.Labels) != 0 {
		t.Fatalf("reader returned mutable client object: %+v %v", again, err)
	}
}

func TestKubernetesRuntimeLaunchPodReaderRejectsInvalidOrIncompleteIdentity(t *testing.T) {
	reader, err := NewKubernetesRuntimeLaunchPodReader(fake.NewSimpleClientset().CoreV1())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []fleetcontroller.ResourceKey{{Namespace: "", Name: "worker"}, {Namespace: "vela-system", Name: "bad/name"}, {Namespace: "vela-system", Name: "missing"}} {
		if _, err := reader.GetWorkerInstancePod(t.Context(), key); err == nil {
			t.Fatalf("invalid or missing key accepted: %+v", key)
		}
	}
	if _, err := NewKubernetesRuntimeLaunchPodReader(nil); !errors.Is(err, ErrRuntimeLaunchPodReader) {
		t.Fatalf("nil client error=%v", err)
	}
	if _, err := reader.GetWorkerInstancePod(context.Background(), fleetcontroller.ResourceKey{Namespace: "vela-system", Name: "worker"}); err == nil {
		t.Fatal("incomplete Pod identity accepted")
	}
}

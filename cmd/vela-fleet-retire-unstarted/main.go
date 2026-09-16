// vela-fleet-retire-unstarted is a one-shot Fleet maintenance workload. The
// admission webhook must still authorize both mutations against Registry.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/vivym/vela/internal/fleetcontract"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/runtimelaunch"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

type retirementInput struct {
	SchemaVersion int                                  `json:"schema_version"`
	Rollout       fleetcontroller.ResidencyPlanRollout `json:"rollout"`
	PodUIDs       map[string]types.UID                 `json:"pod_uids"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: vela-fleet-retire-unstarted <retirement.json>")
	}
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		return err
	}
	if len(data) > fleetcontroller.MaximumResidencyPlanRolloutBytes {
		return errors.New("retirement input is too large")
	}
	var input retirementInput
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("retirement requires exactly one JSON document")
	}
	if input.SchemaVersion != 1 || len(input.PodUIDs) == 0 || len(input.PodUIDs) > 16 {
		return errors.New("invalid retirement targets")
	}
	if err := fleetcontroller.ValidateResidencyPlanRollout(input.Rollout); err != nil {
		return err
	}
	var targets []corev1.Pod
	for _, bundle := range input.Rollout.WorkerBundles {
		if !slices.Contains(input.Rollout.WithdrawnWorkerBundleIDs, bundle.WorkerBundleID) {
			return errors.New("every retirement bundle must be withdrawn")
		}
		pods, _, err := fleetcontroller.MaterializeWorkerInstanceLaunchResources(bundle)
		if err != nil {
			return err
		}
		targets = append(targets, pods...)
	}
	if len(targets) != len(input.PodUIDs) {
		return errors.New("retirement must pin every withdrawn Pod UID")
	}
	for _, pod := range targets {
		if input.PodUIDs[pod.Name] == "" {
			return errors.New("missing pinned Pod UID")
		}
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// Preflight every target before the first mutation. DeleteOptions and the
	// subsequent patch also compare resourceVersion, closing gate-release races.
	for _, desired := range targets {
		pod, err := client.CoreV1().Pods(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if err := validateUnstarted(*pod, desired, input.PodUIDs[desired.Name]); err != nil {
			return err
		}
	}
	for _, desired := range targets {
		if err := retire(ctx, client.CoreV1().Pods(desired.Namespace), desired, input.PodUIDs[desired.Name]); err != nil {
			return err
		}
		fmt.Printf("retired unstarted Pod %s/%s uid=%s\n", desired.Namespace, desired.Name, input.PodUIDs[desired.Name])
	}
	return nil
}

func validateUnstarted(live, desired corev1.Pod, uid types.UID) error {
	if uid == "" || live.UID != uid || live.ResourceVersion == "" ||
		live.Spec.NodeName != "" || live.Status.Phase != corev1.PodPending || live.Status.StartTime != nil ||
		len(live.Status.ContainerStatuses) != 0 || len(live.Status.InitContainerStatuses) != 0 ||
		len(live.Status.EphemeralContainerStatuses) != 0 ||
		!slices.Equal(live.Spec.SchedulingGates, []corev1.PodSchedulingGate{{Name: runtimelaunch.Gate}}) ||
		!slices.Equal(live.Finalizers, []string{fleetcontract.ProtectionFinalizer}) {
		return fmt.Errorf("Pod %s is not the pinned, protected, unstarted instance", live.Name)
	}
	// A prior accepted DELETE may have left only the protected finalizer.
	live.DeletionTimestamp = nil
	live.DeletionGracePeriodSeconds = nil
	if !fleetcontroller.WorkerInstancePodMatches(live, desired) {
		return fmt.Errorf("Pod %s differs from the withdrawn plan", live.Name)
	}
	return nil
}

func retire(ctx context.Context, pods typedcore.PodInterface, desired corev1.Pod, uid types.UID) error {
	live, err := pods.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := validateUnstarted(*live, desired, uid); err != nil {
		return err
	}
	if live.DeletionTimestamp == nil {
		rv := live.ResourceVersion
		if err := pods.Delete(ctx, desired.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}); err != nil {
			return fmt.Errorf("Registry-authorized DELETE: %w", err)
		}
	}
	live, err = pods.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := validateUnstarted(*live, desired, uid); err != nil {
		return err
	}
	if live.DeletionTimestamp == nil {
		return errors.New("DELETE was not accepted; retaining protection")
	}
	patch, err := json.Marshal([]map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": uid},
		{"op": "test", "path": "/metadata/resourceVersion", "value": live.ResourceVersion},
		{"op": "replace", "path": "/metadata/finalizers", "value": []string{}},
	})
	if err != nil {
		return err
	}
	if _, err := pods.Patch(ctx, desired.Name, types.JSONPatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("Registry-authorized finalizer removal: %w", err)
	}
	for {
		_, err := pods.Get(ctx, desired.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

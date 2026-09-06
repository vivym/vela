package nodeagent

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontract"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
)

var ErrRuntimeLaunchPlan = errors.New("runtime launch does not match the Registry-bound member plan")

// RuntimeLaunchPlan authenticates a historical configuration, not current
// startup permission. Only VerifyRuntimeLaunchPlan constructs a usable plan.
// It owns its data; returned copies cannot change later comparisons.
type RuntimeLaunchPlan struct {
	binding  *velav1.WorkerBootstrapBinding
	manifest []byte
	pod      corev1.Pod
	uid      uint32
	gid      uint32
}

// VerifyRuntimeLaunchPlan binds the canonical bundle preimage and exact member
// to a signed Registry journal receipt. nodeIdentity is trusted local config.
// The signature's age is not a readiness, activation or retirement decision.
func VerifyRuntimeLaunchPlan(nodeIdentity string, verifier *journalbinding.Verifier, binding *velav1.WorkerBootstrapBinding, bundleManifest []byte) (*RuntimeLaunchPlan, error) {
	if !validText(nodeIdentity, maxIdentityText) || len(bundleManifest) == 0 || len(bundleManifest) > fleet.MaximumWorkerBootstrapManifestBytes {
		return nil, ErrRuntimeLaunchPlan
	}
	verified, err := verifier.Verify(binding)
	if err != nil || verified.GetClaim().GetNodeIdentity() != nodeIdentity {
		return nil, errors.Join(ErrRuntimeLaunchPlan, err)
	}
	digest := sha256.Sum256(bundleManifest)
	if !bytes.Equal(digest[:], verified.Claim.BundleDigest) {
		return nil, ErrRuntimeLaunchPlan
	}
	bundle, err := fleetcontroller.ParseWorkerBundleActuationManifest(bundleManifest)
	if err != nil {
		return nil, errors.Join(ErrRuntimeLaunchPlan, err)
	}
	claim := verified.Claim
	workerID, memberID := uuid.MustParse(claim.WorkerInstanceId), uuid.MustParse(claim.WorkerMemberId)
	manifest, err := fleetcontroller.WorkerMemberLaunchManifest(bundle, workerID, memberID)
	if err != nil || manifest.WorkerInstanceEpoch != claim.WorkerInstanceEpoch || manifest.WorkerMemberEpoch != claim.WorkerMemberEpoch {
		return nil, errors.Join(ErrRuntimeLaunchPlan, err)
	}
	pods, _, err := fleetcontroller.MaterializeWorkerInstanceLaunchResources(bundle)
	if err != nil {
		return nil, errors.Join(ErrRuntimeLaunchPlan, err)
	}
	var desired *corev1.Pod
	for i := range pods {
		if pods[i].Labels[fleetcontract.WorkerMemberIDLabel] == memberID.String() {
			if desired != nil || pods[i].Spec.NodeSelector[corev1.LabelHostname] != nodeIdentity {
				return nil, ErrRuntimeLaunchPlan
			}
			desired = &pods[i]
		}
	}
	if desired == nil {
		return nil, ErrRuntimeLaunchPlan
	}
	uid, gid, err := runtimeLaunchCredentials(*desired)
	if err != nil {
		return nil, err
	}
	encoded, err := modelruntime.EncodeLaunchManifest(manifest)
	if err != nil {
		return nil, errors.Join(ErrRuntimeLaunchPlan, err)
	}
	return &RuntimeLaunchPlan{binding: verified, manifest: encoded, pod: *desired.DeepCopy(), uid: uid, gid: gid}, nil
}

// MatchManifest compares every canonical launch field, including commands,
// environments, image identity, paths, timeouts and all member/device epochs.
func (plan *RuntimeLaunchPlan) MatchManifest(manifest modelruntime.LaunchManifest) error {
	if plan == nil || plan.binding == nil {
		return ErrRuntimeLaunchPlan
	}
	encoded, err := modelruntime.EncodeLaunchManifest(manifest)
	if err != nil || !bytes.Equal(encoded, plan.manifest) {
		return errors.Join(ErrRuntimeLaunchPlan, err)
	}
	return nil
}

func (plan *RuntimeLaunchPlan) RegistryBinding() *velav1.WorkerBootstrapBinding {
	if plan == nil || plan.binding == nil {
		return nil
	}
	return proto.CloneOf(plan.binding)
}

func (plan *RuntimeLaunchPlan) ExpectedPod() *corev1.Pod {
	if plan == nil || plan.binding == nil {
		return nil
	}
	return plan.pod.DeepCopy()
}

func runtimeLaunchCredentials(pod corev1.Pod) (uint32, uint32, error) {
	var uid, gid *int64
	if pod.Spec.SecurityContext != nil {
		uid, gid = pod.Spec.SecurityContext.RunAsUser, pod.Spec.SecurityContext.RunAsGroup
	}
	found := false
	for _, container := range pod.Spec.Containers {
		if container.Name != "model-runtime" {
			continue
		}
		if found {
			return 0, 0, ErrRuntimeLaunchPlan
		}
		found = true
		if container.SecurityContext != nil {
			if container.SecurityContext.RunAsUser != nil {
				uid = container.SecurityContext.RunAsUser
			}
			if container.SecurityContext.RunAsGroup != nil {
				gid = container.SecurityContext.RunAsGroup
			}
		}
	}
	if !found || uid == nil || gid == nil || *uid <= 0 || *gid <= 0 || *uid > math.MaxUint32 || *gid > math.MaxUint32 {
		return 0, 0, ErrRuntimeLaunchPlan
	}
	return uint32(*uid), uint32(*gid), nil
}

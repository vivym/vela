package nodeagent

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	leasesapi "github.com/containerd/containerd/api/services/leases/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	runtimeImageLeaseKindLabel        = "vela.ai/runtime-image-observer"
	runtimeImageLeaseNodeLabel        = "vela.ai/runtime-image-node"
	runtimeImageLeaseSnapshotterLabel = "vela.ai/runtime-image-snapshotter"
	runtimeImageLeaseExpiryLabel      = "vela.ai/runtime-image-expires"
	maximumRuntimeImageRecoveryBatch  = 32
)

func (observer *RuntimeImageObserver) leaseLabels(expires time.Time) map[string]string {
	return map[string]string{
		runtimeImageLeaseKindLabel:        "v2",
		runtimeImageLeaseNodeLabel:        fmt.Sprintf("%x", sha256.Sum256([]byte(observer.local.nodeIdentity))),
		runtimeImageLeaseSnapshotterLabel: observer.snapshotter,
		runtimeImageLeaseExpiryLabel:      expires.UTC().Format(time.RFC3339),
	}
}

// RecoverExpired explicitly reclaims this Node's expired image observations.
// Run it at service startup and periodically; Vela owns the expiration and
// containerd GC must retain the lease until cleanup is proved. A call processes
// at most 32 leases. The count reports completed
// cleanups even when a later operation fails. It grants no Runtime authority.
// Legacy marked leases fail closed because their GC policy cannot retain the
// cleanup journal. Unmarked legacy leases are outside this recovery protocol.
func (observer *RuntimeImageObserver) RecoverExpired(ctx context.Context) (int, error) {
	if err := contextError(ctx); err != nil {
		return 0, err
	}
	if observer == nil || observer.local == nil || observer.local.check == nil || observer.local.bootID == nil ||
		observer.leases == nil || observer.snapshots == nil || observer.mounts == nil || observer.mounted == nil ||
		observer.snapshotter != "native" || len(validation.IsDNS1123Subdomain(observer.namespace)) != 0 ||
		!validText(observer.local.nodeIdentity, maxIdentityText) {
		return 0, ErrRuntimeImage
	}
	ctx, cancel := context.WithTimeout(ctx, runtimeImageTimeout)
	defer cancel()
	from := time.Now().UTC()
	boot, err := observer.local.readBoot()
	if err != nil {
		return 0, err
	}
	if err := observer.local.check(); err != nil {
		return 0, err
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("containerd-namespace", observer.namespace))
	labels := observer.leaseLabels(from)
	filter := fmt.Sprintf("labels.%q,labels.%q==%q,labels.%q==%q", runtimeImageLeaseKindLabel,
		runtimeImageLeaseNodeLabel, labels[runtimeImageLeaseNodeLabel], runtimeImageLeaseSnapshotterLabel, labels[runtimeImageLeaseSnapshotterLabel])
	response, err := observer.leases.List(ctx, &leasesapi.ListRequest{Filters: []string{filter}})
	if err != nil {
		return 0, err
	}
	if response == nil {
		return 0, ErrRuntimeImage
	}
	keys := make([]string, 0, len(response.Leases))
	seen := make(map[string]struct{}, len(response.Leases))
	for _, lease := range response.Leases {
		if lease == nil {
			return 0, ErrRuntimeImage
		}
		id, err := uuid.Parse(strings.TrimPrefix(lease.ID, "vela-image-observation-"))
		if err != nil || id.Version() != 4 || lease.ID != "vela-image-observation-"+id.String() ||
			lease.Labels[runtimeImageLeaseKindLabel] != labels[runtimeImageLeaseKindLabel] ||
			lease.Labels[runtimeImageLeaseNodeLabel] != labels[runtimeImageLeaseNodeLabel] ||
			lease.Labels[runtimeImageLeaseSnapshotterLabel] != labels[runtimeImageLeaseSnapshotterLabel] || len(lease.Labels) != 4 ||
			lease.CreatedAt == nil || lease.CreatedAt.CheckValid() != nil {
			return 0, errors.New("image recovery lease has an unrecognized identity or ownership")
		}
		if _, duplicate := seen[lease.ID]; duplicate {
			return 0, errors.New("image recovery returned duplicate lease identities")
		}
		seen[lease.ID] = struct{}{}
		expires, err := time.Parse(time.RFC3339, lease.Labels[runtimeImageLeaseExpiryLabel])
		if err != nil || expires.Format(time.RFC3339) != lease.Labels[runtimeImageLeaseExpiryLabel] ||
			!expires.After(lease.CreatedAt.AsTime()) || expires.Sub(lease.CreatedAt.AsTime()) > time.Hour {
			return 0, errors.New("image recovery lease has invalid expiration")
		}
		if expires.Before(from) {
			keys = append(keys, lease.ID)
		}
	}
	slices.Sort(keys)
	count := 0
	for _, key := range keys {
		if count == maximumRuntimeImageRecoveryBatch {
			break
		}
		if err := errors.Join(observer.local.check(), ctx.Err()); err != nil {
			return count, err
		}
		// A crash can leave any prefix of the allocation sequence. Missing
		// exact resources are already cleaned; failure preserves the lease.
		resources := runtimeImageResources{observer: observer, key: key, lease: true, view: true, activation: true}
		if err := resources.close(ctx); err != nil {
			return count, err
		}
		count++
	}
	finalBoot, err := observer.local.readBoot()
	if err != nil || boot != finalBoot {
		return count, errors.Join(ErrRuntimeImage, err)
	}
	return count, errors.Join(observer.local.check(), ctx.Err())
}

package nodeagent

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"time"

	leasesapi "github.com/containerd/containerd/api/services/leases/v1"
	mountsapi "github.com/containerd/containerd/api/services/mounts/v1"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/api/types"
	"github.com/google/uuid"
	"github.com/prometheus/procfs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	runtimeImagePhaseLabel     = "vela.ai/runtime-image-phase"
	runtimeImageBootLabel      = "vela.ai/runtime-image-boot"
	runtimeImageMountPathLabel = "vela.ai/runtime-image-mount-path"
)

func (observer *RuntimeImageObserver) setImagePhase(ctx context.Context, key string, labels map[string]string) (*snapshotsapi.Info, error) {
	response, err := observer.snapshots.Update(ctx, &snapshotsapi.UpdateSnapshotRequest{Snapshotter: observer.snapshotter,
		Info: &snapshotsapi.Info{Name: key, Labels: labels}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}}})
	if err != nil {
		return nil, err
	}
	info := response.GetInfo()
	if info == nil || info.Name != key || info.Kind != snapshotsapi.Kind_VIEW || !maps.Equal(info.Labels, labels) {
		return nil, errors.New("image cleanup journal update was not confirmed")
	}
	return info, nil
}

func runtimeImageActivationPath(key string, info *types.ActivationInfo) (string, error) {
	if info == nil || info.Name != key || len(info.Active) != 1 || info.Active[0] == nil ||
		info.Active[0].Mount == nil || info.Active[0].Mount.Type != "bind" || info.Active[0].MountedAt == nil ||
		info.Active[0].MountedAt.CheckValid() != nil || len(info.Active[0].Data) != 0 ||
		!validRuntimeImageSystemMount(info) || !validRuntimeImagePath(info.Active[0].MountPoint) ||
		info.Active[0].MountPoint == "/" || len(info.Active[0].MountPoint)+len(runtimeImageMountPathLabel) > 4096 {
		return "", errors.New("image cleanup activation identity is incomplete")
	}
	return info.Active[0].MountPoint, nil
}

// Some containerd mount-manager versions reconstruct System only for Activate,
// not Info. Cleanup uses the persisted active mountpoint and our leased journal;
// when a System mount is returned it must still describe that exact bind.
func validRuntimeImageSystemMount(info *types.ActivationInfo) bool {
	if len(info.System) == 0 {
		return true
	}
	return len(info.System) == 1 && info.System[0] != nil && info.System[0].Type == "bind" &&
		info.System[0].Source == info.Active[0].MountPoint && info.System[0].Target == "" &&
		slices.Equal(info.System[0].Options, []string{"rbind"})
}

func (observer *RuntimeImageObserver) closeImageActivation(ctx context.Context, key string) error {
	if err := observer.local.check(); err != nil {
		return err
	}
	view, viewErr := observer.snapshots.Stat(ctx, &snapshotsapi.StatSnapshotRequest{Snapshotter: observer.snapshotter, Key: key})
	activation, activationErr := observer.mounts.Info(ctx, &mountsapi.InfoRequest{Name: key})
	if status.Code(viewErr) == codes.NotFound && status.Code(activationErr) == codes.NotFound {
		return nil
	}
	if viewErr != nil {
		return viewErr
	}
	info := view.GetInfo()
	if info == nil || info.Name != key || info.Kind != snapshotsapi.Kind_VIEW {
		return errors.New("image cleanup view identity is incomplete")
	}
	phase, mountPath := info.Labels[runtimeImagePhaseLabel], info.Labels[runtimeImageMountPathLabel]
	boot, err := uuid.Parse(info.Labels[runtimeImageBootLabel])
	if err != nil || boot == uuid.Nil || boot.String() != info.Labels[runtimeImageBootLabel] ||
		(phase != "VIEW" && phase != "ACTIVATING" && phase != "MOUNTED") ||
		(phase == "MOUNTED" && (len(info.Labels) != 3 || !validRuntimeImagePath(mountPath) || mountPath == "/")) ||
		(phase != "MOUNTED" && len(info.Labels) != 2) {
		return errors.New("image cleanup journal is missing or invalid")
	}
	currentBoot, err := observer.local.readBoot()
	if err != nil {
		return err
	}
	if status.Code(activationErr) == codes.NotFound {
		if phase == "VIEW" || boot != currentBoot {
			return nil
		}
		if phase == "ACTIVATING" {
			return errors.New("image activation outcome is unresolved; cleanup journal retained")
		}
	} else if activationErr != nil {
		return activationErr
	} else {
		observedPath, err := runtimeImageActivationPath(key, activation.GetInfo())
		if err != nil || phase == "VIEW" || (phase == "MOUNTED" && mountPath != observedPath) {
			return errors.Join(errors.New("image cleanup activation changed"), err)
		}
		if phase == "ACTIVATING" {
			mountPath = observedPath
			if _, err := observer.setImagePhase(ctx, key, map[string]string{runtimeImagePhaseLabel: "MOUNTED",
				runtimeImageBootLabel: boot.String(), runtimeImageMountPathLabel: mountPath}); err != nil {
				return err
			}
		}
	}
	_, err = observer.mounts.Deactivate(ctx, &mountsapi.DeactivateRequest{Name: key})
	if err != nil && status.Code(err) != codes.NotFound {
		return err
	}
	if status.Code(err) == codes.NotFound {
		// containerd may have erased the activation before a failed umount.
		// Request its orphan cleanup while our view and journal stay leased.
		if err := observer.triggerImageGC(ctx); err != nil {
			return err
		}
	}
	mounted, err := observer.mounted(mountPath)
	if err != nil {
		return err
	}
	if mounted {
		return errors.New("image observation kernel mount is still busy; cleanup journal retained")
	}
	return nil
}

func (observer *RuntimeImageObserver) triggerImageGC(ctx context.Context) error {
	key := "vela-image-cleanup-trigger-" + uuid.NewString()
	response, err := observer.leases.Create(ctx, &leasesapi.CreateRequest{ID: key,
		Labels: map[string]string{"containerd.io/gc.expire": time.Now().Add(time.Minute).UTC().Format(time.RFC3339)}})
	if err != nil || response.GetLease().GetID() != key {
		return errors.Join(errors.New("image GC trigger lease was not confirmed"), err)
	}
	_, err = observer.leases.Delete(ctx, &leasesapi.DeleteRequest{ID: key, Sync: true})
	return err
}

func runtimeImagePathMounted(path string) (bool, error) {
	mounts, err := procfs.GetMounts()
	if err != nil {
		return false, err
	}
	return runtimeImageMountListed(path, mounts), nil
}

func runtimeImageMountListed(path string, mounts []*procfs.MountInfo) bool {
	// procfs exposes the kernel's escaped mountpoint field. Encode the exact
	// expected path so whitespace and backslashes cannot hide a live mount.
	expected := strings.NewReplacer("\\", "\\134", " ", "\\040", "\t", "\\011", "\n", "\\012").Replace(path)
	for _, mount := range mounts {
		if mount.MountPoint == expected {
			return true
		}
	}
	return false
}

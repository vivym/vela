package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"runtime"
	"strings"
	"time"
	"unicode/utf8"

	contentapi "github.com/containerd/containerd/api/services/content/v1"
	leasesapi "github.com/containerd/containerd/api/services/leases/v1"
	mountsapi "github.com/containerd/containerd/api/services/mounts/v1"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/google/uuid"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/vivym/vela/internal/strictjson"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	runtimeImageTimeout          = 30 * time.Second
	maximumRuntimeImageJSONBytes = 1 << 20
	maximumRuntimeImageLayers    = 128
)

var ErrRuntimeImage = errors.New("runtime image snapshot could not be observed consistently")

type RuntimeImageObserverConfig struct {
	RuntimeContainerObserverConfig
	Namespace   string
	Snapshotter string
}

// RuntimeImageTarget must come from a verified release. The digest identifies a
// single-platform manifest, not an index, tag, CRI image ID or caller assertion.
type RuntimeImageTarget struct {
	ManifestDigest string `json:"manifest_digest"`
	ConfigDigest   string `json:"config_digest"`
	ExecutablePath string `json:"executable_path"`
}

// RuntimeImageExecutableObservation measures an already-unpacked image in the
// trusted local containerd. It is not release approval, a container-rootfs
// measurement, loaded-memory attestation or authorization to start a process.
type RuntimeImageExecutableObservation struct {
	SchemaVersion   int                `json:"schema_version"`
	Target          RuntimeImageTarget `json:"target"`
	NodeIdentity    string             `json:"node_identity"`
	BootID          uuid.UUID          `json:"boot_id"`
	Namespace       string             `json:"namespace"`
	Snapshotter     string             `json:"snapshotter"`
	ChainID         string             `json:"chain_id"`
	Digest          [sha256.Size]byte  `json:"digest"`
	SizeBytes       int64              `json:"size_bytes"`
	FileDevice      uint64             `json:"file_device"`
	FileInode       uint64             `json:"file_inode"`
	FileMode        uint32             `json:"file_mode"`
	ObservedFrom    time.Time          `json:"observed_from"`
	ObservedThrough time.Time          `json:"observed_through"`
}

// Separate from the read-only container observer: inspection creates only an
// ephemeral lease, snapshot view and mount activation. It never pulls, unpacks,
// prepares writable snapshots or invokes process/task operations.
type RuntimeImageObserver struct {
	local       *RuntimeContainerObserver
	namespace   string
	snapshotter string
	content     contentapi.ContentClient
	leases      leasesapi.LeasesClient
	snapshots   snapshotsapi.SnapshotsClient
	mounts      mountsapi.MountsClient
	mounted     func(string) (bool, error)
	mountReader *runtimeImageMountReader
}

func DialRuntimeImageObserver(ctx context.Context, config RuntimeImageObserverConfig) (*RuntimeImageObserver, error) {
	if len(validation.IsDNS1123Subdomain(config.Namespace)) != 0 || config.Snapshotter != "native" {
		return nil, errors.New("image observer requires an explicit namespace and qualified native snapshotter")
	}
	reader := &runtimeImageMountReader{}
	local, err := dialRuntimeContainerObserverWithPeerCheck(ctx, config.RuntimeContainerObserverConfig, 0,
		func() (string, error) { return readBootID("/proc/sys/kernel/random/boot_id") }, reader.authenticate)
	if err != nil {
		_ = reader.close()
		return nil, err
	}
	check, closeLocal := local.check, local.close
	local.check = func() error { return errors.Join(check(), reader.check()) }
	local.close = func() error { return errors.Join(closeLocal(), reader.close()) }
	if err := local.check(); err != nil {
		_ = local.Close()
		return nil, err
	}
	return &RuntimeImageObserver{local: local, namespace: config.Namespace, snapshotter: config.Snapshotter,
		content: contentapi.NewContentClient(local.connection), leases: leasesapi.NewLeasesClient(local.connection),
		snapshots: snapshotsapi.NewSnapshotsClient(local.connection), mounts: mountsapi.NewMountsClient(local.connection), mounted: reader.mounted, mountReader: reader}, nil
}

func (observer *RuntimeImageObserver) Close() error {
	if observer == nil {
		return nil
	}
	return observer.local.Close()
}

func (target RuntimeImageTarget) Validate() error {
	if !validRuntimeImageDigest(target.ManifestDigest) || !validRuntimeImageDigest(target.ConfigDigest) ||
		!validRuntimeImagePath(target.ExecutablePath) || target.ExecutablePath == "/" {
		return errors.New("image observation requires exact manifest/config SHA-256 and an absolute executable path")
	}
	return nil
}

func validRuntimeImageDigest(value string) bool {
	return len(value) == 71 && strings.HasPrefix(value, "sha256:") && runtimeContainerIDPattern.MatchString(value[7:])
}

func validRuntimeImagePath(value string) bool {
	return path.IsAbs(value) && path.Clean(value) == value && len(value) <= 4096 && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func (observer *RuntimeImageObserver) InspectExecutable(ctx context.Context, target RuntimeImageTarget) (result RuntimeImageExecutableObservation, retErr error) {
	return observer.inspectExecutable(ctx, target, nil)
}

func (observer *RuntimeImageObserver) inspectExecutable(ctx context.Context, target RuntimeImageTarget, launch *RuntimeImageLaunch) (result RuntimeImageExecutableObservation, retErr error) {
	if err := contextError(ctx); err != nil {
		return result, err
	}
	if observer == nil || observer.local == nil || observer.local.check == nil || observer.local.bootID == nil ||
		observer.content == nil || observer.leases == nil || observer.snapshots == nil || observer.mounts == nil || observer.mounted == nil ||
		observer.snapshotter != "native" || len(validation.IsDNS1123Subdomain(observer.namespace)) != 0 {
		return result, ErrRuntimeImage
	}
	if launch == nil {
		if err := target.Validate(); err != nil {
			return result, err
		}
	} else if !validRuntimeImageDigest(target.ManifestDigest) || target.ConfigDigest != "" || target.ExecutablePath != "" {
		return result, ErrRuntimeImage
	}
	ctx, cancel := context.WithTimeout(ctx, runtimeImageTimeout)
	defer cancel()
	from := time.Now().UTC()
	boot, err := observer.local.readBoot()
	if err != nil {
		return result, err
	}
	if err := observer.local.check(); err != nil {
		return result, err
	}
	key := "vela-image-observation-" + uuid.NewString()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("containerd-namespace", observer.namespace, "containerd-lease", key))
	resources := runtimeImageResources{observer: observer, key: key}
	defer func() {
		// A canceled observation still releases its own exact resources. Failed
		// cleanup cannot produce a successful observation or discard its lease.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cleanupCancel()
		retErr = errors.Join(retErr, resources.close(cleanupCtx), context.Cause(ctx))
		if retErr != nil {
			result = RuntimeImageExecutableObservation{}
		}
	}()
	resources.lease = true
	lease, err := observer.leases.Create(ctx, &leasesapi.CreateRequest{ID: key,
		Labels: observer.leaseLabels(from.Add(time.Hour))})
	if status.Code(err) == codes.AlreadyExists {
		resources.lease = false
	}
	if err != nil || lease.GetLease().GetID() != key {
		return result, errors.Join(errors.New("image lease response has a different identity"), err)
	}
	manifestBytes, err := observer.readJSON(ctx, key, target.ManifestDigest, 0)
	if err != nil {
		return result, err
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return result, err
	}
	if manifest.SchemaVersion != 2 || (manifest.MediaType != ocispec.MediaTypeImageManifest && manifest.MediaType != "application/vnd.docker.distribution.manifest.v2+json") ||
		!validRuntimeImageDigest(manifest.Config.Digest.String()) || (launch == nil && manifest.Config.Digest.String() != target.ConfigDigest) || manifest.Config.Size <= 0 || manifest.Config.Size > maximumRuntimeImageJSONBytes ||
		(manifest.Config.MediaType != ocispec.MediaTypeImageConfig && manifest.Config.MediaType != "application/vnd.docker.container.image.v1+json") ||
		manifest.Subject != nil || manifest.ArtifactType != "" || len(manifest.Layers) == 0 || len(manifest.Layers) > maximumRuntimeImageLayers {
		return result, errors.New("image manifest is not a bounded exact runtime image")
	}
	if launch != nil {
		target.ConfigDigest = manifest.Config.Digest.String()
	}
	for _, layer := range manifest.Layers {
		if !validRuntimeImageDigest(layer.Digest.String()) || layer.Size <= 0 || len(layer.URLs) != 0 || len(layer.Data) != 0 ||
			(layer.MediaType != ocispec.MediaTypeImageLayer && layer.MediaType != ocispec.MediaTypeImageLayerGzip && layer.MediaType != ocispec.MediaTypeImageLayerZstd &&
				layer.MediaType != "application/vnd.docker.image.rootfs.diff.tar.gzip") {
			return result, errors.New("image contains an unsupported layer descriptor")
		}
	}
	configBytes, err := observer.readJSON(ctx, key, target.ConfigDigest, manifest.Config.Size)
	if err != nil {
		return result, err
	}
	var config ocispec.Image
	if err := json.Unmarshal(configBytes, &config); err != nil {
		return result, err
	}
	if config.OS != "linux" || config.Architecture != runtime.GOARCH || config.Variant != "" || config.OSVersion != "" || len(config.OSFeatures) != 0 ||
		config.RootFS.Type != "layers" || len(config.RootFS.DiffIDs) != len(manifest.Layers) {
		return result, errors.New("image configuration platform or rootfs is unsupported")
	}
	if launch != nil {
		arguments, err := runtimeImageDefaultArguments(config.Config)
		if err != nil {
			return result, err
		}
		target.ExecutablePath = arguments[0]
		launch.encoded = bytes.Clone(configBytes)
	}
	for _, diffID := range config.RootFS.DiffIDs {
		if !validRuntimeImageDigest(diffID.String()) {
			return result, errors.New("image configuration has an invalid DiffID")
		}
	}
	chain := identity.ChainID(config.RootFS.DiffIDs).String()
	parent := identity.ChainID(config.RootFS.DiffIDs[:len(config.RootFS.DiffIDs)-1]).String()
	if err := observer.pin(ctx, key, "snapshots/"+observer.snapshotter, chain); err != nil {
		return result, err
	}
	first, err := observer.statSnapshot(ctx, chain, parent, snapshotsapi.Kind_COMMITTED)
	if err != nil {
		return result, err
	}
	resources.view = true
	view, err := observer.snapshots.View(ctx, &snapshotsapi.ViewSnapshotRequest{Snapshotter: observer.snapshotter, Key: key, Parent: chain,
		Labels: map[string]string{runtimeImagePhaseLabel: "VIEW", runtimeImageBootLabel: boot.String()}})
	if status.Code(err) == codes.AlreadyExists {
		resources.view = false
	}
	if err != nil {
		return result, err
	}
	if _, err := observer.statSnapshot(ctx, key, chain, snapshotsapi.Kind_VIEW); err != nil {
		return result, err
	}
	if !validRuntimeNativeView(view.GetMounts()) {
		return result, errors.New("native image view does not specify one read-only bind mount")
	}
	if _, err := observer.setImagePhase(ctx, key, map[string]string{runtimeImagePhaseLabel: "ACTIVATING", runtimeImageBootLabel: boot.String()}); err != nil {
		return result, err
	}
	resources.activation = true
	activation, err := observer.mounts.Activate(ctx, &mountsapi.ActivateRequest{Name: key, Mounts: view.Mounts, Temporary: true})
	if status.Code(err) == codes.AlreadyExists {
		resources.activation = false
	}
	if err != nil {
		return result, err
	}
	if _, err := runtimeImageActivationPath(key, activation.GetInfo()); err != nil {
		return result, err
	}
	viewInfo, err := observer.setImagePhase(ctx, key, map[string]string{runtimeImagePhaseLabel: "MOUNTED", runtimeImageBootLabel: boot.String(),
		runtimeImageMountPathLabel: activation.Info.Active[0].MountPoint})
	if err != nil {
		return result, err
	}
	root, err := openRuntimeImageActivation(key, view.Mounts, activation.GetInfo())
	if err != nil {
		return result, err
	}
	fileDigest, fileIdentity, measureErr := measureRuntimeImageExecutable(ctx, root, target.ExecutablePath)
	if err := errors.Join(measureErr, root.Close()); err != nil {
		return result, err
	}
	lastActivation, err := observer.mounts.Info(ctx, &mountsapi.InfoRequest{Name: key})
	if err != nil || !sameRuntimeImageActivation(activation.GetInfo(), lastActivation.GetInfo()) {
		return result, errors.Join(errors.New("image activation changed during observation"), err)
	}
	lastView, err := observer.statSnapshot(ctx, key, chain, snapshotsapi.Kind_VIEW)
	if err != nil || !proto.Equal(viewInfo, lastView) {
		return result, errors.Join(errors.New("image view changed during observation"), err)
	}
	last, err := observer.statSnapshot(ctx, chain, parent, snapshotsapi.Kind_COMMITTED)
	if err != nil || !proto.Equal(first, last) {
		return result, errors.Join(errors.New("committed image snapshot changed during observation"), err)
	}
	finalBoot, err := observer.local.readBoot()
	if err != nil || finalBoot != boot {
		return result, errors.Join(errors.New("node boot changed during image observation"), err)
	}
	if err := errors.Join(observer.local.check(), ctx.Err()); err != nil {
		return result, err
	}
	through := time.Now().UTC()
	if through.Before(from) || through.Sub(from) > runtimeImageTimeout {
		return result, ErrRuntimeImage
	}
	return RuntimeImageExecutableObservation{SchemaVersion: 1, Target: target, NodeIdentity: observer.local.nodeIdentity,
		BootID: boot, Namespace: observer.namespace, Snapshotter: observer.snapshotter, ChainID: chain,
		Digest: fileDigest, SizeBytes: fileIdentity.size, FileDevice: fileIdentity.device, FileInode: fileIdentity.inode, FileMode: fileIdentity.mode,
		ObservedFrom: from, ObservedThrough: through}, nil
}

func (observer *RuntimeImageObserver) pin(ctx context.Context, key, kind, id string) error {
	_, err := observer.leases.AddResource(ctx, &leasesapi.AddResourceRequest{ID: key, Resource: &leasesapi.Resource{Type: kind, ID: id}})
	return err
}

func (observer *RuntimeImageObserver) readJSON(ctx context.Context, key, hash string, expectedSize int64) ([]byte, error) {
	if err := observer.pin(ctx, key, "content", hash); err != nil {
		return nil, err
	}
	info, err := observer.content.Info(ctx, &contentapi.InfoRequest{Digest: hash})
	if err != nil {
		return nil, err
	}
	size := info.GetInfo().GetSize()
	if info.GetInfo().GetDigest() != hash || size <= 0 || size > maximumRuntimeImageJSONBytes || (expectedSize != 0 && size != expectedSize) {
		return nil, errors.New("image JSON content has an invalid identity or size")
	}
	stream, err := observer.content.Read(ctx, &contentapi.ReadContentRequest{Digest: hash, Size: size})
	if err != nil {
		return nil, err
	}
	data := make([]byte, 0, int(size))
	for {
		part, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if part == nil || part.Offset != int64(len(data)) || len(part.Data) == 0 || int64(len(part.Data)) > size-int64(len(data)) {
			return nil, errors.New("image JSON content stream is noncontiguous or oversized")
		}
		data = append(data, part.Data...)
	}
	if int64(len(data)) != size || digest.FromBytes(data).String() != hash || !utf8.Valid(data) {
		return nil, errors.New("image JSON bytes do not match the expected identity")
	}
	if err := strictjson.RejectDuplicateKeys(data); err != nil {
		return nil, err
	}
	return data, nil
}

func (observer *RuntimeImageObserver) statSnapshot(ctx context.Context, key, parent string, kind snapshotsapi.Kind) (*snapshotsapi.Info, error) {
	response, err := observer.snapshots.Stat(ctx, &snapshotsapi.StatSnapshotRequest{Snapshotter: observer.snapshotter, Key: key})
	if err != nil {
		return nil, err
	}
	info := response.GetInfo()
	if info == nil || info.Name != key || info.Parent != parent || info.Kind != kind ||
		info.CreatedAt == nil || info.CreatedAt.CheckValid() != nil || info.UpdatedAt == nil || info.UpdatedAt.CheckValid() != nil {
		return nil, errors.New("image snapshot identity, parent or kind is inconsistent")
	}
	return proto.CloneOf(info), nil
}

type runtimeImageResources struct {
	observer                *RuntimeImageObserver
	key                     string
	lease, view, activation bool
}

func (resources *runtimeImageResources) close(ctx context.Context) error {
	if resources.activation {
		if err := resources.observer.closeImageActivation(ctx, resources.key); err != nil {
			return fmt.Errorf("deactivate image observation %s (lease retained): %w", resources.key, err)
		}
	}
	if resources.view {
		_, err := resources.observer.snapshots.Remove(ctx, &snapshotsapi.RemoveSnapshotRequest{Snapshotter: resources.observer.snapshotter, Key: resources.key})
		if err != nil && status.Code(err) != codes.NotFound {
			return fmt.Errorf("remove image observation %s (lease retained): %w", resources.key, err)
		}
	}
	if resources.lease {
		_, err := resources.observer.leases.Delete(ctx, &leasesapi.DeleteRequest{ID: resources.key, Sync: true})
		if err != nil && status.Code(err) != codes.NotFound {
			return fmt.Errorf("release image observation %s: %w", resources.key, err)
		}
	}
	return nil
}

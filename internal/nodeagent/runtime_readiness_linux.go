package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/runtimechannel"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RuntimeWorkerEvidenceReporter obtains residency evidence from the original
// Runtime retained by startup. Each report redials and binds the socket creator
// to that pidfd; neither a replacement at the same path nor a stored READY
// template can renew capacity. The caller keeps startup custody alive throughout.
type RuntimeWorkerEvidenceReporter struct {
	Reporter         *WorkerInstanceEvidenceReporter
	Owner            *RuntimeNamespaceOwner
	Plan             *RuntimeLaunchPlan
	RegistryVerifier *journalbinding.Verifier
	SocketPath       string
}

func (reporter *RuntimeWorkerEvidenceReporter) Report(ctx context.Context, template WorkerInstanceEvidenceTemplate) (fleet.WorkerInstanceDecision, error) {
	if ctx == nil || reporter == nil || reporter.Reporter == nil || reporter.Owner == nil || reporter.Plan == nil || reporter.RegistryVerifier == nil {
		return fleet.WorkerInstanceDecision{}, ErrRuntimeLaunchPlan
	}
	manifest, err := reporter.Plan.LaunchManifest()
	if err != nil {
		return fleet.WorkerInstanceDecision{}, err
	}
	if template.Evidence.WorkerInstanceID.String() != manifest.WorkerInstanceID || template.Evidence.InstanceEpoch != manifest.WorkerInstanceEpoch || len(template.Evidence.Members) != 1 || template.Evidence.Members[0].ID.String() != manifest.WorkerMemberID || template.Evidence.Members[0].MemberEpoch != manifest.WorkerMemberEpoch {
		return fleet.WorkerInstanceDecision{}, ErrRuntimeLaunchPlan
	}
	connection, err := reporter.Owner.dialReadiness(ctx, reporter.SocketPath)
	if err != nil {
		return fleet.WorkerInstanceDecision{}, err
	}
	defer func(cleanup func() error) { _ = cleanup() }(connection.Close)
	client := velav1.NewModelRuntimeServiceClient(connection)
	identities, err := stageworkeragent.DiscoverRuntimeIdentities(ctx, client, stageworkeragent.RuntimeIdentityExpectation{WorkerInstanceID: manifest.WorkerInstanceID, WorkerInstanceEpoch: manifest.WorkerInstanceEpoch, WorkerMemberID: manifest.WorkerMemberID, WorkerMemberEpoch: manifest.WorkerMemberEpoch, RegistryBinding: reporter.Plan.RegistryBinding(), RegistryVerifier: reporter.RegistryVerifier})
	if err != nil {
		return fleet.WorkerInstanceDecision{}, err
	}
	if len(identities) != len(manifest.Runtimes) || len(template.Evidence.Residencies) != len(identities) {
		return fleet.WorkerInstanceDecision{}, ErrRuntimeLaunchPlan
	}
	evidence := cloneWorkerInstanceEvidence(template.Evidence)
	seen := make(map[string]bool)
	for _, identity := range identities {
		if seen[identity.ModelResidencyId] || hex.EncodeToString(identity.DeviceSetDigest) != manifest.DeviceSetDigest || hex.EncodeToString(identity.MembershipDigest) != manifest.MembershipDigest {
			return fleet.WorkerInstanceDecision{}, ErrRuntimeLaunchPlan
		}
		seen[identity.ModelResidencyId] = true
		found := false
		for _, expected := range manifest.Runtimes {
			if expected.ModelResidencyID != identity.ModelResidencyId {
				continue
			}
			if expected.RuntimeIdentity != identity.RuntimeIdentity || expected.StageProfileRevisionID != identity.StageProfileRevisionId || identity.ModelRuntimeEpoch < expected.ModelRuntimeEpochFloor {
				return fleet.WorkerInstanceDecision{}, ErrRuntimeLaunchPlan
			}
			for i := range evidence.Residencies {
				resident := &evidence.Residencies[i]
				if resident.ID.String() != identity.ModelResidencyId {
					continue
				}
				if resident.ModelComponentRevision != expected.ModelComponentRevision || resident.RuntimeIdentity != expected.RuntimeIdentity || resident.RuntimeImageDigest != "sha256:"+expected.RuntimeImageDigest {
					return fleet.WorkerInstanceDecision{}, ErrRuntimeLaunchPlan
				}
				proof, err := stageworkeragent.ProbeRuntimeReadiness(ctx, client, identity)
				if err != nil {
					return fleet.WorkerInstanceDecision{}, err
				}
				digest := sha256.Sum256(proof)
				resident.ModelRuntimeEpoch = identity.ModelRuntimeEpoch
				resident.State = "READY"
				resident.WarmupEvidenceDigest = hex.EncodeToString(digest[:])
				resident.CanaryEvidenceDigest = resident.WarmupEvidenceDigest
				found = true
			}
		}
		if !found {
			return fleet.WorkerInstanceDecision{}, ErrRuntimeLaunchPlan
		}
	}
	evidence.Members[0].Readiness = "READY"
	// GPU attestation is collected next. The observer wrapper checks the derived
	// topology against the startup plan immediately before Registry publication.
	delegate := *reporter.Reporter
	delegate.observer = &runtimeReadyObserver{owner: reporter.Owner, plan: reporter.Plan, next: reporter.Reporter.observer}
	return delegate.Report(ctx, WorkerInstanceEvidenceTemplate{Evidence: evidence, ObservedBy: template.ObservedBy})
}

type runtimeReadyObserver struct {
	owner *RuntimeNamespaceOwner
	plan  *RuntimeLaunchPlan
	next  WorkerInstanceObserver
}

func (observer *runtimeReadyObserver) Observe(ctx context.Context, evidence fleet.WorkerInstanceEvidence) (fleet.WorkerInstanceDecision, error) {
	manifest, err := observer.plan.LaunchManifest()
	if err != nil {
		return fleet.WorkerInstanceDecision{}, err
	}
	membership, err := hex.DecodeString(evidence.DeviceSet.MembershipDigest)
	if err != nil {
		return fleet.WorkerInstanceDecision{}, err
	}
	topology, err := hex.DecodeString(evidence.DeviceSet.TopologyDigest)
	if err != nil {
		return fleet.WorkerInstanceDecision{}, err
	}
	deviceSet := sha256.Sum256(append(membership, topology...))
	if manifest.DeviceSetDigest != hex.EncodeToString(deviceSet[:]) || manifest.MembershipDigest != evidence.DeviceSet.MembershipDigest {
		return fleet.WorkerInstanceDecision{}, ErrRuntimeLaunchPlan
	}
	for _, member := range manifest.Members {
		if member.ID != evidence.Members[0].ID.String() || member.IdentityDigest != evidence.Members[0].IdentityDigest || member.DeviceSubsetDigest != evidence.Members[0].DeviceSubsetDigest {
			return fleet.WorkerInstanceDecision{}, ErrRuntimeLaunchPlan
		}
	}
	if err := observer.owner.checkLive(ctx); err != nil {
		return fleet.WorkerInstanceDecision{}, err
	}
	return observer.next.Observe(ctx, evidence)
}

func (owner *RuntimeNamespaceOwner) checkLive(ctx context.Context) error {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err := errors.Join(context.Cause(ctx), owner.checkLocked()); err != nil {
		return err
	}
	return runtimechannel.PollLivePIDFD(int(owner.pidfd.Fd()))
}

func (owner *RuntimeNamespaceOwner) dialReadiness(ctx context.Context, path string) (*grpc.ClientConn, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || strings.ContainsRune(path, 0) {
		return nil, ErrRuntimeLaunchPlan
	}
	dialer := func(ctx context.Context, _ string) (net.Conn, error) { return owner.connectReadiness(ctx, path) }
	return grpc.NewClient("passthrough:///original-model-runtime", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<20), grpc.MaxCallSendMsgSize(1<<20)))
}

func (owner *RuntimeNamespaceOwner) connectReadiness(ctx context.Context, path string) (net.Conn, error) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err := errors.Join(context.Cause(ctx), owner.checkLocked()); err != nil {
		return nil, err
	}
	if err := runtimechannel.PollLivePIDFD(int(owner.pidfd.Fd())); err != nil {
		return nil, err
	}
	// The numeric path only opens a filesystem view while the independently
	// retained pidfd remains live. The connected socket creator is compared below.
	root, err := os.Open(fmt.Sprintf("/proc/%d/root", owner.owner.Process.HostPID))
	if err != nil {
		return nil, err
	}
	defer func(cleanup func() error) { _ = cleanup() }(root.Close)
	fd, err := unix.Openat2(int(root.Fd()), filepath.Dir(path), &unix.OpenHow{Flags: unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC, Resolve: unix.RESOLVE_IN_ROOT | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS})
	if err != nil {
		return nil, err
	}
	defer func(fd int) { _ = unix.Close(fd) }(fd)
	var before unix.Stat_t
	if err := unix.Fstatat(fd, filepath.Base(path), &before, unix.AT_SYMLINK_NOFOLLOW); err != nil || before.Uid != owner.owner.Process.UID || before.Mode&unix.S_IFMT != unix.S_IFSOCK || before.Mode&0o777 != 0o600 {
		return nil, errors.Join(ErrRuntimeCallerIdentity, err)
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", fmt.Sprintf("/proc/self/fd/%d/%s", fd, filepath.Base(path)))
	if err != nil {
		return nil, err
	}
	accepted := false
	defer func() {
		if !accepted {
			_ = connection.Close()
		}
	}()
	raw, err := connection.(*net.UnixConn).SyscallConn()
	if err != nil {
		return nil, err
	}
	var peerErr error
	err = raw.Control(func(socket uintptr) {
		peer, e := unix.GetsockoptUcred(int(socket), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if e != nil || peer.Uid != owner.owner.Process.UID || peer.Gid != owner.owner.Process.GID {
			peerErr = errors.Join(ErrRuntimeCallerIdentity, e)
			return
		}
		pidfd, e := unix.GetsockoptInt(int(socket), unix.SOL_SOCKET, unix.SO_PEERPIDFD)
		if e != nil {
			peerErr = e
			return
		}
		defer func(fd int) { _ = unix.Close(fd) }(pidfd)
		peerErr = runtimechannel.SameLiveProcess(int(owner.pidfd.Fd()), pidfd)
	})
	var after unix.Stat_t
	statErr := unix.Fstatat(fd, filepath.Base(path), &after, unix.AT_SYMLINK_NOFOLLOW)
	// Connecting can change socket timestamps; ownership, inode and mode cannot.
	if err := errors.Join(err, peerErr, statErr); err != nil {
		return nil, err
	}
	if before.Dev != after.Dev || before.Ino != after.Ino || before.Uid != after.Uid || before.Gid != after.Gid || before.Mode != after.Mode {
		return nil, ErrRuntimeCallerIdentity
	}
	accepted = true
	return connection, nil
}

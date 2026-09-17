package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/runtimechannel"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// RuntimeStartupRegistry must be the authenticated Node Fleet client. It has
// deliberately no history-to-permission or retry method.
type RuntimeStartupRegistry interface {
	NodeIdentity() string
	ActorIdentity() string
	ReserveRuntimeStartup(context.Context, fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error)
}

type RuntimeStartupReservationConfig struct {
	Plan                         *RuntimeLaunchPlan
	Pods                         RuntimeLaunchPodReader
	Observer                     *RuntimeContainerObserver
	Caller                       *RuntimeCaller
	Journal                      *modelruntime.ExecutionJournalOwner
	Registry                     RuntimeStartupRegistry
	PolicyAuthorizationPublisher func(context.Context, []byte) error
	// RuntimeOwner is optionally retained before the Runtime sends its startup
	// handshake. The ledger then reuses this exact pidfd for the durable record.
	RuntimeOwner *RuntimeNamespaceOwner
	publication  *RuntimeStartupPublicationConfig
}

// RuntimeStartupRemoteIntent binds Node's held storage and actual journal
// routes to its original process observation. Executable is a sampled file
// observation, not effective OCI approval or proof of uninterrupted execution.
type RuntimeStartupRemoteIntent struct {
	JournalIdentity modelruntime.ExecutionJournalIdentity `json:"journal_identity"`
	JournalDigest   [sha256.Size]byte                     `json:"journal_digest"`
	Epochs          []fleet.RuntimeStartupEpoch           `json:"epochs"`
	Executable      RuntimeExecutableObservation          `json:"executable"`
	Bootstrap       *RuntimeStartupBootstrapObservation   `json:"bootstrap,omitempty"`
	RemoteCLI       *RuntimeRemoteCLIObservation          `json:"remote_cli,omitempty"`
}

// RuntimeStartupReservationRecord records a committed Fleet reservation. It
// has no Fresh or Permit field and is never sufficient for backend startup.
type RuntimeStartupReservationRecord struct {
	OperationID         uuid.UUID         `json:"operation_id"`
	JournalID           uuid.UUID         `json:"journal_id"`
	RequestDigest       [sha256.Size]byte `json:"request_digest"`
	ReservedAt          time.Time         `json:"reserved_at"`
	RecordedAt          time.Time         `json:"recorded_at"`
	PolicyAuthorization []byte            `json:"policy_authorization,omitempty"`
}

// ReserveRemote records the exact owner/intent before calling Fleet once, then
// rechecks the live owner, Pod, executable and Node journal before persisting its
// reservation receipt. Errors leave the intent unresolved; even response loss
// and Node restart cannot initiate another reservation. This method sends no
// caller reply and issues no startup grant. Effective launch approval and a
// same-invocation grant transaction must precede any future permission response.
func (ledger *RuntimeStartupLedger) ReserveRemote(ctx context.Context, config RuntimeStartupReservationConfig) (RuntimeStartupReservationRecord, error) {
	return ledger.reserveRemote(ctx, config, nil)
}

func (ledger *RuntimeStartupLedger) reserveRemote(ctx context.Context, config RuntimeStartupReservationConfig, image *runtimeStartupImageCheck) (RuntimeStartupReservationRecord, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeStartupReservationRecord{}, err
	}
	if ledger == nil || config.Plan == nil || config.Plan.binding == nil || config.Caller == nil || config.Registry == nil ||
		config.Registry.NodeIdentity() != config.Plan.binding.Claim.NodeIdentity || config.Registry.ActorIdentity() != config.Plan.binding.Claim.ActorIdentity {
		return RuntimeStartupReservationRecord{}, ErrRuntimeStartupLedger
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	remote, err := inspectRemoteStartup(ctx, config)
	if err != nil {
		return RuntimeStartupReservationRecord{}, err
	}
	if err := image.inspect(ctx, config); err != nil {
		return RuntimeStartupReservationRecord{}, err
	}
	if image != nil && image.first.RemoteCLI != nil {
		copy := *image.first.RemoteCLI
		remote.RemoteCLI = &copy
	}
	record, err := ledger.record(ctx, config.Plan, config.Pods, config.Observer, config.Caller, &remote, config.RuntimeOwner)
	if err != nil {
		return RuntimeStartupReservationRecord{}, err
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if err := image.check(ctx, config, record); err != nil {
		return RuntimeStartupReservationRecord{}, err
	}
	// Image measurement can block on containerd. Recheck held storage and the
	// original owner afterwards, immediately before consuming Fleet authority.
	if err := ledger.checkRemoteStartup(ctx, config, record); err != nil {
		return RuntimeStartupReservationRecord{}, err
	}
	request, err := remoteFleetRequest(record)
	if err != nil {
		return RuntimeStartupReservationRecord{}, err
	}
	wire, err := json.Marshal(request)
	if err != nil {
		return RuntimeStartupReservationRecord{}, err
	}
	// Preserve our own request snapshot even if an in-process adapter mutates its
	// argument. The production transport also owns all buffers at its boundary.
	sent := request
	sent.RuntimeScope, sent.LaunchDigest = slices.Clone(request.RuntimeScope), slices.Clone(request.LaunchDigest)
	sent.OwnerObservationDigest, sent.Epochs = slices.Clone(request.OwnerObservationDigest), slices.Clone(request.Epochs)
	reservation, err := config.Registry.ReserveRuntimeStartup(ctx, sent)
	if err != nil {
		return RuntimeStartupReservationRecord{}, err
	}
	if !reservation.Fresh || reservation.ReservedAt.IsZero() || !reflect.DeepEqual(reservation.RuntimeStartupRequest, request) {
		return RuntimeStartupReservationRecord{}, ErrRuntimeStartupRecorded
	}
	if config.PolicyAuthorizationPublisher != nil {
		if len(reservation.PolicyAuthorization) == 0 {
			return RuntimeStartupReservationRecord{}, errors.New("fleet reservation did not include policy authorization")
		}
		if err := config.PolicyAuthorizationPublisher(ctx, reservation.PolicyAuthorization); err != nil {
			return RuntimeStartupReservationRecord{}, fmt.Errorf("publish Fleet policy authorization: %w", err)
		}
	}
	if err := image.check(ctx, config, record); err != nil {
		return RuntimeStartupReservationRecord{}, err
	}
	if err := ledger.checkRemoteStartup(ctx, config, record); err != nil {
		return RuntimeStartupReservationRecord{}, err
	}
	result := RuntimeStartupReservationRecord{OperationID: record.OperationID, JournalID: record.Request.JournalID,
		RequestDigest: sha256.Sum256(wire), ReservedAt: reservation.ReservedAt.UTC(), RecordedAt: time.Now().UTC(), PolicyAuthorization: slices.Clone(reservation.PolicyAuthorization)}
	if err := ledger.append(ctx, runtimeStartupEntry{Reservation: &result}); err != nil {
		return RuntimeStartupReservationRecord{}, err
	}
	return result, nil
}

func inspectRemoteStartup(ctx context.Context, config RuntimeStartupReservationConfig) (RuntimeStartupRemoteIntent, error) {
	if err := config.Journal.RequireRootCustody(ctx); err != nil {
		return RuntimeStartupRemoteIntent{}, err
	}
	var manifest modelruntime.LaunchManifest
	if err := json.Unmarshal(config.Plan.manifest, &manifest); err != nil {
		return RuntimeStartupRemoteIntent{}, err
	}
	request, _, err := parseRuntimeStartupPlan(config.Plan, config.Caller.Payload())
	if err != nil {
		return RuntimeStartupRemoteIntent{}, err
	}
	if request.SchemaVersion == 2 && config.publication == nil {
		return RuntimeStartupRemoteIntent{}, ErrRuntimeStartupPublication
	}
	snapshot, err := config.Journal.InspectStartup(ctx, manifest, request)
	if err != nil {
		return RuntimeStartupRemoteIntent{}, err
	}
	status := snapshot.Status()
	result := RuntimeStartupRemoteIntent{JournalIdentity: modelruntime.ExecutionJournalIdentity{
		JournalID: status.JournalID, Scope: status.Scope, Storage: status.Storage}, JournalDigest: snapshot.Digest()}
	bindings, err := modelruntime.RemoteStartupBindings(manifest)
	if err != nil {
		return RuntimeStartupRemoteIntent{}, err
	}
	for _, binding := range bindings {
		result.Epochs = append(result.Epochs, fleet.RuntimeStartupEpoch{ModelResidencyID: uuid.MustParse(binding.ModelResidencyID),
			StageProfileRevisionID: uuid.MustParse(binding.StageProfileRevisionID), ModelRuntimeIdentity: binding.ModelRuntimeIdentity, ModelRuntimeEpoch: binding.ModelRuntimeEpoch})
	}
	slices.SortFunc(result.Epochs, func(a, b fleet.RuntimeStartupEpoch) int {
		return strings.Compare(a.ModelResidencyID.String(), b.ModelResidencyID.String())
	})
	result.Bootstrap, err = inspectStartupPublication(ctx, config)
	if err != nil {
		return RuntimeStartupRemoteIntent{}, err
	}
	return result, nil
}

func (ledger *RuntimeStartupLedger) checkRemoteStartup(ctx context.Context, config RuntimeStartupReservationConfig, record RuntimeStartupRecord) error {
	if err := errors.Join(ledger.check(), context.Cause(ctx)); err != nil {
		return err
	}
	retained, err := retainJournalProcess(ctx, ledger.owners[record.Request.JournalID])
	if err != nil {
		return err
	}
	defer func() { _ = retained.Close() }()
	config.Caller.mu.Lock()
	_, err = config.Caller.inspectLocked(ctx)
	if err == nil {
		err = runtimechannel.SameLiveProcess(int(retained.Fd()), int(config.Caller.pidfd.Fd()))
	}
	config.Caller.mu.Unlock()
	if err != nil {
		return err
	}
	current, err := inspectRemoteStartup(ctx, config)
	if err != nil {
		return err
	}
	current.Executable = record.Remote.Executable
	// image.check independently re-observes the CLI vectors before this check.
	current.RemoteCLI = record.Remote.RemoteCLI
	if !reflect.DeepEqual(current, *record.Remote) {
		return ErrRuntimeStartupLedger
	}
	observation, err := config.Observer.observePlannedCaller(ctx, config.Plan, config.Pods, config.Caller, config.Caller.Payload())
	if err != nil || observation.PodResourceVersion != record.PodResourceVersion {
		return errors.Join(ErrRuntimeLaunchPlan, err)
	}
	originalOwner, currentOwner := record.Owner, observation.Caller
	originalOwner.ObservedFrom, originalOwner.ObservedThrough = currentOwner.ObservedFrom, currentOwner.ObservedThrough
	originalOwner.Container.ObservedFrom, originalOwner.Container.ObservedThrough = currentOwner.Container.ObservedFrom, currentOwner.Container.ObservedThrough
	originalOwner.Process.ObservedAt = currentOwner.Process.ObservedAt
	originalExecutable, currentExecutable := record.Remote.Executable, observation.Executable
	originalExecutable.ObservedFrom, originalExecutable.ObservedThrough = currentExecutable.ObservedFrom, currentExecutable.ObservedThrough
	originalExecutable.Process.ObservedAt = currentExecutable.Process.ObservedAt
	if originalOwner != currentOwner || originalExecutable != currentExecutable {
		return ErrRuntimeNamespaceOwnerLost
	}
	return context.Cause(ctx)
}

func remoteFleetRequest(record RuntimeStartupRecord) (fleet.RuntimeStartupRequest, error) {
	if record.Remote == nil {
		return fleet.RuntimeStartupRequest{}, ErrRuntimeStartupLedger
	}
	var binding velav1.WorkerBootstrapBinding
	if err := proto.Unmarshal(record.RegistryBinding, &binding); err != nil {
		return fleet.RuntimeStartupRequest{}, err
	}
	bootstrapID, err := uuid.Parse(binding.GetClaim().GetRequestId())
	if err != nil || bootstrapID == uuid.Nil {
		return fleet.RuntimeStartupRequest{}, ErrRuntimeStartupLedger
	}
	wire, err := json.Marshal(record)
	if err != nil {
		return fleet.RuntimeStartupRequest{}, err
	}
	digest := sha256.Sum256(wire)
	return fleet.RuntimeStartupRequest{RequestID: record.OperationID, BootstrapRequestID: bootstrapID,
		NodeIdentity: record.Request.NodeIdentity, ActorIdentity: binding.Claim.ActorIdentity,
		RuntimeJournalID: record.Request.JournalID, RuntimeScope: slices.Clone(record.Request.JournalScope[:]),
		IncarnationID: record.Request.IncarnationID, LaunchDigest: slices.Clone(record.Request.LaunchDigest[:]),
		OwnerObservationDigest: digest[:], Epochs: slices.Clone(record.Remote.Epochs)}, nil
}

func validateRemoteStartupRecord(record RuntimeStartupRecord) error {
	remote := record.Remote
	if remote.RemoteCLI != nil && (record.Request.SchemaVersion != 2 || remote.Bootstrap == nil || remote.RemoteCLI.SchemaVersion != 1 || remote.RemoteCLI.ArgumentsDigest == ([sha256.Size]byte{}) || remote.RemoteCLI.EnvironmentDigest == ([sha256.Size]byte{})) {
		return ErrRuntimeStartupLedger
	}
	if record.Request.SchemaVersion == 2 && (remote.Bootstrap == nil || remote.Bootstrap.Publication.BootstrapDigest != record.Request.BootstrapDigest || remote.Bootstrap.BootstrapPath != record.Request.BootstrapPath) {
		return ErrRuntimeStartupLedger
	}
	if remote.Bootstrap != nil {
		bootstrap := remote.Bootstrap
		publication := bootstrap.Publication
		if !validRuntimeBootstrapPath(bootstrap.BootstrapPath) || bootstrap.MountID == 0 || !strings.HasPrefix(bootstrap.MountNamespace, "mnt:[") || !strings.HasSuffix(bootstrap.MountNamespace, "]") ||
			publication.SchemaVersion != 1 || publication.ID == uuid.Nil || publication.NodeIdentity != record.Request.NodeIdentity || publication.BindingDigest != record.Request.RegistryBindingDigest ||
			publication.RuntimeGID != record.Owner.Process.GID || publication.RecordedAt.IsZero() || publication.RecordedAt.Location() != time.UTC ||
			publication.Directory.Inode == 0 || publication.RecordFile.Inode == 0 || publication.BootstrapFile.Inode == 0 || publication.BootstrapBytes <= 0 || publication.BootstrapBytes > modelruntime.MaximumRemoteBootstrapBytes || publication.BootstrapDigest == ([sha256.Size]byte{}) {
			return ErrRuntimeStartupLedger
		}
	}
	process := remote.Executable.Process
	process.ObservedAt = record.Owner.Process.ObservedAt
	if remote.JournalIdentity.JournalID != record.Request.JournalID || remote.JournalIdentity.Scope != record.Request.JournalScope ||
		!remote.JournalIdentity.Storage.Valid() || remote.JournalDigest == ([sha256.Size]byte{}) ||
		remote.Executable.Digest == ([sha256.Size]byte{}) || remote.Executable.SizeBytes <= 0 || remote.Executable.FileInode == 0 ||
		process != record.Owner.Process || len(remote.Epochs) == 0 || len(remote.Epochs) > 64 {
		return ErrRuntimeStartupLedger
	}
	for i, epoch := range remote.Epochs {
		if epoch.ModelResidencyID == uuid.Nil || epoch.StageProfileRevisionID == uuid.Nil || epoch.ModelRuntimeEpoch <= 0 ||
			!validText(epoch.ModelRuntimeIdentity, 300) || (i > 0 && strings.Compare(remote.Epochs[i-1].ModelResidencyID.String(), epoch.ModelResidencyID.String()) >= 0) {
			return ErrRuntimeStartupLedger
		}
	}
	return nil
}

func (ledger *RuntimeStartupLedger) applyReservation(record RuntimeStartupReservationRecord) error {
	startup, ok := ledger.starts[record.JournalID]
	_, duplicate := ledger.reservations[record.JournalID]
	_, exited := ledger.exits[record.JournalID]
	if ledger.header.SchemaVersion < 2 || !ok || duplicate || exited || record.OperationID != startup.OperationID ||
		record.ReservedAt.IsZero() || record.RecordedAt.Before(startup.RecordedAt) ||
		record.ReservedAt.Location() != time.UTC || record.RecordedAt.Location() != time.UTC {
		return ErrRuntimeStartupLedger
	}
	request, err := remoteFleetRequest(startup)
	if err != nil {
		return err
	}
	wire, err := json.Marshal(request)
	if err != nil || record.RequestDigest != sha256.Sum256(wire) {
		return ErrRuntimeStartupLedger
	}
	ledger.reservations[record.JournalID] = record
	return nil
}

// InspectReservation is history only. Reopening this ledger never reconstructs
// its original pidfd or permits a repeated Fleet request or caller reply.
func (ledger *RuntimeStartupLedger) InspectReservation(ctx context.Context, journalID uuid.UUID) (RuntimeStartupReservationRecord, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeStartupReservationRecord{}, err
	}
	if ledger == nil {
		return RuntimeStartupReservationRecord{}, ErrRuntimeStartupLedger
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if err := errors.Join(ledger.check(), context.Cause(ctx)); err != nil {
		return RuntimeStartupReservationRecord{}, err
	}
	result, ok := ledger.reservations[journalID]
	if !ok {
		return RuntimeStartupReservationRecord{}, os.ErrNotExist
	}
	return result, nil
}

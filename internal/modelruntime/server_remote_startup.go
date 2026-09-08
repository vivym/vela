package modelruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"time"

	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// RemoteRuntimeStartup is trusted server assembly for first startup only. Node
// must already own the Registry-bound journal, persist its startup intent and
// reserve the exact epochs derived from the approved manifest. It must authorize
// this original process once; a repeated callback success is not proof of that.
// No local epoch allocation, file adoption or replacement backend is supported.
type RemoteRuntimeStartup struct {
	Journal   RemoteExecutionJournalConfig
	Authorize func(context.Context, RemoteBackendStartupRequest) error
}

// RemoteBackendStartupRequest declares Node custody and all proposed live
// bindings explicitly. It is intentionally not the local-file startup request.
// The authorizer must independently match these facts to live Fleet approval,
// retained process identity and its own durable journal/epoch reservation.
type RemoteBackendStartupRequest struct {
	Intent          BackendStartupRequest
	JournalIdentity ExecutionJournalIdentity
	Bindings        []stageauthority.RuntimeBinding
}

// RemoteStartupBindings chooses the first epoch strictly above each approved
// launch floor. The Node authorizer must reserve and approve these exact values;
// computing them alone grants no startup or replacement permission.
func RemoteStartupBindings(manifest LaunchManifest) ([]stageauthority.RuntimeBinding, error) {
	bindings, err := manifest.RuntimeBindings()
	if err != nil {
		return nil, err
	}
	for i := range bindings {
		floor := manifest.Runtimes[i].ModelRuntimeEpochFloor
		if floor < 0 || floor == math.MaxInt64 {
			return nil, ErrBackendStartupDenied
		}
		bindings[i].ModelRuntimeEpoch = floor + 1
	}
	return bindings, nil
}

func prepareRemoteRuntimeStartup(ctx context.Context, config *RuntimeServerConfig) (*remoteExecutionJournal, error) {
	if config.RemoteStartup == nil {
		return nil, nil
	}
	if config.EpochStore != nil || config.ExecutionFloor != nil || config.BackendStartupGate != nil || config.RegistryBinding == nil || config.RegistryVerifier == nil {
		return nil, errors.New("remote startup requires Registry binding and exclusive Node journal/epoch custody")
	}
	remote := *config.RemoteStartup
	if remote.Authorize == nil {
		return nil, ErrBackendStartupGateRequired
	}
	manifest, err := EncodeLaunchManifest(config.Manifest)
	if err != nil {
		return nil, err
	}
	declared, err := EncodeLaunchManifest(remote.Journal.Manifest)
	if err != nil || !bytes.Equal(manifest, declared) || remote.Journal.Validator != config.Validator {
		return nil, ErrBackendStartupDenied
	}
	remote.Journal.Manifest = cloneLaunchManifest(config.Manifest)
	config.RemoteStartup = &remote
	bindings, err := RemoteStartupBindings(config.Manifest)
	if err != nil {
		return nil, err
	}
	if err := config.RegistryVerifier.VerifyJournal(config.RegistryBinding, journalbinding.RuntimeJournal, journalbinding.Journal{
		WorkerInstanceID: config.Manifest.WorkerInstanceID, WorkerInstanceEpoch: config.Manifest.WorkerInstanceEpoch,
		WorkerMemberID: config.Manifest.WorkerMemberID, WorkerMemberEpoch: config.Manifest.WorkerMemberEpoch,
		JournalID: remote.Journal.Identity.JournalID, Scope: remote.Journal.Identity.Scope,
	}); err != nil {
		return nil, err
	}
	store, err := newRemoteExecutionJournal(ctx, remote.Journal)
	if err != nil {
		return nil, err
	}
	config.EpochStore = EpochStoreFunc(func(binding stageauthority.RuntimeBinding) (int64, error) {
		for _, expected := range bindings {
			if binding.ModelResidencyID == expected.ModelResidencyID && binding.ModelRuntimeIdentity == expected.ModelRuntimeIdentity && binding.StageProfileRevisionID == expected.StageProfileRevisionID {
				return expected.ModelRuntimeEpoch, nil
			}
		}
		return 0, stageauthority.ErrRuntimeMismatch
	})
	return store, nil
}

func (store *remoteExecutionJournal) authorizeRemoteStartup(ctx context.Context, binding *velav1.WorkerBootstrapBinding, authorize func(context.Context, RemoteBackendStartupRequest) error) error {
	if authorize == nil {
		return ErrBackendStartupGateRequired
	}
	if err := store.checkFreshStartup(ctx); err != nil {
		return err
	}
	bindings, err := RemoteStartupBindings(store.config.Manifest)
	if err != nil {
		return err
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(binding)
	if err != nil {
		return err
	}
	lifecycle := store.config.Startup
	request := RemoteBackendStartupRequest{Intent: BackendStartupRequest{SchemaVersion: 1, NodeIdentity: binding.GetClaim().GetNodeIdentity(),
		RegistryBindingDigest: sha256.Sum256(wire), JournalID: store.state.ID, JournalScope: store.state.Scope,
		IncarnationID: lifecycle.IncarnationID, LaunchDigest: lifecycle.LaunchDigest}, JournalIdentity: store.config.Identity, Bindings: bindings}
	if err := request.Intent.Validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	if err := authorize(ctx, request); err != nil {
		return errors.Join(ErrBackendStartupDenied, err)
	}
	return errors.Join(context.Cause(ctx), store.checkFreshStartup(ctx))
}

func (store *remoteExecutionJournal) checkFreshStartup(ctx context.Context) error {
	if err := store.checkContext(ctx); err != nil {
		return err
	}
	return store.requireFreshStartup()
}

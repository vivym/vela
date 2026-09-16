package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/vivym/vela/internal/authoritypolicy"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/runtimelaunch"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

type durableWorkerLaunch struct {
	manifest  modelruntime.LaunchManifest
	admission stageworkeragent.AssignmentAdmissionConfig
	leader    bool
}

func loadDurableWorkerLaunch(configuration config) (*durableWorkerLaunch, error) {
	if configuration.launchManifestFile == "" && configuration.assignmentAdmissionRoot == "" && configuration.assignmentAdmissionLimit == 0 &&
		configuration.journalBindingFile == "" && configuration.journalBindingVerifierFile == "" {
		return nil, nil
	}
	if configuration.launchManifestFile == "" || configuration.journalBindingFile == "" || configuration.journalBindingVerifierFile == "" ||
		configuration.assignmentAdmissionLimit < 1 || configuration.assignmentAdmissionLimit > 64 ||
		!filepath.IsAbs(configuration.assignmentAdmissionRoot) || filepath.Clean(configuration.assignmentAdmissionRoot) != configuration.assignmentAdmissionRoot {
		return nil, errors.New("durable Worker requires launch manifest, Registry binding and verifier, canonical assignment journal directory and history bound")
	}
	for _, root := range []string{configuration.productionStateRoot, configuration.inputRoot, configuration.inputTransferJournalRoot, configuration.outputRoot, configuration.materializationJournalRoot} {
		if root == "" {
			continue
		}
		if root == configuration.assignmentAdmissionRoot || strings.HasPrefix(root, configuration.assignmentAdmissionRoot+string(filepath.Separator)) ||
			strings.HasPrefix(configuration.assignmentAdmissionRoot, root+string(filepath.Separator)) {
			return nil, errors.New("assignment journal must not overlap other Worker state or content roots")
		}
	}
	manifest, err := modelruntime.LoadLaunchManifest(configuration.launchManifestFile)
	if err != nil {
		return nil, err
	}
	admission, err := offlineAdmissionConfig(manifest, stageworkeragent.AssignmentAdmissionConfig{
		Directory: configuration.assignmentAdmissionRoot, MaxRecords: configuration.assignmentAdmissionLimit,
		MaxClockSkew: authoritypolicy.ProductionMaxClockSkew,
	})
	if err != nil {
		return nil, err
	}
	if manifest.WorkerInstanceID != configuration.workerInstanceID.String() || manifest.WorkerInstanceEpoch != configuration.workerInstanceEpoch ||
		manifest.WorkerMemberID != configuration.workerMemberID.String() || manifest.WorkerMemberEpoch != configuration.workerMemberEpoch ||
		admission.InputRoot != configuration.inputRoot || admission.OutputRoot != configuration.outputRoot ||
		len(manifest.LocalDevices) != len(configuration.devices) || len(manifest.Members) != len(configuration.members) {
		return nil, errors.New("worker configuration does not match approved launch topology or scratch roots")
	}
	for _, runtime := range manifest.Runtimes {
		if runtime.ScratchRoot != configuration.scratchRoot {
			return nil, errors.New("worker scratch root does not match launch manifest")
		}
	}
	seenDevices := make(map[string]bool)
	for _, device := range configuration.devices {
		if device == nil || len(device.ProtoReflect().GetUnknown()) != 0 || seenDevices[device.GetDeviceId()] ||
			!slices.ContainsFunc(manifest.LocalDevices, func(approved modelruntime.DriverDevice) bool {
				return approved.DeviceID == device.GetDeviceId() && approved.DeviceEpoch == device.GetDeviceEpoch()
			}) {
			return nil, errors.New("worker local devices do not match launch manifest")
		}
		seenDevices[device.GetDeviceId()] = true
	}
	if _, _, _, err := configuredRuntimeMembers(configuration); err != nil {
		return nil, err
	}
	for _, member := range configuration.members {
		if !slices.ContainsFunc(admission.Bindings, func(approved stageworkeragent.AdmissionRuntimeBinding) bool {
			return approved.Runtime.WorkerMemberID == member.workerMemberID.String() && approved.Runtime.WorkerMemberEpoch == member.memberEpoch &&
				approved.IdentityDigest == member.identityDigest
		}) {
			return nil, errors.New("worker membership does not match launch manifest")
		}
	}
	publicationRoot := runtimelaunch.MemberRoot(configuration.workerMemberID.String()) + "/worker-bootstrap/"
	if configuration.journalBindingFile == publicationRoot+"binding.json" && configuration.journalBindingVerifierFile == publicationRoot+"verifier.json" {
		admission.RegistryBinding, admission.RegistryVerifier, err = journalbinding.LoadNodePublication(configuration.journalBindingFile, configuration.journalBindingVerifierFile, uint32(os.Getegid()))
	} else {
		admission.RegistryVerifier, err = journalbinding.ReadVerifierFile(configuration.journalBindingVerifierFile)
		if err == nil {
			admission.RegistryBinding, err = journalbinding.LoadFile(configuration.journalBindingFile, admission.RegistryVerifier)
		}
	}
	if err != nil {
		return nil, err
	}
	return &durableWorkerLaunch{manifest: manifest, admission: admission,
		leader: configuration.workerMemberID == configuration.members[0].workerMemberID}, nil
}

func (launch *durableWorkerLaunch) openAdmission(ctx context.Context, validator *stageauthority.Validator) (*stageworkeragent.FileAssignmentAdmission, error) {
	launch.admission.Validator = validator
	configuration := launch.admission
	configuration.DeferRuntimeRoutes = true
	gate, err := stageworkeragent.NewFileAssignmentAdmission(configuration)
	if err != nil {
		return nil, fmt.Errorf("recover Worker assignment journal before startup: %w", err)
	}
	history, err := gate.Snapshot(ctx)
	if err != nil {
		return nil, errors.Join(err, gate.Close())
	}
	if !launch.leader && (history.Latest != nil || len(history.Pending) != 0 || history.Floor != 0 || len(history.Retirements) != 0) {
		return nil, errors.Join(errors.New("nonleader Worker cannot recover Leader assignment or retirement history"), gate.Close())
	}
	return gate, nil
}

func (launch *durableWorkerLaunch) bindMember(id string, identities []*velav1.ModelRuntimeIdentity) ([]stageworkeragent.AdmissionRuntimeBinding, error) {
	index := slices.IndexFunc(launch.admission.Bindings, func(binding stageworkeragent.AdmissionRuntimeBinding) bool {
		return binding.Runtime.WorkerMemberID == id
	})
	if index < 0 || len(identities) != len(launch.manifest.Runtimes) {
		return nil, errors.New("runtime discovery does not cover approved member routes")
	}
	topology := launch.admission.Bindings[index]
	result := make([]stageworkeragent.AdmissionRuntimeBinding, 0, len(identities))
	seen := make(map[string]bool)
	for _, identity := range identities {
		if identity == nil || seen[identity.GetModelResidencyId()] {
			return nil, errors.New("runtime discovery contains a missing or duplicate residency")
		}
		routeIndex := slices.IndexFunc(launch.manifest.Runtimes, func(route modelruntime.LaunchRuntime) bool {
			return route.ModelResidencyID == identity.GetModelResidencyId()
		})
		if routeIndex < 0 {
			return nil, errors.New("runtime discovery contains an unapproved residency")
		}
		route := launch.manifest.Runtimes[routeIndex]
		expected := &velav1.ModelRuntimeIdentity{
			WorkerInstanceId: topology.Runtime.WorkerInstanceID, WorkerInstanceEpoch: topology.Runtime.WorkerInstanceEpoch,
			WorkerMemberId: id, WorkerMemberEpoch: topology.Runtime.WorkerMemberEpoch,
			DeviceSetDigest: bytes.Clone(topology.Runtime.DeviceSetDigest), MembershipDigest: bytes.Clone(topology.Runtime.MembershipDigest),
			ModelResidencyId: route.ModelResidencyID, RuntimeIdentity: route.RuntimeIdentity, StageProfileRevisionId: route.StageProfileRevisionID,
			ModelRuntimeEpoch: identity.GetModelRuntimeEpoch(),
		}
		if identity.GetModelRuntimeEpoch() <= route.ModelRuntimeEpochFloor || !proto.Equal(expected, identity) {
			return nil, errors.New("runtime discovery does not match approved launch identity and epoch floor")
		}
		binding := topology
		binding.Runtime.ModelResidencyID, binding.Runtime.ModelRuntimeIdentity = route.ModelResidencyID, route.RuntimeIdentity
		binding.Runtime.StageProfileRevisionID, binding.Runtime.ModelRuntimeEpoch = route.StageProfileRevisionID, identity.GetModelRuntimeEpoch()
		result = append(result, binding)
		seen[route.ModelResidencyID] = true
	}
	return result, nil
}

func (launch *durableWorkerLaunch) bindExecution(ctx context.Context, members []stageworkeragent.RuntimeMember, local []*velav1.ModelRuntimeIdentity) ([]stageworkeragent.AdmissionRuntimeBinding, *stageworkeragent.ExecutionFloorConfig, error) {
	bindings := make([]stageworkeragent.AdmissionRuntimeBinding, 0)
	floor := &stageworkeragent.ExecutionFloorConfig{Validator: launch.admission.Validator, CurrentReaders: make(map[string]*velav1.ModelRuntimeIdentity)}
	for _, member := range members {
		identities := local
		if member.ID != launch.manifest.WorkerMemberID {
			index := slices.IndexFunc(launch.manifest.Members, func(approved modelruntime.LaunchMemberEpoch) bool { return approved.ID == member.ID })
			if index < 0 {
				return nil, nil, errors.New("runtime client is outside approved membership")
			}
			var err error
			identities, err = stageworkeragent.DiscoverRuntimeIdentities(ctx, member.Client, stageworkeragent.RuntimeIdentityExpectation{
				WorkerInstanceID: launch.manifest.WorkerInstanceID, WorkerInstanceEpoch: launch.manifest.WorkerInstanceEpoch,
				WorkerMemberID: member.ID, WorkerMemberEpoch: launch.manifest.Members[index].Epoch,
				RegistryVerifier: launch.admission.RegistryVerifier,
			})
			if err != nil {
				return nil, nil, err
			}
		}
		current, err := launch.bindMember(member.ID, identities)
		if err != nil {
			return nil, nil, err
		}
		bindings = append(bindings, current...)
		for _, binding := range current {
			floor.Bindings = append(floor.Bindings, stageworkeragent.ExecutionFloorBinding(binding))
		}
		reader := slices.MinFunc(identities, func(a, b *velav1.ModelRuntimeIdentity) int {
			return strings.Compare(a.GetModelResidencyId(), b.GetModelResidencyId())
		})
		floor.CurrentReaders[member.ID] = proto.Clone(reader).(*velav1.ModelRuntimeIdentity)
	}
	if launch.leader {
		if len(floor.CurrentReaders) != len(launch.manifest.Members) {
			return nil, nil, errors.New("durable Leader requires every approved Runtime member")
		}
		return bindings, floor, nil
	}
	// A follower never resolves Leader inputs or collects global retirement proof.
	// Its unopened peer routes remain topology-only and cannot admit execution.
	for _, binding := range launch.admission.Bindings {
		if binding.Runtime.WorkerMemberID != launch.manifest.WorkerMemberID {
			bindings = append(bindings, binding)
		}
	}
	return bindings, nil, nil
}

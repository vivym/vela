package modelruntime

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"strings"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

// Journal ownership is member-wide and independent of running backends or
// resident profile/epoch changes. Current execution routes are checked separately.
type executionJournalScope struct {
	binding stageauthority.RuntimeBinding
	floor   *executionFloorVerifier
}

func (scope executionJournalScope) digest() ([sha256.Size]byte, error) {
	binding := cloneBinding(scope.binding)
	binding.ModelResidencyID, binding.ModelRuntimeIdentity, binding.StageProfileRevisionID = "", "", ""
	binding.ModelRuntimeEpoch = 0
	slices.SortFunc(binding.Devices, func(a, b stageauthority.DeviceEpoch) int { return strings.Compare(a.ID, b.ID) })
	slices.SortFunc(binding.Members, func(a, b stageauthority.MemberEpoch) int { return strings.Compare(a.ID, b.ID) })
	document := struct {
		Binding stageauthority.RuntimeBinding
		Members []ExecutionFloorMember
	}{Binding: binding}
	for _, member := range binding.Members {
		document.Members = append(document.Members, scope.floor.members[member.ID])
	}
	wire, err := json.Marshal(document)
	return sha256.Sum256(wire), err
}

func (scope executionJournalScope) matchRetainedExecutionScope(authority *velav1.StageAuthority) error {
	baseline := scope.binding
	if authority.GetWorkerInstanceId() != baseline.WorkerInstanceID || authority.GetWorkerInstanceEpoch() != baseline.WorkerInstanceEpoch ||
		!bytes.Equal(authority.GetDeviceSetDigest(), baseline.DeviceSetDigest) || !bytes.Equal(authority.GetMembershipDigest(), baseline.MembershipDigest) ||
		len(authority.GetDevices()) != len(baseline.Devices) || len(authority.GetMembers()) != len(scope.floor.members) {
		return stageauthority.ErrRuntimeMismatch
	}
	for _, device := range baseline.Devices {
		found := false
		for _, signed := range authority.GetDevices() {
			found = found || signed.GetDeviceId() == device.ID && signed.GetDeviceEpoch() == device.Epoch
		}
		if !found {
			return stageauthority.ErrRuntimeMismatch
		}
	}
	for _, member := range authority.GetMembers() {
		trusted, found := scope.floor.members[member.GetWorkerMemberId()]
		if !found || member.GetMemberEpoch() != trusted.MemberEpoch || !bytes.Equal(member.GetIdentityDigest(), trusted.IdentityDigest) {
			return stageauthority.ErrRuntimeMismatch
		}
	}
	return nil
}

func (scope executionJournalScope) matchExecutionFloorScope(value *velav1.StageTerminalDisposition) error {
	baseline := scope.binding
	if value.GetWorkerInstanceId() != baseline.WorkerInstanceID || value.GetWorkerInstanceEpoch() != baseline.WorkerInstanceEpoch ||
		!bytes.Equal(value.GetDeviceSetDigest(), baseline.DeviceSetDigest) || !bytes.Equal(value.GetMembershipDigest(), baseline.MembershipDigest) ||
		len(value.GetDevices()) != len(baseline.Devices) {
		return stageauthority.ErrRuntimeMismatch
	}
	for _, device := range baseline.Devices {
		found := false
		for _, signed := range value.GetDevices() {
			found = found || signed.GetDeviceId() == device.ID && signed.GetDeviceEpoch() == device.Epoch
		}
		if !found {
			return stageauthority.ErrRuntimeMismatch
		}
	}
	for _, allocation := range value.GetAllocations() {
		if len(allocation.GetMembers()) != len(scope.floor.members) {
			return stageauthority.ErrRuntimeMismatch
		}
		localFound := false
		for _, member := range allocation.GetMembers() {
			trusted, found := scope.floor.members[member.GetWorkerMemberId()]
			if !found || member.GetMemberEpoch() != trusted.MemberEpoch ||
				!bytes.Equal(member.GetIdentityDigest(), trusted.IdentityDigest) || !bytes.Equal(member.GetDeviceSubsetDigest(), trusted.DeviceSubsetDigest) {
				return stageauthority.ErrRuntimeMismatch
			}
			localFound = localFound || member.GetWorkerMemberId() == baseline.WorkerMemberID
		}
		if !localFound {
			return stageauthority.ErrRuntimeMismatch
		}
	}
	return nil
}

func (supervisor *Supervisor) journalScope() executionJournalScope {
	return executionJournalScope{binding: cloneBinding(supervisor.services[0].binding), floor: supervisor.floor}
}

func (supervisor *Supervisor) matchRetainedExecutionScope(authority *velav1.StageAuthority) error {
	return supervisor.journalScope().matchRetainedExecutionScope(authority)
}

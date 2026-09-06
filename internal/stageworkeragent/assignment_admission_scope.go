package stageworkeragent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

// This scope owns member-wide history across resident Runtime replacements.
// Runtime routes are deliberately absent; current admission validates them.
type assignmentAdmissionScope struct {
	WorkerInstanceID    string
	WorkerInstanceEpoch int64
	WorkerMemberID      string
	DeviceSetDigest     []byte
	Devices             []stageauthority.DeviceEpoch
	MembershipDigest    []byte
	Members             []admissionScopeMember
}

type admissionScopeMember struct {
	ID                 string
	Epoch              int64
	IdentityDigest     [sha256.Size]byte
	DeviceSubsetDigest [sha256.Size]byte
}

func newAssignmentAdmissionScope(config AssignmentAdmissionConfig, bindings []AdmissionRuntimeBinding) (assignmentAdmissionScope, error) {
	invalid := errors.New("assignment admission topology is invalid or incomplete")
	var scope assignmentAdmissionScope
	members := make(map[string]admissionScopeMember)
	for _, binding := range bindings {
		runtime := binding.Runtime
		devices, epochs := slices.Clone(runtime.Devices), slices.Clone(runtime.Members)
		slices.SortFunc(devices, func(a, b stageauthority.DeviceEpoch) int { return strings.Compare(a.ID, b.ID) })
		slices.SortFunc(epochs, func(a, b stageauthority.MemberEpoch) int { return strings.Compare(a.ID, b.ID) })
		if len(devices) == 0 || len(devices) > 64 || len(epochs) == 0 || len(epochs) > 64 ||
			!admissionScopeDigest(runtime.DeviceSetDigest) || !admissionScopeDigest(runtime.MembershipDigest) {
			return scope, invalid
		}
		for i, device := range devices {
			if !admissionScopeUUID(device.ID) || device.Epoch <= 0 || i > 0 && devices[i-1].ID == device.ID {
				return scope, invalid
			}
		}
		localFound := false
		for i, member := range epochs {
			if !admissionScopeUUID(member.ID) || member.Epoch <= 0 || i > 0 && epochs[i-1].ID == member.ID {
				return scope, invalid
			}
			localFound = localFound || member.ID == runtime.WorkerMemberID && member.Epoch == runtime.WorkerMemberEpoch
		}
		if !localFound {
			return scope, invalid
		}
		if scope.WorkerInstanceID == "" {
			scope = assignmentAdmissionScope{
				WorkerInstanceID: config.WorkerInstanceID.String(), WorkerInstanceEpoch: config.WorkerInstanceEpoch,
				WorkerMemberID: config.WorkerMemberID.String(), DeviceSetDigest: bytes.Clone(runtime.DeviceSetDigest),
				Devices: devices, MembershipDigest: bytes.Clone(runtime.MembershipDigest),
			}
			for _, member := range epochs {
				scope.Members = append(scope.Members, admissionScopeMember{ID: member.ID, Epoch: member.Epoch})
			}
		}
		if !bytes.Equal(runtime.DeviceSetDigest, scope.DeviceSetDigest) || !bytes.Equal(runtime.MembershipDigest, scope.MembershipDigest) ||
			!slices.Equal(devices, scope.Devices) || len(epochs) != len(scope.Members) {
			return scope, invalid
		}
		for i, member := range epochs {
			if member.ID != scope.Members[i].ID || member.Epoch != scope.Members[i].Epoch {
				return scope, invalid
			}
		}
		member := admissionScopeMember{ID: runtime.WorkerMemberID, Epoch: runtime.WorkerMemberEpoch,
			IdentityDigest: binding.IdentityDigest, DeviceSubsetDigest: binding.DeviceSubsetDigest}
		if previous, found := members[member.ID]; found && previous != member {
			return scope, invalid
		}
		members[member.ID] = member
	}
	if _, found := members[scope.WorkerMemberID]; !found || len(members) != len(scope.Members) {
		return scope, invalid
	}
	for i, member := range scope.Members {
		trusted, found := members[member.ID]
		if !found || trusted.Epoch != member.Epoch {
			return scope, invalid
		}
		scope.Members[i] = trusted
	}
	return scope, nil
}

func (scope assignmentAdmissionScope) digest() ([sha256.Size]byte, error) {
	wire, err := json.Marshal(scope)
	return sha256.Sum256(wire), err
}

func (scope assignmentAdmissionScope) matchAuthority(authority *velav1.StageAuthority) error {
	if authority.GetWorkerInstanceId() != scope.WorkerInstanceID || authority.GetWorkerInstanceEpoch() != scope.WorkerInstanceEpoch ||
		!bytes.Equal(authority.GetDeviceSetDigest(), scope.DeviceSetDigest) || !bytes.Equal(authority.GetMembershipDigest(), scope.MembershipDigest) ||
		len(authority.GetDevices()) != len(scope.Devices) || len(authority.GetMembers()) != len(scope.Members) {
		return stageauthority.ErrRuntimeMismatch
	}
	for _, device := range scope.Devices {
		if !slices.ContainsFunc(authority.GetDevices(), func(signed *velav1.StageAuthorityDeviceEpoch) bool {
			return signed.GetDeviceId() == device.ID && signed.GetDeviceEpoch() == device.Epoch
		}) {
			return stageauthority.ErrRuntimeMismatch
		}
	}
	for _, member := range scope.Members {
		if !slices.ContainsFunc(authority.GetMembers(), func(signed *velav1.StageAuthorityMemberEpoch) bool {
			return signed.GetWorkerMemberId() == member.ID && signed.GetMemberEpoch() == member.Epoch && bytes.Equal(signed.GetIdentityDigest(), member.IdentityDigest[:])
		}) {
			return stageauthority.ErrRuntimeMismatch
		}
	}
	return nil
}

func admissionScopeUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}

func admissionScopeDigest(value []byte) bool {
	return len(value) == sha256.Size && !bytes.Equal(value, make([]byte, sha256.Size))
}

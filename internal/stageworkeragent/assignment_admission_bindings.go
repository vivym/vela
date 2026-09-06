package stageworkeragent

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"strings"
	"unicode/utf8"
)

// BindRuntimeRoutes completes deferred startup discovery without reopening the
// journal. Only execution routes may change; all recorded topology stays fixed.
func (gate *FileAssignmentAdmission) BindRuntimeRoutes(ctx context.Context, bindings []AdmissionRuntimeBinding) error {
	if gate == nil || ctx == nil {
		return errors.New("assignment route binding requires context and journal ownership")
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return err
	}
	if gate.routesBound || gate.active != nil {
		return errors.New("assignment Runtime routes are already bound")
	}
	cloned, err := cloneAdmissionRuntimeBindings(gate.scope.WorkerInstanceID, gate.scope.WorkerInstanceEpoch, bindings)
	if err != nil {
		return err
	}
	localRoute := false
	for _, binding := range cloned {
		runtime := binding.Runtime
		if runtime.ModelResidencyID == "" && runtime.ModelRuntimeIdentity == "" && runtime.StageProfileRevisionID == "" && runtime.ModelRuntimeEpoch == 0 {
			continue
		}
		if !admissionScopeUUID(runtime.ModelResidencyID) || !admissionScopeUUID(runtime.StageProfileRevisionID) || runtime.ModelRuntimeEpoch <= 0 ||
			runtime.ModelRuntimeIdentity == "" || len(runtime.ModelRuntimeIdentity) > 200 || !utf8.ValidString(runtime.ModelRuntimeIdentity) ||
			strings.TrimSpace(runtime.ModelRuntimeIdentity) != runtime.ModelRuntimeIdentity || strings.ContainsAny(runtime.ModelRuntimeIdentity, "\x00\r\n\t") {
			return errors.New("assignment Runtime discovery contains an incomplete execution route")
		}
		localRoute = localRoute || runtime.WorkerMemberID == gate.scope.WorkerMemberID
	}
	if !localRoute {
		return errors.New("assignment Runtime discovery requires an approved local execution route")
	}
	scope, err := newAssignmentAdmissionScope(AssignmentAdmissionConfig{
		WorkerInstanceID: gate.state.WorkerInstanceID, WorkerInstanceEpoch: gate.state.WorkerInstanceEpoch,
		WorkerMemberID: gate.state.WorkerMemberID,
	}, cloned)
	if err != nil {
		return err
	}
	digest, err := scope.digest()
	if err != nil || digest != gate.scopeDigest {
		return errors.New("assignment Runtime routes changed the recorded journal topology")
	}
	if err := gate.available(ctx); err != nil {
		return err
	}
	gate.bindings, gate.routesBound = cloned, true
	return nil
}

func cloneAdmissionRuntimeBindings(workerID string, workerEpoch int64, source []AdmissionRuntimeBinding) ([]AdmissionRuntimeBinding, error) {
	if len(source) == 0 || len(source) > 16*64 {
		return nil, errors.New("assignment admission Runtime binding count is invalid")
	}
	bindings := make([]AdmissionRuntimeBinding, 0, len(source))
	for _, binding := range source {
		if binding.Runtime.WorkerInstanceID != workerID || binding.Runtime.WorkerInstanceEpoch != workerEpoch ||
			binding.Runtime.WorkerMemberID == "" || binding.IdentityDigest == ([sha256.Size]byte{}) || binding.DeviceSubsetDigest == ([sha256.Size]byte{}) {
			return nil, errors.New("assignment admission Runtime binding is invalid")
		}
		binding.Runtime.Devices = slices.Clone(binding.Runtime.Devices)
		binding.Runtime.Members = slices.Clone(binding.Runtime.Members)
		binding.Runtime.DeviceSetDigest = slices.Clone(binding.Runtime.DeviceSetDigest)
		binding.Runtime.MembershipDigest = slices.Clone(binding.Runtime.MembershipDigest)
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

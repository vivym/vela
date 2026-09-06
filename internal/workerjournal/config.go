// Package workerjournal adapts the launch contract to offline Worker journal
// preparation without coupling the Worker admission core to Runtime backends.
package workerjournal

import (
	"crypto/sha256"
	"errors"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageworkeragent"
)

// AssignmentConfig carries topology only. It observes no Runtime epoch and
// supplies no execution route; live startup must independently discover those.
func AssignmentConfig(manifest modelruntime.LaunchManifest, config stageworkeragent.AssignmentAdmissionConfig) (stageworkeragent.AssignmentAdmissionConfig, error) {
	bindings, err := manifest.RuntimeBindings()
	if err != nil {
		return stageworkeragent.AssignmentAdmissionConfig{}, err
	}
	members, err := manifest.ExecutionFloorMembers()
	if err != nil {
		return stageworkeragent.AssignmentAdmissionConfig{}, err
	}
	config.WorkerInstanceID, err = uuid.Parse(manifest.WorkerInstanceID)
	if err != nil {
		return stageworkeragent.AssignmentAdmissionConfig{}, err
	}
	config.WorkerMemberID, err = uuid.Parse(manifest.WorkerMemberID)
	if err != nil {
		return stageworkeragent.AssignmentAdmissionConfig{}, err
	}
	config.WorkerInstanceEpoch = manifest.WorkerInstanceEpoch
	config.InputRoot, config.OutputRoot = manifest.Runtimes[0].InputRoot, manifest.Runtimes[0].OutputRoot
	for _, runtime := range manifest.Runtimes {
		if runtime.InputRoot != config.InputRoot || runtime.OutputRoot != config.OutputRoot {
			return stageworkeragent.AssignmentAdmissionConfig{}, errors.New("worker journal requires one shared local input/output root pair")
		}
	}
	baseline := bindings[0]
	baseline.ModelResidencyID, baseline.ModelRuntimeIdentity, baseline.StageProfileRevisionID = "", "", ""
	baseline.ModelRuntimeEpoch = 0
	config.Bindings = nil
	for _, member := range members {
		binding := baseline
		binding.WorkerMemberID, binding.WorkerMemberEpoch = member.WorkerMemberID, member.MemberEpoch
		config.Bindings = append(config.Bindings, stageworkeragent.AdmissionRuntimeBinding{
			Runtime: binding, IdentityDigest: [sha256.Size]byte(member.IdentityDigest),
			DeviceSubsetDigest: [sha256.Size]byte(member.DeviceSubsetDigest),
		})
	}
	return config, nil
}

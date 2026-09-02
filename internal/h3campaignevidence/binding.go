package h3campaignevidence

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/releasebundle"
)

func LoadEvidenceBinding(
	bundlePath string,
	residencyPlanRevisionID uuid.UUID,
	validationEnvironment string,
	collectorIdentity string,
) (EvidenceBinding, error) {
	if bundlePath == "" || residencyPlanRevisionID == uuid.Nil ||
		!validText(validationEnvironment, 500) || !validText(collectorIdentity, 500) {
		return EvidenceBinding{}, errors.New("H3 campaign release binding input is invalid")
	}
	bundle, rollouts, err := releasebundle.LoadResidencyPlanRollouts(bundlePath)
	if err != nil {
		return EvidenceBinding{}, fmt.Errorf("load H3 campaign release bundle: %w", err)
	}
	matches := 0
	for _, rollout := range rollouts {
		if rollout.ApprovedPlan.ID == residencyPlanRevisionID {
			matches++
		}
	}
	if matches != 1 {
		return EvidenceBinding{}, fmt.Errorf(
			"release bundle contains %d ResidencyPlan authorities for %s, want exactly one",
			matches,
			residencyPlanRevisionID,
		)
	}
	binding := EvidenceBinding{
		ReleaseDigest:           bundle.ReleaseDigest,
		ConfigurationRevision:   bundle.ConfigurationRevision,
		ValidationEnvironment:   validationEnvironment,
		CollectorIdentity:       collectorIdentity,
		ResidencyPlanRevisionID: residencyPlanRevisionID,
	}
	binding.seal = sealEvidenceBinding(binding)
	return binding, nil
}

func validEvidenceBinding(binding EvidenceBinding) bool {
	return binding.seal != [32]byte{} && binding.seal == sealEvidenceBinding(binding)
}

func sealEvidenceBinding(binding EvidenceBinding) [32]byte {
	return sha256.Sum256([]byte(
		binding.ReleaseDigest + "\x00" + binding.ConfigurationRevision + "\x00" +
			binding.ValidationEnvironment + "\x00" + binding.CollectorIdentity + "\x00" +
			binding.ResidencyPlanRevisionID.String(),
	))
}

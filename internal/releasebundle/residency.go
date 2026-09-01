package releasebundle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"

	"github.com/vivym/vela/internal/fleetcontroller"
)

// LoadResidencyPlanRollouts verifies a canonical release bundle and extracts
// the exact ResidencyPlan rollout authority from its digest-bound Fleet final
// render. No caller-supplied rollout document participates in this path.
func LoadResidencyPlanRollouts(
	bundlePath string,
) (Bundle, []fleetcontroller.ResidencyPlanRollout, error) {
	bundle, err := Load(bundlePath)
	if err != nil {
		return Bundle{}, nil, err
	}
	var artifact *Artifact
	for index := range bundle.ConfigurationManifest.FinalRenders {
		render := &bundle.ConfigurationManifest.FinalRenders[index]
		if render.Name == "fleet-controller" {
			if artifact != nil {
				return Bundle{}, nil, invalid("Fleet final render is duplicated")
			}
			artifact = &render.Artifact
		}
	}
	if artifact == nil || artifact.MediaType != "application/yaml" {
		return Bundle{}, nil, invalid("Fleet final render is missing or has the wrong media type")
	}
	root, err := openRootedFS(filepath.Dir(bundlePath))
	if err != nil {
		return Bundle{}, nil, invalidf("open release bundle root: %v", err)
	}
	defer func() { _ = root.Close() }()
	encoded, err := readRooted(root, artifact.Ref, maxYAMLArtifactBytes)
	if err != nil {
		return Bundle{}, nil, invalidf("read Fleet final render: %v", err)
	}
	digest := sha256.Sum256(encoded)
	actualDigest := "sha256:" + hex.EncodeToString(digest[:])
	if actualDigest != artifact.Digest || int64(len(encoded)) != artifact.SizeBytes {
		return Bundle{}, nil, invalid("Fleet final render bytes changed after bundle verification")
	}
	inventory := newRenderInventory()
	if err := validateFinalRender("fleet-controller", encoded, &inventory, &yamlGraphBudget{}); err != nil {
		return Bundle{}, nil, fmt.Errorf("%w: reload Fleet final render: %v", ErrInvalidBundle, err)
	}
	if len(inventory.residencyRollouts) == 0 {
		return Bundle{}, nil, invalid("release bundle does not contain target ResidencyPlan rollout authority")
	}
	return bundle, inventory.residencyRollouts, nil
}

package fleetcontroller

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"

	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/strictjson"
)

// ParseWorkerBundleActuationManifest accepts only the canonical digest preimage
// used by Registry bootstrap. It restores RevisionDigest for normal validation.
func ParseWorkerBundleActuationManifest(encoded []byte) (WorkerBundleActuation, error) {
	if len(encoded) == 0 || len(encoded) > fleet.MaximumWorkerBootstrapManifestBytes {
		return WorkerBundleActuation{}, errors.New("worker bootstrap manifest size is invalid")
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return WorkerBundleActuation{}, err
	}
	var document struct {
		Schema string                `json:"schema"`
		Bundle WorkerBundleActuation `json:"bundle"`
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return WorkerBundleActuation{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || document.Schema != "vela.worker-bundle-actuation/v2" || document.Bundle.RevisionDigest != "" {
		return WorkerBundleActuation{}, errors.New("worker bootstrap manifest format is invalid")
	}
	digest := sha256.Sum256(encoded)
	document.Bundle.RevisionDigest = hex.EncodeToString(digest[:])
	canonical, err := WorkerBundleActuationManifest(document.Bundle)
	if err != nil {
		return WorkerBundleActuation{}, err
	}
	if !bytes.Equal(canonical, encoded) {
		return WorkerBundleActuation{}, errors.New("worker bootstrap manifest is not canonical")
	}
	return document.Bundle, nil
}

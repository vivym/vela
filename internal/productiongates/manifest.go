package productiongates

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/vivym/vela/internal/sloevidence"
	"github.com/vivym/vela/internal/strictjson"
)

const maxManifestBytes = 1024 * 1024

var ErrInvalidManifest = errors.New("invalid Production Gate manifest")

// Manifest is the portable index of all Launch Receipts for one release.
// Derived fields are populated only by LoadManifest after evidence verification.
type Manifest struct {
	SchemaVersion int       `json:"schema_version"`
	Receipts      []Receipt `json:"receipts"`

	ReleaseDigest         string                 `json:"-"`
	ConfigurationRevision string                 `json:"-"`
	Digest                string                 `json:"-"`
	Evaluation            Evaluation             `json:"-"`
	TypedEvidence         map[Gate]TypedEvidence `json:"-"`
}

func (manifest Manifest) ValidateBinding(releaseDigest, configurationRevision string) error {
	if manifest.ReleaseDigest != releaseDigest || manifest.ConfigurationRevision != configurationRevision {
		return fmt.Errorf(
			"launch receipts bind release=%s configuration=%s, want release=%s configuration=%s",
			manifest.ReleaseDigest,
			manifest.ConfigurationRevision,
			releaseDigest,
			configurationRevision,
		)
	}
	return nil
}

func LoadManifest(path string) (Manifest, error) {
	return LoadManifestWithin(filepath.Dir(path), filepath.Base(path))
}

// LoadManifestWithin resolves a manifest and all evidence beneath one directory root.
func LoadManifestWithin(directory, reference string) (Manifest, error) {
	reference = filepath.FromSlash(reference)
	if !filepath.IsLocal(reference) {
		return Manifest{}, fmt.Errorf("%w: manifest reference must be a local relative path", ErrInvalidManifest)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return Manifest{}, fmt.Errorf("%w: open manifest root: %v", ErrInvalidManifest, err)
	}
	defer func() { _ = root.Close() }()
	manifestRoot := root
	manifestDirectory := filepath.Dir(reference)
	if manifestDirectory != "." {
		manifestRoot, err = root.OpenRoot(manifestDirectory)
		if err != nil {
			return Manifest{}, fmt.Errorf("%w: open manifest directory: %v", ErrInvalidManifest, err)
		}
		defer func() { _ = manifestRoot.Close() }()
	}
	file, err := manifestRoot.Open(filepath.Base(reference))
	if err != nil {
		return Manifest{}, fmt.Errorf("%w: open manifest: %v", ErrInvalidManifest, err)
	}
	defer func() { _ = file.Close() }()

	limited := io.LimitReader(file, maxManifestBytes+1)
	encoded, err := io.ReadAll(limited)
	if err != nil {
		return Manifest{}, fmt.Errorf("%w: read manifest: %v", ErrInvalidManifest, err)
	}
	if len(encoded) == 0 || len(encoded) > maxManifestBytes {
		return Manifest{}, fmt.Errorf("%w: manifest size must be in 1..%d bytes", ErrInvalidManifest, maxManifestBytes)
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return Manifest{}, fmt.Errorf("%w: decode manifest: %v", ErrInvalidManifest, err)
	}

	var manifest Manifest
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("%w: decode manifest: %v", ErrInvalidManifest, err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return Manifest{}, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	if manifest.SchemaVersion != 1 {
		return Manifest{}, fmt.Errorf("%w: schema_version must be 1", ErrInvalidManifest)
	}

	evaluation := Evaluate(manifest.Receipts)
	if err := evaluation.AllPass(); err != nil {
		return Manifest{}, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	manifest.Evaluation = evaluation
	manifest.ReleaseDigest = manifest.Receipts[0].ReleaseDigest
	manifest.ConfigurationRevision = manifest.Receipts[0].ConfigurationRevision
	manifest.TypedEvidence = make(map[Gate]TypedEvidence, len(AllGates())-1)
	artifactBytes := 0
	for _, receipt := range manifest.Receipts {
		if receipt.ReleaseDigest != manifest.ReleaseDigest ||
			receipt.ConfigurationRevision != manifest.ConfigurationRevision {
			return Manifest{}, fmt.Errorf(
				"%w: all receipts must bind one release digest and configuration revision",
				ErrInvalidManifest,
			)
		}
		typedEvidence, verifiedArtifactBytes, err := verifyEvidence(
			manifestRoot,
			receipt,
			MaxEvidenceArtifactTotalBytes-artifactBytes,
		)
		if err != nil {
			return Manifest{}, err
		}
		artifactBytes += verifiedArtifactBytes
		if artifactBytes > MaxEvidenceArtifactTotalBytes {
			return Manifest{}, fmt.Errorf(
				"%w: referenced evidence artifacts exceed aggregate size limit",
				ErrInvalidManifest,
			)
		}
		if typedEvidence != nil {
			manifest.TypedEvidence[receipt.Gate] = *typedEvidence
		}
	}

	digest := sha256.Sum256(encoded)
	manifest.Digest = "sha256:" + hex.EncodeToString(digest[:])
	return manifest, nil
}

func verifyEvidence(root *os.Root, receipt Receipt, artifactBudget int) (*TypedEvidence, int, error) {
	evidenceRef := filepath.FromSlash(receipt.EvidenceRef)
	if !filepath.IsLocal(evidenceRef) {
		return nil, 0, fmt.Errorf("%w: evidence_ref for %s must be a local relative path", ErrInvalidManifest, receipt.Gate)
	}
	file, err := root.Open(evidenceRef)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: open evidence for %s: %v", ErrInvalidManifest, receipt.Gate, err)
	}
	defer func() { _ = file.Close() }()
	information, err := file.Stat()
	if err != nil || !information.Mode().IsRegular() {
		return nil, 0, fmt.Errorf("%w: evidence for %s must be a regular file", ErrInvalidManifest, receipt.Gate)
	}
	maximumEvidenceBytes := MaxTypedEvidenceBytes
	if receipt.Gate == GateObservabilityOnCall {
		maximumEvidenceBytes = sloevidence.MaxEvidenceBytes
	}
	encoded, err := io.ReadAll(io.LimitReader(file, int64(maximumEvidenceBytes)+1))
	if err != nil || len(encoded) == 0 || len(encoded) > maximumEvidenceBytes {
		return nil, 0, fmt.Errorf("%w: read bounded evidence for %s: %v", ErrInvalidManifest, receipt.Gate, err)
	}
	actualDigest := sloevidence.Digest(encoded)
	if actualDigest != receipt.EvidenceDigest {
		return nil, 0, fmt.Errorf("%w: evidence digest mismatch for %s", ErrInvalidManifest, receipt.Gate)
	}
	if receipt.Gate == GateObservabilityOnCall {
		evidence, decodeErr := sloevidence.Decode(
			encoded,
			receipt.ReleaseDigest,
			receipt.ConfigurationRevision,
		)
		if decodeErr != nil || evidence.ValidationEnvironment != receipt.ValidationEnvironment ||
			evidence.Owner != receipt.Owner {
			return nil, 0, fmt.Errorf("%w: observability evidence semantics are invalid: %v", ErrInvalidManifest, decodeErr)
		}
		artifactBytes := 0
		for _, artifact := range evidence.Artifacts {
			encodedArtifact, artifactErr := readAndVerifyReferencedArtifact(
				root,
				string(receipt.Gate)+"/"+string(artifact.Kind),
				artifact.Ref,
				artifact.Digest,
			)
			if artifactErr != nil {
				return nil, 0, artifactErr
			}
			artifactBytes += len(encodedArtifact)
			if artifactBytes > artifactBudget {
				return nil, 0, fmt.Errorf(
					"%w: referenced evidence artifacts exceed aggregate size limit",
					ErrInvalidManifest,
				)
			}
			if err := evidence.ValidateArtifact(artifact.Kind, encodedArtifact); err != nil {
				return nil, 0, fmt.Errorf("%w: observability artifact semantics are invalid: %v", ErrInvalidManifest, err)
			}
		}
		return nil, artifactBytes, nil
	}
	typedEvidence, decodeErr := DecodeTypedEvidence(encoded, receipt)
	if decodeErr != nil {
		return nil, 0, fmt.Errorf("%w: %s evidence semantics are invalid: %v", ErrInvalidManifest, receipt.Gate, decodeErr)
	}
	artifactBytes := 0
	verifiedArtifacts := make(map[string]TypedEvidenceArtifact, len(typedEvidence.Artifacts))
	for _, artifact := range typedEvidence.Artifacts {
		encodedArtifact, artifactErr := readAndVerifyReferencedArtifact(
			root,
			string(receipt.Gate)+"/"+artifact.Kind,
			artifact.Ref,
			artifact.Digest,
		)
		if artifactErr != nil {
			return nil, 0, artifactErr
		}
		artifactBytes += len(encodedArtifact)
		if artifactBytes > artifactBudget {
			return nil, 0, fmt.Errorf(
				"%w: referenced evidence artifacts exceed aggregate size limit",
				ErrInvalidManifest,
			)
		}
		decodedArtifact, decodeArtifactErr := DecodeTypedEvidenceArtifact(encodedArtifact, typedEvidence, artifact)
		if decodeArtifactErr != nil {
			return nil, 0, fmt.Errorf(
				"%w: %s artifact semantics are invalid: %v",
				ErrInvalidManifest,
				receipt.Gate,
				decodeArtifactErr,
			)
		}
		verifiedArtifacts[artifact.Kind] = decodedArtifact
	}
	if err := ValidateTypedEvidenceArtifacts(typedEvidence, verifiedArtifacts); err != nil {
		return nil, 0, fmt.Errorf("%w: %s artifact set is invalid: %v", ErrInvalidManifest, receipt.Gate, err)
	}
	return &typedEvidence, artifactBytes, nil
}

func readAndVerifyReferencedArtifact(
	root *os.Root,
	context,
	reference,
	expectedDigest string,
) ([]byte, error) {
	reference = filepath.FromSlash(reference)
	if !filepath.IsLocal(reference) {
		return nil, fmt.Errorf("%w: %s artifact reference must be local", ErrInvalidManifest, context)
	}
	file, err := root.Open(reference)
	if err != nil {
		return nil, fmt.Errorf("%w: open %s artifact %q: %v", ErrInvalidManifest, context, reference, err)
	}
	defer func() { _ = file.Close() }()
	information, err := file.Stat()
	if err != nil || !information.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %s artifact %q must be a regular file", ErrInvalidManifest, context, reference)
	}
	encoded, err := io.ReadAll(io.LimitReader(file, MaxEvidenceArtifactBytes+1))
	if err != nil || len(encoded) == 0 || len(encoded) > MaxEvidenceArtifactBytes {
		return nil, fmt.Errorf("%w: read bounded %s artifact %q: %v", ErrInvalidManifest, context, reference, err)
	}
	actualDigest := sloevidence.Digest(encoded)
	if actualDigest != expectedDigest {
		return nil, fmt.Errorf("%w: %s artifact digest mismatch for %q", ErrInvalidManifest, context, reference)
	}
	return encoded, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("manifest contains more than one JSON value")
		}
		return fmt.Errorf("decode trailing manifest data: %w", err)
	}
	return nil
}

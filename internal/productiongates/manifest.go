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

	"github.com/vivym/vela/internal/strictjson"
)

const maxManifestBytes = 1024 * 1024

var ErrInvalidManifest = errors.New("invalid Production Gate manifest")

// Manifest is the portable index of all Launch Receipts for one release.
// Derived fields are populated only by LoadManifest after evidence verification.
type Manifest struct {
	SchemaVersion int       `json:"schema_version"`
	Receipts      []Receipt `json:"receipts"`

	ReleaseDigest         string     `json:"-"`
	ConfigurationRevision string     `json:"-"`
	Digest                string     `json:"-"`
	Evaluation            Evaluation `json:"-"`
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
	for _, receipt := range manifest.Receipts {
		if receipt.ReleaseDigest != manifest.ReleaseDigest ||
			receipt.ConfigurationRevision != manifest.ConfigurationRevision {
			return Manifest{}, fmt.Errorf(
				"%w: all receipts must bind one release digest and configuration revision",
				ErrInvalidManifest,
			)
		}
		if err := verifyEvidence(manifestRoot, receipt); err != nil {
			return Manifest{}, err
		}
	}

	digest := sha256.Sum256(encoded)
	manifest.Digest = "sha256:" + hex.EncodeToString(digest[:])
	return manifest, nil
}

func verifyEvidence(root *os.Root, receipt Receipt) error {
	evidenceRef := filepath.FromSlash(receipt.EvidenceRef)
	if !filepath.IsLocal(evidenceRef) {
		return fmt.Errorf("%w: evidence_ref for %s must be a local relative path", ErrInvalidManifest, receipt.Gate)
	}
	file, err := root.Open(evidenceRef)
	if err != nil {
		return fmt.Errorf("%w: open evidence for %s: %v", ErrInvalidManifest, receipt.Gate, err)
	}
	defer func() { _ = file.Close() }()
	information, err := file.Stat()
	if err != nil || !information.Mode().IsRegular() {
		return fmt.Errorf("%w: evidence for %s must be a regular file", ErrInvalidManifest, receipt.Gate)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return fmt.Errorf("%w: hash evidence for %s: %v", ErrInvalidManifest, receipt.Gate, err)
	}
	actualDigest := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	if actualDigest != receipt.EvidenceDigest {
		return fmt.Errorf("%w: evidence digest mismatch for %s", ErrInvalidManifest, receipt.Gate)
	}
	return nil
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

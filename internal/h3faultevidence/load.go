package h3faultevidence

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

var ErrInvalidCampaign = errors.New("invalid H3 fault campaign evidence")

func Load(path string) (Campaign, error) {
	if path == "" {
		return Campaign{}, fmt.Errorf("%w: manifest path is required", ErrInvalidCampaign)
	}
	return LoadWithin(filepath.Dir(path), filepath.Base(path))
}

func LoadWithin(directory, reference string) (Campaign, error) {
	reference = filepath.FromSlash(reference)
	if !filepath.IsLocal(reference) {
		return Campaign{}, fmt.Errorf("%w: manifest reference must be local", ErrInvalidCampaign)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return Campaign{}, fmt.Errorf("%w: open campaign root: %v", ErrInvalidCampaign, err)
	}
	defer func() { _ = root.Close() }()
	manifestRoot := root
	manifestDirectory := filepath.Dir(reference)
	if manifestDirectory != "." {
		manifestRoot, err = root.OpenRoot(manifestDirectory)
		if err != nil {
			return Campaign{}, fmt.Errorf("%w: open manifest directory: %v", ErrInvalidCampaign, err)
		}
		defer func() { _ = manifestRoot.Close() }()
	}
	encoded, err := readRegularFile(manifestRoot, filepath.Base(reference), MaxManifestBytes)
	if err != nil {
		return Campaign{}, fmt.Errorf("%w: read manifest: %v", ErrInvalidCampaign, err)
	}
	var manifest Manifest
	if err := decodeStrict(encoded, &manifest); err != nil {
		return Campaign{}, fmt.Errorf("%w: decode manifest: %v", ErrInvalidCampaign, err)
	}
	if err := validateManifest(manifest); err != nil {
		return Campaign{}, err
	}

	receipts := make(map[Scenario]ScenarioReceipt, len(manifest.Scenarios))
	for _, scenario := range manifest.Scenarios {
		ref := filepath.FromSlash(scenario.Ref)
		if !filepath.IsLocal(ref) {
			return Campaign{}, invalid("scenario %s reference must be local", scenario.Scenario)
		}
		receiptBytes, err := readRegularFile(manifestRoot, ref, MaxReceiptBytes)
		if err != nil {
			return Campaign{}, invalid("read scenario %s receipt: %v", scenario.Scenario, err)
		}
		actual := sha256.Sum256(receiptBytes)
		if "sha256:"+hex.EncodeToString(actual[:]) != scenario.Digest {
			return Campaign{}, invalid("scenario %s receipt digest mismatch", scenario.Scenario)
		}
		var receipt ScenarioReceipt
		if err := decodeStrict(receiptBytes, &receipt); err != nil {
			return Campaign{}, invalid("decode scenario %s receipt: %v", scenario.Scenario, err)
		}
		if err := validateScenarioReceipt(manifest, scenario.Scenario, receipt); err != nil {
			return Campaign{}, err
		}
		receipts[scenario.Scenario] = receipt
	}
	manifestDigest := sha256.Sum256(encoded)
	return Campaign{
		Manifest: manifest, ManifestDigest: "sha256:" + hex.EncodeToString(manifestDigest[:]),
		Receipts: receipts,
	}, nil
}

func readRegularFile(root *os.Root, reference string, maximum int64) ([]byte, error) {
	file, err := root.Open(reference)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	information, err := file.Stat()
	if err != nil || !information.Mode().IsRegular() {
		return nil, errors.New("reference must resolve to a regular file")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 || int64(len(encoded)) > maximum {
		return nil, fmt.Errorf("file size must be in 1..%d bytes", maximum)
	}
	return encoded, nil
}

func decodeStrict(encoded []byte, target any) error {
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON data")
	}
	return nil
}

func invalid(format string, values ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidCampaign, fmt.Sprintf(format, values...))
}

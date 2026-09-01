package h3campaignrunner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/vivym/vela/internal/strictjson"
)

const maximumManifestBytes = 1 << 20

func LoadManifest(path string) (Manifest, error) {
	if path == "" {
		return Manifest{}, errors.New("h3 campaign manifest path is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("open H3 campaign manifest: %w", err)
	}
	defer func() { _ = file.Close() }()
	information, err := file.Stat()
	if err != nil || !information.Mode().IsRegular() {
		return Manifest{}, errors.New("h3 campaign manifest must be a regular file")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maximumManifestBytes+1))
	if err != nil {
		return Manifest{}, fmt.Errorf("read H3 campaign manifest: %w", err)
	}
	if len(encoded) == 0 || len(encoded) > maximumManifestBytes {
		return Manifest{}, fmt.Errorf(
			"h3 campaign manifest size must be in 1..%d bytes", maximumManifestBytes,
		)
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return Manifest{}, fmt.Errorf("decode H3 campaign manifest: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode H3 campaign manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Manifest{}, errors.New("decode H3 campaign manifest: trailing JSON data")
	}
	if _, _, err := ValidateManifest(manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

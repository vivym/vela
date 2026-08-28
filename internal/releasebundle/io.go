package releasebundle

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/vivym/vela/internal/strictjson"
)

func Build(path string) (Bundle, []byte, error) {
	directory, reference := filepath.Dir(path), filepath.Base(path)
	root, err := os.OpenRoot(directory)
	if err != nil {
		return Bundle{}, nil, fmt.Errorf("%w: open plan root: %v", ErrInvalidBundle, err)
	}
	defer func() { _ = root.Close() }()
	encoded, err := readRooted(root, reference, maxPlanBytes)
	if err != nil {
		return Bundle{}, nil, fmt.Errorf("%w: read build plan: %v", ErrInvalidBundle, err)
	}
	var plan BuildPlan
	if err := decodeStrictJSON(encoded, &plan); err != nil {
		return Bundle{}, nil, fmt.Errorf("%w: decode build plan: %v", ErrInvalidBundle, err)
	}
	bundle, err := build(root, plan)
	if err != nil {
		return Bundle{}, nil, err
	}
	output, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return Bundle{}, nil, fmt.Errorf("%w: encode bundle: %v", ErrInvalidBundle, err)
	}
	output = append(output, '\n')
	return bundle, output, nil
}

func Load(path string) (Bundle, error) {
	return LoadWithin(filepath.Dir(path), filepath.Base(path))
}

func LoadWithin(directory, reference string) (Bundle, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return Bundle{}, fmt.Errorf("%w: open bundle root: %v", ErrInvalidBundle, err)
	}
	defer func() { _ = root.Close() }()
	encoded, err := readRooted(root, reference, maxBundleBytes)
	if err != nil {
		return Bundle{}, fmt.Errorf("%w: read bundle: %v", ErrInvalidBundle, err)
	}
	var bundle Bundle
	if err := decodeStrictJSON(encoded, &bundle); err != nil {
		return Bundle{}, fmt.Errorf("%w: decode bundle: %v", ErrInvalidBundle, err)
	}
	if err := verify(root, bundle); err != nil {
		return Bundle{}, err
	}
	return bundle, nil
}

func decodeStrictJSON(encoded []byte, destination any) error {
	if len(encoded) == 0 {
		return errors.New("JSON document is empty")
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("JSON document contains trailing data")
	}
	return nil
}

func readRooted(root *os.Root, reference string, maximum int64) ([]byte, error) {
	normalized, err := normalizeArtifactReference(reference)
	if err != nil {
		return nil, err
	}
	return readNormalizedRooted(root, normalized, maximum)
}

func normalizeArtifactReference(reference string) (string, error) {
	if reference == "" || strings.Contains(reference, `\`) || path.Clean(reference) != reference {
		return "", errors.New("artifact reference must be a canonical slash-separated path")
	}
	normalized := filepath.FromSlash(reference)
	if !filepath.IsLocal(normalized) || normalized == "." {
		return "", errors.New("artifact reference must be a local relative file path")
	}
	return normalized, nil
}

func readNormalizedRooted(root *os.Root, normalized string, maximum int64) ([]byte, error) {
	prefix := ""
	for _, component := range strings.Split(normalized, string(filepath.Separator)) {
		if prefix == "" {
			prefix = component
		} else {
			prefix = filepath.Join(prefix, component)
		}
		information, err := root.Lstat(prefix)
		if err != nil {
			return nil, err
		}
		if information.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("artifact path must not contain a symbolic link")
		}
	}
	file, err := root.Open(normalized)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	information, err := file.Stat()
	if err != nil || !information.Mode().IsRegular() || information.Size() <= 0 || information.Size() > maximum {
		return nil, fmt.Errorf("artifact must be a regular file of 1..%d bytes", maximum)
	}
	content, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || len(content) == 0 || int64(len(content)) > maximum {
		return nil, fmt.Errorf("read bounded artifact: %w", err)
	}
	return content, nil
}

func artifactFor(root *os.Root, reference, mediaType string, maximum int64) (Artifact, []byte, error) {
	normalized, err := normalizeArtifactReference(reference)
	if err != nil {
		return Artifact{}, nil, err
	}
	content, err := readNormalizedRooted(root, normalized, maximum)
	if err != nil {
		return Artifact{}, nil, err
	}
	digest := sha256.Sum256(content)
	return Artifact{
		Ref: filepath.ToSlash(normalized), MediaType: mediaType,
		Digest: "sha256:" + hex.EncodeToString(digest[:]), SizeBytes: int64(len(content)),
	}, content, nil
}

func canonicalDigest(value any) (string, int64, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", 0, err
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), int64(len(encoded)), nil
}

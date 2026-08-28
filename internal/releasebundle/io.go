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

type artifactReference struct {
	role      string
	reference string
	maximum   int64
}

type artifactReader struct {
	root       *os.Root
	normalized map[string]string
	remaining  int64
}

func preflightArtifactGraph(root *os.Root, plan BuildPlan) (*artifactReader, error) {
	references := collectArtifactReferences(plan)
	if len(references) > maxArtifactCount {
		return nil, fmt.Errorf("artifact graph exceeds %d entries", maxArtifactCount)
	}
	reader := &artifactReader{
		root:       root,
		normalized: make(map[string]string, len(references)),
		remaining:  maxArtifactBytes,
	}
	roles := make(map[string]string, len(references))
	for _, artifact := range references {
		normalized, err := normalizeArtifactReference(artifact.reference)
		if err != nil {
			return nil, fmt.Errorf("%s reference: %w", artifact.role, err)
		}
		if prior, duplicate := roles[normalized]; duplicate {
			return nil, fmt.Errorf(
				"artifact reference %q is shared by %s and %s",
				artifact.reference,
				prior,
				artifact.role,
			)
		}
		roles[normalized] = artifact.role
		reader.normalized[artifact.reference] = normalized
	}
	totalBytes := int64(0)
	for _, artifact := range references {
		file, size, err := openNormalizedRooted(root, reader.normalized[artifact.reference], artifact.maximum)
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", artifact.role, err)
		}
		_ = file.Close()
		if size > maxArtifactBytes-totalBytes {
			return nil, fmt.Errorf("artifact graph exceeds %d bytes", maxArtifactBytes)
		}
		totalBytes += size
	}
	return reader, nil
}

func collectArtifactReferences(plan BuildPlan) []artifactReference {
	references := make([]artifactReference, 0,
		len(plan.FinalRenders)+1+2*len(plan.Packages)+
			3*len(plan.WorkerMaterializations)+2*len(plan.OCIManifests),
	)
	for _, render := range plan.FinalRenders {
		references = append(references, artifactReference{
			role: "render/" + render.Name, reference: render.Ref, maximum: maxMetadataBytes,
		})
	}
	references = append(references, artifactReference{
		role: "node-agent-unit", reference: plan.NodeAgentUnit.Ref, maximum: maxMetadataBytes,
	})
	for _, item := range plan.Packages {
		references = append(references,
			artifactReference{
				role: "package-contract/" + item.Name, reference: item.ContractRef, maximum: maxMetadataBytes,
			},
			artifactReference{
				role: "package/" + item.Name, reference: item.ArtifactRef, maximum: maxPackageBytes,
			},
		)
	}
	for _, item := range plan.WorkerMaterializations {
		references = append(references,
			artifactReference{
				role: "worker-runtime/" + item.NodeIdentity, reference: item.WorkerRuntimeRef, maximum: maxMetadataBytes,
			},
			artifactReference{
				role: "runner-profiles/" + item.NodeIdentity, reference: item.RunnerProfilesRef, maximum: maxMetadataBytes,
			},
			artifactReference{
				role: "runner-gpu-roles/" + item.NodeIdentity, reference: item.RunnerGPURolesRef, maximum: maxMetadataBytes,
			},
		)
	}
	for _, image := range plan.OCIManifests {
		references = append(references,
			artifactReference{
				role: "oci-manifest/" + image.Image, reference: image.Ref, maximum: maxMetadataBytes,
			},
			artifactReference{
				role: "oci-config/" + image.Image, reference: image.ConfigRef, maximum: maxMetadataBytes,
			},
		)
	}
	return references
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
	file, _, err := openNormalizedRooted(root, normalized, maximum)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	content, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, fmt.Errorf("read bounded artifact: %w", err)
	}
	if len(content) == 0 || int64(len(content)) > maximum {
		return nil, fmt.Errorf("artifact content must be in 1..%d bytes", maximum)
	}
	return content, nil
}

func openNormalizedRooted(root *os.Root, normalized string, maximum int64) (*os.File, int64, error) {
	prefix := ""
	for _, component := range strings.Split(normalized, string(filepath.Separator)) {
		if prefix == "" {
			prefix = component
		} else {
			prefix = filepath.Join(prefix, component)
		}
		information, err := root.Lstat(prefix)
		if err != nil {
			return nil, 0, err
		}
		if information.Mode()&os.ModeSymlink != 0 {
			return nil, 0, errors.New("artifact path must not contain a symbolic link")
		}
	}
	file, err := root.Open(normalized)
	if err != nil {
		return nil, 0, err
	}
	information, err := file.Stat()
	if err != nil || !information.Mode().IsRegular() || information.Size() <= 0 || information.Size() > maximum {
		_ = file.Close()
		return nil, 0, fmt.Errorf("artifact must be a regular file of 1..%d bytes", maximum)
	}
	return file, information.Size(), nil
}

func (reader *artifactReader) artifactFor(
	reference,
	mediaType string,
	maximum int64,
) (Artifact, []byte, error) {
	normalized, file, limit, err := reader.openPreflightedArtifact(reference, maximum)
	if err != nil {
		return Artifact{}, nil, err
	}
	defer func() { _ = file.Close() }()
	content, err := io.ReadAll(io.LimitReader(file, limit))
	if err != nil {
		return Artifact{}, nil, fmt.Errorf("read bounded artifact: %w", err)
	}
	actual := int64(len(content))
	if err := reader.consume(actual, maximum); err != nil {
		return Artifact{}, nil, err
	}
	digest := sha256.Sum256(content)
	return artifactDescriptor(normalized, mediaType, digest[:], actual), content, nil
}

func (reader *artifactReader) digestArtifact(
	reference,
	mediaType string,
	maximum int64,
) (Artifact, error) {
	normalized, file, limit, err := reader.openPreflightedArtifact(reference, maximum)
	if err != nil {
		return Artifact{}, err
	}
	defer func() { _ = file.Close() }()
	digest := sha256.New()
	actual, err := io.Copy(digest, io.LimitReader(file, limit))
	if err != nil {
		return Artifact{}, fmt.Errorf("hash bounded artifact: %w", err)
	}
	if err := reader.consume(actual, maximum); err != nil {
		return Artifact{}, err
	}
	return artifactDescriptor(normalized, mediaType, digest.Sum(nil), actual), nil
}

func (reader *artifactReader) openPreflightedArtifact(
	reference string,
	maximum int64,
) (string, *os.File, int64, error) {
	normalized, ok := reader.normalized[reference]
	if !ok {
		return "", nil, 0, errors.New("artifact reference was not preflighted")
	}
	file, _, err := openNormalizedRooted(reader.root, normalized, maximum)
	if err != nil {
		return "", nil, 0, err
	}
	return normalized, file, min(maximum, reader.remaining) + 1, nil
}

func (reader *artifactReader) consume(actual, maximum int64) error {
	if actual <= 0 || actual > maximum {
		return fmt.Errorf("artifact content must be in 1..%d bytes", maximum)
	}
	if actual > reader.remaining {
		return fmt.Errorf("artifact graph exceeds %d actual bytes", maxArtifactBytes)
	}
	reader.remaining -= actual
	return nil
}

func artifactDescriptor(normalized, mediaType string, digest []byte, size int64) Artifact {
	return Artifact{
		Ref: filepath.ToSlash(normalized), MediaType: mediaType,
		Digest: "sha256:" + hex.EncodeToString(digest), SizeBytes: size,
	}
}

func canonicalDigest(value any) (string, int64, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", 0, err
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), int64(len(encoded)), nil
}

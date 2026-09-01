package releaseartifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/distribution/reference"
	ocidigest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ociv1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/vivym/vela/internal/releasebundle"
	"golang.org/x/sys/unix"
)

const (
	velaImageArtifactSchemaVersion = 1
	velaImageCount                 = 4
	maximumOCIImageLayoutBytes     = int64(8 << 30)
)

type VelaImageArtifactBuildRequest struct {
	VelaImageBuildRequest
	OutputDirectory string
}

type velaImageArtifactManifest struct {
	SchemaVersion int                              `json:"schema_version"`
	Revision      string                           `json:"revision"`
	OCIManifests  []releasebundle.OCIManifestInput `json:"oci_manifests"`
}

type velaImageSpecification struct {
	name       string
	entrypoint string
}

func BuildVelaImageArtifacts(ctx context.Context, request VelaImageArtifactBuildRequest) error {
	return buildVelaImageArtifacts(ctx, request, nil)
}

func PublishVelaImageArtifacts(ctx context.Context, request VelaImageArtifactBuildRequest) error {
	return buildVelaImageArtifacts(ctx, request, newRemoteVelaImageRegistry())
}

func buildVelaImageArtifacts(
	ctx context.Context,
	request VelaImageArtifactBuildRequest,
	registry velaImageRegistryClient,
) error {
	validated, err := validateVelaImageBuildInputs(ctx, request.VelaImageBuildRequest)
	if err != nil {
		return err
	}
	outputDirectory, parent, err := canonicalNewOutputDirectory(request.OutputDirectory)
	if err != nil {
		return fmt.Errorf("resolve Vela image artifact output: %w", err)
	}
	if err := validateVelaImageRepositories(validated.ImagePrefix, validated.Revision); err != nil {
		return err
	}

	candidate, err := os.MkdirTemp(parent, ".vela-image-artifacts-*")
	if err != nil {
		return fmt.Errorf("create Vela image artifact candidate: %w", err)
	}
	defer func() { _ = os.RemoveAll(candidate) }()
	if err := os.Chmod(candidate, 0o700); err != nil {
		return fmt.Errorf("protect Vela image artifact candidate: %w", err)
	}
	layoutRoot, err := os.MkdirTemp(parent, ".vela-oci-layouts-*")
	if err != nil {
		return fmt.Errorf("create OCI layout workspace: %w", err)
	}
	defer func() { _ = os.RemoveAll(layoutRoot) }()
	if err := os.Chmod(layoutRoot, 0o700); err != nil {
		return fmt.Errorf("protect OCI layout workspace: %w", err)
	}

	if err := runVelaImageArtifactBake(ctx, validated, layoutRoot); err != nil {
		return err
	}
	manifest := velaImageArtifactManifest{
		SchemaVersion: velaImageArtifactSchemaVersion,
		Revision:      validated.Revision,
		OCIManifests:  make([]releasebundle.OCIManifestInput, 0, velaImageCount),
	}
	for _, specification := range velaImageSpecifications() {
		input, err := captureOCIImageArtifact(
			filepath.Join(layoutRoot, specification.name),
			candidate,
			validated,
			specification,
		)
		if err != nil {
			return fmt.Errorf("capture %s OCI artifact: %w", specification.name, err)
		}
		manifest.OCIManifests = append(manifest.OCIManifests, input)
	}
	if err := writeJSONFile(filepath.Join(candidate, "vela-images.json"), manifest); err != nil {
		return fmt.Errorf("write Vela image artifact manifest: %w", err)
	}
	if err := verifyVelaImageArtifactCandidate(candidate, validated); err != nil {
		return fmt.Errorf("verify Vela image artifact candidate: %w", err)
	}
	if registry != nil {
		receipt, err := publishVelaImageLayouts(ctx, layoutRoot, candidate, manifest, registry)
		if err != nil {
			return err
		}
		if err := writeJSONFile(filepath.Join(candidate, velaRegistryPublicationFile), receipt); err != nil {
			return fmt.Errorf("write Vela registry publication receipt: %w", err)
		}
		if err := verifyVelaImagePublicationCandidate(candidate, validated, receipt); err != nil {
			return fmt.Errorf("verify Vela image publication candidate: %w", err)
		}
	}
	if err := syncDirectory(candidate); err != nil {
		return fmt.Errorf("sync Vela image artifact candidate: %w", err)
	}
	if err := renameNoReplace(candidate, outputDirectory); err != nil {
		return fmt.Errorf("publish Vela image artifacts: %w", err)
	}
	if err := syncDirectory(parent); err != nil {
		return fmt.Errorf("sync Vela image artifact parent: %w", err)
	}
	return nil
}

func velaImageSpecifications() [velaImageCount]velaImageSpecification {
	return [velaImageCount]velaImageSpecification{
		{name: "vela-control", entrypoint: "/usr/local/bin/vela-control"},
		{name: "vela-fleet-controller", entrypoint: "/usr/local/bin/vela-fleet-controller"},
		{name: "vela-model-runtime", entrypoint: "/usr/local/bin/vela-model-runtime"},
		{name: "vela-stage-worker-agent", entrypoint: "/usr/local/bin/vela-stage-worker-agent"},
	}
}

func validateVelaImageRepositories(prefix, revision string) error {
	for _, specification := range velaImageSpecifications() {
		repository := prefix + "/" + specification.name
		named, err := reference.ParseNormalizedNamed(repository)
		if err != nil || reference.FamiliarName(named) != repository {
			return fmt.Errorf("vela image repository %q is not canonical", repository)
		}
		if _, tagged := named.(reference.NamedTagged); tagged {
			return fmt.Errorf("vela image repository %q must not contain a tag", repository)
		}
		if _, digested := named.(reference.Digested); digested {
			return fmt.Errorf("vela image repository %q must not contain a digest", repository)
		}
		if _, err := reference.WithTag(named, revision); err != nil {
			return fmt.Errorf("release revision is not a valid OCI tag: %w", err)
		}
	}
	return nil
}

func runVelaImageArtifactBake(
	ctx context.Context,
	request VelaImageBuildRequest,
	layoutRoot string,
) error {
	arguments := []string{
		"buildx", "bake",
		"--allow=fs.write=" + layoutRoot,
		"--file", "docker-bake.hcl",
		"--provenance=false",
	}
	for _, specification := range velaImageSpecifications() {
		layout := filepath.Join(layoutRoot, specification.name)
		arguments = append(arguments,
			"--set="+specification.name+".output=type=oci,dest="+layout+",tar=false",
		)
	}
	arguments = append(arguments, "vela-all")
	return runVelaImageBakeCommand(
		ctx, request, arguments, "export Vela OCI image layouts",
	)
}

func captureOCIImageArtifact(
	layout, candidate string,
	request VelaImageBuildRequest,
	specification velaImageSpecification,
) (releasebundle.OCIManifestInput, error) {
	manifestEncoded, configEncoded, manifestDigest, err := validateOCIImageLayout(
		layout,
		request.Revision,
		specification,
	)
	if err != nil {
		return releasebundle.OCIManifestInput{}, err
	}
	manifestReference := specification.name + ".manifest.json"
	configReference := specification.name + ".config.json"
	if err := writeExactFile(filepath.Join(candidate, manifestReference), manifestEncoded); err != nil {
		return releasebundle.OCIManifestInput{}, fmt.Errorf("write manifest: %w", err)
	}
	if err := writeExactFile(filepath.Join(candidate, configReference), configEncoded); err != nil {
		return releasebundle.OCIManifestInput{}, fmt.Errorf("write config: %w", err)
	}
	return releasebundle.OCIManifestInput{
		Image: request.ImagePrefix + "/" + specification.name + "@" + manifestDigest,
		Ref:   manifestReference, ConfigRef: configReference,
	}, nil
}

func validateOCIImageLayout(
	layout, revision string,
	specification velaImageSpecification,
) ([]byte, []byte, string, error) {
	root, err := openOCILayoutRoot(layout)
	if err != nil {
		return nil, nil, "", fmt.Errorf("open OCI layout: %w", err)
	}
	defer func() { _ = root.Close() }()
	layoutEncoded, err := root.readMetadata("oci-layout", maximumMetadataBytes)
	if err != nil {
		return nil, nil, "", fmt.Errorf("read OCI layout marker: %w", err)
	}
	var marker ociv1.ImageLayout
	if err := decodeStrictJSON(layoutEncoded, &marker); err != nil || marker.Version != ociv1.ImageLayoutVersion {
		return nil, nil, "", errors.New("OCI layout marker is invalid")
	}
	indexEncoded, err := root.readMetadata("index.json", maximumMetadataBytes)
	if err != nil {
		return nil, nil, "", fmt.Errorf("read OCI index: %w", err)
	}
	var index ociv1.Index
	if err := decodeStrictJSON(indexEncoded, &index); err != nil {
		return nil, nil, "", fmt.Errorf("decode OCI index: %w", err)
	}
	if index.Versioned != (specs.Versioned{SchemaVersion: 2}) ||
		index.MediaType != ociv1.MediaTypeImageIndex || len(index.Manifests) != 1 {
		return nil, nil, "", errors.New("OCI index must contain exactly one image manifest")
	}
	descriptor := index.Manifests[0]
	if descriptor.MediaType != ociv1.MediaTypeImageManifest || descriptor.Platform == nil ||
		!exactLinuxAMD64(*descriptor.Platform) {
		return nil, nil, "", errors.New("OCI index manifest must bind linux/amd64")
	}
	manifestEncoded, err := readVerifiedOCIMetadataBlob(root, descriptor, maximumMetadataBytes)
	if err != nil {
		return nil, nil, "", fmt.Errorf("read OCI manifest blob: %w", err)
	}
	var manifest ociv1.Manifest
	if err := decodeStrictJSON(manifestEncoded, &manifest); err != nil {
		return nil, nil, "", fmt.Errorf("decode OCI manifest: %w", err)
	}
	if manifest.Versioned != (specs.Versioned{SchemaVersion: 2}) ||
		manifest.MediaType != ociv1.MediaTypeImageManifest || manifest.Config.MediaType != ociv1.MediaTypeImageConfig ||
		len(manifest.Layers) == 0 || len(manifest.Layers) > 4096 {
		return nil, nil, "", errors.New("OCI image manifest contract is invalid")
	}
	configEncoded, err := readVerifiedOCIMetadataBlob(root, manifest.Config, maximumMetadataBytes)
	if err != nil {
		return nil, nil, "", fmt.Errorf("read OCI config blob: %w", err)
	}
	totalLayerBytes := int64(0)
	for _, layer := range manifest.Layers {
		if !validOCIImageLayerMediaType(layer.MediaType) || layer.Size <= 0 ||
			layer.Size > maximumOCIImageLayoutBytes-totalLayerBytes {
			return nil, nil, "", errors.New("OCI image layer bytes exceed the bounded layout budget")
		}
		totalLayerBytes += layer.Size
		if err := verifyOCIBlob(root, layer); err != nil {
			return nil, nil, "", fmt.Errorf("verify OCI layer blob: %w", err)
		}
	}
	if err := validateOCIImageConfig(
		configEncoded, revision, len(manifest.Layers), specification,
	); err != nil {
		return nil, nil, "", err
	}
	return manifestEncoded, configEncoded, descriptor.Digest.String(), nil
}

func validOCIImageLayerMediaType(mediaType string) bool {
	return mediaType == ociv1.MediaTypeImageLayer ||
		mediaType == ociv1.MediaTypeImageLayerGzip ||
		mediaType == ociv1.MediaTypeImageLayerZstd
}

func exactLinuxAMD64(platform ociv1.Platform) bool {
	return platform.OS == "linux" && platform.Architecture == "amd64" &&
		platform.Variant == "" && platform.OSVersion == "" && len(platform.OSFeatures) == 0
}

func openVerifiedOCIBlob(
	root *ociLayoutRoot,
	descriptor ociv1.Descriptor,
	maximum int64,
) (*os.File, error) {
	if descriptor.Digest.Algorithm() != ocidigest.SHA256 || descriptor.Digest.Validate() != nil ||
		descriptor.Size <= 0 || len(descriptor.URLs) != 0 || len(descriptor.Data) != 0 {
		return nil, errors.New("OCI blob descriptor is invalid")
	}
	if maximum <= 0 || descriptor.Size > maximum {
		return nil, errors.New("OCI metadata blob exceeds the size limit")
	}
	file, size, err := root.openRegular(
		filepath.Join("blobs", "sha256", descriptor.Digest.Encoded()),
		maximum,
	)
	if err != nil {
		return nil, err
	}
	if descriptor.Size != size {
		_ = file.Close()
		return nil, errors.New("OCI blob digest or size does not match its descriptor")
	}
	return file, nil
}

func readVerifiedOCIMetadataBlob(
	root *ociLayoutRoot,
	descriptor ociv1.Descriptor,
	maximum int64,
) ([]byte, error) {
	file, err := openVerifiedOCIBlob(root, descriptor, maximum)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	digest := sha256.New()
	content, err := io.ReadAll(io.TeeReader(file, digest))
	if err != nil {
		return nil, err
	}
	if descriptor.Digest.String() != "sha256:"+hex.EncodeToString(digest.Sum(nil)) {
		return nil, errors.New("OCI blob digest or size does not match its descriptor")
	}
	return content, nil
}

func verifyOCIBlob(root *ociLayoutRoot, descriptor ociv1.Descriptor) error {
	file, err := openVerifiedOCIBlob(root, descriptor, descriptor.Size)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return err
	}
	if descriptor.Digest.String() != "sha256:"+hex.EncodeToString(digest.Sum(nil)) {
		return errors.New("OCI blob digest or size does not match its descriptor")
	}
	return nil
}

type ociLayoutRoot struct {
	directory *os.File
	name      string
}

func openOCILayoutRoot(name string) (*ociLayoutRoot, error) {
	if name == "" || !filepath.IsAbs(name) || filepath.Clean(name) != name {
		return nil, errors.New("OCI layout path must be canonical and absolute")
	}
	fd, err := unix.Open(name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.New("OCI layout must be a non-symlink directory")
	}
	return &ociLayoutRoot{directory: os.NewFile(uintptr(fd), name), name: name}, nil
}

func (root *ociLayoutRoot) Close() error {
	if root == nil || root.directory == nil {
		return nil
	}
	return root.directory.Close()
}

func (root *ociLayoutRoot) readMetadata(name string, maximum int64) ([]byte, error) {
	file, _, err := root.openRegular(name, maximum)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	return io.ReadAll(file)
}

func (root *ociLayoutRoot) openRegular(name string, maximum int64) (*os.File, int64, error) {
	components := strings.Split(name, string(filepath.Separator))
	currentFD := int(root.directory.Fd())
	ownedFD := -1
	closeOwned := func() {
		if ownedFD >= 0 {
			_ = unix.Close(ownedFD)
			ownedFD = -1
		}
	}
	for _, component := range components[:len(components)-1] {
		nextFD, err := unix.Openat(
			currentFD,
			component,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
			0,
		)
		if err != nil {
			closeOwned()
			return nil, 0, errors.New("OCI blob path must not contain a symbolic link or non-directory component")
		}
		closeOwned()
		currentFD = nextFD
		ownedFD = nextFD
	}
	fd, err := unix.Openat(
		currentFD,
		components[len(components)-1],
		unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		0,
	)
	closeOwned()
	if err != nil {
		return nil, 0, fmt.Errorf("open OCI file without following symbolic links: %w", err)
	}
	file := os.NewFile(uintptr(fd), filepath.Join(root.name, name))
	information, err := file.Stat()
	if err != nil || !information.Mode().IsRegular() || information.Size() <= 0 ||
		(maximum > 0 && information.Size() > maximum) {
		_ = file.Close()
		return nil, 0, errors.New("OCI file must be a bounded non-empty regular file")
	}
	return file, information.Size(), nil
}

func validateOCIImageConfig(
	encoded []byte,
	revision string,
	layerCount int,
	specification velaImageSpecification,
) error {
	var config ociv1.Image
	if err := decodeStrictJSON(encoded, &config); err != nil {
		return fmt.Errorf("decode OCI config: %w", err)
	}
	if !exactLinuxAMD64(config.Platform) ||
		config.Config.User != "10001:10001" ||
		!slices.Equal(config.Config.Entrypoint, []string{specification.entrypoint}) ||
		len(config.Config.Cmd) != 0 ||
		config.Config.Labels["org.opencontainers.image.title"] != specification.name ||
		config.Config.Labels["org.opencontainers.image.revision"] != revision {
		return errors.New("OCI config does not bind the exact runtime contract")
	}
	if config.RootFS.Type != "layers" || len(config.RootFS.DiffIDs) != layerCount {
		return errors.New("OCI config rootfs does not match the image layers")
	}
	for _, diffID := range config.RootFS.DiffIDs {
		if diffID.Algorithm() != ocidigest.SHA256 || diffID.Validate() != nil {
			return errors.New("OCI config rootfs contains an invalid diff ID")
		}
	}
	return nil
}

func writeExactFile(path string, content []byte) error {
	if len(content) == 0 {
		return errors.New("artifact content is empty")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func verifyVelaImageArtifactCandidate(directory string, request VelaImageBuildRequest) error {
	if err := verifyVelaImageArtifactInventory(directory, false); err != nil {
		return err
	}
	return verifyVelaImageArtifactFiles(directory, request)
}

func verifyVelaImagePublicationCandidate(
	directory string,
	request VelaImageBuildRequest,
	expected velaRegistryPublicationReceipt,
) error {
	if err := verifyVelaImageArtifactInventory(directory, true); err != nil {
		return err
	}
	if err := verifyVelaImageArtifactFiles(directory, request); err != nil {
		return err
	}
	encoded, err := readRegularMetadata(filepath.Join(directory, velaRegistryPublicationFile))
	if err != nil {
		return fmt.Errorf("read registry publication receipt: %w", err)
	}
	var receipt velaRegistryPublicationReceipt
	if err := decodeStrictJSON(encoded, &receipt); err != nil {
		return fmt.Errorf("decode registry publication receipt: %w", err)
	}
	if receipt.SchemaVersion != velaRegistryPublicationSchemaVersion ||
		receipt.Revision != request.Revision || len(receipt.Images) != velaImageCount ||
		!slices.Equal(receipt.Images, expected.Images) {
		return errors.New("registry publication receipt does not bind the exact Vela image set")
	}
	return nil
}

func verifyVelaImageArtifactInventory(directory string, includePublicationReceipt bool) error {
	expectedInventory := []string{"vela-images.json"}
	for _, specification := range velaImageSpecifications() {
		expectedInventory = append(expectedInventory,
			specification.name+".config.json",
			specification.name+".manifest.json",
		)
	}
	if includePublicationReceipt {
		expectedInventory = append(expectedInventory, velaRegistryPublicationFile)
	}
	slices.Sort(expectedInventory)
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	actualInventory := make([]string, 0, len(entries))
	for _, entry := range entries {
		actualInventory = append(actualInventory, entry.Name())
	}
	if !slices.Equal(actualInventory, expectedInventory) {
		return fmt.Errorf("inventory is not exact: got %v want %v", actualInventory, expectedInventory)
	}
	return nil
}

func verifyVelaImageArtifactFiles(directory string, request VelaImageBuildRequest) error {
	manifestEncoded, err := readRegularMetadata(filepath.Join(directory, "vela-images.json"))
	if err != nil {
		return err
	}
	var artifactManifest velaImageArtifactManifest
	if err := decodeStrictJSON(manifestEncoded, &artifactManifest); err != nil {
		return err
	}
	if artifactManifest.SchemaVersion != velaImageArtifactSchemaVersion ||
		artifactManifest.Revision != request.Revision ||
		len(artifactManifest.OCIManifests) != velaImageCount {
		return errors.New("vela image artifact manifest header is invalid")
	}
	for index, specification := range velaImageSpecifications() {
		input := artifactManifest.OCIManifests[index]
		if input.Ref != specification.name+".manifest.json" ||
			input.ConfigRef != specification.name+".config.json" {
			return errors.New("vela image artifact references are not exact")
		}
		manifest, err := readRegularMetadata(filepath.Join(directory, input.Ref))
		if err != nil {
			return err
		}
		digest := sha256.Sum256(manifest)
		expectedImage := request.ImagePrefix + "/" + specification.name + "@sha256:" + hex.EncodeToString(digest[:])
		if input.Image != expectedImage {
			return errors.New("vela image reference does not bind its manifest digest")
		}
		var imageManifest ociv1.Manifest
		if err := decodeStrictJSON(manifest, &imageManifest); err != nil {
			return err
		}
		config, err := readRegularMetadata(filepath.Join(directory, input.ConfigRef))
		if err != nil {
			return err
		}
		if err := releasebundle.ValidateOCIManifestInput(input, manifest, config); err != nil {
			return fmt.Errorf("validate published OCI manifest through release bundle: %w", err)
		}
		if err := validateOCIImageConfig(
			config,
			request.Revision,
			len(imageManifest.Layers),
			specification,
		); err != nil {
			return err
		}
	}
	return nil
}

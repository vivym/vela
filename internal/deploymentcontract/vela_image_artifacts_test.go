package deploymentcontract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	ocidigest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ociv1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/vivym/vela/internal/releasebundle"
)

type velaImageArtifactTarget struct {
	name       string
	entrypoint string
}

var velaImageArtifactTargets = [...]velaImageArtifactTarget{
	{name: "vela-control", entrypoint: "/usr/local/bin/vela-control"},
	{name: "vela-fleet-controller", entrypoint: "/usr/local/bin/vela-fleet-controller"},
	{name: "vela-h3-stage-runtime", entrypoint: "/usr/local/bin/vela-model-runtime"},
	{name: "vela-stage-worker-agent", entrypoint: "/usr/local/bin/vela-stage-worker-agent"},
}

func TestBuildVelaImageArtifactsProducesReleaseBundleInputs(t *testing.T) {
	fixture := newVelaImageArtifactFixture(t)
	expectedInputs := make([]releasebundle.OCIManifestInput, 0, len(velaImageArtifactTargets))
	for _, target := range velaImageArtifactTargets {
		expectedInputs = append(expectedInputs, releasebundle.OCIManifestInput{
			Image: fixture.imagePrefix + "/" + target.name + "@sha256:" + fixture.manifestDigests[target.name],
			Ref:   target.name + ".manifest.json", ConfigRef: target.name + ".config.json",
		})
	}
	output := filepath.Join(fixture.temporary, "artifacts")
	encoded, err := fixture.run(t, output, copyOCILayoutsFakeDocker)
	if err != nil {
		t.Fatalf("build Vela image artifacts: %v\n%s", err, encoded)
	}
	if string(encoded) != output+"/vela-images.json\n" {
		t.Fatalf("build output = %q", encoded)
	}

	manifestEncoded, err := os.ReadFile(filepath.Join(output, "vela-images.json"))
	if err != nil {
		t.Fatalf("read Vela image manifest: %v", err)
	}
	var manifest struct {
		SchemaVersion int                              `json:"schema_version"`
		Revision      string                           `json:"revision"`
		OCIManifests  []releasebundle.OCIManifestInput `json:"oci_manifests"`
	}
	if err := json.Unmarshal(manifestEncoded, &manifest); err != nil {
		t.Fatalf("decode Vela image manifest: %v", err)
	}
	if manifest.SchemaVersion != 1 || manifest.Revision != fixture.revision ||
		!reflect.DeepEqual(manifest.OCIManifests, expectedInputs) {
		t.Fatalf("Vela image manifest = %#v", manifest)
	}
	entries, err := os.ReadDir(output)
	if err != nil {
		t.Fatalf("read Vela image artifact inventory: %v", err)
	}
	actualInventory := make([]string, 0, len(entries))
	for _, entry := range entries {
		actualInventory = append(actualInventory, entry.Name())
	}
	expectedInventory := []string{"vela-images.json"}
	for _, target := range velaImageArtifactTargets {
		expectedInventory = append(expectedInventory,
			target.name+".config.json", target.name+".manifest.json",
		)
	}
	slices.Sort(expectedInventory)
	if !slices.Equal(actualInventory, expectedInventory) {
		t.Fatalf("Vela image artifact inventory = %v, want %v", actualInventory, expectedInventory)
	}
	for _, input := range manifest.OCIManifests {
		manifestBytes, err := os.ReadFile(filepath.Join(output, input.Ref))
		if err != nil {
			t.Fatalf("read exported OCI manifest: %v", err)
		}
		configBytes, err := os.ReadFile(filepath.Join(output, input.ConfigRef))
		if err != nil {
			t.Fatalf("read exported OCI config: %v", err)
		}
		if err := releasebundle.ValidateOCIManifestInput(input, manifestBytes, configBytes); err != nil {
			t.Fatalf("validate exported release-bundle input: %v", err)
		}
	}
}

func TestBuildVelaImageArtifactsRejectsInvalidLayerDescriptor(t *testing.T) {
	fixture := newVelaImageArtifactFixture(t)
	rewriteOCILayoutManifest(t, filepath.Join(fixture.layoutRoot, "vela-stage-worker-agent"), func(manifest *ociv1.Manifest) {
		manifest.Layers[0].MediaType = ""
	})
	assertVelaImageArtifactBuildRejected(t, fixture, copyOCILayoutsFakeDocker, "invalid OCI layer descriptor")
}

func TestBuildVelaImageArtifactsRejectsInvalidSubjectDescriptor(t *testing.T) {
	fixture := newVelaImageArtifactFixture(t)
	rewriteOCILayoutManifest(t, filepath.Join(fixture.layoutRoot, "vela-control"), func(manifest *ociv1.Manifest) {
		manifest.Subject = &ociv1.Descriptor{
			MediaType: ociv1.MediaTypeImageManifest,
			Digest:    ocidigest.Digest("sha256:" + fmt.Sprintf("%064d", 2)),
			Size:      1,
			URLs:      []string{"https://registry.example.com/forbidden"},
		}
	})
	assertVelaImageArtifactBuildRejected(t, fixture, copyOCILayoutsFakeDocker, "invalid OCI subject descriptor")
}

func TestBuildVelaImageArtifactsRejectsSymlinkedOCILayout(t *testing.T) {
	fixture := newVelaImageArtifactFixture(t)
	const fakeDocker = `#!/bin/sh
set -eu
for argument in "$@"; do
  case "$argument" in
    --set=*.output=type=oci,dest=*,tar=false)
      specification=${argument#--set=}
      target=${specification%%.output=*}
      destination=${specification#*dest=}
      destination=${destination%,tar=false}
      ln -s "$VELA_TEST_OCI_LAYOUTS/$target" "$destination"
      ;;
  esac
done
`
	assertVelaImageArtifactBuildRejected(t, fixture, fakeDocker, "symlinked OCI layouts")
}

func TestBuildVelaImageArtifactsRejectsSymlinkedOCIBlobDirectory(t *testing.T) {
	fixture := newVelaImageArtifactFixture(t)
	const fakeDocker = `#!/bin/sh
set -eu
for argument in "$@"; do
  case "$argument" in
    --set=*.output=type=oci,dest=*,tar=false)
      specification=${argument#--set=}
      target=${specification%%.output=*}
      destination=${specification#*dest=}
      destination=${destination%,tar=false}
      mkdir "$destination"
      cp "$VELA_TEST_OCI_LAYOUTS/$target/oci-layout" "$destination/oci-layout"
      cp "$VELA_TEST_OCI_LAYOUTS/$target/index.json" "$destination/index.json"
      ln -s "$VELA_TEST_OCI_LAYOUTS/$target/blobs" "$destination/blobs"
      ;;
  esac
done
`
	assertVelaImageArtifactBuildRejected(t, fixture, fakeDocker, "symlinked OCI blob directory")
}

func TestBuildVelaImageArtifactsRejectsConfigWithoutRootFS(t *testing.T) {
	fixture := newVelaImageArtifactFixture(t)
	rewriteOCILayoutConfig(
		t,
		filepath.Join(fixture.layoutRoot, "vela-control"),
		func(config *ociv1.Image) { config.RootFS = ociv1.RootFS{} },
	)
	assertVelaImageArtifactBuildRejected(t, fixture, copyOCILayoutsFakeDocker, "OCI config without rootfs")
}

func TestBuildVelaImageArtifactsRejectsNonExactPlatform(t *testing.T) {
	fixture := newVelaImageArtifactFixture(t)
	rewriteOCILayoutConfig(
		t,
		filepath.Join(fixture.layoutRoot, "vela-control"),
		func(config *ociv1.Image) { config.Variant = "v8" },
	)
	assertVelaImageArtifactBuildRejected(t, fixture, copyOCILayoutsFakeDocker, "non-exact OCI platform")
}

func TestBuildVelaImageArtifactsRejectsUnexpectedDefaultCommand(t *testing.T) {
	fixture := newVelaImageArtifactFixture(t)
	rewriteOCILayoutConfig(
		t,
		filepath.Join(fixture.layoutRoot, "vela-control"),
		func(config *ociv1.Image) { config.Config.Cmd = []string{"unexpected"} },
	)
	assertVelaImageArtifactBuildRejected(t, fixture, copyOCILayoutsFakeDocker, "unexpected default command")
}

func TestBuildVelaImageArtifactsRejectsMissingH3RuntimeCommandDigest(t *testing.T) {
	fixture := newVelaImageArtifactFixture(t)
	rewriteOCILayoutConfig(
		t,
		filepath.Join(fixture.layoutRoot, "vela-h3-stage-runtime"),
		func(config *ociv1.Image) { delete(config.Config.Labels, "vela.ai.h3-dit.sha256") },
	)
	assertVelaImageArtifactBuildRejected(
		t,
		fixture,
		copyOCILayoutsFakeDocker,
		"missing H3 DiT command digest label",
	)
}

func writeOCIImageLayoutFixture(
	t *testing.T,
	directory, title, revision, entrypoint string,
	additionalLabels map[string]string,
) string {
	t.Helper()
	labels := map[string]string{
		"org.opencontainers.image.revision": revision,
		"org.opencontainers.image.title":    title,
	}
	for name, value := range additionalLabels {
		labels[name] = value
	}
	config := ociv1.Image{
		Platform: ociv1.Platform{OS: "linux", Architecture: "amd64"},
		Config: ociv1.ImageConfig{
			User: "10001:10001", Entrypoint: []string{entrypoint}, Labels: labels,
		},
		RootFS: ociv1.RootFS{Type: "layers", DiffIDs: []ocidigest.Digest{ocidigest.Digest("sha256:" + fmt.Sprintf("%064d", 1))}},
	}
	configEncoded := marshalJSONFixture(t, config)
	configDigest := sha256.Sum256(configEncoded)
	layerEncoded := []byte("fixture-layer-" + title)
	layerDigest := sha256.Sum256(layerEncoded)
	manifest := ociv1.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ociv1.MediaTypeImageManifest,
		Config: ociv1.Descriptor{
			MediaType: ociv1.MediaTypeImageConfig,
			Digest:    ocidigest.Digest("sha256:" + hex.EncodeToString(configDigest[:])),
			Size:      int64(len(configEncoded)),
		},
		Layers: []ociv1.Descriptor{{
			MediaType: ociv1.MediaTypeImageLayer,
			Digest:    ocidigest.Digest("sha256:" + hex.EncodeToString(layerDigest[:])),
			Size:      int64(len(layerEncoded)),
		}},
	}
	manifestEncoded := marshalJSONFixture(t, manifest)
	manifestDigest := sha256.Sum256(manifestEncoded)
	index := ociv1.Index{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ociv1.MediaTypeImageIndex,
		Manifests: []ociv1.Descriptor{{
			MediaType: ociv1.MediaTypeImageManifest,
			Digest:    ocidigest.Digest("sha256:" + hex.EncodeToString(manifestDigest[:])),
			Size:      int64(len(manifestEncoded)),
			Platform:  &ociv1.Platform{OS: "linux", Architecture: "amd64"},
		}},
	}
	writeOCIFileFixture(t, directory, "oci-layout", []byte("{\"imageLayoutVersion\":\"1.0.0\"}\n"))
	writeOCIFileFixture(t, directory, "index.json", marshalJSONFixture(t, index))
	writeOCIFileFixture(t, directory, filepath.Join("blobs", "sha256", hex.EncodeToString(configDigest[:])), configEncoded)
	writeOCIFileFixture(t, directory, filepath.Join("blobs", "sha256", hex.EncodeToString(layerDigest[:])), layerEncoded)
	writeOCIFileFixture(t, directory, filepath.Join("blobs", "sha256", hex.EncodeToString(manifestDigest[:])), manifestEncoded)
	return hex.EncodeToString(manifestDigest[:])
}

func rewriteOCILayoutManifest(
	t *testing.T,
	directory string,
	mutate func(*ociv1.Manifest),
) {
	t.Helper()
	indexPath := filepath.Join(directory, "index.json")
	indexEncoded, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("read OCI fixture index: %v", err)
	}
	var index ociv1.Index
	if err := json.Unmarshal(indexEncoded, &index); err != nil {
		t.Fatalf("decode OCI fixture index: %v", err)
	}
	manifestPath := filepath.Join(directory, "blobs", "sha256", index.Manifests[0].Digest.Encoded())
	manifestEncoded, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read OCI fixture manifest: %v", err)
	}
	var manifest ociv1.Manifest
	if err := json.Unmarshal(manifestEncoded, &manifest); err != nil {
		t.Fatalf("decode OCI fixture manifest: %v", err)
	}
	mutate(&manifest)
	manifestEncoded = marshalJSONFixture(t, manifest)
	manifestDigest := sha256.Sum256(manifestEncoded)
	index.Manifests[0].Digest = ocidigest.Digest("sha256:" + hex.EncodeToString(manifestDigest[:]))
	index.Manifests[0].Size = int64(len(manifestEncoded))
	writeOCIFileFixture(
		t, directory,
		filepath.Join("blobs", "sha256", hex.EncodeToString(manifestDigest[:])),
		manifestEncoded,
	)
	if err := os.WriteFile(indexPath, marshalJSONFixture(t, index), 0o600); err != nil {
		t.Fatalf("rewrite OCI fixture index: %v", err)
	}
}

func rewriteOCILayoutConfig(
	t *testing.T,
	directory string,
	mutate func(*ociv1.Image),
) {
	t.Helper()
	indexEncoded, err := os.ReadFile(filepath.Join(directory, "index.json"))
	if err != nil {
		t.Fatalf("read OCI fixture index: %v", err)
	}
	var index ociv1.Index
	if err := json.Unmarshal(indexEncoded, &index); err != nil {
		t.Fatalf("decode OCI fixture index: %v", err)
	}
	manifestPath := filepath.Join(directory, "blobs", "sha256", index.Manifests[0].Digest.Encoded())
	manifestEncoded, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read OCI fixture manifest: %v", err)
	}
	var manifest ociv1.Manifest
	if err := json.Unmarshal(manifestEncoded, &manifest); err != nil {
		t.Fatalf("decode OCI fixture manifest: %v", err)
	}
	configPath := filepath.Join(directory, "blobs", "sha256", manifest.Config.Digest.Encoded())
	configEncoded, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read OCI fixture config: %v", err)
	}
	var config ociv1.Image
	if err := json.Unmarshal(configEncoded, &config); err != nil {
		t.Fatalf("decode OCI fixture config: %v", err)
	}
	mutate(&config)
	configEncoded = marshalJSONFixture(t, config)
	configDigest := sha256.Sum256(configEncoded)
	writeOCIFileFixture(
		t, directory,
		filepath.Join("blobs", "sha256", hex.EncodeToString(configDigest[:])),
		configEncoded,
	)
	rewriteOCILayoutManifest(t, directory, func(current *ociv1.Manifest) {
		current.Config.Digest = ocidigest.Digest("sha256:" + hex.EncodeToString(configDigest[:]))
		current.Config.Size = int64(len(configEncoded))
	})
}

type velaImageArtifactFixture struct {
	repository      string
	temporary       string
	layoutRoot      string
	revision        string
	imagePrefix     string
	manifestDigests map[string]string
	runtimeBase     string
	commandContext  string
	commandDigests  map[string]string
}

func newVelaImageArtifactFixture(t *testing.T) velaImageArtifactFixture {
	t.Helper()
	fixture := velaImageArtifactFixture{
		repository:      deploymentRepositoryRoot(t),
		temporary:       canonicalTemporaryDirectory(t),
		revision:        "release-test-r3",
		imagePrefix:     "registry.example.com/vela",
		manifestDigests: make(map[string]string, len(velaImageArtifactTargets)),
		runtimeBase:     "registry.example.com/minimax/h3-runtime-base@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	fixture.commandContext, fixture.commandDigests = newH3RuntimeCommandFixture(t)
	fixture.layoutRoot = filepath.Join(fixture.temporary, "layouts")
	if err := os.Mkdir(fixture.layoutRoot, 0o700); err != nil {
		t.Fatalf("create OCI fixture root: %v", err)
	}
	for _, target := range velaImageArtifactTargets {
		labels := map[string]string(nil)
		if target.name == "vela-h3-stage-runtime" {
			labels = map[string]string{
				"vela.ai.h3-runtime-base":       fixture.runtimeBase,
				"vela.ai.h3-encoder.sha256":     fixture.commandDigests["h3-encoder"],
				"vela.ai.h3-dit.sha256":         fixture.commandDigests["h3-dit"],
				"vela.ai.h3-vae-decoder.sha256": fixture.commandDigests["h3-vae-decoder"],
			}
		}
		fixture.manifestDigests[target.name] = writeOCIImageLayoutFixture(
			t, filepath.Join(fixture.layoutRoot, target.name), target.name,
			fixture.revision, target.entrypoint, labels,
		)
	}
	return fixture
}

func (fixture velaImageArtifactFixture) run(
	t *testing.T,
	output, fakeDocker string,
) ([]byte, error) {
	t.Helper()
	fakeBin := filepath.Join(fixture.temporary, "bin")
	if err := os.Mkdir(fakeBin, 0o700); err != nil {
		t.Fatalf("create fake Docker directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fakeBin, "docker"), []byte(fakeDocker), 0o700); err != nil {
		t.Fatalf("write fake Docker: %v", err)
	}
	command := exec.Command("make", "-s", "--no-print-directory", "build-vela-image-artifacts")
	command.Dir = fixture.repository
	command.Env = append(os.Environ(),
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"RELEASE_REVISION="+fixture.revision,
		"RELEASE_IMAGE_PREFIX="+fixture.imagePrefix,
		"RELEASE_ARTIFACT_DIR="+output,
		"H3_RUNTIME_BASE="+fixture.runtimeBase,
		"H3_RUNTIME_COMMAND_CONTEXT="+fixture.commandContext,
		"H3_ENCODER_SHA256="+fixture.commandDigests["h3-encoder"],
		"H3_DIT_SHA256="+fixture.commandDigests["h3-dit"],
		"H3_VAE_DECODER_SHA256="+fixture.commandDigests["h3-vae-decoder"],
		"VELA_TEST_OCI_LAYOUTS="+fixture.layoutRoot,
	)
	return command.CombinedOutput()
}

func assertVelaImageArtifactBuildRejected(
	t *testing.T,
	fixture velaImageArtifactFixture,
	fakeDocker, reason string,
) {
	t.Helper()
	output := filepath.Join(fixture.temporary, "artifacts")
	if encoded, err := fixture.run(t, output, fakeDocker); err == nil {
		t.Fatalf("build accepted %s:\n%s", reason, encoded)
	}
	if _, err := os.Lstat(output); !os.IsNotExist(err) {
		t.Fatalf("published output after %s: %v", reason, err)
	}
}

const copyOCILayoutsFakeDocker = `#!/bin/sh
set -eu
for argument in "$@"; do
  case "$argument" in
    --set=*.output=type=oci,dest=*,tar=false)
      specification=${argument#--set=}
      target=${specification%%.output=*}
      destination=${specification#*dest=}
      destination=${destination%,tar=false}
      mkdir "$destination"
      cp -R "$VELA_TEST_OCI_LAYOUTS/$target/." "$destination/"
      ;;
  esac
done
`

func marshalJSONFixture(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode OCI fixture: %v", err)
	}
	return encoded
}

func writeOCIFileFixture(t *testing.T, root, name string, content []byte) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create OCI fixture directory: %v", err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write OCI fixture %s: %v", name, err)
	}
}

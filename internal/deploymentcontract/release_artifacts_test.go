package deploymentcontract

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/releaseartifacts"
	"github.com/vivym/vela/internal/releasebundle"
)

func TestBuildHostPackagesProducesReleaseBundleInputs(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	output := filepath.Join(canonicalTemporaryDirectory(t), "artifacts")
	const revision = "release-test-r1"
	if encoded, err := runHostPackageMake(repository, revision, output); err != nil {
		t.Fatalf("build host packages: %v\n%s", err, encoded)
	}

	manifest := loadHostPackageManifest(t, filepath.Join(output, "host-packages.json"))
	if manifest.SchemaVersion != 1 || manifest.Revision != revision ||
		!reflect.DeepEqual(manifest.Packages, []releasebundle.PackageInput{
			{Name: "node-agent", ContractRef: "node-agent-contract.json", ArtifactRef: "vela-node-agent"},
		}) {
		t.Fatalf("host package manifest = %#v", manifest)
	}

	expectedEntrypoints := map[string]string{
		"node-agent": "/usr/local/bin/vela-node-agent",
	}
	for _, item := range manifest.Packages {
		artifactPath := filepath.Join(output, item.ArtifactRef)
		artifact, err := os.ReadFile(artifactPath)
		if err != nil {
			t.Fatalf("read %s artifact: %v", item.Name, err)
		}
		contract := loadPackageContract(t, filepath.Join(output, item.ContractRef))
		digest := sha256.Sum256(artifact)
		if contract.SchemaVersion != 1 || contract.Name != "vela-"+item.Name ||
			contract.OS != "linux" || contract.Architecture != "amd64" ||
			contract.Revision != revision ||
			contract.Entrypoint != expectedEntrypoints[item.Name] ||
			contract.ArtifactDigest != "sha256:"+hex.EncodeToString(digest[:]) ||
			contract.ArtifactSizeBytes != int64(len(artifact)) {
			t.Fatalf("%s package contract = %#v", item.Name, contract)
		}
	}

	assertLinuxAMD64NodeAgent(t, filepath.Join(output, "vela-node-agent"))
}

func TestBuildH3MockBackendProducesExactVerifiedContext(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	output := filepath.Join(canonicalTemporaryDirectory(t), "h3-mock-context")
	command := exec.Command("make", "-s", "build-h3-mock-backend")
	command.Dir = repository
	command.Env = append(os.Environ(), "H3_MOCK_BACKEND_CONTEXT="+output)
	encoded, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build H3 mock backend: %v\n%s", err, encoded)
	}
	entries, err := os.ReadDir(output)
	if err != nil {
		t.Fatalf("read mock backend context: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "h3-backend" || !entries[0].Type().IsRegular() {
		t.Fatalf("mock backend context inventory = %v", entries)
	}
	backendPath := filepath.Join(output, "h3-backend")
	content, err := os.ReadFile(backendPath)
	if err != nil {
		t.Fatalf("read mock backend: %v", err)
	}
	digest := sha256.Sum256(content)
	if err := releaseartifacts.VerifyH3Backend(output, hex.EncodeToString(digest[:])); err != nil {
		t.Fatalf("verify mock backend: %v", err)
	}
	information, err := os.Stat(backendPath)
	if err != nil {
		t.Fatalf("stat mock backend: %v", err)
	}
	if information.Mode().Perm() != 0o555 {
		t.Fatalf("mock backend mode = %04o, want 0555", information.Mode().Perm())
	}
}

func TestBuildH3StageMockRuntimeProducesExactVerifiedCommands(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	output := filepath.Join(canonicalTemporaryDirectory(t), "h3-stage-mock-commands")
	command := exec.Command("make", "-s", "build-h3-stage-mock-runtime")
	command.Dir = repository
	command.Env = append(os.Environ(), "H3_STAGE_MOCK_RUNTIME_CONTEXT="+output)
	encoded, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build H3 Stage mock runtime: %v\n%s", err, encoded)
	}
	entries, err := os.ReadDir(output)
	if err != nil {
		t.Fatalf("read H3 Stage mock runtime context: %v", err)
	}
	wantNames := []string{"h3-dit", "h3-encoder", "h3-vae-decoder"}
	if len(entries) != len(wantNames) {
		t.Fatalf("H3 Stage mock runtime inventory=%v", entries)
	}
	digests := make(map[string]string, len(entries))
	for index, entry := range entries {
		if entry.Name() != wantNames[index] || !entry.Type().IsRegular() {
			t.Fatalf("H3 Stage mock runtime inventory=%v", entries)
		}
		path := filepath.Join(output, entry.Name())
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		digest := sha256.Sum256(content)
		digests[entry.Name()] = hex.EncodeToString(digest[:])
		information, err := os.Stat(path)
		if err != nil || information.Mode().Perm() != 0o555 {
			t.Fatalf("%s mode=%v error=%v", entry.Name(), information.Mode().Perm(), err)
		}
	}
	if err := releaseartifacts.VerifyH3RuntimeCommands(
		output, digests["h3-encoder"], digests["h3-dit"], digests["h3-vae-decoder"],
	); err != nil {
		t.Fatalf("verify H3 Stage mock runtime commands: %v", err)
	}
	if digests["h3-encoder"] != digests["h3-dit"] ||
		digests["h3-encoder"] != digests["h3-vae-decoder"] {
		t.Fatalf("mock commands were not published from one runtime: %v", digests)
	}

	if encoded, err := command.CombinedOutput(); err == nil {
		t.Fatalf("second H3 Stage mock runtime build unexpectedly succeeded:\n%s", encoded)
	}
}

func TestVerifyH3RuntimeCommandsRequiresExactDigestBoundInventory(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	mockContext := filepath.Join(canonicalTemporaryDirectory(t), "h3-mock-context")
	command := exec.Command("make", "-s", "build-h3-mock-backend")
	command.Dir = repository
	command.Env = append(os.Environ(), "H3_MOCK_BACKEND_CONTEXT="+mockContext)
	if encoded, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build H3 mock backend: %v\n%s", err, encoded)
	}
	backend, err := os.ReadFile(filepath.Join(mockContext, "h3-backend"))
	if err != nil {
		t.Fatalf("read mock backend: %v", err)
	}
	runtimeContext := filepath.Join(canonicalTemporaryDirectory(t), "h3-runtime-commands")
	if err := os.Mkdir(runtimeContext, 0o700); err != nil {
		t.Fatalf("create runtime command context: %v", err)
	}
	digests := make(map[string]string, 3)
	for _, name := range []string{"h3-encoder", "h3-dit", "h3-vae-decoder"} {
		path := filepath.Join(runtimeContext, name)
		if err := os.WriteFile(path, backend, 0o555); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		digest := sha256.Sum256(backend)
		digests[name] = hex.EncodeToString(digest[:])
	}
	if err := releaseartifacts.VerifyH3RuntimeCommands(
		runtimeContext,
		digests["h3-encoder"],
		digests["h3-dit"],
		digests["h3-vae-decoder"],
	); err != nil {
		t.Fatalf("verify H3 runtime commands: %v", err)
	}

	if err := os.WriteFile(filepath.Join(runtimeContext, "unexpected"), backend, 0o555); err != nil {
		t.Fatalf("write unexpected runtime command: %v", err)
	}
	if err := releaseartifacts.VerifyH3RuntimeCommands(
		runtimeContext,
		digests["h3-encoder"],
		digests["h3-dit"],
		digests["h3-vae-decoder"],
	); err == nil {
		t.Fatal("H3 runtime command verifier accepted an unexpected file")
	}
}

func TestBuildHostPackagesRejectsSymlinkOutputParent(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	temporary := canonicalTemporaryDirectory(t)
	realParent := filepath.Join(temporary, "real-parent")
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatalf("create real output parent: %v", err)
	}
	linkedParent := filepath.Join(temporary, "linked-parent")
	if err := os.Symlink(realParent, linkedParent); err != nil {
		t.Fatalf("link output parent: %v", err)
	}
	output := filepath.Join(linkedParent, "artifacts")
	if encoded, err := runHostPackageMake(repository, "release-test-r1", output); err == nil {
		t.Fatalf("build through symlink output parent unexpectedly succeeded:\n%s", encoded)
	}
	if _, err := os.Lstat(output); !os.IsNotExist(err) {
		t.Fatalf("published output after rejecting symlink parent: %v", err)
	}
}

func TestBuildHostPackagesRejectsSymlinkInOutputParentAncestry(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	temporary := canonicalTemporaryDirectory(t)
	realParent := filepath.Join(temporary, "real-parent")
	if err := os.MkdirAll(filepath.Join(realParent, "nested"), 0o700); err != nil {
		t.Fatalf("create real output ancestry: %v", err)
	}
	linkedAncestor := filepath.Join(temporary, "linked-ancestor")
	if err := os.Symlink(realParent, linkedAncestor); err != nil {
		t.Fatalf("link output ancestor: %v", err)
	}
	output := filepath.Join(linkedAncestor, "nested", "artifacts")
	if encoded, err := runHostPackageMake(repository, "release-test-r1", output); err == nil {
		t.Fatalf("build through symlink output ancestry unexpectedly succeeded:\n%s", encoded)
	}
	if _, err := os.Lstat(filepath.Join(realParent, "nested", "artifacts")); !os.IsNotExist(err) {
		t.Fatalf("published output after rejecting symlink ancestor: %v", err)
	}
}

func TestBuildHostPackagesProducesReproducibleFixedInventory(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	outputs := []string{
		filepath.Join(canonicalTemporaryDirectory(t), "first"),
		filepath.Join(canonicalTemporaryDirectory(t), "second"),
	}
	for _, output := range outputs {
		if encoded, err := runHostPackageMake(repository, "release-test-r1", output); err != nil {
			t.Fatalf("build host packages in %s: %v\n%s", output, err, encoded)
		}
	}

	const expectedInventory = "host-packages.json\nnode-agent-contract.json\nvela-node-agent\n"
	for _, output := range outputs {
		entries, err := os.ReadDir(output)
		if err != nil {
			t.Fatalf("read host package inventory in %s: %v", output, err)
		}
		var inventory strings.Builder
		for _, entry := range entries {
			inventory.WriteString(entry.Name())
			inventory.WriteByte('\n')
		}
		if inventory.String() != expectedInventory {
			t.Fatalf("host package inventory in %s = %q", output, inventory.String())
		}
	}

	for _, name := range strings.Split(strings.TrimSpace(expectedInventory), "\n") {
		first, err := os.ReadFile(filepath.Join(outputs[0], name))
		if err != nil {
			t.Fatalf("read first %s: %v", name, err)
		}
		second, err := os.ReadFile(filepath.Join(outputs[1], name))
		if err != nil {
			t.Fatalf("read second %s: %v", name, err)
		}
		if !bytes.Equal(first, second) {
			t.Fatalf("host package %s is not byte-for-byte reproducible", name)
		}
	}
}

func TestBuildHostPackagesLeavesNoOutputAfterBuildFailure(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	sourceWithoutNodeAgent := canonicalTemporaryDirectory(t)
	parent := canonicalTemporaryDirectory(t)
	output := filepath.Join(parent, "artifacts")
	command := exec.Command(
		"go", "run", "./cmd/vela-release-artifacts", "build-host-packages",
		sourceWithoutNodeAgent, "release-test-r1", output,
	)
	command.Dir = repository
	if encoded, err := command.CombinedOutput(); err == nil {
		t.Fatalf("host package build without Node Agent source unexpectedly succeeded:\n%s", encoded)
	}
	if _, err := os.Lstat(output); !os.IsNotExist(err) {
		t.Fatalf("published output after failed build: %v", err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatalf("read failed build parent: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed build left candidate files: %v", entries)
	}
}

func TestBuildHostPackagesDoesNotReplaceExistingOutput(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	output := filepath.Join(canonicalTemporaryDirectory(t), "artifacts")
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatalf("create existing output: %v", err)
	}
	sentinel := filepath.Join(output, "owned-by-caller")
	if err := os.WriteFile(sentinel, []byte("preserve me\n"), 0o600); err != nil {
		t.Fatalf("write existing output sentinel: %v", err)
	}
	if encoded, err := runHostPackageMake(repository, "release-test-r1", output); err == nil {
		t.Fatalf("host package build over existing output unexpectedly succeeded:\n%s", encoded)
	}
	content, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatalf("read existing output sentinel: %v", err)
	}
	if string(content) != "preserve me\n" {
		t.Fatalf("existing output sentinel = %q", content)
	}
	entries, err := os.ReadDir(output)
	if err != nil {
		t.Fatalf("read existing output: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "owned-by-caller" {
		t.Fatalf("existing output inventory changed: %v", entries)
	}
}

func TestBuildHostPackagesRejectsPlaceholderRevision(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	parent := canonicalTemporaryDirectory(t)
	output := filepath.Join(parent, "artifacts")
	if encoded, err := runHostPackageMake(repository, "replace-with-release-sha", output); err == nil {
		t.Fatalf("host package build with placeholder revision unexpectedly succeeded:\n%s", encoded)
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatalf("read placeholder revision output parent: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("placeholder revision left output files: %v", entries)
	}
}

func TestBuildHostPackagesVerifiesCandidateBeforePublication(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	temporary := canonicalTemporaryDirectory(t)
	fakeBin := filepath.Join(temporary, "bin")
	if err := os.Mkdir(fakeBin, 0o700); err != nil {
		t.Fatalf("create fake build tool directory: %v", err)
	}
	fakeGo := `#!/bin/sh
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-o" ]; then
    shift
    printf 'not an ELF binary\n' > "$1"
    exit 0
  fi
  shift
done
exit 2
`
	if err := os.WriteFile(filepath.Join(fakeBin, "go"), []byte(fakeGo), 0o700); err != nil {
		t.Fatalf("write fake go: %v", err)
	}
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	output := filepath.Join(temporary, "artifacts")
	if err := releaseartifacts.BuildHostPackages(
		context.Background(), repository, "release-test-r1", output,
	); err == nil {
		t.Fatal("host package build published an invalid Node Agent")
	}
	if _, err := os.Lstat(output); !os.IsNotExist(err) {
		t.Fatalf("published output after candidate verification failure: %v", err)
	}
}

func TestBuildHostPackagesDoesNotReplaceOutputCreatedDuringBuild(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	temporary := canonicalTemporaryDirectory(t)
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("resolve real go tool: %v", err)
	}
	fakeBin := filepath.Join(temporary, "bin")
	if err := os.Mkdir(fakeBin, 0o700); err != nil {
		t.Fatalf("create coordinated build tool directory: %v", err)
	}
	coordinatedGo := `#!/bin/sh
: > "$VELA_TEST_BUILD_STARTED"
while [ ! -e "$VELA_TEST_BUILD_CONTINUE" ]; do
  sleep 0.01
done
exec "$VELA_TEST_REAL_GO" "$@"
`
	if err := os.WriteFile(filepath.Join(fakeBin, "go"), []byte(coordinatedGo), 0o700); err != nil {
		t.Fatalf("write coordinated go wrapper: %v", err)
	}
	started := filepath.Join(temporary, "build-started")
	continued := filepath.Join(temporary, "build-continue")
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("VELA_TEST_REAL_GO", realGo)
	t.Setenv("VELA_TEST_BUILD_STARTED", started)
	t.Setenv("VELA_TEST_BUILD_CONTINUE", continued)
	output := filepath.Join(temporary, "artifacts")
	result := make(chan error, 1)
	go func() {
		result <- releaseartifacts.BuildHostPackages(
			context.Background(), repository, "release-test-r1", output,
		)
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(started); err == nil {
			break
		} else if !os.IsNotExist(err) {
			t.Fatalf("observe coordinated build: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for coordinated build")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatalf("create concurrent caller output: %v", err)
	}
	before, err := os.Stat(output)
	if err != nil {
		t.Fatalf("stat concurrent caller output: %v", err)
	}
	if err := os.WriteFile(continued, nil, 0o600); err != nil {
		t.Fatalf("continue coordinated build: %v", err)
	}
	if err := <-result; err == nil {
		t.Fatal("host package build replaced output created during the build")
	}
	after, err := os.Stat(output)
	if err != nil {
		t.Fatalf("stat concurrent caller output after build: %v", err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("host package build changed the concurrent caller output identity")
	}
}

func TestPrintVelaImageBuildDefinesExactPinnedTargets(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	commandContext, commandDigests := newH3RuntimeCommandFixture(t)
	const (
		revision    = "release-test-r2"
		imagePrefix = "registry.example.com/vela"
		runtimeBase = "docker.io/library/debian@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	)
	encoded, err := runPrintVelaImageBuild(
		repository,
		revision,
		imagePrefix,
		runtimeBase,
		commandContext,
		commandDigests,
	)
	if err != nil {
		t.Fatalf("print Vela image build: %v\n%s", err, encoded)
	}
	var definition struct {
		Group map[string]struct {
			Targets []string `json:"targets"`
		} `json:"group"`
		Target map[string]struct {
			Context    string            `json:"context"`
			Dockerfile string            `json:"dockerfile"`
			Args       map[string]string `json:"args"`
			Contexts   map[string]string `json:"contexts"`
			Tags       []string          `json:"tags"`
			Platforms  []string          `json:"platforms"`
		} `json:"target"`
	}
	if err := json.Unmarshal(encoded, &definition); err != nil {
		t.Fatalf("decode Vela image build definition: %v\n%s", err, encoded)
	}
	expectedTargets := []string{
		"vela-control", "vela-fleet-controller", "vela-h3-stage-runtime", "vela-stage-worker-agent",
	}
	slices.Sort(definition.Group["vela-all"].Targets)
	if !slices.Equal(definition.Group["vela-all"].Targets, expectedTargets) {
		t.Fatalf("Vela image group targets = %v", definition.Group["vela-all"].Targets)
	}
	const (
		goBase     = "docker.io/library/golang:1.26.7-bookworm@sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2"
		debianBase = "docker.io/library/debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171"
	)
	for _, name := range expectedTargets {
		target, present := definition.Target[name]
		if !present {
			t.Fatalf("Vela image target %q is absent", name)
		}
		if target.Context != "." || target.Dockerfile != "Dockerfile" ||
			!slices.Equal(target.Platforms, []string{"linux/amd64"}) ||
			!slices.Equal(target.Tags, []string{imagePrefix + "/" + name + ":" + revision}) ||
			target.Args["RELEASE_REVISION"] != revision || target.Args["GO_BASE"] != goBase ||
			target.Args["DEBIAN_BASE"] != debianBase {
			t.Fatalf("Vela image target %q = %#v", name, target)
		}
		if name == "vela-h3-stage-runtime" {
			if target.Args["H3_RUNTIME_BASE"] != runtimeBase ||
				target.Args["H3_ENCODER_SHA256"] != commandDigests["h3-encoder"] ||
				target.Args["H3_DIT_SHA256"] != commandDigests["h3-dit"] ||
				target.Args["H3_VAE_DECODER_SHA256"] != commandDigests["h3-vae-decoder"] ||
				target.Contexts["h3_runtime_commands"] != commandContext ||
				len(target.Args) != 7 || len(target.Contexts) != 1 {
				t.Fatalf("H3 stage runtime target = %#v", target)
			}
		} else if len(target.Args) != 3 || len(target.Contexts) != 0 {
			t.Fatalf("non-H3 Vela image target %q carries H3 composition inputs: %#v", name, target)
		}
	}
}

func TestPrintVelaImageBuildRequiresExplicitImagePrefix(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	command := exec.Command("make", "-s", "print-vela-image-build")
	command.Dir = repository
	command.Env = append(environmentWithout(os.Environ(), "RELEASE_IMAGE_PREFIX"),
		"RELEASE_REVISION=release-test-r2",
	)
	if encoded, err := command.CombinedOutput(); err == nil {
		t.Fatalf("print image build without image prefix unexpectedly succeeded:\n%s", encoded)
	}
}

func TestBuildVelaImagesGrantsExactRuntimeCommandContextRead(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	commandContext, commandDigests := newH3RuntimeCommandFixture(t)
	temporary := canonicalTemporaryDirectory(t)
	fakeBin := filepath.Join(temporary, "bin")
	if err := os.Mkdir(fakeBin, 0o700); err != nil {
		t.Fatalf("create fake Docker directory: %v", err)
	}
	const fakeDocker = `#!/bin/sh
set -eu
printf '%s\n' "$@" >"$VELA_TEST_DOCKER_ARGUMENTS"
`
	if err := os.WriteFile(filepath.Join(fakeBin, "docker"), []byte(fakeDocker), 0o700); err != nil {
		t.Fatalf("write fake Docker: %v", err)
	}
	argumentsFile := filepath.Join(temporary, "docker-arguments")
	command := exec.Command("make", "-s", "--no-print-directory", "build-vela-images")
	command.Dir = repository
	command.Env = append(os.Environ(),
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"VELA_TEST_DOCKER_ARGUMENTS="+argumentsFile,
		"RELEASE_REVISION=release-test-r2",
		"RELEASE_IMAGE_PREFIX=registry.example.com/vela",
		"H3_RUNTIME_BASE=docker.io/library/debian@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"H3_RUNTIME_COMMAND_CONTEXT="+commandContext,
		"H3_ENCODER_SHA256="+commandDigests["h3-encoder"],
		"H3_DIT_SHA256="+commandDigests["h3-dit"],
		"H3_VAE_DECODER_SHA256="+commandDigests["h3-vae-decoder"],
	)
	if encoded, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build Vela images with fake Docker: %v\n%s", err, encoded)
	}
	encoded, err := os.ReadFile(argumentsFile)
	if err != nil {
		t.Fatalf("read Docker arguments: %v", err)
	}
	if !strings.Contains(string(encoded), "--allow=fs.read="+commandContext+"\n") {
		t.Fatalf("Docker arguments do not grant exact command context read:\n%s", encoded)
	}
}

func TestVelaImageDockerfilePinsRuntimeContract(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	encoded, err := os.ReadFile(filepath.Join(repository, "Dockerfile"))
	if err != nil {
		t.Fatalf("read Vela image Dockerfile: %v", err)
	}
	dockerfile := string(encoded)
	for _, required := range []string{
		"# syntax=docker/dockerfile:1.20@sha256:26147acbda4f14c5add9946e2fd2ed543fc402884fd75146bd342a7f6271dc1d",
		"build-essential=12.9",
		"musl-tools=1.2.3-1",
		"nasm=2.16.01-1",
		"xz-utils=5.4.1-1+deb12u1",
		"ADD --checksum=sha256:05ee0b03119b45c0bdb4df654b96802e909e0a752f72e4fe3794f487229e5a41",
		"https://ffmpeg.org/releases/ffmpeg-8.0.1.tar.xz",
	} {
		if !strings.Contains(dockerfile, required) {
			t.Fatalf("Vela image Dockerfile is missing %q", required)
		}
	}

	for _, expected := range []struct {
		name       string
		entrypoint string
		required   []string
	}{
		{
			name:       "vela-control",
			entrypoint: "/usr/local/bin/vela-control",
			required: []string{
				"/usr/local/bin/vela-artifact-validator",
				"/usr/bin/ffprobe",
			},
		},
		{name: "vela-fleet-controller", entrypoint: "/usr/local/bin/vela-fleet-controller"},
		{
			name:       "vela-h3-stage-runtime",
			entrypoint: "/usr/local/bin/vela-model-runtime",
			required: []string{
				"vela.ai.h3-runtime-base=\"${H3_RUNTIME_BASE}\"",
				"vela.ai.h3-encoder.sha256=\"${H3_ENCODER_SHA256}\"",
				"vela.ai.h3-dit.sha256=\"${H3_DIT_SHA256}\"",
				"vela.ai.h3-vae-decoder.sha256=\"${H3_VAE_DECODER_SHA256}\"",
				"/opt/vela/bin/h3-encoder",
				"/opt/vela/bin/h3-dit",
				"/opt/vela/bin/h3-vae-decoder",
				"CMD []",
			},
		},
		{name: "vela-stage-worker-agent", entrypoint: "/usr/local/bin/vela-stage-worker-agent"},
	} {
		stage := finalDockerfileStage(t, dockerfile, expected.name)
		for _, required := range append([]string{
			"org.opencontainers.image.revision=\"${RELEASE_REVISION}\"",
			"USER 10001:10001",
			"ENTRYPOINT [\"" + expected.entrypoint + "\"]",
		}, expected.required...) {
			if !strings.Contains(stage, required) {
				t.Fatalf("Dockerfile stage %s is missing %q", expected.name, required)
			}
		}
	}
}

type hostPackageManifest struct {
	SchemaVersion int                          `json:"schema_version"`
	Revision      string                       `json:"revision"`
	Packages      []releasebundle.PackageInput `json:"packages"`
}

func loadHostPackageManifest(t *testing.T, path string) hostPackageManifest {
	t.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read host package manifest: %v", err)
	}
	var manifest hostPackageManifest
	if err := json.Unmarshal(encoded, &manifest); err != nil {
		t.Fatalf("decode host package manifest: %v", err)
	}
	return manifest
}

func loadPackageContract(t *testing.T, path string) releasebundle.PackageContract {
	t.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read package contract: %v", err)
	}
	var contract releasebundle.PackageContract
	if err := json.Unmarshal(encoded, &contract); err != nil {
		t.Fatalf("decode package contract: %v", err)
	}
	return contract
}

func assertLinuxAMD64NodeAgent(t *testing.T, path string) {
	t.Helper()
	information, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat Node Agent: %v", err)
	}
	if information.Mode().Perm() != 0o755 {
		t.Fatalf("Node Agent mode = %04o, want 0755", information.Mode().Perm())
	}
	binary, err := elf.Open(path)
	if err != nil {
		t.Fatalf("open Node Agent ELF: %v", err)
	}
	defer func() { _ = binary.Close() }()
	if binary.Class != elf.ELFCLASS64 || binary.Machine != elf.EM_X86_64 {
		t.Fatalf("Node Agent platform = class %s machine %s", binary.Class, binary.Machine)
	}
}

func deploymentRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve deployment contract source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func canonicalTemporaryDirectory(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temporary directory: %v", err)
	}
	return directory
}

func runHostPackageMake(repository, revision, output string) ([]byte, error) {
	command := exec.Command("make", "-s", "build-host-packages")
	command.Dir = repository
	command.Env = append(os.Environ(),
		"RELEASE_REVISION="+revision,
		"RELEASE_ARTIFACT_DIR="+output,
	)
	return command.CombinedOutput()
}

func runPrintVelaImageBuild(
	repository, revision, imagePrefix, runtimeBase, commandContext string,
	commandDigests map[string]string,
) ([]byte, error) {
	command := exec.Command("make", "-s", "--no-print-directory", "print-vela-image-build")
	command.Dir = repository
	command.Env = append(os.Environ(),
		"RELEASE_REVISION="+revision,
		"RELEASE_IMAGE_PREFIX="+imagePrefix,
		"H3_RUNTIME_BASE="+runtimeBase,
		"H3_RUNTIME_COMMAND_CONTEXT="+commandContext,
		"H3_ENCODER_SHA256="+commandDigests["h3-encoder"],
		"H3_DIT_SHA256="+commandDigests["h3-dit"],
		"H3_VAE_DECODER_SHA256="+commandDigests["h3-vae-decoder"],
		"GO_BASE=docker.io/library/golang:latest",
		"DEBIAN_BASE=docker.io/library/debian:latest",
	)
	return command.CombinedOutput()
}

func newH3RuntimeCommandFixture(t *testing.T) (string, map[string]string) {
	t.Helper()
	directory := filepath.Join(canonicalTemporaryDirectory(t), "h3-runtime-commands")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("create H3 runtime command fixture: %v", err)
	}
	digests := make(map[string]string, 3)
	for _, name := range []string{"h3-encoder", "h3-dit", "h3-vae-decoder"} {
		digests[name] = writeTestELF64AMD64(t, filepath.Join(directory, name))
	}
	return directory, digests
}

func writeTestELF64AMD64(t *testing.T, path string) string {
	t.Helper()
	const codeOffset = 64 + 56
	binaryImage := testELF64AMD64Header(1)
	entry := uint64(0x400000 + codeOffset)
	binary.LittleEndian.PutUint64(binaryImage[24:32], entry)
	binaryImage = append(binaryImage, make([]byte, 56)...)
	binary.LittleEndian.PutUint32(binaryImage[64:68], uint32(elf.PT_LOAD))
	binary.LittleEndian.PutUint32(binaryImage[68:72], uint32(elf.PF_R|elf.PF_X))
	binary.LittleEndian.PutUint64(binaryImage[80:88], 0x400000)
	binary.LittleEndian.PutUint64(binaryImage[88:96], 0x400000)
	binaryImage = append(binaryImage, []byte{0xb8, 0x3c, 0, 0, 0, 0x31, 0xff, 0x0f, 0x05}...)
	binary.LittleEndian.PutUint64(binaryImage[96:104], uint64(len(binaryImage)))
	binary.LittleEndian.PutUint64(binaryImage[104:112], uint64(len(binaryImage)))
	binary.LittleEndian.PutUint64(binaryImage[112:120], 0x1000)
	if err := os.WriteFile(path, binaryImage, 0o755); err != nil {
		t.Fatalf("write test ELF64 x86-64 backend: %v", err)
	}
	digest := sha256.Sum256(binaryImage)
	return hex.EncodeToString(digest[:])
}

func testELF64AMD64Header(programHeaders uint16) []byte {
	header := make([]byte, 64)
	copy(header, []byte{0x7f, 'E', 'L', 'F', byte(elf.ELFCLASS64), byte(elf.ELFDATA2LSB), 1})
	binary.LittleEndian.PutUint16(header[16:18], uint16(elf.ET_EXEC))
	binary.LittleEndian.PutUint16(header[18:20], uint16(elf.EM_X86_64))
	binary.LittleEndian.PutUint32(header[20:24], 1)
	binary.LittleEndian.PutUint64(header[32:40], 64)
	binary.LittleEndian.PutUint16(header[52:54], 64)
	binary.LittleEndian.PutUint16(header[54:56], 56)
	binary.LittleEndian.PutUint16(header[56:58], programHeaders)
	binary.LittleEndian.PutUint16(header[58:60], 64)
	return header
}

func environmentWithout(environment []string, excluded string) []string {
	filtered := make([]string, 0, len(environment))
	for _, entry := range environment {
		key, _, _ := strings.Cut(entry, "=")
		if key != excluded {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func finalDockerfileStage(t *testing.T, dockerfile, name string) string {
	t.Helper()
	startMarker := " AS " + name + "\n"
	start := strings.Index(dockerfile, startMarker)
	if start < 0 {
		t.Fatalf("Dockerfile stage %s is absent", name)
	}
	start += len(startMarker)
	remaining := dockerfile[start:]
	if end := strings.Index(remaining, "\nFROM "); end >= 0 {
		remaining = remaining[:end]
	}
	return remaining
}

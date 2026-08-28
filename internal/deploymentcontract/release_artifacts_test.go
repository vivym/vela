package deploymentcontract

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"encoding/json"
	"io"
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
			{Name: "h3-runner", ContractRef: "h3-runner-contract.json", ArtifactRef: "vela_h3_runner-0.1.0-py3-none-any.whl"},
			{Name: "node-agent", ContractRef: "node-agent-contract.json", ArtifactRef: "vela-node-agent"},
		}) {
		t.Fatalf("host package manifest = %#v", manifest)
	}

	expectedEntrypoints := map[string]string{
		"h3-runner":  "/opt/vela/bin/vela-h3-runner",
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
	assertRunnerWheel(t, filepath.Join(output, "vela_h3_runner-0.1.0-py3-none-any.whl"))
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

	const expectedInventory = "h3-runner-contract.json\nhost-packages.json\nnode-agent-contract.json\nvela-node-agent\nvela_h3_runner-0.1.0-py3-none-any.whl\n"
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
	fakeUV := `#!/bin/sh
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--out-dir" ]; then
    shift
    printf 'not a wheel\n' > "$1/vela_h3_runner-0.1.0-py3-none-any.whl"
    exit 0
  fi
  shift
done
exit 2
`
	if err := os.WriteFile(filepath.Join(fakeBin, "uv"), []byte(fakeUV), 0o700); err != nil {
		t.Fatalf("write fake uv: %v", err)
	}
	t.Setenv("PATH", fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"))
	output := filepath.Join(temporary, "artifacts")
	if err := releaseartifacts.BuildHostPackages(
		context.Background(), repository, "release-test-r1", output,
	); err == nil {
		t.Fatal("host package build published an invalid Runner wheel")
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

func assertRunnerWheel(t *testing.T, path string) {
	t.Helper()
	wheel, err := zip.OpenReader(path)
	if err != nil {
		t.Fatalf("open Runner wheel: %v", err)
	}
	defer func() { _ = wheel.Close() }()
	names := make([]string, 0, len(wheel.File))
	for _, file := range wheel.File {
		names = append(names, file.Name)
	}
	slices.Sort(names)
	for _, required := range []string{
		"vela/v1/runner_pb2.py",
		"vela/v1/runner_pb2_grpc.py",
		"vela_h3_runner/main.py",
		"vela_h3_runner/runtime.py",
		"vela_h3_runner/server.py",
	} {
		if !slices.Contains(names, required) {
			t.Fatalf("Runner wheel is missing %q: %v", required, names)
		}
	}
	entryPoints := readWheelFile(t, wheel.File, "vela_h3_runner-0.1.0.dist-info/entry_points.txt")
	if !strings.Contains(entryPoints, "vela-h3-runner = vela_h3_runner.main:main") {
		t.Fatalf("Runner wheel entry points = %q", entryPoints)
	}
}

func readWheelFile(t *testing.T, files []*zip.File, name string) string {
	t.Helper()
	for _, file := range files {
		if file.Name != name {
			continue
		}
		reader, err := file.Open()
		if err != nil {
			t.Fatalf("open Runner wheel file %q: %v", name, err)
		}
		defer func() { _ = reader.Close() }()
		content, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("read Runner wheel file %q: %v", name, err)
		}
		return string(content)
	}
	t.Fatalf("Runner wheel is missing %q", name)
	return ""
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

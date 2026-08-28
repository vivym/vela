package deploymentcontract

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/binary"
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

func TestPrintVelaImageBuildDefinesExactPinnedTargets(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	backendContext := canonicalTemporaryDirectory(t)
	const (
		revision    = "release-test-r2"
		imagePrefix = "registry.example.com/vela"
	)
	backendSHA := writeTestELF64AMD64(t, filepath.Join(backendContext, "h3-backend"))
	encoded, err := runPrintVelaImageBuild(
		repository, revision, imagePrefix, backendContext, backendSHA,
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
		"vela-control", "vela-fleet-controller", "vela-h3-runner", "vela-worker-agent",
	}
	slices.Sort(definition.Group["vela-all"].Targets)
	if !slices.Equal(definition.Group["vela-all"].Targets, expectedTargets) {
		t.Fatalf("Vela image group targets = %v", definition.Group["vela-all"].Targets)
	}
	const (
		goBase     = "docker.io/library/golang:1.26.7-bookworm@sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2"
		pythonBase = "docker.io/library/python:3.13.11-slim-bookworm@sha256:20080e807bfc404f8450b185cf0fc95d553462673598549613735f70a5b4d5d0"
		debianBase = "docker.io/library/debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171"
		uvBase     = "ghcr.io/astral-sh/uv:0.8.22@sha256:9874eb7afe5ca16c363fe80b294fe700e460df29a55532bbfea234a0f12eddb1"
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
			target.Args["PYTHON_BASE"] != pythonBase || target.Args["DEBIAN_BASE"] != debianBase ||
			target.Args["UV_BASE"] != uvBase {
			t.Fatalf("Vela image target %q = %#v", name, target)
		}
	}
	runner := definition.Target["vela-h3-runner"]
	if runner.Contexts["h3_backend"] != backendContext ||
		runner.Args["H3_BACKEND_SHA256"] != backendSHA {
		t.Fatalf("H3 Runner external backend inputs = %#v", runner)
	}
}

func TestPrintVelaImageBuildRequiresExplicitImagePrefix(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	backendContext := canonicalTemporaryDirectory(t)
	backendSHA := writeTestELF64AMD64(t, filepath.Join(backendContext, "h3-backend"))
	command := exec.Command("make", "-s", "print-vela-image-build")
	command.Dir = repository
	command.Env = append(environmentWithout(os.Environ(), "RELEASE_IMAGE_PREFIX"),
		"RELEASE_REVISION=release-test-r2",
		"H3_BACKEND_CONTEXT="+backendContext,
		"H3_BACKEND_SHA256="+backendSHA,
	)
	if encoded, err := command.CombinedOutput(); err == nil {
		t.Fatalf("print image build without image prefix unexpectedly succeeded:\n%s", encoded)
	}
}

func TestPrintVelaImageBuildRejectsInvalidH3BackendInputs(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	const validSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for _, test := range []struct {
		name    string
		prepare func(t *testing.T) (string, string)
	}{
		{
			name: "missing context",
			prepare: func(t *testing.T) (string, string) {
				t.Helper()
				return filepath.Join(canonicalTemporaryDirectory(t), "absent"), validSHA
			},
		},
		{
			name: "missing backend",
			prepare: func(t *testing.T) (string, string) {
				t.Helper()
				return canonicalTemporaryDirectory(t), validSHA
			},
		},
		{
			name: "wrong digest",
			prepare: func(t *testing.T) (string, string) {
				t.Helper()
				context := canonicalTemporaryDirectory(t)
				writeTestELF64AMD64(t, filepath.Join(context, "h3-backend"))
				return context, validSHA
			},
		},
		{
			name: "non ELF backend",
			prepare: func(t *testing.T) (string, string) {
				t.Helper()
				context := canonicalTemporaryDirectory(t)
				backend := filepath.Join(context, "h3-backend")
				if err := os.WriteFile(backend, []byte("not an ELF binary\n"), 0o755); err != nil {
					t.Fatalf("write non-ELF backend: %v", err)
				}
				digest := sha256.Sum256([]byte("not an ELF binary\n"))
				return context, hex.EncodeToString(digest[:])
			},
		},
		{
			name: "non executable backend",
			prepare: func(t *testing.T) (string, string) {
				t.Helper()
				context := canonicalTemporaryDirectory(t)
				backend := filepath.Join(context, "h3-backend")
				digest := writeTestELF64AMD64(t, backend)
				if err := os.Chmod(backend, 0o644); err != nil {
					t.Fatalf("remove backend execute permission: %v", err)
				}
				return context, digest
			},
		},
		{
			name: "header only backend",
			prepare: func(t *testing.T) (string, string) {
				t.Helper()
				context := canonicalTemporaryDirectory(t)
				backend := filepath.Join(context, "h3-backend")
				header := testELF64AMD64Header(0)
				if err := os.WriteFile(backend, header, 0o755); err != nil {
					t.Fatalf("write header-only backend: %v", err)
				}
				digest := sha256.Sum256(header)
				return context, hex.EncodeToString(digest[:])
			},
		},
		{
			name: "symlink backend",
			prepare: func(t *testing.T) (string, string) {
				t.Helper()
				context := canonicalTemporaryDirectory(t)
				target := filepath.Join(context, "target")
				digest := writeTestELF64AMD64(t, target)
				if err := os.Symlink(target, filepath.Join(context, "h3-backend")); err != nil {
					t.Fatalf("create backend symlink: %v", err)
				}
				return context, digest
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			context, digest := test.prepare(t)
			if encoded, err := runPrintVelaImageBuild(
				repository, "release-test-r2", "registry.example.com/vela", context, digest,
			); err == nil {
				t.Fatalf("print image build with invalid H3 backend unexpectedly succeeded:\n%s", encoded)
			}
		})
	}
}

func TestBuildVelaImagesStagesVerifiedPrivateBackendContext(t *testing.T) {
	repository := deploymentRepositoryRoot(t)
	backendContext := canonicalTemporaryDirectory(t)
	backend := filepath.Join(backendContext, "h3-backend")
	backendSHA := writeTestELF64AMD64(t, backend)
	temporary := canonicalTemporaryDirectory(t)
	fakeBin := filepath.Join(temporary, "bin")
	if err := os.Mkdir(fakeBin, 0o700); err != nil {
		t.Fatalf("create fake Docker directory: %v", err)
	}
	capture := filepath.Join(temporary, "staged-context")
	fakeDocker := `#!/bin/sh
set -eu
printf '%s\n' "$H3_BACKEND_CONTEXT" > "$VELA_TEST_STAGED_CONTEXT_CAPTURE"
test "$H3_BACKEND_CONTEXT" != "$VELA_TEST_SOURCE_CONTEXT"
test "$(find "$H3_BACKEND_CONTEXT" -mindepth 1 -maxdepth 1 -type f -name h3-backend | wc -l | tr -d ' ')" = 1
test "$(find "$H3_BACKEND_CONTEXT" -mindepth 1 -maxdepth 1 | wc -l | tr -d ' ')" = 1
cmp "$VELA_TEST_SOURCE_CONTEXT/h3-backend" "$H3_BACKEND_CONTEXT/h3-backend"
`
	if err := os.WriteFile(filepath.Join(fakeBin, "docker"), []byte(fakeDocker), 0o700); err != nil {
		t.Fatalf("write fake Docker: %v", err)
	}
	command := exec.Command("make", "-s", "build-vela-images")
	command.Dir = repository
	command.Env = append(os.Environ(),
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"RELEASE_REVISION=release-test-r2",
		"RELEASE_IMAGE_PREFIX=registry.example.com/vela",
		"H3_BACKEND_CONTEXT="+backendContext,
		"H3_BACKEND_SHA256="+backendSHA,
		"VELA_TEST_SOURCE_CONTEXT="+backendContext,
		"VELA_TEST_STAGED_CONTEXT_CAPTURE="+capture,
	)
	if encoded, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build Vela images through staged backend context: %v\n%s", err, encoded)
	}
	staged, err := os.ReadFile(capture)
	if err != nil {
		t.Fatalf("read staged backend context capture: %v", err)
	}
	stagedPath := strings.TrimSpace(string(staged))
	if stagedPath == "" || stagedPath == backendContext {
		t.Fatalf("staged backend context = %q", stagedPath)
	}
	if _, err := os.Lstat(stagedPath); !os.IsNotExist(err) {
		t.Fatalf("staged backend context remains after build: %v", err)
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
		"/usr/local/bin/vela-release-artifacts verify-h3-backend \\",
		"/backend-context \"${H3_BACKEND_SHA256}\"",
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
		{name: "vela-worker-agent", entrypoint: "/usr/local/bin/vela-worker-agent"},
		{
			name:       "vela-h3-runner",
			entrypoint: "/opt/vela/venv/bin/vela-h3-runner",
			required:   []string{"/opt/vela/bin/h3-backend"},
		},
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

func runPrintVelaImageBuild(
	repository, revision, imagePrefix, backendContext, backendSHA string,
) ([]byte, error) {
	command := exec.Command("make", "-s", "print-vela-image-build")
	command.Dir = repository
	command.Env = append(os.Environ(),
		"RELEASE_REVISION="+revision,
		"RELEASE_IMAGE_PREFIX="+imagePrefix,
		"H3_BACKEND_CONTEXT="+backendContext,
		"H3_BACKEND_SHA256="+backendSHA,
		"GO_BASE=docker.io/library/golang:latest",
		"PYTHON_BASE=docker.io/library/python:latest",
		"DEBIAN_BASE=docker.io/library/debian:latest",
		"UV_BASE=ghcr.io/astral-sh/uv:latest",
	)
	return command.CombinedOutput()
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

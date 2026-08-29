package releaseartifacts

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/vivym/vela/internal/releasebundle"
	"github.com/vivym/vela/internal/strictjson"
)

const (
	hostPackageSchemaVersion = 1
	runnerArtifactName       = "vela_h3_runner-0.1.0-py3-none-any.whl"
	nodeAgentArtifactName    = "vela-node-agent"
	minimumSourceDateEpoch   = "315532800"
	maximumMetadataBytes     = 64 << 10
	runnerEntrypoint         = "/opt/vela/bin/vela-h3-runner"
	nodeAgentEntrypoint      = "/usr/local/bin/vela-node-agent"
)

type hostPackageManifest struct {
	SchemaVersion int                          `json:"schema_version"`
	Revision      string                       `json:"revision"`
	Packages      []releasebundle.PackageInput `json:"packages"`
}

type hostPackageSpec struct {
	input      releasebundle.PackageInput
	entrypoint string
}

func BuildHostPackages(ctx context.Context, sourceRoot, revision, outputDirectory string) error {
	if ctx == nil {
		return errors.New("host package build context is required")
	}
	sourceRoot, err := canonicalExistingDirectory(sourceRoot)
	if err != nil {
		return fmt.Errorf("resolve source root: %w", err)
	}
	outputDirectory, parent, err := canonicalNewOutputDirectory(outputDirectory)
	if err != nil {
		return fmt.Errorf("resolve output directory: %w", err)
	}
	if !releasebundle.ValidRevision(revision) {
		return errors.New("release revision is invalid")
	}

	candidate, err := os.MkdirTemp(parent, ".vela-host-packages-*")
	if err != nil {
		return fmt.Errorf("create host package candidate: %w", err)
	}
	defer func() { _ = os.RemoveAll(candidate) }()
	if err := os.Chmod(candidate, 0o700); err != nil {
		return fmt.Errorf("protect host package candidate: %w", err)
	}

	if err := buildNodeAgent(ctx, sourceRoot, filepath.Join(candidate, nodeAgentArtifactName)); err != nil {
		return err
	}
	if err := buildRunnerWheel(ctx, sourceRoot, candidate); err != nil {
		return err
	}
	specifications := hostPackageSpecifications()
	packages := make([]releasebundle.PackageInput, 0, len(specifications))
	for _, item := range specifications {
		packages = append(packages, item.input)
		artifactPath := filepath.Join(candidate, item.input.ArtifactRef)
		digest, size, err := digestFile(artifactPath)
		if err != nil {
			return fmt.Errorf("digest %s package: %w", item.input.Name, err)
		}
		contract := releasebundle.PackageContract{
			SchemaVersion:     hostPackageSchemaVersion,
			Name:              "vela-" + item.input.Name,
			OS:                "linux",
			Architecture:      "amd64",
			Revision:          revision,
			Entrypoint:        item.entrypoint,
			ArtifactDigest:    "sha256:" + digest,
			ArtifactSizeBytes: size,
		}
		if err := writeJSONFile(filepath.Join(candidate, item.input.ContractRef), contract); err != nil {
			return fmt.Errorf("write %s package contract: %w", item.input.Name, err)
		}
	}
	manifest := hostPackageManifest{
		SchemaVersion: hostPackageSchemaVersion,
		Revision:      revision,
		Packages:      packages,
	}
	if err := writeJSONFile(filepath.Join(candidate, "host-packages.json"), manifest); err != nil {
		return fmt.Errorf("write host package manifest: %w", err)
	}
	if err := verifyHostPackageCandidate(candidate, revision); err != nil {
		return fmt.Errorf("verify host package candidate: %w", err)
	}
	if err := syncDirectory(candidate); err != nil {
		return fmt.Errorf("sync host package candidate: %w", err)
	}
	if err := renameNoReplace(candidate, outputDirectory); err != nil {
		return fmt.Errorf("publish host packages: %w", err)
	}
	if err := syncDirectory(parent); err != nil {
		return fmt.Errorf("sync host package parent: %w", err)
	}
	return nil
}

func hostPackageSpecifications() [2]hostPackageSpec {
	return [2]hostPackageSpec{
		{
			input: releasebundle.PackageInput{
				Name: "h3-runner", ContractRef: "h3-runner-contract.json", ArtifactRef: runnerArtifactName,
			},
			entrypoint: runnerEntrypoint,
		},
		{
			input: releasebundle.PackageInput{
				Name: "node-agent", ContractRef: "node-agent-contract.json", ArtifactRef: nodeAgentArtifactName,
			},
			entrypoint: nodeAgentEntrypoint,
		},
	}
}

func buildNodeAgent(ctx context.Context, sourceRoot, output string) error {
	command := exec.CommandContext(
		ctx,
		"go",
		"build",
		"-mod=readonly",
		"-trimpath",
		"-buildvcs=false",
		"-ldflags=-buildid= -s -w",
		"-o",
		output,
		"./cmd/vela-node-agent",
	)
	command.Dir = sourceRoot
	command.Env = buildEnvironment(map[string]string{
		"CGO_ENABLED": "0",
		"GOARCH":      "amd64",
		"GOOS":        "linux",
	})
	if encoded, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("build linux/amd64 Node Agent: %w: %s", err, strings.TrimSpace(string(encoded)))
	}
	if err := os.Chmod(output, 0o755); err != nil {
		return fmt.Errorf("set Node Agent package mode: %w", err)
	}
	return syncFile(output)
}

func buildRunnerWheel(ctx context.Context, sourceRoot, outputDirectory string) error {
	command := exec.CommandContext(
		ctx,
		"uv",
		"build",
		"--wheel",
		"--no-build-logs",
		"--no-sources",
		"--out-dir",
		outputDirectory,
		filepath.Join(sourceRoot, "runner"),
	)
	command.Dir = sourceRoot
	command.Env = buildEnvironment(map[string]string{
		"SOURCE_DATE_EPOCH": minimumSourceDateEpoch,
		"UV_NO_PROGRESS":    "true",
	})
	if encoded, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("build H3 Runner wheel: %w: %s", err, strings.TrimSpace(string(encoded)))
	}
	if err := os.Remove(filepath.Join(outputDirectory, ".gitignore")); err != nil {
		return fmt.Errorf("remove uv build metadata: %w", err)
	}
	wheel := filepath.Join(outputDirectory, runnerArtifactName)
	information, err := os.Stat(wheel)
	if err != nil || !information.Mode().IsRegular() || information.Size() <= 0 {
		return errors.New("H3 Runner build did not produce the exact release wheel")
	}
	if err := os.Chmod(wheel, 0o644); err != nil {
		return fmt.Errorf("set H3 Runner package mode: %w", err)
	}
	return syncFile(wheel)
}

func canonicalExistingDirectory(name string) (string, error) {
	absolute, err := filepath.Abs(name)
	if err != nil || filepath.Clean(name) != name {
		return "", errors.New("path must be canonical")
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	if resolved != absolute {
		return "", errors.New("path must not contain symbolic links")
	}
	information, err := os.Stat(resolved)
	if err != nil || !information.IsDir() {
		return "", errors.New("path must identify a directory")
	}
	return resolved, nil
}

func canonicalNewOutputDirectory(name string) (string, string, error) {
	if name == "" || !filepath.IsAbs(name) || filepath.Clean(name) != name {
		return "", "", errors.New("output path must be canonical and absolute")
	}
	if _, err := os.Lstat(name); err == nil {
		return "", "", errors.New("output path already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", "", err
	}
	requestedParent := filepath.Dir(name)
	information, err := os.Lstat(requestedParent)
	if err != nil {
		return "", "", err
	}
	if information.Mode()&os.ModeSymlink != 0 {
		return "", "", errors.New("output parent must not be a symbolic link")
	}
	parent, err := canonicalExistingDirectory(requestedParent)
	if err != nil {
		return "", "", fmt.Errorf("resolve output parent: %w", err)
	}
	return filepath.Join(parent, filepath.Base(name)), parent, nil
}

func buildEnvironment(overrides map[string]string) []string {
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	blocked := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		blocked[key] = struct{}{}
	}
	environment := make([]string, 0, len(os.Environ())+len(keys))
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, replace := blocked[key]; !replace {
			environment = append(environment, entry)
		}
	}
	for _, key := range keys {
		environment = append(environment, key+"="+overrides[key])
	}
	return environment
}

func digestFile(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = file.Close() }()
	digest := sha256.New()
	size, err := io.Copy(digest, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(digest.Sum(nil)), size, nil
}

func writeJSONFile(path string, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	return writeExactFile(path, encoded)
}

func verifyHostPackageCandidate(directory, revision string) error {
	specifications := hostPackageSpecifications()
	expectedPackages := make([]releasebundle.PackageInput, 0, len(specifications))
	expectedInventory := []string{"host-packages.json"}
	for _, item := range specifications {
		expectedPackages = append(expectedPackages, item.input)
		expectedInventory = append(expectedInventory, item.input.ContractRef, item.input.ArtifactRef)
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

	manifestEncoded, err := readRegularMetadata(filepath.Join(directory, "host-packages.json"))
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	var manifest hostPackageManifest
	if err := decodeStrictJSON(manifestEncoded, &manifest); err != nil {
		return fmt.Errorf("decode manifest: %w", err)
	}
	if manifest.SchemaVersion != hostPackageSchemaVersion || manifest.Revision != revision ||
		!slices.Equal(manifest.Packages, expectedPackages) {
		return errors.New("manifest does not bind the exact host package set")
	}

	for _, item := range specifications {
		artifactPath := filepath.Join(directory, item.input.ArtifactRef)
		if err := requireRegularFile(artifactPath); err != nil {
			return fmt.Errorf("validate %s artifact: %w", item.input.Name, err)
		}
		digest, size, err := digestFile(artifactPath)
		if err != nil {
			return fmt.Errorf("digest %s artifact: %w", item.input.Name, err)
		}
		contractEncoded, err := readRegularMetadata(filepath.Join(directory, item.input.ContractRef))
		if err != nil {
			return fmt.Errorf("read %s contract: %w", item.input.Name, err)
		}
		contract, err := releasebundle.ValidatePackageContract(
			item.input.Name,
			contractEncoded,
			releasebundle.Artifact{Digest: "sha256:" + digest, SizeBytes: size},
		)
		if err != nil {
			return err
		}
		if contract.Revision != revision || contract.Entrypoint != item.entrypoint {
			return fmt.Errorf("%s contract does not bind the requested revision and entrypoint", item.input.Name)
		}
	}
	if err := verifyNodeAgentArtifact(filepath.Join(directory, nodeAgentArtifactName)); err != nil {
		return err
	}
	return verifyRunnerArtifact(filepath.Join(directory, runnerArtifactName))
}

func readRegularMetadata(name string) ([]byte, error) {
	information, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !information.Mode().IsRegular() || information.Size() <= 0 || information.Size() > maximumMetadataBytes {
		return nil, errors.New("metadata must be a bounded regular file")
	}
	return os.ReadFile(name)
}

func requireRegularFile(name string) error {
	information, err := os.Lstat(name)
	if err != nil {
		return err
	}
	if !information.Mode().IsRegular() || information.Size() <= 0 {
		return errors.New("artifact must be a non-empty regular file")
	}
	return nil
}

func decodeStrictJSON(encoded []byte, destination any) error {
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

func verifyNodeAgentArtifact(name string) error {
	information, err := os.Lstat(name)
	if err != nil {
		return fmt.Errorf("stat Node Agent: %w", err)
	}
	if information.Mode().Perm() != 0o755 {
		return errors.New("node Agent mode must be 0755")
	}
	binary, err := elf.Open(name)
	if err != nil {
		return fmt.Errorf("open Node Agent ELF: %w", err)
	}
	defer func() { _ = binary.Close() }()
	if binary.Class != elf.ELFCLASS64 || binary.Machine != elf.EM_X86_64 {
		return errors.New("node Agent must be a linux/amd64 ELF64 binary")
	}
	return nil
}

func verifyRunnerArtifact(name string) error {
	information, err := os.Lstat(name)
	if err != nil {
		return fmt.Errorf("stat Runner wheel: %w", err)
	}
	if information.Mode().Perm() != 0o644 {
		return errors.New("runner wheel mode must be 0644")
	}
	wheel, err := zip.OpenReader(name)
	if err != nil {
		return fmt.Errorf("open Runner wheel: %w", err)
	}
	defer func() { _ = wheel.Close() }()
	required := map[string]bool{
		"vela/v1/runner_pb2.py":                           false,
		"vela/v1/runner_pb2_grpc.py":                      false,
		"vela_h3_runner/main.py":                          false,
		"vela_h3_runner/runtime.py":                       false,
		"vela_h3_runner/server.py":                        false,
		"vela_h3_runner-0.1.0.dist-info/entry_points.txt": false,
	}
	seen := make(map[string]struct{}, len(wheel.File))
	for _, file := range wheel.File {
		trimmed := strings.TrimSuffix(file.Name, "/")
		if trimmed == "" || path.IsAbs(trimmed) || path.Clean(trimmed) != trimmed ||
			strings.Contains(trimmed, `\`) || file.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("runner wheel member %q is unsafe", file.Name)
		}
		if _, duplicate := seen[file.Name]; duplicate {
			return fmt.Errorf("runner wheel member %q is duplicated", file.Name)
		}
		seen[file.Name] = struct{}{}
		if _, needed := required[file.Name]; needed {
			required[file.Name] = true
		}
	}
	for member, present := range required {
		if !present {
			return fmt.Errorf("runner wheel is missing %q", member)
		}
	}
	entryPoints, err := readWheelMember(wheel.File, "vela_h3_runner-0.1.0.dist-info/entry_points.txt")
	if err != nil {
		return err
	}
	if entryPoints != "[console_scripts]\nvela-h3-runner = vela_h3_runner.main:main\n" {
		return errors.New("runner wheel console entrypoint is invalid")
	}
	return nil
}

func readWheelMember(files []*zip.File, name string) (string, error) {
	for _, file := range files {
		if file.Name != name {
			continue
		}
		if file.UncompressedSize64 > maximumMetadataBytes {
			return "", errors.New("runner wheel metadata is too large")
		}
		reader, err := file.Open()
		if err != nil {
			return "", err
		}
		encoded, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil {
			return "", readErr
		}
		if closeErr != nil {
			return "", closeErr
		}
		return string(encoded), nil
	}
	return "", fmt.Errorf("runner wheel is missing %q", name)
}

func syncFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return file.Sync()
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}

package h3stagemock

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestRetiredAuthorityBoundAdmitsFinalRetirementThenFailsClosed(t *testing.T) {
	const profile = "49000000-0000-0000-0000-000000000005"
	runtime := &session{
		component: "ENCODER",
		initialization: &initializeV1{
			StageProfileRevisionID: profile,
		},
		retiredAuthorities: make(map[string]struct{}, maximumRetiredAuthorities),
	}
	for index := 1; index < maximumRetiredAuthorities; index++ {
		runtime.retiredAuthorities[retiredDigest(index)] = struct{}{}
	}
	first := internalStageIdentity("1", strings.Repeat("a", 64), profile)
	runtime.active = &execution{identity: first, state: stateOutputSealed}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(&velav1.StageExecutionSpec{
		ParametersJson:             []byte(`{"seed":17}`),
		ExpectedOutputManifestJson: []byte(`{"conditioning":true}`),
	})
	if err != nil {
		t.Fatalf("marshal execution spec: %v", err)
	}
	second := internalStageIdentity("2", strings.Repeat("b", 64), profile)
	if err := runtime.prepare(&prepareRequestV1{Identity: second, ExecutionSpec: encoded}); err != nil {
		t.Fatalf("admit final authority retirement: %v", err)
	}
	if len(runtime.retiredAuthorities) != maximumRetiredAuthorities ||
		!sameStageIdentity(runtime.active.identity, second) {
		t.Fatalf("retired=%d active=%#v", len(runtime.retiredAuthorities), runtime.active.identity)
	}
	runtime.active.state = stateOutputSealed
	third := internalStageIdentity("3", strings.Repeat("c", 64), profile)
	if err := runtime.prepare(&prepareRequestV1{Identity: third, ExecutionSpec: encoded}); err == nil || !strings.Contains(err.Error(), "bound is exhausted") {
		t.Fatalf("prepare beyond retirement bound error=%v", err)
	}
	if !sameStageIdentity(runtime.active.identity, second) {
		t.Fatalf("failed-closed prepare replaced active identity: %#v", runtime.active.identity)
	}
}

func TestBoundOutputRootCannotBeRedirectedByPathReplacement(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve test root: %v", err)
	}
	outputPath := filepath.Join(base, "output")
	outsidePath := filepath.Join(base, "outside")
	for _, path := range []string{outputPath, outsidePath} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatalf("create test directory: %v", err)
		}
	}
	root, err := openPrivateRoot(outputPath)
	if err != nil {
		t.Fatalf("bind output root: %v", err)
	}
	if err := ensurePrivateDirectory(root, ".staging"); err != nil {
		_ = root.Close()
		t.Fatalf("prepare staging directory: %v", err)
	}
	originalPath := outputPath + ".original"
	if err := os.Rename(outputPath, originalPath); err != nil {
		_ = root.Close()
		t.Fatalf("move bound output root: %v", err)
	}
	if err := os.Symlink(outsidePath, outputPath); err != nil {
		_ = root.Close()
		t.Fatalf("replace output path with symlink: %v", err)
	}

	identity := internalStageIdentity(
		"1", strings.Repeat("d", 64), "49000000-0000-0000-0000-000000000005",
	)
	execution := &execution{
		identity: identity, state: stateRunning,
		parametersDigest: strings.Repeat("e", 64),
	}
	runtime := &session{
		component: "ENCODER", outputRoot: root, active: execution,
		initialization: &initializeV1{OutputRoot: outputPath},
	}
	if err := runtime.publish(execution); err != nil {
		_ = root.Close()
		t.Fatalf("publish through bound output root: %v", err)
	}
	wantRelative := filepath.Join(identity.StageAttemptID, "conditioning.bin")
	want, err := os.ReadFile(filepath.Join(originalPath, wantRelative))
	if err != nil || len(want) == 0 {
		_ = runtime.close()
		t.Fatalf("read output from original root: size=%d error=%v", len(want), err)
	}
	if _, err := os.Lstat(filepath.Join(outsidePath, wantRelative)); !os.IsNotExist(err) {
		_ = runtime.close()
		t.Fatalf("replacement root received output: %v", err)
	}
	if err := runtime.close(); err != nil {
		t.Fatalf("close runtime: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(originalPath, wantRelative)); !os.IsNotExist(err) {
		t.Fatalf("unsealed output remained in original root: %v", err)
	}
}

func TestSubrootBindingRejectsReplacedScratchPath(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve test root: %v", err)
	}
	scratchPath := filepath.Join(base, "scratch")
	inputPath := filepath.Join(scratchPath, "inputs")
	for _, path := range []string{scratchPath, inputPath} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatalf("create original runtime root: %v", err)
		}
	}
	scratchRoot, err := openPrivateRoot(scratchPath)
	if err != nil {
		t.Fatalf("bind scratch root: %v", err)
	}
	defer func() { _ = scratchRoot.Close() }()

	originalPath := scratchPath + ".original"
	if err := os.Rename(scratchPath, originalPath); err != nil {
		t.Fatalf("move bound scratch root: %v", err)
	}
	if err := os.Mkdir(scratchPath, 0o700); err != nil {
		t.Fatalf("replace scratch root: %v", err)
	}
	if err := os.Mkdir(inputPath, 0o700); err != nil {
		t.Fatalf("create replacement input root: %v", err)
	}

	inputRoot, err := openPrivateSubroot(scratchRoot, scratchPath, inputPath)
	if inputRoot != nil {
		_ = inputRoot.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "changed while binding") {
		t.Fatalf("bind replacement input root error=%v", err)
	}
}

func retiredDigest(index int) string {
	encoded := strings.Repeat("0", 64)
	digits := []byte(encoded)
	for position := len(digits) - 1; index > 0; position-- {
		digits[position] = "0123456789abcdef"[index&15]
		index >>= 4
	}
	return string(digits)
}

func internalStageIdentity(suffix, digest, profile string) stageIdentityV1 {
	return stageIdentityV1{
		AuthorityDigest:        digest,
		JobID:                  "49100000-0000-0000-0000-000000000001",
		AttemptID:              "49200000-0000-0000-0000-000000000001",
		StageRunID:             "49300000-0000-0000-0000-00000000000" + suffix,
		StageAttemptID:         "49400000-0000-0000-0000-00000000000" + suffix,
		StageLeaseID:           "49500000-0000-0000-0000-00000000000" + suffix,
		AttemptFence:           2,
		StageFence:             3,
		StageVersion:           4,
		StageProfileRevisionID: profile,
	}
}

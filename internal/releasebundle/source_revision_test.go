package releasebundle

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var testSourceRevision = strings.Repeat("a", 40)

func buildTestBundle(planPath string) (Bundle, []byte, error) {
	return buildPlan(planPath, testSourceRevision)
}

func TestBuildFromSourceRequiresExactCleanGitToplevel(t *testing.T) {
	sourceRoot := newCleanGitSource(t)
	missingPlan := filepath.Join(t.TempDir(), "missing-plan.json")

	_, _, err := BuildFromSource(sourceRoot, missingPlan)
	if err == nil || !strings.Contains(err.Error(), "read build plan") {
		t.Fatalf("BuildFromSource(clean source, missing plan) error = %v", err)
	}

	writeTestFile(t, filepath.Join(sourceRoot, "untracked"), []byte("dirty"))
	_, _, err = BuildFromSource(sourceRoot, missingPlan)
	if err == nil || !strings.Contains(err.Error(), "source checkout is not clean") {
		t.Fatalf("BuildFromSource(dirty source) error = %v", err)
	}

	subdirectory := filepath.Join(sourceRoot, "subdirectory")
	if err := os.Mkdir(subdirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	_, _, err = BuildFromSource(subdirectory, missingPlan)
	if err == nil || !strings.Contains(err.Error(), "exact Git repository toplevel") {
		t.Fatalf("BuildFromSource(subdirectory) error = %v", err)
	}
}

func TestSchemaV2RejectsLegacyWorkerMaterializationFields(t *testing.T) {
	t.Run("build plan", func(t *testing.T) {
		directory := t.TempDir()
		planPath := filepath.Join(directory, "plan.json")
		writeTestFile(t, planPath, []byte(`{"schema_version":2,"worker_materializations":[]}`))
		_, _, err := buildTestBundle(planPath)
		if err == nil || !strings.Contains(err.Error(), `unknown field "worker_materializations"`) {
			t.Fatalf("build legacy plan error = %v", err)
		}
	})

	t.Run("configuration manifest", func(t *testing.T) {
		directory := t.TempDir()
		bundlePath := filepath.Join(directory, "bundle.json")
		writeTestFile(t, bundlePath, []byte(`{
			"schema_version":2,
			"configuration_manifest":{"worker_materializations":[]}
		}`))
		_, err := Load(bundlePath)
		if err == nil || !strings.Contains(err.Error(), `unknown field "worker_materializations"`) {
			t.Fatalf("load legacy manifest error = %v", err)
		}
	})
}

func newCleanGitSource(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatal(err)
	}
	directory = resolved
	writeTestFile(t, filepath.Join(directory, "source.txt"), []byte("source"))
	for _, arguments := range [][]string{
		{"init", "--quiet"},
		{"config", "user.name", "Vela Test"},
		{"config", "user.email", "vela-test@example.invalid"},
		{"add", "source.txt"},
		{"commit", "--quiet", "-m", "test source"},
	} {
		gitRun(t, directory, arguments...)
	}
	return directory
}

func gitHead(t *testing.T, directory string) string {
	t.Helper()
	return strings.TrimSpace(gitRun(t, directory, "rev-parse", "--verify", "HEAD^{commit}"))
}

func gitRun(t *testing.T, directory string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", arguments, err, output)
	}
	return string(output)
}

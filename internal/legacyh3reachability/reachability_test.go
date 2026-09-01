package legacyh3reachability

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/releasebundle"
)

func TestScanProducesPassOnlyForContractedSourceAndRelease(t *testing.T) {
	root := cleanStageSourceFixture(t)
	bundle := contractedBundleFixture()
	evidence, encoded, digest, err := scanVerified(
		root,
		bundle,
		strings.Repeat("d", 40),
		"ci/reachability",
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	)
	if err != nil || evidence.Result != ResultPass || len(evidence.Checks) != len(contract) ||
		len(encoded) == 0 || !digestPattern.MatchString(digest) {
		t.Fatalf("contracted reachability evidence = %#v digest=%q error=%v", evidence, digest, err)
	}
	path := filepath.Join(t.TempDir(), "reachability.json")
	if err := Write(path, encoded); err != nil {
		t.Fatalf("write reachability evidence: %v", err)
	}
	loaded, _, loadedDigest, err := Load(path, bundle)
	if err != nil || loaded.Result != ResultPass || loadedDigest != digest {
		t.Fatalf("load reachability evidence = %#v digest=%q error=%v", loaded, loadedDigest, err)
	}
}

func TestScanBindsCleanGitHEADToContractedRelease(t *testing.T) {
	root := cleanStageSourceFixture(t)
	runFixtureGit(t, root, "init", "--quiet")
	runFixtureGit(t, root, "config", "user.email", "reachability@example.invalid")
	runFixtureGit(t, root, "config", "user.name", "Reachability Test")
	runFixtureGit(t, root, "add", ".")
	runFixtureGit(t, root, "commit", "--quiet", "-m", "contracted source")
	revision := strings.TrimSpace(runFixtureGit(t, root, "rev-parse", "HEAD"))
	bundle := contractedBundleFixture()
	bundle.ConfigurationManifest.SourceRevision = revision

	if evidence, _, _, err := Scan(
		root,
		bundle,
		revision,
		"ci/reachability",
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	); err != nil || evidence.Result != ResultPass {
		t.Fatalf("scan clean release source = %#v error=%v", evidence, err)
	}
	mismatchedBundle := bundle
	mismatchedBundle.ConfigurationManifest.SourceRevision = strings.Repeat("e", 40)
	if _, _, _, err := Scan(
		root,
		mismatchedBundle,
		revision,
		"ci/reachability",
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	); err == nil {
		t.Fatal("release/source revision mismatch was accepted")
	}
	if _, _, _, err := Scan(
		filepath.Join(root, "internal"),
		bundle,
		revision,
		"ci/reachability",
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	); err == nil {
		t.Fatal("Git repository subdirectory was accepted as the source root")
	}
	writeFixture(t, root, "untracked.txt", "drift\n")
	if _, _, _, err := Scan(
		root,
		bundle,
		revision,
		"ci/reachability",
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	); err == nil {
		t.Fatal("dirty source checkout was accepted")
	}
}

func TestLoadRejectsForgedPassObservationsAndSymlinks(t *testing.T) {
	root := cleanStageSourceFixture(t)
	bundle := contractedBundleFixture()
	evidence, encoded, _, err := scanVerified(
		root,
		bundle,
		strings.Repeat("d", 40),
		"ci/reachability",
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("scan contracted source: %v", err)
	}
	evidence.Checks[len(evidence.Checks)-1].Matches = []string{"forged"}
	if err := ValidatePass(evidence, bundle); err == nil {
		t.Fatal("forged PASS observations were accepted")
	}

	directory := t.TempDir()
	evidencePath := filepath.Join(directory, "reachability.json")
	if err := Write(evidencePath, encoded); err != nil {
		t.Fatalf("write reachability evidence: %v", err)
	}
	if err := Write(evidencePath, encoded); err == nil {
		t.Fatal("reachability evidence was replaced")
	}
	linkedPath := filepath.Join(directory, "linked.json")
	if err := os.Symlink(evidencePath, linkedPath); err != nil {
		t.Fatalf("link reachability evidence: %v", err)
	}
	if _, _, _, err := Load(linkedPath, bundle); err == nil {
		t.Fatal("symlinked reachability evidence was accepted")
	}
}

func TestScanRejectsSymlinkedSourceSurface(t *testing.T) {
	root := cleanStageSourceFixture(t)
	stageRuntime := filepath.Join(root, "internal", "stageworkeragent")
	if err := os.RemoveAll(stageRuntime); err != nil {
		t.Fatalf("remove Stage Worker fixture: %v", err)
	}
	if err := os.Symlink(t.TempDir(), stageRuntime); err != nil {
		t.Fatalf("link Stage Worker fixture: %v", err)
	}
	if _, _, _, err := scanVerified(
		root,
		contractedBundleFixture(),
		strings.Repeat("d", 40),
		"ci/reachability",
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	); err == nil {
		t.Fatal("symlinked source surface was accepted")
	}
}

func TestScanRejectsNonUTCOrControlBearingBindings(t *testing.T) {
	root := cleanStageSourceFixture(t)
	bundle := contractedBundleFixture()
	for _, test := range []struct {
		name           string
		sourceRevision string
		observedBy     string
		observedAt     time.Time
	}{
		{
			name:           "non UTC",
			sourceRevision: strings.Repeat("d", 40),
			observedBy:     "ci/reachability",
			observedAt:     time.Date(2026, 9, 1, 8, 0, 0, 0, time.FixedZone("CST", 8*60*60)),
		},
		{
			name:           "control character",
			sourceRevision: "0123456789abcdef\tforged",
			observedBy:     "ci/reachability",
			observedAt:     time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, _, _, err := scanVerified(
				root,
				bundle,
				test.sourceRevision,
				test.observedBy,
				test.observedAt,
			); err == nil {
				t.Fatal("invalid reachability binding was accepted")
			}
		})
	}
}

func TestScanReportsLegacySourceAndReleaseSurfaces(t *testing.T) {
	root := cleanStageSourceFixture(t)
	writeFixture(t, root, "proto/vela/v1/runner.proto", "syntax = \"proto3\";")
	writeFixture(t, root, "Dockerfile", "FROM scratch AS vela-worker-agent\n")
	bundle := contractedBundleFixture()
	bundle.ConfigurationManifest.FinalRenders = append(
		bundle.ConfigurationManifest.FinalRenders,
		releasebundle.NamedArtifact{Name: "worker-agent"},
	)
	bundle.ConfigurationManifest.Packages = append(
		bundle.ConfigurationManifest.Packages,
		releasebundle.Package{Name: "h3-runner"},
	)
	evidence, _, _, err := scanVerified(
		root,
		bundle,
		strings.Repeat("d", 40),
		"ci/reachability",
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	)
	if err != nil || evidence.Result != ResultFail {
		t.Fatalf("legacy reachability evidence = %#v error=%v", evidence, err)
	}
	if err := ValidatePass(evidence, bundle); err == nil {
		t.Fatal("FAIL reachability evidence was accepted for authorization")
	}
}

func TestScanReportsKnownResidualLegacyOwnershipSurfaces(t *testing.T) {
	for _, test := range []struct {
		name    string
		path    string
		content string
		checkID string
	}{
		{
			name: "generated WorkerAssignment protocol", path: "proto/gen/vela/v1/worker_control.pb.go",
			content: "type WorkerAssignment struct {}\n", checkID: "legacy-worker-assignment-protocol",
		},
		{
			name: "Fleet DaemonSet", path: "internal/fleetcontroller/h3_daemonset.go",
			content: "func h3WorkerAgentContainer() {}\n", checkID: "legacy-worker-orchestration",
		},
		{
			name: "OCI image publication", path: "internal/releaseartifacts/oci_images.go",
			content: `{name: "vela-worker-agent"}`, checkID: "legacy-release-surface",
		},
		{
			name: "host package publication", path: "internal/releaseartifacts/host_packages.go",
			content: `Name: "h3-runner"`, checkID: "legacy-release-surface",
		},
		{
			name: "failure SQL", path: "db/queries/failure.sql",
			content: "AND execution_authority_kind = 'LEGACY_WORKER'\n", checkID: "legacy-assignment-sql",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := cleanStageSourceFixture(t)
			writeFixture(t, root, test.path, test.content)
			evidence, _, _, err := scanVerified(
				root,
				contractedBundleFixture(),
				strings.Repeat("d", 40),
				"ci/reachability",
				time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
			)
			if err != nil || evidence.Result != ResultFail {
				t.Fatalf("residual surface evidence = %#v error=%v", evidence, err)
			}
			for _, check := range evidence.Checks {
				if check.ID == test.checkID {
					if check.Passed || len(check.Matches) == 0 {
						t.Fatalf("residual surface check = %#v", check)
					}
					return
				}
			}
			t.Fatalf("reachability check %q is absent", test.checkID)
		})
	}
}

func TestCurrentRepositorySatisfiesPermanentReachability(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve reachability test path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
	bundle := contractedBundleFixture()
	evidence, _, _, err := scanVerified(
		root,
		bundle,
		bundle.ConfigurationManifest.SourceRevision,
		"repository-test",
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("scan current repository: %v", err)
	}
	if evidence.Result != ResultPass {
		var failed []Check
		for _, check := range evidence.Checks {
			if !check.Passed {
				failed = append(failed, check)
			}
		}
		t.Fatalf("permanent Legacy H3 reachability failed: %#v", failed)
	}
	for _, check := range evidence.Checks {
		if !check.Passed {
			t.Fatalf("permanent reachability check failed: %#v", check)
		}
	}
	if err := ValidatePass(evidence, bundle); err != nil {
		t.Fatalf("validate permanent reachability PASS: %v", err)
	}
}

func cleanStageSourceFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, path := range []string{
		"proto/vela/v1/stage_worker_control.proto",
		"cmd/vela-stage-worker-agent/main.go",
		"internal/stageworkeragent/agent.go",
		"internal/stagescheduler/service.go",
		"deploy/stage-worker/kustomization.yaml",
	} {
		writeFixture(t, root, path, "stage\n")
	}
	return root
}

func contractedBundleFixture() releasebundle.Bundle {
	return releasebundle.Bundle{
		SchemaVersion:         ContractedReleaseSchemaVersion,
		ReleaseDigest:         "sha256:" + strings.Repeat("a", 64),
		ConfigurationRevision: "sha256:" + strings.Repeat("b", 64),
		ConfigurationManifest: releasebundle.ConfigurationManifest{
			SchemaVersion:  ContractedReleaseSchemaVersion,
			SourceRevision: strings.Repeat("d", 40),
			FinalRenders:   []releasebundle.NamedArtifact{{Name: "stage-worker"}},
		},
		OCIImages: []releasebundle.OCIImage{{
			Image: "ghcr.io/vivym/vela-stage-worker-agent@sha256:" + strings.Repeat("c", 64),
		}},
	}
}

func writeFixture(t *testing.T, root, relative, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create fixture directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
}

func runFixtureGit(t *testing.T, root string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root}, arguments...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", arguments, err, output)
	}
	return string(output)
}

package hack_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalIntegrationShardEvidence(t *testing.T) {
	for _, scenario := range []struct {
		name, want string
		failed     bool
	}{
		{"pass", "PASS=2 SKIP=0 FAIL=0 MISSING=0", false},
		{"skip", "PASS=1 SKIP=1 FAIL=0 MISSING=0", false},
		{"missing", "MISSING=1", true},
		{"duplicate", "duplicate", true},
		{"unexpected", "unexpected test result", true},
		{"exit-error", "shard 1 failed", true},
		{"discovery-error", "test discovery failed", true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			directory := t.TempDir()
			fakeGo := `#!/bin/sh
set -eu
case " $* " in
  *' -list '*)
    printf '%s\n' TestAlpha TestBeta
    if [ "$VELA_FAKE_MODE" = discovery-error ]; then exit 1; fi
    exit 0 ;;
esac
touch "$VELA_FAKE_CALLS"
printf '%s\n' '--- PASS: TestAlpha (0.00s)' '    --- PASS: TestAlpha/nested (0.00s)'
case "$VELA_FAKE_MODE" in
  missing) ;;
  skip) printf '%s\n' '--- SKIP: TestBeta (0.00s)' ;;
  duplicate) printf '%s\n' '--- PASS: TestAlpha (0.00s)' '--- PASS: TestBeta (0.00s)' ;;
  *) printf '%s\n' '--- PASS: TestBeta (0.00s)' ;;
esac
if [ "$VELA_FAKE_MODE" = unexpected ]; then printf '%s\n' '--- PASS: TestUnexpected (0.00s)'; fi
printf '%s\n' PASS 'ok example/integration 0.001s'
if [ "$VELA_FAKE_MODE" = exit-error ]; then exit 3; fi
`
			if err := os.WriteFile(filepath.Join(directory, "go"), []byte(fakeGo), 0o700); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("bash", "./run-integration-shards.sh", "1")
			command.Env = append(os.Environ(), "PATH="+directory+string(os.PathListSeparator)+os.Getenv("PATH"),
				"TMPDIR="+directory, "VELA_FAKE_MODE="+scenario.name,
				"VELA_FAKE_CALLS="+filepath.Join(directory, "executed"),
				"VELA_INTEGRATION_CONCURRENCY=1", "VELA_INTEGRATION_SHARDS_DRY_RUN=0")
			output, err := command.CombinedOutput()
			if (err != nil) != scenario.failed || !strings.Contains(string(output), scenario.want) {
				t.Fatalf("scenario %s: err=%v output=%s; want failed=%t and %q", scenario.name, err, output, scenario.failed, scenario.want)
			}
			if scenario.name == "discovery-error" {
				if _, err := os.Stat(filepath.Join(directory, "executed")); !os.IsNotExist(err) {
					t.Fatal("partial discovery started a test shard")
				}
				return
			}
			results, err := filepath.Glob(filepath.Join(directory, "vela-integration-shards.*", "results-0.tsv"))
			if err != nil || len(results) != 1 {
				t.Fatalf("expected retained result evidence: %v %v", results, err)
			}
			if scenario.name == "skip" {
				contents, err := os.ReadFile(results[0])
				if err != nil || !strings.Contains(string(contents), "TestBeta\tSKIP\n") {
					t.Fatalf("skip not retained in evidence: %s %v", contents, err)
				}
			}
		})
	}
}

package hack_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestIntegrationShardsDiscoverTaggedPackagesAndKeepPackageIdentity(t *testing.T) {
	directory := t.TempDir()
	goPath := filepath.Join(directory, "go")
	const fakeGo = `#!/bin/sh
set -eu
case "$1" in
list)
  if [ "$2" = -tags=integration ]; then
    printf '%s\n' 'example/cmd/bootstrap unit_test.go,integration_test.go ' 'example/internal/integration suite_test.go ' 'example/internal/unit unit_test.go '
  else
    printf '%s\n' 'example/cmd/bootstrap unit_test.go ' 'example/internal/unit unit_test.go '
  fi
  ;;
test)
  package=$3
  if [ "$4" = -list ]; then
    case "$package" in
      example/cmd/bootstrap) printf '%s\n' TestShared TestBootstrap ;;
      example/internal/integration) printf '%s\n' TestShared TestStageA TestStageB ;;
      *) exit 3 ;;
    esac
    printf 'ok\t%s\t0.001s\n' "$package"
  else
    [ "$4" = -count=1 ]
    [ "$5" = -v ]
    [ "$6" = -run ]
    printf '%s %s\n' "$package" "$7" >>"$VELA_SHARD_TEST_LOG"
    case "$package" in
      example/cmd/bootstrap)
        case "$7" in *TestShared*) printf '%s\n' '--- PASS: TestShared (0.00s)' ;; *TestBootstrap*) printf '%s\n' '--- PASS: TestBootstrap (0.00s)' ;; esac ;;
      example/internal/integration)
        case "$7" in *TestShared*) printf '%s\n' '--- PASS: TestShared (0.00s)' ;; *TestStageA*) printf '%s\n' '--- PASS: TestStageA (0.00s)' ;; *TestStageB*) printf '%s\n' '--- PASS: TestStageB (0.00s)' ;; esac ;;
    esac
  fi
  ;;
*) exit 4 ;;
esac
`
	if err := os.WriteFile(goPath, []byte(fakeGo), 0o700); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"example/cmd/bootstrap ^(TestShared)$\nexample/internal/integration ^(TestStageA)$\n",
		"example/cmd/bootstrap ^(TestBootstrap)$\nexample/internal/integration ^(TestStageB)$\n",
		"example/internal/integration ^(TestShared)$\n",
	}
	for shard, expected := range want {
		t.Run(strconv.Itoa(shard), func(t *testing.T) {
			logPath := filepath.Join(directory, "shard-"+strconv.Itoa(shard))
			command := exec.Command("sh", "./test-integration-shard.sh", strconv.Itoa(shard), "3")
			command.Env = append(os.Environ(), "PATH="+directory+string(os.PathListSeparator)+os.Getenv("PATH"), "VELA_SHARD_TEST_LOG="+logPath)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("run shard: %v\n%s", err, output)
			}
			observed, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			if string(observed) != expected {
				t.Fatalf("selected tests = %q, want %q", observed, expected)
			}
		})
	}
}

func TestIntegrationShardsRejectInvalidIndices(t *testing.T) {
	for _, arguments := range [][]string{{}, {"0", "0"}, {"2", "2"}, {"-1", "3"}, {"0", "x"}} {
		t.Run(strings.Join(arguments, "/"), func(t *testing.T) {
			command := exec.Command("sh", append([]string{"./test-integration-shard.sh"}, arguments...)...)
			if output, err := command.CombinedOutput(); err == nil {
				t.Fatalf("invalid shard accepted: %s", output)
			}
		})
	}
}

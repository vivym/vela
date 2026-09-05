package hack_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCNPGFailoverRejectsMissingBuildSelectedTestBeforeClusterCreation(t *testing.T) {
	script, err := os.ReadFile("test-cnpg-failover.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, output := range []string{
		"ok example/internal/integration 0.01s [no tests to run]",
		"TestCloudNativePGSingleNodeFailoverPreservesAuthorityAndNoQuorumFailsClosedExtra",
	} {
		t.Run(output, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, "hack"), 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "hack", "test-cnpg-failover.sh")
			if err := os.WriteFile(path, script, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "go"), []byte("#!/bin/sh\nprintf '%s\\n' \"$VELA_CNPG_TEST_DISCOVERY\"\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("sh", path)
			command.Env = append(os.Environ(), "PATH="+root+string(os.PathListSeparator)+os.Getenv("PATH"),
				"VELA_CNPG_IMAGE_PLATFORM=linux/arm64", "VELA_CNPG_TEST_DISCOVERY="+output)
			result, err := command.CombinedOutput()
			if err == nil || !strings.Contains(string(result), "required CNPG failover test is absent from the selected Go build") {
				t.Fatalf("missing exact test accepted: error=%v output=%s", err, result)
			}
			if _, err := os.Stat(filepath.Join(root, "bin")); !os.IsNotExist(err) {
				t.Fatalf("missing test reached cluster tool setup: %v", err)
			}
		})
	}
}

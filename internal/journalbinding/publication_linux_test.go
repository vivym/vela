package journalbinding

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

func TestNodePublicationReadByNonRootWorker(t *testing.T) {
	if root := os.Getenv("VELA_TEST_BINDING_PUBLICATION"); root != "" {
		_, _, err := LoadNodePublication(filepath.Join(root, "binding.json"), filepath.Join(root, "verifier.json"), uint32(os.Getegid()))
		if (err == nil) != (os.Getenv("VELA_TEST_PUBLICATION_VALID") == "1") {
			t.Fatalf("unexpected Node publication result: %v", err)
		}
		return
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root to exercise the real non-root Worker credential")
	}
	root, err := os.MkdirTemp("/run", "vela-binding-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	signer, verifier := testKeys(t)
	binding, err := signer.Sign(testBinding())
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := Encode(binding)
	keys, _ := json.Marshal(verifier.keys)
	for name, content := range map[string][]byte{"binding.json": wire, "verifier.json": keys} {
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, content, 0o440); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(path, 0, 10001); err != nil {
			t.Fatal(err)
		}
	}
	for _, scenario := range []string{"valid", "wrong-group", "worker-owned", "writable", "world-readable", "writable-parent", "symlink", "tampered"} {
		t.Run(scenario, func(t *testing.T) {
			path := filepath.Join(root, "binding.json")
			_ = os.Remove(path)
			if err := os.WriteFile(path, wire, 0o440); err != nil {
				t.Fatal(err)
			}
			_ = os.Chown(path, 0, 10001)
			_ = os.Chmod(root, 0o755)
			switch scenario {
			case "wrong-group":
				_ = os.Chown(path, 0, 10002)
			case "worker-owned":
				_ = os.Chown(path, 10001, 10001)
			case "writable":
				_ = os.Chmod(path, 0o640)
			case "world-readable":
				_ = os.Chmod(path, 0o444)
			case "writable-parent":
				_ = os.Chmod(root, 0o777)
			case "symlink":
				_ = os.Remove(path)
				_ = os.Symlink("verifier.json", path)
			case "tampered":
				_ = os.WriteFile(path, []byte(`{"schema_version":1}`), 0o440)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestNodePublicationReadByNonRootWorker$")
			valid := "0"
			if scenario == "valid" {
				valid = "1"
			}
			command.Env = append(os.Environ(), "VELA_TEST_BINDING_PUBLICATION="+root, "VELA_TEST_PUBLICATION_VALID="+valid)
			command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 10001, Gid: 10001, NoSetGroups: true}}
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("non-root Worker: %v\n%s", err, output)
			}
		})
	}
}

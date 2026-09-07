//go:build integration && linux

package nodeagent

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestRuntimeImageMountPathObservation(t *testing.T) {
	if os.Getenv("VELA_TEST_CONTAINERD_SANDBOX") != "1" {
		t.Skip("requires a disposable mount namespace")
	}
	root := t.TempDir()
	source := filepath.Join(root, "source")
	target := filepath.Join(root, "space tab\tline\nslash\\tail")
	for _, path := range []string{source, target} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := unix.Mount(source, target, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	mounted := true
	t.Cleanup(func() {
		if mounted {
			_ = unix.Unmount(target, 0)
		}
	})
	for path, expected := range map[string]bool{target: true, source: false, target + "-other": false} {
		if actual, err := runtimeImagePathMounted(path); err != nil || actual != expected {
			t.Fatalf("kernel mount observation for %q: %v %v", path, actual, err)
		}
	}
	if err := unix.Unmount(target, 0); err != nil {
		t.Fatal(err)
	}
	mounted = false
	if actual, err := runtimeImagePathMounted(target); err != nil || actual {
		t.Fatalf("unmounted directory remained a mount: %v %v", actual, err)
	}
}

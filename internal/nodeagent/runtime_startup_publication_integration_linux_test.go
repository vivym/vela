//go:build integration && linux

package nodeagent

import (
	"os"
	"os/exec"
	"runtime"
	"testing"

	"golang.org/x/sys/unix"
)

func TestRuntimeStartupPublicationMountHelper(t *testing.T) {
	if os.Getenv("VELA_PUBLICATION_MOUNT_TARGET") != "1" {
		t.Skip("private mount-namespace mutation helper")
	}
	// Clone and seal the mount from retained FDs before entering the caller's
	// namespace. No pathname crosses between incompatible proc/root views.
	tree, err := unix.OpenTree(4, "", unix.OPEN_TREE_CLONE|unix.OPEN_TREE_CLOEXEC|unix.AT_EMPTY_PATH)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(tree) }()
	if err := unix.MountSetattr(tree, "", unix.AT_EMPTY_PATH, &unix.MountAttr{Attr_set: unix.MOUNT_ATTR_RDONLY}); err != nil {
		t.Fatal(err)
	}
	// The namespace change is confined to this disposable child OS thread.
	runtime.LockOSThread()
	if err := unix.Unshare(unix.CLONE_FS); err != nil {
		t.Fatal(err)
	}
	if err := unix.Setns(3, unix.CLONE_NEWNS); err != nil {
		t.Fatal(err)
	}
	if err := unix.MoveMount(tree, "", 5, "", unix.MOVE_MOUNT_F_EMPTY_PATH|unix.MOVE_MOUNT_T_EMPTY_PATH); err != nil {
		t.Fatal(err)
	}
}

func replaceStartupPublicationMount(t *testing.T, caller *RuntimeCaller, source string) {
	t.Helper()
	anchor, err := caller.process.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = anchor.Close() }()
	fd, err := unix.Openat(int(anchor.Fd()), "ns/mnt", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	namespace := os.NewFile(uintptr(fd), "original-caller-mount-namespace")
	defer func() { _ = namespace.Close() }()
	targetFD, err := unix.Openat(int(anchor.Fd()), "root/runtime-config", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	target := os.NewFile(uintptr(targetFD), "caller-publication-mount")
	defer func() { _ = target.Close() }()
	directory, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = directory.Close() }()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), binary, "-test.run=^TestRuntimeStartupPublicationMountHelper$", "-test.timeout=5s")
	command.Env = []string{"VELA_PUBLICATION_MOUNT_TARGET=1"}
	command.ExtraFiles = []*os.File{namespace, directory, target}
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("actual Runtime mount replacement failed: %v %s", err, output)
	}
}

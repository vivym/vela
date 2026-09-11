//go:build linux

package runtimechannel

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestSameLiveProcessReportsPidfdFilesystem(t *testing.T) {
	fd, err := unix.PidfdOpen(unix.Getpid(), 0)
	if err != nil {
		t.Skipf("pidfd_open unavailable: %v", err)
	}
	defer unix.Close(fd)

	var filesystem unix.Statfs_t
	if err := unix.Fstatfs(fd, &filesystem); err != nil {
		t.Fatalf("stat pidfd filesystem: %v", err)
	}
	err = SameLiveProcess(fd, fd)
	if err != nil {
		t.Fatalf("SameLiveProcess filesystem %#x: %v", filesystem.Type, err)
	}
}

func TestSameLiveProcessRejectsDifferentLegacyPIDFD(t *testing.T) {
	self, err := unix.PidfdOpen(unix.Getpid(), 0)
	if err != nil {
		t.Skipf("pidfd_open unavailable: %v", err)
	}
	defer unix.Close(self)
	child, err := os.StartProcess("/bin/sh", []string{"sh", "-c", "sleep 2"}, &os.ProcAttr{})
	if err != nil {
		t.Fatalf("start child: %v", err)
	}
	defer child.Kill()
	other, err := unix.PidfdOpen(child.Pid, 0)
	if err != nil {
		t.Fatalf("open child pidfd: %v", err)
	}
	defer unix.Close(other)
	if err := SameLiveProcess(self, other); err == nil {
		t.Fatal("different pidfds were accepted as the same process")
	}
}

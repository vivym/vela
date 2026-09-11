//go:build linux

package runtimechannel

import (
	"errors"
	"strings"
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
	if filesystem.Type == unix.PID_FS_MAGIC {
		if err != nil {
			t.Fatalf("SameLiveProcess on pidfs: %v", err)
		}
		return
	}
	if !errors.Is(err, ErrIdentity) {
		t.Fatalf("SameLiveProcess filesystem %#x error = %v, want ErrIdentity", filesystem.Type, err)
	}
	if !strings.Contains(err.Error(), "requires pidfs") || !strings.Contains(err.Error(), "filesystem type") {
		t.Fatalf("SameLiveProcess diagnostic = %v", err)
	}
}

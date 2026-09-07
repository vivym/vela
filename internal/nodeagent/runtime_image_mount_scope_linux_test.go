package nodeagent

import (
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestRuntimeImageMountReaderLifetime(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires a root-owned local daemon socket")
	}
	path := filepath.Join(t.TempDir(), "daemon.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	connection, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	before := runtimeImagePIDFDCount(t)
	reader := &runtimeImageMountReader{}
	t.Cleanup(func() { _ = reader.close() })
	for range 8 {
		if err := reader.authenticate(connection); err != nil {
			t.Fatal(err)
		}
	}
	if reader.pid != int32(os.Getpid()) || runtimeImagePIDFDCount(t) != before+1 {
		t.Fatal("reconnecting to the same daemon did not retain exactly one original pidfd")
	}
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			if mounted, err := reader.mounted("/"); err != nil || !mounted {
				t.Errorf("live daemon root mount was not observed: %v %v", mounted, err)
			}
		})
	}
	group.Wait()
	for range 2 {
		if err := reader.close(); err != nil {
			t.Fatal(err)
		}
	}
	if runtimeImagePIDFDCount(t) != before {
		t.Fatal("closed mount reader retained a pidfd")
	}
	if mounted, err := reader.mounted("/not-a-mount"); err == nil || mounted {
		t.Fatalf("closed reader reported kernel mount absence: %v %v", mounted, err)
	}
	if err := reader.authenticate(connection); err == nil || runtimeImagePIDFDCount(t) != before {
		t.Fatal("closed reader was reopened or leaked a pidfd")
	}
}

func runtimeImagePIDFDCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name())); err == nil && target == "anon_inode:[pidfd]" {
			count++
		}
	}
	return count
}

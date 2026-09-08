package nodeagent

import (
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestRuntimeContainerDaemonClosesHandles(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("the daemon peer must be root")
	}
	directory, err := os.MkdirTemp("/run", "vela-peer-handles-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(directory) }()
	path := filepath.Join(directory, "peer.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	connection, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	accepted, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = accepted.Close() }()
	// Warm netpoll before counting persistent descriptors.
	initial, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for range 8 {
		daemon := &runtimeContainerDaemon{}
		if err := daemon.check(); err == nil {
			t.Fatal("uninitialized daemon was usable")
		}
		if err := daemon.authenticate(connection); err != nil {
			t.Fatal(err)
		}
		group := &sync.WaitGroup{}
		for range 4 {
			group.Go(func() {
				_ = daemon.authenticate(connection)
				_ = daemon.check()
				if directory, err := daemon.openDirectory("/"); err == nil {
					_ = directory.Close()
				}
				_ = daemon.close()
			})
		}
		group.Wait()
		if err := daemon.close(); err != nil {
			t.Fatal(err)
		}
		if err := daemon.authenticate(connection); err == nil {
			t.Fatal("closed daemon repinned a process")
		}
	}
	final, err := os.ReadDir("/proc/self/fd")
	if err != nil || len(final) != len(initial) {
		t.Fatalf("peer handles leaked across concurrent close/reconnect: before=%d after=%d err=%v", len(initial), len(final), err)
	}
}

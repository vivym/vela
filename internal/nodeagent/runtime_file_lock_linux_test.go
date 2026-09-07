package nodeagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

type runtimeFileLockFixture struct {
	Original  int `json:"original"`
	Duplicate int `json:"duplicate"`
	Other     int `json:"other"`
}

func TestRuntimeCallerFileLock(t *testing.T) {
	credentials := RuntimeCallerCredentials{UID: 10001, GID: 10001}
	connection, _, wait := runtimeCallerConfiguredConnection(t, "file-lock", "unixpacket", nil, credentials, true)
	caller, err := ReceiveRuntimeCaller(t.Context(), connection, credentials)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = caller.Close() })
	var fixture runtimeFileLockFixture
	if err := json.Unmarshal(caller.Payload(), &fixture); err != nil {
		t.Fatal(err)
	}
	before := runtimeCallerDescriptorCount(t)
	original, err := caller.InspectFileLock(t.Context(), fixture.Original)
	if err != nil || original.Process.NamespacePID != 1 || original.FileInode == 0 || original.LockPID != original.Process.HostPID {
		t.Fatalf("observe authenticated caller's flock: %+v %v", original, err)
	}
	// Query from a different process: F_GETLK ignores a process's own locks,
	// so a same-process comparison would not distinguish the lock families.
	processRoot, err := caller.process.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	queryFD, openErr := unix.Openat(int(processRoot.Fd()), "fd/"+strconv.Itoa(fixture.Original), unix.O_RDWR|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	_ = processRoot.Close()
	if openErr != nil {
		t.Fatal(openErr)
	}
	query := unix.Flock_t{Type: unix.F_WRLCK, Whence: io.SeekStart}
	queryErr := unix.FcntlFlock(uintptr(queryFD), unix.F_GETLK, &query)
	conflictErr := unix.Flock(queryFD, unix.LOCK_EX|unix.LOCK_NB)
	_ = unix.Close(queryFD)
	if queryErr != nil || query.Type != unix.F_UNLCK || !errors.Is(conflictErr, unix.EWOULDBLOCK) {
		t.Fatalf("cross-process POSIX query=%v type=%d flock=%v", queryErr, query.Type, conflictErr)
	}
	duplicate, err := caller.InspectFileLock(t.Context(), fixture.Duplicate)
	if err != nil || duplicate.FileDevice != original.FileDevice || duplicate.FileInode != original.FileInode {
		t.Fatalf("duplicate descriptor changed locked inode: %+v %v", duplicate, err)
	}
	for _, descriptor := range []int{fixture.Other, 0, -1, 1 << 20} {
		if _, err := caller.InspectFileLock(t.Context(), descriptor); err == nil {
			t.Fatalf("unlocked, non-regular or invalid descriptor accepted: %d", descriptor)
		}
	}
	control := func(action byte) {
		t.Helper()
		if _, err := connection.Write([]byte{action}); err != nil {
			t.Fatal(err)
		}
		var ack [1]byte
		if _, err := io.ReadFull(connection, ack[:]); err != nil || ack[0] != action {
			t.Fatalf("control %q: %v", action, err)
		}
	}
	for _, fault := range []struct {
		name            string
		action, restore byte
	}{
		{"unlock-through-duplicate", 'u', 'r'}, {"shared-lock", 's', 'r'}, {"file-permissions", 'm', 'p'}, {"hardlink", 'h', 'd'},
	} {
		t.Run(fault.name, func(t *testing.T) {
			control(fault.action)
			if _, err := caller.InspectFileLock(t.Context(), fixture.Original); !errors.Is(err, ErrRuntimeFileLock) {
				t.Fatalf("untrusted lock observation returned: %v", err)
			}
			control(fault.restore)
			current, err := caller.InspectFileLock(t.Context(), fixture.Original)
			if err != nil || current.FileInode != original.FileInode {
				t.Fatalf("restored lock cannot be observed: %v", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := caller.InspectFileLock(ctx, fixture.Original); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled observation succeeded: %v", err)
	}
	control('c')
	if _, err := caller.InspectFileLock(t.Context(), fixture.Original); err == nil {
		t.Fatal("closed source descriptor accepted")
	}
	if _, err := caller.InspectFileLock(t.Context(), fixture.Duplicate); err != nil {
		t.Fatalf("remaining duplicate lost its lock: %v", err)
	}
	if runtimeCallerDescriptorCount(t) != before {
		t.Fatal("passive lock observations leaked Node descriptors")
	}
	control('q')
	wait()
	if _, err := caller.InspectFileLock(t.Context(), fixture.Duplicate); err == nil {
		t.Fatal("exited process retained live lock evidence")
	}
}

func runtimeFileLockProcess(t *testing.T, connection *net.UnixConn) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	if _, err := file.WriteString("f90ed00c-79f4-4ea9-a051-10a80ceaa821"); err != nil {
		t.Fatal(err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	other, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	duplicate, err := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(duplicate) }()
	payload, err := json.Marshal(runtimeFileLockFixture{Original: int(file.Fd()), Duplicate: duplicate, Other: int(other.Fd())})
	if err != nil {
		t.Fatal(err)
	}
	challenge := make([]byte, len(runtimeCallerProtocol)+32)
	if count, err := connection.Read(challenge); err != nil || count != len(challenge) {
		t.Fatalf("receive challenge: %v", err)
	}
	if _, err := connection.Write(append(challenge, payload...)); err != nil {
		t.Fatal(err)
	}
	for {
		var action [1]byte
		if _, err := io.ReadFull(connection, action[:]); err != nil {
			return
		}
		switch action[0] {
		case 'u':
			err = unix.Flock(duplicate, unix.LOCK_UN)
		case 'r':
			err = unix.Flock(duplicate, unix.LOCK_EX|unix.LOCK_NB)
		case 's':
			err = unix.Flock(duplicate, unix.LOCK_SH|unix.LOCK_NB)
		case 'm':
			err = os.Chmod(path, 0o644)
		case 'p':
			err = os.Chmod(path, 0o600)
		case 'h':
			err = os.Link(path, path+".alias")
		case 'd':
			err = os.Remove(path + ".alias")
		case 'c':
			err = file.Close()
		case 'q':
		default:
			t.Fatalf("unknown control: %q", action[0])
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := connection.Write(action[:]); err != nil {
			t.Fatal(err)
		}
		if action[0] == 'q' {
			return
		}
	}
}

func TestRuntimeFileLockInfoRejectsUnprovenLocks(t *testing.T) {
	identity := runtimeFileLockIdentity{device: unix.Mkdev(0, 63), inode: 42}
	valid := "pos:\t0\nflags:\t02000002\nmnt_id:\t7\nino:\t42\nlock:\t1: FLOCK  ADVISORY  WRITE 123 00:3f:42 0 EOF\n"
	if _, err := parseRuntimeFileLockInfo(valid, identity, 123); err != nil {
		t.Fatal(err)
	}
	for _, fault := range []struct{ name, old, replacement string }{
		{"unlocked", "lock:\t1: FLOCK  ADVISORY  WRITE 123 00:3f:42 0 EOF\n", ""},
		{"duplicate", "pos:\t0\n", "pos:\t0\npos:\t0\n"},
		{"unknown-field", "pos:\t0\n", "unknown:\t0\n"},
		{"negative-position", "pos:\t0", "pos:\t-1"},
		{"read-only", "02000002", "02000000"},
		{"no-cloexec", "02000002", "02"},
		{"opath", "02000002", "012000002"},
		{"missing-mount", "mnt_id:\t7", "mnt_id:\t0"},
		{"wrong-inode", "ino:\t42", "ino:\t43"},
		{"posix", "FLOCK", "POSIX"},
		{"ofd", "FLOCK", "OFDLCK"},
		{"shared", "WRITE", "READ"},
		{"mandatory", "ADVISORY", "MSNFS"},
		{"other-locker", "WRITE 123", "WRITE 124"},
		{"other-device", "00:3f:42", "00:40:42"},
		{"other-lock-inode", "00:3f:42", "00:3f:43"},
		{"partial-range", "0 EOF", "1 36"},
		{"waiter", "1: FLOCK", "1: -> FLOCK"},
		{"multiple-locks", "lock:\t1:", "lock:\t2:"},
	} {
		t.Run(fault.name, func(t *testing.T) {
			wire := strings.ReplaceAll(valid, fault.old, fault.replacement)
			if _, err := parseRuntimeFileLockInfo(wire, identity, 123); !errors.Is(err, ErrRuntimeFileLock) {
				t.Fatalf("unproven kernel lock accepted: %v", err)
			}
		})
	}
}

func TestRuntimeFileLockKernelEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	other, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	read := func(fd int) string {
		t.Helper()
		wire, err := os.ReadFile(fmt.Sprintf("/proc/self/fdinfo/%d", fd))
		if err != nil {
			t.Fatal(err)
		}
		return string(wire)
	}
	if !strings.Contains(read(int(file.Fd())), "FLOCK  ADVISORY  WRITE") || strings.Contains(read(int(other.Fd())), "lock:") {
		t.Fatal("kernel lock evidence did not distinguish independent opens of the same inode")
	}
	t.Logf("held descriptor fdinfo:\n%s", read(int(file.Fd())))
	duplicate, err := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if duplicate >= 0 {
			_ = unix.Close(duplicate)
		}
	}()
	if !strings.Contains(read(duplicate), "FLOCK  ADVISORY  WRITE") {
		t.Fatal("duplicated open file description lost lock evidence")
	}
	passive, err := unix.Open(fmt.Sprintf("/proc/self/fd/%d", file.Fd()), unix.O_PATH|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(passive) }()
	if strings.Contains(read(passive), "lock:") {
		t.Fatal("O_PATH observation acquired the source lock")
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := unix.Flock(int(other.Fd()), unix.LOCK_EX|unix.LOCK_NB); err == nil {
		t.Fatal("closing one duplicate released the retained open-file-description lock")
	}
	if err := unix.Close(duplicate); err != nil {
		t.Fatal(err)
	}
	duplicate = -1
	if err := unix.Flock(int(other.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatalf("passive inode observation extended lock lifetime: %v", err)
	}
}

func assertRuntimeFixtureFileLock(t *testing.T, caller *RuntimeCaller, expected unix.Stat_t) {
	t.Helper()
	// Only the fixture supplies a trusted original inode and enumerates fds.
	// Production still needs an independently retained journal birth identity.
	directory, err := caller.process.Open("fd")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = directory.Close() }()
	names, err := directory.Readdirnames(1024)
	if err != nil && !errors.Is(err, io.EOF) || len(names) == 1024 {
		t.Fatalf("enumerate bounded Runtime fixture descriptors: %v", err)
	}
	for _, name := range names {
		descriptor, err := strconv.Atoi(name)
		if err != nil {
			t.Fatal(err)
		}
		observed, err := caller.InspectFileLock(t.Context(), descriptor)
		if err == nil && observed.FileDevice == expected.Dev && observed.FileInode == expected.Ino && observed.SizeBytes == expected.Size {
			return
		}
	}
	t.Fatal("actual Runtime has no observed descriptor holding its original fixture journal flock")
}

package nodeagent

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestRuntimeExecutableObservation(t *testing.T) {
	connection, process, wait := runtimeCallerConnection(t, "normal", "unixpacket")
	caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = caller.Close() })
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	expected, size := runtimeExecutableDigest(t, binary)
	before := runtimeCallerDescriptorCount(t)
	for range 3 {
		observed, err := caller.InspectExecutable(t.Context())
		if err != nil || observed.Digest != expected || observed.SizeBytes != size || observed.FileInode == 0 ||
			observed.Process.HostPID != int32(process.Pid) || observed.Process.UID != 65532 || observed.Process.StartTicks == 0 ||
			observed.ObservedFrom.IsZero() || observed.ObservedThrough.Before(observed.ObservedFrom) ||
			observed.ObservedThrough.Sub(observed.ObservedFrom) > runtimeCallerTimeout {
			t.Fatalf("wrong executable observation: %+v %v", observed, err)
		}
	}
	if after := runtimeCallerDescriptorCount(t); after != before {
		t.Fatalf("executable observation leaked descriptors: %d -> %d", before, after)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if result, err := caller.InspectExecutable(ctx); result != (RuntimeExecutableObservation{}) || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled executable read: %+v %v", result, err)
	}
	if result, err := caller.InspectExecutable(nil); result != (RuntimeExecutableObservation{}) || err == nil { //nolint:staticcheck // Verify explicit rejection of a missing context.
		t.Fatal("missing context accepted")
	}
	if _, err := connection.Write([]byte("exit")); err != nil {
		t.Fatal(err)
	}
	wait()
	if result, err := caller.InspectExecutable(t.Context()); result != (RuntimeExecutableObservation{}) || err == nil {
		t.Fatal("exited process retained an executable observation")
	}
	if err := caller.Close(); err != nil {
		t.Fatal(err)
	}
	for _, unavailable := range []*RuntimeCaller{caller, nil, {}} {
		if result, err := unavailable.InspectExecutable(t.Context()); result != (RuntimeExecutableObservation{}) || err == nil {
			t.Fatal("closed/nil/empty caller retained an executable observation")
		}
	}
}

func TestRuntimeExecutablePathAndExec(t *testing.T) {
	for _, scenario := range []string{"path-replaced", "path-deleted", "same-pid-exec"} {
		t.Run(scenario, func(t *testing.T) {
			directory, err := os.MkdirTemp("/tmp", "vela-runtime-executable-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(directory) })
			if err := os.Chmod(directory, 0o755); err != nil {
				t.Fatal(err)
			}
			binary := runtimeExecutableCopy(t, directory, "original", "")
			connection, _, _ := runtimeCallerExecutableConnection(t, binary, "exec-after-challenge", "unixpacket", []byte("original declaration"),
				RuntimeCallerCredentials{UID: 65532, GID: 65532}, false)
			caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = caller.Close() })
			first, err := caller.InspectExecutable(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			var changedDigest [sha256.Size]byte
			if scenario == "path-deleted" {
				if err := os.Remove(binary); err != nil {
					t.Fatal(err)
				}
			} else {
				replacement := runtimeExecutableCopy(t, directory, "replacement", "different executable file bytes")
				changedDigest, _ = runtimeExecutableDigest(t, replacement)
				if changedDigest == first.Digest {
					t.Fatal("replacement fixture has the original file identity")
				}
				if err := os.Rename(replacement, binary); err != nil {
					t.Fatal(err)
				}
			}
			unlinked, err := caller.InspectExecutable(t.Context())
			if err != nil || unlinked.Digest != first.Digest || unlinked.FileInode != first.FileInode || unlinked.FileLinks != 0 {
				t.Fatalf("measurement followed a replaceable pathname: %+v %v", unlinked, err)
			}
			if scenario != "same-pid-exec" {
				return
			}
			if _, err := connection.Write([]byte("exec")); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				current, err := caller.InspectExecutable(t.Context())
				if err == nil && current.Digest == changedDigest {
					if current.Process.HostPID != first.Process.HostPID || current.Process.StartTicks != first.Process.StartTicks ||
						current.FileInode == first.FileInode || string(caller.Payload()) != "original declaration" {
						t.Fatalf("exec fixture changed the process/declaration instead of its executable: %+v", current)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("same-pid exec did not change the observed executable: %+v %v", current, err)
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestRuntimeExecutableFileBounds(t *testing.T) {
	directory, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = directory.Close() }()
	if file, err := openRuntimeExecutable(directory); file != nil || !errors.Is(err, ErrRuntimeExecutable) {
		t.Fatal("non-procfs directory accepted for a kernel executable lookup")
	}
	if _, err := runtimeExecutableIdentity(directory); !errors.Is(err, ErrRuntimeExecutable) {
		t.Fatal("directory accepted as an executable file")
	}
	file, err := os.CreateTemp(t.TempDir(), "bounded-executable-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	for _, size := range []int64{0, maximumRuntimeExecutableBytes + 1} {
		if err := file.Truncate(size); err != nil {
			t.Fatal(err)
		}
		if _, err := runtimeExecutableIdentity(file); !errors.Is(err, ErrRuntimeExecutable) {
			t.Fatalf("invalid executable size %d accepted: %v", size, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reader := &runtimeExecutableReader{ctx: ctx, file: file}
	if count, err := reader.Read(make([]byte, 1)); count != 0 || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled content read accessed the file")
	}
	proc, err := os.Open("/proc/self")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = proc.Close() }()
	executable, err := openRuntimeExecutable(proc)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = executable.Close() }()
	flags, err := unix.FcntlInt(executable.Fd(), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatal("executable descriptor can escape exec")
	}
}

func runtimeExecutableCopy(t *testing.T, directory, name, trailer string) string {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	input, err := os.Open(binary)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = input.Close() }()
	path := filepath.Join(directory, name)
	output, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o555)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(output, input); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(output, trailer); err != nil {
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func runtimeExecutableDigest(t *testing.T, path string) ([sha256.Size]byte, int64) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		t.Fatal(err)
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest, size
}

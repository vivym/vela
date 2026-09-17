//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestValidationReceiptIsMarkedAndAtomicallyWritten(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "receipt.json")
	started := time.Now().UTC().Add(-time.Second)
	workload := &validationWorkload{receiptPath: path, scenario: "normal", startedAt: started}
	if err := writeValidationReceipt(workload, nil, nil); err != nil {
		t.Fatal(err)
	}
	wire, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var receipt validationReceipt
	if err := json.Unmarshal(wire, &receipt); err != nil {
		t.Fatal(err)
	}
	if !receipt.ValidationOnly || receipt.Scenario != "normal" || receipt.Outcome != "completed" || receipt.StartedAt.IsZero() || receipt.FinishedAt.IsZero() {
		t.Fatalf("unexpected validation receipt: %+v", receipt)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "receipt.json" {
		t.Fatalf("temporary receipt files leaked: %#v", entries)
	}
}

func TestStandaloneValidationFailureReceiptIsAtomicAndFailed(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "receipt.json")
	want := errors.New("simulated worker start failure")
	if err := writeStandaloneValidationFailureReceipt(path, "worker-start", want); err != nil {
		t.Fatal(err)
	}
	wire, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var receipt validationReceipt
	if err := json.Unmarshal(wire, &receipt); err != nil {
		t.Fatal(err)
	}
	if !receipt.ValidationOnly || receipt.Scenario != "worker-start" || receipt.Outcome != "failed" || receipt.Error != want.Error() || receipt.FDCount != 0 {
		t.Fatalf("unexpected failure receipt: %+v", receipt)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "receipt.json" {
		t.Fatalf("temporary receipt files leaked: %#v", entries)
	}
}

func TestValidationWorkerCommandIsSimpleArgv(t *testing.T) {
	got := splitValidationCommand("/bin/sleep 3600")
	if len(got) != 2 || got[0] != "/bin/sleep" || got[1] != "3600" {
		t.Fatalf("unexpected worker argv: %#v", got)
	}
	if got := splitValidationCommand("   "); len(got) != 0 {
		t.Fatalf("blank worker command parsed as %#v", got)
	}
}

func TestRecvFrameStopsWhenContextIsCanceled(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func(fd int) { _ = unix.Close(fd) }(fds[0])
	defer func(fd int) { _ = unix.Close(fd) }(fds[1])
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	_, err = recvFrame(ctx, fds[0])
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("recvFrame error = %v, want context.Canceled", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("recvFrame did not honor an already-canceled context promptly")
	}
}

func TestRecvFrameReturnsEOFOnPeerClose(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func(fd int) { _ = unix.Close(fd) }(fds[0])
	peer := os.NewFile(uintptr(fds[1]), "validation-launcher-test-peer")
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = recvFrame(context.Background(), fds[0])
	if !errors.Is(err, os.ErrClosed) && !errors.Is(err, context.Canceled) && !errors.Is(err, unix.ECONNRESET) && !errors.Is(err, unix.ENOTCONN) && !errors.Is(err, io.EOF) {
		t.Fatalf("recvFrame after peer close = %v", err)
	}
}

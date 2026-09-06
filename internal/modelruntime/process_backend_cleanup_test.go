//go:build darwin || linux

package modelruntime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestProcessBackendCleansUpInheritedPipesAndChildWriter(t *testing.T) {
	for _, mode := range []string{"shutdown", "timeout", "unexpected_exit", "full_response_queue"} {
		t.Run(mode, func(t *testing.T) {
			backend, root := startCleanupProcessBackend(t, mode, io.Discard)
			switch mode {
			case "shutdown":
				if err := backend.Close(); err != nil {
					t.Errorf("Close after acknowledged shutdown: %v", err)
				}
			case "timeout":
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()
				_, err := backend.Probe(ctx, velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP)
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("Probe error = %v, want deadline exceeded", err)
				}
			default:
				if err := os.WriteFile(filepath.Join(root, "exit-gate"), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-backend.Done():
			case <-time.After(2 * time.Second):
				t.Error("Done did not close after driver exit with inherited stdout/stderr")
			}
			before, err := os.Stat(filepath.Join(root, "child-writes"))
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(150 * time.Millisecond)
			after, err := os.Stat(filepath.Join(root, "child-writes"))
			if err != nil {
				t.Fatal(err)
			}
			if after.Size() != before.Size() {
				t.Errorf("child kept writing after teardown: bytes %d -> %d", before.Size(), after.Size())
			}
			if mode != "shutdown" && backend.Err() == nil {
				t.Error("Err did not report exited driver")
			}
		})
	}
}

func TestProcessBackendCancelsBlockedRequestWrite(t *testing.T) {
	backend, _ := startCleanupProcessBackend(t, "no_read", io.Discard)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	prepared := make(chan error, 1)
	go func() {
		prepared <- backend.Prepare(ctx, processBackendAuthority(1), &velav1.StageExecutionSpec{
			ParametersJson: []byte(strings.Repeat("x", 512<<10)),
		})
	}()
	// The first request is larger than an OS pipe and the driver never reads it.
	time.Sleep(50 * time.Millisecond)
	queuedContext, queuedCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer queuedCancel()
	queued := make(chan error, 1)
	go func() {
		_, err := backend.Probe(queuedContext, velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP)
		queued <- err
	}()
	select {
	case err := <-queued:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("queued Probe error = %v, want deadline exceeded", err)
		}
		select {
		case <-backend.Done():
			t.Error("canceled queued request terminated the active driver")
		default:
		}
	case <-time.After(150 * time.Millisecond):
		t.Error("queued Probe did not honor its context while another request blocked")
	}
	select {
	case err := <-prepared:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("blocked Prepare error = %v, want deadline exceeded", err)
		}
	case <-time.After(600 * time.Millisecond):
		t.Error("Prepare stayed blocked in stdin Write after context cancellation")
	}
}

func TestProcessBackendReportsBlockedStderrWriter(t *testing.T) {
	writer := &cleanupBlockingWriter{started: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	backend, _ := startCleanupProcessBackend(t, "shutdown", writer)
	t.Cleanup(func() { close(writer.release) })
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("stderr writer did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- backend.Close() }()
	select {
	case err := <-closed:
		if err == nil || !strings.Contains(err.Error(), "stderr drain timed out") {
			t.Errorf("Close error = %v, want incomplete stderr drain", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked on caller-owned stderr writer")
	}
	select {
	case <-writer.finished:
		t.Error("test writer unexpectedly returned before its owner released it")
	default:
	}
	if backend.Err() == nil {
		t.Error("Err concealed incomplete stderr drain")
	}
}

func TestProcessBackendReportsEscapedChildWithInheritedStdout(t *testing.T) {
	backend, root := startCleanupProcessBackend(t, "escaped_child", io.Discard)
	if err := backend.Close(); err == nil || !strings.Contains(err.Error(), "stdout drain timed out") {
		t.Errorf("Close error = %v, want incomplete stdout drain", err)
	}
	select {
	case <-backend.Done():
	case <-time.After(time.Second):
		t.Fatal("Done blocked on escaped child's inherited stdout")
	}
	before, err := os.Stat(filepath.Join(root, "child-writes"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	after, err := os.Stat(filepath.Join(root, "child-writes"))
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() <= before.Size() {
		t.Fatal("test did not demonstrate that an escaped child survives group cleanup")
	}
}

func TestProcessBackendCleansUpFailedInitialization(t *testing.T) {
	backend, root, err := newCleanupProcessBackend(t, "initialize_failure", io.Discard)
	if backend != nil || err == nil || !strings.Contains(err.Error(), "initialization rejected") {
		t.Fatalf("NewProcessBackend = %v, %v; want rejected initialization", backend, err)
	}
	before, err := os.Stat(filepath.Join(root, "child-writes"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	after, err := os.Stat(filepath.Join(root, "child-writes"))
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Errorf("child kept writing after rejected initialization: bytes %d -> %d", before.Size(), after.Size())
	}
}

func TestProcessBackendReportsFailedInitializationCleanup(t *testing.T) {
	writer := &cleanupBlockingWriter{started: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	t.Cleanup(func() { close(writer.release) })
	backend, _, err := newCleanupProcessBackend(t, "initialize_failure", writer)
	if backend != nil || err == nil || !strings.Contains(err.Error(), "initialization rejected") {
		t.Fatalf("NewProcessBackend = %v, %v; want rejected initialization", backend, err)
	}
	if !strings.Contains(err.Error(), "stderr drain timed out") {
		t.Errorf("initialization error concealed incomplete teardown: %v", err)
	}
}

type cleanupBlockingWriter struct {
	started  chan struct{}
	release  chan struct{}
	finished chan struct{}
	once     sync.Once
}

func (writer *cleanupBlockingWriter) Write(data []byte) (int, error) {
	writer.once.Do(func() {
		close(writer.started)
		<-writer.release
		close(writer.finished)
	})
	return len(data), nil
}

func startCleanupProcessBackend(t *testing.T, mode string, stderr io.Writer) (*ProcessBackend, string) {
	t.Helper()
	backend, root, err := newCleanupProcessBackend(t, mode, stderr)
	if err != nil {
		t.Fatalf("NewProcessBackend: %v", err)
	}
	return backend, root
}

func newCleanupProcessBackend(t *testing.T, mode string, stderr io.Writer) (*ProcessBackend, string, error) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	for _, name := range []string{"inputs", "outputs"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	var backend *ProcessBackend
	t.Cleanup(func() {
		// The RED regression must not leave its deliberately orphaned writer running.
		_ = os.WriteFile(filepath.Join(root, "stop-gate"), nil, 0o600)
		_ = os.WriteFile(filepath.Join(root, "exit-gate"), nil, 0o600)
		if mode == "escaped_child" {
			deadline := time.Now().Add(time.Second)
			for {
				if _, err := os.Stat(filepath.Join(root, "child-stopped")); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Error("escaped test writer did not acknowledge cleanup")
					break
				}
				time.Sleep(time.Millisecond)
			}
		}
		if backend != nil {
			_ = backend.Close()
			select {
			case <-backend.Done():
			case <-time.After(3 * time.Second):
				t.Error("backend did not finish after test cleanup")
			}
		}
	})
	backend, err = NewProcessBackend(context.Background(), processBackendBinding(), ProcessBackendConfig{
		Component: "ENCODER", ModelComponentRevision: "cpu-cleanup-test-r1",
		Command: []string{executable, "-test.run=^TestProcessBackendCleanupDriverHelper$"},
		Environment: []string{
			"TEST_PROCESS_BACKEND_CLEANUP=" + mode,
			"TEST_PROCESS_BACKEND_CLEANUP_ROOT=" + root,
			"GORACE=atexit_sleep_ms=0",
		},
		LocalDevices: []DriverDevice{{
			DeviceID: "33000000-0000-0000-0000-000000000001", DeviceEpoch: 7, ResourceClass: "CPU",
		}},
		ScratchRoot: root, InputRoot: filepath.Join(root, "inputs"), OutputRoot: filepath.Join(root, "outputs"),
		InitializationTimeout: 5 * time.Second, ShutdownTimeout: 300 * time.Millisecond, Stderr: stderr,
	})
	return backend, root, err
}

func TestProcessBackendCleanupDriverHelper(t *testing.T) {
	mode := os.Getenv("TEST_PROCESS_BACKEND_CLEANUP")
	if mode == "" {
		return
	}
	root := os.Getenv("TEST_PROCESS_BACKEND_CLEANUP_ROOT")
	helperDeadline := time.Now().Add(15 * time.Second)
	stopping := func() bool {
		_, err := os.Stat(filepath.Join(root, "stop-gate"))
		_, rootErr := os.Stat(root)
		return err == nil || errors.Is(rootErr, os.ErrNotExist) || time.Now().After(helperDeadline)
	}
	if mode == "child" || mode == "escaped_writer" {
		file, err := os.OpenFile(filepath.Join(root, "child-writes"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			os.Exit(2)
		}
		for {
			if stopping() {
				_ = file.Close()
				_ = os.WriteFile(filepath.Join(root, "child-stopped"), nil, 0o600)
				os.Exit(0)
			}
			_, _ = file.WriteString("x")
			if mode == "child" {
				_, _ = fmt.Fprintln(os.Stderr, "child heartbeat")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var request driverRequestV1
		if json.Unmarshal(scanner.Bytes(), &request) != nil {
			os.Exit(2)
		}
		response := driverResponseV1{SchemaVersion: driverProtocolVersion, RequestID: request.RequestID, Acknowledged: true}
		switch request.Operation {
		case "initialize":
			executable, _ := os.Executable()
			child := exec.Command(executable, "-test.run=^TestProcessBackendCleanupDriverHelper$")
			child.Env = append(os.Environ(), "TEST_PROCESS_BACKEND_CLEANUP=child")
			child.Stdout, child.Stderr = os.Stdout, os.Stderr
			if mode == "escaped_child" {
				child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
				child.Env = append(os.Environ(), "TEST_PROCESS_BACKEND_CLEANUP=escaped_writer")
			}
			if child.Start() != nil {
				os.Exit(2)
			}
			for {
				if info, err := os.Stat(filepath.Join(root, "child-writes")); err == nil && info.Size() > 0 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			response.Initialized = true
			if mode == "initialize_failure" {
				response.Initialized = false
				response.Error = "initialization rejected"
			}
			_ = encoder.Encode(response)
			if mode == "no_read" {
				for !stopping() {
					time.Sleep(10 * time.Millisecond)
				}
				os.Exit(0)
			}
			if mode == "unexpected_exit" || mode == "full_response_queue" {
				for {
					if _, err := os.Stat(filepath.Join(root, "exit-gate")); err == nil {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if mode == "full_response_queue" {
					for range 4 {
						_ = encoder.Encode(response)
					}
				}
				os.Exit(3)
			}
		case "prepare":
			if mode == "prepare_timeout" {
				_ = os.WriteFile(filepath.Join(root, "prepare-blocked"), nil, 0o600)
				for !stopping() {
					time.Sleep(10 * time.Millisecond)
				}
				os.Exit(0)
			}
		case "probe":
			if mode == "timeout" {
				for {
					if stopping() {
						os.Exit(0)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		case "shutdown":
			_ = encoder.Encode(response)
			os.Exit(0)
		}
	}
	os.Exit(0)
}

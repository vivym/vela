package nodeagent

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func observedRuntimeCallerConnection(t *testing.T, payload []byte, credentials RuntimeCallerCredentials) (*net.UnixConn, *os.Process, *RuntimeObserverCustody) {
	t.Helper()
	if _, err := os.Stat("/exec-observer"); err != nil {
		t.Skip("requires native exec-observer")
	}
	root, err := os.MkdirTemp("/tmp", "vela-observed-caller-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(root, "caller.sock")
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socket, Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	if err := os.Chmod(socket, 0o666); err != nil {
		t.Fatal(err)
	}
	node, creator := runtimeObserverSocketpair(t)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), "/exec-observer", "--new-pid", "--custody-fd3", strconv.Itoa(int(credentials.UID)), strconv.Itoa(int(credentials.GID)), binary, "-test.run=^TestRuntimeCallerProcessHelper$", "-test.timeout=30s")
	command.ExtraFiles = []*os.File{creator}
	command.Env = []string{runtimeCallerTestMode + "=hold-after-disconnect", "VELA_RUNTIME_CALLER_TEST_SOCKET=" + socket, "VELA_RUNTIME_CALLER_TEST_NETWORK=unixpacket", "VELA_RUNTIME_CALLER_TEST_PAYLOAD=" + base64.StdEncoding.EncodeToString(payload)}
	fd := -1
	command.SysProcAttr = &syscall.SysProcAttr{PidFD: &fd}
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	_ = creator.Close()
	original := os.NewFile(uintptr(fd), "observer-original")
	defer func() { _ = original.Close() }()
	done := make(chan struct{})
	go func() { _ = command.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = command.Process.Kill()
		<-done
		if t.Failed() {
			t.Log(output.String())
		}
	})
	custody, err := ReceiveRuntimeObserverCustody(t.Context(), node, original)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = custody.Close() })
	if err := custody.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := listener.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	connection, err := listener.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection, command.Process, custody
}

func observedActivationFixture(t *testing.T) (startupActivationFixture, *RuntimeObserverCustody) {
	t.Helper()
	var custody *RuntimeObserverCustody
	_, config := newRemoteReservationFixture(t, false, &custody)
	return startupActivationFromReservation(t, config), custody
}

func awaitObservedExit(t *testing.T, custody *RuntimeObserverCustody) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		exited, err := custody.TargetExited(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if exited {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("original target did not exit")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRuntimeJournalObservation(t *testing.T) {
	for _, scenario := range []string{"close", "cancel", "observer-stopped", "observer-killed", "channel-loss", "ledger-close", "endpoint-close"} {
		t.Run(scenario, func(t *testing.T) {
			f, custody := observedActivationFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			observation, record, err := f.ledger.ActivateObservedJournalWriteGrant(ctx, f.plan, f.grant, custody, 50*time.Millisecond, 500*time.Millisecond)
			if err != nil || record.OperationID != f.reservation.OperationID {
				t.Fatalf("activate: %+v %v", record, err)
			}
			t.Cleanup(func() { _ = observation.Close() })
			f.write(t, true)
			if _, _, err := f.ledger.ActivateObservedJournalWriteGrant(ctx, f.plan, f.grant, custody, 50*time.Millisecond, 500*time.Millisecond); err == nil {
				t.Fatal("observation attached twice")
			}
			switch scenario {
			case "close":
				err = observation.Close()
			case "cancel":
				cancel()
			case "observer-stopped":
				err = unix.PidfdSendSignal(int(custody.observer.Fd()), unix.SIGSTOP, nil, 0)
			case "observer-killed":
				err = unix.PidfdSendSignal(int(custody.observer.Fd()), unix.SIGKILL, nil, 0)
			case "channel-loss":
				err = custody.connection.Close()
			case "ledger-close":
				err = f.ledger.Close()
			case "endpoint-close":
				err = f.endpoint.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-observation.done:
			case <-time.After(5 * time.Second):
				t.Fatal("monitor did not stop")
			}
			f.write(t, false)
			awaitObservedExit(t, custody)
			if _, err := f.ledger.ActivateReservedJournalWriteGrant(t.Context(), f.plan, f.grant); err == nil {
				t.Fatal("revoked grant reactivated")
			}
		})
	}
}

func TestRuntimeJournalObservationRejectsWrongTarget(t *testing.T) {
	f, _ := observedActivationFixture(t)
	_, other := observedActivationFixture(t)
	observation, _, err := f.ledger.ActivateObservedJournalWriteGrant(t.Context(), f.plan, f.grant, other, time.Millisecond, time.Second)
	if err == nil || observation != nil {
		t.Fatal("unrelated observer accepted")
	}
	f.write(t, false)
	if len(f.ledger.grantAttempts) != 0 {
		t.Fatal("wrong observer consumed grant")
	}
	if err := other.Check(t.Context()); err != nil {
		t.Fatal("mismatched request revoked unrelated observer")
	}
}

func TestRuntimeJournalObservationPersistenceOutage(t *testing.T) {
	for _, scenario := range []string{"cancel", "stopped", "after-sync-stopped"} {
		t.Run(scenario, func(t *testing.T) {
			f, custody := observedActivationFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			f.ledger.boundary = func(phase string) error {
				point := "before-append"
				if scenario == "after-sync-stopped" {
					point = "after-sync"
				}
				if phase == point {
					close(entered)
					<-release
				}
				return nil
			}
			result := make(chan error, 1)
			go func() {
				observation, _, err := f.ledger.ActivateObservedJournalWriteGrant(ctx, f.plan, f.grant, custody, 50*time.Millisecond, 500*time.Millisecond)
				if observation != nil {
					_ = observation.Close()
				}
				result <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("did not enter persistence")
			}
			if scenario == "cancel" {
				cancel()
			} else if err := unix.PidfdSendSignal(int(custody.observer.Fd()), unix.SIGSTOP, nil, 0); err != nil {
				t.Fatal(err)
			}
			// Exit and route refusal must occur BEFORE releasing ledger persistence.
			awaitObservedExit(t, custody)
			f.write(t, false)
			unblock()
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("outage activated grant")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("activation did not join")
			}
			if !f.endpoint.readOnly {
				t.Fatal("outage left writable endpoint")
			}
		})
	}
}

func TestRuntimeJournalObservationRejectsExpiredCheck(t *testing.T) {
	f, custody := observedActivationFixture(t)
	observation, _, err := f.ledger.ActivateObservedJournalWriteGrant(t.Context(), f.plan, f.grant, custody, time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = observation.Close() }()
	f.write(t, true)
	// Model a delayed monitoring goroutine; route admission independently enforces
	// the last response's expiry. No wall-clock change or expired signature involved.
	expired := time.Now().Add(-time.Second)
	observation.deadline.Store(&expired)
	f.write(t, false)
	if err := observation.check(t.Context()); err == nil {
		t.Fatal("expired lifetime revived through successful check")
	}
	select {
	case <-observation.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("expired monitor did not stop")
	}
	if observation.Err() == nil {
		t.Fatal("expired lifetime lost its failure cause")
	}
	f.write(t, false)
	if err := observation.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	awaitObservedExit(t, custody)
}

func TestRuntimeJournalObservationConcurrentClose(t *testing.T) {
	for iteration := range 8 {
		t.Run(strconv.Itoa(iteration), func(t *testing.T) {
			f, custody := observedActivationFixture(t)
			start := make(chan struct{})
			closed := make(chan error, 1)
			activated := make(chan *RuntimeJournalObservation, 1)
			go func() { <-start; closed <- f.endpoint.Close() }()
			go func() {
				<-start
				observation, _, _ := f.ledger.ActivateObservedJournalWriteGrant(t.Context(), f.plan, f.grant, custody, 50*time.Millisecond, 500*time.Millisecond)
				activated <- observation
			}()
			close(start)
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("endpoint Close deadlocked")
			}
			select {
			case observation := <-activated:
				if observation == nil {
					observation = f.endpoint.observation.Load()
				}
				if observation != nil {
					select {
					case <-observation.Done():
					case <-time.After(5 * time.Second):
						t.Fatal("Close missed concurrent attachment")
					}
					if err := observation.Close(); err != nil {
						t.Fatal(err)
					}
					awaitObservedExit(t, custody)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("concurrent activation deadlocked")
			}
			f.write(t, false)
		})
	}
}

func TestRuntimeJournalObservationPreservesRevocationError(t *testing.T) {
	f, custody := observedActivationFixture(t)
	observation, _, err := f.ledger.ActivateObservedJournalWriteGrant(t.Context(), f.plan, f.grant, custody, time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Destroy the caller-owned custody handle deliberately. Route revocation still
	// succeeds, but sending SIGKILL through the lost descriptor must remain visible.
	if err := custody.observer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := observation.Close(); !errors.Is(err, unix.EBADF) {
		t.Fatalf("lost revocation error: %v", err)
	}
	if !errors.Is(observation.Err(), unix.EBADF) {
		t.Fatalf("lost asynchronous error: %v", observation.Err())
	}
	f.write(t, false)
	awaitObservedExit(t, custody)
}

func TestRuntimeJournalObservationCancellationWithBusyEndpoint(t *testing.T) {
	f, custody := observedActivationFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	observation, _, err := f.ledger.ActivateObservedJournalWriteGrant(ctx, f.plan, f.grant, custody, time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = observation.Close() }()
	f.write(t, true)
	// Hold the same endpoint mutex as Handle/Apply to model blocked journal I/O.
	// Duplicate attachment must not hold custody while waiting for this mutex.
	f.endpoint.mu.Lock()
	var once sync.Once
	release := func() { once.Do(f.endpoint.mu.Unlock) }
	defer release()
	repeated := make(chan error, 1)
	go func() {
		_, _, err := f.ledger.ActivateObservedJournalWriteGrant(ctx, f.plan, f.grant, custody, time.Second, time.Second)
		repeated <- err
	}()
	select {
	case err := <-repeated:
		if err == nil {
			t.Fatal("duplicate observation succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("duplicate observation waited for endpoint I/O")
	}
	cancel()
	awaitObservedExit(t, custody)
	select {
	case <-observation.Done():
	case <-time.After(time.Second):
		t.Fatal("revocation waited for endpoint I/O")
	}
	release()
	f.write(t, false)
}

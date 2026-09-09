package nodeagent

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func runtimeObserverSocketpair(t *testing.T) (*net.UnixConn, *os.File) {
	t.Helper()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	nodeFile, creatorFile := os.NewFile(uintptr(pair[0]), "node-observer-control"), os.NewFile(uintptr(pair[1]), "observer-inherited-control")
	connection, err := net.FileConn(nodeFile)
	_ = nodeFile.Close()
	if err != nil {
		_ = creatorFile.Close()
		t.Fatal(err)
	}
	node := connection.(*net.UnixConn)
	t.Cleanup(func() { _ = node.Close(); _ = creatorFile.Close() })
	return node, creatorFile
}

func TestRuntimeObserverCustody(t *testing.T) {
	if _, err := os.Stat("/exec-probe"); err != nil {
		t.Skip("requires the native observer/probe sandbox")
	}
	for _, scenario := range []string{"held-start-once", "close-before-start", "different-caller", "stopped-check", "queued-check-and-revoke", "channel-loss"} {
		t.Run(scenario, func(t *testing.T) {
			node, creatorFile := runtimeObserverSocketpair(t)
			command := exec.CommandContext(t.Context(), "/exec-observer", "--custody-fd3", "65534", "65534", "/exec-probe")
			command.ExtraFiles = []*os.File{creatorFile}
			observerFD := -1
			command.SysProcAttr = &syscall.SysProcAttr{PidFD: &observerFD}
			input, err := command.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			output, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			var diagnostics bytes.Buffer
			command.Stdout, command.Stderr = writer, &diagnostics
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			_ = writer.Close()
			_ = creatorFile.Close()
			original := os.NewFile(uintptr(observerFD), "original-observer-pidfd")
			defer func() { _ = original.Close() }()
			done := make(chan struct{})
			go func() { _ = command.Wait(); close(done) }()
			t.Cleanup(func() {
				_ = command.Process.Kill()
				<-done
				_ = input.Close()
				_ = output.Close()
				if t.Failed() {
					t.Logf("observer diagnostics: %s", diagnostics.String())
				}
			})
			custody, err := ReceiveRuntimeObserverCustody(t.Context(), node, original)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = custody.Close() }()
			// Custody holds an independent creator pidfd, not the borrowed file.
			if err := original.Close(); err != nil {
				t.Fatal(err)
			}
			if exited, err := custody.TargetExited(t.Context()); err != nil || exited {
				t.Fatalf("held target is not live: %v %v", exited, err)
			}
			if err := output.SetReadDeadline(time.Now().Add(40 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			var early [64]byte
			if count, err := output.Read(early[:]); count != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("target executable ran before custody acknowledgement: %q %v", early[:count], err)
			}
			awaitExit := func() {
				t.Helper()
				deadline := time.Now().Add(5 * time.Second)
				for {
					exited, err := custody.TargetExited(t.Context())
					if err != nil || time.Now().After(deadline) {
						t.Fatalf("no original target exit: %v %v", exited, err)
					}
					if exited {
						return
					}
					time.Sleep(time.Millisecond)
				}
			}
			if scenario == "close-before-start" {
				if err := custody.Revoke(); err != nil {
					t.Fatal(err)
				}
				awaitExit()
				if err := custody.Start(t.Context()); err == nil {
					t.Fatal("revoked offer restarted")
				}
				return
			}
			if err := custody.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := output.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			line, err := bufio.NewReader(output).ReadString('\n')
			if err != nil || !strings.HasPrefix(line, "READY ") {
				t.Fatalf("offered target did not run: %q %v", line, err)
			}
			pid, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "READY ")))
			if err != nil || checkRuntimePIDFD(int(custody.target.Fd()), int32(pid)) != nil {
				t.Fatal("running target differs from offered pidfd")
			}
			if err := custody.Check(t.Context()); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "held-start-once":
				if err := custody.Start(t.Context()); err == nil {
					t.Fatal("second start attempt accepted")
				}
				if err := custody.Check(t.Context()); err != nil {
					t.Fatalf("rejected duplicate Start damaged healthy creator: %v", err)
				}
			case "different-caller":
				connection, _, _ := runtimeCallerConnection(t, "normal", "unixpacket")
				caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = caller.Close() }()
				if err := custody.MatchCaller(t.Context(), caller); err == nil {
					t.Fatal("different original process matched custody")
				}
			case "stopped-check", "queued-check-and-revoke":
				if err := unix.PidfdSendSignal(int(custody.observer.Fd()), unix.SIGSTOP, nil, 0); err != nil {
					t.Fatal(err)
				}
				if scenario == "stopped-check" {
					ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
					defer cancel()
					if err := custody.Check(ctx); !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("stopped creator falsely answered live check: %v", err)
					}
				} else {
					pending := make(chan error, 1)
					go func() { pending <- custody.Check(t.Context()) }()
					deadline := time.Now().Add(time.Second)
					for len(custody.exchangeGate) == 0 {
						if time.Now().After(deadline) {
							t.Fatal("first check did not enter its exchange")
						}
						time.Sleep(time.Millisecond)
					}
					ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
					defer cancel()
					started := time.Now()
					if err := custody.Check(ctx); !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
						t.Fatalf("queued check ignored context: %v", err)
					}
					started = time.Now()
					if err := custody.Revoke(); err != nil || time.Since(started) > time.Second {
						t.Fatalf("revocation waited for the blocked exchange: %v", err)
					}
					select {
					case err := <-pending:
						if err == nil {
							t.Fatal("revoked pending check returned success")
						}
					case <-time.After(time.Second):
						t.Fatal("revocation did not interrupt pending check")
					}
				}
				awaitExit()
				return
			case "channel-loss":
				// Close the Node endpoint itself to exercise creator EOF handling.
				// The protocol owner normally closes through custody.Close.
				if err := node.Close(); err != nil {
					t.Fatal(err)
				}
				awaitExit()
				return
			}
			if _, err := input.Write([]byte("exit\n")); err != nil {
				t.Fatal(err)
			}
			awaitExit()
			if err := custody.Close(); err != nil {
				t.Fatal(err)
			}
			if exited, err := custody.TargetExited(t.Context()); err == nil || exited {
				t.Fatal("closed handle fabricated an exit proof")
			}
		})
	}
}

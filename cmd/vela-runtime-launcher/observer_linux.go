//go:build linux

// vela-runtime-observer attaches to the exact Runtime process whose pidfd is
// supplied by the production launcher. It owns the ptrace backstop and the
// observer custody protocol; it does not issue startup authority.
package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	custodyProtocol = "vela-exec-custody-v1"
	ptraceSeize     = 0x4206
	ptraceInterrupt = 0x4207
	ptraceSetOpts   = 0x4200
	ptraceCont      = 7
	ptraceListen    = 0x4208
	ptraceEventStop = 128
	ptraceOExitKill = 1 << 20
	wall            = 0x40000000
)

func runAttachedObserver(args []string) error {
	// Linux ptrace ownership belongs to the tracer thread, not its Go process.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if os.Geteuid() != 0 || len(args) != 3 || args[0] != "--attach-fd4" {
		return errors.New("vela-runtime-observer requires root and --attach-fd4 UID GID")
	}
	uid, err := parseCredential(args[1])
	if err != nil {
		return err
	}
	gid, err := parseCredential(args[2])
	if err != nil {
		return err
	}
	if _, err := unix.FcntlInt(uintptr(3), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		return fmt.Errorf("protect custody fd: %w", err)
	}
	if _, err := unix.FcntlInt(uintptr(4), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		return fmt.Errorf("protect target pidfd: %w", err)
	}
	targetPID, err := pidFromFD(4)
	if err != nil {
		return err
	}
	if err := verifyTargetCredentials(targetPID, uid, gid); err != nil {
		return err
	}
	if err := seize(targetPID); err != nil {
		return fmt.Errorf("attach runtime target: %w", err)
	}
	defer func() { _ = ptrace(ptraceCont, targetPID, 0, 0) }()
	if err := handleCustody(uid, gid, targetPID); err != nil {
		_ = unix.PidfdSendSignal(4, unix.SIGKILL, nil, 0)
		return err
	}
	return nil
}

func verifyTargetCredentials(pid int, uid, gid uint32) error {
	if pid <= 0 || uid == 0 || gid == 0 {
		return errors.New("observer target credentials are invalid")
	}
	wire, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return fmt.Errorf("read observer target credentials: %w", err)
	}
	var gotUID, gotGID uint64
	var haveUID, haveGID bool
	for _, line := range strings.Split(string(wire), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		switch fields[0] {
		case "Uid:":
			gotUID, err = strconv.ParseUint(fields[2], 10, 32)
			haveUID = err == nil
		case "Gid:":
			gotGID, err = strconv.ParseUint(fields[2], 10, 32)
			haveGID = err == nil
		}
	}
	if !haveUID || !haveGID || gotUID != uint64(uid) || gotGID != uint64(gid) {
		return errors.New("observer target credentials do not match signed Runtime identity")
	}
	return nil
}

func parseCredential(value string) (uint32, error) {
	n, err := strconv.ParseUint(value, 10, 32)
	if err != nil || n == 0 || n == uint64(^uint32(0)) {
		return 0, errors.New("observer credential is invalid")
	}
	return uint32(n), nil
}

func pidFromFD(fd int) (int, error) {
	wire, err := os.ReadFile(fmt.Sprintf("/proc/self/fdinfo/%d", fd))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(wire), "\n") {
		if strings.HasPrefix(line, "Pid:") {
			n, parseErr := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "Pid:")))
			if parseErr == nil && n > 0 {
				return n, nil
			}
		}
	}
	return 0, errors.New("target pidfd has no live PID")
}

func ptrace(request, pid int, addr, data uintptr) error {
	_, _, errno := unix.RawSyscall6(unix.SYS_PTRACE, uintptr(request), uintptr(pid), addr, data, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func seize(pid int) error {
	if err := ptrace(ptraceSeize, pid, 0, ptraceOExitKill); err != nil {
		return err
	}
	if err := ptrace(ptraceInterrupt, pid, 0, 0); err != nil {
		return err
	}
	var status unix.WaitStatus
	if _, err := unix.Wait4(pid, &status, wall, nil); err != nil {
		return err
	}
	if !status.Stopped() {
		return errors.New("runtime target did not enter a ptrace stop")
	}
	return nil
}

func handleCustody(uid, gid uint32, targetPID int) error {
	connection, err := net.FileConn(os.NewFile(3, "runtime-observer-custody"))
	if err != nil {
		return err
	}
	defer connection.Close()
	unixConnection, ok := connection.(*net.UnixConn)
	if !ok {
		return errors.New("custody endpoint is not Unix")
	}
	if err := unixConnection.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	challenge, err := readFrame(unixConnection)
	if err != nil || len(challenge) != len(custodyProtocol)+1+32 || string(challenge[:len(custodyProtocol)]) != custodyProtocol || challenge[len(custodyProtocol)] != 'C' {
		return errors.New("invalid custody challenge")
	}
	// Replace the operation byte without allocating an authority-bearing value.
	offer := append([]byte(nil), challenge...)
	offer[len(custodyProtocol)] = 'O'
	if err := sendFrameWithFD(unixConnection, offer, 4); err != nil {
		return err
	}
	ack, err := readFrame(unixConnection)
	if err != nil || len(ack) != len(offer) || string(ack[:len(custodyProtocol)]) != custodyProtocol || ack[len(custodyProtocol)] != 'A' || !equalNonce(ack, offer) {
		return errors.New("invalid custody acknowledgement")
	}
	// Re-check the credentials immediately before releasing the ptrace/pidfd
	// gate.  The first check happens before seize; keeping this second check
	// closes the small window in which a target could change credentials (for
	// example through an exec transition) while the custody handshake is in
	// flight.  The observer contract is bound to the signed Runtime identity
	// for the entire handoff, not only at attach time.
	if err := verifyTargetCredentials(targetPID, uid, gid); err != nil {
		return fmt.Errorf("observer target credentials changed during custody: %w", err)
	}
	if err := ptrace(ptraceCont, targetPID, 0, 0); err != nil {
		// A concurrent SIGCONT can leave a seized tracee running before the
		// explicit PTRACE_CONT reaches it. Linux reports ESRCH for that case even
		// though the original pidfd is still live. Treat only a dead pidfd as a
		// fatal release failure; the pidfd SIGCONT below remains the authoritative
		// wake-up for the exact process.
		if !errors.Is(err, unix.ESRCH) || unix.PidfdSendSignal(4, 0, nil, 0) != nil {
			return fmt.Errorf("continue runtime target after custody acknowledgement: %w", err)
		}
	}
	if err := unix.PidfdSendSignal(4, unix.SIGCONT, nil, 0); err != nil {
		return fmt.Errorf("release runtime target through pidfd: %w", err)
	}
	ready := append([]byte(nil), offer...)
	ready[len(custodyProtocol)] = 'R'
	if err := writeFrame(unixConnection, ready); err != nil {
		return err
	}
	idleDeadline := time.Now().Add(30 * time.Second)
	for {
		// Keep signal delivery moving on the same locked tracer thread while
		// waiting for Node heartbeats. A live pidfd may name a stopped process.
		if err := continueTraceeEvents(targetPID); err != nil {
			return err
		}
		readDeadline := time.Now().Add(20 * time.Millisecond)
		if idleDeadline.Before(readDeadline) {
			readDeadline = idleDeadline
		}
		if err := unixConnection.SetReadDeadline(readDeadline); err != nil {
			return err
		}
		frame, err := readFrame(unixConnection)
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) && time.Now().Before(idleDeadline) {
				continue
			}
			return err
		}
		// Each Check carries a fresh nonce on the private custody socket. Echo
		// that challenge; it deliberately differs from the handshake nonce.
		if !validLivenessChallenge(frame) {
			return errors.New("invalid custody liveness challenge")
		}
		live := append([]byte(nil), frame...)
		live[len(custodyProtocol)] = 'L'
		if err := unixConnection.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return err
		}
		if err := writeFrame(unixConnection, live); err != nil {
			return err
		}
		idleDeadline = time.Now().Add(30 * time.Second)
	}
}

func continueTraceeEvents(pid int) error {
	for {
		var status unix.WaitStatus
		got, err := unix.Wait4(pid, &status, wall|unix.WNOHANG, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("observe Runtime ptrace event: %w", err)
		}
		if got == 0 {
			return nil
		}
		if status.Exited() || status.Signaled() {
			return errors.New("observed Runtime exited")
		}
		if !status.Stopped() {
			return errors.New("unexpected Runtime ptrace event")
		}
		signal := status.StopSignal()
		request, forwarded := ptraceCont, uintptr(signal)
		if uint32(status)>>16 == ptraceEventStop {
			// Preserve job-control group stops. SIGCONT wakes PTRACE_LISTEN;
			// the resulting synthetic SIGTRAP must not reach the Runtime.
			forwarded = 0
			if signal == unix.SIGSTOP || signal == unix.SIGTSTP || signal == unix.SIGTTIN || signal == unix.SIGTTOU {
				request = ptraceListen
			}
		}
		if err := ptrace(request, pid, 0, forwarded); err != nil {
			return fmt.Errorf("continue Runtime ptrace event: %w", err)
		}
	}
}

func validLivenessChallenge(frame []byte) bool {
	return len(frame) == len(custodyProtocol)+1+32 && string(frame[:len(custodyProtocol)]) == custodyProtocol && frame[len(custodyProtocol)] == 'P'
}

func equalNonce(left, right []byte) bool {
	return len(left) == len(right) && string(left[len(custodyProtocol)+1:]) == string(right[len(custodyProtocol)+1:])
}

func readFrame(connection *net.UnixConn) ([]byte, error) {
	buffer := make([]byte, len(custodyProtocol)+1+32+1)
	n, _, flags, _, err := connection.ReadMsgUnix(buffer, nil)
	if err != nil {
		return nil, err
	}
	if flags&unix.MSG_TRUNC != 0 {
		return nil, errors.New("custody frame truncated")
	}
	return buffer[:n], nil
}

func writeFrame(connection *net.UnixConn, frame []byte) error {
	n, err := connection.Write(frame)
	if err != nil {
		return err
	}
	if n != len(frame) {
		return io.ErrShortWrite
	}
	return nil
}

func sendFrameWithFD(connection *net.UnixConn, frame []byte, fd int) error {
	n, _, err := connection.WriteMsgUnix(frame, unix.UnixRights(fd), nil)
	if err != nil {
		return err
	}
	if n != len(frame) {
		return io.ErrShortWrite
	}
	return nil
}

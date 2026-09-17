//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/runtimechannel"
	"github.com/vivym/vela/internal/strictjson"
	"golang.org/x/sys/unix"
)

const runtimeLauncherProtocolVersion = 2

type runtimeLauncherRequest struct {
	Version           int               `json:"version"`
	Manifest          json.RawMessage   `json:"manifest"`
	ExpectedPod       json.RawMessage   `json:"expected_pod"`
	ExpectedPodDigest [sha256.Size]byte `json:"expected_pod_digest"`
	StartupSocket     string            `json:"startup_socket"`
}

type runtimeLauncherReply struct {
	Version int `json:"version"`
	// Validation-only helpers identify themselves on the wire. The production
	// Node path rejects this marker immediately; production helpers send false.
	ValidationOnly bool                             `json:"validation_only"`
	Target         nodeagent.RuntimeContainerTarget `json:"target"`
	WorkerTarget   nodeagent.RuntimeContainerTarget `json:"worker_target"`
	// The helper returns four SCM_RIGHTS descriptors: runtime pidfd, worker
	// owner pidfd, observer pidfd, observer socket endpoint.
	FDCount int `json:"fd_count"`
}

func (reply runtimeLauncherReply) validate(rightsCount int) error {
	if reply.Version != runtimeLauncherProtocolVersion || reply.FDCount != 4 || rightsCount != 4 {
		return errors.New("runtime launcher handoff schema is invalid")
	}
	if reply.ValidationOnly {
		return errors.New("validation-only runtime launcher cannot enter the production Node startup path")
	}
	if err := reply.Target.Validate(); err != nil {
		return fmt.Errorf("runtime launcher target: %w", err)
	}
	if err := reply.WorkerTarget.Validate(); err != nil {
		return fmt.Errorf("runtime worker target: %w", err)
	}
	return nil
}

type execRuntimeStartupLauncher struct {
	path string
}

func newExecRuntimeStartupLauncher(path string) runtimeStartupLauncher {
	return &execRuntimeStartupLauncher{path: path}
}

func (launcher *execRuntimeStartupLauncher) Launch(ctx context.Context, plan *nodeagent.RuntimeLaunchPlan, startupSocket string) (runtimeStartupLaunch, error) {
	if ctx == nil || plan == nil || launcher == nil || !filepath.IsAbs(launcher.path) || filepath.Clean(launcher.path) != launcher.path || !filepath.IsAbs(startupSocket) || filepath.Clean(startupSocket) != startupSocket {
		return runtimeStartupLaunch{}, errRuntimeStartupLauncherUnavailable
	}
	if err := trustedLauncherBinary(launcher.path); err != nil {
		return runtimeStartupLaunch{}, err
	}
	request, err := encodeRuntimeLauncherRequest(plan, startupSocket)
	if err != nil {
		return runtimeStartupLaunch{}, err
	}
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return runtimeStartupLaunch{}, fmt.Errorf("create runtime launcher control socketpair: %w", err)
	}
	parentFD, childFD := fds[0], fds[1]
	parent := os.NewFile(uintptr(parentFD), "runtime-launcher-control")
	child := os.NewFile(uintptr(childFD), "runtime-launcher-control-child")
	cmd := exec.CommandContext(ctx, launcher.path, "--vela-runtime-launcher-control-fd=3")
	cmd.ExtraFiles = []*os.File{child}
	cmd.Stderr = os.Stderr
	helperPIDFD := -1
	cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &helperPIDFD}
	if err := cmd.Start(); err != nil {
		_ = parent.Close()
		_ = child.Close()
		return runtimeStartupLaunch{}, fmt.Errorf("start runtime launcher helper: %w", err)
	}
	_ = child.Close()
	if helperPIDFD < 0 {
		_ = parent.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return runtimeStartupLaunch{}, errors.New("runtime launcher helper did not provide a pidfd")
	}
	helper := os.NewFile(uintptr(helperPIDFD), "runtime-launcher-helper-pidfd")
	if err := setCloexec(helperPIDFD); err != nil {
		_ = helper.Close()
		_ = parent.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return runtimeStartupLaunch{}, fmt.Errorf("runtime launcher helper pidfd: %w", err)
	}
	control := &runtimeLauncherControl{file: parent, cmd: cmd}
	if err := control.send(ctx, request); err != nil {
		_ = control.Close()
		_ = helper.Close()
		return runtimeStartupLaunch{}, err
	}
	packet, rights, err := control.recv(ctx, 4)
	if err != nil {
		_ = control.Close()
		_ = helper.Close()
		return runtimeStartupLaunch{}, fmt.Errorf("receive runtime launcher handoff: %w", err)
	}
	var reply runtimeLauncherReply
	if err := strictLauncherJSON(packet, &reply); err != nil {
		closeRights(rights)
		_ = control.Close()
		_ = helper.Close()
		return runtimeStartupLaunch{}, fmt.Errorf("runtime launcher handoff schema is invalid: %w", err)
	}
	if err := reply.validate(len(rights)); err != nil {
		closeRights(rights)
		_ = control.Close()
		_ = helper.Close()
		return runtimeStartupLaunch{}, err
	}
	for _, fd := range rights[:3] {
		if err := setCloexec(fd); err != nil {
			closeRights(rights)
			_ = control.Close()
			_ = helper.Close()
			return runtimeStartupLaunch{}, fmt.Errorf("runtime launcher descriptor is not close-on-exec: %w", err)
		}
		if err := runtimechannel.ValidatePIDFD(fd); err != nil {
			closeRights(rights)
			_ = control.Close()
			_ = helper.Close()
			return runtimeStartupLaunch{}, fmt.Errorf("runtime launcher supplied invalid pidfd: %w", err)
		}
	}
	if err := setCloexec(rights[2]); err != nil {
		closeRights(rights)
		_ = control.Close()
		_ = helper.Close()
		return runtimeStartupLaunch{}, fmt.Errorf("runtime launcher observer descriptor is not close-on-exec: %w", err)
	}
	runtimeFD := os.NewFile(uintptr(rights[0]), "runtime-pidfd")
	worker := os.NewFile(uintptr(rights[1]), "runtime-worker-pidfd")
	observer := os.NewFile(uintptr(rights[2]), "runtime-observer-pidfd")
	endpoint := os.NewFile(uintptr(rights[3]), "runtime-observer-socket")
	conn, err := net.FileConn(endpoint)
	_ = endpoint.Close()
	if err != nil {
		_ = runtimeFD.Close()
		_ = worker.Close()
		_ = observer.Close()
		_ = control.Close()
		_ = helper.Close()
		return runtimeStartupLaunch{}, fmt.Errorf("wrap runtime observer socket: %w", err)
	}
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		_ = conn.Close()
		_ = runtimeFD.Close()
		_ = worker.Close()
		_ = observer.Close()
		_ = control.Close()
		_ = helper.Close()
		return runtimeStartupLaunch{}, errors.New("runtime launcher observer endpoint is not Unix")
	}
	return runtimeStartupLaunch{RuntimePIDFD: runtimeFD, WorkerOwnerPIDFD: worker, ObserverPIDFD: observer, LauncherPIDFD: helper, ObserverConn: unixConn, Target: reply.Target, WorkerTarget: reply.WorkerTarget, Close: control.Close}, nil
}

func encodeRuntimeLauncherRequest(plan *nodeagent.RuntimeLaunchPlan, startupSocket string) (runtimeLauncherRequest, error) {
	if plan == nil || !filepath.IsAbs(startupSocket) || filepath.Clean(startupSocket) != startupSocket {
		return runtimeLauncherRequest{}, errRuntimeStartupLauncherUnavailable
	}
	manifest, err := plan.LaunchManifest()
	if err != nil {
		return runtimeLauncherRequest{}, err
	}
	manifestWire, err := json.Marshal(manifest)
	if err != nil {
		return runtimeLauncherRequest{}, err
	}
	expectedPod := plan.ExpectedPod()
	if expectedPod == nil {
		return runtimeLauncherRequest{}, errors.New("runtime launcher verified plan has no expected Pod")
	}
	podWire, err := json.Marshal(expectedPod)
	if err != nil {
		return runtimeLauncherRequest{}, fmt.Errorf("encode runtime launcher expected Pod: %w", err)
	}
	podDigest := sha256.Sum256(podWire)
	return runtimeLauncherRequest{Version: runtimeLauncherProtocolVersion, Manifest: manifestWire, ExpectedPod: podWire, ExpectedPodDigest: podDigest, StartupSocket: startupSocket}, nil
}

type runtimeLauncherControl struct {
	file   *os.File
	cmd    *exec.Cmd
	mu     sync.Mutex
	closed bool
}

func (control *runtimeLauncherControl) send(ctx context.Context, value any) error {
	if ctx == nil {
		return context.Canceled
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.closed || control.file == nil {
		return os.ErrClosed
	}
	wire, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(wire) == 0 || len(wire) > 64<<10 {
		return errors.New("runtime launcher frame is too large")
	}
	fd := int(control.file.Fd())
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		pollfds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT | unix.POLLHUP}}
		_, err := unix.Poll(pollfds, 100)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return err
		}
		if pollfds[0].Revents&unix.POLLHUP != 0 {
			return io.ErrClosedPipe
		}
		if pollfds[0].Revents&unix.POLLOUT == 0 {
			continue
		}
		written, err := unix.Write(fd, wire)
		if err != nil {
			if errors.Is(err, unix.EINTR) || errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
				continue
			}
			return err
		}
		if written != len(wire) {
			return io.ErrShortWrite
		}
		return nil
	}
}

func (control *runtimeLauncherControl) recv(ctx context.Context, expectedRights int) ([]byte, []int, error) {
	if ctx == nil {
		return nil, nil, context.Canceled
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.closed || control.file == nil {
		return nil, nil, os.ErrClosed
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		pollfds := []unix.PollFd{{Fd: int32(control.file.Fd()), Events: unix.POLLIN | unix.POLLHUP}}
		_, err := unix.Poll(pollfds, 100)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return nil, nil, err
		}
		if pollfds[0].Revents&(unix.POLLIN|unix.POLLHUP) != 0 {
			break
		}
	}
	buf := make([]byte, 64<<10)
	oob := make([]byte, unix.CmsgSpace(16*4))
	var n, oobn, flags int
	var err error
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		n, oobn, flags, _, err = unix.Recvmsg(int(control.file.Fd()), buf, oob, unix.MSG_CMSG_CLOEXEC)
		if !errors.Is(err, unix.EINTR) {
			break
		}
	}
	if err != nil {
		return nil, nil, err
	}
	if flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return nil, nil, errors.New("runtime launcher frame was truncated")
	}
	msgs, err := unix.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return nil, nil, err
	}
	var rights []int
	for _, msg := range msgs {
		fds, e := unix.ParseUnixRights(&msg)
		if e != nil {
			return nil, nil, e
		}
		rights = append(rights, fds...)
	}
	if expectedRights >= 0 && len(rights) != expectedRights {
		closeRights(rights)
		return nil, nil, errors.New("runtime launcher descriptor count mismatch")
	}
	return buf[:n], rights, nil
}

func (control *runtimeLauncherControl) Close() error {
	if control == nil {
		return nil
	}
	control.mu.Lock()
	if control.closed {
		control.mu.Unlock()
		return nil
	}
	control.closed = true
	file := control.file
	cmd := control.cmd
	control.file = nil
	control.mu.Unlock()
	if file != nil {
		_ = file.Close()
	}
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case err := <-done:
			if expectedLauncherTermination(err) {
				return nil
			}
			return err
		case <-time.After(2 * time.Second):
			_ = cmd.Process.Kill()
			return <-done
		}
	}
	return nil
}

func expectedLauncherTermination(err error) bool {
	if err == nil {
		return true
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ProcessState == nil {
		return false
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGTERM
}

func strictLauncherJSON(wire []byte, value any) error {
	if err := strictjson.RejectDuplicateKeys(wire); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("trailing runtime launcher data")
	}
	return nil
}

func closeRights(rights []int) {
	for _, fd := range rights {
		_ = unix.Close(fd)
	}
}

func setCloexec(fd int) error {
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil {
		return err
	}
	if flags&unix.FD_CLOEXEC != 0 {
		return nil
	}
	_, err = unix.FcntlInt(uintptr(fd), unix.F_SETFD, flags|unix.FD_CLOEXEC)
	return err
}

func trustedLauncherBinary(path string) error {
	for directory := filepath.Dir(path); ; directory = filepath.Dir(directory) {
		info, err := os.Lstat(directory)
		if err != nil {
			return err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || !info.IsDir() || stat.Uid != 0 || info.Mode()&0o022 != 0 {
			return errors.New("runtime launcher parent directory must be root-owned and non-writable")
		}
		if directory == "/" {
			break
		}
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || stat.Uid != 0 || info.Mode()&0o022 != 0 || info.Mode()&0o111 == 0 {
		return errors.New("runtime launcher helper must be root-owned and non-writable")
	}
	return nil
}

//go:build linux

package artifactvalidator

import (
	"bytes"
	"context"
	"debug/elf"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type productionSandbox struct {
	helper         *os.File
	ffprobe        *os.File
	rootDirectory  string
	maxOutputBytes int64
	maxStderrBytes int64
}

func NewProductionSandbox(config SandboxConfig) (Sandbox, error) {
	if !validSandboxRoot(config.RootDirectory) || config.MaxOutputBytes <= 0 ||
		config.MaxOutputBytes > 16*1024*1024 || config.MaxStderrBytes <= 0 ||
		config.MaxStderrBytes > 1024*1024 {
		return nil, errors.New("invalid production Artifact sandbox configuration")
	}
	helper, err := openSandboxExecutable(config.HelperPath, false)
	if err != nil {
		return nil, errors.New("invalid production Artifact sandbox helper")
	}
	ffprobe, err := openSandboxExecutable(config.FFprobePath, true)
	if err != nil {
		_ = helper.Close()
		return nil, errors.New("invalid production Artifact ffprobe executable")
	}
	return &productionSandbox{
		helper:         helper,
		ffprobe:        ffprobe,
		rootDirectory:  filepath.Clean(config.RootDirectory),
		maxOutputBytes: config.MaxOutputBytes,
		maxStderrBytes: config.MaxStderrBytes,
	}, nil
}

func (sandbox *productionSandbox) Probe(ctx context.Context, input *os.File) (output []byte, err error) {
	if sandbox == nil || sandbox.helper == nil || sandbox.ffprobe == nil || ctx == nil || input == nil {
		return nil, errors.New("production Artifact sandbox is not configured")
	}
	if os.Geteuid() == 0 || os.Getegid() == 0 {
		return nil, errors.New("production Artifact sandbox launcher must run as non-root")
	}
	info, err := input.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return nil, errors.New("artifact sandbox input must be a non-empty regular file")
	}
	if _, err := input.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind Artifact sandbox input: %w", err)
	}
	sandboxRoot, err := os.MkdirTemp(sandbox.rootDirectory, ".vela-ffprobe-sandbox-*")
	if err != nil {
		return nil, fmt.Errorf("create Artifact sandbox root: %w", err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(sandboxRoot); cleanupErr != nil {
			output = nil
			err = errors.Join(err, fmt.Errorf("remove Artifact sandbox root: %w", cleanupErr))
		}
	}()
	if err := os.Chmod(sandboxRoot, 0o700); err != nil {
		return nil, fmt.Errorf("restrict Artifact sandbox root: %w", err)
	}
	helperPath := filepath.Join(sandboxRoot, "artifact-validator-helper")
	if err := stageSandboxExecutable(sandbox.helper, helperPath); err != nil {
		return nil, fmt.Errorf("stage pinned Artifact sandbox helper: %w", err)
	}

	command := exec.CommandContext(
		ctx,
		helperPath,
		"--root", sandboxRoot,
	)
	command.Dir = "/"
	command.Env = []string{"LANG=C", "LC_ALL=C", "TZ=UTC"}
	command.ExtraFiles = []*os.File{input, sandbox.ffprobe, sandbox.helper}
	command.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: unix.CLONE_NEWUSER |
			unix.CLONE_NEWNET |
			unix.CLONE_NEWNS |
			unix.CLONE_NEWPID |
			unix.CLONE_NEWIPC |
			unix.CLONE_NEWUTS,
		UidMappings: []syscall.SysProcIDMap{{
			ContainerID: 0,
			HostID:      os.Geteuid(),
			Size:        1,
		}},
		GidMappingsEnableSetgroups: false,
		GidMappings: []syscall.SysProcIDMap{{
			ContainerID: 0,
			HostID:      os.Getegid(),
			Size:        1,
		}},
		Credential: &syscall.Credential{Uid: 0, Gid: 0},
		Pdeathsig:  syscall.SIGKILL,
	}
	stdout := newBoundedBuffer(sandbox.maxOutputBytes)
	stderr := newBoundedBuffer(sandbox.maxStderrBytes)
	command.Stdout = stdout
	command.Stderr = stderr
	if err := command.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if stdout.overflowed() || stderr.overflowed() {
			return nil, errors.New("sandboxed ffprobe output exceeded configured bounds")
		}
		return nil, fmt.Errorf(
			"sandboxed ffprobe failed: %w: %s",
			err,
			stderr.String(),
		)
	}
	if stdout.overflowed() || stderr.overflowed() {
		return nil, errors.New("sandboxed ffprobe output exceeded configured bounds")
	}
	return stdout.Bytes(), nil
}

func stageSandboxExecutable(source *os.File, destination string) (err error) {
	info, err := source.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return errors.New("pinned sandbox executable is invalid")
	}
	staged, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o500)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := staged.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close staged sandbox executable: %w", closeErr))
		}
		if err != nil {
			if removeErr := os.Remove(destination); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				err = errors.Join(err, fmt.Errorf("remove staged sandbox executable: %w", removeErr))
			}
		}
	}()
	written, err := io.Copy(staged, io.NewSectionReader(source, 0, info.Size()))
	if err != nil {
		return err
	}
	if written != info.Size() {
		return errors.New("staged sandbox executable size is incomplete")
	}
	if err := staged.Chmod(0o500); err != nil {
		return err
	}
	return staged.Sync()
}

func openSandboxExecutable(path string, requireStatic bool) (*os.File, error) {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) {
		return nil, errors.New("sandbox executable path must be absolute")
	}
	descriptor, err := unix.Open(cleaned, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(descriptor), cleaned)
	if file == nil {
		_ = unix.Close(descriptor)
		return nil, errors.New("open sandbox executable descriptor")
	}
	if !validSandboxExecutableFile(file, requireStatic) {
		_ = file.Close()
		return nil, errors.New("sandbox executable descriptor is invalid")
	}
	return file, nil
}

func validSandboxExecutableFile(file *os.File, requireStatic bool) bool {
	if file == nil {
		return false
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 ||
		info.Mode().Perm()&0o111 == 0 {
		return false
	}
	if !requireStatic {
		return true
	}
	binary, err := elf.NewFile(file)
	if err != nil {
		return false
	}
	defer func() { _ = binary.Close() }()
	libraries, err := binary.ImportedLibraries()
	return err == nil && len(libraries) == 0
}

func validSandboxRoot(path string) bool {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) {
		return false
	}
	info, err := os.Stat(cleaned)
	return err == nil && info.IsDir() && info.Mode().Perm()&0o022 == 0
}

type boundedBuffer struct {
	mu       sync.Mutex
	buffer   bytes.Buffer
	limit    int64
	overflow bool
}

func newBoundedBuffer(limit int64) *boundedBuffer {
	return &boundedBuffer{limit: limit}
}

func (buffer *boundedBuffer) Write(content []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	remaining := buffer.limit - int64(buffer.buffer.Len())
	if int64(len(content)) > remaining {
		if remaining > 0 {
			_, _ = buffer.buffer.Write(content[:remaining])
		}
		buffer.overflow = true
		return len(content), nil
	}
	return buffer.buffer.Write(content)
}

func (buffer *boundedBuffer) Bytes() []byte {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return append([]byte(nil), buffer.buffer.Bytes()...)
}

func (buffer *boundedBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.String()
}

func (buffer *boundedBuffer) overflowed() bool {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.overflow
}

func RunSandboxHelper(arguments []string) error {
	sandboxRoot, err := parseSandboxHelperArguments(arguments)
	if err != nil {
		return err
	}
	if os.Geteuid() != 0 || os.Getegid() != 0 {
		return errors.New("artifact sandbox helper did not enter the isolated user namespace")
	}
	input := os.NewFile(uintptr(3), "artifact-input")
	if err := validateInheritedSandboxDescriptor(input); err != nil {
		return fmt.Errorf("artifact sandbox input descriptor is invalid: %w", err)
	}
	ffprobe := os.NewFile(uintptr(4), "ffprobe")
	if err := validateInheritedSandboxDescriptor(ffprobe); err != nil {
		return fmt.Errorf("artifact sandbox ffprobe descriptor is invalid: %w", err)
	}
	helper := os.NewFile(uintptr(5), "artifact-validator-helper")
	if helper == nil {
		return errors.New("artifact sandbox helper descriptor is missing")
	}
	if err := helper.Close(); err != nil {
		return fmt.Errorf("close Artifact sandbox helper descriptor: %w", err)
	}
	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		return fmt.Errorf("make Artifact sandbox mounts private: %w", err)
	}
	if err := unix.Mount(
		"tmpfs",
		sandboxRoot,
		"tmpfs",
		unix.MS_NOSUID|unix.MS_NODEV,
		"size=8388608,mode=0755",
	); err != nil {
		return fmt.Errorf("mount Artifact sandbox root: %w", err)
	}
	if err := unix.Chroot(sandboxRoot); err != nil {
		return fmt.Errorf("enter Artifact sandbox root: %w", err)
	}
	if err := os.Chdir("/"); err != nil {
		return fmt.Errorf("set Artifact sandbox directory: %w", err)
	}
	if err := applySandboxRlimits(); err != nil {
		return err
	}
	if err := unix.Sethostname([]byte("vela-artifact-validator")); err != nil {
		return fmt.Errorf("set Artifact sandbox hostname: %w", err)
	}
	for capability := 0; capability <= unix.CAP_LAST_CAP; capability++ {
		if capability == unix.CAP_SETPCAP {
			continue
		}
		if err := unix.Prctl(unix.PR_CAPBSET_DROP, uintptr(capability), 0, 0, 0); err != nil &&
			!errors.Is(err, unix.EINVAL) {
			return fmt.Errorf("drop Artifact sandbox capability %d: %w", capability, err)
		}
	}
	if err := unix.Prctl(unix.PR_CAPBSET_DROP, unix.CAP_SETPCAP, 0, 0, 0); err != nil {
		return fmt.Errorf("drop Artifact sandbox CAP_SETPCAP: %w", err)
	}
	capabilities := [2]unix.CapUserData{}
	if err := unix.Capset(
		&unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3},
		&capabilities[0],
	); err != nil {
		return fmt.Errorf("clear Artifact sandbox capabilities: %w", err)
	}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("set Artifact sandbox no_new_privs: %w", err)
	}
	unix.CloseOnExec(int(ffprobe.Fd()))
	err = execSandboxFFprobe(
		int(ffprobe.Fd()),
		append([]string{"ffprobe"}, ffprobeDescriptorArguments(int(input.Fd()))...),
		[]string{"LANG=C", "LC_ALL=C", "TZ=UTC"},
	)
	runtime.KeepAlive(input)
	runtime.KeepAlive(ffprobe)
	return err
}

func validateInheritedSandboxDescriptor(file *os.File) error {
	if file == nil {
		return errors.New("descriptor is missing")
	}
	_, err := unix.FcntlInt(file.Fd(), unix.F_GETFD, 0)
	if err != nil {
		return fmt.Errorf("inspect descriptor: %w", err)
	}
	return nil
}

func execSandboxFFprobe(fd int, arguments []string, environment []string) error {
	emptyPath, err := syscall.BytePtrFromString("")
	if err != nil {
		return fmt.Errorf("encode Artifact sandbox executable path: %w", err)
	}
	argumentPointers, err := syscall.SlicePtrFromStrings(arguments)
	if err != nil {
		return fmt.Errorf("encode Artifact sandbox arguments: %w", err)
	}
	environmentPointers, err := syscall.SlicePtrFromStrings(environment)
	if err != nil {
		return fmt.Errorf("encode Artifact sandbox environment: %w", err)
	}
	_, _, errno := unix.RawSyscall6(
		unix.SYS_EXECVEAT,
		uintptr(fd),
		uintptr(unsafe.Pointer(emptyPath)),
		uintptr(unsafe.Pointer(&argumentPointers[0])),
		uintptr(unsafe.Pointer(&environmentPointers[0])),
		unix.AT_EMPTY_PATH,
		0,
	)
	runtime.KeepAlive(emptyPath)
	runtime.KeepAlive(argumentPointers)
	runtime.KeepAlive(environmentPointers)
	if errno != 0 {
		return fmt.Errorf("execute pinned Artifact ffprobe descriptor: %w", errno)
	}
	return errors.New("pinned Artifact ffprobe descriptor returned without execution")
}

func ffprobeDescriptorArguments(fd int) []string {
	arguments := ffprobeArgumentsWithProtocol("fd:", "fd")
	return append(
		arguments[:len(arguments)-1],
		"-fd", strconv.Itoa(fd), arguments[len(arguments)-1],
	)
}

func parseSandboxHelperArguments(arguments []string) (string, error) {
	if len(arguments) != 2 || arguments[0] != "--root" {
		return "", errors.New("invalid Artifact sandbox helper arguments")
	}
	sandboxRoot := filepath.Clean(arguments[1])
	if !filepath.IsAbs(sandboxRoot) || sandboxRoot == "/" {
		return "", errors.New("invalid Artifact sandbox helper path")
	}
	return sandboxRoot, nil
}

func applySandboxRlimits() error {
	limits := []struct {
		resource int
		value    uint64
	}{
		{resource: unix.RLIMIT_CPU, value: 5},
		{resource: unix.RLIMIT_AS, value: 512 * 1024 * 1024},
		{resource: unix.RLIMIT_DATA, value: 512 * 1024 * 1024},
		{resource: unix.RLIMIT_STACK, value: 64 * 1024 * 1024},
		{resource: unix.RLIMIT_FSIZE, value: 1024 * 1024},
		{resource: unix.RLIMIT_NPROC, value: 32},
		{resource: unix.RLIMIT_NOFILE, value: 32},
		{resource: unix.RLIMIT_CORE, value: 0},
	}
	for _, limit := range limits {
		if err := unix.Setrlimit(limit.resource, &unix.Rlimit{
			Cur: limit.value,
			Max: limit.value,
		}); err != nil {
			return fmt.Errorf(
				"set Artifact sandbox rlimit %s: %w",
				strconv.Itoa(limit.resource),
				err,
			)
		}
	}
	return nil
}

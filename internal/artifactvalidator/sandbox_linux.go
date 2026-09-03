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
	"strings"
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
		if chmodErr := os.Chmod(sandboxRoot, 0o700); chmodErr != nil &&
			!errors.Is(chmodErr, os.ErrNotExist) {
			output = nil
			err = errors.Join(err, fmt.Errorf("unlock Artifact sandbox root for removal: %w", chmodErr))
		}
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
	ffprobePath := filepath.Join(sandboxRoot, "artifact-ffprobe")
	if err := stageSandboxExecutable(sandbox.ffprobe, ffprobePath); err != nil {
		return nil, fmt.Errorf("stage pinned Artifact ffprobe: %w", err)
	}
	inputPath := filepath.Join(sandboxRoot, "artifact-input")
	if err := stageSandboxFile(input, inputPath, 0o400); err != nil {
		return nil, fmt.Errorf("stage pinned Artifact input: %w", err)
	}

	command := exec.CommandContext(
		ctx,
		helperPath,
		"--root", sandboxRoot,
	)
	command.Dir = "/"
	command.Env = []string{"LANG=C", "LC_ALL=C", "TZ=UTC"}
	command.ExtraFiles = []*os.File{sandbox.helper}
	sandboxUID := os.Geteuid()
	sandboxGID := os.Getegid()
	command.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: unix.CLONE_NEWUSER |
			unix.CLONE_NEWNET |
			unix.CLONE_NEWNS |
			unix.CLONE_NEWPID |
			unix.CLONE_NEWIPC |
			unix.CLONE_NEWUTS,
		UidMappings: []syscall.SysProcIDMap{{
			ContainerID: sandboxUID,
			HostID:      sandboxUID,
			Size:        1,
		}},
		GidMappingsEnableSetgroups: false,
		GidMappings: []syscall.SysProcIDMap{{
			ContainerID: sandboxGID,
			HostID:      sandboxGID,
			Size:        1,
		}},
		Pdeathsig: syscall.SIGKILL,
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
	return stageSandboxFile(source, destination, 0o500)
}

func stageSandboxFile(source *os.File, destination string, mode os.FileMode) (err error) {
	info, err := source.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return errors.New("pinned sandbox file is invalid")
	}
	if mode != 0o400 && mode != 0o500 {
		return errors.New("pinned sandbox file mode is invalid")
	}
	staged, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := staged.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close staged sandbox file: %w", closeErr))
		}
		if err != nil {
			if removeErr := os.Remove(destination); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				err = errors.Join(err, fmt.Errorf("remove staged sandbox file: %w", removeErr))
			}
		}
	}()
	written, err := io.Copy(staged, io.NewSectionReader(source, 0, info.Size()))
	if err != nil {
		return err
	}
	if written != info.Size() {
		return errors.New("staged sandbox file size is incomplete")
	}
	if err := staged.Chmod(mode); err != nil {
		return err
	}
	return staged.Sync()
}

func openSandboxInput(path string) (*os.File, error) {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) {
		return nil, errors.New("sandbox input path must be absolute")
	}
	descriptor, err := unix.Open(cleaned, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(descriptor), cleaned)
	if file == nil {
		_ = unix.Close(descriptor)
		return nil, errors.New("open sandbox input descriptor")
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o400 || info.Size() <= 0 {
		_ = file.Close()
		return nil, errors.New("sandbox input descriptor is invalid")
	}
	return file, nil
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
	runtime.LockOSThread()
	threadRestricted := false
	defer func() {
		if !threadRestricted {
			runtime.UnlockOSThread()
		}
	}()

	sandboxRoot, err := parseSandboxHelperArguments(arguments)
	if err != nil {
		return err
	}
	if err := validateSandboxIdentity(); err != nil {
		return err
	}
	helper := os.NewFile(uintptr(3), "artifact-validator-helper")
	if helper == nil {
		return errors.New("artifact sandbox helper descriptor is missing")
	}
	if err := helper.Close(); err != nil {
		return fmt.Errorf("close Artifact sandbox helper descriptor: %w", err)
	}
	if err := prepareSandboxRoot(sandboxRoot); err != nil {
		return err
	}
	ffprobe, err := openSandboxExecutable(filepath.Join(sandboxRoot, "artifact-ffprobe"), true)
	if err != nil {
		return errors.New("staged Artifact ffprobe is invalid")
	}
	defer func() { _ = ffprobe.Close() }()
	input, err := openSandboxInput(filepath.Join(sandboxRoot, "artifact-input"))
	if err != nil {
		return errors.New("staged Artifact input is invalid")
	}
	defer func() { _ = input.Close() }()
	if err := os.Chdir(sandboxRoot); err != nil {
		return fmt.Errorf("set Artifact sandbox directory: %w", err)
	}
	if err := applySandboxRlimits(); err != nil {
		return err
	}
	// Capability, no_new_privs, and Landlock changes are thread-scoped and
	// irreversible. Keep this goroutine pinned until exec or process exit.
	threadRestricted = true
	if err := clearSandboxCapabilities(); err != nil {
		return err
	}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("set Artifact sandbox no_new_privs: %w", err)
	}
	if err := restrictSandboxFilesystem(ffprobe, input); err != nil {
		return err
	}
	if err := ffprobe.Close(); err != nil {
		return fmt.Errorf("close staged Artifact ffprobe descriptor: %w", err)
	}
	if err := input.Close(); err != nil {
		return fmt.Errorf("close staged Artifact input descriptor: %w", err)
	}
	err = execSandboxFFprobe(
		"./artifact-ffprobe",
		append([]string{"ffprobe"}, ffprobeArguments("artifact-input")...),
		[]string{"LANG=C", "LC_ALL=C", "TZ=UTC"},
	)
	runtime.KeepAlive(input)
	runtime.KeepAlive(ffprobe)
	return err
}

func prepareSandboxRoot(sandboxRoot string) error {
	helperPath := filepath.Join(sandboxRoot, "artifact-validator-helper")
	if err := os.Remove(helperPath); err != nil {
		return fmt.Errorf("remove staged Artifact sandbox helper: %w", err)
	}
	entries, err := os.ReadDir(sandboxRoot)
	if err != nil {
		return fmt.Errorf("inspect Artifact sandbox root: %w", err)
	}
	if len(entries) != 2 {
		return errors.New("artifact sandbox root contains an unexpected entry")
	}
	expected := map[string]os.FileMode{"artifact-ffprobe": 0o500, "artifact-input": 0o400}
	for _, entry := range entries {
		mode, exists := expected[entry.Name()]
		info, infoErr := entry.Info()
		if !exists || infoErr != nil || !info.Mode().IsRegular() || info.Mode().Perm() != mode {
			return errors.New("artifact sandbox root contains an unexpected entry")
		}
	}
	if err := os.Chmod(sandboxRoot, 0o500); err != nil {
		return fmt.Errorf("lock Artifact sandbox root: %w", err)
	}
	return nil
}

func validateSandboxIdentity() error {
	uid := os.Geteuid()
	gid := os.Getegid()
	if uid <= 0 || gid <= 0 {
		return errors.New("artifact sandbox helper must remain non-root")
	}
	for _, identity := range []struct {
		path string
		id   int
	}{
		{path: "/proc/self/uid_map", id: uid},
		{path: "/proc/self/gid_map", id: gid},
	} {
		mapping, err := os.ReadFile(identity.path)
		if err != nil {
			return fmt.Errorf("read Artifact sandbox identity map: %w", err)
		}
		if !sandboxIDMapMatches(mapping, identity.id) {
			return errors.New("artifact sandbox helper has an unexpected identity map")
		}
	}
	return nil
}

func sandboxIDMapMatches(mapping []byte, id int) bool {
	fields := strings.Fields(string(mapping))
	if len(fields) != 3 {
		return false
	}
	containerID, containerErr := strconv.Atoi(fields[0])
	hostID, hostErr := strconv.Atoi(fields[1])
	size, sizeErr := strconv.Atoi(fields[2])
	return containerErr == nil && hostErr == nil && sizeErr == nil &&
		containerID == id && hostID == id && size == 1
}

func clearSandboxCapabilities() error {
	capabilities := [2]unix.CapUserData{}
	header := unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
	if err := unix.Capset(&header, &capabilities[0]); err != nil {
		return fmt.Errorf("clear Artifact sandbox capabilities: %w", err)
	}
	if err := unix.Prctl(unix.PR_CAP_AMBIENT, unix.PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0); err != nil {
		return fmt.Errorf("clear Artifact sandbox ambient capabilities: %w", err)
	}
	if err := unix.Capget(&header, &capabilities[0]); err != nil {
		return fmt.Errorf("verify cleared Artifact sandbox capabilities: %w", err)
	}
	for index, capabilitySet := range capabilities {
		if capabilitySet.Effective != 0 || capabilitySet.Permitted != 0 || capabilitySet.Inheritable != 0 {
			return fmt.Errorf("artifact sandbox capability word %d remains nonzero", index)
		}
	}
	return nil
}

func restrictSandboxFilesystem(ffprobe *os.File, inputs ...*os.File) error {
	abi, err := landlockCreateRuleset(nil, 0, unix.LANDLOCK_CREATE_RULESET_VERSION)
	if err != nil {
		return fmt.Errorf("query Artifact sandbox Landlock ABI: %w", err)
	}
	if abi < 1 {
		return errors.New("artifact sandbox Landlock ABI is unavailable")
	}
	access := landlockFilesystemAccessMask(abi)
	ruleset, err := landlockCreateRuleset(
		&unix.LandlockRulesetAttr{Access_fs: access},
		unsafe.Sizeof(unix.LandlockRulesetAttr{}),
		0,
	)
	if err != nil {
		return fmt.Errorf("create Artifact sandbox Landlock ruleset: %w", err)
	}
	defer func() { _ = unix.Close(ruleset) }()

	allowed := unix.LandlockPathBeneathAttr{
		Allowed_access: unix.LANDLOCK_ACCESS_FS_EXECUTE | unix.LANDLOCK_ACCESS_FS_READ_FILE,
		Parent_fd:      int32(ffprobe.Fd()),
	}
	if err := landlockAddPathRule(ruleset, &allowed); err != nil {
		return fmt.Errorf("allow pinned Artifact ffprobe in Landlock ruleset: %w", err)
	}
	for _, input := range inputs {
		if input == nil {
			return errors.New("artifact sandbox input is missing from Landlock ruleset")
		}
		allowedInput := unix.LandlockPathBeneathAttr{
			Allowed_access: unix.LANDLOCK_ACCESS_FS_READ_FILE,
			Parent_fd:      int32(input.Fd()),
		}
		if err := landlockAddPathRule(ruleset, &allowedInput); err != nil {
			return fmt.Errorf("allow pinned Artifact input in Landlock ruleset: %w", err)
		}
	}
	if err := landlockRestrictSelf(ruleset); err != nil {
		return fmt.Errorf("enforce Artifact sandbox Landlock ruleset: %w", err)
	}
	return nil
}

func landlockFilesystemAccessMask(abi int) uint64 {
	access := uint64(
		unix.LANDLOCK_ACCESS_FS_EXECUTE |
			unix.LANDLOCK_ACCESS_FS_WRITE_FILE |
			unix.LANDLOCK_ACCESS_FS_READ_FILE |
			unix.LANDLOCK_ACCESS_FS_READ_DIR |
			unix.LANDLOCK_ACCESS_FS_REMOVE_DIR |
			unix.LANDLOCK_ACCESS_FS_REMOVE_FILE |
			unix.LANDLOCK_ACCESS_FS_MAKE_CHAR |
			unix.LANDLOCK_ACCESS_FS_MAKE_DIR |
			unix.LANDLOCK_ACCESS_FS_MAKE_REG |
			unix.LANDLOCK_ACCESS_FS_MAKE_SOCK |
			unix.LANDLOCK_ACCESS_FS_MAKE_FIFO |
			unix.LANDLOCK_ACCESS_FS_MAKE_BLOCK |
			unix.LANDLOCK_ACCESS_FS_MAKE_SYM,
	)
	if abi >= 2 {
		access |= unix.LANDLOCK_ACCESS_FS_REFER
	}
	if abi >= 3 {
		access |= unix.LANDLOCK_ACCESS_FS_TRUNCATE
	}
	if abi >= 5 {
		access |= unix.LANDLOCK_ACCESS_FS_IOCTL_DEV
	}
	return access
}

func landlockCreateRuleset(attr *unix.LandlockRulesetAttr, size uintptr, flags int) (int, error) {
	pointer := uintptr(0)
	if attr != nil {
		pointer = uintptr(unsafe.Pointer(attr))
	}
	result, _, errno := unix.Syscall(
		unix.SYS_LANDLOCK_CREATE_RULESET,
		pointer,
		size,
		uintptr(flags),
	)
	runtime.KeepAlive(attr)
	if errno != 0 {
		return 0, errno
	}
	return int(result), nil
}

func landlockAddPathRule(ruleset int, attr *unix.LandlockPathBeneathAttr) error {
	_, _, errno := unix.Syscall6(
		unix.SYS_LANDLOCK_ADD_RULE,
		uintptr(ruleset),
		unix.LANDLOCK_RULE_PATH_BENEATH,
		uintptr(unsafe.Pointer(attr)),
		0,
		0,
		0,
	)
	runtime.KeepAlive(attr)
	if errno != 0 {
		return errno
	}
	return nil
}

func landlockRestrictSelf(ruleset int) error {
	_, _, errno := unix.Syscall(unix.SYS_LANDLOCK_RESTRICT_SELF, uintptr(ruleset), 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func execSandboxFFprobe(path string, arguments []string, environment []string) error {
	if err := unix.Exec(path, arguments, environment); err != nil {
		return fmt.Errorf("execute staged pinned Artifact ffprobe: %w", err)
	}
	return errors.New("staged pinned Artifact ffprobe returned without execution")
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

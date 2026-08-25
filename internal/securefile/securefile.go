package securefile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// Read returns a bounded regular file owned by the effective service user.
// The path itself must not be a symlink, and replacements between lstat and
// open are rejected.
func Read(path string, maxBytes int64, private bool) ([]byte, error) {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned != path || maxBytes <= 0 {
		return nil, errors.New("secure file path or size bound is invalid")
	}
	pathInfo, err := os.Lstat(cleaned)
	if err != nil {
		return nil, err
	}
	if err := validateRegular(pathInfo, maxBytes, private, false); err != nil {
		return nil, err
	}
	file, err := os.Open(cleaned)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	openedInfo, err := file.Stat()
	if err != nil || !os.SameFile(pathInfo, openedInfo) {
		return nil, errors.New("secure file changed while it was opened")
	}
	if err := validateRegular(openedInfo, maxBytes, private, false); err != nil {
		return nil, err
	}
	content, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(content) == 0 || int64(len(content)) > maxBytes {
		return nil, errors.New("secure file exceeds its configured bound")
	}
	return content, nil
}

func ValidateDirectory(path string) error {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned != path {
		return errors.New("secure directory path is invalid")
	}
	info, err := os.Lstat(cleaned)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0o022 != 0 || !ownedByTrustedUser(info) {
		return errors.New("secure directory owner, type, or permissions are invalid")
	}
	return nil
}

// ResolveTrustedDirectory returns a canonical directory path after validating
// every ancestor against the executable-path trust contract.
func ResolveTrustedDirectory(path string) (string, error) {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned != path {
		return "", errors.New("secure directory path is invalid")
	}
	resolved, err := filepath.EvalSymlinks(cleaned)
	if err != nil {
		return "", err
	}
	directory, err := openTrustedDirectory(resolved)
	if err != nil {
		return "", err
	}
	if err := directory.Close(); err != nil {
		return "", fmt.Errorf("close secure directory: %w", err)
	}
	return resolved, nil
}

// OpenTrustedDirectory returns a descriptor reached without following any
// component symlink. Callers can use openat to keep subsequent file access
// bound to the validated directory inode.
func OpenTrustedDirectory(path string) (*os.File, error) {
	return openTrustedDirectory(path)
}

func ValidateExecutable(path string) error {
	file, err := OpenExecutable(path)
	if err != nil {
		return err
	}
	return file.Close()
}

// OpenExecutable returns a validated executable inode rather than a pathname
// that would need to be resolved again by the caller.
func OpenExecutable(path string) (*os.File, error) {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned != path {
		return nil, errors.New("secure executable path is invalid")
	}
	pathInfo, err := os.Lstat(cleaned)
	if err != nil {
		return nil, err
	}
	if err := validateExecutable(pathInfo); err != nil {
		return nil, errors.New("secure executable owner, type, or permissions are invalid")
	}
	resolvedParent, err := filepath.EvalSymlinks(filepath.Dir(cleaned))
	if err != nil {
		return nil, err
	}
	parent, err := openTrustedDirectory(resolvedParent)
	if err != nil {
		return nil, err
	}
	defer func() { _ = parent.Close() }()
	fd, err := unix.Openat(int(parent.Fd()), filepath.Base(cleaned), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), cleaned)
	openedInfo, err := file.Stat()
	if err != nil || !os.SameFile(pathInfo, openedInfo) || validateExecutable(openedInfo) != nil {
		_ = file.Close()
		return nil, errors.New("secure executable changed while it was opened")
	}
	return file, nil
}

func openTrustedDirectory(path string) (*os.File, error) {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned != path {
		return nil, errors.New("secure executable directory path is invalid")
	}
	fd, err := unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open secure executable root directory: %w", err)
	}
	current := os.NewFile(uintptr(fd), string(filepath.Separator))
	closeOnError := func(err error) (*os.File, error) {
		_ = current.Close()
		return nil, err
	}
	if info, statErr := current.Stat(); statErr != nil || validateTrustedDirectory(info) != nil {
		return closeOnError(errors.New("secure executable root directory is not trusted"))
	}
	if cleaned == string(filepath.Separator) {
		return current, nil
	}
	for _, component := range strings.Split(strings.TrimPrefix(cleaned, string(filepath.Separator)), string(filepath.Separator)) {
		nextFD, openErr := unix.Openat(int(current.Fd()), component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if openErr != nil {
			return closeOnError(fmt.Errorf("open secure executable ancestor %q: %w", component, openErr))
		}
		next := os.NewFile(uintptr(nextFD), filepath.Join(current.Name(), component))
		info, statErr := next.Stat()
		if statErr != nil || validateTrustedDirectory(info) != nil {
			_ = next.Close()
			return closeOnError(fmt.Errorf("secure executable ancestor %q is not trusted", component))
		}
		_ = current.Close()
		current = next
	}
	return current, nil
}

func OpenPrivateState(path string) (*os.File, error) {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned != path {
		return nil, errors.New("private state path is invalid")
	}
	file, err := os.OpenFile(cleaned, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || validateRegular(info, int64(^uint64(0)>>1), true, true) != nil {
		_ = file.Close()
		return nil, errors.New("private state owner, type, or permissions are invalid")
	}
	return file, nil
}

func validateRegular(info os.FileInfo, maxBytes int64, private, allowEmpty bool) error {
	if info == nil || !info.Mode().IsRegular() || !allowEmpty && info.Size() <= 0 || info.Size() > maxBytes ||
		info.Mode().Perm()&0o022 != 0 || private && info.Mode().Perm()&0o077 != 0 ||
		!ownedByTrustedUser(info) {
		return errors.New("secure file owner, type, permissions, or size are invalid")
	}
	return nil
}

func validateExecutable(info os.FileInfo) error {
	if err := validateRegular(info, int64(^uint64(0)>>1), false, false); err != nil || info.Mode().Perm()&0o111 == 0 {
		return errors.New("secure executable owner, type, or permissions are invalid")
	}
	return nil
}

func validateTrustedDirectory(info os.FileInfo) error {
	if info == nil || !info.IsDir() || !ownedByTrustedUser(info) {
		return errors.New("secure executable directory owner, type, or permissions are invalid")
	}
	// Root-owned sticky directories such as /tmp prevent other principals from
	// replacing an entry they do not own; every descendant is still validated.
	if info.Mode().Perm()&0o022 != 0 && (info.Mode()&os.ModeSticky == 0 || !ownedByRoot(info)) {
		return errors.New("secure executable directory owner, type, or permissions are invalid")
	}
	return nil
}

func ownedByRoot(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0
}

func ownedByTrustedUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (stat.Uid == 0 || int(stat.Uid) == os.Geteuid())
}

package securefile

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
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

func ValidateExecutable(path string) error {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned != path {
		return errors.New("secure executable path is invalid")
	}
	info, err := os.Lstat(cleaned)
	if err != nil {
		return err
	}
	if err := validateRegular(info, int64(^uint64(0)>>1), false, false); err != nil || info.Mode().Perm()&0o111 == 0 {
		return errors.New("secure executable owner, type, or permissions are invalid")
	}
	return nil
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

func ownedByTrustedUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (stat.Uid == 0 || int(stat.Uid) == os.Geteuid())
}

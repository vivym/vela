package supplychain

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

type rootedFS struct {
	directory *os.File
	name      string
}

func openAbsoluteRoot(directory string) (*rootedFS, error) {
	if !canonicalAbsolutePath(directory) {
		return nil, errors.New("root path must be canonical and absolute")
	}
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return nil, err
	}
	directory = resolved
	root, err := openRootedFS(string(filepath.Separator))
	if err != nil {
		return nil, err
	}
	if directory == string(filepath.Separator) {
		return root, nil
	}
	normalized := strings.TrimPrefix(directory, string(filepath.Separator))
	opened, err := root.openRoot(normalized)
	_ = root.Close()
	return opened, err
}

func readAbsoluteFile(path string, maximum int64) ([]byte, error) {
	if !canonicalAbsolutePath(path) {
		return nil, errors.New("file path must be canonical and absolute")
	}
	root, err := openAbsoluteRoot(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	file, err := root.openFile(filepath.Base(path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	information, err := file.Stat()
	if err != nil || !information.Mode().IsRegular() || information.Size() <= 0 || information.Size() > maximum {
		return nil, fmt.Errorf("file must be a regular file of 1..%d bytes", maximum)
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, fmt.Errorf("read bounded file: %w", err)
	}
	if len(encoded) == 0 || int64(len(encoded)) > maximum {
		return nil, fmt.Errorf("file content must be in 1..%d bytes", maximum)
	}
	return encoded, nil
}

func openRootedFS(name string) (*rootedFS, error) {
	fd, err := unix.Open(name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return &rootedFS{directory: os.NewFile(uintptr(fd), name), name: name}, nil
}

func (root *rootedFS) Close() error {
	if root == nil || root.directory == nil {
		return nil
	}
	return root.directory.Close()
}

func (root *rootedFS) openRoot(normalized string) (*rootedFS, error) {
	file, err := root.open(normalized, true)
	if err != nil {
		return nil, err
	}
	return &rootedFS{directory: file, name: filepath.Join(root.name, normalized)}, nil
}

func (root *rootedFS) openFile(normalized string) (*os.File, error) {
	return root.open(normalized, false)
}

func (root *rootedFS) open(normalized string, directory bool) (*os.File, error) {
	components := strings.Split(normalized, string(filepath.Separator))
	currentFD := int(root.directory.Fd())
	ownedFD := -1
	closeOwned := func() {
		if ownedFD >= 0 {
			_ = unix.Close(ownedFD)
			ownedFD = -1
		}
	}
	for _, component := range components[:len(components)-1] {
		nextFD, err := unix.Openat(
			currentFD,
			component,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
			0,
		)
		if err != nil {
			closeOwned()
			return nil, errors.New("evidence path must not contain a symbolic link or non-directory component")
		}
		closeOwned()
		currentFD = nextFD
		ownedFD = nextFD
	}
	flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC
	if directory {
		flags |= unix.O_DIRECTORY
	}
	fd, err := unix.Openat(currentFD, components[len(components)-1], flags, 0)
	closeOwned()
	if err != nil {
		return nil, fmt.Errorf("open evidence without following symbolic links: %w", err)
	}
	return os.NewFile(uintptr(fd), filepath.Join(root.name, normalized)), nil
}

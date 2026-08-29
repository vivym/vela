//go:build linux || darwin

package h3mockbackend

import (
	"errors"
	"os"
	"syscall"
)

func currentProcessUID() (uint32, error) {
	return uint32(os.Geteuid()), nil
}

func filesystemOwnerUID(info os.FileInfo) (uint32, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("filesystem object has no Unix owner")
	}
	return stat.Uid, nil
}

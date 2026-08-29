//go:build !linux && !darwin

package h3mockbackend

import (
	"errors"
	"os"
)

func currentProcessUID() (uint32, error) {
	return 0, errors.New("mock backend filesystem owner validation is unsupported on this platform")
}

func filesystemOwnerUID(os.FileInfo) (uint32, error) {
	return 0, errors.New("mock backend filesystem owner validation is unsupported on this platform")
}

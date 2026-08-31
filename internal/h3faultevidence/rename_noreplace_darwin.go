//go:build darwin

package h3faultevidence

import "golang.org/x/sys/unix"

func renameNoReplace(source, destination string) error {
	return unix.RenamexNp(source, destination, unix.RENAME_EXCL)
}

//go:build !darwin && !linux

package h3faultevidence

import "errors"

func renameNoReplace(_, _ string) error {
	return errors.New("atomic no-replace publication is unsupported on this platform")
}

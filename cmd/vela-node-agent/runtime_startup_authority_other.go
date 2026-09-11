//go:build !linux

package main

import "errors"

func runRuntimeStartupGate(config) error {
	return errors.New("runtime startup authority requires Linux")
}

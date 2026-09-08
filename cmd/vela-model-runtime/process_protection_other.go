//go:build !linux

package main

// Non-Linux local development does not claim the Linux process boundary.
func protectRuntimeProcess() error { return nil }

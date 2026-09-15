//go:build linux

package main

import (
	"os"
	"testing"
)

func TestVerifyTargetCredentialsBindsEffectiveIdentity(t *testing.T) {
	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	if uid == 0 || gid == 0 {
		t.Skip("observer contract requires a non-root target identity")
	}
	if err := verifyTargetCredentials(os.Getpid(), uid, gid); err != nil {
		t.Fatalf("current process credentials rejected: %v", err)
	}
	if err := verifyTargetCredentials(os.Getpid(), uid+1, gid); err == nil {
		t.Fatal("credential mismatch was accepted")
	}
}

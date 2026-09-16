//go:build linux

package main

import (
	"bytes"
	"os"
	"testing"
)

func TestCustodyLivenessAcceptsFreshChallenges(t *testing.T) {
	for _, nonce := range []byte{1, 2} {
		frame := append([]byte(custodyProtocol+"P"), bytes.Repeat([]byte{nonce}, 32)...)
		if !validLivenessChallenge(frame) {
			t.Fatal("fresh liveness challenge was rejected")
		}
		for _, bad := range [][]byte{frame[:len(frame)-1], append(append([]byte(nil), frame...), 0), append([]byte("invalid-custody-v1P"), frame[len(custodyProtocol)+1:]...)} {
			if validLivenessChallenge(bad) {
				t.Fatal("malformed liveness challenge was accepted")
			}
		}
		frame[len(custodyProtocol)] = 'A'
		if validLivenessChallenge(frame) {
			t.Fatal("handshake acknowledgement was accepted as liveness")
		}
	}
}

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

package main

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	"github.com/vivym/vela/internal/securefile"
)

func readRuntimePolicyPrivateKey(path string) (ed25519.PrivateKey, error) {
	if path == "" {
		return nil, errors.New("runtime policy private key path is empty")
	}
	key, err := securefile.Read(path, ed25519.PrivateKeySize, true)
	if err != nil {
		return nil, fmt.Errorf("read runtime policy private key: %w", err)
	}
	if len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("runtime policy private key must be exactly 64 bytes")
	}
	return ed25519.PrivateKey(key), nil
}

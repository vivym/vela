package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/stageauthority"
)

func validateWorkerBootstrapSigningConfig(configuration config) error {
	if (configuration.workerBootstrapSigningKeyringFile == "") != (configuration.workerBootstrapActiveKeyID == "") {
		return errors.New("VELA_WORKER_BOOTSTRAP_SIGNING_KEYRING_FILE and VELA_WORKER_BOOTSTRAP_ACTIVE_KEY_ID must be configured together")
	}
	return nil
}

func newWorkerBootstrapSigner(configuration config) (*journalbinding.Signer, error) {
	if err := validateWorkerBootstrapSigningConfig(configuration); err != nil {
		return nil, err
	}
	if configuration.workerBootstrapSigningKeyringFile == "" {
		return nil, nil
	}
	keys, err := stageauthority.ReadKeyringFile(configuration.workerBootstrapSigningKeyringFile)
	if err != nil {
		return nil, fmt.Errorf("read Registry journal signing keyring: %w", err)
	}
	defer stageauthority.ClearKeyring(keys)
	executionKeys, err := stageauthority.ReadKeyringFile(configuration.leaseKeyringFile)
	if err != nil {
		return nil, fmt.Errorf("read execution keyring for Registry key separation: %w", err)
	}
	defer stageauthority.ClearKeyring(executionKeys)
	for _, seed := range keys {
		if len(seed) != ed25519.SeedSize {
			return nil, errors.New("registry journal signing keys must be exact Ed25519 seeds")
		}
		for _, executionKey := range executionKeys {
			// Workers receive execution secrets and can also derive their Ed25519
			// seeds. Neither representation can serve as exclusive Registry proof.
			derived := sha256.Sum256(executionKey)
			if bytes.Equal(seed, executionKey) || bytes.Equal(seed, derived[:]) {
				return nil, errors.New("registry journal signing keys must be independent of Worker execution keys")
			}
		}
	}
	return journalbinding.NewSigner(configuration.workerBootstrapActiveKeyID, keys[configuration.workerBootstrapActiveKeyID])
}

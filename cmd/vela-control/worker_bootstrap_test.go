package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkerBootstrapSigningConfiguration(t *testing.T) {
	setValidConfigEnvironment(t)
	t.Setenv("VELA_WORKER_BOOTSTRAP_SIGNING_KEYRING_FILE", "")
	t.Setenv("VELA_WORKER_BOOTSTRAP_ACTIVE_KEY_ID", "")
	if signer, err := newWorkerBootstrapSigner(config{}); err != nil || signer != nil {
		t.Fatalf("optional signer: %v", err)
	}
	for _, name := range []string{"VELA_WORKER_BOOTSTRAP_SIGNING_KEYRING_FILE", "VELA_WORKER_BOOTSTRAP_ACTIVE_KEY_ID"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "only-one-field")
			if _, err := loadConfig(); err == nil {
				t.Fatal("accepted partial signing configuration")
			}
		})
	}
	executionSeed := bytes.Repeat([]byte{1}, 32)
	derived := sha256.Sum256(executionSeed)
	for name, seeds := range map[string]map[string][]byte{
		"valid":                  {"registry": bytes.Repeat([]byte{2}, 32)},
		"raw execution key":      {"registry": executionSeed},
		"derived execution seed": {"registry": derived[:]},
		"inactive shared key":    {"registry": bytes.Repeat([]byte{2}, 32), "retired": executionSeed},
		"invalid seed length":    {"registry": bytes.Repeat([]byte{2}, 64)},
		"missing active key":     {"other": bytes.Repeat([]byte{2}, 32)},
	} {
		t.Run(name, func(t *testing.T) {
			writeKeys := func(name string, keys map[string][]byte) string {
				t.Helper()
				wire, err := json.Marshal(keys)
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(t.TempDir(), name)
				if err := os.WriteFile(path, wire, 0o600); err != nil {
					t.Fatal(err)
				}
				return path
			}
			configuration := config{workerBootstrapSigningKeyringFile: writeKeys("registry.json", seeds), workerBootstrapActiveKeyID: "registry",
				leaseKeyringFile: writeKeys("execution.json", map[string][]byte{"execution": executionSeed})}
			signer, err := newWorkerBootstrapSigner(configuration)
			if name == "valid" {
				if err != nil || signer == nil {
					t.Fatalf("valid dedicated signer: %v", err)
				}
				t.Setenv("VELA_WORKER_BOOTSTRAP_SIGNING_KEYRING_FILE", configuration.workerBootstrapSigningKeyringFile)
				t.Setenv("VELA_WORKER_BOOTSTRAP_ACTIVE_KEY_ID", configuration.workerBootstrapActiveKeyID)
				loaded, err := loadConfig()
				if err != nil || loaded.workerBootstrapSigningKeyringFile != configuration.workerBootstrapSigningKeyringFile || loaded.workerBootstrapActiveKeyID != "registry" {
					t.Fatalf("environment signing configuration: %v", err)
				}
				if err := os.Chmod(configuration.workerBootstrapSigningKeyringFile, 0o644); err != nil {
					t.Fatal(err)
				}
				if signer, err := newWorkerBootstrapSigner(configuration); err == nil || signer != nil {
					t.Fatal("accepted exposed signing seeds")
				}
			} else if err == nil || signer != nil {
				t.Fatal("accepted invalid or Worker-known Registry key")
			}
		})
	}
}

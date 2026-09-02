package stageauthority

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadKeyringFilePreservesRotationSetAndRejectsDuplicateKeys(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "authority-keyring.json")
	keyOne := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	keyTwo := base64.StdEncoding.EncodeToString([]byte("abcdef0123456789abcdef0123456789"))
	if err := os.WriteFile(
		path,
		[]byte(`{"stage-key-v1":"`+keyOne+`","stage-key-v2":"`+keyTwo+`"}`),
		0o600,
	); err != nil {
		t.Fatalf("write authority keyring: %v", err)
	}
	keyring, err := ReadKeyringFile(path)
	if err != nil {
		t.Fatalf("ReadKeyringFile: %v", err)
	}
	defer ClearKeyring(keyring)
	if string(keyring["stage-key-v1"]) != "0123456789abcdef0123456789abcdef" ||
		string(keyring["stage-key-v2"]) != "abcdef0123456789abcdef0123456789" || len(keyring) != 2 {
		t.Fatalf("authority keyring = %#v", keyring)
	}

	if err := os.WriteFile(
		path,
		[]byte(`{"stage-key-v1":"`+keyOne+`","stage-key-v1":"`+keyTwo+`"}`),
		0o600,
	); err != nil {
		t.Fatalf("write duplicate authority keyring: %v", err)
	}
	if _, err := ReadKeyringFile(path); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate authority keyring error = %v", err)
	}
}

func TestReadVerifierKeyringFileRequiresEd25519PublicKeys(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "authority-verifier-keyring.json")
	publicKey := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	if err := os.WriteFile(path, []byte(`{"stage-key-v1":"`+publicKey+`"}`), 0o600); err != nil {
		t.Fatalf("write verifier keyring: %v", err)
	}
	keyring, err := ReadVerifierKeyringFile(path)
	if err != nil {
		t.Fatalf("ReadVerifierKeyringFile: %v", err)
	}
	defer ClearKeyring(keyring)
	if string(keyring["stage-key-v1"]) != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("verifier keyring = %#v", keyring)
	}

	tooLong := base64.StdEncoding.EncodeToString(make([]byte, 64))
	if err := os.WriteFile(path, []byte(`{"stage-key-v1":"`+tooLong+`"}`), 0o600); err != nil {
		t.Fatalf("write invalid verifier keyring: %v", err)
	}
	if _, err := ReadVerifierKeyringFile(path); err == nil {
		t.Fatal("ReadVerifierKeyringFile accepted a non-public-key payload")
	}
}

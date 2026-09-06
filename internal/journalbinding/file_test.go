package journalbinding

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestBindingFiles(t *testing.T) {
	signer, verifier := testKeys(t)
	signed, err := signer.Sign(testBinding())
	if err != nil {
		t.Fatal(err)
	}
	wire, err := Encode(signed)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "binding.json")
	if err := os.WriteFile(path, wire, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadFile(path, verifier)
	if err != nil || !proto.Equal(loaded, signed) {
		t.Fatalf("binding round trip: %v", err)
	}
	for name, invalid := range map[string][]byte{
		"duplicate":          append([]byte(`{"schema_version":1,`), wire[1:]...),
		"alias duplicate":    append([]byte(`{"schemaVersion":1,`), wire[1:]...),
		"unknown":            append([]byte(`{"unknown":1,`), wire[1:]...),
		"multiple documents": append(bytes.Clone(wire), wire...),
		"truncated":          wire[:len(wire)-1],
		"oversize":           bytes.Repeat([]byte{' '}, MaximumBytes+1),
		"tampered":           bytes.Replace(wire, []byte("node-a"), []byte("node-b"), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, invalid, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadFile(path, verifier); err == nil {
				t.Fatal("accepted invalid binding file")
			}
		})
	}
	if _, err := Encode(nil); err == nil {
		t.Fatal("encoded nil binding")
	}
	if _, err := Encode(testBinding()); err == nil {
		t.Fatal("encoded unsigned binding")
	}
	if err := os.WriteFile(path, wire, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFile(path, verifier); err == nil {
		t.Fatal("accepted world-readable binding")
	}
	keyPath := filepath.Join(t.TempDir(), "verifiers.json")
	keyWire := []byte(`{"registry-1":"` + base64.StdEncoding.EncodeToString(verifier.keys["registry-1"]) + `"}`)
	if err := os.WriteFile(keyPath, keyWire, 0o600); err != nil {
		t.Fatal(err)
	}
	loadedVerifier, err := ReadVerifierFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loadedVerifier.Verify(signed); err != nil {
		t.Fatal(err)
	}
}

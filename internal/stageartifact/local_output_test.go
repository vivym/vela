package stageartifact_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/stageartifact"
)

func TestParseLocalOutputManifestV1AcceptsExactSingleOutputContract(t *testing.T) {
	payload := []byte("sealed encoder tensor")
	digest := sha256.Sum256(payload)
	manifestJSON := localOutputManifestJSON(hex.EncodeToString(digest[:]), int64(len(payload)))

	manifest, err := stageartifact.ParseLocalOutputManifestV1([]byte(manifestJSON))
	if err != nil {
		t.Fatalf("ParseLocalOutputManifestV1: %v", err)
	}
	if manifest.OutputPort != "latent" || manifest.LocalLocator != "outputs/encoder.bin" ||
		manifest.ContentType != "application/x-minimax-h3-encoder" ||
		manifest.PayloadSHA256 != digest || manifest.SizeBytes != int64(len(payload)) ||
		manifest.Lineage.StageRunID.String() != "13000000-0000-0000-0000-000000000003" {
		t.Fatalf("manifest = %#v", manifest)
	}
}

func TestParseLocalOutputManifestV1RejectsExpandedOrAmbiguousContract(t *testing.T) {
	payloadDigest := sha256.Sum256([]byte("sealed encoder tensor"))
	valid := localOutputManifestJSON(hex.EncodeToString(payloadDigest[:]), 21)

	tests := map[string]string{
		"unknown field":        strings.Replace(valid, `"size_bytes":21`, `"size_bytes":21,"object_store_token":"secret"`, 1),
		"absolute locator":     strings.Replace(valid, `"outputs/encoder.bin"`, `"/var/lib/vela/encoder.bin"`, 1),
		"parent traversal":     strings.Replace(valid, `"outputs/encoder.bin"`, `"../encoder.bin"`, 1),
		"non canonical digest": strings.Replace(valid, hex.EncodeToString(payloadDigest[:]), strings.ToUpper(hex.EncodeToString(payloadDigest[:])), 1),
		"missing lineage":      strings.Replace(valid, `"lineage":{`, `"wrong_lineage":{`, 1),
	}
	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := stageartifact.ParseLocalOutputManifestV1([]byte(document)); err == nil {
				t.Fatal("ParseLocalOutputManifestV1 accepted invalid manifest")
			}
		})
	}
}

func TestFilesystemLocalOutputSourceOpensOnlyExactPayloadUnderConfiguredRoot(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "outputs"), 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	payload := []byte("sealed encoder tensor")
	path := filepath.Join(root, "outputs", "encoder.bin")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	digest := sha256.Sum256(payload)
	manifest, err := stageartifact.ParseLocalOutputManifestV1([]byte(
		localOutputManifestJSON(hex.EncodeToString(digest[:]), int64(len(payload))),
	))
	if err != nil {
		t.Fatalf("ParseLocalOutputManifestV1: %v", err)
	}
	source, err := stageartifact.NewFilesystemLocalOutputSource(root)
	if err != nil {
		t.Fatalf("NewFilesystemLocalOutputSource: %v", err)
	}

	reader, err := source.Open(context.Background(), manifest)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	opened, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	buffer := make([]byte, len(opened))
	if _, err := reader.Read(buffer); err != nil {
		t.Fatalf("Read payload: %v", err)
	}
	if string(buffer) != string(payload) {
		t.Fatalf("payload = %q, want %q", buffer, payload)
	}

	manifest.LocalLocator = "outputs/missing.bin"
	if _, err := source.Open(context.Background(), manifest); err == nil {
		t.Fatal("Open accepted a missing local payload")
	}
}

func localOutputManifestJSON(payloadDigest string, sizeBytes int64) string {
	return `{"schema_version":1,"output_port":"latent","local_locator":"outputs/encoder.bin",` +
		`"content_type":"application/x-minimax-h3-encoder","payload_sha256":"` + payloadDigest + `",` +
		`"size_bytes":` + strconv.FormatInt(sizeBytes, 10) + `,` +
		`"lineage":{"attempt_id":"13000000-0000-0000-0000-000000000002",` +
		`"stage_run_id":"13000000-0000-0000-0000-000000000003",` +
		`"stage_attempt_id":"13000000-0000-0000-0000-000000000004",` +
		`"stage_lease_id":"13000000-0000-0000-0000-000000000006",` +
		`"attempt_fence":2,"stage_fence":3,` +
		`"stage_profile_revision_id":"63000000-0000-0000-0000-000000000001"}}`
}

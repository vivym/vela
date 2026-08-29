package supplychain

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/releasebundle"
)

func TestLoadWithinVerifiesCompleteSupplyChain(t *testing.T) {
	fixture := writeEvidenceFixture(t)

	evidence, err := Load(
		filepath.Join(fixture.directory, "supply-chain.json"),
		filepath.Join(fixture.directory, "supply-chain-policy.json"),
		fixture.bundle,
	)
	if err != nil {
		t.Fatalf("load supply-chain evidence: %v", err)
	}
	if evidence.ReleaseDigest != fixture.bundle.ReleaseDigest ||
		evidence.ConfigurationRevision != fixture.bundle.ConfigurationRevision ||
		len(evidence.Images) != len(fixture.bundle.OCIImages) ||
		!strings.HasPrefix(evidence.ManifestDigest, "sha256:") ||
		!strings.HasPrefix(evidence.PolicyDigest, "sha256:") {
		t.Fatalf("verified supply-chain evidence = %#v", evidence)
	}
}

func TestLoadAcceptsRelativeManifestAndPolicyPaths(t *testing.T) {
	fixture := writeEvidenceFixture(t)
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	manifestPath, err := filepath.Rel(workingDirectory, filepath.Join(fixture.directory, "supply-chain.json"))
	if err != nil {
		t.Fatalf("make manifest path relative: %v", err)
	}
	policyPath, err := filepath.Rel(workingDirectory, filepath.Join(fixture.directory, "supply-chain-policy.json"))
	if err != nil {
		t.Fatalf("make policy path relative: %v", err)
	}

	if _, err := Load(manifestPath, policyPath, fixture.bundle); err != nil {
		t.Fatalf("load supply-chain evidence through relative CLI paths: %v", err)
	}
}

func TestLoadRejectsInvalidSPDXDownloadLocations(t *testing.T) {
	for _, test := range []struct {
		name     string
		location string
		omit     bool
	}{
		{name: "missing", omit: true},
		{name: "empty"},
		{name: "control character", location: "https://registry.example.invalid/\x00image"},
		{name: "relative URI", location: "registry.example.invalid/image"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := writeEvidenceFixture(t)
			setSPDXDownloadLocation(t, fixture, test.location, test.omit)

			_, err := Load(
				filepath.Join(fixture.directory, "supply-chain.json"),
				filepath.Join(fixture.directory, "supply-chain-policy.json"),
				fixture.bundle,
			)
			if err == nil || !strings.Contains(err.Error(), "package download location") {
				t.Fatalf("Load error = %v, want invalid package download location", err)
			}
		})
	}
}

func TestLoadAcceptsStandardSPDXDownloadLocations(t *testing.T) {
	for _, location := range []string{
		"NONE",
		"NOASSERTION",
		"git+https://github.com/example/vela.git@v1.0.0",
	} {
		t.Run(location, func(t *testing.T) {
			fixture := writeEvidenceFixture(t)
			setSPDXDownloadLocation(t, fixture, location, false)

			if _, err := Load(
				filepath.Join(fixture.directory, "supply-chain.json"),
				filepath.Join(fixture.directory, "supply-chain-policy.json"),
				fixture.bundle,
			); err != nil {
				t.Fatalf("load standard SPDX download location %q: %v", location, err)
			}
		})
	}
}

func TestLoadWithinRejectsTamperedOrIncompleteEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, evidenceFixture)
		match  string
	}{
		{
			name: "missing image",
			mutate: func(t *testing.T, fixture evidenceFixture) {
				t.Helper()
				mutateJSONFile(t, filepath.Join(fixture.directory, "supply-chain.json"), func(value map[string]any) {
					value["images"] = value["images"].([]any)[:1]
				})
			},
			match: "exact release image set",
		},
		{
			name: "tampered SBOM",
			mutate: func(t *testing.T, fixture evidenceFixture) {
				t.Helper()
				path := filepath.Join(fixture.directory, "images", "control.spdx.json")
				encoded, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("read SBOM fixture: %v", err)
				}
				if err := os.WriteFile(path, append(encoded, '\n'), 0o600); err != nil {
					t.Fatalf("tamper SBOM fixture: %v", err)
				}
			},
			match: "SBOM digest",
		},
		{
			name: "tampered image statement signature",
			mutate: func(t *testing.T, fixture evidenceFixture) {
				t.Helper()
				mutateJSONFile(t, filepath.Join(fixture.directory, "images", "control.statement.dsse.json"), func(value map[string]any) {
					signatures := value["signatures"].([]any)
					signatures[0].(map[string]any)["sig"] = base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
				})
			},
			match: "image statement signature",
		},
		{
			name: "vulnerability threshold exceeded",
			mutate: func(t *testing.T, fixture evidenceFixture) {
				t.Helper()
				mutateJSONFile(t, filepath.Join(fixture.directory, "images", "control.vulnerabilities.json"), func(value map[string]any) {
					value["high"] = float64(1)
				})
			},
			match: "trusted policy threshold",
		},
		{
			name: "same signer and approver key",
			mutate: func(t *testing.T, fixture evidenceFixture) {
				t.Helper()
				mutateJSONFile(t, filepath.Join(fixture.directory, "supply-chain-policy.json"), func(value map[string]any) {
					signer := value["image_signers"].([]any)[0].(map[string]any)
					approver := value["vulnerability_approvers"].([]any)[0].(map[string]any)
					approver["public_key"] = signer["public_key"]
				})
			},
			match: "independent keys",
		},
		{
			name: "expired image signer",
			mutate: func(t *testing.T, fixture evidenceFixture) {
				t.Helper()
				mutateJSONFile(t, filepath.Join(fixture.directory, "supply-chain-policy.json"), func(value map[string]any) {
					value["image_signers"].([]any)[0].(map[string]any)["not_after"] = "2026-08-29T04:00:30Z"
				})
			},
			match: "validity window",
		},
		{
			name: "stale scanner database",
			mutate: func(t *testing.T, fixture evidenceFixture) {
				t.Helper()
				mutateJSONFile(t, filepath.Join(fixture.directory, "images", "control.vulnerabilities.json"), func(value map[string]any) {
					value["database_updated_at"] = "2026-08-27T04:00:00Z"
				})
			},
			match: "database age",
		},
		{
			name: "escaped SBOM reference",
			mutate: func(t *testing.T, fixture evidenceFixture) {
				t.Helper()
				mutateJSONFile(t, filepath.Join(fixture.directory, "supply-chain.json"), func(value map[string]any) {
					value["images"].([]any)[0].(map[string]any)["sbom_ref"] = "../outside.spdx.json"
				})
			},
			match: "reference",
		},
		{
			name: "unknown manifest field",
			mutate: func(t *testing.T, fixture evidenceFixture) {
				t.Helper()
				mutateJSONFile(t, filepath.Join(fixture.directory, "supply-chain.json"), func(value map[string]any) {
					value["waiver"] = true
				})
			},
			match: "unknown field",
		},
		{
			name: "symlinked SBOM",
			mutate: func(t *testing.T, fixture evidenceFixture) {
				t.Helper()
				path := filepath.Join(fixture.directory, "images", "control.spdx.json")
				target := filepath.Join(fixture.directory, "control-target.spdx.json")
				if err := os.Rename(path, target); err != nil {
					t.Fatalf("move SBOM fixture: %v", err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatalf("link SBOM fixture: %v", err)
				}
			},
			match: "symbolic links",
		},
		{
			name: "approval not after scan",
			mutate: func(t *testing.T, fixture evidenceFixture) {
				t.Helper()
				resignEnvelopePayload(
					t,
					filepath.Join(fixture.directory, "images", "control.approval.dsse.json"),
					vulnerabilityApprovalPayloadType,
					"security-approver-1",
					fixture.approverPrivate,
					func(payload map[string]any) {
						payload["approved_at"] = "2026-08-29T04:00:00Z"
					},
				)
			},
			match: "approval time",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := writeEvidenceFixture(t)
			test.mutate(t, fixture)
			_, err := Load(
				filepath.Join(fixture.directory, "supply-chain.json"),
				filepath.Join(fixture.directory, "supply-chain-policy.json"),
				fixture.bundle,
			)
			if err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("LoadWithin error = %v, want %q", err, test.match)
			}
		})
	}
}

func TestLoadRequiresPinnedIndependentPolicy(t *testing.T) {
	fixture := writeEvidenceFixture(t)
	manifestPath := filepath.Join(fixture.directory, "supply-chain.json")
	policyPath := filepath.Join(fixture.directory, "supply-chain-policy.json")
	if _, err := LoadWithPolicyDigest(
		manifestPath,
		policyPath,
		testDigest("wrong-policy"),
		fixture.bundle,
	); err == nil || !strings.Contains(err.Error(), "trust policy digest mismatch") {
		t.Fatalf("policy digest mismatch error = %v", err)
	}
	target := filepath.Join(fixture.directory, "policy-target.json")
	if err := os.Rename(policyPath, target); err != nil {
		t.Fatalf("move trust policy fixture: %v", err)
	}
	if err := os.Symlink(target, policyPath); err != nil {
		t.Fatalf("link trust policy fixture: %v", err)
	}
	if _, err := Load(manifestPath, policyPath, fixture.bundle); err == nil ||
		!strings.Contains(err.Error(), "symbolic links") {
		t.Fatalf("symlinked policy error = %v", err)
	}
}

func TestLoadWithinRejectsDuplicateJSONKeys(t *testing.T) {
	fixture := writeEvidenceFixture(t)
	path := filepath.Join(fixture.directory, "supply-chain.json")
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read supply-chain manifest fixture: %v", err)
	}
	encoded = []byte(strings.Replace(
		string(encoded),
		`"schema_version":1`,
		`"schema_version":1,"schema_version":1`,
		1,
	))
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write duplicate-key manifest fixture: %v", err)
	}
	_, err = Load(
		filepath.Join(fixture.directory, "supply-chain.json"),
		filepath.Join(fixture.directory, "supply-chain-policy.json"),
		fixture.bundle,
	)
	if err == nil || !strings.Contains(err.Error(), "duplicate JSON key") {
		t.Fatalf("LoadWithin error = %v, want duplicate-key rejection", err)
	}
}

type evidenceFixture struct {
	directory       string
	bundle          releasebundle.Bundle
	signerPrivate   ed25519.PrivateKey
	approverPrivate ed25519.PrivateKey
}

func writeEvidenceFixture(t *testing.T) evidenceFixture {
	t.Helper()
	directory := t.TempDir()
	if err := os.Mkdir(filepath.Join(directory, "images"), 0o700); err != nil {
		t.Fatalf("create image evidence directory: %v", err)
	}
	now := time.Date(2026, 8, 29, 4, 0, 0, 0, time.UTC)
	signerPublic, signerPrivate, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate image signer key: %v", err)
	}
	approverPublic, approverPrivate, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate vulnerability approver key: %v", err)
	}
	bundle := releasebundle.Bundle{
		SchemaVersion:         1,
		ReleaseDigest:         testDigest("release"),
		ConfigurationRevision: testDigest("configuration"),
		OCIImages: []releasebundle.OCIImage{
			testBundleImage("registry.example/vela/control", "control"),
			testBundleImage("registry.example/vela/worker", "worker"),
		},
	}
	policy := map[string]any{
		"schema_version":          1,
		"image_signers":           []any{testTrustedKey("release-signer-1", signerPublic, now)},
		"vulnerability_approvers": []any{testTrustedKey("security-approver-1", approverPublic, now)},
		"scanners":                []any{map[string]any{"name": "grype", "version": "0.99.1"}},
		"vulnerability_policy": map[string]any{
			"maximum_critical": 0, "maximum_high": 0,
			"maximum_database_age_seconds": 86400,
		},
	}
	policyEncoded := writeJSON(t, filepath.Join(directory, "supply-chain-policy.json"), policy)
	policyDigest := digestBytes(policyEncoded)

	publication := map[string]any{
		"schema_version": 1,
		"revision":       "release-r1",
		"images": []any{
			testPublicationImage(bundle.OCIImages[0]),
			testPublicationImage(bundle.OCIImages[1]),
		},
	}
	publicationEncoded := writeJSON(t, filepath.Join(directory, "registry-publication.json"), publication)
	publicationDigest := digestBytes(publicationEncoded)
	manifestImages := make([]any, 0, len(bundle.OCIImages))
	for index, image := range bundle.OCIImages {
		name := []string{"control", "worker"}[index]
		sbomRef := "images/" + name + ".spdx.json"
		sbomEncoded := writeJSON(t, filepath.Join(directory, filepath.FromSlash(sbomRef)), testSPDX(image, now))
		sbomDigest := digestBytes(sbomEncoded)
		rawRef := "images/" + name + ".scanner.json"
		rawEncoded := writeJSON(t, filepath.Join(directory, filepath.FromSlash(rawRef)), map[string]any{"matches": []any{}})
		reportRef := "images/" + name + ".vulnerabilities.json"
		report := map[string]any{
			"schema_version": 1, "image": image.Image, "sbom_digest": sbomDigest,
			"scanner_name": "grype", "scanner_version": "0.99.1",
			"database_digest":     testDigest("grype-db"),
			"database_updated_at": now.Add(-time.Hour).Format(time.RFC3339),
			"scanned_at":          now.Format(time.RFC3339),
			"critical":            0, "high": 0, "medium": 1, "low": 2, "unknown": 0,
			"scanner_output_ref": rawRef, "scanner_output_digest": digestBytes(rawEncoded),
		}
		reportEncoded := writeJSON(t, filepath.Join(directory, filepath.FromSlash(reportRef)), report)
		reportDigest := digestBytes(reportEncoded)
		statement := map[string]any{
			"schema_version": 1, "image": image.Image,
			"release_digest": bundle.ReleaseDigest, "configuration_revision": bundle.ConfigurationRevision,
			"publication_receipt_digest": publicationDigest,
			"sbom_digest":                sbomDigest, "vulnerability_report_digest": reportDigest,
			"signed_at": now.Add(time.Minute).Format(time.RFC3339),
		}
		statementRef := "images/" + name + ".statement.dsse.json"
		writeJSON(t, filepath.Join(directory, filepath.FromSlash(statementRef)), testEnvelope(t, imageStatementPayloadType, "release-signer-1", signerPrivate, statement))
		approval := map[string]any{
			"schema_version": 1, "image": image.Image,
			"release_digest": bundle.ReleaseDigest, "configuration_revision": bundle.ConfigurationRevision,
			"sbom_digest": sbomDigest, "vulnerability_report_digest": reportDigest,
			"policy_digest": policyDigest, "decision": "APPROVED",
			"scanner_name": "grype", "scanner_version": "0.99.1",
			"database_digest":  testDigest("grype-db"),
			"maximum_critical": 0, "maximum_high": 0,
			"maximum_database_age_seconds": 86400,
			"approved_at":                  now.Add(2 * time.Minute).Format(time.RFC3339),
		}
		approvalRef := "images/" + name + ".approval.dsse.json"
		writeJSON(t, filepath.Join(directory, filepath.FromSlash(approvalRef)), testEnvelope(t, vulnerabilityApprovalPayloadType, "security-approver-1", approverPrivate, approval))
		manifestImages = append(manifestImages, map[string]any{
			"image": image.Image, "publication_receipt_digest": publicationDigest,
			"statement_ref": statementRef, "sbom_ref": sbomRef,
			"vulnerability_report_ref": reportRef, "approval_ref": approvalRef,
		})
	}
	manifest := map[string]any{
		"schema_version": 1, "release_digest": bundle.ReleaseDigest,
		"configuration_revision": bundle.ConfigurationRevision,
		"publication_receipts": []any{map[string]any{
			"ref": "registry-publication.json", "digest": publicationDigest,
		}},
		"images": manifestImages,
	}
	writeJSON(t, filepath.Join(directory, "supply-chain.json"), manifest)
	return evidenceFixture{
		directory: directory, bundle: bundle,
		signerPrivate: signerPrivate, approverPrivate: approverPrivate,
	}
}

func setSPDXDownloadLocation(t *testing.T, fixture evidenceFixture, location string, omit bool) {
	t.Helper()
	sbomPath := filepath.Join(fixture.directory, "images", "control.spdx.json")
	sbomEncoded := mutateJSONFile(t, sbomPath, func(value map[string]any) {
		spdxPackage := value["packages"].([]any)[0].(map[string]any)
		if omit {
			delete(spdxPackage, "downloadLocation")
			return
		}
		spdxPackage["downloadLocation"] = location
	})
	sbomDigest := digestBytes(sbomEncoded)

	reportPath := filepath.Join(fixture.directory, "images", "control.vulnerabilities.json")
	reportEncoded := mutateJSONFile(t, reportPath, func(value map[string]any) {
		value["sbom_digest"] = sbomDigest
	})
	reportDigest := digestBytes(reportEncoded)

	resignEnvelopePayload(
		t,
		filepath.Join(fixture.directory, "images", "control.statement.dsse.json"),
		imageStatementPayloadType,
		"release-signer-1",
		fixture.signerPrivate,
		func(payload map[string]any) {
			payload["sbom_digest"] = sbomDigest
			payload["vulnerability_report_digest"] = reportDigest
		},
	)
	resignEnvelopePayload(
		t,
		filepath.Join(fixture.directory, "images", "control.approval.dsse.json"),
		vulnerabilityApprovalPayloadType,
		"security-approver-1",
		fixture.approverPrivate,
		func(payload map[string]any) {
			payload["sbom_digest"] = sbomDigest
			payload["vulnerability_report_digest"] = reportDigest
		},
	)
}

func resignEnvelopePayload(
	t *testing.T,
	path,
	payloadType,
	keyID string,
	privateKey ed25519.PrivateKey,
	mutate func(map[string]any),
) {
	t.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read DSSE envelope fixture: %v", err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatalf("decode DSSE envelope fixture: %v", err)
	}
	payloadEncoded, err := base64.StdEncoding.DecodeString(envelope["payload"].(string))
	if err != nil {
		t.Fatalf("decode DSSE payload fixture: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(payloadEncoded, &payload); err != nil {
		t.Fatalf("decode DSSE payload JSON fixture: %v", err)
	}
	mutate(payload)
	writeJSON(t, path, testEnvelope(t, payloadType, keyID, privateKey, payload))
}

func testBundleImage(repository, seed string) releasebundle.OCIImage {
	digest := testDigest(seed)
	return releasebundle.OCIImage{
		Image: repository + "@" + digest,
		Descriptor: releasebundle.Artifact{
			MediaType: releasebundle.OCIManifestMediaType,
			Digest:    digest, SizeBytes: int64(100 + len(seed)),
		},
		Platform: releasebundle.Platform{OS: "linux", Architecture: "amd64"},
	}
}

func testTrustedKey(keyID string, publicKey ed25519.PublicKey, now time.Time) map[string]any {
	return map[string]any{
		"key_id": keyID, "algorithm": "ed25519",
		"public_key": base64.StdEncoding.EncodeToString(publicKey),
		"not_before": now.Add(-time.Hour).Format(time.RFC3339),
		"not_after":  now.Add(time.Hour).Format(time.RFC3339),
	}
}

func testPublicationImage(image releasebundle.OCIImage) map[string]any {
	return map[string]any{
		"image": image.Image, "manifest_digest": image.Descriptor.Digest,
		"manifest_media_type": image.Descriptor.MediaType,
		"manifest_size_bytes": image.Descriptor.SizeBytes,
	}
}

func testSPDX(image releasebundle.OCIImage, created time.Time) map[string]any {
	repository, digest, _ := strings.Cut(image.Image, "@")
	return map[string]any{
		"spdxVersion": "SPDX-2.3", "dataLicense": "CC0-1.0",
		"SPDXID": "SPDXRef-DOCUMENT", "name": repository + " SBOM",
		"documentNamespace": "https://vela.example.invalid/spdx/" + strings.TrimPrefix(digest, "sha256:"),
		"creationInfo": map[string]any{
			"created": created.Format(time.RFC3339), "creators": []string{"Tool: syft-1.30.0"},
		},
		"documentDescribes": []string{"SPDXRef-IMAGE"},
		"packages": []any{map[string]any{
			"SPDXID": "SPDXRef-IMAGE", "name": repository,
			"versionInfo": digest, "downloadLocation": "https://registry.example.invalid/v2/" + repository,
			"filesAnalyzed": false,
			"checksums": []any{map[string]any{
				"algorithm": "SHA256", "checksumValue": strings.TrimPrefix(digest, "sha256:"),
			}},
		}},
	}
}

func testEnvelope(t *testing.T, payloadType, keyID string, privateKey ed25519.PrivateKey, payload any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encode DSSE payload: %v", err)
	}
	signature := ed25519.Sign(privateKey, testPAE(payloadType, encoded))
	return map[string]any{
		"payloadType": payloadType,
		"payload":     base64.StdEncoding.EncodeToString(encoded),
		"signatures": []any{map[string]any{
			"keyid": keyID, "sig": base64.StdEncoding.EncodeToString(signature),
		}},
	}
}

func testPAE(payloadType string, payload []byte) []byte {
	return []byte(fmt.Sprintf("DSSEv1 %d %s %d %s", len(payloadType), payloadType, len(payload), payload))
}

func testDigest(seed string) string {
	digest := sha256.Sum256([]byte(seed))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func writeJSON(t *testing.T, path string, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode %s: %v", filepath.Base(path), err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write %s: %v", filepath.Base(path), err)
	}
	return encoded
}

func mutateJSONFile(t *testing.T, path string, mutate func(map[string]any)) []byte {
	t.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read JSON fixture: %v", err)
	}
	var value map[string]any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatalf("decode JSON fixture: %v", err)
	}
	mutate(value)
	return writeJSON(t, path, value)
}

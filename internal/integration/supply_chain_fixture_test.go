//go:build integration

package integration_test

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

const (
	catalogImageStatementPayloadType = "application/vnd.vela.supply-chain.image-statement.v1+json"
	catalogApprovalPayloadType       = "application/vnd.vela.supply-chain.vulnerability-approval.v1+json"
)

func writeCatalogSupplyChainFixture(t *testing.T, directory string, bundle releasebundle.Bundle) {
	t.Helper()
	evidenceDirectory := filepath.Join(directory, "supply-chain")
	if err := os.Mkdir(evidenceDirectory, 0o700); err != nil {
		t.Fatalf("create Catalog supply-chain directory: %v", err)
	}
	now := time.Date(2026, 8, 29, 4, 0, 0, 0, time.UTC)
	signerPrivate := ed25519.NewKeyFromSeed(catalogSupplyChainSeed("release-signer"))
	approverPrivate := ed25519.NewKeyFromSeed(catalogSupplyChainSeed("security-approver"))
	policy := map[string]any{
		"schema_version":          1,
		"image_signers":           []any{catalogTrustedKey("release-signer-1", signerPrivate.Public().(ed25519.PublicKey), now)},
		"vulnerability_approvers": []any{catalogTrustedKey("security-approver-1", approverPrivate.Public().(ed25519.PublicKey), now)},
		"scanners":                []any{map[string]any{"name": "grype", "version": "0.99.1"}},
		"vulnerability_policy": map[string]any{
			"maximum_critical": 0, "maximum_high": 0,
			"maximum_database_age_seconds": 86400,
		},
	}
	policyEncoded := writeCatalogSupplyChainJSON(t, filepath.Join(evidenceDirectory, "policy.json"), policy)
	policyDigest := catalogSupplyChainDigest(policyEncoded)
	publicationImages := make([]any, 0, len(bundle.OCIImages))
	for _, image := range bundle.OCIImages {
		publicationImages = append(publicationImages, map[string]any{
			"image": image.Image, "manifest_digest": image.Descriptor.Digest,
			"manifest_media_type": image.Descriptor.MediaType,
			"manifest_size_bytes": image.Descriptor.SizeBytes,
		})
	}
	publicationEncoded := writeCatalogSupplyChainJSON(
		t,
		filepath.Join(evidenceDirectory, "registry-publication.json"),
		map[string]any{"schema_version": 1, "revision": "release-r1", "images": publicationImages},
	)
	publicationDigest := catalogSupplyChainDigest(publicationEncoded)
	manifestImages := make([]any, 0, len(bundle.OCIImages))
	for index, image := range bundle.OCIImages {
		name := fmt.Sprintf("image-%d", index)
		sbomRef := name + ".spdx.json"
		sbomEncoded := writeCatalogSupplyChainJSON(
			t,
			filepath.Join(evidenceDirectory, filepath.FromSlash(sbomRef)),
			catalogSPDXFixture(image, now),
		)
		sbomDigest := catalogSupplyChainDigest(sbomEncoded)
		rawRef := name + ".scanner.json"
		rawEncoded := writeCatalogSupplyChainJSON(
			t,
			filepath.Join(evidenceDirectory, filepath.FromSlash(rawRef)),
			map[string]any{"matches": []any{}},
		)
		reportRef := name + ".vulnerabilities.json"
		report := map[string]any{
			"schema_version": 1, "image": image.Image, "sbom_digest": sbomDigest,
			"scanner_name": "grype", "scanner_version": "0.99.1",
			"database_digest":     catalogReleaseDigest("grype-db"),
			"database_updated_at": now.Add(-time.Hour).Format(time.RFC3339),
			"scanned_at":          now.Format(time.RFC3339),
			"critical":            0, "high": 0, "medium": 1, "low": 2, "unknown": 0,
			"scanner_output_ref":    rawRef,
			"scanner_output_digest": catalogSupplyChainDigest(rawEncoded),
		}
		reportEncoded := writeCatalogSupplyChainJSON(
			t,
			filepath.Join(evidenceDirectory, filepath.FromSlash(reportRef)),
			report,
		)
		reportDigest := catalogSupplyChainDigest(reportEncoded)
		statement := map[string]any{
			"schema_version": 1, "image": image.Image,
			"release_digest":             bundle.ReleaseDigest,
			"configuration_revision":     bundle.ConfigurationRevision,
			"publication_receipt_digest": publicationDigest,
			"sbom_digest":                sbomDigest, "vulnerability_report_digest": reportDigest,
			"signed_at": now.Add(time.Minute).Format(time.RFC3339),
		}
		statementRef := name + ".statement.dsse.json"
		writeCatalogSupplyChainJSON(
			t,
			filepath.Join(evidenceDirectory, filepath.FromSlash(statementRef)),
			catalogDSSEEnvelope(t, catalogImageStatementPayloadType, "release-signer-1", signerPrivate, statement),
		)
		approval := map[string]any{
			"schema_version": 1, "image": image.Image,
			"release_digest":         bundle.ReleaseDigest,
			"configuration_revision": bundle.ConfigurationRevision,
			"sbom_digest":            sbomDigest, "vulnerability_report_digest": reportDigest,
			"policy_digest": policyDigest, "decision": "APPROVED",
			"scanner_name": "grype", "scanner_version": "0.99.1",
			"database_digest":  catalogReleaseDigest("grype-db"),
			"maximum_critical": 0, "maximum_high": 0,
			"maximum_database_age_seconds": 86400,
			"approved_at":                  now.Add(2 * time.Minute).Format(time.RFC3339),
		}
		approvalRef := name + ".approval.dsse.json"
		writeCatalogSupplyChainJSON(
			t,
			filepath.Join(evidenceDirectory, filepath.FromSlash(approvalRef)),
			catalogDSSEEnvelope(t, catalogApprovalPayloadType, "security-approver-1", approverPrivate, approval),
		)
		manifestImages = append(manifestImages, map[string]any{
			"image": image.Image, "publication_receipt_digest": publicationDigest,
			"statement_ref": statementRef, "sbom_ref": sbomRef,
			"vulnerability_report_ref": reportRef, "approval_ref": approvalRef,
		})
	}
	writeCatalogSupplyChainJSON(
		t,
		filepath.Join(evidenceDirectory, "manifest.json"),
		map[string]any{
			"schema_version": 1, "release_digest": bundle.ReleaseDigest,
			"configuration_revision": bundle.ConfigurationRevision,
			"publication_receipts": []any{map[string]any{
				"ref": "registry-publication.json", "digest": publicationDigest,
			}},
			"images": manifestImages,
		},
	)
}

func catalogSupplyChainSeed(value string) []byte {
	digest := sha256.Sum256([]byte(value))
	return digest[:]
}

func catalogTrustedKey(keyID string, publicKey ed25519.PublicKey, now time.Time) map[string]any {
	return map[string]any{
		"key_id": keyID, "algorithm": "ed25519",
		"public_key": base64.StdEncoding.EncodeToString(publicKey),
		"not_before": now.Add(-time.Hour).Format(time.RFC3339),
		"not_after":  now.Add(time.Hour).Format(time.RFC3339),
	}
}

func catalogSPDXFixture(image releasebundle.OCIImage, created time.Time) map[string]any {
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
			"versionInfo": digest, "downloadLocation": "NOASSERTION", "filesAnalyzed": false,
			"checksums": []any{map[string]any{
				"algorithm": "SHA256", "checksumValue": strings.TrimPrefix(digest, "sha256:"),
			}},
		}},
	}
}

func catalogDSSEEnvelope(
	t *testing.T,
	payloadType, keyID string,
	privateKey ed25519.PrivateKey,
	payload any,
) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encode Catalog DSSE payload: %v", err)
	}
	signature := ed25519.Sign(privateKey, []byte(fmt.Sprintf(
		"DSSEv1 %d %s %d %s",
		len(payloadType), payloadType, len(encoded), encoded,
	)))
	return map[string]any{
		"payloadType": payloadType, "payload": base64.StdEncoding.EncodeToString(encoded),
		"signatures": []any{map[string]any{
			"keyid": keyID, "sig": base64.StdEncoding.EncodeToString(signature),
		}},
	}
}

func writeCatalogSupplyChainJSON(t *testing.T, path string, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode Catalog supply-chain fixture: %v", err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write Catalog supply-chain fixture: %v", err)
	}
	return encoded
}

func catalogSupplyChainDigest(encoded []byte) string {
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

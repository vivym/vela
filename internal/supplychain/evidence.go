package supplychain

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/vivym/vela/internal/releasebundle"
	"github.com/vivym/vela/internal/strictjson"
)

type validatedKey struct {
	publicKey ed25519.PublicKey
	notBefore time.Time
	notAfter  time.Time
}

type validatedPolicy struct {
	imageSigners  map[string]validatedKey
	approvers     map[string]validatedKey
	scanners      map[string]struct{}
	vulnerability VulnerabilityPolicy
}

type evidenceReader struct {
	root      *os.Root
	remaining int64
	used      map[string]string
}

func Load(manifestPath, policyPath string, bundle releasebundle.Bundle) (Evidence, error) {
	manifestDirectory, manifestReference := filepath.Dir(manifestPath), filepath.Base(manifestPath)
	policyDirectory, policyReference := filepath.Dir(policyPath), filepath.Base(policyPath)
	if manifestDirectory != policyDirectory {
		return Evidence{}, fmt.Errorf("%w: manifest and trust policy must share one evidence root", ErrInvalidEvidence)
	}
	return LoadWithin(manifestDirectory, manifestReference, policyReference, bundle)
}

func LoadWithin(directory, manifestReference, policyReference string, bundle releasebundle.Bundle) (Evidence, error) {
	manifestNormalized, err := normalizeReference(manifestReference)
	if err != nil {
		return Evidence{}, invalidf("supply-chain manifest reference: %v", err)
	}
	policyNormalized, err := normalizeReference(policyReference)
	if err != nil {
		return Evidence{}, invalidf("supply-chain trust policy reference: %v", err)
	}
	manifestDirectory := filepath.Dir(manifestNormalized)
	if filepath.Dir(policyNormalized) != manifestDirectory {
		return Evidence{}, invalidf("supply-chain manifest and trust policy must share one evidence directory")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return Evidence{}, invalidf("open evidence root: %v", err)
	}
	defer func() { _ = root.Close() }()
	evidenceRoot := root
	if manifestDirectory != "." {
		evidenceRoot, err = root.OpenRoot(manifestDirectory)
		if err != nil {
			return Evidence{}, invalidf("open supply-chain evidence directory: %v", err)
		}
		defer func() { _ = evidenceRoot.Close() }()
	}
	reader := &evidenceReader{root: evidenceRoot, remaining: maxEvidenceBytes, used: make(map[string]string)}
	manifestEncoded, err := reader.readUnique(filepath.Base(manifestNormalized), "supply-chain manifest", maxManifestBytes)
	if err != nil {
		return Evidence{}, err
	}
	policyEncoded, err := reader.readUnique(filepath.Base(policyNormalized), "supply-chain trust policy", maxPolicyBytes)
	if err != nil {
		return Evidence{}, err
	}
	var manifest Manifest
	if err := decodeStrictJSON(manifestEncoded, &manifest); err != nil {
		return Evidence{}, invalidf("decode supply-chain manifest: %v", err)
	}
	var policy TrustPolicy
	if err := decodeStrictJSON(policyEncoded, &policy); err != nil {
		return Evidence{}, invalidf("decode supply-chain trust policy: %v", err)
	}
	validatedTrust, err := validateTrustPolicy(policy)
	if err != nil {
		return Evidence{}, err
	}
	policyDigest := digestBytes(policyEncoded)
	if err := validateManifestHeader(manifest, bundle); err != nil {
		return Evidence{}, err
	}
	publications, err := loadPublicationReceipts(reader, manifest, bundle)
	if err != nil {
		return Evidence{}, err
	}
	verified, err := verifyImages(reader, manifest, bundle, validatedTrust, policyDigest, publications)
	if err != nil {
		return Evidence{}, err
	}
	return Evidence{
		ReleaseDigest: bundle.ReleaseDigest, ConfigurationRevision: bundle.ConfigurationRevision,
		ManifestDigest: digestBytes(manifestEncoded), PolicyDigest: policyDigest, Images: verified,
	}, nil
}

func validateTrustPolicy(policy TrustPolicy) (validatedPolicy, error) {
	if policy.SchemaVersion != SchemaVersion || len(policy.ImageSigners) == 0 ||
		len(policy.ImageSigners) > maxKeys || len(policy.VulnerabilityApprovers) == 0 ||
		len(policy.VulnerabilityApprovers) > maxKeys || len(policy.Scanners) == 0 ||
		len(policy.Scanners) > maxScanners {
		return validatedPolicy{}, invalidf("trust policy inventory is invalid")
	}
	if policy.VulnerabilityPolicy.MaximumCritical < 0 ||
		policy.VulnerabilityPolicy.MaximumHigh < 0 ||
		policy.VulnerabilityPolicy.MaximumDatabaseAgeSeconds <= 0 ||
		policy.VulnerabilityPolicy.MaximumDatabaseAgeSeconds > maxDatabaseAgeSeconds {
		return validatedPolicy{}, invalidf("vulnerability policy is invalid")
	}
	result := validatedPolicy{
		imageSigners:  make(map[string]validatedKey, len(policy.ImageSigners)),
		approvers:     make(map[string]validatedKey, len(policy.VulnerabilityApprovers)),
		scanners:      make(map[string]struct{}, len(policy.Scanners)),
		vulnerability: policy.VulnerabilityPolicy,
	}
	publicKeys := make(map[string]string, len(policy.ImageSigners)+len(policy.VulnerabilityApprovers))
	if err := validateKeys(policy.ImageSigners, "image signer", result.imageSigners, publicKeys); err != nil {
		return validatedPolicy{}, err
	}
	if err := validateKeys(policy.VulnerabilityApprovers, "vulnerability approver", result.approvers, publicKeys); err != nil {
		return validatedPolicy{}, err
	}
	for _, scanner := range policy.Scanners {
		if !validText(scanner.Name, 100) || !validText(scanner.Version, 100) {
			return validatedPolicy{}, invalidf("trusted scanner identity is invalid")
		}
		key := scanner.Name + "\x00" + scanner.Version
		if _, duplicate := result.scanners[key]; duplicate {
			return validatedPolicy{}, invalidf("trusted scanner identity is duplicated")
		}
		result.scanners[key] = struct{}{}
	}
	return result, nil
}

func validateKeys(keys []TrustedKey, role string, destination map[string]validatedKey, publicKeys map[string]string) error {
	for _, key := range keys {
		if !validText(key.KeyID, 200) || key.Algorithm != "ed25519" {
			return invalidf("%s identity is invalid", role)
		}
		decoded, err := decodeCanonicalBase64(key.PublicKey)
		if err != nil || len(decoded) != ed25519.PublicKeySize {
			return invalidf("%s public key is invalid", role)
		}
		notBefore, err := parseCanonicalTime(key.NotBefore)
		if err != nil {
			return invalidf("%s not_before is invalid", role)
		}
		notAfter, err := parseCanonicalTime(key.NotAfter)
		if err != nil || !notAfter.After(notBefore) {
			return invalidf("%s validity window is invalid", role)
		}
		if _, duplicate := destination[key.KeyID]; duplicate {
			return invalidf("%s key ID is duplicated", role)
		}
		encodedKey := base64.StdEncoding.EncodeToString(decoded)
		if priorRole, duplicate := publicKeys[encodedKey]; duplicate {
			return invalidf("image signers and vulnerability approvers must use independent keys: %s and %s", priorRole, role)
		}
		publicKeys[encodedKey] = role
		destination[key.KeyID] = validatedKey{
			publicKey: ed25519.PublicKey(decoded), notBefore: notBefore, notAfter: notAfter,
		}
	}
	return nil
}

func validateManifestHeader(manifest Manifest, bundle releasebundle.Bundle) error {
	if manifest.SchemaVersion != SchemaVersion || !validDigest(manifest.ReleaseDigest) ||
		!validDigest(manifest.ConfigurationRevision) || manifest.ReleaseDigest != bundle.ReleaseDigest ||
		manifest.ConfigurationRevision != bundle.ConfigurationRevision {
		return invalidf("supply-chain manifest does not bind the release bundle")
	}
	if len(bundle.OCIImages) == 0 || len(bundle.OCIImages) > maxImages ||
		len(manifest.Images) != len(bundle.OCIImages) || len(manifest.PublicationReceipts) == 0 ||
		len(manifest.PublicationReceipts) > maxImages {
		return invalidf("supply-chain manifest must bind the exact release image set")
	}
	for index, image := range bundle.OCIImages {
		if manifest.Images[index].Image != image.Image || !validBundleImage(image) {
			return invalidf("supply-chain manifest must bind the exact release image set")
		}
	}
	return nil
}

func loadPublicationReceipts(reader *evidenceReader, manifest Manifest, bundle releasebundle.Bundle) (map[string]string, error) {
	bundleImages := make(map[string]releasebundle.OCIImage, len(bundle.OCIImages))
	for _, image := range bundle.OCIImages {
		bundleImages[image.Image] = image
	}
	publicationForImage := make(map[string]string, len(bundle.OCIImages))
	digests := make(map[string]struct{}, len(manifest.PublicationReceipts))
	for _, reference := range manifest.PublicationReceipts {
		if !validDigest(reference.Digest) {
			return nil, invalidf("registry publication receipt digest is invalid")
		}
		if _, duplicate := digests[reference.Digest]; duplicate {
			return nil, invalidf("registry publication receipt digest is duplicated")
		}
		digests[reference.Digest] = struct{}{}
		encoded, err := reader.readUnique(reference.Ref, "registry publication receipt", maxMetadataBytes)
		if err != nil {
			return nil, err
		}
		if digestBytes(encoded) != reference.Digest {
			return nil, invalidf("registry publication receipt digest mismatch")
		}
		var receipt registryPublicationReceipt
		if err := decodeStrictJSON(encoded, &receipt); err != nil {
			return nil, invalidf("decode registry publication receipt: %v", err)
		}
		if receipt.SchemaVersion != 1 || !validText(receipt.Revision, 200) ||
			len(receipt.Images) == 0 || len(receipt.Images) > maxImages {
			return nil, invalidf("registry publication receipt is invalid")
		}
		for _, published := range receipt.Images {
			expected, exists := bundleImages[published.Image]
			if !exists || published.ManifestDigest != expected.Descriptor.Digest ||
				published.ManifestMediaType != expected.Descriptor.MediaType ||
				published.ManifestSizeBytes != expected.Descriptor.SizeBytes {
				return nil, invalidf("registry publication receipt does not match the release bundle")
			}
			if _, duplicate := publicationForImage[published.Image]; duplicate {
				return nil, invalidf("release image has multiple registry publication receipts")
			}
			publicationForImage[published.Image] = reference.Digest
		}
	}
	if len(publicationForImage) != len(bundleImages) {
		return nil, invalidf("registry publication receipts do not cover the exact release image set")
	}
	return publicationForImage, nil
}

func verifyImages(
	reader *evidenceReader,
	manifest Manifest,
	bundle releasebundle.Bundle,
	policy validatedPolicy,
	policyDigest string,
	publications map[string]string,
) ([]VerifiedImage, error) {
	verified := make([]VerifiedImage, 0, len(bundle.OCIImages))
	for index, image := range bundle.OCIImages {
		binding := manifest.Images[index]
		if binding.PublicationReceiptDigest != publications[image.Image] {
			return nil, invalidf("image publication receipt binding is invalid for %s", image.Image)
		}
		sbomEncoded, err := reader.readUnique(binding.SBOMRef, "SPDX SBOM for "+image.Image, maxMetadataBytes)
		if err != nil {
			return nil, err
		}
		sbomDigest := digestBytes(sbomEncoded)
		sbomCreated, err := validateSPDX(sbomEncoded, image)
		if err != nil {
			return nil, err
		}
		reportEncoded, err := reader.readUnique(binding.VulnerabilityReportRef, "vulnerability report for "+image.Image, maxMetadataBytes)
		if err != nil {
			return nil, err
		}
		reportDigest := digestBytes(reportEncoded)
		report, scannedAt, err := validateVulnerabilityReport(reader, reportEncoded, image.Image, sbomDigest, sbomCreated, policy)
		if err != nil {
			return nil, err
		}
		statementEncoded, err := reader.readUnique(binding.StatementRef, "image statement for "+image.Image, maxMetadataBytes)
		if err != nil {
			return nil, err
		}
		statementPayload, signerID, err := verifyEnvelope(statementEncoded, imageStatementPayloadType, policy.imageSigners, "image statement signature")
		if err != nil {
			return nil, err
		}
		var statement imageStatement
		if err := decodeStrictJSON(statementPayload, &statement); err != nil {
			return nil, invalidf("decode image statement: %v", err)
		}
		signedAt, err := validateImageStatement(statement, image.Image, bundle, binding.PublicationReceiptDigest, sbomDigest, reportDigest)
		if err != nil {
			return nil, err
		}
		if !keyValidAt(policy.imageSigners[signerID], signedAt) {
			return nil, invalidf("image statement signature is outside the trusted key validity window")
		}
		approvalEncoded, err := reader.readUnique(binding.VulnerabilityApprovalRef, "vulnerability approval for "+image.Image, maxMetadataBytes)
		if err != nil {
			return nil, err
		}
		approvalPayload, approverID, err := verifyEnvelope(approvalEncoded, vulnerabilityApprovalPayloadType, policy.approvers, "vulnerability approval signature")
		if err != nil {
			return nil, err
		}
		var approval vulnerabilityApproval
		if err := decodeStrictJSON(approvalPayload, &approval); err != nil {
			return nil, invalidf("decode vulnerability approval: %v", err)
		}
		approvedAt, err := validateApproval(approval, image.Image, bundle, policyDigest, sbomDigest, reportDigest, report, policy.vulnerability)
		if err != nil {
			return nil, err
		}
		if approvedAt.Before(scannedAt) || approvedAt.Before(signedAt) || !keyValidAt(policy.approvers[approverID], approvedAt) {
			return nil, invalidf("vulnerability approval time or trusted key validity is invalid")
		}
		verified = append(verified, VerifiedImage{
			Image: image.Image, ImageSignerKeyID: signerID, VulnerabilityApproverID: approverID,
			SBOMDigest: sbomDigest, VulnerabilityReportDigest: reportDigest,
		})
	}
	return verified, nil
}

func validateSPDX(encoded []byte, image releasebundle.OCIImage) (time.Time, error) {
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return time.Time{}, invalidf("decode SPDX SBOM: %v", err)
	}
	var document spdxDocument
	if err := json.Unmarshal(encoded, &document); err != nil {
		return time.Time{}, invalidf("decode SPDX SBOM: %v", err)
	}
	if document.SPDXVersion != "SPDX-2.3" || document.DataLicense != "CC0-1.0" ||
		document.SPDXID != "SPDXRef-DOCUMENT" || !validText(document.Name, 500) ||
		len(document.DocumentDescribes) != 1 || len(document.Packages) == 0 ||
		len(document.Packages) > maxSPDXPackages || len(document.CreationInfo.Creators) == 0 ||
		len(document.CreationInfo.Creators) > 64 {
		return time.Time{}, invalidf("SPDX SBOM header is invalid")
	}
	namespace, err := url.Parse(document.DocumentNamespace)
	if err != nil || namespace.Scheme != "https" || namespace.Host == "" || namespace.Fragment != "" {
		return time.Time{}, invalidf("SPDX SBOM document namespace is invalid")
	}
	createdAt, err := parseCanonicalTime(document.CreationInfo.Created)
	if err != nil {
		return time.Time{}, invalidf("SPDX SBOM creation time is invalid")
	}
	for _, creator := range document.CreationInfo.Creators {
		if !validText(creator, 500) {
			return time.Time{}, invalidf("SPDX SBOM creator is invalid")
		}
	}
	repository, digest, _ := strings.Cut(image.Image, "@")
	wantedID := document.DocumentDescribes[0]
	found := false
	ids := make(map[string]struct{}, len(document.Packages))
	for _, candidate := range document.Packages {
		if !validText(candidate.SPDXID, 500) {
			return time.Time{}, invalidf("SPDX SBOM package identity is invalid")
		}
		if _, duplicate := ids[candidate.SPDXID]; duplicate {
			return time.Time{}, invalidf("SPDX SBOM package identity is duplicated")
		}
		ids[candidate.SPDXID] = struct{}{}
		if candidate.SPDXID != wantedID {
			continue
		}
		if found || candidate.Name != repository || candidate.VersionInfo != digest ||
			candidate.DownloadLocation != "NOASSERTION" || candidate.FilesAnalyzed == nil || *candidate.FilesAnalyzed ||
			!hasExactSHA256(candidate.Checksums, strings.TrimPrefix(digest, "sha256:")) {
			return time.Time{}, invalidf("SPDX SBOM subject does not bind image %s", image.Image)
		}
		found = true
	}
	if !found {
		return time.Time{}, invalidf("SPDX SBOM subject does not bind image %s", image.Image)
	}
	return createdAt, nil
}

func validateVulnerabilityReport(
	reader *evidenceReader,
	encoded []byte,
	image, sbomDigest string,
	sbomCreated time.Time,
	policy validatedPolicy,
) (vulnerabilityReport, time.Time, error) {
	var report vulnerabilityReport
	if err := decodeStrictJSON(encoded, &report); err != nil {
		return report, time.Time{}, invalidf("decode vulnerability report: %v", err)
	}
	if report.SBOMDigest != sbomDigest {
		return report, time.Time{}, invalidf("vulnerability report SBOM digest does not match the verified SBOM")
	}
	if report.SchemaVersion != 1 || report.Image != image ||
		!validDigest(report.DatabaseDigest) || !validDigest(report.ScannerOutputDigest) ||
		report.Critical < 0 || report.High < 0 || report.Medium < 0 || report.Low < 0 || report.Unknown != 0 {
		return report, time.Time{}, invalidf("vulnerability report semantics are invalid")
	}
	if _, trusted := policy.scanners[report.ScannerName+"\x00"+report.ScannerVersion]; !trusted {
		return report, time.Time{}, invalidf("vulnerability scanner identity is not trusted")
	}
	databaseUpdatedAt, err := parseCanonicalTime(report.DatabaseUpdatedAt)
	if err != nil {
		return report, time.Time{}, invalidf("vulnerability database time is invalid")
	}
	scannedAt, err := parseCanonicalTime(report.ScannedAt)
	if err != nil || scannedAt.Before(sbomCreated) || databaseUpdatedAt.After(scannedAt) ||
		scannedAt.Sub(databaseUpdatedAt) > time.Duration(policy.vulnerability.MaximumDatabaseAgeSeconds)*time.Second {
		return report, time.Time{}, invalidf("vulnerability scan time or database age is invalid")
	}
	if report.Critical > policy.vulnerability.MaximumCritical || report.High > policy.vulnerability.MaximumHigh {
		return report, time.Time{}, invalidf("vulnerability report exceeds the trusted policy threshold")
	}
	output, err := reader.readUnique(report.ScannerOutputRef, "vulnerability scanner output for "+image, maxMetadataBytes)
	if err != nil {
		return report, time.Time{}, err
	}
	if digestBytes(output) != report.ScannerOutputDigest {
		return report, time.Time{}, invalidf("vulnerability scanner output digest mismatch")
	}
	return report, scannedAt, nil
}

func validateImageStatement(
	statement imageStatement,
	image string,
	bundle releasebundle.Bundle,
	publicationDigest, sbomDigest, reportDigest string,
) (time.Time, error) {
	if statement.SchemaVersion != 1 || statement.Image != image ||
		statement.ReleaseDigest != bundle.ReleaseDigest ||
		statement.ConfigurationRevision != bundle.ConfigurationRevision ||
		statement.PublicationReceiptDigest != publicationDigest || statement.SBOMDigest != sbomDigest {
		return time.Time{}, invalidf("image statement SBOM digest or release binding is invalid")
	}
	if statement.VulnerabilityReportDigest != reportDigest {
		return time.Time{}, invalidf("image statement vulnerability report digest is invalid")
	}
	signedAt, err := parseCanonicalTime(statement.SignedAt)
	if err != nil {
		return time.Time{}, invalidf("image statement signed_at is invalid")
	}
	return signedAt, nil
}

func validateApproval(
	approval vulnerabilityApproval,
	image string,
	bundle releasebundle.Bundle,
	policyDigest, sbomDigest, reportDigest string,
	report vulnerabilityReport,
	policy VulnerabilityPolicy,
) (time.Time, error) {
	if approval.SchemaVersion != 1 || approval.Image != image || approval.Decision != "APPROVED" ||
		approval.ReleaseDigest != bundle.ReleaseDigest ||
		approval.ConfigurationRevision != bundle.ConfigurationRevision ||
		approval.SBOMDigest != sbomDigest || approval.VulnerabilityReportDigest != reportDigest ||
		approval.PolicyDigest != policyDigest || approval.ScannerName != report.ScannerName ||
		approval.ScannerVersion != report.ScannerVersion || approval.DatabaseDigest != report.DatabaseDigest ||
		approval.MaximumCritical != policy.MaximumCritical || approval.MaximumHigh != policy.MaximumHigh ||
		approval.MaximumDatabaseAgeSeconds != policy.MaximumDatabaseAgeSeconds {
		return time.Time{}, invalidf("vulnerability approval does not bind the exact report and trust policy")
	}
	approvedAt, err := parseCanonicalTime(approval.ApprovedAt)
	if err != nil {
		return time.Time{}, invalidf("vulnerability approval time is invalid")
	}
	return approvedAt, nil
}

func verifyEnvelope(encoded []byte, payloadType string, keys map[string]validatedKey, role string) ([]byte, string, error) {
	var envelope dsseEnvelope
	if err := decodeStrictJSON(encoded, &envelope); err != nil {
		return nil, "", invalidf("decode %s: %v", role, err)
	}
	if envelope.PayloadType != payloadType || len(envelope.Signatures) != 1 {
		return nil, "", invalidf("%s envelope is invalid", role)
	}
	signature := envelope.Signatures[0]
	key, trusted := keys[signature.KeyID]
	if !trusted {
		return nil, "", invalidf("%s uses an untrusted key", role)
	}
	payload, err := decodeCanonicalBase64(envelope.Payload)
	if err != nil || len(payload) == 0 || len(payload) > maxMetadataBytes {
		return nil, "", invalidf("%s payload is invalid", role)
	}
	signatureBytes, err := decodeCanonicalBase64(signature.Signature)
	if err != nil || len(signatureBytes) != ed25519.SignatureSize ||
		!ed25519.Verify(key.publicKey, pae(payloadType, payload), signatureBytes) {
		return nil, "", invalidf("%s verification failed", role)
	}
	return payload, signature.KeyID, nil
}

func (reader *evidenceReader) readUnique(reference, role string, maximum int64) ([]byte, error) {
	normalized, err := normalizeReference(reference)
	if err != nil {
		return nil, invalidf("%s reference: %v", role, err)
	}
	if prior, duplicate := reader.used[normalized]; duplicate {
		return nil, invalidf("artifact reference %q is shared by %s and %s", reference, prior, role)
	}
	reader.used[normalized] = role
	file, err := reader.root.Open(normalized)
	if err != nil {
		return nil, invalidf("open %s: %v", role, err)
	}
	defer func() { _ = file.Close() }()
	information, err := file.Stat()
	if err != nil || !information.Mode().IsRegular() || information.Size() <= 0 ||
		information.Size() > int64(maximum) || information.Size() > reader.remaining {
		return nil, invalidf("%s must be a bounded non-empty regular file", role)
	}
	encoded, err := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	if err != nil || len(encoded) == 0 || int64(len(encoded)) > maximum {
		return nil, invalidf("read bounded %s: %v", role, err)
	}
	reader.remaining -= int64(len(encoded))
	return encoded, nil
}

func normalizeReference(reference string) (string, error) {
	if reference == "" || strings.Contains(reference, `\`) || path.Clean(reference) != reference {
		return "", errors.New("reference must be canonical and slash-separated")
	}
	normalized := filepath.FromSlash(reference)
	if !filepath.IsLocal(normalized) || normalized == "." {
		return "", errors.New("reference must be a local relative file path")
	}
	return normalized, nil
}

func validBundleImage(image releasebundle.OCIImage) bool {
	_, digest, found := strings.Cut(image.Image, "@")
	return found && !strings.Contains(digest, "@") && digest == image.Descriptor.Digest &&
		validDigest(digest) && image.Descriptor.MediaType == releasebundle.OCIManifestMediaType &&
		image.Descriptor.SizeBytes > 0 && image.Platform.OS == "linux" && image.Platform.Architecture == "amd64"
}

func hasExactSHA256(checksums []spdxChecksum, expected string) bool {
	found := false
	seen := make(map[string]struct{}, len(checksums))
	for _, checksum := range checksums {
		if _, duplicate := seen[checksum.Algorithm]; duplicate {
			return false
		}
		seen[checksum.Algorithm] = struct{}{}
		if checksum.Algorithm == "SHA256" {
			if checksum.ChecksumValue != expected {
				return false
			}
			found = true
		}
	}
	return found
}

func validDigest(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && len(decoded) == sha256.Size && value == "sha256:"+hex.EncodeToString(decoded)
}

func validText(value string, maximum int) bool {
	return len(value) > 0 && len(value) <= maximum && strings.TrimSpace(value) == value &&
		strings.IndexFunc(value, unicode.IsControl) == -1
}

func parseCanonicalTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil || parsed.Location() != time.UTC || parsed.Format(time.RFC3339) != value {
		return time.Time{}, errors.New("timestamp must be canonical UTC RFC3339 at second precision")
	}
	return parsed, nil
}

func keyValidAt(key validatedKey, instant time.Time) bool {
	return !instant.Before(key.notBefore) && !instant.After(key.notAfter)
}

func decodeCanonicalBase64(value string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil || base64.StdEncoding.EncodeToString(decoded) != value {
		return nil, errors.New("base64 value is not canonical")
	}
	return decoded, nil
}

func pae(payloadType string, payload []byte) []byte {
	var encoded bytes.Buffer
	encoded.WriteString("DSSEv1 ")
	encoded.WriteString(strconv.Itoa(len(payloadType)))
	encoded.WriteByte(' ')
	encoded.WriteString(payloadType)
	encoded.WriteByte(' ')
	encoded.WriteString(strconv.Itoa(len(payload)))
	encoded.WriteByte(' ')
	encoded.Write(payload)
	return encoded.Bytes()
}

func digestBytes(encoded []byte) string {
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func decodeStrictJSON(encoded []byte, destination any) error {
	if len(encoded) == 0 {
		return errors.New("JSON document is empty")
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("JSON document contains trailing data")
	}
	return nil
}

func invalidf(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidEvidence, fmt.Sprintf(format, arguments...))
}

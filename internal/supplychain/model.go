package supplychain

import "errors"

const (
	SchemaVersion                    = 1
	imageStatementPayloadType        = "application/vnd.vela.supply-chain.image-statement.v1+json"
	vulnerabilityApprovalPayloadType = "application/vnd.vela.supply-chain.vulnerability-approval.v1+json"

	maxManifestBytes      = 1 << 20
	maxPolicyBytes        = 1 << 20
	maxMetadataBytes      = 16 << 20
	maxEvidenceBytes      = 128 << 20
	maxImages             = 512
	maxKeys               = 32
	maxScanners           = 32
	maxSPDXPackages       = 4096
	maxDatabaseAgeSeconds = 7 * 24 * 60 * 60
)

var ErrInvalidEvidence = errors.New("invalid supply-chain evidence")

type Manifest struct {
	SchemaVersion         int                    `json:"schema_version"`
	ReleaseDigest         string                 `json:"release_digest"`
	ConfigurationRevision string                 `json:"configuration_revision"`
	PublicationReceipts   []ArtifactReference    `json:"publication_receipts"`
	Images                []ImageEvidenceBinding `json:"images"`
}

type ArtifactReference struct {
	Ref    string `json:"ref"`
	Digest string `json:"digest"`
}

type ImageEvidenceBinding struct {
	Image                    string `json:"image"`
	PublicationReceiptDigest string `json:"publication_receipt_digest"`
	StatementRef             string `json:"statement_ref"`
	SBOMRef                  string `json:"sbom_ref"`
	VulnerabilityReportRef   string `json:"vulnerability_report_ref"`
	VulnerabilityApprovalRef string `json:"approval_ref"`
}

type TrustPolicy struct {
	SchemaVersion          int                 `json:"schema_version"`
	ImageSigners           []TrustedKey        `json:"image_signers"`
	VulnerabilityApprovers []TrustedKey        `json:"vulnerability_approvers"`
	Scanners               []TrustedScanner    `json:"scanners"`
	VulnerabilityPolicy    VulnerabilityPolicy `json:"vulnerability_policy"`
}

type TrustedKey struct {
	KeyID     string `json:"key_id"`
	Algorithm string `json:"algorithm"`
	PublicKey string `json:"public_key"`
	NotBefore string `json:"not_before"`
	NotAfter  string `json:"not_after"`
}

type TrustedScanner struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type VulnerabilityPolicy struct {
	MaximumCritical           int `json:"maximum_critical"`
	MaximumHigh               int `json:"maximum_high"`
	MaximumDatabaseAgeSeconds int `json:"maximum_database_age_seconds"`
}

type Evidence struct {
	ReleaseDigest         string
	ConfigurationRevision string
	ManifestDigest        string
	PolicyDigest          string
	Images                []VerifiedImage
}

type VerifiedImage struct {
	Image                     string
	ImageSignerKeyID          string
	VulnerabilityApproverID   string
	SBOMDigest                string
	VulnerabilityReportDigest string
}

type registryPublicationReceipt struct {
	SchemaVersion int                        `json:"schema_version"`
	Revision      string                     `json:"revision"`
	Images        []registryPublicationImage `json:"images"`
}

type registryPublicationImage struct {
	Image             string `json:"image"`
	ManifestDigest    string `json:"manifest_digest"`
	ManifestMediaType string `json:"manifest_media_type"`
	ManifestSizeBytes int64  `json:"manifest_size_bytes"`
}

type dsseEnvelope struct {
	PayloadType string          `json:"payloadType"`
	Payload     string          `json:"payload"`
	Signatures  []dsseSignature `json:"signatures"`
}

type dsseSignature struct {
	KeyID     string `json:"keyid"`
	Signature string `json:"sig"`
}

type imageStatement struct {
	SchemaVersion             int    `json:"schema_version"`
	Image                     string `json:"image"`
	ReleaseDigest             string `json:"release_digest"`
	ConfigurationRevision     string `json:"configuration_revision"`
	PublicationReceiptDigest  string `json:"publication_receipt_digest"`
	SBOMDigest                string `json:"sbom_digest"`
	VulnerabilityReportDigest string `json:"vulnerability_report_digest"`
	SignedAt                  string `json:"signed_at"`
}

type vulnerabilityApproval struct {
	SchemaVersion             int    `json:"schema_version"`
	Image                     string `json:"image"`
	ReleaseDigest             string `json:"release_digest"`
	ConfigurationRevision     string `json:"configuration_revision"`
	SBOMDigest                string `json:"sbom_digest"`
	VulnerabilityReportDigest string `json:"vulnerability_report_digest"`
	PolicyDigest              string `json:"policy_digest"`
	Decision                  string `json:"decision"`
	ScannerName               string `json:"scanner_name"`
	ScannerVersion            string `json:"scanner_version"`
	DatabaseDigest            string `json:"database_digest"`
	MaximumCritical           int    `json:"maximum_critical"`
	MaximumHigh               int    `json:"maximum_high"`
	MaximumDatabaseAgeSeconds int    `json:"maximum_database_age_seconds"`
	ApprovedAt                string `json:"approved_at"`
}

type vulnerabilityReport struct {
	SchemaVersion       int    `json:"schema_version"`
	Image               string `json:"image"`
	SBOMDigest          string `json:"sbom_digest"`
	ScannerName         string `json:"scanner_name"`
	ScannerVersion      string `json:"scanner_version"`
	DatabaseDigest      string `json:"database_digest"`
	DatabaseUpdatedAt   string `json:"database_updated_at"`
	ScannedAt           string `json:"scanned_at"`
	Critical            int    `json:"critical"`
	High                int    `json:"high"`
	Medium              int    `json:"medium"`
	Low                 int    `json:"low"`
	Unknown             int    `json:"unknown"`
	ScannerOutputRef    string `json:"scanner_output_ref"`
	ScannerOutputDigest string `json:"scanner_output_digest"`
}

type spdxDocument struct {
	SPDXVersion       string           `json:"spdxVersion"`
	DataLicense       string           `json:"dataLicense"`
	SPDXID            string           `json:"SPDXID"`
	Name              string           `json:"name"`
	DocumentNamespace string           `json:"documentNamespace"`
	CreationInfo      spdxCreationInfo `json:"creationInfo"`
	DocumentDescribes []string         `json:"documentDescribes"`
	Packages          []spdxPackage    `json:"packages"`
}

type spdxCreationInfo struct {
	Created  string   `json:"created"`
	Creators []string `json:"creators"`
}

type spdxPackage struct {
	SPDXID           string         `json:"SPDXID"`
	Name             string         `json:"name"`
	VersionInfo      string         `json:"versionInfo"`
	DownloadLocation string         `json:"downloadLocation"`
	FilesAnalyzed    *bool          `json:"filesAnalyzed"`
	Checksums        []spdxChecksum `json:"checksums"`
}

type spdxChecksum struct {
	Algorithm     string `json:"algorithm"`
	ChecksumValue string `json:"checksumValue"`
}

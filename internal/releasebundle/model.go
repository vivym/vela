package releasebundle

import "errors"

const (
	SchemaVersion = 2

	ConfigurationMediaType     = "application/vnd.vela.release.configuration.v2+json"
	ReleaseArtifactType        = "application/vnd.vela.release.bundle.v2+json"
	ReleaseDescriptorMediaType = "application/vnd.vela.release.descriptor.v2+json"
	OCIManifestMediaType       = "application/vnd.oci.image.manifest.v1+json"
	OCIImageConfigMediaType    = "application/vnd.oci.image.config.v1+json"

	maxPlanBytes          = 4 << 20
	maxBundleBytes        = 16 << 20
	maxMetadataBytes      = 16 << 20
	maxPackageBytes       = 256 << 20
	maxArtifactBytes      = 1 << 30
	maxArtifactCount      = 4096
	maxWorkerNodeCount    = 1024
	maxYAMLArtifactBytes  = 256 << 10
	maxYAMLDocuments      = 128
	maxYAMLNodes          = 100_000
	maxYAMLGraphDocuments = 4096
	maxYAMLGraphNodes     = 400_000
	maxYAMLDepth          = 64
	maxYAMLAliases        = 0
)

var ErrInvalidBundle = errors.New("invalid release bundle")

type ArtifactInput struct {
	Name string `json:"name"`
	Ref  string `json:"ref"`
}

type PackageInput struct {
	Name        string `json:"name"`
	ContractRef string `json:"contract_ref"`
	ArtifactRef string `json:"artifact_ref"`
}

type ExternalResource struct {
	Kind         string   `json:"kind"`
	Namespace    string   `json:"namespace"`
	Name         string   `json:"name"`
	Revision     string   `json:"revision"`
	RequiredKeys []string `json:"required_keys,omitempty"`
	Consumers    []string `json:"consumers,omitempty"`
}

type Platform struct {
	Architecture string `json:"architecture"`
	OS           string `json:"os"`
}

type OCIManifestInput struct {
	Image     string `json:"image"`
	Ref       string `json:"ref"`
	ConfigRef string `json:"config_ref"`
}

type BuildPlan struct {
	SchemaVersion     int                `json:"schema_version"`
	FinalRenders      []ArtifactInput    `json:"final_renders"`
	NodeAgentUnit     ArtifactInput      `json:"node_agent_unit"`
	Packages          []PackageInput     `json:"packages"`
	ExternalResources []ExternalResource `json:"external_resources"`
	OCIManifests      []OCIManifestInput `json:"oci_manifests"`
}

type Artifact struct {
	Ref       string `json:"ref"`
	MediaType string `json:"media_type"`
	Digest    string `json:"digest"`
	SizeBytes int64  `json:"size_bytes"`
}

type NamedArtifact struct {
	Name     string   `json:"name"`
	Artifact Artifact `json:"artifact"`
}

type Package struct {
	Name     string   `json:"name"`
	Contract Artifact `json:"contract"`
	Artifact Artifact `json:"artifact"`
}

type OCIImage struct {
	Image      string   `json:"image"`
	Descriptor Artifact `json:"descriptor"`
	Config     Artifact `json:"config"`
	Platform   Platform `json:"platform"`
}

type ConfigurationManifest struct {
	SchemaVersion     int                `json:"schema_version"`
	MediaType         string             `json:"media_type"`
	SourceRevision    string             `json:"source_revision"`
	FinalRenders      []NamedArtifact    `json:"final_renders"`
	NodeAgentUnit     NamedArtifact      `json:"node_agent_unit"`
	Packages          []Package          `json:"packages"`
	ExternalResources []ExternalResource `json:"external_resources"`
}

type Descriptor struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Platform    *Platform         `json:"platform,omitempty"`
}

type ReleaseDescriptor struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType"`
	ArtifactType  string       `json:"artifactType"`
	Config        Descriptor   `json:"config"`
	Manifests     []Descriptor `json:"manifests"`
}

type Bundle struct {
	SchemaVersion         int                   `json:"schema_version"`
	ReleaseDigest         string                `json:"release_digest"`
	ConfigurationRevision string                `json:"configuration_revision"`
	ConfigurationManifest ConfigurationManifest `json:"configuration_manifest"`
	ReleaseDescriptor     ReleaseDescriptor     `json:"release_descriptor"`
	OCIImages             []OCIImage            `json:"oci_images"`
}

type PackageContract struct {
	SchemaVersion     int    `json:"schema_version"`
	Name              string `json:"name"`
	OS                string `json:"os"`
	Architecture      string `json:"architecture"`
	Revision          string `json:"revision"`
	Entrypoint        string `json:"entrypoint"`
	ArtifactDigest    string `json:"artifact_digest"`
	ArtifactSizeBytes int64  `json:"artifact_size_bytes"`
}

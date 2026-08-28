package releasebundle

import "errors"

const (
	SchemaVersion = 1

	ConfigurationMediaType     = "application/vnd.vela.release.configuration.v1+json"
	ReleaseArtifactType        = "application/vnd.vela.release.bundle.v1+json"
	ReleaseDescriptorMediaType = "application/vnd.vela.release.descriptor.v1+json"
	OCIManifestMediaType       = "application/vnd.oci.image.manifest.v1+json"
	OCIImageConfigMediaType    = "application/vnd.oci.image.config.v1+json"

	maxPlanBytes       = 4 << 20
	maxBundleBytes     = 16 << 20
	maxMetadataBytes   = 16 << 20
	maxPackageBytes    = 256 << 20
	maxArtifactBytes   = 1 << 30
	maxArtifactCount   = 4096
	maxWorkerNodeCount = 1024
	maxYAMLDocuments   = 128
	maxYAMLNodes       = 100_000
	maxYAMLDepth       = 64
	maxYAMLAliases     = 0
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

type WorkerMaterializationInput struct {
	NodeIdentity                   string `json:"node_identity"`
	Namespace                      string `json:"namespace"`
	WorkerID                       string `json:"worker_id"`
	WorkerEpoch                    int64  `json:"worker_epoch"`
	WorkerPoolID                   string `json:"worker_pool_id"`
	FleetRevision                  string `json:"fleet_revision"`
	NodeAgentIdentity              string `json:"node_agent_identity"`
	WorkerRuntimeConfigMap         string `json:"worker_runtime_config_map"`
	WorkerRuntimeRef               string `json:"worker_runtime_ref"`
	RunnerProfilesConfigMap        string `json:"runner_profiles_config_map"`
	RunnerProfilesRef              string `json:"runner_profiles_ref"`
	RunnerGPURolesConfigMap        string `json:"runner_gpu_roles_config_map"`
	RunnerGPURolesRef              string `json:"runner_gpu_roles_ref"`
	WorkerControlTLSSecret         string `json:"worker_control_tls_secret"`
	WorkerControlTLSSecretRevision string `json:"worker_control_tls_secret_revision"`
	ExecutionProfileRevisionID     string `json:"execution_profile_revision_id"`
	InferenceBackendRevision       string `json:"inference_backend_revision"`
	ModelRevisionID                string `json:"model_revision_id"`
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
	SchemaVersion          int                          `json:"schema_version"`
	FinalRenders           []ArtifactInput              `json:"final_renders"`
	NodeAgentUnit          ArtifactInput                `json:"node_agent_unit"`
	Packages               []PackageInput               `json:"packages"`
	WorkerMaterializations []WorkerMaterializationInput `json:"worker_materializations"`
	ExternalResources      []ExternalResource           `json:"external_resources"`
	OCIManifests           []OCIManifestInput           `json:"oci_manifests"`
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

type WorkerMaterialization struct {
	NodeIdentity                   string   `json:"node_identity"`
	Namespace                      string   `json:"namespace"`
	WorkerID                       string   `json:"worker_id"`
	WorkerEpoch                    int64    `json:"worker_epoch"`
	WorkerPoolID                   string   `json:"worker_pool_id"`
	FleetRevision                  string   `json:"fleet_revision"`
	NodeAgentIdentity              string   `json:"node_agent_identity"`
	WorkerRuntimeConfigMap         string   `json:"worker_runtime_config_map"`
	WorkerRuntime                  Artifact `json:"worker_runtime"`
	RunnerProfilesConfigMap        string   `json:"runner_profiles_config_map"`
	RunnerProfiles                 Artifact `json:"runner_profiles"`
	RunnerGPURolesConfigMap        string   `json:"runner_gpu_roles_config_map"`
	RunnerGPURoles                 Artifact `json:"runner_gpu_roles"`
	WorkerControlTLSSecret         string   `json:"worker_control_tls_secret"`
	WorkerControlTLSSecretRevision string   `json:"worker_control_tls_secret_revision"`
	ExecutionProfileRevisionID     string   `json:"execution_profile_revision_id"`
	InferenceBackendRevision       string   `json:"inference_backend_revision"`
	ModelRevisionID                string   `json:"model_revision_id"`
}

type OCIImage struct {
	Image      string   `json:"image"`
	Descriptor Artifact `json:"descriptor"`
	Config     Artifact `json:"config"`
	Platform   Platform `json:"platform"`
}

type ConfigurationManifest struct {
	SchemaVersion          int                     `json:"schema_version"`
	MediaType              string                  `json:"media_type"`
	FinalRenders           []NamedArtifact         `json:"final_renders"`
	NodeAgentUnit          NamedArtifact           `json:"node_agent_unit"`
	Packages               []Package               `json:"packages"`
	WorkerMaterializations []WorkerMaterialization `json:"worker_materializations"`
	ExternalResources      []ExternalResource      `json:"external_resources"`
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

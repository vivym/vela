package labv2contract

const (
	Namespace           = "vela-lab-v2"
	GraphID             = "84000000-0000-0000-0000-000000000501"
	StageAuthorityKeyID = "lab-stage-authority-v1"

	Worker1Name         = "vela-lab-worker-1"
	Worker2Name         = "vela-lab-worker-2"
	ThumbnailWorkerName = "vela-lab-control-1"

	Worker1ID         = "84000000-0000-0000-0000-000000000101"
	Worker2ID         = "84000000-0000-0000-0000-000000000102"
	ThumbnailWorkerID = "84000000-0000-0000-0000-000000000103"

	Worker1MemberID         = "84000000-0000-0000-0000-000000000111"
	Worker2MemberID         = "84000000-0000-0000-0000-000000000112"
	ThumbnailWorkerMemberID = "84000000-0000-0000-0000-000000000113"

	Worker1DeviceID   = "84000000-0000-0000-0000-000000000121"
	Worker2DeviceID   = "84000000-0000-0000-0000-000000000122"
	ThumbnailDeviceID = "84000000-0000-0000-0000-000000000123"

	Worker1NodeID   = "84000000-0000-0000-0000-000000000131"
	Worker2NodeID   = "84000000-0000-0000-0000-000000000132"
	ThumbnailNodeID = "84000000-0000-0000-0000-000000000133"

	Worker1DeviceSetID   = "84000000-0000-0000-0000-000000000141"
	Worker2DeviceSetID   = "84000000-0000-0000-0000-000000000142"
	ThumbnailDeviceSetID = "84000000-0000-0000-0000-000000000143"

	AuxWorkerProfileID       = "84000000-0000-0000-0000-000000000301"
	DiTWorkerProfileID       = "84000000-0000-0000-0000-000000000302"
	ThumbnailWorkerProfileID = "84000000-0000-0000-0000-000000000303"

	EncoderResidencyID   = "84000000-0000-0000-0000-000000000401"
	VAEResidencyID       = "84000000-0000-0000-0000-000000000402"
	DiTResidencyID       = "84000000-0000-0000-0000-000000000403"
	ThumbnailResidencyID = "84000000-0000-0000-0000-000000000404"

	EncoderStageProfileID   = "49000000-0000-0000-0000-000000000040"
	DiTStageProfileID       = "49000000-0000-0000-0000-000000000041"
	VAEStageProfileID       = "49000000-0000-0000-0000-000000000042"
	ThumbnailStageProfileID = "49000000-0000-0000-0000-000000000043"

	EncoderStageDefinitionID   = "49000000-0000-0000-0000-000000000030"
	DiTStageDefinitionID       = "49000000-0000-0000-0000-000000000031"
	VAEStageDefinitionID       = "49000000-0000-0000-0000-000000000032"
	ThumbnailStageDefinitionID = "49000000-0000-0000-0000-000000000033"

	EncoderEquivalenceID   = "49000000-0000-0000-0000-000000000023"
	DiTEquivalenceID       = "49000000-0000-0000-0000-000000000024"
	VAEEquivalenceID       = "49000000-0000-0000-0000-000000000025"
	ThumbnailEquivalenceID = "49000000-0000-0000-0000-000000000026"

	EncoderCapacityPoolID   = "84000000-0000-0000-0000-000000000611"
	VAECapacityPoolID       = "84000000-0000-0000-0000-000000000612"
	DiTCapacityPoolID       = "84000000-0000-0000-0000-000000000613"
	ThumbnailCapacityPoolID = "84000000-0000-0000-0000-000000000614"

	Worker1BundleID         = "84000000-0000-0000-0000-000000000621"
	Worker2BundleID         = "84000000-0000-0000-0000-000000000622"
	ThumbnailWorkerBundleID = "84000000-0000-0000-0000-000000000623"

	EncoderComponent         = "ENCODER"
	DiTComponent             = "DIT"
	VAEComponent             = "VAE_DECODER"
	EncoderComponentRevision = "h3-encoder-v1"
	DiTComponentRevision     = "h3-dit-v1"
	VAEComponentRevision     = "h3-vae-v1"
	EncoderRuntimeCommand    = "/opt/vela/bin/h3-encoder"
	DiTRuntimeCommand        = "/opt/vela/bin/h3-dit"
	VAERuntimeCommand        = "/opt/vela/bin/h3-vae-decoder"
	AuxSharedSlot            = "H3_AUX_ENCODER_VAE"

	ThumbnailComponent         = "CPU_MEDIA"
	ThumbnailComponentRevision = "lab-thumbnail-mock-v1"
	ThumbnailWorkerRole        = "cpu-thumbnail"
	ThumbnailResourceClass     = "CPU"
	GPUResourceClass           = "GPU"
	ThumbnailRuntimeCommand    = "/usr/local/bin/vela-lab-cpu-thumbnail-mock"

	ThumbnailCPUMilli       int64 = 2000
	ThumbnailMemoryBytes    int64 = 4294967296
	ThumbnailScratchBytes   int64 = 34359738368
	ThumbnailConcurrency    int64 = 1
	ThumbnailCapacityVector       = `{"cpu_milli":2000,"memory_bytes":4294967296,"scratch_bytes":34359738368,"concurrency":1}`
	GPUCapacityVector             = `{"concurrency":1}`
)

type RuntimeImageClass string

const (
	H3RuntimeImageClass        RuntimeImageClass = "H3_RUNTIME"
	BootstrapRuntimeImageClass RuntimeImageClass = "BOOTSTRAP_RUNTIME"
)

type StageDescriptor struct {
	Key                     string
	ProfileID               string
	StableID                string
	DefinitionID            string
	Component               string
	ComponentRevision       string
	RuntimeIdentityPrefix   string
	RuntimeCommand          string
	ResidencyID             string
	EquivalenceID           string
	CapacityPoolID          string
	WorkerProfileID         string
	CertifiedCapacityVector string
	ProfileContentDigest    byte
	RuntimeImageClass       RuntimeImageClass
}

type WorkerDescriptor struct {
	AssetIndex            string
	Name                  string
	InstanceID            string
	MemberID              string
	DeviceID              string
	NodeID                string
	DeviceSetID           string
	WorkerProfileID       string
	WorkerProfileStableID string
	WorkerProfileDigest   byte
	Role                  string
	SharedSlot            string
	DeviceSetKind         string
	ResourceClass         string
	PrimaryCapacityPoolID string
	BundleID              string
	GPUUUID               string
	PCIBDF                string
	StageKeys             []string
	CapacityVector        map[string]int64
	ReadinessChecks       map[string]bool
}

func StageDescriptors() []StageDescriptor {
	return []StageDescriptor{
		{
			Key: "encoder", ProfileID: EncoderStageProfileID, StableID: "h3-encoder-lab-v2",
			DefinitionID: EncoderStageDefinitionID, Component: EncoderComponent,
			ComponentRevision: EncoderComponentRevision, RuntimeIdentityPrefix: "lab-h3-encoder",
			RuntimeCommand: EncoderRuntimeCommand, ResidencyID: EncoderResidencyID,
			EquivalenceID: EncoderEquivalenceID, CapacityPoolID: EncoderCapacityPoolID,
			WorkerProfileID: AuxWorkerProfileID, CertifiedCapacityVector: GPUCapacityVector,
			ProfileContentDigest: 0x50, RuntimeImageClass: H3RuntimeImageClass,
		},
		{
			Key: "dit", ProfileID: DiTStageProfileID, StableID: "h3-dit-lab-v2",
			DefinitionID: DiTStageDefinitionID, Component: DiTComponent,
			ComponentRevision: DiTComponentRevision, RuntimeIdentityPrefix: "lab-h3-dit",
			RuntimeCommand: DiTRuntimeCommand, ResidencyID: DiTResidencyID,
			EquivalenceID: DiTEquivalenceID, CapacityPoolID: DiTCapacityPoolID,
			WorkerProfileID: DiTWorkerProfileID, CertifiedCapacityVector: GPUCapacityVector,
			ProfileContentDigest: 0x51, RuntimeImageClass: H3RuntimeImageClass,
		},
		{
			Key: "vae", ProfileID: VAEStageProfileID, StableID: "h3-vae-lab-v2",
			DefinitionID: VAEStageDefinitionID, Component: VAEComponent,
			ComponentRevision: VAEComponentRevision, RuntimeIdentityPrefix: "lab-h3-vae",
			RuntimeCommand: VAERuntimeCommand, ResidencyID: VAEResidencyID,
			EquivalenceID: VAEEquivalenceID, CapacityPoolID: VAECapacityPoolID,
			WorkerProfileID: AuxWorkerProfileID, CertifiedCapacityVector: GPUCapacityVector,
			ProfileContentDigest: 0x52, RuntimeImageClass: H3RuntimeImageClass,
		},
		{
			Key: "thumbnail", ProfileID: ThumbnailStageProfileID, StableID: "h3-cpu-thumbnail-lab-v2",
			DefinitionID: ThumbnailStageDefinitionID, Component: ThumbnailComponent,
			ComponentRevision: ThumbnailComponentRevision, RuntimeIdentityPrefix: "lab-thumbnail-mock",
			RuntimeCommand: ThumbnailRuntimeCommand, ResidencyID: ThumbnailResidencyID,
			EquivalenceID: ThumbnailEquivalenceID, CapacityPoolID: ThumbnailCapacityPoolID,
			WorkerProfileID: ThumbnailWorkerProfileID, CertifiedCapacityVector: ThumbnailCapacityVector,
			ProfileContentDigest: 0x53, RuntimeImageClass: BootstrapRuntimeImageClass,
		},
	}
}

func WorkerDescriptors() []WorkerDescriptor {
	return []WorkerDescriptor{
		{
			AssetIndex: "1", Name: Worker1Name, InstanceID: Worker1ID, MemberID: Worker1MemberID,
			DeviceID: Worker1DeviceID, NodeID: Worker1NodeID, DeviceSetID: Worker1DeviceSetID,
			WorkerProfileID: AuxWorkerProfileID, WorkerProfileStableID: "h3-aux-single-gpu-lab-v2",
			WorkerProfileDigest: 0x33, Role: "aux", SharedSlot: AuxSharedSlot,
			DeviceSetKind: "single-gpu", ResourceClass: GPUResourceClass,
			PrimaryCapacityPoolID: EncoderCapacityPoolID, BundleID: Worker1BundleID,
			GPUUUID: "GPU-84000000-0000-0000-0000-000000000121", PCIBDF: "0000:41:00.0",
			StageKeys: []string{"encoder", "vae"}, CapacityVector: map[string]int64{"concurrency": 1},
			ReadinessChecks: map[string]bool{"membership_barrier": true, "warmup": true},
		},
		{
			AssetIndex: "2", Name: Worker2Name, InstanceID: Worker2ID, MemberID: Worker2MemberID,
			DeviceID: Worker2DeviceID, NodeID: Worker2NodeID, DeviceSetID: Worker2DeviceSetID,
			WorkerProfileID: DiTWorkerProfileID, WorkerProfileStableID: "h3-dit-single-gpu-lab-v2",
			WorkerProfileDigest: 0x34, Role: "dit", DeviceSetKind: "single-gpu",
			ResourceClass: GPUResourceClass, PrimaryCapacityPoolID: DiTCapacityPoolID,
			BundleID: Worker2BundleID, GPUUUID: "GPU-84000000-0000-0000-0000-000000000122",
			PCIBDF: "0000:42:00.0", StageKeys: []string{"dit"},
			CapacityVector:  map[string]int64{"concurrency": 1},
			ReadinessChecks: map[string]bool{"membership_barrier": true, "warmup": true},
		},
		{
			AssetIndex: "thumbnail", Name: ThumbnailWorkerName, InstanceID: ThumbnailWorkerID,
			MemberID: ThumbnailWorkerMemberID, DeviceID: ThumbnailDeviceID, NodeID: ThumbnailNodeID,
			DeviceSetID: ThumbnailDeviceSetID, WorkerProfileID: ThumbnailWorkerProfileID,
			WorkerProfileStableID: "h3-cpu-thumbnail-worker-lab-v2", WorkerProfileDigest: 0x35,
			Role: ThumbnailWorkerRole, DeviceSetKind: "cpu-slot", ResourceClass: ThumbnailResourceClass,
			PrimaryCapacityPoolID: ThumbnailCapacityPoolID, BundleID: ThumbnailWorkerBundleID,
			StageKeys: []string{"thumbnail"},
			CapacityVector: map[string]int64{
				"cpu_milli": ThumbnailCPUMilli, "memory_bytes": ThumbnailMemoryBytes,
				"scratch_bytes": ThumbnailScratchBytes, "concurrency": ThumbnailConcurrency,
			},
			ReadinessChecks: map[string]bool{"process": true, "warmup": true},
		},
	}
}

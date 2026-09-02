package modelruntime

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/strictjson"
)

const (
	launchManifestSchemaVersion = 1
	maxLaunchManifestBytes      = 1 << 20
	maxLaunchDevices            = 64
	maxLaunchMembers            = 64
	maxLaunchRuntimes           = 2
	maxLaunchEnvironmentEntries = 128
	maxLaunchEnvironmentBytes   = 4096
	h3AUXSharedSlotException    = "H3_AUX_ENCODER_VAE"
)

var launchComponentPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,99}$`)

type LaunchManifest struct {
	SchemaVersion           int                 `json:"schema_version"`
	WorkerProfileRevisionID string              `json:"worker_profile_revision_id"`
	WorkerRole              string              `json:"worker_role"`
	CapacitySlots           int                 `json:"capacity_slots"`
	SharedSlotException     string              `json:"shared_slot_exception,omitempty"`
	WorkerInstanceID        string              `json:"worker_instance_id"`
	WorkerInstanceEpoch     int64               `json:"worker_instance_epoch"`
	WorkerMemberID          string              `json:"worker_member_id"`
	WorkerMemberEpoch       int64               `json:"worker_member_epoch"`
	DeviceSetDigest         string              `json:"device_set_digest"`
	MembershipDigest        string              `json:"membership_digest"`
	Devices                 []LaunchDeviceEpoch `json:"devices"`
	Members                 []LaunchMemberEpoch `json:"members"`
	LocalDevices            []DriverDevice      `json:"local_devices"`
	Runtimes                []LaunchRuntime     `json:"runtimes"`
}

type LaunchDeviceEpoch struct {
	ID    string `json:"id"`
	Epoch int64  `json:"epoch"`
}

type LaunchMemberEpoch struct {
	ID    string `json:"id"`
	Epoch int64  `json:"epoch"`
}

type LaunchRuntime struct {
	ModelResidencyID       string   `json:"model_residency_id"`
	RuntimeIdentity        string   `json:"runtime_identity"`
	StageProfileRevisionID string   `json:"stage_profile_revision_id"`
	ModelRuntimeEpochFloor int64    `json:"model_runtime_epoch_floor"`
	Component              string   `json:"component"`
	ModelComponentRevision string   `json:"model_component_revision"`
	RuntimeImageDigest     string   `json:"runtime_image_digest"`
	Command                []string `json:"command"`
	Environment            []string `json:"environment,omitempty"`
	ScratchRoot            string   `json:"scratch_root"`
	InputRoot              string   `json:"input_root"`
	OutputRoot             string   `json:"output_root"`
	InitializationTimeout  string   `json:"initialization_timeout"`
	ShutdownTimeout        string   `json:"shutdown_timeout"`
}

func LoadLaunchManifest(path string) (LaunchManifest, error) {
	encoded, err := securefile.Read(path, maxLaunchManifestBytes, true)
	if err != nil {
		return LaunchManifest{}, fmt.Errorf("read ModelRuntime launch manifest: %w", err)
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return LaunchManifest{}, fmt.Errorf("decode ModelRuntime launch manifest: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var manifest LaunchManifest
	if err := decoder.Decode(&manifest); err != nil {
		return LaunchManifest{}, fmt.Errorf("decode ModelRuntime launch manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return LaunchManifest{}, errors.New("ModelRuntime launch manifest must contain one JSON document")
	}
	if err := validateLaunchManifest(manifest); err != nil {
		return LaunchManifest{}, err
	}
	return cloneLaunchManifest(manifest), nil
}

func EncodeLaunchManifest(manifest LaunchManifest) ([]byte, error) {
	if err := validateLaunchManifest(manifest); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("encode ModelRuntime launch manifest: %w", err)
	}
	if len(encoded) > maxLaunchManifestBytes {
		return nil, errors.New("ModelRuntime launch manifest exceeds the size limit")
	}
	return encoded, nil
}

func (manifest LaunchManifest) RuntimeBindings() ([]stageauthority.RuntimeBinding, error) {
	if err := validateLaunchManifest(manifest); err != nil {
		return nil, err
	}
	deviceSetDigest, _ := hex.DecodeString(manifest.DeviceSetDigest)
	membershipDigest, _ := hex.DecodeString(manifest.MembershipDigest)
	bindings := make([]stageauthority.RuntimeBinding, 0, len(manifest.Runtimes))
	for _, runtime := range manifest.Runtimes {
		binding := stageauthority.RuntimeBinding{
			WorkerInstanceID: manifest.WorkerInstanceID, WorkerInstanceEpoch: manifest.WorkerInstanceEpoch,
			WorkerMemberID: manifest.WorkerMemberID, WorkerMemberEpoch: manifest.WorkerMemberEpoch,
			DeviceSetDigest:  append([]byte(nil), deviceSetDigest...),
			MembershipDigest: append([]byte(nil), membershipDigest...),
			ModelResidencyID: runtime.ModelResidencyID, ModelRuntimeIdentity: runtime.RuntimeIdentity,
			StageProfileRevisionID: runtime.StageProfileRevisionID,
		}
		for _, device := range manifest.Devices {
			binding.Devices = append(binding.Devices, stageauthority.DeviceEpoch{ID: device.ID, Epoch: device.Epoch})
		}
		for _, member := range manifest.Members {
			binding.Members = append(binding.Members, stageauthority.MemberEpoch{ID: member.ID, Epoch: member.Epoch})
		}
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

func (runtime LaunchRuntime) ProcessBackendConfig(localDevices []DriverDevice) (ProcessBackendConfig, error) {
	initializationTimeout, err := time.ParseDuration(runtime.InitializationTimeout)
	if err != nil {
		return ProcessBackendConfig{}, errors.New("ModelRuntime initialization timeout is invalid")
	}
	shutdownTimeout, err := time.ParseDuration(runtime.ShutdownTimeout)
	if err != nil {
		return ProcessBackendConfig{}, errors.New("ModelRuntime shutdown timeout is invalid")
	}
	return ProcessBackendConfig{
		Component: runtime.Component, ModelComponentRevision: runtime.ModelComponentRevision,
		Command: append([]string(nil), runtime.Command...), Environment: append([]string(nil), runtime.Environment...),
		LocalDevices: append([]DriverDevice(nil), localDevices...), ScratchRoot: runtime.ScratchRoot,
		InputRoot: runtime.InputRoot, OutputRoot: runtime.OutputRoot, InitializationTimeout: initializationTimeout,
		ShutdownTimeout: shutdownTimeout,
	}, nil
}

func validateLaunchManifest(manifest LaunchManifest) error {
	if manifest.SchemaVersion != launchManifestSchemaVersion ||
		uuid.Validate(manifest.WorkerProfileRevisionID) != nil || manifest.CapacitySlots <= 0 ||
		manifest.CapacitySlots > 100 ||
		uuid.Validate(manifest.WorkerInstanceID) != nil || manifest.WorkerInstanceEpoch <= 0 ||
		uuid.Validate(manifest.WorkerMemberID) != nil || manifest.WorkerMemberEpoch <= 0 ||
		!validLaunchDigest(manifest.DeviceSetDigest) || !validLaunchDigest(manifest.MembershipDigest) ||
		len(manifest.Devices) == 0 || len(manifest.Devices) > maxLaunchDevices ||
		len(manifest.Members) == 0 || len(manifest.Members) > maxLaunchMembers ||
		len(manifest.LocalDevices) == 0 || len(manifest.LocalDevices) > maxLaunchDevices ||
		len(manifest.Runtimes) == 0 || len(manifest.Runtimes) > maxLaunchRuntimes {
		return errors.New("ModelRuntime launch manifest authority is incomplete")
	}
	devices := make(map[string]int64, len(manifest.Devices))
	for _, device := range manifest.Devices {
		if uuid.Validate(device.ID) != nil || device.Epoch <= 0 {
			return errors.New("ModelRuntime launch manifest DeviceSet is invalid")
		}
		if _, duplicate := devices[device.ID]; duplicate {
			return errors.New("ModelRuntime launch manifest DeviceSet is duplicated")
		}
		devices[device.ID] = device.Epoch
	}
	members := make(map[string]int64, len(manifest.Members))
	for _, member := range manifest.Members {
		if uuid.Validate(member.ID) != nil || member.Epoch <= 0 {
			return errors.New("ModelRuntime launch manifest membership is invalid")
		}
		if _, duplicate := members[member.ID]; duplicate {
			return errors.New("ModelRuntime launch manifest membership is duplicated")
		}
		members[member.ID] = member.Epoch
	}
	if members[manifest.WorkerMemberID] != manifest.WorkerMemberEpoch {
		return errors.New("ModelRuntime launch manifest omits its WorkerMember epoch")
	}
	seenLocalDevices := make(map[string]struct{}, len(manifest.LocalDevices))
	seenGPUs := make(map[string]struct{}, len(manifest.LocalDevices))
	seenBDFs := make(map[string]struct{}, len(manifest.LocalDevices))
	for _, device := range manifest.LocalDevices {
		if devices[device.DeviceID] != device.DeviceEpoch ||
			!driverGPUUUIDPattern.MatchString(device.GPUUUID) || !driverPCIBDFPattern.MatchString(device.PCIBDF) {
			return errors.New("ModelRuntime local DeviceSet does not match launch authority")
		}
		if _, duplicate := seenLocalDevices[device.DeviceID]; duplicate {
			return errors.New("ModelRuntime local DeviceSet is duplicated")
		}
		if _, duplicate := seenGPUs[device.GPUUUID]; duplicate {
			return errors.New("ModelRuntime local GPU identity is duplicated")
		}
		if _, duplicate := seenBDFs[device.PCIBDF]; duplicate {
			return errors.New("ModelRuntime local PCI identity is duplicated")
		}
		seenLocalDevices[device.DeviceID] = struct{}{}
		seenGPUs[device.GPUUUID] = struct{}{}
		seenBDFs[device.PCIBDF] = struct{}{}
	}
	routes := make(map[string]struct{}, len(manifest.Runtimes))
	residencies := make(map[string]struct{}, len(manifest.Runtimes))
	for _, runtime := range manifest.Runtimes {
		if err := validateLaunchRuntime(runtime); err != nil {
			return err
		}
		if _, duplicate := residencies[runtime.ModelResidencyID]; duplicate {
			return errors.New("ModelRuntime launch manifest reuses a ModelResidency")
		}
		residencies[runtime.ModelResidencyID] = struct{}{}
		route := strings.Join([]string{
			runtime.ModelResidencyID, runtime.RuntimeIdentity, runtime.StageProfileRevisionID,
		}, "\x00")
		if _, duplicate := routes[route]; duplicate {
			return errors.New("ModelRuntime launch manifest runtime route is duplicated")
		}
		routes[route] = struct{}{}
	}
	return validateLaunchTopology(manifest)
}

func validateLaunchTopology(manifest LaunchManifest) error {
	components := make(map[string]int, len(manifest.Runtimes))
	for _, runtime := range manifest.Runtimes {
		components[runtime.Component]++
	}
	standardH3 := func(component string) bool {
		return manifest.SharedSlotException == "" && manifest.CapacitySlots == 1 &&
			len(manifest.Runtimes) == 1 && components[component] == 1 &&
			len(manifest.Devices) == 1 && len(manifest.Members) == 1 && len(manifest.LocalDevices) == 1
	}
	switch manifest.WorkerRole {
	case "encoder":
		if standardH3("ENCODER") {
			return nil
		}
	case "dit":
		if standardH3("DIT") {
			return nil
		}
	case "vae":
		if standardH3("VAE_DECODER") {
			return nil
		}
	case "aux":
		if manifest.SharedSlotException == h3AUXSharedSlotException && manifest.CapacitySlots == 1 &&
			len(manifest.Runtimes) == 2 && components["ENCODER"] == 1 && components["VAE_DECODER"] == 1 &&
			len(manifest.Devices) == 1 && len(manifest.Members) == 1 && len(manifest.LocalDevices) == 1 {
			return nil
		}
	case "llm":
		if manifest.SharedSlotException == "" && len(manifest.Runtimes) == 1 && components["LLM"] == 1 {
			return nil
		}
	}
	return errors.New("ModelRuntime launch manifest does not match a certified WorkerProfile topology")
}

func validateLaunchRuntime(runtime LaunchRuntime) error {
	if uuid.Validate(runtime.ModelResidencyID) != nil || uuid.Validate(runtime.StageProfileRevisionID) != nil ||
		runtime.ModelRuntimeEpochFloor < 0 ||
		!validDriverText(runtime.RuntimeIdentity, 300) || !launchComponentPattern.MatchString(runtime.Component) ||
		!validDriverText(runtime.ModelComponentRevision, 300) || !validLaunchDigest(runtime.RuntimeImageDigest) ||
		len(runtime.Command) == 0 || len(runtime.Command) > 128 || !filepath.IsAbs(runtime.Command[0]) ||
		filepath.Clean(runtime.Command[0]) != runtime.Command[0] ||
		!validLaunchRoot(runtime.ScratchRoot) || !validLaunchRoot(runtime.InputRoot) ||
		!validLaunchRoot(runtime.OutputRoot) || runtime.InputRoot == runtime.OutputRoot {
		return errors.New("ModelRuntime launch manifest runtime is invalid")
	}
	for _, argument := range runtime.Command {
		if !validDriverText(argument, 4096) {
			return errors.New("ModelRuntime launch manifest driver command is invalid")
		}
	}
	for name, root := range map[string]string{"input": runtime.InputRoot, "output": runtime.OutputRoot} {
		relative, err := filepath.Rel(runtime.ScratchRoot, root)
		if err != nil || relative == "." || relative == ".." ||
			strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("ModelRuntime %s root must be below its scratch root", name)
		}
	}
	if len(runtime.Environment) > maxLaunchEnvironmentEntries {
		return errors.New("ModelRuntime launch manifest environment is too large")
	}
	seenEnvironment := make(map[string]struct{}, len(runtime.Environment))
	for _, entry := range runtime.Environment {
		name, _, found := strings.Cut(entry, "=")
		if !found || name == "" || len(entry) > maxLaunchEnvironmentBytes ||
			strings.ContainsAny(name, "\x00=") || strings.ContainsRune(entry, '\x00') {
			return errors.New("ModelRuntime launch manifest environment is invalid")
		}
		if _, duplicate := seenEnvironment[name]; duplicate || name == "VELA_MODEL_DRIVER_PROTOCOL" {
			return errors.New("ModelRuntime launch manifest environment is duplicated or reserved")
		}
		seenEnvironment[name] = struct{}{}
	}
	initializationTimeout, initializationErr := time.ParseDuration(runtime.InitializationTimeout)
	shutdownTimeout, shutdownErr := time.ParseDuration(runtime.ShutdownTimeout)
	if initializationErr != nil || initializationTimeout <= 0 || initializationTimeout > 24*time.Hour ||
		shutdownErr != nil || shutdownTimeout <= 0 || shutdownTimeout > 10*time.Minute {
		return errors.New("ModelRuntime launch manifest lifecycle timeout is invalid")
	}
	return nil
}

func validLaunchDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func validLaunchRoot(value string) bool {
	return filepath.IsAbs(value) && filepath.Clean(value) == value && !strings.ContainsRune(value, '\x00')
}

func cloneLaunchManifest(manifest LaunchManifest) LaunchManifest {
	cloned := manifest
	cloned.Devices = append([]LaunchDeviceEpoch(nil), manifest.Devices...)
	cloned.Members = append([]LaunchMemberEpoch(nil), manifest.Members...)
	cloned.LocalDevices = append([]DriverDevice(nil), manifest.LocalDevices...)
	cloned.Runtimes = append([]LaunchRuntime(nil), manifest.Runtimes...)
	for index := range cloned.Runtimes {
		cloned.Runtimes[index].Command = append([]string(nil), manifest.Runtimes[index].Command...)
		cloned.Runtimes[index].Environment = append([]string(nil), manifest.Runtimes[index].Environment...)
	}
	return cloned
}

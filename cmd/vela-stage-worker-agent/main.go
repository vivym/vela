package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

const maxStageWorkerConfigJSONBytes = 64 << 10

type config struct {
	workerInstanceID                uuid.UUID
	workerInstanceEpoch             int64
	workerMemberID                  uuid.UUID
	workerMemberEpoch               int64
	devices                         []*velav1.StageAuthorityDeviceEpoch
	capacityVector                  map[string]int64
	controlAddress                  string
	controlServerName               string
	tlsCertificateFile              string
	tlsPrivateKeyFile               string
	controlCAFile                   string
	runtimeSocket                   string
	runtimeExpectedUID              uint32
	scratchRoot                     string
	productionStateRoot             string
	inputRoot                       string
	inputTransferJournalRoot        string
	outputRoot                      string
	materializationJournalRoot      string
	materializationJournalLimit     int
	authorityKeyringFile            string
	authorityActiveKeyID            string
	connectorRevisionID             uuid.UUID
	capacityTTL                     time.Duration
	heartbeatInterval               time.Duration
	retryMinimum                    time.Duration
	retryMaximum                    time.Duration
	artifactS3Endpoint              string
	artifactS3Region                string
	artifactS3Bucket                string
	artifactS3AccessKeyFile         string
	artifactS3SecretKeyFile         string
	artifactS3CAFile                string
	artifactS3PathStyle             bool
	artifactSignedGETTTL            time.Duration
	sourceLossRetry                 time.Duration
	sourceLossConsumedResourceUnits int64
}

type deviceInput struct {
	DeviceID    string `json:"device_id"`
	DeviceEpoch int64  `json:"device_epoch"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "vela-stage-worker-agent stopped: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configuration, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runWithContext(ctx, configuration)
}

func loadConfig() (config, error) {
	workerInstanceID, err := requiredUUID("VELA_WORKER_INSTANCE_ID")
	if err != nil {
		return config{}, err
	}
	workerMemberID, err := requiredUUID("VELA_WORKER_MEMBER_ID")
	if err != nil {
		return config{}, err
	}
	workerInstanceEpoch, err := requiredPositiveInt64("VELA_WORKER_INSTANCE_EPOCH")
	if err != nil {
		return config{}, err
	}
	workerMemberEpoch, err := requiredPositiveInt64("VELA_WORKER_MEMBER_EPOCH")
	if err != nil {
		return config{}, err
	}
	devices, err := parseDevices(os.Getenv("VELA_STAGE_WORKER_DEVICES_JSON"))
	if err != nil {
		return config{}, err
	}
	capacityVector, err := parseCapacityVector(os.Getenv("VELA_STAGE_WORKER_CAPACITY_VECTOR_JSON"))
	if err != nil {
		return config{}, err
	}
	controlAddress := strings.TrimSpace(os.Getenv("VELA_WORKER_CONTROL_ADDRESS"))
	controlHost, controlPort, addressErr := net.SplitHostPort(controlAddress)
	if addressErr != nil || controlHost == "" || controlPort == "" {
		return config{}, errors.New("VELA_WORKER_CONTROL_ADDRESS must contain a host and port")
	}
	controlServerName, err := requiredText("VELA_WORKER_CONTROL_SERVER_NAME", 253)
	if err != nil {
		return config{}, err
	}
	runtimeExpectedUIDValue, err := requiredPositiveInt64("VELA_MODEL_RUNTIME_EXPECTED_UID")
	if err != nil || runtimeExpectedUIDValue > int64(^uint32(0)) {
		return config{}, errors.New("VELA_MODEL_RUNTIME_EXPECTED_UID must be a positive uint32")
	}
	scratchRoot, err := requiredAbsolutePath("VELA_WORKER_SCRATCH_ROOT")
	if err != nil {
		return config{}, err
	}
	productionStateRoot, err := requiredPathUnder("VELA_STAGE_WORKER_PRODUCTION_STATE_ROOT", scratchRoot)
	if err != nil {
		return config{}, err
	}
	inputRoot, err := requiredPathUnder("VELA_STAGE_WORKER_INPUT_ROOT", scratchRoot)
	if err != nil {
		return config{}, err
	}
	inputTransferJournalRoot, err := requiredPathUnder(
		"VELA_STAGE_WORKER_INPUT_TRANSFER_JOURNAL_ROOT", scratchRoot,
	)
	if err != nil {
		return config{}, err
	}
	outputRoot, err := requiredPathUnder("VELA_STAGE_WORKER_OUTPUT_ROOT", scratchRoot)
	if err != nil {
		return config{}, err
	}
	journalRoot, err := requiredPathUnder(
		"VELA_STAGE_WORKER_MATERIALIZATION_JOURNAL_ROOT", scratchRoot,
	)
	if err != nil {
		return config{}, err
	}
	journalLimit64, err := requiredPositiveInt64("VELA_STAGE_WORKER_MATERIALIZATION_JOURNAL_LIMIT")
	if err != nil || journalLimit64 > 100_000 {
		return config{}, errors.New("VELA_STAGE_WORKER_MATERIALIZATION_JOURNAL_LIMIT is invalid")
	}
	authorityActiveKeyID, err := requiredText("VELA_STAGE_WORKER_AUTHORITY_ACTIVE_KEY_ID", 100)
	if err != nil {
		return config{}, err
	}
	connectorRevisionID, err := requiredUUID("VELA_STAGE_WORKER_CONNECTOR_REVISION_ID")
	if err != nil {
		return config{}, err
	}
	capacityTTL, err := requiredDuration("VELA_STAGE_WORKER_CAPACITY_TTL", time.Second, time.Hour)
	if err != nil {
		return config{}, err
	}
	heartbeatInterval, err := requiredDuration(
		"VELA_STAGE_WORKER_HEARTBEAT_INTERVAL", time.Millisecond, time.Minute,
	)
	if err != nil {
		return config{}, err
	}
	retryMinimum, err := requiredDuration("VELA_STAGE_WORKER_RETRY_MINIMUM", time.Millisecond, time.Hour)
	if err != nil {
		return config{}, err
	}
	retryMaximum, err := requiredDuration("VELA_STAGE_WORKER_RETRY_MAXIMUM", retryMinimum, time.Hour)
	if err != nil {
		return config{}, err
	}
	sourceLossRetry, err := requiredDuration(
		"VELA_STAGE_WORKER_SOURCE_LOSS_RETRY", time.Second, time.Hour,
	)
	if err != nil {
		return config{}, err
	}
	artifactSignedGETTTL, err := requiredDuration(
		"VELA_ARTIFACT_S3_SIGNED_GET_TTL", time.Second, 15*time.Minute,
	)
	if err != nil {
		return config{}, err
	}
	sourceLossConsumedResourceUnits, err := requiredPositiveInt64(
		"VELA_STAGE_WORKER_SOURCE_LOSS_CONSUMED_RESOURCE_UNITS",
	)
	if err != nil {
		return config{}, err
	}
	artifactEndpoint, err := requiredHTTPSEndpoint("VELA_ARTIFACT_S3_ENDPOINT")
	if err != nil {
		return config{}, err
	}
	pathStyle, err := strconv.ParseBool(os.Getenv("VELA_ARTIFACT_S3_PATH_STYLE"))
	if err != nil {
		return config{}, errors.New("VELA_ARTIFACT_S3_PATH_STYLE must be true or false")
	}
	configuration := config{
		workerInstanceID: workerInstanceID, workerInstanceEpoch: workerInstanceEpoch,
		workerMemberID: workerMemberID, workerMemberEpoch: workerMemberEpoch,
		devices: devices, capacityVector: capacityVector,
		controlAddress: controlAddress, controlServerName: controlServerName,
		runtimeExpectedUID: uint32(runtimeExpectedUIDValue), scratchRoot: scratchRoot,
		productionStateRoot: productionStateRoot, inputRoot: inputRoot,
		inputTransferJournalRoot: inputTransferJournalRoot,
		outputRoot:               outputRoot, materializationJournalRoot: journalRoot,
		materializationJournalLimit: int(journalLimit64),
		authorityActiveKeyID:        authorityActiveKeyID, connectorRevisionID: connectorRevisionID,
		capacityTTL: capacityTTL, heartbeatInterval: heartbeatInterval,
		retryMinimum: retryMinimum, retryMaximum: retryMaximum,
		artifactS3Endpoint: artifactEndpoint, artifactS3PathStyle: pathStyle,
		artifactSignedGETTTL:            artifactSignedGETTTL,
		sourceLossRetry:                 sourceLossRetry,
		sourceLossConsumedResourceUnits: sourceLossConsumedResourceUnits,
	}
	for name, target := range map[string]*string{
		"VELA_WORKER_TLS_CERT_FILE":                &configuration.tlsCertificateFile,
		"VELA_WORKER_TLS_KEY_FILE":                 &configuration.tlsPrivateKeyFile,
		"VELA_WORKER_CONTROL_CA_FILE":              &configuration.controlCAFile,
		"VELA_MODEL_RUNTIME_SOCKET":                &configuration.runtimeSocket,
		"VELA_STAGE_WORKER_AUTHORITY_KEYRING_FILE": &configuration.authorityKeyringFile,
		"VELA_ARTIFACT_S3_ACCESS_KEY_ID_FILE":      &configuration.artifactS3AccessKeyFile,
		"VELA_ARTIFACT_S3_SECRET_ACCESS_KEY_FILE":  &configuration.artifactS3SecretKeyFile,
		"VELA_ARTIFACT_S3_CA_FILE":                 &configuration.artifactS3CAFile,
	} {
		value, pathErr := requiredAbsolutePath(name)
		if pathErr != nil {
			return config{}, pathErr
		}
		*target = value
	}
	for name, target := range map[string]*string{
		"VELA_ARTIFACT_S3_REGION": &configuration.artifactS3Region,
		"VELA_ARTIFACT_S3_BUCKET": &configuration.artifactS3Bucket,
	} {
		value, textErr := requiredText(name, 253)
		if textErr != nil {
			return config{}, textErr
		}
		*target = value
	}
	if err := requireDistinctStageWorkerRoots(map[string]string{
		"production state":        productionStateRoot,
		"input":                   inputRoot,
		"input transfer journal":  inputTransferJournalRoot,
		"output":                  outputRoot,
		"materialization journal": journalRoot,
	}); err != nil {
		return config{}, err
	}
	return configuration, nil
}

func requireDistinctStageWorkerRoots(roots map[string]string) error {
	seen := make(map[string]string, len(roots))
	for name, path := range roots {
		if existing, duplicate := seen[path]; duplicate {
			return fmt.Errorf("Stage Worker %s root conflicts with %s root", name, existing)
		}
		seen[path] = name
	}
	return nil
}

func requiredUUID(name string) (uuid.UUID, error) {
	value, err := uuid.Parse(os.Getenv(name))
	if err != nil || value == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%s must be a UUID", name)
	}
	return value, nil
}

func requiredPositiveInt64(name string) (int64, error) {
	value, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func requiredDuration(name string, minimum, maximum time.Duration) (time.Duration, error) {
	value, err := time.ParseDuration(os.Getenv(name))
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s is outside the supported duration range", name)
	}
	return value, nil
}

func requiredText(name string, maximum int) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" || value != os.Getenv(name) || len(value) > maximum || strings.ContainsRune(value, '\x00') {
		return "", fmt.Errorf("%s is required and must be canonical text", name)
	}
	return value, nil
}

func requiredAbsolutePath(name string) (string, error) {
	value := filepath.Clean(os.Getenv(name))
	if !filepath.IsAbs(value) || value != os.Getenv(name) || strings.ContainsRune(value, '\x00') {
		return "", fmt.Errorf("%s must be a canonical absolute path", name)
	}
	return value, nil
}

func requiredPathUnder(name, root string) (string, error) {
	value, err := requiredAbsolutePath(name)
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(root, value)
	if err != nil || relative == "." || relative == ".." ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%s must be under VELA_WORKER_SCRATCH_ROOT", name)
	}
	return value, nil
}

func requiredHTTPSEndpoint(name string) (string, error) {
	value, err := requiredText(name, 2048)
	if err != nil {
		return "", err
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("%s must be an HTTPS endpoint", name)
	}
	return value, nil
}

func parseDevices(encoded string) ([]*velav1.StageAuthorityDeviceEpoch, error) {
	var inputs []deviceInput
	if err := decodeBoundedJSON(encoded, &inputs); err != nil || len(inputs) == 0 || len(inputs) > 64 {
		return nil, errors.New("VELA_STAGE_WORKER_DEVICES_JSON is invalid")
	}
	seen := make(map[uuid.UUID]struct{}, len(inputs))
	devices := make([]*velav1.StageAuthorityDeviceEpoch, 0, len(inputs))
	for _, input := range inputs {
		id, err := uuid.Parse(input.DeviceID)
		if err != nil || id == uuid.Nil || input.DeviceEpoch <= 0 {
			return nil, errors.New("VELA_STAGE_WORKER_DEVICES_JSON is invalid")
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, errors.New("VELA_STAGE_WORKER_DEVICES_JSON contains duplicate devices")
		}
		seen[id] = struct{}{}
		devices = append(devices, &velav1.StageAuthorityDeviceEpoch{
			DeviceId: id.String(), DeviceEpoch: input.DeviceEpoch,
		})
	}
	return devices, nil
}

func parseCapacityVector(encoded string) (map[string]int64, error) {
	var vector map[string]int64
	if err := decodeBoundedJSON(encoded, &vector); err != nil || len(vector) == 0 || len(vector) > 100 {
		return nil, errors.New("VELA_STAGE_WORKER_CAPACITY_VECTOR_JSON is invalid")
	}
	for key, value := range vector {
		if key == "" || strings.TrimSpace(key) != key || len(key) > 100 || value < 0 {
			return nil, errors.New("VELA_STAGE_WORKER_CAPACITY_VECTOR_JSON is invalid")
		}
	}
	return vector, nil
}

func decodeBoundedJSON(encoded string, target any) error {
	if len(encoded) == 0 || len(encoded) > maxStageWorkerConfigJSONBytes {
		return errors.New("bounded JSON input is empty or too large")
	}
	decoder := json.NewDecoder(bytes.NewReader([]byte(encoded)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("bounded JSON input contains trailing data")
	}
	return nil
}

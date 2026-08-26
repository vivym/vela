package debugdumpcontract

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"slices"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	ContentType = "application/vnd.vela.debug-dump+json"
	MaxBytes    = 64 * 1024
)

var (
	failureClassPattern       = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,99}$`)
	failureFingerprintPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$`)
	backendStagePattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,99}$`)
	backendRevisionPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9@._:/+-]{0,199}$`)
	gpuUUIDPattern            = regexp.MustCompile(`^GPU-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

type Envelope struct {
	AuthorizationID          string   `json:"authorization_id"`
	AttemptID                string   `json:"attempt_id"`
	BackendStage             string   `json:"backend_stage"`
	FailureClass             string   `json:"failure_class"`
	FailureFingerprint       string   `json:"failure_fingerprint"`
	GPUUUIDs                 []string `json:"gpu_uuids"`
	InferenceBackendRevision string   `json:"inference_backend_revision"`
	JobID                    string   `json:"job_id"`
	LeaseFence               int64    `json:"lease_fence"`
	RetryRecommended         bool     `json:"retry_recommended"`
	SchemaVersion            int      `json:"schema_version"`
	WorkerEpoch              int64    `json:"worker_epoch"`
	WorkerID                 string   `json:"worker_id"`
	WorkerReusable           bool     `json:"worker_reusable"`
}

func Parse(raw []byte) (Envelope, error) {
	if len(raw) == 0 || len(raw) > MaxBytes {
		return Envelope{}, errors.New("debug dump envelope size is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var envelope Envelope
	if err := decoder.Decode(&envelope); err != nil {
		return Envelope{}, errors.New("debug dump envelope schema is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Envelope{}, errors.New("debug dump envelope has trailing data")
	}
	canonical, err := json.Marshal(envelope)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Envelope{}, errors.New("debug dump envelope is not canonical")
	}
	if envelope.SchemaVersion != 1 || envelope.LeaseFence <= 0 || envelope.WorkerEpoch <= 0 ||
		!canonicalUUID(envelope.AuthorizationID) || !canonicalUUID(envelope.AttemptID) ||
		!canonicalUUID(envelope.JobID) || !canonicalUUID(envelope.WorkerID) ||
		!failureClassPattern.MatchString(envelope.FailureClass) ||
		!failureFingerprintPattern.MatchString(envelope.FailureFingerprint) ||
		!backendStagePattern.MatchString(envelope.BackendStage) ||
		!backendRevisionPattern.MatchString(envelope.InferenceBackendRevision) ||
		len(envelope.GPUUUIDs) > 8 || !slices.IsSorted(envelope.GPUUUIDs) {
		return Envelope{}, errors.New("debug dump envelope fields are invalid")
	}
	for index, gpuID := range envelope.GPUUUIDs {
		if !gpuUUIDPattern.MatchString(gpuID) ||
			(index > 0 && gpuID == envelope.GPUUUIDs[index-1]) {
			return Envelope{}, errors.New("debug dump envelope GPU identity is invalid")
		}
	}
	return envelope, nil
}

func SafeBackendStage(value string) bool {
	return backendStagePattern.MatchString(value)
}

func canonicalUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}

func SafePrintable(value string, maxRunes int) bool {
	if !utf8.ValidString(value) || value == "" {
		return false
	}
	count := 0
	for _, character := range value {
		if !unicode.IsPrint(character) || unicode.IsSpace(character) {
			return false
		}
		count++
		if count > maxRunes {
			return false
		}
	}
	return true
}

package nodeagent

import (
	"errors"
	"regexp"
	"time"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/util/validation"
)

var runtimeContainerIDPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

type RuntimeContainerObserverConfig struct {
	SocketPath   string
	NodeIdentity string
}

// RuntimeContainerTarget must come from trusted Node/Fleet inventory. Matching
// caller-supplied IDs only correlates an observation; it grants no authority.
type RuntimeContainerTarget struct {
	ContainerID      string    `json:"container_id"`
	SandboxID        string    `json:"sandbox_id"`
	PodUID           uuid.UUID `json:"pod_uid"`
	PodNamespace     string    `json:"pod_namespace"`
	PodName          string    `json:"pod_name"`
	ContainerName    string    `json:"container_name"`
	ContainerAttempt uint32    `json:"container_attempt"`
}

// RuntimeContainerObservation is a bounded CRI read, not a containment, drain
// or retirement receipt. In particular, SandboxPIDNamespace describes the
// sandbox configuration, not the actual namespace or PID 1 of its container.
type RuntimeContainerObservation struct {
	SchemaVersion       int                    `json:"schema_version"`
	Target              RuntimeContainerTarget `json:"target"`
	NodeIdentity        string                 `json:"node_identity"`
	BootID              uuid.UUID              `json:"boot_id"`
	RuntimeName         string                 `json:"runtime_name"`
	RuntimeVersion      string                 `json:"runtime_version"`
	RuntimeAPIVersion   string                 `json:"runtime_api_version"`
	ContainerState      string                 `json:"container_state"`
	CreatedAt           time.Time              `json:"created_at"`
	StartedAt           time.Time              `json:"started_at"`
	FinishedAt          time.Time              `json:"finished_at"`
	ExitCode            int32                  `json:"exit_code"`
	ImageRef            string                 `json:"image_ref"`
	ImageConfigDigest   string                 `json:"image_config_digest,omitempty"`
	SandboxCreatedAt    time.Time              `json:"sandbox_created_at"`
	SandboxAttempt      uint32                 `json:"sandbox_attempt"`
	SandboxState        string                 `json:"sandbox_state"`
	SandboxPIDNamespace string                 `json:"sandbox_pid_namespace"`
	SandboxPIDTargetID  string                 `json:"sandbox_pid_target_id,omitempty"`
	ObservedFrom        time.Time              `json:"observed_from"`
	ObservedThrough     time.Time              `json:"observed_through"`
}

func (target RuntimeContainerTarget) Validate() error {
	if !runtimeContainerIDPattern.MatchString(target.ContainerID) || !runtimeContainerIDPattern.MatchString(target.SandboxID) ||
		target.ContainerID == target.SandboxID || target.PodUID == uuid.Nil ||
		len(validation.IsDNS1123Label(target.PodNamespace)) != 0 || len(validation.IsDNS1123Subdomain(target.PodName)) != 0 ||
		len(validation.IsDNS1123Label(target.ContainerName)) != 0 {
		return errors.New("runtime container target requires exact container, sandbox and Pod identities")
	}
	return nil
}

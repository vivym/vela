package workerbootstrap

import (
	"context"
	"crypto/sha256"

	"github.com/google/uuid"
)

// ProvisionedJournals reports a completed ownership transfer, not permission to
// mount storage, activate a Worker, start a backend, or retire an earlier owner.
type ProvisionedJournals struct {
	SchemaVersion int               `json:"schema_version"`
	ProvisionID   uuid.UUID         `json:"provision_id"`
	RequestID     uuid.UUID         `json:"request_id"`
	OriginDigest  [sha256.Size]byte `json:"origin_digest"`
}

// Provision creates first-use storage below a pre-existing empty root-only Node
// directory. Its scratch child is prepared as root, then transferred to the Fleet
// journal UID/GID 10001. The parent and independent evidence remain root-only.
// Callers must keep this new path out of workload mounts until qualified Fleet
// activation. Trusted root administrators remain outside this boundary.
//
// This operation never adopts existing files or retries initialization, including
// after an interrupted transfer. Preserve incomplete state for reconciliation.
func Provision(ctx context.Context, config Config, directory string, authority Authority) (ProvisionedJournals, error) {
	return provision(ctx, config, directory, authority, nil)
}

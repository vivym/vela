package fleetcontract

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/google/uuid"
)

// WorkerMemberIdentityDigest binds Registry membership to the canonical mTLS
// identity. Mutable topology metadata has its own device-subset digest.
func WorkerMemberIdentityDigest(id uuid.UUID) string {
	digest := sha256.Sum256([]byte("spiffe://vela.internal/stage-worker/" + id.String()))
	return hex.EncodeToString(digest[:])
}

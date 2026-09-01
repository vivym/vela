package remediation

import (
	"crypto/sha256"
	"testing"

	"github.com/google/uuid"
)

func TestValidateRequestAllowsEmptyCertificationOnlyForQuarantine(t *testing.T) {
	evidence := sha256.Sum256([]byte("validation"))
	request := Request{
		OperationID: uuid.New(), WorkerInstanceID: uuid.New(), WorkerInstanceEpoch: 1,
		NodeIdentity: "node", DeviceIdentity: "device", FailureClass: "fault",
		EvidenceDigest: evidence[:], ActionLevel: ActionL7Quarantine,
		IdempotencyKey: "idempotency", RequestedBy: "actor",
	}
	if err := validateRequest(request); err != nil {
		t.Fatalf("L7 request with empty certification revision: %v", err)
	}
	request.ActionLevel = ActionL0ProcessRestart
	if err := validateRequest(request); err == nil {
		t.Fatal("automatic remediation with empty certification revision was accepted")
	}
}

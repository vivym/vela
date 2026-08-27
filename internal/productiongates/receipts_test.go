package productiongates

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"
)

func TestEvaluationRequiresAllNineVersionedReceipts(t *testing.T) {
	evaluation := Evaluate(nil)
	if evaluation.Pass != 0 || evaluation.Fail != 0 || evaluation.Missing != len(AllGates()) || evaluation.Invalid != 0 {
		t.Fatalf("empty evaluation = %#v", evaluation)
	}
	if err := evaluation.AllPass(); !errors.Is(err, ErrMissingGates) {
		t.Fatalf("empty AllPass error = %v, want missing-gates error", err)
	}

	receipts := make([]Receipt, 0, len(AllGates()))
	for index, gate := range AllGates() {
		receipts = append(receipts, validReceipt(gate, time.Unix(int64(index+1), 0).UTC()))
	}
	evaluation = Evaluate(receipts)
	if evaluation.Pass != len(AllGates()) || evaluation.Fail != 0 || evaluation.Missing != 0 || evaluation.Invalid != 0 {
		t.Fatalf("all-pass evaluation = %#v", evaluation)
	}
	if err := evaluation.AllPass(); err != nil {
		t.Fatalf("all-pass AllPass error = %v", err)
	}
}

func TestReceiptValidationRejectsMissingEvidenceAndBadDigest(t *testing.T) {
	receipt := validReceipt(GateDataDisasterRecovery, time.Unix(10, 0).UTC())
	receipt.EvidenceDigest = "sha256:bad"
	if err := receipt.Validate(); !errors.Is(err, ErrInvalidReceipt) {
		t.Fatalf("bad evidence digest error = %v, want invalid receipt", err)
	}
	receipt = validReceipt(GateDataDisasterRecovery, time.Unix(10, 0).UTC())
	receipt.EvidenceRef = ""
	if err := receipt.Validate(); !errors.Is(err, ErrInvalidReceipt) {
		t.Fatalf("missing evidence reference error = %v, want invalid receipt", err)
	}
	receipt = validReceipt(GateDataDisasterRecovery, time.Unix(10, 0).UTC())
	receipt.CompletedAt = receipt.StartedAt.Add(-time.Second)
	if err := receipt.Validate(); !errors.Is(err, ErrInvalidReceipt) {
		t.Fatalf("reversed receipt timestamps error = %v, want invalid receipt", err)
	}
	receipt = validReceipt(GateDataDisasterRecovery, time.Unix(10, 0).UTC())
	receipt.Owner = "platform\toncall"
	if err := receipt.Validate(); !errors.Is(err, ErrInvalidReceipt) {
		t.Fatalf("control-character owner error = %v, want invalid receipt", err)
	}
}

func TestEvaluationFailsClosedOnDuplicateOrFailedReceipt(t *testing.T) {
	base := time.Unix(100, 0).UTC()
	first := validReceipt(GatePresetCertification, base)
	second := validReceipt(GatePresetCertification, base.Add(time.Second))
	second.Result = ResultFail
	evaluation := Evaluate([]Receipt{first, second})
	gate := evaluation.Gates[GatePresetCertification]
	if gate.Status != GateStatusInvalid || evaluation.Invalid != 1 {
		t.Fatalf("duplicate evaluation = %#v, gate=%#v", evaluation, gate)
	}
	failed := validReceipt(GateRealH3Soak, base)
	failed.Result = ResultFail
	evaluation = Evaluate([]Receipt{failed})
	if evaluation.Gates[GateRealH3Soak].Status != GateStatusFail || evaluation.Fail != 1 {
		t.Fatalf("failed evaluation = %#v", evaluation)
	}
	if err := evaluation.AllPass(); !errors.Is(err, ErrMissingGates) {
		t.Fatalf("failed AllPass error = %v, want missing-gates error", err)
	}
}

func validReceipt(gate Gate, startedAt time.Time) Receipt {
	return Receipt{
		SchemaVersion:         1,
		Gate:                  gate,
		ReleaseDigest:         digest("release"),
		ConfigurationRevision: "config-rev-1",
		ValidationEnvironment: "test-environment",
		Result:                ResultPass,
		Owner:                 "platform-oncall@example.invalid",
		AcceptanceThreshold:   "all required assertions pass",
		ObservedResult:        "all required assertions pass",
		EvidenceRef:           "artifacts/receipts/evidence.json",
		EvidenceDigest:        digest("evidence"),
		StartedAt:             startedAt,
		CompletedAt:           startedAt.Add(time.Minute),
		RecordedAt:            startedAt.Add(2 * time.Minute),
	}
}

func digest(value string) string {
	hash := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(hash[:])
}

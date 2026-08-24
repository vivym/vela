package productiongates

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Gate string

const (
	GatePresetCertification      Gate = "preset-certification"
	GateRealH3Soak               Gate = "real-h3-soak"
	GateStateEventFaultInjection Gate = "state-event-fault-injection"
	GateGPURemediation           Gate = "gpu-remediation"
	GateOrganizationIsolation    Gate = "organization-isolation-content-safety"
	GateDataDisasterRecovery     Gate = "data-disaster-recovery"
	GateReleaseRollback          Gate = "release-rollback"
	GateCommercialLifecycle      Gate = "commercial-data-lifecycle"
	GateObservabilityOnCall      Gate = "observability-on-call"
)

type Result string

const (
	ResultPass Result = "PASS"
	ResultFail Result = "FAIL"
)

type GateStatus string

const (
	GateStatusPass    GateStatus = "PASS"
	GateStatusFail    GateStatus = "FAIL"
	GateStatusMissing GateStatus = "MISSING"
	GateStatusInvalid GateStatus = "INVALID"
)

var (
	ErrInvalidReceipt = errors.New("invalid Production Gate receipt")
	ErrMissingGates   = errors.New("Production Gate receipts are incomplete")
)

// Receipt is the immutable evidence index for one gate and one release.
// EvidenceRef points at an externally retained artifact; EvidenceDigest binds
// the bytes that were reviewed instead of trusting a mutable URL or label.
type Receipt struct {
	SchemaVersion         int       `json:"schema_version"`
	Gate                  Gate      `json:"gate"`
	ReleaseDigest         string    `json:"release_digest"`
	ConfigurationRevision string    `json:"configuration_revision"`
	ValidationEnvironment string    `json:"validation_environment"`
	Result                Result    `json:"result"`
	Owner                 string    `json:"owner"`
	AcceptanceThreshold   string    `json:"acceptance_threshold"`
	ObservedResult        string    `json:"observed_result"`
	EvidenceRef           string    `json:"evidence_ref"`
	EvidenceDigest        string    `json:"evidence_digest"`
	StartedAt             time.Time `json:"started_at"`
	CompletedAt           time.Time `json:"completed_at"`
	RecordedAt            time.Time `json:"recorded_at"`
}

type GateEvaluation struct {
	Status       GateStatus
	Receipt      *Receipt
	InvalidError error
}

type Evaluation struct {
	Gates   map[Gate]GateEvaluation
	Pass    int
	Fail    int
	Missing int
	Invalid int
}

func AllGates() []Gate {
	return []Gate{
		GatePresetCertification,
		GateRealH3Soak,
		GateStateEventFaultInjection,
		GateGPURemediation,
		GateOrganizationIsolation,
		GateDataDisasterRecovery,
		GateReleaseRollback,
		GateCommercialLifecycle,
		GateObservabilityOnCall,
	}
}

func IsKnownGate(gate Gate) bool {
	for _, known := range AllGates() {
		if gate == known {
			return true
		}
	}
	return false
}

func (receipt Receipt) Validate() error {
	if receipt.SchemaVersion != 1 || !IsKnownGate(receipt.Gate) ||
		!validOCIDigest(receipt.ReleaseDigest) ||
		!validBoundedText(receipt.ConfigurationRevision, 300) ||
		!validBoundedText(receipt.ValidationEnvironment, 500) ||
		!validBoundedText(receipt.Owner, 300) ||
		!validBoundedText(receipt.AcceptanceThreshold, 4000) ||
		!validBoundedText(receipt.ObservedResult, 4000) ||
		!validBoundedText(receipt.EvidenceRef, 2000) ||
		!validSHA256Digest(receipt.EvidenceDigest) ||
		receipt.StartedAt.IsZero() || receipt.CompletedAt.IsZero() || receipt.RecordedAt.IsZero() ||
		receipt.CompletedAt.Before(receipt.StartedAt) || receipt.RecordedAt.Before(receipt.CompletedAt) {
		return fmt.Errorf("%w: required fields or timestamp ordering are invalid", ErrInvalidReceipt)
	}
	if receipt.Result != ResultPass && receipt.Result != ResultFail {
		return fmt.Errorf("%w: result must be PASS or FAIL", ErrInvalidReceipt)
	}
	return nil
}

func Evaluate(receipts []Receipt) Evaluation {
	evaluation := Evaluation{Gates: make(map[Gate]GateEvaluation, len(AllGates()))}
	for _, gate := range AllGates() {
		evaluation.Gates[gate] = GateEvaluation{Status: GateStatusMissing}
	}
	sorted := append([]Receipt(nil), receipts...)
	sort.SliceStable(sorted, func(left, right int) bool {
		return sorted[left].Gate < sorted[right].Gate
	})
	for index := range sorted {
		receipt := sorted[index]
		if !IsKnownGate(receipt.Gate) {
			evaluation.Invalid++
			continue
		}
		if prior := evaluation.Gates[receipt.Gate]; prior.Status != GateStatusMissing {
			evaluation.Gates[receipt.Gate] = GateEvaluation{
				Status:       GateStatusInvalid,
				InvalidError: fmt.Errorf("%w: duplicate receipt for %s", ErrInvalidReceipt, receipt.Gate),
			}
			evaluation.Invalid++
			continue
		}
		if err := receipt.Validate(); err != nil {
			evaluation.Gates[receipt.Gate] = GateEvaluation{Status: GateStatusInvalid, InvalidError: err}
			evaluation.Invalid++
			continue
		}
		copy := receipt
		status := GateStatusFail
		if receipt.Result == ResultPass {
			status = GateStatusPass
		}
		evaluation.Gates[receipt.Gate] = GateEvaluation{Status: status, Receipt: &copy}
		if status == GateStatusPass {
			evaluation.Pass++
		} else {
			evaluation.Fail++
		}
	}
	for _, gate := range AllGates() {
		if evaluation.Gates[gate].Status == GateStatusMissing {
			evaluation.Missing++
		}
	}
	return evaluation
}

func (evaluation Evaluation) AllPass() error {
	if evaluation.Pass == len(AllGates()) && evaluation.Fail == 0 && evaluation.Missing == 0 && evaluation.Invalid == 0 {
		return nil
	}
	missing := make([]string, 0)
	for _, gate := range AllGates() {
		status := evaluation.Gates[gate].Status
		if status != GateStatusPass {
			missing = append(missing, string(gate)+"="+string(status))
		}
	}
	return fmt.Errorf("%w: %s", ErrMissingGates, strings.Join(missing, ", "))
}

func validOCIDigest(value string) bool {
	return validSHA256Digest(value)
}

func validSHA256Digest(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && len(decoded) == sha256.Size
}

func validBoundedText(value string, max int) bool {
	return len(value) > 0 && len(value) <= max && strings.TrimSpace(value) == value &&
		!strings.ContainsAny(value, "\r\n\x00")
}

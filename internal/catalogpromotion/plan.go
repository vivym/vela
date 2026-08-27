package catalogpromotion

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/strictjson"
)

const maxPlanBytes = 1024 * 1024

var ErrInvalidPlan = errors.New("invalid Catalog promotion plan")

type Plan struct {
	SchemaVersion   int                      `json:"schema_version"`
	ManifestRef     string                   `json:"manifest_ref"`
	Certifications  []CertificationPromotion `json:"certifications"`
	RateCards       []RateCardPromotion      `json:"rate_cards"`
	EnableEvidenced bool                     `json:"enable_evidenced"`
}

type CertificationPromotion struct {
	EvidenceID                 uuid.UUID `json:"evidence_id"`
	ProfileCertificationID     uuid.UUID `json:"profile_certification_id"`
	InferenceBackendRevisionID uuid.UUID `json:"inference_backend_revision_id"`
	HardwareDriverBaseline     string    `json:"hardware_driver_baseline"`
	BenchmarkCorpusRevision    string    `json:"benchmark_corpus_revision"`
	QualityThresholdPPM        int32     `json:"quality_threshold_ppm"`
	QualityObservedPPM         int32     `json:"quality_observed_ppm"`
	SuccessRateThresholdPPM    int32     `json:"success_rate_threshold_ppm"`
	SuccessRateObservedPPM     int32     `json:"success_rate_observed_ppm"`
	P50Milliseconds            int64     `json:"p50_milliseconds"`
	P95ThresholdMilliseconds   int64     `json:"p95_threshold_milliseconds"`
	P95ObservedMilliseconds    int64     `json:"p95_observed_milliseconds"`
	CostThresholdMinor         int64     `json:"cost_threshold_minor"`
	CostObservedMinor          int64     `json:"cost_observed_minor"`
	CostCurrency               string    `json:"cost_currency"`
	ConfidenceThresholdPPM     int32     `json:"confidence_threshold_ppm"`
	ConfidenceObservedPPM      int32     `json:"confidence_observed_ppm"`
}

type RateCardPromotion struct {
	BindingID          uuid.UUID `json:"binding_id"`
	RateCardRevisionID uuid.UUID `json:"rate_card_revision_id"`
}

func LoadPlan(path string) (Plan, error) {
	file, err := os.Open(path)
	if err != nil {
		return Plan{}, fmt.Errorf("%w: open plan: %v", ErrInvalidPlan, err)
	}
	defer func() { _ = file.Close() }()
	encoded, err := io.ReadAll(io.LimitReader(file, maxPlanBytes+1))
	if err != nil {
		return Plan{}, fmt.Errorf("%w: read plan: %v", ErrInvalidPlan, err)
	}
	if len(encoded) == 0 || len(encoded) > maxPlanBytes {
		return Plan{}, fmt.Errorf("%w: plan size must be in 1..%d bytes", ErrInvalidPlan, maxPlanBytes)
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return Plan{}, fmt.Errorf("%w: decode plan: %v", ErrInvalidPlan, err)
	}
	var plan Plan
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return Plan{}, fmt.Errorf("%w: decode plan: %v", ErrInvalidPlan, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Plan{}, fmt.Errorf("%w: plan must contain exactly one JSON value", ErrInvalidPlan)
	}
	if err := plan.validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func (plan Plan) validate() error {
	if plan.SchemaVersion != 1 ||
		!filepath.IsLocal(filepath.FromSlash(plan.ManifestRef)) ||
		!validText(plan.ManifestRef, 2000) ||
		len(plan.Certifications) == 0 || len(plan.RateCards) == 0 ||
		!plan.EnableEvidenced {
		return fmt.Errorf("%w: required release fields are missing", ErrInvalidPlan)
	}
	evidenceIDs := make(map[uuid.UUID]bool, len(plan.Certifications))
	certificationIDs := make(map[uuid.UUID]bool, len(plan.Certifications))
	for _, promotion := range plan.Certifications {
		if promotion.EvidenceID == uuid.Nil || promotion.ProfileCertificationID == uuid.Nil ||
			promotion.InferenceBackendRevisionID == uuid.Nil ||
			evidenceIDs[promotion.EvidenceID] || certificationIDs[promotion.ProfileCertificationID] ||
			!validText(promotion.HardwareDriverBaseline, 300) ||
			!validText(promotion.BenchmarkCorpusRevision, 300) ||
			!validPPM(promotion.QualityThresholdPPM) ||
			promotion.QualityObservedPPM < promotion.QualityThresholdPPM ||
			!validPPM(promotion.QualityObservedPPM) ||
			!validPPM(promotion.SuccessRateThresholdPPM) ||
			promotion.SuccessRateObservedPPM < promotion.SuccessRateThresholdPPM ||
			!validPPM(promotion.SuccessRateObservedPPM) ||
			promotion.P50Milliseconds <= 0 ||
			promotion.P95ObservedMilliseconds < promotion.P50Milliseconds ||
			promotion.P95ThresholdMilliseconds < promotion.P95ObservedMilliseconds ||
			promotion.CostThresholdMinor < 0 || promotion.CostObservedMinor < 0 ||
			promotion.CostObservedMinor > promotion.CostThresholdMinor ||
			!validCurrency(promotion.CostCurrency) ||
			!validPPM(promotion.ConfidenceThresholdPPM) ||
			promotion.ConfidenceObservedPPM < promotion.ConfidenceThresholdPPM ||
			!validPPM(promotion.ConfidenceObservedPPM) {
			return fmt.Errorf("%w: invalid or duplicate ProfileCertification promotion", ErrInvalidPlan)
		}
		evidenceIDs[promotion.EvidenceID] = true
		certificationIDs[promotion.ProfileCertificationID] = true
	}
	bindingIDs := make(map[uuid.UUID]bool, len(plan.RateCards))
	rateCardIDs := make(map[uuid.UUID]bool, len(plan.RateCards))
	for _, promotion := range plan.RateCards {
		if promotion.BindingID == uuid.Nil || promotion.RateCardRevisionID == uuid.Nil ||
			bindingIDs[promotion.BindingID] || rateCardIDs[promotion.RateCardRevisionID] {
			return fmt.Errorf("%w: invalid or duplicate RateCard promotion", ErrInvalidPlan)
		}
		bindingIDs[promotion.BindingID] = true
		rateCardIDs[promotion.RateCardRevisionID] = true
	}
	return nil
}

func validText(value string, max int) bool {
	return len(value) > 0 && len(value) <= max && strings.TrimSpace(value) == value &&
		strings.IndexFunc(value, unicode.IsControl) == -1
}

func validCurrency(value string) bool {
	if len(value) != 3 {
		return false
	}
	for _, character := range value {
		if character < 'A' || character > 'Z' {
			return false
		}
	}
	return true
}

func validPPM(value int32) bool {
	return value >= 0 && value <= 1_000_000
}

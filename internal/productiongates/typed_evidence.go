package productiongates

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/strictjson"
)

const (
	MaxTypedEvidenceBytes         = 4 * 1024 * 1024
	MaxEvidenceArtifactBytes      = 16 * 1024 * 1024
	MaxEvidenceArtifactTotalBytes = 128 * 1024 * 1024
)

var ErrInvalidTypedEvidence = errors.New("invalid typed Production Gate evidence")

type EvidenceComparator string

const (
	EvidenceEqual          EvidenceComparator = "EQ"
	EvidenceLessOrEqual    EvidenceComparator = "LTE"
	EvidenceGreaterOrEqual EvidenceComparator = "GTE"
)

type EvidenceCheck struct {
	ID     string `json:"id"`
	Passed bool   `json:"passed"`
}

type EvidenceMeasurement struct {
	ID         string             `json:"id"`
	Unit       string             `json:"unit"`
	Comparator EvidenceComparator `json:"comparator"`
	Threshold  int64              `json:"threshold"`
	Observed   int64              `json:"observed"`
}

type EvidenceArtifact struct {
	Kind   string `json:"kind"`
	Ref    string `json:"ref"`
	Digest string `json:"digest"`
}

type PresetCertificationClaims struct {
	SaleableGroupIDs []string                   `json:"saleable_group_ids"`
	Certifications   []PresetCertificationClaim `json:"certifications"`
	RateCards        []RateCardPromotionClaim   `json:"rate_cards"`
}

type RateCardPromotionClaim struct {
	BindingID          string `json:"binding_id"`
	RateCardRevisionID string `json:"rate_card_revision_id"`
}

type PresetCertificationClaim struct {
	EvidenceID                 string `json:"evidence_id"`
	ProfileCertificationID     string `json:"profile_certification_id"`
	InferenceBackendRevisionID string `json:"inference_backend_revision_id"`
	SaleableGroupID            string `json:"saleable_group_id"`
	StablePreset               string `json:"stable_preset"`
	HardwareDriverBaseline     string `json:"hardware_driver_baseline"`
	BenchmarkCorpusRevision    string `json:"benchmark_corpus_revision"`
	SampleCount                int64  `json:"sample_count"`
	QualityThresholdPPM        int32  `json:"quality_threshold_ppm"`
	QualityObservedPPM         int32  `json:"quality_observed_ppm"`
	SuccessRateThresholdPPM    int32  `json:"success_rate_threshold_ppm"`
	SuccessRateObservedPPM     int32  `json:"success_rate_observed_ppm"`
	P50Milliseconds            int64  `json:"p50_milliseconds"`
	P95ThresholdMilliseconds   int64  `json:"p95_threshold_milliseconds"`
	P95ObservedMilliseconds    int64  `json:"p95_observed_milliseconds"`
	CostThresholdMinor         int64  `json:"cost_threshold_minor"`
	CostObservedMinor          int64  `json:"cost_observed_minor"`
	CostCurrency               string `json:"cost_currency"`
	ConfidenceThresholdPPM     int32  `json:"confidence_threshold_ppm"`
	ConfidenceObservedPPM      int32  `json:"confidence_observed_ppm"`
}

// TypedEvidence is the fail-closed semantic envelope for every Production Gate
// except observability-on-call, whose established schema lives in sloevidence.
type TypedEvidence struct {
	SchemaVersion         int                        `json:"schema_version"`
	Gate                  Gate                       `json:"gate"`
	CriteriaRevision      string                     `json:"criteria_revision"`
	ReleaseDigest         string                     `json:"release_digest"`
	ConfigurationRevision string                     `json:"configuration_revision"`
	ValidationEnvironment string                     `json:"validation_environment"`
	Owner                 string                     `json:"owner"`
	StartedAt             time.Time                  `json:"started_at"`
	CompletedAt           time.Time                  `json:"completed_at"`
	Checks                []EvidenceCheck            `json:"checks"`
	Measurements          []EvidenceMeasurement      `json:"measurements"`
	Artifacts             []EvidenceArtifact         `json:"artifacts"`
	PresetCertification   *PresetCertificationClaims `json:"preset_certification,omitempty"`
}

// TypedEvidenceArtifact binds one referenced artifact to its parent evidence
// envelope. Its check and measurement sets are partial; the verified union of
// every required artifact must exactly reproduce the parent envelope.
type TypedEvidenceArtifact struct {
	SchemaVersion         int                        `json:"schema_version"`
	Gate                  Gate                       `json:"gate"`
	Kind                  string                     `json:"kind"`
	ReleaseDigest         string                     `json:"release_digest"`
	ConfigurationRevision string                     `json:"configuration_revision"`
	ValidationEnvironment string                     `json:"validation_environment"`
	Owner                 string                     `json:"owner"`
	StartedAt             time.Time                  `json:"started_at"`
	CompletedAt           time.Time                  `json:"completed_at"`
	Checks                []EvidenceCheck            `json:"checks"`
	Measurements          []EvidenceMeasurement      `json:"measurements"`
	PresetCertification   *PresetCertificationClaims `json:"preset_certification,omitempty"`
}

type EvidenceMeasurementRequirement struct {
	ID         string
	Unit       string
	Comparator EvidenceComparator
	Threshold  int64
}

type TypedEvidenceContract struct {
	CriteriaRevision string
	CheckIDs         []string
	Measurements     []EvidenceMeasurementRequirement
	ArtifactKinds    []string
}

func TypedEvidenceContractForGate(gate Gate) (TypedEvidenceContract, bool) {
	contract, ok := typedEvidenceContract(gate)
	if !ok {
		return TypedEvidenceContract{}, false
	}
	contract.CheckIDs = append([]string(nil), contract.CheckIDs...)
	contract.Measurements = append([]EvidenceMeasurementRequirement(nil), contract.Measurements...)
	contract.ArtifactKinds = append([]string(nil), contract.ArtifactKinds...)
	return contract, true
}

func DecodeTypedEvidence(encoded []byte, receipt Receipt) (TypedEvidence, error) {
	if len(encoded) == 0 || len(encoded) > MaxTypedEvidenceBytes {
		return TypedEvidence{}, fmt.Errorf(
			"%w: evidence size must be in 1..%d bytes",
			ErrInvalidTypedEvidence,
			MaxTypedEvidenceBytes,
		)
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return TypedEvidence{}, fmt.Errorf("%w: %v", ErrInvalidTypedEvidence, err)
	}
	var evidence TypedEvidence
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evidence); err != nil {
		return TypedEvidence{}, fmt.Errorf("%w: decode: %v", ErrInvalidTypedEvidence, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return TypedEvidence{}, fmt.Errorf("%w: trailing JSON data", ErrInvalidTypedEvidence)
	}
	if err := evidence.Validate(receipt); err != nil {
		return TypedEvidence{}, err
	}
	return evidence, nil
}

func DecodeTypedEvidenceArtifact(
	encoded []byte,
	evidence TypedEvidence,
	reference EvidenceArtifact,
) (TypedEvidenceArtifact, error) {
	if len(encoded) == 0 || len(encoded) > MaxEvidenceArtifactBytes {
		return TypedEvidenceArtifact{}, fmt.Errorf(
			"%w: artifact size must be in 1..%d bytes",
			ErrInvalidTypedEvidence,
			MaxEvidenceArtifactBytes,
		)
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return TypedEvidenceArtifact{}, fmt.Errorf("%w: artifact %s: %v", ErrInvalidTypedEvidence, reference.Kind, err)
	}
	var artifact TypedEvidenceArtifact
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&artifact); err != nil {
		return TypedEvidenceArtifact{}, fmt.Errorf("%w: decode artifact %s: %v", ErrInvalidTypedEvidence, reference.Kind, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return TypedEvidenceArtifact{}, fmt.Errorf("%w: trailing JSON data in artifact %s", ErrInvalidTypedEvidence, reference.Kind)
	}
	if err := artifact.validate(evidence, reference); err != nil {
		return TypedEvidenceArtifact{}, err
	}
	return artifact, nil
}

func (evidence TypedEvidence) Validate(receipt Receipt) error {
	contract, ok := typedEvidenceContract(receipt.Gate)
	if !ok || evidence.SchemaVersion != 1 || evidence.Gate != receipt.Gate ||
		evidence.CriteriaRevision != contract.CriteriaRevision ||
		evidence.ReleaseDigest != receipt.ReleaseDigest ||
		evidence.ConfigurationRevision != receipt.ConfigurationRevision ||
		evidence.ValidationEnvironment != receipt.ValidationEnvironment ||
		evidence.Owner != receipt.Owner ||
		!evidence.StartedAt.Equal(receipt.StartedAt) ||
		!evidence.CompletedAt.Equal(receipt.CompletedAt) ||
		evidence.StartedAt.IsZero() || evidence.CompletedAt.Before(evidence.StartedAt) {
		return fmt.Errorf("%w: gate, criteria, receipt binding or time window is invalid", ErrInvalidTypedEvidence)
	}
	if err := validateEvidenceChecks(evidence.Checks, contract.CheckIDs); err != nil {
		return err
	}
	if err := validateEvidenceMeasurements(evidence.Measurements, contract.Measurements); err != nil {
		return err
	}
	if err := validateEvidenceArtifacts(evidence.Artifacts, contract.ArtifactKinds); err != nil {
		return err
	}
	if evidence.Gate == GatePresetCertification {
		if evidence.PresetCertification == nil {
			return fmt.Errorf("%w: preset certification claims are required", ErrInvalidTypedEvidence)
		}
		if err := evidence.PresetCertification.validate(); err != nil {
			return err
		}
		if measurementObserved(evidence.Measurements, "saleable-group-count") !=
			int64(evidence.PresetCertification.saleableGroupCount()) {
			return fmt.Errorf("%w: preset saleable group count does not match claims", ErrInvalidTypedEvidence)
		}
	} else if evidence.PresetCertification != nil {
		return fmt.Errorf("%w: preset certification claims are forbidden for %s", ErrInvalidTypedEvidence, evidence.Gate)
	}
	if evidence.Gate == GateRealH3Soak {
		duration := evidence.CompletedAt.Sub(evidence.StartedAt)
		if duration%time.Second != 0 ||
			measurementObserved(evidence.Measurements, "soak-duration-seconds") != int64(duration/time.Second) {
			return fmt.Errorf("%w: soak duration does not match the receipt window", ErrInvalidTypedEvidence)
		}
	}
	if receipt.AcceptanceThreshold != evidence.AcceptanceThreshold() ||
		receipt.ObservedResult != evidence.ObservedResult() {
		return fmt.Errorf("%w: receipt summaries do not match verified evidence", ErrInvalidTypedEvidence)
	}
	return nil
}

func (artifact TypedEvidenceArtifact) validate(
	evidence TypedEvidence,
	reference EvidenceArtifact,
) error {
	contract, ok := typedEvidenceContract(evidence.Gate)
	if !ok || artifact.SchemaVersion != 1 || artifact.Gate != evidence.Gate ||
		artifact.Kind != reference.Kind || artifact.ReleaseDigest != evidence.ReleaseDigest ||
		artifact.ConfigurationRevision != evidence.ConfigurationRevision ||
		artifact.ValidationEnvironment != evidence.ValidationEnvironment ||
		artifact.Owner != evidence.Owner || !artifact.StartedAt.Equal(evidence.StartedAt) ||
		!artifact.CompletedAt.Equal(evidence.CompletedAt) {
		return fmt.Errorf("%w: artifact %s binding is invalid", ErrInvalidTypedEvidence, reference.Kind)
	}
	if len(artifact.Checks) == 0 && len(artifact.Measurements) == 0 && artifact.PresetCertification == nil {
		return fmt.Errorf("%w: artifact %s has no semantic observations", ErrInvalidTypedEvidence, reference.Kind)
	}
	if err := validateEvidenceCheckSubset(artifact.Checks, contract.CheckIDs); err != nil {
		return fmt.Errorf("%w: artifact %s: %v", ErrInvalidTypedEvidence, reference.Kind, err)
	}
	if err := validateEvidenceMeasurementSubset(artifact.Measurements, contract.Measurements); err != nil {
		return fmt.Errorf("%w: artifact %s: %v", ErrInvalidTypedEvidence, reference.Kind, err)
	}
	claimsRequired := evidence.Gate == GatePresetCertification
	if claimsRequired {
		if artifact.PresetCertification == nil || evidence.PresetCertification == nil ||
			!presetCertificationClaimsEqual(*artifact.PresetCertification, *evidence.PresetCertification) {
			return fmt.Errorf("%w: artifact %s does not reproduce preset claims", ErrInvalidTypedEvidence, reference.Kind)
		}
	} else if artifact.PresetCertification != nil {
		return fmt.Errorf("%w: artifact %s contains forbidden preset claims", ErrInvalidTypedEvidence, reference.Kind)
	}
	return nil
}

func ValidateTypedEvidenceArtifacts(
	evidence TypedEvidence,
	artifacts map[string]TypedEvidenceArtifact,
) error {
	if len(artifacts) != len(evidence.Artifacts) {
		return fmt.Errorf("%w: verified artifact set is incomplete", ErrInvalidTypedEvidence)
	}
	checks := make(map[string]EvidenceCheck, len(evidence.Checks))
	measurements := make(map[string]EvidenceMeasurement, len(evidence.Measurements))
	for _, reference := range evidence.Artifacts {
		artifact, present := artifacts[reference.Kind]
		if !present {
			return fmt.Errorf("%w: verified artifact %s is missing", ErrInvalidTypedEvidence, reference.Kind)
		}
		for _, check := range artifact.Checks {
			if _, duplicate := checks[check.ID]; duplicate {
				return fmt.Errorf("%w: check %s occurs in multiple artifacts", ErrInvalidTypedEvidence, check.ID)
			}
			checks[check.ID] = check
		}
		for _, measurement := range artifact.Measurements {
			if _, duplicate := measurements[measurement.ID]; duplicate {
				return fmt.Errorf("%w: measurement %s occurs in multiple artifacts", ErrInvalidTypedEvidence, measurement.ID)
			}
			measurements[measurement.ID] = measurement
		}
	}
	if len(checks) != len(evidence.Checks) || len(measurements) != len(evidence.Measurements) {
		return fmt.Errorf("%w: artifact observations do not cover the evidence envelope", ErrInvalidTypedEvidence)
	}
	for _, expected := range evidence.Checks {
		if actual, present := checks[expected.ID]; !present || actual != expected {
			return fmt.Errorf("%w: artifact check %s does not match the evidence envelope", ErrInvalidTypedEvidence, expected.ID)
		}
	}
	for _, expected := range evidence.Measurements {
		if actual, present := measurements[expected.ID]; !present || actual != expected {
			return fmt.Errorf("%w: artifact measurement %s does not match the evidence envelope", ErrInvalidTypedEvidence, expected.ID)
		}
	}
	return nil
}

func (evidence TypedEvidence) AcceptanceThreshold() string {
	return evidence.CriteriaRevision
}

func (evidence TypedEvidence) ObservedResult() string {
	return fmt.Sprintf(
		"PASS checks=%d measurements=%d artifacts=%d",
		len(evidence.Checks),
		len(evidence.Measurements),
		len(evidence.Artifacts),
	)
}

func validateEvidenceChecks(checks []EvidenceCheck, required []string) error {
	if len(checks) != len(required) {
		return fmt.Errorf("%w: required check set is incomplete", ErrInvalidTypedEvidence)
	}
	want := make(map[string]struct{}, len(required))
	for _, id := range required {
		want[id] = struct{}{}
	}
	seen := make(map[string]struct{}, len(checks))
	for _, check := range checks {
		if _, known := want[check.ID]; !known {
			return fmt.Errorf("%w: unknown check %q", ErrInvalidTypedEvidence, check.ID)
		}
		if _, duplicate := seen[check.ID]; duplicate {
			return fmt.Errorf("%w: duplicate check %q", ErrInvalidTypedEvidence, check.ID)
		}
		if !check.Passed {
			return fmt.Errorf("%w: required check %q did not pass", ErrInvalidTypedEvidence, check.ID)
		}
		seen[check.ID] = struct{}{}
	}
	return nil
}

func validateEvidenceCheckSubset(checks []EvidenceCheck, allowed []string) error {
	want := make(map[string]struct{}, len(allowed))
	for _, id := range allowed {
		want[id] = struct{}{}
	}
	seen := make(map[string]struct{}, len(checks))
	for _, check := range checks {
		if _, known := want[check.ID]; !known || !check.Passed {
			return fmt.Errorf("check %q is unknown or did not pass", check.ID)
		}
		if _, duplicate := seen[check.ID]; duplicate {
			return fmt.Errorf("duplicate check %q", check.ID)
		}
		seen[check.ID] = struct{}{}
	}
	return nil
}

func validateEvidenceMeasurements(
	measurements []EvidenceMeasurement,
	required []EvidenceMeasurementRequirement,
) error {
	if len(measurements) != len(required) {
		return fmt.Errorf("%w: required measurement set is incomplete", ErrInvalidTypedEvidence)
	}
	want := make(map[string]EvidenceMeasurementRequirement, len(required))
	for _, requirement := range required {
		want[requirement.ID] = requirement
	}
	seen := make(map[string]struct{}, len(measurements))
	for _, measurement := range measurements {
		requirement, known := want[measurement.ID]
		if !known || measurement.Unit != requirement.Unit ||
			measurement.Comparator != requirement.Comparator ||
			measurement.Threshold != requirement.Threshold || measurement.Observed < 0 {
			return fmt.Errorf("%w: measurement %q contract is invalid", ErrInvalidTypedEvidence, measurement.ID)
		}
		if _, duplicate := seen[measurement.ID]; duplicate {
			return fmt.Errorf("%w: duplicate measurement %q", ErrInvalidTypedEvidence, measurement.ID)
		}
		if !measurementPasses(measurement) {
			return fmt.Errorf("%w: measurement %q does not pass", ErrInvalidTypedEvidence, measurement.ID)
		}
		seen[measurement.ID] = struct{}{}
	}
	return nil
}

func validateEvidenceMeasurementSubset(
	measurements []EvidenceMeasurement,
	allowed []EvidenceMeasurementRequirement,
) error {
	want := make(map[string]EvidenceMeasurementRequirement, len(allowed))
	for _, requirement := range allowed {
		want[requirement.ID] = requirement
	}
	seen := make(map[string]struct{}, len(measurements))
	for _, measurement := range measurements {
		requirement, known := want[measurement.ID]
		if !known || measurement.Unit != requirement.Unit ||
			measurement.Comparator != requirement.Comparator ||
			measurement.Threshold != requirement.Threshold || measurement.Observed < 0 ||
			!measurementPasses(measurement) {
			return fmt.Errorf("measurement %q contract is invalid or does not pass", measurement.ID)
		}
		if _, duplicate := seen[measurement.ID]; duplicate {
			return fmt.Errorf("duplicate measurement %q", measurement.ID)
		}
		seen[measurement.ID] = struct{}{}
	}
	return nil
}

func measurementPasses(measurement EvidenceMeasurement) bool {
	switch measurement.Comparator {
	case EvidenceEqual:
		return measurement.Observed == measurement.Threshold
	case EvidenceLessOrEqual:
		return measurement.Observed <= measurement.Threshold
	case EvidenceGreaterOrEqual:
		return measurement.Observed >= measurement.Threshold
	default:
		return false
	}
}

func measurementObserved(measurements []EvidenceMeasurement, id string) int64 {
	for _, measurement := range measurements {
		if measurement.ID == id {
			return measurement.Observed
		}
	}
	return -1
}

func validateEvidenceArtifacts(artifacts []EvidenceArtifact, required []string) error {
	if len(artifacts) != len(required) {
		return fmt.Errorf("%w: required artifact set is incomplete", ErrInvalidTypedEvidence)
	}
	want := make(map[string]struct{}, len(required))
	for _, kind := range required {
		want[kind] = struct{}{}
	}
	seenKinds := make(map[string]struct{}, len(artifacts))
	seenRefs := make(map[string]struct{}, len(artifacts))
	for _, artifact := range artifacts {
		if _, known := want[artifact.Kind]; !known ||
			!filepath.IsLocal(filepath.FromSlash(artifact.Ref)) ||
			!validBoundedText(artifact.Ref, 2000) || !validSHA256Digest(artifact.Digest) {
			return fmt.Errorf("%w: artifact %q is invalid", ErrInvalidTypedEvidence, artifact.Kind)
		}
		if _, duplicate := seenKinds[artifact.Kind]; duplicate {
			return fmt.Errorf("%w: duplicate artifact kind %q", ErrInvalidTypedEvidence, artifact.Kind)
		}
		if _, duplicate := seenRefs[artifact.Ref]; duplicate {
			return fmt.Errorf("%w: duplicate artifact reference %q", ErrInvalidTypedEvidence, artifact.Ref)
		}
		seenKinds[artifact.Kind] = struct{}{}
		seenRefs[artifact.Ref] = struct{}{}
	}
	return nil
}

func (claims PresetCertificationClaims) validate() error {
	if len(claims.SaleableGroupIDs) == 0 || len(claims.SaleableGroupIDs) > 10_000 ||
		len(claims.Certifications) == 0 || len(claims.Certifications) > 30_000 ||
		len(claims.RateCards) == 0 || len(claims.RateCards) > 10_000 {
		return fmt.Errorf("%w: preset certification or RateCard claim count is invalid", ErrInvalidTypedEvidence)
	}
	wantGroups := make(map[string]struct{}, len(claims.SaleableGroupIDs))
	for _, groupID := range claims.SaleableGroupIDs {
		if !validBoundedText(groupID, 500) {
			return fmt.Errorf("%w: saleable group ID is invalid", ErrInvalidTypedEvidence)
		}
		if _, duplicate := wantGroups[groupID]; duplicate {
			return fmt.Errorf("%w: duplicate saleable group ID", ErrInvalidTypedEvidence)
		}
		wantGroups[groupID] = struct{}{}
	}
	seenEvidence := make(map[uuid.UUID]struct{}, len(claims.Certifications))
	seenCertification := make(map[uuid.UUID]struct{}, len(claims.Certifications))
	groups := make(map[string]map[string]struct{})
	for _, claim := range claims.Certifications {
		evidenceID, evidenceErr := uuid.Parse(claim.EvidenceID)
		certificationID, certificationErr := uuid.Parse(claim.ProfileCertificationID)
		backendID, backendErr := uuid.Parse(claim.InferenceBackendRevisionID)
		if evidenceErr != nil || certificationErr != nil || backendErr != nil ||
			evidenceID == uuid.Nil || certificationID == uuid.Nil || backendID == uuid.Nil ||
			!validBoundedText(claim.SaleableGroupID, 500) ||
			(claim.StablePreset != "quality" && claim.StablePreset != "balanced" && claim.StablePreset != "fast") ||
			!validBoundedText(claim.HardwareDriverBaseline, 300) ||
			!validBoundedText(claim.BenchmarkCorpusRevision, 300) || claim.SampleCount <= 0 ||
			!validPPM(claim.QualityThresholdPPM) || !validPPM(claim.QualityObservedPPM) ||
			claim.QualityObservedPPM < claim.QualityThresholdPPM ||
			!validPPM(claim.SuccessRateThresholdPPM) || !validPPM(claim.SuccessRateObservedPPM) ||
			claim.SuccessRateObservedPPM < claim.SuccessRateThresholdPPM ||
			claim.P50Milliseconds <= 0 || claim.P95ObservedMilliseconds < claim.P50Milliseconds ||
			claim.P95ThresholdMilliseconds < claim.P95ObservedMilliseconds ||
			claim.CostThresholdMinor < 0 || claim.CostObservedMinor < 0 ||
			claim.CostObservedMinor > claim.CostThresholdMinor || !validCurrency(claim.CostCurrency) ||
			!validPPM(claim.ConfidenceThresholdPPM) || !validPPM(claim.ConfidenceObservedPPM) ||
			claim.ConfidenceObservedPPM < claim.ConfidenceThresholdPPM {
			return fmt.Errorf("%w: preset certification claim is invalid", ErrInvalidTypedEvidence)
		}
		if _, duplicate := seenEvidence[evidenceID]; duplicate {
			return fmt.Errorf("%w: duplicate certification evidence ID", ErrInvalidTypedEvidence)
		}
		if _, duplicate := seenCertification[certificationID]; duplicate {
			return fmt.Errorf("%w: duplicate ProfileCertification ID", ErrInvalidTypedEvidence)
		}
		seenEvidence[evidenceID] = struct{}{}
		seenCertification[certificationID] = struct{}{}
		presets := groups[claim.SaleableGroupID]
		if presets == nil {
			presets = make(map[string]struct{}, 3)
			groups[claim.SaleableGroupID] = presets
		}
		if _, duplicate := presets[claim.StablePreset]; duplicate {
			return fmt.Errorf("%w: duplicate stable Preset in saleable group", ErrInvalidTypedEvidence)
		}
		presets[claim.StablePreset] = struct{}{}
	}
	for group, presets := range groups {
		if _, saleable := wantGroups[group]; !saleable || len(presets) != 3 {
			return fmt.Errorf("%w: saleable group %q lacks exactly three Presets", ErrInvalidTypedEvidence, group)
		}
	}
	if len(groups) != len(wantGroups) {
		return fmt.Errorf("%w: certification claims do not cover the saleable group snapshot", ErrInvalidTypedEvidence)
	}
	seenBindings := make(map[uuid.UUID]struct{}, len(claims.RateCards))
	seenRateCards := make(map[uuid.UUID]struct{}, len(claims.RateCards))
	for _, rateCard := range claims.RateCards {
		bindingID, bindingErr := uuid.Parse(rateCard.BindingID)
		revisionID, revisionErr := uuid.Parse(rateCard.RateCardRevisionID)
		if bindingErr != nil || revisionErr != nil || bindingID == uuid.Nil || revisionID == uuid.Nil {
			return fmt.Errorf("%w: RateCard promotion claim is invalid", ErrInvalidTypedEvidence)
		}
		if _, duplicate := seenBindings[bindingID]; duplicate {
			return fmt.Errorf("%w: duplicate RateCard binding ID", ErrInvalidTypedEvidence)
		}
		if _, duplicate := seenRateCards[revisionID]; duplicate {
			return fmt.Errorf("%w: duplicate RateCard revision ID", ErrInvalidTypedEvidence)
		}
		seenBindings[bindingID] = struct{}{}
		seenRateCards[revisionID] = struct{}{}
	}
	return nil
}

func (claims PresetCertificationClaims) saleableGroupCount() int {
	return len(claims.SaleableGroupIDs)
}

func presetCertificationClaimsEqual(left, right PresetCertificationClaims) bool {
	leftGroups := append([]string(nil), left.SaleableGroupIDs...)
	rightGroups := append([]string(nil), right.SaleableGroupIDs...)
	sort.Strings(leftGroups)
	sort.Strings(rightGroups)
	leftCertifications := sortedPresetCertificationClaims(left.Certifications)
	rightCertifications := sortedPresetCertificationClaims(right.Certifications)
	leftRateCards := sortedRateCardPromotionClaims(left.RateCards)
	rightRateCards := sortedRateCardPromotionClaims(right.RateCards)
	return reflect.DeepEqual(leftGroups, rightGroups) &&
		reflect.DeepEqual(leftCertifications, rightCertifications) &&
		reflect.DeepEqual(leftRateCards, rightRateCards)
}

func validPPM(value int32) bool {
	return value >= 0 && value <= 1_000_000
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

func typedEvidenceContract(gate Gate) (TypedEvidenceContract, bool) {
	contracts := typedEvidenceContracts()
	contract, ok := contracts[gate]
	return contract, ok
}

func typedEvidenceContracts() map[Gate]TypedEvidenceContract {
	return map[Gate]TypedEvidenceContract{
		GatePresetCertification: evidenceContract(
			GatePresetCertification,
			[]string{"catalog-snapshot-covered", "versioned-corpus", "quality-thresholds", "success-rate-thresholds", "latency-thresholds", "cost-thresholds", "confidence-thresholds"},
			[]EvidenceMeasurementRequirement{
				measurementRequirement("saleable-group-count", "count", EvidenceGreaterOrEqual, 1),
				measurementRequirement("distinct-preset-count", "count", EvidenceEqual, 3),
				measurementRequirement("uncertified-saleable-combination-count", "count", EvidenceEqual, 0),
				measurementRequirement("failed-threshold-count", "count", EvidenceEqual, 0),
			},
			[]string{"saleable-catalog-snapshot", "benchmark-observations", "certification-results"},
		),
		GateRealH3Soak: evidenceContract(
			GateRealH3Soak,
			[]string{"real-h3-hardware", "mixed-output-spec-load", "fault-induced-queue-delay", "scratch-pressure-recovery", "storage-pressure-recovery", "nminusone-coexistence", "preset-slo"},
			[]EvidenceMeasurementRequirement{
				measurementRequirement("soak-duration-seconds", "seconds", EvidenceGreaterOrEqual, 72*60*60),
				measurementRequirement("accepted-job-count", "count", EvidenceGreaterOrEqual, 1),
				measurementRequirement("lost-accepted-job-count", "count", EvidenceEqual, 0),
				measurementRequirement("duplicate-visible-completion-count", "count", EvidenceEqual, 0),
				measurementRequirement("duplicate-charge-count", "count", EvidenceEqual, 0),
				measurementRequirement("unreconciled-job-count", "count", EvidenceEqual, 0),
			},
			[]string{"hardware-inventory", "saleable-sku-snapshot", "soak-observations", "job-ledger-reconciliation", "mixed-version-inventory"},
		),
		GateStateEventFaultInjection: evidenceContract(
			GateStateEventFaultInjection,
			[]string{"process-kill", "worker-control-network-partition", "node-reboot", "outbox-post-commit-crash", "publisher-pre-puback-crash", "publisher-post-puback-pre-mark-crash", "consumer-post-db-pre-ack-crash", "assignment-post-commit-pre-response-crash", "retry-budget-exhaustion", "stale-fence-late-completion"},
			[]EvidenceMeasurementRequirement{
				measurementRequirement("lost-accepted-job-count", "count", EvidenceEqual, 0),
				measurementRequirement("duplicate-visible-completion-count", "count", EvidenceEqual, 0),
				measurementRequirement("duplicate-charge-count", "count", EvidenceEqual, 0),
				measurementRequirement("stale-authority-acceptance-count", "count", EvidenceEqual, 0),
			},
			[]string{"scenario-matrix", "authority-before-after", "raw-event-payloads"},
		),
		GateGPURemediation: evidenceContract(
			GateGPURemediation,
			[]string{"l0-l5-capability-matrix", "identity-validation", "rate-limit", "post-check", "warmup-canary", "quarantine", "l6-two-person-approval", "l7-fail-closed"},
			[]EvidenceMeasurementRequirement{
				measurementRequirement("hardware-target-count", "count", EvidenceGreaterOrEqual, 1),
				measurementRequirement("uncertified-required-action-count", "count", EvidenceEqual, 0),
				measurementRequirement("identity-mismatch-accepted-count", "count", EvidenceEqual, 0),
				measurementRequirement("unapproved-l6-execution-count", "count", EvidenceEqual, 0),
				measurementRequirement("failed-postcheck-without-quarantine-count", "count", EvidenceEqual, 0),
			},
			[]string{"hardware-capability-matrix", "remediation-operations", "negative-approval-exercises"},
		),
		GateOrganizationIsolation: evidenceContract(
			GateOrganizationIsolation,
			[]string{"cross-organization", "cross-project", "fixed-role-matrix", "credential-revocation", "signed-url-scope", "rls", "composite-foreign-key", "break-glass-audit", "customer-content-no-reuse"},
			[]EvidenceMeasurementRequirement{
				measurementRequirement("negative-probe-count", "count", EvidenceGreaterOrEqual, 1),
				measurementRequirement("unexpected-allow-count", "count", EvidenceEqual, 0),
				measurementRequirement("credential-revocation-bypass-count", "count", EvidenceEqual, 0),
				measurementRequirement("customer-content-reuse-count", "count", EvidenceEqual, 0),
				measurementRequirement("unaudited-break-glass-count", "count", EvidenceEqual, 0),
			},
			[]string{"surface-role-snapshot", "negative-probe-results", "credential-signed-url-results", "break-glass-audit", "content-reuse-audit"},
		),
		GateDataDisasterRecovery: evidenceContract(
			GateDataDisasterRecovery,
			[]string{"single-node-failover", "postgresql-pitr", "protected-deletion-marker", "artifact-sample-restore", "retention-replay", "jetstream-rebuild", "outbox-replay", "secret-rotation", "quarterly-exercise"},
			[]EvidenceMeasurementRequirement{
				measurementRequirement("single-node-metadata-rpo-seconds", "seconds", EvidenceEqual, 0),
				measurementRequirement("single-node-rto-seconds", "seconds", EvidenceLessOrEqual, 5*60),
				measurementRequirement("site-metadata-rpo-seconds", "seconds", EvidenceLessOrEqual, 15*60),
				measurementRequirement("site-metadata-rto-seconds", "seconds", EvidenceLessOrEqual, 4*60*60),
				measurementRequirement("restored-artifact-sample-count", "count", EvidenceGreaterOrEqual, 1),
				measurementRequirement("duplicate-authority-count", "count", EvidenceEqual, 0),
			},
			[]string{"deployment-backup-inventory", "single-node-failover", "site-pitr", "artifact-sampled-restore", "rebuild-replay-rotation"},
		),
		GateReleaseRollback: evidenceContract(
			GateReleaseRollback,
			[]string{"expand", "backfill", "switch", "contract", "nminusone-rest", "nminusone-protobuf", "nminusone-event", "nminusone-worker", "long-job-drain", "old-backlog-consumption", "rollback"},
			[]EvidenceMeasurementRequirement{
				measurementRequirement("nminusone-coexistence-seconds", "seconds", EvidenceGreaterOrEqual, 1),
				measurementRequirement("retained-backlog-event-count", "count", EvidenceGreaterOrEqual, 1),
				measurementRequirement("accepted-job-loss-count", "count", EvidenceEqual, 0),
				measurementRequirement("duplicate-visible-completion-count", "count", EvidenceEqual, 0),
				measurementRequirement("duplicate-charge-count", "count", EvidenceEqual, 0),
				measurementRequirement("unconsumed-backlog-count", "count", EvidenceEqual, 0),
			},
			[]string{"release-inventory", "compatibility-checks", "rollout-timeline", "long-job-ledger", "retained-backlog", "rollback-results"},
		),
		GateCommercialLifecycle: evidenceContract(
			GateCommercialLifecycle,
			[]string{"admission-error-contract", "concurrent-credit", "cancellation-race", "failed-job-no-charge", "invoice-export", "webhook-72h-retry", "retention-policy", "content-deletion"},
			[]EvidenceMeasurementRequirement{
				measurementRequirement("credit-invariant-violation-count", "count", EvidenceEqual, 0),
				measurementRequirement("incorrect-charge-count", "count", EvidenceEqual, 0),
				measurementRequirement("duplicate-invoice-line-count", "count", EvidenceEqual, 0),
				measurementRequirement("webhook-undelivered-after-window-count", "count", EvidenceEqual, 0),
				measurementRequirement("retention-violation-count", "count", EvidenceEqual, 0),
				measurementRequirement("deletion-resurrection-count", "count", EvidenceEqual, 0),
			},
			[]string{"admission-credit-scenarios", "completion-cancel-failure-scenarios", "invoice-export-scenarios", "webhook-scenarios", "retention-content-deletion-scenarios"},
		),
	}
}

func evidenceContract(
	gate Gate,
	checks []string,
	measurements []EvidenceMeasurementRequirement,
	artifacts []string,
) TypedEvidenceContract {
	return TypedEvidenceContract{
		CriteriaRevision: "vela.production-gates/" + string(gate) + "/v1",
		CheckIDs:         checks,
		Measurements:     measurements,
		ArtifactKinds:    artifacts,
	}
}

func measurementRequirement(
	id,
	unit string,
	comparator EvidenceComparator,
	threshold int64,
) EvidenceMeasurementRequirement {
	return EvidenceMeasurementRequirement{ID: id, Unit: unit, Comparator: comparator, Threshold: threshold}
}

func sortedPresetCertificationClaims(
	claims []PresetCertificationClaim,
) []PresetCertificationClaim {
	sorted := append([]PresetCertificationClaim(nil), claims...)
	sort.Slice(sorted, func(left, right int) bool {
		return sorted[left].EvidenceID < sorted[right].EvidenceID
	})
	return sorted
}

func sortedRateCardPromotionClaims(claims []RateCardPromotionClaim) []RateCardPromotionClaim {
	sorted := append([]RateCardPromotionClaim(nil), claims...)
	sort.Slice(sorted, func(left, right int) bool {
		if sorted[left].BindingID == sorted[right].BindingID {
			return sorted[left].RateCardRevisionID < sorted[right].RateCardRevisionID
		}
		return sorted[left].BindingID < sorted[right].BindingID
	})
	return sorted
}

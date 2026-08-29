package sloevidence

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/vivym/vela/internal/slo"
	"github.com/vivym/vela/internal/strictjson"
)

const MaxEvidenceBytes = 16 * 1024 * 1024

var ErrInvalidEvidence = errors.New("invalid statistical SLO evidence")

type ArtifactKind string

const (
	ArtifactGatewayObservations ArtifactKind = "gateway-observations"
	ArtifactSaleableSKUSnapshot ArtifactKind = "saleable-sku-snapshot"
	ArtifactDashboard           ArtifactKind = "dashboard"
	ArtifactAlertRules          ArtifactKind = "alert-rules"
	ArtifactRuleTests           ArtifactKind = "rule-tests"
	ArtifactRunbook             ArtifactKind = "runbook"
	ArtifactPageEvents          ArtifactKind = "page-events"
)

type Artifact struct {
	Kind   ArtifactKind `json:"kind"`
	Ref    string       `json:"ref"`
	Digest string       `json:"digest"`
}

type APIEvidence struct {
	EligibleCount int                    `json:"eligible_count"`
	GoodCount     int                    `json:"good_count"`
	MinimumSample int                    `json:"minimum_sample"`
	Report        slo.AvailabilityReport `json:"report"`
}

type CohortEvidence struct {
	Contract     slo.Contract      `json:"contract"`
	Observations []slo.Observation `json:"observations"`
	Report       slo.Report        `json:"report"`
}

type Exercise struct {
	AlertFiredAt     time.Time  `json:"alert_fired_at"`
	AlertDeliveredAt time.Time  `json:"alert_delivered_at"`
	AlertAckedAt     time.Time  `json:"alert_acked_at"`
	ResolvedAt       time.Time  `json:"resolved_at"`
	Result           slo.Result `json:"result"`
}

// Evidence is the machine-verifiable payload for the existing
// observability-on-call Production Gate. Repository fixtures validate this
// contract but are not production evidence.
type Evidence struct {
	SchemaVersion               int              `json:"schema_version"`
	ReleaseDigest               string           `json:"release_digest"`
	ConfigurationRevision       string           `json:"configuration_revision"`
	ValidationEnvironment       string           `json:"validation_environment"`
	Owner                       string           `json:"owner"`
	Coverage                    string           `json:"coverage"`
	Window                      slo.Window       `json:"window"`
	EvaluatedAt                 time.Time        `json:"evaluated_at"`
	SaleableContractRevisionIDs []string         `json:"saleable_contract_revision_ids"`
	API                         APIEvidence      `json:"api"`
	Cohorts                     []CohortEvidence `json:"cohorts"`
	Artifacts                   []Artifact       `json:"artifacts"`
	Exercise                    Exercise         `json:"exercise"`
}

func Decode(encoded []byte, expectedRelease, expectedConfiguration string) (Evidence, error) {
	if len(encoded) == 0 || len(encoded) > MaxEvidenceBytes {
		return Evidence{}, fmt.Errorf("%w: evidence size must be in 1..%d bytes", ErrInvalidEvidence, MaxEvidenceBytes)
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return Evidence{}, fmt.Errorf("%w: %v", ErrInvalidEvidence, err)
	}
	var evidence Evidence
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evidence); err != nil {
		return Evidence{}, fmt.Errorf("%w: decode: %v", ErrInvalidEvidence, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Evidence{}, fmt.Errorf("%w: trailing JSON data", ErrInvalidEvidence)
	}
	if err := evidence.Validate(expectedRelease, expectedConfiguration); err != nil {
		return Evidence{}, err
	}
	return evidence, nil
}

func (evidence Evidence) Validate(expectedRelease, expectedConfiguration string) error {
	if evidence.SchemaVersion != 1 || !validDigest(evidence.ReleaseDigest) ||
		evidence.ReleaseDigest != expectedRelease ||
		evidence.ConfigurationRevision != expectedConfiguration ||
		!bounded(evidence.ConfigurationRevision, 300) ||
		!bounded(evidence.ValidationEnvironment, 500) || !bounded(evidence.Owner, 300) ||
		evidence.Coverage != "24x7" || evidence.EvaluatedAt.IsZero() {
		return fmt.Errorf("%w: release, configuration, environment, owner or coverage binding is invalid", ErrInvalidEvidence)
	}
	apiReport, err := slo.EvaluateAvailability(
		evidence.API.EligibleCount,
		evidence.API.GoodCount,
		evidence.API.MinimumSample,
	)
	if err != nil || apiReport.Result != slo.ResultPass || !reflect.DeepEqual(apiReport, evidence.API.Report) {
		return fmt.Errorf("%w: API availability report is not a recomputed 99.9%% PASS", ErrInvalidEvidence)
	}
	if len(evidence.Cohorts) == 0 || len(evidence.Cohorts) > 10_000 {
		return fmt.Errorf("%w: cohort count must be in 1..10000", ErrInvalidEvidence)
	}
	contractIDs := make([]string, 0, len(evidence.Cohorts))
	presets := make(map[string]bool, 3)
	seenContracts := make(map[string]struct{}, len(evidence.Cohorts))
	for _, cohort := range evidence.Cohorts {
		if _, duplicate := seenContracts[cohort.Contract.RevisionID]; duplicate {
			return fmt.Errorf("%w: duplicate contract revision %q", ErrInvalidEvidence, cohort.Contract.RevisionID)
		}
		seenContracts[cohort.Contract.RevisionID] = struct{}{}
		contractIDs = append(contractIDs, cohort.Contract.RevisionID)
		presets[cohort.Contract.GenerationPreset] = true
		report, evaluateErr := slo.Evaluate(
			cohort.Contract,
			evidence.Window,
			evidence.EvaluatedAt,
			cohort.Observations,
		)
		if evaluateErr != nil || report.Result != slo.ResultPass || !reflect.DeepEqual(report, cohort.Report) {
			return fmt.Errorf("%w: cohort %q is not an exact recomputed PASS", ErrInvalidEvidence, cohort.Contract.RevisionID)
		}
	}
	if !presets["quality"] || !presets["balanced"] || !presets["fast"] {
		return fmt.Errorf("%w: independent quality, balanced and fast cohorts are required", ErrInvalidEvidence)
	}
	wantIDs := append([]string(nil), evidence.SaleableContractRevisionIDs...)
	sort.Strings(wantIDs)
	sort.Strings(contractIDs)
	if len(wantIDs) != len(contractIDs) || !reflect.DeepEqual(wantIDs, contractIDs) {
		return fmt.Errorf("%w: cohort set does not exactly cover the saleable contract snapshot", ErrInvalidEvidence)
	}
	for index := 1; index < len(wantIDs); index++ {
		if wantIDs[index] == wantIDs[index-1] {
			return fmt.Errorf("%w: duplicate saleable contract revision %q", ErrInvalidEvidence, wantIDs[index])
		}
	}
	if err := validateArtifacts(evidence.Artifacts); err != nil {
		return err
	}
	if evidence.Exercise.Result != slo.ResultPass ||
		evidence.Exercise.AlertFiredAt.IsZero() ||
		evidence.Exercise.AlertDeliveredAt.Before(evidence.Exercise.AlertFiredAt) ||
		evidence.Exercise.AlertAckedAt.Before(evidence.Exercise.AlertDeliveredAt) ||
		evidence.Exercise.ResolvedAt.Before(evidence.Exercise.AlertAckedAt) {
		return fmt.Errorf("%w: paging exercise is missing, failed or out of order", ErrInvalidEvidence)
	}
	return nil
}

func validateArtifacts(artifacts []Artifact) error {
	required := map[ArtifactKind]bool{
		ArtifactGatewayObservations: false,
		ArtifactSaleableSKUSnapshot: false,
		ArtifactDashboard:           false,
		ArtifactAlertRules:          false,
		ArtifactRuleTests:           false,
		ArtifactRunbook:             false,
		ArtifactPageEvents:          false,
	}
	if len(artifacts) != len(required) {
		return fmt.Errorf("%w: exactly one artifact of every required kind is required", ErrInvalidEvidence)
	}
	for _, artifact := range artifacts {
		present, known := required[artifact.Kind]
		if !known || present || !filepath.IsLocal(filepath.FromSlash(artifact.Ref)) ||
			!bounded(artifact.Ref, 2000) || !validDigest(artifact.Digest) {
			return fmt.Errorf("%w: invalid or duplicate artifact %q", ErrInvalidEvidence, artifact.Kind)
		}
		required[artifact.Kind] = true
	}
	return nil
}

func Digest(encoded []byte) string {
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func validDigest(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && len(decoded) == sha256.Size
}

func bounded(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value
}

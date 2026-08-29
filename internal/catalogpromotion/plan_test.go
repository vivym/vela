package catalogpromotion

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestLoadPlanAcceptsOneReleasePromotion(t *testing.T) {
	path := writePlanFixture(t, func(_ *Plan) {})
	plan, err := LoadPlan(path)
	if err != nil {
		t.Fatalf("load Catalog promotion plan: %v", err)
	}
	if plan.SchemaVersion != 2 || plan.ManifestRef != "launch-receipts.json" ||
		plan.ReleaseBundleRef != "release-bundle.json" ||
		plan.SupplyChainManifestRef != "supply-chain.json" ||
		len(plan.Certifications) != 1 || len(plan.RateCards) != 1 || !plan.EnableEvidenced {
		t.Fatalf("loaded Catalog promotion plan = %#v", plan)
	}
}

func TestLoadPlanRejectsAmbiguousOrEscapedInput(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*Plan)
	}{
		{
			name: "path escape",
			mutate: func(plan *Plan) {
				plan.ManifestRef = "../launch-receipts.json"
			},
		},
		{
			name: "release bundle omitted",
			mutate: func(plan *Plan) {
				plan.ReleaseBundleRef = ""
			},
		},
		{
			name: "supply-chain manifest omitted",
			mutate: func(plan *Plan) {
				plan.SupplyChainManifestRef = ""
			},
		},
		{
			name: "legacy schema",
			mutate: func(plan *Plan) {
				plan.SchemaVersion = 1
			},
		},
		{
			name: "release bundle path escape",
			mutate: func(plan *Plan) {
				plan.ReleaseBundleRef = "../release-bundle.json"
			},
		},
		{
			name: "release bundle noncanonical path",
			mutate: func(plan *Plan) {
				plan.ReleaseBundleRef = "bundle/../release-bundle.json"
			},
		},
		{
			name: "duplicate certification",
			mutate: func(plan *Plan) {
				plan.Certifications = append(plan.Certifications, plan.Certifications[0])
			},
		},
		{
			name: "duplicate rate card",
			mutate: func(plan *Plan) {
				plan.RateCards = append(plan.RateCards, plan.RateCards[0])
			},
		},
		{
			name: "switch omitted",
			mutate: func(plan *Plan) {
				plan.EnableEvidenced = false
			},
		},
		{
			name: "control character",
			mutate: func(plan *Plan) {
				plan.Certifications[0].HardwareDriverBaseline = "h3\tdriver"
			},
		},
		{
			name: "invalid currency",
			mutate: func(plan *Plan) {
				plan.Certifications[0].CostCurrency = "123"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := writePlanFixture(t, test.mutate)
			if _, err := LoadPlan(path); !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("LoadPlan error = %v, want invalid plan", err)
			}
		})
	}
}

func TestLoadPlanRejectsUnknownFieldAndTrailingDocument(t *testing.T) {
	for _, suffix := range []string{`,"waiver":true}`, `}\n{}`} {
		path := writePlanFixture(t, func(_ *Plan) {})
		encoded, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read plan fixture: %v", err)
		}
		encoded = []byte(strings.TrimSuffix(string(encoded), "}") + suffix)
		if err := os.WriteFile(path, encoded, 0o600); err != nil {
			t.Fatalf("rewrite malformed plan: %v", err)
		}
		if _, err := LoadPlan(path); !errors.Is(err, ErrInvalidPlan) {
			t.Fatalf("LoadPlan error = %v, want invalid plan", err)
		}
	}
}

func TestLoadPlanRejectsDuplicateJSONKeys(t *testing.T) {
	for _, replacement := range []string{
		`"schema_version":2,"schema_version":2`,
		`"evidence_id":"35000000-0000-0000-0000-000000000101","evidence_id":"35000000-0000-0000-0000-000000000101"`,
	} {
		path := writePlanFixture(t, func(_ *Plan) {})
		encoded, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read plan fixture: %v", err)
		}
		key := strings.SplitN(replacement, ",", 2)[0]
		encoded = []byte(strings.Replace(string(encoded), key, replacement, 1))
		if err := os.WriteFile(path, encoded, 0o600); err != nil {
			t.Fatalf("rewrite duplicate-key plan: %v", err)
		}
		if _, err := LoadPlan(path); !errors.Is(err, ErrInvalidPlan) ||
			!strings.Contains(err.Error(), "duplicate JSON key") {
			t.Fatalf("LoadPlan error = %v, want duplicate-key rejection", err)
		}
	}
}

func writePlanFixture(t *testing.T, mutate func(*Plan)) string {
	t.Helper()
	plan := Plan{
		SchemaVersion:          2,
		ManifestRef:            "launch-receipts.json",
		ReleaseBundleRef:       "release-bundle.json",
		SupplyChainManifestRef: "supply-chain.json",
		EnableEvidenced:        true,
		Certifications: []CertificationPromotion{{
			EvidenceID:                 uuid.MustParse("35000000-0000-0000-0000-000000000101"),
			ProfileCertificationID:     uuid.MustParse("35000000-0000-0000-0000-000000000102"),
			InferenceBackendRevisionID: uuid.MustParse("35000000-0000-0000-0000-000000000103"),
			HardwareDriverBaseline:     "h3-8gpu-driver-r1",
			BenchmarkCorpusRevision:    "h3-video-quality-v2",
			QualityThresholdPPM:        820000,
			QualityObservedPPM:         850000,
			SuccessRateThresholdPPM:    990000,
			SuccessRateObservedPPM:     999000,
			P50Milliseconds:            900000,
			P95ThresholdMilliseconds:   1800000,
			P95ObservedMilliseconds:    1700000,
			CostThresholdMinor:         500000,
			CostObservedMinor:          450000,
			CostCurrency:               "CNY",
			ConfidenceThresholdPPM:     950000,
			ConfidenceObservedPPM:      990000,
		}},
		RateCards: []RateCardPromotion{{
			BindingID:          uuid.MustParse("35000000-0000-0000-0000-000000000201"),
			RateCardRevisionID: uuid.MustParse("35000000-0000-0000-0000-000000000202"),
		}},
	}
	mutate(&plan)
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("encode promotion plan: %v", err)
	}
	path := filepath.Join(t.TempDir(), "catalog-promotion.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write promotion plan: %v", err)
	}
	return path
}

package sloevidence

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"time"

	"github.com/vivym/vela/internal/slo"
	"github.com/vivym/vela/internal/strictjson"
)

const (
	maximumGatewayStreams      = 2
	maximumGatewayBuckets      = 100_000
	maximumGatewayBucketCount  = 2_000_000_000
	maximumGatewayBucketPeriod = 24 * time.Hour
)

type GatewayObservationSource string

const (
	GatewaySourceExternalGateway GatewayObservationSource = "external-gateway"
	GatewaySourceSyntheticProbe  GatewayObservationSource = "synthetic-probe"
)

type GatewayObservations struct {
	SchemaVersion int                        `json:"schema_version"`
	Window        slo.Window                 `json:"window"`
	Streams       []GatewayObservationStream `json:"streams"`
}

type GatewayObservationStream struct {
	Source  GatewayObservationSource   `json:"source"`
	Buckets []GatewayObservationBucket `json:"buckets"`
}

type GatewayObservationBucket struct {
	Start         time.Time `json:"start"`
	End           time.Time `json:"end"`
	EligibleCount int       `json:"eligible_count"`
	GoodCount     int       `json:"good_count"`
}

type SaleableSKUSnapshot struct {
	SchemaVersion int            `json:"schema_version"`
	CapturedAt    time.Time      `json:"captured_at"`
	Contracts     []slo.Contract `json:"contracts"`
}

// ValidateArtifact reverses the two source artifacts that determine the
// computed API and saleable-contract results. Other required artifacts remain
// byte-bound by their digest and are validated by their deployment contracts.
func (evidence Evidence) ValidateArtifact(kind ArtifactKind, encoded []byte) error {
	switch kind {
	case ArtifactGatewayObservations:
		var observations GatewayObservations
		if err := decodeStrictArtifact(encoded, &observations); err != nil {
			return fmt.Errorf("%w: decode gateway observations: %v", ErrInvalidEvidence, err)
		}
		return evidence.validateGatewayObservations(observations)
	case ArtifactSaleableSKUSnapshot:
		var snapshot SaleableSKUSnapshot
		if err := decodeStrictArtifact(encoded, &snapshot); err != nil {
			return fmt.Errorf("%w: decode saleable SKU snapshot: %v", ErrInvalidEvidence, err)
		}
		return evidence.validateSaleableSnapshot(snapshot)
	default:
		return nil
	}
}

func (evidence Evidence) validateGatewayObservations(observations GatewayObservations) error {
	if observations.SchemaVersion != 1 || observations.Window != evidence.Window ||
		len(observations.Streams) != maximumGatewayStreams {
		return fmt.Errorf("%w: gateway observation window or source set is invalid", ErrInvalidEvidence)
	}
	seenSources := make(map[GatewayObservationSource]bool, maximumGatewayStreams)
	totalEligible := 0
	totalGood := 0
	for _, stream := range observations.Streams {
		if (stream.Source != GatewaySourceExternalGateway && stream.Source != GatewaySourceSyntheticProbe) ||
			seenSources[stream.Source] || len(stream.Buckets) == 0 || len(stream.Buckets) > maximumGatewayBuckets {
			return fmt.Errorf("%w: gateway observation source or bucket count is invalid", ErrInvalidEvidence)
		}
		seenSources[stream.Source] = true
		nextStart := evidence.Window.Start
		for _, bucket := range stream.Buckets {
			if !bucket.Start.Equal(nextStart) || !bucket.Start.Before(bucket.End) ||
				bucket.End.After(evidence.Window.End) ||
				bucket.End.Sub(bucket.Start) > maximumGatewayBucketPeriod ||
				bucket.EligibleCount < 0 || bucket.EligibleCount > maximumGatewayBucketCount ||
				bucket.GoodCount < 0 || bucket.GoodCount > bucket.EligibleCount ||
				totalEligible > maximumGatewayBucketCount-bucket.EligibleCount ||
				totalGood > maximumGatewayBucketCount-bucket.GoodCount {
				return fmt.Errorf("%w: gateway observation bucket is invalid or unbounded", ErrInvalidEvidence)
			}
			totalEligible += bucket.EligibleCount
			totalGood += bucket.GoodCount
			nextStart = bucket.End
		}
		if !nextStart.Equal(evidence.Window.End) {
			return fmt.Errorf("%w: gateway observation buckets are not contiguous over the evidence window", ErrInvalidEvidence)
		}
	}
	if !seenSources[GatewaySourceExternalGateway] || !seenSources[GatewaySourceSyntheticProbe] ||
		totalEligible != evidence.API.EligibleCount || totalGood != evidence.API.GoodCount {
		return fmt.Errorf("%w: gateway observation counts do not reproduce API evidence", ErrInvalidEvidence)
	}
	return nil
}

func (evidence Evidence) validateSaleableSnapshot(snapshot SaleableSKUSnapshot) error {
	if snapshot.SchemaVersion != 1 || snapshot.CapturedAt.Before(evidence.Window.End) ||
		snapshot.CapturedAt.After(evidence.EvaluatedAt) || len(snapshot.Contracts) == 0 ||
		len(snapshot.Contracts) > 10_000 || len(snapshot.Contracts) != len(evidence.Cohorts) {
		return fmt.Errorf("%w: saleable SKU snapshot boundary is invalid", ErrInvalidEvidence)
	}
	want := make([]slo.Contract, 0, len(evidence.Cohorts))
	for _, cohort := range evidence.Cohorts {
		want = append(want, cohort.Contract)
	}
	got := append([]slo.Contract(nil), snapshot.Contracts...)
	sort.Slice(want, func(i, j int) bool { return want[i].RevisionID < want[j].RevisionID })
	sort.Slice(got, func(i, j int) bool { return got[i].RevisionID < got[j].RevisionID })
	for index := 1; index < len(got); index++ {
		if got[index].RevisionID == got[index-1].RevisionID {
			return fmt.Errorf("%w: duplicate saleable snapshot contract %q", ErrInvalidEvidence, got[index].RevisionID)
		}
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("%w: saleable SKU snapshot does not exactly reproduce evidence cohorts", ErrInvalidEvidence)
	}
	return nil
}

func decodeStrictArtifact(encoded []byte, target any) error {
	if len(encoded) == 0 || len(encoded) > MaxEvidenceBytes {
		return fmt.Errorf("artifact size must be in 1..%d bytes", MaxEvidenceBytes)
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON data")
	}
	return nil
}

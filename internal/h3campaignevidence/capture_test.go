package h3campaignevidence

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCaptureDoubleReadsOneStableAuthoritativeCampaign(t *testing.T) {
	input := physicalCampaignInput()
	input.seal = sealEvidenceBinding(input.EvidenceBinding)
	now := time.Now().UTC()
	setCaptureFixtureTimes(&input, now)
	selection := Selection{
		SameNodeJobID:  input.Runs[0].JobID,
		CrossNodeJobID: input.Runs[1].JobID,
		CacheJobID:     input.CacheRun.JobID,
	}
	snapshot := DatabaseSnapshot{
		Provenance: DatabaseReadProvenance{
			DatabaseTime: now.Add(-time.Second), BackendPID: "8421",
			SnapshotID: "00000003-0000001B-1",
		},
		Runs: input.Runs, CacheRun: input.CacheRun,
	}
	reader := &sequenceCampaignReader{snapshots: []DatabaseSnapshot{snapshot, snapshot}}

	evidence, err := Capture(
		context.Background(), reader,
		CaptureRequest{
			EvidenceBinding: input.EvidenceBinding,
			Selection:       selection,
		},
	)
	if err != nil || reader.calls != 2 || evidence.CacheRun.JobID != selection.CacheJobID {
		t.Fatalf("Capture stable campaign evidence=%#v calls=%d error=%v", evidence, reader.calls, err)
	}
}

func TestCaptureRejectsCampaignAuthorityDrift(t *testing.T) {
	input := physicalCampaignInput()
	input.seal = sealEvidenceBinding(input.EvidenceBinding)
	now := time.Now().UTC()
	setCaptureFixtureTimes(&input, now)
	selection := Selection{
		SameNodeJobID:  input.Runs[0].JobID,
		CrossNodeJobID: input.Runs[1].JobID,
		CacheJobID:     input.CacheRun.JobID,
	}
	stable := DatabaseSnapshot{
		Provenance: DatabaseReadProvenance{
			DatabaseTime: now.Add(-time.Second), BackendPID: "8421",
			SnapshotID: "00000003-0000001B-1",
		},
		Runs: input.Runs, CacheRun: input.CacheRun,
	}
	drifted := stable
	drifted.Runs = append([]RunSnapshot(nil), stable.Runs...)
	drifted.Runs[1].Transfers = append(
		[]TransferSnapshot(nil), stable.Runs[1].Transfers...,
	)
	drifted.Runs[1].Transfers[0].State = "REVOKED"
	reader := &sequenceCampaignReader{snapshots: []DatabaseSnapshot{stable, drifted}}

	_, err := Capture(
		context.Background(), reader,
		CaptureRequest{
			EvidenceBinding: input.EvidenceBinding,
			Selection:       selection,
		},
	)
	if !errors.Is(err, ErrUnstableCampaignAuthority) {
		t.Fatalf("Capture drift error = %v, want ErrUnstableCampaignAuthority", err)
	}
}

func TestCaptureRejectsMutatedSealedReleaseBinding(t *testing.T) {
	input := physicalCampaignInput()
	input.seal = sealEvidenceBinding(input.EvidenceBinding)
	input.ReleaseDigest = digest('9')
	reader := &sequenceCampaignReader{}
	_, err := Capture(context.Background(), reader, CaptureRequest{
		EvidenceBinding: input.EvidenceBinding,
		Selection: Selection{
			SameNodeJobID: input.Runs[0].JobID, CrossNodeJobID: input.Runs[1].JobID,
			CacheJobID: input.CacheRun.JobID,
		},
	})
	if err == nil || reader.calls != 0 {
		t.Fatalf("Capture mutated binding error=%v calls=%d", err, reader.calls)
	}
}

type sequenceCampaignReader struct {
	snapshots []DatabaseSnapshot
	calls     int
}

func setCaptureFixtureTimes(input *Input, now time.Time) {
	for index := range input.CacheRun.Hits {
		input.CacheRun.Hits[index].ReferenceAcquiredAt = now.Add(-time.Minute)
		input.CacheRun.Hits[index].PinAcquiredAt = now.Add(-time.Minute)
	}
}

func (reader *sequenceCampaignReader) Capture(
	_ context.Context,
	_ Selection,
) (DatabaseSnapshot, error) {
	if reader.calls >= len(reader.snapshots) {
		return DatabaseSnapshot{}, errors.New("unexpected Campaign capture")
	}
	snapshot := reader.snapshots[reader.calls]
	reader.calls++
	return snapshot, nil
}

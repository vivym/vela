package stagescheduler

import (
	"bytes"
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsExposeOnlyBoundedStageSchedulerLabels(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 8, 0, 0, 0, time.UTC)
	candidate := schedulingCandidate(
		"52000000-0000-0000-0000-000000000050",
		"52000000-0000-0000-0000-000000000051",
		"52000000-0000-0000-0000-000000000052",
		now.Add(-time.Minute),
	)
	snapshot := schedulingSnapshot(now, []Candidate{candidate})
	expected, selected, err := Decide(snapshot)
	if err != nil || !selected {
		t.Fatalf("prepare metric shadow evidence selected=%t error=%v", selected, err)
	}
	repository := &recordingStageRepository{
		captured: CapturedSnapshot{
			ID:       uuid.MustParse("52000000-0000-0000-0000-000000000053"),
			Snapshot: snapshot,
		},
		shadow: []ShadowSnapshot{{
			ID:                     uuid.MustParse("52000000-0000-0000-0000-000000000054"),
			Snapshot:               snapshot,
			ExpectedEvidenceDigest: expected.EvidenceDigest,
		}},
	}
	metrics := NewMetrics()
	service, err := NewService(repository, &recordingStageCoordinator{}, Config{
		SchedulerID:      "stage-scheduler/metrics",
		ClaimTTL:         30 * time.Second,
		LeaseTTL:         time.Minute,
		LocalDeadlineTTL: 50 * time.Second,
		SigningKeyID:     "stage-authority-key-v1",
		Now:              func() time.Time { return now },
		Random:           bytes.NewReader(bytes.Repeat([]byte{0x73}, 512)),
		Metrics:          metrics,
	})
	if err != nil {
		t.Fatalf("NewService with metrics: %v", err)
	}
	if _, ok, err := service.Acquire(
		context.Background(),
		testWorkerAuthority(snapshot, candidate.StageProfileRevisionID),
		CapacityObservation{Sequence: snapshot.ObservationSequence},
	); err != nil || !ok {
		t.Fatalf("Acquire metric fixture ok=%t error=%v", ok, err)
	}
	if _, err := service.ReplayShadow(context.Background(), 10); err != nil {
		t.Fatalf("ReplayShadow metric fixture: %v", err)
	}
	if _, err := service.ReconcileExpired(context.Background(), 10); err != nil {
		t.Fatalf("ReconcileExpired metric fixture: %v", err)
	}

	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(metrics); err != nil {
		t.Fatalf("register StageScheduler metrics: %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather StageScheduler metrics: %v", err)
	}
	wantFamilies := map[string]bool{
		"vela_stage_scheduler_acquire_total":         false,
		"vela_stage_scheduler_claim_reconcile_total": false,
		"vela_stage_scheduler_shadow_replay_total":   false,
	}
	for _, family := range families {
		if _, tracked := wantFamilies[family.GetName()]; !tracked {
			continue
		}
		wantFamilies[family.GetName()] = true
		for _, metric := range family.Metric {
			labels := make([]string, 0, len(metric.Label))
			for _, label := range metric.Label {
				labels = append(labels, label.GetName())
				if label.GetName() == "organization_id" || label.GetName() == "project_id" ||
					label.GetName() == "job_id" || label.GetName() == "attempt_id" ||
					label.GetName() == "worker_instance_id" || label.GetName() == "stage_run_id" {
					t.Fatalf("metric %s contains forbidden label %s", family.GetName(), label.GetName())
				}
			}
			slices.Sort(labels)
			if !slices.Equal(labels, []string{"algorithm_revision", "outcome", "reason"}) {
				t.Fatalf("metric %s labels = %v", family.GetName(), labels)
			}
		}
	}
	if !wantFamilies["vela_stage_scheduler_acquire_total"] ||
		!wantFamilies["vela_stage_scheduler_shadow_replay_total"] ||
		!wantFamilies["vela_stage_scheduler_claim_reconcile_total"] {
		t.Fatalf("StageScheduler metric families = %#v", wantFamilies)
	}
}

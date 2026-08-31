package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
)

func TestRunRequiresLiveCaptureConfiguration(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(nil, func(string) string { return "" }, &stdout, &stderr); code != 2 {
		t.Fatalf("run exit code = %d, want 2; stderr=%s", code, stderr.String())
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "usage:") ||
		strings.Contains(stderr.String(), "snapshot") {
		t.Fatalf("run output = stdout %q stderr %q", stdout.String(), stderr.String())
	}
}

func TestSelectRolloutRequiresOneExactPlanFromRelease(t *testing.T) {
	firstID := uuid.MustParse("49350000-0000-0000-0000-000000000001")
	secondID := uuid.MustParse("49350000-0000-0000-0000-000000000002")
	rollouts := []fleetcontroller.ResidencyPlanRollout{
		{ApprovedPlan: fleet.ApprovedResidencyPlan{ID: firstID}},
		{ApprovedPlan: fleet.ApprovedResidencyPlan{ID: secondID}},
	}
	selected, err := selectRollout(rollouts, secondID)
	if err != nil || selected.ApprovedPlan.ID != secondID {
		t.Fatalf("select exact rollout = %#v error=%v", selected, err)
	}
	if _, err := selectRollout(rollouts, uuid.New()); err == nil {
		t.Fatal("missing release-bound rollout was accepted")
	}
	rollouts = append(rollouts, rollouts[1])
	if _, err := selectRollout(rollouts, secondID); err == nil {
		t.Fatal("duplicate release-bound rollout was accepted")
	}
}

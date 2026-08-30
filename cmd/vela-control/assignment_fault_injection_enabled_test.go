//go:build vela_lab_fault_injection

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/workercontrol"
)

func TestLabAssignmentFaultCoordinatorPublishesPayloadFreeMarkerAndWaits(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), "marker.json")
	assignment := enabledTestAssignment()
	delegate := &enabledTestAssignmentCoordinator{assignment: assignment}
	coordinator, err := newLabAssignmentFaultCoordinator(delegate, markerPath, time.Minute)
	if err != nil {
		t.Fatalf("create lab Assignment fault coordinator: %v", err)
	}
	worker := workercontrol.AuthenticatedWorker{ID: assignment.WorkerID}
	candidate := enabledTestAssignmentCandidate(assignment)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, acquireErr := coordinator.Acquire(ctx, worker, assignment.WorkerEpoch, candidate)
		result <- acquireErr
	}()

	markerBytes := waitForLabAssignmentMarker(t, markerPath)
	var output bytes.Buffer
	if err := readLabAssignmentFaultMarker(markerPath, &output); err != nil {
		t.Fatalf("read lab Assignment fault marker: %v", err)
	}
	if !bytes.Equal(output.Bytes(), markerBytes) {
		t.Fatalf("marker command output = %q, want %q", output.Bytes(), markerBytes)
	}
	for _, private := range []string{assignment.LeaseToken, assignment.RequestContent} {
		if private != "" && bytes.Contains(markerBytes, []byte(private)) {
			t.Fatalf("lab Assignment marker contains private payload %q", private)
		}
	}
	if delegate.callCount() != 1 {
		t.Fatalf("delegate calls = %d, want 1", delegate.callCount())
	}

	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("paused Assignment error = %v, want context cancellation", err)
	}
}

func TestLabAssignmentFaultCoordinatorReplaysNormallyAfterMarkerExists(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), "marker.json")
	assignment := enabledTestAssignment()
	marker := labAssignmentFaultMarker{
		Schema:                    labAssignmentFaultMarkerSchema,
		Phase:                     assignmentPostCommitPreResponse,
		JobID:                     assignment.JobID.String(),
		AttemptID:                 assignment.AttemptID.String(),
		WorkerID:                  assignment.WorkerID.String(),
		WorkerEpoch:               assignment.WorkerEpoch,
		AttemptNumber:             assignment.AttemptNumber,
		LeaseFence:                assignment.LeaseFence,
		SchedulerDispatchIntentID: assignment.SchedulerDispatchIntentID.String(),
		AssignmentCommitted:       true,
	}
	if err := writeLabAssignmentFaultMarker(markerPath, marker); err != nil {
		t.Fatalf("write prior lab Assignment marker: %v", err)
	}
	delegate := &enabledTestAssignmentCoordinator{assignment: assignment}
	coordinator, err := newLabAssignmentFaultCoordinator(delegate, markerPath, time.Minute)
	if err != nil {
		t.Fatalf("create lab Assignment fault coordinator: %v", err)
	}
	got, err := coordinator.Acquire(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: assignment.WorkerID},
		assignment.WorkerEpoch,
		enabledTestAssignmentCandidate(assignment),
	)
	if err != nil {
		t.Fatalf("replay Assignment after marker: %v", err)
	}
	if got.AttemptID != assignment.AttemptID || got.LeaseFence != assignment.LeaseFence {
		t.Fatalf("replayed Assignment = %#v, want %#v", got, assignment)
	}
}

func TestLabAssignmentFaultCoordinatorRejectsInvalidPriorMarker(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), "marker.json")
	if err := os.WriteFile(markerPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write invalid prior lab Assignment marker: %v", err)
	}
	assignment := enabledTestAssignment()
	coordinator, err := newLabAssignmentFaultCoordinator(
		&enabledTestAssignmentCoordinator{assignment: assignment},
		markerPath,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("create lab Assignment fault coordinator: %v", err)
	}
	_, err = coordinator.Acquire(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: assignment.WorkerID},
		assignment.WorkerEpoch,
		enabledTestAssignmentCandidate(assignment),
	)
	if err == nil {
		t.Fatal("Assignment with invalid prior marker succeeded")
	}
}

func TestReadLabAssignmentFaultMarkerRejectsDuplicateKeysAndExtraFields(t *testing.T) {
	valid := "{\"schema\":\"vela-lab-assignment-fault-marker-v1\",\"phase\":\"assignment-post-commit-pre-response-crash\",\"job_id\":\"00000000-0000-0000-0000-000000000001\",\"attempt_id\":\"00000000-0000-0000-0000-000000000002\",\"worker_id\":\"00000000-0000-0000-0000-000000000003\",\"worker_epoch\":7,\"attempt_number\":1,\"lease_fence\":1,\"scheduler_dispatch_intent_id\":\"00000000-0000-0000-0000-000000000004\",\"assignment_committed\":true"
	for _, test := range []struct {
		name    string
		encoded string
	}{
		{name: "duplicate key", encoded: valid + ",\"schema\":\"vela-lab-assignment-fault-marker-v1\"}\n"},
		{name: "extra field", encoded: valid + ",\"unexpected\":true}\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			markerPath := filepath.Join(t.TempDir(), "marker.json")
			if err := os.WriteFile(markerPath, []byte(test.encoded), 0o600); err != nil {
				t.Fatalf("write invalid lab Assignment marker: %v", err)
			}
			if err := readLabAssignmentFaultMarker(markerPath, &bytes.Buffer{}); err == nil {
				t.Fatal("read invalid lab Assignment marker succeeded")
			}
		})
	}
}

func TestWriteLabAssignmentFaultMarkerDoesNotReplaceExistingMarker(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), "marker.json")
	want := []byte("existing\n")
	if err := os.WriteFile(markerPath, want, 0o600); err != nil {
		t.Fatalf("write existing lab Assignment marker: %v", err)
	}
	if err := writeLabAssignmentFaultMarker(markerPath, labAssignmentFaultMarker{}); err == nil {
		t.Fatal("replaced existing lab Assignment marker")
	}
	got, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatalf("read existing lab Assignment marker: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("existing marker = %q, want %q", got, want)
	}
}

func waitForLabAssignmentMarker(t *testing.T, markerPath string) []byte {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		encoded, err := os.ReadFile(markerPath)
		if err == nil {
			return encoded
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read lab Assignment marker: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("lab Assignment marker was not written")
	return nil
}

func enabledTestAssignment() workercontrol.Assignment {
	return workercontrol.Assignment{
		JobID:                     uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		AttemptID:                 uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		WorkerID:                  uuid.MustParse("00000000-0000-0000-0000-000000000003"),
		WorkerEpoch:               7,
		AttemptNumber:             1,
		LeaseFence:                1,
		SchedulerDispatchIntentID: uuid.MustParse("00000000-0000-0000-0000-000000000004"),
		LeaseToken:                "private-lease-token",
		RequestContent:            `{"prompt":"private-prompt"}`,
	}
}

func enabledTestAssignmentCandidate(assignment workercontrol.Assignment) *workercontrol.AssignmentCandidate {
	return &workercontrol.AssignmentCandidate{
		JobID: assignment.JobID,
		SchedulerClaim: &workercontrol.SchedulerClaim{
			IntentID: assignment.SchedulerDispatchIntentID,
		},
	}
}

type enabledTestAssignmentCoordinator struct {
	mutex      sync.Mutex
	calls      int
	assignment workercontrol.Assignment
}

func (coordinator *enabledTestAssignmentCoordinator) Acquire(
	context.Context,
	workercontrol.AuthenticatedWorker,
	int64,
	*workercontrol.AssignmentCandidate,
) (workercontrol.Assignment, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	coordinator.calls++
	return coordinator.assignment, nil
}

func (coordinator *enabledTestAssignmentCoordinator) callCount() int {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	return coordinator.calls
}

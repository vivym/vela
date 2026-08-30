//go:build vela_lab_fault_injection

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workertransport"
)

type recordingVisibleCompletionCoordinator struct {
	workertransport.WorkerCoordinator
	calls int
}

func (coordinator *recordingVisibleCompletionCoordinator) CompleteVisibleCompletion(
	context.Context,
	workercontrol.AuthenticatedWorker,
	workercontrol.LeaseCredentials,
	workercontrol.VisibleCompletionCandidate,
) (workercontrol.VisibleCompletionResult, error) {
	coordinator.calls++
	return workercontrol.VisibleCompletionResult{
		Decision: workercontrol.VisibleCompletionCommitted,
	}, nil
}

func TestVisibleCompletionFaultBlocksExactTargetBeforeDelegate(t *testing.T) {
	targetWorker := uuid.New()
	attemptID := uuid.New()
	completionID := uuid.New()
	artifactIDs := []uuid.UUID{uuid.New(), uuid.New()}
	markerPath := filepath.Join(t.TempDir(), "marker.json")
	delegate := &recordingVisibleCompletionCoordinator{}
	configured, err := newLabVisibleCompletionFaultCoordinator(
		delegate, targetWorker, markerPath, time.Second,
	)
	if err != nil {
		t.Fatalf("newLabVisibleCompletionFaultCoordinator: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, callErr := configured.CompleteVisibleCompletion(
			ctx,
			workercontrol.AuthenticatedWorker{ID: targetWorker},
			workercontrol.LeaseCredentials{
				AttemptID: attemptID, WorkerEpoch: 7, Fence: 9, Token: "secret-token",
			},
			workercontrol.VisibleCompletionCandidate{
				CompletionID: completionID, ExpectedJobVersion: 11, ArtifactIDs: artifactIDs,
			},
		)
		done <- callErr
	}()

	deadline := time.Now().Add(time.Second)
	for {
		marker, loadErr := loadLabVisibleCompletionFaultMarker(markerPath)
		if loadErr == nil {
			if marker.WorkerID != targetWorker.String() || marker.AttemptID != attemptID.String() ||
				marker.CompletionID != completionID.String() || marker.ExpectedJobVersion != 11 ||
				!reflect.DeepEqual(marker.ArtifactIDs, []string{
					artifactIDs[0].String(), artifactIDs[1].String(),
				}) || !marker.BlockedBeforeService {
				t.Fatalf("Visible Completion fault marker = %#v", marker)
			}
			break
		}
		if !errors.Is(loadErr, os.ErrNotExist) || time.Now().After(deadline) {
			t.Fatalf("load Visible Completion fault marker: %v", loadErr)
		}
		time.Sleep(time.Millisecond)
	}
	if delegate.calls != 0 {
		t.Fatalf("Visible Completion delegate calls before cancellation = %d", delegate.calls)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked Visible Completion error = %v", err)
	}
	if delegate.calls != 0 {
		t.Fatalf("Visible Completion delegate calls after cancellation = %d", delegate.calls)
	}

	var output bytes.Buffer
	err = readLabVisibleCompletionFaultMarker(markerPath, &output)
	if err != nil || bytes.Contains(output.Bytes(), []byte("secret-token")) {
		t.Fatalf("read Visible Completion fault marker = error %v output %q", err, output.String())
	}
}

func TestVisibleCompletionFaultDelegatesDifferentWorker(t *testing.T) {
	delegate := &recordingVisibleCompletionCoordinator{}
	configured, err := newLabVisibleCompletionFaultCoordinator(
		delegate, uuid.New(), filepath.Join(t.TempDir(), "marker.json"), time.Second,
	)
	if err != nil {
		t.Fatalf("newLabVisibleCompletionFaultCoordinator: %v", err)
	}
	result, err := configured.CompleteVisibleCompletion(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: uuid.New()},
		workercontrol.LeaseCredentials{},
		workercontrol.VisibleCompletionCandidate{},
	)
	if err != nil || result.Decision != workercontrol.VisibleCompletionCommitted || delegate.calls != 1 {
		t.Fatalf("delegated Visible Completion = %#v calls %d error %v", result, delegate.calls, err)
	}
}

func TestVisibleCompletionFaultRejectsChangedReplay(t *testing.T) {
	targetWorker := uuid.New()
	attemptID := uuid.New()
	markerPath := filepath.Join(t.TempDir(), "marker.json")
	delegate := &recordingVisibleCompletionCoordinator{}
	configured, err := newLabVisibleCompletionFaultCoordinator(
		delegate, targetWorker, markerPath, time.Second,
	)
	if err != nil {
		t.Fatalf("newLabVisibleCompletionFaultCoordinator: %v", err)
	}
	credentials := workercontrol.LeaseCredentials{
		AttemptID: attemptID, WorkerEpoch: 7, Fence: 9, Token: "secret-token",
	}
	first := workercontrol.VisibleCompletionCandidate{
		CompletionID: uuid.New(), ExpectedJobVersion: 11, ArtifactIDs: []uuid.UUID{uuid.New()},
	}
	marker, err := newLabVisibleCompletionFaultMarker(
		workercontrol.AuthenticatedWorker{ID: targetWorker}, credentials, first,
	)
	if err != nil {
		t.Fatalf("newLabVisibleCompletionFaultMarker: %v", err)
	}
	if err := writeLabVisibleCompletionFaultMarker(markerPath, marker); err != nil {
		t.Fatalf("writeLabVisibleCompletionFaultMarker: %v", err)
	}
	changed := first
	changed.ArtifactIDs = []uuid.UUID{uuid.New()}
	if _, err := configured.CompleteVisibleCompletion(
		context.Background(), workercontrol.AuthenticatedWorker{ID: targetWorker},
		credentials, changed,
	); err == nil || delegate.calls != 0 {
		t.Fatalf("changed Visible Completion replay = calls %d error %v", delegate.calls, err)
	}
}

//go:build vela_lab_fault_injection

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/scheduler"
	"github.com/vivym/vela/internal/strictjson"
	"github.com/vivym/vela/internal/workercontrol"
)

const (
	labAssignmentFaultWait           = 2 * time.Minute
	labAssignmentFaultMarkerMaxBytes = 4 * 1024
)

type labAssignmentFaultCoordinator struct {
	delegate   scheduler.AssignmentCoordinator
	markerPath string
	wait       time.Duration
}

type labAssignmentFaultMarker struct {
	Schema                    string `json:"schema"`
	Phase                     string `json:"phase"`
	JobID                     string `json:"job_id"`
	AttemptID                 string `json:"attempt_id"`
	WorkerID                  string `json:"worker_id"`
	WorkerEpoch               int64  `json:"worker_epoch"`
	AttemptNumber             int32  `json:"attempt_number"`
	LeaseFence                int64  `json:"lease_fence"`
	SchedulerDispatchIntentID string `json:"scheduler_dispatch_intent_id"`
	AssignmentCommitted       bool   `json:"assignment_committed"`
}

func runLabAssignmentFaultCommand(args []string, output io.Writer) (bool, error) {
	if len(args) == 0 || args[0] != labAssignmentFaultReadMarkerArg {
		return false, nil
	}
	if len(args) != 1 {
		return true, errors.New("lab Assignment fault command accepts no additional arguments")
	}
	return true, readLabAssignmentFaultMarker(labAssignmentFaultMarkerPath, output)
}

func configureSchedulerAssignmentCoordinator(
	delegate scheduler.AssignmentCoordinator,
) (scheduler.AssignmentCoordinator, error) {
	phase, configured := os.LookupEnv(labAssignmentFaultPhaseEnv)
	if !configured {
		return delegate, nil
	}
	if phase != assignmentPostCommitPreResponse {
		return nil, fmt.Errorf("unsupported lab Assignment fault phase %q", phase)
	}
	return newLabAssignmentFaultCoordinator(
		delegate,
		labAssignmentFaultMarkerPath,
		labAssignmentFaultWait,
	)
}

func newLabAssignmentFaultCoordinator(
	delegate scheduler.AssignmentCoordinator,
	markerPath string,
	wait time.Duration,
) (scheduler.AssignmentCoordinator, error) {
	if delegate == nil {
		return nil, errors.New("lab Assignment fault coordinator delegate is required")
	}
	if markerPath == "" || filepath.Base(markerPath) == "." {
		return nil, errors.New("lab Assignment fault marker path is required")
	}
	if wait <= 0 {
		return nil, errors.New("lab Assignment fault wait must be positive")
	}
	return &labAssignmentFaultCoordinator{
		delegate:   delegate,
		markerPath: markerPath,
		wait:       wait,
	}, nil
}

func (coordinator *labAssignmentFaultCoordinator) Acquire(
	ctx context.Context,
	worker workercontrol.AuthenticatedWorker,
	workerEpoch int64,
	candidate *workercontrol.AssignmentCandidate,
) (workercontrol.Assignment, error) {
	assignment, err := coordinator.delegate.Acquire(ctx, worker, workerEpoch, candidate)
	if err != nil || candidate == nil {
		return assignment, err
	}

	information, markerErr := os.Lstat(coordinator.markerPath)
	if markerErr == nil {
		if !information.Mode().IsRegular() {
			return workercontrol.Assignment{}, errors.New("lab Assignment fault marker is not a regular file")
		}
		if err := readLabAssignmentFaultMarker(coordinator.markerPath, io.Discard); err != nil {
			return workercontrol.Assignment{}, fmt.Errorf("validate prior lab Assignment fault marker: %w", err)
		}
		return assignment, nil
	}
	if !errors.Is(markerErr, os.ErrNotExist) {
		return workercontrol.Assignment{}, fmt.Errorf("inspect lab Assignment fault marker: %w", markerErr)
	}
	if !validLabAssignmentFaultTarget(assignment, worker, workerEpoch, candidate) {
		return workercontrol.Assignment{}, errors.New("lab Assignment fault target identity is invalid")
	}

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
	if err := writeLabAssignmentFaultMarker(coordinator.markerPath, marker); err != nil {
		return workercontrol.Assignment{}, err
	}
	timer := time.NewTimer(coordinator.wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return workercontrol.Assignment{}, ctx.Err()
	case <-timer.C:
		return workercontrol.Assignment{}, errors.New(
			"timed out waiting for lab Assignment fault process termination",
		)
	}
}

func validLabAssignmentFaultTarget(
	assignment workercontrol.Assignment,
	worker workercontrol.AuthenticatedWorker,
	workerEpoch int64,
	candidate *workercontrol.AssignmentCandidate,
) bool {
	return candidate != nil && candidate.SchedulerClaim != nil &&
		assignment.JobID != uuid.Nil && assignment.JobID == candidate.JobID &&
		assignment.AttemptID != uuid.Nil &&
		assignment.WorkerID != uuid.Nil && assignment.WorkerID == worker.ID &&
		assignment.WorkerEpoch > 0 && assignment.WorkerEpoch == workerEpoch &&
		assignment.AttemptNumber > 0 && assignment.LeaseFence > 0 &&
		assignment.SchedulerDispatchIntentID != uuid.Nil &&
		assignment.SchedulerDispatchIntentID == candidate.SchedulerClaim.IntentID
}

func readLabAssignmentFaultMarker(path string, output io.Writer) error {
	information, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect lab Assignment fault marker: %w", err)
	}
	if !information.Mode().IsRegular() || information.Mode().Perm() != 0o600 {
		return errors.New("lab Assignment fault marker must be a private regular file")
	}
	if information.Size() < 1 || information.Size() > labAssignmentFaultMarkerMaxBytes {
		return errors.New("lab Assignment fault marker size is outside the supported range")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read lab Assignment fault marker: %w", err)
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return errors.New("lab Assignment fault marker does not have the exact schema")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil || len(fields) != 10 {
		return errors.New("lab Assignment fault marker does not have the exact schema")
	}
	for _, field := range []string{
		"schema", "phase", "job_id", "attempt_id", "worker_id", "worker_epoch",
		"attempt_number", "lease_fence", "scheduler_dispatch_intent_id", "assignment_committed",
	} {
		if _, present := fields[field]; !present {
			return errors.New("lab Assignment fault marker does not have the exact schema")
		}
	}
	var marker labAssignmentFaultMarker
	if err := json.Unmarshal(encoded, &marker); err != nil {
		return fmt.Errorf("decode lab Assignment fault marker: %w", err)
	}
	if !validLabAssignmentFaultMarker(marker) {
		return errors.New("lab Assignment fault marker identity is invalid")
	}
	if _, err := output.Write(encoded); err != nil {
		return fmt.Errorf("write lab Assignment fault marker: %w", err)
	}
	return nil
}

func validLabAssignmentFaultMarker(marker labAssignmentFaultMarker) bool {
	jobID, jobErr := uuid.Parse(marker.JobID)
	attemptID, attemptErr := uuid.Parse(marker.AttemptID)
	workerID, workerErr := uuid.Parse(marker.WorkerID)
	intentID, intentErr := uuid.Parse(marker.SchedulerDispatchIntentID)
	return marker.Schema == labAssignmentFaultMarkerSchema &&
		marker.Phase == assignmentPostCommitPreResponse &&
		jobErr == nil && jobID != uuid.Nil &&
		attemptErr == nil && attemptID != uuid.Nil &&
		workerErr == nil && workerID != uuid.Nil &&
		intentErr == nil && intentID != uuid.Nil &&
		marker.WorkerEpoch > 0 && marker.AttemptNumber > 0 && marker.LeaseFence > 0 &&
		marker.AssignmentCommitted
}

func writeLabAssignmentFaultMarker(path string, marker labAssignmentFaultMarker) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create lab Assignment fault marker directory: %w", err)
	}
	encoded, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode lab Assignment fault marker: %w", err)
	}
	encoded = append(encoded, '\n')
	temporary, err := os.CreateTemp(directory, ".marker-*")
	if err != nil {
		return fmt.Errorf("create temporary lab Assignment fault marker: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("protect temporary lab Assignment fault marker: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		return fmt.Errorf("write temporary lab Assignment fault marker: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync temporary lab Assignment fault marker: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary lab Assignment fault marker: %w", err)
	}
	if err := os.Link(temporaryPath, path); err != nil {
		return fmt.Errorf("publish lab Assignment fault marker: %w", err)
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open lab Assignment fault marker directory: %w", err)
	}
	defer directoryHandle.Close()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("sync lab Assignment fault marker directory: %w", err)
	}
	return nil
}

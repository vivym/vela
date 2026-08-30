//go:build vela_lab_fault_injection

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/strictjson"
	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workertransport"
)

const (
	labVisibleCompletionFaultWait           = 5 * time.Minute
	labVisibleCompletionFaultMarkerMaxBytes = 16 * 1024
)

type labVisibleCompletionFaultCoordinator struct {
	workertransport.WorkerCoordinator
	targetWorker uuid.UUID
	markerPath   string
	wait         time.Duration
}

type labVisibleCompletionFaultMarker struct {
	Schema               string   `json:"schema"`
	Phase                string   `json:"phase"`
	WorkerID             string   `json:"worker_id"`
	AttemptID            string   `json:"attempt_id"`
	WorkerEpoch          int64    `json:"worker_epoch"`
	LeaseFence           int64    `json:"lease_fence"`
	CompletionID         string   `json:"completion_id"`
	ExpectedJobVersion   int64    `json:"expected_job_version"`
	ArtifactIDs          []string `json:"artifact_ids"`
	CandidateSHA256      string   `json:"candidate_sha256"`
	BlockedBeforeService bool     `json:"blocked_before_service"`
}

type labVisibleCompletionCandidatePayload struct {
	CompletionID       string   `json:"completion_id"`
	ExpectedJobVersion int64    `json:"expected_job_version"`
	ArtifactIDs        []string `json:"artifact_ids"`
}

func runLabVisibleCompletionFaultCommand(args []string, output io.Writer) (bool, error) {
	if len(args) == 0 || args[0] != labVisibleCompletionFaultMarkerArg {
		return false, nil
	}
	if len(args) != 1 {
		return true, errors.New(
			"lab Visible Completion fault marker command accepts no additional arguments",
		)
	}
	return true, readLabVisibleCompletionFaultMarker(labVisibleCompletionFaultMarkerPath, output)
}

func configureWorkerTransportCoordinator(
	delegate workertransport.WorkerCoordinator,
) (workertransport.WorkerCoordinator, error) {
	phase, phaseConfigured := os.LookupEnv(labVisibleCompletionFaultPhaseEnv)
	worker, workerConfigured := os.LookupEnv(labVisibleCompletionFaultWorkerEnv)
	if !phaseConfigured && !workerConfigured {
		return delegate, nil
	}
	if !phaseConfigured || !workerConfigured || phase != labVisibleCompletionPreCoordinator {
		return nil, errors.New("lab Visible Completion fault configuration is incomplete or unsupported")
	}
	workerID, err := uuid.Parse(worker)
	if err != nil || workerID == uuid.Nil || workerID.String() != worker {
		return nil, errors.New("lab Visible Completion fault Worker ID is invalid")
	}
	return newLabVisibleCompletionFaultCoordinator(
		delegate,
		workerID,
		labVisibleCompletionFaultMarkerPath,
		labVisibleCompletionFaultWait,
	)
}

func newLabVisibleCompletionFaultCoordinator(
	delegate workertransport.WorkerCoordinator,
	targetWorker uuid.UUID,
	markerPath string,
	wait time.Duration,
) (workertransport.WorkerCoordinator, error) {
	if delegate == nil {
		return nil, errors.New("lab Visible Completion fault coordinator delegate is required")
	}
	if targetWorker == uuid.Nil {
		return nil, errors.New("lab Visible Completion fault target Worker is required")
	}
	if markerPath == "" || filepath.Base(markerPath) == "." {
		return nil, errors.New("lab Visible Completion fault marker path is required")
	}
	if wait <= 0 {
		return nil, errors.New("lab Visible Completion fault wait must be positive")
	}
	return &labVisibleCompletionFaultCoordinator{
		WorkerCoordinator: delegate,
		targetWorker:      targetWorker,
		markerPath:        markerPath,
		wait:              wait,
	}, nil
}

func (coordinator *labVisibleCompletionFaultCoordinator) CompleteVisibleCompletion(
	ctx context.Context,
	worker workercontrol.AuthenticatedWorker,
	credentials workercontrol.LeaseCredentials,
	candidate workercontrol.VisibleCompletionCandidate,
) (workercontrol.VisibleCompletionResult, error) {
	if worker.ID != coordinator.targetWorker {
		return coordinator.WorkerCoordinator.CompleteVisibleCompletion(
			ctx, worker, credentials, candidate,
		)
	}
	marker, err := newLabVisibleCompletionFaultMarker(worker, credentials, candidate)
	if err != nil {
		return workercontrol.VisibleCompletionResult{}, err
	}
	prior, err := loadLabVisibleCompletionFaultMarker(coordinator.markerPath)
	if errors.Is(err, os.ErrNotExist) {
		if err := writeLabVisibleCompletionFaultMarker(coordinator.markerPath, marker); err != nil {
			return workercontrol.VisibleCompletionResult{}, err
		}
	} else if err != nil {
		return workercontrol.VisibleCompletionResult{}, err
	} else if !sameLabVisibleCompletionFaultMarker(prior, marker) {
		return workercontrol.VisibleCompletionResult{}, errors.New(
			"lab Visible Completion fault replay changed the exact candidate authority",
		)
	}

	timer := time.NewTimer(coordinator.wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return workercontrol.VisibleCompletionResult{}, ctx.Err()
	case <-timer.C:
		return workercontrol.VisibleCompletionResult{}, errors.New(
			"timed out waiting for lab Visible Completion fault process termination",
		)
	}
}

func newLabVisibleCompletionFaultMarker(
	worker workercontrol.AuthenticatedWorker,
	credentials workercontrol.LeaseCredentials,
	candidate workercontrol.VisibleCompletionCandidate,
) (labVisibleCompletionFaultMarker, error) {
	if worker.ID == uuid.Nil || credentials.AttemptID == uuid.Nil || credentials.WorkerEpoch <= 0 ||
		credentials.Fence <= 0 || credentials.Token == "" || candidate.CompletionID == uuid.Nil ||
		candidate.ExpectedJobVersion <= 0 || len(candidate.ArtifactIDs) == 0 ||
		len(candidate.ArtifactIDs) > 32 {
		return labVisibleCompletionFaultMarker{}, errors.New(
			"lab Visible Completion fault target authority is invalid",
		)
	}
	artifactIDs := make([]string, len(candidate.ArtifactIDs))
	seen := make(map[uuid.UUID]struct{}, len(candidate.ArtifactIDs))
	for index, artifactID := range candidate.ArtifactIDs {
		if artifactID == uuid.Nil {
			return labVisibleCompletionFaultMarker{}, errors.New(
				"lab Visible Completion fault candidate Artifact ID is invalid",
			)
		}
		if _, exists := seen[artifactID]; exists {
			return labVisibleCompletionFaultMarker{}, errors.New(
				"lab Visible Completion fault candidate contains a duplicate Artifact",
			)
		}
		seen[artifactID] = struct{}{}
		artifactIDs[index] = artifactID.String()
	}
	payload := labVisibleCompletionCandidatePayload{
		CompletionID:       candidate.CompletionID.String(),
		ExpectedJobVersion: candidate.ExpectedJobVersion,
		ArtifactIDs:        artifactIDs,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return labVisibleCompletionFaultMarker{}, fmt.Errorf(
			"encode lab Visible Completion fault candidate: %w", err,
		)
	}
	digest := sha256.Sum256(encoded)
	return labVisibleCompletionFaultMarker{
		Schema:               "vela-lab-visible-completion-fault-marker-v1",
		Phase:                labVisibleCompletionPreCoordinator,
		WorkerID:             worker.ID.String(),
		AttemptID:            credentials.AttemptID.String(),
		WorkerEpoch:          credentials.WorkerEpoch,
		LeaseFence:           credentials.Fence,
		CompletionID:         candidate.CompletionID.String(),
		ExpectedJobVersion:   candidate.ExpectedJobVersion,
		ArtifactIDs:          artifactIDs,
		CandidateSHA256:      hex.EncodeToString(digest[:]),
		BlockedBeforeService: true,
	}, nil
}

func sameLabVisibleCompletionFaultMarker(
	left labVisibleCompletionFaultMarker,
	right labVisibleCompletionFaultMarker,
) bool {
	leftEncoded, leftErr := json.Marshal(left)
	rightEncoded, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftEncoded) == string(rightEncoded)
}

func readLabVisibleCompletionFaultMarker(path string, output io.Writer) error {
	marker, err := loadLabVisibleCompletionFaultMarker(path)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode lab Visible Completion fault marker: %w", err)
	}
	encoded = append(encoded, '\n')
	if _, err := output.Write(encoded); err != nil {
		return fmt.Errorf("write lab Visible Completion fault marker: %w", err)
	}
	return nil
}

func loadLabVisibleCompletionFaultMarker(path string) (labVisibleCompletionFaultMarker, error) {
	information, err := os.Lstat(path)
	if err != nil {
		return labVisibleCompletionFaultMarker{}, err
	}
	if !information.Mode().IsRegular() || information.Mode().Perm() != 0o600 ||
		information.Size() < 1 || information.Size() > labVisibleCompletionFaultMarkerMaxBytes {
		return labVisibleCompletionFaultMarker{}, errors.New(
			"lab Visible Completion fault marker is not a private bounded regular file",
		)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return labVisibleCompletionFaultMarker{}, fmt.Errorf(
			"read lab Visible Completion fault marker: %w", err,
		)
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return labVisibleCompletionFaultMarker{}, errors.New(
			"lab Visible Completion fault marker does not have the exact schema",
		)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil || len(fields) != 11 {
		return labVisibleCompletionFaultMarker{}, errors.New(
			"lab Visible Completion fault marker does not have the exact schema",
		)
	}
	for _, field := range []string{
		"schema", "phase", "worker_id", "attempt_id", "worker_epoch", "lease_fence",
		"completion_id", "expected_job_version", "artifact_ids", "candidate_sha256",
		"blocked_before_service",
	} {
		if _, present := fields[field]; !present {
			return labVisibleCompletionFaultMarker{}, errors.New(
				"lab Visible Completion fault marker does not have the exact schema",
			)
		}
	}
	var marker labVisibleCompletionFaultMarker
	if err := json.Unmarshal(encoded, &marker); err != nil {
		return labVisibleCompletionFaultMarker{}, fmt.Errorf(
			"decode lab Visible Completion fault marker: %w", err,
		)
	}
	if !validLabVisibleCompletionFaultMarker(marker) {
		return labVisibleCompletionFaultMarker{}, errors.New(
			"lab Visible Completion fault marker identity is invalid",
		)
	}
	return marker, nil
}

func validLabVisibleCompletionFaultMarker(marker labVisibleCompletionFaultMarker) bool {
	workerID, workerErr := uuid.Parse(marker.WorkerID)
	attemptID, attemptErr := uuid.Parse(marker.AttemptID)
	completionID, completionErr := uuid.Parse(marker.CompletionID)
	if marker.Schema != "vela-lab-visible-completion-fault-marker-v1" ||
		marker.Phase != labVisibleCompletionPreCoordinator || workerErr != nil || workerID == uuid.Nil ||
		attemptErr != nil || attemptID == uuid.Nil || completionErr != nil || completionID == uuid.Nil ||
		marker.WorkerEpoch <= 0 || marker.LeaseFence <= 0 || marker.ExpectedJobVersion <= 0 ||
		len(marker.ArtifactIDs) == 0 || len(marker.ArtifactIDs) > 32 ||
		len(marker.CandidateSHA256) != sha256.Size*2 || !marker.BlockedBeforeService {
		return false
	}
	if _, err := hex.DecodeString(marker.CandidateSHA256); err != nil {
		return false
	}
	artifactIDs := make([]uuid.UUID, len(marker.ArtifactIDs))
	for index, encoded := range marker.ArtifactIDs {
		artifactID, err := uuid.Parse(encoded)
		if err != nil || artifactID == uuid.Nil || artifactID.String() != encoded {
			return false
		}
		artifactIDs[index] = artifactID
	}
	rebuilt, err := newLabVisibleCompletionFaultMarker(
		workercontrol.AuthenticatedWorker{ID: workerID},
		workercontrol.LeaseCredentials{
			AttemptID: attemptID, WorkerEpoch: marker.WorkerEpoch,
			Fence: marker.LeaseFence, Token: "not-recorded",
		},
		workercontrol.VisibleCompletionCandidate{
			CompletionID: completionID, ExpectedJobVersion: marker.ExpectedJobVersion,
			ArtifactIDs: artifactIDs,
		},
	)
	return err == nil && sameLabVisibleCompletionFaultMarker(marker, rebuilt)
}

func writeLabVisibleCompletionFaultMarker(
	path string,
	marker labVisibleCompletionFaultMarker,
) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create lab Visible Completion fault marker directory: %w", err)
	}
	encoded, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode lab Visible Completion fault marker: %w", err)
	}
	encoded = append(encoded, '\n')
	temporary, err := os.CreateTemp(directory, ".marker-*")
	if err != nil {
		return fmt.Errorf("create temporary lab Visible Completion fault marker: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("protect temporary lab Visible Completion fault marker: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		return fmt.Errorf("write temporary lab Visible Completion fault marker: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync temporary lab Visible Completion fault marker: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary lab Visible Completion fault marker: %w", err)
	}
	if err := os.Link(temporaryPath, path); err != nil {
		return fmt.Errorf("publish lab Visible Completion fault marker: %w", err)
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open lab Visible Completion fault marker directory: %w", err)
	}
	defer directoryHandle.Close()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("sync lab Visible Completion fault marker directory: %w", err)
	}
	return nil
}

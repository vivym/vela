//go:build vela_lab_fault_injection

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/strictjson"
	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workertransport"
)

const (
	labStaleCompletionRecoveryRoot  = "/recovery"
	labStaleCompletionSignalPath    = "/signal/go.json"
	labStaleCompletionControl       = "vela-lab-control.vela-lab.svc:8443"
	labStaleCompletionServerName    = "vela-lab-control.vela-lab.svc"
	labStaleCompletionTLSCert       = "/tls/tls.crt"
	labStaleCompletionTLSKey        = "/tls/tls.key"
	labStaleCompletionTLSCA         = "/tls/ca.crt"
	labStaleCompletionWait          = 12 * time.Minute
	labStaleCompletionAuthorityPoll = time.Millisecond
	labStaleCompletionMaxBytes      = 1 << 20
	labStaleCompletionLoadedStatus  = "schema=vela-lab-stale-completion-probe-v1 phase=AUTHORITY_LOADED production_gates=0/9\n"
)

type labPendingControlOperation struct {
	AttemptID           uuid.UUID                                 `json:"attempt_id"`
	JobID               uuid.UUID                                 `json:"job_id"`
	WorkerID            uuid.UUID                                 `json:"worker_id"`
	WorkerEpoch         int64                                     `json:"worker_epoch"`
	LeaseFence          int64                                     `json:"lease_fence"`
	LeaseToken          string                                    `json:"lease_token"`
	CompletionID        uuid.UUID                                 `json:"completion_id"`
	HeartbeatSequence   int64                                     `json:"heartbeat_sequence"`
	GPUHealthSummary    json.RawMessage                           `json:"gpu_health_summary"`
	JobVersion          int64                                     `json:"job_version"`
	Outputs             []json.RawMessage                         `json:"outputs"`
	Artifacts           []labStaleFinalizationArtifact            `json:"artifacts"`
	CompletionCandidate *workercontrol.VisibleCompletionCandidate `json:"completion_candidate"`
	VisibleCompletion   json.RawMessage                           `json:"visible_completion,omitempty"`
}

type labStaleFinalizationArtifact struct {
	ArtifactID     uuid.UUID                  `json:"artifact_id"`
	UploadID       uuid.UUID                  `json:"upload_id"`
	Kind           workercontrol.ArtifactKind `json:"kind"`
	Ordinal        int32                      `json:"ordinal"`
	ObjectKey      string                     `json:"object_key"`
	ClaimID        uuid.UUID                  `json:"claim_id"`
	VerificationID uuid.UUID                  `json:"verification_id"`
}

type labStaleCompletionSignal struct {
	Schema                string    `json:"schema"`
	JobID                 uuid.UUID `json:"job_id"`
	OriginalAttemptID     uuid.UUID `json:"original_attempt_id"`
	OriginalFence         int64     `json:"original_fence"`
	ReplacementAttemptID  uuid.UUID `json:"replacement_attempt_id"`
	ReplacementFence      int64     `json:"replacement_fence"`
	OriginalJobVersion    int64     `json:"original_job_version"`
	ReplacementJobVersion int64     `json:"replacement_job_version"`
}

type labStaleCompletionReceipt struct {
	Schema                string   `json:"schema"`
	JobID                 string   `json:"job_id"`
	OriginalAttemptID     string   `json:"original_attempt_id"`
	OriginalWorkerID      string   `json:"original_worker_id"`
	OriginalWorkerEpoch   int64    `json:"original_worker_epoch"`
	OriginalFence         int64    `json:"original_fence"`
	ReplacementAttemptID  string   `json:"replacement_attempt_id"`
	ReplacementFence      int64    `json:"replacement_fence"`
	OriginalJobVersion    int64    `json:"original_job_version"`
	ReplacementJobVersion int64    `json:"replacement_job_version"`
	CompletionID          string   `json:"completion_id"`
	ArtifactIDs           []string `json:"artifact_ids"`
	CandidateSHA256       string   `json:"candidate_sha256"`
	AuthorityFileSHA256   string   `json:"authority_file_sha256"`
	BinarySHA256          string   `json:"binary_sha256"`
	Decision              string   `json:"decision"`
	ProductionGates       string   `json:"production_gates"`
}

type labStaleFinalizationReadiness int

const (
	labStaleFinalizationInvalid labStaleFinalizationReadiness = iota
	labStaleFinalizationPending
	labStaleFinalizationReady
)

func runLabStaleCompletionProbeCommand(args []string, output io.Writer) (bool, error) {
	if len(args) == 0 || args[0] != labStaleCompletionProbeArg {
		return false, nil
	}
	if len(args) == 2 && args[1] == "--validate-only" {
		binarySHA256, err := labStaleCompletionBinarySHA256()
		if err != nil {
			return true, err
		}
		_, err = fmt.Fprintf(
			output,
			"schema=vela-lab-stale-completion-probe-v1 validation=PASS binary_sha256=%s production_gates=0/9\n",
			binarySHA256,
		)
		return true, err
	}
	if len(args) != 6 {
		return true, errors.New(
			"lab stale Completion probe requires Job, Attempt, Worker, epoch, and fence arguments",
		)
	}
	jobID, err := uuid.Parse(args[1])
	if err != nil || jobID == uuid.Nil || jobID.String() != args[1] {
		return true, errors.New("lab stale Completion probe Job ID is invalid")
	}
	attemptID, err := uuid.Parse(args[2])
	if err != nil || attemptID == uuid.Nil || attemptID.String() != args[2] {
		return true, errors.New("lab stale Completion probe Attempt ID is invalid")
	}
	workerID, err := uuid.Parse(args[3])
	if err != nil || workerID == uuid.Nil || workerID.String() != args[3] {
		return true, errors.New("lab stale Completion probe Worker ID is invalid")
	}
	workerEpoch, err := strconv.ParseInt(args[4], 10, 64)
	if err != nil || workerEpoch <= 0 || strconv.FormatInt(workerEpoch, 10) != args[4] {
		return true, errors.New("lab stale Completion probe Worker epoch is invalid")
	}
	fence, err := strconv.ParseInt(args[5], 10, 64)
	if err != nil || fence <= 0 || strconv.FormatInt(fence, 10) != args[5] {
		return true, errors.New("lab stale Completion probe fence is invalid")
	}

	authorityPath := filepath.Join(
		labStaleCompletionRecoveryRoot,
		"attempts",
		attemptID.String(),
		"upload--finalization.json",
	)
	deadline := time.Now().Add(labStaleCompletionWait)
	state, authorityBytes, err := waitForLabStaleCompletionAuthority(
		authorityPath, jobID, attemptID, workerID, workerEpoch, fence, deadline,
	)
	if err != nil {
		return true, err
	}
	if _, err := io.WriteString(
		output,
		labStaleCompletionLoadedStatus,
	); err != nil {
		return true, err
	}
	signal, err := waitForLabStaleCompletionSignal(
		labStaleCompletionSignalPath, state, deadline,
	)
	if err != nil {
		return true, err
	}

	transportCredentials, err := workertransport.NewClientTLSCredentials(
		labStaleCompletionTLSCert,
		labStaleCompletionTLSKey,
		labStaleCompletionTLSCA,
		labStaleCompletionServerName,
	)
	if err != nil {
		return true, fmt.Errorf("configure stale Completion probe mTLS: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := workertransport.DialClient(ctx, labStaleCompletionControl, transportCredentials)
	if err != nil {
		return true, err
	}
	defer func() { _ = client.Close() }()
	candidate := *state.CompletionCandidate
	result, err := client.CompleteVisibleCompletion(
		ctx,
		workercontrol.LeaseCredentials{
			AttemptID:   state.AttemptID,
			WorkerEpoch: state.WorkerEpoch,
			Fence:       state.LeaseFence,
			Token:       state.LeaseToken,
		},
		candidate,
	)
	if err != nil {
		return true, err
	}
	if result.Decision != workercontrol.VisibleCompletionRejectedStaleLease {
		return true, fmt.Errorf(
			"stale Completion decision = %s, want %s",
			result.Decision,
			workercontrol.VisibleCompletionRejectedStaleLease,
		)
	}
	authorityDigest := sha256.Sum256(authorityBytes)
	binarySHA256, err := labStaleCompletionBinarySHA256()
	if err != nil {
		return true, err
	}
	artifactIDs := make([]string, len(candidate.ArtifactIDs))
	for index, artifactID := range candidate.ArtifactIDs {
		artifactIDs[index] = artifactID.String()
	}
	candidateSHA256, err := labStaleCompletionCandidateSHA256(candidate)
	if err != nil {
		return true, err
	}
	receipt := labStaleCompletionReceipt{
		Schema:                "vela-lab-stale-completion-probe-v1",
		JobID:                 jobID.String(),
		OriginalAttemptID:     attemptID.String(),
		OriginalWorkerID:      workerID.String(),
		OriginalWorkerEpoch:   workerEpoch,
		OriginalFence:         fence,
		ReplacementAttemptID:  signal.ReplacementAttemptID.String(),
		ReplacementFence:      signal.ReplacementFence,
		OriginalJobVersion:    signal.OriginalJobVersion,
		ReplacementJobVersion: signal.ReplacementJobVersion,
		CompletionID:          candidate.CompletionID.String(),
		ArtifactIDs:           artifactIDs,
		CandidateSHA256:       candidateSHA256,
		AuthorityFileSHA256:   hex.EncodeToString(authorityDigest[:]),
		BinarySHA256:          binarySHA256,
		Decision:              string(result.Decision),
		ProductionGates:       "0/9",
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return true, err
	}
	encoded = append(encoded, '\n')
	_, err = output.Write(encoded)
	return true, err
}

func waitForLabStaleCompletionAuthority(
	path string,
	jobID uuid.UUID,
	attemptID uuid.UUID,
	workerID uuid.UUID,
	workerEpoch int64,
	fence int64,
	deadline time.Time,
) (labPendingControlOperation, []byte, error) {
	for time.Now().Before(deadline) {
		state, encoded, err := readLabStaleCompletionAuthority(path)
		if err == nil {
			switch labStaleFinalizationStateReadiness(
				state, jobID, attemptID, workerID, workerEpoch, fence,
			) {
			case labStaleFinalizationReady:
				return state, encoded, nil
			case labStaleFinalizationPending:
				time.Sleep(labStaleCompletionAuthorityPoll)
				continue
			default:
				return labPendingControlOperation{}, nil,
					errors.New("lab stale Completion authority identity is invalid")
			}
		}
		if !errors.Is(err, os.ErrNotExist) {
			return labPendingControlOperation{}, nil, err
		}
		// The recovery record exists only while one control RPC is in flight.
		time.Sleep(labStaleCompletionAuthorityPoll)
	}
	return labPendingControlOperation{}, nil,
		errors.New("timed out waiting for lab stale Completion authority")
}

func readLabStaleCompletionAuthority(path string) (labPendingControlOperation, []byte, error) {
	encoded, err := readLabStaleCompletionFile(path, 0o600)
	if err != nil {
		return labPendingControlOperation{}, nil, err
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return labPendingControlOperation{}, nil,
			errors.New("lab stale Completion authority contains duplicate keys")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var state labPendingControlOperation
	if err := decoder.Decode(&state); err != nil {
		return labPendingControlOperation{}, nil,
			fmt.Errorf("decode lab stale Completion authority: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return labPendingControlOperation{}, nil,
			errors.New("lab stale Completion authority must contain one document")
	}
	return state, encoded, nil
}

func validLabStaleFinalizationState(
	state labPendingControlOperation,
	jobID uuid.UUID,
	attemptID uuid.UUID,
	workerID uuid.UUID,
	workerEpoch int64,
	fence int64,
) bool {
	return labStaleFinalizationStateReadiness(
		state, jobID, attemptID, workerID, workerEpoch, fence,
	) == labStaleFinalizationReady
}

func labStaleFinalizationStateReadiness(
	state labPendingControlOperation,
	jobID uuid.UUID,
	attemptID uuid.UUID,
	workerID uuid.UUID,
	workerEpoch int64,
	fence int64,
) labStaleFinalizationReadiness {
	if state.JobID != jobID || state.AttemptID != attemptID || state.WorkerID != workerID ||
		state.WorkerEpoch != workerEpoch || state.LeaseFence != fence || state.LeaseToken == "" ||
		state.CompletionID == uuid.Nil || state.HeartbeatSequence <= 0 ||
		len(state.GPUHealthSummary) == 0 || !json.Valid(state.GPUHealthSummary) ||
		len(state.Outputs) == 0 || len(state.Outputs) > 32 ||
		state.JobVersion < 0 || len(state.Artifacts) > 32 || len(state.VisibleCompletion) != 0 {
		return labStaleFinalizationInvalid
	}
	if state.JobVersion == 0 {
		if len(state.Artifacts) != 0 || state.CompletionCandidate != nil {
			return labStaleFinalizationInvalid
		}
		return labStaleFinalizationPending
	}
	if len(state.Artifacts) == 0 {
		return labStaleFinalizationInvalid
	}
	planned := make(map[uuid.UUID]struct{}, len(state.Artifacts))
	for _, artifact := range state.Artifacts {
		if artifact.ArtifactID == uuid.Nil || artifact.UploadID == uuid.Nil ||
			artifact.ObjectKey == "" || artifact.ClaimID == uuid.Nil ||
			artifact.VerificationID == uuid.Nil ||
			(artifact.Kind != workercontrol.ArtifactKindVideo &&
				artifact.Kind != workercontrol.ArtifactKindThumbnail) {
			return labStaleFinalizationInvalid
		}
		if _, exists := planned[artifact.ArtifactID]; exists {
			return labStaleFinalizationInvalid
		}
		planned[artifact.ArtifactID] = struct{}{}
	}
	if state.CompletionCandidate == nil {
		return labStaleFinalizationPending
	}
	candidate := *state.CompletionCandidate
	if candidate.CompletionID != state.CompletionID ||
		candidate.ExpectedJobVersion != state.JobVersion ||
		len(candidate.ArtifactIDs) != len(state.Artifacts) {
		return labStaleFinalizationInvalid
	}
	for _, artifactID := range candidate.ArtifactIDs {
		if _, exists := planned[artifactID]; !exists {
			return labStaleFinalizationInvalid
		}
		delete(planned, artifactID)
	}
	if len(planned) != 0 {
		return labStaleFinalizationInvalid
	}
	return labStaleFinalizationReady
}

func waitForLabStaleCompletionSignal(
	path string,
	state labPendingControlOperation,
	deadline time.Time,
) (labStaleCompletionSignal, error) {
	for time.Now().Before(deadline) {
		signal, err := readLabStaleCompletionSignal(path)
		if err == nil {
			if signal.Schema != "vela-lab-stale-completion-signal-v1" ||
				signal.JobID != state.JobID || signal.OriginalAttemptID != state.AttemptID ||
				signal.OriginalFence != state.LeaseFence ||
				signal.OriginalJobVersion != state.JobVersion ||
				signal.ReplacementAttemptID == uuid.Nil ||
				signal.ReplacementAttemptID == state.AttemptID ||
				signal.ReplacementFence <= state.LeaseFence ||
				signal.ReplacementJobVersion <= signal.OriginalJobVersion {
				return labStaleCompletionSignal{},
					errors.New("lab stale Completion signal identity is invalid")
			}
			return signal, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return labStaleCompletionSignal{}, err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return labStaleCompletionSignal{}, errors.New("timed out waiting for lab stale Completion signal")
}

func readLabStaleCompletionSignal(path string) (labStaleCompletionSignal, error) {
	encoded, err := readLabStaleCompletionProjectedFile(path, 0o444)
	if err != nil {
		return labStaleCompletionSignal{}, err
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return labStaleCompletionSignal{},
			errors.New("lab stale Completion signal contains duplicate keys")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var signal labStaleCompletionSignal
	if err := decoder.Decode(&signal); err != nil {
		return labStaleCompletionSignal{}, fmt.Errorf("decode lab stale Completion signal: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return labStaleCompletionSignal{}, errors.New("lab stale Completion signal must contain one document")
	}
	return signal, nil
}

func labStaleCompletionCandidateSHA256(
	candidate workercontrol.VisibleCompletionCandidate,
) (string, error) {
	artifactIDs := make([]string, len(candidate.ArtifactIDs))
	for index, artifactID := range candidate.ArtifactIDs {
		if artifactID == uuid.Nil {
			return "", errors.New("lab stale Completion candidate Artifact ID is invalid")
		}
		artifactIDs[index] = artifactID.String()
	}
	payload := labVisibleCompletionCandidatePayload{
		CompletionID:       candidate.CompletionID.String(),
		ExpectedJobVersion: candidate.ExpectedJobVersion,
		ArtifactIDs:        artifactIDs,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode lab stale Completion candidate: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func labStaleCompletionBinarySHA256() (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve stale Completion probe executable: %w", err)
	}
	file, err := os.Open(executable)
	if err != nil {
		return "", fmt.Errorf("open stale Completion probe executable: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, io.LimitReader(file, 256<<20)); err != nil {
		return "", fmt.Errorf("hash stale Completion probe executable: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func readLabStaleCompletionProjectedFile(path string, mode os.FileMode) ([]byte, error) {
	information, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if information.Mode()&os.ModeSymlink == 0 {
		return readLabStaleCompletionFile(path, mode)
	}

	root := filepath.Dir(path)
	name := filepath.Base(path)
	rootInformation, err := os.Lstat(root)
	if err != nil || !rootInformation.IsDir() || rootInformation.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("lab stale Completion file is unsafe")
	}
	link, err := os.Readlink(path)
	if err != nil || link != filepath.Join("..data", name) {
		return nil, errors.New("lab stale Completion file is unsafe")
	}

	dataLink := filepath.Join(root, "..data")
	dataInformation, err := os.Lstat(dataLink)
	if err != nil || dataInformation.Mode()&os.ModeSymlink == 0 {
		return nil, errors.New("lab stale Completion file is unsafe")
	}
	version, err := os.Readlink(dataLink)
	if err != nil || filepath.IsAbs(version) || filepath.Base(version) != version ||
		len(version) <= 2 || version[:2] != ".." || version == "..data" {
		return nil, errors.New("lab stale Completion file is unsafe")
	}
	versionRoot := filepath.Join(root, version)
	versionInformation, err := os.Lstat(versionRoot)
	if err != nil || !versionInformation.IsDir() || versionInformation.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("lab stale Completion file is unsafe")
	}

	projectedPath := filepath.Join(versionRoot, name)
	projectedInformation, err := os.Lstat(projectedPath)
	if err != nil || !projectedInformation.Mode().IsRegular() ||
		projectedInformation.Mode()&os.ModeSymlink != 0 ||
		projectedInformation.Mode().Perm() != mode || projectedInformation.Size() < 1 ||
		projectedInformation.Size() > labStaleCompletionMaxBytes {
		return nil, errors.New("lab stale Completion file is unsafe")
	}
	file, err := os.Open(projectedPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	openedInformation, err := file.Stat()
	if err != nil || !os.SameFile(projectedInformation, openedInformation) {
		return nil, errors.New("lab stale Completion file is unsafe")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, labStaleCompletionMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(encoded)) != projectedInformation.Size() {
		return nil, errors.New("lab stale Completion file is unsafe")
	}
	return encoded, nil
}

func readLabStaleCompletionFile(path string, mode os.FileMode) ([]byte, error) {
	information, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !information.Mode().IsRegular() || information.Mode()&os.ModeSymlink != 0 ||
		information.Mode().Perm() != mode || information.Size() < 1 ||
		information.Size() > labStaleCompletionMaxBytes {
		return nil, errors.New("lab stale Completion file is unsafe")
	}
	return os.ReadFile(path)
}

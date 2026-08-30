package workeragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/runnertransport"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workerrecovery"
	"github.com/vivym/vela/internal/workertransport"
	"golang.org/x/sys/unix"
)

const finalizationStateName = "finalization.json"

var (
	errFinalizationAuthorityLost = errors.New("worker finalization authority was lost")
	errLeaseDeadlineElapsed      = errors.New("worker Lease deadline elapsed")
)

type artifactValidationFailure struct {
	artifactID    uuid.UUID
	uploadID      uuid.UUID
	leaseDeadline time.Duration
}

func (failure *artifactValidationFailure) Error() string {
	return "artifact failed certified output validation"
}

type localFinalizationState struct {
	AttemptID           uuid.UUID                                 `json:"attempt_id"`
	JobID               uuid.UUID                                 `json:"job_id"`
	WorkerID            uuid.UUID                                 `json:"worker_id"`
	WorkerEpoch         int64                                     `json:"worker_epoch"`
	LeaseFence          int64                                     `json:"lease_fence"`
	LeaseToken          string                                    `json:"lease_token"`
	CompletionID        uuid.UUID                                 `json:"completion_id"`
	HeartbeatSequence   int64                                     `json:"heartbeat_sequence"`
	GPUHealthSummary    json.RawMessage                           `json:"gpu_health_summary"`
	JobVersion          int64                                     `json:"job_version,omitempty"`
	Outputs             []runnertransport.Output                  `json:"outputs"`
	Artifacts           []localArtifactState                      `json:"artifacts,omitempty"`
	CompletionCandidate *workercontrol.VisibleCompletionCandidate `json:"completion_candidate,omitempty"`
	VisibleCompletion   *workercontrol.VisibleCompletionResult    `json:"visible_completion,omitempty"`
}

type localArtifactState struct {
	ArtifactID     uuid.UUID                  `json:"artifact_id"`
	UploadID       uuid.UUID                  `json:"upload_id"`
	Kind           workercontrol.ArtifactKind `json:"kind"`
	Ordinal        int32                      `json:"ordinal"`
	ObjectKey      string                     `json:"object_key"`
	ClaimID        uuid.UUID                  `json:"claim_id"`
	VerificationID uuid.UUID                  `json:"verification_id"`
}

type securedOutput struct {
	path string
	info os.FileInfo
}

func (agent *Agent) finalizeOutputs(
	ctx context.Context,
	handle *workerrecovery.Handle,
	assignment workercontrol.Assignment,
	credentials workercontrol.LeaseCredentials,
	outputs []runnertransport.Output,
	heartbeatSequence int64,
	gpuHealthSummary json.RawMessage,
	deadline time.Duration,
) (workercontrol.VisibleCompletionResult, func(context.Context) error, error) {
	state, err := loadOrCreateFinalizationState(
		ctx, handle, assignment, outputs, heartbeatSequence, gpuHealthSummary,
	)
	if err != nil {
		return workercontrol.VisibleCompletionResult{}, nil, err
	}
	if state.VisibleCompletion != nil {
		completion := *state.VisibleCompletion
		cleanup := func(cleanupContext context.Context) error {
			return agent.cleanupCommittedOutputs(cleanupContext, assignment.AttemptID, state.Outputs)
		}
		return completion, cleanup, nil
	}
	if state.CompletionCandidate != nil {
		operationContext, cancelOperation, err := agent.finalizationOperationContext(ctx, deadline)
		if err != nil {
			return workercontrol.VisibleCompletionResult{}, nil, err
		}
		completion, cleanup, completionErr := agent.commitVisibleCompletion(
			operationContext, handle, assignment, credentials, state, outputs,
			*state.CompletionCandidate,
		)
		cancelOperation()
		return completion, cleanup, completionErr
	}
	operationContext, cancelOperation, err := agent.finalizationOperationContext(ctx, deadline)
	if err != nil {
		return workercontrol.VisibleCompletionResult{}, nil, err
	}
	plan, err := agent.finalization.BeginFinalization(operationContext, credentials)
	cancelOperation()
	if err != nil {
		if deadline > 0 && ctx.Err() == nil &&
			(errors.Is(operationContext.Err(), context.DeadlineExceeded) ||
				agent.requireLeaseTime(deadline) != nil) {
			return workercontrol.VisibleCompletionResult{}, nil, errLeaseDeadlineElapsed
		}
		return workercontrol.VisibleCompletionResult{}, nil, err
	}
	if plan.Decision == workercontrol.FinalizationRejectedStaleLease {
		return workercontrol.VisibleCompletionResult{}, nil, errFinalizationAuthorityLost
	}
	if err := validateFinalizationPlan(plan, assignment, outputs); err != nil {
		return workercontrol.VisibleCompletionResult{}, nil, err
	}
	if err := bindFinalizationPlan(ctx, handle, &state, plan); err != nil {
		return workercontrol.VisibleCompletionResult{}, nil, err
	}
	heartbeats, err := agent.startFinalizationHeartbeats(
		ctx,
		handle,
		assignment,
		credentials,
		state,
		deadline,
	)
	if err != nil {
		return workercontrol.VisibleCompletionResult{}, nil, err
	}

	outputByIdentity := make(map[string]runnertransport.Output, len(outputs))
	for _, output := range outputs {
		outputByIdentity[outputIdentity(output.Kind, output.Ordinal)] = output
	}
	artifactIDs := make([]uuid.UUID, 0, len(state.Artifacts))
	for _, artifact := range state.Artifacts {
		output := outputByIdentity[outputIdentity(string(artifact.Kind), artifact.Ordinal)]
		_, err := agent.finalizeArtifact(heartbeats.Context(), credentials, artifact, output)
		if err != nil {
			stopErr := heartbeats.Stop()
			var validationFailure *artifactValidationFailure
			if stopErr == nil && errors.As(err, &validationFailure) {
				validationFailure.leaseDeadline, stopErr = heartbeats.LeaseDeadline()
			}
			return workercontrol.VisibleCompletionResult{}, nil, errors.Join(err, stopErr)
		}
		artifactIDs = append(artifactIDs, artifact.ArtifactID)
	}
	candidate := workercontrol.VisibleCompletionCandidate{
		CompletionID: state.CompletionID, ExpectedJobVersion: plan.JobVersion,
		ArtifactIDs: artifactIDs,
	}
	state, err = heartbeats.persistCompletionCandidate(heartbeats.Context(), candidate)
	if err != nil {
		return workercontrol.VisibleCompletionResult{}, nil, errors.Join(err, heartbeats.Stop())
	}
	completion, cleanup, err := agent.requestVisibleCompletion(
		heartbeats.Context(),
		assignment,
		credentials,
		state,
		outputs,
		candidate,
	)
	if err != nil {
		return workercontrol.VisibleCompletionResult{}, nil, errors.Join(err, heartbeats.Stop())
	}
	if err := heartbeats.Stop(); err != nil {
		return workercontrol.VisibleCompletionResult{}, nil, err
	}
	persistContext, cancelPersist := boundedCleanupContext(ctx)
	defer cancelPersist()
	state.VisibleCompletion = &completion
	if err := writeFinalizationState(persistContext, handle, state); err != nil {
		return workercontrol.VisibleCompletionResult{}, nil, err
	}
	return completion, cleanup, nil
}

func (agent *Agent) commitVisibleCompletion(
	ctx context.Context,
	handle *workerrecovery.Handle,
	assignment workercontrol.Assignment,
	credentials workercontrol.LeaseCredentials,
	state localFinalizationState,
	outputs []runnertransport.Output,
	candidate workercontrol.VisibleCompletionCandidate,
) (workercontrol.VisibleCompletionResult, func(context.Context) error, error) {
	completion, cleanup, err := agent.requestVisibleCompletion(
		ctx, assignment, credentials, state, outputs, candidate,
	)
	if err != nil {
		return workercontrol.VisibleCompletionResult{}, nil, err
	}
	state.VisibleCompletion = &completion
	if err := writeFinalizationState(ctx, handle, state); err != nil {
		return workercontrol.VisibleCompletionResult{}, nil, err
	}
	return completion, cleanup, nil
}

func (agent *Agent) requestVisibleCompletion(
	ctx context.Context,
	assignment workercontrol.Assignment,
	credentials workercontrol.LeaseCredentials,
	state localFinalizationState,
	outputs []runnertransport.Output,
	candidate workercontrol.VisibleCompletionCandidate,
) (workercontrol.VisibleCompletionResult, func(context.Context) error, error) {
	completion, err := agent.finalization.CompleteVisibleCompletion(ctx, credentials, candidate)
	if err != nil {
		return workercontrol.VisibleCompletionResult{}, nil, err
	}
	switch completion.Decision {
	case workercontrol.VisibleCompletionRejectedStaleLease,
		workercontrol.VisibleCompletionCancellationWon,
		workercontrol.VisibleCompletionAlreadyFailed,
		workercontrol.VisibleCompletionCandidateConflict:
		return workercontrol.VisibleCompletionResult{}, nil, errFinalizationAuthorityLost
	case workercontrol.VisibleCompletionCommitted, workercontrol.VisibleCompletionAlreadySucceeded:
	default:
		return workercontrol.VisibleCompletionResult{}, nil,
			fmt.Errorf("visible Completion did not commit: %s", completion.Decision)
	}
	plan := workercontrol.FinalizationPlan{JobVersion: state.JobVersion}
	if err := validateVisibleCompletion(completion, state, plan, outputs); err != nil {
		return workercontrol.VisibleCompletionResult{}, nil, err
	}
	cleanup := func(cleanupContext context.Context) error {
		return agent.cleanupCommittedOutputs(cleanupContext, assignment.AttemptID, state.Outputs)
	}
	return completion, cleanup, nil
}

func (agent *Agent) finalizationOperationContext(
	ctx context.Context,
	deadline time.Duration,
) (context.Context, context.CancelFunc, error) {
	if deadline > 0 {
		return agent.leaseOperationContext(ctx, deadline)
	}
	if ctx == nil {
		return nil, nil, errors.New("worker finalization context is required")
	}
	operationContext, cancel := context.WithTimeout(ctx, agent.heartbeatInterval)
	return operationContext, cancel, nil
}

func loadOrCreateFinalizationState(
	ctx context.Context,
	handle *workerrecovery.Handle,
	assignment workercontrol.Assignment,
	outputs []runnertransport.Output,
	heartbeatSequence int64,
	gpuHealthSummary json.RawMessage,
) (localFinalizationState, error) {
	state, err := readFinalizationState(ctx, handle)
	if err == nil {
		if state.AttemptID != assignment.AttemptID || state.JobID != assignment.JobID ||
			state.WorkerID != assignment.WorkerID ||
			state.WorkerEpoch != assignment.WorkerEpoch || state.LeaseFence != assignment.LeaseFence ||
			state.LeaseToken != assignment.LeaseToken || state.CompletionID == uuid.Nil ||
			!sameOutputReceipts(state.Outputs, outputs) {
			return localFinalizationState{}, errors.New("local Finalization State does not match Assignment authority")
		}
		return state, nil
	}
	if !errors.Is(err, workerrecovery.ErrStateNotFound) {
		return localFinalizationState{}, err
	}
	state = localFinalizationState{
		AttemptID: assignment.AttemptID, JobID: assignment.JobID,
		WorkerID:    assignment.WorkerID,
		WorkerEpoch: assignment.WorkerEpoch, LeaseFence: assignment.LeaseFence,
		LeaseToken: assignment.LeaseToken, CompletionID: uuid.New(),
		HeartbeatSequence: heartbeatSequence,
		GPUHealthSummary:  append(json.RawMessage(nil), gpuHealthSummary...),
		Outputs:           append([]runnertransport.Output(nil), outputs...),
	}
	if err := writeFinalizationState(ctx, handle, state); err != nil {
		return localFinalizationState{}, err
	}
	return state, nil
}

func readFinalizationState(
	ctx context.Context,
	handle *workerrecovery.Handle,
) (localFinalizationState, error) {
	content, err := handle.Read(ctx, workerrecovery.StageUpload, finalizationStateName)
	if err != nil {
		return localFinalizationState{}, err
	}
	var state localFinalizationState
	if err := strictDecodeJSON(content, &state); err != nil {
		return localFinalizationState{}, fmt.Errorf("decode Local Finalization State: %w", err)
	}
	if state.AttemptID == uuid.Nil || state.JobID == uuid.Nil || state.WorkerID == uuid.Nil ||
		state.WorkerEpoch <= 0 || state.LeaseFence <= 0 || state.LeaseToken == "" ||
		state.CompletionID == uuid.Nil || state.HeartbeatSequence <= 0 ||
		len(state.GPUHealthSummary) == 0 || len(state.GPUHealthSummary) > 16*1024 ||
		!json.Valid(state.GPUHealthSummary) || len(state.Outputs) == 0 || len(state.Outputs) > 32 {
		return localFinalizationState{}, errors.New("local Finalization State is incomplete")
	}
	if state.VisibleCompletion != nil {
		plan := workercontrol.FinalizationPlan{JobVersion: state.JobVersion}
		if err := validateVisibleCompletion(
			*state.VisibleCompletion,
			state,
			plan,
			state.Outputs,
		); err != nil {
			return localFinalizationState{}, fmt.Errorf("validate persisted Visible Completion: %w", err)
		}
	}
	if state.CompletionCandidate != nil {
		if err := validateCompletionCandidate(*state.CompletionCandidate, state); err != nil {
			return localFinalizationState{}, fmt.Errorf("validate persisted Visible Completion candidate: %w", err)
		}
	}
	return state, nil
}

func validateCompletionCandidate(
	candidate workercontrol.VisibleCompletionCandidate,
	state localFinalizationState,
) error {
	if candidate.CompletionID != state.CompletionID || candidate.CompletionID == uuid.Nil ||
		candidate.ExpectedJobVersion != state.JobVersion || state.JobVersion <= 0 ||
		len(candidate.ArtifactIDs) == 0 || len(candidate.ArtifactIDs) != len(state.Artifacts) {
		return errors.New("local Visible Completion candidate does not match Finalization State")
	}
	planned := make(map[uuid.UUID]struct{}, len(state.Artifacts))
	for _, artifact := range state.Artifacts {
		if artifact.ArtifactID == uuid.Nil {
			return errors.New("the Visible Completion candidate references an incomplete Artifact plan")
		}
		planned[artifact.ArtifactID] = struct{}{}
	}
	for _, artifactID := range candidate.ArtifactIDs {
		if _, ok := planned[artifactID]; !ok {
			return errors.New("the Visible Completion candidate changed its Artifact set")
		}
		delete(planned, artifactID)
	}
	if len(planned) != 0 {
		return errors.New("the Visible Completion candidate omitted a planned Artifact")
	}
	return nil
}

func bindFinalizationPlan(
	ctx context.Context,
	handle *workerrecovery.Handle,
	state *localFinalizationState,
	plan workercontrol.FinalizationPlan,
) error {
	if state.JobVersion != 0 || len(state.Artifacts) != 0 {
		if state.JobVersion != plan.JobVersion || len(state.Artifacts) != len(plan.Artifacts) {
			return errors.New("replayed Finalization plan changed its authority")
		}
		for index, planned := range plan.Artifacts {
			stored := state.Artifacts[index]
			if stored.ArtifactID != planned.ArtifactID || stored.UploadID != planned.UploadID ||
				stored.Kind != planned.Kind || stored.Ordinal != planned.Ordinal ||
				stored.ObjectKey != planned.ObjectKey || stored.ClaimID == uuid.Nil ||
				stored.VerificationID == uuid.Nil {
				return errors.New("replayed Finalization Artifact plan changed")
			}
		}
		return nil
	}
	state.JobVersion = plan.JobVersion
	state.Artifacts = make([]localArtifactState, len(plan.Artifacts))
	for index, artifact := range plan.Artifacts {
		state.Artifacts[index] = localArtifactState{
			ArtifactID: artifact.ArtifactID, UploadID: artifact.UploadID,
			Kind: artifact.Kind, Ordinal: artifact.Ordinal, ObjectKey: artifact.ObjectKey,
			ClaimID: uuid.New(), VerificationID: uuid.New(),
		}
	}
	return writeFinalizationState(ctx, handle, *state)
}

func writeFinalizationState(
	ctx context.Context,
	handle *workerrecovery.Handle,
	state localFinalizationState,
) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode Local Finalization State: %w", err)
	}
	if _, err := handle.Write(
		ctx, workerrecovery.StageUpload, finalizationStateName, bytes.NewReader(encoded),
	); err != nil {
		return fmt.Errorf("persist Local Finalization State: %w", err)
	}
	return nil
}

func validateFinalizationPlan(
	plan workercontrol.FinalizationPlan,
	assignment workercontrol.Assignment,
	outputs []runnertransport.Output,
) error {
	if plan.Decision != workercontrol.FinalizationGranted || plan.AttemptID != assignment.AttemptID ||
		plan.JobID != assignment.JobID || plan.JobVersion <= 0 ||
		plan.FinalizationStartedAt.IsZero() ||
		!plan.FinalizationDeadlineAt.After(plan.FinalizationStartedAt) ||
		len(plan.Artifacts) != len(outputs) {
		return errors.New("finalization plan does not match Assignment authority and outputs")
	}
	outputIdentities := make(map[string]struct{}, len(outputs))
	for _, output := range outputs {
		outputIdentities[outputIdentity(output.Kind, output.Ordinal)] = struct{}{}
	}
	artifactIDs := make(map[uuid.UUID]struct{}, len(plan.Artifacts))
	uploadIDs := make(map[uuid.UUID]struct{}, len(plan.Artifacts))
	for _, artifact := range plan.Artifacts {
		if artifact.ArtifactID == uuid.Nil || artifact.UploadID == uuid.Nil || artifact.ObjectKey == "" ||
			artifact.ExpiresAt.IsZero() || (artifact.Kind != workercontrol.ArtifactKindVideo &&
			artifact.Kind != workercontrol.ArtifactKindThumbnail) {
			return errors.New("finalization plan contains an invalid Artifact")
		}
		if _, ok := outputIdentities[outputIdentity(string(artifact.Kind), artifact.Ordinal)]; !ok {
			return errors.New("finalization plan does not match runner output identities")
		}
		if _, exists := artifactIDs[artifact.ArtifactID]; exists {
			return errors.New("finalization plan contains duplicate Artifact identities")
		}
		if _, exists := uploadIDs[artifact.UploadID]; exists {
			return errors.New("finalization plan contains duplicate upload identities")
		}
		artifactIDs[artifact.ArtifactID] = struct{}{}
		uploadIDs[artifact.UploadID] = struct{}{}
	}
	return nil
}

func (agent *Agent) finalizeArtifact(
	ctx context.Context,
	credentials workercontrol.LeaseCredentials,
	artifact localArtifactState,
	output runnertransport.Output,
) (securedOutput, error) {
	file, snapshot, err := agent.openVerifiedOutput(ctx, output, credentials.AttemptID)
	if err != nil {
		return securedOutput{}, err
	}
	defer func() { _ = file.Close() }()
	completedParts := make([]workercontrol.ArtifactUploadPart, 0)
	secondDigest := sha256.New()
	remaining := output.SizeBytes
	for partNumber := int32(1); remaining > 0; partNumber++ {
		if partNumber > 10_000 {
			return securedOutput{}, errors.New("runner output requires too many multipart parts")
		}
		partSize := agent.artifactPartSize
		if remaining < partSize {
			partSize = remaining
		}
		payload := make([]byte, int(partSize))
		if _, err := io.ReadFull(file, payload); err != nil {
			return securedOutput{}, fmt.Errorf("read runner output part: %w", err)
		}
		_, _ = secondDigest.Write(payload)
		partDigest := sha256.Sum256(payload)
		intent := workertransport.ArtifactUploadPartIntent{
			Number: partNumber, SizeBytes: partSize, SHA256: partDigest,
		}
		claim, err := agent.finalization.ClaimArtifactUpload(
			ctx, credentials, artifact.UploadID, artifact.ClaimID, intent,
		)
		if err != nil {
			return securedOutput{}, err
		}
		if claim.Decision == workercontrol.ArtifactUploadClaimRejectedStaleLease {
			return securedOutput{}, errFinalizationAuthorityLost
		}
		if claim.Decision != workercontrol.ArtifactUploadClaimGranted ||
			claim.ClaimID != artifact.ClaimID || claim.UploadID != artifact.UploadID ||
			claim.ArtifactID != artifact.ArtifactID || claim.ObjectKey != artifact.ObjectKey ||
			claim.ExpectedContentType != output.ContentType {
			return securedOutput{}, fmt.Errorf("artifact upload claim is not usable: %s", claim.Decision)
		}
		var completed workercontrol.ArtifactUploadPart
		if claim.PartAlreadyUploaded {
			completed, err = exactCompletedPart(claim.CompletedParts, intent)
		} else if claim.UploadPart == nil {
			return securedOutput{}, errors.New("artifact upload claim omitted its signed part")
		} else {
			completed, err = agent.partUploader.Upload(ctx, *claim.UploadPart, payload)
		}
		if err != nil {
			return securedOutput{}, err
		}
		if err := validateCompletedPart(completed, intent); err != nil {
			return securedOutput{}, err
		}
		completedParts = append(completedParts, completed)
		remaining -= partSize
	}
	var uploadedDigest [sha256.Size]byte
	copy(uploadedDigest[:], secondDigest.Sum(nil))
	if uploadedDigest != output.SHA256 {
		return securedOutput{}, errors.New("runner output changed while multipart upload was prepared")
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(snapshot.info, after) || after.Size() != output.SizeBytes ||
		!after.ModTime().Equal(snapshot.info.ModTime()) {
		return securedOutput{}, errors.New("runner output changed during multipart upload")
	}
	uploadResult, err := agent.finalization.CompleteArtifactMultipartUpload(
		ctx, credentials, artifact.UploadID, artifact.ClaimID,
		workercontrol.ArtifactUploadReport{
			SizeBytes: output.SizeBytes, SHA256: output.SHA256,
			ContentType: output.ContentType, CompletedParts: completedParts,
		},
	)
	if err != nil {
		return securedOutput{}, err
	}
	if uploadResult.Decision == workercontrol.ArtifactUploadRejected {
		return securedOutput{}, errFinalizationAuthorityLost
	}
	if uploadResult.Decision != workercontrol.ArtifactUploadRecorded ||
		uploadResult.UploadID != artifact.UploadID || uploadResult.ArtifactID != artifact.ArtifactID ||
		uploadResult.ObjectVersionID == "" {
		return securedOutput{}, fmt.Errorf("artifact multipart completion was not recorded: %s", uploadResult.Decision)
	}
	verification, err := agent.finalization.VerifyArtifact(
		ctx, credentials, artifact.UploadID, artifact.VerificationID,
	)
	if err != nil {
		return securedOutput{}, err
	}
	switch verification.Decision {
	case workercontrol.ArtifactVerificationRejected:
		return securedOutput{}, errFinalizationAuthorityLost
	case workercontrol.ArtifactValidationFailed:
		if verification.VerificationID != artifact.VerificationID ||
			verification.UploadID != artifact.UploadID || verification.ArtifactID != artifact.ArtifactID ||
			verification.ObjectVersionID != uploadResult.ObjectVersionID {
			return securedOutput{}, errors.New("artifact validation failure did not match exact upload authority")
		}
		return securedOutput{}, &artifactValidationFailure{
			artifactID: artifact.ArtifactID,
			uploadID:   artifact.UploadID,
		}
	case workercontrol.ArtifactVerified:
		if verification.VerificationID == artifact.VerificationID &&
			verification.UploadID == artifact.UploadID && verification.ArtifactID == artifact.ArtifactID &&
			verification.ObjectVersionID == uploadResult.ObjectVersionID {
			return snapshot, nil
		}
	}
	if verification.Decision != workercontrol.ArtifactVerified {
		return securedOutput{}, fmt.Errorf("artifact verification did not succeed: %s", verification.Decision)
	}
	if verification.VerificationID != artifact.VerificationID ||
		verification.UploadID != artifact.UploadID || verification.ArtifactID != artifact.ArtifactID ||
		verification.ObjectVersionID != uploadResult.ObjectVersionID {
		return securedOutput{}, fmt.Errorf("artifact verification did not succeed: %s", verification.Decision)
	}
	return snapshot, nil
}

func (agent *Agent) openVerifiedOutput(
	ctx context.Context,
	output runnertransport.Output,
	attemptID uuid.UUID,
) (*os.File, securedOutput, error) {
	if ctx == nil {
		return nil, securedOutput{}, errors.New("runner output verification context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, securedOutput{}, err
	}
	cleaned := filepath.Clean(output.Path)
	attemptRoot := filepath.Join(agent.outputRoot, attemptID.String())
	if !filepath.IsAbs(cleaned) || cleaned != output.Path {
		return nil, securedOutput{}, errors.New("runner output is not directly inside its exact Attempt root")
	}
	resolvedParent, err := securefile.ResolveTrustedDirectory(filepath.Dir(cleaned))
	if err != nil {
		return nil, securedOutput{}, fmt.Errorf("resolve runner output directory: %w", err)
	}
	if resolvedParent != attemptRoot {
		return nil, securedOutput{}, errors.New("runner output is not directly inside its exact Attempt root")
	}
	parent, err := securefile.OpenTrustedDirectory(attemptRoot)
	if err != nil {
		return nil, securedOutput{}, fmt.Errorf("validate runner output directory: %w", err)
	}
	defer func() { _ = parent.Close() }()
	return agent.openVerifiedOutputAt(ctx, parent, output, attemptID)
}

func (agent *Agent) openVerifiedOutputAt(
	ctx context.Context,
	parent *os.File,
	output runnertransport.Output,
	attemptID uuid.UUID,
) (*os.File, securedOutput, error) {
	if ctx == nil {
		return nil, securedOutput{}, errors.New("runner output verification context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, securedOutput{}, err
	}
	if parent == nil {
		return nil, securedOutput{}, errors.New("runner output directory is not open")
	}
	cleaned := filepath.Clean(output.Path)
	attemptRoot := filepath.Join(agent.outputRoot, attemptID.String())
	if !filepath.IsAbs(cleaned) || cleaned != output.Path {
		return nil, securedOutput{}, errors.New("runner output is not directly inside its exact Attempt root")
	}
	rawAttemptRoot := filepath.Dir(cleaned)
	resolvedOutputRoot, err := securefile.ResolveTrustedDirectory(filepath.Dir(rawAttemptRoot))
	if err != nil || filepath.Base(rawAttemptRoot) != attemptID.String() ||
		resolvedOutputRoot != agent.outputRoot {
		return nil, securedOutput{}, errors.New("runner output is not directly inside its exact Attempt root")
	}
	path := filepath.Join(attemptRoot, filepath.Base(cleaned))
	fd, err := unix.Openat(
		int(parent.Fd()), filepath.Base(cleaned),
		unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		0,
	)
	if err != nil {
		return nil, securedOutput{}, fmt.Errorf("open runner output: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	opened, err := file.Stat()
	if err != nil || validateOutputFileInfo(opened, agent.outputOwnerUID, output.SizeBytes) != nil {
		_ = file.Close()
		if err == nil {
			return nil, securedOutput{}, validateOutputFileInfo(opened, agent.outputOwnerUID, output.SizeBytes)
		}
		return nil, securedOutput{}, errors.New("runner output changed while it was opened")
	}
	digest, written, err := agent.hashOutput(ctx, file)
	if err != nil {
		_ = file.Close()
		return nil, securedOutput{}, err
	}
	if written != output.SizeBytes {
		_ = file.Close()
		return nil, securedOutput{}, errors.New("runner output size changed while it was hashed")
	}
	if digest != output.SHA256 {
		_ = file.Close()
		return nil, securedOutput{}, errors.New("runner output SHA-256 does not match its receipt")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, securedOutput{}, fmt.Errorf("rewind runner output: %w", err)
	}
	return file, securedOutput{path: path, info: opened}, nil
}

func (agent *Agent) hashOutput(
	ctx context.Context,
	file *os.File,
) ([sha256.Size]byte, int64, error) {
	if ctx == nil || file == nil {
		return [sha256.Size]byte{}, 0, errors.New("runner output hash input is incomplete")
	}
	if agent.beforeOutputHash != nil {
		agent.beforeOutputHash(ctx)
	}
	if err := ctx.Err(); err != nil {
		return [sha256.Size]byte{}, 0, err
	}
	hasher := sha256.New()
	buffer := make([]byte, 128<<10)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return [sha256.Size]byte{}, written, err
		}
		read, readErr := file.Read(buffer)
		if read > 0 {
			_, _ = hasher.Write(buffer[:read])
			written += int64(read)
			if agent.afterOutputHashChunk != nil {
				agent.afterOutputHashChunk(ctx, written)
			}
			if err := ctx.Err(); err != nil {
				return [sha256.Size]byte{}, written, err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return [sha256.Size]byte{}, written, fmt.Errorf("read runner output for SHA-256: %w", readErr)
		}
		if read == 0 {
			return [sha256.Size]byte{}, written, io.ErrNoProgress
		}
	}
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return digest, written, nil
}

func validateOutputFileInfo(info os.FileInfo, ownerUID uint32, size int64) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o600 || stat.Uid != ownerUID || info.Size() != size || size <= 0 {
		return errors.New("runner output owner, mode, type, or size is invalid")
	}
	if stat.Nlink != 1 {
		return errors.New("runner output link count is invalid")
	}
	return nil
}

func exactCompletedPart(
	parts []workercontrol.ArtifactUploadPart,
	intent workertransport.ArtifactUploadPartIntent,
) (workercontrol.ArtifactUploadPart, error) {
	for _, part := range parts {
		if part.Number == intent.Number {
			return part, nil
		}
	}
	return workercontrol.ArtifactUploadPart{}, errors.New("resumed Artifact claim omitted its completed part")
}

func validateCompletedPart(
	part workercontrol.ArtifactUploadPart,
	intent workertransport.ArtifactUploadPartIntent,
) error {
	if part.Number != intent.Number || part.SizeBytes != intent.SizeBytes || part.ETag == "" ||
		part.ChecksumSHA256 != base64.StdEncoding.EncodeToString(intent.SHA256[:]) {
		return errors.New("completed Artifact part does not match its signed intent")
	}
	return nil
}

func validateVisibleCompletion(
	result workercontrol.VisibleCompletionResult,
	state localFinalizationState,
	plan workercontrol.FinalizationPlan,
	outputs []runnertransport.Output,
) error {
	if result.CompletionID == uuid.Nil || result.JobID != state.JobID ||
		result.AttemptID != state.AttemptID || result.ArtifactSetID == uuid.Nil ||
		result.ChargeID == uuid.Nil || result.JobVersion != plan.JobVersion+1 ||
		result.ManifestSHA256 == [sha256.Size]byte{} || result.CompletedAt.IsZero() ||
		len(result.Artifacts) != len(state.Artifacts) {
		return errors.New("visible Completion receipt does not match Finalization authority")
	}
	if result.Decision == workercontrol.VisibleCompletionCommitted && result.CompletionID != state.CompletionID {
		return errors.New("visible Completion receipt changed its completion identity")
	}
	outputByIdentity := make(map[string]runnertransport.Output, len(outputs))
	for _, output := range outputs {
		outputByIdentity[outputIdentity(output.Kind, output.Ordinal)] = output
	}
	plannedByID := make(map[uuid.UUID]localArtifactState, len(state.Artifacts))
	for _, artifact := range state.Artifacts {
		plannedByID[artifact.ArtifactID] = artifact
	}
	for _, artifact := range result.Artifacts {
		planned, ok := plannedByID[artifact.ArtifactID]
		output := outputByIdentity[outputIdentity(string(artifact.Kind), artifact.Ordinal)]
		if !ok || artifact.Kind != planned.Kind || artifact.Ordinal != planned.Ordinal ||
			artifact.ObjectKey != planned.ObjectKey || artifact.ObjectVersionID == "" ||
			artifact.SizeBytes != output.SizeBytes || artifact.SHA256 != output.SHA256 ||
			artifact.ContentType != output.ContentType {
			return errors.New("visible Completion committed Artifact changed its immutable receipt")
		}
		delete(plannedByID, artifact.ArtifactID)
	}
	if len(plannedByID) != 0 {
		return errors.New("visible Completion omitted a planned Artifact")
	}
	return nil
}

func (agent *Agent) cleanupCommittedOutputs(
	ctx context.Context,
	attemptID uuid.UUID,
	outputs []runnertransport.Output,
) error {
	if ctx == nil {
		return errors.New("terminal output cleanup context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	parent, quarantineRoot, quarantineName, fresh, err := agent.quarantineAttemptOutputDirectory(attemptID)
	if err != nil || parent == nil {
		return err
	}
	defer func() { _ = parent.Close() }()
	defer func() { _ = quarantineRoot.Close() }()
	for _, output := range outputs {
		file, snapshot, err := agent.openVerifiedOutputAt(ctx, parent, output, attemptID)
		if errors.Is(err, os.ErrNotExist) {
			if fresh {
				return errors.New("runner output disappeared before terminal quarantine")
			}
			continue
		}
		if err != nil {
			return err
		}
		currentFD, err := unix.Openat(
			int(parent.Fd()), filepath.Base(snapshot.path),
			unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
			0,
		)
		if err != nil {
			_ = file.Close()
			return errors.New("runner output changed before terminal cleanup")
		}
		currentFile := os.NewFile(uintptr(currentFD), snapshot.path)
		current, currentErr := currentFile.Stat()
		closeCurrentErr := currentFile.Close()
		if currentErr != nil || closeCurrentErr != nil || !os.SameFile(snapshot.info, current) ||
			validateOutputFileInfo(current, agent.outputOwnerUID, output.SizeBytes) != nil {
			_ = file.Close()
			return errors.New("runner output changed before terminal cleanup")
		}
		if err := unix.Unlinkat(int(parent.Fd()), filepath.Base(snapshot.path), 0); err != nil {
			_ = file.Close()
			return fmt.Errorf("remove terminal runner output: %w", err)
		}
		unlinked, statErr := file.Stat()
		if statErr != nil || !os.SameFile(snapshot.info, unlinked) || inodeLinkCount(unlinked) != 0 {
			_ = file.Close()
			return errors.New("runner output inode was not removed during terminal cleanup")
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("close terminal runner output: %w", err)
		}
		if err := parent.Sync(); err != nil {
			return fmt.Errorf("sync quarantined Attempt output directory: %w", err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(agent.outputQuarantineRoot, quarantineName))
	if err != nil {
		return fmt.Errorf("inspect quarantined Attempt output directory: %w", err)
	}
	for _, entry := range entries {
		if entry.Name() != ".cleanup-complete" {
			return errors.New("quarantined Attempt output directory contains an unexpected entry")
		}
	}
	if err := ensureCleanupMarker(parent, agent.outputOwnerUID); err != nil {
		return err
	}
	if err := parent.Sync(); err != nil {
		return fmt.Errorf("sync terminal output cleanup marker: %w", err)
	}
	outputRoot, err := securefile.OpenTrustedDirectory(agent.outputRoot)
	if err != nil {
		return fmt.Errorf("open output root after terminal quarantine: %w", err)
	}
	defer func() { _ = outputRoot.Close() }()
	replacementFD, replacementErr := unix.Openat(
		int(outputRoot.Fd()), attemptID.String(),
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		0,
	)
	if replacementErr == nil {
		_ = unix.Close(replacementFD)
		return errors.New("terminal Attempt output directory was replaced during quarantine")
	}
	if !errors.Is(replacementErr, os.ErrNotExist) {
		return fmt.Errorf("confirm terminal Attempt output quarantine: %w", replacementErr)
	}
	if err := unix.Unlinkat(int(parent.Fd()), ".cleanup-complete", 0); err != nil {
		return fmt.Errorf("remove terminal output cleanup marker: %w", err)
	}
	if err := parent.Sync(); err != nil {
		return fmt.Errorf("sync quarantined Attempt output directory before removal: %w", err)
	}
	openedInfo, err := parent.Stat()
	if err != nil {
		return fmt.Errorf("inspect quarantined Attempt output directory: %w", err)
	}
	if err := unix.Unlinkat(int(quarantineRoot.Fd()), quarantineName, unix.AT_REMOVEDIR); err != nil {
		return fmt.Errorf("remove quarantined Attempt output directory: %w", err)
	}
	unlinkedInfo, err := parent.Stat()
	if err != nil || !os.SameFile(openedInfo, unlinkedInfo) {
		return errors.New("quarantined Attempt output directory inode was not removed")
	}
	if err := quarantineRoot.Sync(); err != nil {
		return fmt.Errorf("sync output quarantine root after terminal cleanup: %w", err)
	}
	return nil
}

func ensureCleanupMarker(parent *os.File, ownerUID uint32) error {
	markerFD, err := unix.Openat(
		int(parent.Fd()), ".cleanup-complete",
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		0o600,
	)
	if err == nil {
		marker := os.NewFile(uintptr(markerFD), ".cleanup-complete")
		if syncErr := marker.Sync(); syncErr != nil {
			_ = marker.Close()
			return fmt.Errorf("sync terminal output cleanup marker: %w", syncErr)
		}
		if closeErr := marker.Close(); closeErr != nil {
			return fmt.Errorf("close terminal output cleanup marker: %w", closeErr)
		}
		return nil
	}
	if !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("record terminal output cleanup completion: %w", err)
	}
	markerFD, err = unix.Openat(
		int(parent.Fd()), ".cleanup-complete",
		unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		0,
	)
	if err != nil {
		return errors.New("terminal output cleanup marker is invalid")
	}
	marker := os.NewFile(uintptr(markerFD), ".cleanup-complete")
	info, statErr := marker.Stat()
	closeErr := marker.Close()
	stat, ok := infoSysStat(info)
	if statErr != nil || closeErr != nil || !ok || !info.Mode().IsRegular() ||
		info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 ||
		stat.Uid != ownerUID || stat.Nlink != 1 || info.Size() != 0 {
		return errors.New("terminal output cleanup marker is invalid")
	}
	return nil
}

func infoSysStat(info os.FileInfo) (*syscall.Stat_t, bool) {
	if info == nil {
		return nil, false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return stat, ok
}

func (agent *Agent) quarantineAttemptOutputDirectory(
	attemptID uuid.UUID,
) (*os.File, *os.File, string, bool, error) {
	quarantineRoot, err := securefile.OpenTrustedDirectory(agent.outputQuarantineRoot)
	if err != nil {
		return nil, nil, "", false, fmt.Errorf("open output quarantine root: %w", err)
	}
	closeQuarantine := func(err error) (*os.File, *os.File, string, bool, error) {
		_ = quarantineRoot.Close()
		return nil, nil, "", false, err
	}
	existing, err := findAttemptQuarantine(agent.outputQuarantineRoot, attemptID)
	if err != nil {
		return closeQuarantine(err)
	}
	if existing != "" {
		parent, err := openQuarantinedAttempt(quarantineRoot, existing, attemptID)
		if err != nil {
			return closeQuarantine(err)
		}
		return parent, quarantineRoot, existing, false, nil
	}
	outputRoot, err := securefile.OpenTrustedDirectory(agent.outputRoot)
	if err != nil {
		return closeQuarantine(fmt.Errorf("open output root for terminal quarantine: %w", err))
	}
	defer func() { _ = outputRoot.Close() }()
	attemptFD, err := unix.Openat(
		int(outputRoot.Fd()), attemptID.String(),
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		0,
	)
	if errors.Is(err, os.ErrNotExist) {
		_ = quarantineRoot.Close()
		return nil, nil, "", false, nil
	}
	if err != nil {
		return closeQuarantine(fmt.Errorf("open terminal Attempt output directory: %w", err))
	}
	original := os.NewFile(uintptr(attemptFD), filepath.Join(agent.outputRoot, attemptID.String()))
	defer func() { _ = original.Close() }()
	originalInfo, err := original.Stat()
	if err != nil || !originalInfo.IsDir() {
		return closeQuarantine(errors.New("terminal Attempt output directory is invalid"))
	}
	name, err := outputQuarantineName(attemptID, originalInfo)
	if err != nil {
		return closeQuarantine(err)
	}
	if agent.beforeOutputQuarantine != nil {
		agent.beforeOutputQuarantine()
	}
	if err := unix.Renameat(
		int(outputRoot.Fd()), attemptID.String(), int(quarantineRoot.Fd()), name,
	); err != nil {
		return closeQuarantine(fmt.Errorf("quarantine terminal Attempt output directory: %w", err))
	}
	if err := outputRoot.Sync(); err != nil {
		return closeQuarantine(fmt.Errorf("sync output root after terminal quarantine: %w", err))
	}
	if err := quarantineRoot.Sync(); err != nil {
		return closeQuarantine(fmt.Errorf("sync output quarantine root: %w", err))
	}
	parent, err := openQuarantinedAttempt(quarantineRoot, name, attemptID)
	if err != nil {
		return closeQuarantine(err)
	}
	quarantinedInfo, err := parent.Stat()
	if err != nil || !os.SameFile(originalInfo, quarantinedInfo) {
		_ = parent.Close()
		return closeQuarantine(errors.New("attempt output directory changed before terminal quarantine"))
	}
	return parent, quarantineRoot, name, true, nil
}

func findAttemptQuarantine(root string, attemptID uuid.UUID) (string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", fmt.Errorf("list output quarantine root: %w", err)
	}
	prefix := attemptID.String() + "-"
	found := ""
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		if found != "" {
			return "", errors.New("multiple Attempt output quarantines require operator reconciliation")
		}
		found = entry.Name()
	}
	return found, nil
}

func openQuarantinedAttempt(root *os.File, name string, attemptID uuid.UUID) (*os.File, error) {
	fd, err := unix.Openat(
		int(root.Fd()), name,
		unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open quarantined Attempt output directory: %w", err)
	}
	parent := os.NewFile(uintptr(fd), filepath.Join(root.Name(), name))
	info, err := parent.Stat()
	if err != nil || !info.IsDir() {
		_ = parent.Close()
		return nil, errors.New("quarantined Attempt output directory is invalid")
	}
	expectedName, err := outputQuarantineName(attemptID, info)
	if err != nil || expectedName != name {
		_ = parent.Close()
		return nil, errors.New("quarantined Attempt output directory identity is invalid")
	}
	return parent, nil
}

func outputQuarantineName(attemptID uuid.UUID, info os.FileInfo) (string, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() {
		return "", errors.New("attempt output directory identity is unavailable")
	}
	return fmt.Sprintf("%s-%x-%x", attemptID, uint64(stat.Dev), uint64(stat.Ino)), nil
}

func inodeLinkCount(info os.FileInfo) uint64 {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return math.MaxUint64
	}
	return uint64(stat.Nlink)
}

func sameOutputReceipts(left, right []runnertransport.Output) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func outputIdentity(kind string, ordinal int32) string {
	return fmt.Sprintf("%s/%d", kind, ordinal)
}

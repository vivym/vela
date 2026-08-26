package workeragent

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/debugdumpcontract"
	"github.com/vivym/vela/internal/runnertransport"
	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workertransport"
)

func (agent *Agent) uploadDebugDumpBestEffort(
	ctx context.Context,
	assignment workercontrol.Assignment,
	credentials workercontrol.LeaseCredentials,
	dump *runnertransport.DebugDump,
	leaseDeadline time.Duration,
) {
	if dump == nil {
		return
	}
	if err := agent.uploadDebugDump(ctx, assignment, credentials, dump, leaseDeadline); err != nil {
		agent.reportDebugDumpError(err)
	}
}

func (agent *Agent) uploadDebugDump(
	ctx context.Context,
	assignment workercontrol.Assignment,
	credentials workercontrol.LeaseCredentials,
	dump *runnertransport.DebugDump,
	leaseDeadline time.Duration,
) error {
	if agent.debugDumps == nil || agent.debugDumpUploader == nil {
		return errors.New("authorized debug dump upload is not configured")
	}
	authorization := assignment.DebugDumpAuthorization
	if authorization == nil || authorization.AuthorizationID == uuid.Nil {
		return errors.New("failed Runner status has no debug dump authorization")
	}
	if dump == nil {
		return errors.New("failed Runner status has no debug dump")
	}
	envelope, envelopeErr := debugdumpcontract.Parse(dump.Content)
	if envelopeErr != nil ||
		envelope.AuthorizationID != authorization.AuthorizationID.String() ||
		envelope.AttemptID != assignment.AttemptID.String() ||
		envelope.JobID != assignment.JobID.String() ||
		envelope.WorkerID != assignment.WorkerID.String() ||
		envelope.WorkerEpoch != assignment.WorkerEpoch ||
		envelope.LeaseFence != assignment.LeaseFence ||
		len(dump.Content) == 0 || len(dump.Content) > debugdumpcontract.MaxBytes ||
		dump.SizeBytes != int64(len(dump.Content)) || dump.SHA256 == [sha256.Size]byte{} ||
		sha256.Sum256(dump.Content) != dump.SHA256 || dump.ContentType != debugdumpcontract.ContentType {
		return errors.New("failed Runner status has an invalid debug dump")
	}
	now := agent.monotonicNow()
	if now < 0 || leaseDeadline <= now+defaultTerminalControlTimeout {
		return errors.New("insufficient Lease time remains for optional debug dump upload")
	}
	timeout := agent.debugDumpUploadTimeout
	if available := leaseDeadline - now - defaultTerminalControlTimeout; timeout > available {
		timeout = available
	}
	uploadContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	debugDumpID := deterministicDebugDumpID(
		assignment.AttemptID, authorization.AuthorizationID, dump.SHA256,
	)
	claimID := uuid.NewSHA1(debugDumpID, []byte("vela.debug-dump.upload-claim.v1"))
	intent := workercontrol.DebugDumpUploadIntent{
		DebugDumpID: debugDumpID, AuthorizationID: authorization.AuthorizationID,
		SizeBytes: dump.SizeBytes, SHA256: dump.SHA256, ContentType: dump.ContentType,
	}
	partIntent := workertransport.DebugDumpUploadPartIntent{
		Number: 1, SizeBytes: dump.SizeBytes, SHA256: dump.SHA256,
	}
	claim, err := agent.debugDumps.ClaimDebugDumpUpload(
		uploadContext, credentials, intent, claimID, partIntent,
	)
	if err != nil {
		return fmt.Errorf("claim authorized debug dump upload: %w", err)
	}
	if claim.Decision != workercontrol.DebugDumpUploadClaimGranted {
		return fmt.Errorf("claim authorized debug dump upload: %s", claim.Decision)
	}
	if claim.ClaimID != claimID || claim.DebugDumpID != debugDumpID ||
		claim.AuthorizationID != authorization.AuthorizationID ||
		claim.ExpectedSizeBytes != dump.SizeBytes || claim.ExpectedSHA256 != dump.SHA256 ||
		claim.ExpectedContentType != dump.ContentType {
		return errors.New("authorized debug dump upload claim changed its authority")
	}

	var completed workercontrol.DebugDumpUploadPart
	if claim.PartAlreadyUploaded {
		if claim.UploadPart != nil || len(claim.CompletedParts) != 1 {
			return errors.New("authorized debug dump upload replay receipt is invalid")
		}
		completed = claim.CompletedParts[0]
	} else {
		if claim.UploadPart == nil || len(claim.CompletedParts) != 0 {
			return errors.New("authorized debug dump upload claim omitted its signed part")
		}
		completed, err = agent.debugDumpUploader.UploadDebugDump(
			uploadContext, *claim.UploadPart, append([]byte(nil), dump.Content...),
		)
		if err != nil {
			return fmt.Errorf("upload authorized debug dump: %w", err)
		}
	}
	report := workercontrol.DebugDumpUploadReport{
		SizeBytes: dump.SizeBytes, SHA256: dump.SHA256, ContentType: dump.ContentType,
		CompletedParts: []workercontrol.DebugDumpUploadPart{completed},
	}
	result, err := agent.debugDumps.CompleteDebugDumpMultipartUpload(
		uploadContext, credentials, debugDumpID, authorization.AuthorizationID, claimID, report,
	)
	if err != nil {
		return fmt.Errorf("complete authorized debug dump upload: %w", err)
	}
	if result.Decision != workercontrol.DebugDumpUploadRecorded || result.DebugDumpID != debugDumpID ||
		result.AuthorizationID != authorization.AuthorizationID || result.ObjectVersionID == "" ||
		result.Version <= 0 {
		return errors.New("authorized debug dump upload receipt is invalid")
	}
	return nil
}

func deterministicDebugDumpID(
	attemptID, authorizationID uuid.UUID,
	digest [sha256.Size]byte,
) uuid.UUID {
	seed := make([]byte, 0, len("vela.debug-dump.v1\x00")+len(authorizationID)+len(digest))
	seed = append(seed, "vela.debug-dump.v1\x00"...)
	seed = append(seed, authorizationID[:]...)
	seed = append(seed, digest[:]...)
	return uuid.NewSHA1(attemptID, seed)
}

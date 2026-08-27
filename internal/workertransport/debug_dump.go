package workertransport

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/debugdumpcontract"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

const debugDumpUploadContentType = debugdumpcontract.ContentType

type DebugDumpUploadPartIntent struct {
	Number    int32
	SizeBytes int64
	SHA256    [sha256.Size]byte
}

type SignedDebugDumpUploadPart struct {
	Number          int32
	SizeBytes       int64
	SHA256          [sha256.Size]byte
	URL             string
	RequiredHeaders map[string]string
	ExpiresAt       time.Time
}

type DebugDumpUploadClaim struct {
	workercontrol.DebugDumpUploadClaim
	UploadPart          *SignedDebugDumpUploadPart
	PartAlreadyUploaded bool
}

func (client *Client) ClaimDebugDumpUpload(
	ctx context.Context,
	credentials workercontrol.LeaseCredentials,
	intent workercontrol.DebugDumpUploadIntent,
	claimID uuid.UUID,
	part DebugDumpUploadPartIntent,
) (DebugDumpUploadClaim, error) {
	lease, err := protoLeaseCredentials(credentials)
	if err != nil {
		return DebugDumpUploadClaim{}, err
	}
	if intent.DebugDumpID == uuid.Nil || intent.AuthorizationID == uuid.Nil ||
		intent.SizeBytes <= 0 || intent.SizeBytes > debugdumpcontract.MaxBytes ||
		intent.SHA256 == [sha256.Size]byte{} || intent.ContentType != debugDumpUploadContentType ||
		claimID == uuid.Nil || part.Number != 1 || part.SizeBytes != intent.SizeBytes ||
		part.SHA256 != intent.SHA256 {
		return DebugDumpUploadClaim{}, errors.New("debug dump upload claim intent is invalid")
	}
	response, err := client.exchange(ctx, &velav1.ConnectRequest{
		Operation: &velav1.ConnectRequest_ClaimDebugDumpUpload{
			ClaimDebugDumpUpload: &velav1.ClaimDebugDumpUploadRequest{
				Lease: lease, DebugDumpId: intent.DebugDumpID.String(),
				AuthorizationId: intent.AuthorizationID.String(), SizeBytes: intent.SizeBytes,
				Sha256: intent.SHA256[:], ContentType: intent.ContentType,
				Part: &velav1.DebugDumpUploadPartIntent{
					Number: part.Number, SizeBytes: part.SizeBytes, Sha256: part.SHA256[:],
				},
				ClaimId: claimID.String(),
			},
		},
	})
	if err != nil {
		return DebugDumpUploadClaim{}, err
	}
	return parseDebugDumpUploadClaim(
		response.GetDebugDumpUploadClaim(), intent, claimID, part,
	)
}

func (client *Client) CompleteDebugDumpMultipartUpload(
	ctx context.Context,
	credentials workercontrol.LeaseCredentials,
	debugDumpID, authorizationID, claimID uuid.UUID,
	report workercontrol.DebugDumpUploadReport,
) (workercontrol.DebugDumpUploadResult, error) {
	lease, err := protoLeaseCredentials(credentials)
	if err != nil {
		return workercontrol.DebugDumpUploadResult{}, err
	}
	parts, err := protoCompletedDebugDumpParts(report)
	if err != nil || debugDumpID == uuid.Nil || authorizationID == uuid.Nil || claimID == uuid.Nil {
		return workercontrol.DebugDumpUploadResult{}, errors.New("debug dump multipart completion receipt is invalid")
	}
	response, err := client.exchange(ctx, &velav1.ConnectRequest{
		Operation: &velav1.ConnectRequest_CompleteDebugDumpMultipartUpload{
			CompleteDebugDumpMultipartUpload: &velav1.CompleteDebugDumpMultipartUploadRequest{
				Lease: lease, DebugDumpId: debugDumpID.String(), ClaimId: claimID.String(),
				AuthorizationId: authorizationID.String(), SizeBytes: report.SizeBytes,
				Sha256: report.SHA256[:], ContentType: report.ContentType,
				CompletedParts: parts,
			},
		},
	})
	if err != nil {
		return workercontrol.DebugDumpUploadResult{}, err
	}
	return parseDebugDumpUploadResult(
		response.GetDebugDumpUploadResult(), debugDumpID, authorizationID,
	)
}

func parseDebugDumpUploadIntent(
	request *velav1.ClaimDebugDumpUploadRequest,
) (workercontrol.DebugDumpUploadIntent, *DebugDumpUploadPartIntent, bool) {
	if request == nil || request.GetSizeBytes() <= 0 ||
		request.GetSizeBytes() > debugdumpcontract.MaxBytes || len(request.GetSha256()) != sha256.Size ||
		request.GetContentType() != debugDumpUploadContentType || request.GetPart() == nil {
		return workercontrol.DebugDumpUploadIntent{}, nil, false
	}
	debugDumpID, dumpOK := parseRequiredUUID(request.GetDebugDumpId())
	authorizationID, authorizationOK := parseRequiredUUID(request.GetAuthorizationId())
	var digest [sha256.Size]byte
	copy(digest[:], request.GetSha256())
	part := request.GetPart()
	if !dumpOK || !authorizationOK || digest == [sha256.Size]byte{} || part.GetNumber() != 1 ||
		part.GetSizeBytes() != request.GetSizeBytes() || len(part.GetSha256()) != sha256.Size ||
		!equalDigest(part.GetSha256(), digest) {
		return workercontrol.DebugDumpUploadIntent{}, nil, false
	}
	return workercontrol.DebugDumpUploadIntent{
			DebugDumpID: debugDumpID, AuthorizationID: authorizationID,
			SizeBytes: request.GetSizeBytes(), SHA256: digest,
			ContentType: request.GetContentType(),
		}, &DebugDumpUploadPartIntent{
			Number: 1, SizeBytes: request.GetSizeBytes(), SHA256: digest,
		}, true
}

func parseDebugDumpMultipartCompletion(
	request *velav1.CompleteDebugDumpMultipartUploadRequest,
) (workercontrol.DebugDumpUploadReport, []artifactstore.CompletedPart, bool) {
	if request == nil || request.GetSizeBytes() <= 0 ||
		request.GetSizeBytes() > debugdumpcontract.MaxBytes || len(request.GetSha256()) != sha256.Size ||
		request.GetContentType() != debugDumpUploadContentType || len(request.GetCompletedParts()) != 1 {
		return workercontrol.DebugDumpUploadReport{}, nil, false
	}
	part := request.GetCompletedParts()[0]
	var digest [sha256.Size]byte
	copy(digest[:], request.GetSha256())
	if digest == [sha256.Size]byte{} || part == nil || part.GetNumber() != 1 ||
		part.GetSizeBytes() != request.GetSizeBytes() || part.GetEtag() == "" ||
		len(part.GetEtag()) > 1000 || strings.ContainsRune(part.GetEtag(), '\x00') ||
		len(part.GetChecksumSha256()) != sha256.Size || !equalDigest(part.GetChecksumSha256(), digest) {
		return workercontrol.DebugDumpUploadReport{}, nil, false
	}
	checksum := base64.StdEncoding.EncodeToString(digest[:])
	return workercontrol.DebugDumpUploadReport{
			SizeBytes: request.GetSizeBytes(), SHA256: digest, ContentType: request.GetContentType(),
			CompletedParts: []workercontrol.DebugDumpUploadPart{{
				Number: 1, ETag: part.GetEtag(), SizeBytes: part.GetSizeBytes(),
				ChecksumSHA256: checksum,
			}},
		}, []artifactstore.CompletedPart{{
			Number: 1, ETag: part.GetEtag(), SizeBytes: part.GetSizeBytes(),
			ChecksumSHA256: checksum,
		}}, true
}

func (server *Server) prepareDebugDumpUploadClaim(
	ctx context.Context,
	worker workercontrol.AuthenticatedWorker,
	credentials workercontrol.LeaseCredentials,
	claimID uuid.UUID,
	claim workercontrol.DebugDumpUploadClaim,
	part *DebugDumpUploadPartIntent,
) (*velav1.DebugDumpUploadClaim, error) {
	if claim.Decision != workercontrol.DebugDumpUploadClaimGranted {
		return debugDumpUploadClaimMessage(claim, nil, false)
	}
	if part == nil || claim.DebugDumpID == uuid.Nil || claim.AuthorizationID == uuid.Nil ||
		claim.ObjectKey == "" || claim.ExpectedSizeBytes != part.SizeBytes ||
		claim.ExpectedSHA256 != part.SHA256 || claim.ExpectedContentType != debugDumpUploadContentType ||
		claim.UploadExpiresAt.IsZero() {
		return nil, errors.New("granted debug dump upload claim is incomplete")
	}
	upload := artifactstore.MultipartUpload{
		ObjectKey: claim.ObjectKey, UploadID: claim.MultipartUploadID,
		ContentType: claim.ExpectedContentType,
	}
	if upload.UploadID == "" {
		created, err := server.uploadStore.CreateMultipartUpload(
			ctx, claim.ObjectKey, claim.ExpectedContentType,
		)
		if err != nil {
			return nil, err
		}
		if created.ObjectKey != claim.ObjectKey || created.ContentType != claim.ExpectedContentType ||
			created.UploadID == "" {
			return nil, server.abortOrphanMultipart(
				ctx, created, errors.New("created debug dump multipart session does not match its claim"),
			)
		}
		recorded, err := server.debugDumps.RecordDebugDumpMultipartSession(
			ctx, worker, credentials, claim.DebugDumpID, claimID, created.UploadID,
		)
		if err != nil {
			return nil, server.abortOrphanMultipart(ctx, created, err)
		}
		switch recorded.Decision {
		case workercontrol.DebugDumpMultipartSessionRecorded:
			if recorded.DebugDumpID != claim.DebugDumpID ||
				recorded.AuthorizationID != claim.AuthorizationID ||
				recorded.MultipartUploadID != created.UploadID {
				return nil, server.abortOrphanMultipart(
					ctx, created, errors.New("recorded debug dump multipart session changed identity"),
				)
			}
			claim.MultipartUploadID = created.UploadID
			claim.Version = recorded.Version
			upload = created
		case workercontrol.DebugDumpMultipartSessionConflict:
			if err := server.abortOrphanMultipart(ctx, created, nil); err != nil {
				return nil, err
			}
			status, err := server.debugDumps.InspectDebugDumpUpload(
				ctx, worker, credentials, claim.DebugDumpID,
			)
			if err != nil {
				return nil, err
			}
			if status.Decision != workercontrol.DebugDumpUploadRecorded ||
				status.State != workercontrol.DebugDumpUploadStateUploading ||
				status.DebugDumpID != claim.DebugDumpID ||
				status.AuthorizationID != claim.AuthorizationID || status.MultipartUploadID == "" ||
				status.ObjectKey != claim.ObjectKey {
				claim.Decision = workercontrol.DebugDumpUploadClaimConflict
				return debugDumpUploadClaimMessage(claim, nil, false)
			}
			claim.MultipartUploadID = status.MultipartUploadID
			claim.Version = status.Version
			claim.UploadExpiresAt = status.UploadExpiresAt
			upload.UploadID = status.MultipartUploadID
		case workercontrol.DebugDumpMultipartSessionRejected:
			if err := server.abortOrphanMultipart(ctx, created, nil); err != nil {
				return nil, err
			}
			claim.Decision = workercontrol.DebugDumpUploadClaimRejected
			return debugDumpUploadClaimMessage(claim, nil, false)
		default:
			return nil, server.abortOrphanMultipart(
				ctx, created, errors.New("unknown debug dump multipart session decision"),
			)
		}
	}
	completed, err := server.uploadStore.ListParts(ctx, upload)
	if err != nil {
		return nil, err
	}
	if len(completed) > 1 {
		return nil, errors.New("debug dump multipart session contains more than one part")
	}
	wantedChecksum := base64.StdEncoding.EncodeToString(part.SHA256[:])
	if len(completed) == 1 && completed[0].Number == 1 &&
		completed[0].SizeBytes == part.SizeBytes && completed[0].ChecksumSHA256 == wantedChecksum {
		claim.CompletedParts = []workercontrol.DebugDumpUploadPart{{
			Number: 1, ETag: completed[0].ETag, SizeBytes: completed[0].SizeBytes,
			ChecksumSHA256: completed[0].ChecksumSHA256,
		}}
		return debugDumpUploadClaimMessage(claim, nil, true)
	}
	signed, err := server.uploadStore.PresignUploadPart(
		ctx, upload, 1, part.SizeBytes, part.SHA256, claim.UploadExpiresAt,
	)
	if err != nil {
		return nil, err
	}
	if signed.Method != http.MethodPut || signed.URL == "" || signed.ExpiresAt.After(claim.UploadExpiresAt) ||
		signed.Headers.Get("Content-Length") != strconv.FormatInt(part.SizeBytes, 10) ||
		signed.Headers.Get("X-Amz-Checksum-Sha256") != wantedChecksum {
		return nil, errors.New("signed debug dump upload part does not match its intent")
	}
	return debugDumpUploadClaimMessage(claim, &signedDebugDumpPart{
		intent: *part, signed: signed,
	}, false)
}

func (server *Server) completeDebugDumpMultipartUpload(
	ctx context.Context,
	worker workercontrol.AuthenticatedWorker,
	credentials workercontrol.LeaseCredentials,
	debugDumpID, authorizationID uuid.UUID,
	report workercontrol.DebugDumpUploadReport,
	completed []artifactstore.CompletedPart,
) (workercontrol.DebugDumpUploadResult, error) {
	status, err := server.debugDumps.InspectDebugDumpUpload(ctx, worker, credentials, debugDumpID)
	if err != nil {
		return workercontrol.DebugDumpUploadResult{}, err
	}
	if status.Decision != workercontrol.DebugDumpUploadRecorded {
		return workercontrol.DebugDumpUploadResult{Decision: workercontrol.DebugDumpUploadRejected}, nil
	}
	if status.DebugDumpID != debugDumpID || status.AuthorizationID != authorizationID ||
		status.ObjectKey == "" || status.ExpectedSizeBytes != report.SizeBytes ||
		status.ExpectedSHA256 != report.SHA256 || status.ExpectedContentType != report.ContentType {
		return workercontrol.DebugDumpUploadResult{Decision: workercontrol.DebugDumpUploadConflict}, nil
	}
	if status.State == workercontrol.DebugDumpUploadStateAvailable {
		if status.ObjectVersionID == "" {
			return workercontrol.DebugDumpUploadResult{}, errors.New("available debug dump has no object version")
		}
		report.ObjectVersionID = status.ObjectVersionID
		return server.debugDumps.RecordDebugDumpUploaded(
			ctx, worker, credentials, debugDumpID, report,
		)
	}
	if status.State != workercontrol.DebugDumpUploadStateUploading || status.MultipartUploadID == "" {
		return workercontrol.DebugDumpUploadResult{Decision: workercontrol.DebugDumpUploadConflict}, nil
	}
	upload := artifactstore.MultipartUpload{
		ObjectKey: status.ObjectKey, UploadID: status.MultipartUploadID,
		ContentType: status.ExpectedContentType,
	}
	listed, err := server.uploadStore.ListParts(ctx, upload)
	if err != nil {
		return workercontrol.DebugDumpUploadResult{}, err
	}
	if !artifactstore.EqualCompletedParts(listed, completed) {
		return workercontrol.DebugDumpUploadResult{Decision: workercontrol.DebugDumpUploadConflict}, nil
	}
	intent, err := server.debugDumps.RecordDebugDumpCompletionIntent(
		ctx, worker, credentials, debugDumpID, report,
	)
	if err != nil {
		return workercontrol.DebugDumpUploadResult{}, err
	}
	if intent.Decision != workercontrol.DebugDumpCompletionIntentRecorded ||
		intent.DebugDumpID != debugDumpID || intent.AuthorizationID != authorizationID {
		if intent.Decision == workercontrol.DebugDumpCompletionIntentRejected {
			return workercontrol.DebugDumpUploadResult{Decision: workercontrol.DebugDumpUploadRejected}, nil
		}
		return workercontrol.DebugDumpUploadResult{Decision: workercontrol.DebugDumpUploadConflict}, nil
	}
	objectVersion, completeErr := server.uploadStore.CompleteMultipartUpload(ctx, upload, completed)
	if completeErr != nil {
		objectVersion, err = server.uploadStore.HeadCurrentVersion(ctx, status.ObjectKey)
		if err != nil {
			return workercontrol.DebugDumpUploadResult{}, errors.Join(
				completeErr, fmt.Errorf("recover completed debug dump object version: %w", err),
			)
		}
	}
	if objectVersion.ObjectKey != status.ObjectKey || objectVersion.VersionID == "" ||
		objectVersion.SizeBytes != report.SizeBytes || objectVersion.ContentType != report.ContentType {
		return workercontrol.DebugDumpUploadResult{Decision: workercontrol.DebugDumpUploadConflict}, nil
	}
	expectedChecksum, err := artifactstore.MultipartCompositeChecksum(completed)
	if err != nil {
		return workercontrol.DebugDumpUploadResult{}, err
	}
	if objectVersion.ChecksumSHA256 != expectedChecksum {
		return workercontrol.DebugDumpUploadResult{Decision: workercontrol.DebugDumpUploadConflict}, nil
	}
	report.ObjectVersionID = objectVersion.VersionID
	return server.debugDumps.RecordDebugDumpUploaded(ctx, worker, credentials, debugDumpID, report)
}

type signedDebugDumpPart struct {
	intent DebugDumpUploadPartIntent
	signed artifactstore.SignedUploadPart
}

func debugDumpUploadClaimMessage(
	claim workercontrol.DebugDumpUploadClaim,
	signed *signedDebugDumpPart,
	partAlreadyUploaded bool,
) (*velav1.DebugDumpUploadClaim, error) {
	message := &velav1.DebugDumpUploadClaim{
		Decision: string(claim.Decision), ClaimId: optionalUUID(claim.ClaimID),
		DebugDumpId:     optionalUUID(claim.DebugDumpID),
		AuthorizationId: optionalUUID(claim.AuthorizationID), ObjectKey: claim.ObjectKey,
		ExpectedSizeBytes: claim.ExpectedSizeBytes, ExpectedSha256: claim.ExpectedSHA256[:],
		ExpectedContentType: claim.ExpectedContentType,
		ClaimExpiresAt:      timestamp(claim.ClaimExpiresAt), UploadExpiresAt: timestamp(claim.UploadExpiresAt),
		Version: claim.Version, MultipartUploadId: claim.MultipartUploadID,
		CompletedParts:      debugDumpPartReports(claim.CompletedParts),
		PartAlreadyUploaded: partAlreadyUploaded,
	}
	if signed == nil {
		return message, nil
	}
	requiredHeaders, err := transferableSignedUploadHeaders(signed.signed)
	if err != nil {
		return nil, fmt.Errorf("validate signed debug dump upload part headers: %w", err)
	}
	message.UploadPart = &velav1.SignedDebugDumpUploadPart{
		Number: signed.intent.Number, SizeBytes: signed.intent.SizeBytes,
		Sha256: signed.intent.SHA256[:], Url: signed.signed.URL,
		RequiredHeaders: requiredHeaders, ExpiresAt: timestamp(signed.signed.ExpiresAt),
	}
	return message, nil
}

func debugDumpMultipartSession(
	result workercontrol.DebugDumpMultipartSession,
) *velav1.DebugDumpMultipartSession {
	return &velav1.DebugDumpMultipartSession{
		Decision: string(result.Decision), DebugDumpId: optionalUUID(result.DebugDumpID),
		AuthorizationId:   optionalUUID(result.AuthorizationID),
		MultipartUploadId: result.MultipartUploadID, Version: result.Version,
	}
}

func debugDumpUploadResult(result workercontrol.DebugDumpUploadResult) *velav1.DebugDumpUploadResult {
	return &velav1.DebugDumpUploadResult{
		Decision: string(result.Decision), DebugDumpId: optionalUUID(result.DebugDumpID),
		AuthorizationId: optionalUUID(result.AuthorizationID),
		ObjectVersionId: result.ObjectVersionID, Version: result.Version,
	}
}

func debugDumpPartReports(parts []workercontrol.DebugDumpUploadPart) []*velav1.DebugDumpUploadPartReport {
	reports := make([]*velav1.DebugDumpUploadPartReport, len(parts))
	for index, part := range parts {
		checksum, _ := base64.StdEncoding.DecodeString(part.ChecksumSHA256)
		reports[index] = &velav1.DebugDumpUploadPartReport{
			Number: part.Number, Etag: part.ETag, SizeBytes: part.SizeBytes,
			ChecksumSha256: checksum,
		}
	}
	return reports
}

func protoCompletedDebugDumpParts(
	report workercontrol.DebugDumpUploadReport,
) ([]*velav1.DebugDumpUploadPartReport, error) {
	if report.ObjectVersionID != "" || report.SizeBytes <= 0 ||
		report.SizeBytes > debugdumpcontract.MaxBytes || report.SHA256 == [sha256.Size]byte{} ||
		report.ContentType != debugDumpUploadContentType || len(report.CompletedParts) != 1 {
		return nil, errors.New("debug dump multipart completion receipt is invalid")
	}
	part := report.CompletedParts[0]
	checksum, err := base64.StdEncoding.DecodeString(part.ChecksumSHA256)
	if err != nil || part.Number != 1 || part.SizeBytes != report.SizeBytes || part.ETag == "" ||
		len(checksum) != sha256.Size || !equalDigest(checksum, report.SHA256) {
		return nil, errors.New("debug dump multipart completed part is invalid")
	}
	return []*velav1.DebugDumpUploadPartReport{{
		Number: 1, Etag: part.ETag, SizeBytes: part.SizeBytes, ChecksumSha256: checksum,
	}}, nil
}

func parseDebugDumpUploadClaim(
	message *velav1.DebugDumpUploadClaim,
	want workercontrol.DebugDumpUploadIntent,
	wantClaimID uuid.UUID,
	wantPart DebugDumpUploadPartIntent,
) (DebugDumpUploadClaim, error) {
	if message == nil {
		return DebugDumpUploadClaim{}, errors.New("worker control response omitted debug dump upload claim")
	}
	decision := workercontrol.DebugDumpUploadClaimDecision(message.GetDecision())
	if decision == workercontrol.DebugDumpUploadClaimConflict ||
		decision == workercontrol.DebugDumpUploadClaimRejected {
		return DebugDumpUploadClaim{DebugDumpUploadClaim: workercontrol.DebugDumpUploadClaim{
			Decision: decision,
		}}, nil
	}
	if decision != workercontrol.DebugDumpUploadClaimGranted {
		return DebugDumpUploadClaim{}, errors.New("worker control debug dump upload decision is invalid")
	}
	claimID, claimErr := requiredUUID(message.GetClaimId())
	debugDumpID, dumpErr := requiredUUID(message.GetDebugDumpId())
	authorizationID, authorizationErr := requiredUUID(message.GetAuthorizationId())
	claimExpiresAt, claimExpiryErr := requiredTimestamp(message.GetClaimExpiresAt())
	uploadExpiresAt, uploadExpiryErr := requiredTimestamp(message.GetUploadExpiresAt())
	var expectedDigest [sha256.Size]byte
	copy(expectedDigest[:], message.GetExpectedSha256())
	objectKey := message.GetObjectKey()
	if claimErr != nil || dumpErr != nil || authorizationErr != nil || claimExpiryErr != nil ||
		uploadExpiryErr != nil || claimID != wantClaimID || debugDumpID != want.DebugDumpID ||
		authorizationID != want.AuthorizationID || message.GetExpectedSizeBytes() != want.SizeBytes ||
		expectedDigest != want.SHA256 || message.GetExpectedContentType() != want.ContentType ||
		!strings.HasPrefix(objectKey, "debug-dumps/") || path.IsAbs(objectKey) ||
		path.Clean(objectKey) != objectKey || message.GetMultipartUploadId() == "" ||
		message.GetVersion() <= 0 || claimExpiresAt.After(uploadExpiresAt) {
		return DebugDumpUploadClaim{}, errors.New("worker control debug dump upload claim authority is invalid")
	}
	parts, err := parseCompletedDebugDumpParts(message.GetCompletedParts())
	if err != nil {
		return DebugDumpUploadClaim{}, err
	}
	var signed *SignedDebugDumpUploadPart
	if message.GetUploadPart() != nil {
		signed, err = parseSignedDebugDumpPart(message.GetUploadPart(), wantPart, uploadExpiresAt)
		if err != nil {
			return DebugDumpUploadClaim{}, err
		}
	}
	if message.GetPartAlreadyUploaded() {
		if signed != nil || len(parts) != 1 || parts[0].Number != 1 ||
			parts[0].SizeBytes != wantPart.SizeBytes {
			return DebugDumpUploadClaim{}, errors.New("worker control debug dump part receipt is invalid")
		}
	} else if signed == nil {
		return DebugDumpUploadClaim{}, errors.New("worker control debug dump claim omitted its signed part")
	}
	return DebugDumpUploadClaim{
		DebugDumpUploadClaim: workercontrol.DebugDumpUploadClaim{
			Decision: decision, ClaimID: claimID, DebugDumpID: debugDumpID,
			AuthorizationID: authorizationID, ObjectKey: objectKey,
			ExpectedSizeBytes: message.GetExpectedSizeBytes(), ExpectedSHA256: expectedDigest,
			ExpectedContentType: message.GetExpectedContentType(),
			MultipartUploadID:   message.GetMultipartUploadId(), CompletedParts: parts,
			ClaimExpiresAt: claimExpiresAt, UploadExpiresAt: uploadExpiresAt,
			Version: message.GetVersion(),
		},
		UploadPart: signed, PartAlreadyUploaded: message.GetPartAlreadyUploaded(),
	}, nil
}

func parseSignedDebugDumpPart(
	message *velav1.SignedDebugDumpUploadPart,
	want DebugDumpUploadPartIntent,
	uploadExpiresAt time.Time,
) (*SignedDebugDumpUploadPart, error) {
	if message == nil || message.GetNumber() != want.Number ||
		message.GetSizeBytes() != want.SizeBytes || !equalDigest(message.GetSha256(), want.SHA256) {
		return nil, errors.New("signed debug dump upload part changed its intent")
	}
	expiresAt, err := requiredTimestamp(message.GetExpiresAt())
	if err != nil || expiresAt.After(uploadExpiresAt) {
		return nil, errors.New("signed debug dump upload part expiry is invalid")
	}
	parsedURL := message.GetUrl()
	if parsedURL == "" || len(message.GetRequiredHeaders()) == 0 {
		return nil, errors.New("signed debug dump upload part is incomplete")
	}
	return &SignedDebugDumpUploadPart{
		Number: want.Number, SizeBytes: want.SizeBytes, SHA256: want.SHA256,
		URL: parsedURL, RequiredHeaders: cloneStringMap(message.GetRequiredHeaders()),
		ExpiresAt: expiresAt,
	}, nil
}

func parseCompletedDebugDumpParts(
	messages []*velav1.DebugDumpUploadPartReport,
) ([]workercontrol.DebugDumpUploadPart, error) {
	if len(messages) > 1 {
		return nil, errors.New("worker control debug dump upload has too many completed parts")
	}
	parts := make([]workercontrol.DebugDumpUploadPart, len(messages))
	for index, message := range messages {
		if message == nil || message.GetNumber() != 1 || message.GetSizeBytes() <= 0 ||
			message.GetSizeBytes() > debugdumpcontract.MaxBytes || message.GetEtag() == "" ||
			len(message.GetChecksumSha256()) != sha256.Size {
			return nil, errors.New("worker control completed debug dump part is invalid")
		}
		parts[index] = workercontrol.DebugDumpUploadPart{
			Number: 1, ETag: message.GetEtag(), SizeBytes: message.GetSizeBytes(),
			ChecksumSHA256: base64.StdEncoding.EncodeToString(message.GetChecksumSha256()),
		}
	}
	return parts, nil
}

func parseDebugDumpUploadResult(
	message *velav1.DebugDumpUploadResult,
	wantDumpID, wantAuthorizationID uuid.UUID,
) (workercontrol.DebugDumpUploadResult, error) {
	if message == nil {
		return workercontrol.DebugDumpUploadResult{}, errors.New("worker control response omitted debug dump upload result")
	}
	decision := workercontrol.DebugDumpUploadDecision(message.GetDecision())
	if decision == workercontrol.DebugDumpUploadConflict || decision == workercontrol.DebugDumpUploadRejected {
		return workercontrol.DebugDumpUploadResult{Decision: decision}, nil
	}
	if decision != workercontrol.DebugDumpUploadRecorded {
		return workercontrol.DebugDumpUploadResult{}, errors.New("worker control debug dump upload decision is invalid")
	}
	debugDumpID, dumpErr := requiredUUID(message.GetDebugDumpId())
	authorizationID, authorizationErr := requiredUUID(message.GetAuthorizationId())
	if dumpErr != nil || authorizationErr != nil || debugDumpID != wantDumpID ||
		authorizationID != wantAuthorizationID || message.GetObjectVersionId() == "" ||
		message.GetVersion() <= 0 {
		return workercontrol.DebugDumpUploadResult{}, errors.New("worker control debug dump receipt is invalid")
	}
	return workercontrol.DebugDumpUploadResult{
		Decision: decision, DebugDumpID: debugDumpID, AuthorizationID: authorizationID,
		ObjectVersionID: message.GetObjectVersionId(), Version: message.GetVersion(),
	}, nil
}

func equalDigest(raw []byte, digest [sha256.Size]byte) bool {
	return len(raw) == sha256.Size && string(raw) == string(digest[:])
}

func cloneStringMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

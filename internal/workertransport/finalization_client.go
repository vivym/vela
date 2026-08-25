package workertransport

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"math"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

type ArtifactUploadPartIntent struct {
	Number    int32
	SizeBytes int64
	SHA256    [sha256.Size]byte
}

type SignedArtifactUploadPart struct {
	Number          int32
	SizeBytes       int64
	SHA256          [sha256.Size]byte
	URL             string
	RequiredHeaders map[string]string
	ExpiresAt       time.Time
}

type ArtifactUploadClaim struct {
	workercontrol.ArtifactUploadClaim
	CompletedParts      []workercontrol.ArtifactUploadPart
	UploadPart          *SignedArtifactUploadPart
	PartAlreadyUploaded bool
}

func (client *Client) BeginFinalization(
	ctx context.Context,
	credentials workercontrol.LeaseCredentials,
) (workercontrol.FinalizationPlan, error) {
	lease, err := protoLeaseCredentials(credentials)
	if err != nil {
		return workercontrol.FinalizationPlan{}, err
	}
	response, err := client.exchange(ctx, &velav1.ConnectRequest{
		Operation: &velav1.ConnectRequest_BeginFinalization{
			BeginFinalization: &velav1.BeginFinalizationRequest{Lease: lease},
		},
	})
	if err != nil {
		return workercontrol.FinalizationPlan{}, err
	}
	return parseFinalizationPlan(response.GetFinalizationPlan())
}

func (client *Client) ClaimArtifactUpload(
	ctx context.Context,
	credentials workercontrol.LeaseCredentials,
	uploadID uuid.UUID,
	claimID uuid.UUID,
	part ArtifactUploadPartIntent,
) (ArtifactUploadClaim, error) {
	lease, err := protoLeaseCredentials(credentials)
	if err != nil {
		return ArtifactUploadClaim{}, err
	}
	if uploadID == uuid.Nil || claimID == uuid.Nil || part.Number <= 0 || part.Number > 10_000 ||
		part.SizeBytes <= 0 || part.SHA256 == [sha256.Size]byte{} {
		return ArtifactUploadClaim{}, errors.New("artifact upload claim intent is invalid")
	}
	response, err := client.exchange(ctx, &velav1.ConnectRequest{
		Operation: &velav1.ConnectRequest_ClaimArtifactUpload{
			ClaimArtifactUpload: &velav1.ClaimArtifactUploadRequest{
				Lease: lease, UploadId: uploadID.String(), ClaimId: claimID.String(),
				Part: &velav1.ArtifactUploadPartIntent{
					Number: part.Number, SizeBytes: part.SizeBytes,
					Sha256: append([]byte(nil), part.SHA256[:]...),
				},
			},
		},
	})
	if err != nil {
		return ArtifactUploadClaim{}, err
	}
	return parseArtifactUploadClaim(response.GetArtifactUploadClaim(), uploadID, claimID, part)
}

func (client *Client) CompleteArtifactMultipartUpload(
	ctx context.Context,
	credentials workercontrol.LeaseCredentials,
	uploadID uuid.UUID,
	claimID uuid.UUID,
	report workercontrol.ArtifactUploadReport,
) (workercontrol.ArtifactUploadResult, error) {
	lease, err := protoLeaseCredentials(credentials)
	if err != nil {
		return workercontrol.ArtifactUploadResult{}, err
	}
	parts, err := protoCompletedArtifactParts(report)
	if err != nil || uploadID == uuid.Nil || claimID == uuid.Nil {
		return workercontrol.ArtifactUploadResult{}, errors.New("artifact multipart completion receipt is invalid")
	}
	response, err := client.exchange(ctx, &velav1.ConnectRequest{
		Operation: &velav1.ConnectRequest_CompleteArtifactMultipartUpload{
			CompleteArtifactMultipartUpload: &velav1.CompleteArtifactMultipartUploadRequest{
				Lease: lease, UploadId: uploadID.String(), ClaimId: claimID.String(),
				SizeBytes: report.SizeBytes, Sha256: append([]byte(nil), report.SHA256[:]...),
				ContentType: report.ContentType, CompletedParts: parts,
			},
		},
	})
	if err != nil {
		return workercontrol.ArtifactUploadResult{}, err
	}
	return parseArtifactUploadResult(response.GetArtifactUploadResult(), uploadID)
}

func (client *Client) VerifyArtifact(
	ctx context.Context,
	credentials workercontrol.LeaseCredentials,
	uploadID uuid.UUID,
	verificationID uuid.UUID,
) (workercontrol.ArtifactVerificationResult, error) {
	lease, err := protoLeaseCredentials(credentials)
	if err != nil {
		return workercontrol.ArtifactVerificationResult{}, err
	}
	if uploadID == uuid.Nil || verificationID == uuid.Nil {
		return workercontrol.ArtifactVerificationResult{}, errors.New("artifact verification identity is invalid")
	}
	response, err := client.exchange(ctx, &velav1.ConnectRequest{
		Operation: &velav1.ConnectRequest_VerifyArtifact{
			VerifyArtifact: &velav1.VerifyArtifactRequest{
				Lease: lease, UploadId: uploadID.String(), VerificationId: verificationID.String(),
			},
		},
	})
	if err != nil {
		return workercontrol.ArtifactVerificationResult{}, err
	}
	return parseArtifactVerificationResult(
		response.GetArtifactVerificationResult(), uploadID, verificationID,
	)
}

func (client *Client) CompleteVisibleCompletion(
	ctx context.Context,
	credentials workercontrol.LeaseCredentials,
	candidate workercontrol.VisibleCompletionCandidate,
) (workercontrol.VisibleCompletionResult, error) {
	lease, err := protoLeaseCredentials(credentials)
	if err != nil {
		return workercontrol.VisibleCompletionResult{}, err
	}
	if candidate.CompletionID == uuid.Nil || candidate.ExpectedJobVersion <= 0 ||
		candidate.ExpectedJobVersion == math.MaxInt64 || len(candidate.ArtifactIDs) == 0 ||
		len(candidate.ArtifactIDs) > 32 {
		return workercontrol.VisibleCompletionResult{}, errors.New("visible Completion candidate is invalid")
	}
	artifactIDs := make([]string, len(candidate.ArtifactIDs))
	seen := make(map[uuid.UUID]struct{}, len(candidate.ArtifactIDs))
	for index, artifactID := range candidate.ArtifactIDs {
		if artifactID == uuid.Nil {
			return workercontrol.VisibleCompletionResult{}, errors.New("visible Completion Artifact identity is invalid")
		}
		if _, exists := seen[artifactID]; exists {
			return workercontrol.VisibleCompletionResult{}, errors.New("visible Completion Artifact identities are not unique")
		}
		seen[artifactID] = struct{}{}
		artifactIDs[index] = artifactID.String()
	}
	response, err := client.exchange(ctx, &velav1.ConnectRequest{
		Operation: &velav1.ConnectRequest_CompleteVisibleCompletion{
			CompleteVisibleCompletion: &velav1.CompleteVisibleCompletionRequest{
				Lease: lease,
				Candidate: &velav1.VisibleCompletionCandidate{
					CompletionId:       candidate.CompletionID.String(),
					ExpectedJobVersion: candidate.ExpectedJobVersion,
					ArtifactIds:        artifactIDs,
				},
			},
		},
	})
	if err != nil {
		return workercontrol.VisibleCompletionResult{}, err
	}
	return parseVisibleCompletionResult(response.GetVisibleCompletionResult(), credentials, candidate)
}

func parseVisibleCompletionResult(
	message *velav1.VisibleCompletionResult,
	credentials workercontrol.LeaseCredentials,
	candidate workercontrol.VisibleCompletionCandidate,
) (workercontrol.VisibleCompletionResult, error) {
	if message == nil {
		return workercontrol.VisibleCompletionResult{}, errors.New("worker control response omitted Visible Completion result")
	}
	decision := workercontrol.VisibleCompletionDecision(message.GetDecision())
	switch decision {
	case workercontrol.VisibleCompletionCancellationWon,
		workercontrol.VisibleCompletionAlreadyFailed,
		workercontrol.VisibleCompletionCandidateConflict,
		workercontrol.VisibleCompletionIncompleteArtifact,
		workercontrol.VisibleCompletionRejectedStaleLease:
		return workercontrol.VisibleCompletionResult{Decision: decision}, nil
	case workercontrol.VisibleCompletionCommitted, workercontrol.VisibleCompletionAlreadySucceeded:
	default:
		return workercontrol.VisibleCompletionResult{}, errors.New("worker control Visible Completion decision is invalid")
	}
	completionID, err := requiredUUID(message.GetCompletionId())
	if err != nil || (decision == workercontrol.VisibleCompletionCommitted && completionID != candidate.CompletionID) {
		return workercontrol.VisibleCompletionResult{}, errors.New("worker control Visible Completion identity is invalid")
	}
	jobID, err := requiredUUID(message.GetJobId())
	if err != nil {
		return workercontrol.VisibleCompletionResult{}, err
	}
	attemptID, err := requiredUUID(message.GetAttemptId())
	if err != nil || attemptID != credentials.AttemptID {
		return workercontrol.VisibleCompletionResult{}, errors.New("worker control Visible Completion Attempt is invalid")
	}
	artifactSetID, err := requiredUUID(message.GetArtifactSetId())
	if err != nil {
		return workercontrol.VisibleCompletionResult{}, err
	}
	chargeID, err := requiredUUID(message.GetChargeId())
	if err != nil {
		return workercontrol.VisibleCompletionResult{}, err
	}
	completedAt, err := requiredTimestamp(message.GetCompletedAt())
	if err != nil {
		return workercontrol.VisibleCompletionResult{}, err
	}
	if message.GetJobVersion() != candidate.ExpectedJobVersion+1 ||
		len(message.GetManifestSha256()) != sha256.Size || len(message.GetArtifacts()) == 0 ||
		len(message.GetArtifacts()) > 32 {
		return workercontrol.VisibleCompletionResult{}, errors.New("worker control Visible Completion receipt is incomplete")
	}
	var manifest [sha256.Size]byte
	copy(manifest[:], message.GetManifestSha256())
	if manifest == [sha256.Size]byte{} {
		return workercontrol.VisibleCompletionResult{}, errors.New("worker control Visible Completion manifest is invalid")
	}
	artifacts := make([]workercontrol.CommittedArtifact, len(message.GetArtifacts()))
	candidateArtifacts := make(map[uuid.UUID]struct{}, len(candidate.ArtifactIDs))
	for _, artifactID := range candidate.ArtifactIDs {
		candidateArtifacts[artifactID] = struct{}{}
	}
	seenArtifacts := make(map[uuid.UUID]struct{}, len(artifacts))
	seenIdentities := make(map[string]struct{}, len(artifacts))
	seenKeys := make(map[string]struct{}, len(artifacts))
	for index, messageArtifact := range message.GetArtifacts() {
		if messageArtifact == nil {
			return workercontrol.VisibleCompletionResult{}, errors.New("worker control committed Artifact is absent")
		}
		artifactID, artifactErr := requiredUUID(messageArtifact.GetArtifactId())
		kind := workercontrol.ArtifactKind(messageArtifact.GetKind())
		objectKey := messageArtifact.GetObjectKey()
		objectVersionID := messageArtifact.GetObjectVersionId()
		contentType, parameters, contentTypeErr := mime.ParseMediaType(messageArtifact.GetContentType())
		var digest [sha256.Size]byte
		copy(digest[:], messageArtifact.GetSha256())
		identity := string(kind) + "/" + strconv.FormatInt(int64(messageArtifact.GetOrdinal()), 10)
		if artifactErr != nil || (kind != workercontrol.ArtifactKindVideo && kind != workercontrol.ArtifactKindThumbnail) ||
			messageArtifact.GetOrdinal() < 0 || len(objectKey) == 0 || len(objectKey) > 2048 ||
			path.IsAbs(objectKey) || path.Clean(objectKey) != objectKey || !strings.HasPrefix(objectKey, "artifacts/") ||
			objectVersionID == "" || len(objectVersionID) > 2000 || strings.ContainsRune(objectVersionID, '\x00') ||
			messageArtifact.GetSizeBytes() <= 0 || len(messageArtifact.GetSha256()) != sha256.Size ||
			digest == [sha256.Size]byte{} || contentTypeErr != nil || len(parameters) != 0 ||
			contentType != messageArtifact.GetContentType() {
			return workercontrol.VisibleCompletionResult{}, errors.New("worker control committed Artifact receipt is invalid")
		}
		if _, exists := seenArtifacts[artifactID]; exists {
			return workercontrol.VisibleCompletionResult{}, errors.New("worker control committed Artifact identities are not unique")
		}
		if _, expected := candidateArtifacts[artifactID]; !expected {
			return workercontrol.VisibleCompletionResult{}, errors.New("worker control committed Artifact is outside the candidate set")
		}
		if _, exists := seenIdentities[identity]; exists {
			return workercontrol.VisibleCompletionResult{}, errors.New("worker control committed Artifact identities are not unique")
		}
		if _, exists := seenKeys[objectKey]; exists {
			return workercontrol.VisibleCompletionResult{}, errors.New("worker control committed Artifact object keys are not unique")
		}
		seenArtifacts[artifactID] = struct{}{}
		delete(candidateArtifacts, artifactID)
		seenIdentities[identity] = struct{}{}
		seenKeys[objectKey] = struct{}{}
		artifacts[index] = workercontrol.CommittedArtifact{
			ArtifactID: artifactID, Kind: kind, Ordinal: messageArtifact.GetOrdinal(),
			ObjectKey: objectKey, ObjectVersionID: objectVersionID,
			SizeBytes: messageArtifact.GetSizeBytes(), SHA256: digest, ContentType: contentType,
		}
	}
	if len(candidateArtifacts) != 0 {
		return workercontrol.VisibleCompletionResult{}, errors.New("worker control Visible Completion Artifact set changed")
	}
	return workercontrol.VisibleCompletionResult{
		Decision: decision, CompletionID: completionID, JobID: jobID, AttemptID: attemptID,
		ArtifactSetID: artifactSetID, ChargeID: chargeID, JobVersion: message.GetJobVersion(),
		ManifestSHA256: manifest, Artifacts: artifacts, CompletedAt: completedAt,
	}, nil
}

func parseArtifactVerificationResult(
	message *velav1.ArtifactVerificationResult,
	wantUploadID uuid.UUID,
	wantVerificationID uuid.UUID,
) (workercontrol.ArtifactVerificationResult, error) {
	if message == nil {
		return workercontrol.ArtifactVerificationResult{}, errors.New("worker control response omitted Artifact verification result")
	}
	decision := workercontrol.ArtifactVerificationDecision(message.GetDecision())
	switch decision {
	case workercontrol.ArtifactVerificationBusy, workercontrol.ArtifactVerificationRejected:
		return workercontrol.ArtifactVerificationResult{Decision: decision}, nil
	case workercontrol.ArtifactValidationFailed, workercontrol.ArtifactVerified:
	default:
		return workercontrol.ArtifactVerificationResult{}, errors.New("worker control Artifact verification decision is invalid")
	}
	verificationID, err := requiredUUID(message.GetVerificationId())
	if err != nil || verificationID != wantVerificationID {
		return workercontrol.ArtifactVerificationResult{}, errors.New("worker control Artifact verification identity is invalid")
	}
	uploadID, err := requiredUUID(message.GetUploadId())
	if err != nil || uploadID != wantUploadID {
		return workercontrol.ArtifactVerificationResult{}, errors.New("worker control verified upload identity is invalid")
	}
	artifactID, err := requiredUUID(message.GetArtifactId())
	if err != nil {
		return workercontrol.ArtifactVerificationResult{}, err
	}
	objectVersionID := message.GetObjectVersionId()
	if objectVersionID == "" || len(objectVersionID) > 2000 || strings.ContainsRune(objectVersionID, '\x00') {
		return workercontrol.ArtifactVerificationResult{}, errors.New("worker control Artifact verification receipt is invalid")
	}
	if decision == workercontrol.ArtifactValidationFailed {
		if message.GetVersion() != 0 || message.GetVerifiedAt() != nil {
			return workercontrol.ArtifactVerificationResult{}, errors.New("worker control validation failure receipt is invalid")
		}
		return workercontrol.ArtifactVerificationResult{
			Decision: decision, VerificationID: verificationID, UploadID: uploadID,
			ArtifactID: artifactID, ObjectVersionID: objectVersionID,
		}, nil
	}
	verifiedAt, err := requiredTimestamp(message.GetVerifiedAt())
	if err != nil || message.GetVersion() <= 0 {
		return workercontrol.ArtifactVerificationResult{}, errors.New("worker control Artifact verification receipt is invalid")
	}
	return workercontrol.ArtifactVerificationResult{
		Decision: decision, VerificationID: verificationID, UploadID: uploadID,
		ArtifactID: artifactID, ObjectVersionID: objectVersionID,
		Version: message.GetVersion(), VerifiedAt: verifiedAt,
	}, nil
}

func protoCompletedArtifactParts(
	report workercontrol.ArtifactUploadReport,
) ([]*velav1.ArtifactUploadPartReport, error) {
	contentType, parameters, err := mime.ParseMediaType(report.ContentType)
	if report.ObjectVersionID != "" || report.SizeBytes <= 0 ||
		report.SHA256 == [sha256.Size]byte{} || err != nil || len(parameters) != 0 ||
		contentType != report.ContentType || len(report.CompletedParts) == 0 ||
		len(report.CompletedParts) > 10_000 {
		return nil, errors.New("artifact multipart completion receipt is invalid")
	}
	parts := make([]*velav1.ArtifactUploadPartReport, len(report.CompletedParts))
	var totalSize int64
	for index, part := range report.CompletedParts {
		checksum, decodeErr := base64.StdEncoding.DecodeString(part.ChecksumSHA256)
		if part.Number != int32(index+1) || part.SizeBytes <= 0 || part.ETag == "" ||
			len(part.ETag) > 1000 || strings.ContainsRune(part.ETag, '\x00') || decodeErr != nil ||
			len(checksum) != sha256.Size || totalSize > report.SizeBytes-part.SizeBytes {
			return nil, errors.New("artifact multipart completed part is invalid")
		}
		totalSize += part.SizeBytes
		parts[index] = &velav1.ArtifactUploadPartReport{
			Number: part.Number, Etag: part.ETag, SizeBytes: part.SizeBytes,
			ChecksumSha256: append([]byte(nil), checksum...),
		}
	}
	if totalSize != report.SizeBytes {
		return nil, errors.New("artifact multipart completed parts do not match object size")
	}
	return parts, nil
}

func parseArtifactUploadResult(
	message *velav1.ArtifactUploadResult,
	wantUploadID uuid.UUID,
) (workercontrol.ArtifactUploadResult, error) {
	if message == nil {
		return workercontrol.ArtifactUploadResult{}, errors.New("worker control response omitted Artifact upload result")
	}
	decision := workercontrol.ArtifactUploadDecision(message.GetDecision())
	if decision == workercontrol.ArtifactUploadConflict || decision == workercontrol.ArtifactUploadRejected {
		return workercontrol.ArtifactUploadResult{Decision: decision}, nil
	}
	if decision != workercontrol.ArtifactUploadRecorded {
		return workercontrol.ArtifactUploadResult{}, errors.New("worker control Artifact upload decision is invalid")
	}
	uploadID, err := requiredUUID(message.GetUploadId())
	if err != nil || uploadID != wantUploadID {
		return workercontrol.ArtifactUploadResult{}, errors.New("worker control Artifact upload result identity is invalid")
	}
	artifactID, err := requiredUUID(message.GetArtifactId())
	if err != nil {
		return workercontrol.ArtifactUploadResult{}, err
	}
	objectVersionID := message.GetObjectVersionId()
	if objectVersionID == "" || len(objectVersionID) > 2000 || strings.ContainsRune(objectVersionID, '\x00') ||
		message.GetVersion() <= 0 {
		return workercontrol.ArtifactUploadResult{}, errors.New("worker control Artifact object version receipt is invalid")
	}
	return workercontrol.ArtifactUploadResult{
		Decision: decision, UploadID: uploadID, ArtifactID: artifactID,
		ObjectVersionID: objectVersionID, Version: message.GetVersion(),
	}, nil
}

func parseArtifactUploadClaim(
	message *velav1.ArtifactUploadClaim,
	wantUploadID uuid.UUID,
	wantClaimID uuid.UUID,
	wantPart ArtifactUploadPartIntent,
) (ArtifactUploadClaim, error) {
	if message == nil {
		return ArtifactUploadClaim{}, errors.New("worker control response omitted Artifact upload claim")
	}
	decision := workercontrol.ArtifactUploadClaimDecision(message.GetDecision())
	if decision == workercontrol.ArtifactUploadClaimBusy ||
		decision == workercontrol.ArtifactUploadClaimRejectedStaleLease {
		return ArtifactUploadClaim{ArtifactUploadClaim: workercontrol.ArtifactUploadClaim{
			Decision: decision,
		}}, nil
	}
	if decision != workercontrol.ArtifactUploadClaimGranted {
		return ArtifactUploadClaim{}, errors.New("worker control Artifact upload claim decision is invalid")
	}
	claimID, err := requiredUUID(message.GetClaimId())
	if err != nil || claimID != wantClaimID {
		return ArtifactUploadClaim{}, errors.New("worker control Artifact upload claim identity is invalid")
	}
	uploadID, err := requiredUUID(message.GetUploadId())
	if err != nil || uploadID != wantUploadID {
		return ArtifactUploadClaim{}, errors.New("worker control Artifact upload identity is invalid")
	}
	artifactID, err := requiredUUID(message.GetArtifactId())
	if err != nil {
		return ArtifactUploadClaim{}, err
	}
	var claimExpiresAt time.Time
	if message.GetClaimExpiresAt() != nil {
		claimExpiresAt, err = requiredTimestamp(message.GetClaimExpiresAt())
		if err != nil {
			return ArtifactUploadClaim{}, err
		}
	}
	uploadExpiresAt, err := requiredTimestamp(message.GetUploadExpiresAt())
	if err != nil {
		return ArtifactUploadClaim{}, err
	}
	contentType, parameters, contentTypeErr := mime.ParseMediaType(message.GetExpectedContentType())
	objectKey := message.GetObjectKey()
	multipartID := message.GetMultipartUploadId()
	if contentTypeErr != nil || len(parameters) != 0 || contentType != message.GetExpectedContentType() ||
		len(objectKey) == 0 || len(objectKey) > 2048 || path.IsAbs(objectKey) ||
		path.Clean(objectKey) != objectKey || !strings.HasPrefix(objectKey, "artifacts/") ||
		multipartID == "" || len(multipartID) > 2000 || strings.ContainsRune(multipartID, '\x00') ||
		message.GetVersion() <= 0 || (!claimExpiresAt.IsZero() && claimExpiresAt.After(uploadExpiresAt)) {
		return ArtifactUploadClaim{}, errors.New("worker control Artifact upload claim authority is invalid")
	}
	completedParts, err := parseCompletedArtifactParts(message.GetCompletedParts())
	if err != nil {
		return ArtifactUploadClaim{}, err
	}
	var signed *SignedArtifactUploadPart
	if message.GetUploadPart() != nil {
		signed, err = parseSignedArtifactUploadPart(message.GetUploadPart(), wantPart, uploadExpiresAt)
		if err != nil {
			return ArtifactUploadClaim{}, err
		}
	}
	if message.GetPartAlreadyUploaded() {
		if signed != nil || !containsExactCompletedPart(completedParts, wantPart) {
			return ArtifactUploadClaim{}, errors.New("worker control completed Artifact part receipt is invalid")
		}
	} else if signed == nil {
		return ArtifactUploadClaim{}, errors.New("worker control Artifact upload claim omitted its signed part")
	}
	return ArtifactUploadClaim{
		ArtifactUploadClaim: workercontrol.ArtifactUploadClaim{
			Decision: decision, ClaimID: claimID, UploadID: uploadID, ArtifactID: artifactID,
			ObjectKey: objectKey, ExpectedContentType: contentType, MultipartUploadID: multipartID,
			ClaimExpiresAt: claimExpiresAt, UploadExpiresAt: uploadExpiresAt, Version: message.GetVersion(),
		},
		CompletedParts: completedParts, UploadPart: signed,
		PartAlreadyUploaded: message.GetPartAlreadyUploaded(),
	}, nil
}

func parseSignedArtifactUploadPart(
	message *velav1.SignedArtifactUploadPart,
	want ArtifactUploadPartIntent,
	uploadExpiresAt time.Time,
) (*SignedArtifactUploadPart, error) {
	if message.GetNumber() != want.Number || message.GetSizeBytes() != want.SizeBytes ||
		len(message.GetSha256()) != sha256.Size || string(message.GetSha256()) != string(want.SHA256[:]) {
		return nil, errors.New("signed Artifact upload part does not match its intent")
	}
	expiresAt, err := requiredTimestamp(message.GetExpiresAt())
	parsedURL, parseErr := url.Parse(message.GetUrl())
	if err != nil || parseErr != nil || expiresAt.After(uploadExpiresAt) ||
		parsedURL.Host == "" || parsedURL.User != nil ||
		(parsedURL.Scheme != "https" && parsedURL.Scheme != "http") || parsedURL.Fragment != "" {
		return nil, errors.New("signed Artifact upload part URL or expiry is invalid")
	}
	headers := make(map[string]string, len(message.GetRequiredHeaders()))
	canonical := make(http.Header, len(message.GetRequiredHeaders()))
	for name, value := range message.GetRequiredHeaders() {
		if strings.TrimSpace(name) == "" || value == "" || strings.ContainsRune(value, '\x00') {
			return nil, errors.New("signed Artifact upload part headers are invalid")
		}
		canonical.Set(name, value)
		headers[http.CanonicalHeaderKey(name)] = value
	}
	if canonical.Get("Content-Length") != strconv.FormatInt(want.SizeBytes, 10) ||
		canonical.Get("X-Amz-Checksum-Sha256") != base64.StdEncoding.EncodeToString(want.SHA256[:]) {
		return nil, errors.New("signed Artifact upload part headers do not bind its payload")
	}
	return &SignedArtifactUploadPart{
		Number: want.Number, SizeBytes: want.SizeBytes, SHA256: want.SHA256,
		URL: message.GetUrl(), RequiredHeaders: headers, ExpiresAt: expiresAt,
	}, nil
}

func parseCompletedArtifactParts(
	messages []*velav1.ArtifactUploadPartReport,
) ([]workercontrol.ArtifactUploadPart, error) {
	if len(messages) > 10_000 {
		return nil, errors.New("worker control returned too many completed Artifact parts")
	}
	parts := make([]workercontrol.ArtifactUploadPart, len(messages))
	var previous int32
	for index, message := range messages {
		if message == nil || message.GetNumber() <= previous || message.GetNumber() > 10_000 ||
			message.GetSizeBytes() <= 0 || message.GetEtag() == "" || len(message.GetEtag()) > 1000 ||
			strings.ContainsRune(message.GetEtag(), '\x00') || len(message.GetChecksumSha256()) != sha256.Size {
			return nil, errors.New("worker control completed Artifact part is invalid")
		}
		previous = message.GetNumber()
		parts[index] = workercontrol.ArtifactUploadPart{
			Number: message.GetNumber(), ETag: message.GetEtag(), SizeBytes: message.GetSizeBytes(),
			ChecksumSHA256: base64.StdEncoding.EncodeToString(message.GetChecksumSha256()),
		}
	}
	return parts, nil
}

func containsExactCompletedPart(
	parts []workercontrol.ArtifactUploadPart,
	want ArtifactUploadPartIntent,
) bool {
	wantChecksum := base64.StdEncoding.EncodeToString(want.SHA256[:])
	for _, part := range parts {
		if part.Number == want.Number {
			return part.SizeBytes == want.SizeBytes && part.ChecksumSHA256 == wantChecksum
		}
	}
	return false
}

func parseFinalizationPlan(message *velav1.FinalizationPlan) (workercontrol.FinalizationPlan, error) {
	if message == nil {
		return workercontrol.FinalizationPlan{}, errors.New("worker control response omitted Finalization plan")
	}
	decision := workercontrol.FinalizationDecision(message.GetDecision())
	if decision == workercontrol.FinalizationRejectedStaleLease {
		return workercontrol.FinalizationPlan{Decision: decision}, nil
	}
	if decision != workercontrol.FinalizationGranted {
		return workercontrol.FinalizationPlan{}, errors.New("worker control Finalization decision is invalid")
	}
	attemptID, err := requiredUUID(message.GetAttemptId())
	if err != nil {
		return workercontrol.FinalizationPlan{}, err
	}
	jobID, err := requiredUUID(message.GetJobId())
	if err != nil {
		return workercontrol.FinalizationPlan{}, err
	}
	startedAt, err := requiredTimestamp(message.GetFinalizationStartedAt())
	if err != nil {
		return workercontrol.FinalizationPlan{}, err
	}
	deadlineAt, err := requiredTimestamp(message.GetFinalizationDeadlineAt())
	if err != nil {
		return workercontrol.FinalizationPlan{}, err
	}
	if message.GetJobVersion() <= 0 || !deadlineAt.After(startedAt) ||
		len(message.GetArtifacts()) == 0 || len(message.GetArtifacts()) > 32 {
		return workercontrol.FinalizationPlan{}, errors.New("worker control Finalization authority is invalid")
	}
	artifacts := make([]workercontrol.PlannedArtifact, len(message.GetArtifacts()))
	artifactIDs := make(map[string]struct{}, len(artifacts))
	uploadIDs := make(map[string]struct{}, len(artifacts))
	identities := make(map[string]struct{}, len(artifacts))
	objectKeys := make(map[string]struct{}, len(artifacts))
	for index, planned := range message.GetArtifacts() {
		if planned == nil {
			return workercontrol.FinalizationPlan{}, errors.New("worker control Finalization Artifact is absent")
		}
		artifactID, artifactErr := requiredUUID(planned.GetArtifactId())
		if artifactErr != nil {
			return workercontrol.FinalizationPlan{}, artifactErr
		}
		uploadID, uploadErr := requiredUUID(planned.GetUploadId())
		if uploadErr != nil {
			return workercontrol.FinalizationPlan{}, uploadErr
		}
		kind := workercontrol.ArtifactKind(planned.GetKind())
		if kind != workercontrol.ArtifactKindVideo && kind != workercontrol.ArtifactKindThumbnail {
			return workercontrol.FinalizationPlan{}, errors.New("worker control Finalization Artifact kind is invalid")
		}
		objectKey := planned.GetObjectKey()
		if planned.GetOrdinal() < 0 || len(objectKey) == 0 || len(objectKey) > 2048 ||
			strings.ContainsRune(objectKey, '\x00') || path.IsAbs(objectKey) ||
			path.Clean(objectKey) != objectKey || !strings.HasPrefix(objectKey, "artifacts/") {
			return workercontrol.FinalizationPlan{}, errors.New("worker control Finalization Artifact identity is invalid")
		}
		expiresAt, expiresErr := requiredTimestamp(planned.GetExpiresAt())
		if expiresErr != nil || !expiresAt.After(startedAt) {
			return workercontrol.FinalizationPlan{}, errors.New("worker control Finalization Artifact expiry is invalid")
		}
		identity := string(kind) + "/" + strconv.FormatInt(int64(planned.GetOrdinal()), 10)
		for key, set := range map[string]map[string]struct{}{
			artifactID.String(): artifactIDs,
			uploadID.String():   uploadIDs,
			identity:            identities,
			objectKey:           objectKeys,
		} {
			if _, exists := set[key]; exists {
				return workercontrol.FinalizationPlan{}, errors.New("worker control Finalization Artifact identities are not unique")
			}
			set[key] = struct{}{}
		}
		artifacts[index] = workercontrol.PlannedArtifact{
			ArtifactID: artifactID, UploadID: uploadID, Kind: kind,
			Ordinal: planned.GetOrdinal(), ObjectKey: objectKey, ExpiresAt: expiresAt,
		}
	}
	return workercontrol.FinalizationPlan{
		Decision: decision, AttemptID: attemptID, JobID: jobID, JobVersion: message.GetJobVersion(),
		FinalizationStartedAt: startedAt, FinalizationDeadlineAt: deadlineAt, Artifacts: artifacts,
	}, nil
}

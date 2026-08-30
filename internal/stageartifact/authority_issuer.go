package stageartifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SealRepository interface {
	Seal(context.Context, SealCommand) (MaterializationLease, error)
}

type MaterializationAuthorityIssuer struct {
	repository SealRepository
	signer     *materializationauthority.Signer
	keyID      string
	ttl        time.Duration
}

type IssueMaterializationRequest struct {
	Stage          stageauthority.Verified
	SourceSPIFFEID string
	LocalReceipt   *velav1.LocalMaterializationReceipt
}

type IssuedMaterialization struct {
	Authority *velav1.MaterializationAuthority
	Lease     MaterializationLease
}

func NewMaterializationAuthorityIssuer(
	repository SealRepository,
	signer *materializationauthority.Signer,
	keyID string,
	ttl time.Duration,
) (*MaterializationAuthorityIssuer, error) {
	keyID = strings.TrimSpace(keyID)
	if repository == nil || signer == nil || keyID == "" || len(keyID) > 100 ||
		ttl <= 0 || ttl > 24*time.Hour {
		return nil, errors.New("MaterializationAuthority issuer configuration is invalid")
	}
	return &MaterializationAuthorityIssuer{
		repository: repository, signer: signer, keyID: keyID, ttl: ttl,
	}, nil
}

func (issuer *MaterializationAuthorityIssuer) Seal(
	ctx context.Context,
	request IssueMaterializationRequest,
) (IssuedMaterialization, error) {
	if issuer == nil || issuer.repository == nil || issuer.signer == nil || ctx == nil {
		return IssuedMaterialization{}, errors.New("MaterializationAuthority issuer is not configured")
	}
	stage := request.Stage.Authority
	if stage == nil || len(stage.GetMembers()) != 1 || stage.GetMembers()[0] == nil {
		return IssuedMaterialization{}, errors.New("single-output materialization requires exactly one WorkerMember")
	}
	stageDigest, err := stageauthority.Digest(stage)
	if err != nil || stageDigest != request.Stage.Digest {
		return IssuedMaterialization{}, errors.New("verified StageAuthority digest is inconsistent")
	}
	spiffeID := strings.TrimSpace(request.SourceSPIFFEID)
	if spiffeID == "" || len(spiffeID) > 500 || spiffeID != request.SourceSPIFFEID {
		return IssuedMaterialization{}, errors.New("materialization source SPIFFE identity is invalid")
	}
	receipt := request.LocalReceipt
	if receipt == nil || receipt.GetReceiptId() == "" || len(receipt.GetReceiptId()) > 1000 ||
		len(receipt.GetManifestSha256()) != sha256.Size || receipt.GetTotalSizeBytes() <= 0 ||
		receipt.GetSealedAt() == nil || receipt.GetSealedAt().CheckValid() != nil {
		return IssuedMaterialization{}, errors.New("local materialization receipt is invalid")
	}
	manifestDigest := sha256.Sum256(receipt.GetOutputManifestJson())
	if !bytes.Equal(manifestDigest[:], receipt.GetManifestSha256()) {
		return IssuedMaterialization{}, errors.New("local materialization receipt manifest digest is mismatched")
	}
	manifest, err := ParseLocalOutputManifestV1(receipt.GetOutputManifestJson())
	if err != nil {
		return IssuedMaterialization{}, err
	}
	if manifest.SizeBytes != receipt.GetTotalSizeBytes() ||
		!manifestMatchesStage(manifest, stage) {
		return IssuedMaterialization{}, errors.New("local output manifest does not match StageAuthority or receipt")
	}
	lineageDigest, err := manifest.LineageDigest()
	if err != nil {
		return IssuedMaterialization{}, err
	}
	issuedAt := receipt.GetSealedAt().AsTime().UTC()
	if issuedAt.Before(stage.GetIssuedAt().AsTime().UTC()) ||
		!issuedAt.Before(stage.GetExpiresAt().AsTime().UTC()) {
		return IssuedMaterialization{}, errors.New("local output was sealed outside StageAuthority validity")
	}
	artifactID := deterministicMaterializationID(
		"artifact", stage.GetStageAttemptId(), manifest.OutputPort,
	)
	leaseID := deterministicMaterializationID(
		"lease", stage.GetStageAttemptId(), manifest.OutputPort,
	)
	objectKey := fmt.Sprintf(
		"artifacts/stage/%s/%s/%s/%s.bin",
		stage.GetAttemptId(), stage.GetStageRunId(), stage.GetStageAttemptId(), manifest.OutputPort,
	)
	spiffeDigest := sha256.Sum256([]byte(spiffeID))
	authority, err := issuer.signer.Sign(&velav1.MaterializationAuthority{
		SchemaVersion: 1, StageAuthorityDigest: stageDigest[:],
		StageMaterializationLeaseId: leaseID.String(), StageArtifactId: artifactID.String(),
		ObjectKey: objectKey, ContentType: manifest.ContentType,
		Sha256: manifest.PayloadSHA256[:], SizeBytes: manifest.SizeBytes,
		LocalReceiptId:     receipt.GetReceiptId(),
		LocalReceiptDigest: append([]byte(nil), receipt.GetManifestSha256()...),
		SigningKeyId:       issuer.keyID, IssuedAt: timestamppb.New(issuedAt),
		ExpiresAt:                 timestamppb.New(issuedAt.Add(issuer.ttl)),
		SourceWorkerInstanceId:    stage.GetWorkerInstanceId(),
		SourceWorkerInstanceEpoch: stage.GetWorkerInstanceEpoch(),
		SourceWorkerMemberId:      stage.GetMembers()[0].GetWorkerMemberId(),
		SourceWorkerMemberEpoch:   stage.GetMembers()[0].GetMemberEpoch(),
		SourceSpiffeIdDigest:      spiffeDigest[:],
	})
	if err != nil {
		return IssuedMaterialization{}, fmt.Errorf("sign MaterializationAuthority: %w", err)
	}
	tokenDigest, err := materializationauthority.Digest(authority)
	if err != nil {
		return IssuedMaterialization{}, err
	}
	lease, err := issuer.repository.Seal(ctx, SealCommand{
		CommandID: deterministicMaterializationID(
			"seal-command", stage.GetStageAttemptId(), manifest.OutputPort,
		),
		AttemptID: uuid.MustParse(stage.GetAttemptId()), StageRunID: uuid.MustParse(stage.GetStageRunId()),
		StageAttemptID:       uuid.MustParse(stage.GetStageAttemptId()),
		StageAllocationID:    uuid.MustParse(stage.GetStageAllocationId()),
		StageLeaseID:         uuid.MustParse(stage.GetStageLeaseId()),
		ExpectedAttemptFence: stage.GetAttemptFence(), ExpectedStageFence: stage.GetStageFence(),
		ExpectedStageVersion: stage.GetStageVersion(), OutputPort: manifest.OutputPort,
		LocalReceiptID: receipt.GetReceiptId(), LocalReceiptDigest: manifestDigest,
		ManifestSHA256: manifestDigest, SHA256: manifest.PayloadSHA256,
		LineageDigest: lineageDigest, TokenDigest: tokenDigest, SizeBytes: manifest.SizeBytes,
		ArtifactID: artifactID, MaterializationLeaseID: leaseID,
		ObjectKey: objectKey, ContentType: manifest.ContentType,
		SealedAt: issuedAt, LeaseExpiresAt: issuedAt.Add(issuer.ttl),
	})
	if err != nil {
		return IssuedMaterialization{}, fmt.Errorf("persist MaterializationAuthority: %w", err)
	}
	if lease.ID != leaseID || lease.ArtifactID != artifactID || lease.ObjectKey != objectKey ||
		lease.ContentType != manifest.ContentType || lease.SHA256 != manifest.PayloadSHA256 ||
		lease.TokenDigest != tokenDigest || lease.SizeBytes != manifest.SizeBytes ||
		!lease.IssuedAt.Equal(issuedAt) || !lease.ExpiresAt.Equal(issuedAt.Add(issuer.ttl)) {
		return IssuedMaterialization{}, errors.New("persisted MaterializationAuthority identity is mismatched")
	}
	return IssuedMaterialization{Authority: authority, Lease: lease}, nil
}

func manifestMatchesStage(manifest LocalOutputManifestV1, stage *velav1.StageAuthority) bool {
	lineage := manifest.Lineage
	return lineage.AttemptID.String() == stage.GetAttemptId() &&
		lineage.StageRunID.String() == stage.GetStageRunId() &&
		lineage.StageAttemptID.String() == stage.GetStageAttemptId() &&
		lineage.StageLeaseID.String() == stage.GetStageLeaseId() &&
		lineage.AttemptFence == stage.GetAttemptFence() &&
		lineage.StageFence == stage.GetStageFence() &&
		lineage.StageProfileRevisionID.String() == stage.GetStageProfileRevisionId()
}

func deterministicMaterializationID(kind, stageAttemptID, outputPort string) uuid.UUID {
	return uuid.NewSHA1(
		uuid.NameSpaceOID,
		[]byte("vela/stage-materialization/"+kind+"/"+stageAttemptID+"/"+outputPort),
	)
}

package stageartifact_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMaterializationAuthorityIssuerSealsExactLocalOutputContract(t *testing.T) {
	now := time.Date(2026, 8, 30, 15, 0, 0, 0, time.UTC)
	verified := verifiedStageAuthority(t, now)
	payload := []byte("sealed encoder tensor")
	payloadDigest := sha256.Sum256(payload)
	manifestJSON := exactOutputManifestJSON(t, verified.Authority, payloadDigest, int64(len(payload)))
	manifestDigest := sha256.Sum256(manifestJSON)
	receipt := &velav1.LocalMaterializationReceipt{
		ReceiptId: "encoder-receipt-v1", ManifestSha256: manifestDigest[:],
		TotalSizeBytes: int64(len(payload)), SealedAt: timestamppb.New(now),
		OutputManifestJson: manifestJSON,
	}
	repository := &recordingSealRepository{}
	keys := map[string][]byte{
		"materialization-key-v1": bytes.Repeat([]byte{0xa4}, 32),
		"stage-key-v1":           bytes.Repeat([]byte{0x7c}, 32),
	}
	signer, err := materializationauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	issuer, err := stageartifact.NewMaterializationAuthorityIssuer(
		repository, signer, 30*time.Minute,
	)
	if err != nil {
		t.Fatalf("NewMaterializationAuthorityIssuer: %v", err)
	}

	request := stageartifact.IssueMaterializationRequest{
		Stage: verified, SourceSPIFFEID: "spiffe://vela/worker/member-1",
		LocalReceipt: receipt,
	}
	issued, err := issuer.Seal(context.Background(), request)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if repository.calls != 1 || issued.Lease.ID != repository.command.MaterializationLeaseID ||
		issued.Authority.GetStageArtifactId() != repository.command.ArtifactID.String() ||
		issued.Authority.GetSigningKeyId() != verified.Authority.GetSigningKeyId() ||
		issued.Authority.GetObjectKey() != repository.command.ObjectKey ||
		issued.Authority.GetSourceWorkerInstanceId() != verified.Authority.GetWorkerInstanceId() ||
		issued.Authority.GetSourceWorkerMemberId() != verified.Authority.GetMembers()[0].GetWorkerMemberId() ||
		issued.Authority.GetSourceWorkerMemberEpoch() != verified.Authority.GetMembers()[0].GetMemberEpoch() {
		t.Fatalf("issued=%#v repository=%#v", issued, repository.command)
	}
	authorityDigest, err := materializationauthority.Digest(issued.Authority)
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}
	if repository.command.TokenDigest != authorityDigest ||
		repository.command.ManifestSHA256 != manifestDigest ||
		repository.command.SHA256 != payloadDigest {
		t.Fatalf("Seal command integrity = %#v", repository.command)
	}
	validator, err := materializationauthority.NewValidator(keys, func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	if _, err := validator.Validate(issued.Authority); err != nil {
		t.Fatalf("Validate issued authority: %v", err)
	}
	rotatedKeys := map[string][]byte{
		"stage-key-v1": bytes.Repeat([]byte{0x7c}, 32),
		"stage-key-v2": bytes.Repeat([]byte{0x7d}, 32),
	}
	rotatedSigner, err := materializationauthority.NewSigner(rotatedKeys)
	if err != nil {
		t.Fatalf("construct rotated MaterializationAuthority signer: %v", err)
	}
	rotatedRepository := &recordingSealRepository{}
	rotatedIssuer, err := stageartifact.NewMaterializationAuthorityIssuer(
		rotatedRepository, rotatedSigner, 30*time.Minute,
	)
	if err != nil {
		t.Fatalf("construct rotated MaterializationAuthority issuer: %v", err)
	}
	replayed, err := rotatedIssuer.Seal(context.Background(), request)
	if err != nil {
		t.Fatalf("replay MaterializationAuthority after rotation: %v", err)
	}
	if !proto.Equal(replayed.Authority, issued.Authority) ||
		rotatedRepository.command.TokenDigest != repository.command.TokenDigest {
		t.Fatal("MaterializationAuthority replay changed after active-key rotation")
	}
}

func TestMaterializationAuthorityIssuerRejectsReceiptLineageBeforeSeal(t *testing.T) {
	now := time.Date(2026, 8, 30, 15, 0, 0, 0, time.UTC)
	verified := verifiedStageAuthority(t, now)
	payloadDigest := sha256.Sum256([]byte("sealed encoder tensor"))
	manifestJSON := exactOutputManifestJSON(t, verified.Authority, payloadDigest, 21)
	var document map[string]any
	if err := json.Unmarshal(manifestJSON, &document); err != nil {
		t.Fatalf("Unmarshal manifest: %v", err)
	}
	document["lineage"].(map[string]any)["stage_fence"] = float64(999)
	manifestJSON, _ = json.Marshal(document)
	manifestDigest := sha256.Sum256(manifestJSON)
	repository := &recordingSealRepository{}
	signer, _ := materializationauthority.NewSigner(map[string][]byte{
		"materialization-key-v1": bytes.Repeat([]byte{0xa4}, 32),
		"stage-key-v1":           bytes.Repeat([]byte{0x7c}, 32),
	})
	issuer, _ := stageartifact.NewMaterializationAuthorityIssuer(
		repository, signer, 30*time.Minute,
	)
	_, err := issuer.Seal(context.Background(), stageartifact.IssueMaterializationRequest{
		Stage: verified, SourceSPIFFEID: "spiffe://vela/worker/member-1",
		LocalReceipt: &velav1.LocalMaterializationReceipt{
			ReceiptId: "encoder-receipt-v1", ManifestSha256: manifestDigest[:],
			TotalSizeBytes: 21, SealedAt: timestamppb.New(now),
			OutputManifestJson: manifestJSON,
		},
	})
	if err == nil || repository.calls != 0 {
		t.Fatalf("Seal mismatched lineage error=%v calls=%d", err, repository.calls)
	}
}

type recordingSealRepository struct {
	calls   int
	command stageartifact.SealCommand
}

func (repository *recordingSealRepository) Seal(
	_ context.Context,
	command stageartifact.SealCommand,
) (stageartifact.MaterializationLease, error) {
	repository.calls++
	repository.command = command
	return stageartifact.MaterializationLease{
		ID: command.MaterializationLeaseID, ArtifactID: command.ArtifactID,
		ObjectKey: command.ObjectKey, ContentType: command.ContentType,
		SHA256: command.SHA256, TokenDigest: command.TokenDigest,
		SizeBytes: command.SizeBytes, IssuedAt: command.SealedAt,
		ExpiresAt: command.LeaseExpiresAt,
	}, nil
}

func exactOutputManifestJSON(
	t *testing.T,
	authority *velav1.StageAuthority,
	payloadDigest [sha256.Size]byte,
	sizeBytes int64,
) []byte {
	t.Helper()
	document, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"output_port":    "conditioning", "local_locator": "outputs/encoder.bin",
		"content_type":   "application/x-minimax-h3-encoder",
		"payload_sha256": hex.EncodeToString(payloadDigest[:]), "size_bytes": sizeBytes,
		"lineage": map[string]any{
			"attempt_id": authority.GetAttemptId(), "stage_run_id": authority.GetStageRunId(),
			"stage_attempt_id": authority.GetStageAttemptId(), "stage_lease_id": authority.GetStageLeaseId(),
			"attempt_fence": authority.GetAttemptFence(), "stage_fence": authority.GetStageFence(),
			"stage_profile_revision_id": authority.GetStageProfileRevisionId(),
		},
	})
	if err != nil {
		t.Fatalf("Marshal manifest: %v", err)
	}
	return document
}

func verifiedStageAuthority(t *testing.T, now time.Time) stageauthority.Verified {
	t.Helper()
	keys := map[string][]byte{"stage-key-v1": bytes.Repeat([]byte{0x7c}, 32)}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("New StageAuthority Signer: %v", err)
	}
	authority, err := signer.Sign(&velav1.StageAuthority{
		SchemaVersion: 1,
		JobId:         "13000000-0000-0000-0000-000000000001", AttemptId: "13000000-0000-0000-0000-000000000002",
		StageRunId: "13000000-0000-0000-0000-000000000003", StageAttemptId: "13000000-0000-0000-0000-000000000004",
		StageAllocationId: "13000000-0000-0000-0000-000000000005", StageLeaseId: "13000000-0000-0000-0000-000000000006",
		AttemptFence: 2, StageFence: 3, StageVersion: 4,
		WorkerInstanceId: "23000000-0000-0000-0000-000000000001", WorkerInstanceEpoch: 5,
		DeviceSetDigest: bytes.Repeat([]byte{0x81}, 32),
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "33000000-0000-0000-0000-000000000001", DeviceEpoch: 7,
		}},
		MembershipDigest: bytes.Repeat([]byte{0x82}, 32),
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: "43000000-0000-0000-0000-000000000001", MemberEpoch: 8, ModelRuntimeEpoch: 9,
		}},
		ModelResidencyId: "53000000-0000-0000-0000-000000000001", ModelRuntimeIdentity: "encoder-runtime-1",
		ModelRuntimeBarrierGeneration: 9,
		StageProfileRevisionId:        "63000000-0000-0000-0000-000000000001",
		CapacityObservationSequence:   10,
		CapacityVector:                map[string]int64{"active_stage_slots": 1, "gpu_count": 1},
		LeaseToken:                    bytes.Repeat([]byte{0x83}, 32), ExecutionNonce: bytes.Repeat([]byte{0x84}, 32),
		ExecutionSpecDigest: bytes.Repeat([]byte{0x85}, 32), SigningKeyId: "stage-key-v1",
		IssuedAt: timestamppb.New(now.Add(-time.Minute)), ExpiresAt: timestamppb.New(now.Add(time.Hour)),
		MonotonicValidFor: durationpb.New(61 * time.Minute),
	})
	if err != nil {
		t.Fatalf("Sign StageAuthority: %v", err)
	}
	validator, err := stageauthority.NewValidator(keys, func() time.Time { return now })
	if err != nil {
		t.Fatalf("New StageAuthority Validator: %v", err)
	}
	verified, err := validator.ValidateEnvelope(authority)
	if err != nil {
		t.Fatalf("Validate StageAuthority: %v", err)
	}
	return verified
}

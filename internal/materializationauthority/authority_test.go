package materializationauthority_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/vivym/vela/internal/materializationauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMaterializationAuthorityBindsExactPublicationAndLocalReceipt(t *testing.T) {
	now := time.Date(2026, 8, 30, 14, 0, 0, 0, time.UTC)
	keys := map[string][]byte{"materialization-key-v1": bytes.Repeat([]byte{0xa4}, 32)}
	signer, err := materializationauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	validator, err := materializationauthority.NewValidator(keys, func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}

	signed, err := signer.Sign(materializationAuthority(now))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	verified, err := validator.Validate(signed)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if verified.Digest == ([32]byte{}) || len(signed.GetToken()) != 32 ||
		!proto.Equal(verified.Authority, signed) {
		t.Fatalf("verified = %#v signed=%#v", verified, signed)
	}

	for name, mutate := range map[string]func(*velav1.MaterializationAuthority){
		"artifact": func(authority *velav1.MaterializationAuthority) {
			authority.StageArtifactId = "49600000-0000-0000-0000-000000000099"
		},
		"object key": func(authority *velav1.MaterializationAuthority) {
			authority.ObjectKey += ".forged"
		},
		"payload digest": func(authority *velav1.MaterializationAuthority) {
			authority.Sha256[0] ^= 0xff
		},
		"local receipt": func(authority *velav1.MaterializationAuthority) {
			authority.LocalReceiptId = "forged-receipt"
		},
		"source worker": func(authority *velav1.MaterializationAuthority) {
			authority.SourceWorkerInstanceEpoch++
		},
		"source member": func(authority *velav1.MaterializationAuthority) {
			authority.SourceWorkerMemberEpoch++
		},
		"source identity": func(authority *velav1.MaterializationAuthority) {
			authority.SourceSpiffeIdDigest[0] ^= 0xff
		},
	} {
		t.Run(name, func(t *testing.T) {
			tampered := proto.Clone(signed).(*velav1.MaterializationAuthority)
			mutate(tampered)
			if _, err := validator.Validate(tampered); !errors.Is(
				err, materializationauthority.ErrInvalidSignature,
			) {
				t.Fatalf("Validate tampered error = %v", err)
			}
		})
	}
}

func TestMaterializationAuthorityExpiresIndependentlyOfRevokedStageAuthority(t *testing.T) {
	issuedAt := time.Date(2026, 8, 30, 14, 0, 0, 0, time.UTC)
	keys := map[string][]byte{"materialization-key-v1": bytes.Repeat([]byte{0xa4}, 32)}
	signer, err := materializationauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	signed, err := signer.Sign(materializationAuthority(issuedAt))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	validator, err := materializationauthority.NewValidator(
		keys, func() time.Time { return issuedAt.Add(31 * time.Minute) },
	)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	if _, err := validator.Validate(signed); !errors.Is(err, materializationauthority.ErrStale) {
		t.Fatalf("Validate expired error = %v", err)
	}
}

func materializationAuthority(now time.Time) *velav1.MaterializationAuthority {
	return &velav1.MaterializationAuthority{
		SchemaVersion:               1,
		StageAuthorityDigest:        bytes.Repeat([]byte{0xb1}, 32),
		StageMaterializationLeaseId: "49600000-0000-0000-0000-000000000001",
		StageArtifactId:             "49600000-0000-0000-0000-000000000002",
		ObjectKey:                   "artifacts/stage/org/project/attempt/encoder/output.bin",
		ContentType:                 "application/x-minimax-h3-encoder",
		Sha256:                      bytes.Repeat([]byte{0xb2}, 32),
		SizeBytes:                   4096,
		LocalReceiptId:              "encoder-receipt-v1",
		LocalReceiptDigest:          bytes.Repeat([]byte{0xb3}, 32),
		SigningKeyId:                "materialization-key-v1",
		IssuedAt:                    timestamppb.New(now),
		ExpiresAt:                   timestamppb.New(now.Add(30 * time.Minute)),
		SourceWorkerInstanceId:      "23000000-0000-0000-0000-000000000001",
		SourceWorkerInstanceEpoch:   5,
		SourceWorkerMemberId:        "43000000-0000-0000-0000-000000000001",
		SourceWorkerMemberEpoch:     8,
		SourceSpiffeIdDigest:        bytes.Repeat([]byte{0xb4}, 32),
	}
}

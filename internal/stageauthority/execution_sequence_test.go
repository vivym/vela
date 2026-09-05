package stageauthority_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageAuthorityExecutionSequenceIsSignedAndImmutableAcrossRenewal(t *testing.T) {
	now := time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC)
	keys := map[string][]byte{"stage-key-7": bytes.Repeat([]byte{0x71}, 32)}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := stageauthority.NewValidator(keys, func() time.Time { return now.Add(time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	unsigned := validAuthority(now)
	unsigned.SchemaVersion = 2
	unsigned.ExecutionSequence = 42
	signed, err := signer.Sign(unsigned)
	if err != nil {
		t.Fatalf("sign ordered execution authority: %v", err)
	}
	if _, err := validator.Validate(signed, validBinding()); err != nil {
		t.Fatalf("validate ordered execution authority: %v", err)
	}
	tampered := proto.Clone(signed).(*velav1.StageAuthority)
	tampered.ExecutionSequence++
	if _, err := validator.Validate(tampered, validBinding()); !errors.Is(err, stageauthority.ErrInvalidSignature) {
		t.Fatalf("tampered execution sequence: %v", err)
	}
	renewed := proto.Clone(signed).(*velav1.StageAuthority)
	renewed.IssuedAt = timestamppb.New(now.Add(time.Second))
	renewed.ExpiresAt = timestamppb.New(now.Add(time.Minute))
	renewed.Signature = nil
	renewed, err = signer.Sign(renewed)
	if err != nil {
		t.Fatal(err)
	}
	if err := stageauthority.ValidateRenewal(signed, renewed); err != nil {
		t.Fatalf("unchanged execution sequence renewal: %v", err)
	}
	renewed.ExecutionSequence++
	renewed.Signature = nil
	renewed, err = signer.Sign(renewed)
	if err != nil {
		t.Fatal(err)
	}
	if err := stageauthority.ValidateRenewal(signed, renewed); !errors.Is(err, stageauthority.ErrRenewalMismatch) {
		t.Fatalf("changed execution sequence renewal: %v", err)
	}
	for _, tc := range []struct {
		version  uint32
		sequence int64
	}{{2, 0}, {2, -1}, {1, 42}, {3, 42}} {
		invalid := proto.Clone(unsigned).(*velav1.StageAuthority)
		invalid.SchemaVersion, invalid.ExecutionSequence = tc.version, tc.sequence
		if _, err := signer.Sign(invalid); !errors.Is(err, stageauthority.ErrInvalid) {
			t.Fatalf("schema=%d sequence=%d: %v", tc.version, tc.sequence, err)
		}
	}
	legacy, err := signer.Sign(validAuthority(now))
	if err != nil {
		t.Fatalf("sign legacy identity: %v", err)
	}
	if _, err := validator.ValidateEnvelopeForReplay(legacy, 0); err != nil {
		t.Fatalf("read legacy identity for recovery: %v", err)
	}
}

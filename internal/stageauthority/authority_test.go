package stageauthority_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSignedStageAuthorityValidatesOnlyForExactResidentRuntime(t *testing.T) {
	now := time.Date(2026, 8, 30, 4, 0, 0, 0, time.UTC)
	key := bytes.Repeat([]byte{0x42}, 32)
	signer, err := stageauthority.NewSigner(map[string][]byte{"stage-key-7": key})
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	signed, err := signer.Sign(validAuthority(now))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	validator, err := stageauthority.NewValidator(
		map[string][]byte{"stage-key-7": key},
		func() time.Time { return now.Add(time.Second) },
	)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	verified, err := validator.Validate(signed, validBinding())
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if verified.Authority.GetStageLeaseId() != signed.GetStageLeaseId() ||
		verified.Digest == ([32]byte{}) ||
		verified.MonotonicValidFor != 29*time.Second {
		t.Fatalf("verified authority = %#v", verified)
	}
	secondBinding := validBinding()
	secondBinding.WorkerMemberID = "40000000-0000-0000-0000-000000000002"
	secondBinding.WorkerMemberEpoch = 14
	secondBinding.ModelRuntimeEpoch = 18
	if _, err := validator.Validate(signed, secondBinding); err != nil {
		t.Fatalf("Validate second member with independent runtime epoch: %v", err)
	}

	signed.StageFence++
	if _, err := validator.Validate(signed, validBinding()); !errors.Is(err, stageauthority.ErrInvalidSignature) {
		t.Fatalf("tampered authority error = %v, want invalid signature", err)
	}
}

func TestVerifierKeyringCannotMintAcceptedStageAuthority(t *testing.T) {
	now := time.Date(2026, 8, 30, 4, 15, 0, 0, time.UTC)
	signingKeys := map[string][]byte{"stage-key-7": bytes.Repeat([]byte{0x52}, 32)}
	verifierKeys, err := stageauthority.DeriveVerifierKeyring(signingKeys)
	if err != nil {
		t.Fatalf("DeriveVerifierKeyring: %v", err)
	}
	defer stageauthority.ClearKeyring(verifierKeys)
	validator, err := stageauthority.NewVerifier(
		verifierKeys,
		func() time.Time { return now.Add(time.Second) },
	)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	signer, err := stageauthority.NewSigner(signingKeys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	signed, err := signer.Sign(validAuthority(now))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if _, err := validator.Validate(signed, validBinding()); err != nil {
		t.Fatalf("Validate with public verifier keyring: %v", err)
	}

	forger, err := stageauthority.NewSigner(verifierKeys)
	if err != nil {
		t.Fatalf("NewSigner with public bytes: %v", err)
	}
	forged, err := forger.Sign(validAuthority(now))
	if err != nil {
		t.Fatalf("forge with public bytes: %v", err)
	}
	if _, err := validator.Validate(forged, validBinding()); !errors.Is(err, stageauthority.ErrInvalidSignature) {
		t.Fatalf("forged authority error = %v, want invalid signature", err)
	}
}

func TestStageAuthorityRejectsStaleOrMismatchedEpochs(t *testing.T) {
	now := time.Date(2026, 8, 30, 4, 30, 0, 0, time.UTC)
	key := bytes.Repeat([]byte{0x24}, 32)
	signer, err := stageauthority.NewSigner(map[string][]byte{"stage-key-7": key})
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	signed, err := signer.Sign(validAuthority(now))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	for _, testCase := range []struct {
		name    string
		now     time.Time
		binding func() stageauthority.RuntimeBinding
		want    error
	}{
		{
			name:    "absolute expiry",
			now:     now.Add(31 * time.Second),
			binding: validBinding,
			want:    stageauthority.ErrStale,
		},
		{
			name: "WorkerInstance epoch",
			now:  now.Add(time.Second),
			binding: func() stageauthority.RuntimeBinding {
				binding := validBinding()
				binding.WorkerInstanceEpoch++
				return binding
			},
			want: stageauthority.ErrRuntimeMismatch,
		},
		{
			name: "Device epoch",
			now:  now.Add(time.Second),
			binding: func() stageauthority.RuntimeBinding {
				binding := validBinding()
				binding.Devices[0].Epoch++
				return binding
			},
			want: stageauthority.ErrRuntimeMismatch,
		},
		{
			name: "member epoch",
			now:  now.Add(time.Second),
			binding: func() stageauthority.RuntimeBinding {
				binding := validBinding()
				binding.Members[0].Epoch++
				return binding
			},
			want: stageauthority.ErrRuntimeMismatch,
		},
		{
			name: "ModelRuntime epoch",
			now:  now.Add(time.Second),
			binding: func() stageauthority.RuntimeBinding {
				binding := validBinding()
				binding.ModelRuntimeEpoch++
				return binding
			},
			want: stageauthority.ErrRuntimeMismatch,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			validator, validatorErr := stageauthority.NewValidator(
				map[string][]byte{"stage-key-7": key},
				func() time.Time { return testCase.now },
			)
			if validatorErr != nil {
				t.Fatalf("NewValidator: %v", validatorErr)
			}
			if _, validateErr := validator.Validate(signed, testCase.binding()); !errors.Is(validateErr, testCase.want) {
				t.Fatalf("Validate error = %v, want %v", validateErr, testCase.want)
			}
		})
	}
}

func TestStageAuthoritySignatureAndDigestAreCanonical(t *testing.T) {
	now := time.Date(2026, 8, 30, 5, 0, 0, 0, time.UTC)
	key := bytes.Repeat([]byte{0x18}, 32)
	signer, err := stageauthority.NewSigner(map[string][]byte{"stage-key-7": key})
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	left := validAuthority(now)
	right := proto.Clone(left).(*velav1.StageAuthority)
	right.Devices[0], right.Devices[1] = right.Devices[1], right.Devices[0]
	right.Members[0], right.Members[1] = right.Members[1], right.Members[0]

	signedLeft, err := signer.Sign(left)
	if err != nil {
		t.Fatalf("Sign left: %v", err)
	}
	signedRight, err := signer.Sign(right)
	if err != nil {
		t.Fatalf("Sign right: %v", err)
	}
	leftDigest, err := stageauthority.Digest(signedLeft)
	if err != nil {
		t.Fatalf("Digest left: %v", err)
	}
	rightDigest, err := stageauthority.Digest(signedRight)
	if err != nil {
		t.Fatalf("Digest right: %v", err)
	}
	if leftDigest != rightDigest || !bytes.Equal(signedLeft.Signature, signedRight.Signature) {
		t.Fatalf("canonical authority digests differ: %x != %x", leftDigest, rightDigest)
	}
}

func TestStageAuthorityRenewalRequiresStableIdentityAndMonotonicAuthority(t *testing.T) {
	now := time.Date(2026, 8, 30, 5, 30, 0, 0, time.UTC)
	key := bytes.Repeat([]byte{0x28}, 32)
	signer, err := stageauthority.NewSigner(map[string][]byte{"stage-key-7": key})
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	current, err := signer.Sign(validAuthority(now))
	if err != nil {
		t.Fatalf("Sign current: %v", err)
	}
	renewal := proto.Clone(current).(*velav1.StageAuthority)
	renewal.StageVersion++
	renewal.IssuedAt = timestamppb.New(now.Add(10 * time.Second))
	renewal.ExpiresAt = timestamppb.New(now.Add(time.Minute))
	renewal.MonotonicValidFor = durationpb.New(50 * time.Second)
	renewal, err = signer.Sign(renewal)
	if err != nil {
		t.Fatalf("Sign renewal: %v", err)
	}
	if err := stageauthority.ValidateRenewal(current, renewal); err != nil {
		t.Fatalf("ValidateRenewal: %v", err)
	}

	tests := map[string]func(*velav1.StageAuthority){
		"execution identity": func(authority *velav1.StageAuthority) {
			authority.StageLeaseId = "10000000-0000-0000-0000-000000000099"
		},
		"stage version": func(authority *velav1.StageAuthority) {
			authority.StageVersion = current.GetStageVersion() - 1
		},
		"issued at": func(authority *velav1.StageAuthority) {
			authority.IssuedAt = current.GetIssuedAt()
			authority.MonotonicValidFor = durationpb.New(time.Minute)
		},
		"expires at": func(authority *velav1.StageAuthority) {
			authority.ExpiresAt = current.GetExpiresAt()
			authority.MonotonicValidFor = durationpb.New(20 * time.Second)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := proto.Clone(renewal).(*velav1.StageAuthority)
			mutate(candidate)
			candidate, signErr := signer.Sign(candidate)
			if signErr != nil {
				t.Fatalf("Sign candidate: %v", signErr)
			}
			if renewalErr := stageauthority.ValidateRenewal(current, candidate); !errors.Is(renewalErr, stageauthority.ErrRenewalMismatch) {
				t.Fatalf("ValidateRenewal error = %v, want renewal mismatch", renewalErr)
			}
		})
	}
}

func validAuthority(now time.Time) *velav1.StageAuthority {
	return &velav1.StageAuthority{
		SchemaVersion:       1,
		JobId:               "10000000-0000-0000-0000-000000000001",
		AttemptId:           "10000000-0000-0000-0000-000000000002",
		StageRunId:          "10000000-0000-0000-0000-000000000003",
		StageAttemptId:      "10000000-0000-0000-0000-000000000004",
		StageAllocationId:   "10000000-0000-0000-0000-000000000005",
		StageLeaseId:        "10000000-0000-0000-0000-000000000006",
		AttemptFence:        3,
		StageFence:          4,
		StageVersion:        5,
		WorkerInstanceId:    "20000000-0000-0000-0000-000000000001",
		WorkerInstanceEpoch: 7,
		DeviceSetDigest:     bytes.Repeat([]byte{0xa1}, 32),
		Devices: []*velav1.StageAuthorityDeviceEpoch{
			{DeviceId: "30000000-0000-0000-0000-000000000002", DeviceEpoch: 12},
			{DeviceId: "30000000-0000-0000-0000-000000000001", DeviceEpoch: 11},
		},
		MembershipDigest: bytes.Repeat([]byte{0xb2}, 32),
		Members: []*velav1.StageAuthorityMemberEpoch{
			{WorkerMemberId: "40000000-0000-0000-0000-000000000002", MemberEpoch: 14, ModelRuntimeEpoch: 18,
				IdentityDigest: bytes.Repeat([]byte{0xb3}, 32)},
			{WorkerMemberId: "40000000-0000-0000-0000-000000000001", MemberEpoch: 13, ModelRuntimeEpoch: 17,
				IdentityDigest: bytes.Repeat([]byte{0xb4}, 32)},
		},
		ModelResidencyId:              "50000000-0000-0000-0000-000000000001",
		ModelRuntimeIdentity:          "dit-runtime-7",
		ModelRuntimeBarrierGeneration: 23,
		StageProfileRevisionId:        "60000000-0000-0000-0000-000000000001",
		CapacityObservationSequence:   19,
		CapacityVector: map[string]int64{
			"active_stage_slots": 1,
			"gpu_count":          2,
		},
		LeaseToken:          bytes.Repeat([]byte{0xc3}, 32),
		ExecutionNonce:      bytes.Repeat([]byte{0xd4}, 32),
		ExecutionSpecDigest: bytes.Repeat([]byte{0xe5}, 32),
		SigningKeyId:        "stage-key-7",
		IssuedAt:            timestamppb.New(now),
		ExpiresAt:           timestamppb.New(now.Add(30 * time.Second)),
		MonotonicValidFor:   durationpb.New(30 * time.Second),
	}
}

func validBinding() stageauthority.RuntimeBinding {
	return stageauthority.RuntimeBinding{
		WorkerInstanceID:    "20000000-0000-0000-0000-000000000001",
		WorkerInstanceEpoch: 7,
		WorkerMemberID:      "40000000-0000-0000-0000-000000000001",
		WorkerMemberEpoch:   13,
		DeviceSetDigest:     bytes.Repeat([]byte{0xa1}, 32),
		Devices: []stageauthority.DeviceEpoch{
			{ID: "30000000-0000-0000-0000-000000000001", Epoch: 11},
			{ID: "30000000-0000-0000-0000-000000000002", Epoch: 12},
		},
		MembershipDigest: bytes.Repeat([]byte{0xb2}, 32),
		Members: []stageauthority.MemberEpoch{
			{ID: "40000000-0000-0000-0000-000000000001", Epoch: 13},
			{ID: "40000000-0000-0000-0000-000000000002", Epoch: 14},
		},
		ModelResidencyID:       "50000000-0000-0000-0000-000000000001",
		ModelRuntimeIdentity:   "dit-runtime-7",
		ModelRuntimeEpoch:      17,
		StageProfileRevisionID: "60000000-0000-0000-0000-000000000001",
	}
}

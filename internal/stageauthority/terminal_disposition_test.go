package stageauthority_test

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTerminalDispositionAuthenticatesCompleteHistoricalScope(t *testing.T) {
	signer, verifier, original, value, now := terminalDispositionFixture(t)
	unchanged := proto.Clone(value)
	signed, err := signer.SignTerminalDisposition(value)
	if err != nil || !proto.Equal(value, unchanged) {
		t.Fatalf("sign mutated input or failed: %v", err)
	}
	verified, err := verifier.ValidateTerminalDisposition(signed, original, value.GetWorkerMemberId(), 7)
	if err != nil || verified.Disposition.GetCutoff() != 11 || len(verified.Disposition.GetAllocations()) != 2 {
		t.Fatalf("validate historical cutoff: %+v, %v", verified, err)
	}
	if _, err := verifier.ValidateEnvelope(original); !errors.Is(err, stageauthority.ErrStale) {
		t.Fatalf("fixture must use expired execution authority: %v", err)
	}
	reordered := proto.Clone(signed).(*velav1.StageTerminalDisposition)
	slices.Reverse(reordered.Devices)
	slices.Reverse(reordered.Allocations)
	for _, allocation := range reordered.Allocations {
		slices.Reverse(allocation.Members)
	}
	replayed, err := verifier.ValidateTerminalDisposition(reordered, original, value.GetWorkerMemberId(), 7)
	if err != nil || replayed.Digest != verified.Digest {
		t.Fatalf("set ordering changed canonical digest: %v", err)
	}
	for _, session := range []int64{0, 6, 8} {
		if _, err := verifier.ValidateTerminalDisposition(signed, original, value.GetWorkerMemberId(), session); err == nil {
			t.Fatalf("accepted mismatched session %d", session)
		}
	}
	if _, err := verifier.ValidateTerminalDisposition(signed, original, uuid.NewString(), 7); err == nil {
		t.Fatal("accepted another Worker member")
	}
	changedOriginal := proto.Clone(original).(*velav1.StageAuthority)
	changedOriginal.ExecutionSpecDigest[0] ^= 1
	changedOriginal, err = signer.Sign(changedOriginal)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.ValidateTerminalDisposition(signed, changedOriginal, value.GetWorkerMemberId(), 7); err == nil {
		t.Fatal("accepted substituted validly signed original")
	}
	for _, instant := range []time.Time{now.Add(-time.Nanosecond), now.Add(time.Minute), now.Add(time.Hour)} {
		validator, err := stageauthority.NewValidator(map[string][]byte{"stage-key-7": bytes.Repeat([]byte{0x42}, 32)}, func() time.Time { return instant })
		if err != nil {
			t.Fatal(err)
		}
		if _, err := validator.ValidateTerminalDispositionEnvelope(signed); !errors.Is(err, stageauthority.ErrStale) {
			t.Fatalf("accepted future or expired disposition at %s: %v", instant, err)
		}
	}
}

func TestTerminalDispositionSignatureRecoveryDoesNotGrantFreshness(t *testing.T) {
	signer, verifier, _, value, now := terminalDispositionFixture(t)
	signed, err := signer.SignTerminalDisposition(value)
	if err != nil {
		t.Fatal(err)
	}
	original, err := verifier.ValidateTerminalDispositionEnvelope(signed)
	if err != nil {
		t.Fatal(err)
	}
	for _, instant := range []time.Time{now.Add(-time.Hour), now.Add(time.Hour)} {
		validator, err := stageauthority.NewValidator(map[string][]byte{"stage-key-7": bytes.Repeat([]byte{0x42}, 32)}, func() time.Time { return instant })
		if err != nil {
			t.Fatal(err)
		}
		if _, err := validator.ValidateTerminalDispositionEnvelope(signed); !errors.Is(err, stageauthority.ErrStale) {
			t.Fatalf("historical fact acquired current freshness: %v", err)
		}
		retained, err := validator.ValidateTerminalDispositionSignature(signed)
		if err != nil || retained.Digest != original.Digest {
			t.Fatalf("retained restrictive signature: %+v %v", retained, err)
		}
	}
	signed.Signature[0] ^= 1
	if _, err := verifier.ValidateTerminalDispositionSignature(signed); !errors.Is(err, stageauthority.ErrInvalidSignature) {
		t.Fatalf("signature recovery accepted tampering: %v", err)
	}
}

func TestTerminalDispositionRejectsTamperingAndIncompleteScope(t *testing.T) {
	signer, verifier, original, value, _ := terminalDispositionFixture(t)
	signed, err := signer.SignTerminalDisposition(value)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*velav1.StageTerminalDisposition){
		"cutoff":          func(d *velav1.StageTerminalDisposition) { d.Cutoff++ },
		"original digest": func(d *velav1.StageTerminalDisposition) { d.OriginalAuthorityDigest[0] ^= 1 },
		"organization":    func(d *velav1.StageTerminalDisposition) { d.OrganizationId = uuid.NewString() },
		"project":         func(d *velav1.StageTerminalDisposition) { d.ProjectId = uuid.NewString() },
		"terminal state": func(d *velav1.StageTerminalDisposition) {
			d.TerminalState = velav1.StageTerminalState_STAGE_TERMINAL_STATE_CANCELED
		},
		"stage fence":                       func(d *velav1.StageTerminalDisposition) { d.StageFence++ },
		"stage version":                     func(d *velav1.StageTerminalDisposition) { d.StageVersion++ },
		"worker epoch":                      func(d *velav1.StageTerminalDisposition) { d.WorkerInstanceEpoch++ },
		"session":                           func(d *velav1.StageTerminalDisposition) { d.ControlSessionEpoch++ },
		"device epoch":                      func(d *velav1.StageTerminalDisposition) { d.Devices[0].DeviceEpoch++ },
		"device set":                        func(d *velav1.StageTerminalDisposition) { d.DeviceSetDigest[0] ^= 1 },
		"membership":                        func(d *velav1.StageTerminalDisposition) { d.MembershipDigest[0] ^= 1 },
		"expiry":                            func(d *velav1.StageTerminalDisposition) { d.ExpiresAt.Seconds++ },
		"unseen allocation runtime":         func(d *velav1.StageTerminalDisposition) { d.Allocations[1].Members[0].ModelRuntimeEpoch++ },
		"unseen allocation residency":       func(d *velav1.StageTerminalDisposition) { d.Allocations[1].ModelResidencyId = uuid.NewString() },
		"unseen allocation profile":         func(d *velav1.StageTerminalDisposition) { d.Allocations[1].StageProfileRevisionId = uuid.NewString() },
		"unseen allocation barrier":         func(d *velav1.StageTerminalDisposition) { d.Allocations[1].BarrierGeneration++ },
		"device subset":                     func(d *velav1.StageTerminalDisposition) { d.Allocations[1].Members[0].DeviceSubsetDigest[0] ^= 1 },
		"key id":                            func(d *velav1.StageTerminalDisposition) { d.SigningKeyId = "unknown-key" },
		"signature":                         func(d *velav1.StageTerminalDisposition) { d.Signature[0] ^= 1 },
		"truncate history and lower cutoff": func(d *velav1.StageTerminalDisposition) { d.Allocations = d.Allocations[:1]; d.Cutoff = 7 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(signed).(*velav1.StageTerminalDisposition)
			mutate(changed)
			if _, err := verifier.ValidateTerminalDisposition(changed, original, value.GetWorkerMemberId(), 7); err == nil {
				t.Fatal("accepted tampered disposition")
			}
		})
	}
	for name, mutate := range map[string]func(*velav1.StageTerminalDisposition){
		"schema":        func(d *velav1.StageTerminalDisposition) { d.SchemaVersion++ },
		"unknown state": func(d *velav1.StageTerminalDisposition) { d.TerminalState = 99 },
		"retain cannot be signed": func(d *velav1.StageTerminalDisposition) {
			d.InputDisposition = velav1.StageInputDisposition_STAGE_INPUT_DISPOSITION_RETAIN
		},
		"missing original":      func(d *velav1.StageTerminalDisposition) { d.Allocations = d.Allocations[1:] },
		"missing unseen member": func(d *velav1.StageTerminalDisposition) { d.Allocations[1].Members = d.Allocations[1].Members[:1] },
		"duplicate allocation":  func(d *velav1.StageTerminalDisposition) { d.Allocations = append(d.Allocations, d.Allocations[1]) },
		"duplicate member":      func(d *velav1.StageTerminalDisposition) { d.Allocations[0].Members[1] = d.Allocations[0].Members[0] },
		"duplicate device":      func(d *velav1.StageTerminalDisposition) { d.Devices = append(d.Devices, d.Devices[0]) },
		"no history":            func(d *velav1.StageTerminalDisposition) { d.Allocations = nil },
		"missing digest":        func(d *velav1.StageTerminalDisposition) { d.Allocations[1].Members[0].IdentityDigest = nil },
		"nil allocation":        func(d *velav1.StageTerminalDisposition) { d.Allocations[1] = nil },
		"nil member":            func(d *velav1.StageTerminalDisposition) { d.Allocations[1].Members[0] = nil },
		"nil device":            func(d *velav1.StageTerminalDisposition) { d.Devices[0] = nil },
		"invalid cutoff":        func(d *velav1.StageTerminalDisposition) { d.Cutoff = 7 },
		"unbounded lifetime": func(d *velav1.StageTerminalDisposition) {
			d.ExpiresAt = timestamppb.New(d.ObservedAt.AsTime().Add(time.Hour))
		},
		"unknown top field": func(d *velav1.StageTerminalDisposition) { d.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 1}) },
		"unknown nested field": func(d *velav1.StageTerminalDisposition) {
			d.Allocations[1].Members[0].ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 1})
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(value).(*velav1.StageTerminalDisposition)
			mutate(changed)
			if _, err := signer.SignTerminalDisposition(changed); !errors.Is(err, stageauthority.ErrInvalidTerminalDisposition) {
				t.Fatalf("signed malformed facts: %v", err)
			}
		})
	}
}

func TestTerminalDispositionUsesDedicatedSignatureDomainAndPublicVerifier(t *testing.T) {
	signer, verifier, original, value, _ := terminalDispositionFixture(t)
	signed, err := signer.SignTerminalDisposition(value)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := stageauthority.DeriveVerifierKeyring(map[string][]byte{"stage-key-7": bytes.Repeat([]byte{0x42}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	unsigned := proto.Clone(signed).(*velav1.StageTerminalDisposition)
	unsigned.Signature = nil
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(unsigned)
	if err != nil {
		t.Fatal(err)
	}
	public := ed25519.PublicKey(keys["stage-key-7"])
	if ed25519.Verify(public, wire, signed.GetSignature()) ||
		!ed25519.Verify(public, append([]byte("vela-stage-terminal-disposition-v1\x00"), wire...), signed.GetSignature()) {
		t.Fatal("signature domain is missing or incorrect")
	}
	signed.Signature = slices.Clone(original.GetSignature())
	if _, err := verifier.ValidateTerminalDispositionEnvelope(signed); !errors.Is(err, stageauthority.ErrInvalidSignature) {
		t.Fatalf("accepted execution signature as disposition: %v", err)
	}
	forger, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	forged, err := forger.SignTerminalDisposition(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.ValidateTerminalDispositionEnvelope(forged); !errors.Is(err, stageauthority.ErrInvalidSignature) {
		t.Fatalf("accepted signature minted from public key bytes: %v", err)
	}
}

func TestTerminalDispositionRejectsSignedFactsForDifferentOriginalScope(t *testing.T) {
	signer, verifier, original, value, _ := terminalDispositionFixture(t)
	for name, mutate := range map[string]func(*velav1.StageTerminalDisposition){
		"Job":                func(d *velav1.StageTerminalDisposition) { d.JobId = uuid.NewString() },
		"Attempt":            func(d *velav1.StageTerminalDisposition) { d.AttemptId = uuid.NewString() },
		"StageRun":           func(d *velav1.StageTerminalDisposition) { d.StageRunId = uuid.NewString() },
		"Worker":             func(d *velav1.StageTerminalDisposition) { d.WorkerInstanceId = uuid.NewString() },
		"Worker epoch":       func(d *velav1.StageTerminalDisposition) { d.WorkerInstanceEpoch++ },
		"version regression": func(d *velav1.StageTerminalDisposition) { d.StageVersion = original.GetStageVersion() },
		"fence regression":   func(d *velav1.StageTerminalDisposition) { d.StageFence = original.GetStageFence() - 1 },
		"original runtime":   func(d *velav1.StageTerminalDisposition) { d.Allocations[0].Members[0].ModelRuntimeEpoch++ },
		"original nonce": func(d *velav1.StageTerminalDisposition) {
			d.Allocations[0].ExecutionNonce = bytes.Repeat([]byte{0x91}, 32)
		},
		"original barrier":  func(d *velav1.StageTerminalDisposition) { d.Allocations[0].BarrierGeneration++ },
		"original sequence": func(d *velav1.StageTerminalDisposition) { d.Allocations[0].ExecutionSequence++ },
		"device binding":    func(d *velav1.StageTerminalDisposition) { d.Devices[0].DeviceEpoch++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(value).(*velav1.StageTerminalDisposition)
			mutate(changed)
			signed, err := signer.SignTerminalDisposition(changed)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := verifier.ValidateTerminalDisposition(signed, original, value.GetWorkerMemberId(), 7); err == nil {
				t.Fatal("accepted valid signature for mismatched original scope")
			}
		})
	}
}

func TestTerminalDispositionBoundsFitControlTransport(t *testing.T) {
	signer, verifier, _, value, _ := terminalDispositionFixture(t)
	first := value.Allocations[0]
	for len(first.Members) < 64 {
		member := proto.Clone(first.Members[0]).(*velav1.StageTerminalMember)
		member.WorkerMemberId = uuid.NewString()
		first.Members = append(first.Members, member)
	}
	value.Allocations = []*velav1.StageTerminalAllocation{first}
	for len(value.Allocations) < 256 {
		allocation := proto.Clone(first).(*velav1.StageTerminalAllocation)
		allocation.StageAttemptId, allocation.StageAllocationId, allocation.StageLeaseId = uuid.NewString(), uuid.NewString(), uuid.NewString()
		allocation.ExecutionSequence += int64(len(value.Allocations))
		value.Allocations = append(value.Allocations, allocation)
	}
	value.Cutoff = value.Allocations[255].ExecutionSequence
	signed, err := signer.SignTerminalDisposition(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.ValidateTerminalDispositionEnvelope(signed); err != nil {
		t.Fatal(err)
	}
	if size := proto.Size(signed); size >= 3<<20 {
		t.Fatalf("maximum history exceeds transport budget: %d", size)
	}
	value.Allocations = append(value.Allocations, first)
	if _, err := signer.SignTerminalDisposition(value); err == nil {
		t.Fatal("accepted unbounded history")
	}
}

func terminalDispositionFixture(t *testing.T) (*stageauthority.Signer, *stageauthority.Validator, *velav1.StageAuthority, *velav1.StageTerminalDisposition, time.Time) {
	t.Helper()
	issuedAt := time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC)
	now := issuedAt.Add(2 * time.Minute)
	keys := map[string][]byte{"stage-key-7": bytes.Repeat([]byte{0x42}, 32)}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	public, err := stageauthority.DeriveVerifierKeyring(keys)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := stageauthority.NewVerifier(public, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	input := validAuthority(issuedAt)
	input.SchemaVersion, input.ExecutionSequence = stageauthority.SchemaVersionV2, 7
	original, err := signer.Sign(input)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := stageauthority.Digest(original)
	if err != nil {
		t.Fatal(err)
	}
	d := &velav1.StageTerminalDisposition{
		SchemaVersion: 1, InputDisposition: velav1.StageInputDisposition_STAGE_INPUT_DISPOSITION_INPUTS_UNUSED,
		OriginalAuthorityDigest: digest[:], OrganizationId: uuid.NewString(), ProjectId: uuid.NewString(),
		JobId: original.GetJobId(), AttemptId: original.GetAttemptId(), StageRunId: original.GetStageRunId(),
		StageAttemptId: original.GetStageAttemptId(), StageAllocationId: original.GetStageAllocationId(), StageLeaseId: original.GetStageLeaseId(),
		TerminalState: velav1.StageTerminalState_STAGE_TERMINAL_STATE_FAILED, StageFence: original.GetStageFence() + 1, StageVersion: original.GetStageVersion() + 1,
		WorkerInstanceId: original.GetWorkerInstanceId(), WorkerInstanceEpoch: original.GetWorkerInstanceEpoch(),
		WorkerMemberId: original.GetMembers()[0].GetWorkerMemberId(), ControlSessionEpoch: 7,
		DeviceSetDigest: original.GetDeviceSetDigest(), MembershipDigest: original.GetMembershipDigest(), Devices: original.GetDevices(), Cutoff: 11,
		ObservedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(time.Minute)), SigningKeyId: "stage-key-7",
	}
	allocation := &velav1.StageTerminalAllocation{
		StageAttemptId: original.GetStageAttemptId(), StageAllocationId: original.GetStageAllocationId(), StageLeaseId: original.GetStageLeaseId(),
		ExecutionSequence: 7, ExecutionNonce: original.GetExecutionNonce(), ModelResidencyId: original.GetModelResidencyId(),
		ModelRuntimeIdentity: original.GetModelRuntimeIdentity(), BarrierGeneration: original.GetModelRuntimeBarrierGeneration(), StageProfileRevisionId: original.GetStageProfileRevisionId(),
	}
	for _, member := range original.GetMembers() {
		allocation.Members = append(allocation.Members, &velav1.StageTerminalMember{
			WorkerMemberId: member.GetWorkerMemberId(), MemberEpoch: member.GetMemberEpoch(), ModelRuntimeEpoch: member.GetModelRuntimeEpoch(),
			IdentityDigest: member.GetIdentityDigest(), DeviceSubsetDigest: bytes.Repeat([]byte{0x53}, 32),
		})
	}
	retry := proto.Clone(allocation).(*velav1.StageTerminalAllocation)
	retry.StageAttemptId, retry.StageAllocationId, retry.StageLeaseId = uuid.NewString(), uuid.NewString(), uuid.NewString()
	retry.ExecutionSequence, retry.BarrierGeneration = 11, retry.BarrierGeneration+5
	retry.ModelResidencyId, retry.StageProfileRevisionId = uuid.NewString(), uuid.NewString()
	for _, member := range retry.Members {
		member.ModelRuntimeEpoch += 9
	}
	d.Allocations = []*velav1.StageTerminalAllocation{allocation, retry}
	return signer, verifier, original, d, now
}

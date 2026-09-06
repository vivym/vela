package journalbinding

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testBinding() *velav1.WorkerBootstrapBinding {
	requestID := uuid.NewString()
	return &velav1.WorkerBootstrapBinding{
		SchemaVersion: SchemaVersion,
		Claim: &velav1.WorkerBootstrapClaim{
			RequestId: requestID, WorkerInstanceId: uuid.NewString(), WorkerInstanceEpoch: 3,
			WorkerMemberId: uuid.NewString(), WorkerMemberEpoch: 5, NodeIdentity: "node-a",
			ActorIdentity: "node-agent:node-a", BundleDigest: bytes.Repeat([]byte{1}, sha256.Size),
			ClaimedAt: timestamppb.New(time.Date(2026, 9, 6, 1, 0, 0, 0, time.UTC)),
		},
		Pair: &velav1.WorkerBootstrapJournalPair{
			RequestId: requestID, ActorIdentity: "node-agent:node-a",
			WorkerJournalId: uuid.NewString(), RuntimeJournalId: uuid.NewString(),
			WorkerScope: bytes.Repeat([]byte{2}, sha256.Size), RuntimeScope: bytes.Repeat([]byte{3}, sha256.Size),
			RecordedAt: timestamppb.New(time.Date(2026, 9, 6, 1, 1, 0, 0, time.UTC)),
		},
	}
}

func testKeys(t *testing.T) (*Signer, *Verifier) {
	t.Helper()
	seed := bytes.Repeat([]byte{7}, ed25519.SeedSize)
	signer, err := NewSigner("registry-1", seed)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string][]byte{"registry-1": ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)}
	verifier, err := NewVerifier(keys)
	if err != nil {
		t.Fatal(err)
	}
	clear(seed)
	clear(keys["registry-1"])
	delete(keys, "registry-1")
	return signer, verifier
}

func TestBindingSignatureAndJournalIdentity(t *testing.T) {
	signer, verifier := testKeys(t)
	input := testBinding()
	original := proto.Clone(input)
	signed, err := signer.Sign(input)
	if err != nil || !proto.Equal(input, original) {
		t.Fatalf("sign mutated input or failed: %v", err)
	}
	verified, err := verifier.Verify(signed)
	if err != nil || !proto.Equal(verified, signed) {
		t.Fatalf("verify: %v", err)
	}
	verified.Claim.NodeIdentity = "mutated"
	if signed.Claim.NodeIdentity != "node-a" {
		t.Fatal("verification shared mutable data")
	}
	for _, kind := range []JournalKind{WorkerJournal, RuntimeJournal} {
		journal := Journal{WorkerInstanceID: input.Claim.WorkerInstanceId, WorkerInstanceEpoch: 3,
			WorkerMemberID: input.Claim.WorkerMemberId, WorkerMemberEpoch: 5}
		if kind == WorkerJournal {
			journal.JournalID = uuid.MustParse(input.Pair.WorkerJournalId)
			copy(journal.Scope[:], input.Pair.WorkerScope)
		} else {
			journal.JournalID = uuid.MustParse(input.Pair.RuntimeJournalId)
			copy(journal.Scope[:], input.Pair.RuntimeScope)
		}
		if err := verifier.VerifyJournal(signed, kind, journal); err != nil {
			t.Fatal(err)
		}
		for name, change := range map[string]func(*Journal){
			"worker":       func(j *Journal) { j.WorkerInstanceID = uuid.NewString() },
			"worker epoch": func(j *Journal) { j.WorkerInstanceEpoch++ },
			"member":       func(j *Journal) { j.WorkerMemberID = uuid.NewString() },
			"member epoch": func(j *Journal) { j.WorkerMemberEpoch++ },
			"id":           func(j *Journal) { j.JournalID = uuid.New() },
			"scope":        func(j *Journal) { j.Scope[0] ^= 1 },
		} {
			t.Run(name, func(t *testing.T) {
				changed := journal
				change(&changed)
				if verifier.VerifyJournal(signed, kind, changed) == nil {
					t.Fatal("accepted a different journal identity")
				}
			})
		}
		if verifier.VerifyJournal(signed, 0, journal) == nil {
			t.Fatal("accepted unknown journal kind")
		}
	}
}

func TestBindingRejectsTamperingAndMalformedMetadata(t *testing.T) {
	signer, verifier := testKeys(t)
	for name, change := range map[string]func(*velav1.WorkerBootstrapBinding){
		"schema":                func(v *velav1.WorkerBootstrapBinding) { v.SchemaVersion++ },
		"missing claim":         func(v *velav1.WorkerBootstrapBinding) { v.Claim = nil },
		"missing receipt":       func(v *velav1.WorkerBootstrapBinding) { v.Pair = nil },
		"request mismatch":      func(v *velav1.WorkerBootstrapBinding) { v.Pair.RequestId = uuid.NewString() },
		"actor mismatch":        func(v *velav1.WorkerBootstrapBinding) { v.Pair.ActorIdentity = "other" },
		"nil worker":            func(v *velav1.WorkerBootstrapBinding) { v.Claim.WorkerInstanceId = uuid.Nil.String() },
		"noncanonical uuid":     func(v *velav1.WorkerBootstrapBinding) { v.Claim.WorkerMemberId = "URN:UUID:" + v.Claim.WorkerMemberId },
		"zero epoch":            func(v *velav1.WorkerBootstrapBinding) { v.Claim.WorkerInstanceEpoch = 0 },
		"negative member epoch": func(v *velav1.WorkerBootstrapBinding) { v.Claim.WorkerMemberEpoch = -1 },
		"empty node":            func(v *velav1.WorkerBootstrapBinding) { v.Claim.NodeIdentity = "" },
		"bad text":              func(v *velav1.WorkerBootstrapBinding) { v.Claim.NodeIdentity = "node\n" },
		"invalid utf8":          func(v *velav1.WorkerBootstrapBinding) { v.Claim.NodeIdentity = "\xff" },
		"missing claim time":    func(v *velav1.WorkerBootstrapBinding) { v.Claim.ClaimedAt = nil },
		"missing receipt time":  func(v *velav1.WorkerBootstrapBinding) { v.Pair.RecordedAt = nil },
		"zero time":             func(v *velav1.WorkerBootstrapBinding) { v.Pair.RecordedAt = timestamppb.New(time.Time{}) },
		"invalid time":          func(v *velav1.WorkerBootstrapBinding) { v.Pair.RecordedAt.Nanos = -1 },
		"same journals":         func(v *velav1.WorkerBootstrapBinding) { v.Pair.RuntimeJournalId = v.Pair.WorkerJournalId },
		"zero digest":           func(v *velav1.WorkerBootstrapBinding) { clear(v.Claim.BundleDigest) },
		"short scope":           func(v *velav1.WorkerBootstrapBinding) { v.Pair.WorkerScope = []byte{1} },
		"zero scope":            func(v *velav1.WorkerBootstrapBinding) { clear(v.Pair.RuntimeScope) },
		"oversize": func(v *velav1.WorkerBootstrapBinding) {
			v.Claim.NodeIdentity = string(bytes.Repeat([]byte{'a'}, MaximumBytes))
		},
		"unknown envelope":  func(v *velav1.WorkerBootstrapBinding) { v.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown claim":     func(v *velav1.WorkerBootstrapBinding) { v.Claim.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown receipt":   func(v *velav1.WorkerBootstrapBinding) { v.Pair.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown timestamp": func(v *velav1.WorkerBootstrapBinding) { v.Pair.RecordedAt.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			input := testBinding()
			signed, err := signer.Sign(input)
			if err != nil {
				t.Fatal(err)
			}
			change(input)
			if _, err := signer.Sign(input); err == nil {
				t.Fatal("signed invalid metadata")
			}
			change(signed)
			if _, err := verifier.Verify(signed); err == nil {
				t.Fatal("accepted tampered metadata")
			}
		})
	}
	for name, change := range map[string]func(*velav1.WorkerBootstrapBinding){
		"valid node":        func(v *velav1.WorkerBootstrapBinding) { v.Claim.NodeIdentity = "node-b" },
		"valid epoch":       func(v *velav1.WorkerBootstrapBinding) { v.Claim.WorkerMemberEpoch++ },
		"signature":         func(v *velav1.WorkerBootstrapBinding) { v.Signature[0] ^= 1 },
		"missing signature": func(v *velav1.WorkerBootstrapBinding) { v.Signature = nil },
		"unknown key":       func(v *velav1.WorkerBootstrapBinding) { v.SigningKeyId = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			signed, err := signer.Sign(testBinding())
			if err != nil {
				t.Fatal(err)
			}
			change(signed)
			if _, err := verifier.Verify(signed); err == nil {
				t.Fatal("accepted tampered signature or content")
			}
		})
	}
}

func TestBindingDomainAndDefensiveConfiguration(t *testing.T) {
	signer, verifier := testKeys(t)
	for _, invalid := range []*Signer{nil, {}, {keyID: "test"}} {
		if _, err := invalid.Sign(testBinding()); err == nil {
			t.Fatal("accepted unconfigured signer")
		}
	}
	if _, err := signer.Sign(nil); err == nil {
		t.Fatal("signed nil")
	}
	signed, err := signer.Sign(testBinding())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := signer.Sign(signed); err == nil {
		t.Fatal("silently replaced an existing signature")
	}
	for _, invalid := range []*Verifier{nil, {}, {keys: map[string]ed25519.PublicKey{"registry-1": {1}}}} {
		if _, err := invalid.Verify(signed); err == nil {
			t.Fatal("accepted unconfigured verifier")
		}
	}
	if _, err := verifier.Verify(nil); err == nil {
		t.Fatal("verified nil")
	}
	unsigned := proto.Clone(signed).(*velav1.WorkerBootstrapBinding)
	unsigned.Signature = nil
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(unsigned)
	if err != nil {
		t.Fatal(err)
	}
	for _, domain := range []string{"", "vela-stage-authority-v1\x00"} {
		signed.Signature = ed25519.Sign(signer.key, append([]byte(domain), wire...))
		if _, err := verifier.Verify(signed); err == nil {
			t.Fatal("accepted wrong signature domain")
		}
	}
	for _, seed := range [][]byte{nil, make([]byte, ed25519.SeedSize-1), make([]byte, ed25519.PrivateKeySize)} {
		if _, err := NewSigner("test", seed); err == nil {
			t.Fatal("accepted invalid seed length")
		}
	}
	if _, err := NewSigner(" test", make([]byte, ed25519.SeedSize)); err == nil {
		t.Fatal("accepted invalid signer key id")
	}
	for _, keys := range []map[string][]byte{nil, {}, {"": make([]byte, 32)}, {"test": {1}}} {
		if _, err := NewVerifier(keys); err == nil {
			t.Fatal("accepted invalid public key configuration")
		}
	}
	wrong, err := NewVerifier(map[string][]byte{"registry-1": ed25519.NewKeyFromSeed(make([]byte, 32)).Public().(ed25519.PublicKey)})
	if err != nil {
		t.Fatal(err)
	}
	signed, err = signer.Sign(testBinding())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrong.Verify(signed); err == nil {
		t.Fatal("accepted wrong public key")
	}
}

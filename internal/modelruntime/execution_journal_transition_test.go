package modelruntime_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func transitionFixture(t *testing.T) (*executionFloorFixture, string) {
	t.Helper()
	directory := privateExecutionStateDirectory(t)
	return durableExecutionFixture(t, directory, true, "", 9, time.Date(2026, 9, 8, 8, 0, 0, 0, time.UTC)), directory
}

func applyJournalMutation(t *testing.T, f *executionFloorFixture, mutation modelruntime.ExecutionMutationForTest) {
	t.Helper()
	if err := modelruntime.ApplyExecutionMutationForTest(f.supervisor, mutation); err != nil {
		t.Fatal(err)
	}
}

func assertJournalMutationRejected(t *testing.T, f *executionFloorFixture, directory string, mutation modelruntime.ExecutionMutationForTest) {
	t.Helper()
	path := filepath.Join(directory, "execution-admission.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	syncs := 0
	restore := modelruntime.SetExecutionStateSyncHookForTest(f.supervisor, func(sync func() error) error {
		syncs++
		return sync()
	})
	err = modelruntime.ApplyExecutionMutationForTest(f.supervisor, mutation)
	restore()
	if err == nil || syncs != 0 {
		t.Fatalf("invalid %s mutation: error=%v syncs=%d", mutation.Kind, err, syncs)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	afterInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	memory, err := modelruntime.ExecutionJournalDocumentForTest(f.supervisor)
	if err != nil || !bytes.Equal(before, after) || !bytes.Equal(before, memory) || !os.SameFile(info, afterInfo) {
		t.Fatalf("rejected %s mutation changed live or durable state: %v", mutation.Kind, err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 2 {
		t.Fatalf("rejected mutation left temporary files: %v %v", entries, err)
	}
}

func TestJournalTransitionIndependentlyValidatesAdmissionAndFloor(t *testing.T) {
	for _, kind := range []string{"admit", "floor"} {
		for _, invalid := range []string{"signature", "scope", "expired", "nil"} {
			t.Run(kind+"/"+invalid, func(t *testing.T) {
				f, directory := transitionFixture(t)
				mutation := modelruntime.ExecutionMutationForTest{Kind: kind,
					Authority: stageauthority.Verified{Authority: proto.Clone(f.authorities[0]).(*velav1.StageAuthority)}, Disposition: f.disposition(t)}
				switch invalid {
				case "signature":
					mutation.Authority.Authority.Signature[0] ^= 1
					mutation.Disposition.Signature[0] ^= 1
				case "scope":
					mutation.Authority.Authority.WorkerInstanceEpoch++
					mutation.Disposition.WorkerInstanceEpoch++
					var err error
					mutation.Authority.Authority, err = f.signer.Sign(mutation.Authority.Authority)
					if err != nil {
						t.Fatal(err)
					}
					mutation.Disposition, err = f.signer.SignTerminalDisposition(mutation.Disposition)
					if err != nil {
						t.Fatal(err)
					}
				case "expired":
					f.clock.Advance(time.Hour)
				case "nil":
					mutation.Authority.Authority, mutation.Disposition = nil, nil
				}
				assertJournalMutationRejected(t, f, directory, mutation)
				// Refusal at this boundary leaves the owner available for legal work.
				if kind == "admit" {
					mutation.Authority.Authority = f.authority(t, 0, 12)
				} else {
					mutation.Disposition = f.disposition(t)
				}
				applyJournalMutation(t, f, mutation)
			})
		}
	}
}

func TestJournalTransitionRejectsForgedVerifiedMetadata(t *testing.T) {
	for _, kind := range []string{"candidates", "seal", "health", "drain"} {
		t.Run(kind, func(t *testing.T) {
			f, directory := transitionFixture(t)
			authority := f.authorities[0]
			verified, err := f.validator.ValidateEnvelope(authority)
			if err != nil {
				t.Fatal(err)
			}
			applyJournalMutation(t, f, modelruntime.ExecutionMutationForTest{Kind: "admit", Authority: verified})
			applyJournalMutation(t, f, modelruntime.ExecutionMutationForTest{Kind: "candidates", Authority: verified, Confirmed: &verified})
			manifest := []byte(`{"output":"exact-version"}`)
			digest := sha256.Sum256(manifest)
			receipt := &velav1.LocalMaterializationReceipt{OutputManifestJson: manifest, ManifestSha256: digest[:],
				ReceiptId: uuid.NewSHA1(uuid.NameSpaceOID, append([]byte(authority.GetStageLeaseId()+"\x00"), digest[:]...)).String(),
				SealedAt:  timestamppb.New(f.clock.Now()), TotalSizeBytes: 1}
			mutation := modelruntime.ExecutionMutationForTest{Kind: kind, Authority: verified, Confirmed: &verified,
				Receipt: receipt, Health: workerHealthEvidence(f, true), Observed: f.clock.Now(),
				Drain: modelruntime.BackendDrain{Contract: modelruntime.ExecutionDrainContract, AuthorityDigest: verified.Digest, ExecutionSequence: authority.GetExecutionSequence()}}
			bad := mutation
			bad.Authority.Authority = proto.Clone(authority).(*velav1.StageAuthority)
			bad.Authority.Authority.Signature[0] ^= 1
			assertJournalMutationRejected(t, f, directory, bad)
			if kind == "candidates" {
				bad = mutation
				badConfirmed := verified
				badConfirmed.Authority = proto.Clone(authority).(*velav1.StageAuthority)
				badConfirmed.Authority.Signature[0] ^= 1
				bad.Confirmed = &badConfirmed
				assertJournalMutationRejected(t, f, directory, bad)
			}
			if kind == "health" {
				bad = mutation
				bad.Health = nil
				assertJournalMutationRejected(t, f, directory, bad)
			}
			if kind == "drain" {
				bad = mutation
				bad.Authority.Digest[0] ^= 1
				bad.Drain.AuthorityDigest = bad.Authority.Digest
				assertJournalMutationRejected(t, f, directory, bad)
			}
			// A forged wrapper digest is discarded; valid signed bytes determine the
			// identity. Historical metadata remains recordable after expiration.
			mutation.Authority.Digest[0] ^= 1
			f.clock.Advance(time.Hour)
			applyJournalMutation(t, f, mutation)
			if kind != "drain" {
				syncs := 0
				restore := modelruntime.SetExecutionStateSyncHookForTest(f.supervisor, func(sync func() error) error { syncs++; return sync() })
				applyJournalMutation(t, f, mutation)
				restore()
				if syncs != 0 {
					t.Fatalf("exact replay wrote %d checkpoints", syncs)
				}
			}
		})
	}
}

func TestJournalTransitionRejectsWholeCandidateWithoutAliasing(t *testing.T) {
	for _, kind := range []string{"invalid-history", "abort-candidates", "invalid-schema", "invalid-id", "invalid-scope", "invalid-root", "invalid-lock", "invalid-proof"} {
		t.Run(kind, func(t *testing.T) {
			f, directory := transitionFixture(t)
			verified, err := f.validator.ValidateEnvelope(f.authorities[0])
			if err != nil {
				t.Fatal(err)
			}
			applyJournalMutation(t, f, modelruntime.ExecutionMutationForTest{Kind: "admit", Authority: verified})
			mutation := modelruntime.ExecutionMutationForTest{Kind: kind, Authority: verified, Confirmed: &verified}
			assertJournalMutationRejected(t, f, directory, mutation)
			mutation.Kind = "candidates"
			applyJournalMutation(t, f, mutation)
		})
	}
}

func TestJournalTransitionExpiryBeforeWriteDoesNotPoisonAdmission(t *testing.T) {
	for _, kind := range []string{"admit", "floor"} {
		t.Run(kind, func(t *testing.T) {
			f, directory := transitionFixture(t)
			path := filepath.Join(directory, "execution-admission.json")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			validator, err := stageauthority.NewValidator(map[string][]byte{"stage-key-9": bytes.Repeat([]byte{0x5a}, 32)}, func() time.Time {
				calls++
				// Floor uses this verifier twice; Service admission uses its own
				// verifier first. Only the final owner check observes expiration.
				if kind == "floor" && calls == 1 {
					return f.clock.Now()
				}
				return f.clock.Now().Add(time.Hour)
			})
			if err != nil {
				t.Fatal(err)
			}
			restore := modelruntime.SetExecutionJournalValidatorForTest(f.supervisor, validator)
			if kind == "admit" {
				response, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{
					Authority: f.authorities[0], ExecutionSpec: runtimeExecutionSpec()})
				if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
					t.Fatalf("expiration at owner admission: %v %v", response, err)
				}
			} else if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); !errors.Is(err, stageauthority.ErrStale) {
				t.Fatalf("expiration at owner floor: %v", err)
			}
			restore()
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) || f.backend.calls.Load() != 0 || calls == 0 {
				t.Fatalf("expired owner check changed durable state or entered backend: %v", err)
			}
			if kind == "admit" {
				prepareFloorRuntime(t, f.supervisor, f.authorities[0])
			} else if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
				t.Fatalf("expiration poisoned subsequent legal floor: %v", err)
			}
		})
	}
}
